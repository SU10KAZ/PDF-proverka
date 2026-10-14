"""Review scheduling tested with synthetic documents, never paid providers."""
from copy import deepcopy
import json
from pathlib import Path
import threading

import pytest

from backend.app.services.project_change_v3 import engine, parallel_calls, run_storage
from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError, set_test_provider
from backend.app.services.stage_comparison.ai.gateway import CancelToken
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env, _run  # noqa: F401

STAGES = ["SOURCE_VERIFICATION", "UNMATCHED_REVIEW"]
CONCURRENCY = dict(zip(STAGES, [parallel_calls.VERIFICATION_CONCURRENCY_ENV,
                               parallel_calls.UNMATCHED_CONCURRENCY_ENV]))
LIMIT = dict(zip(STAGES, [engine.SOURCE_VERIFICATION_MAX_BATCHES_ENV,
                         engine.UNMATCHED_REVIEW_MAX_BATCHES_ENV]))
ARTIFACT = dict(zip(STAGES, ["project_change_v3_verification_results", "project_change_v3_supplemental_results"]))


def _stored(session_id, name="state"):
    path = run_storage.artifact_path(session_id, gf.PAIR_ID, name)
    return json.loads(path.read_text()) if path.exists() else None


def prepare(built, monkeypatch, stage, count):
    for key in LIMIT.values():
        monkeypatch.delenv(key, raising=False)
    for key in CONCURRENCY.values():
        monkeypatch.delenv(key, raising=False)
    handlers = gf.fake_handlers()
    if stage == "SOURCE_VERIFICATION":
        original = handlers["MINING"]

        def mining(**kwargs):
            answer = original(**kwargs)
            for change in answer["projectchanges"]:
                parameter = change["changed_parameters"][0]
                change["changed_parameters"] = [{**parameter, "name": f"Synthetic parameter {i}",
                                                 "old_value": f"old-synthetic-{i}", "new_value": f"new-synthetic-{i}"}
                                                 for i in range(count)]
            return answer

        handlers["MINING"] = mining

        def verify(**kwargs):
            return {"pair": gf.PAIR_ID, "results": [
                {"work_item_id": item["work_item_id"], "verdict": "CONFIRMED",
                 "old_value": item["parameter"]["old_value"], "new_value": item["parameter"]["new_value"],
                 "unit": item["parameter"]["unit"], "explanation": "Synthetic verification",
                 "evidence_refs": [{key: source[key] for key in ("side", "physical_page", "block_id")}
                                   for source in item["sources"]]}
                for item in kwargs["data"]["items"]]}

        handlers[stage] = verify
    else:
        monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, "0")
        pages = deepcopy(gf.PAGES["NEW"])
        for page in range(2, count + 2):
            pages[page] = [(f"n{page}_text", "text", [0.1, 0.1, 0.8, 0.3], "Приточная установка П1 расход 1200 м3/ч")]
        monkeypatch.setitem(gf.PAGES, "NEW", pages)
        work = Path(built["right"]["pdf_path"]).parent
        gf._pdf(work / "document.pdf", "NEW")
        gf._markdown(work / "document.md", "NEW")
        gf._blocks(work / "blocks.json", "NEW")
        original_mapping = handlers["MAPPING"]

        def mapping(**kwargs):
            answer = original_mapping(**kwargs)
            answer["regions"] = answer["regions"][:1]
            answer["regions"][0]["old_pages"] = [1, 2]
            answer["unmatched_new"] = list(range(2, count + 2))
            return answer

        handlers["MAPPING"] = mapping
        original_planner = engine.build_supplemental_batches
        monkeypatch.setattr(engine, "build_supplemental_batches",
                            lambda **kwargs: original_planner(**kwargs, max_primary_pages=1))
        template = []
        original_mining = handlers["MINING"]

        def mining(**kwargs):
            answer = original_mining(**kwargs)
            template[:] = deepcopy(answer["projectchanges"])
            return answer

        handlers["MINING"] = mining

        def unmatched(**kwargs):
            data = kwargs["data"]
            region = data["frozen_region"]
            page = data["coverage_review"]["primary_pages"][0]
            records = {(p["side"], p["physical_page"]): p for p in data["pages"]}
            change = deepcopy(template[0])
            change.update(projectchange_id=f"PC-{region['region_id']}-C001", new_pages=[page], old_pages=[1])
            change["evidence_items"] = [template[0]["evidence_items"][0],
                                        gf._evidence(records, "NEW", page, f"n{page}_text", "NEW_STATE", "1200 м3/ч")]
            change["modalities"] = ["TEXT"]
            return {"pair": gf.PAIR_ID, "region_id": region["region_id"], "projectchanges": [change],
                    "unresolved_hints": [], "coverage_notes": []}

        handlers[stage] = unmatched
        handlers["DEDUPE"] = lambda **kwargs: {"pair": gf.PAIR_ID, "decisions": [
            {"decision": "KEEP_SEPARATE", "projectchange_ids": [c["projectchange_id"]], "reason": "synthetic"}
            for c in kwargs["data"]["projectchanges"]], "notes": []}
    return handlers


def index_of(kwargs):
    if "_VERIFY-B" in kwargs["call_id"]:
        return int(kwargs["call_id"].rsplit("B", 1)[1])
    return int(kwargs["data"]["frozen_region"]["region_id"].rsplit("R", 1)[1])


def attempts(state, stage):
    return [json.loads(Path(row["path"]).read_text()) for row in state["provenance"].get("review_attempts", [])
            if row["stage"] == stage]


@pytest.mark.parametrize("stage", STAGES)
@pytest.mark.parametrize("workers,count", [(10, 13), (1, 3)])
def test_full_queue_bounded_concurrency_immediate_save_and_stable_merge(env, monkeypatch, stage, workers, count):
    handlers = prepare(env, monkeypatch, stage, count * 8 if stage == "SOURCE_VERIFICATION" else count)
    monkeypatch.setenv(CONCURRENCY[stage], str(workers))
    barrier = threading.Barrier(workers)
    fast_saved = threading.Event()
    later_started = threading.Event()
    lock = threading.Lock()
    active = {"now": 0, "peak": 0}
    subjects = []
    saved = []
    real_atomic = run_storage.atomic
    coordinator = threading.get_ident()

    def atomic(path, record):
        result = real_atomic(path, record)
        if isinstance(record, dict) and record.get("schema") == "projectchange_v3_review_attempt/1":
            assert threading.get_ident() == coordinator
            saved.append(record["batch_id"])
            if record["batch_id"].endswith(f"{workers:03}"):
                fast_saved.set()
        return result

    monkeypatch.setattr(run_storage, "atomic", atomic)

    class ReviewFake(FakeProvider):
        last_transport = None

        def complete(self, **kwargs):
            if kwargs["stage"] != stage:
                return super().complete(**kwargs)
            index = index_of(kwargs)
            self.last_transport = {"marker": index, "usage": {"input_tokens": index}}
            if stage == "UNMATCHED_REVIEW":
                subjects.append(deepcopy(kwargs["data"]["coverage_review"]["existing_subjects"]))
            with lock:
                active["now"] += 1
                active["peak"] = max(active["peak"], active["now"])
            try:
                if index <= workers:
                    barrier.wait(15)
                if index > workers:
                    later_started.set()
                if index == 1 and workers > 1:
                    assert fast_saved.wait(15)
                    assert later_started.wait(15), "refill must not wait for the whole first wave"
                return super().complete(**kwargs)
            finally:
                with lock:
                    active["now"] -= 1

    set_test_provider(ReviewFake(handlers=handlers))
    state = _run(env["session_id"])
    assert state["status"] in {"COMPLETED", "REVIEW"}, state["message"]
    assert active == {"now": 0, "peak": workers}
    rows = _stored(env["session_id"], ARTIFACT[stage])["batches"]
    assert len(rows) == count and all(r["status"] == "ACCEPTED" for r in rows)
    assert [r["batch_id"] for r in rows] == sorted(r["batch_id"] for r in rows)
    assert len(attempts(state, stage)) == count
    assert all(r["transport_receipt"]["marker"] == int(r["batch_id"][-3:]) for r in attempts(state, stage))
    if workers > 1:
        assert saved.index(rows[workers - 1]["batch_id"]) < saved.index(rows[0]["batch_id"])
    if stage == "SOURCE_VERIFICATION":
        assert state["quality_source_verification_pending"] == 0
        assert state["quality_independently_verified"] == count * 8
    else:
        assert len(subjects) == count and all(row == subjects[0] for row in subjects)
        assert len(subjects[0]) == 1, "supplemental additions cannot change another batch input"
        changes = _stored(env["session_id"], "project_change_v3_miner_results")["projectchanges"]
        assert [c["projectchange_id"] for c in changes[1:]] == [f"PC-U-R{i:03}-C001" for i in range(1, count + 1)]
        assert state["coverage_content_unmatched_pending"] == 0


@pytest.mark.parametrize("limit,expected,pending", [(None, 9, 0), ("4", 4, 37), ("0", 0, 69)])
def test_69_parameters_are_all_planned_unless_explicitly_capped(env, monkeypatch, limit, expected, pending):
    handlers = prepare(env, monkeypatch, "SOURCE_VERIFICATION", 69)
    if limit is not None:
        monkeypatch.setenv(engine.SOURCE_VERIFICATION_MAX_BATCHES_ENV, limit)
    set_test_provider(FakeProvider(handlers=handlers))
    state = _run(env["session_id"])
    assert state["status"] in {"COMPLETED", "REVIEW"}, state["message"]
    assert len(_stored(env["session_id"], ARTIFACT["SOURCE_VERIFICATION"])["batches"]) == expected
    assert state["quality_source_verification_pending"] == pending


@pytest.mark.parametrize("stage", STAGES)
@pytest.mark.parametrize("failure", ["provider", "validation", "persistence", "cancel"])
def test_failure_policy_cancellation_and_inflight_preservation(env, monkeypatch, stage, failure):
    handlers = prepare(env, monkeypatch, stage, 64 if stage == "SOURCE_VERIFICATION" else 8)
    monkeypatch.setenv(CONCURRENCY[stage], "3")
    barrier = threading.Barrier(3)
    stop = threading.Event()
    token = CancelToken()
    observed = []
    original_failure = engine._V3Failure
    real_atomic = run_storage.atomic
    base = handlers[stage]

    class ObservedFailure(original_failure):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            stop.set()

    monkeypatch.setattr(engine, "_V3Failure", ObservedFailure)

    def atomic(path, record):
        if (isinstance(record, dict) and record.get("schema") == "projectchange_v3_review_attempt/1"
                and record["batch_id"].endswith("001") and failure == "persistence"):
            raise OSError("synthetic disk failure")
        return real_atomic(path, record)

    monkeypatch.setattr(run_storage, "atomic", atomic)

    def blocked(**kwargs):
        index = index_of(kwargs)
        observed.append(index)
        if index <= 3:
            barrier.wait(15)
        answer = base(**kwargs)
        if index == 1:
            if failure == "cancel":
                token.cancel()
                stop.set()
            elif failure == "provider":
                raise ProviderError("provider_timeout", "synthetic timeout")
            elif failure == "validation":
                answer["pair"] = "wrong-pair"
        elif index <= 3 and failure in ("cancel", "persistence"):
            assert stop.wait(15)
        return answer

    handlers[stage] = blocked
    set_test_provider(FakeProvider(handlers=handlers))
    state = engine.run_v3_pipeline(session_id=env["session_id"], pair_id=gf.PAIR_ID, cancel_token=token)
    stored = attempts(state, stage)
    assert {r["batch_id"][-3:] for r in stored if r["status"] == "ACCEPTED"} >= {"002", "003"}
    if failure in ("persistence", "cancel"):
        assert state["status"] == "FAILED"
        assert state["reason_code"] == ("v3_cancelled" if failure == "cancel" else "review_attempt_persistence_failed")
        assert sorted(observed) == [1, 2, 3]
        assert _stored(env["session_id"], "project_change_v3_result") is None
    else:
        assert state["status"] == "REVIEW", state["message"]
        assert sorted(observed) == list(range(1, 9)), "a failed review batch keeps other review batches running"
        assert len(stored) == 8 and stored[0]["status"] == "FAILED"
        if failure == "validation":
            assert stored[0]["response"]["pair"] == "wrong-pair"
        if stage == "SOURCE_VERIFICATION":
            assert state["quality_source_verification_pending"] == 8
        else:
            assert state["coverage_content_unmatched_pending"] > 0


@pytest.mark.parametrize("key,value", [(key, value) for key in CONCURRENCY.values() for value in ("0", "11", "bad")]
                         + [(key, value) for key in LIMIT.values() for value in ("-1", "bad")])
def test_invalid_review_settings_fail_before_any_inference(env, monkeypatch, key, value):
    monkeypatch.setenv(key, value)
    fake = FakeProvider(handlers=gf.fake_handlers())
    set_test_provider(fake)
    state = _run(env["session_id"])
    assert state["reason_code"] == "review_settings_invalid"
    assert fake.calls == []
