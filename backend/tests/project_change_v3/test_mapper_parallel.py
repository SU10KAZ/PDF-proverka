"""Parallel portioned Mapper with synthetic PDFs and local fake providers only."""
from copy import deepcopy
import json
from pathlib import Path
import threading

import pytest

from backend.app.services.project_change_v3 import engine, parallel_calls, run_storage
from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError, set_test_provider
from backend.app.services.stage_comparison.ai.gateway import CancelToken
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env, _run, _stored  # noqa: F401
from backend.tests.project_change_v3.test_mapper_portions import _region


def prepare(built, monkeypatch, count):
    pages = deepcopy(gf.PAGES["NEW"])
    for page in range(3, count + 1):
        pages[page] = [(f"n{page}_text", "text", [0.1, 0.1, 0.8, 0.3], f"Synthetic page {page}")]
    monkeypatch.setitem(gf.PAGES, "NEW", pages)
    work = Path(built["right"]["pdf_path"]).parent
    gf._pdf(work / "document.pdf", "NEW")
    gf._markdown(work / "document.md", "NEW")
    gf._blocks(work / "blocks.json", "NEW")
    monkeypatch.setattr(engine, "mapper_portions_mod_single_envelope_error", lambda *_: "synthetic portioning")
    monkeypatch.setattr(engine, "mapper_portions_mod_fits",
                        lambda pair, rows: sum(p["side"] == "NEW" for p in rows) <= 1)
    monkeypatch.setenv(engine.UNMATCHED_REVIEW_ENV, "0")
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, "0")


def handlers():
    def mapping(**kwargs):
        new = [p["physical_page"] for p in kwargs["data"]["pages"] if p["side"] == "NEW"]
        return {"pair": gf.PAIR_ID, "regions": [_region("R", [1], new)],
                "unmatched_old": [2], "unmatched_new": [], "coverage_notes": []}

    def mining(**kwargs):
        return {"pair": gf.PAIR_ID, "region_id": kwargs["data"]["frozen_region"]["region_id"],
                "projectchanges": [], "unresolved_hints": [], "coverage_notes": []}

    return {"MAPPING": mapping, "MINING": mining}


def attempts(state):
    return [json.loads(Path(row["path"]).read_text()) for row in state["provenance"]["mapper_attempts"]]


@pytest.mark.parametrize("workers,count", [(10, 13), (10, 7), (1, 3)])
def test_bounded_mapping_preserves_portion_order_and_receipts(env, monkeypatch, workers, count):  # noqa: F811
    prepare(env, monkeypatch, count)
    if workers == 10:
        monkeypatch.delenv(parallel_calls.MAPPER_CONCURRENCY_ENV)
    else:
        monkeypatch.setenv(parallel_calls.MAPPER_CONCURRENCY_ENV, str(workers))
    width = min(workers, count)
    barrier = threading.Barrier(width)
    fast_saved = threading.Event()
    lock = threading.Lock()
    active = {"now": 0, "peak": 0}
    real_atomic = run_storage.atomic
    coordinator = threading.get_ident()
    saved_order = []

    def atomic(path, record):
        result = real_atomic(path, record)
        if isinstance(record, dict) and record.get("schema") == "projectchange_v3_mapper_attempt/1":
            assert threading.get_ident() == coordinator
            index = record["portion"]["index"]
            saved_order.append(index)
            if index == width:
                fast_saved.set()
        return result

    monkeypatch.setattr(run_storage, "atomic", atomic)

    class MapperFake(FakeProvider):
        last_transport = None

        def complete(self, **kwargs):
            if kwargs["stage"] != "MAPPING":
                return super().complete(**kwargs)
            assert run_storage.active_dir(env["session_id"], gf.PAIR_ID) is not None
            portion = kwargs["data"]["portion"]["index"]
            self.last_transport = {"marker": portion, "usage": {"input_tokens": portion}}
            with lock:
                active["now"] += 1
                active["peak"] = max(active["peak"], active["now"])
            try:
                if portion <= width:
                    barrier.wait(15)
                if portion == 1 and width > 1:
                    assert fast_saved.wait(15), "fast portion must persist while first is still running"
                return super().complete(**kwargs)
            finally:
                with lock:
                    active["now"] -= 1

    set_test_provider(MapperFake(handlers=handlers()))
    state = _run(env["session_id"])
    assert state["status"] in {"REVIEW", "COMPLETED"}, state.get("message")
    assert active == {"now": 0, "peak": width}
    assert state["provenance"]["mapper_concurrency"] == workers
    assert state["pipeline_stages"]["MAPPING"]["processed"] == count
    assert [r["portion"]["index"] for r in attempts(state)] == list(range(1, count + 1))
    assert all(r["accepted"] and r["transport_receipt"]["marker"] == r["portion"]["index"] for r in attempts(state))
    maps = _stored(env["session_id"], "project_change_v3_mapper_portions")["portion_maps"]
    assert [answer["regions"][0]["new_pages"] for answer in maps] == [[i] for i in range(1, count + 1)]
    merged = _stored(env["session_id"], "project_change_v3_semantic_map")
    assert [r["region_id"] for r in merged["regions"]] == [f"P{i:02}.R" for i in range(1, count + 1)]
    if width > 1:
        assert saved_order.index(width) < saved_order.index(1)


@pytest.mark.parametrize("failure", ["provider", "validation", "persistence", "cancel"])
def test_mapping_failure_drains_answers_and_never_starts_miner(env, monkeypatch, failure):  # noqa: F811
    prepare(env, monkeypatch, 8)
    monkeypatch.setenv(parallel_calls.MAPPER_CONCURRENCY_ENV, "3")
    barrier = threading.Barrier(3)
    stop = threading.Event()
    token = CancelToken()
    original_failure = engine._V3Failure
    real_atomic = run_storage.atomic
    calls = []

    class ObservedFailure(original_failure):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            stop.set()

    monkeypatch.setattr(engine, "_V3Failure", ObservedFailure)

    def atomic(path, record):
        if (failure == "persistence" and isinstance(record, dict) and record.get("schema") == "projectchange_v3_mapper_attempt/1"
                and record["portion"]["index"] == 1):
            raise OSError("synthetic disk failure")
        return real_atomic(path, record)

    monkeypatch.setattr(run_storage, "atomic", atomic)
    base = handlers()
    mapping = base["MAPPING"]

    def blocked(**kwargs):
        index = kwargs["data"]["portion"]["index"]
        calls.append(index)
        barrier.wait(15)
        answer = mapping(**kwargs)
        if index == 1:
            if failure == "provider":
                raise ProviderError("provider_timeout", "synthetic timeout")
            if failure == "validation":
                answer["pair"] = "wrong-pair"
            if failure == "cancel":
                token.cancel()
                stop.set()
        else:
            assert stop.wait(15)
        return answer

    base["MAPPING"] = blocked
    fake = FakeProvider(handlers=base)
    set_test_provider(fake)
    state = engine.run_v3_pipeline(session_id=env["session_id"], pair_id=gf.PAIR_ID, cancel_token=token)
    assert state["status"] == "FAILED"
    assert state["reason_code"] == {"provider": "provider_timeout", "validation": "mapper_validation_failed",
                                    "persistence": "mapper_attempt_persistence_failed", "cancel": "v3_cancelled"}[failure]
    assert sorted(calls) == [1, 2, 3]
    assert all(c["stage"] == "MAPPING" for c in fake.calls)
    assert {r["portion"]["index"] for r in attempts(state) if r["accepted"]} >= {2, 3}
    if failure == "validation":
        assert attempts(state)[0]["response"]["pair"] == "wrong-pair"
        assert attempts(state)[0]["accepted"] is False
    for name in ("project_change_v3_semantic_map", "project_change_v3_result"):
        assert _stored(env["session_id"], name) is None


@pytest.mark.parametrize("value", ["0", "11", "bad"])
def test_invalid_mapper_concurrency_stops_before_calls(env, monkeypatch, value):  # noqa: F811
    monkeypatch.setenv(parallel_calls.MAPPER_CONCURRENCY_ENV, value)
    fake = FakeProvider(handlers=handlers())
    set_test_provider(fake)
    state = _run(env["session_id"])
    assert state["reason_code"] == "mapper_concurrency_invalid"
    assert fake.calls == []
