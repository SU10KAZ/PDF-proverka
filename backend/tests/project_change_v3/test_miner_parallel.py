"""Real engine, synthetic documents and blocking fake providers; zero inference."""
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


def handlers(count):
    base = gf.fake_handlers()

    def mapping(**kwargs):
        result = base["MAPPING"](**kwargs)
        template, last = result["regions"]
        result["regions"] = [dict(deepcopy(template), region_id=f"P{i:03}") for i in range(1, count)]
        result["regions"].append(dict(last, region_id=f"P{count:03}"))
        return result

    def mining(**kwargs):
        region = kwargs["data"]["frozen_region"]
        rid = region["region_id"]
        data = {**kwargs["data"], "frozen_region": {**region,
                "region_id": "R-002" if rid == f"P{count:03}" else "R-001"}}
        result = base["MINING"](**{**kwargs, "data": data})
        result["region_id"] = rid
        for change in result["projectchanges"]:
            change["projectchange_id"] = f"PC-{rid}"
        return result

    def dedupe(**kwargs):
        return {"pair": gf.PAIR_ID, "decisions": [
            {"decision": "KEEP_SEPARATE", "projectchange_ids": [f"PC-P{i:03}"], "reason": "test"}
            for i in range(1, count)], "notes": []}

    return {"MAPPING": mapping, "MINING": mining, "DEDUPE": dedupe}


def checkpoint(built, state):
    return json.loads(Path(state["provenance"]["miner_checkpoint"]["path"]).read_text())


def test_ten_in_flight_persist_out_of_order_without_mixing_receipts_or_results(env, monkeypatch):  # noqa: F811
    monkeypatch.delenv(parallel_calls.MINER_CONCURRENCY_ENV)
    assert parallel_calls.concurrency(parallel_calls.MINER_CONCURRENCY_ENV) == 10
    barrier = threading.Barrier(10)
    tenth_saved = threading.Event()
    lock = threading.Lock()
    activity = {"active": 0, "peak": 0}
    writer_thread = threading.get_ident()
    writes = []
    real_write = engine.write_miner_checkpoint

    def write(path, value):
        assert threading.get_ident() == writer_thread
        rows = deepcopy(value["regions"])
        writes.append(rows)
        digest = real_write(path, value)
        if any(row["region_id"] == "P010" for row in rows):
            tenth_saved.set()
        return digest

    monkeypatch.setattr(engine, "write_miner_checkpoint", write)

    class ConcurrentFake(FakeProvider):
        last_transport = None
        last_response = None

        def complete(self, **kwargs):
            if kwargs["stage"] != "MINING":
                return super().complete(**kwargs)
            assert run_storage.active_dir(env["session_id"], gf.PAIR_ID) is not None
            rid = kwargs["data"]["frozen_region"]["region_id"]
            self.last_transport = {"transport": "fake", "marker": rid, "usage": {"input_tokens": int(rid[1:])}}
            with lock:
                activity["active"] += 1
                activity["peak"] = max(activity["peak"], activity["active"])
            try:
                if int(rid[1:]) <= 10:
                    barrier.wait(timeout=15)
                if rid == "P001":
                    assert tenth_saved.wait(15), "fast answer was not checkpointed while slow answer was running"
                return super().complete(**kwargs)
            finally:
                with lock:
                    activity["active"] -= 1

    set_test_provider(ConcurrentFake(handlers=handlers(13)))
    state = _run(env["session_id"])
    assert state["status"] == "REVIEW", state.get("message")
    assert activity == {"active": 0, "peak": 10}
    assert state["provenance"]["miner_concurrency"] == 10
    rows = checkpoint(env, state)["regions"]
    assert [row["region_id"] for row in rows] == [f"P{i:03}" for i in range(1, 14)]
    assert any(any(r["region_id"] == "P010" for r in w) and all(r["region_id"] != "P001" for r in w)
               for w in writes)
    assert all(row["transport_receipt"]["marker"] == row["region_id"] for row in rows)
    assert all(row["usage"]["input_tokens"] == row["ordinal"] for row in rows)
    result = _stored(env["session_id"], "project_change_v3_result")
    assert [c["projectchange_id"] for c in result["projectchanges"]] == [f"PC-P{i:03}" for i in range(1, 13)]
    assert state["pipeline_stages"]["MINING"]["processed"] == 13


@pytest.mark.parametrize("failure", ["provider", "checkpoint", "cancel"])
def test_failure_stops_admission_and_drains_in_flight_answers(env, monkeypatch, failure):  # noqa: F811
    monkeypatch.setenv(parallel_calls.MINER_CONCURRENCY_ENV, "3")
    barrier = threading.Barrier(3)
    stop_observed = threading.Event()
    token = CancelToken()
    base = handlers(8)
    mining = base["MINING"]
    real_write = engine.write_miner_checkpoint
    calls = []
    original_failure = engine._V3Failure

    class ObservedFailure(original_failure):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            stop_observed.set()

    monkeypatch.setattr(engine, "_V3Failure", ObservedFailure)

    def write(path, value):
        if failure == "checkpoint" and not stop_observed.is_set():
            raise OSError("synthetic disk failure")
        return real_write(path, value)

    monkeypatch.setattr(engine, "write_miner_checkpoint", write)

    def blocked(**kwargs):
        rid = kwargs["data"]["frozen_region"]["region_id"]
        calls.append(rid)
        barrier.wait(timeout=15)
        if rid == "P001":
            if failure == "provider":
                raise ProviderError("provider_timeout", "synthetic timeout")
            if failure == "cancel":
                token.cancel()
                stop_observed.set()
        else:
            assert stop_observed.wait(15)
        return mining(**kwargs)

    base["MINING"] = blocked
    set_test_provider(FakeProvider(handlers=base))
    state = engine.run_v3_pipeline(session_id=env["session_id"], pair_id=gf.PAIR_ID, cancel_token=token)
    assert state["status"] == "FAILED"
    assert state["reason_code"] == {"provider": "provider_timeout", "checkpoint": "miner_checkpoint_persistence_failed",
                                    "cancel": "v3_cancelled"}[failure]
    assert sorted(calls) == ["P001", "P002", "P003"]
    rows = checkpoint(env, state)["regions"]
    assert {r["region_id"] for r in rows} >= {"P002", "P003"}
    assert state["provenance"]["miner_attempt_store"]["saved"] == (2 if failure == "provider" else 3)
    assert _stored(env["session_id"], "project_change_v3_result") is None


@pytest.mark.parametrize("value", ["0", "11", "-1", "bad"])
def test_invalid_concurrency_fails_before_any_provider_call(env, monkeypatch, value):  # noqa: F811
    monkeypatch.setenv(parallel_calls.MINER_CONCURRENCY_ENV, value)
    provider = FakeProvider(handlers=handlers(2))
    set_test_provider(provider)
    state = _run(env["session_id"])
    assert state["reason_code"] == "miner_concurrency_invalid"
    assert provider.calls == []


def test_parallel_retry_keeps_the_same_input_and_own_receipt(env, monkeypatch):  # noqa: F811
    monkeypatch.setenv(parallel_calls.MINER_CONCURRENCY_ENV, "2")
    barrier = threading.Barrier(2)
    base = handlers(3)
    seen = []

    class RetryFake(FakeProvider):
        last_transport = None

        def complete(self, **kwargs):
            answer = super().complete(**kwargs)
            if kwargs["stage"] != "MINING":
                return answer
            rid = kwargs["data"]["frozen_region"]["region_id"]
            retry = "_RETRY_" in kwargs["call_id"]
            seen.append((rid, retry, deepcopy(kwargs)))
            self.last_transport = {"marker": kwargs["call_id"], "usage": {"input_tokens": 17}}
            if rid in {"P001", "P002"} and not retry:
                barrier.wait(15)
            if rid == "P001" and not retry:
                answer["projectchanges"][0]["old_pages"] = [999]
            return answer

    set_test_provider(RetryFake(handlers=base))
    state = _run(env["session_id"])
    assert state["status"] == "REVIEW", state.get("message")
    first, second = [kwargs for rid, _, kwargs in seen if rid == "P001"]
    assert first.pop("call_id") + "_RETRY_1" == second.pop("call_id")
    assert first == second
    row = checkpoint(env, state)["regions"][0]
    assert [a["validation"] for a in row["attempts"]] == ["REJECTED", "ACCEPTED"]
    assert row["transport_receipt"]["marker"] == row["accepted_call_id"]
    assert state["provenance"]["miner_attempt_store"]["saved"] == 4
