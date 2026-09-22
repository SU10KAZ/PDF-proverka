"""Miner checkpoint (engine 3.5.1): accepted, paid regions survive a later failure — zero model calls.

Before 3.5.1 the accepted Miner answers lived only in process memory until the
LAST region finished; the aborted DEV 5 run showed that a failure in the middle
loses every paid answer.  What must hold now:

* after EVERY accepted region the run-scoped checkpoint on disk holds that
  region and all earlier ones (atomic write, read back);
* a later provider failure fails the run, keeps the checkpoint and publishes
  nothing — the checkpoint is an internal artifact, not a partial result;
* a checkpoint that cannot be written stops the run (never a silent COMPLETED);
* one file per run: a second run never sees the regions of the first.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import engine
from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import _run, _stored
from backend.tests.project_change_v3.test_failure_states import env  # noqa: F401 — fixture


def _checkpoints(session_id: str) -> dict[str, dict]:
    from backend.app.services.project_change_v3 import run_storage

    active = run_storage.active_dir(session_id, gf.PAIR_ID)
    # During a Mapper/Miner callback only this generation is visible. Outside
    # a run, inspect all explicit generations, including failed ones.
    directories = [active] if active is not None else [
        run_storage.run_dir(session_id, gf.PAIR_ID, rid)
        for rid in run_storage.run_ids(session_id, gf.PAIR_ID)
    ]
    return {
        p.name: json.loads(p.read_text(encoding="utf-8"))
        for directory in directories
        for p in sorted((directory / "project_change_v3" / engine.MINER_CHECKPOINT_DIR).glob("*.json"))
    }


def _only_checkpoint(session_id: str) -> dict:
    (value,) = _checkpoints(session_id).values()
    return value


def _three_regions(snapshots: list, *, fail_third: bool) -> dict:
    """The fixture Mapper plus R-003; the Miner records the checkpoint it finds before each call."""
    handlers = gf.fake_handlers()
    mapping, mining = handlers["MAPPING"], handlers["MINING"]

    def mapping3(**kwargs):
        answer = mapping(**kwargs)
        third = json.loads(json.dumps(answer["regions"][1]))
        third.update(region_id="R-003", scope="Шум, второй регион")
        answer["regions"].append(third)
        return answer

    def mining3(**kwargs):
        region_id = kwargs["data"]["frozen_region"]["region_id"]
        snapshots.append((region_id, [r["region_id"] for r in _only_or_empty(kwargs["pair_id"])]))
        if region_id == "R-003":
            if fail_third:
                raise ProviderError("provider_gateway_error", "simulated provider failure on region 3")
            answer = mining(**{**kwargs, "data": {**kwargs["data"],
                                                   "frozen_region": {**kwargs["data"]["frozen_region"],
                                                                     "region_id": "R-002"}}})
            answer["region_id"] = "R-003"
            for hint in answer["unresolved_hints"]:
                hint["hint_id"] = "H003"
            return answer
        return mining(**kwargs)

    return {**handlers, "MAPPING": mapping3, "MINING": mining3}


_SESSION: dict[str, str] = {}


def _only_or_empty(_pair_id: str) -> list[dict]:
    found = _checkpoints(_SESSION["id"])
    assert len(found) <= 1, "one run wrote more than one checkpoint file"
    return next(iter(found.values()))["regions"] if found else []


@pytest.fixture
def session(env):  # noqa: F811
    _SESSION["id"] = env["session_id"]
    yield env
    _SESSION.clear()


def _use(handlers: dict) -> None:
    from backend.app.services.project_change_v3.provider import set_test_provider

    set_test_provider(FakeProvider(handlers=handlers))


def test_each_accepted_region_is_on_disk_before_the_next_call(session):
    snapshots: list = []
    _use(_three_regions(snapshots, fail_third=False))
    state = _run(session["session_id"])
    assert state["status"] in {"REVIEW", "COMPLETED"}, state
    # Before region k is asked, the checkpoint already holds regions 1..k-1.
    assert snapshots == [("R-001", []), ("R-002", ["R-001"]), ("R-003", ["R-001", "R-002"])]
    checkpoint = _only_checkpoint(session["session_id"])
    assert [r["region_id"] for r in checkpoint["regions"]] == ["R-001", "R-002", "R-003"]
    assert [r["ordinal"] for r in checkpoint["regions"]] == [1, 2, 3]


def test_provider_failure_on_region_3_keeps_regions_1_and_2_and_publishes_nothing(session):
    snapshots: list = []
    _use(_three_regions(snapshots, fail_third=True))
    state = _run(session["session_id"])
    assert state["status"] == "FAILED" and state["reason_code"] == "provider_gateway_error"
    checkpoint = _only_checkpoint(session["session_id"])
    assert checkpoint["run_id"] == state["run_id"] and checkpoint["pair_id"] == gf.PAIR_ID
    assert [r["region_id"] for r in checkpoint["regions"]] == ["R-001", "R-002"]
    assert checkpoint["regions"][0]["result"]["projectchanges"][0]["projectchange_id"] == "PC-R-001-C001"
    assert checkpoint["internal_run_artifact"] is True and checkpoint["published"] is False
    # The FAILED state points at the checkpoint; nothing is published as a result.
    ref = state["provenance"]["miner_checkpoint"]
    assert ref["regions"] == 2 and Path(ref["path"]).name == f"project_change_v3_miner_checkpoint_{state['run_id']}.json"
    for name in ("project_change_v3_result", "project_change_v3_miner_results", "project_change_v3_human_mapping_ui"):
        assert _stored(session["session_id"], name) is None, name
    assert state["legacy_invoked"] is False


def test_checkpoint_carries_what_the_audit_needs(session):
    _use(_three_regions([], fail_third=True))
    state = _run(session["session_id"])
    checkpoint = _only_checkpoint(session["session_id"])
    for key in ("run_id", "pair_id", "session_id", "engine_version", "provider", "model", "reasoning",
                "mapper_prompt_sha256", "miner_prompt_sha256", "dedupe_prompt_sha256", "miner_schema_sha256",
                "semantic_map_sha256", "source", "regions_total"):
        assert checkpoint.get(key), key
    assert checkpoint["engine_version"] == "3.5.2" and checkpoint["regions_total"] == 3
    assert checkpoint["source"]["old_pdf_sha256"] and checkpoint["source"]["new_pdf_sha256"]
    region = checkpoint["regions"][0]
    assert region["validation"] == "ACCEPTED" and region["accepted_call_id"] == f"{gf.PAIR_ID}_R-001"
    assert [a["attempt"] for a in region["attempts"]] == [1] and region["attempts"][0]["accepted"] is True
    assert len(region["model_visible_payload_sha256"]) == 64 and len(region["region_source_data_sha256"]) == 64
    assert region["model_visible_input"]["prompt_sha256"] == checkpoint["miner_prompt_sha256"]
    assert region["accepted_at"] and region["result"]["region_id"] == "R-001"
    assert state["status"] == "FAILED"


def test_a_checkpoint_that_cannot_be_written_stops_the_run(session, monkeypatch):
    def broken(path, checkpoint):
        raise OSError("disk full")

    monkeypatch.setattr(engine, "write_miner_checkpoint", broken)
    state = _run(session["session_id"])
    assert state["status"] == "FAILED" and state["reason_code"] == "miner_checkpoint_persistence_failed"
    assert _stored(session["session_id"], "project_change_v3_result") is None
    assert _stored(session["session_id"], "project_change_v3_miner_results") is None


def test_a_complete_run_builds_the_final_result_and_names_its_checkpoint(session):
    state = _run(session["session_id"])
    assert state["status"] == "REVIEW" and state["reason_code"] == "v3_completed"
    result = _stored(session["session_id"], "project_change_v3_result")
    miner = _stored(session["session_id"], "project_change_v3_miner_results")
    assert [c["projectchange_id"] for c in result["projectchanges"]] == ["PC-R-001-C001"]
    assert len(result["unresolved_hints"]) == 1
    ref = result["provenance"]["miner_checkpoint"]
    path = Path(ref["path"])
    import hashlib

    assert ref["regions"] == 2 and hashlib.sha256(path.read_bytes()).hexdigest() == ref["sha256"]
    assert miner["miner_checkpoint"] == ref
    checkpoint = json.loads(path.read_text(encoding="utf-8"))
    # The final Miner results are exactly what the checkpoint holds, region for region.
    assert [r["result"] for r in checkpoint["regions"]] == miner["regions"]


def test_every_run_has_its_own_checkpoint(session):
    first = _run(session["session_id"])
    second = _run(session["session_id"])
    assert first["run_id"] != second["run_id"]
    found = _checkpoints(session["session_id"])
    assert sorted(found) == sorted(f"project_change_v3_miner_checkpoint_{s['run_id']}.json" for s in (first, second))
    assert all(len(c["regions"]) == 2 for c in found.values())


def test_write_is_atomic_and_read_back(tmp_path):
    path = engine.miner_checkpoint_path(tmp_path, "run/../../escape")
    assert path.parent == tmp_path / engine.MINER_CHECKPOINT_DIR  # an odd run id never leaves the folder
    digest = engine.write_miner_checkpoint(path, {"run_id": "r", "regions": [{"region_id": "R-001"}]})
    import hashlib

    assert digest == hashlib.sha256(path.read_bytes()).hexdigest()
    assert not list(path.parent.glob("*.tmp"))
