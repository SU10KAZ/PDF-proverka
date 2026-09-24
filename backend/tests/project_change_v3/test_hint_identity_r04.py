"""R-04: a V3 hint is {region_id, hint_id}; the same H001 in several regions never collides in the UI."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.project_change_v3.hint_identity import EXACT, ORDINAL_FALLBACK, hint_identities
from backend.app.services.project_change_v3.presentation import _evidence_id
from backend.tests.project_change_v3 import generic_fixture as gf


def _hint(hid, text):
    return {"hint_id": hid, "kind": "UNRESOLVED_HINT", "engineering_subject": text, "suspected_change": text,
            "old_pages": [1], "new_pages": [1], "evidence_items": [], "missing_proof_or_conflict": text}


RESULT = {"run_id": "r1", "unresolved_hints": [_hint("H001", "a"), _hint("H002", "b"), _hint("H001", "c")]}
MINER = {"run_id": "r1", "regions": [{"region_id": "A-R001", "unresolved_hints": [_hint("H001", "a"), _hint("H002", "b")]},
                                     {"region_id": "A-R002", "unresolved_hints": [_hint("H001", "c")]}]}


def test_same_hint_id_in_two_regions_gets_two_exact_identities():
    ids = hint_identities(RESULT, MINER)
    assert [i["key"] for i in ids] == ["A-R001/H001", "A-R001/H002", "A-R002/H001"]
    assert {i["status"] for i in ids} == {EXACT}


@pytest.mark.parametrize("miner", [
    None,
    {**MINER, "run_id": "other"},
    {"run_id": "r1", "regions": [MINER["regions"][1], MINER["regions"][0]]},   # order differs → content mismatch
    {"run_id": "r1", "regions": MINER["regions"][:1]},                         # count differs
])
def test_no_region_is_guessed_when_it_cannot_be_proven(miner):
    ids = hint_identities(RESULT, miner)
    assert [i["key"] for i in ids] == ["#1/H001", "#2/H002", "#3/H001"]
    assert all(i["status"] == ORDINAL_FALLBACK and i["region_id"] == "" for i in ids)


# ------------------------------------------------------------------ production path
@pytest.fixture
def env(tmp_path, monkeypatch):
    from backend.tests.project_change_consolidator import consolidator_fixture as cf

    comparison_root = tmp_path / "comparison"
    comparison_root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(comparison_root))
    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "1")
    monkeypatch.delenv("PROJECTCHANGE_CONSOLIDATOR_SHADOW", raising=False)
    built = gf.build_comparison(tmp_path)
    from backend.app.services.project_change_v3 import scope

    monkeypatch.setattr(scope, "_object_stage_paths", lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])})
    from backend.app.services.project_change_v3.provider import FakeProvider, reset_test_provider, set_test_provider

    set_test_provider(FakeProvider(handlers=cf.v3_handlers()))  # H001 in R-002 AND in R-003
    from backend.app.api.routers import project_change_preview, stage_comparison

    app = FastAPI()
    for r in (stage_comparison.router, project_change_preview.router, project_change_preview.availability_router):
        app.include_router(r)
    client = TestClient(app)
    state = client.post(f"/api/stage-comparison/sessions/{built['session_id']}/pairs/{gf.PAIR_ID}/production/run",
                        json={"input_mode": "DOCUMENT"}).json()
    assert state["reason_code"] == "v3_completed"
    try:
        yield {**built, "client": client, "run_id": state["run_id"], "root": comparison_root}
    finally:
        reset_test_provider()


def _changes(env):
    return env["client"].get(f"/api/stage-comparison/sessions/{env['session_id']}/pairs/{gf.PAIR_ID}"
                             "/production/changes").json()


def _crop(env, e):
    response = env["client"].get(e["image_url"])
    assert response.status_code == 200, response.text
    return response.content


def test_repeated_h001_opens_the_correct_evidence(env):
    body = _changes(env)
    hints = {h["hint_ref"]: h for h in body["unresolved_hints"]}
    assert set(hints) == {"R-002/H001", "R-003/H001"}
    assert len({h["id"] for h in body["unresolved_hints"]}) == 2
    assert all(env["run_id"] in h["id"] and h["identity_status"] == "EXACT" for h in body["unresolved_hints"])
    ev_002, ev_003 = hints["R-002/H001"]["evidence"][0], hints["R-003/H001"]["evidence"][0]
    assert ev_002["id"] != ev_003["id"]
    assert (ev_002["side"], ev_002["page"], ev_002["block_id"]) == ("OLD", 2, "o2_text")
    assert (ev_003["side"], ev_003["page"], ev_003["block_id"]) == ("NEW", 1, "n1_table")
    # R-003/H001 cites the same table block as the R-001 card: identical crops prove the right fragment opened.
    card = next(i for i in body["project_changes"] if i["projectchange_id"] == "PC-R-001-C001")
    card_table = next(e for e in card["evidence"] if e["block_id"] == "n1_table")
    assert _crop(env, ev_003) == _crop(env, card_table)
    assert _crop(env, ev_002) != _crop(env, ev_003)


def test_card_evidence_ids_are_unchanged(env):
    body = _changes(env)
    for item in body["project_changes"]:
        for index, e in enumerate(item["evidence"]):
            assert e["id"] == _evidence_id(env["session_id"], gf.PAIR_ID, env["run_id"], item["projectchange_id"], index)


def test_historical_run_without_miner_results_stays_readable_and_unambiguous(env):
    from backend.app.services.stage_comparison import paths

    run_dir = paths.production_dir(env["session_id"], gf.PAIR_ID) / "runs" / env["run_id"]
    miner = run_dir / "project_change_v3_miner_results.json"
    miner.chmod(0o644)
    miner.unlink()
    body = _changes(env)
    assert [h["identity_status"] for h in body["unresolved_hints"]] == [ORDINAL_FALLBACK, ORDINAL_FALLBACK]
    assert [h["region_id"] for h in body["unresolved_hints"]] == ["", ""]
    assert len({h["id"] for h in body["unresolved_hints"]}) == 2
    ev = [h["evidence"][0] for h in body["unresolved_hints"]]
    assert ev[0]["id"] != ev[1]["id"]
    assert _crop(env, ev[1]) != _crop(env, ev[0])


# ------------------------------------------------------------------ frozen DEV5 (skipped where the corpus is absent)
CA = Path("/home/coder/auditmanager/corpus-audits")
ASTRA = CA / "20260924_dev5_astra_v31_final_freeze"
OPUS = CA / "20260921_v3_dev5_opus_freeze_a631b49aaaac4db0af66a495c155c629" / "artifacts"


def _dev5(side):
    if side == "ASTRA":
        if not (ASTRA / "FINAL_RESULT.json").is_file():
            pytest.skip("frozen DEV5 Astra corpus not on this machine")
        result = json.loads((ASTRA / "FINAL_RESULT.json").read_text(encoding="utf-8"))
        ckpt = json.loads((ASTRA / "frozen/miner/checkpoint_14_regions.json").read_text(encoding="utf-8"))
        miner = {"run_id": result["run_id"], "regions": [
            {"region_id": r["region_id"], "unresolved_hints": r["result"]["unresolved_hints"]} for r in ckpt["regions"]]}
        return result, miner
    if not (OPUS / "project_change_v3_result.json").is_file():
        pytest.skip("frozen DEV5 Opus corpus not on this machine")
    return (json.loads((OPUS / "project_change_v3_result.json").read_text(encoding="utf-8")),
            json.loads((OPUS / "project_change_v3_miner_results.json").read_text(encoding="utf-8")))


@pytest.mark.parametrize("side,hints", [("ASTRA", 52), ("OPUS", 39)])
def test_dev5_repeated_hint_ids_are_resolved_exactly(side, hints):
    result, miner = _dev5(side)
    ids = hint_identities(result, miner)
    raw = [i["hint_id"] for i in ids]
    assert len(ids) == hints and len(set(raw)) < len(raw)          # H001 … repeat across regions
    assert all(i["status"] == EXACT for i in ids) and len({i["key"] for i in ids}) == hints
    run = result["run_id"]
    old = [_evidence_id("s", "p", run, f"hint:{i['hint_id']}", 0) for i in ids]
    new = [_evidence_id("s", "p", run, f"hint:{i['key']}", 0) for i in ids]
    assert len(set(old)) < len(old)                                  # the R-04 collision
    assert len(set(new)) == len(new)                                 # fixed
