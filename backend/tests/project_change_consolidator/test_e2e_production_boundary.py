"""Phase E: fake V3 production run → completed run → shadow Consolidator (fake provider, 0 model calls).

Proves that a shadow run reads a real completed run through ``run_storage``,
consolidates across regions with correct hint identity, and leaves every
production artifact and every production reader byte-identical.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.project_change_consolidator import contracts as C
from backend.app.services.project_change_consolidator.shadow import run_shadow
from backend.app.services.project_change_consolidator.source_view import load_completed_run
from backend.app.services.project_change_consolidator.storage import SHADOW_DIR_NAME, ShadowStore
from backend.tests.project_change_consolidator import consolidator_fixture as cf
from backend.tests.project_change_consolidator import responses as R
from backend.tests.project_change_v3 import generic_fixture as gf


def _tree(root: Path, skip: str = "") -> dict[str, str]:
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file() and not (skip and skip in p.parts)}


@pytest.fixture
def env(tmp_path, monkeypatch):
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

    set_test_provider(FakeProvider(handlers=cf.v3_handlers()))
    from backend.app.api.routers import project_change_preview, stage_comparison

    app = FastAPI()
    for r in (stage_comparison.router, project_change_preview.router, project_change_preview.availability_router):
        app.include_router(r)
    try:
        yield {**built, "client": TestClient(app), "root": comparison_root}
    finally:
        reset_test_provider()


def _readers(env) -> dict[str, str]:
    from backend.app.services.project_change_catalog.catalog import build_catalog

    client, sid = env["client"], env["session_id"]
    out = {
        "pair_changes": client.get(f"/api/stage-comparison/sessions/{sid}/pairs/{gf.PAIR_ID}/production/changes").text,
        "object_view": client.get(f"/api/stage-comparison/objects/{gf.OBJECT_ID}/project-changes").text,
        "catalog": json.dumps(build_catalog(), ensure_ascii=False, sort_keys=True),
    }
    return {k: hashlib.sha256(v.encode()).hexdigest() for k, v in out.items()}


def consolidate(call_id, pair_id, data, schema, images):
    if data["mode"] == C.MODE_SINGLETON_REVIEW:
        return R.keep_all(data, channel="DOCUMENTARY_CHANGE", flags=["DOCUMENTARY_SUSPECTED"])
    return R.merge(data, material={(h["hint_key"]["region_id"], h["hint_key"]["hint_id"]) for h in data["hints"]
                                   if h["kind"] == "SOURCE_CONFLICT"})


def test_shadow_consolidation_of_a_completed_production_run(env):
    from backend.app.services.project_change_v3.provider import FakeProvider
    from backend.app.services.stage_comparison import paths

    client, sid = env["client"], env["session_id"]
    state = client.post(f"/api/stage-comparison/sessions/{sid}/pairs/{gf.PAIR_ID}/production/run",
                        json={"input_mode": "DOCUMENT"}).json()
    assert state["reason_code"] == "v3_completed", state
    run_id = state["run_id"]
    production = paths.production_dir(sid, gf.PAIR_ID)
    before_tree, before_readers = _tree(production), _readers(env)

    bundle = load_completed_run(sid, gf.PAIR_ID, run_id)
    table = bundle.hint_table()
    assert [e.ref for e in table.entries] == ["R-002/H001", "R-003/H001"]  # same H001, two identities

    fake = FakeProvider(handlers={C.STAGE: consolidate})
    root = production / SHADOW_DIR_NAME
    manifest = run_shadow(bundle, fake, root, max_calls=12,
                          watch_roots=[production / "runs" / run_id, production / "current_run.json"])
    assert manifest["state"] == "COMPLETED", manifest["reason_code"]
    assert [c["stage"] for c in fake.calls] == ["CONSOLIDATE", "CONSOLIDATE"]

    # production artifacts and every production reader are byte-identical
    assert _tree(production, skip=SHADOW_DIR_NAME) == before_tree
    assert _readers(env) == before_readers

    loaded = ShadowStore(root).load_completed(run_id, manifest["consolidator_run_id"],
                                              source_result_sha256=bundle.result_sha256)
    result = loaded["result"]
    [merged] = [x for x in result["engineering_changes"] if x["origin"] == "CONSOLIDATED"]
    assert merged["lineage"]["member_ids"] == ["PC-R-001-C001", "PC-R-002-C001"]
    assert merged["lineage"]["member_regions"] == {"PC-R-001-C001": "R-001", "PC-R-002-C001": "R-002"}
    conflict = merged["open_conflicts"][0]["hint"]
    assert (conflict["region_id"], conflict["hint_id"]) == ("R-003", "H001")  # not R-002/H001
    assert "расчёт расхода П1 не приведён" in conflict["missing_proof_or_conflict"]
    assert [x["projectchange_id"] for x in result["documentary_changes"]] == ["PC-R-003-C001"]
    assert result["source"]["source_run_id"] == run_id


def test_shadow_does_not_run_without_the_flag(env):
    """The ordinary production run never starts a shadow run (flag OFF by default)."""
    from backend.app.services.stage_comparison import paths

    client, sid = env["client"], env["session_id"]
    client.post(f"/api/stage-comparison/sessions/{sid}/pairs/{gf.PAIR_ID}/production/run", json={"input_mode": "DOCUMENT"})
    assert not (paths.production_dir(sid, gf.PAIR_ID) / SHADOW_DIR_NAME).exists()
