"""UI binding: read-only «Исходные | Итоговые» over COMPLETED shadow runs (0 model calls)."""
from __future__ import annotations

import hashlib
import json
import shutil
import stat
from pathlib import Path

import pytest

from backend.app.services.project_change_consolidator import contracts as C
from backend.app.services.project_change_consolidator.shadow import run_shadow
from backend.app.services.project_change_consolidator.source_view import load_completed_run
from backend.app.services.project_change_consolidator.storage import SHADOW_DIR_NAME
from backend.tests.project_change_consolidator.test_e2e_production_boundary import consolidate, env  # noqa: F401
from backend.tests.project_change_v3 import generic_fixture as gf


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _production_run(env):
    client, sid = env["client"], env["session_id"]
    state = client.post(f"/api/stage-comparison/sessions/{sid}/pairs/{gf.PAIR_ID}/production/run",
                        json={"input_mode": "DOCUMENT"}).json()
    assert state["reason_code"] == "v3_completed"
    return state["run_id"]


def _shadow(env, run_id, root=None):
    from backend.app.services.project_change_v3.provider import FakeProvider
    from backend.app.services.stage_comparison import paths

    production = paths.production_dir(env["session_id"], gf.PAIR_ID)
    bundle = load_completed_run(env["session_id"], gf.PAIR_ID, run_id)
    manifest = run_shadow(bundle, FakeProvider(handlers={C.STAGE: consolidate}),
                          root or production / SHADOW_DIR_NAME, max_calls=12)
    assert manifest["state"] == "COMPLETED"
    return manifest


def _base(env):
    return f"/api/stage-comparison/sessions/{env['session_id']}/pairs/{gf.PAIR_ID}"


def test_no_consolidation_means_original_only(env):
    _production_run(env)
    body = env["client"].get(_base(env) + "/consolidated").json()
    assert body == {"schema": "projectchange-consolidations/1", "available": False, "consolidations": []}


def test_consolidated_view_of_a_production_run(env):
    client = env["client"]
    run_id = _production_run(env)
    manifest = _shadow(env, run_id)
    listing = client.get(_base(env) + "/consolidated").json()
    [entry] = listing["consolidations"]
    assert entry["source_kind"] == "PRODUCTION_RUN" and entry["source_is_current_run"] is True
    assert entry["consolidator_run_id"] == manifest["consolidator_run_id"]
    view = client.get(_base(env) + f"/consolidated/{run_id}/{manifest['consolidator_run_id']}").json()
    assert [c["id"] for c in view["original"]] == ["PC-R-001-C001", "PC-R-002-C001", "PC-R-003-C001"]
    merged = next(x for x in view["consolidated"] if x["kind"] == "CONSOLIDATED")
    assert [m["id"] for m in merged["lineage"]["members"]] == ["PC-R-001-C001", "PC-R-002-C001"]
    # R-04 continuity: the consolidated card's evidence ARE the members' evidence of the normal view.
    normal = client.get(_base(env) + "/production/changes").json()
    normal_ids = {e["id"]: e for i in normal["project_changes"] for e in i["evidence"]}
    assert all(e["id"] in normal_ids for e in merged["evidence"])
    for e in merged["evidence"]:
        mine = client.get(e["image_url"])
        theirs = client.get(normal_ids[e["id"]]["image_url"])
        assert mine.status_code == 200 and mine.content == theirs.content
        assert (e["side"], e["page"], e["block_id"], e["region"]) == tuple(
            normal_ids[e["id"]][k] for k in ("side", "page", "block_id", "region"))
    hint = merged["conflicts"][0]["hint"]
    assert hint["hint_ref"] == "R-003/H001"
    normal_hint = next(h for h in normal["unresolved_hints"] if h["hint_ref"] == "R-003/H001")
    assert hint["evidence"][0]["id"] == normal_hint["evidence"][0]["id"]
    doc = next(x for x in view["consolidated"] if x["kind"] == "SOURCE_CARD")
    assert doc["channel"] == "DOCUMENTARY_CHANGE" and doc["id"].endswith("PC-R-003-C001")


def test_unbound_or_unfinished_consolidations_are_never_shown(env):
    client = env["client"]
    run_id = _production_run(env)
    manifest = _shadow(env, run_id)
    from backend.app.services.stage_comparison import paths

    run_dir = paths.production_dir(env["session_id"], gf.PAIR_ID) / SHADOW_DIR_NAME / run_id / manifest[
        "consolidator_run_id"]
    result = run_dir / "SHADOW_RESULT.json"
    result.chmod(0o644)
    result.write_text(result.read_text().replace("Сводное решение", "Подменено"))
    assert client.get(_base(env) + "/consolidated").json()["consolidations"] == []
    assert client.get(_base(env) + f"/consolidated/{run_id}/{manifest['consolidator_run_id']}").status_code == 404
    assert client.get(_base(env) + "/consolidated/..%2F..%2Fx/abc").status_code == 404


def test_frozen_source_import_binds_by_hash_and_leaves_runs_untouched(env, tmp_path):
    from backend.app.services.project_change_consolidator.source_import import ImportRefused, import_frozen_consolidation
    from backend.app.services.stage_comparison import paths

    client = env["client"]
    run_id = _production_run(env)
    production = paths.production_dir(env["session_id"], gf.PAIR_ID)
    run_dir = production / "runs" / run_id
    # A frozen research copy of the source run (explicit files + sha256).
    frozen = tmp_path / "frozen_research_run"
    shutil.copytree(run_dir, frozen)
    for p in frozen.rglob("*"):
        if p.is_file():
            p.chmod(stat.S_IRUSR | stat.S_IWUSR)
    src = frozen / "project_change_v3" / "source"
    spec = {"source_run_id": run_id,
            "result": {"path": str(frozen / "project_change_v3_result.json"),
                       "sha256": _sha(frozen / "project_change_v3_result.json")},
            "hint_regions": {"kind": "miner_results", "path": str(frozen / "project_change_v3_miner_results.json"),
                             "sha256": _sha(frozen / "project_change_v3_miner_results.json")},
            "semantic_map": {"path": str(frozen / "project_change_v3_semantic_map.json"),
                             "sha256": _sha(frozen / "project_change_v3_semantic_map.json")},
            "source_package": {"dir": str(src), "page_sha256": {str(p.relative_to(src)): _sha(p)
                                                                for p in src.rglob("page.json")}}}
    from backend.app.services.project_change_consolidator.source_view import load_frozen_bundle
    from backend.app.services.project_change_v3.provider import FakeProvider

    research = tmp_path / "research_shadow"
    manifest = run_shadow(load_frozen_bundle(spec), FakeProvider(handlers={C.STAGE: consolidate}), research,
                          max_calls=12)
    shadow_dir = research / run_id / manifest["consolidator_run_id"]
    with pytest.raises(ImportRefused) as exc:  # the source is still a production run of the pair
        import_frozen_consolidation(env["session_id"], gf.PAIR_ID, spec, shadow_dir, label="t", engine={})
    assert exc.value.code == "SOURCE_IS_A_PRODUCTION_RUN"
    # The pair keeps no production run of this id: only the frozen research copy remains.
    for p in run_dir.rglob("*"):
        p.chmod(0o755 if p.is_dir() else 0o644)
    shutil.rmtree(run_dir)
    (production / "current_run.json").unlink()
    receipt = import_frozen_consolidation(env["session_id"], gf.PAIR_ID, spec, shadow_dir,
                                          label="Замороженный прогон", engine={"model": "fake"})
    assert receipt["production_run_store_unchanged"] and receipt["consolidator_result_sha256"] == manifest[
        "shadow_result_sha256"]
    assert not (production / "runs" / run_id).exists() and not (production / "current_run.json").exists()
    [entry] = client.get(_base(env) + "/consolidated").json()["consolidations"]
    assert entry["source_kind"] == "IMPORTED_FROZEN_RUN" and entry["source_is_current_run"] is False
    view = client.get(_base(env) + f"/consolidated/{run_id}/{manifest['consolidator_run_id']}").json()
    merged = next(x for x in view["consolidated"] if x["kind"] == "CONSOLIDATED")
    crops = [client.get(e["image_url"]) for e in merged["evidence"]]
    assert all(r.status_code == 200 and r.content.startswith(b"\x89PNG") for r in crops)
    assert merged["conflicts"][0]["hint"]["hint_ref"] == "R-003/H001"
    # A second import of the same consolidation is refused (append-only).
    with pytest.raises(Exception):
        import_frozen_consolidation(env["session_id"], gf.PAIR_ID, spec, shadow_dir, label="t", engine={})
    # Tampering with an imported file unbinds the view.
    imported = production / SHADOW_DIR_NAME / run_id / "SOURCE_IMPORT" / "SOURCE_RESULT.json"
    imported.chmod(0o644)
    imported.write_text(imported.read_text().replace("Штамп", "Штамп!"))
    assert client.get(_base(env) + "/consolidated").json()["consolidations"] == []
    json.loads((production / SHADOW_DIR_NAME / run_id / "SOURCE_IMPORT" / "SOURCE_IMPORT_RECEIPT.json").read_text())
