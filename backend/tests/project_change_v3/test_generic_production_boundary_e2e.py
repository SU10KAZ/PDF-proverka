"""Zero-model generic E2E through the REAL production boundary.

HTTP production/run → session/pair resolution → _resolved_document_paths →
V3 source prep → FakeProvider Mapper → validation → FakeProvider Miner →
validation → FakeProvider dedupe → persistence → /production/changes →
object ProjectChangeView → UI contract (ProjectChangeView.fromEnvelope in
node) → generic Human Mapping API + browser context → isolated writes.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.tests.project_change_v3 import generic_fixture as gf

REPO = Path(__file__).resolve().parents[3]
FIXTURES = REPO / "backend/app/data/human_mapping_fixtures"
FORBIDDEN_TRUTH = re.compile(r"REAL15|PROVEN10|\bF13\b|GOOD_HUMAN_LEVEL|TOO_ATOMIC|OVER_MERGED|truth", re.I)


def _tree_digest(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*")) if p.is_file()
    }


@pytest.fixture
def generic_env(tmp_path, monkeypatch):
    comparison_root = tmp_path / "comparison"
    comparison_root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(comparison_root))
    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "1")
    built = gf.build_comparison(tmp_path)

    from backend.app.services.project_change_v3 import scope

    monkeypatch.setattr(scope, "_object_stage_paths",
                        lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])})
    from backend.app.services.project_change_v3.provider import FakeProvider, reset_test_provider, set_test_provider

    fake = FakeProvider(handlers=gf.fake_handlers())
    set_test_provider(fake)

    from backend.app.api.routers import human_mapping, project_change_preview, stage_comparison

    app = FastAPI()
    for r in (stage_comparison.router, project_change_preview.router, project_change_preview.availability_router,
              human_mapping.router, human_mapping.api_router):
        app.include_router(r)
    try:
        yield {**built, "client": TestClient(app), "fake": fake, "root": comparison_root}
    finally:
        reset_test_provider()


def _run(env):
    client = env["client"]
    url = f"/api/stage-comparison/sessions/{env['session_id']}/pairs/{gf.PAIR_ID}/production/run"
    response = client.post(url, json={"input_mode": "DOCUMENT"})
    assert response.status_code == 200, response.text
    return response.json()


def test_generic_full_production_boundary(generic_env):
    env = generic_env
    client = env["client"]
    fixtures_before = _tree_digest(FIXTURES)

    # 1. Normal production run entry → V3 engine (never legacy), zero model calls.
    state = _run(env)
    assert state["engine"] == "projectchange_v3"
    assert state["status"] == "REVIEW"  # one unresolved hint
    assert state["reason_code"] == "v3_completed"
    assert state["legacy_invoked"] is False and state["model_calls"] == 0
    assert state["human_mapping_published"] is True
    assert state["human_mapping_object_id"] == gf.OBJECT_ID
    assert [c["stage"] for c in env["fake"].calls] == ["MAPPING", "MINING", "MINING", "DEDUPE"]
    assert all(c["pair_id"] == gf.PAIR_ID for c in env["fake"].calls)
    # The resolved OLD/NEW documents are exactly the session pair's files.
    manifest = json.loads((env["root"] / "sessions" / env["session_id"] / "pairs" / gf.PAIR_ID / "production"
                           / "project_change_v3_source_manifest.json").read_text(encoding="utf-8"))
    assert manifest["old_pdf_sha256"] == hashlib.sha256(Path(env["left"]["pdf_path"]).read_bytes()).hexdigest()
    assert manifest["new_pdf_sha256"] == hashlib.sha256(Path(env["right"]["pdf_path"]).read_bytes()).hexdigest()

    # 2. Normal production ProjectChange API (pair level) serves the V3 result.
    changes = client.get(f"/api/stage-comparison/sessions/{env['session_id']}/pairs/{gf.PAIR_ID}/production/changes")
    assert changes.status_code == 200, changes.text
    body = changes.json()
    assert body["engine"] == "projectchange_v3" and body["available"] is True
    assert body["run_id"] == state["run_id"]
    [item] = body["project_changes"]
    assert item["projectchange_id"] == "PC-R-001-C001" and item["region_id"] == "R-001"
    assert item["engineering_subject"] == "Приточная установка П1"
    assert item["old_state"] == "1000 м3/ч" and item["new_state"] == "1200 м3/ч"
    assert item["old_pages"] == [1] and item["new_pages"] == [1]
    assert item["changed_parameters"][0]["new_value"] == "1200"
    assert item["details"][0]["label"] == "Расход (П1)" and item["details"][0]["new"] == "1200 м3/ч"
    assert item["dedupe_lineage"] == ["PC-R-001-C001"] and item["confidence"] == 0.8
    assert {e["source_type"] for e in item["evidence"]} == {"TEXT", "TABLE", "GRAPHIC"}
    graphic = [e for e in item["evidence"] if e["source_type"] == "GRAPHIC"]
    assert all(e["crop_ref"].startswith("source/") and e["crop_ref"].endswith(".png") for e in graphic)
    assert all(e["bbox"] and e["region"]["units"] == "normalized" for e in item["evidence"])
    assert body["unresolved_hints"][0]["hint_id"] == "H001"
    assert "provenance" in body["run"] and body["run"]["provenance"]["engine_version"] == "3.3.0"

    # 3. Object-scoped ProjectChangeView — the feed the stage-comparison UI reads.
    view = client.get(f"/api/stage-comparison/objects/{gf.OBJECT_ID}/project-changes")
    assert view.status_code == 200, view.text
    envelope = view.json()
    assert envelope["schema_version"] == "project-change-view/1"
    assert envelope["object_id"] == gf.OBJECT_ID and envelope["availability"] == "AVAILABLE"
    assert [i["projectchange_id"] for i in envelope["items"]] == ["PC-R-001-C001"]
    assert envelope["viewer_session"]["pairs"][0]["id"] == gf.PAIR_ID
    assert not FORBIDDEN_TRUTH.search(json.dumps(envelope, ensure_ascii=False))
    for e in envelope["items"][0]["evidence"]:
        crop = client.get(e["image_url"])
        assert crop.status_code == 200 and crop.content.startswith(b"\x89PNG"), e["image_url"]

    # 4. The UI contract renders them for the opened pair (real frontend code).
    node = shutil.which("node")
    if node:
        script = (
            "const V=require(process.argv[1]);let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{"
            "const env=JSON.parse(s);const all=V.fromEnvelope(env,process.argv[2]);"
            "const inPair=V.inPair(all,process.argv[3]);"
            "console.log(JSON.stringify({all:all.length,inPair:inPair.length,"
            "binding:all.map(c=>c.pair_binding_error),"
            "evidence:inPair[0]?inPair[0].evidence.map(e=>[e.source_type,!!e.image_url,!!e.region]):[]}))})"
        )
        out = subprocess.run(
            [node, "-e", script, str(REPO / "frontend/static/js/project-change-view.js"), gf.OBJECT_ID, gf.PAIR_ID],
            input=json.dumps(envelope), capture_output=True, text=True, check=True,
        )
        rendered = json.loads(out.stdout)
        assert rendered["all"] == 1 and rendered["inPair"] == 1 and rendered["binding"] == [False]
        assert sorted(x[0] for x in rendered["evidence"]) == ["GRAPHIC", "GRAPHIC", "TABLE", "TEXT"]
        assert all(x[1] and x[2] for x in rendered["evidence"])

    # 5. Generic Human Mapping browser context: exactly this comparison, never A/B.
    page = client.get(f"/human-mapping/?object={gf.OBJECT_ID}&comparison={gf.PAIR_ID}")
    assert page.status_code == 200
    context = json.loads(re.search(r"const CTX=(\{.*?\});", page.text).group(1))
    assert context == {"object": gf.OBJECT_ID, "pair": gf.PAIR_ID, "fixture_letter": None,
                       "fixture_nav": False, "label": gf.PAIR_ID}
    assert "__HM_PAIR__" not in page.text and "||'A'" not in page.text
    assert "b.tables[0].map(" not in page.text  # V1.2.4 crashed on frozen string tables
    legacy_param = client.get(f"/human-mapping/?object={gf.OBJECT_ID}&pair={gf.PAIR_ID}")
    assert json.loads(re.search(r"const CTX=(\{.*?\});", legacy_param.text).group(1))["pair"] == gf.PAIR_ID
    # A/B letters are NOT aliases outside the fixture object.
    alias = client.get(f"/human-mapping/?object={gf.OBJECT_ID}&comparison=A")
    assert json.loads(re.search(r"const CTX=(\{.*?\});", alias.text).group(1))["pair"] == "A"
    assert client.get(f"/api/human-mapping/objects/{gf.OBJECT_ID}/comparisons/A/ui-data").status_code == 404

    # 6. Generic Human Mapping API: semantic membership ≠ page context.
    api = f"/api/human-mapping/objects/{gf.OBJECT_ID}/comparisons/{gf.PAIR_ID}"
    ui = client.get(api + "/ui-data").json()
    assert ui["pair"] == gf.PAIR_ID and ui["object_id"] == gf.OBJECT_ID
    r1, r2 = ui["regions"]
    assert [b["id"] for b in r1["old_blocks"]] == ["o1_text", "o1_graphic"]
    assert [b["id"] for b in r1["new_blocks"]] == ["n1_table", "n1_graphic"]
    assert {b["id"] for b in r1["pages"]["OLD"][0]["blocks"]} == {"o1_text", "o1_graphic", "o1_extra"}
    assert r1["mapping_state"] == "MAPPED"
    assert r2["membership_state"] == {"OLD": "MAPPED", "NEW": "EMPTY"}
    assert r2["new_blocks"] == [] and r2["mapping_state"] == "REVIEW_INSUFFICIENT_MAPPING"
    assert [p["page"] for p in r2["pages"]["NEW"]] == [2]  # mapped page kept as visual context
    image = r1["pages"]["OLD"][0]["image"]
    assert image.startswith(f"/api/human-mapping/objects/{gf.OBJECT_ID}/comparisons/{gf.PAIR_ID}/assets/")
    assert client.get(image).content.startswith(b"\x89PNG")
    assert client.get(r1["old_blocks"][1]["crop"]).content.startswith(b"\x89PNG")
    # TABLE detail renders the frozen Markdown table (function taken from the served page).
    table_block = next(b for b in r1["new_blocks"] if b["type"] == "TABLE")
    assert isinstance(table_block["tables"][0], str)
    if shutil.which("node"):
        fn = re.search(r"function tableHtml\(t\)\{.*?\n", page.text).group(0)
        esc = "const esc=s=>String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));"
        html = subprocess.run([shutil.which("node"), "-e", esc + fn + "process.stdout.write(tableHtml(JSON.parse(process.argv[1])))",
                               json.dumps(table_block["tables"][0])], capture_output=True, text=True, check=True).stdout
        assert html.startswith("<table>") and "<td>1200 м3/ч</td>" in html

    # 7. Writes land ONLY in obj_generic_test / generic_pair_not_A_or_B.
    review = client.post(api + "/reviews", json={
        "region_id": "R-001", "old_block_ids": ["o1_text"], "new_block_ids": ["n1_table"],
        "status": "HUMAN_CONFIRMED", "comment": "generic-e2e"})
    assert review.status_code == 200 and review.json()["comparison_id"] == gf.PAIR_ID
    for status in ("HUMAN_REJECTED", "HUMAN_UNCERTAIN"):
        assert client.post(api + "/reviews", json={
            "region_id": "R-001", "old_block_ids": ["o1_text"], "new_block_ids": ["n1_table"],
            "status": status}).status_code == 200
    add = client.post(api + "/block-links", json={
        "event_type": "ADD_BLOCK_LINK", "region_id": "R-001", "link_id": "human:e2e-1",
        "old_block_id": "o1_text", "new_block_id": "n1_table"})
    assert add.status_code == 200, add.text
    reassign = client.post(api + "/block-links", json={
        "event_type": "REASSIGN_BLOCK_LINK", "region_id": "R-001", "link_id": "human:e2e-2",
        "old_block_id": "o1_graphic", "new_block_id": "n1_table", "previous_link_id": "human:e2e-1"})
    assert reassign.status_code == 200, reassign.text
    delete = client.post(api + "/block-links", json={
        "event_type": "DELETE_BLOCK_LINK", "region_id": "R-001", "link_id": "human:e2e-2",
        "old_block_id": "o1_graphic", "new_block_id": "n1_table"})
    assert delete.status_code == 200, delete.text
    # EMPTY NEW membership: page-context block is linkable, a foreign block is not.
    ok = client.post(api + "/block-links", json={
        "event_type": "ADD_BLOCK_LINK", "region_id": "R-002", "link_id": "human:e2e-3",
        "old_block_id": "o2_text", "new_block_id": "n2_text"})
    assert ok.status_code == 200, ok.text
    bad = client.post(api + "/block-links", json={
        "event_type": "ADD_BLOCK_LINK", "region_id": "R-002", "link_id": "human:e2e-4",
        "old_block_id": "o2_text", "new_block_id": "n1_table"})
    assert bad.status_code == 400 and bad.json()["detail"]["error"] == "NEW_BLOCK_NOT_IN_REGION"
    assert len(client.get(api + "/reviews").json()) == 3
    assert len(client.get(api + "/block-links").json()) == 4

    hm_root = env["root"] / "human_mapping"
    stored = sorted(str(p.relative_to(hm_root)) for p in hm_root.rglob("*.jsonl"))
    assert stored == [f"{gf.OBJECT_ID}/{gf.PAIR_ID}/human_block_link_edits.jsonl",
                      f"{gf.OBJECT_ID}/{gf.PAIR_ID}/reviews.jsonl"]
    assert sorted(p.name for p in hm_root.iterdir()) == [gf.OBJECT_ID]
    assert sorted(p.name for p in (hm_root / gf.OBJECT_ID).iterdir()) == [gf.PAIR_ID]
    assert _tree_digest(FIXTURES) == fixtures_before  # sealed A/B fixtures untouched

    receipt_path = os.environ.get("PC_V3_E2E_RECEIPT")
    if receipt_path:
        Path(receipt_path).write_text(json.dumps({
            "schema": "projectchange-v3-generic-e2e/1",
            "object_id": gf.OBJECT_ID, "pair_id": gf.PAIR_ID, "session_id": env["session_id"],
            "boundary": "POST /api/stage-comparison/sessions/{sid}/pairs/{pid}/production/run",
            "state": {k: state[k] for k in ("status", "reason_code", "engine", "engine_version", "run_id",
                                             "model_calls", "legacy_invoked", "human_mapping_published",
                                             "human_mapping_object_id", "projectchange_count",
                                             "unresolved_hint_count", "semantic_region_count")},
            "provider_calls": [c["stage"] for c in env["fake"].calls],
            "production_changes": {"kind": body["kind"], "available": body["available"],
                                   "items": len(body["project_changes"]),
                                   "evidence_types": sorted({e["source_type"] for e in item["evidence"]}),
                                   "hints": len(body["unresolved_hints"])},
            "object_view": {"schema_version": envelope["schema_version"], "items": len(envelope["items"]),
                            "evidence_crops_png": len(envelope["items"][0]["evidence"]),
                            "truth_tokens": bool(FORBIDDEN_TRUTH.search(json.dumps(envelope, ensure_ascii=False)))},
            "ui_render_node": rendered if node else "node unavailable",
            "human_mapping": {"context": context, "regions": len(ui["regions"]),
                              "r1_members": [len(r1["old_blocks"]), len(r1["new_blocks"])],
                              "r1_page_context_blocks_old": len(r1["pages"]["OLD"][0]["blocks"]),
                              "r2_state": r2["mapping_state"], "r2_membership": r2["membership_state"],
                              "stored": stored, "fixture_ab_unchanged": True},
            "model_calls": 0,
        }, ensure_ascii=False, indent=2), encoding="utf-8")


def test_legacy_engine_does_not_serve_v3_presentation(generic_env, monkeypatch):
    env = generic_env
    _run(env)
    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "legacy")
    view = env["client"].get(f"/api/stage-comparison/objects/{gf.OBJECT_ID}/project-changes").json()
    assert view["availability"] == "UNAVAILABLE" and view["items"] == []


def test_failed_run_is_never_published(generic_env):
    from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider

    env = generic_env
    handlers = gf.fake_handlers()
    handlers["MAPPING"] = lambda **kw: {**gf.fake_handlers()["MAPPING"](**kw), "pair": "A"}  # identity drift
    set_test_provider(FakeProvider(handlers=handlers))
    state = _run(env)
    assert state["status"] == "FAILED" and state["reason_code"] == "mapper_validation_failed"
    view = env["client"].get(f"/api/stage-comparison/objects/{gf.OBJECT_ID}/project-changes").json()
    assert view["availability"] == "UNAVAILABLE"
    changes = env["client"].get(
        f"/api/stage-comparison/sessions/{env['session_id']}/pairs/{gf.PAIR_ID}/production/changes").json()
    assert changes["available"] is False and changes["project_changes"] == []
