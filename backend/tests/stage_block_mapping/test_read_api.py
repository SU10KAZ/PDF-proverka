"""Read-only block-mapping API: contracts, writable() matrix, parity and "writes nothing"."""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

import jsonschema
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_block_mapping
from backend.app.services.human_mapping_production import validation

from .conftest import OID, PID, RUN, SID, build_run, hm_event, region

SCHEMAS = Path(__file__).parent / "schemas"
BASE = f"/api/stage-comparison/sessions/{SID}/pairs/{PID}/block-mapping"


def schema_check(body, name):
    schema = json.loads((SCHEMAS / f"stage-block-mapping-{name}_v1.schema.json").read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(schema).validate(body)
    return body


@pytest.fixture
def client(world):
    app = FastAPI()
    app.include_router(stage_block_mapping.router)
    return TestClient(app)


def status(client):
    response = client.get(BASE + "/status")
    assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
    return schema_check(response.json(), "status")


def the_run(body, run_id=RUN):
    return next(r for r in body["runs"] if r["run_id"] == run_id)


def fresh(world):
    from backend.app.services.stage_block_mapping import service
    service.clear_cache()


# ── status and the writable() matrix ─────────────────────────────────────────
def test_status_of_a_healthy_frozen_run(client, world):
    body = status(client)
    run = the_run(body)
    assert body["object_id"] == OID and body["current_run_id"] == RUN
    assert body["latest_attempt"] == {"run_id": RUN, "state": "COMPLETED_FROZEN", "reason_code": "v3_completed",
                                      "created_at": run["created_at"], "completed_at": run["completed_at"]}
    assert (run["hm_available"], run["pdf_match"], run["source_stale"], run["blocks_content_match"]) == (True, True, False, None)
    assert run["region_count"] == 2 and run["history"] == {"reviews": 0, "block_link_events": 0, "poisoned": False, "poison_codes": []}
    # Flag off (R1/R2): a healthy run is read-only only because of the flag, which is checked last.
    assert (run["writable"], run["write_block_reason"], run["write_block_reasons"]) == (False, "WRITES_DISABLED", ["WRITES_DISABLED"])
    assert body["capabilities"] == {"block_mapping_writes": False}
    assert body["source_blocks"]["OLD"] == {**body["source_blocks"]["OLD"], "available": True, "page_count": 2,
                                            "block_count": 4, "stamp_count": 1}


@pytest.mark.parametrize("raw,expected", [("1", True), ("TRUE", True), (" yes ", True), ("On", True),
                                          ("0", False), ("", False), ("nope", False)])
def test_write_flag_parsing(client, world, monkeypatch, raw, expected):
    monkeypatch.setenv("STAGE_BLOCK_MAPPING_WRITES", raw)
    run = the_run(status(client))
    assert run["writable"] is expected
    assert run["write_block_reasons"] == ([] if expected else ["WRITES_DISABLED"])


def test_pdf_change_closes_writes_first(client, world, monkeypatch):
    monkeypatch.setenv("STAGE_BLOCK_MAPPING_WRITES", "1")
    world["documents"]["OLD"]["pdf"].write_bytes(b"%PDF replaced")
    fresh(world)
    run = the_run(status(client))
    assert run["pdf_match"] is False and run["blocks_content_match"] is None
    assert run["write_block_reasons"] == ["SOURCE_PDF_CHANGED", "SOURCE_BLOCKS_CHANGED"]
    assert run["write_block_reason"] == "SOURCE_PDF_CHANGED"


def test_touched_recognition_with_same_content_stays_writable(client, world, monkeypatch):
    monkeypatch.setenv("STAGE_BLOCK_MAPPING_WRITES", "1")
    blocks = world["documents"]["NEW"]["blocks"]
    stat = blocks.stat()
    os.utime(blocks, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000_000))
    fresh(world)
    run = the_run(status(client))
    assert (run["source_stale"], run["blocks_content_match"], run["writable"]) == (True, True, True)  # K0


def test_changed_recognition_closes_writes(client, world, monkeypatch):
    monkeypatch.setenv("STAGE_BLOCK_MAPPING_WRITES", "1")
    blocks = world["documents"]["NEW"]["blocks"]
    payload = json.loads(blocks.read_text(encoding="utf-8"))
    payload["blocks"][0]["coords_norm"] = [0.2, 0.2, 0.5, 0.3]
    blocks.write_text(json.dumps(payload), encoding="utf-8")
    fresh(world)
    run = the_run(status(client))
    assert (run["source_stale"], run["blocks_content_match"]) == (True, False)
    assert run["write_block_reasons"] == ["SOURCE_BLOCKS_CHANGED"]


def test_poisoned_history_is_reported_before_the_flag(client, world):
    hm_event(world["run_dir"], "review", region_id="R1", old_block_ids=["o1", "o3"], new_block_ids=["n1"],
             status="HUMAN_CONFIRMED")
    run = the_run(status(client))
    assert run["history"]["poisoned"] is True and run["history"]["poison_codes"] == ["BLOCK_OUTSIDE_REGION"]
    assert run["write_block_reasons"] == ["HISTORY_POISONED", "WRITES_DISABLED"]


@pytest.mark.parametrize("mutate,code", [
    (lambda row: row.update(run_id=None), "HUMAN_EVENT_SCOPE_MISMATCH"),
    (lambda row: row.update(region_id="GONE"), "STALE_REGION"),
    (lambda row: row.update(timestamp="вчера"), "INVALID_EVENT_TIMESTAMP"),
    (lambda row: row.update(new_block_ids=[]), "EMPTY_REVIEW_ENDPOINTS"),
    (lambda row: row.update(status="UNREVIEWED"), "INVALID_STATUS"),
])
def test_every_history_poison_code(client, world, mutate, code):
    row = {"region_id": "R1", "old_block_ids": ["o1"], "new_block_ids": ["n1"], "status": "HUMAN_CONFIRMED"}
    mutate(row)
    hm_event(world["run_dir"], "review", **row)
    assert code in the_run(status(client))["history"]["poison_codes"]


def test_duplicate_edge_replay_is_poison(client, world):
    for i in range(2):
        hm_event(world["run_dir"], "edit", event_type="ADD_BLOCK_LINK", region_id="R1", link_id=f"human:{i}",
                 old_block_id="o1", new_block_id="n1")
    assert the_run(status(client))["history"]["poison_codes"] == ["INVALID_BLOCK_LINK_REPLAY"]


def test_foreign_ui_data_makes_hm_unavailable(client, world):
    path = world["run_dir"] / "human_mapping" / "ui_data.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(json.dumps({**data, "object_id": "someone-else"}), encoding="utf-8")
    run = the_run(status(client))
    assert (run["hm_available"], run["hm_reason"], run["write_block_reason"]) == (False, "HM_SCOPE_MISMATCH", "HM_UNAVAILABLE")
    assert client.get(f"{BASE}/runs/{RUN}/region-index").json()["detail"] == {
        "error": "HM_UNAVAILABLE", "ok": False, "hm_reason": "HM_SCOPE_MISMATCH"}


def test_tampered_frozen_artifact_invalidates_the_run(client, world):
    path = world["run_dir"] / "project_change_v3_semantic_map.json"
    path.write_text(path.read_text(encoding="utf-8") + " ", encoding="utf-8")
    run = the_run(status(client))
    assert run["write_block_reason"] == "RUN_INVALID"
    assert client.get(f"{BASE}/runs/{RUN}/region-index").status_code == 409


def test_failed_attempt_is_latest_but_not_a_run(client, world):
    from backend.app.services.project_change_v3 import run_storage as rs
    directory = rs.create(SID, PID, "run2", OID)
    manifest = rs.read(directory / "run_manifest.json")
    rs.atomic(directory / "run_manifest.json", {**manifest, "state": "FAILED", "created_at": "2099-01-01T00:00:00+00:00"})
    rs.atomic(directory / "state.json", {"run_id": "run2", "status": "FAILED", "reason_code": "v3_inference_kill_switch"})
    body = status(client)
    assert [r["run_id"] for r in body["runs"]] == [RUN]
    assert body["latest_attempt"]["run_id"] == "run2" and body["latest_attempt"]["state"] == "FAILED"
    assert body["latest_attempt"]["reason_code"] == "v3_inference_kill_switch"


# ── region index ─────────────────────────────────────────────────────────────
def test_region_index_mirrors_ui_data(client, world):
    response = client.get(f"{BASE}/runs/{RUN}/region-index")
    body = schema_check(response.json(), "region-index")
    raw = (world["run_dir"] / "human_mapping" / "ui_data.json").read_bytes()
    assert response.headers["etag"] == f'"{hashlib.sha256(raw).hexdigest()}"' == f'"{body["ui_data_sha256"]}"'
    assert response.headers["cache-control"] == "private, max-age=86400"
    assert client.get(f"{BASE}/runs/{RUN}/region-index", headers={"If-None-Match": response.headers["etag"]}).status_code == 304
    ui = json.loads(raw)
    for entry, source in zip(body["regions"], ui["regions"]):
        assert entry["id"] == source["id"] and entry["title"] == source["domain"]
        for side, key in (("OLD", "old_blocks"), ("NEW", "new_blocks")):
            assert [m["id"] for m in entry["members"][side]] == [b["id"] for b in source[key]]
            assert set(entry["allowed"][side]) == validation.allowed_block_ids(source, side)
            assert entry["pages"][side] == [p["page"] for p in source["pages"][side]]
    assert [r["member_cardinality"] for r in body["regions"]] == ["N:N", "1:1"]
    assert "structured_md" not in json.dumps(body)


# ── page blocks (run and recognition layers) ──────────────────────────────────
def test_run_and_source_page_blocks_agree(client, world):
    run_body = schema_check(client.get(f"{BASE}/runs/{RUN}/page-blocks", params={"side": "OLD", "pages": "1,2,9"}).json(),
                            "page-blocks")
    src_body = schema_check(client.get(f"{BASE}/source/page-blocks", params={"side": "OLD", "pages": "1,2,9"}).json(),
                            "page-blocks")
    for run_page, src_page in zip(run_body["pages"][:2], src_body["pages"][:2]):
        assert run_page["blocks"] == src_page["blocks"]
        assert run_page["geometry"]["geometry_source"] == "RUN_FULL_PAGE_PNG"
        assert src_page["geometry"]["geometry_source"] == "BLOCKS_JSON_PAGE_META"
        assert run_page["geometry"]["rotation"] == src_page["geometry"]["rotation"]
    assert [p["geometry"]["rotation"] for p in run_body["pages"][:2]] == [0, 270]
    assert (run_body["pages"][2]["reason"], src_body["pages"][2]["reason"]) == ("PAGE_OUT_OF_RANGE", "PAGE_OUT_OF_RANGE")
    assert {b["source_block_type"] for b in run_body["pages"][1]["blocks"]} == {"text", "stamp"}  # stamps always sent
    assert run_body["source_identity"]["blocks_sha256"] is None and src_body["run_id"] is None


def test_run_rotation_is_withheld_once_the_pdf_changed(client, world):
    world["documents"]["OLD"]["pdf"].write_bytes(b"%PDF replaced")
    fresh(world)
    pages = client.get(f"{BASE}/runs/{RUN}/page-blocks", params={"side": "OLD", "pages": "2"}).json()["pages"]
    assert pages[0]["available"] and pages[0]["geometry"]["rotation"] is None


def test_source_layer_fails_closed_without_markdown(client, world):
    world["documents"]["NEW"]["markdown"].unlink()
    fresh(world)
    response = client.get(f"{BASE}/source/page-blocks", params={"side": "NEW", "pages": "1"})
    assert response.status_code == 409
    assert response.json()["detail"] == {"error": "SOURCE_BLOCKS_UNAVAILABLE", "ok": False, "reason": "MARKDOWN_MISSING", "side": "NEW"}
    assert status(client)["source_blocks"]["NEW"]["reason"] == "MARKDOWN_MISSING"


def test_block_details(client, world):
    graphic = schema_check(client.get(f"{BASE}/runs/{RUN}/blocks/OLD/o2").json(), "block")
    assert (graphic["physical_page"], graphic["modality"], graphic["hm_crop_asset"]) == (1, "GRAPHIC", "assets/old/p001/o2.png")
    table = schema_check(client.get(f"{BASE}/source/blocks/OLD/o3").json(), "block")
    assert table["modality"] == "TABLE" and table["tables"] and table["hm_crop_asset"] is None
    assert client.get(f"{BASE}/runs/{RUN}/blocks/OLD/nope").status_code == 404


@pytest.mark.parametrize("path,params,code", [
    ("/runs/bad!id/region-index", {}, "INVALID_SCOPE_ID"),
    ("/runs/missing/region-index", {}, "RUN_NOT_FOUND"),
    (f"/runs/{RUN}/page-blocks", {"side": "LEFT", "pages": "1"}, "INVALID_SIDE"),
    (f"/runs/{RUN}/page-blocks", {"side": "OLD", "pages": "0"}, "INVALID_PAGES"),
    (f"/runs/{RUN}/page-blocks", {"side": "OLD", "pages": ",".join(map(str, range(1, 32)))}, "INVALID_PAGES"),
])
def test_refusals(client, world, path, params, code):
    assert client.get(BASE + path, params=params).json()["detail"]["error"] == code


def test_unknown_pair_and_session(client, world):
    assert client.get(f"/api/stage-comparison/sessions/{SID}/pairs/nope/block-mapping/status").json()["detail"]["error"] == "PAIR_NOT_FOUND"
    assert client.get(f"/api/stage-comparison/sessions/nope/pairs/{PID}/block-mapping/status").json()["detail"]["error"] == "SESSION_NOT_FOUND"


# ── bridge check ─────────────────────────────────────────────────────────────
def test_bridge_check_counts_anchors_and_caches(client, world):
    hm_event(world["run_dir"], "edit", event_type="ADD_BLOCK_LINK", region_id="R1", link_id="human:a",
             old_block_id="o1", new_block_id="n1")
    hm_event(world["run_dir"], "review", region_id="R1", old_block_ids=["o1"], new_block_ids=["n1"], status="HUMAN_CONFIRMED")
    first = schema_check(client.get(f"{BASE}/runs/{RUN}/bridge-check").json(), "bridge-check")
    assert first["ok"] and not first["cached"] and first["notice"].startswith("Это предварительная проверка")
    assert first["result"]["confirmed_anchor_count"] == 1 and first["result"]["unconstrained_region_count"] == 1
    assert first["result"]["anchors"][0] == {"link_id": "human:a", "region_id": "R1", "old_block_id": "o1",
                                                   "new_block_id": "n1", "source": "HUMAN_MANUAL"}
    assert client.get(f"{BASE}/runs/{RUN}/bridge-check").json()["cached"] is True
    hm_event(world["run_dir"], "review", region_id="R1", old_block_ids=["o1"], new_block_ids=["n1"], status="HUMAN_REJECTED")
    after = client.get(f"{BASE}/runs/{RUN}/bridge-check").json()
    assert after["cached"] is False and after["result"]["rejected_link_count"] == 1


def test_bridge_refusal_is_a_200_with_reason(client, world):
    hm_event(world["run_dir"], "review", region_id="R1", old_block_ids=["o3"], new_block_ids=["n1"], status="HUMAN_CONFIRMED")
    body = schema_check(client.get(f"{BASE}/runs/{RUN}/bridge-check").json(), "bridge-check")
    assert body["ok"] is False and body["result"] is None and body["error"]["reason"] == "BLOCK_OUTSIDE_REGION"


# ── nothing is written, no model is called ────────────────────────────────────
def tree_state(root: Path) -> dict[str, tuple[str, int]]:
    return {str(p.relative_to(root)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
            for p in sorted(root.rglob("*")) if p.is_file()}


def test_readonly_endpoints_write_nothing(client, world, monkeypatch):
    monkeypatch.setenv("STAGE_BLOCK_MAPPING_WRITES", "1")
    hm_event(world["run_dir"], "review", region_id="R1", old_block_ids=["o1"], new_block_ids=["n1"],
             status="HUMAN_UNCERTAIN")
    before = tree_state(world["tmp"])
    time.sleep(0.01)
    calls = [BASE + "/status", f"{BASE}/runs/{RUN}/region-index", f"{BASE}/runs/{RUN}/page-blocks?side=OLD&pages=1,2",
             f"{BASE}/runs/{RUN}/page-blocks?side=NEW&pages=1", f"{BASE}/runs/{RUN}/blocks/OLD/o2",
             f"{BASE}/source/page-blocks?side=OLD&pages=1,2", f"{BASE}/source/blocks/NEW/n1",
             f"{BASE}/runs/{RUN}/bridge-check", f"{BASE}/runs/missing/region-index", f"{BASE}/runs/bad!/bridge-check"]
    for url in calls * 2:
        assert client.get(url).status_code in (200, 400, 404)
    assert tree_state(world["tmp"]) == before
    assert world["sentinel"].calls == []


def test_regions_for_a_second_run_are_keyed_by_run(client, world):
    second = build_run(world["documents"], run_id="run3", regions=[region("R1", ["o3"], ["n2"])])
    body = client.get(f"{BASE}/runs/run3/region-index").json()
    assert [r["id"] for r in body["regions"]] == ["R1"] and body["run_id"] == "run3"
    assert body["regions"][0]["members"]["OLD"][0]["id"] == "o3"  # same id "R1", different meaning in another run
    assert second.name == "run3"
