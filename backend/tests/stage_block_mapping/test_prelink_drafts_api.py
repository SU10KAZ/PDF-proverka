"""Pre-analysis prelink drafts API (human-prelink-drafts/1): rules, revisions, flags, staleness — 0 models."""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import prelink_drafts as router
from backend.app.services.stage_comparison import paths, prelink_drafts

from .conftest import PID, SID

BASE = f"/api/stage-comparison/sessions/{SID}/pairs/{PID}/prelinks"


@pytest.fixture
def client(world, monkeypatch):
    monkeypatch.setenv("STAGE_PRELINK_DRAFTS", "1")
    app = FastAPI()
    app.include_router(router.router)
    return TestClient(app)


def fresh():
    from backend.app.services.stage_block_mapping import service
    service.clear_cache()


def tree_state(root: Path) -> dict[str, tuple[str, int]]:
    return {str(p.relative_to(root)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
            for p in sorted(root.rglob("*")) if p.is_file()}


def create(client, old, new, revision=None, note=""):
    if revision is None:
        revision = client.get(BASE).json()["revision"]
    return client.post(BASE, json={"expected_revision": revision, "old_block_ids": old, "new_block_ids": new,
                                   "note": note})


def error(response):
    return response.json()["detail"]["error"]


def drafts_file():
    return paths.prelink_drafts_path(SID, PID)


# ── creation and cardinality ─────────────────────────────────────────────────
@pytest.mark.parametrize("old,new,kind", [(["o1"], ["n1"], "1:1"), (["o1"], ["n1", "n2"], "1:N"),
                                          (["o1", "o3"], ["n1"], "N:1"), (["o1", "o2"], ["n1", "n2"], "N:N")])
def test_one_group_of_any_cardinality_without_edges(client, world, old, new, kind):
    response = create(client, old, new)
    assert response.status_code == 201, response.json()
    body = response.json()
    [item] = body["prelinks"]
    assert item["cardinality"] == kind and item["label"] == "PL-1" and item["validity"] == "VALID"
    assert body["revision"] == 1
    stored = json.loads(drafts_file().read_text(encoding="utf-8"))
    prelinks_value = prelink_drafts.check_value(stored)
    assert set(prelinks_value["prelinks"][0]) == set(prelink_drafts.PRELINK_SCHEMA["properties"])
    assert "edges" not in json.dumps(stored) and "pairs" not in prelinks_value["prelinks"][0]


def test_geometry_comes_from_the_live_rows_never_from_the_client(client, world):
    body = create(client, ["o2"], ["n2"]).json()
    [item] = body["prelinks"]
    assert item["old_blocks"][0]["bbox"] == [0.5, 0.1, 0.9, 0.8]
    assert item["old_blocks"][0]["modality"] == "GRAPHIC" and item["old_blocks"][0]["physical_page"] == 1
    extra = client.post(BASE, json={"expected_revision": 1, "old_block_ids": ["o1"], "new_block_ids": ["n1"],
                                    "bbox": [0, 0, 1, 1]})
    assert extra.status_code == 422  # unknown fields are refused outright


@pytest.mark.parametrize("old,new,code", [
    ([], ["n1"], "PRELINK_SIDE_EMPTY"),
    (["zz"], ["n1"], "PRELINK_BLOCK_NOT_FOUND"),
    (["n1"], ["n2"], "PRELINK_WRONG_SIDE"),
    (["os"], ["n1"], "PRELINK_STAMP_NOT_ALLOWED"),
    (["o1", "o1"], ["n1"], "PRELINK_DUPLICATE_BLOCK"),
])
def test_composition_rules(client, world, old, new, code):
    response = create(client, old, new)
    assert response.status_code == 400 and error(response) == code
    assert not drafts_file().exists()


def test_limits_and_note(client, world, monkeypatch):
    assert error(create(client, ["o1"], ["n1"], note="x" * 501)) == "PRELINK_NOTE_TOO_LONG"
    monkeypatch.setattr(prelink_drafts, "MAX_BLOCKS_PER_SIDE", 1)
    assert error(create(client, ["o1", "o2"], ["n1"])) == "PRELINK_TOO_MANY_BLOCKS"
    monkeypatch.setattr(prelink_drafts, "MAX_BLOCKS_PER_SIDE", 12)
    monkeypatch.setattr(prelink_drafts, "MAX_PRELINKS", 1)
    assert create(client, ["o1"], ["n1"]).status_code == 201
    assert error(create(client, ["o2"], ["n2"])) == "PRELINK_LIMIT_REACHED"


def test_same_composition_twice_is_a_duplicate(client, world):
    assert create(client, ["o1", "o2"], ["n1"]).status_code == 201
    response = create(client, ["o2", "o1"], ["n1"])
    assert response.status_code == 409 and error(response) == "PRELINK_DUPLICATE"


# ── revisions ────────────────────────────────────────────────────────────────
def test_wrong_revision_is_409_and_the_file_is_untouched(client, world):
    create(client, ["o1"], ["n1"])
    before = drafts_file().read_bytes()
    stale = create(client, ["o2"], ["n2"], revision=0)
    assert stale.status_code == 409 and error(stale) == "PRELINK_REVISION_CONFLICT"
    assert stale.json()["detail"]["current_revision"] == 1
    assert drafts_file().read_bytes() == before


def test_two_writers_with_one_revision_one_wins(client, world):
    first = create(client, ["o1"], ["n1"], revision=0)
    second = create(client, ["o2"], ["n2"], revision=0)
    assert (first.status_code, second.status_code) == (201, 409)


def test_labels_are_stable_and_never_reused(client, world):
    create(client, ["o1"], ["n1"])
    body = create(client, ["o2"], ["n2"]).json()
    first = next(p for p in body["prelinks"] if p["label"] == "PL-1")
    body = client.delete(f"{BASE}/{first['prelink_id']}?expected_revision={body['revision']}").json()
    assert [p["label"] for p in body["prelinks"]] == ["PL-2"]
    body = create(client, ["o3"], ["n1"]).json()
    assert sorted(p["label"] for p in body["prelinks"]) == ["PL-2", "PL-3"]


def test_put_replaces_the_whole_composition_and_keeps_the_id(client, world):
    body = create(client, ["o1"], ["n1"]).json()
    pid = body["prelinks"][0]["prelink_id"]
    response = client.put(f"{BASE}/{pid}", json={"expected_revision": 1, "old_block_ids": ["o1", "o3"],
                                                "new_block_ids": ["n2"], "note": "заменил"})
    assert response.status_code == 200
    [item] = response.json()["prelinks"]
    assert item["prelink_id"] == pid and item["cardinality"] == "N:1" and item["label"] == "PL-1"
    missing = client.put(f"{BASE}/pl_{'0' * 32}", json={"expected_revision": 2, "old_block_ids": ["o1"],
                                                        "new_block_ids": ["n1"]})
    assert missing.status_code == 404 and error(missing) == "PRELINK_NOT_FOUND"


# ── flags ────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw,enabled", [("1", True), ("true", True), ("on", True), ("yes", True),
                                         ("", False), ("0", False), ("off", False)])
def test_flag_parsing(client, world, monkeypatch, raw, enabled):
    monkeypatch.setenv("STAGE_PRELINK_DRAFTS", raw)
    caps = client.get(BASE).json()["capabilities"]
    assert caps["drafts_api"] is enabled and caps["drafts_writable"] is enabled


def test_flag_off_reads_and_deletes_but_never_creates(client, world, monkeypatch):
    body = create(client, ["o1"], ["n1"]).json()
    monkeypatch.setenv("STAGE_PRELINK_DRAFTS", "0")
    got = client.get(BASE)
    assert got.status_code == 200 and got.json()["capabilities"]["blocked_reason"] == "PRELINK_DRAFTS_DISABLED"
    refused = create(client, ["o2"], ["n2"])
    assert refused.status_code == 403 and error(refused) == "PRELINK_DRAFTS_DISABLED"
    pid = body["prelinks"][0]["prelink_id"]
    assert client.put(f"{BASE}/{pid}", json={"expected_revision": 1, "old_block_ids": ["o2"],
                                             "new_block_ids": ["n2"]}).status_code == 403
    deleted = client.delete(f"{BASE}/{pid}?expected_revision=1")
    assert deleted.status_code == 200 and deleted.json()["prelinks"] == []


# ── staleness: computed on read, never written, no re-binding ────────────────
def _blocks(world, side):
    return json.loads(world["documents"][side]["blocks"].read_text(encoding="utf-8"))


def _write_blocks(world, side, payload):
    world["documents"][side]["blocks"].write_text(json.dumps(payload), encoding="utf-8")
    fresh()


def validity_of(client):
    return [(p["label"], p["validity"], [d.get("reason") for d in p["validity_details"]])
            for p in client.get(BASE).json()["prelinks"]]


def test_touching_files_keeps_valid(client, world):
    create(client, ["o1"], ["n1"])
    doc = world["documents"]["NEW"]["blocks"]
    doc.write_bytes(doc.read_bytes())
    fresh()
    assert validity_of(client) == [("PL-1", "VALID", [])]


def test_unrelated_recognition_change_revalidates(client, world):
    create(client, ["o1"], ["n1"])
    payload = _blocks(world, "NEW")
    payload["blocks"][1]["coords_norm"] = [0.2, 0.4, 0.9, 0.95]  # n2 moved, n1 untouched
    _write_blocks(world, "NEW", payload)
    assert validity_of(client) == [("PL-1", "REVALIDATED", [])]


def test_missing_block_and_moved_block_are_stale(client, world):
    create(client, ["o1"], ["n1"])
    create(client, ["o2"], ["n2"])
    payload = _blocks(world, "NEW")
    payload["blocks"] = [b for b in payload["blocks"] if b["block_id"] != "n1"]
    payload["blocks"][0]["coords_norm"] = [0.1, 0.5, 0.9, 0.9]  # n2 moved
    _write_blocks(world, "NEW", payload)
    rows = dict((label, (state, reasons)) for label, state, reasons in validity_of(client))
    assert rows["PL-1"] == ("STALE_BLOCKS", ["BLOCK_MISSING"])
    assert rows["PL-2"] == ("STALE_BLOCKS", ["GEOMETRY_OR_TYPE_CHANGED"])


def test_changed_text_is_stale_text(client, world):
    create(client, ["o1"], ["n1"])
    path = world["documents"]["NEW"]["markdown"]
    path.write_text(path.read_text(encoding="utf-8").replace("Текст NEW 1", "Текст NEW 1 (изм.)"), encoding="utf-8")
    fresh()
    assert validity_of(client) == [("PL-1", "STALE_TEXT", ["TEXT_CHANGED"])]


def test_changed_pdf_makes_every_prelink_stale(client, world):
    create(client, ["o1"], ["n1"])
    create(client, ["o2"], ["n2"])
    world["documents"]["OLD"]["pdf"].write_bytes(b"%PDF another version")
    fresh()
    assert {state for _l, state, _r in validity_of(client)} == {"STALE_PDF"}


def test_reading_never_writes_and_calls_no_model(client, world):
    create(client, ["o1"], ["n1"])
    payload = _blocks(world, "NEW")
    payload["blocks"] = [b for b in payload["blocks"] if b["block_id"] != "n1"]
    _write_blocks(world, "NEW", payload)
    before = tree_state(world["tmp"])
    time.sleep(0.01)
    for _ in range(2):
        assert client.get(BASE).status_code == 200
    assert tree_state(world["tmp"]) == before
    assert world["sentinel"].calls == []


def test_source_unavailable_blocks_writes_but_not_delete(client, world):
    body = create(client, ["o1"], ["n1"]).json()
    world["documents"]["NEW"]["blocks"].unlink()
    fresh()
    view = client.get(BASE).json()
    assert view["capabilities"]["blocked_reason"] == "SOURCE_BLOCKS_UNAVAILABLE"
    assert view["prelinks"][0]["validity"] == "SOURCE_UNAVAILABLE"
    refused = create(client, ["o2"], ["n2"])
    assert refused.status_code == 409 and error(refused) == "SOURCE_BLOCKS_UNAVAILABLE"
    pid = body["prelinks"][0]["prelink_id"]
    assert client.delete(f"{BASE}/{pid}?expected_revision=1").status_code == 200


def test_corrupt_file_is_never_ignored_or_rewritten(client, world):
    drafts_file().parent.mkdir(parents=True, exist_ok=True)
    drafts_file().write_text('{"schema": "human-prelink-drafts/1"', encoding="utf-8")
    before = drafts_file().read_bytes()
    for response in (client.get(BASE), create(client, ["o1"], ["n1"], revision=0),
                     client.delete(f"{BASE}/pl_{'1' * 32}?expected_revision=0")):
        assert response.status_code == 409 and error(response) == "PRELINK_DRAFTS_INVALID"
    assert drafts_file().read_bytes() == before


# ── independence from the other layers ───────────────────────────────────────
def test_drafts_never_touch_sheet_links_hm_history_or_runs(client, world):
    sheet = paths.sheet_links_path(SID, PID)
    sheet.write_text(json.dumps({"links": [{"id": "l1", "left_pages": [1], "right_pages": [1]}]}), encoding="utf-8")
    watched = [sheet] + sorted(p for p in world["run_dir"].rglob("*") if p.is_file())
    before = {str(p): p.read_bytes() for p in watched}
    body = create(client, ["o1"], ["n1"]).json()
    client.put(f"{BASE}/{body['prelinks'][0]['prelink_id']}",
               json={"expected_revision": 1, "old_block_ids": ["o1", "o2"], "new_block_ids": ["n1"]})
    assert {str(p): p.read_bytes() for p in watched} == before


def test_sheet_map_change_does_not_touch_identity(client, world):
    create(client, ["o1"], ["n1"])
    before = drafts_file().read_bytes()
    paths.sheet_links_path(SID, PID).write_text(json.dumps({"links": [
        {"id": "l9", "left_pages": [2], "right_pages": [1]}]}), encoding="utf-8")
    assert validity_of(client) == [("PL-1", "VALID", [])]
    assert drafts_file().read_bytes() == before


# ── another PDF is a hard boundary: no re-binding, even by an explicit save ──
def _change_pdf(world, side="OLD"):
    world["documents"][side]["pdf"].write_bytes(b"%PDF another version")
    fresh()


def _item(label):
    state = json.loads(drafts_file().read_text(encoding="utf-8"))
    item = next(i for i in state["prelinks"] if i["label_no"] == int(label[3:]))
    return item, state["source_identities"][item["source_identity_id"]]


@pytest.mark.parametrize("side", ["OLD", "NEW"])
def test_save_after_a_pdf_change_is_refused_and_the_link_stays_stale(client, world, side):
    pid = create(client, ["o1"], ["n1"]).json()["prelinks"][0]["prelink_id"]
    _change_pdf(world, side)
    before = drafts_file().read_bytes()
    for new_ids in (["n1"], ["n1", "n2"]):     # the same composition and another one
        response = client.put(f"{BASE}/{pid}", json={"expected_revision": 1, "old_block_ids": ["o1"],
                                                    "new_block_ids": new_ids})
        assert response.status_code == 409 and error(response) == "PRELINK_SOURCE_CHANGED"
        assert response.json()["detail"]["prelink_id"] == pid
    assert drafts_file().read_bytes() == before
    assert validity_of(client) == [("PL-1", "STALE_PDF", ["PDF_CHANGED"])]


def test_version_change_is_a_source_change_too(client, world):
    create(client, ["o1"], ["n1"])
    state = json.loads(drafts_file().read_text(encoding="utf-8"))
    identity = state["source_identities"][state["prelinks"][0]["source_identity_id"]]
    assert prelink_drafts.same_source(identity, identity)
    other = {**identity, "OLD": {**identity["OLD"], "version_id": "v-other"}}
    assert not prelink_drafts.same_source(other, identity)
    assert not prelink_drafts.same_source({**identity, "NEW": {**identity["NEW"], "pdf_sha256": "0" * 64}}, identity)


def test_the_same_link_for_the_current_pdf_is_a_new_link(client, world):
    old_id = create(client, ["o1"], ["n1"]).json()["prelinks"][0]["prelink_id"]
    _change_pdf(world)
    response = create(client, ["o1"], ["n1"])
    assert response.status_code == 201, response.text
    body = response.json()
    rows = {p["label"]: p for p in body["prelinks"]}
    assert (rows["PL-1"]["prelink_id"], rows["PL-1"]["validity"]) == (old_id, "STALE_PDF")
    assert rows["PL-2"]["prelink_id"] != old_id and rows["PL-2"]["validity"] == "VALID"
    assert rows["PL-2"]["source_identity_id"] == body["live_source_identity_id"] != rows["PL-1"]["source_identity_id"]
    # On the current PDF the rule of duplicates is unchanged.
    again = create(client, ["o1"], ["n1"])
    assert again.status_code == 409 and error(again) == "PRELINK_DUPLICATE"


def test_no_write_silently_rebinds_a_link_of_another_pdf(client, world):
    create(client, ["o1"], ["n1"])
    _change_pdf(world)
    stale_before, identity_before = _item("PL-1")
    other = create(client, ["o2"], ["n2"]).json()["prelinks"][-1]["prelink_id"]        # write on the new PDF
    revision = client.get(BASE).json()["revision"]
    assert client.put(f"{BASE}/{other}", json={"expected_revision": revision, "old_block_ids": ["o2", "o3"],
                                               "new_block_ids": ["n2"]}).status_code == 200
    revision = client.get(BASE).json()["revision"]
    assert client.delete(f"{BASE}/{other}", params={"expected_revision": revision}).status_code == 200
    stale_after, identity_after = _item("PL-1")
    assert (stale_after, identity_after) == (stale_before, identity_before)
    assert validity_of(client) == [("PL-1", "STALE_PDF", ["PDF_CHANGED"])]
    # Deleting stays possible.
    pid = stale_after["prelink_id"]
    revision = client.get(BASE).json()["revision"]
    assert client.delete(f"{BASE}/{pid}", params={"expected_revision": revision}).status_code == 200


def test_text_and_block_staleness_keep_the_explicit_repair_of_the_contract(client, world):
    # STALE_TEXT / STALE_BLOCKS on the same PDF: «Подтвердить заново» / «Заменить блок» by an explicit save.
    text_id = create(client, ["o1"], ["n1"]).json()["prelinks"][0]["prelink_id"]
    moved_id = create(client, ["o2"], ["n2"]).json()["prelinks"][-1]["prelink_id"]
    path = world["documents"]["NEW"]["markdown"]
    path.write_text(path.read_text(encoding="utf-8").replace("Текст NEW 1", "Текст NEW 1 (изм.)"), encoding="utf-8")
    payload = _blocks(world, "NEW")
    next(b for b in payload["blocks"] if b["block_id"] == "n2")["coords_norm"] = [0.1, 0.5, 0.9, 0.9]
    _write_blocks(world, "NEW", payload)
    assert dict((label, state) for label, state, _r in validity_of(client)) == {"PL-1": "STALE_TEXT",
                                                                                "PL-2": "STALE_BLOCKS"}
    duplicate = create(client, ["o1"], ["n1"])           # same PDF: still the same link, not a new one
    assert duplicate.status_code == 409 and error(duplicate) == "PRELINK_DUPLICATE"
    for pid, old, new in ((text_id, ["o1"], ["n1"]), (moved_id, ["o2"], ["n2"])):
        revision = client.get(BASE).json()["revision"]
        response = client.put(f"{BASE}/{pid}", json={"expected_revision": revision, "old_block_ids": old,
                                                    "new_block_ids": new})
        assert response.status_code == 200, response.text
    assert [(label, state) for label, state, _r in validity_of(client)] == [("PL-1", "VALID"), ("PL-2", "VALID")]
