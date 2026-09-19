"""Human Mapping from a REAL comparison pair seeded by a source-identical sealed fixture.

Binding rule: both source PDFs of the real pair are byte-identical (sha256) to
an approved fixture's OLD/NEW sources.  The frozen mapping is initial read-only
data; every human write stays under the real object/pair; fixture files and
stores are never written.  Zero model calls.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import human_mapping
from backend.app.services.human_mapping_production import fixture_binding
from backend.tests.project_change_v3 import generic_fixture as gf

REAL_FIXTURES = human_mapping.FIXTURES
REAL_IDENTITY = fixture_binding.IDENTITY


def sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree_digest(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture
def env(tmp_path, monkeypatch):
    comparison_root = tmp_path / "comparison"
    monkeypatch.setenv("COMPARISON_ROOT", str(comparison_root))
    built = gf.build_comparison(tmp_path)
    from backend.app.services.project_change_v3 import scope

    monkeypatch.setattr(scope, "_object_stage_paths", lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])})
    # An approved fixture whose sources ARE this real pair's PDFs: the sealed АР1 mapping as data.
    fixtures = tmp_path / "data" / "human_mapping_fixtures"
    fixtures.mkdir(parents=True)
    shutil.copy(REAL_FIXTURES / "UI_DATA_PAIR_A.json", fixtures / "UI_DATA_PAIR_A.json")
    (fixtures / "assets").symlink_to(REAL_FIXTURES / "assets")
    old, new = sha(built["left"]["pdf_path"]), sha(built["right"]["pdf_path"])

    def identity(old_sha=old, new_sha=new, ui_sha=None):
        record = {"schema": "human-mapping-fixture-source-identity/1", "fixtures_dir": fixtures.name, "fixtures": [{
            "fixture_alias": "seed", "pair_key": "sealed_pair_key", "label": "АР1", "ui_data": "UI_DATA_PAIR_A.json",
            "ui_data_sha256": ui_sha or sha(fixtures / "UI_DATA_PAIR_A.json"), "regions": 25,
            "old": {"pdf_sha256": old_sha}, "new": {"pdf_sha256": new_sha}}]}
        path = fixtures.parent / "human_mapping_fixture_sources.json"
        path.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
        monkeypatch.setattr(fixture_binding, "IDENTITY", path)
        fixture_binding.clear_cache()
        return path

    identity()
    app = FastAPI()
    app.include_router(human_mapping.router)
    app.include_router(human_mapping.api_router)
    yield {**built, "client": TestClient(app), "identity": identity, "fixtures": fixtures,
           "root": comparison_root, "old": old, "new": new}
    fixture_binding.clear_cache()


def api(pair=gf.PAIR_ID):
    return f"/api/human-mapping/objects/{gf.OBJECT_ID}/comparisons/{pair}"


def test_real_pair_opens_the_source_identical_fixture_mapping(env):
    body = env["client"].get(api() + "/ui-data").json()
    assert len(body["regions"]) == 25
    assert body["pair"] == body["pair_key"] == gf.PAIR_ID and body["object_id"] == gf.OBJECT_ID
    assert body["session_id"] == env["session_id"]
    seed = body["seed"]
    assert seed["match_status"] == "BOUND" and seed["role"] == "INITIAL_HM_DATA_READ_ONLY"
    assert (seed["old_sha256"], seed["new_sha256"]) == (env["old"], env["new"])
    assert seed["fixture_source"]["ui_data"] == "UI_DATA_PAIR_A.json" and "_ui_data_path" not in seed
    page = body["regions"][0]["pages"]["OLD"][0]
    assert page["image"].startswith(api() + "/assets/") and page["blocks"]
    image = env["client"].get(page["image"])
    assert image.status_code == 200 and image.headers["content-type"] == "image/png"

    html = env["client"].get(f"/human-mapping/?object={gf.OBJECT_ID}&comparison={gf.PAIR_ID}").text
    context = json.loads(html.split("const CTX=", 1)[1].split(";\n", 1)[0])
    assert context["pair"] == gf.PAIR_ID and context["fixture_letter"] is None and not context["fixture_nav"]
    assert context["data_source"] == "SOURCE_IDENTICAL_FIXTURE_SEED" and context["label"] == "АР1"
    assert context["session_id"] == env["session_id"]


def test_human_writes_stay_in_the_real_pair_scope(env):
    client, fixtures = env["client"], env["fixtures"]
    fixture_before = tree_digest(fixtures)
    real_fixture_before = sha(REAL_FIXTURES / "UI_DATA_PAIR_A.json")
    data = client.get(api() + "/ui-data").json()
    region = next(r for r in data["regions"] if len(r["old_blocks"]) >= 2 and len(r["new_blocks"]) >= 2)
    o1, o2 = (b["id"] for b in region["old_blocks"][:2])
    n1 = region["new_blocks"][0]["id"]
    rows = []
    for status in ("HUMAN_CONFIRMED", "HUMAN_REJECTED", "HUMAN_UNCERTAIN"):
        response = client.post(api() + "/reviews", json={
            "region_id": region["id"], "status": status, "old_block_ids": [o1], "new_block_ids": [n1],
            "previous_review_id": rows[-1]["review_id"] if rows else None})
        assert response.status_code == 200, response.text
        rows.append(response.json())
    for event in ({"event_type": "ADD_BLOCK_LINK", "link_id": "human:1", "old_block_id": o1, "new_block_id": n1},
                  {"event_type": "REASSIGN_BLOCK_LINK", "link_id": "human:2", "old_block_id": o2, "new_block_id": n1,
                   "previous_link_id": "human:1"},
                  {"event_type": "DELETE_BLOCK_LINK", "link_id": "human:2", "old_block_id": o2, "new_block_id": n1}):
        response = client.post(api() + "/block-links", json={"region_id": region["id"], **event})
        assert response.status_code == 200, response.text
        rows.append(response.json())
    for row in rows:
        assert row["object_id"] == gf.OBJECT_ID and row["pair"] == row["pair_key"] == row["comparison_id"] == gf.PAIR_ID

    stored = sorted(str(p.relative_to(env["root"])) for p in env["root"].joinpath("human_mapping").rglob("*.jsonl"))
    assert stored == [f"human_mapping/{gf.OBJECT_ID}/{gf.PAIR_ID}/human_block_link_edits.jsonl",
                      f"human_mapping/{gf.OBJECT_ID}/{gf.PAIR_ID}/reviews.jsonl"]
    assert [r["status"] for r in client.get(api() + "/reviews").json()] == [
        "HUMAN_CONFIRMED", "HUMAN_REJECTED", "HUMAN_UNCERTAIN"]
    assert len(client.get(api() + "/block-links").json()) == 3
    assert tree_digest(fixtures) == fixture_before  # the seed is only read
    assert sha(REAL_FIXTURES / "UI_DATA_PAIR_A.json") == real_fixture_before
    for key in human_mapping.PAIR_KEYS.values():  # fixture stores untouched
        assert client.get(f"/api/human-mapping/objects/{human_mapping.FIXTURE_OBJECT}/comparisons/{key}/reviews").json() == []


@pytest.mark.parametrize("mutation", ["new_differs", "old_differs", "swapped", "fixture_tampered"])
def test_no_exact_match_means_ordinary_generic_behaviour(env, mutation):
    other = "0" * 64
    kwargs = {"new_differs": {"new_sha": other}, "old_differs": {"old_sha": other},
              "swapped": {"old_sha": env["new"], "new_sha": env["old"]},
              "fixture_tampered": {"ui_sha": other}}[mutation]
    env["identity"](**kwargs)
    response = env["client"].get(api() + "/ui-data")
    assert response.status_code == 404 and response.json()["detail"] == "HUMAN_MAPPING_UI_DATA_NOT_FOUND"
    receipt = fixture_binding.bind_real_pair(gf.OBJECT_ID, gf.PAIR_ID)
    assert receipt["match_status"] == "NO_MATCH" and receipt["fixture_source"] is None
    assert env["client"].post(api() + "/reviews", json={"region_id": "A-R001", "status": "HUMAN_CONFIRMED",
                                                          "old_block_ids": ["x"], "new_block_ids": ["y"]}).status_code == 404


def test_unknown_pair_is_not_bound(env):
    assert fixture_binding.bind_real_pair(gf.OBJECT_ID, "p_missing")["match_status"] == "REAL_PAIR_NOT_FOUND"
    assert env["client"].get(api("p_missing") + "/ui-data").status_code == 404


def test_published_comparison_data_wins_over_the_seed(env):
    from backend.app.services.human_mapping_production import storage

    target = storage.pair_dir(gf.OBJECT_ID, gf.PAIR_ID) / "ui_data.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"pair": gf.PAIR_ID, "regions": [{"id": "LIVE-1"}]}), encoding="utf-8")
    assert [r["id"] for r in env["client"].get(api() + "/ui-data").json()["regions"]] == ["LIVE-1"]


def test_committed_identity_is_sealed_against_the_fixtures_and_the_snapshot():
    fixtures = fixture_binding.approved_fixtures(REAL_IDENTITY)
    assert {f["label"]: f["regions"] for f in fixtures} == {"АР1": 25, "ИОС4.2": 13}
    snapshot = json.loads((REAL_IDENTITY.parent / "project_change_preview_snapshot_v3" / "presentation.json")
                          .read_text(encoding="utf-8"))["documents"]
    for fixture in fixtures:
        ui = json.loads(fixture["ui_data_path"].read_text(encoding="utf-8"))
        assert ui["pair_key"] == fixture["pair_key"] and len(ui["regions"]) == fixture["regions"]
        for side in ("old", "new"):
            assert snapshot[f"{fixture['pair_key']}:{side}"]["source_sha256"] == fixture[side]["pdf_sha256"]


def test_binding_code_names_no_pair():
    source = Path(fixture_binding.__file__).read_text(encoding="utf-8")
    for literal in ("p11ad4a09d9", "p7b37b4e31b", "ad0a31", "caea6d", "4f3e5916", '"A"', '"B"', "АР1", "ИОС"):
        assert literal not in source
