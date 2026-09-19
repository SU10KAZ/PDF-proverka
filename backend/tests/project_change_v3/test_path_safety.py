"""Unsafe object/pair/comparison IDs are rejected BEFORE any filesystem access."""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.human_mapping_production import storage

UNSAFE = ["..", "../x", "x/../y", "/etc/passwd", "a/b", "a\\b", "", ".", "_smoke", "x" * 129, "ÿ", "a b", "a\x00b"]
VALID = ["obj_generic_test", "generic_pair_not_A_or_B", "4f3e5916", "p16b108b9f5", "A"]


def _tree(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*")) if root.exists() else []


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    from backend.app.api.routers import human_mapping

    app = FastAPI()
    app.include_router(human_mapping.router)
    app.include_router(human_mapping.api_router)
    return TestClient(app)


@pytest.mark.parametrize("value", UNSAFE)
def test_storage_rejects_unsafe_ids_without_touching_disk(tmp_path, monkeypatch, value):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    for kind in ("object", "pair"):
        with pytest.raises(storage.InvalidScopeId):
            storage.require_safe_id(value, kind)
    with pytest.raises(storage.InvalidScopeId):
        storage.pair_dir(value, "ok_pair")
    with pytest.raises(storage.InvalidScopeId):
        storage.pair_dir("ok_object", value)
    with pytest.raises(storage.InvalidScopeId):
        storage.append_review("ok_object", value, {"x": 1})
    with pytest.raises(storage.InvalidScopeId):
        storage.append_block_link(value, "ok_pair", {"x": 1})
    assert _tree(tmp_path / "comparison") == []


@pytest.mark.parametrize("value", VALID)
def test_storage_accepts_valid_ids(value):
    assert storage.require_safe_id(value, "pair") == value


def test_distinct_unsafe_ids_never_collapse(tmp_path, monkeypatch):
    # _safe_id-style stripping would map both to "ab"; the strict validator rejects both.
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    for value in ("a/b", "a.b"):
        with pytest.raises(storage.InvalidScopeId):
            storage.pair_dir("obj", value)


@pytest.mark.parametrize("value", ["..", "../x", "x/../y", "/etc", "a/b", "a\\b", "", "_smoke", "ÿ"])
@pytest.mark.parametrize("field", ["object", "comparison", "pair"])
def test_page_query_ids_are_http_400(client, tmp_path, field, value):
    params = {"object": "obj_generic_test", "comparison": "generic_pair_not_A_or_B"}
    if field == "pair":
        params.pop("comparison")
    params[field] = value
    response = client.get("/human-mapping/", params=params)
    assert response.status_code == 400
    assert _tree(tmp_path / "comparison") == []


# Path-segment IDs: values that decode into one segment reach the handler and are
# rejected with 400; values containing "/" cannot match a single path segment,
# so routing stops them (404/405) before any handler or filesystem access.
SEGMENT_400 = ["%2E%2E", "a%5Cb", "_smoke", "%C3%BF", "a%20b"]
SEGMENT_ROUTING = ["..%2Fx", "x%2F..%2Fy", "%2Fetc", "a%2Fb"]


@pytest.mark.parametrize("encoded", SEGMENT_400 + SEGMENT_ROUTING)
@pytest.mark.parametrize("position", ["object", "pair"])
def test_api_path_ids_never_escape(client, tmp_path, encoded, position):
    obj = encoded if position == "object" else "obj_generic_test"
    pair = encoded if position == "pair" else "generic_pair_not_A_or_B"
    base = f"/api/human-mapping/objects/{obj}/comparisons/{pair}"
    expected = {400} if encoded in SEGMENT_400 else {400, 404, 405}
    for method, suffix, body in (
        ("get", "/ui-data", None),
        ("get", "/reviews", None),
        ("get", "/block-links", None),
        ("post", "/reviews", {"region_id": "R", "old_block_ids": ["a"], "new_block_ids": ["b"], "status": "HUMAN_CONFIRMED"}),
        ("post", "/block-links", {"event_type": "ADD_BLOCK_LINK", "region_id": "R", "link_id": "l",
                                  "old_block_id": "a", "new_block_id": "b"}),
        ("get", "/assets/x.png", None),
    ):
        response = getattr(client, method)(base + suffix, **({"json": body} if body else {}))
        assert response.status_code in expected, (method, suffix, response.status_code)
    assert _tree(tmp_path / "comparison") == []


def test_asset_traversal_inside_valid_scope_is_denied(client, tmp_path):
    root = tmp_path / "comparison" / "human_mapping" / "obj_generic_test" / "generic_pair_not_A_or_B"
    (root / "assets").mkdir(parents=True)
    (root / "secret.txt").write_text("secret", encoding="utf-8")
    (root / "assets_evil").mkdir()
    (root / "assets_evil" / "x.png").write_bytes(b"\x89PNGevil")
    base = "/api/human-mapping/objects/obj_generic_test/comparisons/generic_pair_not_A_or_B/assets/"
    for path in ("..%2Fsecret.txt", "%2E%2E/secret.txt", "..%2Fassets_evil%2Fx.png"):
        assert client.get(base + path).status_code == 404


def test_valid_generic_scope_writes_only_its_directory(client, tmp_path):
    hm = tmp_path / "comparison" / "human_mapping" / "obj_generic_test" / "generic_pair_not_A_or_B"
    hm.mkdir(parents=True)
    (hm / "ui_data.json").write_text(
        '{"schema":"human-mapping-ui-data/1","pair":"generic_pair_not_A_or_B","pair_key":"generic_pair_not_A_or_B",'
        '"regions":[{"id":"R","old_blocks":[{"id":"a","type":"TEXT"}],"new_blocks":[{"id":"b","type":"TEXT"}],'
        '"pages":{"OLD":[],"NEW":[]}}]}', encoding="utf-8")
    base = "/api/human-mapping/objects/obj_generic_test/comparisons/generic_pair_not_A_or_B"
    assert client.post(base + "/reviews", json={"region_id": "R", "old_block_ids": ["a"], "new_block_ids": ["b"],
                                                "status": "HUMAN_CONFIRMED"}).status_code == 200
    assert _tree(tmp_path / "comparison") == [
        "human_mapping", "human_mapping/obj_generic_test", "human_mapping/obj_generic_test/generic_pair_not_A_or_B",
        "human_mapping/obj_generic_test/generic_pair_not_A_or_B/reviews.jsonl",
        "human_mapping/obj_generic_test/generic_pair_not_A_or_B/reviews.jsonl.lock",
        "human_mapping/obj_generic_test/generic_pair_not_A_or_B/ui_data.json",
    ]


def test_fixture_aliases_only_for_fixture_object(client):
    # Pair "A" of an arbitrary object is an ordinary (absent) comparison, not fixture A.
    assert client.get("/api/human-mapping/objects/obj_generic_test/comparisons/A/ui-data").status_code == 404
    assert client.get("/api/human-mapping/objects/obj_generic_test/assets/A_old_001.png").status_code == 404
    fixture = client.get("/api/human-mapping/objects/4f3e5916/comparisons/A/ui-data")
    assert fixture.status_code == 200 and len(fixture.json()["regions"]) == 25
    image = fixture.json()["regions"][0]["pages"]["OLD"][0]["image"]
    assert image.startswith("/api/human-mapping/objects/4f3e5916/comparisons/A/assets/")
    assert client.get(image).content.startswith(b"\x89PNG")


def test_page_without_comparison(client):
    assert client.get("/human-mapping/", params={"object": "obj_generic_test"}).status_code == 400
    assert client.get("/human-mapping/", params={"comparison": "generic_pair_not_A_or_B"}).status_code == 400
    chooser = client.get("/human-mapping/")
    assert chooser.status_code == 200 and '"pair": null' in chooser.text and '"fixture_nav": true' in chooser.text
