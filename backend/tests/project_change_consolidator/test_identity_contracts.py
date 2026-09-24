"""Phase A: contracts + canonical hint/card identity + read-only source loader (0 model calls)."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import jsonschema
import pytest

from backend.app.services.project_change_consolidator import contracts as C
from backend.app.services.project_change_consolidator.identity import (
    IdentityError, canonical_hints, card_table, hint_ref,
)
from backend.app.services.project_change_consolidator.source_view import BundleError, load_frozen_bundle


def _hint(hid, text):
    return {"hint_id": hid, "kind": "SOURCE_CONFLICT", "engineering_subject": text, "suspected_change": text,
            "old_pages": [1], "new_pages": [1], "evidence_items": [], "missing_proof_or_conflict": text}


REGIONS = [("R-A", [_hint("H001", "a1"), _hint("H002", "a2")]), ("R-B", [_hint("H001", "b1")]),
           ("R-C", [_hint("H001", "c1"), _hint("H002", "c2")])]
RESULT_HINTS = [h for _, hs in REGIONS for h in hs]


def test_schemas_are_valid_draft_2020_12():
    jsonschema.Draft202012Validator.check_schema(C.INPUT_SCHEMA)
    jsonschema.Draft202012Validator.check_schema(C.DECISION_SCHEMA)


def test_decision_schema_uses_transport_safe_keywords_only():
    allowed = {"type", "properties", "required", "additionalProperties", "enum", "items", "minItems", "maxItems"}
    seen = set()

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key != "properties":
                    seen.add(key)
                    walk(value)
                else:
                    for sub in value.values():
                        walk(sub)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(C.DECISION_SCHEMA)
    assert seen <= allowed, seen - allowed


def test_same_hint_id_in_different_regions_gets_different_keys():
    table = canonical_hints(RESULT_HINTS, REGIONS, method="TEST")
    keys = [e.key for e in table.entries]
    assert keys == [("R-A", "H001"), ("R-A", "H002"), ("R-B", "H001"), ("R-C", "H001"), ("R-C", "H002")]
    assert len(set(keys)) == 5
    assert table.get("R-B", "H001").hint["engineering_subject"] == "b1"
    assert table.get("R-C", "H001").hint["engineering_subject"] == "c1"
    assert table.receipt["distinct_hint_ids"] == 2 and table.receipt["hints_sharing_an_id"] == 5
    assert hint_ref("R-C", "H002") == "R-C/H002"


def test_mapping_sha_is_deterministic():
    a = canonical_hints(RESULT_HINTS, REGIONS, method="TEST").receipt["mapping_sha256"]
    b = canonical_hints(copy.deepcopy(RESULT_HINTS), copy.deepcopy(REGIONS), method="TEST").receipt["mapping_sha256"]
    assert a == b


@pytest.mark.parametrize("mutate,code", [
    (lambda r, h: ([(r[0][0], r[0][1][:-1])] + r[1:], h), "HINT_ADAPTER_COUNT_MISMATCH"),
    (lambda r, h: ([(r[0][0], [dict(r[0][1][0], suspected_change="x")] + r[0][1][1:])] + r[1:], h),
     "HINT_ADAPTER_CONTENT_MISMATCH"),
    (lambda r, h: ([r[2], r[1], r[0]], h), "HINT_ADAPTER_CONTENT_MISMATCH"),
    (lambda r, h: ([(r[0][0], [r[0][1][0], r[0][1][0]])] + r[1:], [h[0]] + h[:1] + h[2:]), "HINT_KEY_COLLISION"),
])
def test_adapter_fails_closed(mutate, code):
    regions, hints = mutate(copy.deepcopy(REGIONS), copy.deepcopy(RESULT_HINTS))
    with pytest.raises(IdentityError) as exc:
        canonical_hints(hints, regions, method="TEST")
    assert exc.value.code == code


def test_unserializable_hint_identity_fails_closed():
    with pytest.raises(IdentityError) as exc:
        canonical_hints([_hint("H/1", "x")], [("R-A", [_hint("H/1", "x")])], method="TEST")
    assert exc.value.code == "HINT_ID_UNSERIALIZABLE"


def test_card_table_rejects_collision_and_missing_region():
    card = {"projectchange_id": "PC-1"}
    with pytest.raises(IdentityError) as exc:
        card_table({"projectchanges": [card, dict(card)], "projectchange_regions": {"PC-1": "R-A"}})
    assert exc.value.code == "CARD_ID_COLLISION"
    with pytest.raises(IdentityError) as exc:
        card_table({"projectchanges": [card], "projectchange_regions": {}})
    assert exc.value.code == "CARD_REGION_MISSING"
    table = card_table({"projectchanges": [card], "projectchange_regions": {"PC-1": "R-A"}})
    assert table["PC-1"].region_id == "R-A"


def _write(path: Path, value) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(value, ensure_ascii=False).encode()
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def _bundle_spec(root: Path):
    result = {"run_id": "run1", "pair_id": "pair1", "session_id": "s1", "projectchanges": [],
              "projectchange_regions": {}, "unresolved_hints": RESULT_HINTS}
    miner = {"run_id": "run1", "regions": [{"region_id": r, "unresolved_hints": h, "projectchanges": []}
                                            for r, h in REGIONS]}
    page = {"side": "OLD", "physical_page": 1, "blocks": [{"block_id": "b1", "modality": "TEXT", "bbox": [0, 0, 1, 1]}]}
    return {
        "source_run_id": "run1",
        "result": {"path": str(root / "res.json"), "sha256": _write(root / "res.json", result)},
        "hint_regions": {"kind": "miner_results", "path": str(root / "miner.json"), "sha256": _write(root / "miner.json", miner)},
        "semantic_map": {"path": str(root / "map.json"), "sha256": _write(root / "map.json", {"regions": []})},
        "source_package": {"dir": str(root / "src"),
                           "page_sha256": {"old/p001/page.json": _write(root / "src/old/p001/page.json", page)}},
    }


def test_frozen_bundle_happy_path(tmp_path):
    bundle = load_frozen_bundle(_bundle_spec(tmp_path))
    assert bundle.source_run_id == "run1" and len(bundle.hint_table().entries) == 5
    assert bundle.block("OLD", 1, "b1")["modality"] == "TEXT"
    assert bundle.block("OLD", 1, "zz") is None


def test_frozen_bundle_rejects_sha_mismatch_and_forbidden_paths(tmp_path):
    spec = _bundle_spec(tmp_path)
    bad = copy.deepcopy(spec)
    bad["result"]["sha256"] = "0" * 64
    with pytest.raises(BundleError) as exc:
        load_frozen_bundle(bad)
    assert exc.value.code == "SOURCE_SHA_MISMATCH"
    review = tmp_path / "quality_review" / "res.json"
    bad = copy.deepcopy(spec)
    bad["result"] = {"path": str(review), "sha256": _write(review, {"run_id": "run1"})}
    with pytest.raises(BundleError) as exc:
        load_frozen_bundle(bad)
    assert exc.value.code == "FORBIDDEN_SOURCE_PATH"
    bad = copy.deepcopy(spec)
    bad["source_run_id"] = "other"
    with pytest.raises(BundleError) as exc:
        load_frozen_bundle(bad)
    assert exc.value.code == "SOURCE_RUN_MISMATCH"


def test_source_package_page_hash_is_checked(tmp_path):
    spec = _bundle_spec(tmp_path)
    spec["source_package"]["page_sha256"]["old/p001/page.json"] = "1" * 64
    bundle = load_frozen_bundle(spec)
    with pytest.raises(BundleError) as exc:
        bundle.page("OLD", 1)
    assert exc.value.code == "SOURCE_PACKAGE_SHA_MISMATCH"
