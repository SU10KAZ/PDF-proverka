#!/usr/bin/env python3
"""Parity: production V3 Human Mapping builder vs sealed A/B UI fixtures.

Zero model calls.  Input is the frozen V3 semantic maps and prepared source
page records (no truth, no validation, no holdout).  The production builder
runs against a scratch work dir whose ``source`` is a read-only symlink to the
frozen page records; the result is compared region by region with
``backend/app/data/human_mapping_fixtures/UI_DATA_PAIR_{A,B}.json``.

Usage:
    python scripts/audit_projectchange_v3_hm_membership_parity.py --work <scratch> --out <json>
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from backend.app.services.project_change_v3.hm_builder import (  # noqa: E402
    MAPPED,
    build_human_mapping_ui_data,
)

FROZEN = Path("/home/coder/auditmanager/corpus-audits/20260914_project_change_272/ai_first_semantic_mapping_projectchange_v3")
FIXTURES = REPO / "backend/app/data/human_mapping_fixtures"


def _member(block: dict[str, Any]) -> tuple:
    return (block["id"], block["side"], block["type"], block["page"], tuple(block["bbox"]),
            block["structured_md"], json.dumps(block["tables"], ensure_ascii=False),
            block.get("relevance", ""), bool(block.get("crop")))


def _context(block: dict[str, Any]) -> tuple:
    return (block["id"], block["type"], block["page"], tuple(block["bbox"]), block["structured_md"],
            json.dumps(block["tables"], ensure_ascii=False), bool(block.get("crop")))


def audit_pair(pair: str, work: Path) -> dict[str, Any]:
    pair_work = work / f"pair_{pair.lower()}"
    pair_work.mkdir(parents=True, exist_ok=True)
    link = pair_work / "source"
    if not link.exists():
        link.symlink_to(FROZEN / "source" / f"pair_{pair.lower()}", target_is_directory=True)
    semantic_map = json.loads((FROZEN / f"PAIR_{pair}_SEMANTIC_MAP.json").read_text(encoding="utf-8"))
    fixture = json.loads((FIXTURES / f"UI_DATA_PAIR_{pair}.json").read_text(encoding="utf-8"))
    production = build_human_mapping_ui_data(
        pair_id=fixture["pair_key"], object_id="parity_audit", semantic_map=semantic_map, work_dir=pair_work,
    )
    fixture_regions = {r["id"]: r for r in fixture["regions"]}
    rows = []
    for region in production["regions"]:
        sealed = fixture_regions.get(region["id"])
        if sealed is None:
            rows.append({"region_id": region["id"], "present_in_fixture": False})
            continue
        row: dict[str, Any] = {"region_id": region["id"], "present_in_fixture": True}
        for side, key in (("OLD", "old_blocks"), ("NEW", "new_blocks")):
            prod_ids = [b["id"] for b in region[key]]
            sealed_ids = [b["id"] for b in sealed[key]]
            row[f"{side.lower()}_membership_ids_equal"] = prod_ids == sealed_ids
            row[f"{side.lower()}_membership_fields_equal"] = [_member(b) for b in region[key]] == [_member(b) for b in sealed[key]]
            row[f"{side.lower()}_members"] = [len(prod_ids), len(sealed_ids)]
            row[f"{side.lower()}_context_pages_equal"] = [p["page"] for p in region["pages"][side]] == [p["page"] for p in sealed["pages"][side]]
            row[f"{side.lower()}_context_blocks_equal"] = (
                [[_context(b) for b in p["blocks"]] for p in region["pages"][side]]
                == [[_context(b) for b in p["blocks"]] for p in sealed["pages"][side]]
            )
            row[f"{side.lower()}_context_block_count"] = sum(len(p["blocks"]) for p in region["pages"][side])
            row[f"{side.lower()}_membership_is_subset_of_context"] = set(prod_ids) <= {
                b["id"] for p in region["pages"][side] for b in p["blocks"]
            }
        row["meta_equal"] = all(region[k] == sealed[k] for k in ("domain", "scope", "reason", "confidence"))
        row["mapping_state"] = region["mapping_state"]
        row["membership_match"] = row["old_membership_fields_equal"] and row["new_membership_fields_equal"]
        row["full_match"] = row["membership_match"] and all(
            row[f"{s}_{k}"] for s in ("old", "new") for k in ("context_pages_equal", "context_blocks_equal")
        ) and row["meta_equal"] and region["mapping_state"] == MAPPED
        rows.append(row)
    return {
        "fixture_regions": len(fixture["regions"]),
        "production_regions": len(production["regions"]),
        "membership_match": sum(1 for r in rows if r.get("membership_match")),
        "full_match": sum(1 for r in rows if r.get("full_match")),
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    work = Path(args.work)
    result = {
        "schema": "projectchange-v3-hm-membership-parity/1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo_head": __import__("subprocess").run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
        "model_calls": 0,
        "truth_opened": False,
        "validation_opened": False,
        "final_holdout_opened": False,
        "notes": [
            "Asset URL layout differs by design (production assets/<side>/pNNN/..., fixture assets/<pair>_<side>_NNN...); compared as crop presence.",
            "Top-level source_semantic_map/source_sha256 differ by design: production hashes the canonical persisted map.",
        ],
        "pairs": {pair: audit_pair(pair, work) for pair in ("A", "B")},
    }
    result["membership_match_total"] = sum(p["membership_match"] for p in result["pairs"].values())
    result["full_match_total"] = sum(p["full_match"] for p in result["pairs"].values())
    result["regions_total"] = sum(p["fixture_regions"] for p in result["pairs"].values())
    Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("membership_match_total", "full_match_total", "regions_total")}
                     | {p: {k: v for k, v in r.items() if k != "rows"} for p, r in result["pairs"].items()}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
