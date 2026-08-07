#!/usr/bin/env python3
"""Merge a complete rejected-findings audit with successful retry results.

The base manifest defines the final case set and ordering.  A successful,
current overlay result replaces the base result only for the same ``case_id``.
Canonical identity fields must remain identical.  Source audit directories are
read-only; all generated artifacts are written to a separate output directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from compare_rejected_audit_runs import load_run


IDENTITY_FIELDS = (
    "object_id",
    "object_name",
    "discipline",
    "document",
    "version_id",
    "item_id",
    "finding_problem",
    "expert_reason",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _jsonl_bytes(rows: list[dict[str, Any]]) -> bytes:
    return b"".join(
        json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        for row in rows
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def merge_runs(
    base_dir: Path,
    overlay_dir: Path,
    output_dir: Path,
    *,
    allow_incomplete_base: bool = False,
) -> dict[str, Any]:
    base_dir = base_dir.resolve()
    overlay_dir = overlay_dir.resolve()
    output_dir = output_dir.resolve()
    if output_dir in {base_dir, overlay_dir}:
        raise ValueError("Output directory must differ from both source audit directories")

    base = load_run(base_dir)
    overlay = load_run(overlay_dir)
    base_ids = set(base["manifest"])
    overlay_ids = set(overlay["manifest"])
    base_success_ids = set(base["successful_results"])
    base_complete = len(base_success_ids) == len(base["manifest"])
    if not base_complete and not allow_incomplete_base:
        raise ValueError("Base audit must have a successful current result for every case")
    final_order = [
        case_id
        for case_id in base["manifest_order"]
        if case_id in base_success_ids
    ]
    final_ids = set(final_order)
    outside = sorted(overlay_ids - final_ids)
    if outside:
        raise ValueError(
            f"Overlay contains {len(outside)} case IDs outside successful base results"
        )

    mismatches: dict[str, list[str]] = {}
    for field in IDENTITY_FIELDS:
        bad = sorted(
            case_id
            for case_id in overlay_ids
            if base["manifest"][case_id].get(field)
            != overlay["manifest"][case_id].get(field)
        )
        if bad:
            mismatches[field] = bad
    if mismatches:
        counts = {field: len(case_ids) for field, case_ids in mismatches.items()}
        raise ValueError(f"Canonical identity mismatch: {counts}")

    overlay_success = overlay["successful_results"]
    manifest_rows: list[dict[str, Any]] = []
    result_rows: list[dict[str, Any]] = []
    replaced_ids: list[str] = []
    retained_ids: list[str] = []
    for case_id in final_order:
        if case_id in overlay_success:
            manifest = overlay["manifest"][case_id]
            result = overlay_success[case_id]
            replaced_ids.append(case_id)
        else:
            manifest = base["manifest"][case_id]
            result = base["successful_results"][case_id]
            retained_ids.append(case_id)
        if str(manifest.get("input_hash") or "") != str(result.get("input_hash") or ""):
            raise ValueError(f"Result/input hash mismatch for {case_id}")
        manifest_rows.append(manifest)
        result_rows.append(result)

    verdicts = Counter(str(row.get("verdict") or "") for row in result_rows)
    actions = Counter(str(row.get("recommended_action") or "") for row in result_rows)
    candidate_rows = [
        row for row in result_rows if row.get("recommended_action") == "manual_recheck"
    ]
    generated_at = _now_iso()
    base_inventory = (
        json.loads((base_dir / "inventory.json").read_text(encoding="utf-8"))
        if (base_dir / "inventory.json").is_file()
        else {}
    )
    base_summary = (
        json.loads((base_dir / "summary.json").read_text(encoding="utf-8"))
        if (base_dir / "summary.json").is_file()
        else {}
    )
    merge_metadata = {
        "base_dir": str(base_dir),
        "overlay_dir": str(overlay_dir),
        "base_manifest_cases": len(base["manifest"]),
        "base_cases": len(final_order),
        "base_results_missing": len(base["manifest"]) - len(final_order),
        "incomplete_base_explicitly_allowed": bool(allow_incomplete_base),
        "overlay_cases": len(overlay["manifest"]),
        "overlay_successful_replacements": len(replaced_ids),
        "base_results_retained": len(retained_ids),
        "identity_fields_checked": list(IDENTITY_FIELDS),
    }
    summary = {
        "schema_version": 1,
        "generated_at": generated_at,
        "period": base_inventory.get("period") or base_summary.get("period") or "2026-07",
        "filters": base_summary.get("filters") or base_inventory.get("filters") or {},
        "selected_cases": len(result_rows),
        "completed": len(result_rows),
        "remaining": 0,
        "latest_errors": 0,
        "malformed_result_lines_ignored": 0,
        "stale_results_ignored": 0,
        "verdicts": dict(verdicts),
        "manual_recheck_candidates": len(candidate_rows),
        "completion_pct": 100.0,
        "merge": merge_metadata,
    }
    inventory = json.loads(json.dumps(base_inventory, ensure_ascii=False))
    inventory.update(
        {
            "generated_at": generated_at,
            "source": "merged rejected-findings audit",
            "period": summary["period"],
            "filters": summary["filters"],
            "merge": merge_metadata,
        }
    )
    inventory_counts = dict(inventory.get("counts") or {})
    inventory_counts.update(
        {
            "selected_cases": len(result_rows),
            "source_base_manifest_cases": len(base["manifest"]),
            "source_base_successful_cases": len(final_order),
        }
    )
    inventory["counts"] = inventory_counts
    candidates = {
        "schema_version": 1,
        "generated_at": generated_at,
        "candidate_count": len(candidate_rows),
        "candidates": candidate_rows,
    }

    manifest_payload = _jsonl_bytes(manifest_rows)
    results_payload = _jsonl_bytes(result_rows)
    _atomic_write(output_dir / "manifest.jsonl", manifest_payload)
    _atomic_write(output_dir / "results.jsonl", results_payload)
    inventory_payload = _json_bytes(inventory)
    _atomic_write(output_dir / "inventory.json", inventory_payload)
    _atomic_write(output_dir / "summary.json", _json_bytes(summary))
    _atomic_write(output_dir / "candidates.json", _json_bytes(candidates))
    receipt = {
        "generated_at": generated_at,
        "base_manifest_sha256": base["manifest_sha256"],
        "overlay_manifest_sha256": overlay["manifest_sha256"],
        "merged_manifest_sha256": _sha256_bytes(manifest_payload),
        "merged_results_sha256": _sha256_bytes(results_payload),
        "merged_inventory_sha256": _sha256_bytes(inventory_payload),
        "total_cases": len(result_rows),
        "source_base_manifest_cases": len(base["manifest"]),
        "source_base_successful_cases": len(final_order),
        "incomplete_base_explicitly_allowed": bool(allow_incomplete_base),
        "updated_from_overlay": len(replaced_ids),
        "retained_from_base": len(retained_ids),
        "candidate_count": len(candidate_rows),
        "verdicts": dict(verdicts),
        "actions": dict(actions),
    }
    _atomic_write(output_dir / "merge_receipt.json", _json_bytes(receipt))
    return receipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--overlay-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--allow-incomplete-base",
        action="store_true",
        help=(
            "Merge only successful current base results. Intended for an explicitly "
            "limited pilot whose manifest contains additional pending cases."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    receipt = merge_runs(
        args.base_dir,
        args.overlay_dir,
        args.output_dir,
        allow_incomplete_base=args.allow_incomplete_base,
    )
    print(json.dumps(receipt, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
