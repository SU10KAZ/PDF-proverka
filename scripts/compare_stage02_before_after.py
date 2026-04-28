"""Compare stage-02 block analysis before and after Flash -> Pro triage.

No API calls are made. By default the script compares the latest
`02_blocks_analysis.before_flash_pro_triage.*.json` backup with the current
`_output/02_blocks_analysis.json` and writes a compact report next to the
latest Flash -> Pro triage experiment.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from run_stage02_recall_hybrid import resolve_project  # noqa: E402


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def extract_block_analyses(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    rows = payload.get("block_analyses")
    if isinstance(rows, list):
        return [x for x in rows if isinstance(x, dict)]
    rows = payload.get("blocks")
    if isinstance(rows, list):
        return [x for x in rows if isinstance(x, dict)]
    return []


def _kv_count(row: dict) -> int:
    kv = row.get("key_values_read") or []
    return len(kv) if isinstance(kv, list) else 0


def _findings_count(row: dict) -> int:
    findings = row.get("findings") or []
    return len(findings) if isinstance(findings, list) else 0


def _summary_len(row: dict) -> int:
    return len((row.get("summary") or "").strip())


def _is_unreadable(row: dict) -> bool:
    return bool(row.get("unreadable_text"))


def summarize_rows(rows: list[dict]) -> dict:
    block_ids = [str(r.get("block_id", "")) for r in rows if r.get("block_id")]
    source_counts = Counter(str(r.get("_triage_source", "unknown")) for r in rows)
    return {
        "total_blocks": len(rows),
        "unique_blocks": len(set(block_ids)),
        "duplicate_blocks": len(block_ids) - len(set(block_ids)),
        "total_findings": sum(_findings_count(r) for r in rows),
        "blocks_with_findings": sum(1 for r in rows if _findings_count(r) > 0),
        "total_key_values": sum(_kv_count(r) for r in rows),
        "empty_key_values": sum(1 for r in rows if _kv_count(r) == 0),
        "empty_summary": sum(1 for r in rows if _summary_len(r) == 0),
        "short_summary": sum(1 for r in rows if 0 < _summary_len(r) < 40),
        "unreadable_blocks": sum(1 for r in rows if _is_unreadable(r)),
        "triage_source_counts": dict(sorted(source_counts.items())),
    }


def compare_rows(before_rows: list[dict], after_rows: list[dict]) -> tuple[dict, list[dict]]:
    before = {str(r.get("block_id")): r for r in before_rows if r.get("block_id")}
    after = {str(r.get("block_id")): r for r in after_rows if r.get("block_id")}
    all_ids = sorted(set(before) | set(after))

    rows: list[dict] = []
    for bid in all_ids:
        b = before.get(bid, {})
        a = after.get(bid, {})
        before_findings = _findings_count(b)
        after_findings = _findings_count(a)
        before_kv = _kv_count(b)
        after_kv = _kv_count(a)
        before_summary = _summary_len(b)
        after_summary = _summary_len(a)
        reasons: list[str] = []

        if bid not in before:
            reasons.append("new_block_after")
        if bid not in after:
            reasons.append("missing_after")
        if before_findings > 0 and after_findings == 0:
            reasons.append("findings_disappeared")
        if before_findings == 0 and after_findings > 0:
            reasons.append("findings_added")
        if before_kv >= 5 and after_kv < before_kv * 0.5:
            reasons.append("kv_collapse")
        if before_summary >= 100 and after_summary < 40:
            reasons.append("summary_collapse")
        if not _is_unreadable(b) and _is_unreadable(a):
            reasons.append("unreadable_regression")
        if _is_unreadable(b) and not _is_unreadable(a):
            reasons.append("unreadable_recovered")
        if a.get("_triage_source") == "pro":
            reasons.append("pro_rechecked")

        rows.append(
            {
                "block_id": bid,
                "page": a.get("page", b.get("page", "")),
                "after_source": a.get("_triage_source", ""),
                "before_findings": before_findings,
                "after_findings": after_findings,
                "findings_delta": after_findings - before_findings,
                "before_key_values": before_kv,
                "after_key_values": after_kv,
                "key_values_delta": after_kv - before_kv,
                "before_summary_len": before_summary,
                "after_summary_len": after_summary,
                "summary_len_delta": after_summary - before_summary,
                "before_unreadable": _is_unreadable(b),
                "after_unreadable": _is_unreadable(a),
                "review_reasons": ", ".join(reasons),
            }
        )

    before_summary = summarize_rows(before_rows)
    after_summary = summarize_rows(after_rows)
    changed_rows = [
        r for r in rows
        if r["findings_delta"] or r["key_values_delta"] or r["summary_len_delta"]
        or r["before_unreadable"] != r["after_unreadable"]
        or r["after_source"] == "pro"
        or "missing_after" in r["review_reasons"]
    ]
    suspicious_rows = [
        r for r in rows
        if any(
            marker in r["review_reasons"]
            for marker in (
                "missing_after",
                "findings_disappeared",
                "kv_collapse",
                "summary_collapse",
                "unreadable_regression",
            )
        )
    ]
    summary = {
        "before": before_summary,
        "after": after_summary,
        "delta": {
            "total_blocks": after_summary["total_blocks"] - before_summary["total_blocks"],
            "total_findings": after_summary["total_findings"] - before_summary["total_findings"],
            "blocks_with_findings": after_summary["blocks_with_findings"] - before_summary["blocks_with_findings"],
            "total_key_values": after_summary["total_key_values"] - before_summary["total_key_values"],
            "empty_key_values": after_summary["empty_key_values"] - before_summary["empty_key_values"],
            "empty_summary": after_summary["empty_summary"] - before_summary["empty_summary"],
            "short_summary": after_summary["short_summary"] - before_summary["short_summary"],
            "unreadable_blocks": after_summary["unreadable_blocks"] - before_summary["unreadable_blocks"],
        },
        "changed_blocks": len(changed_rows),
        "suspicious_blocks": len(suspicious_rows),
        "pro_rechecked_blocks": sum(1 for r in rows if r["after_source"] == "pro"),
    }
    return summary, rows


def _latest_backup(output_dir: Path) -> Path:
    backups = sorted(output_dir.glob("02_blocks_analysis.before_flash_pro_triage.*.json"))
    if not backups:
        raise FileNotFoundError(f"No Flash+Pro backup found in {output_dir}")
    return backups[-1]


def _default_out_dir(project_dir: Path) -> Path:
    root = project_dir / "_experiments" / "stage02_flash_pro_triage"
    runs = sorted([p for p in root.glob("*") if p.is_dir()]) if root.exists() else []
    if runs:
        return runs[-1]
    return project_dir / "_experiments" / "stage02_flash_pro_triage_compare" / _ts()


def build_markdown(summary: dict, before_path: Path, after_path: Path) -> str:
    before = summary["before"]
    after = summary["after"]
    delta = summary["delta"]
    return "\n".join(
        [
            "# Stage 02 Before/After Comparison",
            "",
            f"- Before: `{before_path}`",
            f"- After: `{after_path}`",
            "",
            "| Metric | Before | After | Delta |",
            "|---|---:|---:|---:|",
            f"| Blocks | {before['total_blocks']} | {after['total_blocks']} | {delta['total_blocks']} |",
            f"| Total findings | {before['total_findings']} | {after['total_findings']} | {delta['total_findings']} |",
            f"| Blocks with findings | {before['blocks_with_findings']} | {after['blocks_with_findings']} | {delta['blocks_with_findings']} |",
            f"| Total KV | {before['total_key_values']} | {after['total_key_values']} | {delta['total_key_values']} |",
            f"| Empty KV blocks | {before['empty_key_values']} | {after['empty_key_values']} | {delta['empty_key_values']} |",
            f"| Empty summary blocks | {before['empty_summary']} | {after['empty_summary']} | {delta['empty_summary']} |",
            f"| Short summary blocks | {before['short_summary']} | {after['short_summary']} | {delta['short_summary']} |",
            f"| Unreadable blocks | {before['unreadable_blocks']} | {after['unreadable_blocks']} | {delta['unreadable_blocks']} |",
            "",
            "## Flash -> Pro",
            "",
            f"- Pro rechecked blocks: {summary['pro_rechecked_blocks']}",
            f"- Changed blocks: {summary['changed_blocks']}",
            f"- Suspicious blocks for manual review: {summary['suspicious_blocks']}",
            f"- After source counts: `{after['triage_source_counts']}`",
            "",
            "## Manual Review Shortlist",
            "",
            "See `before_after_block_deltas.csv`; filter non-empty `review_reasons`.",
            "",
        ]
    )


def run_compare(args: argparse.Namespace) -> Path:
    resolution = resolve_project(args.pdf, Path(args.project_dir) if args.project_dir else None)
    project_dir = resolution.project_dir
    output_dir = project_dir / "_output"
    before_path = Path(args.before) if args.before else _latest_backup(output_dir)
    after_path = Path(args.after) if args.after else output_dir / "02_blocks_analysis.json"
    if not after_path.exists():
        raise FileNotFoundError(f"After file not found: {after_path}")

    before_rows = extract_block_analyses(_load_json(before_path))
    after_rows = extract_block_analyses(_load_json(after_path))
    summary, rows = compare_rows(before_rows, after_rows)

    out_dir = Path(args.out_dir) if args.out_dir else _default_out_dir(project_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    _save_json(out_dir / "before_after_comparison.json", {
        "project_dir": str(project_dir),
        "before_path": str(before_path),
        "after_path": str(after_path),
        **summary,
    })
    _save_csv(out_dir / "before_after_block_deltas.csv", rows)
    (out_dir / "before_after_comparison.md").write_text(
        build_markdown(summary, before_path, after_path),
        encoding="utf-8",
    )
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare stage-02 before/after files")
    parser.add_argument("--pdf", required=True)
    parser.add_argument("--project-dir")
    parser.add_argument("--before", help="Explicit old 02_blocks_analysis JSON")
    parser.add_argument("--after", help="Explicit new 02_blocks_analysis JSON")
    parser.add_argument("--out-dir")
    return parser.parse_args()


def main() -> None:
    out_dir = run_compare(parse_args())
    print(f"Comparison artifacts: {out_dir}")


if __name__ == "__main__":
    main()
