#!/usr/bin/env python3
"""
build_human_benchmark_manifest.py
-----------------------------------
Scan project directories, collect all human-reviewed projects, and produce a
manifest JSON that pins file hashes for reproducible benchmarking.

Usage:
    python backend/scripts/build_human_benchmark_manifest.py \\
        --projects-root projects \\
        --output-dir /tmp/human_manifest

    # Filter by section
    python backend/scripts/build_human_benchmark_manifest.py \\
        --projects-root projects \\
        --sections KJ AR \\
        --output-dir /tmp/human_manifest_kj_ar

Outputs:
    <output-dir>/human_benchmark_manifest.json   — machine-readable manifest
    <output-dir>/human_benchmark_manifest.md     — human-readable summary

The manifest is the source of truth for experiment_matrix runs.
Update it by re-running this script whenever new expert_review.json files
are added to projects.

NOT connected to production pipeline. Read-only access to project artifacts.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path
from typing import Optional

# ─── sys.path bootstrap ───────────────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ─── Constants ────────────────────────────────────────────────────────────────

_DEFAULT_PROJECTS_ROOT = _PROJECT_ROOT / "projects"

# Canonical artifact filenames to hash
_ARTIFACTS = [
    "03_findings.json",
    "expert_review.json",
    "02_blocks_analysis.json",
    "document_graph.json",
]

# ─── Utilities ────────────────────────────────────────────────────────────────

def _sha256(path: Path) -> str:
    """Return hex SHA-256 of file contents."""
    h = hashlib.sha256()
    try:
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return ""


def _detect_section(project_dir: Path) -> str:
    """
    Detect section/discipline from project directory structure.

    Priority:
    1. project_info.json → section field
    2. Grandparent directory name (e.g. projects/214.Alia/KJ/proj/ → KJ)
    3. Fallback: empty string
    """
    info_path = project_dir / "project_info.json"
    if info_path.exists():
        try:
            info = json.loads(info_path.read_text(encoding="utf-8"))
            sec = str(info.get("section") or "").strip()
            if sec:
                return sec.upper()
        except (json.JSONDecodeError, OSError):
            pass

    # Walk up: project_dir parent should be section folder
    parent = project_dir.parent
    name = parent.name.strip()
    # Filter out project-root-level folders like "214. Alia (ASTERUS)"
    if name and not any(c.isdigit() for c in name[:3]):
        return name.upper()
    # parent.parent.name is discipline
    grandparent = parent.parent.name.strip()
    if grandparent and not any(c.isdigit() for c in grandparent[:3]):
        return grandparent.upper()
    return name.upper() if name else "UNKNOWN"


def _load_human_decisions(expert_review_path: Path) -> tuple[int, int]:
    """
    Parse expert_review.json and return (accepted, rejected) counts.
    Only counts finding-type decisions.
    """
    try:
        data = json.loads(expert_review_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return 0, 0

    decisions = [
        d for d in (data.get("decisions") or [])
        if d.get("item_type", "finding") == "finding"
    ]
    accepted = sum(1 for d in decisions if d.get("decision") == "accepted")
    rejected = sum(1 for d in decisions if d.get("decision") == "rejected")
    return accepted, rejected


def _load_human_decisions_detailed(
    expert_review_path: Path,
    findings_ids: set[str],
) -> dict:
    """
    Parse expert_review.json and return a detailed breakdown including:
    - total finding-type decisions
    - matched to findings (item_id is in findings_ids)
    - unmatched (item_id not in findings_ids)
    - duplicate item_ids (same item_id appears more than once)
    - accepted/rejected/other counts for matched decisions only

    Returns a dict with all fields set to 0/[] on parse error.
    """
    empty = {
        "expert_review_total_items": 0,
        "expert_review_finding_items": 0,
        "human_decisions_matched_to_findings": 0,
        "human_decisions_unmatched": 0,
        "human_decisions_duplicate_item_ids": [],
        "human_accepted_matched": 0,
        "human_rejected_matched": 0,
        "human_unknown_or_other_status": 0,
        "findings_without_human_decision": 0,
    }

    try:
        data = json.loads(expert_review_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return empty

    all_decisions = data.get("decisions") or []
    finding_decisions = [
        d for d in all_decisions
        if d.get("item_type", "finding") == "finding"
    ]

    # Detect duplicates by item_id
    from collections import Counter
    id_counts = Counter(d.get("item_id") for d in finding_decisions if d.get("item_id"))
    duplicate_ids = sorted(iid for iid, cnt in id_counts.items() if cnt > 1)

    # Matched vs unmatched (use last decision for duplicates when matching)
    # Build a deduplicated map: item_id → last decision (preserves determinism)
    deduped: dict[str, str] = {}
    for d in finding_decisions:
        iid = d.get("item_id")
        if iid:
            deduped[iid] = d.get("decision", "unknown")

    matched_ids = {iid for iid in deduped if iid in findings_ids}
    unmatched_ids = {iid for iid in deduped if iid not in findings_ids}

    accepted_matched = sum(1 for iid in matched_ids if deduped[iid] == "accepted")
    rejected_matched = sum(1 for iid in matched_ids if deduped[iid] == "rejected")
    other_matched = sum(
        1 for iid in matched_ids
        if deduped[iid] not in ("accepted", "rejected")
    )

    findings_without_decision = len(findings_ids - matched_ids)

    return {
        "expert_review_total_items": len(all_decisions),
        "expert_review_finding_items": len(finding_decisions),
        "human_decisions_matched_to_findings": len(matched_ids),
        "human_decisions_unmatched": len(unmatched_ids),
        "human_decisions_duplicate_item_ids": duplicate_ids,
        "human_accepted_matched": accepted_matched,
        "human_rejected_matched": rejected_matched,
        "human_unknown_or_other_status": other_matched,
        "findings_without_human_decision": findings_without_decision,
    }


def _load_findings_count(findings_path: Path) -> int:
    """Return number of findings in 03_findings.json."""
    try:
        raw = json.loads(findings_path.read_text(encoding="utf-8"))
        return len(raw.get("findings") or raw.get("items") or [])
    except (json.JSONDecodeError, OSError):
        return 0


def _load_findings_ids(findings_path: Path) -> set[str]:
    """Return set of finding IDs from 03_findings.json."""
    try:
        raw = json.loads(findings_path.read_text(encoding="utf-8"))
        items = raw.get("findings") or raw.get("items") or []
        return {str(f.get("id") or f.get("finding_id") or "") for f in items if f.get("id") or f.get("finding_id")}
    except (json.JSONDecodeError, OSError):
        return set()


# ─── Manifest record builder ──────────────────────────────────────────────────

def build_manifest_record(project_dir: Path) -> Optional[dict]:
    """
    Build one manifest record for a project directory.

    Returns None if the project cannot be included (missing findings or review).
    """
    output_dir = project_dir / "_output"
    findings_path = output_dir / "03_findings.json"
    review_path = output_dir / "expert_review.json"

    has_findings = findings_path.exists()
    has_review = review_path.exists()

    if not has_review:
        return None

    # Check that review has finding decisions
    try:
        data = json.loads(review_path.read_text(encoding="utf-8"))
        finding_decs = [
            d for d in (data.get("decisions") or [])
            if d.get("item_type", "finding") == "finding"
        ]
        if not finding_decs:
            return None
    except (json.JSONDecodeError, OSError):
        return None

    # Check findings
    if not has_findings:
        return None

    section = _detect_section(project_dir)
    accepted, rejected = _load_human_decisions(review_path)
    findings_count = _load_findings_count(findings_path)
    findings_ids = _load_findings_ids(findings_path)

    # Detailed human decision breakdown (matched/unmatched/duplicates)
    detailed = _load_human_decisions_detailed(review_path, findings_ids)

    # Check optional artifacts
    blocks_path = output_dir / "02_blocks_analysis.json"
    graph_path = output_dir / "document_graph.json"
    has_blocks = blocks_path.exists()
    has_graph = graph_path.exists()

    # Compute hashes for all present artifacts
    hashes: dict[str, str] = {}
    for artifact_name in _ARTIFACTS:
        artifact_path = output_dir / artifact_name
        if artifact_path.exists():
            hashes[artifact_name] = _sha256(artifact_path)

    # Build per-project warnings
    warnings: list[str] = []
    if detailed["human_decisions_duplicate_item_ids"]:
        warnings.append(
            f"duplicate_human_decisions: {detailed['human_decisions_duplicate_item_ids']}"
        )
    if detailed["human_decisions_unmatched"] > 0:
        warnings.append(
            f"unmatched_human_decisions: {detailed['human_decisions_unmatched']}"
        )
    matched_total = detailed["human_accepted_matched"] + detailed["human_rejected_matched"]
    if matched_total > findings_count:
        warnings.append(
            f"human_decisions_exceed_findings: matched={matched_total} > findings={findings_count}"
        )
    if detailed["findings_without_human_decision"] > 0:
        warnings.append(
            f"findings_without_review: {detailed['findings_without_human_decision']}"
        )

    return {
        "project_name": project_dir.name,
        "project_path": str(project_dir),
        "section": section,
        "has_findings": has_findings,
        "has_expert_review": has_review,
        "has_blocks": has_blocks,
        "has_document_graph": has_graph,
        "findings_count": findings_count,
        "human_accepted": accepted,
        "human_rejected": rejected,
        "human_decisions_total": accepted + rejected,
        # Detailed validation fields
        "expert_review_total_items": detailed["expert_review_total_items"],
        "expert_review_finding_items": detailed["expert_review_finding_items"],
        "human_decisions_matched_to_findings": detailed["human_decisions_matched_to_findings"],
        "human_decisions_unmatched": detailed["human_decisions_unmatched"],
        "human_decisions_duplicate_item_ids": detailed["human_decisions_duplicate_item_ids"],
        "human_accepted_matched": detailed["human_accepted_matched"],
        "human_rejected_matched": detailed["human_rejected_matched"],
        "human_unknown_or_other_status": detailed["human_unknown_or_other_status"],
        "findings_without_human_decision": detailed["findings_without_human_decision"],
        "warnings": warnings,
        "hashes": hashes,
    }


# ─── Full scan ────────────────────────────────────────────────────────────────

def build_manifest(
    projects_root: Path,
    sections: Optional[list[str]] = None,
    explicit_paths: Optional[list[Path]] = None,
) -> dict:
    """
    Scan all projects and build manifest.

    Args:
        projects_root: root of projects directory
        sections: optional list of section codes to filter (e.g. ["KJ", "AR"])
        explicit_paths: if provided, scan only these project paths

    Returns:
        manifest dict with records + aggregate stats
    """
    section_filter = {s.upper() for s in sections} if sections else None

    if explicit_paths:
        candidate_review_paths = [
            p / "_output" / "expert_review.json" for p in explicit_paths
        ]
    else:
        candidate_review_paths = sorted(projects_root.rglob("expert_review.json"))

    records: list[dict] = []
    skipped: list[dict] = []

    for review_path in candidate_review_paths:
        project_dir = review_path.parent.parent  # review_path is _output/expert_review.json
        record = build_manifest_record(project_dir)
        if record is None:
            skipped.append({
                "project_path": str(project_dir),
                "reason": "missing findings or no finding decisions",
            })
            continue

        if section_filter and record["section"].upper() not in section_filter:
            continue

        records.append(record)

    # Aggregate stats
    total_findings = sum(r["findings_count"] for r in records)
    total_accepted = sum(r["human_accepted"] for r in records)
    total_rejected = sum(r["human_rejected"] for r in records)
    total_accepted_matched = sum(r.get("human_accepted_matched", 0) for r in records)
    total_rejected_matched = sum(r.get("human_rejected_matched", 0) for r in records)
    total_unmatched = sum(r.get("human_decisions_unmatched", 0) for r in records)
    all_duplicate_ids = [
        iid
        for r in records
        for iid in r.get("human_decisions_duplicate_item_ids", [])
    ]

    # By section
    from collections import defaultdict
    by_section: dict[str, dict] = defaultdict(lambda: {
        "projects": 0, "findings": 0, "accepted": 0, "rejected": 0,
        "accepted_matched": 0, "rejected_matched": 0,
        "has_blocks": 0, "has_graph": 0,
    })
    for r in records:
        sec = r["section"]
        by_section[sec]["projects"] += 1
        by_section[sec]["findings"] += r["findings_count"]
        by_section[sec]["accepted"] += r["human_accepted"]
        by_section[sec]["rejected"] += r["human_rejected"]
        by_section[sec]["accepted_matched"] += r.get("human_accepted_matched", r["human_accepted"])
        by_section[sec]["rejected_matched"] += r.get("human_rejected_matched", r["human_rejected"])
        if r["has_blocks"]:
            by_section[sec]["has_blocks"] += 1
        if r["has_document_graph"]:
            by_section[sec]["has_graph"] += 1

    # Manifest-level warnings
    manifest_warnings: list[str] = []
    if all_duplicate_ids:
        manifest_warnings.append(f"duplicate_human_decisions: {len(all_duplicate_ids)} item(s) appear more than once")
    if total_unmatched > 0:
        manifest_warnings.append(f"unmatched_human_decisions: {total_unmatched} decision(s) have no matching finding")
    if total_accepted_matched + total_rejected_matched > total_findings:
        manifest_warnings.append(
            f"human_decisions_exceed_findings: matched={total_accepted_matched + total_rejected_matched} > findings={total_findings}"
        )
    projects_missing_review = sum(
        1 for r in records if r.get("findings_without_human_decision", 0) > 0
    )
    if projects_missing_review > 0:
        manifest_warnings.append(f"findings_without_review: {projects_missing_review} project(s) have unreviewed findings")

    return {
        "manifest_version": "2",
        "projects_root": str(projects_root),
        "sections_filter": list(section_filter) if section_filter else None,
        "stats": {
            "total_projects": len(records),
            "total_findings": total_findings,
            "total_accepted": total_accepted,
            "total_rejected": total_rejected,
            "total_accepted_matched": total_accepted_matched,
            "total_rejected_matched": total_rejected_matched,
            "total_with_blocks": sum(1 for r in records if r["has_blocks"]),
            "total_with_graph": sum(1 for r in records if r["has_document_graph"]),
        },
        "by_section": {k: dict(v) for k, v in sorted(by_section.items())},
        "manifest_warnings": manifest_warnings,
        "records": records,
        "skipped": skipped,
    }


# ─── Markdown renderer ────────────────────────────────────────────────────────

def render_markdown(manifest: dict) -> str:
    stats = manifest["stats"]
    by_section = manifest["by_section"]
    records = manifest["records"]
    skipped = manifest["skipped"]
    manifest_warnings = manifest.get("manifest_warnings") or []

    lines = [
        "# Human Benchmark Manifest",
        "",
        "## Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total projects | **{stats['total_projects']}** |",
        f"| Total findings | {stats['total_findings']} |",
        f"| Human accepted (raw) | {stats['total_accepted']} |",
        f"| Human rejected (raw) | {stats['total_rejected']} |",
        f"| Human accepted (matched) | {stats.get('total_accepted_matched', stats['total_accepted'])} |",
        f"| Human rejected (matched) | {stats.get('total_rejected_matched', stats['total_rejected'])} |",
        f"| Projects with blocks | {stats['total_with_blocks']} |",
        f"| Projects with document graph | {stats['total_with_graph']} |",
        "",
    ]

    if manifest_warnings:
        lines += [
            "## ⚠️ Manifest Warnings",
            "",
            "_The following data quality issues were detected in expert_review.json files:_",
            "",
        ]
        for w in manifest_warnings:
            lines.append(f"- **{w}**")
        lines.append("")
        lines += [
            "> **Note:** Use `human_accepted_matched` / `human_rejected_matched` for metrics.",
            "> Raw counts may include unmatched or duplicate expert review decisions.",
            "",
        ]

    lines += [
        "## By Section",
        "",
        "| Section | Projects | Findings | H.Acc(raw) | H.Rej(raw) | H.Acc(matched) | H.Rej(matched) | Blocks | Graph |",
        "|---------|----------|----------|------------|------------|----------------|----------------|--------|-------|",
    ]
    for sec, s in sorted(by_section.items()):
        lines.append(
            f"| {sec} | {s['projects']} | {s['findings']} | {s['accepted']} "
            f"| {s['rejected']} | {s.get('accepted_matched', s['accepted'])} "
            f"| {s.get('rejected_matched', s['rejected'])} "
            f"| {s['has_blocks']} | {s['has_graph']} |"
        )
    lines.append("")

    lines += [
        "## Project Records",
        "",
        "| Project | Section | Findings | H.Acc | H.Rej | Matched | Unmatched | Dup | Warnings | Blocks | Graph |",
        "|---------|---------|----------|-------|-------|---------|-----------|-----|----------|--------|-------|",
    ]
    for r in records:
        w_count = len(r.get("warnings") or [])
        w_marker = f"⚠️ {w_count}" if w_count else "✓"
        lines.append(
            f"| {r['project_name'][:35]} | {r['section']} | {r['findings_count']} "
            f"| {r['human_accepted']} | {r['human_rejected']} "
            f"| {r.get('human_decisions_matched_to_findings', '?')} "
            f"| {r.get('human_decisions_unmatched', 0)} "
            f"| {len(r.get('human_decisions_duplicate_item_ids') or [])} "
            f"| {w_marker} "
            f"| {'✓' if r['has_blocks'] else '✗'} | {'✓' if r['has_document_graph'] else '✗'} |"
        )
    lines.append("")

    # Per-project warnings detail
    warned_records = [r for r in records if r.get("warnings")]
    if warned_records:
        lines += [
            "## Per-Project Warnings",
            "",
        ]
        for r in warned_records:
            lines.append(f"**{r['project_name']}** ({r['section']}):")
            for w in r["warnings"]:
                lines.append(f"  - {w}")
            lines.append("")

    if skipped:
        lines += [
            f"## Skipped ({len(skipped)})",
            "",
            "_Projects missing findings or expert review decisions:_",
            "",
        ]
        for s in skipped[:10]:
            lines.append(f"- `{s['project_path']}`: {s['reason']}")
        if len(skipped) > 10:
            lines.append(f"- _...and {len(skipped) - 10} more_")
        lines.append("")

    lines += [
        "---",
        "_Generated by build_human_benchmark_manifest.py. Read-only access to project artifacts._",
        "_To update: re-run this script after adding new expert_review.json files._",
    ]
    return "\n".join(lines) + "\n"


# ─── Output writer ────────────────────────────────────────────────────────────

def write_outputs(output_dir: Path, manifest: dict, export_csv: bool = False) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "human_benchmark_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    md = render_markdown(manifest)
    (output_dir / "human_benchmark_manifest.md").write_text(md, encoding="utf-8")

    if export_csv:
        fields = [
            "project_name", "project_path", "section", "findings_count",
            "human_accepted", "human_rejected", "has_blocks", "has_document_graph",
        ]
        with (output_dir / "human_benchmark_manifest.csv").open(
            "w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(manifest["records"])


# ─── Console print ────────────────────────────────────────────────────────────

def print_manifest(manifest: dict) -> None:
    stats = manifest["stats"]
    by_section = manifest["by_section"]
    manifest_warnings = manifest.get("manifest_warnings") or []
    print()
    print("=" * 72)
    print("HUMAN BENCHMARK MANIFEST")
    print("=" * 72)
    print(f"  Total projects     : {stats['total_projects']}")
    print(f"  Total findings     : {stats['total_findings']}")
    print(f"  Human accepted     : {stats['total_accepted']} (raw)  "
          f"{stats.get('total_accepted_matched', stats['total_accepted'])} (matched)")
    print(f"  Human rejected     : {stats['total_rejected']} (raw)  "
          f"{stats.get('total_rejected_matched', stats['total_rejected'])} (matched)")
    print(f"  With blocks        : {stats['total_with_blocks']}")
    print(f"  With graph         : {stats['total_with_graph']}")
    if manifest_warnings:
        print()
        print("  ⚠️  WARNINGS:")
        for w in manifest_warnings:
            print(f"      {w}")
        print("  Use matched counts for benchmark metrics.")
    print()
    print("  ── By Section ──────────────────────────────────────────────────")
    print(f"  {'Section':<8} {'Projects':>8} {'Findings':>8} {'H.Acc':>6} {'H.Rej':>6} {'Matched':>8}")
    print(f"  {'-'*8} {'-'*8} {'-'*8} {'-'*6} {'-'*6} {'-'*8}")
    for sec, s in sorted(by_section.items()):
        matched = s.get("accepted_matched", s["accepted"]) + s.get("rejected_matched", s["rejected"])
        print(f"  {sec:<8} {s['projects']:>8} {s['findings']:>8} "
              f"{s['accepted']:>6} {s['rejected']:>6} {matched:>8}")
    print()
    print("=" * 72)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Scan project directories and build a manifest of human-reviewed projects "
            "for reproducible critic_v2 benchmarking."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full scan
  python %(prog)s --projects-root projects --output-dir /tmp/manifest

  # KJ + AR only
  python %(prog)s --projects-root projects --sections KJ AR --output-dir /tmp/manifest_kj_ar

  # Specific projects
  python %(prog)s \\
    --project "projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.1-К1К2 (2).pdf" \\
    --output-dir /tmp/manifest_single
""",
    )
    parser.add_argument(
        "--projects-root", type=Path, default=_DEFAULT_PROJECTS_ROOT,
        help=f"Root of projects directory (default: {_DEFAULT_PROJECTS_ROOT})",
    )
    parser.add_argument(
        "--project", dest="explicit_projects", action="append", type=Path,
        metavar="PATH",
        help="Explicit project path (can repeat). Overrides --projects-root scan.",
    )
    parser.add_argument(
        "--sections", nargs="+", default=None,
        help="Filter by section codes (e.g. KJ AR EOM).",
    )
    parser.add_argument(
        "--output-dir", type=Path, required=True,
        help="Where to write manifest outputs.",
    )
    parser.add_argument(
        "--export-csv", action="store_true",
        help="Also write human_benchmark_manifest.csv.",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress console output.",
    )
    args = parser.parse_args()

    # Validate projects-root
    if not args.explicit_projects and not args.projects_root.exists():
        print(f"ERROR: --projects-root not found: {args.projects_root}", file=sys.stderr)
        return 1

    if not args.quiet:
        print(f"\nBuilding manifest from: {args.projects_root}")
        if args.sections:
            print(f"Section filter: {args.sections}")

    manifest = build_manifest(
        projects_root=args.projects_root,
        sections=args.sections,
        explicit_paths=args.explicit_projects,
    )

    write_outputs(args.output_dir, manifest, export_csv=args.export_csv)

    if not args.quiet:
        print_manifest(manifest)
        print(f"\n  Output files:")
        print(f"    {args.output_dir}/human_benchmark_manifest.json")
        print(f"    {args.output_dir}/human_benchmark_manifest.md")
        if args.export_csv:
            print(f"    {args.output_dir}/human_benchmark_manifest.csv")
        print()
        print("  NOTE: Production artifacts NOT modified.")
        print("        This script is read-only.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
