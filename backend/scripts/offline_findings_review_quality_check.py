#!/usr/bin/env python3
"""
offline_findings_review_quality_check.py
------------------------------------------
Автономный offline-runner для проверки качества findings без подключения LLM.

Режимы:
  --validate-fixtures-only
      Проверить структуру всех JSON-файлов в папке fixtures.

  --run-critic-v2 --input <path> --output-dir <dir>
      Прогнать 03_findings.json через critic v2 (deterministic, без LLM).
      Сохраняет:
        critic_v2_decisions.json
        critic_v2_metrics.json
        critic_v2_accepted.json
        critic_v2_rejected.json

  --input <findings.json> --fixture <fixture.json>
      Structural coverage analysis (legacy mode).

  --validate-only / --input-dir <dir>
      Validate fixture structure (legacy alias for --validate-fixtures-only).

НЕ ПОДКЛЮЧЁН к production pipeline.
НЕ вызывает LLM.
НЕ меняет файлы проекта.

Использование:
    python backend/scripts/offline_findings_review_quality_check.py \\
        --validate-fixtures-only

    python backend/scripts/offline_findings_review_quality_check.py \\
        --run-critic-v2 \\
        --input projects/<name>/_output/03_findings.json \\
        --output-dir /tmp/critic_v2_out

    python backend/scripts/offline_findings_review_quality_check.py \\
        --input backend/tests/fixtures/findings_review/bad_findings.json \\
        --fixture backend/tests/fixtures/findings_review/bad_findings.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

# ─── Constants ───────────────────────────────────────────────────────────────

REQUIRED_FINDING_FIELDS = {
    "id", "severity", "category", "sheet", "page",
    "problem", "description", "solution", "risk",
    "evidence", "related_block_ids",
}

REQUIRED_FIXTURE_FIELDS = {
    "id", "category", "input_finding", "expected_decision",
    "expected_reason", "comment",
}

VALID_EXPECTED_DECISIONS = {"accept", "reject", "borderline"}

VALID_SEVERITIES = {
    "КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
    "РЕКОМЕНДАТЕЛЬНОЕ", "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
}

# ─── Validators ──────────────────────────────────────────────────────────────


def validate_finding(finding: dict) -> list[str]:
    errors = []
    missing = REQUIRED_FINDING_FIELDS - set(finding.keys())
    if missing:
        errors.append(f"Missing fields: {sorted(missing)}")
    if "severity" in finding and finding["severity"] not in VALID_SEVERITIES:
        errors.append(f"Unknown severity: '{finding['severity']}'")
    if "evidence" in finding and not isinstance(finding["evidence"], list):
        errors.append("'evidence' must be a list")
    if "related_block_ids" in finding and not isinstance(finding["related_block_ids"], list):
        errors.append("'related_block_ids' must be a list")
    return errors


def validate_fixture_entry(entry: dict) -> list[str]:
    errors = []
    missing = REQUIRED_FIXTURE_FIELDS - set(entry.keys())
    if missing:
        errors.append(f"Missing top-level fields: {sorted(missing)}")
    if "expected_decision" in entry:
        decision = entry["expected_decision"]
        if decision not in VALID_EXPECTED_DECISIONS:
            errors.append(
                f"Unknown expected_decision: '{decision}'. "
                f"Valid: {sorted(VALID_EXPECTED_DECISIONS)}"
            )
    if "expected_reason" in entry:
        reason = entry["expected_reason"]
        if not isinstance(reason, str) or not reason.strip():
            errors.append("expected_reason is empty or not a string")
    if "input_finding" in entry:
        finding_errors = validate_finding(entry["input_finding"])
        for e in finding_errors:
            errors.append(f"input_finding: {e}")
    return errors


# ─── Loaders ─────────────────────────────────────────────────────────────────

def load_findings(path: Path) -> tuple[list[dict], list[str]]:
    errors = []
    if not path.exists():
        return [], [f"File not found: {path}"]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return [], [f"JSON parse error: {e}"]

    findings = data if isinstance(data, list) else data.get("findings", data.get("items", []))
    if not isinstance(findings, list):
        return [], ["Could not extract findings list from JSON"]

    for i, f in enumerate(findings):
        fid = f.get("id", f"index[{i}]")
        for err in validate_finding(f):
            errors.append(f"Finding '{fid}': {err}")

    return findings, errors


def _is_duplicate_set(entry: dict) -> bool:
    """Duplicate-set entries have 'findings' list instead of 'input_finding'."""
    return "findings" in entry and isinstance(entry["findings"], list)


def validate_duplicate_set(entry: dict) -> list[str]:
    errors = []
    sid = entry.get("id", "???")
    for field in ("id", "description", "findings", "expected_decisions"):
        if field not in entry:
            errors.append(f"Missing field: '{field}'")
    findings = entry.get("findings", [])
    if not isinstance(findings, list) or len(findings) < 2:
        errors.append("'findings' must be a list with ≥2 items")
    else:
        finding_ids = set()
        for f in findings:
            fid = f.get("id", "???")
            if fid in finding_ids:
                errors.append(f"Duplicate finding id in set: '{fid}'")
            finding_ids.add(fid)
            for err in validate_finding(f):
                errors.append(f"Finding '{fid}': {err}")
        decision_ids = set(entry.get("expected_decisions", {}).keys())
        missing = finding_ids - decision_ids
        if missing:
            errors.append(f"expected_decisions missing for: {sorted(missing)}")
    if "comment" not in entry:
        errors.append("Missing field: 'comment'")
    return errors


def load_fixture(path: Path) -> tuple[list[dict], list[str]]:
    errors = []
    if not path.exists():
        return [], [f"File not found: {path}"]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return [], [f"JSON parse error: {e}"]

    if not isinstance(data, list):
        return [], ["Fixture file must be a JSON array"]

    for i, entry in enumerate(data):
        eid = entry.get("id", f"index[{i}]")
        if _is_duplicate_set(entry):
            for err in validate_duplicate_set(entry):
                errors.append(f"DupSet '{eid}': {err}")
        else:
            for err in validate_fixture_entry(entry):
                errors.append(f"Fixture '{eid}': {err}")

    return data, errors


# ─── Coverage analysis ───────────────────────────────────────────────────────

def analyze_coverage(
    findings: list[dict],
    fixtures: list[dict],
) -> dict:
    finding_ids = {f["id"] for f in findings}
    fixture_finding_ids = {e["input_finding"]["id"] for e in fixtures if "input_finding" in e}

    covered = finding_ids & fixture_finding_ids
    uncovered_findings = finding_ids - fixture_finding_ids
    orphan_fixtures = fixture_finding_ids - finding_ids

    decisions_by_category: dict[str, list[str]] = {}
    for e in fixtures:
        cat = e.get("category", "unknown")
        dec = e.get("expected_decision", "unknown")
        decisions_by_category.setdefault(cat, []).append(dec)

    return {
        "total_findings": len(findings),
        "total_fixtures": len(fixtures),
        "covered_by_fixtures": len(covered),
        "uncovered_findings": sorted(uncovered_findings),
        "orphan_fixture_ids": sorted(orphan_fixtures),
        "decisions_by_category": decisions_by_category,
    }


# ─── Validate-only mode (all fixture files) ──────────────────────────────────

def validate_all_fixtures(fixtures_dir: Path) -> int:
    fixture_files = list(fixtures_dir.glob("*.json"))
    if not fixture_files:
        print(f"  No JSON files found in {fixtures_dir}", file=sys.stderr)
        return 1

    total_errors = 0
    for fpath in sorted(fixture_files):
        print(f"\n  Validating: {fpath.name}")
        _, errors = load_fixture(fpath)
        if errors:
            for e in errors:
                print(f"    ERROR: {e}")
            total_errors += len(errors)
        else:
            # Count entries
            data = json.loads(fpath.read_text(encoding="utf-8"))
            count = len(data) if isinstance(data, list) else 0
            print(f"    OK — {count} entries, no errors")

    return total_errors


# ─── Report ──────────────────────────────────────────────────────────────────

def print_report(
    findings: list[dict],
    fixtures: list[dict],
    coverage: dict,
    input_path: Optional[Path],
    fixture_path: Optional[Path],
) -> None:
    print("\n" + "=" * 60)
    print("OFFLINE FINDINGS REVIEW QUALITY CHECK")
    print("=" * 60)
    if input_path:
        print(f"  Input findings: {input_path}")
    if fixture_path:
        print(f"  Fixture file:   {fixture_path}")
    print()
    print(f"  Total findings in input:   {coverage['total_findings']}")
    print(f"  Total entries in fixture:  {coverage['total_fixtures']}")
    print(f"  Findings covered:          {coverage['covered_by_fixtures']}")
    print()

    if coverage["uncovered_findings"]:
        print(f"  Uncovered findings ({len(coverage['uncovered_findings'])}):")
        for fid in coverage["uncovered_findings"]:
            print(f"    - {fid}")
    else:
        print("  All findings covered by fixture.")

    if coverage["orphan_fixture_ids"]:
        print(f"\n  Orphan fixture entries (not in input): {len(coverage['orphan_fixture_ids'])}")
        for fid in coverage["orphan_fixture_ids"]:
            print(f"    - {fid}")

    print("\n  Expected decisions by category:")
    for cat, decisions in sorted(coverage["decisions_by_category"].items()):
        counts: dict[str, int] = {}
        for d in decisions:
            counts[d] = counts.get(d, 0) + 1
        counts_str = ", ".join(f"{v}x {k}" for k, v in sorted(counts.items()))
        print(f"    {cat}: {counts_str}")

    print()
    print("  NOTE: This is a structural validation only.")
    print("        LLM critic is NOT invoked.")
    print("        To run critic v2: use --run-critic-v2 --input <path> --output-dir <dir>")
    print("=" * 60)


# ─── Critic v2 mode ───────────────────────────────────────────────────────────

def _load_findings_for_critic(path: Path) -> tuple[list[dict], list[str]]:
    """Load findings from a 03_findings.json or a fixture file (extracts input_finding)."""
    errors = []
    if not path.exists():
        return [], [f"File not found: {path}"]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return [], [f"JSON parse error: {e}"]

    # Handle fixture format: list of {input_finding: {...}}
    if isinstance(data, list) and data and "input_finding" in data[0]:
        return [e["input_finding"] for e in data if "input_finding" in e], errors

    # Handle 03_findings.json format
    findings = data if isinstance(data, list) else data.get("findings", data.get("items", []))
    if not isinstance(findings, list):
        return [], ["Could not extract findings list from JSON"]
    return findings, errors


def _load_blocks_index(blocks_path: Optional[Path]) -> Optional[set]:
    """Load block_ids set from 02_blocks_analysis.json. Returns None if not provided."""
    if blocks_path is None or not blocks_path.exists():
        return None
    try:
        data = json.loads(blocks_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        print(f"  WARNING: Could not load blocks index: {e}", file=sys.stderr)
        return None
    # Extract all block_ids — handle both list and dict formats
    block_ids: set = set()
    analyses = (
        data.get("block_analyses", data.get("blocks", data))
        if isinstance(data, dict) else data
    )
    if isinstance(analyses, list):
        for item in analyses:
            if isinstance(item, dict):
                bid = item.get("block_id") or item.get("id")
                if bid:
                    block_ids.add(str(bid))
    elif isinstance(analyses, dict):
        block_ids.update(str(k) for k in analyses.keys())
    return block_ids or None


def _ensure_sys_path() -> None:
    """Ensure project root is on sys.path when running script directly."""
    _script_dir = Path(__file__).resolve().parent
    _project_root = _script_dir.parent.parent
    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))


def _decision_to_dict(d) -> dict:
    return {
        "finding_id": d.finding_id,
        "decision": d.decision,
        "usefulness_score": d.usefulness_score,
        "reject_reason": d.reject_reason,
        "reject_explanation": d.reject_explanation,
        "merged_into": d.merged_into,
        "impact_area": d.impact_area,
        "severity": d.severity,
        "has_evidence": d.has_evidence,
        "has_action": d.has_action,
        "has_impact": d.has_impact,
        "evidence_quality": d.evidence_quality,
    }


def _write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def run_critic_v2_mode(
    input_path: Path,
    output_dir: Path,
    blocks_path: Optional[Path] = None,
    llm_gate: bool = False,
    llm_provider: str = "mock",
    llm_model: Optional[str] = None,
    prompt_path: Optional[Path] = None,
    max_candidates: int = 50,
) -> int:
    """Run critic v2 on input findings and save results to output_dir."""
    _ensure_sys_path()

    try:
        from backend.app.pipeline.stages.findings_review.critic_v2 import (
            run_critic_v2_offline,
        )
        from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import (
            merge_llm_decisions,
            run_llm_gate,
        )
    except ImportError as e:
        print(f"ERROR: Could not import critic v2 engine: {e}", file=sys.stderr)
        print("  Ensure you run from the project root or install the package.", file=sys.stderr)
        return 1

    print(f"\nRunning critic v2 on: {input_path}")
    findings, errors = _load_findings_for_critic(input_path)
    if errors:
        for e in errors:
            print(f"  ERROR: {e}", file=sys.stderr)
        return 1
    if not findings:
        print("  No findings found.", file=sys.stderr)
        return 1

    # Optional: blocks index
    blocks_index = _load_blocks_index(blocks_path)
    if blocks_index is not None:
        print(f"  Blocks index: {len(blocks_index)} blocks from {blocks_path}")
    else:
        print("  Blocks index: not provided — evidence refs treated as unverified (weak/partial)")

    print(f"  Loaded {len(findings)} findings")

    # ── Step 1: Deterministic engine ──
    result = run_critic_v2_offline(findings, blocks_index=blocks_index)
    findings_by_id = {f["id"]: f for f in findings if "id" in f}
    output_dir.mkdir(parents=True, exist_ok=True)

    decisions_data = [_decision_to_dict(d) for d in result.decisions]
    _write_json(output_dir / "critic_v2_decisions.json", decisions_data)

    m = result.metrics
    metrics_data = {
        "total_input": m.total_input,
        "accepted": m.accepted,
        "borderline": m.borderline,
        "low_priority": m.low_priority,
        "merged": m.merged,
        "rejected_by_rules": m.rejected_by_rules,
        "rejected_by_score": m.rejected_by_score,
        "rejection_reasons": m.rejection_reasons,
        "average_usefulness_score": m.average_usefulness_score,
    }
    _write_json(output_dir / "critic_v2_metrics.json", metrics_data)

    # ── Step 2: LLM gate (optional) ──
    if llm_gate:
        print(f"\n  ── LLM Gate ({llm_provider}) ──")
        gate = run_llm_gate(
            result.decisions,
            findings_by_id,
            provider=llm_provider,
            prompt_path=prompt_path,
            max_candidates=max_candidates,
        )

        # Save LLM candidates list
        candidates_data = [
            {"finding_id": d.finding_id, "decision": d.decision,
             "evidence_quality": d.evidence_quality, "usefulness_score": d.usefulness_score}
            for d in result.decisions
            if d.finding_id not in gate.skipped_ids
        ]
        _write_json(output_dir / "critic_v2_llm_candidates.json", candidates_data)

        # Save LLM raw decisions
        llm_decisions_data = [
            {
                "finding_id": d.finding_id,
                "llm_decision": d.llm_decision,
                "usefulness_score": d.usefulness_score,
                "reject_reason": d.reject_reason,
                "explanation": d.explanation,
                "rewritten_title": d.rewritten_title,
                "rewritten_description": d.rewritten_description,
                "rewritten_action_required": d.rewritten_action_required,
                "provider": d.provider,
            }
            for d in gate.decisions
        ]
        _write_json(output_dir / "critic_v2_llm_decisions.json", llm_decisions_data)

        # Merge and produce final decisions
        final_decisions, final_accepted, final_rejected, final_borderline = merge_llm_decisions(
            result.decisions, gate.decisions, findings_by_id,
        )

        final_data = [_decision_to_dict(d) for d in final_decisions]
        _write_json(output_dir / "critic_v2_final_decisions.json", final_data)
        _write_json(output_dir / "critic_v2_accepted.json", final_accepted)
        _write_json(output_dir / "critic_v2_rejected.json", final_rejected)
        _write_json(output_dir / "critic_v2_borderline.json", final_borderline)

        print(f"  Prompt: {gate.prompt_path_used}")
        print(f"  Candidates sent: {gate.candidates_sent}")
        print(f"  Skipped (rule-rejected/merged/no-evidence): {len(gate.skipped_ids)}")
        if gate.errors:
            for e in gate.errors:
                print(f"  LLM ERROR: {e}")
        print(f"\n  ── Final Decisions (after LLM gate) ──")
        acc = sum(1 for d in final_decisions if d.decision == "accept")
        brd = sum(1 for d in final_decisions if d.decision in ("borderline", "low_priority"))
        rej = sum(1 for d in final_decisions if d.decision == "reject")
        mrg = sum(1 for d in final_decisions if d.decision == "merge")
        print(f"  Accepted:   {acc}")
        print(f"  Borderline: {brd}")
        print(f"  Rejected:   {rej}")
        print(f"  Merged:     {mrg}")
        print(f"\n  Files written:")
        print(f"    critic_v2_decisions.json       ({len(decisions_data)} deterministic)")
        print(f"    critic_v2_metrics.json")
        print(f"    critic_v2_llm_candidates.json  ({len(candidates_data)} sent to LLM)")
        print(f"    critic_v2_llm_decisions.json   ({len(llm_decisions_data)} LLM responses)")
        print(f"    critic_v2_final_decisions.json ({len(final_data)} final)")
        print(f"    critic_v2_accepted.json        ({len(final_accepted)} findings)")
        print(f"    critic_v2_rejected.json        ({len(final_rejected)} findings)")
        print(f"    critic_v2_borderline.json      ({len(final_borderline)} findings)")
    else:
        # No LLM gate — save deterministic results only
        _write_json(output_dir / "critic_v2_accepted.json", result.accepted_findings)
        _write_json(output_dir / "critic_v2_rejected.json", result.rejected_findings)

        print(f"\n  Results saved to: {output_dir}")
        print(f"\n  ── Critic v2 Metrics ──")
        print(f"  Total input:        {m.total_input}")
        print(f"  Accepted:           {m.accepted}")
        print(f"  Borderline:         {m.borderline}")
        print(f"  Merged (duplicates):{m.merged}")
        print(f"  Rejected by rules:  {m.rejected_by_rules}")
        print(f"  Rejected by score:  {m.rejected_by_score}")
        print(f"  Avg usefulness:     {m.average_usefulness_score}")
        if m.rejection_reasons:
            print(f"\n  Rejection reasons:")
            for reason, count in sorted(m.rejection_reasons.items(), key=lambda x: -x[1]):
                print(f"    {reason}: {count}")
        print(f"\n  Files written:")
        print(f"    critic_v2_decisions.json  ({len(decisions_data)} entries)")
        print(f"    critic_v2_metrics.json")
        print(f"    critic_v2_accepted.json   ({len(result.accepted_findings)} findings)")
        print(f"    critic_v2_rejected.json   ({len(result.rejected_findings)} findings)")

    print()
    print("  NOTE: critic v2 is deterministic only." if not llm_gate else
          f"  NOTE: critic v2 + LLM gate ({llm_provider}) — no production pipeline changes.")
    if blocks_index is None:
        print("        Phantom block detection: DISABLED (no --blocks-index provided)")
    else:
        print("        Phantom block detection: ENABLED")
    print("        For LLM gate prompt: Experiments_Kuldyaev/new_critic/findings_critic_task.ru.v1.md")
    return 0


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Offline findings review quality check (no LLM).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Validate all fixtures:
    python %(prog)s --validate-fixtures-only

  Run critic v2 on a real project:
    python %(prog)s --run-critic-v2 --input projects/<name>/_output/03_findings.json \\
        --output-dir /tmp/critic_v2_out

  Run critic v2 on a fixture file:
    python %(prog)s --run-critic-v2 \\
        --input backend/tests/fixtures/findings_review/bad_findings.json \\
        --output-dir /tmp/critic_v2_bad_check

  Coverage analysis (legacy):
    python %(prog)s --input 03_findings.json --fixture bad_findings.json
""",
    )
    parser.add_argument("--input", type=Path, help="Path to 03_findings.json or fixture file.")
    parser.add_argument("--fixture", type=Path, help="Path to a specific fixture JSON file.")
    parser.add_argument(
        "--output-dir", type=Path, default=Path("/tmp/critic_v2_output"),
        help="Output directory for critic v2 results (default: /tmp/critic_v2_output).",
    )
    parser.add_argument(
        "--input-dir", type=Path,
        help="Validate all JSON files in a fixtures directory.",
    )
    parser.add_argument(
        "--validate-only", action="store_true",
        help="Only validate fixture structure (legacy alias for --validate-fixtures-only).",
    )
    parser.add_argument(
        "--validate-fixtures-only", action="store_true",
        help="Validate all fixture JSON files in the default fixtures directory.",
    )
    parser.add_argument(
        "--run-critic-v2", action="store_true",
        help="Run critic v2 deterministic engine on --input findings.",
    )
    parser.add_argument(
        "--blocks-index", type=Path, dest="blocks_index",
        help=(
            "Path to 02_blocks_analysis.json for phantom block detection. "
            "When provided, enables verification of evidence block_ids. "
            "Without it, refs are treated as weak/partial evidence."
        ),
    )
    parser.add_argument(
        "--llm-gate", action="store_true",
        help="Run LLM gate after deterministic critic v2 (requires --run-critic-v2).",
    )
    parser.add_argument(
        "--llm-provider", default="mock",
        choices=["mock", "noop", "claude_runner", "openrouter"],
        help="LLM provider for gate. 'mock' works offline without API. (default: mock)",
    )
    parser.add_argument(
        "--llm-model", default=None,
        help="Optional model override for LLM provider.",
    )
    parser.add_argument(
        "--prompt-path", type=Path, default=None,
        help=(
            "Path to custom critic prompt .md file. "
            "Default: Experiments_Kuldyaev/new_critic/findings_critic_task.ru.v1.md"
        ),
    )
    parser.add_argument(
        "--max-candidates", type=int, default=50,
        help="Max findings to send to LLM gate (default: 50).",
    )
    args = parser.parse_args()

    # ── Validate fixtures mode ──
    if args.validate_fixtures_only or args.validate_only or args.input_dir:
        target_dir = args.input_dir or Path("backend/tests/fixtures/findings_review")
        if not target_dir.exists():
            print(f"ERROR: Directory not found: {target_dir}", file=sys.stderr)
            return 1
        print(f"\nValidating all fixtures in: {target_dir}")
        errors = validate_all_fixtures(target_dir)
        if errors:
            print(f"\nFAILED: {errors} validation error(s).", file=sys.stderr)
            return 1
        print("\nAll fixtures valid.")
        return 0

    # ── Critic v2 mode ──
    if args.run_critic_v2:
        if not args.input:
            print("ERROR: --run-critic-v2 requires --input <path>", file=sys.stderr)
            return 1
        return run_critic_v2_mode(
            args.input,
            args.output_dir,
            blocks_path=getattr(args, "blocks_index", None),
            llm_gate=getattr(args, "llm_gate", False),
            llm_provider=getattr(args, "llm_provider", "mock"),
            llm_model=getattr(args, "llm_model", None),
            prompt_path=getattr(args, "prompt_path", None),
            max_candidates=getattr(args, "max_candidates", 50),
        )

    # ── Coverage analysis mode (legacy) ──
    if not args.input or not args.fixture:
        parser.print_help()
        print("\nERROR: Provide --run-critic-v2, --validate-fixtures-only, "
              "or --input + --fixture.", file=sys.stderr)
        return 1

    findings, finding_errors = load_findings(args.input)
    fixtures, fixture_errors = load_fixture(args.fixture)
    all_errors = finding_errors + fixture_errors
    if all_errors:
        print(f"\nValidation errors ({len(all_errors)}):")
        for e in all_errors:
            print(f"  ERROR: {e}")
        return 1

    coverage = analyze_coverage(findings, fixtures)
    print_report(findings, fixtures, coverage, args.input, args.fixture)
    return 0


if __name__ == "__main__":
    sys.exit(main())
