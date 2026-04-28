"""
Stage 02 Flash -> Pro selective triage runner.

Purpose:
  Keep the quality advantage of Gemini Pro single-block only where it matters,
  while using cheap Gemini Flash single-block as the full-project first pass.

Policy:
  1. Flash analyzes every block as an independent single-block request.
  2. Pro is used only as an independent single-block second pass for blocks
     that are both important enough and risky enough:
       - complex/risky block with Flash findings
       - complex/risky block where Flash failed or marked unreadable
       - finding block with weak/uncertain Flash extraction
       - optionally all finding blocks via --include-simple-findings
  3. Never pack multiple blocks into the Pro prompt.
  4. Do not change production defaults, do not recrop/rebuild blocks.

Usage:
  python scripts/run_stage02_flash_pro_triage.py \\
      --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" \\
      --dry-run

  python scripts/run_stage02_flash_pro_triage.py \\
      --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" \\
      --parallelism-flash 3 \\
      --parallelism-pro 2 \\
      --max-pro-cost-usd 8
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from run_gemini_openrouter_stage02_experiment import (  # noqa: E402
    BatchResultEnvelope,
    RunMetrics,
    build_page_contexts,
    build_system_prompt,
    classify_risk,
    compute_metrics,
    run_batches_async,
)
from run_stage02_recall_hybrid import (  # noqa: E402
    LOW_KV_THRESHOLD,
    SHORT_SUMMARY_THRESHOLD,
    resolve_project,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("flash_pro_triage")

MODEL_FLASH = "google/gemini-2.5-flash"
MODEL_PRO = "google/gemini-3.1-pro-preview"

# Conservative planning defaults based on observed project experiments.
FLASH_SINGLE_BLOCK_EST_USD = 0.0015
PRO_SINGLE_BLOCK_EST_USD = 0.10


@dataclass
class TriageEntry:
    block_id: str
    decision: str
    priority: int
    reasons: list[str]
    risk: str
    page: int
    size_kb: float
    findings_count: int
    kv_count: int
    summary_len: int
    unreadable: bool
    analysis_present: bool


@dataclass
class CostEstimate:
    total_blocks: int
    pro_blocks: int
    flash_cost_per_block: float
    pro_cost_per_block: float
    flash_cost_usd: float
    pro_cost_usd: float
    total_cost_usd: float
    pro_budget_ok: bool
    max_pro_cost_usd: float | None = None
    stop_reason: str | None = None


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _collect_analyses(
    results: list[BatchResultEnvelope],
) -> tuple[dict[str, dict], set[str], set[str]]:
    analyses: dict[str, dict] = {}
    inferred_ids: set[str] = set()
    failed_ids: set[str] = set()

    for res in results:
        if res.is_error or not res.parsed_data:
            failed_ids.update(res.input_block_ids)
            continue
        if res.inferred_block_id_count:
            inferred_ids.update(res.input_block_ids)
        for item in res.parsed_data.get("block_analyses", []):
            if not isinstance(item, dict):
                continue
            bid = str(item.get("block_id") or "")
            if bid:
                analyses[bid] = item

    return analyses, inferred_ids, failed_ids


def _is_complex_block(block: dict) -> bool:
    risk = classify_risk(block)
    size_kb = float(block.get("size_kb", 0) or 0)
    ocr_len = int(block.get("ocr_text_len", 0) or 0)
    return bool(
        risk in ("heavy", "normal")
        or block.get("is_full_page")
        or block.get("merged_block_ids")
        or block.get("quadrant")
        or size_kb >= 500
        or ocr_len >= 800
    )


def _has_high_value_finding(analysis: dict | None) -> bool:
    if not analysis:
        return False
    high_words = ("КРИТИЧ", "ЭКСПЛУАТ", "ЭКОНОМ", "ПРОВЕРИТЬ")
    for finding in analysis.get("findings") or []:
        severity = str((finding or {}).get("severity") or "").upper()
        if any(word in severity for word in high_words):
            return True
    return False


def _weak_or_uncertain_flash(
    analysis: dict | None,
    *,
    inferred: bool,
) -> bool:
    if analysis is None:
        return True
    summary = str(analysis.get("summary") or "").strip()
    kv_count = len(analysis.get("key_values_read") or [])
    return bool(
        inferred
        or analysis.get("unreadable_text")
        or len(summary) < SHORT_SUMMARY_THRESHOLD
        or kv_count <= LOW_KV_THRESHOLD
    )


def select_pro_escalation_blocks(
    all_blocks: list[dict],
    flash_results: list[BatchResultEnvelope],
    *,
    include_simple_findings: bool = False,
    include_unreadable_rescue: bool = True,
    max_pro_blocks: int | None = None,
) -> tuple[list[TriageEntry], dict]:
    """Select blocks for Pro single-block second pass.

    The default is intentionally cost-aware: simple/light Flash-positive blocks
    stay Flash-only unless they look weak/uncertain or have high-value severity.
    """
    analyses, inferred_ids, failed_ids = _collect_analyses(flash_results)
    entries: list[TriageEntry] = []
    flash_positive = 0
    complex_blocks = 0
    simple_findings_flash_only = 0

    for block in all_blocks:
        bid = str(block["block_id"])
        analysis = analyses.get(bid)
        risk = classify_risk(block)
        complex_block = _is_complex_block(block)
        if complex_block:
            complex_blocks += 1

        findings_count = len((analysis or {}).get("findings") or [])
        if findings_count:
            flash_positive += 1

        kv_count = len((analysis or {}).get("key_values_read") or [])
        summary_len = len(str((analysis or {}).get("summary") or "").strip())
        unreadable = bool((analysis or {}).get("unreadable_text"))
        inferred = bid in inferred_ids
        failed = bid in failed_ids or analysis is None
        weak = _weak_or_uncertain_flash(analysis, inferred=inferred)
        high_value_finding = _has_high_value_finding(analysis)

        reasons: list[str] = []
        priority = 0

        if failed and complex_block:
            priority = max(priority, 100)
            reasons.append("complex_flash_missing_or_failed")
        if include_unreadable_rescue and unreadable and complex_block:
            priority = max(priority, 95)
            reasons.append("complex_unreadable_flash_output")
        if findings_count > 0 and complex_block:
            priority = max(priority, 90)
            reasons.append("complex_block_with_flash_findings")
        if findings_count > 0 and high_value_finding:
            priority = max(priority, 85)
            reasons.append("high_value_flash_finding")
        if findings_count > 0 and weak:
            priority = max(priority, 80)
            reasons.append("finding_with_weak_or_uncertain_flash_read")
        if include_simple_findings and findings_count > 0:
            priority = max(priority, 60)
            reasons.append("all_flash_findings_enabled")

        if not reasons:
            if findings_count > 0 and not complex_block:
                simple_findings_flash_only += 1
            continue

        entries.append(
            TriageEntry(
                block_id=bid,
                decision="pro_single_block",
                priority=priority,
                reasons=sorted(set(reasons)),
                risk=risk,
                page=int(block.get("page", 0) or 0),
                size_kb=float(block.get("size_kb", 0) or 0),
                findings_count=findings_count,
                kv_count=kv_count,
                summary_len=summary_len,
                unreadable=unreadable,
                analysis_present=analysis is not None,
            )
        )

    entries.sort(key=lambda e: (-e.priority, -e.findings_count, -e.size_kb, e.page, e.block_id))
    total_selected_before_cap = len(entries)
    if max_pro_blocks is not None:
        entries = entries[:max(0, max_pro_blocks)]

    summary = {
        "total_blocks": len(all_blocks),
        "flash_positive_blocks": flash_positive,
        "complex_blocks": complex_blocks,
        "selected_for_pro": len(entries),
        "selected_for_pro_before_cap": total_selected_before_cap,
        "flash_only_blocks": len(all_blocks) - len(entries),
        "simple_findings_flash_only": simple_findings_flash_only,
        "max_pro_blocks": max_pro_blocks,
        "include_simple_findings": include_simple_findings,
        "include_unreadable_rescue": include_unreadable_rescue,
        "selection_rate_pct": round(len(entries) / max(1, len(all_blocks)) * 100, 1),
    }
    return entries, summary


def estimate_triage_cost(
    total_blocks: int,
    pro_blocks: int,
    *,
    flash_cost_per_block: float = FLASH_SINGLE_BLOCK_EST_USD,
    pro_cost_per_block: float = PRO_SINGLE_BLOCK_EST_USD,
    max_pro_cost_usd: float | None = None,
) -> CostEstimate:
    flash_cost = total_blocks * flash_cost_per_block
    pro_cost = pro_blocks * pro_cost_per_block
    total_cost = flash_cost + pro_cost
    pro_budget_ok = True
    stop_reason = None
    if max_pro_cost_usd is not None and pro_cost > max_pro_cost_usd:
        pro_budget_ok = False
        stop_reason = (
            f"Estimated Pro second-pass cost ${pro_cost:.2f} exceeds "
            f"cap ${max_pro_cost_usd:.2f}"
        )
    return CostEstimate(
        total_blocks=total_blocks,
        pro_blocks=pro_blocks,
        flash_cost_per_block=flash_cost_per_block,
        pro_cost_per_block=pro_cost_per_block,
        flash_cost_usd=round(flash_cost, 6),
        pro_cost_usd=round(pro_cost, 6),
        total_cost_usd=round(total_cost, 6),
        pro_budget_ok=pro_budget_ok,
        max_pro_cost_usd=max_pro_cost_usd,
        stop_reason=stop_reason,
    )


def merge_flash_and_pro_results(
    all_blocks: list[dict],
    flash_results: list[BatchResultEnvelope],
    pro_results: list[BatchResultEnvelope],
    escalation_ids: list[str],
) -> tuple[list[dict], dict]:
    """Merge final block analyses, preferring successful Pro for escalated IDs."""
    flash_map, _, _ = _collect_analyses(flash_results)
    pro_map, _, pro_failed_ids = _collect_analyses(pro_results)
    escalation_set = set(escalation_ids)

    final_rows: list[dict] = []
    used_pro = 0
    flash_fallback = 0
    missing_final = 0

    for block in all_blocks:
        bid = str(block["block_id"])
        source = "flash_single_block"
        analysis = flash_map.get(bid)

        if bid in escalation_set:
            pro_analysis = pro_map.get(bid)
            if pro_analysis and bid not in pro_failed_ids:
                analysis = pro_analysis
                source = "pro_single_block"
                used_pro += 1
            else:
                source = "flash_fallback_pro_failed"
                flash_fallback += 1

        if analysis is None:
            missing_final += 1
            analysis = {
                "block_id": bid,
                "page": block.get("page", 0),
                "label": block.get("ocr_label", ""),
                "summary": "",
                "key_values_read": [],
                "findings": [],
                "unreadable_text": True,
                "unreadable_details": "No Flash or Pro analysis available",
            }

        row = dict(analysis)
        row["_triage_source"] = source
        final_rows.append(row)

    summary = {
        "total_blocks": len(all_blocks),
        "escalation_blocks": len(escalation_ids),
        "used_pro": used_pro,
        "flash_fallback_after_pro_failure": flash_fallback,
        "missing_final": missing_final,
        "coverage_pct": round((len(all_blocks) - missing_final) / max(1, len(all_blocks)) * 100, 2),
    }
    return final_rows, summary


def write_stage02_output(
    project_dir: Path,
    final_rows: list[dict],
    merge_summary: dict,
    *,
    source_artifacts_dir: Path,
) -> Path:
    """Persist merged triage output as the normal stage-02 artifact."""
    output_dir = project_dir / "_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "02_blocks_analysis.json"
    if out_path.exists():
        backup_path = output_dir / f"02_blocks_analysis.before_flash_pro_triage.{_ts()}.json"
        backup_path.write_text(out_path.read_text(encoding="utf-8"), encoding="utf-8")

    payload = {
        "stage": "02_blocks_analysis",
        "meta": {
            "source": "flash_pro_triage",
            "blocks_reviewed": len(final_rows),
            "total_blocks_expected": merge_summary.get("total_blocks", len(final_rows)),
            "coverage_pct": merge_summary.get("coverage_pct", 0.0),
            "pro_escalation_blocks": merge_summary.get("escalation_blocks", 0),
            "pro_used_blocks": merge_summary.get("used_pro", 0),
            "flash_fallback_after_pro_failure": merge_summary.get(
                "flash_fallback_after_pro_failure", 0
            ),
            "artifacts_dir": str(source_artifacts_dir),
        },
        "block_analyses": final_rows,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def build_triage_policy_md() -> str:
    return """# Stage 02 Flash -> Pro Triage Policy

## Practical algorithm

1. Run `google/gemini-2.5-flash` on every block in single-block mode.
2. Build a Pro escalation set from Flash output and block metadata.
3. Run `google/gemini-3.1-pro-preview` only on selected blocks, still single-block.
4. Merge final stage 02 output by replacing escalated Flash analyses with successful Pro analyses.

## Default Pro escalation rules

- Complex/risky block with one or more Flash findings.
- Complex/risky block where Flash failed, returned no usable analysis, or marked the block unreadable.
- Any Flash finding with weak/uncertain extraction: inferred block id, weak summary, very low KV, unreadable.
- Any high-value Flash finding severity: critical, operational, economic, or cross-section check.

## Cost guardrail

Simple/light Flash-positive blocks stay Flash-only by default. Use
`--include-simple-findings` only when recall matters more than cost.

## Non-goals

- No Pro multi-block batching.
- No Claude comparison.
- No Flash production default changes.
- No recrop or block rebuild.
- No stage 03+ changes.
"""


def build_triage_summary_md(
    flash_m: RunMetrics,
    pro_m: RunMetrics | None,
    selection_summary: dict,
    cost_estimate: CostEstimate,
    merge_summary: dict | None,
) -> str:
    lines = [
        "# Flash -> Pro Triage Summary\n",
        "## First pass: Flash single-block\n",
        f"- Blocks: {flash_m.total_input_blocks}",
        f"- Coverage: {flash_m.coverage_pct:.1f}%",
        f"- Findings: {flash_m.total_findings} on {flash_m.blocks_with_findings} blocks",
        f"- KV total / median: {flash_m.total_key_values} / {flash_m.median_key_values:.1f}",
        f"- Cost: ${flash_m.total_cost_usd:.4f}",
        "",
        "## Pro escalation set\n",
        f"- Selected for Pro single-block: {selection_summary['selected_for_pro']} "
        f"of {selection_summary['total_blocks']} "
        f"({selection_summary['selection_rate_pct']}%)",
        f"- Flash-positive blocks: {selection_summary['flash_positive_blocks']}",
        f"- Complex blocks: {selection_summary['complex_blocks']}",
        f"- Simple findings left Flash-only: {selection_summary['simple_findings_flash_only']}",
        "",
        "## Preflight cost estimate\n",
        f"- Flash estimate: ${cost_estimate.flash_cost_usd:.4f}",
        f"- Pro estimate: ${cost_estimate.pro_cost_usd:.4f}",
        f"- Total estimate: ${cost_estimate.total_cost_usd:.4f}",
        f"- Pro budget gate: {'PASS' if cost_estimate.pro_budget_ok else 'STOP'}",
    ]
    if cost_estimate.stop_reason:
        lines.append(f"- Stop reason: {cost_estimate.stop_reason}")
    if pro_m is not None:
        lines += [
            "",
            "## Second pass: Pro single-block",
            f"- Blocks: {pro_m.total_input_blocks}",
            f"- Coverage: {pro_m.coverage_pct:.1f}%",
            f"- Missing / duplicate / extra: {pro_m.missing_count} / {pro_m.duplicate_count} / {pro_m.extra_count}",
            f"- Findings: {pro_m.total_findings} on {pro_m.blocks_with_findings} blocks",
            f"- Cost: ${pro_m.total_cost_usd:.4f}",
        ]
    if merge_summary is not None:
        lines += [
            "",
            "## Final merge",
            f"- Used Pro analyses: {merge_summary['used_pro']}",
            f"- Flash fallback after Pro failure: {merge_summary['flash_fallback_after_pro_failure']}",
            f"- Final coverage: {merge_summary['coverage_pct']:.1f}%",
        ]
    return "\n".join(lines) + "\n"


def build_winner_recommendation_md(
    selection_summary: dict,
    cost_estimate: CostEstimate,
    pro_m: RunMetrics | None,
    merge_summary: dict | None,
    *,
    dry_run: bool = False,
) -> str:
    if dry_run:
        verdict = "Dry-run validation only: no quality verdict and no real model calls were made."
    elif not cost_estimate.pro_budget_ok:
        verdict = "STOP before Pro second pass: budget cap would be exceeded."
    elif selection_summary["selected_for_pro"] == 0:
        verdict = "Flash-only is enough for this run: no Pro escalation blocks selected."
    elif pro_m is None:
        verdict = "Ready to run Pro second pass, but it has not been executed."
    elif merge_summary and merge_summary["flash_fallback_after_pro_failure"] == 0:
        verdict = "Recommended practical mode: Flash full pass + Pro single-block on selected risky findings."
    else:
        verdict = "Usable with caution: some escalated blocks fell back to Flash after Pro failure."

    return (
        "# Winner Recommendation — Flash -> Pro Triage\n\n"
        f"**Verdict:** {verdict}\n\n"
        "## Recommended operating mode\n\n"
        "- Flash analyzes all blocks in single-block mode.\n"
        "- Pro never receives multi-block prompts.\n"
        "- Pro is reserved for complex/risky Flash-positive or failed/unreadable blocks.\n"
        "- Simple/light Flash findings stay Flash-only by default to control spend.\n\n"
        "## Cost posture\n\n"
        f"- Estimated Flash cost: ${cost_estimate.flash_cost_usd:.4f}\n"
        f"- Estimated Pro cost: ${cost_estimate.pro_cost_usd:.4f}\n"
        f"- Estimated total: ${cost_estimate.total_cost_usd:.4f}\n"
        f"- Dry-run: {dry_run}\n\n"
        "## Next step\n\n"
        "Run this policy on the big KJ project once the Google Batch path is available, "
        "or keep using OpenRouter for the same single-block mechanics with standard pricing.\n"
    )


async def main_async(args: argparse.Namespace) -> None:
    from webapp.config import OPENROUTER_API_KEY

    if not OPENROUTER_API_KEY and not args.dry_run:
        logger.error("OPENROUTER_API_KEY not set; use --dry-run for local validation")
        sys.exit(1)

    resolution = resolve_project(args.pdf, Path(args.project_dir) if args.project_dir else None)
    all_blocks = resolution.blocks
    project_info_path = resolution.project_dir / "project_info.json"
    project_info = (
        json.loads(project_info_path.read_text(encoding="utf-8"))
        if project_info_path.exists()
        else {}
    )

    exp_dir = resolution.project_dir / "_experiments" / "stage02_flash_pro_triage" / _ts()
    exp_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "pdf": args.pdf,
        "project_dir": str(resolution.project_dir),
        "block_source": resolution.block_source,
        "blocks_index": str(resolution.blocks_index_path),
        "total_blocks": resolution.total_blocks,
        "models": {"flash": MODEL_FLASH, "pro": MODEL_PRO},
        "mode": "flash_full_single_block_then_pro_selected_single_block",
        "parallelism_flash": args.parallelism_flash,
        "parallelism_pro": args.parallelism_pro,
        "include_simple_findings": args.include_simple_findings,
        "max_pro_blocks": args.max_pro_blocks,
        "max_pro_cost_usd": args.max_pro_cost_usd,
        "dry_run": args.dry_run,
        "constraints": [
            "no_pro_multiblock_batching",
            "no_claude_comparison",
            "no_flash_production_default_change",
            "no_recrop_or_rebuild_blocks",
        ],
    }
    _save_json(exp_dir / "manifest.json", manifest)
    (exp_dir / "triage_policy.md").write_text(build_triage_policy_md(), encoding="utf-8")

    system_prompt = build_system_prompt(project_info, len(all_blocks))
    page_contexts = build_page_contexts(resolution.project_dir)

    logger.info("[F1] Flash full single-block pass: %d blocks", len(all_blocks))
    flash_batches = [[b] for b in all_blocks]
    flash_results, flash_elapsed = await run_batches_async(
        flash_batches,
        resolution.project_dir,
        project_info,
        system_prompt,
        page_contexts,
        all_blocks,
        MODEL_FLASH,
        args.parallelism_flash,
        "F1_flash_full_single",
        strict_schema=True,
        response_healing=True,
        require_parameters=True,
        provider_data_collection=None,
        dry_run=args.dry_run,
    )
    flash_m = compute_metrics(
        flash_results,
        all_blocks,
        run_id="F1_flash_full_single",
        model_id=MODEL_FLASH,
        batch_profile="single",
        parallelism=args.parallelism_flash,
        mode="single_block",
        elapsed_s=flash_elapsed,
        strict_schema_enabled=True,
        response_healing_enabled=True,
        require_parameters_enabled=True,
        dry_run=args.dry_run,
    )
    _save_json(exp_dir / "flash_full_summary.json", asdict(flash_m))

    entries, selection_summary = select_pro_escalation_blocks(
        all_blocks,
        flash_results,
        include_simple_findings=args.include_simple_findings,
        include_unreadable_rescue=not args.no_unreadable_rescue,
        max_pro_blocks=args.max_pro_blocks,
    )
    escalation_ids = [e.block_id for e in entries]
    _save_json(exp_dir / "pro_escalation_block_ids.json", escalation_ids)
    _save_json(exp_dir / "pro_escalation_manifest.json", [asdict(e) for e in entries])
    _save_csv(exp_dir / "pro_escalation_manifest.csv", [asdict(e) for e in entries])

    cost_estimate = estimate_triage_cost(
        total_blocks=len(all_blocks),
        pro_blocks=len(escalation_ids),
        flash_cost_per_block=args.flash_cost_per_block,
        pro_cost_per_block=args.pro_cost_per_block,
        max_pro_cost_usd=args.max_pro_cost_usd,
    )
    _save_json(exp_dir / "preflight_cost_estimate.json", asdict(cost_estimate))

    pro_m: RunMetrics | None = None
    merge_summary: dict | None = None
    pro_results: list[BatchResultEnvelope] = []

    if escalation_ids and cost_estimate.pro_budget_ok:
        block_by_id = {b["block_id"]: b for b in all_blocks}
        pro_blocks = [block_by_id[bid] for bid in escalation_ids if bid in block_by_id]
        logger.info("[P2] Pro selected single-block pass: %d blocks", len(pro_blocks))
        pro_results, pro_elapsed = await run_batches_async(
            [[b] for b in pro_blocks],
            resolution.project_dir,
            project_info,
            system_prompt,
            page_contexts,
            pro_blocks,
            MODEL_PRO,
            args.parallelism_pro,
            "P2_pro_selected_single",
            strict_schema=True,
            response_healing=True,
            require_parameters=True,
            provider_data_collection=None,
            dry_run=args.dry_run,
        )
        pro_m = compute_metrics(
            pro_results,
            pro_blocks,
            run_id="P2_pro_selected_single",
            model_id=MODEL_PRO,
            batch_profile="single",
            parallelism=args.parallelism_pro,
            mode="single_block",
            elapsed_s=pro_elapsed,
            strict_schema_enabled=True,
            response_healing_enabled=True,
            require_parameters_enabled=True,
            dry_run=args.dry_run,
        )
        _save_json(exp_dir / "pro_selected_summary.json", asdict(pro_m))

    final_rows, merge_summary = merge_flash_and_pro_results(
        all_blocks,
        flash_results,
        pro_results,
        escalation_ids if pro_results else [],
    )
    _save_json(exp_dir / "final_merged_block_analyses.json", final_rows)
    _save_json(exp_dir / "final_merge_summary.json", merge_summary)

    stage_output_path = None
    if args.write_stage_output:
        stage_output_path = write_stage02_output(
            resolution.project_dir,
            final_rows,
            merge_summary,
            source_artifacts_dir=exp_dir,
        )
        logger.info("Wrote stage-02 output: %s", stage_output_path)

    (exp_dir / "triage_summary.md").write_text(
        build_triage_summary_md(flash_m, pro_m, selection_summary, cost_estimate, merge_summary),
        encoding="utf-8",
    )
    _save_json(exp_dir / "triage_summary.json", {
        "flash": asdict(flash_m),
        "pro": asdict(pro_m) if pro_m else None,
        "selection": selection_summary,
        "cost_estimate": asdict(cost_estimate),
        "merge": merge_summary,
        "stage_output": str(stage_output_path) if stage_output_path else None,
    })
    (exp_dir / "winner_recommendation.md").write_text(
        build_winner_recommendation_md(
            selection_summary,
            cost_estimate,
            pro_m,
            merge_summary,
            dry_run=args.dry_run,
        ),
        encoding="utf-8",
    )
    (exp_dir / "project_resolution.md").write_text(
        f"# Project Resolution\n\n"
        f"- PDF: {args.pdf}\n"
        f"- Project dir: {resolution.project_dir}\n"
        f"- Block source: `{resolution.block_source}`\n"
        f"- Blocks index: {resolution.blocks_index_path}\n"
        f"- Total blocks: {resolution.total_blocks}\n"
        f"- Recrop/rebuild: NO\n",
        encoding="utf-8",
    )

    logger.info("Artifacts saved to %s", exp_dir)
    print("\nFLASH -> PRO TRIAGE COMPLETE")
    print(f"Artifacts: {exp_dir}")
    print(f"Flash blocks={flash_m.total_input_blocks} coverage={flash_m.coverage_pct:.1f}% cost=${flash_m.total_cost_usd:.4f}")
    print(f"Selected for Pro: {selection_summary['selected_for_pro']} blocks")
    if stage_output_path:
        print(f"Stage output: {stage_output_path}")
    if pro_m:
        print(f"Pro coverage={pro_m.coverage_pct:.1f}% cost=${pro_m.total_cost_usd:.4f}")
    elif cost_estimate.stop_reason:
        print(f"Pro skipped: {cost_estimate.stop_reason}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage 02 Flash -> Pro selective triage runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--pdf", required=True, help="PDF filename to search under projects/")
    parser.add_argument("--project-dir", help="Optional explicit project directory")
    parser.add_argument("--parallelism-flash", type=int, default=3, choices=[1, 2, 3, 4])
    parser.add_argument("--parallelism-pro", type=int, default=2, choices=[1, 2, 3, 4])
    parser.add_argument("--include-simple-findings", action="store_true")
    parser.add_argument("--no-unreadable-rescue", action="store_true")
    parser.add_argument("--max-pro-blocks", type=int)
    parser.add_argument("--max-pro-cost-usd", type=float)
    parser.add_argument("--flash-cost-per-block", type=float, default=FLASH_SINGLE_BLOCK_EST_USD)
    parser.add_argument("--pro-cost-per-block", type=float, default=PRO_SINGLE_BLOCK_EST_USD)
    parser.add_argument(
        "--write-stage-output",
        action="store_true",
        help="Write merged result to _output/02_blocks_analysis.json",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    main()
