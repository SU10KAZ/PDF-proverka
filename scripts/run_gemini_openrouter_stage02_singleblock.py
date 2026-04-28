"""
Single-block OpenRouter experiment: Flash на полный документ + Pro selective escalation.

Продолжение после Phase A / Budget experiment.
Phase A, B-lite, C-lite, D (Flash batch full) НЕ перезапускаются.
Pro full-document run НЕ делается (запрещено спекой).

Последовательность:
  S1: Flash single-block на полный документ (parallelism=3)
  Rank: скоринг слабых блоков по findings/KV/summary/inferred_id
  S2: Pro single-block на top-N weakest (N=15 by default)
  Hybrid projection: прогноз стоимости гибридной политики на full doc

Артефакты: <project_dir>/_experiments/gemini_openrouter_stage02_singleblock/<timestamp>/

Usage:
  python scripts/run_gemini_openrouter_stage02_singleblock.py \\
      --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" \\
      --budget-cap-usd 2.50 \\
      --escalation-sample-size 15
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from run_gemini_openrouter_stage02_experiment import (  # noqa: E402
    BatchResultEnvelope,
    MODEL_FLASH,
    MODEL_PRO,
    RunMetrics,
    build_page_contexts,
    build_system_prompt,
    classify_risk,
    compute_metrics,
    load_blocks_index,
    run_batches_async,
)
from run_gemini_openrouter_stage02_budget import (  # noqa: E402
    BudgetTracker,
    _emit_budget_stop,
    _emit_budget_timeline_md,
    _save_csv,
    _save_json,
    _ts,
    per_block_est,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("openrouter_singleblock")


# ───────────────────────── Weak-block ranking ────────────────────────────────

# Weights per spec section 4, in descending importance
WEAK_WEIGHTS = {
    "no_findings":        4,
    "empty_summary":      3,
    "empty_key_values":   3,
    "low_kv":             2,   # kv_count <= 2
    "heavy_weak":         2,   # heavy/full-page/merged with weak output
    "short_summary":      1,   # <60 chars
    "inferred_block_id":  1,
    "schema_recovered":   1,   # placeholder (no signal for now)
}

LOW_KV_THRESHOLD = 2
SHORT_SUMMARY_THRESHOLD = 60


def score_weakness(block: dict, analysis: dict | None, inferred: bool) -> tuple[int, list[str]]:
    """Return (score, reasons[]) — higher = weaker."""
    if analysis is None:
        # no analysis at all (missing from output) — treat as maximally weak
        return 100, ["block_missing_from_output"]

    score = 0
    reasons: list[str] = []

    findings = analysis.get("findings") or []
    if not findings:
        score += WEAK_WEIGHTS["no_findings"]
        reasons.append("no_findings")

    summary = str(analysis.get("summary") or "").strip()
    if not summary:
        score += WEAK_WEIGHTS["empty_summary"]
        reasons.append("empty_summary")
    elif len(summary) < SHORT_SUMMARY_THRESHOLD:
        score += WEAK_WEIGHTS["short_summary"]
        reasons.append("short_summary")

    kv = analysis.get("key_values_read") or []
    if not kv:
        score += WEAK_WEIGHTS["empty_key_values"]
        reasons.append("empty_key_values")
    elif len(kv) <= LOW_KV_THRESHOLD:
        score += WEAK_WEIGHTS["low_kv"]
        reasons.append("low_kv")

    risk = classify_risk(block)
    heavy = (risk == "heavy") or block.get("is_full_page") or block.get("merged_block_ids")
    weak_output = (not findings) or (not kv) or (len(summary) < SHORT_SUMMARY_THRESHOLD)
    if heavy and weak_output:
        score += WEAK_WEIGHTS["heavy_weak"]
        reasons.append("heavy_weak")

    if inferred:
        score += WEAK_WEIGHTS["inferred_block_id"]
        reasons.append("inferred_block_id")

    return score, reasons


def rank_weak_blocks(
    results: list[BatchResultEnvelope],
    all_blocks: list[dict],
) -> list[dict]:
    """Return list sorted desc by weakness score.

    Each item: {block_id, score, reasons, page, size_kb, risk, findings_count,
                kv_count, summary_len, inferred}.
    """
    block_by_id = {b["block_id"]: b for b in all_blocks}
    analyses_by_bid: dict[str, dict] = {}
    inferred_bids: set[str] = set()
    for res in results:
        if res.inferred_block_id_count:
            # only single-block runs can infer
            for bid in res.input_block_ids:
                inferred_bids.add(bid)
        if res.is_error or not res.parsed_data:
            continue
        for a in res.parsed_data.get("block_analyses", []):
            if isinstance(a, dict):
                bid = a.get("block_id") or ""
                if bid:
                    analyses_by_bid[bid] = a

    ranked: list[dict] = []
    for bid, b in block_by_id.items():
        a = analyses_by_bid.get(bid)
        inferred = bid in inferred_bids
        score, reasons = score_weakness(b, a, inferred)
        ranked.append({
            "block_id": bid,
            "score": score,
            "reasons": reasons,
            "page": b.get("page"),
            "size_kb": float(b.get("size_kb", 0) or 0),
            "risk": classify_risk(b),
            "findings_count": len((a or {}).get("findings") or []),
            "kv_count": len((a or {}).get("key_values_read") or []),
            "summary_len": len(str((a or {}).get("summary") or "").strip()),
            "inferred": inferred,
            "analysis_present": a is not None,
        })
    # Tie-break: size_kb desc, then page asc for determinism
    ranked.sort(key=lambda x: (-x["score"], -x["size_kb"], x["page"] or 0))
    return ranked


def _build_weak_blocks_md(ranked: list[dict], top_n: int) -> str:
    top = ranked[:top_n]
    lines = [
        "# Weak Blocks Ranked (Flash single-block full doc)\n",
        f"Top {top_n} weakest of {len(ranked)} blocks shown below. "
        f"Full ranking is in `weak_blocks_ranked.json`.\n",
        "| Rank | block_id | Score | Risk | Page | Size KB | Findings | KV | Summary len | Reasons |",
        "|------|----------|-------|------|------|---------|----------|----|-------------|---------|",
    ]
    for i, x in enumerate(top, 1):
        lines.append(
            f"| {i} | {x['block_id']} | {x['score']} | {x['risk']} | {x['page']} "
            f"| {x['size_kb']:.0f} | {x['findings_count']} | {x['kv_count']} "
            f"| {x['summary_len']} | {','.join(x['reasons'])} |"
        )
    return "\n".join(lines) + "\n"


# ───────────────────────── Pro vs Flash per-sample diff ──────────────────────

def diff_pro_vs_flash(
    pro_results: list[BatchResultEnvelope],
    flash_results_full: list[BatchResultEnvelope],
    sample_ids: list[str],
) -> dict:
    """Compare Pro vs Flash on exact same sample block_ids."""
    def _collect(results, id_set):
        out = {}
        for res in results:
            if res.is_error or not res.parsed_data:
                continue
            for a in res.parsed_data.get("block_analyses", []):
                if isinstance(a, dict):
                    bid = a.get("block_id") or ""
                    if bid in id_set:
                        out[bid] = a
        return out

    sid_set = set(sample_ids)
    flash_map = _collect(flash_results_full, sid_set)
    pro_map = _collect(pro_results, sid_set)

    improved = 0
    unchanged = 0
    degraded = 0
    added_findings = 0
    added_kv = 0
    flash_total_findings = 0
    flash_total_kv = 0
    pro_total_findings = 0
    pro_total_kv = 0
    unreadable_recovery = 0

    per_block_diff: list[dict] = []

    for bid in sample_ids:
        f = flash_map.get(bid) or {}
        p = pro_map.get(bid) or {}

        f_findings = len(f.get("findings") or [])
        p_findings = len(p.get("findings") or [])
        f_kv = len(f.get("key_values_read") or [])
        p_kv = len(p.get("key_values_read") or [])
        f_unreadable = bool(f.get("unreadable_text"))
        p_unreadable = bool(p.get("unreadable_text"))

        flash_total_findings += f_findings
        flash_total_kv += f_kv
        pro_total_findings += p_findings
        pro_total_kv += p_kv

        delta_findings = p_findings - f_findings
        delta_kv = p_kv - f_kv
        added_findings += max(0, delta_findings)
        added_kv += max(0, delta_kv)

        # improved = Pro added findings OR recovered from unreadable/empty
        is_improved = (
            (delta_findings > 0)
            or (f_unreadable and not p_unreadable)
            or (f_kv == 0 and p_kv > 0)
        )
        # degraded = Pro lost findings OR became unreadable
        is_degraded = (
            (delta_findings < 0 and f_findings > 0)
            or (not f_unreadable and p_unreadable)
        )
        if f_unreadable and not p_unreadable:
            unreadable_recovery += 1

        if is_improved and not is_degraded:
            improved += 1
            status = "improved"
        elif is_degraded and not is_improved:
            degraded += 1
            status = "degraded"
        else:
            unchanged += 1
            status = "unchanged"

        per_block_diff.append({
            "block_id": bid,
            "status": status,
            "flash_findings": f_findings,
            "pro_findings": p_findings,
            "delta_findings": delta_findings,
            "flash_kv": f_kv,
            "pro_kv": p_kv,
            "delta_kv": delta_kv,
            "flash_unreadable": f_unreadable,
            "pro_unreadable": p_unreadable,
        })

    return {
        "sample_size": len(sample_ids),
        "improved": improved,
        "unchanged": unchanged,
        "degraded": degraded,
        "unreadable_recovery": unreadable_recovery,
        "added_findings": added_findings,
        "added_kv": added_kv,
        "flash_total_findings": flash_total_findings,
        "flash_total_kv": flash_total_kv,
        "pro_total_findings": pro_total_findings,
        "pro_total_kv": pro_total_kv,
        "per_block_diff": per_block_diff,
    }


# ───────────────────────── Hybrid projection ─────────────────────────────────

def project_hybrid_policy(
    *,
    flash_full_m: RunMetrics,
    sample_diff: dict,
    sample_pro_m: RunMetrics,
    ranked_all: list[dict],
    total_blocks: int,
) -> dict:
    """Project full-doc cost of Flash + selective Pro policy.

    Trigger rule: Pro escalation on blocks where `findings_count=0 AND kv_count<=2`
    (OR unreadable=true). Uses ranking data to estimate how many blocks qualify.
    """
    # Count blocks that would trigger escalation under the policy
    policy_triggered = 0
    for x in ranked_all:
        fnd = x["findings_count"]
        kv = x["kv_count"]
        # Skip blocks that never got analyzed (analysis_present=False) — treated separately
        if not x["analysis_present"]:
            policy_triggered += 1
            continue
        if fnd == 0 and kv <= LOW_KV_THRESHOLD:
            policy_triggered += 1
            continue
        if "empty_summary" in x["reasons"]:
            policy_triggered += 1
            continue

    # Flash cost scaling
    flash_cost_per_block = flash_full_m.cost_per_valid_block or 0.0

    # Pro cost scaling (from sample)
    if sample_pro_m.total_input_blocks > 0:
        pro_cost_per_block = sample_pro_m.total_cost_usd / sample_pro_m.total_input_blocks
    else:
        pro_cost_per_block = per_block_est(MODEL_PRO, "single")

    total_flash = flash_cost_per_block * total_blocks
    total_pro_hybrid = pro_cost_per_block * policy_triggered
    total_hybrid = total_flash + total_pro_hybrid

    # Improvement projection (linear scaling from sample)
    sample_size = max(1, sample_diff["sample_size"])
    improved_rate = sample_diff["improved"] / sample_size
    added_findings_rate = sample_diff["added_findings"] / sample_size
    projected_improved_blocks = int(round(improved_rate * policy_triggered))
    projected_added_findings = int(round(added_findings_rate * policy_triggered))

    extra_cost_per_improved = (
        total_pro_hybrid / max(1, projected_improved_blocks)
        if projected_improved_blocks > 0 else None
    )

    return {
        "trigger_rule": "Pro escalation: (analysis missing) OR (findings=0 AND kv<=2) OR (empty_summary)",
        "policy_triggered_count": policy_triggered,
        "total_blocks": total_blocks,
        "triggered_pct": round(policy_triggered / max(1, total_blocks) * 100, 1),
        "flash_cost_per_block": round(flash_cost_per_block, 6),
        "pro_cost_per_block_sample": round(pro_cost_per_block, 6),
        "projected_flash_full_cost": round(total_flash, 4),
        "projected_pro_escalation_cost": round(total_pro_hybrid, 4),
        "projected_hybrid_total_cost": round(total_hybrid, 4),
        "projected_improved_blocks": projected_improved_blocks,
        "projected_added_findings": projected_added_findings,
        "projected_extra_cost_per_improved": (
            round(extra_cost_per_improved, 5) if extra_cost_per_improved else None
        ),
        "sample_improved": sample_diff["improved"],
        "sample_unchanged": sample_diff["unchanged"],
        "sample_degraded": sample_diff["degraded"],
    }


def _build_hybrid_policy_md(projection: dict) -> str:
    lines = [
        "# Hybrid Policy Projection\n",
        "Practical trigger (voluntary): run Pro selectively only when Flash output is weak.\n",
        f"**Trigger rule**: {projection['trigger_rule']}\n",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total blocks in doc | {projection['total_blocks']} |",
        f"| Blocks that would trigger Pro | {projection['policy_triggered_count']} ({projection['triggered_pct']}%) |",
        f"| Flash cost per block (from full run) | ${projection['flash_cost_per_block']:.5f} |",
        f"| Pro cost per block (from sample) | ${projection['pro_cost_per_block_sample']:.5f} |",
        f"| **Projected Flash full cost** | ${projection['projected_flash_full_cost']:.4f} |",
        f"| **Projected Pro escalation cost** | ${projection['projected_pro_escalation_cost']:.4f} |",
        f"| **Projected hybrid total cost** | ${projection['projected_hybrid_total_cost']:.4f} |",
        f"| Projected improved blocks (Pro) | {projection['projected_improved_blocks']} |",
        f"| Projected added findings (Pro) | {projection['projected_added_findings']} |",
    ]
    if projection["projected_extra_cost_per_improved"] is not None:
        lines.append(f"| Projected extra $/improved block | ${projection['projected_extra_cost_per_improved']:.5f} |")
    lines.append("")
    lines.append("## Sample outcomes (Pro vs Flash on same blocks)")
    lines.append(f"- Improved: {projection['sample_improved']}")
    lines.append(f"- Unchanged: {projection['sample_unchanged']}")
    lines.append(f"- Degraded: {projection['sample_degraded']}")
    lines.append("")
    lines.append("> Projections assume the sample's improved-rate generalizes to the full document.")
    lines.append("> This is an estimate, NOT a measured full-doc hybrid run.")
    return "\n".join(lines) + "\n"


# ───────────────────────── Pro sample summary md ──────────────────────────────

def _build_pro_sample_summary_md(
    pro_m: RunMetrics,
    diff: dict,
    sample_size: int,
) -> str:
    lines = [
        "# Phase S2 — Pro Selective Escalation Sample\n",
        f"Sample: {sample_size} weakest blocks from Flash single-block full run.\n",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Coverage | {pro_m.coverage_pct:.1f}% |",
        f"| Missing | {pro_m.missing_count} |",
        f"| Duplicate | {pro_m.duplicate_count} |",
        f"| Extra | {pro_m.extra_count} |",
        f"| Unreadable recovery | {diff['unreadable_recovery']} |",
        f"| Improved blocks | {diff['improved']} / {sample_size} |",
        f"| Unchanged blocks | {diff['unchanged']} |",
        f"| Degraded blocks | {diff['degraded']} |",
        f"| Additional findings | +{diff['added_findings']} (Flash {diff['flash_total_findings']} -> Pro {diff['pro_total_findings']}) |",
        f"| Additional KV | +{diff['added_kv']} (Flash {diff['flash_total_kv']} -> Pro {diff['pro_total_kv']}) |",
        f"| Pro total cost USD | ${pro_m.total_cost_usd:.4f} |",
    ]
    if diff['added_findings'] > 0:
        lines.append(f"| Pro cost per added finding | ${pro_m.total_cost_usd / diff['added_findings']:.5f} |")
    if diff['improved'] > 0:
        lines.append(f"| Pro cost per improved block | ${pro_m.total_cost_usd / diff['improved']:.5f} |")
    lines.append(f"| Elapsed (s) | {pro_m.elapsed_s:.1f} |")
    return "\n".join(lines) + "\n"


def _build_flash_single_summary_md(m: RunMetrics) -> str:
    return (
        f"# Phase S1 — Flash Single-Block Full Document\n\n"
        f"Model: **{m.model_id}** | Mode: **single_block** | Parallelism: {m.parallelism}\n\n"
        f"| Metric | Value |\n|--------|-------|\n"
        f"| Total input blocks | {m.total_input_blocks} |\n"
        f"| Risk heavy/normal/light | {m.risk_heavy}/{m.risk_normal}/{m.risk_light} |\n"
        f"| Total single-block requests | {m.total_batches} |\n"
        f"| Coverage | {m.coverage_pct:.1f}% |\n"
        f"| Missing / Duplicate / Extra | {m.missing_count} / {m.duplicate_count} / {m.extra_count} |\n"
        f"| Inferred block_id | {m.inferred_block_id_count} |\n"
        f"| Unreadable | {m.unreadable_count} |\n"
        f"| Empty summary | {m.empty_summary_count} |\n"
        f"| Empty key_values | {m.empty_key_values_count} |\n"
        f"| Blocks with findings | {m.blocks_with_findings} |\n"
        f"| Total findings | {m.total_findings} |\n"
        f"| Findings/100 blocks | {m.findings_per_100_blocks:.1f} |\n"
        f"| Total / median KV | {m.total_key_values} / {m.median_key_values:.1f} |\n"
        f"| Elapsed | {m.elapsed_s:.1f}s |\n"
        f"| Avg / median / p95 per-block dur | {m.avg_batch_duration_s:.2f}s / {m.median_batch_duration_s:.2f}s / {m.p95_batch_duration_s:.2f}s |\n"
        f"| Prompt / completion / reasoning / cached tokens | {m.total_prompt_tokens} / {m.total_output_tokens} / {m.total_reasoning_tokens} / {m.total_cached_tokens} |\n"
        f"| Total cost USD | ${m.total_cost_usd:.4f} |\n"
        f"| Cost/valid block | ${m.cost_per_valid_block:.5f} |\n"
        f"| Cost/finding | ${m.cost_per_finding:.5f} |\n"
        f"| Cost source actual/est | {m.cost_sources_actual}/{m.cost_sources_estimated} |\n"
    )


# ───────────────────────── Winner recommendation ─────────────────────────────

def build_final_recommendation(
    *,
    flash_m: RunMetrics,
    pro_m: RunMetrics | None,
    diff: dict | None,
    projection: dict | None,
    budget: BudgetTracker,
    sample_size: int,
    total_blocks: int,
) -> str:
    # Completeness check for Flash
    flash_complete = (
        flash_m.coverage_pct == 100.0
        and flash_m.missing_count == 0
        and flash_m.duplicate_count == 0
        and flash_m.extra_count == 0
    )

    if not flash_complete:
        flash_verdict = (
            f"NOT production-ready as-is (coverage {flash_m.coverage_pct}%, "
            f"missing {flash_m.missing_count}, dup {flash_m.duplicate_count}, extra {flash_m.extra_count})"
        )
    else:
        flash_verdict = "production-ready for mainline"

    if pro_m is None or diff is None:
        pro_verdict = "NOT tested (budget or error)"
        escalation_recommend = "SKIPPED"
    else:
        completeness_ok = (
            pro_m.missing_count == 0 and pro_m.duplicate_count == 0 and pro_m.extra_count == 0
        )
        if not completeness_ok:
            pro_verdict = (
                f"unreliable on sample (missing {pro_m.missing_count}, dup {pro_m.duplicate_count})"
            )
            escalation_recommend = "NOT RECOMMENDED (Pro sample itself had completeness issues)"
        else:
            improved = diff["improved"]
            degraded = diff["degraded"]
            added_findings = diff["added_findings"]
            extra_per_improved = (
                pro_m.total_cost_usd / improved if improved > 0 else None
            )
            meaningful = (
                improved >= 3
                and degraded == 0
                and added_findings >= 5
                and (extra_per_improved is not None and extra_per_improved < 0.50)
            )
            if meaningful:
                pro_verdict = (
                    f"PASS — improved {improved}/{sample_size}, +{added_findings} findings, "
                    f"0 degradations, ${extra_per_improved:.4f} per improved block."
                )
                escalation_recommend = "RECOMMENDED"
            else:
                pro_verdict = (
                    f"MARGINAL — improved {improved}/{sample_size}, +{added_findings} findings, "
                    f"{degraded} degraded."
                )
                escalation_recommend = "NOT RECOMMENDED (gate not met)"

    # Final practical answer
    if flash_complete and escalation_recommend == "RECOMMENDED":
        practical = (
            "**Flash single-block + selective Pro escalation** (hybrid). "
            "Flash on full doc, Pro only on weak blocks matching trigger rule."
        )
    elif flash_complete:
        practical = "**Flash single-block only** — Pro escalation not justified by sample."
    else:
        practical = (
            "**Do NOT adopt Gemini/OpenRouter as stage 02 mainline yet**. "
            "Flash single-block has completeness regressions; Pro full-run not tested."
        )

    lines = [
        "# Winner Recommendation (single-block + selective escalation)\n",
        "## Practical answer\n",
        f"**Recommendation**: {practical}\n",
        "",
        "| Question | Answer |",
        "|----------|--------|",
        f"| 1. Flash single-block as practical mainline? | {flash_verdict} |",
        f"| 2. Selective Pro escalation needed? | {escalation_recommend} |",
        f"| 3. Trigger rule for escalation | {projection['trigger_rule'] if projection else 'N/A'} |",
        f"| 4. Projected Flash full cost | ${projection['projected_flash_full_cost']:.4f} |" if projection else "| 4. Projected Flash full cost | N/A |",
        f"| 4. Projected hybrid total cost | ${projection['projected_hybrid_total_cost']:.4f} |" if projection else "| 4. Projected hybrid total cost | N/A |",
        f"| 5. Total actual spend (this round) | ${budget.spent_usd:.4f} of ${budget.cap_usd:.2f} |",
        "",
        "## Flash single-block full-doc results",
        f"- Model: {flash_m.model_id}",
        f"- Blocks: {flash_m.total_input_blocks} (heavy {flash_m.risk_heavy} / normal {flash_m.risk_normal} / light {flash_m.risk_light})",
        f"- Coverage: {flash_m.coverage_pct:.1f}% (missing {flash_m.missing_count}, dup {flash_m.duplicate_count}, extra {flash_m.extra_count})",
        f"- Findings: {flash_m.total_findings} on {flash_m.blocks_with_findings} blocks ({flash_m.findings_per_100_blocks:.1f}/100)",
        f"- KV total: {flash_m.total_key_values} (median {flash_m.median_key_values:.1f}/block)",
        f"- Cost: ${flash_m.total_cost_usd:.4f} (cost/valid block ${flash_m.cost_per_valid_block:.5f})",
        f"- Elapsed: {flash_m.elapsed_s:.1f}s (p95 batch {flash_m.p95_batch_duration_s:.1f}s)",
        f"- Pro verdict on sample: {pro_verdict}",
        "",
    ]
    if projection:
        lines.extend([
            "## Projected hybrid economics",
            f"- {projection['policy_triggered_count']} of {projection['total_blocks']} blocks ({projection['triggered_pct']}%) would trigger Pro",
            f"- Projected hybrid full-doc cost: **${projection['projected_hybrid_total_cost']:.4f}**",
            f"  - Flash leg: ${projection['projected_flash_full_cost']:.4f}",
            f"  - Pro escalation leg: ${projection['projected_pro_escalation_cost']:.4f}",
        ])
        if projection["projected_extra_cost_per_improved"] is not None:
            lines.append(
                f"- Projected $/improved block: ${projection['projected_extra_cost_per_improved']:.5f}"
            )
        lines.append("")
    lines.extend([
        "## Constraints honored",
        "- Phase A / B / C / D not rerun.",
        "- No full-document Pro run.",
        "- Production stage_models.json UNCHANGED.",
        "- Claude CLI path untouched.",
        "- Actual `usage.cost` used when available.",
    ])
    return "\n".join(lines) + "\n"


# ───────────────────────── CLI + main ─────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Flash single-block full-doc + Pro selective escalation (budget-safe)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--pdf", required=True)
    p.add_argument("--project-dir")
    p.add_argument("--budget-cap-usd", type=float, default=2.50)
    p.add_argument("--parallelism", type=int, default=3, choices=[1, 2, 3, 4])
    p.add_argument("--escalation-sample-size", type=int, default=15)
    p.add_argument("--escalation-sample-max", type=int, default=20,
                   help="Increase sample to this value if budget allows")
    p.add_argument("--escalation-parallelism", type=int, default=2, choices=[1, 2, 3, 4])
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


async def main_async(args: argparse.Namespace) -> None:
    from webapp.config import OPENROUTER_API_KEY
    from webapp.services.project_service import resolve_project_by_pdf, ProjectByPdfError

    if args.project_dir:
        project_dir = Path(args.project_dir)
        project_id = project_dir.name
    else:
        try:
            project_id, project_dir = resolve_project_by_pdf(args.pdf)
        except ProjectByPdfError as e:
            logger.error("Cannot resolve project: %s", e)
            sys.exit(1)

    logger.info("Project dir: %s", project_dir)
    project_info_path = project_dir / "project_info.json"
    if not project_info_path.exists():
        logger.error("project_info.json not found: %s", project_info_path)
        sys.exit(1)
    project_info = json.loads(project_info_path.read_text(encoding="utf-8"))

    if not OPENROUTER_API_KEY and not args.dry_run:
        logger.error("OPENROUTER_API_KEY not set; use --dry-run for validation")
        sys.exit(1)

    all_blocks = load_blocks_index(project_dir)

    ts = _ts()
    exp_dir = project_dir / "_experiments" / "gemini_openrouter_stage02_singleblock" / ts
    exp_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Experiment dir: %s", exp_dir)

    budget = BudgetTracker(cap_usd=args.budget_cap_usd)

    manifest = {
        "timestamp": ts,
        "pdf": args.pdf,
        "project_id": project_id,
        "project_dir": str(project_dir),
        "total_blocks": len(all_blocks),
        "budget_cap_usd": args.budget_cap_usd,
        "parallelism_flash": args.parallelism,
        "parallelism_pro_escalation": args.escalation_parallelism,
        "escalation_sample_size": args.escalation_sample_size,
        "escalation_sample_max": args.escalation_sample_max,
        "dry_run": args.dry_run,
        "phase_a_rerun": False,
        "phase_b_c_d_rerun": False,
        "pro_full_run": False,
        "models": {"mainline_under_test": MODEL_FLASH, "escalation": MODEL_PRO},
    }
    _save_json(exp_dir / "manifest.json", manifest)

    # ── Phase S1: Flash single-block full doc ──
    ev_s1 = budget.preflight(
        phase="S1", run_id="S1_flash_singleblock_full",
        planned_blocks=len(all_blocks), planned_batches=len(all_blocks),
        per_block_usd=per_block_est(MODEL_FLASH, "single"),
    )
    logger.info(
        "[S1] preflight: blocks=%d est=$%.4f remaining=$%.4f approved=%s",
        len(all_blocks), ev_s1.estimated_usd, ev_s1.remaining_before, ev_s1.approved,
    )
    if not ev_s1.approved:
        _emit_budget_stop(exp_dir, budget, "S1 Flash full run exceeds budget cap")
        _save_json(exp_dir / "budget_usage_timeline.json", budget.to_dict())
        _emit_budget_timeline_md(exp_dir, budget)
        logger.error("Cannot run S1 within budget.")
        return

    system_prompt = build_system_prompt(project_info, len(all_blocks))
    page_contexts = build_page_contexts(project_dir)
    single_batches = [[b] for b in all_blocks]

    flash_results, elapsed = await run_batches_async(
        single_batches,
        project_dir, project_info,
        system_prompt, page_contexts, all_blocks,
        MODEL_FLASH, args.parallelism, "S1_flash_single",
        strict_schema=True, response_healing=True,
        require_parameters=True, provider_data_collection=None,
        dry_run=args.dry_run,
    )
    flash_m = compute_metrics(
        flash_results, all_blocks,
        run_id="S1_flash_single", model_id=MODEL_FLASH,
        batch_profile="single", parallelism=args.parallelism,
        mode="single_block", elapsed_s=elapsed,
        strict_schema_enabled=True, response_healing_enabled=True,
        require_parameters_enabled=True, dry_run=args.dry_run,
    )
    budget.commit(ev_s1, flash_m.total_cost_usd)
    _save_json(exp_dir / "flash_singleblock_full_metrics.json", asdict(flash_m))
    (exp_dir / "flash_singleblock_full_summary.md").write_text(
        _build_flash_single_summary_md(flash_m), encoding="utf-8"
    )
    logger.info(
        "[S1] coverage=%.1f%% findings=%d kv=%d cost=$%.4f elapsed=%.1fs -> remaining=$%.4f",
        flash_m.coverage_pct, flash_m.total_findings, flash_m.total_key_values,
        flash_m.total_cost_usd, flash_m.elapsed_s, budget.remaining,
    )

    # ── Rank weak blocks ──
    ranked = rank_weak_blocks(flash_results, all_blocks)
    _save_json(exp_dir / "weak_blocks_ranked.json", ranked)

    # Decide escalation sample size based on budget headroom
    sample_size = args.escalation_sample_size
    pro_per_block = per_block_est(MODEL_PRO, "single")
    for try_size in range(args.escalation_sample_max, args.escalation_sample_size - 1, -1):
        if try_size * pro_per_block <= budget.remaining:
            sample_size = try_size
            break

    sample_ids = [x["block_id"] for x in ranked[:sample_size]]
    _save_json(exp_dir / "escalation_sample_block_ids.json", sample_ids)
    (exp_dir / "weak_blocks_ranked.md").write_text(
        _build_weak_blocks_md(ranked, top_n=sample_size), encoding="utf-8"
    )
    logger.info(
        "Weak-block ranking: %d blocks scored; top-%d sample_ids saved (max_requested=%d)",
        len(ranked), len(sample_ids), args.escalation_sample_max,
    )

    # ── Phase S2: Pro escalation ──
    pro_m: RunMetrics | None = None
    diff: dict | None = None
    sample_blocks = [b for b in all_blocks if b["block_id"] in set(sample_ids)]

    ev_s2 = budget.preflight(
        phase="S2", run_id=f"S2_pro_escalation_{len(sample_blocks)}",
        planned_blocks=len(sample_blocks), planned_batches=len(sample_blocks),
        per_block_usd=per_block_est(MODEL_PRO, "single"),
    )
    logger.info(
        "[S2] preflight: sample=%d est=$%.4f remaining=$%.4f approved=%s",
        len(sample_blocks), ev_s2.estimated_usd, ev_s2.remaining_before, ev_s2.approved,
    )
    if ev_s2.approved and sample_blocks:
        sample_prompt = build_system_prompt(project_info, len(sample_blocks))
        pro_batches = [[b] for b in sample_blocks]
        pro_results, pro_elapsed = await run_batches_async(
            pro_batches,
            project_dir, project_info,
            sample_prompt, page_contexts, sample_blocks,
            MODEL_PRO, args.escalation_parallelism, "S2_pro_escalation",
            strict_schema=True, response_healing=True,
            require_parameters=True, provider_data_collection=None,
            dry_run=args.dry_run,
        )
        pro_m = compute_metrics(
            pro_results, sample_blocks,
            run_id="S2_pro_escalation", model_id=MODEL_PRO,
            batch_profile="single", parallelism=args.escalation_parallelism,
            mode="single_block", elapsed_s=pro_elapsed,
            strict_schema_enabled=True, response_healing_enabled=True,
            require_parameters_enabled=True, dry_run=args.dry_run,
        )
        budget.commit(ev_s2, pro_m.total_cost_usd)
        _save_json(exp_dir / "pro_escalation_sample_metrics.json", asdict(pro_m))

        diff = diff_pro_vs_flash(pro_results, flash_results, sample_ids)
        _save_json(exp_dir / "pro_escalation_sample_diff.json", diff)
        (exp_dir / "pro_escalation_sample_summary.md").write_text(
            _build_pro_sample_summary_md(pro_m, diff, len(sample_blocks)),
            encoding="utf-8",
        )
        logger.info(
            "[S2] Pro: coverage=%.1f%% findings=%d improved=%d degraded=%d cost=$%.4f -> remaining=$%.4f",
            pro_m.coverage_pct, pro_m.total_findings, diff["improved"], diff["degraded"],
            pro_m.total_cost_usd, budget.remaining,
        )
    else:
        _emit_budget_stop(exp_dir, budget, "Phase S2 Pro escalation skipped (budget)")

    # ── Hybrid projection ──
    projection: dict | None = None
    if pro_m is not None and diff is not None:
        projection = project_hybrid_policy(
            flash_full_m=flash_m,
            sample_diff=diff,
            sample_pro_m=pro_m,
            ranked_all=ranked,
            total_blocks=len(all_blocks),
        )
        _save_json(exp_dir / "hybrid_policy_projection.json", projection)
        (exp_dir / "hybrid_policy_recommendation.md").write_text(
            _build_hybrid_policy_md(projection), encoding="utf-8"
        )

    # ── Winner recommendation ──
    rec = build_final_recommendation(
        flash_m=flash_m, pro_m=pro_m, diff=diff, projection=projection,
        budget=budget, sample_size=len(sample_blocks), total_blocks=len(all_blocks),
    )
    (exp_dir / "winner_recommendation.md").write_text(rec, encoding="utf-8")

    _save_json(exp_dir / "budget_usage_timeline.json", budget.to_dict())
    _emit_budget_timeline_md(exp_dir, budget)

    print("\n" + "=" * 70)
    print("SINGLE-BLOCK EXPERIMENT COMPLETE")
    print(f"Artifacts: {exp_dir}")
    print(f"Spent: ${budget.spent_usd:.4f} of ${budget.cap_usd:.2f} (remaining ${budget.remaining:.4f})")
    print("=" * 70)
    print(rec[:1600])


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
