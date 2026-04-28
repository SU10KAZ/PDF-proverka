"""
Budget-aware stage 02 (block_batch) experiment — OpenRouter only, Flash-first.

Продолжение после Phase A (см. run_gemini_openrouter_stage02_experiment.py).
Phase A НЕ перезапускается.
Pro full-run по документу НЕ делается.

Последовательность:
  B-lite : Flash × {b8,b10,b12} на fixed subset (top-2 profiles)
  C-lite : top-2 profiles × parallelism {2,3} на subset (winner profile+para)
  D      : Один full-document run на Flash winner profile+parallelism
  E      : Только 15-блок Pro fallback sample (если остался budget)

Все run'ы проходят через preflight cost estimation, и если planned spend
превышает remaining budget — останавливаемся и фиксируем budget_stop_reason.

Артефакты:
  <project_dir>/_experiments/gemini_openrouter_stage02_budget/<timestamp>/

Usage:
  python scripts/run_gemini_openrouter_stage02_budget.py \\
      --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" \\
      --reuse-phase-a \\
      --budget-cap-usd 6.0 \\
      --fallback-sample-size 15
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

# Reuse existing runner helpers
from run_gemini_openrouter_stage02_experiment import (  # noqa: E402
    BATCH_PROFILES,
    BatchResultEnvelope,
    HARD_CAP,
    MODEL_FLASH,
    MODEL_PRO,
    RunMetrics,
    apply_byte_cap_split,
    build_messages,
    build_page_contexts,
    build_system_prompt,
    classify_risk,
    compute_metrics,
    load_blocks_index,
    pack_blocks,
    run_batches_async,
    select_batch_profile_winner,
    select_parallelism_winner,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("openrouter_budget")


# ───────────────────────── Budget tracker ─────────────────────────────────────

@dataclass
class BudgetEvent:
    phase: str
    run_id: str
    planned_blocks: int
    planned_batches: int
    estimated_usd: float
    approved: bool
    actual_usd: float = 0.0
    remaining_before: float = 0.0
    remaining_after: float = 0.0
    note: str = ""


@dataclass
class BudgetTracker:
    cap_usd: float
    spent_usd: float = 0.0
    events: list[BudgetEvent] = field(default_factory=list)
    stopped: bool = False
    stop_reason: str = ""

    @property
    def remaining(self) -> float:
        return max(0.0, self.cap_usd - self.spent_usd)

    def preflight(
        self,
        *,
        phase: str,
        run_id: str,
        planned_blocks: int,
        planned_batches: int,
        per_block_usd: float,
    ) -> BudgetEvent:
        est = round(per_block_usd * planned_blocks, 4)
        ev = BudgetEvent(
            phase=phase, run_id=run_id,
            planned_blocks=planned_blocks,
            planned_batches=planned_batches,
            estimated_usd=est,
            approved=False,
            remaining_before=round(self.remaining, 4),
        )
        approved = est <= self.remaining
        ev.approved = approved
        if not approved:
            ev.note = (
                f"planned ${est:.4f} exceeds remaining ${self.remaining:.4f}"
            )
            self.stopped = True
            self.stop_reason = f"[{phase}/{run_id}] {ev.note}"
        self.events.append(ev)
        return ev

    def commit(self, ev: BudgetEvent, actual_usd: float) -> None:
        self.spent_usd = round(self.spent_usd + actual_usd, 6)
        ev.actual_usd = round(actual_usd, 6)
        ev.remaining_after = round(self.remaining, 6)

    def to_dict(self) -> dict:
        return {
            "cap_usd": self.cap_usd,
            "spent_usd": round(self.spent_usd, 6),
            "remaining_usd": round(self.remaining, 6),
            "stopped": self.stopped,
            "stop_reason": self.stop_reason,
            "events": [asdict(e) for e in self.events],
        }


# Per-block cost preflight estimate (USD), based on Phase A measured data.
# Flash in single-block mode measured $0.00263/block.
# For batched Flash, ~30% discount is typical (amortized system prompt).
# Use conservative estimate — overestimate slightly to avoid busting cap.
PER_BLOCK_EST = {
    ("flash", "single"): 0.0035,
    ("flash", "batch"):  0.0025,
    ("pro",   "single"): 0.0700,  # measured $0.0645 — round up
    ("pro",   "batch"):  0.0450,
}


def per_block_est(model_id: str, mode: str) -> float:
    key = ("flash", mode) if "flash" in model_id else ("pro", mode)
    return PER_BLOCK_EST.get(key, 0.05)


# ───────────────────────── Helpers ────────────────────────────────────────────

def _save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _find_phase_a_subset(project_dir: Path) -> Path | None:
    """Locate a subset file from a prior OpenRouter Phase A run (preferred) or any prior exp."""
    root = project_dir / "_experiments"
    if not root.exists():
        return None
    # Prefer the OpenRouter experiment directory
    for dirname in ["gemini_openrouter_stage02", "gemini_direct_stage02", "block_batch_ab"]:
        sub = root / dirname
        if sub.exists():
            candidates = sorted(
                sub.rglob("fixed_subset_block_ids.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if candidates:
                return candidates[0]
    # Fallback: any subset
    any_candidates = sorted(
        root.rglob("fixed_subset_block_ids.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return any_candidates[0] if any_candidates else None


def _load_subset(subset_file: Path, all_blocks: list[dict]) -> tuple[list[str], list[dict]]:
    ids = json.loads(subset_file.read_text(encoding="utf-8"))
    id_set = set(ids)
    blocks = [b for b in all_blocks if b["block_id"] in id_set]
    if len(blocks) != len(ids):
        raise RuntimeError(
            f"Subset invalid: file has {len(ids)} IDs, only {len(blocks)} matched "
            f"in current blocks/index.json. File: {subset_file}"
        )
    return ids, blocks


# ───────────────────────── Weak-block selection ───────────────────────────────

def select_weak_blocks(
    full_results: list[BatchResultEnvelope],
    all_blocks: list[dict],
    sample_size: int = 15,
) -> tuple[list[str], list[dict]]:
    """Select N "weakest" blocks from Flash full-doc results for Pro escalation.

    Priority (sorted descending by composite weakness score):
      3 pts: unreadable_text=true
      2 pts: empty key_values_read
      2 pts: 0 findings
      1 pt:  very low kv count (<=2)
      1 pt:  very short summary (<40 chars)
      +heavy-risk tie-break.
    """
    block_by_id = {b["block_id"]: b for b in all_blocks}

    analyses_by_bid: dict[str, dict] = {}
    for res in full_results:
        if res.is_error or not res.parsed_data:
            # Treat error blocks as maximally weak
            for bid in res.input_block_ids:
                if bid not in analyses_by_bid:
                    analyses_by_bid[bid] = {"_error": True}
            continue
        for a in res.parsed_data.get("block_analyses", []):
            if isinstance(a, dict):
                bid = a.get("block_id") or ""
                if bid:
                    analyses_by_bid[bid] = a

    scored: list[tuple[int, str, dict]] = []
    for bid, a in analyses_by_bid.items():
        if a.get("_error"):
            score = 10
        else:
            score = 0
            if a.get("unreadable_text"):
                score += 3
            kv = a.get("key_values_read", []) or []
            if not kv:
                score += 2
            elif len(kv) <= 2:
                score += 1
            findings = a.get("findings", []) or []
            if not findings:
                score += 2
            summary = str(a.get("summary", "") or "").strip()
            if len(summary) < 40:
                score += 1
        if classify_risk(block_by_id.get(bid, {})) == "heavy":
            score += 1
        scored.append((score, bid, a))

    # Higher score = weaker. Break ties with size_kb desc (heavier first).
    scored.sort(
        key=lambda t: (-t[0], -float(block_by_id.get(t[1], {}).get("size_kb", 0))),
    )
    sample_ids: list[str] = []
    for score, bid, _ in scored:
        if len(sample_ids) >= sample_size:
            break
        if bid and bid in block_by_id:
            sample_ids.append(bid)

    sample_blocks = [block_by_id[bid] for bid in sample_ids]
    return sample_ids, sample_blocks


# ───────────────────────── Phase runners ──────────────────────────────────────

async def run_phase_b_lite(
    subset_blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    profiles: list[str],
    parallelism: int,
    byte_cap_kb: int,
    budget: BudgetTracker,
    dry_run: bool,
) -> tuple[list[RunMetrics], str | None, str | None]:
    """Flash × {profiles} on subset. Returns (metrics_list, winner, runner_up)."""
    system_prompt = build_system_prompt(project_info, len(subset_blocks))
    page_contexts = build_page_contexts(project_dir)
    metrics_list: list[RunMetrics] = []

    for profile_name in profiles:
        risk_targets = BATCH_PROFILES[profile_name]
        batches = pack_blocks(subset_blocks, risk_targets)
        batches = apply_byte_cap_split(batches, byte_cap_kb=byte_cap_kb)
        run_id = f"B_{profile_name}_subset"

        ev = budget.preflight(
            phase="B-lite", run_id=run_id,
            planned_blocks=len(subset_blocks), planned_batches=len(batches),
            per_block_usd=per_block_est(MODEL_FLASH, "batch"),
        )
        logger.info(
            "[B-lite/%s] preflight: batches=%d est=$%.4f remaining=$%.4f approved=%s",
            profile_name, len(batches), ev.estimated_usd, ev.remaining_before, ev.approved,
        )
        if not ev.approved:
            logger.warning("[B-lite/%s] SKIP (budget)", profile_name)
            break

        results, elapsed = await run_batches_async(
            batches,
            project_dir, project_info,
            system_prompt, page_contexts, subset_blocks,
            MODEL_FLASH, parallelism, run_id,
            strict_schema=True, response_healing=True,
            require_parameters=True, provider_data_collection=None,
            dry_run=dry_run,
        )
        m = compute_metrics(
            results, subset_blocks,
            run_id=run_id, model_id=MODEL_FLASH,
            batch_profile=profile_name, parallelism=parallelism,
            mode="batch", elapsed_s=elapsed,
            strict_schema_enabled=True,
            response_healing_enabled=True,
            require_parameters_enabled=True,
            dry_run=dry_run,
        )
        budget.commit(ev, m.total_cost_usd)
        metrics_list.append(m)
        _save_json(exp_dir / f"phase_b_lite_{profile_name}_metrics.json", asdict(m))
        logger.info(
            "[B-lite/%s] coverage=%.1f%% findings=%d kv_total=%d cost=$%.4f elapsed=%.1fs -> remaining=$%.4f",
            profile_name, m.coverage_pct, m.total_findings, m.total_key_values,
            m.total_cost_usd, m.elapsed_s, budget.remaining,
        )

    if not metrics_list:
        return [], None, None

    winner, runner_up = select_batch_profile_winner(metrics_list)

    _save_json(exp_dir / "phase_b_lite_summary.json", [asdict(m) for m in metrics_list])
    _save_csv(exp_dir / "phase_b_lite_summary.csv", [asdict(m) for m in metrics_list])
    (exp_dir / "phase_b_lite_summary.md").write_text(
        _build_b_lite_md(metrics_list, winner, runner_up), encoding="utf-8"
    )
    return metrics_list, winner, runner_up


def _build_b_lite_md(metrics: list[RunMetrics], winner: str, runner_up: str) -> str:
    lines = [
        "# Phase B-lite — Flash Batch Profile Screening on Subset\n",
        f"Winner: **{winner}** | Runner-up: **{runner_up}**\n",
        "| Profile | Coverage | Batches | Avg Size | Max Size | Avg KB | KV total | Findings | Cost USD | Cost/block | Elapsed (s) |",
        "|---------|----------|---------|----------|----------|--------|----------|----------|----------|------------|-------------|",
    ]
    for m in metrics:
        marker = " ★" if m.batch_profile == winner else (" ●" if m.batch_profile == runner_up else "")
        lines.append(
            f"| {m.batch_profile}{marker} | {m.coverage_pct:.1f}% | {m.total_batches} "
            f"| {m.avg_batch_size:.1f} | {m.max_batch_size} | {m.avg_batch_kb:.0f} "
            f"| {m.total_key_values} | {m.total_findings} | ${m.total_cost_usd:.4f} "
            f"| ${m.cost_per_valid_block:.5f} | {m.elapsed_s:.1f} |"
        )
    return "\n".join(lines) + "\n"


async def run_phase_c_lite(
    subset_blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    top_profiles: list[str],
    parallelism_options: list[int],
    byte_cap_kb: int,
    budget: BudgetTracker,
    dry_run: bool,
) -> tuple[list[RunMetrics], int | None, str | None]:
    """top-2 profiles × {parallelisms} on subset. Returns (metrics, win_para, win_profile)."""
    system_prompt = build_system_prompt(project_info, len(subset_blocks))
    page_contexts = build_page_contexts(project_dir)
    metrics_list: list[RunMetrics] = []

    seen_profiles = []
    for p in top_profiles:
        if p not in seen_profiles:
            seen_profiles.append(p)

    for profile_name in seen_profiles:
        risk_targets = BATCH_PROFILES[profile_name]
        batches = pack_blocks(subset_blocks, risk_targets)
        batches = apply_byte_cap_split(batches, byte_cap_kb=byte_cap_kb)

        for para in parallelism_options:
            run_id = f"C_{profile_name}_p{para}_subset"
            ev = budget.preflight(
                phase="C-lite", run_id=run_id,
                planned_blocks=len(subset_blocks), planned_batches=len(batches),
                per_block_usd=per_block_est(MODEL_FLASH, "batch"),
            )
            logger.info(
                "[C-lite/%s/p%d] preflight: est=$%.4f remaining=$%.4f approved=%s",
                profile_name, para, ev.estimated_usd, ev.remaining_before, ev.approved,
            )
            if not ev.approved:
                logger.warning("[C-lite] budget exhausted — stopping")
                break

            results, elapsed = await run_batches_async(
                batches,
                project_dir, project_info,
                system_prompt, page_contexts, subset_blocks,
                MODEL_FLASH, para, run_id,
                strict_schema=True, response_healing=True,
                require_parameters=True, provider_data_collection=None,
                dry_run=dry_run,
            )
            m = compute_metrics(
                results, subset_blocks,
                run_id=run_id, model_id=MODEL_FLASH,
                batch_profile=profile_name, parallelism=para,
                mode="batch", elapsed_s=elapsed,
                strict_schema_enabled=True,
                response_healing_enabled=True,
                require_parameters_enabled=True,
                dry_run=dry_run,
            )
            budget.commit(ev, m.total_cost_usd)
            metrics_list.append(m)
            _save_json(exp_dir / f"phase_c_lite_{profile_name}_p{para}_metrics.json", asdict(m))
            logger.info(
                "[C-lite/%s/p%d] coverage=%.1f%% elapsed=%.1fs retries=%d cost=$%.4f -> remaining=$%.4f",
                profile_name, para, m.coverage_pct, m.elapsed_s,
                m.retry_count, m.total_cost_usd, budget.remaining,
            )
        if budget.stopped:
            break

    if not metrics_list:
        return [], None, None

    win_para, win_prof = select_parallelism_winner(metrics_list)

    _save_json(exp_dir / "phase_c_lite_summary.json", [asdict(m) for m in metrics_list])
    _save_csv(exp_dir / "phase_c_lite_summary.csv", [asdict(m) for m in metrics_list])
    (exp_dir / "phase_c_lite_summary.md").write_text(
        _build_c_lite_md(metrics_list, win_para, win_prof), encoding="utf-8"
    )
    return metrics_list, win_para, win_prof


def _build_c_lite_md(metrics: list[RunMetrics], win_para: int, win_prof: str) -> str:
    lines = [
        "# Phase C-lite — Flash Parallelism Screening on Subset\n",
        f"Winner: **profile={win_prof}, parallelism={win_para}**\n",
        "| Profile | Parallelism | Coverage | Failed | Retries | Elapsed (s) | Cost USD | Cost/block |",
        "|---------|-------------|----------|--------|---------|-------------|----------|------------|",
    ]
    for m in metrics:
        marker = " ★" if (m.parallelism == win_para and m.batch_profile == win_prof) else ""
        lines.append(
            f"| {m.batch_profile} | {m.parallelism}{marker} | {m.coverage_pct:.1f}% "
            f"| {m.failed_batches} | {m.retry_count} | {m.elapsed_s:.1f} "
            f"| ${m.total_cost_usd:.4f} | ${m.cost_per_valid_block:.5f} |"
        )
    return "\n".join(lines) + "\n"


async def run_phase_d(
    all_blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    winner_profile: str,
    winner_parallelism: int,
    byte_cap_kb: int,
    budget: BudgetTracker,
    dry_run: bool,
) -> tuple[RunMetrics | None, list[BatchResultEnvelope]]:
    """One full-document Flash run on winner profile/parallelism."""
    system_prompt = build_system_prompt(project_info, len(all_blocks))
    page_contexts = build_page_contexts(project_dir)
    risk_targets = BATCH_PROFILES[winner_profile]
    batches = pack_blocks(all_blocks, risk_targets)
    batches = apply_byte_cap_split(batches, byte_cap_kb=byte_cap_kb)

    run_id = f"D_{winner_profile}_p{winner_parallelism}_full"
    ev = budget.preflight(
        phase="D", run_id=run_id,
        planned_blocks=len(all_blocks), planned_batches=len(batches),
        per_block_usd=per_block_est(MODEL_FLASH, "batch"),
    )
    logger.info(
        "[D] preflight: profile=%s para=%d blocks=%d batches=%d est=$%.4f remaining=$%.4f approved=%s",
        winner_profile, winner_parallelism,
        len(all_blocks), len(batches), ev.estimated_usd, ev.remaining_before, ev.approved,
    )
    if not ev.approved:
        logger.warning("[D] SKIP (budget)")
        return None, []

    results, elapsed = await run_batches_async(
        batches,
        project_dir, project_info,
        system_prompt, page_contexts, all_blocks,
        MODEL_FLASH, winner_parallelism, run_id,
        strict_schema=True, response_healing=True,
        require_parameters=True, provider_data_collection=None,
        dry_run=dry_run,
    )
    m = compute_metrics(
        results, all_blocks,
        run_id=run_id, model_id=MODEL_FLASH,
        batch_profile=winner_profile, parallelism=winner_parallelism,
        mode="batch", elapsed_s=elapsed,
        strict_schema_enabled=True,
        response_healing_enabled=True,
        require_parameters_enabled=True,
        dry_run=dry_run,
    )
    budget.commit(ev, m.total_cost_usd)
    _save_json(exp_dir / "flash_full_winner_metrics.json", asdict(m))
    (exp_dir / "flash_full_summary.md").write_text(
        _build_flash_full_md(m), encoding="utf-8"
    )
    logger.info(
        "[D] coverage=%.1f%% batches=%d findings=%d kv_total=%d cost=$%.4f elapsed=%.1fs -> remaining=$%.4f",
        m.coverage_pct, m.total_batches, m.total_findings, m.total_key_values,
        m.total_cost_usd, m.elapsed_s, budget.remaining,
    )
    return m, results


def _build_flash_full_md(m: RunMetrics) -> str:
    return (
        f"# Phase D — Flash Full-Document Run\n\n"
        f"Model: **{m.model_id}**\n"
        f"Profile: **{m.batch_profile}**, Parallelism: **{m.parallelism}**\n\n"
        f"| Metric | Value |\n"
        f"|--------|-------|\n"
        f"| Total input blocks | {m.total_input_blocks} |\n"
        f"| Risk heavy/normal/light | {m.risk_heavy}/{m.risk_normal}/{m.risk_light} |\n"
        f"| Total batches | {m.total_batches} |\n"
        f"| Avg / median / max batch size | {m.avg_batch_size:.1f} / {m.median_batch_size:.1f} / {m.max_batch_size} |\n"
        f"| Avg / median batch KB | {m.avg_batch_kb:.0f} / {m.median_batch_kb:.0f} |\n"
        f"| Avg / median prompt tokens | {m.avg_prompt_tokens:.0f} / {m.median_prompt_tokens:.0f} |\n"
        f"| Elapsed | {m.elapsed_s:.1f}s |\n"
        f"| Avg / median / p95 batch dur | {m.avg_batch_duration_s:.2f}s / {m.median_batch_duration_s:.2f}s / {m.p95_batch_duration_s:.2f}s |\n"
        f"| Coverage | {m.coverage_pct:.1f}% |\n"
        f"| Missing / Duplicate / Extra | {m.missing_count} / {m.duplicate_count} / {m.extra_count} |\n"
        f"| Unreadable | {m.unreadable_count} |\n"
        f"| Blocks with findings | {m.blocks_with_findings} |\n"
        f"| Total findings | {m.total_findings} |\n"
        f"| Findings/100 blocks | {m.findings_per_100_blocks:.1f} |\n"
        f"| Total / median key values | {m.total_key_values} / {m.median_key_values:.1f} |\n"
        f"| Retry / timeout / provider errors | {m.retry_count} / {m.timeout_errors} / {m.provider_errors} |\n"
        f"| Total cost USD (actual) | ${m.total_cost_usd:.4f} |\n"
        f"| Cost/valid block | ${m.cost_per_valid_block:.5f} |\n"
        f"| Cost/finding | ${m.cost_per_finding:.5f} |\n"
        f"| Cost source actual/est | {m.cost_sources_actual}/{m.cost_sources_estimated} |\n"
    )


async def run_phase_e_pro_fallback(
    all_blocks: list[dict],
    full_flash_results: list[BatchResultEnvelope],
    full_flash_metrics: RunMetrics,
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    sample_size: int,
    budget: BudgetTracker,
    dry_run: bool,
) -> RunMetrics | None:
    """15-block Pro escalation on weakest Flash blocks (if budget allows)."""
    sample_ids, sample_blocks = select_weak_blocks(
        full_flash_results, all_blocks, sample_size=sample_size
    )
    if not sample_blocks:
        logger.info("[E] No weak blocks — skip")
        return None

    _save_json(exp_dir / "pro_fallback_sample_ids.json", sample_ids)

    parallelism = 3  # preflight below will decide; 3 is default
    ev = budget.preflight(
        phase="E", run_id=f"E_pro_fallback_{sample_size}",
        planned_blocks=len(sample_blocks), planned_batches=len(sample_blocks),
        per_block_usd=per_block_est(MODEL_PRO, "single"),
    )
    logger.info(
        "[E] preflight: sample=%d est=$%.4f remaining=$%.4f approved=%s",
        len(sample_blocks), ev.estimated_usd, ev.remaining_before, ev.approved,
    )
    if not ev.approved:
        logger.warning("[E] SKIP (budget)")
        return None

    system_prompt = build_system_prompt(project_info, len(sample_blocks))
    page_contexts = build_page_contexts(project_dir)
    single_batches = [[b] for b in sample_blocks]

    results, elapsed = await run_batches_async(
        single_batches,
        project_dir, project_info,
        system_prompt, page_contexts, sample_blocks,
        MODEL_PRO, parallelism, "E_pro_fallback",
        strict_schema=True, response_healing=True,
        require_parameters=True, provider_data_collection=None,
        dry_run=dry_run,
    )
    pro_m = compute_metrics(
        results, sample_blocks,
        run_id="E_pro_fallback", model_id=MODEL_PRO,
        batch_profile="single", parallelism=parallelism,
        mode="single_block", elapsed_s=elapsed,
        strict_schema_enabled=True,
        response_healing_enabled=True,
        require_parameters_enabled=True,
        dry_run=dry_run,
    )
    budget.commit(ev, pro_m.total_cost_usd)

    # Compare Pro vs Flash on SAME blocks
    flash_on_sample = _flash_metrics_on_ids(full_flash_results, sample_ids, all_blocks)

    roi_md = _build_pro_fallback_md(
        pro_m, flash_on_sample, full_flash_metrics,
        sample_ids, sample_blocks, len(sample_blocks),
    )
    (exp_dir / "pro_fallback_sample.md").write_text(roi_md, encoding="utf-8")
    _save_json(exp_dir / "pro_fallback_sample.json", {
        "pro": asdict(pro_m),
        "flash_on_same_sample": flash_on_sample,
        "sample_ids": sample_ids,
    })
    logger.info(
        "[E] Pro: coverage=%.1f%% findings=%d kv=%d cost=$%.4f -> remaining=$%.4f",
        pro_m.coverage_pct, pro_m.total_findings, pro_m.total_key_values,
        pro_m.total_cost_usd, budget.remaining,
    )
    return pro_m


def _flash_metrics_on_ids(
    full_results: list[BatchResultEnvelope],
    sample_ids: list[str],
    all_blocks: list[dict],
) -> dict:
    """Compute Flash sub-metrics on the Pro fallback sample block_ids."""
    sid_set = set(sample_ids)
    block_by_id = {b["block_id"]: b for b in all_blocks}

    total = len(sample_ids)
    returned = 0
    unreadable = 0
    empty_kv = 0
    zero_findings = 0
    total_findings = 0
    total_kv = 0
    summaries_short = 0

    for res in full_results:
        if res.is_error or not res.parsed_data:
            continue
        for a in res.parsed_data.get("block_analyses", []):
            if not isinstance(a, dict):
                continue
            bid = a.get("block_id") or ""
            if bid not in sid_set:
                continue
            returned += 1
            if a.get("unreadable_text"):
                unreadable += 1
            kv = a.get("key_values_read", []) or []
            if not kv:
                empty_kv += 1
            total_kv += len(kv)
            f = a.get("findings", []) or []
            if not f:
                zero_findings += 1
            total_findings += len(f)
            if len(str(a.get("summary", "") or "").strip()) < 40:
                summaries_short += 1

    return {
        "sample_size": total,
        "returned": returned,
        "unreadable": unreadable,
        "empty_key_values": empty_kv,
        "zero_findings": zero_findings,
        "total_findings": total_findings,
        "total_key_values": total_kv,
        "short_summaries": summaries_short,
    }


def _build_pro_fallback_md(
    pro_m: RunMetrics,
    flash_on_sample: dict,
    flash_full: RunMetrics,
    sample_ids: list[str],
    sample_blocks: list[dict],
    sample_size: int,
) -> str:
    f_findings = flash_on_sample["total_findings"]
    f_kv = flash_on_sample["total_key_values"]
    p_findings = pro_m.total_findings
    p_kv = pro_m.total_key_values

    added_findings = p_findings - f_findings
    added_kv = p_kv - f_kv
    pct_findings = (added_findings / f_findings * 100) if f_findings else float("inf")

    # Improved_blocks heuristic: Pro found findings where Flash had 0, OR Pro filled empty KV.
    # Simplified: approx via Pro overall
    improved_blocks_proxy = 0
    # (exact per-block diff requires iterating parsed data again; using proxy via deltas)

    cost_per_added_finding = (pro_m.total_cost_usd / added_findings) if added_findings > 0 else None
    extra_cost_per_block = pro_m.total_cost_usd / max(1, sample_size)

    recommend = _selective_pro_recommendation(
        pro_m=pro_m,
        flash_on_sample=flash_on_sample,
        added_findings=added_findings,
        added_kv=added_kv,
        sample_size=sample_size,
    )

    lines = [
        "# Phase E — Pro Selective Fallback ROI\n",
        f"Sample: {sample_size} blocks (weakest from Flash full-doc run)\n",
        "## Pro vs Flash on SAME blocks\n",
        "| Metric | Flash (from full run) | Pro (this phase) | Delta |",
        "|--------|-----------------------|------------------|-------|",
        f"| Returned | {flash_on_sample['returned']}/{sample_size} | {pro_m.total_input_blocks - pro_m.missing_count}/{sample_size} | — |",
        f"| Unreadable | {flash_on_sample['unreadable']} | {pro_m.unreadable_count} | {pro_m.unreadable_count - flash_on_sample['unreadable']} |",
        f"| Empty KV | {flash_on_sample['empty_key_values']} | {pro_m.empty_key_values_count} | {pro_m.empty_key_values_count - flash_on_sample['empty_key_values']} |",
        f"| Zero findings | {flash_on_sample['zero_findings']} | {sample_size - pro_m.blocks_with_findings} | — |",
        f"| Total findings | {f_findings} | {p_findings} | **{added_findings:+d}** |",
        f"| Total KV | {f_kv} | {p_kv} | **{added_kv:+d}** |",
        "",
        "## Cost\n",
        f"- Pro total cost: **${pro_m.total_cost_usd:.4f}**",
        f"- Pro cost per sample block: **${extra_cost_per_block:.5f}**",
        f"- Pro cost per ADDED finding: **${cost_per_added_finding:.5f}**" if cost_per_added_finding else "- Pro cost per ADDED finding: N/A (no added findings)",
        "",
        "## Recommendation\n",
        recommend,
        "",
    ]
    return "\n".join(lines) + "\n"


def _selective_pro_recommendation(
    pro_m: RunMetrics,
    flash_on_sample: dict,
    added_findings: int,
    added_kv: int,
    sample_size: int,
) -> str:
    reasons: list[str] = []
    # completeness
    if pro_m.missing_count > 0 or pro_m.duplicate_count > 0 or pro_m.extra_count > 0:
        reasons.append(
            f"- NOT RECOMMENDED (Pro itself had completeness issues: "
            f"missing={pro_m.missing_count}, dup={pro_m.duplicate_count}, extra={pro_m.extra_count})"
        )
        return "\n".join(reasons)

    # Heuristic: additional findings ratio
    base_findings = flash_on_sample["total_findings"] or 1
    delta_ratio = added_findings / base_findings

    improved_proxy = 0
    # Gains in KV recovery or findings vs Flash on these same blocks
    if added_findings > 0:
        improved_proxy += min(added_findings, sample_size)
    if (flash_on_sample["empty_key_values"] > 0
            and pro_m.empty_key_values_count < flash_on_sample["empty_key_values"]):
        improved_proxy += flash_on_sample["empty_key_values"] - pro_m.empty_key_values_count

    if improved_proxy >= 3 and (delta_ratio >= 0.20 or pro_m.empty_key_values_count < flash_on_sample["empty_key_values"]):
        return (
            f"- **RECOMMENDED** selective Pro escalation for weak-heuristic blocks.\n"
            f"- Improved-block proxy: {improved_proxy} of {sample_size}.\n"
            f"- Additional findings: {added_findings:+d} ({delta_ratio:.1%} vs Flash on same blocks).\n"
            f"- Extra cost per improved block: ~${pro_m.total_cost_usd / max(1, improved_proxy):.5f}.\n"
        )
    return (
        f"- NOT RECOMMENDED for stage 02 mainline escalation.\n"
        f"- Improved-block proxy: {improved_proxy} of {sample_size} (threshold >=3 not met or ratio weak).\n"
        f"- Additional findings: {added_findings:+d} ({delta_ratio:.1%}).\n"
    )


# ───────────────────────── CLI ────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Budget-aware OpenRouter stage 02 experiment (Flash-first)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--pdf", required=True)
    p.add_argument("--project-dir", help="Override project directory")
    p.add_argument("--subset-file", help="Override subset file (must exist)")
    p.add_argument("--reuse-phase-a", action="store_true",
                   help="Reuse subset from a prior Phase A run (default true if subset auto-found)")
    p.add_argument("--skip-model-phase", action="store_true",
                   help="(Always on in this script — Phase A is NOT rerun)")
    p.add_argument("--budget-cap-usd", type=float, default=6.0)
    p.add_argument("--batch-profiles", default="b8,b10,b12",
                   help="Comma-separated profile list for Phase B-lite")
    p.add_argument("--parallelism-values", default="2,3",
                   help="Comma-separated parallelism list for Phase C-lite")
    p.add_argument("--no-pro-full", action="store_true",
                   help="(Always on — Pro full-document run is never executed in this script)")
    p.add_argument("--fallback-sample-size", type=int, default=15)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--byte-cap-kb", type=int, default=None)
    return p.parse_args()


async def main_async(args: argparse.Namespace) -> None:
    from webapp.config import (
        OPENROUTER_API_KEY,
        OPENROUTER_STAGE02_RAW_BYTE_CAP_KB,
    )
    from webapp.services.project_service import resolve_project_by_pdf, ProjectByPdfError

    # Resolve project
    if args.project_dir:
        project_dir = Path(args.project_dir)
        project_id = project_dir.name
    else:
        try:
            project_id, project_dir = resolve_project_by_pdf(args.pdf)
        except ProjectByPdfError as e:
            logger.error("Cannot resolve project: %s", e)
            sys.exit(1)

    project_info_path = project_dir / "project_info.json"
    if not project_info_path.exists():
        logger.error("project_info.json not found: %s", project_info_path)
        sys.exit(1)
    project_info = json.loads(project_info_path.read_text(encoding="utf-8"))

    if not OPENROUTER_API_KEY and not args.dry_run:
        logger.error("OPENROUTER_API_KEY not set; use --dry-run for validation")
        sys.exit(1)

    # Blocks
    all_blocks = load_blocks_index(project_dir)

    # Subset: MUST reuse (per spec)
    if args.subset_file:
        subset_file = Path(args.subset_file)
    else:
        subset_file = _find_phase_a_subset(project_dir)
    if not subset_file or not subset_file.exists():
        logger.error("No existing subset found. Per spec, Phase A is NOT rerun; subset MUST exist.")
        sys.exit(1)

    subset_ids, subset_blocks = _load_subset(subset_file, all_blocks)
    logger.info("Reusing subset: %s (%d blocks)", subset_file, len(subset_ids))

    # Experiment dir
    ts = _ts()
    exp_dir = project_dir / "_experiments" / "gemini_openrouter_stage02_budget" / ts
    exp_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Experiment dir: %s", exp_dir)

    byte_cap_kb = args.byte_cap_kb or OPENROUTER_STAGE02_RAW_BYTE_CAP_KB

    profiles = [p.strip() for p in args.batch_profiles.split(",") if p.strip() in BATCH_PROFILES]
    if not profiles:
        logger.error("No valid profiles in --batch-profiles")
        sys.exit(1)
    para_values = sorted({int(x) for x in args.parallelism_values.split(",") if x.strip()})
    if not para_values:
        logger.error("No valid parallelism values in --parallelism-values")
        sys.exit(1)

    budget = BudgetTracker(cap_usd=args.budget_cap_usd)

    manifest = {
        "timestamp": ts,
        "pdf": args.pdf,
        "project_id": project_id,
        "project_dir": str(project_dir),
        "total_blocks": len(all_blocks),
        "subset_file": str(subset_file),
        "subset_size": len(subset_ids),
        "budget_cap_usd": args.budget_cap_usd,
        "batch_profiles": profiles,
        "parallelism_values": para_values,
        "fallback_sample_size": args.fallback_sample_size,
        "byte_cap_kb": byte_cap_kb,
        "no_pro_full": True,
        "skip_model_phase": True,
        "dry_run": args.dry_run,
        "models": {"mainline_under_test": MODEL_FLASH, "fallback_reference": MODEL_PRO},
    }
    _save_json(exp_dir / "manifest.json", manifest)

    # Upfront budget plan
    plan_md = _build_budget_plan_md(
        profiles=profiles, para_values=para_values,
        subset_size=len(subset_ids),
        full_size=len(all_blocks),
        fallback_size=args.fallback_sample_size,
        cap_usd=args.budget_cap_usd,
    )
    (exp_dir / "budget_plan.md").write_text(plan_md, encoding="utf-8")

    logger.info("Budget cap: $%.2f  |  profiles=%s  |  parallelism=%s",
                args.budget_cap_usd, profiles, para_values)

    # ── Phase B-lite ──
    b_metrics, winner_profile, runner_up_profile = await run_phase_b_lite(
        subset_blocks, project_dir, project_info, exp_dir,
        profiles, parallelism=3, byte_cap_kb=byte_cap_kb,
        budget=budget, dry_run=args.dry_run,
    )
    _save_json(exp_dir / "budget_usage_timeline.json", budget.to_dict())
    if not winner_profile:
        _emit_budget_stop(exp_dir, budget, "No B-lite run completed")
        _write_interim_winner(exp_dir, budget=budget, reason="B-lite failed")
        return

    # ── Phase C-lite ──
    c_metrics, winner_parallelism, winner_profile_c = await run_phase_c_lite(
        subset_blocks, project_dir, project_info, exp_dir,
        [winner_profile, runner_up_profile],
        parallelism_options=para_values,
        byte_cap_kb=byte_cap_kb,
        budget=budget, dry_run=args.dry_run,
    )
    _save_json(exp_dir / "budget_usage_timeline.json", budget.to_dict())
    final_profile = winner_profile_c or winner_profile
    final_parallelism = winner_parallelism or 3

    if not c_metrics:
        _emit_budget_stop(exp_dir, budget, "No C-lite run completed")
        _write_interim_winner(
            exp_dir, budget=budget,
            reason="C-lite not completed",
            winner_profile=final_profile,
            winner_parallelism=final_parallelism,
        )
        return

    # ── Phase D ──
    flash_full_m, flash_full_results = await run_phase_d(
        all_blocks, project_dir, project_info, exp_dir,
        final_profile, final_parallelism, byte_cap_kb=byte_cap_kb,
        budget=budget, dry_run=args.dry_run,
    )
    _save_json(exp_dir / "budget_usage_timeline.json", budget.to_dict())
    if flash_full_m is None:
        _emit_budget_stop(exp_dir, budget, "Phase D skipped (budget)")
        _write_interim_winner(
            exp_dir, budget=budget,
            reason="Phase D skipped (budget)",
            winner_profile=final_profile,
            winner_parallelism=final_parallelism,
        )
        return

    # ── Phase E ──
    pro_m: RunMetrics | None = None
    if not args.no_pro_full:  # effectively: spec disallows full-pro; E stays cheap
        pass
    pro_m = await run_phase_e_pro_fallback(
        all_blocks, flash_full_results, flash_full_m,
        project_dir, project_info, exp_dir,
        sample_size=args.fallback_sample_size,
        budget=budget, dry_run=args.dry_run,
    )
    _save_json(exp_dir / "budget_usage_timeline.json", budget.to_dict())

    # ── Final recommendation ──
    rec = _build_winner_recommendation(
        final_profile=final_profile,
        final_parallelism=final_parallelism,
        flash_full_m=flash_full_m,
        pro_m=pro_m,
        budget=budget,
        exp_dir=exp_dir,
    )
    (exp_dir / "winner_recommendation.md").write_text(rec, encoding="utf-8")
    if budget.stopped:
        _emit_budget_stop(exp_dir, budget, "Finished with budget_stop")

    _emit_budget_timeline_md(exp_dir, budget)

    print("\n" + "=" * 70)
    print("BUDGET EXPERIMENT COMPLETE")
    print(f"Artifacts: {exp_dir}")
    print(f"Spent: ${budget.spent_usd:.4f} of ${budget.cap_usd:.2f} (remaining ${budget.remaining:.4f})")
    print("=" * 70)
    print(rec[:1600])


def _build_budget_plan_md(profiles, para_values, subset_size, full_size, fallback_size, cap_usd) -> str:
    b_est = len(profiles) * subset_size * per_block_est(MODEL_FLASH, "batch")
    c_est = len(profiles) * len(para_values) * subset_size * per_block_est(MODEL_FLASH, "batch")
    d_est = full_size * per_block_est(MODEL_FLASH, "batch")
    e_est = fallback_size * per_block_est(MODEL_PRO, "single")
    total_est = b_est + c_est + d_est + e_est
    return (
        f"# Budget Plan\n\n"
        f"Hard cap: **${cap_usd:.2f}**\n\n"
        f"| Phase | Estimated cost (USD) |\n|-------|----------------------|\n"
        f"| B-lite ({len(profiles)} profiles × {subset_size}) | ${b_est:.4f} |\n"
        f"| C-lite ({len(profiles)}×{len(para_values)} × {subset_size}) | ${c_est:.4f} |\n"
        f"| D full Flash ({full_size} blocks) | ${d_est:.4f} |\n"
        f"| E Pro fallback ({fallback_size}) | ${e_est:.4f} |\n"
        f"| **Total estimated** | **${total_est:.4f}** |\n"
    )


def _write_interim_winner(exp_dir: Path, budget: BudgetTracker, reason: str,
                          winner_profile: str = "", winner_parallelism: int = 0) -> None:
    md = (
        f"# Winner Recommendation (interim)\n\n"
        f"**Reason**: {reason}\n\n"
        f"| Field | Value |\n|-------|-------|\n"
        f"| Mainline | google/gemini-2.5-flash (provisional) |\n"
        f"| Batch profile | {winner_profile or 'UNKNOWN'} |\n"
        f"| Parallelism | {winner_parallelism or 'UNKNOWN'} |\n"
        f"| Selective Pro fallback | NOT tested |\n"
        f"| Spent | ${budget.spent_usd:.4f} / ${budget.cap_usd:.2f} |\n"
    )
    (exp_dir / "winner_recommendation.md").write_text(md, encoding="utf-8")


def _emit_budget_stop(exp_dir: Path, budget: BudgetTracker, reason: str) -> None:
    md = (
        f"# Budget Stop\n\n"
        f"**Reason**: {reason}\n\n"
        f"Spent ${budget.spent_usd:.4f} / cap ${budget.cap_usd:.2f} (remaining ${budget.remaining:.4f}).\n"
        f"Stopped: {budget.stopped}\n"
        f"Stop reason: {budget.stop_reason or '—'}\n"
    )
    (exp_dir / "budget_stop_reason.md").write_text(md, encoding="utf-8")


def _emit_budget_timeline_md(exp_dir: Path, budget: BudgetTracker) -> None:
    lines = [
        "# Budget Usage Timeline\n",
        f"Cap: **${budget.cap_usd:.2f}** | Spent: **${budget.spent_usd:.4f}** | Remaining: **${budget.remaining:.4f}**\n",
        f"Stopped: {budget.stopped} | Reason: {budget.stop_reason or '—'}\n",
        "",
        "| # | Phase | Run | Blocks | Batches | Estimated | Approved | Actual | Remaining after | Note |",
        "|---|-------|-----|--------|---------|-----------|----------|--------|------------------|------|",
    ]
    for i, e in enumerate(budget.events, 1):
        lines.append(
            f"| {i} | {e.phase} | {e.run_id} | {e.planned_blocks} | {e.planned_batches} "
            f"| ${e.estimated_usd:.4f} | {'yes' if e.approved else 'no'} "
            f"| ${e.actual_usd:.4f} | ${e.remaining_after:.4f} | {e.note} |"
        )
    (exp_dir / "budget_usage_timeline.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_winner_recommendation(
    *, final_profile: str, final_parallelism: int,
    flash_full_m: RunMetrics, pro_m: RunMetrics | None,
    budget: BudgetTracker, exp_dir: Path,
) -> str:
    pro_recommend = "NOT tested"
    pro_verdict = "N/A"
    if pro_m is not None:
        pro_recommend = (
            f"Pro on {pro_m.total_input_blocks} weakest Flash blocks: "
            f"+{pro_m.total_findings} findings, +{pro_m.total_key_values} KV, "
            f"${pro_m.total_cost_usd:.4f} spend. See `pro_fallback_sample.md` for ROI."
        )
        # Verdict is written inside pro_fallback_sample.md; here we just flag present/absent
        pro_verdict = "See pro_fallback_sample.md for detailed ROI heuristic"

    return (
        f"# Winner Recommendation (budget experiment — OpenRouter)\n\n"
        f"## Practical stage 02 answer\n\n"
        f"| Question | Answer |\n|----------|--------|\n"
        f"| Mainline model | **google/gemini-2.5-flash** (via OpenRouter) |\n"
        f"| Batch profile | **{final_profile}** |\n"
        f"| Parallelism | **{final_parallelism}** |\n"
        f"| Selective Pro escalation | {pro_verdict} |\n"
        f"| Total spent | **${budget.spent_usd:.4f}** of cap ${budget.cap_usd:.2f} |\n\n"
        f"## Why Flash (not Pro) as mainline\n\n"
        f"- Phase A (subset 60 blocks, single-block): Flash **100% coverage**, Pro **98.3%** (1 miss).\n"
        f"- Flash median KV = 19 vs Pro 12; total KV 1869 vs 806 — Flash extracts MORE raw facts.\n"
        f"- Pro found more *findings* (92 vs 38) but at ~25× cost; with miss-rate >0 не может быть\n"
        f"  безусловным mainline для массового контура.\n"
        f"- Selective Pro escalation on weak Flash blocks — правильная стратегия, а не full Pro run.\n\n"
        f"## Phase D (Flash full-doc) summary\n\n"
        f"- Coverage: {flash_full_m.coverage_pct:.1f}% | missing={flash_full_m.missing_count}\n"
        f"- Batches: {flash_full_m.total_batches} (avg size {flash_full_m.avg_batch_size:.1f})\n"
        f"- Findings: {flash_full_m.total_findings} | KV total: {flash_full_m.total_key_values}\n"
        f"- Cost: **${flash_full_m.total_cost_usd:.4f}** (source={flash_full_m.cost_sources_actual}/{flash_full_m.total_batches} actual)\n"
        f"- Cost/valid block: **${flash_full_m.cost_per_valid_block:.5f}**\n"
        f"- Elapsed: {flash_full_m.elapsed_s:.1f}s\n\n"
        f"## Pro fallback (Phase E)\n\n"
        f"{pro_recommend}\n\n"
        f"## Constraints honored\n"
        f"- Pro full-document run not executed (per spec).\n"
        f"- Phase A not rerun (reused subset + metrics).\n"
        f"- Production defaults (`stage_models.json` block_batch) **UNCHANGED**.\n"
        f"- Claude CLI path untouched.\n"
        f"- Actual `usage.cost` preferred over estimate.\n"
    )


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
