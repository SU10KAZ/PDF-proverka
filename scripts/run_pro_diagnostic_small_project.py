"""
Stage 02 narrow diagnostic for google/gemini-3.1-pro-preview.

Purpose:
    The recall-hybrid experiment (20260422_073520) showed Pro losing the
    second-pass on the small KJ project (13АВ-РД-КЖ5.1-К1К2 (2).pdf):
      - coverage 88.2% (2 missing blocks)
      - degraded 6 / improved 10
      - cost ~2x Claude, slower

    This diagnostic isolates THREE knobs (one at a time) on the same
    escalation set to decide whether Pro is genuinely weaker or just
    misconfigured for second-pass:

      Variant 1 (control): reuse baseline Pro from recall-hybrid run.
      Variant 2: Pro + reasoning.effort = "low" (healing ON, parallelism=2)
      Variant 3: Pro + reasoning.effort = "low" (healing OFF, parallelism=2)
      Variant 4 (conditional, only if missing>0 still): Pro + low + parallelism=1
                with the healing setting that gave best completeness.

Locked axes (do NOT vary):
    - OpenRouter path
    - single-block mode (1 block per request)
    - same escalation set (17 blocks)
    - same block PNG sources (_output/blocks/)
    - strict_schema = True (block_batch.openrouter.json)
    - provider.require_parameters = True
    - same project, same PDF, no recrop
    - no full-doc, no batch-mode, no Flash/Claude touched

Empty-response retry rule (single-block mode only):
    If parsed_data.block_analyses == [] for a single-block input:
      1. mark `empty_response_detected = true`
      2. retry once with the SAME params except response_healing = False
      3. if retry succeeds → use retry result
      4. if retry empty → record as `model_failure`
    Counts logged: empty_response_count, empty_response_after_retry_count,
                   empty_response_block_ids (and post-retry survivor list).

Outputs (all under
    <project_dir>/_experiments/pro_diagnostic_small_project/<timestamp>/):
    - manifest.json
    - control_pro_summary.json (copied from baseline)
    - variant_<id>_summary.json
    - variant_<id>_per_block.json
    - variant_<id>_diff_vs_flash.json
    - empty_response_log.json
    - comparison_table.md
    - recommendation.md
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import logging
import shutil
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from run_gemini_openrouter_stage02_experiment import (  # noqa: E402
    build_messages,
    build_page_contexts,
    build_system_prompt,
)
from webapp.services.llm_runner import run_llm  # noqa: E402
from webapp.services.openrouter_block_batch import (  # noqa: E402
    load_openrouter_block_batch_schema,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pro_diagnostic")


# ── Constants ────────────────────────────────────────────────────────────────

PROJECT_REL = "projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.1-К1К2 (2).pdf"
BASELINE_RUN = "20260422_073520"
BASELINE_REL = (
    f"_experiments/stage02_recall_hybrid_small_project/{BASELINE_RUN}"
)
MODEL_PRO = "google/gemini-3.1-pro-preview"
PER_BLOCK_TIMEOUT_SEC = 600
MAX_OUTPUT_TOKENS = 16000


# ── Helpers ──────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                    encoding="utf-8")


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(len(s) * pct / 100)
    return s[min(idx, len(s) - 1)]


# ── Data structures ──────────────────────────────────────────────────────────

@dataclass
class BlockResult:
    block_id: str
    page: int
    duration_ms: int = 0
    is_error: bool = False
    error_message: str = ""
    parsed_block: dict | None = None
    prompt_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    cached_tokens: int = 0
    cost_usd: float = 0.0
    cost_source: str = "estimated"
    finish_reason: str = ""
    response_id: str = ""
    healed_used: bool = False
    # Strict success criteria (single-block):
    #   success iff len(block_analyses)==1 AND analysis.block_id == input bid.
    success: bool = False
    failure_mode: str = ""           # "" if success; else one of the categories below
    analysis_count: int = 0          # primary call's block_analyses length (or 0 if absent)
    analysis_count_retry: int = 0    # retry call's block_analyses length (-1 if no retry)
    analysis_count_final: int = 0    # length used for final classification
    multiple_analyses_in_single_block: bool = False
    wrong_block_id_count: int = 0    # 0 or 1 — analysis returned a non-matching block_id
    returned_block_id_primary: str = ""  # block_id from primary first analysis (may be "")
    returned_block_id_retry: str = ""    # block_id from retry first analysis ("" if no retry)
    empty_response_detected: bool = False
    empty_response_after_retry: bool = False  # primary empty AND retry also empty
    retry_attempted: bool = False
    retry_recovered: bool = False
    model_failure: bool = False  # legacy alias of empty_response_after_retry
    healing_effective: bool = True   # was healing ON for the call that produced final result?
    # Raw artifacts (used by probe; left empty by main variants for compactness)
    raw_text_primary: str = ""
    raw_text_retry: str = ""
    primary_finish_reason: str = ""
    retry_finish_reason: str = ""
    primary_response_id: str = ""
    retry_response_id: str = ""


@dataclass
class VariantConfig:
    variant_id: str
    label: str
    thinking_low: bool
    response_healing: bool
    parallelism: int


@dataclass
class VariantMetrics:
    variant_id: str
    label: str
    thinking_low: bool
    response_healing_initial: bool
    parallelism: int

    total_blocks: int = 0
    coverage_pct: float = 0.0
    missing_count: int = 0
    duplicate_count: int = 0
    extra_count: int = 0
    missing_block_ids: list[str] = field(default_factory=list)

    blocks_with_findings: int = 0
    total_findings: int = 0
    total_key_values: int = 0
    median_key_values: float = 0.0
    empty_summary_count: int = 0
    empty_kv_count: int = 0

    improved_blocks: list[str] = field(default_factory=list)
    unchanged_blocks: list[str] = field(default_factory=list)
    degraded_blocks: list[str] = field(default_factory=list)
    additional_findings_vs_flash: int = 0

    elapsed_s: float = 0.0
    avg_block_duration_s: float = 0.0
    median_block_duration_s: float = 0.0
    p95_block_duration_s: float = 0.0

    total_prompt_tokens: int = 0
    total_output_tokens: int = 0
    total_reasoning_tokens: int = 0
    total_cached_tokens: int = 0

    total_cost_usd: float = 0.0
    cost_per_valid_block: float = 0.0
    cost_sources_actual: int = 0
    cost_sources_estimated: int = 0

    healed_responses_count: int = 0  # blocks where healing layer was active for final result
    empty_response_count: int = 0
    empty_response_after_retry_count: int = 0
    empty_response_block_ids: list[str] = field(default_factory=list)
    empty_after_retry_block_ids: list[str] = field(default_factory=list)
    # Strict-success aggregate metrics (added per refinement task)
    success_count: int = 0
    multiple_analyses_count: int = 0
    multiple_analyses_block_ids: list[str] = field(default_factory=list)
    wrong_block_id_count: int = 0
    wrong_block_id_block_ids: list[str] = field(default_factory=list)
    failure_mode_distribution: dict = field(default_factory=dict)
    analysis_count_distribution: dict = field(default_factory=dict)


# ── Empty-response-aware single-block runner ────────────────────────────────

def _classify_response(
    parsed_data: dict | None,
    input_bid: str,
) -> tuple[str, int, str, bool, int, dict | None]:
    """Classify a single-block response per strict criteria.

    Returns:
        (status, analysis_count, returned_first_block_id,
         multiple_flag, wrong_block_id_count, parsed_block_or_None)

    status ∈ {"success", "empty_array", "multiple_analyses", "wrong_block_id",
              "no_block_analyses_field", "non_dict_analysis"}.

    Success criteria (strict):
      - parsed_data is a dict with key "block_analyses"
      - len(block_analyses) == 1
      - block_analyses[0] is a dict
      - block_analyses[0].block_id == input_bid (or empty/None → infer + treat as success)
    """
    if not isinstance(parsed_data, dict):
        return ("no_block_analyses_field", 0, "", False, 0, None)

    analyses = parsed_data.get("block_analyses")
    if not isinstance(analyses, list):
        return ("no_block_analyses_field", 0, "", False, 0, None)

    n = len(analyses)
    if n == 0:
        return ("empty_array", 0, "", False, 0, None)

    first = analyses[0] if isinstance(analyses[0], dict) else None
    first_bid = (first.get("block_id") if first else "") or ""

    if n > 1:
        return ("multiple_analyses", n, first_bid, True, 0, None)

    # n == 1
    if not isinstance(first, dict):
        return ("non_dict_analysis", 1, "", False, 0, None)

    if not first_bid:
        # Empty/missing → infer from input (still strict-success)
        first["block_id"] = input_bid
        return ("success", 1, first_bid, False, 0, first)

    if first_bid != input_bid:
        return ("wrong_block_id", 1, first_bid, False, 1, None)

    return ("success", 1, first_bid, False, 0, first)


async def run_one_block(
    *,
    block: dict,
    project_dir: Path,
    project_info: dict,
    system_prompt: str,
    page_contexts: dict[int, str],
    model: str,
    thinking_low: bool = False,
    reasoning_effort: str | None = None,  # "low" | "medium" | "high" | None
    response_healing_initial: bool,
    schema: dict,
    timeout: int,
    max_output_tokens: int,
    capture_raw: bool = False,
) -> BlockResult:
    """Run ONE block as a 1-block batch with strict success + empty→retry rule.

    Strict success (single-block mode):
      len(block_analyses) == 1 AND analysis.block_id == input bid (empty bid OK).
    Anything else (empty list, >1 analyses, wrong block_id, missing field) → not success.

    Retry rule:
      Triggered ONLY by status == "empty_array" (or "no_block_analyses_field").
      Always performed once, even if response_healing_initial=False, with
      response_healing=False (otherwise identical params).
    """
    bid = block["block_id"]
    page = int(block.get("page", 0))

    messages = build_messages(
        [block], project_dir, project_info,
        batch_id=1, total_batches=1,
        system_prompt=system_prompt, page_contexts=page_contexts,
    )

    # Resolve reasoning override (explicit effort wins over thinking_low shortcut)
    extra_body: dict = {}
    effort = reasoning_effort
    if effort is None and thinking_low:
        effort = "low"
    if effort is not None:
        extra_body["reasoning"] = {"effort": effort}

    # ── Primary attempt ───────────────────────────────────────────────────
    primary = await run_llm(
        stage="block_batch",
        messages=messages,
        model_override=model,
        temperature=0.2,
        timeout=timeout,
        max_retries=3,
        strict_schema=schema,
        schema_name="block_batch",
        response_healing=response_healing_initial,
        require_parameters=True,
        max_tokens_override=max_output_tokens,
        extra_body=extra_body if extra_body else None,
    )

    res = BlockResult(
        block_id=bid,
        page=page,
        duration_ms=primary.duration_ms,
        is_error=primary.is_error,
        error_message=primary.error_message or "",
        prompt_tokens=primary.input_tokens,
        output_tokens=primary.output_tokens,
        reasoning_tokens=primary.reasoning_tokens,
        cached_tokens=primary.cached_tokens,
        cost_usd=primary.cost_usd,
        cost_source=primary.cost_source,
        finish_reason=primary.finish_reason,
        primary_finish_reason=primary.finish_reason,
        response_id=primary.response_id,
        primary_response_id=primary.response_id,
        healed_used=response_healing_initial,
        healing_effective=response_healing_initial,
        analysis_count_retry=-1,
    )
    if capture_raw:
        res.raw_text_primary = primary.text or ""

    if primary.is_error:
        res.failure_mode = "api_error"
        return res

    p_status, p_n, p_first_bid, p_multi, p_wrong, p_block = _classify_response(
        primary.json_data if isinstance(primary.json_data, dict) else None,
        bid,
    )
    res.analysis_count = p_n
    res.analysis_count_final = p_n
    res.returned_block_id_primary = p_first_bid
    res.multiple_analyses_in_single_block = p_multi
    res.wrong_block_id_count = p_wrong

    # Retry trigger: ONLY for empty array / missing field. NOT for wrong_id / multi.
    EMPTY_STATES = {"empty_array", "no_block_analyses_field"}
    if p_status not in EMPTY_STATES:
        if p_status == "success":
            res.success = True
            res.failure_mode = ""
            res.parsed_block = p_block
        else:
            res.failure_mode = p_status  # multiple_analyses | wrong_block_id | non_dict_analysis
        return res

    # ── Empty response detected → ALWAYS retry once with healing=False ────
    res.empty_response_detected = True
    res.retry_attempted = True
    retry = await run_llm(
        stage="block_batch",
        messages=messages,                    # same messages
        model_override=model,                 # same model
        temperature=0.2,
        timeout=timeout,
        max_retries=3,
        strict_schema=schema,                 # same strict schema
        schema_name="block_batch",
        response_healing=False,               # ← only thing that changes
        require_parameters=True,
        max_tokens_override=max_output_tokens,
        extra_body=extra_body if extra_body else None,  # same reasoning override
    )

    # Accumulate retry cost/tokens/time regardless of outcome
    res.duration_ms += retry.duration_ms
    res.prompt_tokens += retry.input_tokens
    res.output_tokens += retry.output_tokens
    res.reasoning_tokens += retry.reasoning_tokens
    res.cached_tokens += retry.cached_tokens
    res.cost_usd += retry.cost_usd
    if retry.cost_source == "actual":
        res.cost_source = "actual"
    res.retry_finish_reason = retry.finish_reason or ""
    res.retry_response_id = retry.response_id or ""
    if capture_raw:
        res.raw_text_retry = retry.text or ""

    if retry.is_error:
        res.empty_response_after_retry = True
        res.model_failure = True
        res.failure_mode = "empty_then_api_error"
        res.analysis_count_retry = 0
        return res

    r_status, r_n, r_first_bid, r_multi, r_wrong, r_block = _classify_response(
        retry.json_data if isinstance(retry.json_data, dict) else None,
        bid,
    )
    res.analysis_count_retry = r_n
    res.returned_block_id_retry = r_first_bid

    if r_status == "success":
        res.success = True
        res.retry_recovered = True
        res.healing_effective = False  # final result came from no-healing call
        res.failure_mode = ""
        res.parsed_block = r_block
        res.analysis_count_final = r_n
        res.finish_reason = retry.finish_reason or res.finish_reason
        return res

    # Retry produced something non-success
    res.analysis_count_final = r_n
    if r_status in EMPTY_STATES:
        res.empty_response_after_retry = True
        res.model_failure = True
        res.failure_mode = "empty_then_empty"
    else:
        # Primary empty, retry returned but with strict-success failure
        res.model_failure = True
        res.failure_mode = f"empty_then_{r_status}"
        if r_multi:
            res.multiple_analyses_in_single_block = True
        if r_wrong:
            res.wrong_block_id_count = max(res.wrong_block_id_count, 1)
    return res


# ── Variant runner ───────────────────────────────────────────────────────────

async def run_variant(
    *,
    variant: VariantConfig,
    blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    schema: dict,
) -> tuple[VariantMetrics, list[BlockResult]]:
    system_prompt = build_system_prompt(project_info, len(blocks))
    page_contexts = build_page_contexts(project_dir)

    sem = asyncio.Semaphore(variant.parallelism)
    results: list[BlockResult | None] = [None] * len(blocks)

    async def _one(i: int, b: dict) -> None:
        async with sem:
            r = await run_one_block(
                block=b,
                project_dir=project_dir,
                project_info=project_info,
                system_prompt=system_prompt,
                page_contexts=page_contexts,
                model=MODEL_PRO,
                thinking_low=variant.thinking_low,
                response_healing_initial=variant.response_healing,
                schema=schema,
                timeout=PER_BLOCK_TIMEOUT_SEC,
                max_output_tokens=MAX_OUTPUT_TOKENS,
            )
            results[i] = r
            tag = "OK"
            if r.is_error:
                tag = f"ERR({r.error_message[:60]})"
            elif r.model_failure:
                tag = "EMPTY_AFTER_RETRY"
            elif r.empty_response_detected and r.retry_recovered:
                tag = "RECOVERED"
            elif r.parsed_block is None:
                tag = "EMPTY"
            logger.info(
                "[%s] block %02d/%d %s page=%d dur=%.1fs cost=$%.4f(%s) %s",
                variant.variant_id, i + 1, len(blocks), b["block_id"],
                r.page, r.duration_ms / 1000.0, r.cost_usd, r.cost_source, tag,
            )

    start = time.monotonic()
    await asyncio.gather(*(_one(i, b) for i, b in enumerate(blocks)))
    elapsed = time.monotonic() - start

    # Filter out None (shouldn't happen)
    block_results: list[BlockResult] = [r for r in results if r is not None]

    metrics = _aggregate_variant(variant, block_results, elapsed)
    return metrics, block_results


def _aggregate_variant(
    variant: VariantConfig,
    block_results: list[BlockResult],
    elapsed_s: float,
) -> VariantMetrics:
    m = VariantMetrics(
        variant_id=variant.variant_id,
        label=variant.label,
        thinking_low=variant.thinking_low,
        response_healing_initial=variant.response_healing,
        parallelism=variant.parallelism,
        total_blocks=len(block_results),
        elapsed_s=elapsed_s,
    )

    durations: list[float] = []
    kv_counts: list[float] = []

    for r in block_results:
        durations.append(r.duration_ms / 1000.0)
        m.total_prompt_tokens += r.prompt_tokens
        m.total_output_tokens += r.output_tokens
        m.total_reasoning_tokens += r.reasoning_tokens
        m.total_cached_tokens += r.cached_tokens
        m.total_cost_usd += r.cost_usd
        if r.cost_source == "actual":
            m.cost_sources_actual += 1
        else:
            m.cost_sources_estimated += 1

        if r.empty_response_detected:
            m.empty_response_count += 1
            m.empty_response_block_ids.append(r.block_id)
        if r.empty_response_after_retry:
            m.empty_response_after_retry_count += 1
            m.empty_after_retry_block_ids.append(r.block_id)

        # Strict-success aggregates
        if r.success:
            m.success_count += 1
        if r.multiple_analyses_in_single_block:
            m.multiple_analyses_count += 1
            m.multiple_analyses_block_ids.append(r.block_id)
        if r.wrong_block_id_count:
            m.wrong_block_id_count += r.wrong_block_id_count
            m.wrong_block_id_block_ids.append(r.block_id)
        fmode = r.failure_mode or ("success" if r.success else "unknown")
        m.failure_mode_distribution[fmode] = m.failure_mode_distribution.get(fmode, 0) + 1
        # analysis_count_distribution keyed by str (json compat)
        ac = str(r.analysis_count_final)
        m.analysis_count_distribution[ac] = m.analysis_count_distribution.get(ac, 0) + 1

        # Block missing from coverage iff parsed_block is None or has no block_id
        if r.parsed_block is None or not r.parsed_block.get("block_id"):
            m.missing_count += 1
            m.missing_block_ids.append(r.block_id)
            continue

        pb = r.parsed_block
        if r.healing_effective:
            m.healed_responses_count += 1

        s = str(pb.get("summary", "")).strip()
        if not s:
            m.empty_summary_count += 1
        kv = pb.get("key_values_read") or []
        if not kv:
            m.empty_kv_count += 1
        else:
            m.total_key_values += len(kv)
            kv_counts.append(float(len(kv)))
        findings = pb.get("findings") or []
        if findings:
            m.blocks_with_findings += 1
            m.total_findings += len(findings)

    if m.total_blocks:
        m.coverage_pct = round(
            (m.total_blocks - m.missing_count) / m.total_blocks * 100, 2
        )

    if durations:
        m.avg_block_duration_s = round(sum(durations) / len(durations), 2)
        m.median_block_duration_s = round(statistics.median(durations), 2)
        m.p95_block_duration_s = round(_percentile(durations, 95), 2)
    if kv_counts:
        m.median_key_values = round(statistics.median(kv_counts), 2)

    valid = m.total_blocks - m.missing_count
    if valid:
        m.cost_per_valid_block = round(m.total_cost_usd / valid, 6)

    return m


def _diff_vs_flash(
    block_results: list[BlockResult],
    flash_per_block: list[dict],
) -> tuple[dict, list[dict]]:
    """Return (summary, per_block_diff). Mirrors recall-hybrid logic."""
    flash_by_id = {b["block_id"]: b for b in flash_per_block}
    per_block: list[dict] = []
    flash_total = 0
    engine_total = 0
    improved: list[str] = []
    unchanged: list[str] = []
    degraded: list[str] = []
    missing: list[str] = []

    for r in block_results:
        flash = flash_by_id.get(r.block_id, {})
        flash_findings = len(flash.get("findings") or [])
        flash_kv = len(flash.get("key_values_read") or [])
        flash_total += flash_findings

        if r.parsed_block is None:
            missing.append(r.block_id)
            per_block.append({
                "block_id": r.block_id,
                "status": "missing_in_engine",
                "flash_findings": flash_findings,
                "engine_findings": 0,
                "delta_findings": -flash_findings,
                "flash_kv": flash_kv,
                "engine_kv": 0,
                "empty_response_detected": r.empty_response_detected,
                "model_failure": r.model_failure,
            })
            continue

        engine_findings = len(r.parsed_block.get("findings") or [])
        engine_kv = len(r.parsed_block.get("key_values_read") or [])
        engine_total += engine_findings
        delta = engine_findings - flash_findings

        if delta > 0:
            status = "improved"
            improved.append(r.block_id)
        elif delta < 0:
            status = "degraded"
            degraded.append(r.block_id)
        else:
            status = "unchanged"
            unchanged.append(r.block_id)

        per_block.append({
            "block_id": r.block_id,
            "status": status,
            "flash_findings": flash_findings,
            "engine_findings": engine_findings,
            "delta_findings": delta,
            "flash_kv": flash_kv,
            "engine_kv": engine_kv,
            "empty_response_detected": r.empty_response_detected,
            "retry_recovered": r.retry_recovered,
        })

    summary = {
        "engine": "Pro (gemini-3.1-pro-preview)",
        "escalation_set_size": len(block_results),
        "improved": len(improved),
        "unchanged": len(unchanged),
        "degraded": len(degraded),
        "missing_in_engine": len(missing),
        "flash_total_findings_on_set": flash_total,
        "engine_total_findings": engine_total,
        "added_findings_vs_flash": engine_total - flash_total,
        "improved_block_ids": improved,
        "degraded_block_ids": degraded,
        "missing_block_ids": missing,
        "unchanged_block_ids": unchanged,
    }
    return summary, per_block


# ── Control (baseline) loader ────────────────────────────────────────────────

def load_control(baseline_dir: Path) -> dict:
    """Reuse baseline Pro summary + diff as control variant."""
    summary = json.loads(
        (baseline_dir / "second_pass_pro_summary.json").read_text(encoding="utf-8")
    )
    diff = json.loads(
        (baseline_dir / "second_pass_pro_diff.json").read_text(encoding="utf-8")
    )

    control_metrics = {
        "variant_id": "v1_control_baseline",
        "label": "control: baseline Pro (reuse, healing=ON, parallelism=2, no thinking override)",
        "thinking_low": False,
        "response_healing_initial": True,
        "parallelism": int(summary.get("parallelism", 2)),
        "total_blocks": int(summary.get("total_input_blocks", 0)),
        "coverage_pct": float(summary.get("coverage_pct", 0.0)),
        "missing_count": int(summary.get("missing_count", 0)),
        "duplicate_count": int(summary.get("duplicate_count", 0)),
        "extra_count": int(summary.get("extra_count", 0)),
        "missing_block_ids": [
            d["block_id"] for d in diff.get("per_block_diff", [])
            if d.get("status") == "missing_in_engine"
        ],
        "blocks_with_findings": int(summary.get("blocks_with_findings", 0)),
        "total_findings": int(summary.get("total_findings", 0)),
        "total_key_values": int(summary.get("total_key_values", 0)),
        "median_key_values": float(summary.get("median_key_values", 0.0)),
        "empty_summary_count": int(summary.get("empty_summary_count", 0)),
        "empty_kv_count": int(summary.get("empty_key_values_count", 0)),
        "improved_blocks": [
            d["block_id"] for d in diff.get("per_block_diff", [])
            if d.get("status") == "improved"
        ],
        "unchanged_blocks": [
            d["block_id"] for d in diff.get("per_block_diff", [])
            if d.get("status") == "unchanged"
        ],
        "degraded_blocks": [
            d["block_id"] for d in diff.get("per_block_diff", [])
            if d.get("status") == "degraded"
        ],
        "additional_findings_vs_flash": int(diff.get("added_findings", 0)),
        "elapsed_s": float(summary.get("elapsed_s", 0.0)),
        "avg_block_duration_s": float(summary.get("avg_batch_duration_s", 0.0)),
        "median_block_duration_s": float(summary.get("median_batch_duration_s", 0.0)),
        "p95_block_duration_s": float(summary.get("p95_batch_duration_s", 0.0)),
        "total_prompt_tokens": int(summary.get("total_prompt_tokens", 0)),
        "total_output_tokens": int(summary.get("total_output_tokens", 0)),
        "total_reasoning_tokens": int(summary.get("total_reasoning_tokens", 0)),
        "total_cached_tokens": int(summary.get("total_cached_tokens", 0)),
        "total_cost_usd": float(summary.get("total_cost_usd", 0.0)),
        "cost_per_valid_block": float(summary.get("cost_per_valid_block", 0.0)),
        "cost_sources_actual": int(summary.get("cost_sources_actual", 0)),
        "cost_sources_estimated": int(summary.get("cost_sources_estimated", 0)),
        "healed_responses_count": None,  # not tracked in baseline
        "empty_response_count": None,    # not tracked in baseline
        "empty_response_after_retry_count": None,
        "empty_response_block_ids": [],
        "empty_after_retry_block_ids": [],
        "source": "reuse_baseline",
        "baseline_run_id": BASELINE_RUN,
    }
    return control_metrics


# ── Comparison reporting ─────────────────────────────────────────────────────

def _row(d: dict, key: str, fmt: str = "{}") -> str:
    v = d.get(key)
    if v is None:
        return "n/a"
    if isinstance(v, list):
        return str(len(v)) if v else "0"
    try:
        return fmt.format(v)
    except Exception:
        return str(v)


def build_comparison_md(variants: list[dict]) -> str:
    headers = ["Metric"] + [v["variant_id"] for v in variants]
    sep = ["---"] * len(headers)
    rows: list[list[str]] = [headers, sep]

    def add(label: str, fn):
        rows.append([label] + [str(fn(v)) for v in variants])

    add("Label", lambda v: v.get("label", ""))
    add("thinking_low", lambda v: v.get("thinking_low"))
    add("response_healing_initial", lambda v: v.get("response_healing_initial"))
    add("parallelism", lambda v: v.get("parallelism"))
    add("total_blocks", lambda v: v.get("total_blocks", 0))
    add("coverage_pct", lambda v: f"{v.get('coverage_pct', 0):.2f}%")
    add("missing", lambda v: v.get("missing_count", 0))
    add("duplicate", lambda v: v.get("duplicate_count", 0))
    add("extra", lambda v: v.get("extra_count", 0))
    add("missing_block_ids", lambda v: ", ".join(v.get("missing_block_ids", [])) or "—")
    add("improved", lambda v: len(v.get("improved_blocks", [])))
    add("unchanged", lambda v: len(v.get("unchanged_blocks", [])))
    add("degraded", lambda v: len(v.get("degraded_blocks", [])))
    add("degraded_block_ids", lambda v: ", ".join(v.get("degraded_blocks", [])) or "—")
    add("additional findings vs Flash", lambda v: v.get("additional_findings_vs_flash", 0))
    add("blocks_with_findings", lambda v: v.get("blocks_with_findings", 0))
    add("total_findings (engine)", lambda v: v.get("total_findings", 0))
    add("total_key_values (engine)", lambda v: v.get("total_key_values", 0))
    add("median_key_values", lambda v: v.get("median_key_values", 0))
    add("elapsed_s", lambda v: f"{v.get('elapsed_s', 0):.1f}")
    add("avg / median / p95 dur (s)",
        lambda v: f"{v.get('avg_block_duration_s', 0):.1f} / {v.get('median_block_duration_s', 0):.1f} / {v.get('p95_block_duration_s', 0):.1f}")
    add("prompt tok", lambda v: v.get("total_prompt_tokens", 0))
    add("output tok", lambda v: v.get("total_output_tokens", 0))
    add("reasoning tok", lambda v: v.get("total_reasoning_tokens", 0))
    add("cost USD",
        lambda v: f"${v.get('total_cost_usd', 0):.4f}")
    add("cost / valid block",
        lambda v: f"${v.get('cost_per_valid_block', 0):.5f}")
    add("cost source actual/est",
        lambda v: f"{v.get('cost_sources_actual', 0)}/{v.get('cost_sources_estimated', 0)}")
    add("healed responses", lambda v: _row(v, "healed_responses_count"))
    add("empty_response_count", lambda v: _row(v, "empty_response_count"))
    add("empty_response_after_retry_count",
        lambda v: _row(v, "empty_response_after_retry_count"))
    add("empty_response_block_ids",
        lambda v: ", ".join(v.get("empty_response_block_ids") or []) or "—")
    add("empty_after_retry_block_ids",
        lambda v: ", ".join(v.get("empty_after_retry_block_ids") or []) or "—")

    out_lines: list[str] = ["# Pro Diagnostic — variant comparison\n"]
    for r in rows:
        out_lines.append("| " + " | ".join(r) + " |")
    out_lines.append("")
    return "\n".join(out_lines)


def build_recommendation_md(variants: list[dict]) -> str:
    """Answer the 3 diagnostic questions + verdict."""
    by_id = {v["variant_id"]: v for v in variants}
    ctrl = by_id.get("v1_control_baseline")
    v2 = by_id.get("v2_thinking_low_heal_on")
    v3 = by_id.get("v3_thinking_low_heal_off")
    v4 = by_id.get("v4_thinking_low_par1")

    lines: list[str] = ["# Pro Diagnostic — Recommendation\n"]

    lines.append("## Q1. Does `thinking_level=low` fix missing blocks?\n")
    if ctrl and v2:
        lines.append(
            f"- control missing={ctrl.get('missing_count')} "
            f"({', '.join(ctrl.get('missing_block_ids') or []) or '—'})"
        )
        lines.append(
            f"- v2 (low+heal_on) missing={v2.get('missing_count')} "
            f"({', '.join(v2.get('missing_block_ids') or []) or '—'})"
        )
        improved_cov = (v2.get('coverage_pct', 0) - ctrl.get('coverage_pct', 0))
        lines.append(f"- coverage Δ: {improved_cov:+.2f} pp")
        lines.append(
            "- Verdict: "
            + ("YES — `thinking_level=low` recovers coverage."
               if v2.get('coverage_pct', 0) >= 100.0 and v2.get('missing_count', 0) == 0
               else "NO — missing blocks persist with thinking_level=low.")
        )
    lines.append("")

    lines.append("## Q2. Does `response_healing` HURT completeness?\n")
    if v2 and v3:
        lines.append(
            f"- v2 heal_on  missing={v2.get('missing_count')} coverage={v2.get('coverage_pct', 0):.2f}% "
            f"empty_resp={v2.get('empty_response_count', 0)}"
        )
        lines.append(
            f"- v3 heal_off missing={v3.get('missing_count')} coverage={v3.get('coverage_pct', 0):.2f}% "
            f"empty_resp={v3.get('empty_response_count', 0)}"
        )
        if (v3.get('coverage_pct', 0) > v2.get('coverage_pct', 0)
                or v3.get('empty_response_count', 0) < v2.get('empty_response_count', 0)):
            lines.append(
                "- Verdict: YES — turning healing OFF improves coverage / reduces empty responses."
            )
        elif v2.get('coverage_pct', 0) > v3.get('coverage_pct', 0):
            lines.append(
                "- Verdict: NO — healing ON gives BETTER coverage; healing helps."
            )
        else:
            lines.append("- Verdict: NEUTRAL — healing has no measurable impact on completeness.")
    else:
        lines.append("- (insufficient data)")
    lines.append("")

    lines.append("## Q3. Does parallelism=1 help?\n")
    if v4 is None:
        lines.append("- Skipped — Q1/Q2 already gave 100% coverage.")
    else:
        ref = v3 if (v3 and v3.get('coverage_pct', 0) >= v2.get('coverage_pct', 0)) else v2
        ref_id = ref["variant_id"] if ref else "n/a"
        lines.append(
            f"- best-of-Q1/Q2 ({ref_id}) missing={ref.get('missing_count') if ref else 'n/a'} "
            f"coverage={ref.get('coverage_pct', 0):.2f}% elapsed={ref.get('elapsed_s', 0):.1f}s"
        )
        lines.append(
            f"- v4 par=1 missing={v4.get('missing_count')} coverage={v4.get('coverage_pct', 0):.2f}% "
            f"elapsed={v4.get('elapsed_s', 0):.1f}s"
        )
        if v4.get('coverage_pct', 0) > (ref.get('coverage_pct', 0) if ref else 0):
            lines.append("- Verdict: YES — parallelism=1 improves coverage.")
        else:
            lines.append("- Verdict: NO — parallelism reduction did not improve coverage.")
    lines.append("")

    # Best diagnostic config
    lines.append("## Best diagnostic config\n")
    candidates = [v for v in [ctrl, v2, v3, v4] if v]
    candidates.sort(key=lambda x: (
        -x.get("coverage_pct", 0),
        x.get("missing_count", 99),
        x.get("degraded_blocks") and len(x["degraded_blocks"]) or 0,
        -x.get("additional_findings_vs_flash", 0),
        x.get("total_cost_usd", 0),
    ))
    best = candidates[0]
    lines.append(
        f"- **{best['variant_id']}** — {best.get('label', '')}\n"
        f"  - coverage={best.get('coverage_pct', 0):.2f}%, "
        f"missing={best.get('missing_count')}, "
        f"degraded={len(best.get('degraded_blocks') or [])}, "
        f"added findings={best.get('additional_findings_vs_flash')}, "
        f"cost=${best.get('total_cost_usd', 0):.4f}, "
        f"elapsed={best.get('elapsed_s', 0):.1f}s"
    )
    lines.append("")

    # Verdict
    lines.append("## Final verdict\n")
    ctrl_cov = ctrl.get("coverage_pct", 0) if ctrl else 0
    best_cov = best.get("coverage_pct", 0)
    if best is ctrl:
        lines.append("- **Pro реально слабее** — никакая конфигурация не выправила baseline.")
    elif best_cov >= 100.0 and best is not ctrl:
        lines.append(
            "- **Pro был сконфигурирован неудачно** — "
            f"variant `{best['variant_id']}` достигает 100% coverage."
        )
    elif best_cov > ctrl_cov:
        lines.append(
            "- **Pro частично сконфигурирован неудачно** — "
            f"`{best['variant_id']}` улучшает coverage до {best_cov:.1f}%, но не до 100%."
        )
    else:
        lines.append("- **Pro реально слабее** — варианты не превосходят baseline по coverage.")
    lines.append("")

    # Should we re-add Pro to candidates?
    lines.append("## Should we re-add Pro to second-pass candidates?\n")
    if best_cov >= 100.0 and best.get("additional_findings_vs_flash", 0) >= 30:
        lines.append("- **YES** — диагностика нашла рабочую конфигурацию (≥100% coverage, ≥+30 findings).")
    elif best_cov >= 100.0:
        lines.append("- **MAYBE** — coverage достигнут, но added findings ниже Claude (+46). Нужен полный re-run.")
    else:
        lines.append("- **NO** — даже лучшая конфигурация не закрывает completeness gap.")
    lines.append("")

    # Highlight key finding
    if v2 and v2.get("coverage_pct", 0) >= 100.0:
        lines.insert(1, "> **KEY FINDING:** `thinking_level=low` (Variant 2) уже даёт 100% coverage. "
                       "Проблема была в конфигурации, не в самой модели.\n")

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

async def main_async(args: argparse.Namespace) -> int:
    project_dir = Path(args.project_dir).resolve()
    baseline_dir = project_dir / BASELINE_REL
    if not baseline_dir.exists():
        logger.error("Baseline dir missing: %s", baseline_dir)
        return 2

    blocks_index = json.loads(
        (project_dir / "_output" / "blocks" / "index.json").read_text(encoding="utf-8")
    )
    all_blocks_by_id = {b["block_id"]: b for b in blocks_index["blocks"]}

    escalation_ids = json.loads(
        (baseline_dir / "escalation_set_block_ids.json").read_text(encoding="utf-8")
    )
    blocks = [all_blocks_by_id[bid] for bid in escalation_ids if bid in all_blocks_by_id]
    if len(blocks) != len(escalation_ids):
        logger.warning("Some escalation blocks not found in current index.")

    project_info = json.loads((project_dir / "project_info.json").read_text(encoding="utf-8"))
    schema = load_openrouter_block_batch_schema()

    flash_per_block = json.loads(
        (baseline_dir / "flash_full_per_block.json").read_text(encoding="utf-8")
    )

    ts = _ts()
    out_dir = project_dir / "_experiments" / "pro_diagnostic_small_project" / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "timestamp": ts,
        "project_dir": str(project_dir),
        "baseline_run": BASELINE_RUN,
        "model": MODEL_PRO,
        "escalation_set_size": len(blocks),
        "mode": "single_block",
        "strict_schema": True,
        "require_parameters": True,
        "openrouter_path": True,
        "max_output_tokens": MAX_OUTPUT_TOKENS,
        "per_block_timeout_sec": PER_BLOCK_TIMEOUT_SEC,
        "dry_run": args.dry_run,
    }
    _save_json(out_dir / "manifest.json", manifest)

    # ── Variant 1: control (reuse baseline) ──────────────────────────────
    control = load_control(baseline_dir)
    _save_json(out_dir / "v1_control_baseline_summary.json", control)
    # also copy baseline diff for reference
    shutil.copyfile(
        baseline_dir / "second_pass_pro_diff.json",
        out_dir / "v1_control_baseline_diff_vs_flash.json",
    )

    variants_data: list[dict] = [control]
    empty_log: list[dict] = []

    if args.dry_run:
        logger.info("DRY-RUN: skipping API calls. Writing manifest + control only.")
        _save_json(out_dir / "comparison_table.md", "(dry-run, no variants)")
        return 0

    # ── Variant 2 ─────────────────────────────────────────────────────────
    v2_cfg = VariantConfig(
        variant_id="v2_thinking_low_heal_on",
        label="Pro + reasoning.effort=low, healing=ON, parallelism=2",
        thinking_low=True,
        response_healing=True,
        parallelism=2,
    )
    logger.info("=== Running %s ===", v2_cfg.variant_id)
    v2_metrics, v2_blocks = await run_variant(
        variant=v2_cfg, blocks=blocks, project_dir=project_dir,
        project_info=project_info, schema=schema,
    )
    v2_diff_summary, v2_diff_per_block = _diff_vs_flash(v2_blocks, flash_per_block)
    v2_metrics.improved_blocks = v2_diff_summary["improved_block_ids"]
    v2_metrics.unchanged_blocks = v2_diff_summary["unchanged_block_ids"]
    v2_metrics.degraded_blocks = v2_diff_summary["degraded_block_ids"]
    v2_metrics.additional_findings_vs_flash = v2_diff_summary["added_findings_vs_flash"]
    _save_json(out_dir / "v2_summary.json", _vm_to_dict(v2_metrics))
    _save_json(out_dir / "v2_per_block.json",
               [_br_to_dict(r) for r in v2_blocks])
    _save_json(out_dir / "v2_diff_vs_flash.json",
               {"summary": v2_diff_summary, "per_block_diff": v2_diff_per_block})
    variants_data.append(_vm_to_dict(v2_metrics))
    empty_log.append({
        "variant": v2_cfg.variant_id,
        "empty_response_block_ids": v2_metrics.empty_response_block_ids,
        "empty_after_retry_block_ids": v2_metrics.empty_after_retry_block_ids,
    })

    # ── Variant 3 ─────────────────────────────────────────────────────────
    v3_cfg = VariantConfig(
        variant_id="v3_thinking_low_heal_off",
        label="Pro + reasoning.effort=low, healing=OFF, parallelism=2",
        thinking_low=True,
        response_healing=False,
        parallelism=2,
    )
    logger.info("=== Running %s ===", v3_cfg.variant_id)
    v3_metrics, v3_blocks = await run_variant(
        variant=v3_cfg, blocks=blocks, project_dir=project_dir,
        project_info=project_info, schema=schema,
    )
    v3_diff_summary, v3_diff_per_block = _diff_vs_flash(v3_blocks, flash_per_block)
    v3_metrics.improved_blocks = v3_diff_summary["improved_block_ids"]
    v3_metrics.unchanged_blocks = v3_diff_summary["unchanged_block_ids"]
    v3_metrics.degraded_blocks = v3_diff_summary["degraded_block_ids"]
    v3_metrics.additional_findings_vs_flash = v3_diff_summary["added_findings_vs_flash"]
    _save_json(out_dir / "v3_summary.json", _vm_to_dict(v3_metrics))
    _save_json(out_dir / "v3_per_block.json",
               [_br_to_dict(r) for r in v3_blocks])
    _save_json(out_dir / "v3_diff_vs_flash.json",
               {"summary": v3_diff_summary, "per_block_diff": v3_diff_per_block})
    variants_data.append(_vm_to_dict(v3_metrics))
    empty_log.append({
        "variant": v3_cfg.variant_id,
        "empty_response_block_ids": v3_metrics.empty_response_block_ids,
        "empty_after_retry_block_ids": v3_metrics.empty_after_retry_block_ids,
    })

    # ── Variant 4 (conditional) ───────────────────────────────────────────
    needs_v4 = (v2_metrics.missing_count > 0 and v3_metrics.missing_count > 0)
    if needs_v4:
        # Choose healing policy that gave BETTER completeness
        if v3_metrics.coverage_pct >= v2_metrics.coverage_pct:
            best_heal = False
            heal_label = "OFF"
        else:
            best_heal = True
            heal_label = "ON"
        v4_cfg = VariantConfig(
            variant_id="v4_thinking_low_par1",
            label=f"Pro + reasoning.effort=low, healing={heal_label}, parallelism=1",
            thinking_low=True,
            response_healing=best_heal,
            parallelism=1,
        )
        logger.info("=== Running %s ===", v4_cfg.variant_id)
        v4_metrics, v4_blocks = await run_variant(
            variant=v4_cfg, blocks=blocks, project_dir=project_dir,
            project_info=project_info, schema=schema,
        )
        v4_diff_summary, v4_diff_per_block = _diff_vs_flash(v4_blocks, flash_per_block)
        v4_metrics.improved_blocks = v4_diff_summary["improved_block_ids"]
        v4_metrics.unchanged_blocks = v4_diff_summary["unchanged_block_ids"]
        v4_metrics.degraded_blocks = v4_diff_summary["degraded_block_ids"]
        v4_metrics.additional_findings_vs_flash = v4_diff_summary["added_findings_vs_flash"]
        _save_json(out_dir / "v4_summary.json", _vm_to_dict(v4_metrics))
        _save_json(out_dir / "v4_per_block.json",
                   [_br_to_dict(r) for r in v4_blocks])
        _save_json(out_dir / "v4_diff_vs_flash.json",
                   {"summary": v4_diff_summary, "per_block_diff": v4_diff_per_block})
        variants_data.append(_vm_to_dict(v4_metrics))
        empty_log.append({
            "variant": v4_cfg.variant_id,
            "empty_response_block_ids": v4_metrics.empty_response_block_ids,
            "empty_after_retry_block_ids": v4_metrics.empty_after_retry_block_ids,
        })
    else:
        logger.info("Skipping V4: V2 or V3 already reached 100%% coverage.")

    _save_json(out_dir / "empty_response_log.json", empty_log)

    # ── Final reports ────────────────────────────────────────────────────
    cmp_md = build_comparison_md(variants_data)
    (out_dir / "comparison_table.md").write_text(cmp_md, encoding="utf-8")

    rec_md = build_recommendation_md(variants_data)
    (out_dir / "recommendation.md").write_text(rec_md, encoding="utf-8")

    logger.info("Done. Artifacts: %s", out_dir)
    return 0


def _vm_to_dict(m: VariantMetrics) -> dict:
    d = m.__dict__.copy()
    return d


def _br_to_dict(r: BlockResult) -> dict:
    d = r.__dict__.copy()
    return d


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--project-dir",
        default=str(_ROOT / PROJECT_REL),
        help="Project dir (default: KJ small project).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip API calls; emit manifest + control only.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
