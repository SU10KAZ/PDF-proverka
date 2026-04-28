"""
Narrow re-evaluation of Gemini 3.1 Pro as a second-pass engine on the
small KJ project.

Scope:
  - Reuse the existing 17-block escalation set from the recall-hybrid run
  - Reuse existing Flash and Claude artifacts
  - Run only Pro, single-block mode, via OpenRouter
  - No recrop, no rebuild, no full-doc, no batch mode

Requested Pro configs:
  A. pro_high_p2  -> reasoning=high, healing=ON, parallelism=2
  B. pro_high_p1  -> reasoning=high, healing=ON, parallelism=1
  C. pro_low_p2   -> reasoning=low,  healing=ON, parallelism=2

Strict success rule (single-block mode):
  success iff:
    - len(block_analyses) == 1
    - block_analyses[0].block_id == input_block_id

Retry rule:
  - retry exactly once on ANY non-success primary result
  - if initial healing ON -> retry with healing OFF
  - if initial healing OFF -> retry with the same params
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import shutil
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from run_gemini_openrouter_stage02_experiment import (  # noqa: E402
    RunMetrics,
    build_messages,
    build_page_contexts,
    build_system_prompt,
)
from run_stage02_recall_hybrid import (  # noqa: E402
    SHORT_SUMMARY_THRESHOLD,
    select_second_pass_winner,
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
logger = logging.getLogger("pro_second_pass_reeval")


PROJECT_REL = "projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.1-К1К2 (2).pdf"
BASELINE_RUN = "20260422_073520"
BASELINE_REL = (
    f"_experiments/stage02_recall_hybrid_small_project/{BASELINE_RUN}"
)
MODEL_PRO = "google/gemini-3.1-pro-preview"
PER_BLOCK_TIMEOUT_SEC = 600
MAX_OUTPUT_TOKENS = 16000

STATUS_SUCCESS = "success"
STATUS_EMPTY = "empty_array"
STATUS_MULTI = "multiple_analyses"
STATUS_WRONG_ID = "wrong_block_id"
STATUS_NO_FIELD = "no_block_analyses_field"
STATUS_NON_DICT = "non_dict_analysis"
STATUS_API_ERROR = "api_error"


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(len(s) * pct / 100)
    return s[min(idx, len(s) - 1)]


def _retry_healing(initial_healing: bool) -> bool:
    return False if initial_healing else initial_healing


def _runmetrics_from_summary(summary: dict) -> RunMetrics:
    allowed = {f.name for f in fields(RunMetrics)}
    payload = {k: v for k, v in summary.items() if k in allowed}
    return RunMetrics(**payload)


def _classify_response(
    parsed_data: dict | None,
    input_bid: str,
) -> tuple[str, int, str, bool, int, dict | None]:
    """Classify a single-block response under the strict success rule."""
    if not isinstance(parsed_data, dict):
        return (STATUS_NO_FIELD, 0, "", False, 0, None)

    analyses = parsed_data.get("block_analyses")
    if not isinstance(analyses, list):
        return (STATUS_NO_FIELD, 0, "", False, 0, None)

    n = len(analyses)
    if n == 0:
        return (STATUS_EMPTY, 0, "", False, 0, None)

    first = analyses[0] if isinstance(analyses[0], dict) else None
    first_bid = (first.get("block_id") if first else "") or ""

    if n > 1:
        return (STATUS_MULTI, n, first_bid, True, 0, None)

    if not isinstance(first, dict):
        return (STATUS_NON_DICT, 1, "", False, 0, None)

    if not first_bid:
        return (STATUS_WRONG_ID, 1, first_bid, False, 1, None)

    if first_bid != input_bid:
        return (STATUS_WRONG_ID, 1, first_bid, False, 1, None)

    return (STATUS_SUCCESS, 1, first_bid, False, 0, first)


@dataclass
class ConfigSpec:
    config_id: str
    label: str
    reasoning_effort: str
    response_healing_initial: bool
    parallelism: int


@dataclass
class BlockResult:
    block_id: str
    page: int
    success: bool = False
    strict_failure: bool = False
    primary_status: str = ""
    final_status: str = ""
    failure_mode: str = ""
    primary_analysis_count: int = 0
    retry_analysis_count: int = -1
    final_analysis_count: int = 0
    multiple_analyses_in_single_block: bool = False
    wrong_block_id_count: int = 0
    returned_block_id_primary: str = ""
    returned_block_id_retry: str = ""
    retry_attempted: bool = False
    retry_recovered: bool = False
    retry_healing_used: bool = False
    parsed_block: dict | None = None
    primary_response_id: str = ""
    retry_response_id: str = ""
    response_id: str = ""
    primary_finish_reason: str = ""
    retry_finish_reason: str = ""
    finish_reason: str = ""
    primary_is_error: bool = False
    retry_is_error: bool = False
    duration_ms: int = 0
    prompt_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    cached_tokens: int = 0
    cost_usd: float = 0.0
    cost_source: str = "estimated"
    healing_effective: bool = False
    initial_healing: bool = False
    error_message: str = ""


@dataclass
class ConfigMetrics:
    config_id: str
    label: str
    reasoning_effort: str
    response_healing_initial: bool
    parallelism: int
    claude_reused: bool = True

    total_blocks: int = 0
    coverage_pct: float = 0.0
    missing_count: int = 0
    duplicate_count: int = 0
    extra_count: int = 0
    strict_success_count: int = 0
    strict_failure_count: int = 0
    missing_block_ids: list[str] = field(default_factory=list)
    duplicate_block_ids: list[str] = field(default_factory=list)
    extra_block_ids: list[str] = field(default_factory=list)

    primary_status_distribution: dict[str, int] = field(default_factory=dict)
    final_status_distribution: dict[str, int] = field(default_factory=dict)
    fail_mode_distribution_before_retry: dict[str, int] = field(default_factory=dict)
    fail_mode_distribution: dict[str, int] = field(default_factory=dict)

    retry_triggered_count: int = 0
    retry_recovered_count: int = 0
    retry_triggered_block_ids: list[str] = field(default_factory=list)
    retry_recovered_block_ids: list[str] = field(default_factory=list)

    blocks_with_findings: int = 0
    total_findings: int = 0
    total_key_values: int = 0
    median_key_values: float = 0.0
    empty_summary_count: int = 0
    empty_key_values_count: int = 0

    improved_blocks: list[str] = field(default_factory=list)
    unchanged_blocks: list[str] = field(default_factory=list)
    degraded_blocks: list[str] = field(default_factory=list)
    missing_in_engine_block_ids: list[str] = field(default_factory=list)
    additional_findings_vs_flash: int = 0
    additional_blocks_with_findings_vs_flash: int = 0
    additional_key_values_vs_flash: int = 0
    summary_specificity_improvements: int = 0
    summary_specificity_improvement_block_ids: list[str] = field(default_factory=list)

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

    healed_responses_count: int = 0
    primary_response_ids_available: int = 0
    retry_response_ids_available: int = 0


async def run_one_block(
    *,
    block: dict,
    project_dir: Path,
    project_info: dict,
    system_prompt: str,
    page_contexts: dict[int, str],
    config: ConfigSpec,
    schema: dict,
    timeout: int,
    max_output_tokens: int,
) -> BlockResult:
    bid = block["block_id"]
    page = int(block.get("page", 0))

    messages = build_messages(
        [block],
        project_dir,
        project_info,
        batch_id=1,
        total_batches=1,
        system_prompt=system_prompt,
        page_contexts=page_contexts,
    )
    extra_body = {"reasoning": {"effort": config.reasoning_effort}}

    primary = await run_llm(
        stage="block_batch",
        messages=messages,
        model_override=MODEL_PRO,
        temperature=0.2,
        timeout=timeout,
        max_retries=3,
        strict_schema=schema,
        schema_name="block_batch",
        response_healing=config.response_healing_initial,
        require_parameters=True,
        max_tokens_override=max_output_tokens,
        extra_body=extra_body,
    )

    result = BlockResult(
        block_id=bid,
        page=page,
        primary_response_id=primary.response_id or "",
        response_id=primary.response_id or "",
        primary_finish_reason=primary.finish_reason or "",
        finish_reason=primary.finish_reason or "",
        primary_is_error=primary.is_error,
        duration_ms=primary.duration_ms,
        prompt_tokens=primary.input_tokens,
        output_tokens=primary.output_tokens,
        reasoning_tokens=primary.reasoning_tokens,
        cached_tokens=primary.cached_tokens,
        cost_usd=primary.cost_usd,
        cost_source=primary.cost_source,
        healing_effective=config.response_healing_initial,
        initial_healing=config.response_healing_initial,
        error_message=primary.error_message or "",
    )

    if primary.is_error:
        p_status = STATUS_API_ERROR
        p_n = 0
        p_first_bid = ""
        p_multi = False
        p_wrong = 0
        p_block = None
    else:
        p_status, p_n, p_first_bid, p_multi, p_wrong, p_block = _classify_response(
            primary.json_data if isinstance(primary.json_data, dict) else None,
            bid,
        )

    result.primary_status = p_status
    result.primary_analysis_count = p_n
    result.final_analysis_count = p_n
    result.returned_block_id_primary = p_first_bid
    result.multiple_analyses_in_single_block = p_multi
    result.wrong_block_id_count = p_wrong

    if p_status == STATUS_SUCCESS:
        result.success = True
        result.strict_failure = False
        result.final_status = STATUS_SUCCESS
        result.failure_mode = ""
        result.parsed_block = p_block
        return result

    result.retry_attempted = True
    result.retry_healing_used = _retry_healing(config.response_healing_initial)

    retry = await run_llm(
        stage="block_batch",
        messages=messages,
        model_override=MODEL_PRO,
        temperature=0.2,
        timeout=timeout,
        max_retries=3,
        strict_schema=schema,
        schema_name="block_batch",
        response_healing=result.retry_healing_used,
        require_parameters=True,
        max_tokens_override=max_output_tokens,
        extra_body=extra_body,
    )

    result.duration_ms += retry.duration_ms
    result.prompt_tokens += retry.input_tokens
    result.output_tokens += retry.output_tokens
    result.reasoning_tokens += retry.reasoning_tokens
    result.cached_tokens += retry.cached_tokens
    result.cost_usd += retry.cost_usd
    if retry.cost_source == "actual":
        result.cost_source = "actual"
    result.retry_response_id = retry.response_id or ""
    result.retry_finish_reason = retry.finish_reason or ""

    if retry.is_error:
        r_status = STATUS_API_ERROR
        r_n = 0
        r_first_bid = ""
        r_multi = False
        r_wrong = 0
        r_block = None
    else:
        r_status, r_n, r_first_bid, r_multi, r_wrong, r_block = _classify_response(
            retry.json_data if isinstance(retry.json_data, dict) else None,
            bid,
        )

    result.retry_is_error = retry.is_error
    result.retry_analysis_count = r_n
    result.final_analysis_count = r_n
    result.returned_block_id_retry = r_first_bid
    result.response_id = retry.response_id or result.response_id
    result.finish_reason = retry.finish_reason or result.finish_reason
    result.healing_effective = result.retry_healing_used

    if r_multi:
        result.multiple_analyses_in_single_block = True
    if r_wrong:
        result.wrong_block_id_count = max(result.wrong_block_id_count, r_wrong)

    if r_status == STATUS_SUCCESS:
        result.success = True
        result.strict_failure = False
        result.retry_recovered = True
        result.final_status = STATUS_SUCCESS
        result.failure_mode = ""
        result.parsed_block = r_block
        return result

    result.success = False
    result.strict_failure = True
    result.final_status = r_status
    result.failure_mode = r_status
    return result


def build_diff_vs_flash(
    *,
    block_results: list[BlockResult],
    flash_by_id: dict[str, dict],
    escalation_ids: list[str],
    engine_label: str,
) -> tuple[dict, list[dict]]:
    results_by_id = {r.block_id: r for r in block_results}

    improved = unchanged = degraded = 0
    added_findings = added_kv = 0
    flash_total_findings = flash_total_kv = 0
    engine_total_findings = engine_total_kv = 0
    flash_blocks_with_findings = 0
    engine_blocks_with_findings = 0
    unreadable_recovery = 0
    summary_specificity_improvements = 0
    summary_specificity_improvement_block_ids: list[str] = []
    per_block: list[dict] = []

    for bid in escalation_ids:
        r = results_by_id[bid]
        flash = flash_by_id.get(bid) or {}
        f_findings = len(flash.get("findings") or [])
        e_findings = len((r.parsed_block or {}).get("findings") or [])
        f_kv = len(flash.get("key_values_read") or [])
        e_kv = len((r.parsed_block or {}).get("key_values_read") or [])
        f_unreadable = bool(flash.get("unreadable_text"))
        e_unreadable = bool((r.parsed_block or {}).get("unreadable_text"))
        f_summary_len = len(str(flash.get("summary") or "").strip())
        e_summary_len = len(str((r.parsed_block or {}).get("summary") or "").strip())
        engine_missing = not r.success or r.parsed_block is None

        flash_total_findings += f_findings
        flash_total_kv += f_kv
        engine_total_findings += e_findings
        engine_total_kv += e_kv
        if f_findings > 0:
            flash_blocks_with_findings += 1
        if e_findings > 0:
            engine_blocks_with_findings += 1

        delta_findings = e_findings - f_findings
        delta_kv = e_kv - f_kv
        added_findings += max(0, delta_findings)
        added_kv += max(0, delta_kv)

        summary_specificity_improved = (
            e_summary_len > f_summary_len + 30
            and e_summary_len > SHORT_SUMMARY_THRESHOLD
        )
        if summary_specificity_improved:
            summary_specificity_improvements += 1
            summary_specificity_improvement_block_ids.append(bid)

        is_improved = (
            (delta_findings > 0)
            or (f_unreadable and not e_unreadable)
            or (f_kv == 0 and e_kv > 0)
            or summary_specificity_improved
        )
        is_degraded = (
            (delta_findings < 0 and f_findings > 0)
            or (not f_unreadable and e_unreadable)
        )
        if f_unreadable and not e_unreadable:
            unreadable_recovery += 1

        if engine_missing:
            status = "missing_in_engine"
            degraded += 1
        elif is_improved and not is_degraded:
            status = "improved"
            improved += 1
        elif is_degraded and not is_improved:
            status = "degraded"
            degraded += 1
        else:
            status = "unchanged"
            unchanged += 1

        per_block.append({
            "block_id": bid,
            "status": status,
            "flash_findings": f_findings,
            "engine_findings": e_findings,
            "delta_findings": delta_findings,
            "flash_kv": f_kv,
            "engine_kv": e_kv,
            "delta_kv": delta_kv,
            "flash_unreadable": f_unreadable,
            "engine_unreadable": e_unreadable,
            "flash_missing": bid not in flash_by_id,
            "engine_missing": engine_missing,
            "flash_summary_len": f_summary_len,
            "engine_summary_len": e_summary_len,
            "summary_specificity_improved": summary_specificity_improved,
            "primary_status": r.primary_status,
            "final_status": r.final_status,
            "retry_attempted": r.retry_attempted,
            "retry_recovered": r.retry_recovered,
            "primary_response_id": r.primary_response_id,
            "retry_response_id": r.retry_response_id,
            "primary_finish_reason": r.primary_finish_reason,
            "retry_finish_reason": r.retry_finish_reason,
        })

    summary = {
        "engine": engine_label,
        "escalation_set_size": len(escalation_ids),
        "improved": improved,
        "unchanged": unchanged,
        "degraded": degraded,
        "unreadable_recovery": unreadable_recovery,
        "added_findings": added_findings,
        "added_kv": added_kv,
        "flash_total_findings": flash_total_findings,
        "flash_total_kv": flash_total_kv,
        "engine_total_findings": engine_total_findings,
        "engine_total_kv": engine_total_kv,
        "flash_blocks_with_findings": flash_blocks_with_findings,
        "engine_blocks_with_findings": engine_blocks_with_findings,
        "additional_blocks_with_findings": engine_blocks_with_findings - flash_blocks_with_findings,
        "summary_specificity_improvements": summary_specificity_improvements,
        "summary_specificity_improvement_block_ids": summary_specificity_improvement_block_ids,
        "per_block_diff": per_block,
    }
    return summary, per_block


def aggregate_metrics(
    *,
    config: ConfigSpec,
    block_results: list[BlockResult],
    escalation_ids: list[str],
    diff_summary: dict,
    elapsed_s: float,
) -> ConfigMetrics:
    metrics = ConfigMetrics(
        config_id=config.config_id,
        label=config.label,
        reasoning_effort=config.reasoning_effort,
        response_healing_initial=config.response_healing_initial,
        parallelism=config.parallelism,
        total_blocks=len(escalation_ids),
        elapsed_s=elapsed_s,
    )

    durations: list[float] = []
    kv_counts: list[int] = []
    successful_ids: list[str] = []

    for r in block_results:
        durations.append(r.duration_ms / 1000.0)
        metrics.total_prompt_tokens += r.prompt_tokens
        metrics.total_output_tokens += r.output_tokens
        metrics.total_reasoning_tokens += r.reasoning_tokens
        metrics.total_cached_tokens += r.cached_tokens
        metrics.total_cost_usd += r.cost_usd
        if r.cost_source == "actual":
            metrics.cost_sources_actual += 1
        else:
            metrics.cost_sources_estimated += 1

        metrics.primary_status_distribution[r.primary_status] = (
            metrics.primary_status_distribution.get(r.primary_status, 0) + 1
        )
        metrics.final_status_distribution[r.final_status] = (
            metrics.final_status_distribution.get(r.final_status, 0) + 1
        )
        if r.primary_status != STATUS_SUCCESS:
            metrics.fail_mode_distribution_before_retry[r.primary_status] = (
                metrics.fail_mode_distribution_before_retry.get(r.primary_status, 0) + 1
            )
        if r.final_status != STATUS_SUCCESS:
            metrics.fail_mode_distribution[r.final_status] = (
                metrics.fail_mode_distribution.get(r.final_status, 0) + 1
            )

        if r.retry_attempted:
            metrics.retry_triggered_count += 1
            metrics.retry_triggered_block_ids.append(r.block_id)
        if r.retry_recovered:
            metrics.retry_recovered_count += 1
            metrics.retry_recovered_block_ids.append(r.block_id)
        if r.primary_response_id:
            metrics.primary_response_ids_available += 1
        if r.retry_response_id:
            metrics.retry_response_ids_available += 1

        if r.success and r.parsed_block is not None:
            successful_ids.append(r.parsed_block["block_id"])
            if r.healing_effective:
                metrics.healed_responses_count += 1
            summary = str(r.parsed_block.get("summary") or "").strip()
            kv = r.parsed_block.get("key_values_read") or []
            findings = r.parsed_block.get("findings") or []
            if not summary:
                metrics.empty_summary_count += 1
            if not kv:
                metrics.empty_key_values_count += 1
            else:
                metrics.total_key_values += len(kv)
                kv_counts.append(len(kv))
            if findings:
                metrics.blocks_with_findings += 1
                metrics.total_findings += len(findings)

    output_counter = Counter(successful_ids)
    input_set = set(escalation_ids)
    extra_ids = sorted([bid for bid in output_counter if bid not in input_set])
    duplicate_ids = sorted([bid for bid, count in output_counter.items() if count > 1])
    missing_ids = [bid for bid in escalation_ids if output_counter.get(bid, 0) == 0]

    metrics.strict_success_count = len(successful_ids)
    metrics.strict_failure_count = len(escalation_ids) - len(successful_ids)
    metrics.missing_count = len(missing_ids)
    metrics.duplicate_count = len(duplicate_ids)
    metrics.extra_count = len(extra_ids)
    metrics.missing_block_ids = missing_ids
    metrics.duplicate_block_ids = duplicate_ids
    metrics.extra_block_ids = extra_ids
    if escalation_ids:
        metrics.coverage_pct = round(len(successful_ids) / len(escalation_ids) * 100, 2)

    if durations:
        metrics.avg_block_duration_s = round(sum(durations) / len(durations), 2)
        metrics.median_block_duration_s = round(statistics.median(durations), 2)
        metrics.p95_block_duration_s = round(_percentile(durations, 95), 2)
    if kv_counts:
        metrics.median_key_values = round(statistics.median(kv_counts), 2)
    valid_blocks = len(successful_ids)
    if valid_blocks:
        metrics.cost_per_valid_block = round(metrics.total_cost_usd / valid_blocks, 6)

    metrics.improved_blocks = [
        row["block_id"] for row in diff_summary["per_block_diff"] if row["status"] == "improved"
    ]
    metrics.unchanged_blocks = [
        row["block_id"] for row in diff_summary["per_block_diff"] if row["status"] == "unchanged"
    ]
    metrics.degraded_blocks = [
        row["block_id"] for row in diff_summary["per_block_diff"] if row["status"] == "degraded"
    ]
    metrics.missing_in_engine_block_ids = [
        row["block_id"] for row in diff_summary["per_block_diff"] if row["status"] == "missing_in_engine"
    ]
    metrics.additional_findings_vs_flash = diff_summary["added_findings"]
    metrics.additional_blocks_with_findings_vs_flash = diff_summary["additional_blocks_with_findings"]
    metrics.additional_key_values_vs_flash = diff_summary["added_kv"]
    metrics.summary_specificity_improvements = diff_summary["summary_specificity_improvements"]
    metrics.summary_specificity_improvement_block_ids = diff_summary["summary_specificity_improvement_block_ids"]
    return metrics


async def run_config(
    *,
    config: ConfigSpec,
    blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    schema: dict,
    flash_by_id: dict[str, dict],
    escalation_ids: list[str],
) -> tuple[ConfigMetrics, list[BlockResult], dict, list[dict]]:
    system_prompt = build_system_prompt(project_info, len(blocks))
    page_contexts = build_page_contexts(project_dir)
    sem = asyncio.Semaphore(config.parallelism)
    results: list[BlockResult | None] = [None] * len(blocks)

    async def _one(i: int, block: dict) -> None:
        async with sem:
            started = time.monotonic()
            res = await run_one_block(
                block=block,
                project_dir=project_dir,
                project_info=project_info,
                system_prompt=system_prompt,
                page_contexts=page_contexts,
                config=config,
                schema=schema,
                timeout=PER_BLOCK_TIMEOUT_SEC,
                max_output_tokens=MAX_OUTPUT_TOKENS,
            )
            results[i] = res
            wall = time.monotonic() - started
            tag = "SUCCESS" if res.success else f"FAIL({res.final_status})"
            logger.info(
                "[%s] %02d/%d %s %s primary=%s final=%s retry=%s wall=%.1fs cost=$%.4f",
                config.config_id,
                i + 1,
                len(blocks),
                block["block_id"],
                tag,
                res.primary_status,
                res.final_status,
                "yes" if res.retry_attempted else "no",
                wall,
                res.cost_usd,
            )

    start = time.monotonic()
    await asyncio.gather(*(_one(i, b) for i, b in enumerate(blocks)))
    elapsed_s = time.monotonic() - start

    block_results = [r for r in results if r is not None]
    diff_summary, per_block_diff = build_diff_vs_flash(
        block_results=block_results,
        flash_by_id=flash_by_id,
        escalation_ids=escalation_ids,
        engine_label=f"Pro ({config.config_id})",
    )
    metrics = aggregate_metrics(
        config=config,
        block_results=block_results,
        escalation_ids=escalation_ids,
        diff_summary=diff_summary,
        elapsed_s=elapsed_s,
    )
    return metrics, block_results, diff_summary, per_block_diff


def build_retry_log(config_id: str, results: list[BlockResult]) -> list[dict]:
    rows: list[dict] = []
    for r in results:
        rows.append({
            "config_id": config_id,
            "block_id": r.block_id,
            "page": r.page,
            "primary_status": r.primary_status,
            "final_status": r.final_status,
            "retry_attempted": r.retry_attempted,
            "retry_recovered": r.retry_recovered,
            "initial_healing": r.initial_healing,
            "retry_healing_used": r.retry_healing_used,
            "primary_analysis_count": r.primary_analysis_count,
            "retry_analysis_count": r.retry_analysis_count,
            "final_analysis_count": r.final_analysis_count,
            "returned_block_id_primary": r.returned_block_id_primary,
            "returned_block_id_retry": r.returned_block_id_retry,
            "primary_response_id": r.primary_response_id,
            "retry_response_id": r.retry_response_id,
            "primary_finish_reason": r.primary_finish_reason,
            "retry_finish_reason": r.retry_finish_reason,
            "prompt_tokens": r.prompt_tokens,
            "output_tokens": r.output_tokens,
            "reasoning_tokens": r.reasoning_tokens,
            "cached_tokens": r.cached_tokens,
            "cost_usd": r.cost_usd,
            "cost_source": r.cost_source,
            "duration_ms": r.duration_ms,
            "success": r.success,
            "failure_mode": r.failure_mode,
        })
    return rows


def build_failmode_file(metrics: ConfigMetrics) -> dict:
    return {
        "config_id": metrics.config_id,
        "primary_status_distribution": metrics.primary_status_distribution,
        "final_status_distribution": metrics.final_status_distribution,
        "fail_mode_distribution_before_retry": metrics.fail_mode_distribution_before_retry,
        "fail_mode_distribution": metrics.fail_mode_distribution,
        "retry_triggered_count": metrics.retry_triggered_count,
        "retry_recovered_count": metrics.retry_recovered_count,
        "retry_triggered_block_ids": metrics.retry_triggered_block_ids,
        "retry_recovered_block_ids": metrics.retry_recovered_block_ids,
    }


def build_summary_md(metrics: ConfigMetrics, diff_summary: dict) -> str:
    lines = [
        f"# {metrics.config_id}\n",
        f"{metrics.label}\n",
        "## Completeness\n",
        f"| Metric | Value |\n|--------|-------|\n",
        f"| Coverage | {metrics.coverage_pct:.2f}% |\n",
        f"| Missing / Duplicate / Extra | {metrics.missing_count} / {metrics.duplicate_count} / {metrics.extra_count} |\n",
        f"| Strict success / failure | {metrics.strict_success_count} / {metrics.strict_failure_count} |\n",
        f"| Retry triggered / recovered | {metrics.retry_triggered_count} / {metrics.retry_recovered_count} |\n",
        f"| Final fail modes | {json.dumps(metrics.fail_mode_distribution, ensure_ascii=False)} |\n",
        "\n## Quality vs Flash on same escalation set\n",
        f"| Metric | Value |\n|--------|-------|\n",
        f"| Improved / Unchanged / Degraded | {diff_summary['improved']} / {diff_summary['unchanged']} / {diff_summary['degraded']} |\n",
        f"| Additional findings | {metrics.additional_findings_vs_flash:+d} |\n",
        f"| Additional blocks_with_findings | {metrics.additional_blocks_with_findings_vs_flash:+d} |\n",
        f"| Additional key_values | {metrics.additional_key_values_vs_flash:+d} |\n",
        f"| Summary specificity improvements | {metrics.summary_specificity_improvements} |\n",
        "\n## Runtime and cost\n",
        f"| Metric | Value |\n|--------|-------|\n",
        f"| Elapsed | {metrics.elapsed_s:.1f}s |\n",
        f"| Avg / median / p95 per-block | {metrics.avg_block_duration_s:.2f}s / {metrics.median_block_duration_s:.2f}s / {metrics.p95_block_duration_s:.2f}s |\n",
        f"| Prompt / output / reasoning tokens | {metrics.total_prompt_tokens} / {metrics.total_output_tokens} / {metrics.total_reasoning_tokens} |\n",
        f"| Total cost USD | ${metrics.total_cost_usd:.4f} |\n",
        f"| Cost / valid block | ${metrics.cost_per_valid_block:.5f} |\n",
        f"| Healed responses count | {metrics.healed_responses_count} |\n",
        f"| Response IDs available (primary / retry) | {metrics.primary_response_ids_available} / {metrics.retry_response_ids_available} |\n",
    ]
    return "".join(lines)


def build_comparison_rows(
    *,
    claude_summary: dict,
    claude_diff: dict,
    pro_configs: list[tuple[ConfigMetrics, dict]],
) -> list[dict]:
    rows: list[dict] = []

    claude_per_block = claude_diff.get("per_block_diff", [])
    claude_engine_blocks_with_findings = sum(
        1 for row in claude_per_block if int(row.get("engine_findings", 0)) > 0
    )
    claude_flash_blocks_with_findings = sum(
        1 for row in claude_per_block if int(row.get("flash_findings", 0)) > 0
    )
    claude_specificity = sum(
        1
        for row in claude_per_block
        if row.get("engine_summary_len", 0) > row.get("flash_summary_len", 0) + 30
        and row.get("engine_summary_len", 0) > SHORT_SUMMARY_THRESHOLD
    )
    rows.append({
        "engine_id": "claude_reused",
        "label": "Claude reused reference",
        "model_id": claude_summary["model_id"],
        "coverage_pct": claude_summary["coverage_pct"],
        "missing_count": claude_summary["missing_count"],
        "duplicate_count": claude_summary["duplicate_count"],
        "extra_count": claude_summary["extra_count"],
        "strict_success_count": claude_summary["total_input_blocks"] - claude_summary["missing_count"],
        "strict_failure_count": claude_summary["missing_count"],
        "improved": claude_diff["improved"],
        "unchanged": claude_diff["unchanged"],
        "degraded": claude_diff["degraded"],
        "added_findings": claude_diff["added_findings"],
        "additional_blocks_with_findings": claude_engine_blocks_with_findings - claude_flash_blocks_with_findings,
        "added_kv": claude_diff["added_kv"],
        "summary_specificity_improvements": claude_specificity,
        "elapsed_s": claude_summary["elapsed_s"],
        "total_cost_usd": claude_summary["total_cost_usd"],
        "avg_block_duration_s": claude_summary["avg_batch_duration_s"],
        "median_block_duration_s": claude_summary["median_batch_duration_s"],
        "p95_block_duration_s": claude_summary["p95_batch_duration_s"],
        "total_prompt_tokens": claude_summary["total_prompt_tokens"],
        "total_output_tokens": claude_summary["total_output_tokens"],
        "total_reasoning_tokens": claude_summary["total_reasoning_tokens"],
        "retry_triggered_count": None,
        "retry_recovered_count": None,
        "fail_mode_distribution": None,
        "reused": True,
    })

    for metrics, diff_summary in pro_configs:
        rows.append({
            "engine_id": metrics.config_id,
            "label": metrics.label,
            "model_id": MODEL_PRO,
            "coverage_pct": metrics.coverage_pct,
            "missing_count": metrics.missing_count,
            "duplicate_count": metrics.duplicate_count,
            "extra_count": metrics.extra_count,
            "strict_success_count": metrics.strict_success_count,
            "strict_failure_count": metrics.strict_failure_count,
            "improved": diff_summary["improved"],
            "unchanged": diff_summary["unchanged"],
            "degraded": diff_summary["degraded"],
            "added_findings": diff_summary["added_findings"],
            "additional_blocks_with_findings": diff_summary["additional_blocks_with_findings"],
            "added_kv": diff_summary["added_kv"],
            "summary_specificity_improvements": diff_summary["summary_specificity_improvements"],
            "elapsed_s": metrics.elapsed_s,
            "total_cost_usd": metrics.total_cost_usd,
            "avg_block_duration_s": metrics.avg_block_duration_s,
            "median_block_duration_s": metrics.median_block_duration_s,
            "p95_block_duration_s": metrics.p95_block_duration_s,
            "total_prompt_tokens": metrics.total_prompt_tokens,
            "total_output_tokens": metrics.total_output_tokens,
            "total_reasoning_tokens": metrics.total_reasoning_tokens,
            "retry_triggered_count": metrics.retry_triggered_count,
            "retry_recovered_count": metrics.retry_recovered_count,
            "fail_mode_distribution": metrics.fail_mode_distribution,
            "reused": False,
        })
    return rows


def rank_rows(rows: list[dict]) -> list[dict]:
    def key(row: dict) -> tuple:
        complete = (
            row["coverage_pct"] == 100.0
            and row["missing_count"] == 0
            and row["duplicate_count"] == 0
            and row["extra_count"] == 0
        )
        return (
            1 if complete else 0,
            float(row["coverage_pct"]),
            -int(row["missing_count"]),
            int(row["improved"]),
            int(row["added_findings"]),
            -int(row["degraded"]),
            -float(row["total_cost_usd"]),
            -float(row["elapsed_s"]),
        )

    return sorted(rows, key=key, reverse=True)


def build_comparison_md(rows: list[dict]) -> str:
    headers = [
        "Engine",
        "Coverage",
        "Missing/Dup/Extra",
        "Strict S/F",
        "Improved",
        "Added findings",
        "Degraded",
        "Add blocks_with_findings",
        "Add KV",
        "Summary specificity",
        "Retry trig/rec",
        "Cost USD",
        "Elapsed s",
        "Reuse",
    ]
    lines = [
        "# Comparison vs Claude\n",
        "| " + " | ".join(headers) + " |",
        "|" + "---|" * len(headers),
    ]

    for row in rows:
        retry = "n/a"
        if row["retry_triggered_count"] is not None:
            retry = f"{row['retry_triggered_count']}/{row['retry_recovered_count']}"
        lines.append(
            "| {engine} | {cov:.2f}% | {m}/{d}/{e} | {ss}/{sf} | {imp} | +{af} | {deg} | {ab:+d} | {ak:+d} | {ssi} | {retry} | ${cost:.4f} | {elapsed:.1f} | {reuse} |".format(
                engine=row["engine_id"],
                cov=row["coverage_pct"],
                m=row["missing_count"],
                d=row["duplicate_count"],
                e=row["extra_count"],
                ss=row["strict_success_count"],
                sf=row["strict_failure_count"],
                imp=row["improved"],
                af=row["added_findings"],
                deg=row["degraded"],
                ab=row["additional_blocks_with_findings"],
                ak=row["added_kv"],
                ssi=row["summary_specificity_improvements"],
                retry=retry,
                cost=row["total_cost_usd"],
                elapsed=row["elapsed_s"],
                reuse="reused" if row["reused"] else "ran_now",
            )
        )

    lines.append("")
    return "\n".join(lines)


def load_saved_pro_results(out_dir: Path) -> list[tuple[ConfigMetrics, dict]]:
    configs = ["pro_high_p2", "pro_high_p1", "pro_low_p2"]
    out: list[tuple[ConfigMetrics, dict]] = []
    for config_id in configs:
        summary = _load_json(out_dir / f"{config_id}_summary.json")
        diff_payload = _load_json(out_dir / f"{config_id}_diff_vs_flash.json")
        metrics = ConfigMetrics(**summary)
        out.append((metrics, diff_payload["summary"]))
    return out


def finalize_reports(out_dir: Path) -> None:
    claude_summary = _load_json(out_dir / "claude_reused_summary.json")
    claude_diff = _load_json(out_dir / "claude_reused_diff.json")
    pro_results = load_saved_pro_results(out_dir)
    comparison_rows = build_comparison_rows(
        claude_summary=claude_summary,
        claude_diff=claude_diff,
        pro_configs=pro_results,
    )
    ranked_rows = rank_rows(comparison_rows)
    comparison_md = build_comparison_md(comparison_rows)
    winner_md = build_winner_recommendation_md(
        ranked_rows=ranked_rows,
        claude_summary=claude_summary,
        claude_diff=claude_diff,
        pro_results=pro_results,
    )

    (out_dir / "comparison_vs_claude.md").write_text(comparison_md, encoding="utf-8")
    (out_dir / "winner_recommendation.md").write_text(winner_md, encoding="utf-8")
    _save_json(out_dir / "comparison_rows.json", comparison_rows)
    _save_json(out_dir / "ranked_rows.json", ranked_rows)


def build_winner_recommendation_md(
    *,
    ranked_rows: list[dict],
    claude_summary: dict,
    claude_diff: dict,
    pro_results: list[tuple[ConfigMetrics, dict]],
) -> str:
    winner = ranked_rows[0]
    claude_row = next(row for row in ranked_rows if row["engine_id"] == "claude_reused")
    claude_rm = _runmetrics_from_summary(claude_summary)

    lines = [
        "# Winner Recommendation\n",
        f"**Winner**: `{winner['engine_id']}`\n",
        "## Ranking logic\n",
        "1. completeness",
        "2. improved blocks",
        "3. additional findings",
        "4. degraded blocks",
        "5. cost/time\n",
        "## Pairwise Pro vs Claude\n",
    ]

    pro_complete_ids: list[str] = []
    for metrics, diff_summary in pro_results:
        pro_rm = RunMetrics(
            run_id=metrics.config_id,
            model_id=MODEL_PRO,
            parallelism=metrics.parallelism,
            total_input_blocks=metrics.total_blocks,
            coverage_pct=metrics.coverage_pct,
            missing_count=metrics.missing_count,
            duplicate_count=metrics.duplicate_count,
            extra_count=metrics.extra_count,
            total_cost_usd=metrics.total_cost_usd,
            elapsed_s=metrics.elapsed_s,
        )
        pair_winner, rationale = select_second_pass_winner(
            pro_rm,
            diff_summary,
            claude_rm,
            claude_diff,
            metrics.total_blocks,
        )
        if metrics.coverage_pct == 100.0 and metrics.missing_count == 0:
            pro_complete_ids.append(metrics.config_id)
        lines.append(f"### {metrics.config_id}")
        lines.append(f"- Pairwise winner vs Claude: `{pair_winner}`")
        lines.append(f"- Rationale: {rationale.split('**Rationale**:')[-1].strip()}")

    lines.append("")
    if winner["engine_id"] == "claude_reused":
        lines.append("- Claude still wins as second-pass engine on the small KJ benchmark.")
    else:
        lines.append(f"- Pro wins in config `{winner['engine_id']}` on the requested decision logic.")

    if pro_complete_ids:
        lines.append(
            "- Pro can return to the candidate set in: "
            + ", ".join(f"`{cid}`" for cid in pro_complete_ids)
            + "."
        )
    else:
        lines.append("- Pro should not return to the candidate set yet because no tested config achieved complete strict-success coverage.")

    if "pro_high" in winner["engine_id"]:
        lines.append("- The evidence favors high reasoning over low reasoning for recall-oriented second pass.")
    elif winner["engine_id"] == "pro_low_p2":
        lines.append("- Low reasoning was sufficient on this benchmark, but validate carefully before any broader conclusion.")
    else:
        lines.append("- Low reasoning remains useful as a completeness/cost control, not the leading recall choice.")

    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-dir",
        default=str(_ROOT / PROJECT_REL),
        help="Project directory for the small KJ PDF.",
    )
    parser.add_argument(
        "--resume-out-dir",
        default="",
        help="Existing run directory to finalize without making new API calls.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write manifests only, do not call the provider.",
    )
    return parser.parse_args()


async def main_async(args: argparse.Namespace) -> int:
    if args.resume_out_dir:
        out_dir = Path(args.resume_out_dir).resolve()
        finalize_reports(out_dir)
        logger.info("Finalized reports in existing run dir: %s", out_dir)
        return 0

    project_dir = Path(args.project_dir).resolve()
    baseline_dir = project_dir / BASELINE_REL
    if not baseline_dir.exists():
        logger.error("Missing baseline artifacts: %s", baseline_dir)
        return 2

    escalation_manifest_path = baseline_dir / "escalation_set_manifest.json"
    escalation_ids_path = baseline_dir / "escalation_set_block_ids.json"
    flash_per_block_path = baseline_dir / "flash_full_per_block.json"
    claude_summary_path = baseline_dir / "second_pass_claude_summary.json"
    claude_diff_path = baseline_dir / "second_pass_claude_diff.json"

    escalation_manifest = _load_json(escalation_manifest_path)
    escalation_ids = _load_json(escalation_ids_path)
    if len(escalation_ids) != 17:
        logger.error("Expected exactly 17 escalation blocks, got %d", len(escalation_ids))
        return 2

    blocks_index = _load_json(project_dir / "_output" / "blocks" / "index.json")
    all_blocks_by_id = {b["block_id"]: b for b in blocks_index["blocks"]}
    missing_from_index = [bid for bid in escalation_ids if bid not in all_blocks_by_id]
    if missing_from_index:
        logger.error("Escalation blocks missing from current _output index: %s", ", ".join(missing_from_index))
        return 2
    blocks = [all_blocks_by_id[bid] for bid in escalation_ids]

    project_info = _load_json(project_dir / "project_info.json")
    flash_per_block = _load_json(flash_per_block_path)
    flash_by_id = {b["block_id"]: b for b in flash_per_block if b.get("block_id")}
    claude_summary = _load_json(claude_summary_path)
    claude_diff = _load_json(claude_diff_path)

    ts = _ts()
    out_dir = project_dir / "_experiments" / "pro_second_pass_reeval_small_project" / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "timestamp": ts,
        "project_dir": str(project_dir),
        "baseline_run": BASELINE_RUN,
        "model": MODEL_PRO,
        "stage": "02 block analysis",
        "mode": "single_block",
        "strict_schema": True,
        "require_parameters": True,
        "openrouter_path": True,
        "max_output_tokens": MAX_OUTPUT_TOKENS,
        "per_block_timeout_sec": PER_BLOCK_TIMEOUT_SEC,
        "claude_reused": True,
        "claude_rerun": False,
        "flash_rerun": False,
        "escalation_set_reused": True,
        "escalation_set_size": len(escalation_ids),
        "escalation_set_source": str(escalation_ids_path),
        "source_artifacts": {
            "escalation_set_manifest": str(escalation_manifest_path),
            "flash_full_per_block": str(flash_per_block_path),
            "claude_summary": str(claude_summary_path),
            "claude_diff": str(claude_diff_path),
        },
        "configs": [
            {
                "config_id": "pro_high_p2",
                "reasoning_effort": "high",
                "response_healing_initial": True,
                "parallelism": 2,
            },
            {
                "config_id": "pro_high_p1",
                "reasoning_effort": "high",
                "response_healing_initial": True,
                "parallelism": 1,
            },
            {
                "config_id": "pro_low_p2",
                "reasoning_effort": "low",
                "response_healing_initial": True,
                "parallelism": 2,
            },
        ],
        "dry_run": args.dry_run,
    }
    _save_json(out_dir / "manifest.json", manifest)
    _save_json(out_dir / "escalation_set_manifest.json", escalation_manifest)
    shutil.copyfile(escalation_ids_path, out_dir / "escalation_set_block_ids.json")
    shutil.copyfile(claude_summary_path, out_dir / "claude_reused_summary.json")
    shutil.copyfile(claude_diff_path, out_dir / "claude_reused_diff.json")

    if args.dry_run:
        logger.info("Dry-run only. Wrote manifest and reused artifacts to %s", out_dir)
        return 0

    schema = load_openrouter_block_batch_schema()
    configs = [
        ConfigSpec(
            config_id="pro_high_p2",
            label="Pro high reasoning, healing ON, parallelism=2",
            reasoning_effort="high",
            response_healing_initial=True,
            parallelism=2,
        ),
        ConfigSpec(
            config_id="pro_high_p1",
            label="Pro high reasoning, healing ON, parallelism=1",
            reasoning_effort="high",
            response_healing_initial=True,
            parallelism=1,
        ),
        ConfigSpec(
            config_id="pro_low_p2",
            label="Pro low reasoning, healing ON, parallelism=2",
            reasoning_effort="low",
            response_healing_initial=True,
            parallelism=2,
        ),
    ]

    pro_results: list[tuple[ConfigMetrics, dict]] = []
    retry_log_rows: list[dict] = []

    for config in configs:
        logger.info("=== Running %s ===", config.config_id)
        metrics, block_results, diff_summary, per_block_diff = await run_config(
            config=config,
            blocks=blocks,
            project_dir=project_dir,
            project_info=project_info,
            schema=schema,
            flash_by_id=flash_by_id,
            escalation_ids=escalation_ids,
        )
        pro_results.append((metrics, diff_summary))
        retry_log_rows.extend(build_retry_log(config.config_id, block_results))

        _save_json(out_dir / f"{config.config_id}_summary.json", metrics.__dict__)
        (out_dir / f"{config.config_id}_summary.md").write_text(
            build_summary_md(metrics, diff_summary),
            encoding="utf-8",
        )
        _save_json(
            out_dir / f"{config.config_id}_per_block.json",
            [r.__dict__ for r in block_results],
        )
        _save_json(
            out_dir / f"{config.config_id}_diff_vs_flash.json",
            {"summary": diff_summary, "per_block_diff": per_block_diff},
        )
        _save_json(
            out_dir / f"{config.config_id}_fail_mode_distribution.json",
            build_failmode_file(metrics),
        )

    _save_json(out_dir / "retry_log.json", retry_log_rows)

    finalize_reports(out_dir)

    logger.info("Done. Artifacts: %s", out_dir)
    return 0


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
