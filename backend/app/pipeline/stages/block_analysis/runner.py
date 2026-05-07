"""
block_analysis/runner.py
------------------------
Stage runner и pure helper functions для этапа block_analysis.

Содержит:
  - Pure helper functions (expand_block_batches_for_single_block_mode,
    build_single_block_runtime_plan, write_single_block_runtime_plan,
    load_or_create_single_block_runtime_plan, runtime_batch_failure_entry,
    write_block_analysis_runtime_summary, attach_stage02_coverage_to_findings,
    validate_and_repair_json) — перенесены из manager.py в предыдущих pass-ах.
  - run_block_analysis_findings_only(ctx) — полный block_analysis stage runner.

Публичный API (runner):
  run_block_analysis_findings_only(ctx) -> StageResult
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from backend.app.core.config import (
    CLAUDE_BLOCK_BATCH_CLEAN_CWD,
    get_stage_model,
)
from backend.app.pipeline.stage_result import StageResult
from backend.app.services.common.project_service import resolve_project_dir

if TYPE_CHECKING:
    from backend.app.pipeline.context import PipelineStageContext

RUNTIME_BATCHES_FILE = "block_batches.runtime.json"


def expand_block_batches_for_single_block_mode(batches: list[dict]) -> tuple[list[dict], bool]:
    """Перевести stage 02 в single-block режим (для всех моделей и пресетов).

    После архитектурного решения (Идея 7 в ideas.md): один image-блок = один
    LLM-запрос для ВСЕХ моделей. Раньше единственно для локальных Gemma, теперь
    унифицировано — Gemma-enrichment перенесён в stage 1 prep, stage 02 работает
    только с одиночными блоками. Это даёт:
      - меньше blast radius при битом PNG;
      - стабильнее качество (нет деградации обдумывания при батчах графики);
      - симметрию между провайдерами (Sonnet/Opus/GPT/Gemini получают один блок).

    Генератор `blocks.py batches` остаётся общим для аллокации блоков, здесь мы
    адаптируем runtime-план перед запуском CLI.
    """
    single_block_batches: list[dict] = []
    next_batch_id = 1

    for batch in batches:
        source_batch_id = batch.get("batch_id")
        for block in batch.get("blocks", []):
            block_copy = dict(block)
            page = block_copy.get("page")
            single_block_batches.append({
                "batch_id": next_batch_id,
                "blocks": [block_copy],
                "pages_included": [page] if page is not None else [],
                "block_count": 1,
                "total_size_kb": block_copy.get("size_kb", 0),
                "single_block_mode": True,
                "source_batch_id": source_batch_id,
            })
            next_batch_id += 1

    return single_block_batches, True


def build_single_block_runtime_plan(
    source_batches: list[dict],
    *,
    source: str = "expanded_from_blocks_py_batches",
) -> dict:
    """Build the persisted Stage 02 runtime plan used by progress/resume/retry."""
    batches, _ = expand_block_batches_for_single_block_mode(source_batches)
    return {
        "schema_version": 1,
        "mode": "single_block",
        "source": source,
        "total_batches": len(batches),
        "total_blocks": sum(int(b.get("block_count") or len(b.get("blocks", []))) for b in batches),
        "batches": batches,
    }


def write_single_block_runtime_plan(
    output_dir: Path,
    source_batches: list[dict],
    *,
    source: str = "expanded_from_blocks_py_batches",
) -> dict:
    """Build and persist Stage 02 runtime plan to disk."""
    plan = build_single_block_runtime_plan(source_batches, source=source)
    (output_dir / RUNTIME_BATCHES_FILE).write_text(
        json.dumps(plan, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return plan


def load_or_create_single_block_runtime_plan(
    output_dir: Path,
    source_batches: list[dict],
    *,
    force_rebuild: bool = False,
) -> dict:
    """Load existing Stage 02 runtime plan or build a fresh one."""
    runtime_path = output_dir / RUNTIME_BATCHES_FILE
    if runtime_path.exists() and not force_rebuild:
        try:
            plan = json.loads(runtime_path.read_text(encoding="utf-8"))
            if (
                plan.get("schema_version") == 1
                and plan.get("mode") == "single_block"
                and isinstance(plan.get("batches"), list)
            ):
                return plan
        except (json.JSONDecodeError, OSError):
            pass
    return write_single_block_runtime_plan(output_dir, source_batches)


def runtime_batch_failure_entry(batch: dict, error: str, *, reason: str) -> dict:
    """Build a failure entry dict for a single-block batch."""
    block = (batch.get("blocks") or [{}])[0]
    return {
        "batch_id": batch.get("batch_id"),
        "block_id": block.get("block_id"),
        "page": block.get("page"),
        "reason": reason,
        "error": error,
    }


def write_block_analysis_runtime_summary(
    output_dir: Path,
    runtime_plan: dict,
    *,
    failed_batches: list[dict],
    completed_batches: int,
) -> dict:
    """Write block_analysis_summary.json to output_dir and return the summary dict."""
    total = int(runtime_plan.get("total_batches") or len(runtime_plan.get("batches", [])))
    summary = {
        "schema_version": 1,
        "stage": "block_analysis",
        "mode": runtime_plan.get("mode", "single_block"),
        "runtime_plan_path": str(output_dir / RUNTIME_BATCHES_FILE),
        "total_batches": total,
        "completed_batches": int(completed_batches),
        "failed_batches_count": len(failed_batches),
        "failed_batches": failed_batches,
        "created_at": datetime.now().isoformat(),
    }
    (output_dir / "block_analysis_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary


def attach_stage02_coverage_to_findings(project_id: str) -> dict:
    """Attach deterministic Stage 02 coverage warnings to final findings."""
    output_dir = resolve_project_dir(project_id) / "_output"
    findings_path = output_dir / "03_findings.json"
    blocks_path = output_dir / "02_blocks_analysis.json"
    gemma_summary_path = output_dir / "gemma_enrichment_summary.json"
    block_summary_path = output_dir / "block_analysis_summary.json"

    if not findings_path.exists():
        return {}

    def _load(path: Path) -> dict:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError):
            return {}

    def _dedupe(items: list[dict], *, by_block_only: bool = False) -> list[dict]:
        seen: set[tuple[str, str]] = set()
        out: list[dict] = []
        for item in items:
            bid = str(item.get("block_id") or "")
            reason = str(item.get("reason") or item.get("coverage_status") or "")
            key = (bid, "" if by_block_only else reason)
            if not bid or key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out

    findings_data = _load(findings_path)
    if not findings_data:
        return {}

    data02 = _load(blocks_path)
    meta02 = data02.get("stage02_meta") or data02.get("meta") or {}
    block_analyses = data02.get("block_analyses") or []

    gemma_summary = _load(gemma_summary_path)
    gemma_uncovered = list(gemma_summary.get("uncovered_blocks") or [])
    if not gemma_uncovered:
        gemma_uncovered = [
            {"block_id": bid, "page": None, "reason": "gemma_enrichment_failed"}
            for bid in gemma_summary.get("uncovered_block_ids") or []
        ]
    gemma_uncovered.extend(meta02.get("uncovered_blocks") or [])
    stage02_crop_missing = list(meta02.get("stage02_crop_missing_blocks") or [])
    base_gemma_coverage = meta02.get("base_gemma_coverage") or {}
    high_detail_candidates = int(meta02.get("high_detail_candidates") or 0)
    high_detail_successful = int(meta02.get("high_detail_successful") or 0)
    high_detail_skipped_large = int(meta02.get("high_detail_skipped_large") or 0)
    base_only_blocks = [
        item if isinstance(item, dict) else {"block_id": str(item)}
        for item in (meta02.get("blocks_analyzed_only_with_100_dpi_base") or [])
    ]
    upgraded_blocks = [
        item if isinstance(item, dict) else {"block_id": str(item)}
        for item in (meta02.get("blocks_upgraded_to_300") or [])
    ]

    single_block_failed = list(meta02.get("failed_blocks") or [])
    block_summary = _load(block_summary_path)
    single_block_failed.extend(block_summary.get("failed_batches") or [])

    excluded = []
    for ba in block_analyses:
        status = ba.get("coverage_status")
        if status in {"missing_gemma_enrichment", "single_block_analysis_failed", "cancelled"}:
            excluded.append({
                "block_id": ba.get("block_id"),
                "page": ba.get("page"),
                "sheet": ba.get("sheet"),
                "reason": status,
                "details": ba.get("unreadable_details") or ba.get("_error"),
            })
    excluded.extend([
        {
            "block_id": b.get("block_id"),
            "page": b.get("page"),
            "reason": b.get("reason") or "missing_stage02_crop",
            "details": b.get("error") or "Gemma base index contains this block, but Stage 02 100 DPI crop is missing",
        }
        for b in stage02_crop_missing
    ])
    excluded.extend([
        {
            "block_id": b.get("block_id"),
            "page": b.get("page"),
            "reason": b.get("reason") or "gemma_enrichment_failed",
            "details": b.get("error"),
        }
        for b in gemma_uncovered
    ])
    excluded.extend([
        {
            "block_id": b.get("block_id"),
            "page": b.get("page"),
            "reason": b.get("reason") or "single_block_analysis_failed",
            "details": b.get("error"),
        }
        for b in single_block_failed
    ])

    gemma_uncovered = _dedupe(gemma_uncovered)
    single_block_failed = _dedupe(single_block_failed)
    excluded = _dedupe(excluded, by_block_only=True)

    coverage = {
        "schema_version": 1,
        "summary": {
            "gemma_uncovered_count": len(gemma_uncovered),
            "single_block_failed_count": len(single_block_failed),
            "stage02_crop_missing_count": len(stage02_crop_missing),
            "excluded_from_full_analysis_count": len(excluded),
            "base_gemma_covered_count": int(base_gemma_coverage.get("blocks_ok") or 0),
            "base_gemma_total_count": int(base_gemma_coverage.get("blocks_total") or 0),
            "high_detail_candidates": high_detail_candidates,
            "high_detail_successful": high_detail_successful,
            "high_detail_skipped_large": high_detail_skipped_large,
            "base_only_blocks_count": len(base_only_blocks),
            "upgraded_to_300_count": len(upgraded_blocks),
        },
        "gemma_uncovered_blocks": gemma_uncovered,
        "single_block_failed_blocks": single_block_failed,
        "stage02_crop_missing_blocks": stage02_crop_missing,
        "blocks_analyzed_only_with_100_dpi_base": base_only_blocks,
        "blocks_upgraded_to_300": upgraded_blocks,
        "excluded_blocks_from_full_analysis": excluded,
        "sections": [
            {
                "title": "Непокрытые блоки Gemma enrichment",
                "blocks": gemma_uncovered,
            },
            {
                "title": "Ошибки single-block анализа",
                "blocks": single_block_failed,
            },
            {
                "title": "Блоки, исключённые из полноценного анализа",
                "blocks": excluded,
            },
            {
                "title": "Блоки, оставшиеся на base 100 DPI",
                "blocks": base_only_blocks,
            },
            {
                "title": "Блоки, upgraded до 300 DPI",
                "blocks": upgraded_blocks,
            },
        ],
    }

    meta = findings_data.setdefault("meta", {})
    meta["analysis_coverage"] = coverage
    findings_data["analysis_coverage"] = coverage
    findings_path.write_text(
        json.dumps(findings_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return coverage


def validate_and_repair_json(file_path: Path) -> tuple[bool, str]:
    """
    Проверить JSON-файл и попытаться починить, если невалиден.

    Типичная проблема: LLM пишет неэкранированные кавычки внутри строк,
    например: "раздел ТХ.А "Технологические решения"" вместо
              "раздел ТХ.А \"Технологические решения\""

    Returns:
        (is_valid, message) — True если файл валиден (или починен).
    """
    import re

    if not file_path.exists():
        return False, "Файл не существует"

    raw = file_path.read_text(encoding="utf-8")

    # 1. Пробуем валидный JSON
    try:
        json.loads(raw)
        return True, "OK"
    except json.JSONDecodeError:
        pass

    # 2. Бэкап перед ремонтом
    backup_path = file_path.with_suffix(".json.broken")
    backup_path.write_text(raw, encoding="utf-8")

    # 3. Ремонт: заменяем неэкранированные " внутри строковых значений
    # Стратегия: ищем паттерны ": "...внутренние "кавычки"..." и экранируем
    def _fix_inner_quotes(text: str) -> str:
        """Экранировать неэкранированные кавычки внутри JSON-строк."""
        result = []
        i = 0
        in_string = False
        escape_next = False

        while i < len(text):
            ch = text[i]

            if escape_next:
                result.append(ch)
                escape_next = False
                i += 1
                continue

            if ch == '\\' and in_string:
                result.append(ch)
                escape_next = True
                i += 1
                continue

            if ch == '"':
                if not in_string:
                    in_string = True
                    result.append(ch)
                else:
                    # Это " внутри строки — конец строки или внутренняя кавычка?
                    # Смотрим что после: если , ] } : или пробел+один из них — конец строки
                    rest = text[i + 1:].lstrip()
                    if not rest or rest[0] in (',', ']', '}', ':'):
                        in_string = False
                        result.append(ch)
                    else:
                        # Внутренняя кавычка — экранируем
                        result.append('\\"')
                i += 1
                continue

            result.append(ch)
            i += 1

        return ''.join(result)

    fixed = _fix_inner_quotes(raw)

    try:
        json.loads(fixed)
        file_path.write_text(fixed, encoding="utf-8")
        return True, f"Repaired (бэкап: {backup_path.name})"
    except json.JSONDecodeError:
        pass

    # 4. Fallback: замена типографских кавычек на экранированные
    fixed2 = raw.replace('“', '\\”').replace('”', '\\”')
    fixed2 = re.sub(
        r'(?<=”: “)(.+?)(?=”[,\s\n\r]*[}\]])',
        lambda m: m.group(0).replace('”', '\\”') if '”' in m.group(0) else m.group(0),
        fixed2,
    )

    try:
        json.loads(fixed2)
        file_path.write_text(fixed2, encoding="utf-8")
        return True, f"Repaired via fallback (бэкап: {backup_path.name})"
    except json.JSONDecodeError as e:
        # Не удалось починить — возвращаем оригинал
        file_path.write_text(raw, encoding="utf-8")
        return False, f"Ремонт не удался: {e}"


# ─── run_block_analysis_findings_only ────────────────────────────────────────

async def run_block_analysis_findings_only(
    ctx: "PipelineStageContext",
    *,
    force: bool = False,
    mode: str | None = None,
) -> StageResult:
    """ЭТАП 02 в режиме findings_only_gemma_pair.

    Single-block: GPT-5.4 + gemma-enrichment + extended categories на каждый блок.
    Пишет финальный _output/02_blocks_analysis.json напрямую.
    Поддерживает cancel через cancel_event и progress через ctx.progress_sync.

    Не управляет:
    - job.stage / job.status / job.progress_total (выставляет оркестратор);
    - heartbeat / cleanup (оркестратор);
    - _assert_gemma_ready / _ensure_stage02_crops (оркестратор).
    """
    pid = ctx.project_id
    project_dir = ctx.project_dir
    output_dir = ctx.output_dir

    try:
        from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
            run_findings_only_for_project,
            check_prerequisites,
            FindingsOnlyError,
            DEFAULT_MODEL,
            DEFAULT_EFFORT,
            DEFAULT_PARALLELISM,
        )
    except ImportError as exc:
        ctx.update_pipeline_log(
            "block_analysis", "error",
            error=f"gemma_findings_only import error: {exc}",
        )
        return StageResult.fail(f"gemma_findings_only модуль не найден: {exc}")

    check = check_prerequisites(project_dir)
    for r in check.get("reasons", []):
        await ctx.log(f"  · {r}", "warn" if not check["ok"] else "info")
    if not check["ok"]:
        error = "findings_only_gemma_pair: prerequisites failed (нужен Gemma-enrichment)"
        ctx.update_pipeline_log("block_analysis", "error", error=error)
        return StageResult.fail(
            "Stage 02 (findings_only_gemma_pair): нет Gemma-обогащения. "
            "Запустите 'Подготовить данные' с Gemma-enrichment."
        )

    ui_model = get_stage_model("block_batch")
    findings_only_compatible = {"openai/gpt-5.4"}
    if ui_model in findings_only_compatible:
        model = ui_model
    else:
        model = DEFAULT_MODEL
        await ctx.log(
            f"  · UI модель block_batch={ui_model} несовместима с findings_only режимом — "
            f"используем {DEFAULT_MODEL}",
            "warn",
        )
    effort = DEFAULT_EFFORT

    ctx.update_pipeline_log("block_analysis", "running")
    await ctx.log(
        f"═══ ЭТАП 02 (findings_only_gemma_pair): "
        f"{check['with_enrichment']}/{check['blocks_total']} "
        f"блоков, model={model}, effort={effort}, parallelism={DEFAULT_PARALLELISM} ═══"
    )

    cancel_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _on_progress(event: dict) -> None:
        t = event.get("type")
        if t == "started":
            asyncio.run_coroutine_threadsafe(
                ctx.log(
                    f"  Источники enrichment: {event.get('enrichment_sources')}, "
                    f"extended={event.get('extended_prompt')}, section={event.get('section')}"
                ),
                loop,
            )
        elif t == "block_done":
            completed = event.get("completed", 0)
            total = event.get("total", 0)
            bid = event.get("block_id", "?")
            pg = event.get("page", "?")
            ms = event.get("elapsed_ms") or 0
            ok = event.get("ok")
            n = event.get("findings", 0)
            if ok:
                msg = (
                    f"  [{completed:>3}/{total}] OK {bid} p={pg} t={ms/1000:.1f}s "
                    f"findings={n} in={event.get('input_tokens')} "
                    f"out={event.get('output_tokens')} "
                    f"reason={event.get('reasoning_tokens')}"
                )
                asyncio.run_coroutine_threadsafe(ctx.log(msg), loop)
            else:
                err = (event.get("error") or "")[:80]
                asyncio.run_coroutine_threadsafe(
                    ctx.log(f"  [{completed:>3}/{total}] FAIL {bid}: {err}", "warn"),
                    loop,
                )
            if ctx.progress_sync:
                ctx.progress_sync(completed, total)
            # cancel_event is set by the caller (orchestrator sets job.status=CANCELLED);
            # here we check it via a sentinel that the orchestrator wires in.
        elif t == "block_skip":
            completed = event.get("completed")
            total = event.get("total")
            if completed and total and ctx.progress_sync:
                ctx.progress_sync(completed, total)
            asyncio.run_coroutine_threadsafe(
                ctx.log(
                    f"  SKIP {event.get('block_id')} p={event.get('page')}: "
                    f"{event.get('reason')} — блок не анализировался полноценно",
                    "warn",
                ),
                loop,
            )

    try:
        result = await run_findings_only_for_project(
            project_dir,
            model=model,
            reasoning_effort=effort,
            claude_clean_cwd=CLAUDE_BLOCK_BATCH_CLEAN_CWD,
            on_progress=_on_progress,
            cancel_event=cancel_event,
        )
    except FindingsOnlyError as e:
        ctx.update_pipeline_log(
            "block_analysis", "error",
            error=f"findings_only_gemma_pair: {e}",
        )
        return StageResult.fail(f"Stage 02 (findings_only_gemma_pair): {e}")

    summary = result["summary"]
    totals = summary["totals"]

    if summary.get("cancelled"):
        ctx.update_pipeline_log(
            "block_analysis", "error",
            error="findings_only_gemma_pair: отменено пользователем",
        )
        return StageResult.cancel()

    if summary["blocks_failed"] > 0 and summary["blocks_ok"] == 0:
        error = f"Все {summary['blocks_failed']} блоков упали"
        ctx.update_pipeline_log("block_analysis", "error", error=error)
        return StageResult.fail(f"Stage 02 (findings_only_gemma_pair): все блоки упали")

    msg = (
        f"OK ({summary['blocks_ok']}/{summary['blocks_total']} блоков, "
        f"{summary['wall_clock_s']:.0f}s, "
        f"{totals['findings']} findings, "
        f"~${totals['estimated_cost_usd_total']:.3f})"
    )
    detail = {
        "uncovered_blocks": summary.get("uncovered_blocks", []),
        "failed_blocks": summary.get("failed_blocks", []),
        "task_exceptions": summary.get("task_exceptions", []),
    }

    if summary.get("blocks_skipped_no_enrichment", 0) > 0:
        msg += f" — {summary['blocks_skipped_no_enrichment']} блоков без Gemma enrichment"
        await ctx.log(
            "  ⚠ Непокрытые Gemma enrichment блоки: "
            + ", ".join(
                b.get("block_id", "?") for b in summary.get("uncovered_blocks", [])[:20]
            ),
            "warn",
        )

    if summary["blocks_failed"] > 0:
        msg += f" — {summary['blocks_failed']} блоков упали"
        ctx.update_pipeline_log("block_analysis", "done", message=msg, detail=detail)
        await ctx.log(f"  ⚠ {msg}", "warn")
    else:
        ctx.update_pipeline_log("block_analysis", "done", message=msg, detail=detail)
        await ctx.log(f"  ✓ {msg}")

    if ctx.record_block_analysis_usage:
        ctx.record_block_analysis_usage(summary)

    return StageResult.ok(
        blocks_ok=summary["blocks_ok"],
        blocks_total=summary["blocks_total"],
        blocks_failed=summary["blocks_failed"],
        findings_count=totals["findings"],
    )
