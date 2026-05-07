"""
gemma_enrichment/runner.py
--------------------------
Stage runner для этапа gemma_enrichment (Gemma OCR enrichment MD-файла).

Содержит:
  - run_gemma_enrichment_stage(ctx, *, force) -> StageResult

Публичный API:
  run_gemma_enrichment_stage(ctx, *, force) -> StageResult

Не управляет:
  - job.stage / job.status / job.progress_total (оркестратор);
  - heartbeat / cleanup (оркестратор);
  - _assert_gemma_ready / _ensure_gemma_ready_or_run (оркестратор);
  - LM Studio model load/unload lifecycle (оркестратор).
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING

from backend.app.pipeline.stage_result import StageResult
from backend.app.pipeline.stages.crop_blocks.runner import (
    existing_crop_matches_policy,
    run_policy_recrop,
)
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BLOCKS_DIRNAME,
    gemma_blocks_index_path,
    gemma_enrichment_crop_policy,
)
from backend.app.pipeline.stages.gemma_enrichment.gemma_gate import (
    GEMMA_STAGE_LABEL,
    evaluate_gemma_enrichment,
    find_project_markdown,
)

if TYPE_CHECKING:
    from backend.app.pipeline.context import PipelineStageContext


def _project_rel_path(project_dir: Path) -> str:
    """Вернуть относительный путь к папке проекта для передачи в crop runner."""
    try:
        from backend.app.core.config import PROJECTS_DIR
        return str(project_dir.relative_to(PROJECTS_DIR))
    except Exception:
        return str(project_dir)


async def run_gemma_enrichment_stage(
    ctx: "PipelineStageContext",
    *,
    force: bool = False,
) -> StageResult:
    """ЭТАП 00: Gemma-обогащение MD-файла (после crop, до text_analysis).

    Это обязательный gate: без MD или без валидного enrichment downstream
    этапы не должны стартовать.

    Логика идентична _run_gemma_enrichment_stage из manager.py.
    Оркестраторная часть (job.stage, heartbeat, cleanup) остаётся в manager.
    """
    pid = ctx.project_id
    project_dir = ctx.project_dir
    project_info = ctx.project_info or {}

    ctx.update_pipeline_log("gemma_enrichment", "running")
    print(f"[{pid}] ═══ ЭТАП 2: {GEMMA_STAGE_LABEL} ═══")
    await ctx.log(f"═══ ЭТАП 2: {GEMMA_STAGE_LABEL} ═══")

    # ── Проверка MD-файла ──
    md_path = find_project_markdown(project_dir, project_info)
    if md_path is None:
        err = (
            f"{GEMMA_STAGE_LABEL}: MD-файл не найден. "
            "Анализ без *_document.md не поддерживается."
        )
        await ctx.log(err, "error")
        ctx.update_pipeline_log("gemma_enrichment", "error", error=err)
        return StageResult.fail(err)

    # ── Crop policy recrop при несовпадении ──
    blocks_index = gemma_blocks_index_path(project_dir)
    gemma_crop_policy = gemma_enrichment_crop_policy()
    if blocks_index.exists() and not existing_crop_matches_policy(blocks_index, gemma_crop_policy):
        _recrop_result = await run_policy_recrop(
            ctx,
            project_rel_path=_project_rel_path(project_dir),
            policy=gemma_crop_policy,
            output_dir_name=GEMMA_BLOCKS_DIRNAME,
        )
        if not _recrop_result.success:
            err = _recrop_result.error or "Gemma crop policy recrop failed"
            ctx.update_pipeline_log("gemma_enrichment", "error", error=err)
            return StageResult.fail(err)

    # ── Проверка идемпотентности (skip если уже готово) ──
    state_before = evaluate_gemma_enrichment(project_dir, project_info)
    if state_before.get("ready") and not force:
        status = (
            "partial"
            if state_before.get("status") in {"partial_allowed", "partial"}
            else "done"
        )
        msg = state_before.get("detail", "Gemma enrichment уже готов")
        await ctx.log(msg, "warn" if status == "partial" else "info")
        ctx.update_pipeline_log(
            "gemma_enrichment",
            status,
            message=msg,
            detail={
                "partial_allowed": state_before.get("status") in {"partial_allowed", "partial"},
                "blocks_ok": state_before.get("blocks_ok", 0),
                "blocks_total": state_before.get("blocks_total", 0),
            },
        )
        return StageResult.ok(skipped=True, status=status)

    # ── Импорт enrich_project ──
    try:
        from backend.app.pipeline.stages.gemma_enrichment.gemma_enrich import enrich_project
    except ImportError as exc:
        err = f"gemma_enrich модуль не найден: {exc}"
        await ctx.log(err, "error")
        ctx.update_pipeline_log("gemma_enrichment", "error", error=err)
        return StageResult.fail(err)

    # ── Progress callback для async log из main event loop ──
    main_loop = asyncio.get_running_loop()

    async def progress_cb(event: dict) -> None:
        t = event.get("type")
        if t == "started":
            await ctx.log(
                f"  Gemma enrichment: {event['total']} блоков, "
                f"model={event.get('model')}",
            )
        elif t == "block_done":
            completed = event.get("completed", 0)
            total = event.get("total", 0)
            bid = event.get("block_id", "?")
            pg = event.get("page", "?")
            ok = event.get("ok")
            ms = event.get("elapsed_ms", 0)
            if ok:
                await ctx.log(
                    f"  [{completed:>3}/{total}] OK {bid} p={pg} t={ms/1000:.1f}s"
                )
            else:
                err_msg = (event.get("error") or "")[:80]
                await ctx.log(
                    f"  [{completed:>3}/{total}] FAIL {bid} p={pg}: {err_msg}",
                    "warn",
                )
        elif t == "high_detail_candidates":
            await ctx.log(
                f"  High-detail кандидаты: {event.get('candidates', 0)} из {event.get('total', 0)} блоков",
            )
        elif t == "high_detail_prefilter":
            skipped_large = len(event.get("skipped_large_ids") or [])
            await ctx.log(
                f"  High-detail prefilter: safe={event.get('safe_candidates', 0)}, "
                f"skipped_large={skipped_large}",
                "warn" if skipped_large else "info",
            )
        elif t == "high_detail_block_done":
            completed = event.get("completed", 0)
            total = event.get("total", 0)
            bid = event.get("block_id", "?")
            pg = event.get("page", "?")
            err_msg = (event.get("error") or "")[:80]
            level = "info" if event.get("ok") else "warn"
            prefix = "OK" if event.get("ok") else "FAIL"
            tail = f": {err_msg}" if err_msg else ""
            await ctx.log(
                f"  [HD {completed:>3}/{total}] {prefix} {bid} p={pg} "
                f"t={event.get('elapsed_ms', 0)/1000:.1f}s{tail}",
                level,
            )
        elif t == "no_blocks":
            await ctx.log("  Image-блоков для enrichment не найдено", "warn")

    # ── Запуск enrich_project в отдельном thread (блокирующий asyncio.run) ──
    _is_cancelled = ctx.is_cancelled  # синхронная проверка отмены

    def _run_enrichment_in_thread() -> dict:
        thread_cancel_event = asyncio.Event()
        thread_pause_event = asyncio.Event()
        thread_pause_event.set()  # не на паузе по умолчанию

        async def _thread_progress_cb(event: dict) -> None:
            if _is_cancelled and _is_cancelled():
                thread_cancel_event.set()
            future = asyncio.run_coroutine_threadsafe(progress_cb(event), main_loop)
            await asyncio.wrap_future(future)

        async def _runner() -> dict:
            return await enrich_project(
                project_dir,
                force=force or state_before.get("status") in {"partial", "failed"},
                parallelism=1,  # gemma3.6-35b не тянет параллель
                progress_cb=_thread_progress_cb,
                pause_event=thread_pause_event,
                cancel_event=thread_cancel_event,
            )

        return asyncio.run(_runner())

    try:
        summary = await asyncio.to_thread(_run_enrichment_in_thread)
    except Exception as e:
        ctx.update_pipeline_log(
            "gemma_enrichment", "error", error=f"gemma_enrich exception: {e}"
        )
        return StageResult.fail(f"Gemma enrichment упал: {e}")

    # ── Обработка результата ──
    status = summary.get("status", "unknown")

    if status == "no_blocks":
        summary_path = project_dir / "_output" / "gemma_enrichment_summary.json"
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        await ctx.log("Image-блоков нет — Gemma stage подтверждён")
        ctx.update_pipeline_log(
            "gemma_enrichment", "done", message="image-блоков 0"
        )
        return StageResult.ok(blocks_total=0, status="no_blocks")

    if status == "failed":
        err = "Все блоки упали — Gemma endpoint недоступен?"
        ctx.update_pipeline_log("gemma_enrichment", "error", error=err)
        return StageResult.fail(
            "Gemma enrichment: все блоки упали. "
            "Проверьте CHANDRA_BASE_URL / NGROK_AUTH_USER / NGROK_AUTH_PASS."
        )

    ok_count = summary.get("blocks_ok", 0)
    total = summary.get("blocks_total", 0)
    wall = summary.get("wall_clock_s", 0)
    msg = f"OK ({ok_count}/{total} блоков, {wall:.0f}s)"

    if status == "partial":
        state_after = evaluate_gemma_enrichment(project_dir, project_info)
        msg = f"partial: {msg} — {summary.get('blocks_failed', 0)} блоков упали"
        if state_after.get("ready") and state_after.get("status") in {
            "partial_allowed", "partial"
        }:
            msg = (
                f"{msg}; partial mode допущен, "
                "непокрытые блоки будут отражены в отчёте"
            )
            ctx.update_pipeline_log(
                "gemma_enrichment",
                "partial",
                message=msg,
                detail={
                    "partial_allowed": True,
                    "blocks_ok": ok_count,
                    "blocks_total": total,
                    "blocks_failed": summary.get("blocks_failed", 0),
                },
            )
            await ctx.log(f"  ⚠ {msg}", "warn")
            return StageResult.ok(
                blocks_ok=ok_count,
                blocks_total=total,
                status="partial",
                partial_allowed=True,
            )

        ctx.update_pipeline_log("gemma_enrichment", "error", error=msg)
        return StageResult.fail(
            f"{GEMMA_STAGE_LABEL}: enrichment неполный ({ok_count}/{total}). "
            "Повторите gemma_enrichment или явно включите allow_partial_gemma_enrichment."
        )

    ctx.update_pipeline_log("gemma_enrichment", "done", message=msg)
    await ctx.log(f"  ✓ {msg}")
    return StageResult.ok(blocks_ok=ok_count, blocks_total=total, status="done")
