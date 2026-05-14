"""
prepare/runner.py
-----------------
Stage runner для этапа prepare (подготовка проекта: process_project + document_graph).

Extracted from PipelineManager._run_prepare.
Бизнес-логика не изменена; оркестратор (manager.py) передаёт управление
через PipelineStageContext и читает итоговый StageResult.

Публичный API:
  run_prepare(ctx) -> StageResult
"""
from __future__ import annotations

from backend.app.core.config import (
    BASE_DIR,
    PROCESS_PROJECT_SCRIPT,
)
from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.stage_result import StageResult


def _project_rel_path(project_dir, base_dir=None) -> str:
    """Относительный путь к папке проекта от BASE_DIR (как CLI аргумент)."""
    base = base_dir or BASE_DIR
    try:
        return str(project_dir.relative_to(base))
    except ValueError:
        return str(project_dir)


async def run_prepare(ctx: PipelineStageContext) -> StageResult:
    """Запуск подготовки проекта через process_project.py.

    Управляет:
    - запуском PROCESS_PROJECT_SCRIPT как subprocess;
    - логированием stdout/stderr;
    - обновлением pipeline_log (running → done / error).

    Не управляет:
    - heartbeat (запускается оркестратором до вызова);
    - cleanup active_jobs (дело оркестратора);
    - job.status (оркестратор читает StageResult).
    """
    ctx.update_pipeline_log("prepare", "running")
    await ctx.log("Запуск подготовки проекта...")

    project_rel = _project_rel_path(ctx.project_dir)

    # process_project.py принимает только [project_dir, --force]; legacy
    # `--quality` / `--pages` он не понимает (argparse ругается → exit 2).
    # До 2026-05-14 здесь передавался `--quality standard` — endpoint
    # /api/audit/{id}/prepare падал на ровном месте.
    exit_code, stdout, stderr = await ctx.run_subprocess(
        str(PROCESS_PROJECT_SCRIPT),
        [project_rel],
        on_output=ctx.log,
    )

    if exit_code == 0:
        await ctx.log("Подготовка завершена успешно", "info")
        ctx.update_pipeline_log("prepare", "done", message="OK")
        return StageResult.ok()
    else:
        await ctx.log(f"Ошибка подготовки (код {exit_code})", "error")
        if stderr:
            await ctx.log(stderr, "error")
        error_msg = stderr or f"Exit code: {exit_code}"
        ctx.update_pipeline_log("prepare", "error", error=error_msg)
        return StageResult.fail(error_msg)
