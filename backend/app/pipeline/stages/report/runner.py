"""
report/runner.py
----------------
Stage runner для этапа excel (генерация Excel-отчёта).

Extracted from inline excel-blocks in PipelineManager orchestrators.
Бизнес-логика не изменена; вызывается через PipelineStageContext.

Публичный API:
  run_excel_report(ctx) -> StageResult
"""
from __future__ import annotations

from pathlib import Path

from backend.app.core.config import GENERATE_EXCEL_SCRIPT
from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.stage_result import StageResult


def _error_detail(exit_code: int, stdout: str, stderr: str, max_len: int = 200) -> str:
    combined = ((stderr or "") + "\n" + (stdout or "")).strip()
    if not combined:
        return f"Exit code {exit_code}"
    lines = [ln.strip() for ln in combined.splitlines() if ln.strip()]
    msg = " | ".join(lines[-3:]) if lines else f"Exit code {exit_code}"
    return msg[:max_len] if len(msg) > max_len else msg


async def run_excel_report(ctx: PipelineStageContext) -> StageResult:
    """Сгенерировать Excel-отчёт через generate_excel_report.py.

    Управляет:
    - update_pipeline_log("excel", "running" → "done" / "error");
    - запуском GENERATE_EXCEL_SCRIPT как subprocess с AUDIT_NO_OPEN=1;
    - логированием stdout/stderr;
    - возвратом StageResult.

    Не управляет:
    - job.status / job.stage (выставляет оркестратор);
    - heartbeat / cleanup (дело оркестратора).
    """
    ctx.update_pipeline_log("excel", "running")
    await ctx.log("═══ Генерация Excel ═══")

    project_path = str(ctx.project_dir)
    args = [project_path]
    env_overrides = {
        "AUDIT_NO_OPEN": "1",
        "AUDIT_VERSION_DIR": str(ctx.project_dir),
        "AUDIT_OUTPUT_DIR": str(ctx.output_dir),
    }

    try:
        from backend.app.services.storage.projects_v2_source_resolver import is_projects_v2_version_dir
        if is_projects_v2_version_dir(ctx.project_dir):
            export_dir = Path(ctx.project_dir) / "05_export"
            export_dir.mkdir(parents=True, exist_ok=True)
            safe_project = str(ctx.project_id or "project").replace("/", "_")
            suffix = str(ctx.job_id or "latest").replace("/", "_")
            args.extend(["--out", str(export_dir / f"audit_report_{safe_project}_{suffix}.xlsx")])
    except Exception:
        pass

    exit_code, xls_out, xls_err = await ctx.run_subprocess(
        str(GENERATE_EXCEL_SCRIPT),
        args=args,
        env_overrides=env_overrides,
        on_output=ctx.log,
    )

    if exit_code == 0:
        ctx.update_pipeline_log("excel", "done", message="OK")
        return StageResult.ok()
    else:
        error = _error_detail(exit_code, xls_out or "", xls_err or "")
        ctx.update_pipeline_log("excel", "error", error=error)
        return StageResult.fail(error)
