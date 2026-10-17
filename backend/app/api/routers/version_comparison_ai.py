"""API ИИ-описаний изменений «Сравнения версий» (docs/version_comparison_ai_plan.md §8)."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

from backend.app.services.version_comparison import settings as vc_settings
from backend.app.services.version_comparison import store
from backend.app.services.version_comparison_ai import executor, jobs, settings
from backend.app.services.version_comparison_ai.sheet_changes import series_ai, series_summary

router = APIRouter(prefix="/api/version-comparison", tags=["version-comparison-ai"])


def _guard(fn, *args, **kwargs):
    if not vc_settings.enabled():
        raise HTTPException(404, "Раздел «Сравнение версий» выключен")
    try:
        return fn(*args, **kwargs)
    except (store.CatalogError, jobs.JobError, ValueError) as exc:
        raise HTTPException(400, str(exc)) from exc


def _user() -> str | None:
    try:
        from backend.app.services.common.user_service import get_current_user
        u = get_current_user() or {}
        return u.get("name") or u.get("login") or u.get("id")
    except Exception:
        return None


class JobRequest(BaseModel):
    spids: list[str] = Field(default_factory=list)
    consent: bool = False
    method: str = "records"


class EstimateRequest(BaseModel):
    spids: list[str] = Field(default_factory=list)
    method: str = "records"


@router.get("/ai/settings")
def ai_settings() -> dict[str, Any]:
    snap = settings.snapshot()
    try:
        snap["executor_runtime"] = executor.get().describe_runtime()
    except ValueError as exc:
        snap["executor_runtime"] = {"error": str(exc)}
    if settings.enabled():
        # Офлайн-проверка CLI исполнителя в окружении службы: ни одного обращения к модели.
        try:
            chk = executor.get().validate_runtime()
            snap["executor_check"] = {k: chk.get(k) for k in ("ok", "problems", "binaries")}
        except Exception as exc:  # noqa: BLE001
            snap["executor_check"] = {"ok": False, "problems": [f"{type(exc).__name__}: {exc}"]}
    snap["transfer_notice"] = ("Текст записей, контекст строк и вырезки страниц выбранных листов будут переданы "
                               "исполнителю через Codex на подписке сервера. Секреты в пакеты не входят.")
    return snap


@router.post("/objects/{object_id}/runs/{run_id}/ai/estimate")
async def ai_estimate(object_id: str, run_id: str, body: EstimateRequest) -> dict[str, Any]:
    if body.method == "sheet_changes":
        return await run_in_threadpool(_guard, jobs.estimate_sheet_changes, object_id, run_id, body.spids or None)
    return await run_in_threadpool(_guard, jobs.estimate, object_id, run_id, body.spids or None)


@router.post("/objects/{object_id}/runs/{run_id}/ai/jobs")
async def ai_create(object_id: str, run_id: str, body: JobRequest) -> dict[str, Any]:
    return await run_in_threadpool(_guard, jobs.create_job, object_id, run_id, spids=body.spids or None,
                                   consent=body.consent, user=_user(), method=body.method)


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs")
def ai_list(object_id: str, run_id: str) -> list[dict[str, Any]]:
    return _guard(jobs.list_jobs, object_id, run_id)


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}")
def ai_get(object_id: str, run_id: str, job_id: str) -> dict[str, Any]:
    return _guard(jobs.get_job, object_id, run_id, job_id)


@router.post("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/cancel")
def ai_cancel(object_id: str, run_id: str, job_id: str) -> dict[str, Any]:
    return _guard(jobs.cancel_job, object_id, run_id, job_id)


@router.post("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/resume")
def ai_resume(object_id: str, run_id: str, job_id: str) -> dict[str, Any]:
    return _guard(jobs.resume_job, object_id, run_id, job_id)


@router.post("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/retry")
def ai_retry(object_id: str, run_id: str, job_id: str) -> dict[str, Any]:
    return _guard(jobs.resume_job, object_id, run_id, job_id, retry_failed=True)


@router.get("/objects/{object_id}/runs/{run_id}/ai/latest")
def ai_latest(object_id: str, run_id: str) -> dict[str, Any]:
    """Последнее задание прогона (или null) — открыть сохранённое без вызова модели."""
    return {"job": _guard(jobs.latest_job, object_id, run_id), "enabled": settings.enabled()}


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/view")
def ai_view(object_id: str, run_id: str, job_id: str) -> dict[str, Any]:
    from backend.app.services.version_comparison_ai import view
    return _guard(view.job_view, object_id, run_id, job_id)


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/view.md")
def ai_view_md(object_id: str, run_id: str, job_id: str):
    from fastapi.responses import Response
    from backend.app.services.version_comparison_ai import view
    v = _guard(view.job_view, object_id, run_id, job_id)
    name = f"ИИ-описание {job_id}.md"
    from urllib.parse import quote
    return Response(view.view_md(v), media_type="text/markdown; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}"})


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/report")
def ai_report(object_id: str, run_id: str, job_id: str):
    f = _guard(jobs.report_files, object_id, run_id, job_id)["json"]
    if not f.exists():
        raise HTTPException(404, "отчёт ещё не готов")
    return FileResponse(f, media_type="application/json")


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/report.md")
def ai_report_md(object_id: str, run_id: str, job_id: str):
    f = _guard(jobs.report_files, object_id, run_id, job_id)["md"]
    if not f.exists():
        raise HTTPException(404, "отчёт ещё не готов")
    return FileResponse(f, media_type="text/markdown; charset=utf-8", filename=f"ИИ-описание {job_id}.md")


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/sheets/{spid}")
def ai_sheet(object_id: str, run_id: str, job_id: str, spid: str) -> dict[str, Any]:
    return _guard(jobs.sheet, object_id, run_id, job_id, spid)


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/sheets/{spid}/focus")
def ai_focus(object_id: str, run_id: str, job_id: str, spid: str, ids: str = "") -> dict[str, Any]:
    """Рамки доказательств пункта для просмотрщика: по ID на текущем результате движка."""
    wanted = [i.strip() for i in ids.split(",") if i.strip()]
    return _guard(jobs.focus, object_id, run_id, job_id, spid, wanted)


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/sheets/{spid}/ref-focus")
def ai_ref_focus(object_id: str, run_id: str, job_id: str, spid: str, ref: str) -> dict[str, Any]:
    """Переход к строке СО или другому листу: X… из пакета или V:<пункт>:<объект> из слоя проверки."""
    return _guard(jobs.ref_focus, object_id, run_id, job_id, spid, ref)


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/sheets/{spid}/check-crop/{name}")
def ai_check_crop(object_id: str, run_id: str, job_id: str, spid: str, name: str):
    """Увеличенная вырезка слоя проверки (для человека; модели не отправлялась)."""
    from backend.app.services.version_comparison_ai import verify
    f = _guard(verify.crop_file, jobs.sheet_dir(object_id, run_id, job_id, spid), name)
    if not f or not f.exists():
        raise HTTPException(404, "вырезки нет")
    return FileResponse(f, media_type="image/png")


@router.get("/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/sheets/{spid}/packet/{name:path}")
def ai_packet_file(object_id: str, run_id: str, job_id: str, spid: str, name: str):
    """Что именно было отправлено исполнителю: MD, изображения, манифест."""
    f = _guard(jobs.packet_file, object_id, run_id, job_id, spid, name)
    if not f.exists():
        raise HTTPException(404, "файла нет")
    media = "image/png" if name.endswith(".png") else ("application/json" if name.endswith(".json")
                                                        else "text/markdown; charset=utf-8")
    return FileResponse(f, media_type=media)


# ------------------------------------------------------------------ «Изменения в листе» (способ sheet_changes)
# Все GET — только чтение сохранённого: модель не вызывается (docs/version_comparison_sheet_changes_plan.md §6).

@router.get("/objects/{object_id}/runs/{run_id}/sheets/{spid}/changes")
def sheet_changes_panel(object_id: str, run_id: str, spid: str) -> dict[str, Any]:
    from backend.app.services.version_comparison_ai.sheet_changes import view as sc_view
    return _guard(sc_view.panel, object_id, run_id, spid)


@router.get("/objects/{object_id}/runs/{run_id}/sheets/{spid}/changes/focus")
def sheet_changes_focus(object_id: str, run_id: str, spid: str, item: str) -> dict[str, Any]:
    from backend.app.services.version_comparison_ai.sheet_changes import view as sc_view
    return _guard(sc_view.focus, object_id, run_id, spid, item)


@router.get("/objects/{object_id}/runs/{run_id}/sheets/{spid}/changes.md")
def sheet_changes_md(object_id: str, run_id: str, spid: str):
    from urllib.parse import quote

    from fastapi.responses import Response

    from backend.app.services.version_comparison_ai.sheet_changes import view as sc_view
    text = _guard(sc_view.md, object_id, run_id, spid)
    return Response(text, media_type="text/markdown; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(f'Изменения в листе {spid}.md')}"})


@router.get("/objects/{object_id}/runs/{run_id}/sheets/{spid}/changes/packet/{name:path}")
def sheet_changes_packet(object_id: str, run_id: str, spid: str, name: str):
    """Что было отправлено исполнителю для последнего описания листа: текст, изображения, манифест."""
    from backend.app.services.version_comparison_ai.sheet_changes import view as sc_view
    f = _guard(sc_view.packet_file, object_id, run_id, spid, name)
    if not f.exists():
        raise HTTPException(404, "файла нет")
    media = "image/png" if name.endswith(".png") else ("application/json" if name.endswith(".json")
                                                        else "text/markdown; charset=utf-8")
    return FileResponse(f, media_type=media)


# ---------------------------------------------------------------- «Описать изменения ИИ» по всем соседним переходам

class SeriesStart(BaseModel):
    consent: bool = False


@router.get("/objects/{object_id}/series/{series_id}/ai-sheet-changes/estimate")
async def series_ai_estimate(object_id: str, series_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, series_ai.estimate, object_id, series_id)


@router.post("/objects/{object_id}/series/{series_id}/ai-sheet-changes")
async def series_ai_start(object_id: str, series_id: str, body: SeriesStart) -> dict[str, Any]:
    return await run_in_threadpool(_guard, series_ai.start, object_id, series_id, consent=body.consent, user=_user())


@router.get("/objects/{object_id}/series/{series_id}/ai-sheet-changes")
async def series_ai_status(object_id: str, series_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, series_ai.status, object_id, series_id)


@router.post("/objects/{object_id}/series/{series_id}/ai-sheet-changes/cancel")
async def series_ai_cancel(object_id: str, series_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, series_ai.cancel, object_id, series_id)


@router.get("/objects/{object_id}/series/{series_id}/ai-sheet-changes/summary.md")
async def series_ai_summary(object_id: str, series_id: str) -> Response:
    """Сводка изменений всех переходов серии одним Markdown-файлом (без вызова модели)."""
    from urllib.parse import quote
    out = await run_in_threadpool(_guard, series_summary.build, object_id, series_id)
    return Response(out["text"].encode("utf-8"), media_type="text/markdown; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(out['filename'])}"})


@router.post("/objects/{object_id}/series/{series_id}/ai-sheet-changes/resume")
async def series_ai_resume(object_id: str, series_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, series_ai.resume, object_id, series_id)
