"""API ИИ-описаний изменений «Сравнения версий» (docs/version_comparison_ai_plan.md §8)."""
from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

from backend.app.services.version_comparison import settings as vc_settings
from backend.app.services.version_comparison import store
from backend.app.services.version_comparison_ai import executor, jobs, settings, sheet_match, stage_queue
from backend.app.services.version_comparison_ai.md_report import runner as md_report
from backend.app.services.version_comparison_ai.sheet_changes import series_ai, series_summary


@asynccontextmanager
async def _lifespan(_app):
    # Очередь проектов «Описать изменения ИИ»: следующий проект запускается сам, когда закончился текущий.
    stop = threading.Event()
    thread = threading.Thread(target=series_ai.dispatch_loop, args=(stop,), name="vc-ai-series-queue", daemon=True)
    thread.start()
    # Очередь запуска этапов со страницы «Проекты и этапы»: выбранные этапы — по одному.
    stages = threading.Thread(target=stage_queue.dispatch_loop, args=(stop,), name="vc-stage-queue", daemon=True)
    stages.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=2)
        stages.join(timeout=2)


router = APIRouter(prefix="/api/version-comparison", tags=["version-comparison-ai"], lifespan=_lifespan)


def _guard(fn, *args, **kwargs):
    if not vc_settings.enabled():
        raise HTTPException(404, "Раздел «Сравнение версий» выключен")
    try:
        return fn(*args, **kwargs)
    except (store.CatalogError, jobs.JobError, stage_queue.QueueError, ValueError) as exc:
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


@router.get("/objects/{object_id}/series/{series_id}/whole-specifications/status")
async def whole_specifications_status(object_id: str, series_id: str) -> dict[str, Any]:
    from backend.app.services.version_comparison import specification_export

    from backend.app.services.version_comparison_ai import whole_specifications
    result = await run_in_threadpool(_guard, specification_export.summary_status, object_id, series_id)
    gate = await run_in_threadpool(_guard, whole_specifications.summary_gate, object_id, series_id)
    return result | {"pipeline": gate}


@router.get("/objects/{object_id}/series/{series_id}/ai-sheet-changes/summary.md")
async def series_ai_summary(object_id: str, series_id: str) -> Response:
    """Сводка изменений всех переходов серии одним Markdown-файлом (без вызова модели)."""
    from urllib.parse import quote
    from backend.app.services.version_comparison_ai import whole_specifications
    gate = await run_in_threadpool(_guard, whole_specifications.summary_gate, object_id, series_id)
    if gate and not gate["ready"]:
        raise HTTPException(409, "Общий MD ещё не готов: " + (gate.get("stage") or "процесс не завершён"))
    out = await run_in_threadpool(_guard, series_summary.build, object_id, series_id)
    return Response(out["text"].encode("utf-8"), media_type="text/markdown; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(out['filename'])}"})


@router.post("/objects/{object_id}/series/{series_id}/ai-sheet-changes/pause")
async def series_ai_pause(object_id: str, series_id: str) -> dict[str, Any]:
    """Пауза: начатые листы доводятся, новые не начинаются; очередь проектов отпускается."""
    return await run_in_threadpool(_guard, series_ai.pause, object_id, series_id)


@router.post("/objects/{object_id}/series/{series_id}/ai-sheet-changes/resume")
async def series_ai_resume(object_id: str, series_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, series_ai.resume, object_id, series_id, user=_user())


@router.get("/ai-sheet-changes/queue")
async def series_ai_queue() -> dict[str, Any]:
    """Очередь проектов «Описать изменения ИИ» на всю установку: что идёт и кто ждёт по порядку."""
    return await run_in_threadpool(_guard, series_ai.queue_status)


# ---------------------------------------------------------------- «Проверить сомнительные пары через Луну» (весь объект)

class SheetMatchStart(BaseModel):
    consent: bool = False
    project_ids: list[str] = Field(default_factory=list)
    luna_only: bool = False          # только ИИ-проверка сомнительных пар, без остальных этапов


@router.get("/objects/{object_id}/ai-sheet-match")
async def sheet_match_status(object_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, stage_queue.luna_status, object_id)


@router.post("/objects/{object_id}/ai-sheet-match")
async def sheet_match_start(object_id: str, body: SheetMatchStart) -> dict[str, Any]:
    return await run_in_threadpool(_guard, stage_queue.start_luna, object_id, consent=body.consent, user=_user(),
                                   project_ids=body.project_ids or None, luna_only=body.luna_only)


@router.post("/objects/{object_id}/ai-sheet-match/cancel")
async def sheet_match_cancel(object_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, stage_queue.cancel_luna, object_id)


@router.post("/objects/{object_id}/ai-sheet-match/pause")
async def sheet_match_pause(object_id: str) -> dict[str, Any]:
    """Пауза: начатые вопросы доводятся (ответы в кэше), новые не задаются; продолжение — повторный запуск."""
    return await run_in_threadpool(_guard, stage_queue.pause_luna, object_id)


@router.post("/objects/{object_id}/ai-sheet-match/clear")
async def sheet_match_clear(object_id: str) -> dict[str, Any]:
    """Убрать пары и подсказки Луны из матриц всех шифров объекта."""
    return await run_in_threadpool(_guard, sheet_match.clear, object_id)


# ---------------------------------------------------------------- «Аналитический отчёт (PDF)» по MD-сводке изменений

class MdReportFromSeries(BaseModel):
    consent: bool = False
    reuse: bool = True


@router.get("/md-reports/runtime")
async def md_report_runtime() -> dict[str, Any]:
    """Модель, CLI и предел объёма для отчёта по сводке — без обращения к модели."""
    return await run_in_threadpool(_guard, md_report.runtime)


@router.post("/objects/{object_id}/md-reports")
async def md_report_upload(object_id: str, file: UploadFile = File(...), project_id: str = Form(""),
                           consent: bool = Form(False), reuse: bool = Form(True)) -> dict[str, Any]:
    """Принять MD-сводку (точная копия, sha256) и запустить анализ; ``reuse=false`` — без готовых результатов."""
    raw = await file.read(md_report.settings.MAX_UPLOAD_BYTES + 1)
    return await run_in_threadpool(_guard, md_report.create, object_id, raw, file.filename or "summary.md",
                                   project_id=project_id or None, consent=consent, user=_user(), reuse=reuse)


@router.post("/objects/{object_id}/series/{series_id}/md-report")
async def md_report_from_series(object_id: str, series_id: str, body: MdReportFromSeries) -> dict[str, Any]:
    """Взять текущую «Сводку изменений (MD)» серии как вход отчёта (те же байты, что при скачивании)."""
    def go():
        from backend.app.services.version_comparison import series as vc_series
        md = series_summary.build(object_id, series_id)
        pid = vc_series.get(object_id, series_id)["project_id"]
        return md_report.create(object_id, md["text"].encode("utf-8"), md["filename"], project_id=pid,
                                consent=body.consent, user=_user(), origin={"kind": "series", "series_id": series_id},
                                reuse=body.reuse, records=md.get("records"))
    return await run_in_threadpool(_guard, go)


@router.get("/objects/{object_id}/md-reports")
async def md_report_list(object_id: str, project_id: str = "") -> list[dict[str, Any]]:
    return await run_in_threadpool(_guard, md_report.list_reports, object_id, project_id or None)


@router.get("/objects/{object_id}/md-reports/{report_id}")
async def md_report_get(object_id: str, report_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, md_report.get, object_id, report_id)


@router.post("/objects/{object_id}/md-reports/{report_id}/cancel")
async def md_report_cancel(object_id: str, report_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, md_report.cancel, object_id, report_id)


@router.post("/objects/{object_id}/md-reports/{report_id}/pause")
async def md_report_pause(object_id: str, report_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, md_report.pause, object_id, report_id)


@router.post("/objects/{object_id}/md-reports/{report_id}/resume")
async def md_report_resume(object_id: str, report_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, md_report.resume, object_id, report_id)


@router.delete("/objects/{object_id}/md-reports/{report_id}")
async def md_report_delete(object_id: str, report_id: str) -> dict[str, Any]:
    """Удалить отчёт (кроме формирующегося)."""
    return await run_in_threadpool(_guard, md_report.delete, object_id, report_id)


@router.post("/objects/{object_id}/md-reports/{report_id}/rebuild-pdf")
async def md_report_rebuild_pdf(object_id: str, report_id: str) -> dict[str, Any]:
    """Пересобрать PDF из сохранённого проверенного отчёта — без обращения к модели."""
    return await run_in_threadpool(_guard, md_report.rebuild_pdf, object_id, report_id)


@router.get("/objects/{object_id}/md-reports/{report_id}/report.pdf")
async def md_report_pdf(object_id: str, report_id: str) -> FileResponse:
    f = await run_in_threadpool(_guard, md_report.file_path, object_id, report_id, "report.pdf")
    name = await run_in_threadpool(_guard, md_report.download_name, object_id, report_id)
    return FileResponse(f, media_type="application/pdf", filename=name)


class ClientPrepareBody(BaseModel):
    consent: bool = False


@router.get("/objects/{object_id}/md-reports/{report_id}/client-prepare")
async def md_report_client_state(object_id: str, report_id: str) -> dict[str, Any]:
    """Готовность содержания для заказчика и оценка подготовки (без обращения к модели)."""
    return await run_in_threadpool(_guard, md_report.client_state, object_id, report_id)


@router.post("/objects/{object_id}/md-reports/{report_id}/client-prepare")
async def md_report_client_prepare(object_id: str, report_id: str, body: ClientPrepareBody) -> dict[str, Any]:
    """Подготовить отчёт для заказчика — отбор недостающих переходов; только с явным согласием."""
    return await run_in_threadpool(_guard, lambda: md_report.client_prepare(object_id, report_id, consent=body.consent))


@router.get("/objects/{object_id}/md-reports/{report_id}/client.pdf")
async def md_report_client_pdf(object_id: str, report_id: str) -> FileResponse:
    """PDF для заказчика: одна хронологическая таблица над тем же проверенным отчётом, без ID и реестров.
    Собирается из сохранённых данных без обращения к модели."""
    f = await run_in_threadpool(_guard, md_report.client_pdf, object_id, report_id)
    name = await run_in_threadpool(_guard, md_report.client_download_name, object_id, report_id)
    return FileResponse(f, media_type="application/pdf", filename=name)


@router.get("/objects/{object_id}/md-reports/{report_id}/report.json")
async def md_report_json(object_id: str, report_id: str) -> FileResponse:
    f = await run_in_threadpool(_guard, md_report.file_path, object_id, report_id, "report.json")
    return FileResponse(f, media_type="application/json")


@router.get("/objects/{object_id}/md-reports/{report_id}/source.md")
async def md_report_source(object_id: str, report_id: str) -> FileResponse:
    """Исходный MD отчёта — сохранённый при приёме файл (те же байты и SHA-256), не новая сводка."""
    f = await run_in_threadpool(_guard, md_report.file_path, object_id, report_id, "source.md")
    name = await run_in_threadpool(_guard, md_report.source_name, object_id, report_id)
    return FileResponse(f, media_type="text/markdown; charset=utf-8", filename=name)


@router.get("/objects/{object_id}/md-reports/{report_id}/facts.json")
async def md_report_facts(object_id: str, report_id: str) -> FileResponse:
    """Полный реестр фактов отчёта: ID, паспорт и сохранённые значения записей."""
    f = await run_in_threadpool(_guard, md_report.file_path, object_id, report_id, "facts.json")
    return FileResponse(f, media_type="application/json")


# ---------------------------------------------------------------- очередь запуска этапов («Проекты и этапы»)

class StageQueueItem(BaseModel):
    project_id: str
    stage: str
    restart: bool = False            # «Заново»: ИИ-описание с начала (готовые ответы — из сохранённых)


class StageQueueAdd(BaseModel):
    items: list[StageQueueItem] = Field(default_factory=list)
    consent: bool = False


@router.get("/objects/{object_id}/stage-queue")
async def stage_queue_status(object_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, stage_queue.status, object_id)


@router.post("/objects/{object_id}/stage-queue")
async def stage_queue_add(object_id: str, body: StageQueueAdd) -> dict[str, Any]:
    """Поставить выбранные этапы в очередь: проект за проектом, этапы проекта — по порядку столбцов."""
    return await run_in_threadpool(_guard, stage_queue.enqueue, object_id, [i.model_dump() for i in body.items],
                                   consent=body.consent, user=_user())


@router.post("/objects/{object_id}/stage-queue/pause")
async def stage_queue_pause(object_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, stage_queue.pause, object_id)


@router.post("/objects/{object_id}/stage-queue/resume")
async def stage_queue_resume(object_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, stage_queue.resume, object_id)


@router.post("/objects/{object_id}/stage-queue/clear")
async def stage_queue_clear(object_id: str) -> dict[str, Any]:
    return await run_in_threadpool(_guard, stage_queue.clear, object_id)


@router.post("/objects/{object_id}/stage-queue/remove")
async def stage_queue_remove(object_id: str, body: StageQueueItem) -> dict[str, Any]:
    return await run_in_threadpool(_guard, stage_queue.remove, object_id, body.project_id, body.stage)
