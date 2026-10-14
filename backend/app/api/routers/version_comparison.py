"""API раздела «Сравнение версий» (план docs/version_comparison_plan.md §3.5)."""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

from backend.app.services.common import object_service
from backend.app.services.version_comparison import jobs, paths, profile, series, settings, store, tiles

router = APIRouter(prefix="/api/version-comparison", tags=["version-comparison"])


def _guard(fn, *args, **kwargs):
    if not settings.enabled():
        raise HTTPException(404, "Раздел «Сравнение версий» выключен")
    try:
        return fn(*args, **kwargs)
    except store.CatalogError as exc:
        raise HTTPException(400, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


class ConfirmFile(BaseModel):
    file_key: str
    label: str = ""


class ConfirmProject(BaseModel):
    project_id: str | None = None
    title: str = ""
    code: str = ""
    files: list[ConfirmFile] = Field(default_factory=list)


class ConfirmRequest(BaseModel):
    projects: list[ConfirmProject]


class OrderRequest(BaseModel):
    version_ids: list[str]


class LabelRequest(BaseModel):
    label: str


class TitleRequest(BaseModel):
    title: str


class LinkEdit(BaseModel):
    op: Literal["move", "detach", "absent"]
    page: list[Any] | None = None
    to_row_anchor: list[Any] | None = None
    row_anchor: list[Any] | None = None
    version: str | None = None


class SheetPair(BaseModel):
    a_page: int = Field(ge=1)
    b_page: int = Field(ge=1)


class RunRequest(BaseModel):
    a_version: str
    b_version: str
    scope: Literal["all", "manual"] = "all"
    sheet_pairs: list[SheetPair] = Field(default_factory=list)


@router.get("/objects")
def list_objects() -> dict[str, Any]:
    def run():
        items = []
        for obj in object_service.list_objects():
            oid = str(obj.get("id") or "")
            if not oid:
                continue
            try:
                cat = store.load_catalog(oid)
            except ValueError:
                continue
            items.append({"id": oid, "name": str(obj.get("name") or oid),
                          "projects": len(cat["projects"]), "versions": len(cat["versions"])})
        items.sort(key=lambda i: i["name"].casefold())
        return {"items": items, "storage_root": str(paths.root())}
    return _guard(run)


@router.get("/objects/{object_id}/catalog")
def get_catalog(object_id: str) -> dict[str, Any]:
    def run():
        cat = store.load_catalog(object_id)
        projects = []
        for p in cat["projects"]:
            versions = [cat["versions"][v] for v in p["versions"]]
            projects.append(p | {"version_records": [v | {"profile": jobs.profile_status(object_id, v["sha256"])}
                                                     for v in versions]})
        return {"object_id": object_id, "projects": projects}
    return _guard(run)


@router.post("/objects/{object_id}/uploads")
def create_upload(object_id: str) -> dict[str, Any]:
    return _guard(lambda: {"upload_id": store.create_upload(object_id)})


@router.post("/objects/{object_id}/uploads/{upload_id}/files")
async def upload_file(object_id: str, upload_id: str, file: UploadFile = File(...),
                      relative_path: str = Form("")) -> dict[str, Any]:
    entries = await run_in_threadpool(_guard, store.add_file, object_id, upload_id,
                                      file.filename or "file.pdf", relative_path, file.file)
    return {"files": entries}


@router.get("/objects/{object_id}/uploads/{upload_id}/proposal")
def upload_proposal(object_id: str, upload_id: str) -> dict[str, Any]:
    return _guard(store.proposal, object_id, upload_id)


@router.post("/objects/{object_id}/uploads/{upload_id}/confirm")
def confirm_upload(object_id: str, upload_id: str, body: ConfirmRequest) -> dict[str, Any]:
    def run():
        res = store.confirm(object_id, upload_id, body.model_dump())
        cat = res["catalog"]
        jobs.ensure_profiles(object_id, [cat["versions"][v]["sha256"] for v in res["created_versions"]])
        return {"created_versions": res["created_versions"]}
    return _guard(run)


@router.delete("/objects/{object_id}/uploads/{upload_id}")
def cancel_upload(object_id: str, upload_id: str) -> dict[str, Any]:
    def run():
        shutil.rmtree(paths.upload_dir(object_id, upload_id), ignore_errors=True)
        return {"ok": True}
    return _guard(run)


@router.put("/objects/{object_id}/projects/{project_id}/order")
def reorder(object_id: str, project_id: str, body: OrderRequest) -> dict[str, Any]:
    return _guard(lambda: {"ok": bool(store.reorder(object_id, project_id, body.version_ids))})


@router.patch("/objects/{object_id}/projects/{project_id}")
def rename_project(object_id: str, project_id: str, body: TitleRequest) -> dict[str, Any]:
    return _guard(lambda: {"ok": bool(store.update_project(object_id, project_id, title=body.title))})


@router.patch("/objects/{object_id}/versions/{version_id}")
def relabel(object_id: str, version_id: str, body: LabelRequest) -> dict[str, Any]:
    return _guard(lambda: {"ok": bool(store.update_version(object_id, version_id, label=body.label))})


@router.delete("/objects/{object_id}/versions/{version_id}")
def remove_version(object_id: str, version_id: str) -> dict[str, Any]:
    return _guard(lambda: {"ok": bool(store.remove_version(object_id, version_id))})


@router.post("/objects/{object_id}/versions/{version_id}/profile/retry")
def retry_profile(object_id: str, version_id: str) -> dict[str, Any]:
    def run():
        v = store.load_catalog(object_id)["versions"].get(version_id)
        if not v:
            raise store.CatalogError("версия не найдена")
        jobs.retry_profile(object_id, v["sha256"])
        return {"ok": True}
    return _guard(run)


@router.get("/objects/{object_id}/projects/{project_id}/matrix")
def get_matrix(object_id: str, project_id: str) -> dict[str, Any]:
    return _guard(jobs.matrix, object_id, project_id)


@router.post("/objects/{object_id}/projects/{project_id}/links")
def edit_links(object_id: str, project_id: str, body: LinkEdit) -> dict[str, Any]:
    return _guard(jobs.add_link_edit, object_id, project_id, body.model_dump(exclude_none=True))


@router.post("/objects/{object_id}/projects/{project_id}/links/undo")
def undo_links(object_id: str, project_id: str) -> dict[str, Any]:
    return _guard(jobs.undo_link_edit, object_id, project_id)


@router.get("/objects/{object_id}/sources/{sha}/pages/{page}/thumb")
def page_thumb(object_id: str, sha: str, page: int, w: int = Query(220, ge=64, le=800)):
    def run():
        if not paths.source_pdf(object_id, sha).exists():
            raise store.CatalogError("файл не найден")
        return profile.thumbnail(object_id, sha, page, w)
    path = _guard(run)
    return FileResponse(path, media_type="image/png", headers={"Cache-Control": "private, max-age=86400"})


@router.post("/objects/{object_id}/projects/{project_id}/runs")
def create_run(object_id: str, project_id: str, body: RunRequest) -> dict[str, Any]:
    return _guard(jobs.create_run, object_id, project_id, body.a_version, body.b_version, body.scope,
                  [sp.model_dump() for sp in body.sheet_pairs])


@router.get("/objects/{object_id}/projects/{project_id}/runs")
def list_runs(object_id: str, project_id: str) -> dict[str, Any]:
    return _guard(lambda: {"runs": jobs.list_runs(object_id, project_id)})


@router.get("/objects/{object_id}/runs/{run_id}")
def get_run(object_id: str, run_id: str) -> dict[str, Any]:
    return _guard(jobs.get_run, object_id, run_id)


@router.post("/objects/{object_id}/runs/{run_id}/cancel")
def cancel_run(object_id: str, run_id: str) -> dict[str, Any]:
    return _guard(jobs.cancel_run, object_id, run_id)


@router.post("/objects/{object_id}/runs/{run_id}/resume")
def resume_run(object_id: str, run_id: str) -> dict[str, Any]:
    return _guard(jobs.resume_run, object_id, run_id)


# --------------------------------------------- «Сравнить все соседние» (серии)

@router.post("/objects/{object_id}/projects/{project_id}/series")
def create_series(object_id: str, project_id: str) -> dict[str, Any]:
    return _guard(series.create, object_id, project_id)


@router.get("/objects/{object_id}/projects/{project_id}/series")
def list_series(object_id: str, project_id: str) -> dict[str, Any]:
    return {"series": _guard(series.list_series, object_id, project_id)}


@router.get("/objects/{object_id}/series/{series_id}")
def get_series(object_id: str, series_id: str) -> dict[str, Any]:
    return _guard(series.get, object_id, series_id)


@router.post("/objects/{object_id}/series/{series_id}/cancel")
def cancel_series(object_id: str, series_id: str) -> dict[str, Any]:
    return _guard(series.cancel, object_id, series_id)


@router.post("/objects/{object_id}/series/{series_id}/resume")
def resume_series(object_id: str, series_id: str) -> dict[str, Any]:
    return _guard(series.resume, object_id, series_id)


@router.get("/objects/{object_id}/runs/{run_id}/sheets/{spid}")
def sheet_result(object_id: str, run_id: str, spid: str) -> dict[str, Any]:
    data, _ = _guard(jobs.sheet_result, object_id, run_id, spid)
    return data


@router.get("/objects/{object_id}/runs/{run_id}/sheets/{spid}/preview.png")
def sheet_preview(object_id: str, run_id: str, spid: str):
    _, rdir = _guard(jobs.sheet_result, object_id, run_id, spid)
    if rdir is None or not (Path(rdir) / "preview.png").exists():
        raise HTTPException(404, "превью нет")
    return FileResponse(Path(rdir) / "preview.png", media_type="image/png")


@router.get("/objects/{object_id}/runs/{run_id}/sheets/{spid}/tile")
def sheet_tile(object_id: str, run_id: str, spid: str, mode: Literal["overlay", "a", "b"] = "overlay",
               level: int = Query(0, ge=0, le=12), x: int = Query(0, ge=0), y: int = Query(0, ge=0),
               layers: str = ""):
    def run():
        data, rdir = jobs.sheet_result(object_id, run_id, spid)
        result = data.get("result")
        if not result or rdir is None:
            raise store.CatalogError("результата листа нет")
        run_ = jobs.get_run(object_id, run_id)
        canvas = result["canvas"]
        if mode == "overlay":
            if result.get("identical_content"):
                pdf = paths.source_pdf(object_id, run_["a"]["sha256"])
                return tiles.page_tile(pdf, data["item"]["a_page"], canvas, None, level, x, y, gray_overlay=True)
            names = tuple(n for n in layers.split(",") if n) or tiles.DEFAULT_LAYERS
            return tiles.overlay_tile(Path(rdir), level, x, y, names)
        side = run_[mode]
        pdf = paths.source_pdf(object_id, side["sha256"])
        page = data["item"]["a_page" if mode == "a" else "b_page"]
        matrix = None if mode == "a" else result["transform"]["matrix"]
        return tiles.page_tile(pdf, page, canvas, matrix, level, x, y)
    png = _guard(run)
    if png is None:
        raise HTTPException(404, "тайл вне листа")
    return Response(png, media_type="image/png", headers={"Cache-Control": "private, max-age=3600"})
