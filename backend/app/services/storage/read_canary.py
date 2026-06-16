"""
read_canary.py — opt-in read-only canary для `projects_v2` на ОТДЕЛЬНЫХ endpoint'ах.

Назначение (подготовительный этап, НЕ full cutover): подключить чтение
`projects_v2` к 1-2 безопасным GET-endpoint'ам ТОЛЬКО по ЯВНОМУ opt-in, не меняя
поведение обычных production-запросов и не трогая `AUDIT_STORAGE_BACKEND`.

КОНТРАКТ:
  * обычный запрос (без opt-in) → legacy. Этот модуль не вмешивается, ветка
    legacy остаётся байт-в-байт прежней.
  * opt-in = query `?storage=projects_v2` ИЛИ header `X-Audit-Storage: projects_v2`:
      - флаг `AUDIT_PROJECTS_V2_READ_CANARY_ENABLED` выключен → HTTP 403
        (явный отказ, НЕ тихий возврат legacy — чтобы canary нельзя было «случайно»
        включить и чтобы клиент понимал, что opt-in проигнорирован);
      - флаг включён → чтение из `projects_v2` (read-only, через ProjectsV2Adapter);
      - документ не найден в `projects_v2` → HTTP 404 canary-error
        (НЕ молчаливый fallback в legacy).
  * `AUDIT_STORAGE_BACKEND` этим модулем НЕ читается и НЕ меняется.

ГАРАНТИИ:
  * только чтение (ProjectsV2Adapter не пишет, не создаёт файлы, не делает
    fallback в legacy);
  * флаг читается на КАЖДЫЙ запрос (как у shadow API), поэтому включение/
    выключение не требует переимпорта модуля; default false → canary недоступен.

Подключён ТОЛЬКО к:
  * GET /api/projects                  → v2_projects_list()
  * GET /api/findings/{project_id}     → v2_findings(request, project_id)
Остальные endpoint'ы и UI не затронуты.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from fastapi import HTTPException
from fastapi.responses import FileResponse

_CANARY_FLAG = "AUDIT_PROJECTS_V2_READ_CANARY_ENABLED"
_DEFAULT_FLAG = "AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED"
_TRUE = {"1", "true", "yes", "on"}
_OPT_IN_VALUE = "projects_v2"
_OPT_OUT_VALUE = "legacy"

QUERY_KEY = "storage"
HEADER_KEY = "x-audit-storage"

BACKEND_LEGACY = "legacy"
BACKEND_V2 = "projects_v2"


def canary_flag_enabled() -> bool:
    """True только если оператор ЯВНО включил opt-in canary (env, default false)."""
    return (os.environ.get(_CANARY_FLAG) or "").strip().lower() in _TRUE


def default_read_enabled() -> bool:
    """True только если оператор ЯВНО включил default-read cutover (env, default false).

    Когда True — approved GET-endpoint'ы (те, что зовут resolve_read_backend) читают
    projects_v2 ПО УМОЛЧАНИЮ, без `?storage=projects_v2`. Остальные endpoint'ы и
    AUDIT_STORAGE_BACKEND не затрагиваются.
    """
    return (os.environ.get(_DEFAULT_FLAG) or "").strip().lower() in _TRUE


def opt_in_requested(query_value: Optional[str], header_value: Optional[str]) -> bool:
    """opt-in выражен query-параметром storage=projects_v2 или header'ом."""
    return ((query_value or "").strip().lower() == _OPT_IN_VALUE
            or (header_value or "").strip().lower() == _OPT_IN_VALUE)


def opt_in_from_request(request) -> bool:
    if request is None:
        return False
    q = request.query_params.get(QUERY_KEY)
    h = request.headers.get(HEADER_KEY)
    return opt_in_requested(q, h)


def storage_preference(request) -> Optional[str]:
    """Явное предпочтение storage из query/header: 'projects_v2' | 'legacy' | None."""
    if request is None:
        return None
    for v in (request.query_params.get(QUERY_KEY), request.headers.get(HEADER_KEY)):
        s = (v or "").strip().lower()
        if s == _OPT_IN_VALUE:
            return _OPT_IN_VALUE
        if s == _OPT_OUT_VALUE:
            return _OPT_OUT_VALUE
    return None


def resolve_read_backend(request) -> str:
    """Решает backend чтения для approved canary-endpoint'а: 'legacy' | 'projects_v2'.

    Приоритеты:
      1. явный `?storage=legacy` / header → LEGACY (безопасный force для отката/
         сравнения, всегда honored, без флага);
      2. явный `?storage=projects_v2` / header → V2, но gated canary-флагом
         (флаг OFF → HTTP 403, явный отказ, не silent);
      3. без явного предпочтения:
           - default-read флаг ON  → V2 (limited default read cutover);
           - default-read флаг OFF → LEGACY (обычное поведение, production не меняется).

    AUDIT_STORAGE_BACKEND здесь НЕ читается. Функцию зовут ТОЛЬКО approved
    GET-endpoint'ы, поэтому default-read распространяется только на них.
    """
    pref = storage_preference(request)
    if pref == _OPT_OUT_VALUE:
        return BACKEND_LEGACY
    if pref == _OPT_IN_VALUE:
        if not canary_flag_enabled():
            raise HTTPException(
                status_code=403,
                detail=("projects_v2 read canary disabled: set "
                        "AUDIT_PROJECTS_V2_READ_CANARY_ENABLED=true to opt in"),
            )
        return BACKEND_V2
    # нет явного предпочтения
    if default_read_enabled():
        return BACKEND_V2
    return BACKEND_LEGACY


def _adapter():
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
    return ProjectsV2Adapter()


def _resolve_version(a, doc_dir, cur, requested):
    """Сопоставить запрошенный version_id с реальным v2-id; по умолчанию current.

    Принимает как v2-форму (`v001`), так и legacy-форму (`v1` → `v001`). Неизвестный
    id → current (canary мягкий, не 500). Никакого fallback в legacy-хранилище.
    """
    if not requested:
        return cur
    vids = [v.get("version_id") for v in a.list_versions(doc_dir)]
    r = str(requested).strip().lower()
    if r in vids:
        return r
    if r.startswith("v") and r[1:].isdigit():
        cand = "v%03d" % int(r[1:])
        if cand in vids:
            return cand
    return cur


def _resolve_doc_or_404(request, project_id):
    """(adapter, doc, doc_dir, current_version) или 404. Общий резолвер canary-билдеров.

    `project_id` (legacy путь/идентификатор) → v2 document_code по basename
    (срез `(main)`); `?object_id=` уточняет объект. Не найден → 404 canary-error
    (НЕ silent fallback в legacy).
    """
    a = _adapter()
    if not a.is_available():
        raise HTTPException(status_code=404,
                            detail="projects_v2 storage not available")
    object_id = request.query_params.get("object_id") if request is not None else None
    document_code = Path(project_id).name.replace("(main)", "").strip()
    doc = a.find_document(document_code, object_id=object_id)
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=(f"projects_v2 canary: document '{document_code}' not found in "
                    "projects_v2 (no silent legacy fallback)"),
        )
    return a, doc, Path(doc["doc_dir"]), doc["current_version"]


def _req_version(request):
    return request.query_params.get("version_id") if request is not None else None


def v2_projects_list() -> dict:
    """Read-only список документов из projects_v2 (canary-ответ для /api/projects)."""
    a = _adapter()
    if not a.is_available():
        raise HTTPException(status_code=404,
                            detail="projects_v2 storage not available")
    docs = a.list_documents()
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "count": len(docs),
        "documents": docs,
    }


def v2_findings(request, project_id: str) -> dict:
    """Read-only findings/counts документа из projects_v2 (canary-ответ для findings).

    `project_id` (legacy-идентификатор/путь) резолвится в v2 document_code по
    basename. Опциональный `?object_id=` уточняет объект при неоднозначности.
    Документ не найден → 404 canary-error (НЕ fallback в legacy).
    """
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "document_code": doc["document_code"],
        "object_id": doc["object_id"],
        "object_folder": doc["object_folder"],
        "discipline": doc["discipline"],
        "version_id": ver,
        "version_count": doc["version_count"],
        "analysis_status": a.analysis_status(doc_dir, ver),
        "findings_count": a.findings_count(doc_dir, ver),
        "findings_by_severity": a.findings_by_severity(doc_dir, ver),
        "findings": a.findings_list(doc_dir, ver),
    }


# ---------------------------------------------------------------------------
# Расширенные canary-билдеры (read-only) для UI-просмотра результатов
# ---------------------------------------------------------------------------


def v2_project_details(request, project_id: str) -> dict:
    """Детали проекта/документа из projects_v2 (canary для GET /api/projects/{id})."""
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    meta = a.version_metadata(doc_dir, ver)
    art = a.latest_analysis_files(doc_dir, ver)
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "document_code": doc["document_code"],
        "object_id": doc["object_id"],
        "object_folder": doc["object_folder"],
        "discipline": doc["discipline"],
        "kind": doc.get("kind"),
        "migration_kind": doc.get("migration_kind"),
        "current_version": cur,
        "version_id": ver,
        "version_count": doc["version_count"],
        "analysis_status": meta.get("analysis_status") or a.analysis_status(doc_dir, ver),
        "has_01_text_analysis": art["has_01_text_analysis"],
        "has_02_blocks_analysis": art["has_02_blocks_analysis"],
        "has_03_findings": art["has_03_findings"],
        "findings_count": a.findings_count(doc_dir, ver),
        "findings_by_severity": a.findings_by_severity(doc_dir, ver),
        "has_pipeline_log": a.has_pipeline_log(doc_dir, ver),
        "version_metadata": meta,
    }


def v2_project_versions(request, project_id: str) -> dict:
    """Сводка версий документа из projects_v2 (canary для .../versions)."""
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    versions = []
    for v in a.list_versions(doc_dir):
        vid = v.get("version_id")
        versions.append({"version_id": vid, **a.version_metadata(doc_dir, vid)})
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "document_code": doc["document_code"],
        "current_version": cur,
        "version_count": len(versions),
        "versions": versions,
    }


def v2_finding_by_id(request, project_id: str, finding_id: str) -> dict:
    """Одно замечание по id из projects_v2 (canary для .../finding/{finding_id})."""
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    for f in a.findings_list(doc_dir, ver):
        if not isinstance(f, dict):
            continue
        fid = f.get("id") or f.get("finding_id") or f.get("number")
        if str(fid) == str(finding_id):
            return {
                "storage_backend": BACKEND_V2,
                "canary": True,
                "document_code": doc["document_code"],
                "version_id": ver,
                "finding": f,
            }
    raise HTTPException(
        status_code=404,
        detail=(f"projects_v2 canary: finding '{finding_id}' not found in document "
                f"'{doc['document_code']}' (no silent legacy fallback)"),
    )


def v2_blocks_analysis(request, project_id: str) -> dict:
    """Анализ блоков (02_blocks_analysis) из projects_v2 (canary для .../blocks/analysis)."""
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    data = a.read_blocks_analysis(doc_dir, ver)
    if data is None:
        return {
            "storage_backend": BACKEND_V2,
            "canary": True,
            "document_code": doc["document_code"],
            "version_id": ver,
            "has_02_blocks_analysis": False,
            "block_count": 0,
            "blocks_analysis": None,
        }
    blocks = data.get("blocks_reviewed") or data.get("block_analyses") or []
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "document_code": doc["document_code"],
        "version_id": ver,
        "has_02_blocks_analysis": True,
        "block_count": len(blocks),
        "blocks_analysis": data,
    }


def v2_blocks(request, project_id: str) -> dict:
    """Список image-блоков (canary для GET /api/tiles/{id}/blocks), сгруппирован по страницам.

    Зеркалит legacy-контракт. Индекс блоков не найден → 404 canary-error (без
    silent fallback в legacy).
    """
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    idx = a.read_blocks_index(doc_dir, ver)
    if idx is None:
        raise HTTPException(
            status_code=404,
            detail=(f"projects_v2 canary: blocks index not found for "
                    f"'{doc['document_code']}' (no silent legacy fallback)"),
        )
    pages_map: dict = {}
    for block in idx.get("blocks", []):
        pages_map.setdefault(block.get("page", 0), []).append(block)
    pages = [{"page_num": pn, "block_count": len(pages_map[pn]), "blocks": pages_map[pn]}
             for pn in sorted(pages_map.keys())]
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "project_id": project_id,
        "document_code": doc["document_code"],
        "version_id": ver,
        "total_blocks": idx.get("total_blocks", 0),
        "total_expected": idx.get("total_expected", 0),
        "errors": idx.get("errors", 0),
        "pages": pages,
    }


def v2_block_image(request, project_id: str, block_id: str):
    """PNG кропа блока (canary для .../blocks/image/{block_id}).

    Файл резолвится через blocks index по block_id (или по имени `block_<id>.png`)
    и ОБЯЗАТЕЛЬНО проверяется на принадлежность папке блоков версии (анти-traversal).
    Возвращает FileResponse с заголовком X-Storage-Backend: projects_v2.
    Не найден → 404 canary-error (без silent fallback).
    """
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    bd = a.blocks_dir(doc_dir, ver)
    if bd is None:
        raise HTTPException(status_code=404,
                            detail="projects_v2 canary: blocks dir not found")
    bd_resolved = bd.resolve()

    def _norm(b):
        return b[6:] if b and b.startswith("block_") else (b or "")

    # 1) точное имя файла из индекса (надёжнее, чем угадывать)
    target = None
    idx = a.read_blocks_index(doc_dir, ver) or {}
    want = _norm(block_id)
    for blk in idx.get("blocks", []):
        if _norm(blk.get("block_id", "")) == want:
            fn = blk.get("file")
            if fn:
                target = bd / fn
            break
    # 2) фолбэк на конвенцию block_<id>.png
    if target is None:
        target = bd / f"block_{want}.png"
    # анти-traversal: файл обязан лежать ВНУТРИ папки блоков версии
    try:
        target_resolved = target.resolve()
    except Exception:
        target_resolved = None
    if (target_resolved is None
            or bd_resolved not in target_resolved.parents
            or not target_resolved.is_file()):
        raise HTTPException(
            status_code=404,
            detail=(f"projects_v2 canary: block image '{block_id}' not found for "
                    f"'{doc['document_code']}' (no silent legacy fallback)"),
        )
    return FileResponse(str(target_resolved), media_type="image/png",
                        headers={"X-Storage-Backend": BACKEND_V2})


def v2_version_files(request, project_id: str, version_id: str) -> dict:
    """Список исходных файлов версии из projects_v2 01_input (canary для .../versions/{vid}/files).

    `version_id` — path-параметр endpoint'а (legacy-форма vN маппится в v00N).
    Не silent fallback: документ/версия не в v2 → 404.
    """
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, version_id)
    files = a.input_files(doc_dir, ver)
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "document_code": doc["document_code"],
        "version_id": ver,
        "file_count": len(files),
        "files": files,
    }


# ---------------------------------------------------------------------------
# UI-read completion: block-map + document pages/page (read-only)
# ---------------------------------------------------------------------------


def v2_block_map(request, project_id: str) -> dict:
    """finding_id → [block_ids] + block_info + text_evidence из projects_v2
    (canary для GET /api/findings/{id}/block-map).

    Переиспользует чистые компьют-функции findings_service (одинаковая строгая
    логика, без ложных привязок). Данных нет → корректный пустой map, не 500.
    """
    from backend.app.services.findings import findings_service as fs
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    findings = a.findings_list(doc_dir, ver)
    _bp, block_info, all_block_ids = fs.blocks_data_from_sources(
        a.read_blocks_analysis(doc_dir, ver), a.read_blocks_index(doc_dir, ver))
    block_map = fs.compute_finding_block_map(findings, all_block_ids)
    graph = a.read_document_graph(doc_dir, ver) or {}
    try:
        ocr_index = fs._build_ocr_html_index(a.input_dir(doc_dir, ver))
    except Exception:
        ocr_index = {}
    text_evidence = fs.compute_text_evidence(graph, ocr_index, findings)
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "project_id": project_id,
        "document_code": doc["document_code"],
        "version_id": ver,
        "block_map": block_map,
        "block_info": block_info,
        "text_evidence": text_evidence,
    }


def _v2_parse_pages(a, doc, doc_dir, ver):
    """(parsed_doc | None). Читает MD из v2 и парсит общим parse_md_text."""
    from backend.app.services.common import project_service as ps
    md, md_file = a.md_text(doc_dir, ver)
    if not md:
        return None
    return ps.parse_md_text(md, project_id=doc["document_code"], md_file=md_file or "")


def v2_document_pages(request, project_id: str) -> dict:
    """Оглавление MD-документа из projects_v2 (canary для GET /api/document/{id}/pages).

    Зеркалит legacy: страницы без содержимого блоков (только счётчики). MD нет
    (source_only и т.п.) → 404 canary-error (как legacy 'MD не найден'), без 500.
    """
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    parsed = _v2_parse_pages(a, doc, doc_dir, ver)
    if parsed is None:
        raise HTTPException(
            status_code=404,
            detail=(f"projects_v2 canary: MD document not found for "
                    f"'{doc['document_code']}' (no silent legacy fallback)"))
    pages_light = [{
        "page_num": p["page_num"], "sheet_info": p["sheet_info"],
        "sheet_label": p["sheet_label"], "text_blocks": p["text_blocks"],
        "image_blocks": p["image_blocks"],
    } for p in parsed["pages"]]
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "project_id": doc["document_code"],
        "md_file": parsed["md_file"],
        "total_pages": parsed["total_pages"],
        "version_id": ver,
        "pages": pages_light,
    }


def v2_document_page(request, project_id: str, page_num: int) -> dict:
    """Содержимое одной страницы MD из projects_v2 (canary для .../page/{n}).

    Зеркалит legacy: все блоки страницы. Нет MD/страницы → 404 canary, без 500.
    """
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    parsed = _v2_parse_pages(a, doc, doc_dir, ver)
    if parsed is not None:
        for page in parsed["pages"]:
            if page["page_num"] == page_num:
                return {
                    "storage_backend": BACKEND_V2,
                    "canary": True,
                    "project_id": doc["document_code"],
                    "version_id": ver,
                    "page_num": page["page_num"],
                    "sheet_info": page["sheet_info"],
                    "sheet_label": page["sheet_label"],
                    "blocks": page["blocks"],
                }
    raise HTTPException(
        status_code=404,
        detail=(f"projects_v2 canary: page {page_num} not found for "
                f"'{doc['document_code']}' (no silent legacy fallback)"))
