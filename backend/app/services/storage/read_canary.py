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

_CANARY_FLAG = "AUDIT_PROJECTS_V2_READ_CANARY_ENABLED"
_TRUE = {"1", "true", "yes", "on"}
_OPT_IN_VALUE = "projects_v2"

QUERY_KEY = "storage"
HEADER_KEY = "x-audit-storage"

BACKEND_LEGACY = "legacy"
BACKEND_V2 = "projects_v2"


def canary_flag_enabled() -> bool:
    """True только если оператор ЯВНО включил canary (env, default false)."""
    return (os.environ.get(_CANARY_FLAG) or "").strip().lower() in _TRUE


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


def resolve_read_backend(request) -> str:
    """'legacy' (нет opt-in) | 'projects_v2' (opt-in + флаг).

    Поднимает 403, если opt-in запрошен, но canary-флаг выключен. Без opt-in —
    всегда 'legacy' (никакого 403, обычные запросы не затрагиваются).
    """
    if not opt_in_from_request(request):
        return BACKEND_LEGACY
    if not canary_flag_enabled():
        raise HTTPException(
            status_code=403,
            detail=("projects_v2 read canary disabled: set "
                    "AUDIT_PROJECTS_V2_READ_CANARY_ENABLED=true to opt in"),
        )
    return BACKEND_V2


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
