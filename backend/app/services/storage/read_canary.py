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
    doc_dir = Path(doc["doc_dir"])
    cur = doc["current_version"]
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "document_code": doc["document_code"],
        "object_id": doc["object_id"],
        "object_folder": doc["object_folder"],
        "discipline": doc["discipline"],
        "version_id": cur,
        "version_count": doc["version_count"],
        "analysis_status": a.analysis_status(doc_dir, cur),
        "findings_count": a.findings_count(doc_dir, cur),
        "findings_by_severity": a.findings_by_severity(doc_dir, cur),
        "findings": a.findings_list(doc_dir, cur),
    }
