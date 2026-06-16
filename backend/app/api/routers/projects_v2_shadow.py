"""
projects_v2_shadow.py — READ-ONLY shadow API над новым хранилищем `projects_v2`.

Назначение (подготовительный этап, НЕ cutover): дать возможность ПРОВЕРИТЬ чтение
`projects_v2` через backend, НЕ подключая его к основному UI/API.

ЖЁСТКИЕ ГАРАНТИИ:
  * все endpoint'ы — только чтение через `ProjectsV2Adapter` (адаптер не пишет,
    не создаёт файлы, не меняет metadata, не делает fallback в legacy);
  * по умолчанию ВЫКЛЮЧЕНО флагом `AUDIT_PROJECTS_V2_SHADOW_API_ENABLED=false` →
    каждый endpoint возвращает 404 (как будто его нет), production не меняется;
  * роутер можно безопасно include'ить в app: при выключенном флаге он инертен.

Флаг читается на КАЖДЫЙ запрос (а не при импорте), поэтому включение/выключение
не требует переимпорта модуля и корректно тестируется.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.services.storage.projects_v2_adapter import (
    ProjectsV2Adapter,
    get_storage_backend,
)

router = APIRouter(prefix="/api/projects-v2-shadow", tags=["projects-v2-shadow"])

_SHADOW_FLAG = "AUDIT_PROJECTS_V2_SHADOW_API_ENABLED"
_TRUE = {"1", "true", "yes", "on"}


def shadow_api_enabled() -> bool:
    """True только если оператор ЯВНО включил shadow API (env, default false)."""
    return (os.environ.get(_SHADOW_FLAG) or "").strip().lower() in _TRUE


def _gate() -> None:
    """Dependency: при выключенном флаге endpoint ведёт себя как несуществующий (404)."""
    if not shadow_api_enabled():
        raise HTTPException(status_code=404, detail="projects_v2 shadow API disabled")


def _adapter() -> ProjectsV2Adapter:
    return ProjectsV2Adapter()


def _resolve_doc(adapter: ProjectsV2Adapter, ident: str,
                 object_folder: Optional[str], discipline: Optional[str]) -> Optional[dict]:
    """Находит документ по document_code (опц. object_folder/discipline)."""
    for d in adapter.list_documents(object_folder, discipline):
        if d["document_code"] == ident:
            return d
    return None


# ---------------------------------------------------------------------------
# endpoints (все read-only, gated флагом)
# ---------------------------------------------------------------------------


@router.get("/health", dependencies=[Depends(_gate)])
async def health():
    a = _adapter()
    available = a.is_available()
    objs = a.list_objects() if available else []
    docs = a.list_documents() if available else []
    return {
        "status": "ok",
        "shadow_api_enabled": True,
        "storage_backend_default": get_storage_backend(),
        "read_only": True,
        "v2_root": str(a.v2_root),
        "adapter_available": available,
        "object_count": len(objs),
        "document_count": len(docs),
    }


@router.get("/objects", dependencies=[Depends(_gate)])
async def list_objects():
    a = _adapter()
    objs = a.list_objects()
    return {"count": len(objs), "objects": objs}


@router.get("/documents", dependencies=[Depends(_gate)])
async def list_documents(
    object_folder: Optional[str] = Query(None),
    discipline: Optional[str] = Query(None),
    analysis_status: Optional[str] = Query(None, description="фильтр по статусу текущей версии"),
    limit: Optional[int] = Query(None, ge=1),
):
    a = _adapter()
    docs = a.list_documents(object_folder, discipline)
    if analysis_status:
        def cur_status(d):
            cur = d.get("current_version")
            return a.analysis_status(Path(d["doc_dir"]), cur) if cur else None
        docs = [d for d in docs if cur_status(d) == analysis_status]
    total = len(docs)
    if limit:
        docs = docs[:limit]
    return {"count": len(docs), "total": total, "documents": docs}


@router.get("/documents/{document_id_or_code:path}/versions", dependencies=[Depends(_gate)])
async def document_versions(
    document_id_or_code: str,
    object_folder: Optional[str] = Query(None),
    discipline: Optional[str] = Query(None),
):
    a = _adapter()
    doc = _resolve_doc(a, document_id_or_code, object_folder, discipline)
    if doc is None:
        raise HTTPException(status_code=404, detail="document not found in projects_v2")
    doc_dir = Path(doc["doc_dir"])
    versions = []
    for v in a.list_versions(doc_dir):
        vid = v.get("version_id")
        versions.append({"version_id": vid, **a.version_metadata(doc_dir, vid)})
    return {"document_code": doc["document_code"],
            "current_version": doc["current_version"],
            "version_count": len(versions), "versions": versions}


@router.get("/documents/{document_id_or_code:path}/snapshot", dependencies=[Depends(_gate)])
async def document_snapshot(
    document_id_or_code: str,
    object_folder: Optional[str] = Query(None),
    discipline: Optional[str] = Query(None),
):
    a = _adapter()
    doc = _resolve_doc(a, document_id_or_code, object_folder, discipline)
    if doc is None:
        raise HTTPException(status_code=404, detail="document not found in projects_v2")
    snap = a.document_snapshot(doc["object_folder"], doc["discipline"], doc["document_code"])
    if snap is None:
        raise HTTPException(status_code=404, detail="snapshot unavailable")
    return snap


@router.get("/documents/{document_id_or_code:path}", dependencies=[Depends(_gate)])
async def get_document(
    document_id_or_code: str,
    object_folder: Optional[str] = Query(None),
    discipline: Optional[str] = Query(None),
):
    a = _adapter()
    doc = _resolve_doc(a, document_id_or_code, object_folder, discipline)
    if doc is None:
        raise HTTPException(status_code=404, detail="document not found in projects_v2")
    full = a.get_document(doc["object_folder"], doc["discipline"], doc["document_code"])
    return full


@router.get("/parity/sample", dependencies=[Depends(_gate)])
async def parity_sample():
    """Возвращает СУЩЕСТВУЮЩИЙ parity-отчёт (read-only, не пересчитывает)."""
    a = _adapter()
    rep = a.v2_root / "_system" / "backend_parity_report.json"
    if not rep.is_file():
        return {"available": False,
                "hint": "run scripts/projects_v2/check_backend_parity.py"}
    import json
    try:
        data = json.loads(rep.read_text(encoding="utf-8"))
    except Exception:
        return {"available": False, "error": "parity report unreadable"}
    return {
        "available": True,
        "generated_at": data.get("generated_at"),
        "documents_checked": data.get("documents_checked"),
        "by_type": data.get("by_type"),
        "passed": data.get("passed"),
        "failed": data.get("failed"),
        "parity_ok": data.get("parity_ok"),
        "findings_no_loss_overall": data.get("findings_no_loss_overall"),
        "total_v2_findings": data.get("total_v2_findings"),
        "total_legacy_findings": data.get("total_legacy_findings"),
        "results": [
            {"document_code": r.get("document_code"), "type": r.get("type"),
             "ok": r.get("ok")}
            for r in data.get("results", [])
        ],
    }


def _doc_kind(adapter: ProjectsV2Adapter, doc: dict) -> str:
    if doc.get("migration_kind") == "legacy_findings_preserve":
        return "king_sons_legacy_preserve"
    if doc.get("version_count", 0) > 1:
        return "versioned"
    cur = doc.get("current_version")
    return (adapter.analysis_status(Path(doc["doc_dir"]), cur) if cur else None) or "none"


def _v2_contract_view(adapter: ProjectsV2Adapter, doc: dict, objects_by_folder: dict) -> dict:
    """UI/API-контракт ТОЛЬКО по данным v2 (без legacy). Read-only."""
    doc_dir = Path(doc["doc_dir"])
    cur = doc["current_version"]
    meta = adapter.version_metadata(doc_dir, cur)
    art = adapter.latest_analysis_files(doc_dir, cur)
    obj = objects_by_folder.get(doc["object_folder"], {})
    kb_link = doc_dir / "versions" / str(cur) / "04_review" / "kb_decisions_link.json"
    kb_count = None
    if kb_link.is_file():
        import json as _json
        try:
            kb_count = _json.loads(kb_link.read_text(encoding="utf-8")).get("entry_count")
        except Exception:
            kb_count = None
    return {
        "document_code": doc["document_code"],
        "type": _doc_kind(adapter, doc),
        "object_display_name": obj.get("display_name"),
        "discipline": doc["discipline"],
        "current_version": cur,
        "version_count": doc["version_count"],
        "analysis_status": meta.get("analysis_status"),
        "has_01_text_analysis": art["has_01_text_analysis"],
        "has_02_blocks_analysis": art["has_02_blocks_analysis"],
        "has_03_findings": art["has_03_findings"],
        "findings_count": adapter.findings_count(doc_dir, cur),
        "findings_by_severity": adapter.findings_by_severity(doc_dir, cur),
        "has_pipeline_log": adapter.has_pipeline_log(doc_dir, cur),
        "is_legacy_preserve": meta.get("is_legacy_preserve"),
        "is_source_only": meta.get("is_source_only"),
        "is_legacy_partial": meta.get("is_legacy_partial"),
        "kb_link_entry_count": kb_count,
    }


@router.get("/ui-contract/sample", dependencies=[Depends(_gate)])
async def ui_contract_sample(per_type: int = Query(2, ge=1, le=10)):
    """Sample UI/API-контракта ТОЛЬКО из adapter (v2-сторона), без записи отчётов.

    Полную legacy↔v2 сверку делает CLI check_ui_contract_parity.py — здесь только
    то, что v2 СМОЖЕТ отдать в UI-контракте, по выборке разных типов.
    """
    a = _adapter()
    objects_by_folder = {o["folder_name"]: o for o in a.list_objects()}
    buckets: dict[str, int] = {}
    sample = []
    for d in a.list_documents():
        t = _doc_kind(a, d)
        cap = max(per_type, 3) if t == "king_sons_legacy_preserve" else per_type
        if buckets.get(t, 0) < cap:
            sample.append(_v2_contract_view(a, d, objects_by_folder))
            buckets[t] = buckets.get(t, 0) + 1
    return {"count": len(sample), "by_type": buckets,
            "note": "v2-only contract sample; full legacy↔v2 parity via CLI",
            "sample": sample}
