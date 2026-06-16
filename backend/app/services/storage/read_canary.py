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
import re
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


# ---------------------------------------------------------------------------
# Legacy-compatibility helpers
#
# Цель: v2 read-canary ответы должны быть SHAPE-СОВМЕСТИМЫ с legacy, чтобы
# фронтенд (frontend/static/js/app.js + index.html) не ломался при
# AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=true. Инцидент 2026-06-16: /api/projects
# отдавал v2-native {documents,count} вместо legacy {projects,object_name} →
# data.projects=undefined → весь UI падал. Здесь v2-ответы приводятся к тем же
# top-level/per-item ключам, что и legacy. storage_backend/canary остаются как
# инертные диагностические extra-ключи (frontend их игнорирует), НЕ вместо
# legacy-ключей.
# ---------------------------------------------------------------------------

_VID_RE = re.compile(r"^v0*(\d+)$")


def _denorm_vid(vid):
    """v001 → v1 (legacy-форма version_id, которую ждёт фронтенд).

    Фронтенд строит ids-Set и activeVersionId из versions[].version_id и
    latest_version_id и сравнивает с литералом 'v1' (гейтинг, URL ?version_id=).
    v2 хранит zero-padded 'v001' — без денормализации single-version документ
    трактуется как НЕ-v1 (неверная активная версия, лишний migrated-fetch).
    Нераспознанный id возвращается как есть.
    """
    if not vid:
        return vid
    m = _VID_RE.match(str(vid).strip())
    return ("v%d" % int(m.group(1))) if m else str(vid)


def _vno(vid) -> int:
    """Порядковый номер версии из id ('v003' → 3); по умолчанию 1."""
    m = _VID_RE.match(str(vid or "").strip())
    return int(m.group(1)) if m else 1


def _file_type(name: str) -> str:
    n = (name or "").lower()
    if n.endswith(".pdf"):
        return "pdf"
    if n.endswith(".md"):
        return "md"
    if n.endswith((".html", ".htm")):
        return "html"
    if n.endswith(".json"):
        return "json"
    if n.endswith(".txt"):
        return "txt"
    return "other"


def _current_object_folder(a):
    """(folder_name | None, object_name) текущего активного объекта в projects_v2.

    Сопоставляет get_current_object().id с object_id адаптера, чтобы
    /api/projects был STRICTLY SCOPED к текущему объекту (как legacy
    list_projects), а не отдавал документы всех объектов.

    Контракт (folder важнее name):
      * текущий объект найден в v2     → (folder_name, name);
      * текущий объект есть, но НЕ в v2 → (None, name) → caller отдаёт пустой
        список (НЕ кросс-объектную свалку — это и был баг, замеченный ревью);
      * текущего объекта нет, но в v2 есть объекты → (first_folder, first_name)
        (degenerate, как legacy fallback на первый объект);
      * совсем пусто → (None, None).
    """
    try:
        from backend.app.services.common.object_service import get_current_object
        cur = get_current_object()
    except Exception:
        cur = None
    objs = a.list_objects()
    if cur:
        oid = cur.get("id")
        name = cur.get("name")
        for o in objs:
            if o.get("object_id") == oid:
                return o.get("folder_name"), name
        # текущий объект известен, но в v2 его нет → пусто, без кросс-объекта
        return None, name
    if objs:
        return objs[0].get("folder_name"), objs[0].get("display_name")
    return None, None


def _v2_versions_summary(a, doc, doc_dir, cur) -> dict:
    """Сводка версий в legacy-форме version_service.get_versions_summary.

    version_id денормализуется v00N→vN (frontend сравнивает с 'v1'). Поля
    per-version совпадают с legacy enriched-записью (label/is_latest/
    has_source_files/can_run_audit/...).
    """
    latest = _denorm_vid(cur)
    versions = a.list_versions(doc_dir)
    enriched = []
    for v in versions:
        rawv = v.get("version_id")
        no = v.get("version_no") or _vno(rawv)
        files = a.input_files(doc_dir, rawv)
        pdf_n = sum(1 for f in files if f.lower().endswith(".pdf"))
        md_n = sum(1 for f in files if f.lower().endswith(".md"))
        enriched.append({
            "version_id": _denorm_vid(rawv),
            "version_no": no,
            "label": v.get("label") or ("V%d" % no),
            "folder": v.get("folder") or ".",
            "status": v.get("status", "active"),
            "source": v.get("source", "manual"),
            "created_at": v.get("created_at"),
            "comment": v.get("comment"),
            "is_latest": rawv == cur,
            "has_source_files": (pdf_n > 0 or md_n > 0),
            "pdf_count": pdf_n,
            "md_count": md_n,
            "source_files_count": len(files),
            "can_run_audit": pdf_n > 0,
        })
    return {
        "latest_version_id": latest,
        "version_count": len(versions),
        "has_versions": len(versions) > 1,
        "versions": enriched,
    }


# pipeline_log stage key → PipelineStatus поле (зеркало legacy _get_pipeline_status)
_PIPELINE_LOG_MAP = {
    "crop_blocks": "crop_blocks", "gemma_enrichment": "gemma_enrichment",
    "text_analysis": "text_analysis", "block_analysis": "blocks_analysis",
    "block_retry": "block_retry", "findings_merge": "findings",
    "findings_critic": "findings_critic", "findings_corrector": "findings_corrector",
    "norm_verify": "norms_verified", "optimization": "optimization",
    "optimization_critic": "optimization_critic",
    "optimization_corrector": "optimization_corrector", "excel": "excel",
    "prepare": "crop_blocks", "tile_audit": "blocks_analysis", "main_audit": "findings",
}
_PIPELINE_VALID = {"done", "error", "partial", "running", "skipped", "interrupted"}


def _v2_pipeline_status(a, doc_dir, vid, art):
    """PipelineStatus из pipeline_log.json stages (primary, зеркало legacy);
    fallback — наличие 01/02/03. running/interrupted → error (read-only снимок,
    без проверки live-job)."""
    from backend.app.models.project import PipelineStatus
    log = a.read_pipeline_log(doc_dir, vid)
    if isinstance(log, dict) and isinstance(log.get("stages"), dict):
        stages = log["stages"]
        fields = {}
        for log_key, field in _PIPELINE_LOG_MAP.items():
            s = (stages.get(log_key) or {}).get("status", "pending")
            if s in _PIPELINE_VALID:
                if s in ("interrupted", "running"):
                    s = "error"
                fields[field] = s
        if fields:
            return PipelineStatus(**fields)
    def _st(flag):
        return "done" if flag else "pending"
    return PipelineStatus(
        text_analysis=_st(art["has_01_text_analysis"]),
        blocks_analysis=_st(art["has_02_blocks_analysis"]),
        findings=_st(art["has_03_findings"]),
    )


def _v2_pipeline_issues(a, doc_dir, vid):
    """pipeline_issues из pipeline_log error/interrupted stages (зеркало legacy,
    основной сигнал — упавшие этапы)."""
    log = a.read_pipeline_log(doc_dir, vid)
    if not (isinstance(log, dict) and isinstance(log.get("stages"), dict)):
        return []
    issues = []
    for key, info in log["stages"].items():
        if not isinstance(info, dict):
            continue
        if info.get("status") in ("error", "interrupted"):
            err = info.get("error", "")
            if err and len(err) > 80:
                err = err[:77] + "..."
            issues.append(f"{key}: {err}" if err else f"{key}: ошибка")
    return issues


def _v2_optimization(a, doc_dir, vid):
    """(count, by_type, savings_pct) из optimization.json meta (зеркало legacy)."""
    odata = a.read_optimization(doc_dir, vid)
    if isinstance(odata, dict) and isinstance(odata.get("meta"), dict):
        m = odata["meta"]
        return (int(m.get("total_items", 0) or 0),
                m.get("by_type", {}) or {},
                m.get("estimated_savings_pct", 0) or 0)
    return 0, {}, 0


def _v2_review_statuses(a, doc_dir, vid, findings_count, optimization_count):
    """(expert, findings_review, optimization_review) из 04_review/expert_review.json
    (зеркало legacy: decisions[] accepted/rejected vs total). Нет файла → пусто."""
    expert = freview = oreview = ""
    total_items = findings_count + optimization_count
    if total_items <= 0:
        return expert, freview, oreview
    rdata = a.read_review(doc_dir, vid, "expert_review.json")
    if not (isinstance(rdata, dict) and isinstance(rdata.get("decisions"), list)):
        return expert, freview, oreview
    decisions = [d for d in rdata["decisions"] if isinstance(d, dict)]

    def _reviewed(pred):
        return len([d for d in decisions
                    if pred(d) and d.get("decision") in ("accepted", "rejected")])

    rc = _reviewed(lambda d: True)
    if rc >= total_items:
        expert = "complete"
    elif rc > 0:
        expert = "partial"
    if findings_count > 0:
        fr = _reviewed(lambda d: d.get("item_type") == "finding")
        freview = "complete" if fr >= findings_count else ("partial" if fr > 0 else "")
    if optimization_count > 0:
        orv = _reviewed(lambda d: d.get("item_type") == "optimization")
        oreview = "complete" if orv >= optimization_count else ("partial" if orv > 0 else "")
    return expert, freview, oreview


def _v2_batch_counts(a, doc_dir, vid):
    """(total_batches, completed_batches) из block_batches.json + block_batch_*.json
    (зеркало legacy: total из манифеста, completed = batch-файлы > 100 байт)."""
    bd = a.block_batches_dir(doc_dir, vid)
    if bd is None:
        return 0, 0
    import json as _json
    try:
        bf = bd / "block_batches.json"
        data = _json.loads(bf.read_text(encoding="utf-8")) if bf.is_file() else None
    except Exception:
        data = None
    if not isinstance(data, dict):
        return 0, 0
    total = int(data.get("total_batches", len(data.get("batches", []) or [])) or 0)
    completed = 0
    for i in range(1, total + 1):
        f = bd / ("block_batch_%03d.json" % i)
        try:
            if f.is_file() and f.stat().st_size > 100:
                completed += 1
        except Exception:
            pass
    return total, completed


def _v2_project_status(a, doc, ver=None) -> dict:
    """Legacy ProjectStatus (model_dump dict) из v2-документа.

    Та же форма, что и legacy /api/projects[] и /api/projects/{id} (модель
    ProjectStatus → все ключи + совместимые типы, включая `pipeline`). Реальные
    значения подтягиваются из v2-источников где они есть (optimization.json,
    04_review/expert_review.json, block_batches, pipeline_log, 01_input); там, где
    источника нет — безопасный дефолт модели (см. read_default_gap_closure_report).
    БЕЗ silent fallback в legacy, БЕЗ записи в projects_v2.
    """
    from backend.app.models.project import ProjectStatus
    doc_dir = Path(doc["doc_dir"])
    cur = doc.get("current_version")
    vid_raw = ver or cur or "v1"
    art = a.latest_analysis_files(doc_dir, vid_raw)
    meta = a.version_metadata(doc_dir, vid_raw)

    # findings (count + severity + audit_date) — один проход по файлу замечаний
    fdata = a.read_findings(doc_dir, vid_raw) or {}
    fitems = fdata.get("findings", fdata.get("items", [])) if isinstance(fdata, dict) else []
    if not isinstance(fitems, list):
        fitems = []
    findings_count = len(fitems)
    findings_by_severity: dict = {}
    for it in fitems:
        if isinstance(it, dict):
            sev = str(it.get("severity") or it.get("category") or "unknown")
            findings_by_severity[sev] = findings_by_severity.get(sev, 0) + 1
    # legacy читает только top-level audit_date/generated_at (которых в этих файлах
    # нет → legacy сам даёт None); реальная дата лежит в meta.audit_completed →
    # подтягиваем её как fallback (v2-superset, не сравнивается parity).
    audit_date = None
    if isinstance(fdata, dict):
        _meta_f = fdata.get("meta") if isinstance(fdata.get("meta"), dict) else {}
        audit_date = (fdata.get("audit_date") or fdata.get("generated_at")
                      or _meta_f.get("audit_completed") or _meta_f.get("generated_at"))

    opt_count, opt_by_type, opt_savings = _v2_optimization(a, doc_dir, vid_raw)
    expert_status, freview_status, oreview_status = _v2_review_statuses(
        a, doc_dir, vid_raw, findings_count, opt_count)
    total_batches, completed_batches = _v2_batch_counts(a, doc_dir, vid_raw)
    pipeline = _v2_pipeline_status(a, doc_dir, vid_raw, art)
    pipeline_issues = _v2_pipeline_issues(a, doc_dir, vid_raw)

    pdfs = a.input_pdf_files(doc_dir, vid_raw)
    has_pdf = bool(pdfs)
    pdf_files = [n for n, _ in pdfs]
    pdf_size_mb = round(sum(s for _, s in pdfs) / 1024 / 1024, 1) if pdfs else 0.0
    mds = a.input_md_files(doc_dir, vid_raw)
    has_md = bool(mds)
    md_file_name = mds[0][0] if mds else None
    md_size_kb = round(mds[0][1] / 1024, 1) if mds else 0.0

    vsum = _v2_versions_summary(a, doc, doc_dir, cur)
    idx = a.read_blocks_index(doc_dir, vid_raw) or {}
    return ProjectStatus(
        project_id=doc["document_code"],
        name=doc["document_code"],
        section=(doc.get("discipline") or "EOM"),
        description="",
        has_pdf=has_pdf,
        pdf_size_mb=pdf_size_mb,
        pdf_files=pdf_files,
        has_md_file=has_md,
        md_file_name=md_file_name,
        md_file_size_kb=md_size_kb,
        text_source=("md" if has_md else "none"),
        pipeline=pipeline,
        findings_count=findings_count,
        findings_by_severity=findings_by_severity,
        optimization_count=opt_count,
        optimization_by_type=opt_by_type,
        optimization_savings_pct=opt_savings,
        last_audit_date=audit_date,
        total_batches=total_batches,
        completed_batches=completed_batches,
        has_ocr=bool(idx.get("blocks")),
        block_count=int(idx.get("total_blocks") or 0),
        block_expected=int(idx.get("total_expected") or 0),
        block_errors=int(idx.get("errors") or 0),
        pipeline_issues=pipeline_issues,
        expert_review_status=expert_status,
        findings_review_status=freview_status,
        optimization_review_status=oreview_status,
        version_id=_denorm_vid(vid_raw),
        version_no=(meta.get("version_no") or _vno(vid_raw)),
        version_label=(meta.get("label") or ("V%d" % _vno(vid_raw))),
        latest_version_id=vsum["latest_version_id"],
        version_count=vsum["version_count"],
        has_versions=vsum["has_versions"],
        is_latest_version=(_denorm_vid(vid_raw) == vsum["latest_version_id"]),
        versions_summary=vsum["versions"],
    ).model_dump()


def _classify_blocks_analysis(project_id, blocks_analysis, index_data,
                              block_batches, findings_data) -> dict:
    """Зеркало legacy get_blocks_analysis: {project_id,total_analyzed,counts,blocks}.

    Та же классификация has_findings/no_findings/merged_into/skipped по тем же
    источникам (02_blocks_analysis + block_batches + 03_findings + blocks index).
    Чистая функция (без ФС). Пустые источники → {blocks:{}, counts: нули,
    total_analyzed:0} — фронтенд делает Object.entries(data.blocks), пустой dict
    не падает. legacy-only fallback'и (block_batch_*.json / typed_facts) тут не
    нужны: v2 всегда имеет 02_blocks_analysis.json.
    """
    try:
        from backend.app.api.routers.blocks import _normalize_block_info
    except Exception:
        def _normalize_block_info(b):
            return b

    blocks_map: dict = {}
    block_list = []
    if isinstance(blocks_analysis, dict):
        block_list = (blocks_analysis.get("blocks_reviewed")
                      or blocks_analysis.get("block_analyses") or [])
    for bi in block_list:
        if isinstance(bi, dict):
            bid = bi.get("block_id", "")
            if bid:
                blocks_map[bid] = bi

    merged_parent_map: dict = {}
    batches = (block_batches or {}).get("batches", []) if isinstance(block_batches, dict) else []
    for batch in batches:
        for blk in (batch.get("blocks", []) or []):
            parent_bid = blk.get("block_id", "")
            for child_bid in (blk.get("merged_block_ids") or []):
                if child_bid:
                    merged_parent_map[child_bid] = parent_bid

    blocks_in_findings: set = set()
    flist = (findings_data or {}).get("findings", []) if isinstance(findings_data, dict) else []
    for f in flist:
        if not isinstance(f, dict):
            continue
        for bid in (f.get("source_block_ids") or []):
            if bid:
                blocks_in_findings.add(bid)
        for bid in (f.get("related_block_ids") or []):
            if bid:
                blocks_in_findings.add(bid)
        for ev in (f.get("evidence") or []):
            if isinstance(ev, dict):
                bid = ev.get("block_id")
                if bid:
                    blocks_in_findings.add(bid)

    for bid, block in blocks_map.items():
        has = (block.get("findings") or []) or (bid in blocks_in_findings)
        block["status"] = "has_findings" if has else "no_findings"

    index_blocks = (index_data or {}).get("blocks", []) if isinstance(index_data, dict) else []
    for ib in index_blocks:
        if not isinstance(ib, dict):
            continue
        bid = ib.get("block_id", "")
        if not bid or bid in blocks_map:
            continue
        parent_bid = merged_parent_map.get(bid)
        if parent_bid:
            parent = blocks_map.get(parent_bid, {})
            blocks_map[bid] = {
                "block_id": bid, "page": ib.get("page"),
                "sheet": parent.get("sheet"),
                "sheet_type": parent.get("sheet_type", "other"),
                "summary": parent.get("summary") or "Разобран в составе родительского листа",
                "key_values_read": [], "findings": [],
                "status": "merged_into", "parent_block_id": parent_bid,
                "original_ocr_label": ib.get("ocr_label", ""),
            }
        else:
            blocks_map[bid] = {
                "block_id": bid, "page": ib.get("page"),
                "sheet": None, "sheet_type": "other",
                "summary": "Без значимого содержимого",
                "key_values_read": [], "findings": [],
                "status": "skipped", "is_empty_scope": True,
                "original_ocr_label": ib.get("ocr_label", ""),
            }

    for block in blocks_map.values():
        _normalize_block_info(block)

    counts = {"has_findings": 0, "no_findings": 0, "merged_into": 0, "skipped": 0}
    for block in blocks_map.values():
        s = block.get("status")
        if s in counts:
            counts[s] += 1

    return {
        "project_id": project_id,
        "total_analyzed": len(blocks_map),
        "counts": counts,
        "blocks": blocks_map,
    }


def v2_projects_list() -> dict:
    """Список проектов из projects_v2 в LEGACY-форме для GET /api/projects.

    Возвращает {projects, object_name} — shape-совместимо с legacy (frontend
    читает data.projects + data.object_name). SCOPED к текущему объекту (как
    legacy list_projects), а не ко всем объектам. storage_backend/canary —
    инертные диагностические extra-ключи, НЕ вместо legacy-ключей.
    """
    a = _adapter()
    if not a.is_available():
        raise HTTPException(status_code=404,
                            detail="projects_v2 storage not available")
    folder, object_name = _current_object_folder(a)
    # STRICT scope: текущий объект не найден в v2 → пустой список (а НЕ документы
    # всех объектов под именем текущего — это был баг кросс-объектной свалки).
    docs = a.list_documents(object_folder=folder) if folder else []
    projects = [_v2_project_status(a, d) for d in docs]
    return {
        "projects": projects,
        "object_name": object_name or "Объект",
        "storage_backend": BACKEND_V2,
        "canary": True,
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
    """Детали проекта из projects_v2 (canary для GET /api/projects/{id}).

    Возвращает LEGACY ProjectStatus.model_dump() (та же модель, что legacy
    get_project_status), поэтому присутствуют ВСЕ ключи, включая `pipeline` —
    без него index.html падает на currentProject.pipeline.gemma_enrichment.
    storage_backend/canary добавлены как инертные extra-ключи.
    """
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    status = _v2_project_status(a, doc, ver)
    status["storage_backend"] = BACKEND_V2
    status["canary"] = True
    return status


def v2_project_versions(request, project_id: str) -> dict:
    """Сводка версий из projects_v2 (canary для .../versions) в LEGACY-форме.

    Зеркалит version_service.get_versions_summary: top-level latest_version_id +
    versions[] с denorm version_id (v00N→vN). Frontend читает data.versions и
    data.latest_version_id и сравнивает version_id с 'v1' — zero-padded форма
    ломала выбор активной версии.
    """
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    vsum = _v2_versions_summary(a, doc, doc_dir, cur)
    return {
        "project_id": doc["document_code"],
        "logical_project_id": doc["document_code"],
        "latest_version_id": vsum["latest_version_id"],
        "version_count": vsum["version_count"],
        "has_versions": vsum["has_versions"],
        "versions": vsum["versions"],
        "storage_backend": BACKEND_V2,
        "canary": True,
    }


def v2_finding_by_id(request, project_id: str, finding_id: str) -> dict:
    """Одно замечание по id из projects_v2 (canary для .../finding/{finding_id}).

    Зеркалит legacy get_finding_by_id: поля замечания на ВЕРХНЕМ уровне ответа
    (не вложены под `finding`). storage_backend/canary — инертные extra-ключи
    (у замечаний таких ключей нет, коллизии не будет).
    """
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    for f in a.findings_list(doc_dir, ver):
        if not isinstance(f, dict):
            continue
        fid = f.get("id") or f.get("finding_id") or f.get("number")
        if str(fid) == str(finding_id):
            return {**f, "storage_backend": BACKEND_V2, "canary": True}
    raise HTTPException(
        status_code=404,
        detail=(f"projects_v2 canary: finding '{finding_id}' not found in document "
                f"'{doc['document_code']}' (no silent legacy fallback)"),
    )


def v2_blocks_analysis(request, project_id: str) -> dict:
    """Анализ блоков из projects_v2 (canary для .../blocks/analysis) в LEGACY-форме.

    Frontend делает Object.entries(data.blocks) и читает an.status /
    an.parent_block_id — поэтому нужен КЛАССИФИЦИРОВАННЫЙ dict `blocks`
    (как legacy get_blocks_analysis), а не сырой 02_blocks_analysis.json.
    Источники классификации (02 + block_batches + 03_findings + blocks index)
    читаются из адаптера. Нет данных → blocks:{}, counts: нули (не падает).
    """
    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    result = _classify_blocks_analysis(
        doc["document_code"],
        a.read_blocks_analysis(doc_dir, ver),
        a.read_blocks_index(doc_dir, ver),
        a.read_block_batches(doc_dir, ver),
        a.read_findings_03(doc_dir, ver),
    )
    result["storage_backend"] = BACKEND_V2
    result["canary"] = True
    result["version_id"] = _denorm_vid(ver)
    return result


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
    raw = a.input_files(doc_dir, ver)
    inp = a.input_dir(doc_dir, ver)
    files = []
    for n in raw:
        size = None
        try:
            p = inp / n
            if p.is_file():
                size = p.stat().st_size
        except Exception:
            size = None
        files.append({"name": n, "type": _file_type(n), "size": size, "updated_at": None})
    return {
        "project_id": doc["document_code"],
        "version_id": _denorm_vid(ver),
        "file_count": len(files),
        "files": files,
        "storage_backend": BACKEND_V2,
        "canary": True,
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
