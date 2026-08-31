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

import logging
import os
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

from fastapi import HTTPException
from fastapi.responses import FileResponse

from backend.app.pipeline.stages.block_context.contract import (
    decorate_blocks_vector_state,
)
from backend.app.services.common import block_crop_store

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

    `project_id` → v2 document: ПОЛНЫЙ pid первым, basename — fallback
    (find_document_by_project_id); `?object_id=` уточняет объект. Не найден →
    404 canary-error (НЕ silent fallback в legacy).

    Прежний срез до basename ломал документы, чей document_code содержит
    дисциплинный префикс (напр. «OV/13АВ-РД-ОВ2-К4 V1»): при наличии стейл-дубля
    с чистым basename-кодом ВСЕ canary-read (versions/findings/optimization)
    попадали в дубль → в UI «видна только V1», решения эксперта «пропадали»
    (инцидент 2026-07-02, ОВ2-К4).
    """
    a = _adapter()
    if not a.is_available():
        raise HTTPException(status_code=404,
                            detail="projects_v2 storage not available")
    object_id = request.query_params.get("object_id") if request is not None else None
    raw_pid = str(project_id or "").replace("(main)", "").strip()
    doc = a.find_document_by_project_id(raw_pid, object_id=object_id)
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=(f"projects_v2 canary: document '{raw_pid}' not found in "
                    "projects_v2 (no silent legacy fallback)"),
        )
    return a, doc, Path(doc["doc_dir"]), doc["current_version"]


def _req_version(request):
    return request.query_params.get("version_id") if request is not None else None


# ---------------------------------------------------------------------------
# Защитный fallback на legacy при НЕПОЛНОМ v2-снимке (write lagged behind audit)
#
# Контекст: при `dual_write_shadow` v2-снимок мог обрываться на block_analysis
# (поздние artifacts — 03_findings/optimization/нормы — попадали только в legacy).
# Тогда v2-read отдавал «аудит не проводился» / неполный статус, хотя legacy уже
# содержит полный аудит. Это страховка на время миграции: если v2-findings нет, а
# legacy-аудит завершён — читаем legacy и логируем warning (НЕ скрываем write-bug:
# warning виден, флаг `v2_snapshot_incomplete` в ответе). Если v2 полон — читаем v2
# как раньше. Если и legacy пуст — поведение прежнее.
# ---------------------------------------------------------------------------

# имена findings-файлов в legacy `_output` (приоритет как в findings_service)
_LEGACY_FINDINGS_NAMES = ("03a_norms_verified.json", "03_findings.json",
                          "03_findings_pre_merge.json")


def _legacy_output_dir_for_doc(doc_dir: Path, vid: str) -> Optional[Path]:
    """legacy `_output` папка для версии документа (для fallback-проверок).

    Резолвит legacy_path объекта из object.json и контейнерную/plain раскладку
    (как `_v2_pipeline_summary`-fallback). None — если не найдено. Read-only.
    """
    try:
        import json as _json
        code = doc_dir.name
        discipline = doc_dir.parent.parent.name
        obj_folder_dir = doc_dir.parent.parent.parent.parent  # .../objects/<obj_folder>
        obj_json = obj_folder_dir / "object.json"
        if not obj_json.is_file():
            return None
        legacy_root = Path(_json.loads(obj_json.read_text(encoding="utf-8")).get("legacy_path", ""))
        if not legacy_root.is_dir():
            return None
        m = re.match(r"v0*(\d+)$", str(vid))
        ver_n = int(m.group(1)) if m else 1
        container = legacy_root / discipline / f"{code}(main)"
        if ver_n > 1:
            # Версия > 1: ищем ТОЛЬКО папку этой версии. НЕ падаем на `code/_output`
            # (= v001) — иначе findings старой версии маскируют неаудированную новую.
            # Имена legacy V-папок бывают и с суффиксом '.pdf' — учитываем оба варианта.
            ver_names = [f"{code} V{ver_n}", f"{code} V{ver_n}.pdf"]
            candidates = [container / n / "_output" for n in ver_names]
            candidates += [legacy_root / discipline / n / "_output" for n in ver_names]
        else:
            # v001 — plain / контейнерная раскладка (fallback корректен: та же версия).
            candidates = [
                container / code / "_output",
                legacy_root / discipline / code / "_output",
            ]
        for cand in candidates:
            if cand.is_dir():
                return cand
    except Exception:
        pass
    return None


def _legacy_findings_present(out_dir: Optional[Path]) -> bool:
    """True, если legacy `_output` содержит findings-файл (аудит завершён)."""
    return bool(out_dir) and any((out_dir / n).is_file() for n in _LEGACY_FINDINGS_NAMES)


def _v2_snapshot_incomplete(a, doc_dir: Path, ver: str) -> Optional[Path]:
    """legacy `_output`, если legacy-аудит ПОЛНЕЕ v2-снимка; иначе None.

    Сигнал неполноты — отсутствие findings-артефакта в v2 `03_analysis/latest`
    при наличии findings в legacy `_output`. Это ровно тот случай, когда mirror
    отстал от аудита (снимок замер на block_analysis). Если v2-findings есть —
    снимок считается достаточно полным (None, читаем v2).
    """
    try:
        if a.findings_path(doc_dir, ver) is not None:
            return None
    except Exception:
        return None
    out_dir = _legacy_output_dir_for_doc(doc_dir, ver)
    return out_dir if _legacy_findings_present(out_dir) else None


def _legacy_findings_fallback(project_id: str, request) -> Optional[dict]:
    """Legacy findings (полная форма model_dump) с маркерами fallback; None при сбое."""
    try:
        import backend.app.services.findings.findings_service as _fs
        res = _fs.get_findings(project_id, version_id=_req_version(request))
        if res is None:
            return None
        out = res.model_dump()
        out["storage_backend"] = "legacy_fallback"
        out["v2_snapshot_incomplete"] = True
        return out
    except Exception:
        return None


def _legacy_project_status_fallback(project_id: str, request) -> Optional[dict]:
    """Legacy ProjectStatus (model_dump) с маркерами fallback; None при сбое."""
    try:
        from backend.app.services.common import project_service as _ps
        status = _ps.get_project_status(project_id, version_id=_req_version(request))
        if not status:
            return None
        out = status.model_dump()
        out["storage_backend"] = "legacy_fallback"
        out["v2_snapshot_incomplete"] = True
        return out
    except Exception:
        return None


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


def _current_object_folder(a, object_id: Optional[str] = None):
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
        from backend.app.services.common.object_service import (
            get_current_object,
            get_object_by_id,
        )
        cur = get_object_by_id(object_id) if object_id else get_current_object()
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
    "optimization_corrector": "optimization_corrector",
    "decision_carryover": "decision_carryover", "excel": "excel",
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


def _v2_pipeline_summary(a, doc_dir, vid):
    """pipeline_summary из pipeline_log (зеркало _build_pipeline_summary из project_service).

    Передаём родительскую папку pipeline_log.json как output_dir, так как
    _build_pipeline_summary ищет pipeline_log.json именно там. Fail-soft.
    Fallback: если pipeline_log нет в projects_v2 (аудит ещё не запускался по
    новому пути), пробуем legacy project_service по document_code + discipline.
    """
    try:
        from backend.app.services.common.project_service import _build_pipeline_summary
        log_path = a.pipeline_log_path(doc_dir, vid)
        if log_path and log_path.is_file():
            # Артефакты анализа живут в 03_analysis/latest, а журнал — в
            # 99_service (мигрированные версии) или runs/<run_id>. Без явной
            # artifacts_dir инференс «артефакт на ФС → done» не срабатывал, и
            # завершённый аудит показывался пустым конвейером.
            return _build_pipeline_summary(
                log_path.parent, artifacts_dir=a.latest_dir(doc_dir, vid),
            )
    except Exception:
        pass
    # Fallback: pipeline_log живёт только в legacy projects/ (аудит не
    # мигрировал). Читаем legacy_path из object.json и ищем _output папку.
    # doc_dir = .../objects/<obj_folder>/disciplines/<disc>/documents/<code>
    try:
        import re as _re, json as _json
        from backend.app.services.common.project_service import _build_pipeline_summary
        code = doc_dir.name
        discipline = doc_dir.parent.parent.name
        obj_folder_dir = doc_dir.parent.parent.parent.parent  # .../objects/<obj_folder>
        obj_json = obj_folder_dir / "object.json"
        if not obj_json.is_file():
            return []
        legacy_root = Path(_json.loads(obj_json.read_text()).get("legacy_path", ""))
        if not legacy_root.is_dir():
            return []
        m = _re.match(r'v0*(\d+)$', vid)
        ver_n = int(m.group(1)) if m else 1
        # Контейнерная раскладка: <legacy_root>/<disc>/<code>(main)/<code [V{n}]>/_output
        container = legacy_root / discipline / f"{code}(main)"
        candidates = [
            container / (f"{code} V{ver_n}" if ver_n > 1 else code) / "_output",
            container / code / "_output",  # V1 в контейнере
            legacy_root / discipline / code / "_output",  # без контейнера
        ]
        for cand in candidates:
            if cand.is_dir():
                ps = _build_pipeline_summary(cand)
                if ps:
                    return ps
    except Exception:
        pass
    return []


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


def _base_project_key(project_id: str) -> str:
    """Ключ логического проекта (см. project_service.base_project_key)."""
    from backend.app.services.common.project_service import base_project_key
    return base_project_key(project_id)


def _review_incomplete(findings_count, opt_count, freview, oreview) -> bool:
    """Есть ли незакрытая проверка: категория без элементов галочку не блокирует.

    Требование «обе галочки complete» в лоб держало в счётчике любой проект без
    оптимизаций: их статус остаётся пустым (`_v2_review_statuses` выставляет
    его только при opt_count > 0), и проект никогда не становился проверенным.
    """
    findings_ok = findings_count <= 0 or freview == "complete"
    opt_ok = opt_count <= 0 or oreview == "complete"
    return not (findings_ok and opt_ok)


def _v2_review_pending(a, doc, doc_dir, vid_raw, findings_count, opt_count,
                       freview_status, oreview_status):
    """(review_pending, version_id) — «проект ждёт проверки экспертом».

    Счётчик «Не проверено» в сайдбаре считает ПРОЕКТЫ, а не версии, и смотрит
    на ПОСЛЕДНЮЮ версию, где есть что проверять. Иначе загрузка новой версии
    искажала счётчик в обе стороны: непроверенная V1 + пустая V2 давали то
    «проверено» (у V2 нет результатов → нет и статусов), то двойной счёт, когда
    версия жила отдельной карточкой.

    Правила:
      - у текущей версии есть результаты → решают её две галочки;
      - результатов нет → спускаемся по версиям вниз до первой с результатами;
      - ни у одной версии результатов нет → проверять нечего, `False`.
    """
    if (findings_count + opt_count) > 0:
        return (_review_incomplete(findings_count, opt_count,
                                   freview_status, oreview_status),
                _denorm_vid(vid_raw))

    # `list_documents` отдаёт version_ids (плоский список), а `versions` (список
    # словарей) есть только у document.json — поддерживаем оба источника, иначе
    # спуск к предыдущей версии молча не работает.
    ids = [str(v) for v in (doc.get("version_ids") or []) if v]
    if not ids:
        ids = [
            str(v.get("version_id"))
            for v in (doc.get("versions") or [])
            if isinstance(v, dict) and v.get("version_id")
        ]
    if not ids:
        ids = [
            str(v.get("version_id"))
            for v in (a.list_versions(doc_dir) or [])
            if isinstance(v, dict) and v.get("version_id")
        ]
    if vid_raw in ids:
        ids = ids[:ids.index(vid_raw)]  # только более ранние версии
    for prev in reversed(ids):
        prev_fdata = a.read_findings(doc_dir, prev) or {}
        prev_items = (prev_fdata.get("findings", prev_fdata.get("items", []))
                      if isinstance(prev_fdata, dict) else [])
        prev_fcount = len(prev_items) if isinstance(prev_items, list) else 0
        prev_ocount, _by_type, _savings = _v2_optimization(a, doc_dir, prev)
        if (prev_fcount + prev_ocount) <= 0:
            continue
        _expert, prev_fr, prev_or = _v2_review_statuses(
            a, doc_dir, prev, prev_fcount, prev_ocount)
        return (_review_incomplete(prev_fcount, prev_ocount, prev_fr, prev_or),
                _denorm_vid(prev))
    return False, None


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
    pipeline_summary = _v2_pipeline_summary(a, doc_dir, vid_raw)

    pdfs = a.input_pdf_files(doc_dir, vid_raw)
    has_pdf = bool(pdfs)
    pdf_files = [n for n, _ in pdfs]
    pdf_size_mb = round(sum(s for _, s in pdfs) / 1024 / 1024, 1) if pdfs else 0.0
    mds = a.input_md_files(doc_dir, vid_raw)
    has_md = bool(mds)
    md_file_name = mds[0][0] if mds else None
    md_size_kb = round(mds[0][1] / 1024, 1) if mds else 0.0

    vsum = _v2_versions_summary(a, doc, doc_dir, cur)
    review_pending, review_vid = _v2_review_pending(
        a, doc, doc_dir, vid_raw, findings_count, opt_count,
        freview_status, oreview_status)
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
        pipeline_summary=pipeline_summary,
        expert_review_status=expert_status,
        findings_review_status=freview_status,
        optimization_review_status=oreview_status,
        review_pending=review_pending,
        review_status_version_id=review_vid,
        base_project_key=_base_project_key(doc["document_code"]),
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
    нужны: v2 всегда имеет 01_blocks_analysis.json.
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


def _v2_load_hidden_set() -> set:
    """hidden_projects.json как у legacy (basename project_id). Fail-soft → пусто."""
    try:
        from backend.app.services.common.project_service import _load_hidden_projects
        return _load_hidden_projects()
    except Exception:
        return set()


def _v2_doc_hidden(doc: dict, hidden_set: set) -> bool:
    """Зеркалит legacy-скрытие из iter_project_dirs для v2-списка `/api/projects`:

    * `_`-prefix на уровне документа (leaf) ИЛИ дисциплины — скрыт (как
      `entry.name.startswith("_")` / `sub.name.startswith("_")` в legacy);
    * project_id в hidden_projects.json — скрыт (legacy ключует по basename;
      проверяем и `document_code`, и `discipline/document_code` для надёжности).

    НЕ удаляет с диска; прямой служебный доступ (details/findings) остаётся.
    """
    code = (doc.get("document_code") or "")
    disc = (doc.get("discipline") or "")
    if code.startswith("_") or disc.startswith("_"):
        return True
    if code in hidden_set or (disc and f"{disc}/{code}" in hidden_set):
        return True
    return False


def v2_projects_list(object_id: Optional[str] = None) -> dict:
    """Список проектов из projects_v2 в LEGACY-форме для GET /api/projects.

    Возвращает {projects, object_name} — shape-совместимо с legacy (frontend
    читает data.projects + data.object_name). SCOPED к текущему объекту (как
    legacy list_projects), а не ко всем объектам. storage_backend/canary —
    инертные диагностические extra-ключи, НЕ вместо legacy-ключей.

    Скрытие зеркалит legacy iter_project_dirs (`_`-prefix + hidden_projects.json),
    чтобы default-v2 список совпадал с legacy и `_smoke_*`/скрытые проекты не
    протекали к экспертам.
    """
    a = _adapter()
    if not a.is_available():
        raise HTTPException(status_code=404,
                            detail="projects_v2 storage not available")
    folder, object_name = _current_object_folder(a, object_id=object_id)
    # STRICT scope: текущий объект не найден в v2 → пустой список (а НЕ документы
    # всех объектов под именем текущего — это был баг кросс-объектной свалки).
    docs = a.list_documents(object_folder=folder) if folder else []
    hidden_set = _v2_load_hidden_set()
    docs = [d for d in docs if not _v2_doc_hidden(d, hidden_set)]
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
    # защитный fallback: v2-снимок неполный (mirror отстал), а legacy-аудит готов
    legacy_out = _v2_snapshot_incomplete(a, doc_dir, ver)
    if legacy_out is not None:
        fallback = _legacy_findings_fallback(project_id, request)
        if fallback is not None:
            logger.warning(
                "v2_snapshot_incomplete_fallback_legacy: findings project_id=%s "
                "document=%s version=%s legacy_output=%s",
                project_id, doc["document_code"], ver, legacy_out,
            )
            return fallback
    f_count = a.findings_count(doc_dir, ver)
    f_by_sev = a.findings_by_severity(doc_dir, ver)
    return {
        "storage_backend": BACKEND_V2,
        "canary": True,
        "project_id": project_id,
        "document_code": doc["document_code"],
        "object_id": doc["object_id"],
        "object_folder": doc["object_folder"],
        "discipline": doc["discipline"],
        "version_id": ver,
        "version_count": doc["version_count"],
        "analysis_status": a.analysis_status(doc_dir, ver),
        "findings_count": f_count,
        "findings_by_severity": f_by_sev,
        # legacy-контракт FindingsResponse: фронт читает total/by_severity для
        # строки «Всего:» и бейджей критичности. Без них итог/бейджи пустые.
        "total": f_count,
        "filtered_total": None,
        "by_severity": f_by_sev,
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
    # защитный fallback: v2-снимок неполный (pipeline_log замер на block_analysis,
    # findings не зеркалированы), а legacy-аудит готов → отдаём legacy-статус,
    # чтобы UI не показывал ложный неполный конвейер.
    legacy_out = _v2_snapshot_incomplete(a, doc_dir, ver)
    if legacy_out is not None:
        fallback = _legacy_project_status_fallback(project_id, request)
        if fallback is not None:
            logger.warning(
                "v2_snapshot_incomplete_fallback_legacy: project_status project_id=%s "
                "document=%s version=%s legacy_output=%s",
                project_id, doc["document_code"], ver, legacy_out,
            )
            return fallback
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
    # защитный fallback: v2-снимок неполный, legacy-аудит готов → ищем в legacy
    legacy_out = _v2_snapshot_incomplete(a, doc_dir, ver)
    if legacy_out is not None:
        try:
            import backend.app.services.findings.findings_service as _fs
            res = _fs.get_finding_by_id(project_id, finding_id,
                                        version_id=_req_version(request))
            if res is not None:
                logger.warning(
                    "v2_snapshot_incomplete_fallback_legacy: finding_by_id "
                    "project_id=%s finding=%s legacy_output=%s",
                    project_id, finding_id, legacy_out,
                )
                out = res.model_dump() if hasattr(res, "model_dump") else dict(res)
                out["storage_backend"] = "legacy_fallback"
                out["v2_snapshot_incomplete"] = True
                return out
        except Exception:
            pass
    raise HTTPException(
        status_code=404,
        detail=(f"projects_v2 canary: finding '{finding_id}' not found in document "
                f"'{doc['document_code']}' (no silent legacy fallback)"),
    )


def v2_blocks_analysis(request, project_id: str) -> dict:
    """Анализ блоков из projects_v2 (canary для .../blocks/analysis) в LEGACY-форме.

    Frontend делает Object.entries(data.blocks) и читает an.status /
    an.parent_block_id — поэтому нужен КЛАССИФИЦИРОВАННЫЙ dict `blocks`
    (как legacy get_blocks_analysis), а не сырой 01_blocks_analysis.json.
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
    context_summary = a.read_block_context_summary(doc_dir, ver)
    if not context_summary:
        # У двух старых legacy-preserve снимков нет ни block_context, ни рабочего
        # PDF в 02_work. Роутер Stage 01 для них закономерно вернёт no_sources:
        # фиксируем это сразу в списке, чтобы UI не показывал ложную кнопку TXT.
        version_dir = a.version_dir(doc_dir, ver)
        work_dir = version_dir / "02_work"
        has_router_pdf = (
            (work_dir / "document.pdf").is_file()
            or (version_dir / "document.pdf").is_file()
            or (work_dir.is_dir() and any(work_dir.glob("*.pdf")))
        )
        if not has_router_pdf:
            context_summary = {
                "blocks": [
                    {"block_id": block.get("block_id"), "source_kind": "no_sources"}
                    for block in idx.get("blocks") or []
                    if isinstance(block, dict) and block.get("block_id")
                ]
            }
    decorate_blocks_vector_state(idx.get("blocks") or [], context_summary)
    pages_map: dict = {}
    for block in idx.get("blocks", []):
        pages_map.setdefault(block.get("page", 0), []).append(block)
    pages = []
    for pn in sorted(pages_map.keys()):
        blocks = pages_map[pn]
        page_labels = {
            str(block.get("page_label") or "").strip()
            for block in blocks if str(block.get("page_label") or "").strip()
        }
        pages.append({
            "page_num": pn,
            "page_label": next(iter(page_labels)) if len(page_labels) == 1 else None,
            "block_count": len(blocks),
            "blocks": blocks,
        })
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
    """Изображение кропа блока (canary для .../blocks/image/{block_id}).

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
    file_name = target.name if target is not None else f"block_{want}.png"
    if target is None:
        target = bd / file_name
    # анти-traversal: файл обязан лежать ВНУТРИ папки блоков версии
    try:
        target_resolved = target.resolve()
    except Exception:
        target_resolved = None
    if (target_resolved is None
            or bd_resolved not in target_resolved.parents):
        raise HTTPException(
            status_code=404,
            detail=(f"projects_v2 canary: block image '{block_id}' not found for "
                    f"'{doc['document_code']}' (no silent legacy fallback)"),
        )
    crop_source = "local"
    if not target_resolved.is_file():
        # Кроп эвакуирован (или не докропан): восстанавливаем из локального PDF,
        # при неудаче — по crop_url. Проверка traversal уже пройдена по ИМЕНИ;
        # возвращаемый путь может лежать в LRU-кэше вне папки версии.
        restored = block_crop_store.resolve_block_image(bd, want, file_name=file_name)
        if restored is None:
            raise HTTPException(
                status_code=404,
                detail=(f"projects_v2 canary: block image '{block_id}' not found for "
                        f"'{doc['document_code']}' (no silent legacy fallback)"),
            )
        target_resolved = restored
        crop_source = "restored"
    media_types = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }
    media_type = media_types.get(target_resolved.suffix.lower(), "application/octet-stream")
    return FileResponse(str(target_resolved), media_type=media_type,
                        headers={"X-Storage-Backend": BACKEND_V2,
                                 "X-Crop-Source": crop_source})


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
    # защитный fallback: v2-снимок неполный, legacy-аудит готов → legacy block-map
    legacy_out = _v2_snapshot_incomplete(a, doc_dir, ver)
    if legacy_out is not None:
        try:
            res = fs.get_finding_block_map(project_id, version_id=_req_version(request))
            if res is not None:
                logger.warning(
                    "v2_snapshot_incomplete_fallback_legacy: block_map project_id=%s "
                    "document=%s version=%s legacy_output=%s",
                    project_id, doc["document_code"], ver, legacy_out,
                )
                out = dict(res) if isinstance(res, dict) else res
                if isinstance(out, dict):
                    out["storage_backend"] = "legacy_fallback"
                    out["v2_snapshot_incomplete"] = True
                return out
        except Exception:
            pass
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


def v2_document_pdf(request, project_id: str) -> FileResponse:
    """Исходный PDF выбранной версии из projects_v2 для встроенного просмотра."""
    from backend.app.services.storage.projects_v2_source_resolver import (
        resolve_version_source_files,
    )

    a, doc, doc_dir, cur = _resolve_doc_or_404(request, project_id)
    ver = _resolve_version(a, doc_dir, cur, _req_version(request))
    version_dir = a.version_dir(doc_dir, ver)
    sources = resolve_version_source_files(version_dir, doc["document_code"])
    pdf_path = sources.pdf_path
    if not pdf_path or not Path(pdf_path).is_file():
        raise HTTPException(
            status_code=404,
            detail=(f"projects_v2 canary: PDF not found for "
                    f"'{doc['document_code']}' version '{ver}' "
                    "(no silent legacy fallback)"),
        )
    return FileResponse(
        str(pdf_path),
        media_type="application/pdf",
        filename=Path(pdf_path).name,
        content_disposition_type="inline",
        headers={
            "X-Storage-Backend": BACKEND_V2,
            "X-Audit-Version-Id": ver,
        },
    )
