"""REST API для раздела «Сравнение стадий».

MVP-набор endpoint'ов:
  • POST   /api/stage-comparison/sessions
  • GET    /api/stage-comparison/sessions
  • GET    /api/stage-comparison/sessions/{session_id}
  • GET    /api/stage-comparison/sessions/{session_id}/pairs/{pair_id}
  • GET    /api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-image
  • POST   /api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/auto-link
  • POST   /api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/links
  • DELETE /api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/links
  • GET    /api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/text-diff
  • GET    /api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/graphic-summary
  • POST   /api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/graphic-diff
  • GET    /api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/block-image

Платные LLM-сравнения графики НЕ запускаются автоматически: только при
run_paid=true в POST .../graphic-diff и только через paid_api_guard.
"""
from __future__ import annotations

import base64
import io
import json
import logging
import re
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from backend.app.services.stage_comparison import diff_text, store, findings as findings_mod, jobs as jobs_mod, reports as reports_mod, warnings as warnings_mod, objects as objects_mod
from backend.app.services.stage_comparison import text_llm as text_llm_mod, text_llm_jobs as text_llm_jobs_mod
from backend.app.services.stage_comparison import text_llm_provider as text_llm_provider_mod
from backend.app.services.stage_comparison import text_llm_preflight as text_llm_preflight_mod
from backend.app.services.stage_comparison import text_llm_flat as text_llm_flat_mod
from backend.app.services.stage_comparison import pair_template as pair_template_mod
from backend.app.services.stage_comparison import graphic_llm_local as graphic_local_mod
from backend.app.services.stage_comparison import md_image_enrichment as md_enrichment_mod
from backend.app.services.stage_comparison import md_enrichment_jobs as md_enrichment_jobs_mod
from backend.app.services.stage_comparison import enriched_comparison as enriched_compare_mod
from backend.app.services.stage_comparison import unified_analysis as unified_analysis_mod
from backend.app.services.stage_comparison import unified_analysis_jobs as unified_jobs_mod
from backend.app.services.stage_comparison import unified_findings as unified_findings_mod
from backend.app.services.stage_comparison import unified_grouping as unified_grouping_mod
from backend.app.services.stage_comparison import expert_review as expert_review_mod
from backend.app.services.stage_comparison import paths as sc_paths_mod
from backend.app.services.stage_comparison import saved_config as saved_config_mod

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stage-comparison", tags=["stage-comparison"])


GRAPHIC_DIFF_PROMPT = (
    "Сравни два изображения проектной документации. "
    "Первое изображение относится к предыдущей стадии проекта, "
    "второе — к новой стадии. Найди все значимые отличия: новые элементы, "
    "удалённые элементы, изменение размеров, изменение подписей, "
    "изменение расположения, изменение условных обозначений, "
    "изменение таблиц или схем. Ответ дай структурированным списком "
    "на русском языке. Не выдумывай отличия, если их не видно."
)


# ─── Pydantic-модели тела запросов ───────────────────────────────────────


class CreateSessionRequest(BaseModel):
    stage_a_path: str = Field(..., description="Путь к папке первой стадии")
    stage_b_path: str = Field(..., description="Путь к папке второй стадии")


class CreateLinkRequest(BaseModel):
    left_block_id: str
    right_block_id: str


class DeleteLinkRequest(BaseModel):
    left_block_id: str
    right_block_id: str


class AutoLinkRequest(BaseModel):
    iou_threshold: float = Field(default=0.5, ge=0.0, le=1.0)


class GraphicDiffRequest(BaseModel):
    left_block_id: str
    right_block_id: str
    run_paid: bool = False
    model: Optional[str] = None  # по умолчанию config-овый


class AlignmentItem(BaseModel):
    slot: Optional[int] = None
    left_page: Optional[int] = None
    right_page: Optional[int] = None
    mode: Optional[str] = None
    note: Optional[str] = ""


class SaveAlignmentRequest(BaseModel):
    items: list[AlignmentItem]
    force: bool = False


class InsertBlankRequest(BaseModel):
    slot: int = Field(..., ge=1)
    side: str = Field(..., pattern="^(left|right)$")


class MoveAlignmentRequest(BaseModel):
    slot: int = Field(..., ge=1)
    direction: str = Field(..., pattern="^(up|down)$")


class InsertBlankSideRequest(BaseModel):
    slot: int = Field(..., ge=1)
    side: str = Field(..., pattern="^(left|right)$")


class MovePageSideRequest(BaseModel):
    slot: int = Field(..., ge=1)
    side: str = Field(..., pattern="^(left|right)$")
    direction: str = Field(..., pattern="^(up|down)$")


class DeletePageSideRequest(BaseModel):
    slot: int = Field(..., ge=1)
    side: str = Field(..., pattern="^(left|right)$")


class UpdatePairMatchRequest(BaseModel):
    right_pdf: Optional[str] = None
    right_md: Optional[str] = None
    right_result_json: Optional[str] = None
    status: Optional[str] = "manual"


class CreateManualPairRequest(BaseModel):
    left_pdf: Optional[str] = None
    right_pdf: Optional[str] = None
    left_md: Optional[str] = None
    left_result_json: Optional[str] = None
    right_md: Optional[str] = None
    right_result_json: Optional[str] = None


class DeletePairRequest(BaseModel):
    hard: bool = False


class PatchFindingRequest(BaseModel):
    status: Optional[str] = None
    severity: Optional[str] = None
    user_note: Optional[str] = None


class BulkPatchFindingsRequest(BaseModel):
    ids: list[str]
    patch: dict = Field(default_factory=dict)
    include_deleted: bool = False


class GraphicDiffJobItem(BaseModel):
    pair_id: str
    left_block_id: str
    right_block_id: str


class CreateGraphicDiffJobRequest(BaseModel):
    scope: str = Field(..., pattern="^(selected|pair|session)$")
    pair_id: Optional[str] = None
    items: Optional[list[GraphicDiffJobItem]] = None
    run_paid: bool = False
    confirm_paid: bool = False
    model: Optional[str] = None


class CreateReportRequest(BaseModel):
    format: str = Field("md", pattern="^(md|html|json|pdf|docx)$")
    filters: Optional[dict] = None
    include_rejected: bool = False
    include_ignored: bool = False
    include_images: bool = True
    include_llm_summary: bool = True
    include_user_notes: bool = True
    include_child_findings: bool = True


# ─── Sessions ────────────────────────────────────────────────────────────


@router.post("/sessions")
async def create_session(req: CreateSessionRequest):
    if not (req.stage_a_path or "").strip() or not (req.stage_b_path or "").strip():
        raise HTTPException(400, "stage_a_path и stage_b_path обязательны")
    # Allowlist (Задача 8): если env задан — проверим, что обе папки внутри
    try:
        store.assert_path_in_allowlist(req.stage_a_path)
        store.assert_path_in_allowlist(req.stage_b_path)
    except PermissionError as exc:
        raise HTTPException(403, str(exc)) from exc
    try:
        session, warnings = store.create_session(req.stage_a_path, req.stage_b_path)
    except OSError as exc:
        raise HTTPException(400, f"Ошибка доступа к папкам: {exc}") from exc
    return {
        "session_id": session["id"],
        "pairs": session.get("pairs") or [],
        "warnings": warnings,
        "created_at": session.get("created_at"),
        "stage_a_path": session.get("stage_a_path"),
        "stage_b_path": session.get("stage_b_path"),
    }


@router.get("/objects")
async def list_comparison_objects():
    """Автосписок «объектов» под allowlist-root'ами.

    Объект = подпапка с минимум двумя `stage_*` директориями внутри.
    UI использует это для селекта вместо ручного ввода путей.
    """
    return objects_mod.list_objects()


# ─── Saved configuration ──────────────────────────────────────────────────


class SaveConfigRequest(BaseModel):
    """Тело для PUT /saved-config (legacy path-only сохранение).

    Полная каноничная конфигурация сохраняется через
    POST /sessions/{sid}/save-canonical — там подтягиваются пары/режимы.
    """

    stage_a_path: str = Field(..., description="Абсолютный путь к stage_1 директории")
    stage_b_path: str = Field(..., description="Абсолютный путь к stage_2 директории")
    object_label: Optional[str] = Field(default=None, description="UI-имя объекта")
    stage_a_label: Optional[str] = Field(default=None, description="UI-имя стадии A")
    stage_b_label: Optional[str] = Field(default=None, description="UI-имя стадии B")
    note: Optional[str] = Field(default=None, description="Свободная пометка")


@router.get("/saved-config")
async def get_saved_config_endpoint():
    """Прочитать «сохранённую конфигурацию» Stage Comparison.

    Используется UI для кнопки «Применить сохранённую конфигурацию»:
    клик → fetch → автозаполнение stage_a_path/stage_b_path в форме
    создания сессии.

    Возвращает ``{"saved": false}`` если конфиг ещё не сохранён, иначе
    объект с полями stage_a_path / stage_b_path / object_label /
    stage_a_label / stage_b_label / saved_at / note.
    """
    cfg = saved_config_mod.load_saved_config()
    if cfg is None:
        return {"saved": False}
    return {"saved": True, **cfg}


@router.put("/saved-config")
async def put_saved_config_endpoint(req: SaveConfigRequest):
    """Сохранить «сохранённую конфигурацию» (перезаписать существующую).

    Проверяет allowlist для stage_a_path / stage_b_path, чтобы кто-то не
    закрепил ссылку на чужую папку вне разрешённого root'а.
    """
    try:
        store.assert_path_in_allowlist(req.stage_a_path)
        store.assert_path_in_allowlist(req.stage_b_path)
    except PermissionError as exc:
        raise HTTPException(403, str(exc)) from exc
    try:
        saved = saved_config_mod.save_saved_config(
            stage_a_path=req.stage_a_path,
            stage_b_path=req.stage_b_path,
            object_label=req.object_label,
            stage_a_label=req.stage_a_label,
            stage_b_label=req.stage_b_label,
            note=req.note,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except OSError as exc:
        raise HTTPException(500, f"Ошибка записи saved-config: {exc}") from exc
    return {"saved": True, **saved}


@router.delete("/saved-config")
async def delete_saved_config_endpoint():
    """Удалить «сохранённую конфигурацию» (UI скроет/задизейблит кнопку)."""
    deleted = saved_config_mod.clear_saved_config()
    return {"deleted": deleted}


# ─── Canonical configuration (UX-обёртка над saved-config) ───────────────
# «Каноничная конфигурация» = одна актуальная рабочая конфигурация объекта:
# пара stage_a/stage_b, canonical_session_id и компактный summary пар (без
# артефактов анализа). Старый saved-config endpoint оставлен для обратной
# совместимости (одни пути), но обычный UI работает через canonical-config.


def _canonical_payload(cfg: Optional[dict]) -> dict:
    """Привести cfg к payload для UI с разрешённой каноничной сессией.

    Если canonical_session_id указывает на несуществующую сессию,
    возвращается ``canonical_session_available=false`` — UI покажет
    предупреждение «Каноничная конфигурация недоступна».
    """
    if cfg is None:
        return {"saved": False}
    sid = (cfg.get("canonical_session_id") or "").strip() or None
    session_available = False
    if sid:
        try:
            session_available = store.get_session(sid) is not None
        except Exception:  # noqa: BLE001
            session_available = False
    return {
        "saved": True,
        "canonical_session_available": session_available if sid else None,
        **cfg,
    }


@router.get("/canonical-config")
async def get_canonical_config():
    """Каноничная конфигурация объекта (одна актуальная).

    Возвращает ``{"saved": false}`` если ничего не сохранено. Иначе
    включает все поля saved-config + ``canonical_session_available``
    (bool, есть ли canonical_session_id физически на диске).
    """
    return _canonical_payload(saved_config_mod.load_saved_config())


@router.post("/sessions/{session_id}/save-canonical")
async def save_session_as_canonical(session_id: str, req: Optional[dict] = None):
    """Сохранить текущую сессию как каноничную конфигурацию объекта.

    Тело: ``{object_label?, stage_a_label?, stage_b_label?, note?,
    updated_by?}``. Все поля опциональны — служат для UI badge'а.
    Перезаписывает предыдущую каноничную конфигурацию (история не
    ведётся).
    """
    sess = store.get_session(session_id)
    if sess is None:
        raise HTTPException(404, f"Сессия {session_id} не найдена")
    stage_a_path = (sess.get("stage_a_path") or "").strip()
    stage_b_path = (sess.get("stage_b_path") or "").strip()
    if not stage_a_path or not stage_b_path:
        raise HTTPException(400, "Сессия не содержит stage_a_path/stage_b_path")
    try:
        store.assert_path_in_allowlist(stage_a_path)
        store.assert_path_in_allowlist(stage_b_path)
    except PermissionError as exc:
        raise HTTPException(403, str(exc)) from exc

    body = req if isinstance(req, dict) else {}
    try:
        saved = saved_config_mod.save_saved_config(
            stage_a_path=stage_a_path,
            stage_b_path=stage_b_path,
            object_label=body.get("object_label"),
            stage_a_label=body.get("stage_a_label"),
            stage_b_label=body.get("stage_b_label"),
            note=body.get("note"),
            canonical_session_id=session_id,
            pairs=sess.get("pairs") or [],
            updated_by=body.get("updated_by"),
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except OSError as exc:
        raise HTTPException(500, f"Ошибка записи canonical-config: {exc}") from exc
    return _canonical_payload(saved)


@router.get("/canonical-config/open")
async def open_canonical_config():
    """Получить полные данные каноничной сессии (для автозагрузки UI).

    Контракт:
      * ``saved=false`` — ничего не сохранено.
      * ``canonical_session_available=false`` — canonical_session_id есть,
        но сессия не найдена (повреждена / удалена) → UI предложит
        перезапустить сопоставление.
      * иначе — полный объект сессии + ``canonical_config`` (метаданные
        канона) + ``config_hash_current`` (recompute по факту, для
        invalidation analysis artifacts).
    """
    cfg = saved_config_mod.load_saved_config()
    if cfg is None:
        return {"saved": False}
    sid = (cfg.get("canonical_session_id") or "").strip()
    if not sid:
        # legacy v1 config: только пути, сессии нет
        return {
            "saved": True,
            "canonical_session_id": None,
            "canonical_session_available": None,
            "canonical_config": cfg,
        }
    sess = store.get_session(sid)
    if sess is None:
        return {
            "saved": True,
            "canonical_session_id": sid,
            "canonical_session_available": False,
            "canonical_config": cfg,
        }
    # Пересчитываем config_hash по текущему состоянию пар сессии — если он
    # отличается от saved_hash, UI покажет «результаты могут быть устаревшими».
    current_pairs_summary = []
    for idx, p in enumerate(sess.get("pairs") or []):
        if isinstance(p, dict) and p.get("id"):
            current_pairs_summary.append({
                "pair_id": str(p.get("id")),
                "left_filename": ((p.get("left") or {}).get("filename") or None),
                "right_filename": ((p.get("right") or {}).get("filename") or None),
                "left_pdf_path": ((p.get("left") or {}).get("pdf_path") or None),
                "right_pdf_path": ((p.get("right") or {}).get("pdf_path") or None),
                "disabled": str(p.get("status") or "") == "disabled",
                "status": (p.get("status") or None),
                "analysis_mode": (p.get("analysis_mode") or None),
                "manual_links_count": len(p.get("links") or []),
                "order": idx + 1,
            })
    current_hash = saved_config_mod._compute_config_hash(current_pairs_summary)
    return {
        "saved": True,
        "canonical_session_id": sid,
        "canonical_session_available": True,
        "config_hash_saved": cfg.get("config_hash"),
        "config_hash_current": current_hash,
        "config_stale": bool(cfg.get("config_hash") and cfg.get("config_hash") != current_hash),
        "canonical_config": cfg,
        "session": sess,
    }


@router.get("/sessions/{session_id}/unmatched")
async def list_unmatched_endpoint(session_id: str):
    try:
        return store.list_unmatched(session_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs")
async def create_pair(session_id: str, req: CreateManualPairRequest):
    try:
        # Allowlist для путей (на всякий случай)
        if req.left_pdf:
            store.assert_path_in_allowlist(req.left_pdf)
        if req.right_pdf:
            store.assert_path_in_allowlist(req.right_pdf)
    except PermissionError as exc:
        raise HTTPException(403, str(exc)) from exc
    try:
        pair = store.create_manual_pair(
            session_id,
            left_pdf=req.left_pdf, right_pdf=req.right_pdf,
            left_md=req.left_md, left_result_json=req.left_result_json,
            right_md=req.right_md, right_result_json=req.right_result_json,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return pair


@router.post("/sessions/{session_id}/pairs/confirm-all")
async def confirm_all_maybe_pairs(session_id: str):
    """Подтвердить все пары со статусом ``maybe`` (массовое «Сопоставить все»).

    Не меняет PDF, alignment, links — только перезаписывает статус.
    """
    try:
        return store.confirm_maybe_pairs(session_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put("/sessions/{session_id}/pairs/{pair_id}/match")
async def update_pair_match_endpoint(session_id: str, pair_id: str, req: UpdatePairMatchRequest):
    try:
        if req.right_pdf:
            store.assert_path_in_allowlist(req.right_pdf)
    except PermissionError as exc:
        raise HTTPException(403, str(exc)) from exc
    try:
        return store.update_pair_match(
            session_id, pair_id,
            right_pdf=req.right_pdf, right_md=req.right_md,
            right_result_json=req.right_result_json, status=req.status,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.delete("/sessions/{session_id}/pairs/{pair_id}")
async def delete_pair_endpoint(session_id: str, pair_id: str, hard: bool = Query(False)):
    try:
        store.delete_pair(session_id, pair_id, hard=hard)
        return {"ok": True, "hard": hard}
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


class PairOrderRequest(BaseModel):
    pair_ids: list[str] = Field(..., description="Ordered list of pair_ids (top → bottom)")


@router.put("/sessions/{session_id}/pair-order")
async def set_pair_order_endpoint(session_id: str, req: PairOrderRequest):
    """Drag-and-drop reorder пар в session.json → pair_order.

    UI шлёт список pair_id'ов в желаемом порядке (сверху вниз).
    Backend нормализует (отфильтровывает несуществующие, добавляет в
    конец потерянные) и сохраняет в session.json. Возвращает фактический
    порядок после нормализации.
    """
    try:
        new_order = store.set_pair_order(session_id, req.pair_ids or [])
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return {"ok": True, "pair_order": new_order}


@router.get("/sessions")
async def list_sessions_endpoint():
    return {"sessions": store.list_sessions()}


@router.get("/sessions/{session_id}")
async def get_session_endpoint(session_id: str):
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    return session


# ─── Pair view ───────────────────────────────────────────────────────────


@router.get("/sessions/{session_id}/pairs/{pair_id}")
async def get_pair(session_id: str, pair_id: str):
    view = store.get_pair_view(session_id, pair_id)
    if view is None:
        raise HTTPException(404, "Пара не найдена или сессия отсутствует")
    return view


@router.get("/sessions/{session_id}/pairs/{pair_id}/page-image")
async def get_page_image(
    session_id: str,
    pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
    page: int = Query(1, ge=1),
    target_long_side: int = Query(1400, ge=400, le=3500),
):
    try:
        png = store.render_pdf_page(
            session_id, pair_id, side, page,
            target_long_side=target_long_side,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("page-image render failed")
        raise HTTPException(500, f"Ошибка рендера PDF: {exc}") from exc
    return FileResponse(str(png), media_type="image/png")


@router.get("/sessions/{session_id}/pairs/{pair_id}/block-image")
async def get_block_image(
    session_id: str,
    pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
    block_id: str = Query(...),
    target_long_side: int = Query(1200, ge=200, le=3500),
):
    try:
        png = store.render_block_crop(
            session_id, pair_id, side, block_id,
            target_long_side=target_long_side,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("block-image render failed")
        raise HTTPException(500, f"Ошибка кропа блока: {exc}") from exc
    return FileResponse(str(png), media_type="image/png")


# ─── Block links ─────────────────────────────────────────────────────────


@router.post("/sessions/{session_id}/pairs/{pair_id}/auto-link")
async def auto_link(session_id: str, pair_id: str, req: AutoLinkRequest):
    try:
        return store.run_auto_link(session_id, pair_id, iou_threshold=req.iou_threshold)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/links")
async def create_link(session_id: str, pair_id: str, req: CreateLinkRequest):
    try:
        link = store.add_manual_link(session_id, pair_id, req.left_block_id, req.right_block_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return link


@router.delete("/sessions/{session_id}/pairs/{pair_id}/links")
async def remove_link(session_id: str, pair_id: str, req: DeleteLinkRequest):
    try:
        removed = store.delete_link(session_id, pair_id, req.left_block_id, req.right_block_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return {"removed": removed}


# ─── Pair config templates (auto-restore links + alignment) ─────────────


@router.get("/sessions/{session_id}/pairs/{pair_id}/template-status")
async def pair_template_status_endpoint(session_id: str, pair_id: str):
    """Есть ли сохранённый шаблон для этой пары PDF и применён ли он сейчас."""
    try:
        return pair_template_mod.template_status(session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/save-template")
async def pair_template_save_endpoint(session_id: str, pair_id: str):
    """Сохранить снимок links + page_alignment пары как шаблон по identity путей."""
    try:
        payload = pair_template_mod.save_template(session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    # Не возвращаем тяжёлый links-payload — только метаданные.
    return {
        "ok": True,
        "key": payload.get("key"),
        "saved_at": payload.get("saved_at"),
        "links_count": payload.get("links_count"),
        "left_pdf_name": payload.get("left_pdf_name"),
        "right_pdf_name": payload.get("right_pdf_name"),
    }


@router.post("/sessions/{session_id}/pairs/{pair_id}/apply-template")
async def pair_template_apply_endpoint(session_id: str, pair_id: str):
    """Найти шаблон по identity и применить (links + alignment перезаписываются)."""
    try:
        return pair_template_mod.apply_template(session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/clear-template")
async def pair_template_clear_endpoint(session_id: str, pair_id: str):
    """Снять пометку 'применён шаблон' с пары (сами links и alignment сохраняются)."""
    try:
        return pair_template_mod.clear_applied_template(session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


# ─── Page alignment ──────────────────────────────────────────────────────


@router.get("/sessions/{session_id}/pairs/{pair_id}/page-alignment")
async def get_page_alignment(session_id: str, pair_id: str):
    try:
        return store.get_alignment(session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put("/sessions/{session_id}/pairs/{pair_id}/page-alignment")
async def put_page_alignment(
    session_id: str, pair_id: str, req: SaveAlignmentRequest,
    force: bool = Query(False, description="Сохранить карту даже при ошибках валидации"),
):
    """Сохранение карты страниц.

    Если есть validation_errors И не передан force=true (ни в query, ни в body) —
    backend НЕ сохраняет карту и возвращает HTTP 422 с {ok:false, validation_errors}.

    Если force=true — карта сохраняется, response: {ok:true, saved_with_warnings:true,
    validation_errors:[...]}.
    """
    try:
        items = [it.model_dump() for it in (req.items or [])]
        # force из body имеет приоритет над query
        effective_force = bool(req.force or force)
        payload = store.save_alignment(session_id, pair_id, items, force=effective_force)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if not payload.get("ok"):
        # 422 Unprocessable Entity — клиент пусть покажет ошибки в модалке
        raise HTTPException(status_code=422, detail=payload)
    return payload


@router.post("/sessions/{session_id}/pairs/{pair_id}/page-alignment/suggest")
async def suggest_alignment_endpoint(session_id: str, pair_id: str):
    """Предложить новую карту страниц на основе page fingerprint'ов. Не применяет."""
    try:
        return store.suggest_alignment(session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/page-alignment/insert-blank")
async def insert_blank_alignment(session_id: str, pair_id: str, req: InsertBlankRequest):
    try:
        return store.alignment_insert_blank(session_id, pair_id, req.slot, req.side)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/page-alignment/move")
async def move_alignment(session_id: str, pair_id: str, req: MoveAlignmentRequest):
    try:
        return store.alignment_move(session_id, pair_id, req.slot, req.direction)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/page-alignment/reset")
async def reset_alignment(session_id: str, pair_id: str):
    try:
        return store.alignment_reset(session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/page-alignment/insert-blank-side")
async def insert_blank_alignment_side(session_id: str, pair_id: str, req: InsertBlankSideRequest):
    """Вставить пустой лист только на выбранной стороне (left/right).

    Левая и правая «дорожки» страниц независимы. Другая сторона остаётся на месте,
    slot'ы пересчитываются, links автоматически переоцениваются на stale/cross-page.
    """
    try:
        return store.alignment_insert_blank_side(session_id, pair_id, req.slot, req.side)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/page-alignment/move-page-side")
async def move_page_alignment_side(session_id: str, pair_id: str, req: MovePageSideRequest):
    """Переместить страницу одной стороны вверх/вниз; другая сторона не меняется."""
    try:
        return store.alignment_move_page_side(
            session_id, pair_id, req.slot, req.side, req.direction
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/page-alignment/delete-page-side")
async def delete_page_alignment_side(session_id: str, pair_id: str, req: DeletePageSideRequest):
    """Удалить страницу одной стороны в slot'е; другая сторона не меняется."""
    try:
        return store.alignment_delete_page_side(
            session_id, pair_id, req.slot, req.side
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


# ─── Text diff ───────────────────────────────────────────────────────────


@router.get("/sessions/{session_id}/pairs/{pair_id}/text-diff")
async def text_diff_endpoint(session_id: str, pair_id: str):
    """Legacy: построчный difflib MD-файлов. Используется как debug/fallback
    после переезда на семантический text-llm-diff. См. POST/GET .../text-llm-diff."""
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        raise HTTPException(404, "Пара не найдена")
    left_md = (pair.get("left") or {}).get("md_path")
    right_md = (pair.get("right") or {}).get("md_path")
    diff = diff_text.build_text_diff(left_md, right_md)
    diff["left_md_path"] = left_md
    diff["right_md_path"] = right_md
    return diff


# ─── Text LLM diff (Claude Sonnet, explicit user-triggered) ──────────────


class CreateTextLLMJobRequest(BaseModel):
    scope: str = Field(..., pattern="^(pair|session|selected)$")
    pair_id: Optional[str] = None
    pair_ids: Optional[list[str]] = None
    confirm: bool = False
    force: bool = False  # сейчас runner всегда force=True; зарезервировано


@router.get("/sessions/{session_id}/pairs/{pair_id}/text-llm-diff")
async def get_text_llm_diff_endpoint(session_id: str, pair_id: str):
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        raise HTTPException(404, "Пара не найдена")
    existing = text_llm_mod.get_text_llm_diff(session_id, pair_id)
    if existing is None:
        return {
            "status": "not_run",
            "left_md_path": (pair.get("left") or {}).get("md_path"),
            "right_md_path": (pair.get("right") or {}).get("md_path"),
        }
    return existing


class PreflightRequest(BaseModel):
    scope: str = Field("session", pattern="^(pair|session|selected)$")
    pair_id: Optional[str] = None
    pair_ids: Optional[list[str]] = None
    force: bool = False


@router.post("/sessions/{session_id}/text-llm-preflight")
async def text_llm_preflight_session_endpoint(session_id: str, req: PreflightRequest):
    """Pre-run оценка для batch: суммирует runnable пары, агрегирует skipped."""
    try:
        return text_llm_preflight_mod.estimate_session(
            session_id,
            scope=req.scope, pair_id=req.pair_id, pair_ids=req.pair_ids,
            force=bool(req.force),
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/sessions/{session_id}/text-llm-diff-jobs")
async def create_text_llm_job_endpoint(session_id: str, req: CreateTextLLMJobRequest):
    if not req.confirm:
        # Создаём rejected job, чтобы он остался в истории
        job = text_llm_jobs_mod.create_text_llm_job(
            session_id, scope=req.scope, pair_id=req.pair_id,
            pair_ids=req.pair_ids, confirm=False,
        )
        return job
    # Hard-limit enforcement: для batch (session/selected) проверяем агрегатную
    # оценку и блокируем запуск, если выше HARD_*. Per-pair scope разрешаем
    # после подтверждения — там пользователь явно знает, что запускает.
    if req.scope in ("session", "selected"):
        try:
            preflight = text_llm_preflight_mod.estimate_session(
                session_id, scope=req.scope, pair_id=req.pair_id,
                pair_ids=req.pair_ids, force=bool(req.force),
            )
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        limits = preflight.get("limits") or {}
        if limits.get("cost_hard") or limits.get("duration_hard"):
            raise HTTPException(
                422,
                {
                    "code": "batch_limit_exceeded",
                    "message": "Оценка превышает безопасный лимит. Запустите анализ по отдельным парам.",
                    "preflight": preflight,
                },
            )
    try:
        job = text_llm_jobs_mod.create_text_llm_job(
            session_id, scope=req.scope, pair_id=req.pair_id,
            pair_ids=req.pair_ids, confirm=True,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    if job.get("status") == "queued":
        text_llm_jobs_mod.start_job_in_background(session_id, job["id"])
    return job


@router.get("/sessions/{session_id}/text-llm-diff-jobs/{job_id}")
async def get_text_llm_job_endpoint(session_id: str, job_id: str):
    job = text_llm_jobs_mod.get_job(session_id, job_id)
    if job is None:
        raise HTTPException(404, "Job не найден")
    return job


@router.post("/sessions/{session_id}/text-llm-diff-jobs/{job_id}/cancel")
async def cancel_text_llm_job_endpoint(session_id: str, job_id: str):
    job = text_llm_jobs_mod.cancel_job(session_id, job_id)
    if job is None:
        raise HTTPException(404, "Job не найден")
    return job


@router.get("/sessions/{session_id}/text-llm-diff-flat")
async def text_llm_diff_flat_endpoint(session_id: str):
    """Плоский список текстовых смысловых изменений по всей сессии.

    Агрегирует все `text_llm_diff.json` пар сессии, привязывает каждое
    изменение к PDF-странице/листу/alignment-slot через `text_location`.
    Чтения только — никаких LLM-вызовов, никаких записей на диск.
    """
    try:
        return text_llm_flat_mod.build_flat(session_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/sessions/{session_id}/text-llm-config")
async def text_llm_config_endpoint(session_id: str):
    """Лёгкая инфо-ручка для UI: enabled / provider / model / availability."""
    cfg = text_llm_provider_mod.load_config()
    provider, _ = text_llm_provider_mod.resolve_provider(cfg)
    info = {
        "enabled": cfg.enabled,
        "provider": cfg.provider,
        "model": cfg.model,
        "max_chars": cfg.max_chars,
        "timeout_sec": cfg.timeout_sec,
        "available": False,
        "reason": None,
    }
    if provider is not None:
        ok, reason = provider.check_availability()
        info["available"] = bool(ok)
        info["reason"] = reason
    elif not cfg.enabled:
        info["reason"] = "disabled_via_env"
    else:
        info["reason"] = f"unknown_provider:{cfg.provider}"
    return info


# ─── MD enrichment (Qwen image descriptions для enriched MD) ─────────────


class MdEnrichmentRequest(BaseModel):
    side: str = Field("both", pattern="^(left|right|both)$")
    force: bool = False
    run_model: bool = False


class CreateMdEnrichmentJobRequest(BaseModel):
    scope: str = Field(..., pattern="^(pair|session|selected)$")
    pair_id: Optional[str] = None
    pair_ids: Optional[list[str]] = None
    side: str = Field("both", pattern="^(left|right|both)$")
    force: bool = False
    confirm: bool = False
    skip_done: bool = True


def _md_enrichment_pair_payload(
    session_id: str, pair_id: str, pair: dict,
) -> dict:
    left = pair.get("left") or {}
    right = pair.get("right") or {}
    left_summary = md_enrichment_mod.read_summary_only(session_id, pair_id, "left")
    right_summary = md_enrichment_mod.read_summary_only(session_id, pair_id, "right")
    left_summary["md_path"] = left.get("md_path")
    right_summary["md_path"] = right.get("md_path")

    def _side_ready(side_summary: dict) -> bool:
        if (side_summary.get("status") or "") != "done":
            return False
        if int(side_summary.get("errors") or 0) > 0:
            return False
        if int(side_summary.get("pending") or 0) > 0:
            return False
        enriched = side_summary.get("enriched_md_path")
        if not enriched:
            return False
        try:
            from pathlib import Path as _P
            return _P(enriched).exists()
        except Exception:  # noqa: BLE001
            return False

    return {
        "pair_id": pair_id,
        "left": left_summary,
        "right": right_summary,
        "ready_for_unified_analysis": _side_ready(left_summary) and _side_ready(right_summary),
    }


@router.get("/sessions/{session_id}/pairs/{pair_id}/md-enrichment")
async def get_md_enrichment_endpoint(session_id: str, pair_id: str):
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        raise HTTPException(404, "Пара не найдена")
    return _md_enrichment_pair_payload(session_id, pair_id, pair)


_FAILED_BLOCK_STATUSES = {"error", "no_image", "render_failed"}


@router.get("/sessions/{session_id}/pairs/{pair_id}/failed-blocks")
async def get_failed_blocks_endpoint(session_id: str, pair_id: str):
    """Список упавших image-блоков пары (status ∈ error/no_image/render_failed).

    Используется поповером на цифре «упало» в таблице пар: оператор кликает по
    числу, видит какие именно блоки не распознались, и может перейти к блоку на
    вкладке «Связь блоков». Агрегаты (block_metrics) этот endpoint не меняет —
    только читает per-block items[] из <side>_image_descriptions.json.
    """
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        raise HTTPException(404, "Пара не найдена")

    blocks: list[dict] = []
    for side in ("left", "right"):
        p = sc_paths_mod.text_enrichment_descriptions_path(session_id, pair_id, side)
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, ValueError):
            continue
        for it in data.get("items") or []:
            if not isinstance(it, dict):
                continue
            if (it.get("status") or "").lower() not in _FAILED_BLOCK_STATUSES:
                continue
            blocks.append({
                "side": side,
                "order": it.get("order"),
                "page": it.get("page"),
                "image_order_on_page": it.get("image_order_on_page"),
                "md_block_id": it.get("md_block_id"),
                "side_block_id": it.get("side_block_id"),
                "status": it.get("status"),
                "error": it.get("error"),
                "parse_error_detail": it.get("parse_error_detail"),
                "block_type": it.get("block_type"),
            })

    blocks.sort(key=lambda b: (
        0 if b["side"] == "left" else 1,
        b.get("page") or 0,
        b.get("order") or 0,
    ))
    return {"pair_id": pair_id, "blocks": blocks, "count": len(blocks)}


@router.get("/sessions/{session_id}/pairs/{pair_id}/enriched-md")
async def get_enriched_md_content_endpoint(
    session_id: str, pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
):
    """Содержимое `<side>_enriched.md` для просмотра в UI.

    Используется переключателем PDF ↔ MD в двухпанельном вьюере: вместо
    рендеренных страниц PDF показывается текст enriched MD соответствующей
    стороны. Если enrichment ещё не запускался — `exists=false`, content пуст.
    """
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        raise HTTPException(404, "Пара не найдена")
    md_path = sc_paths_mod.text_enrichment_md_path(session_id, pair_id, side)
    exists = md_path.exists()
    content = ""
    if exists:
        try:
            content = md_path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(500, f"Не удалось прочитать enriched MD: {exc}") from exc
    return {
        "side": side,
        "path": str(md_path),
        "filename": md_path.name,
        "exists": exists,
        "char_count": len(content),
        "content": content,
    }


@router.post("/sessions/{session_id}/pairs/{pair_id}/md-enrichment")
async def run_md_enrichment_endpoint(
    session_id: str, pair_id: str, req: MdEnrichmentRequest,
):
    """Запуск enrichment для одной пары.

    `run_model=False` — dry-run, никаких сетевых вызовов к LM Studio.
    `run_model=True`  — реально вызывает Qwen для каждого image-блока.
    """
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        raise HTTPException(404, "Пара не найдена")

    sides = ("left", "right") if req.side == "both" else (req.side,)
    cfg = graphic_local_mod.load_local_graphic_llm_config()

    if req.run_model:
        ok, reason = graphic_local_mod.check_local_graphic_llm_available(cfg)
        if not ok:
            raise HTTPException(
                422,
                {
                    "code": "local_vlm_unavailable",
                    "message": "Локальный VLM-провайдер не сконфигурирован.",
                    "reason": reason,
                },
            )

    results: dict[str, Any] = {}
    for s in sides:
        side_obj = (pair.get(s) or {})
        md_path = side_obj.get("md_path")
        rjp = side_obj.get("result_json_path")

        def _render(block_id: str, _sid=session_id, _pid=pair_id, _side=s):
            try:
                return store.render_block_crop(_sid, _pid, _side, block_id)
            except Exception:  # noqa: BLE001
                return None

        try:
            summary = await md_enrichment_mod.enrich_side(
                session_id, pair_id, s,
                md_path=md_path, result_json_path=rjp,
                render_crop=_render,
                run_model=bool(req.run_model),
                force=bool(req.force),
                cfg=cfg,
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            logger.exception("md-enrichment failed")
            raise HTTPException(500, f"md-enrichment ошибка: {exc}") from exc

        results[s] = {
            "side": s,
            "status": summary.status,
            "image_blocks": summary.image_blocks,
            "described": summary.described,
            "from_cache": summary.from_cache,
            "errors": summary.errors,
            "pending": summary.pending,
            "warnings": summary.warnings,
            "enriched_md_path": summary.enriched_md_path,
            "md_path": md_path,
        }

    return {"pair_id": pair_id, "ran_model": bool(req.run_model), **results}


@router.post("/sessions/{session_id}/md-enrichment-jobs")
async def create_md_enrichment_job_endpoint(
    session_id: str, req: CreateMdEnrichmentJobRequest,
):
    """Batch enrichment. Без confirm=true создаём rejected-job (для истории)."""
    if not req.confirm:
        try:
            job = md_enrichment_jobs_mod.create_md_enrichment_job(
                session_id, scope=req.scope, pair_id=req.pair_id,
                pair_ids=req.pair_ids, side=req.side,
                force=bool(req.force), confirm=False,
                skip_done=bool(req.skip_done),
            )
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        return md_enrichment_jobs_mod.get_job_with_progress(session_id, job["id"]) or job

    cfg = graphic_local_mod.load_local_graphic_llm_config()
    ok, reason = graphic_local_mod.check_local_graphic_llm_available(cfg)
    if not ok:
        raise HTTPException(
            422,
            {
                "code": "local_vlm_unavailable",
                "message": "Локальный VLM-провайдер не сконфигурирован.",
                "reason": reason,
            },
        )

    try:
        job = md_enrichment_jobs_mod.create_md_enrichment_job(
            session_id, scope=req.scope, pair_id=req.pair_id,
            pair_ids=req.pair_ids, side=req.side,
            force=bool(req.force), confirm=True,
            skip_done=bool(req.skip_done),
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    if job.get("status") == "queued":
        md_enrichment_jobs_mod.start_job_in_background(session_id, job["id"])
    return md_enrichment_jobs_mod.get_job_with_progress(session_id, job["id"]) or job


@router.get("/sessions/{session_id}/md-enrichment-jobs/active")
async def get_active_md_enrichment_job_endpoint(session_id: str):
    """Самая свежая job (running, иначе любая) — для resume в UI.

    Возвращает 204-подобный пустой объект, если jobs ещё не было.
    """
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    job = md_enrichment_jobs_mod.find_active_session_job(session_id)
    return {"job": job}


@router.get("/sessions/{session_id}/md-enrichment-jobs/{job_id}")
async def get_md_enrichment_job_endpoint(session_id: str, job_id: str):
    job = md_enrichment_jobs_mod.get_job_with_progress(session_id, job_id)
    if job is None:
        raise HTTPException(404, "Job не найден")
    return job


@router.post("/sessions/{session_id}/md-enrichment-jobs/{job_id}/cancel")
async def cancel_md_enrichment_job_endpoint(session_id: str, job_id: str):
    job = md_enrichment_jobs_mod.cancel_job(session_id, job_id)
    if job is None:
        raise HTTPException(404, "Job не найден")
    return md_enrichment_jobs_mod.get_job_with_progress(session_id, job_id) or job


# ─── Unified analysis: Qwen enrichment + Opus comparison ────────────────


class UnifiedAnalysisPreflightRequest(BaseModel):
    force_enrichment: bool = False
    force_compare: bool = False


class UnifiedAnalysisRunRequest(BaseModel):
    force_enrichment: bool = False
    force_compare: bool = False
    confirm: bool = False


class CreateUnifiedAnalysisJobRequest(BaseModel):
    scope: str = Field(..., pattern="^(pair|session|selected)$")
    pair_id: Optional[str] = None
    pair_ids: Optional[list[str]] = None
    force_enrichment: bool = False
    force_compare: bool = False
    confirm: bool = False
    # Pre-flight фильтр: пропустить пары, у которых enriched MD не готов /
    # суммарный размер превышает лимит / comparison уже done (если
    # force_compare=false). Используется session-level Opus batch с этапа
    # «Загрузка документации», чтобы не звать Qwen и не запускать too_large.
    skip_ineligible: bool = False


class UnifiedAnalysisBatchPreflightRequest(BaseModel):
    scope: str = Field(..., pattern="^(session|selected)$")
    pair_ids: Optional[list[str]] = None
    force_compare: bool = False


@router.get("/sessions/{session_id}/pairs/{pair_id}/unified-analysis")
async def get_unified_pair_status_endpoint(session_id: str, pair_id: str):
    """Статус unified-анализа для пары: enriched MD / comparison_result / counts."""
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        raise HTTPException(404, "Пара не найдена")

    enriched_status = enriched_compare_mod.enriched_md_status(session_id, pair_id)
    comp = enriched_compare_mod.get_comparison_result(session_id, pair_id)
    left_sum = md_enrichment_mod.read_summary_only(session_id, pair_id, "left")
    right_sum = md_enrichment_mod.read_summary_only(session_id, pair_id, "right")
    return {
        "pair_id": pair_id,
        "pair_label": f"{(pair.get('left') or {}).get('filename') or '—'} ↔ "
                       f"{(pair.get('right') or {}).get('filename') or '—'}",
        "enrichment": {
            "left": left_sum,
            "right": right_sum,
            "enriched_md_ready": enriched_status.get("ready"),
            "enriched_md_total_chars": enriched_status.get("total_chars"),
            "left_path": enriched_status.get("left", {}).get("path"),
            "right_path": enriched_status.get("right", {}).get("path"),
        },
        "comparison": {
            "status": (comp or {}).get("status") or "not_run",
            "changes_count": len((comp or {}).get("changes") or []),
            "updated_at": (comp or {}).get("updated_at"),
            "warnings": (comp or {}).get("warnings") or [],
            "error": (comp or {}).get("error"),
            "result_path": str(sc_paths_mod.enriched_comparison_result_path(session_id, pair_id))
                            if comp else None,
        },
    }


@router.post("/sessions/{session_id}/pairs/{pair_id}/unified-analysis/preflight")
async def unified_pair_preflight_endpoint(
    session_id: str, pair_id: str, req: UnifiedAnalysisPreflightRequest,
):
    """Pre-run оценка: image-блоки, cache hits, Qwen/Opus availability, длительность."""
    try:
        pre = unified_analysis_mod.preflight_pair(
            session_id, pair_id,
            force_enrichment=req.force_enrichment,
            force_compare=req.force_compare,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return pre.as_dict()


@router.post("/sessions/{session_id}/pairs/{pair_id}/unified-analysis")
async def unified_pair_run_endpoint(
    session_id: str, pair_id: str, req: UnifiedAnalysisRunRequest,
):
    """Запустить unified-анализ для одной пары (синхронно).

    Без confirm=true возвращает preflight без запуска моделей. Это защита
    от случайного live-вызова Qwen/Opus.
    """
    if not req.confirm:
        try:
            pre = unified_analysis_mod.preflight_pair(
                session_id, pair_id,
                force_enrichment=req.force_enrichment,
                force_compare=req.force_compare,
            )
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        return {
            "ok": False,
            "status": "rejected_no_confirm",
            "preflight": pre.as_dict(),
        }
    try:
        res = await unified_analysis_mod.run_pair(
            session_id, pair_id,
            force_enrichment=req.force_enrichment,
            force_compare=req.force_compare,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    # После одиночного запуска обновляем unified findings.
    try:
        unified_findings_mod.rebuild_unified_findings(session_id)
    except Exception:  # noqa: BLE001
        logger.exception("unified_findings rebuild failed")
    return {"ok": True, "result": res.as_dict()}


@router.post("/sessions/{session_id}/unified-analysis-jobs")
async def create_unified_job_endpoint(
    session_id: str, req: CreateUnifiedAnalysisJobRequest,
):
    """Batch unified job. Без confirm=true создаём rejected (для истории)."""
    try:
        job = unified_jobs_mod.create_unified_job(
            session_id,
            scope=req.scope, pair_id=req.pair_id, pair_ids=req.pair_ids,
            force_enrichment=req.force_enrichment,
            force_compare=req.force_compare,
            confirm=req.confirm,
            skip_ineligible=req.skip_ineligible,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if job.get("status") == "queued":
        unified_jobs_mod.start_job_in_background(session_id, job["id"])
    return unified_jobs_mod.get_job_with_progress(session_id, job["id"]) or job


@router.post("/sessions/{session_id}/unified-analysis-jobs/preflight")
async def unified_batch_preflight_endpoint(
    session_id: str, req: UnifiedAnalysisBatchPreflightRequest,
):
    """Dry-run сводка перед запуском Opus batch (без запуска моделей)."""
    try:
        return unified_jobs_mod.preflight_session_for_batch(
            session_id,
            scope=req.scope,
            pair_ids=req.pair_ids,
            force_compare=bool(req.force_compare),
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/sessions/{session_id}/unified-analysis-jobs/active")
async def get_active_unified_job_endpoint(session_id: str):
    """Самая релевантная unified-job сессии — для resume в UI.

    Возвращает {"job": <job_with_progress>} или {"job": null}.
    """
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    job = unified_jobs_mod.find_active_session_job(session_id)
    return {"job": job}


@router.get("/sessions/{session_id}/unified-analysis-jobs/{job_id}")
async def get_unified_job_endpoint(session_id: str, job_id: str):
    job = unified_jobs_mod.get_job_with_progress(session_id, job_id)
    if job is None:
        raise HTTPException(404, "Job не найден")
    return job


@router.post("/sessions/{session_id}/unified-analysis-jobs/{job_id}/cancel")
async def cancel_unified_job_endpoint(session_id: str, job_id: str):
    job = unified_jobs_mod.cancel_job(session_id, job_id)
    if job is None:
        raise HTTPException(404, "Job не найден")
    return unified_jobs_mod.get_job_with_progress(session_id, job_id) or job


@router.get("/sessions/{session_id}/unified-diff-flat")
async def unified_diff_flat_endpoint(session_id: str, pair_id: Optional[str] = None):
    """Единый плоский список unified findings по всей сессии.

    Read-only — никаких LLM/моделей. Использует существующие
    comparison_result.json по парам.

    `pair_id`: query-параметр. Если задан, summary и items считаются только
    по этой паре. UI вкладки «Расхождения» вызывает endpoint с pair_id
    активной PDF-пары, чтобы не подмешивать findings других пар. Без
    параметра — поведение по всей сессии (legacy / «Показать все пары»).
    """
    try:
        return unified_findings_mod.build_unified_flat(session_id, pair_id=pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


_SC_UNIFIED_SOURCE_LABELS = {
    "text": "Текст",
    "image_enrichment": "Описание изобр.",
    "scheme_analysis": "Схема",
    "table": "Таблица",
    "stamp": "Штамп",
    "mixed": "Текст + изобр.",
}


@router.get("/sessions/{session_id}/unified-diff-flat/export.xlsx")
async def unified_diff_flat_export_xlsx(
    session_id: str,
    pair_id: Optional[str] = None,
):
    """Экспорт таблицы расхождений в Excel (xlsx).

    Формат соответствует UI-таблице на вкладке «Расхождения»:
    №, Место (лист/стр.PDF/PDF-пара), Важность, Изменение (title + summary),
    Было, Стало, Влияние, Стоимость, Источник, На ручную проверку.

    Если задан `pair_id` — выгружаем только эту PDF-пару (соответствует
    текущему scope в UI). Без параметра — все пары сессии.
    """
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
    except ImportError:
        raise HTTPException(500, "openpyxl not installed")

    try:
        flat = unified_findings_mod.build_unified_flat(session_id, pair_id=pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc

    items = flat.get("items") or []

    wb = Workbook()
    ws = wb.active
    ws.title = "Расхождения"

    header = [
        "№", "Лист", "Стр. PDF", "PDF-пара", "Важность",
        "Изменение", "Описание",
        "Было", "Стало",
        "Влияние", "Стоимость",
        "Источник", "На ручную проверку",
    ]
    ws.append(header)
    thin = Side(border_style="thin", color="D1D5DB")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for cell in ws[1]:
        cell.font = Font(bold=True, size=10)
        cell.fill = PatternFill("solid", fgColor="E5E7EB")
        cell.alignment = Alignment(wrap_text=True, vertical="center", horizontal="center")
        cell.border = border

    sev_fill = {
        "high":   PatternFill("solid", fgColor="FEE2E2"),
        "medium": PatternFill("solid", fgColor="FEF3C7"),
        "low":    PatternFill("solid", fgColor="DBEAFE"),
    }
    was_fill = PatternFill("solid", fgColor="FEF2F2")
    became_fill = PatternFill("solid", fgColor="ECFDF5")
    impact_fill = PatternFill("solid", fgColor="FEFCE8")

    for i, it in enumerate(items, start=1):
        page = it.get("page")
        if isinstance(page, list):
            page_str = ", ".join(str(p) for p in page if p is not None)
        elif page is None:
            page_str = ""
        else:
            page_str = str(page)

        old_value = it.get("old_value") or ((it.get("evidence_left") or {}).get("quote")) or ""
        new_value = it.get("new_value") or ((it.get("evidence_right") or {}).get("quote")) or ""
        cost = it.get("cost_impact")
        cost_str = "" if (not cost or cost == "none") else cost
        source_layer = it.get("source_layer") or ""
        source_label = _SC_UNIFIED_SOURCE_LABELS.get(source_layer, source_layer)

        ws.append([
            i,
            it.get("sheet") or "",
            page_str,
            it.get("pair_label") or "",
            it.get("severity") or "",
            it.get("title") or "",
            it.get("summary") or "",
            old_value,
            new_value,
            it.get("construction_impact") or "",
            cost_str,
            source_label,
            "да" if it.get("requires_human_review") else "",
        ])
        row = ws[ws.max_row]
        sev = (it.get("severity") or "").lower()
        # №-ячейка подкрашена под severity
        if sev in sev_fill:
            row[0].fill = sev_fill[sev]
            row[4].fill = sev_fill[sev]
        row[7].fill = was_fill         # Было
        row[8].fill = became_fill      # Стало
        row[9].fill = impact_fill      # Влияние
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")
            cell.border = border
            cell.font = Font(size=10)

    widths = [5, 14, 10, 30, 11, 38, 50, 38, 38, 32, 12, 18, 11]
    for col_idx, w in enumerate(widths, start=1):
        ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = w
    ws.freeze_panes = "A2"

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    safe_sid = re.sub(r"[^A-Za-z0-9_.-]", "_", session_id)[:64] or "session"
    scope = "pair" if pair_id else "all"
    fname = f"stage_comparison_{safe_sid}_{scope}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ─── Unified GROUPED (deterministic post-processing) ─────────────────────


class RegroupRequest(BaseModel):
    force: bool = True


@router.get("/sessions/{session_id}/unified-grouped")
async def unified_grouped_endpoint(
    session_id: str,
    pair_id: Optional[str] = None,
    include_formal: bool = False,
    force_rebuild: bool = False,
    significance: Optional[str] = None,
    theme: Optional[str] = None,
):
    """Сгруппированный реестр значимых отличий (без LLM).

    Lazy-build: если `unified_findings_grouped.json` отсутствует, собираем
    его из `unified_findings.json` (или live flat) и сохраняем на диск.
    `force_rebuild=true` всегда пересобирает.

    Query params:
        pair_id          фильтр по конкретной паре
        include_formal   true → отдать hidden_formal_groups; default false
        significance     high|medium|low|formal — фильтр groups
        theme            фильтр по теме (см. THEMES в unified_grouping.py)
        force_rebuild    пересборка из flat findings
    """
    try:
        return unified_grouping_mod.get_unified_grouped(
            session_id,
            pair_id=pair_id,
            include_formal=include_formal,
            force_rebuild=force_rebuild,
            significance=significance,
            theme=theme,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


class ExpertDecisionItem(BaseModel):
    item_id: str
    decision: str = Field(..., pattern="^(accepted|rejected)$")
    rejection_reason: Optional[str] = None


class ExpertReviewSubmission(BaseModel):
    decisions: list[ExpertDecisionItem] = []
    removed_ids: list[str] = []
    reviewer: str = ""


@router.get("/sessions/{session_id}/expert-review")
async def get_expert_review_endpoint(session_id: str):
    """Решения эксперта по расхождениям сессии.

    Ключ хранения — стабильный raw `id` расхождения из `unified_findings.json`.
    Группированный вид агрегирует решения по `source_finding_ids` на фронте,
    поэтому регруппировка ничего не теряет.
    """
    return expert_review_mod.get_with_summary(session_id)


@router.post("/sessions/{session_id}/expert-review")
async def post_expert_review_endpoint(session_id: str, req: ExpertReviewSubmission):
    """Применить пачку решений эксперта (apply + removed)."""
    return expert_review_mod.apply_batch(
        session_id,
        decisions=[d.model_dump() for d in req.decisions],
        removed_ids=req.removed_ids,
        reviewer=req.reviewer or "",
    )


@router.post("/sessions/{session_id}/regroup")
async def regroup_endpoint(session_id: str, req: RegroupRequest):
    """Принудительно пересобрать `unified_findings_grouped.json`.

    Не запускает Qwen/Opus. Только пересборка из существующего
    `unified_findings.json` / `unified-diff-flat`.
    """
    try:
        payload = unified_grouping_mod.build_unified_grouped(
            session_id, force=bool(req.force), persist=True,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return {
        "ok": True,
        "session_id": session_id,
        "summary": payload.get("summary", {}),
        "groups_count": len(payload.get("groups") or []),
        "hidden_formal_count": len(payload.get("hidden_formal_groups") or []),
    }


class SetAnalysisModeRequest(BaseModel):
    mode: str = Field(..., pattern="^(block_links|concept_no_block_links)$")


@router.post("/sessions/{session_id}/pairs/{pair_id}/analysis-mode")
async def set_pair_analysis_mode_endpoint(
    session_id: str, pair_id: str, req: SetAnalysisModeRequest,
):
    """Переключить analysis_mode пары.

    `concept_no_block_links` — режим «Блоки без связей»: pipeline сравнивает
    enriched MD целиком, не требуя связей блоков.
    `block_links` — обычный режим с ожиданием связей блоков (default).
    """
    try:
        meta = store.set_pair_analysis_mode(session_id, pair_id, req.mode)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return {
        "ok": True,
        "pair_id": pair_id,
        "analysis_mode": meta.get("analysis_mode"),
        "analysis_mode_updated_at": meta.get("analysis_mode_updated_at"),
    }


@router.get("/sessions/{session_id}/pairs/{pair_id}/analysis-mode")
async def get_pair_analysis_mode_endpoint(session_id: str, pair_id: str):
    """Текущий analysis_mode пары (default `block_links`)."""
    # KeyError мы маскировать не будем — старые пары без поля просто отдают default.
    mode = store.get_pair_analysis_mode(session_id, pair_id)
    return {"pair_id": pair_id, "analysis_mode": mode}


@router.get("/enriched-compare-config")
async def enriched_compare_config_endpoint():
    """Инфо-ручка для UI: включён ли Opus pipeline, какая модель, доступен ли CLI."""
    cfg = enriched_compare_mod.load_config()
    provider, _ = enriched_compare_mod.resolve_provider(cfg)
    info = {
        "enabled": cfg.enabled,
        "provider": cfg.provider,
        "model": cfg.model,
        "max_chars": cfg.max_chars,
        "timeout_sec": cfg.timeout_sec,
        "available": False,
        "reason": None,
    }
    if provider is not None:
        ok, reason = provider.check_availability()
        info["available"] = bool(ok)
        info["reason"] = reason
    elif not cfg.enabled:
        info["reason"] = "disabled_via_env"
    else:
        info["reason"] = f"unknown_provider:{cfg.provider}"
    return info


# ─── Graphic summary ─────────────────────────────────────────────────────


@router.get("/sessions/{session_id}/pairs/{pair_id}/graphic-summary")
async def graphic_summary_endpoint(session_id: str, pair_id: str):
    summary = store.compute_graphic_summary(session_id, pair_id)
    if summary is None:
        raise HTTPException(404, "Сессия/пара не найдены")
    return summary


# ─── Graphic diff (paid LLM, gated) ──────────────────────────────────────


def _png_to_data_url(path: Path) -> str:
    raw = Path(path).read_bytes()
    return "data:image/png;base64," + base64.b64encode(raw).decode("ascii")


class _LLMResultError(RuntimeError):
    """LLM-вызов вернул is_error=True. is_paid_blocked=True для
    paid_api_blocked, иначе обычная ошибка."""

    def __init__(self, reason: str, is_paid_blocked: bool = False):
        super().__init__(reason)
        self.reason = reason
        self.is_paid_blocked = is_paid_blocked


async def _call_graphic_llm(
    *,
    model: str,
    left_png: Path,
    right_png: Path,
    project_id: str,
    job_id: str,
) -> tuple[str, Optional[float], Optional[str]]:
    """Вызов LLM (Gemini / GPT через OpenRouter) с двумя изображениями.

    Возвращает (summary, cost_usd, raw_response).
    Бросает _LLMResultError если result.is_error (включая paid_api_blocked).
    """
    from backend.app.services.llm.llm_runner import run_llm

    messages = [
        {
            "role": "system",
            "content": "Ты — эксперт-аудитор проектной документации. Отвечай кратко, структурированным списком на русском языке.",
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": GRAPHIC_DIFF_PROMPT},
                {"type": "text", "text": "Первое изображение (предыдущая стадия):"},
                {"type": "image_url", "image_url": {"url": _png_to_data_url(left_png)}},
                {"type": "text", "text": "Второе изображение (новая стадия):"},
                {"type": "image_url", "image_url": {"url": _png_to_data_url(right_png)}},
            ],
        },
    ]

    # Используем существующий run_llm: он сам прогоняет paid_api_guard и
    # возвращает LLMResult(is_error=True, error_message="paid_api_blocked:...").
    result = await run_llm(
        stage="stage_comparison_graphic_diff",
        messages=messages,
        response_format=None,                # plain text
        temperature=0.2,
        timeout=300,
        model_override=model,
        project_id=project_id or "stage_comparison",
        job_id=job_id,
        source="stage_comparison.graphic_diff",
    )
    if getattr(result, "is_error", False):
        msg = getattr(result, "error_message", "") or "llm_error"
        raise _LLMResultError(msg, is_paid_blocked=msg.startswith("paid_api_blocked"))
    text = (result.text or "").strip()
    cost = getattr(result, "cost_usd", None)
    return text, cost, result.text


# Кеш model_used_hint между вызовами одной сессии (внутри одного процесса).
# Чтобы не звать /api/v1/models/load каждый раз при single graphic-diff после
# того как первый запрос уже загрузил primary/fallback.
_LOCAL_MODEL_LOAD_CACHE: dict[str, dict] = {}


async def _ensure_local_model_loaded_once(
    cfg, primary_model: str,
) -> tuple[str, bool, dict]:
    """Возвращает (model_used, fallback_used, debug_dict). Кеширует решение
    по ключу (base_url, primary_model) внутри процесса."""
    if not cfg.enable_model_load:
        return primary_model, False, {"messages": ["model_load_disabled_via_env"]}
    key = f"{cfg.base_url}::{primary_model}"
    cached = _LOCAL_MODEL_LOAD_CACHE.get(key)
    if cached and cached.get("ok"):
        return cached["model_used"], cached["fallback_used"], cached
    res = await graphic_local_mod.ensure_lmstudio_model_loaded(
        primary_model, cfg=cfg, allow_fallback=True,
    )
    if res.get("ok"):
        _LOCAL_MODEL_LOAD_CACHE[key] = res
    return res.get("model_used") or primary_model, bool(res.get("fallback_used")), res


async def _graphic_diff_via_local(
    *,
    session_id: str,
    pair_id: str,
    req: "GraphicDiffRequest",
    left_png: Path,
    right_png: Path,
    left_url: str,
    right_url: str,
    cfg,
) -> dict:
    """Платный single graphic-diff через local provider.

    paid_api_guard не вызывается для local provider — это локальная модель,
    не внешний платный API. Согласие пользователя выражается через run_paid=true.
    """
    available, reason = graphic_local_mod.check_local_graphic_llm_available(cfg)
    if not available:
        entry = store.add_graphic_diff_result(
            session_id, pair_id, req.left_block_id, req.right_block_id,
            status="provider_unavailable",
            summary="",
            raw_response=None,
            model=(req.model or cfg.model),
            cost_usd=None,
            error=f"local_graphic_llm_unavailable:{reason}",
            extra={"provider": cfg.provider, "model_used": "", "fallback_used": False},
        )
        return {
            "status": "provider_unavailable",
            "left_block_id": req.left_block_id,
            "right_block_id": req.right_block_id,
            "left_image_url": left_url,
            "right_image_url": right_url,
            "error": f"local_graphic_llm_unavailable:{reason}",
            "provider": cfg.provider,
            "model": (req.model or cfg.model),
            "entry": entry,
        }

    primary_model = (req.model or cfg.model).strip()

    # Snapshot LM Studio до load — нужен, чтобы restore'ить protected модели
    # если они пропадут после нашего load.
    pre_snapshot = await graphic_local_mod.snapshot_loaded_models(cfg)

    model_used, fallback_used, _load_debug = await _ensure_local_model_loaded_once(
        cfg, primary_model,
    )

    result = await graphic_local_mod.compare_images_local(
        left_png, right_png,
        model=primary_model,
        cfg=cfg,
        model_used_hint=model_used,
        fallback_used_hint=fallback_used,
    )

    # Cleanup: если unload_after_request=true — выгружаем primary/fallback
    # (но НЕ protect-list); затем проверяем что protected всё ещё loaded.
    cleanup_info = await graphic_local_mod.cleanup_local_graphic_llm(
        cfg, pre_snapshot, scope="request",
    )
    if cleanup_info.get("unloaded"):
        # Cache становится невалидным — следующий запрос должен снова ensure_load.
        _LOCAL_MODEL_LOAD_CACHE.clear()

    entry = store.add_graphic_diff_result(
        session_id, pair_id, req.left_block_id, req.right_block_id,
        status=result.status,
        summary=result.summary,
        raw_response=result.parsed and json.dumps(result.parsed, ensure_ascii=False),
        model=primary_model,
        cost_usd=None,
        error=result.error,
        extra=result.to_entry_dict(),
    )
    return {
        "status": result.status,
        "left_block_id": req.left_block_id,
        "right_block_id": req.right_block_id,
        "left_image_url": left_url,
        "right_image_url": right_url,
        "summary": result.summary,
        "llm_summary": result.summary,
        "provider": result.provider,
        "model": primary_model,
        "model_used": result.model_used,
        "fallback_used": result.fallback_used,
        "has_significant_difference": result.has_significant_difference,
        "differences": result.differences,
        "confidence": result.confidence,
        "duration_sec": round(result.duration_sec, 3),
        "raw_response_excerpt": result.raw_response_excerpt,
        "error": result.error,
        "cleanup": {
            "unloaded": cleanup_info.get("unloaded") or [],
            "restored": cleanup_info.get("restored") or [],
            "protected_kept": cleanup_info.get("protected_kept") or [],
            "warnings": cleanup_info.get("warnings") or [],
        },
        "entry": entry,
    }


@router.post("/sessions/{session_id}/pairs/{pair_id}/graphic-diff")
async def graphic_diff_endpoint(
    session_id: str,
    pair_id: str,
    req: GraphicDiffRequest,
):
    """Подготовить crop'ы блоков; опционально запустить LLM-сравнение.

    Платный вызов разрешён только при run_paid=true и проходит через
    paid_api_guard. По умолчанию (run_paid=false) возвращает только URL'ы
    crop'ов для подготовки.

    Поведение синхронизировано с batch job (jobs.run_job):
      • paid_api_blocked → 200 OK, status='blocked', stored entry.status='blocked'
      • LLM is_error      → 200 OK, status='error',   stored entry.status='error'
      • success           → status='done'
    """
    # 1. Сгенерировать crop'ы (всегда). Если упадёт — сразу 4xx.
    try:
        left_png = store.render_block_crop(session_id, pair_id, "left", req.left_block_id)
        right_png = store.render_block_crop(session_id, pair_id, "right", req.right_block_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

    base_url = (
        f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/block-image"
    )
    left_url = f"{base_url}?side=left&block_id={req.left_block_id}"
    right_url = f"{base_url}?side=right&block_id={req.right_block_id}"

    # Determine provider: local_openai_compatible или existing (старое
    # OpenRouter/Gemini поведение через run_llm).
    local_cfg = graphic_local_mod.load_local_graphic_llm_config()
    use_local = local_cfg.is_active

    if not req.run_paid:
        return {
            "status": "prepared",
            "left_block_id": req.left_block_id,
            "right_block_id": req.right_block_id,
            "left_image_url": left_url,
            "right_image_url": right_url,
            "prompt": (graphic_local_mod.GRAPHIC_DIFF_LOCAL_PROMPT if use_local else GRAPHIC_DIFF_PROMPT),
            "provider": (local_cfg.provider if use_local else "existing"),
            "note": "Crop'ы подготовлены. Для запуска сравнения вызовите этот endpoint с run_paid=true.",
        }

    # 2. Платный путь
    if use_local:
        return await _graphic_diff_via_local(
            session_id=session_id,
            pair_id=pair_id,
            req=req,
            left_png=left_png,
            right_png=right_png,
            left_url=left_url,
            right_url=right_url,
            cfg=local_cfg,
        )

    # 2b. existing провайдер (OpenRouter/Gemini) — через paid_api_guard
    model = req.model or "google/gemini-3.1-pro-preview"

    try:
        summary, cost, raw_text = await _call_graphic_llm(
            model=model,
            left_png=left_png,
            right_png=right_png,
            project_id=f"stage_comparison/{session_id}",
            job_id=f"{pair_id}:{req.left_block_id}->{req.right_block_id}",
        )
    except _LLMResultError as exc:
        # Унифицированная обработка: paid_api_blocked → 'blocked', иначе 'error'.
        # Возвращаем 200 OK, а status указываем в теле — так клиенту проще
        # отрисовать одинаковый UI, как для batch job.
        entry_status = "blocked" if exc.is_paid_blocked else "error"
        entry = store.add_graphic_diff_result(
            session_id, pair_id, req.left_block_id, req.right_block_id,
            status=entry_status,
            summary="",
            raw_response=None,
            model=model,
            cost_usd=None,
            error=exc.reason,
        )
        return {
            "status": entry_status,
            "left_block_id": req.left_block_id,
            "right_block_id": req.right_block_id,
            "left_image_url": left_url,
            "right_image_url": right_url,
            "error": exc.reason,
            "is_paid_blocked": exc.is_paid_blocked,
            "model": model,
            "entry": entry,
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception("graphic-diff LLM call failed unexpectedly")
        entry = store.add_graphic_diff_result(
            session_id, pair_id, req.left_block_id, req.right_block_id,
            status="error",
            summary="",
            raw_response=None,
            model=model,
            cost_usd=None,
            error=str(exc)[:500],
        )
        return {
            "status": "error",
            "left_block_id": req.left_block_id,
            "right_block_id": req.right_block_id,
            "left_image_url": left_url,
            "right_image_url": right_url,
            "error": str(exc)[:500],
            "is_paid_blocked": False,
            "model": model,
            "entry": entry,
        }

    entry = store.add_graphic_diff_result(
        session_id, pair_id, req.left_block_id, req.right_block_id,
        status="done",
        summary=summary,
        raw_response=raw_text,
        model=model,
        cost_usd=cost,
        error=None,
    )
    return {
        "status": "done",
        "left_block_id": req.left_block_id,
        "right_block_id": req.right_block_id,
        "left_image_url": left_url,
        "right_image_url": right_url,
        "summary": summary,
        "model": model,
        "cost_usd": cost,
        "entry": entry,
    }


# ─── Findings ────────────────────────────────────────────────────────────


@router.post("/sessions/{session_id}/findings/rebuild")
async def rebuild_findings_endpoint(session_id: str):
    try:
        return findings_mod.rebuild_findings(session_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/sessions/{session_id}/findings")
async def list_findings_endpoint(
    session_id: str,
    pair_id: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    include_children: bool = Query(False, description="Включать ли child findings (по умолчанию скрыты)"),
):
    filters = {
        "pair_id": pair_id, "type": type, "category": category,
        "status": status, "severity": severity, "q": q,
        "include_children": include_children,
    }
    try:
        return findings_mod.list_findings(session_id, filters=filters)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/sessions/{session_id}/findings/{finding_id}/children")
async def list_children_endpoint(session_id: str, finding_id: str):
    """Все child findings конкретного parent_id (для разворачивания grouped view)."""
    return {"items": findings_mod.list_child_findings(session_id, finding_id)}


@router.patch("/sessions/{session_id}/findings/{finding_id}")
async def patch_finding_endpoint(session_id: str, finding_id: str, req: PatchFindingRequest):
    patch = {k: v for k, v in req.model_dump().items() if v is not None}
    try:
        return findings_mod.patch_finding(session_id, finding_id, patch)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.patch("/sessions/{session_id}/findings")
async def bulk_patch_findings_endpoint(session_id: str, req: BulkPatchFindingsRequest):
    """Массово обновить несколько findings: status/severity/user_note/append_user_note."""
    return findings_mod.bulk_patch_findings(
        session_id, req.ids, dict(req.patch or {}), include_deleted=req.include_deleted,
    )


@router.delete("/sessions/{session_id}/findings/{finding_id}")
async def delete_finding_endpoint(session_id: str, finding_id: str):
    """Soft-delete: помечает status='ignored', deleted=true. Физически не удаляет."""
    ok = findings_mod.soft_delete_finding(session_id, finding_id)
    if not ok:
        raise HTTPException(404, "finding_not_found")
    return {"ok": True, "deleted": True}


# ─── Graphic LLM config endpoint ─────────────────────────────────────────


@router.get("/graphic-llm-config")
async def graphic_llm_config_endpoint():
    """Безопасная инфо-ручка для UI / диагностики: какой provider активен,
    какие модели сконфигурированы, доступен ли local provider.

    Не возвращает пароль или полные credentials. Для external провайдеров
    (existing/openrouter/gemini) — совместимый минимальный ответ.
    """
    cfg = graphic_local_mod.load_local_graphic_llm_config()
    if cfg.is_active:
        info = graphic_local_mod.config_info_for_endpoint(cfg)
        # Add live LM Studio diagnostics (loaded_models / ctx). Без падений:
        # если endpoint LM Studio недоступен — поля заполняются дефолтами.
        try:
            diag = await graphic_local_mod.loaded_models_diagnostics(cfg)
        except Exception:  # noqa: BLE001
            diag = {
                "endpoint_available": False,
                "loaded_models": [],
                "desired_context_length": cfg.load_context_length,
                "primary_loaded_ctx": None,
                "primary_context_ok": False,
            }
        info.update(diag)
        return info
    # External-провайдер (existing/openrouter/gemini) — нечего проверять локально
    return {
        "provider": cfg.provider,
        "base_url_present": False,
        "model": "",
        "fallback_model": "",
        "auth": "",
        "auth_configured": True,
        "model_load_enabled": False,
        "available": True,                # internal/external — каждый сам по себе
        "reason": None,
    }


# ─── Batch graphic LLM jobs ──────────────────────────────────────────────


@router.post("/sessions/{session_id}/graphic-diff-jobs")
async def create_graphic_diff_job_endpoint(session_id: str, req: CreateGraphicDiffJobRequest):
    """Создать пакетный job сравнения графики через LLM.

    run_paid + confirm_paid должны быть true одновременно. Если нет — job
    создаётся в статусе rejected_no_confirm и LLM не запускается.
    """
    try:
        items = [it.model_dump() for it in (req.items or [])]
        job = jobs_mod.create_graphic_llm_job(
            session_id,
            scope=req.scope, pair_id=req.pair_id, items=items,
            run_paid=req.run_paid, confirm_paid=req.confirm_paid, model=req.model,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc

    # Если status == "queued" — запускаем в фоне (Задача 1).
    # HTTP-запрос возвращается сразу, прогресс читается через GET /graphic-diff-jobs/{id}.
    if job.get("status") == "queued":
        try:
            start_status = jobs_mod.start_job_in_background(session_id, job["id"])
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        return {
            "ok": True,
            "job_id": job["id"],
            "status": "queued" if start_status == "started" else job.get("status"),
            "start_status": start_status,
            "scope": job.get("scope"),
            "model": job.get("model"),
            "progress": job.get("progress"),
        }
    # rejected_no_confirm / другой статус → возвращаем как есть
    return {
        "ok": False,
        "job_id": job["id"],
        "status": job.get("status"),
        "warnings": job.get("warnings"),
        "progress": job.get("progress"),
    }


@router.get("/sessions/{session_id}/graphic-diff-jobs")
async def list_graphic_diff_jobs(session_id: str):
    return {"jobs": jobs_mod.list_jobs(session_id)}


@router.get("/sessions/{session_id}/graphic-diff-jobs/{job_id}")
async def get_graphic_diff_job(session_id: str, job_id: str):
    job = jobs_mod.get_job(session_id, job_id)
    if job is None:
        raise HTTPException(404, "job_not_found")
    return job


@router.post("/sessions/{session_id}/graphic-diff-jobs/{job_id}/cancel")
async def cancel_graphic_diff_job(session_id: str, job_id: str):
    job = jobs_mod.cancel_job(session_id, job_id)
    if job is None:
        raise HTTPException(404, "job_not_found")
    return job


# ─── Warnings (Задача 9) ─────────────────────────────────────────────────


@router.get("/sessions/{session_id}/warnings")
async def list_warnings_endpoint(session_id: str):
    """Сводка предупреждений по качеству исходных данных сессии."""
    try:
        return warnings_mod.compute_warnings(session_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


# ─── Reports ─────────────────────────────────────────────────────────────


@router.post("/sessions/{session_id}/reports")
async def create_report_endpoint(session_id: str, req: CreateReportRequest):
    try:
        return reports_mod.create_report(
            session_id, req.format,
            filters=req.filters,
            include_rejected=req.include_rejected,
            include_ignored=req.include_ignored,
            include_images=req.include_images,
            include_llm_summary=req.include_llm_summary,
            include_user_notes=req.include_user_notes,
            include_child_findings=req.include_child_findings,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/sessions/{session_id}/reports")
async def list_reports_endpoint(session_id: str):
    try:
        return {"reports": reports_mod.list_reports(session_id)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, str(exc)) from exc


@router.get("/sessions/{session_id}/reports/{report_id}/download")
async def download_report_endpoint(session_id: str, report_id: str):
    try:
        p = reports_mod.resolve_report_file(session_id, report_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, str(exc)) from exc
    media_by_ext = {
        ".md": "text/markdown; charset=utf-8",
        ".html": "text/html; charset=utf-8",
        ".json": "application/json; charset=utf-8",
        ".pdf": "application/pdf",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }
    media = media_by_ext.get(p.suffix.lower(), "application/octet-stream")
    return FileResponse(str(p), media_type=media, filename=p.name)
