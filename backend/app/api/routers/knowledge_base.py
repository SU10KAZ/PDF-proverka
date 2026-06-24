"""
REST API для базы знаний — экспертные решения, паттерны, импорт/экспорт.
"""
import logging
import os
import tempfile
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File, Query

from backend.app.core import portal_auth
from backend.app.models.expert_review import (
    ExpertReviewSubmission, CustomerConfirmRequest, PatternActionRequest,
)
from backend.app.services.common import version_service
import backend.app.services.common.user_service as user_service
from backend.app.services.common.project_service import ProjectNotResolvedError
import backend.app.services.knowledge_base.knowledge_base_service as kb_svc
import backend.app.services.knowledge_base.missing_norms_service as mn_svc

router = APIRouter(prefix="/api/knowledge-base", tags=["knowledge-base"])

_log = logging.getLogger(__name__)


def _resolve_reviewer(request: Request, body_reviewer: str = "") -> str:
    """Серверная атрибуция автора экспертной оценки.

    Когда портальная авторизация ВКЛЮЧЕНА — автор берётся СТРОГО из активной
    сессии (логин → сотрудник из users.json), а не из тела запроса. Это
    закрывает два класса багов:

    * клиент мог прислать reviewer глобального дефолта («Узун А. И.»), если его
      сессия не разрезолвилась в сотрудника (откат на current_id) — теперь такой
      случай даёт ЧЕСТНОЕ пустое имя, а не приписывается Узуну;
    * Excel-импорт раньше вообще не знал автора (reviewer="") — теперь знает.

    Когда auth ВЫКЛЮЧЕН (локальная разработка) — доверяем телу запроса как
    раньше (никакой сессии нет).
    """
    settings = portal_auth.get_settings()
    if not settings.enabled:
        return body_reviewer or ""
    username = portal_auth.request_username(request, settings)
    user = user_service.get_user_by_login(username) if username else None
    if user:
        return user.get("name") or ""
    # auth включён, но логин не сопоставлен с сотрудником — НЕ откатываемся на
    # глобального current_id (Узуна). Лучше пустой автор, чем чужой.
    _log.warning(
        "[attribution] auth enabled but session login %r not mapped to a user; "
        "saving review with empty reviewer instead of global default",
        username,
    )
    return ""


async def use_version(version_id: Optional[str] = None):
    """Подвязать version_id к ContextVar на время запроса.

    Должна быть `async`, чтобы set/reset ContextVar шли в одном Context'е
    (см. комментарий в discussions.use_version).
    """
    token = version_service.bind_version(version_id)
    try:
        yield version_id
    finally:
        version_service.unbind_version(token)


@router.post("/expert-review/{project_id:path}")
async def submit_expert_review(
    project_id: str,
    body: ExpertReviewSubmission,
    request: Request,
    _vid: Optional[str] = Depends(use_version),
):
    """Сохранить решения эксперта по проекту."""
    try:
        reviewer = _resolve_reviewer(request, body.reviewer)
        result = kb_svc.save_expert_review(project_id, body.decisions, reviewer, removed_ids=body.removed_ids)
        return {"status": "ok", **result}
    except ProjectNotResolvedError as e:
        # project_id не резолвится в реальную папку → НЕ создаём orphan _output.
        raise HTTPException(404, f"Project directory not resolved: {e}")
    except Exception as e:
        raise HTTPException(500, f"Ошибка сохранения: {e}")


@router.get("/expert-review/{project_id:path}")
async def get_expert_review(
    project_id: str,
    _vid: Optional[str] = Depends(use_version),
):
    """Загрузить сохранённые решения эксперта для проекта."""
    data = kb_svc.load_expert_review(project_id)
    if data is None:
        return {"project_id": project_id, "has_review": False, "data": None}
    return {"project_id": project_id, "has_review": True, "data": data}


@router.get("/entries")
async def get_kb_entries(
    status: Optional[str] = Query(None, description="rejected | accepted | customer_confirmed"),
    section: Optional[str] = Query(None),
    item_type: Optional[str] = Query(None, description="finding | optimization"),
    search: Optional[str] = Query(None),
    object_id: Optional[str] = Query(None, description="id объекта (здание/комплекс)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Получить записи базы знаний с фильтрацией."""
    return kb_svc.get_knowledge_base(
        status=status, section=section, item_type=item_type,
        search=search, object_id=object_id, limit=limit, offset=offset,
    )


@router.get("/stats")
async def get_kb_stats(object_id: Optional[str] = Query(None)):
    """Счётчики по вкладкам (rejected, accepted, customer_confirmed, fixed_by_customer)."""
    return kb_svc.get_kb_stats(object_id=object_id)


@router.post("/customer-confirm")
async def confirm_by_customer(body: CustomerConfirmRequest):
    """Отметить записи как согласованные заказчиком."""
    count = kb_svc.mark_customer_confirmed(body.entry_ids, body.note)
    return {"status": "ok", "confirmed": count}


@router.post("/customer-unconfirm")
async def unconfirm_by_customer(body: CustomerConfirmRequest):
    """Снять отметку согласования заказчиком."""
    count = kb_svc.unmark_customer_confirmed(body.entry_ids)
    return {"status": "ok", "unconfirmed": count}


@router.post("/revoke")
async def revoke_decision(body: dict):
    """Отменить решение — удалить из базы знаний и expert_review проекта."""
    entry_id = body.get("entry_id", "")
    project_id = body.get("project_id", "")
    item_id = body.get("item_id", "")
    try:
        count = kb_svc.revoke_decision(entry_id, project_id, item_id)
        return {"status": "ok", "revoked": count}
    except Exception as e:
        raise HTTPException(500, f"Ошибка отмены: {e}")


@router.get("/patterns")
async def get_patterns():
    """Получить все обнаруженные паттерны."""
    patterns = kb_svc.get_patterns()
    return {"patterns": patterns}


@router.post("/patterns/detect")
async def detect_patterns(min_frequency: int = Query(3, ge=2)):
    """Запустить детекцию паттернов из отклонённых решений."""
    patterns = kb_svc.detect_patterns(min_frequency=min_frequency)
    return {"patterns": patterns, "total": len(patterns)}


@router.post("/patterns/{pattern_id}/approve")
async def approve_pattern(pattern_id: str):
    """Одобрить паттерн."""
    ok = kb_svc.update_pattern_status(pattern_id, "applied")
    if not ok:
        raise HTTPException(404, f"Паттерн {pattern_id} не найден")
    return {"status": "ok"}


@router.post("/patterns/{pattern_id}/dismiss")
async def dismiss_pattern(pattern_id: str):
    """Отклонить паттерн."""
    ok = kb_svc.update_pattern_status(pattern_id, "dismissed")
    if not ok:
        raise HTTPException(404, f"Паттерн {pattern_id} не найден")
    return {"status": "ok"}


@router.post("/patterns/{pattern_id}/edit")
async def edit_pattern(pattern_id: str, body: PatternActionRequest):
    """Отредактировать и применить паттерн."""
    ok = kb_svc.update_pattern_status(pattern_id, "edited", edited_fix=body.edited_fix)
    if not ok:
        raise HTTPException(404, f"Паттерн {pattern_id} не найден")
    return {"status": "ok"}


@router.post("/upload-excel")
async def upload_decisions_excel(
    request: Request,
    file: UploadFile = File(...),
    project_id: Optional[str] = None,
    _vid: Optional[str] = Depends(use_version),
):
    """Загрузить Excel с решениями эксперта.

    `version_id` (query) применяется ко всем найденным в Excel проектам.
    `project_id` (query) используется только как fallback: когда в Excel
    скрытая ячейка / имя листа не позволяют определить настоящий project_id
    (например, старые экспорты для V2 писали в скрытую ячейку basename
    папки = "v2"). UI всегда знает текущий project_id и шлёт его явно.
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Ожидается файл .xlsx")

    suffix = os.path.splitext(file.filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        reviewer = _resolve_reviewer(request)
        results = kb_svc.import_decisions_from_excel(
            tmp_path, default_project_id=project_id, reviewer=reviewer
        )
        return {"status": "ok", "projects": results, "reviewer": reviewer}
    except Exception as e:
        raise HTTPException(500, f"Ошибка импорта: {e}")
    finally:
        os.unlink(tmp_path)


# ─── Нормы для добавления в vault ────────────────────────────────────────────

@router.get("/missing-norms")
async def get_missing_norms(status: Optional[str] = Query(None)):
    """Список норм, не найденных в vault во время проверок."""
    norms = mn_svc.get_missing_norms(status=status)
    stats = mn_svc.get_stats()
    return {"norms": norms, "stats": stats}


@router.post("/missing-norms/{doc_number:path}/mark-added")
async def mark_norm_added(doc_number: str):
    """Отметить норму как добавленную в vault."""
    ok = mn_svc.mark_added(doc_number)
    if not ok:
        raise HTTPException(404, f"Норма '{doc_number}' не найдена")
    return {"status": "ok", "doc_number": doc_number, "new_status": "added"}


@router.post("/missing-norms/{doc_number:path}/dismiss")
async def dismiss_norm(doc_number: str):
    """Снять норму из списка (не требуется)."""
    ok = mn_svc.mark_dismissed(doc_number)
    if not ok:
        raise HTTPException(404, f"Норма '{doc_number}' не найдена")
    return {"status": "ok", "doc_number": doc_number, "new_status": "dismissed"}


@router.post("/missing-norms/{doc_number:path}/restore")
async def restore_norm(doc_number: str):
    """Вернуть норму в список ожидающих."""
    ok = mn_svc.mark_pending(doc_number)
    if not ok:
        raise HTTPException(404, f"Норма '{doc_number}' не найдена")
    return {"status": "ok", "doc_number": doc_number, "new_status": "pending"}


@router.post("/missing-norms/backfill")
async def backfill_missing_norms():
    """Пройти по всем проектам и собрать existing missing_norms_queue.json."""
    from backend.app.core.config import PROJECTS_DIR
    from pathlib import Path
    n = mn_svc.backfill_from_all_projects(Path(PROJECTS_DIR))
    return {"status": "ok", "new_entries": n}
