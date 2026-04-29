"""
REST API для базы знаний — экспертные решения, паттерны, импорт/экспорт.
"""
import os
import tempfile
from typing import Optional

from fastapi import APIRouter, HTTPException, UploadFile, File, Query

from webapp.models.expert_review import (
    ExpertReviewSubmission, CustomerConfirmRequest, PatternActionRequest,
)
from webapp.services import knowledge_base_service as kb_svc
from webapp.services import missing_norms_service as mn_svc

router = APIRouter(prefix="/api/knowledge-base", tags=["knowledge-base"])


@router.post("/expert-review/{project_id:path}")
async def submit_expert_review(project_id: str, body: ExpertReviewSubmission):
    """Сохранить решения эксперта по проекту."""
    try:
        result = kb_svc.save_expert_review(project_id, body.decisions, body.reviewer)
        return {"status": "ok", **result}
    except Exception as e:
        raise HTTPException(500, f"Ошибка сохранения: {e}")


@router.get("/expert-review/{project_id:path}")
async def get_expert_review(project_id: str):
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
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Получить записи базы знаний с фильтрацией."""
    return kb_svc.get_knowledge_base(
        status=status, section=section, item_type=item_type,
        search=search, limit=limit, offset=offset,
    )


@router.get("/stats")
async def get_kb_stats():
    """Счётчики по вкладкам (rejected, accepted, customer_confirmed)."""
    return kb_svc.get_kb_stats()


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
async def upload_decisions_excel(file: UploadFile = File(...)):
    """Загрузить Excel с решениями эксперта."""
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Ожидается файл .xlsx")

    suffix = os.path.splitext(file.filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        results = kb_svc.import_decisions_from_excel(tmp_path)
        return {"status": "ok", "projects": results}
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
    from webapp.config import PROJECTS_DIR
    from pathlib import Path
    n = mn_svc.backfill_from_all_projects(Path(PROJECTS_DIR))
    return {"status": "ok", "new_entries": n}
