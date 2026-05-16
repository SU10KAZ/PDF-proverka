"""Paid API guard — единая точка проверки прав на платный внешний API.

Принцип:
  По умолчанию платные внешние API (OpenRouter/GPT/Gemini) запрещены.
  Право на платный вызов появляется ТОЛЬКО когда пользователь явно нажал
  Start/Retry в UI с галкой "Разрешить платные API для этого запуска".
  Для этого запуска создаётся manual_run_id, который привязывается к scope
  (один project_id или batch project_id-ов) и пробрасывается во все этапы.

Любой автоматический путь (auto-resume, retry, prefetch, фоновая очередь,
orphan job, discussion auto-summary) НЕ имеет manual_run_id и должен быть
заблокирован guard'ом ДО network request.

Fail-closed: если что-то пошло не так с проверкой — блокируем, не пускаем.
"""
from __future__ import annotations

import logging
import re
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from backend.app.core.config import (
    PAID_API_ENABLED,
    PAID_API_REQUIRE_MANUAL_START,
    PAID_API_DAILY_LIMIT_USD,
)
from backend.app.services.llm import paid_api_events

logger = logging.getLogger(__name__)


class PaidApiBlockedError(RuntimeError):
    """Исключение для случаев, когда нужно поднять, а не вернуть LLMResult."""

    def __init__(self, reason: str, ctx: "PaidApiContext | None" = None):
        super().__init__(reason)
        self.reason = reason
        self.ctx = ctx


@dataclass
class PaidApiContext:
    """Контекст одного потенциально платного вызова.

    Передаётся в assert_paid_api_allowed/check_paid_api_allowed.
    Все поля опциональны на уровне dataclass, но guard требует минимум:
      source, model, project_id, stage.
    Без полного project_id (например, чистый "M31A") guard блокирует.
    """

    source: str = ""                 # llm_runner | manager.stage02 | discussion.* | webapp.*
    model: str = ""
    project_id: str = ""
    version_id: str = ""
    stage: str = ""
    manual_run_id: str = ""
    job_id: str = ""
    user_initiated: bool = False     # выставляется только endpoint'ом старта с галкой
    estimated_cost_usd: float = 0.0  # для лимита по сумме


# ─── manual_run_registry ──────────────────────────────────────────────
# IN-MEMORY ONLY набор активных manual_run_id. Не персистится никуда — после
# рестарта backend регистр пуст. Любой resumed job становится orphan и его
# платные этапы блокируются. Это намеренная политика fail-closed для restart.
#
# Каждый manual_run_id привязан к scope:
#   - либо к одному project_id (одиночный аудит),
#   - либо к списку project_id (batch).
#
# Жизненный цикл:
#   issue_manual_run() — endpoint при ручном Start с галкой
#   _bump_used_count() — каждый успешный assert_paid_api_allowed
#   release_manual_run() — после завершения/отмены job/batch
#   рестарт backend — registry сбрасывается в пустой dict


_registry_lock = threading.RLock()
_registry: dict[str, dict] = {}   # manual_run_id → {scope_projects, source_job_id, batch_id, created_at, used_count}


_SHORT_DISCIPLINE_CODES = {
    # Короткие коды дисциплин, которые НЕ являются валидными project_id.
    # Сюда часто проскакивают, если кто-то передал section вместо полного path.
    "AI", "AR", "DOC", "EOM", "GP", "ITP", "KJ", "KM", "M31A", "OV",
    "POS", "PT", "SS", "TX", "VK",
}


def _is_short_discipline_code(project_id: str) -> bool:
    """True если project_id — это просто короткий код дисциплины без полного пути.

    Полный project_id всегда содержит подпуть (например, "AR/13АВ-РД-АР3-К6_в2.pdf"
    или "13АВ-РД-АР3-К6_в2.pdf"). Короткий код типа "M31A" — признак ошибки.
    """
    if not project_id:
        return False
    pid = project_id.strip()
    # Если содержит "/" или "." — это уже путь, не короткий код.
    if "/" in pid or "\\" in pid:
        return False
    # Чистые буквы/цифры без длины ≤6 — подозрительный короткий код.
    return pid.upper() in _SHORT_DISCIPLINE_CODES or (
        len(pid) <= 6 and re.fullmatch(r"[A-Za-z0-9]+", pid) is not None
    )


def issue_manual_run(
    *,
    project_ids: list[str] | str,
    batch_id: str = "",
    source_job_id: str = "",
) -> str:
    """Создать manual_run_id, привязанный к scope.

    Вызывается endpoint'ом /api/audit/.../start* когда пользователь нажал
    "Разрешить платные API" в UI. Возвращает строку UUID.

    Запись существует ТОЛЬКО в памяти текущего backend-процесса. После
    рестарта backend все manual_run'ы исчезают, resumed jobs становятся
    orphan и блокируются guard'ом.
    """
    if isinstance(project_ids, str):
        scope = [project_ids] if project_ids else []
    else:
        scope = [p for p in (project_ids or []) if p]
    if not scope:
        raise ValueError("issue_manual_run: empty project_ids scope")

    mrid = uuid.uuid4().hex
    record = {
        "manual_run_id": mrid,
        "scope_projects": scope,
        "batch_id": batch_id or "",
        "source_job_id": source_job_id or "",
        "created_at": datetime.now().isoformat(),
        "used_count": 0,
    }
    with _registry_lock:
        _registry[mrid] = record
    return mrid


def release_manual_run(manual_run_id: str) -> None:
    """Удалить manual_run_id из registry (после завершения/отмены job/batch)."""
    if not manual_run_id:
        return
    with _registry_lock:
        _registry.pop(manual_run_id, None)


def get_manual_run(manual_run_id: str) -> Optional[dict]:
    """Получить запись registry по manual_run_id (или None если нет)."""
    if not manual_run_id:
        return None
    with _registry_lock:
        rec = _registry.get(manual_run_id)
        return dict(rec) if rec else None


def list_active_manual_runs() -> list[dict]:
    """Снапшот активных manual_run'ов (для отладки/UI)."""
    with _registry_lock:
        return [dict(v) for v in _registry.values()]


def _bump_used_count(manual_run_id: str) -> None:
    """Инкрементировать used_count в registry (для observability)."""
    if not manual_run_id:
        return
    with _registry_lock:
        rec = _registry.get(manual_run_id)
        if rec is not None:
            rec["used_count"] = int(rec.get("used_count", 0)) + 1
            rec["last_used_at"] = datetime.now().isoformat()


def _scope_allows_project(rec: dict, project_id: str) -> bool:
    """True если manual_run scope содержит этот project_id."""
    scope = rec.get("scope_projects") or []
    if not isinstance(scope, list):
        return False
    if project_id in scope:
        return True
    # Также допускаем, если project_id — пустой и в scope ровно один элемент
    # (одиночный manual run).
    return False


# ─── Daily limit helpers ──────────────────────────────────────────────


def _today_spent_usd() -> float:
    """Сколько уже потрачено сегодня (читаем из paid_cost.daily_breakdown)."""
    try:
        from backend.app.services.common.usage_service import paid_cost_tracker
        daily = paid_cost_tracker.get_daily(days=1)
        days = daily.get("days") or []
        today = datetime.now().date().isoformat()
        for d in days:
            if d.get("date") == today:
                return float(d.get("total", 0.0) or 0.0)
    except Exception:  # noqa: BLE001
        # Fail-closed для бухгалтерии означало бы лишний блок — здесь, наоборот,
        # лучше вернуть 0 (не знаем, сколько потрачено) и положиться на остальные
        # проверки. Daily limit — это вспомогательный потолок, не основной guard.
        return 0.0
    return 0.0


# ─── Главная функция ──────────────────────────────────────────────────


def _kill_switch_disabled() -> bool:
    """True если глобально платные API отключены."""
    return not PAID_API_ENABLED


def _block(ctx: PaidApiContext, reason: str) -> None:
    """Записать blocked-event и поднять исключение."""
    paid_api_events.record_blocked_event(
        reason=reason,
        model=ctx.model,
        project_id=ctx.project_id,
        version_id=ctx.version_id,
        stage=ctx.stage,
        source=ctx.source,
        manual_run_id=ctx.manual_run_id,
        job_id=ctx.job_id,
    )
    logger.warning(
        "paid_api_blocked: reason=%s source=%s project=%s stage=%s model=%s job=%s",
        reason, ctx.source, ctx.project_id, ctx.stage, ctx.model, ctx.job_id,
    )
    raise PaidApiBlockedError(reason, ctx)


def assert_paid_api_allowed(ctx: PaidApiContext) -> None:
    """Поднимает PaidApiBlockedError если платный вызов запрещён.

    Должна вызываться СТРОГО ДО network request. Любое последующее
    отправление в OpenRouter/GPT/Gemini в обход этой функции считается багом.
    """
    # 1. Глобальный kill-switch
    if _kill_switch_disabled():
        _block(ctx, "paid_api_disabled")

    # 2. Sanity: source/model/stage обязательны
    if not ctx.source:
        _block(ctx, "missing_source")
    if not ctx.model:
        _block(ctx, "missing_model")
    if not ctx.stage:
        _block(ctx, "missing_stage")

    # 3. project_id — обязателен и не должен быть коротким кодом дисциплины
    pid = (ctx.project_id or "").strip()
    if not pid:
        _block(ctx, "missing_project_id")
    if _is_short_discipline_code(pid):
        _block(ctx, "short_discipline_code_project_id")

    # 4. Требование manual_run_id
    if PAID_API_REQUIRE_MANUAL_START:
        mrid = (ctx.manual_run_id or "").strip()
        if not mrid:
            _block(ctx, "missing_manual_run_id")
        rec = get_manual_run(mrid)
        if rec is None:
            _block(ctx, "unknown_manual_run_id")
        if not _scope_allows_project(rec, pid):
            _block(ctx, "manual_run_scope_mismatch")

    # 5. Daily limit
    limit = float(PAID_API_DAILY_LIMIT_USD or 0.0)
    if limit > 0.0:
        spent = _today_spent_usd()
        projected = spent + max(0.0, float(ctx.estimated_cost_usd or 0.0))
        if spent >= limit or projected > limit:
            _block(ctx, "daily_limit_exceeded")

    # Успех — бампаем used_count и пропускаем.
    if ctx.manual_run_id:
        _bump_used_count(ctx.manual_run_id)


def is_paid_api_enabled() -> bool:
    """True если kill-switch разрешает платные API в принципе."""
    return PAID_API_ENABLED


def status_snapshot() -> dict:
    """Снапшот для GET /api/usage/paid-api/status."""
    spent = _today_spent_usd()
    limit = float(PAID_API_DAILY_LIMIT_USD or 0.0)
    remaining = (max(0.0, limit - spent) if limit > 0 else None)
    paid_tail = paid_api_events.read_paid_events_tail(limit=1)
    blocked_tail = paid_api_events.read_blocked_events_tail(limit=1)
    with _registry_lock:
        active_runs = len(_registry)
    return {
        "paid_api_enabled": PAID_API_ENABLED,
        "require_manual_start": PAID_API_REQUIRE_MANUAL_START,
        "daily_limit_usd": round(limit, 4),
        "today_spent_usd": round(spent, 4),
        "today_remaining_usd": (round(remaining, 4) if remaining is not None else None),
        "blocked_events_count_today": paid_api_events.count_blocked_today(),
        "last_paid_event": (paid_tail[-1] if paid_tail else None),
        "last_blocked_event": (blocked_tail[-1] if blocked_tail else None),
        "active_manual_runs": active_runs,
    }
