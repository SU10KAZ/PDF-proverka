"""Paid API guard — единая точка проверки прав на платный внешний API.

Принцип (после упрощения 2026-05-18):
  Платные внешние API (OpenRouter/GPT/Gemini) разрешены ВСЕМУ, что идёт через
  pipeline, если включён глобальный kill-switch PAID_API_ENABLED. Pipeline
  имеет право вызывать платные модели автоматически — без ручной галки.

Раньше требовался manual_run_id, выдаваемый endpoint'ом при ручном Start с
галкой "Разрешить платные API". Эта политика была убрана — она ломала
auto-resume после рестарта backend (orphan jobs, missing_manual_run_id) и
заставляла оператора жать галку на каждый запуск.

Что осталось:
  1. Глобальный kill-switch PAID_API_ENABLED — если false, всё блокируется.
  2. Sanity-поля контекста (source/model/stage/project_id).
  3. Daily limit USD — аварийный потолок, не требует ручного подтверждения.
  4. Forensic-журналы (paid_api_blocked_events.jsonl).
"""
from __future__ import annotations

import itertools
import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from backend.app.core.config import (
    PAID_API_ENABLED as _CFG_PAID_API_ENABLED,
    PAID_API_DAILY_LIMIT_USD as _CFG_PAID_API_DAILY_LIMIT_USD,
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
    Guard требует минимум: source, model, project_id, stage.
    Без полного project_id (например, чистый "M31A") guard блокирует —
    если только не передан canonical_project_id или object_id.
    """

    source: str = ""                 # llm_runner | manager.stage02 | discussion.* | webapp.*
    model: str = ""
    project_id: str = ""
    version_id: str = ""
    stage: str = ""
    job_id: str = ""
    estimated_cost_usd: float = 0.0  # для лимита по сумме
    # Защита от короткого project_id вроде "M31A": если есть canonical
    # (полный путь относительно projects/) или object_id, guard разрешает
    # короткий display project_id.
    object_id: str = ""
    canonical_project_id: str = ""
    meta: dict[str, Any] = field(default_factory=dict)


_SHORT_DISCIPLINE_CODES = {
    "AI", "AR", "DOC", "EOM", "GP", "ITP", "KJ", "KM", "M31A", "OV",
    "POS", "PT", "SS", "TX", "VK",
}


def _is_short_discipline_code(project_id: str) -> bool:
    """True если project_id — это просто короткий код дисциплины без полного пути."""
    if not project_id:
        return False
    pid = project_id.strip()
    if "/" in pid or "\\" in pid:
        return False
    return pid.upper() in _SHORT_DISCIPLINE_CODES or (
        len(pid) <= 6 and re.fullmatch(r"[A-Za-z0-9]+", pid) is not None
    )


# ─── Runtime flag readers ─────────────────────────────────────────────
# Главное правило: НИКОГДА не используем импортированные имена PAID_API_ENABLED
# как глобальные булевы — они зафиксированы на момент импорта config.py и не
# меняются при правке .env без рестарта backend. Читаем env заново при каждом
# assert: это даёт kill-switch без рестарта и детерминированное поведение в
# тестах (monkeypatch на os.environ).
def _env_bool_runtime(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float_runtime(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _paid_api_enabled_runtime() -> bool:
    """Возвращает PAID_API_ENABLED, перечитанный из env."""
    return _env_bool_runtime("PAID_API_ENABLED", default=_CFG_PAID_API_ENABLED)


def _daily_limit_runtime() -> float:
    return _env_float_runtime("PAID_API_DAILY_LIMIT_USD", default=_CFG_PAID_API_DAILY_LIMIT_USD)


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
        return 0.0
    return 0.0


# ─── In-process reservation ledger (#73) ──────────────────────────────
# Несколько одновременных платных вызовов читали spent ДО того, как любой из
# них успевал записать cost через record_paid → каждый видел один и тот же
# spent, все проходили проверку лимита и вместе перебирали дневной потолок.
# Решение: под локом резервируем оценку стоимости; проверка лимита учитывает
# spent + сумму активных резерваций. TTL-самозалечивание гарантирует, что
# пропущенный release (error-путь) не «съест» бюджет навсегда.
# TTL — лишь БЭКСТОП на случай пропущенного release (на нормальных и error-путях
# release вызывается явно). Значение заведомо больше самого долгого одиночного
# вызова с ретраями (timeout × max_retries + backoff), чтобы НЕ вычистить
# резервацию ещё выполняющегося долгого запроса и не занизить бюджет
# (pre-deploy review). Переопределяется env PAID_API_RESERVATION_TTL_SEC.
_RESERVATION_TTL_SEC_DEFAULT = 3600.0
_reservation_lock = threading.Lock()
_reservations: dict[int, tuple[float, float]] = {}  # id -> (amount_usd, monotonic_ts)
_reservation_ids = itertools.count(1)


def _reservation_ttl_runtime() -> float:
    return _env_float_runtime("PAID_API_RESERVATION_TTL_SEC", default=_RESERVATION_TTL_SEC_DEFAULT)


def _purge_expired_locked(now: float) -> None:
    """Удалить протухшие резервации (вызывать под _reservation_lock)."""
    ttl = _reservation_ttl_runtime()
    expired = [rid for rid, (_, ts) in _reservations.items() if now - ts >= ttl]
    for rid in expired:
        _reservations.pop(rid, None)


def _reserved_total_locked(now: float) -> float:
    """Сумма активных (не протухших) резерваций (под _reservation_lock)."""
    _purge_expired_locked(now)
    return sum(amount for amount, _ in _reservations.values())


@dataclass
class PaidApiReservation:
    """Handle активной резервации бюджета. Вызывающий обязан release()."""

    reservation_id: int
    amount_usd: float
    released: bool = False

    def release(self) -> None:
        if self.released:
            return
        with _reservation_lock:
            _reservations.pop(self.reservation_id, None)
        self.released = True


def release_reservation(res: "PaidApiReservation | None") -> None:
    """None-safe, идемпотентное освобождение резервации."""
    if res is not None:
        res.release()


def active_reservation_count() -> int:
    """Число активных резерваций (для тестов/диагностики)."""
    with _reservation_lock:
        return len(_reservations)


# ─── Главная функция ──────────────────────────────────────────────────


def _block(ctx: PaidApiContext, reason: str) -> None:
    """Записать blocked-event и поднять исключение."""
    paid_api_events.record_blocked_event(
        reason=reason,
        model=ctx.model,
        project_id=ctx.project_id,
        version_id=ctx.version_id,
        stage=ctx.stage,
        source=ctx.source,
        job_id=ctx.job_id,
        extra={
            "object_id": ctx.object_id or "",
            "canonical_project_id": ctx.canonical_project_id or "",
            "parent_pid": os.getppid(),
        },
    )
    logger.warning(
        "paid_api_blocked: reason=%s source=%s project=%s stage=%s model=%s job=%s",
        reason, ctx.source, ctx.project_id, ctx.stage, ctx.model, ctx.job_id,
    )
    raise PaidApiBlockedError(reason, ctx)


def _has_canonical_scope(ctx: PaidApiContext) -> bool:
    """True если короткий project_id допустим благодаря дополнительному scope."""
    canon = (ctx.canonical_project_id or "").strip()
    if canon and ("/" in canon or "\\" in canon):
        return True
    if (ctx.object_id or "").strip():
        return True
    return False


def _assert_basic(ctx: PaidApiContext) -> None:
    """Проверки 1-3 (kill-switch + sanity-поля + project_id). Общие для
    assert_paid_api_allowed и reserve_paid_api."""
    # 1. Глобальный kill-switch (runtime-читаемый)
    if not _paid_api_enabled_runtime():
        _block(ctx, "paid_api_disabled")

    # 2. Sanity: source/model/stage обязательны
    if not ctx.source:
        _block(ctx, "missing_source")
    if not ctx.model:
        _block(ctx, "missing_model")
    if not ctx.stage:
        _block(ctx, "missing_stage")

    # 3. project_id — обязателен и не должен быть коротким кодом дисциплины
    # без canonical/object_id scope.
    pid = (ctx.project_id or "").strip()
    if not pid:
        _block(ctx, "missing_project_id")
    if _is_short_discipline_code(pid) and not _has_canonical_scope(ctx):
        _block(ctx, "short_discipline_code_project_id")


def _enforce_daily_limit(ctx: PaidApiContext, *, reserve: bool) -> "PaidApiReservation | None":
    """Проверка дневного лимита (4) с учётом активных резерваций.

    reserve=True → при успехе зарегистрировать резервацию оценки и вернуть
    handle (вызывающий обязан release()). Блокирует (raise) при превышении.
    """
    limit = _daily_limit_runtime()
    if limit <= 0.0:
        return None  # лимит выключен — резервирование не нужно
    est = max(0.0, float(ctx.estimated_cost_usd or 0.0))
    res: "PaidApiReservation | None" = None
    blocked = False
    # Лок держим коротко: snapshot spent+reserved+est и (при reserve) регистрация.
    with _reservation_lock:
        now = time.monotonic()
        reserved = _reserved_total_locked(now)
        spent = _today_spent_usd()
        if spent + reserved >= limit or (spent + reserved + est) > limit:
            blocked = True
        elif reserve:
            rid = next(_reservation_ids)
            _reservations[rid] = (est, now)
            res = PaidApiReservation(rid, est)
    if blocked:
        _block(ctx, "daily_limit_exceeded")  # I/O вне лока
    return res


def assert_paid_api_allowed(ctx: PaidApiContext) -> None:
    """Поднимает PaidApiBlockedError если платный вызов запрещён.

    Должна вызываться СТРОГО ДО network request. Проверка лимита учитывает
    активные резервации (#73), но сама НЕ резервирует — для пре-флайт проверок.
    """
    _assert_basic(ctx)
    _enforce_daily_limit(ctx, reserve=False)


def reserve_paid_api(ctx: PaidApiContext) -> "PaidApiReservation | None":
    """Как assert_paid_api_allowed, но дополнительно РЕЗЕРВИРУЕТ оценку
    стоимости под локом — конкурентные вызовы видят сумму резерваций и не
    перебирают дневной лимит. Возвращает handle (или None если лимит выключен);
    вызывающий обязан вызвать release_reservation(handle) после record_paid."""
    _assert_basic(ctx)
    return _enforce_daily_limit(ctx, reserve=True)


def is_paid_api_enabled() -> bool:
    """True если kill-switch разрешает платные API в принципе. Runtime."""
    return _paid_api_enabled_runtime()


def status_snapshot() -> dict:
    """Снапшот для GET /api/usage/paid-api/status."""
    enabled = _paid_api_enabled_runtime()
    spent = _today_spent_usd()
    limit = _daily_limit_runtime()
    remaining = (max(0.0, limit - spent) if limit > 0 else None)
    paid_tail = paid_api_events.read_paid_events_tail(limit=1)
    blocked_tail = paid_api_events.read_blocked_events_tail(limit=1)
    return {
        "paid_api_enabled": enabled,
        "daily_limit_usd": round(limit, 4),
        "today_spent_usd": round(spent, 4),
        "today_remaining_usd": (round(remaining, 4) if remaining is not None else None),
        "blocked_events_count_today": paid_api_events.count_blocked_today(),
        "last_paid_event": (paid_tail[-1] if paid_tail else None),
        "last_blocked_event": (blocked_tail[-1] if blocked_tail else None),
    }
