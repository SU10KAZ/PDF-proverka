"""Webapp paid_api_guard — тонкий wrapper над backend guard'ом.

Webapp и backend пишут в один и тот же paid_cost.json / events.jsonl. Поэтому
guard у них общий — webapp импортирует функции из backend.app.services.llm.paid_api_guard.

Если по каким-то причинам backend модуль недоступен (legacy окружение без
PYTHONPATH=root), wrapper падает в fail-closed: любой платный вызов блокируется.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


try:
    from backend.app.services.llm.paid_api_guard import (  # type: ignore
        PaidApiBlockedError,
        PaidApiContext,
        assert_paid_api_allowed,
        is_paid_api_enabled,
        status_snapshot,
    )
    from backend.app.services.llm import paid_api_events  # type: ignore
    _GUARD_AVAILABLE = True
except ImportError as e:  # pragma: no cover — fail-closed shim
    logger.warning("backend paid_api_guard недоступен (%s) — webapp в fail-closed", e)
    _GUARD_AVAILABLE = False

    class PaidApiBlockedError(RuntimeError):  # type: ignore[no-redef]
        def __init__(self, reason: str, ctx=None):
            super().__init__(reason)
            self.reason = reason
            self.ctx = ctx

    class PaidApiContext:  # type: ignore[no-redef]
        def __init__(self, **kw):
            self.__dict__.update(kw)

    def assert_paid_api_allowed(ctx) -> None:  # type: ignore[no-redef]
        raise PaidApiBlockedError("guard_unavailable_failclosed", ctx)

    def is_paid_api_enabled() -> bool:  # type: ignore[no-redef]
        return False

    def status_snapshot():  # type: ignore[no-redef]
        return {
            "paid_api_enabled": False,
            "daily_limit_usd": 0.0,
            "today_spent_usd": 0.0,
            "today_remaining_usd": None,
            "blocked_events_count_today": 0,
            "last_paid_event": None,
            "last_blocked_event": None,
            "_note": "backend paid_api_guard unavailable — fail-closed",
        }

    class _NoopEvents:
        @staticmethod
        def record_paid_event(**kwargs):
            return None

        @staticmethod
        def record_blocked_event(**kwargs):
            return None

        @staticmethod
        def read_paid_events_tail(limit=100):
            return []

        @staticmethod
        def read_blocked_events_tail(limit=100):
            return []

        @staticmethod
        def count_blocked_today():
            return 0

    paid_api_events = _NoopEvents()  # type: ignore


__all__ = [
    "PaidApiBlockedError",
    "PaidApiContext",
    "assert_paid_api_allowed",
    "is_paid_api_enabled",
    "status_snapshot",
    "paid_api_events",
]
