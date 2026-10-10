"""Этап «Сведение дублей» — последний этап прогона сравнения пары.

Идёт сразу за V3 в том же прогоне: под тем же замком пары и с тем же токеном
отмены («Остановить анализ» останавливает и его).  По умолчанию включён;
``PROJECTCHANGE_CONSOLIDATION_STAGE=0`` выключает.  Модель — Claude Opus 5.5
через Claude Code CLI (``PROJECTCHANGE_CONSOLIDATOR_PROVIDER`` переопределяет),
без запасной модели; вызовы идут только через открытый шлюз V3.

Результат V3 этап не меняет: Консолидатор пишет свой замороженный прогон в
``production/projectchange_consolidator_shadow/``, а ход этапа — в
``production/projectchange_consolidation_stage.json`` (его видит портал через
``production/state`` → ``consolidation``).  Любой сбой этапа остаётся сбоем
этапа: прогон V3 при этом считается завершённым.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

FLAG = "PROJECTCHANGE_CONSOLIDATION_STAGE"
PROVIDER_ENV = "PROJECTCHANGE_CONSOLIDATOR_PROVIDER"
MAX_CALLS_ENV = "PROJECTCHANGE_CONSOLIDATOR_MAX_CALLS"
DEFAULT_PROVIDER = "claude_code_cli:claude-opus-5-5:xhigh"
DEFAULT_MAX_CALLS = 40
STATUS_FILE = "projectchange_consolidation_stage.json"
SCHEMA = "projectchange-consolidation-stage/1"

_LIVE: set[tuple[str, str]] = set()
_LIVE_LOCK = threading.Lock()


def enabled() -> bool:
    return os.environ.get(FLAG, "1").strip() != "0"


def max_calls() -> int:
    try:
        return max(0, int(os.environ.get(MAX_CALLS_ENV, str(DEFAULT_MAX_CALLS))))
    except ValueError:
        return DEFAULT_MAX_CALLS


def provider_from_spec(spec: str) -> Any:
    parts = [p.strip() for p in (spec or "").split(":")]
    if len(parts) != 3 or parts[0] != "claude_code_cli" or not parts[1] or not parts[2]:
        raise ValueError(f"unsupported provider configuration {spec!r}")
    from backend.app.services.project_change_v3.provider import ClaudeOpusProvider

    return ClaudeOpusProvider(model=parts[1], reasoning=parts[2])


def source_completed(state: Any) -> bool:
    return (isinstance(state, dict) and state.get("reason_code") == "v3_completed"
            and state.get("status") in ("COMPLETED", "REVIEW") and bool(state.get("run_id")))


def is_live(session_id: str, pair_id: str) -> bool:
    with _LIVE_LOCK:
        return (session_id, pair_id) in _LIVE


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _path(session_id: str, pair_id: str) -> Path:
    from backend.app.services.stage_comparison import paths

    return paths.production_dir(session_id, pair_id) / STATUS_FILE


def read_status(session_id: str, pair_id: str) -> dict[str, Any] | None:
    try:
        value = json.loads(_path(session_id, pair_id).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) and value.get("schema") == SCHEMA else None


def public_status(session_id: str, pair_id: str, run_id: str | None) -> dict[str, Any] | None:
    """Ход этапа для ``production/state``; RUNNING без живого потока — прерван."""
    value = read_status(session_id, pair_id)
    if not value or not run_id or value.get("source_run_id") != run_id:
        return None
    live = is_live(session_id, pair_id)
    out = {**value, "live": live}
    if value.get("status") == "RUNNING" and not live:
        out.update(status="INTERRUPTED", message="Сведение дублей прервано перезапуском портала.")
    return out


def _write(session_id: str, pair_id: str, value: dict[str, Any]) -> dict[str, Any]:
    from backend.app.services.common.atomic_json import atomic_write_json

    body = {"schema": SCHEMA, **value, "updated_at": _now()}
    try:
        atomic_write_json(_path(session_id, pair_id), body)
    except Exception:  # noqa: BLE001 — статус этапа не должен ронять прогон
        log.exception("consolidation stage status not saved for %s", pair_id)
    return body


def run(session_id: str, pair_id: str, v3_state: Any, *, cancel_token: Any = None,
        provider: Any = None) -> dict[str, Any] | None:
    """Свести дубли завершённого прогона V3; никогда не бросает и не меняет ``v3_state``.

    ``None`` — этап не запускался (выключен, V3 не завершён, тестовый провайдер V3).
    """
    if not enabled() or not source_completed(v3_state):
        return None
    if provider is None:
        from backend.app.services.project_change_v3 import provider as v3_provider

        if v3_provider._test_provider is not None:
            return None
    key = (session_id, pair_id)
    with _LIVE_LOCK:
        if key in _LIVE:
            return None
        _LIVE.add(key)
    run_id = str(v3_state["run_id"])
    base = {"source_run_id": run_id, "started_at": _now(), "finished_at": None, "consolidator_run_id": None,
            "model": None, "calls_total": None, "calls_done": 0, "stats": None, "reason_code": "", "message": ""}

    def cancelled() -> bool:
        return bool(cancel_token is not None and getattr(cancel_token, "cancelled", False))

    def finish(status: str, reason_code: str = "", message: str = "", **extra: Any) -> dict[str, Any]:
        base.update(extra, reason_code=reason_code, message=message, finished_at=_now())
        return _write(session_id, pair_id, {**base, "status": status})

    try:
        _write(session_id, pair_id, {**base, "status": "RUNNING", "message": "Подготовка сведения дублей"})
        if provider is None:
            try:
                provider = provider_from_spec(os.environ.get(PROVIDER_ENV, "").strip() or DEFAULT_PROVIDER)
            except ValueError as exc:
                return finish("SKIPPED", "bad_provider_config", str(exc))
            from backend.app.services.project_change_v3 import provider_gate

            gate = provider_gate.check_provider_readiness()
            if not gate.get("available"):
                return finish("SKIPPED", "provider_unavailable",
                              f"Модель недоступна ({gate.get('reason')}), сведение дублей не выполнялось.")
        base["model"] = getattr(provider, "model", None)
        if cancel_token is not None and hasattr(provider, "cancel_token"):
            provider.cancel_token = cancel_token  # отмена прогона убивает CLI-сессию вызова
        from backend.app.services.project_change_v3 import run_storage
        from backend.app.services.stage_comparison import paths

        from .engine import prepare
        from .shadow import run_shadow
        from .source_view import load_completed_run
        from .storage import SHADOW_DIR_NAME, ShadowStore

        bundle = load_completed_run(session_id, pair_id, run_id)
        prepared = prepare(bundle)
        total, allowed = len(prepared.ready_calls), max_calls()
        if total > allowed:
            return finish("SKIPPED", "call_plan_exceeded",
                          f"Нужно {total} вызовов модели, разрешено {allowed} ({MAX_CALLS_ENV}).", calls_total=total)
        base["calls_total"] = total
        _write(session_id, pair_id, {**base, "status": "RUNNING", "message": "Модель сводит дубли изменений"})
        production = paths.production_dir(session_id, pair_id)
        store_root = production / SHADOW_DIR_NAME
        # Замок источника остаётся от прерванного этапа: живого здесь нет (проверено через _LIVE).
        ShadowStore(store_root).release(run_id)

        def on_event(kind: str, data: dict[str, Any]) -> None:
            if kind == "call":
                base["calls_done"] += 1
                _write(session_id, pair_id, {**base, "status": "RUNNING", "message": "Модель сводит дубли изменений"})

        manifest = run_shadow(bundle, provider, store_root, max_calls=allowed,
                              watch_roots=[run_storage.run_dir(session_id, pair_id, run_id),
                                           production / "current_run.json"],
                              prepared=prepared,
                              should_stop=cancelled, on_event=on_event)
        done = {"consolidator_run_id": manifest.get("consolidator_run_id"), "stats": manifest.get("stats"),
                "calls_done": manifest.get("calls_made", base["calls_done"])}
        if cancelled():
            return finish("CANCELLED", "cancelled", "Сведение дублей остановлено.", **done)
        if manifest.get("state") != "COMPLETED":
            return finish("FAILED", str(manifest.get("reason_code") or "validation_failed"),
                          "Результат сведения дублей не прошёл проверку.", **done)
        return finish("COMPLETED", **done)
    except Exception as exc:  # noqa: BLE001 — сбой этапа не роняет прогон V3
        log.exception("consolidation stage failed for %s/%s", pair_id, run_id)
        return finish("FAILED", type(exc).__name__, f"{type(exc).__name__}: {exc}"[:500])
    finally:
        with _LIVE_LOCK:
            _LIVE.discard(key)
