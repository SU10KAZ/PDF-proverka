"""Регистрация агента в центре и загрузка сохранённого состояния.

instance_id — НОВЫЙ на каждый запуск процесса: он отличает «тот же VPS, но
перезапущен» от «тот же процесс». worker_id и токен, наоборот, стабильны и
живут в файлах (§7.3 техпроекта).
"""
from __future__ import annotations

import platform
import time
import uuid
from typing import Any, Optional

from audit_worker import PROTOCOL_VERSION, __version__
from audit_worker.client import CenterClient, CenterError
from audit_worker.config import WorkerConfig
from audit_worker.local_store import WorkerStateStore


class RegistrationRequired(RuntimeError):
    """Нужен bootstrap-секрет: воркер ещё не зарегистрирован."""


#: Состояния связи с центром, отличимые друг от друга. Смешивать их нельзя:
#: обычный сетевой обрыв — повод ждать, а протухший TLS, отозванный токен и
#: несовместимый протокол ожиданием не лечатся и требуют человека.
CENTER_OK = "online"
CENTER_UNREACHABLE = "center_unreachable"     # DNS, connect, таймаут, 5xx
CENTER_TLS_ERROR = "center_tls_error"         # сертификат, handshake
CENTER_AUTH_ERROR = "center_auth_error"       # 401/403: токен отозван/не одобрен
CENTER_PROTOCOL_ERROR = "center_protocol_error"  # 426 и прочие несовместимости


def classify_center_failure(exc: BaseException) -> str:
    """Отнести отказ центра к диагностическому состоянию.

    Ошибки TLS, авторизации и протокола НЕ маскируются под «сеть моргнула»:
    бесконечный backoff по ним означал бы воркер, который молча не работает
    неделю, и никто не знает почему.
    """
    import ssl

    import httpx

    if isinstance(exc, CenterError):
        status = getattr(exc, "status", 0) or 0
        if status in (401, 403):
            return CENTER_AUTH_ERROR
        if status in (426, 400, 422):
            return CENTER_PROTOCOL_ERROR
        return CENTER_UNREACHABLE
    if isinstance(exc, ssl.SSLError):
        return CENTER_TLS_ERROR
    if isinstance(exc, httpx.ConnectError):
        # httpx заворачивает ошибку TLS в ConnectError; отличаем по причине.
        cause = exc.__cause__ or exc.__context__
        if isinstance(cause, ssl.SSLError) or "SSL" in str(exc).upper():
            return CENTER_TLS_ERROR
        return CENTER_UNREACHABLE
    if isinstance(exc, httpx.HTTPError):
        return CENTER_UNREACHABLE
    if isinstance(exc, OSError):
        return CENTER_UNREACHABLE
    raise exc


def new_instance_id() -> str:
    return f"inst_{uuid.uuid4().hex[:16]}"


def load_identity(config: WorkerConfig) -> dict[str, Any]:
    """Прочитать сохранённую личность и выдать новый instance_id на этот запуск."""
    store = WorkerStateStore(config.state_path, config.token_path)
    state = store.load()
    state["instance_id"] = new_instance_id()
    state.setdefault("previous_instance_id", state.get("last_instance_id"))
    state["last_instance_id"] = state["instance_id"]
    state["started_at"] = time.time()
    state["worker_version"] = __version__
    store.save(state)
    return state


def ensure_registered(
    config: WorkerConfig,
    *,
    bootstrap_secret: Optional[str] = None,
) -> dict[str, Any]:
    """Зарегистрироваться, если ещё не сделано. Возвращает {worker_id, token, ...}."""
    store = WorkerStateStore(config.state_path, config.token_path)
    state = load_identity(config)
    token = store.read_token()

    if state.get("worker_id") and token:
        # Личность и токен уже на диске. Обновление регистрации — вежливость, а
        # не условие работы: центр может быть недоступен, а идущие на VPS
        # процессы аудита от этого не зависят (I-01/I-02). Раньше
        # `httpx.ConnectError` уходил наружу, процесс падал с traceback, и под
        # systemd `Restart=always` это давало крэш-луп до возвращения центра —
        # при живых процессах аудита и растущем журнале событий, которые никто
        # не досылал (§32.10 отчёта 05).
        center_state = CENTER_OK
        last_error = None
        with CenterClient(
            config.dispatcher_url,
            token=token,
            worker_id=state["worker_id"],
            instance_id=state["instance_id"],
            timeout=config.request_timeout_sec,
            verify=config.verify_tls,
            transport=config.transport,
        ) as client:
            try:
                client.update_registration(
                    {
                        "instance_id": state["instance_id"],
                        "worker_version": __version__,
                        "protocol_version": PROTOCOL_VERSION,
                        "pipeline_revision": config.pipeline_revision,
                        "capabilities": config.capabilities(),
                    }
                )
            except Exception as exc:  # noqa: BLE001 — классификатор пробросит чужое
                center_state = classify_center_failure(exc)
                last_error = (
                    f"HTTP {exc.status}: {exc.detail}"
                    if isinstance(exc, CenterError)
                    else f"{type(exc).__name__}: {exc}"
                )
        state["center_state"] = center_state
        if last_error is not None:
            state["last_error"] = last_error
        store.save(state)
        return {**state, "token": token}

    # Заявка подана, токена ещё нет — пробуем забрать его claim-секретом.
    # Это возможно только после одобрения оператором.
    if state.get("worker_id") and store.read_claim_secret():
        claimed = try_claim(config, state)
        if claimed:
            return claimed
        if not bootstrap_secret:
            raise RegistrationRequired(
                "Регистрация подана, но ещё не одобрена оператором. "
                "Одобрите воркер на экране «Аудит-воркеры» и запустите снова."
            )

    if not bootstrap_secret:
        raise RegistrationRequired(
            "Воркер не зарегистрирован. Запустите с --bootstrap-secret "
            "(значение DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET центра)."
        )

    with CenterClient(
        config.dispatcher_url,
        instance_id=state["instance_id"],
        timeout=config.request_timeout_sec,
        verify=config.verify_tls,
        transport=config.transport,
    ) as client:
        response = client.register(
            {
                "instance_id": state["instance_id"],
                "display_name_hint": config.display_name or platform.node(),
                "worker_version": __version__,
                "protocol_version": PROTOCOL_VERSION,
                "pipeline_revision": None,
                "capabilities": config.capabilities(),
                "configured_max_slots_hint": config.max_slots,
            },
            bootstrap_secret,
        )

    state["worker_id"] = response["worker_id"]
    state["registration_status"] = response.get("registration_status")
    state["heartbeat_interval_sec"] = response.get("heartbeat_interval_sec", 30)
    state["poll_timeout_sec"] = response.get("poll_timeout_sec", 25)
    state["chunk_size_bytes"] = response.get("chunk_size_bytes")
    store.save(state)

    # Токен на этом шаге НЕ выдаётся: центр вернул одноразовый claim-secret,
    # который сработает только после одобрения оператором.
    claim_secret = response.get("claim_secret")
    if claim_secret:
        store.write_claim_secret(claim_secret)   # права 0600

    claimed = try_claim(config, state)
    if claimed:
        return claimed
    return {**state, "token": store.read_token()}


def try_claim(config: WorkerConfig, state: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Попробовать обменять claim-secret на токен. None, если ещё не одобрено.

    Не считается ошибкой: до одобрения оператором центр отвечает 409, и это
    штатное состояние ожидания, а не сбой.
    """
    store = WorkerStateStore(config.state_path, config.token_path)
    claim_secret = store.read_claim_secret()
    if not claim_secret or not state.get("worker_id"):
        return None

    with CenterClient(
        config.dispatcher_url,
        instance_id=state["instance_id"],
        timeout=config.request_timeout_sec,
        verify=config.verify_tls,
        transport=config.transport,
    ) as client:
        try:
            response = client.claim(
                {
                    "worker_id": state["worker_id"],
                    "instance_id": state["instance_id"],
                    "claim_secret": claim_secret,
                }
            )
        except CenterError as exc:
            if exc.status == 409:
                state["claim_status"] = str(exc.detail)
                store.save(state)
                return None
            raise

    store.write_token(response["worker_token"])
    store.drop_claim_secret()        # одноразовый: больше не нужен
    state["registration_status"] = response.get("registration_status")
    state["claim_status"] = "claimed"
    store.save(state)
    return {**state, "token": response["worker_token"]}
