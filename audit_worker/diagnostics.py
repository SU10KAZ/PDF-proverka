"""Safe, offline Worker diagnostic snapshot used by the 12E cutover gate."""
from __future__ import annotations

import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

from audit_worker.local_store import LocalJobStore, WorkerStateStore, read_json


def _certificate_expiry(config: Any, worker_id: str | None) -> float | None:
    if (
        config.control_transport != "grpc"
        or config.grpc_security_mode != "mtls"
        or not worker_id
        or not config.grpc_client_certificate_path
        or not config.grpc_ca_bundle_path
        or not config.grpc_key_store_dir
    ):
        return None
    try:
        from audit_worker.key_store import platform_key_store
        from audit_worker.mtls_identity import load_identity

        identity = load_identity(
            key_store=platform_key_store(
                config.grpc_key_store_dir, config.grpc_key_store_backend
            ),
            certificate_path=config.grpc_client_certificate_path,
            trust_bundle_path=config.grpc_ca_bundle_path,
            worker_id=worker_id,
        )
        return float(identity.not_after)
    except Exception:  # noqa: BLE001 - doctor must be usable during cert failure
        return None


def _readonly_db_snapshot(path: Any) -> dict[str, Any]:
    """Read operational queue facts without creating, migrating or locking a DB.

    ``LocalDB`` is deliberately a read/write application object: construction
    creates a parent directory and runs forward migrations.  That is correct
    for Agent and Executor, but categorically wrong for the offline ``doctor``
    command used in an incident.  SQLite ``mode=ro`` makes this boundary
    enforceable even when the configured worker root does not yet exist.
    """
    db_path = path if hasattr(path, "is_file") else None
    empty = {
        "status": "absent",
        "claimed_attempts": 0,
        "live_processes": 0,
        "pending_commands": 0,
        "executor": {
            "status": "offline",
            "running_processes": 0,
            "ambiguous_processes": 0,
        },
    }
    if db_path is None or not db_path.is_file():
        return empty

    connection: sqlite3.Connection | None = None
    try:
        # Keep '/' literal in the URI.  No PRAGMA is issued: several common
        # PRAGMAs can mutate connection or journal state despite a read-only
        # operational intent.
        uri = f"file:{quote(str(db_path), safe='/')}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        claimed = int(connection.execute(
            "SELECT COUNT(*) FROM execution_queue WHERE state IN ('claimed', 'running')"
        ).fetchone()[0])
        pending = int(connection.execute(
            "SELECT COUNT(*) FROM local_commands WHERE status IN ('pending', 'processing')"
        ).fetchone()[0])
        processes = connection.execute(
            "SELECT pid, process_start_identity, status FROM process_registry"
        ).fetchall()
        latest = connection.execute(
            "SELECT * FROM executor_instances ORDER BY last_heartbeat_at DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        # A damaged or not-yet-migrated database must not turn a diagnostic
        # command into a repair attempt.  The caller gets a clear, safe state.
        return {**empty, "status": "unreadable"}
    finally:
        if connection is not None:
            connection.close()

    from audit_worker import process_registry as procinfo

    live = sum(
        1
        for row in processes
        if row["status"] == "running"
        and procinfo.is_alive(int(row["pid"] or 0), row["process_start_identity"])
    )
    running = sum(1 for row in processes if row["status"] == "running")
    ambiguous = sum(1 for row in processes if row["status"] == "ambiguous")
    executor: dict[str, Any] = {
        "status": "offline",
        "running_processes": running,
        "ambiguous_processes": ambiguous,
    }
    if latest is not None:
        alive = procinfo.is_alive(
            int(latest["process_pid"]), latest["process_start_identity"]
        )
        age = time.time() - float(latest["last_heartbeat_at"])
        status = "online" if alive and age <= 90 else "stale" if alive else "offline"
        executor.update(
            {
                "executor_instance_id": latest["executor_instance_id"],
                "status": status,
                "last_heartbeat_at": latest["last_heartbeat_at"],
                "version": latest["version"],
            }
        )
    return {
        "status": "read_only",
        "claimed_attempts": claimed,
        "live_processes": live,
        "pending_commands": pending,
        "executor": executor,
    }


def collect_worker_diagnostics(config: Any) -> dict[str, Any]:
    """Return one read-only JSON-safe view without secrets or network calls."""
    state_store = WorkerStateStore(config.state_path, config.token_path)
    state = state_store.load()
    runtime = dict(state.get("runtime_diagnostics") or {})
    jobs = LocalJobStore(config.jobs_dir)
    db = _readonly_db_snapshot(config.local_db_path)
    active = jobs.active()
    pending_events = 0
    last_event_ack = 0
    pending_results = 0
    for meta in jobs.iter_all():
        events_dir = jobs.job_dir(meta["job_id"], meta["attempt_id"]) / "events"
        # Чистое чтение вместо конструирования EventOutbox: его конструктор
        # чинит и СОХРАНЯЕТ курсор, а `doctor` зовут во время инцидента — рядом
        # с живым исполнителем, который в этот же журнал пишет. Диагностика не
        # вправе становиться вторым писателем.
        position = read_outbox_position(events_dir)
        if position is not None:
            pending_events += max(
                0, position["last_written_seq"] - position["last_acked_seq"]
            )
            last_event_ack = max(last_event_ack, position["last_acked_seq"])
        if meta.get("result_hash") and meta.get("retention_until") is None:
            pending_results += 1
    try:
        free = shutil.disk_usage(config.root).free
    except OSError:
        free = None
    transport = "grpc_stream" if config.control_transport == "grpc" else "polling"
    return {
        "generated_at": time.time(),
        "transport_mode": transport,
        "grpc_connection_state": runtime.get(
            "grpc_connection_state", "not_applicable" if transport == "polling" else "unknown"
        ),
        "gateway_status": runtime.get("gateway_status", "unknown"),
        "last_connected_at": runtime.get("last_connected_at"),
        "last_disconnect_at": runtime.get("last_disconnect_at"),
        "last_disconnect_reason": runtime.get("last_disconnect_reason"),
        "connection_epoch": int(state.get("connection_epoch") or 0),
        "mtls_cert_expiry": _certificate_expiry(config, state.get("worker_id")),
        "last_heartbeat": runtime.get("last_heartbeat_at"),
        "last_heartbeat_error_reason": runtime.get("last_heartbeat_error_reason"),
        "active_attempts": [
            {
                "job_id": meta.get("job_id"),
                "attempt_id": meta.get("attempt_id"),
                "local_state": meta.get("local_state"),
            }
            for meta in active
        ],
        "slots": {
            "max_slots": int(config.max_slots),
            "claimed_or_running": db["claimed_attempts"],
            "live_executor_processes": db["live_processes"],
        },
        "local_db_status": db["status"],
        "outbox_pending_count": pending_events,
        "last_event_ack": last_event_ack,
        "pending_cancel_count": db["pending_commands"],
        "pending_result_count": pending_results,
        "data_plane_status": "configured_https"
        if str(config.data_plane_base_url or config.dispatcher_url).startswith("https://")
        else "local_test_or_invalid",
        "worker_accepting_jobs": runtime.get("worker_accepting_jobs"),
        "executor": db["executor"],
        "disk_free_bytes": free,
        "token_present": bool(state_store.read_token()),
    }


#: Как часто агент публикует эксплуатационную сводку в центр. Реже heartbeat:
#: это диагностика для экрана, а не признак живости.
RUNTIME_TELEMETRY_INTERVAL_SEC = 60.0


def read_outbox_position(events_dir: Path) -> dict[str, int] | None:
    """Позиции журнала событий ЧИСТЫМ чтением двух файлов.

    Намеренно НЕ через `EventOutbox`: его конструктор — операция писателя. Он
    создаёт каталог, чинит курсор по сегментам и при расхождении СОХРАНЯЕТ
    исправленные `cursor.json`/`ack.json`. Для журнала, который в этот момент
    наполняет процесс исполнителя, диагностический опрос раз в минуту означал
    бы гонку двух писателей за один файл — ради строки на экране.

    Возвращает None, если журнала ещё нет: «нет данных» лучше выдуманного нуля.
    """
    if not events_dir.is_dir():
        return None
    cursor = read_json(events_dir / "cursor.json", None)
    if not isinstance(cursor, dict):
        return None
    ack = read_json(events_dir / "ack.json", None)
    ack = ack if isinstance(ack, dict) else {}
    try:
        written = int(cursor.get("last_written_seq", 0))
        acked = int(ack.get("last_acked_seq", cursor.get("last_acked_seq", 0)))
    except (TypeError, ValueError):
        return None
    acked = max(0, min(acked, written))
    return {"last_written_seq": max(0, written), "last_acked_seq": acked}


def _outbox_rollup(jobs: LocalJobStore) -> dict[str, Any]:
    """Свести журналы событий воркера в одну честную картину.

    EventOutbox живёт ПОПЫТКАМИ: номер события осмыслен только внутри своей
    попытки. Поэтому числа складываются по-разному и это не небрежность:

    * `last_written_seq` / `last_acked_seq` — у ПОСЛЕДНЕЙ попытки. Сумма
      номеров разных последовательностей не значит ничего;
    * `pending` — сумма по ВСЕМ попыткам. Вот это как раз операционное число:
      «нигде ничего не застряло»;
    * `attempts` — по скольким журналам считали, чтобы первые два числа
      нельзя было прочитать как «всего за всё время».
    """
    latest: dict[str, int] | None = None
    latest_key = float("-inf")
    pending = 0
    attempts = 0
    last_ack_at: float | None = None
    for meta in jobs.iter_all():
        events_dir = jobs.job_dir(meta["job_id"], meta["attempt_id"]) / "events"
        position = read_outbox_position(events_dir)
        if position is None:
            continue
        attempts += 1
        pending += max(0, position["last_written_seq"] - position["last_acked_seq"])
        key = float(meta.get("started_at") or meta.get("created_at") or 0.0)
        if key >= latest_key:
            latest_key, latest = key, position
        try:
            stamp = (events_dir / "ack.json").stat().st_mtime
        except OSError:
            continue
        last_ack_at = stamp if last_ack_at is None else max(last_ack_at, stamp)
    if latest is None:
        return {"attempts": 0, "status": "unavailable"}
    return {
        "attempts": attempts,
        "last_written_seq": latest["last_written_seq"],
        "last_acked_seq": latest["last_acked_seq"],
        "pending": pending,
        "status": "synced" if pending == 0 else "pending",
        **({"last_ack_at": last_ack_at} if last_ack_at is not None else {}),
    }


def collect_runtime_telemetry(config: Any, jobs: LocalJobStore | None = None) -> dict[str, Any]:
    """Безопасная эксплуатационная сводка воркера для экрана диагностики.

    Что здесь ЕСТЬ: счётчики журнала событий, адрес шлюза и режим транспорта.
    Чего здесь нет и быть не может: токенов, приватных ключей, содержимого
    сертификатов, заголовков авторизации, путей к файлам с секретами. Состав
    задан перечислением полей, а не фильтром «убрать лишнее», — иначе новое
    поле утекало бы по умолчанию.

    Адрес шлюза — диагностический факт КОНФИГУРАЦИИ воркера. Центр его только
    показывает: управлять транспортом со стороны экрана нельзя, и значение
    сюда никогда не возвращается.
    """
    store = jobs if jobs is not None else LocalJobStore(config.jobs_dir)
    transport = "grpc_stream" if config.control_transport == "grpc" else "polling"
    telemetry: dict[str, Any] = {
        "at": time.time(),
        "transport": transport,
        "event_outbox": _outbox_rollup(store),
    }
    target = str(getattr(config, "grpc_target", "") or "") if transport == "grpc_stream" else ""
    if target:
        telemetry["gateway_target"] = target[:120]
    release = _installed_release(config)
    if release:
        telemetry["worker_release"] = release[:64]
    return telemetry


def _installed_release(config: Any) -> str:
    """Идентификатор установленного релиза воркера — имя каталога `current`.

    Раскладка воркера: `current` — симлинк на `app/<release_id>`. Читаем ИМЯ,
    а не содержимое: путь до дерева релиза для оператора не сведение, а лишний
    след файловой системы на чужом экране.
    """
    root = getattr(config, "pipeline_root", None)
    if not root:
        return ""
    try:
        return Path(root).resolve().name
    except OSError:
        return ""
