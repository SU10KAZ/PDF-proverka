"""Сверка состояний после рестарта любой из сторон.

Воркер НЕ принимает решений о судьбе задания сам: он сообщает, что у него
есть, и исполняет `action` из закрытого enum ответа. Единственное исключение —
недоступный центр: тогда действует правило I-01 «продолжай», потому что
остановка из-за молчания центра запрещена.
"""
from __future__ import annotations

import time
from typing import Any, Optional

from audit_worker.client import CenterClient, CenterError
from audit_worker.event_outbox import EventOutbox
from audit_worker.local_store import LocalJobStore
from audit_worker.process_registry import ProcessRegistry


def collect_known_jobs(
    store: LocalJobStore, jobs_dir, *, registry: Optional[ProcessRegistry] = None
) -> list[dict[str, Any]]:
    known: list[dict[str, Any]] = []
    for meta in store.iter_all():
        if meta.get("local_state") in ("finished", "superseded", "rejected"):
            continue
        events_dir = store.job_dir(meta["job_id"], meta["attempt_id"]) / "events"
        outbox = EventOutbox(events_dir)
        alive = bool(
            registry
            and registry.alive_for_job(meta["job_id"], meta["attempt_id"])
        )
        known.append(
            {
                "job_id": meta["job_id"],
                "attempt_id": meta["attempt_id"],
                "local_state": meta.get("local_state", "unknown"),
                "last_written_seq": outbox.last_written_seq,
                "last_acked_seq": outbox.last_acked_seq,
                "result_ready": bool(meta.get("result_hash")),
                "result_hash": meta.get("result_hash"),
                "processes_alive": alive,
            }
        )
    return known


def reconcile(
    client: CenterClient,
    store: LocalJobStore,
    *,
    instance_id: str,
    previous_instance_id: Optional[str],
    registry: Optional[ProcessRegistry] = None,
) -> dict[str, Any]:
    """Спросить центр о судьбе локальных заданий.

    При недоступности центра возвращает пустой вердикт: агент продолжает
    работу по локальному состоянию (инвариант I-01).
    """
    known = collect_known_jobs(store, store.jobs_dir, registry=registry)
    payload = {
        "instance_id": instance_id,
        "previous_instance_id": previous_instance_id,
        "restarted_at": time.time(),
        "known_jobs": known,
    }
    try:
        return client.reconcile(payload)
    except CenterError as exc:
        return {"server_time": time.time(), "jobs": [], "unknown_jobs": [],
                "pending_commands": 0, "error": f"HTTP {exc.status}: {exc.detail}"}
    except Exception as exc:  # noqa: BLE001 — недоступность центра не повод падать
        return {"server_time": time.time(), "jobs": [], "unknown_jobs": [],
                "pending_commands": 0, "error": str(exc)}


def survived_processes(
    store: LocalJobStore, registry: ProcessRegistry
) -> list[dict[str, Any]]:
    """Задания, чьи процессы пережили рестарт агента.

    Такие задания трогать нельзя: пересчёт с нуля потерял бы уже сделанную
    работу (§18.2 сценарий 7).
    """
    out = []
    for meta in store.iter_all():
        if meta.get("local_state") != "running":
            continue
        if registry.alive_for_job(meta["job_id"], meta["attempt_id"]):
            out.append(meta)
    return out
