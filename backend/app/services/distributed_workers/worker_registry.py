"""Реестр воркеров: heartbeat, ось СВЯЗИ, представление для оператора.

Главное правило модуля (ADR-006 и инвариант I-02): здесь вычисляется и
меняется ТОЛЬКО `connection_status`. Ни одна функция этого модуля не имеет
права трогать `remote_jobs.state`. Молчание воркера — свойство канала, а не
доказательство остановки аудита.
"""
from __future__ import annotations

import json
import time
from typing import Any, Optional

from backend.app.models.distributed_workers import (
    ConnectivityState,
    RegistrationStatus,
    WorkerState,
)
from backend.app.services.distributed_workers import repositories
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings


def compute_connectivity(
    last_seen_at: Optional[float],
    *,
    settings: DistributedWorkersSettings,
    now: Optional[float] = None,
) -> ConnectivityState:
    if last_seen_at is None:
        return ConnectivityState.OFFLINE
    delta = (now or time.time()) - float(last_seen_at)
    if delta <= settings.heartbeat_stale_sec:
        return ConnectivityState.ONLINE
    if delta <= settings.heartbeat_offline_sec:
        return ConnectivityState.STALE
    return ConnectivityState.OFFLINE


def record_heartbeat(
    *,
    worker_id: str,
    instance_id: str,
    worker_state: str,
    configured_max_slots: int,
    calculated_free_slots: int,
    active_jobs: list[dict[str, Any]],
    resource_snapshot: Optional[dict[str, Any]],
    warnings: list[dict[str, Any]],
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    now = time.time()
    fields: dict[str, Any] = {
        "instance_id": instance_id,
        "last_seen_at": now,
        "connection_status": ConnectivityState.ONLINE.value,
        "worker_state": worker_state,
        "configured_max_slots": max(0, min(5, configured_max_slots)),
        "calculated_free_slots": max(0, min(5, calculated_free_slots)),
        "active_jobs": json.dumps(active_jobs, ensure_ascii=False),
    }
    if resource_snapshot is not None:
        clean = sanitize_resource_snapshot(resource_snapshot)
        fields["resource_snapshot"] = json.dumps(
            {**clean, "warnings": warnings}, ensure_ascii=False
        )
    repositories.update_worker_fields(worker_id, fields, settings=settings)
    if resource_snapshot is not None:
        repositories.record_resource_snapshot(
            worker_id, clean, settings=settings
        )
    return repositories.get_worker(worker_id, settings=settings) or {}


# Поля снимка ресурсов, которые ДОЛЖНЫ быть числами. Всё остальное в этих
# разделах — либо короткая строка из закрытого набора, либо мусор.
_NUMERIC_SNAPSHOT_SECTIONS = ("ram", "cpu", "disk", "processes", "slots")
_SNAPSHOT_TEXT_FIELDS = {"binding_constraint", "explanation"}
_SNAPSHOT_TEXT_MAX = 200


def sanitize_resource_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Привести снимок ресурсов к безопасному виду.

    Снимок приходит с ПОЛУ-ДОВЕРЕННОГО воркера и попадает и в БД, и на экран
    оператора. Два конкретных последствия отсутствия этой чистки:
    строка вместо числа в `elapsed_sec`/метриках роняла операторский экран
    навсегда (снимок сохраняется), а HTML в `ram.total_gb` исполнялся в
    аутентифицированной сессии оператора. Поэтому: числовые поля — только
    числа, текстовые — только короткие строки, лишние ключи отбрасываются.
    """
    clean: dict[str, Any] = {}
    at = snapshot.get("at")
    clean["at"] = float(at) if isinstance(at, (int, float)) else time.time()
    for section in _NUMERIC_SNAPSHOT_SECTIONS:
        raw = snapshot.get(section)
        if not isinstance(raw, dict):
            continue
        out: dict[str, Any] = {}
        for key, value in raw.items():
            if not isinstance(key, str) or len(key) > 64:
                continue
            if isinstance(value, bool):
                out[key] = value
            elif isinstance(value, (int, float)):
                out[key] = value
            elif key in _SNAPSHOT_TEXT_FIELDS and isinstance(value, str):
                out[key] = value[:_SNAPSHOT_TEXT_MAX]
        clean[section] = out
    return clean


def refresh_connectivity(
    *, settings: DistributedWorkersSettings, now: Optional[float] = None
) -> list[dict[str, Any]]:
    """Пересчитать ось связи для всех воркеров.

    Вызывается на чтении (экран, API), а не фоновым таймером: при выключенном
    флаге фоновых задач в системе быть не должно, а стоимость пересчёта —
    одно сравнение на воркера.
    """
    stamp = now or time.time()
    workers = repositories.list_workers(settings=settings)
    for row in workers:
        expected = compute_connectivity(
            row.get("last_seen_at"), settings=settings, now=stamp
        ).value
        if row.get("connection_status") != expected:
            repositories.update_worker_fields(
                row["worker_id"], {"connection_status": expected}, settings=settings
            )
            row["connection_status"] = expected
    return workers


def to_view(row: dict[str, Any], *, now: Optional[float] = None) -> dict[str, Any]:
    """Плоское представление записи воркера для API оператора."""
    stamp = now or time.time()
    snapshot = _loads(row.get("resource_snapshot"), None)
    warnings = list((snapshot or {}).get("warnings") or [])
    last_seen = row.get("last_seen_at")
    return {
        "worker_id": row["worker_id"],
        "display_name": row.get("display_name") or row["worker_id"],
        "instance_id": row.get("instance_id"),
        "registration_status": row.get("registration_status", RegistrationStatus.PENDING.value),
        "connection_status": row.get("connection_status", ConnectivityState.OFFLINE.value),
        "worker_state": row.get("worker_state", WorkerState.UNREGISTERED.value),
        "last_seen_at": last_seen,
        "seconds_since_seen": (stamp - float(last_seen)) if last_seen else None,
        "worker_version": row.get("worker_version"),
        "protocol_version": row.get("protocol_version", 1),
        "pipeline_revision": row.get("pipeline_revision"),
        "capabilities": _loads(row.get("capabilities"), {}),
        "configured_max_slots": row.get("configured_max_slots", 1),
        "calculated_free_slots": row.get("calculated_free_slots", 0),
        "active_jobs": _loads(row.get("active_jobs"), []),
        "resource_snapshot": snapshot,
        "warnings": warnings,
        "created_at": row.get("created_at", 0.0),
        "updated_at": row.get("updated_at", 0.0),
    }


def can_receive_jobs(row: dict[str, Any]) -> tuple[bool, str]:
    """Может ли воркер получить новое задание. Возвращает (можно, причина отказа)."""
    if row.get("registration_status") != RegistrationStatus.APPROVED.value:
        return False, "Регистрация не одобрена оператором"
    if row.get("worker_state") in (
        WorkerState.REVOKED.value,
        WorkerState.DRAINING.value,
        WorkerState.DRAINED.value,
        WorkerState.DEGRADED.value,
    ):
        return False, f"Состояние воркера: {row.get('worker_state')}"
    if row.get("connection_status") != ConnectivityState.ONLINE.value:
        return False, f"Связь: {row.get('connection_status')}"
    return True, ""


def _loads(raw: Optional[str], default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return default
