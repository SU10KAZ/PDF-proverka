"""Регистрация воркеров: заявка → ручное одобрение → выдача токена.

Эндпоинт регистрации публичный по необходимости (воркер приходит сам), поэтому
защита двухступенчатая:
  1. одноразовый bootstrap-секрет (переносится на VPS по SSH при установке);
  2. РУЧНОЕ одобрение оператором — до него heartbeat отбивается 403.

Токен отдаётся ровно один раз в жизни; центр хранит только sha256.
"""
from __future__ import annotations

import json
import time
from typing import Any, Optional

from backend.app.models.distributed_workers import (
    RegistrationStatus,
    WorkerState,
)
from backend.app.services.distributed_workers import auth, repositories
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings


class RegistrationConflict(RuntimeError):
    """Регистрация невозможна в текущем состоянии записи."""


def register_worker(
    *,
    instance_id: str,
    display_name_hint: str,
    worker_version: str,
    protocol_version: int,
    pipeline_revision: Optional[str],
    capabilities: dict[str, Any],
    configured_max_slots_hint: int,
    settings: DistributedWorkersSettings,
) -> tuple[dict[str, Any], Optional[str], bool]:
    """Создать заявку или вернуть существующую.

    Возвращает (worker_row, plain_token | None, created).
    Повторная регистрация того же instance_id НЕ выпускает новый токен —
    иначе перезапуск воркера плодил бы действующие секреты.
    """
    existing = repositories.find_worker_by_instance(instance_id, settings=settings)
    if existing is not None:
        if existing["registration_status"] == RegistrationStatus.REVOKED.value:
            raise RegistrationConflict(
                "Доступ этого воркера отозван. Обратитесь к оператору."
            )
        repositories.update_worker_fields(
            existing["worker_id"],
            {
                "worker_version": worker_version,
                "protocol_version": protocol_version,
                "pipeline_revision": pipeline_revision,
                "capabilities": json.dumps(capabilities, ensure_ascii=False),
                "last_seen_at": time.time(),
            },
            settings=settings,
        )
        refreshed = repositories.get_worker(existing["worker_id"], settings=settings)
        return refreshed or existing, None, False

    display_name = (display_name_hint or "").strip() or f"VPS {instance_id[:12]}"
    worker = repositories.create_worker(
        display_name=display_name,
        instance_id=instance_id,
        worker_version=worker_version,
        protocol_version=protocol_version,
        pipeline_revision=pipeline_revision,
        capabilities=capabilities,
        configured_max_slots=max(1, min(5, configured_max_slots_hint)),
        settings=settings,
    )
    token = auth.generate_token()
    repositories.insert_token(
        worker["worker_id"], auth.hash_token(token), settings=settings
    )
    return worker, token, True


def update_registration(
    *,
    worker_id: str,
    instance_id: str,
    worker_version: str,
    protocol_version: int,
    pipeline_revision: Optional[str],
    capabilities: dict[str, Any],
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    repositories.update_worker_fields(
        worker_id,
        {
            "instance_id": instance_id,
            "worker_version": worker_version,
            "protocol_version": protocol_version,
            "pipeline_revision": pipeline_revision,
            "capabilities": json.dumps(capabilities, ensure_ascii=False),
            "last_seen_at": time.time(),
        },
        settings=settings,
    )
    return repositories.get_worker(worker_id, settings=settings) or {}


def approve_worker(
    *,
    worker_id: str,
    display_name: Optional[str],
    configured_max_slots: int,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    worker = repositories.get_worker(worker_id, settings=settings)
    if worker is None:
        raise RegistrationConflict("Воркер не найден.")
    fields: dict[str, Any] = {
        "registration_status": RegistrationStatus.APPROVED.value,
        "worker_state": WorkerState.IDLE.value,
        "configured_max_slots": max(1, min(5, configured_max_slots)),
    }
    if display_name:
        fields["display_name"] = display_name.strip()
    repositories.update_worker_fields(worker_id, fields, settings=settings)
    return repositories.get_worker(worker_id, settings=settings) or {}


def revoke_worker(
    *, worker_id: str, settings: DistributedWorkersSettings
) -> dict[str, Any]:
    """Мягкий отзыв: токены гасятся, новые задания не выдаются.

    Активные задания на воркере при этом не прерываются принудительно — их
    судьбу решает оператор отдельным действием (инвариант «центр не рвёт
    работу из-за административного действия»).
    """
    worker = repositories.get_worker(worker_id, settings=settings)
    if worker is None:
        raise RegistrationConflict("Воркер не найден.")
    repositories.revoke_tokens(worker_id, settings=settings)
    repositories.update_worker_fields(
        worker_id,
        {
            "registration_status": RegistrationStatus.REVOKED.value,
            "worker_state": WorkerState.REVOKED.value,
        },
        settings=settings,
    )
    return repositories.get_worker(worker_id, settings=settings) or {}


def rotate_token(
    *, worker_id: str, settings: DistributedWorkersSettings
) -> tuple[dict[str, Any], str]:
    """Выпустить новый токен. Старые гасятся немедленно.

    Grace-период (сутки на два действующих токена) описан в техпроекте §20.3
    и появится вместе с автоматическим обновлением воркеров; на этапе 0
    ротация ручная и требует перезапуска воркера с новым токеном.
    """
    worker = repositories.get_worker(worker_id, settings=settings)
    if worker is None:
        raise RegistrationConflict("Воркер не найден.")
    repositories.revoke_tokens(worker_id, settings=settings)
    token = auth.generate_token()
    repositories.insert_token(
        worker_id, auth.hash_token(token), label="rotated", settings=settings
    )
    return repositories.get_worker(worker_id, settings=settings) or {}, token
