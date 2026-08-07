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
from backend.app.services.distributed_workers import auth, database, repositories
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings


class RegistrationConflict(RuntimeError):
    """Регистрация невозможна в текущем состоянии записи."""


class ClaimRejected(RuntimeError):
    """Claim-секрет не принят: не одобрено, уже использовано или неверен."""


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
    """Создать заявку (pending) и выдать ОДНОРАЗОВЫЙ claim-secret.

    Возвращает (worker_row, plain_claim_secret | None, created).

    Токен доступа здесь НЕ выдаётся — он появляется только после одобрения
    оператором, в обмен на claim-secret (см. `claim_token`). Иначе секрет
    доступа существовал бы у ещё не одобренного воркера.

    Повторная регистрация того же instance_id не плодит записи: возвращается
    существующая, а claim-secret перевыпускается ТОЛЬКО если прежний ещё не
    использован (иначе перезапуск воркера обесценивал бы выданный токен).
    """
    existing = repositories.find_worker_by_instance(instance_id, settings=settings)
    if existing is not None:
        status = existing["registration_status"]
        if status == RegistrationStatus.REVOKED.value:
            raise RegistrationConflict(
                "Доступ этого воркера отозван. Обратитесь к оператору."
            )
        if status == RegistrationStatus.REJECTED.value:
            raise RegistrationConflict(
                "Заявка этого воркера отклонена оператором."
            )
        fields: dict[str, Any] = {
            "worker_version": worker_version,
            "protocol_version": protocol_version,
            "pipeline_revision": pipeline_revision,
            "capabilities": json.dumps(capabilities, ensure_ascii=False),
            "last_seen_at": time.time(),
        }
        # Claim-secret по повторной заявке НЕ перевыпускается.
        #
        # `instance_id` воркер назначает себе сам, доказательства владения им
        # нет. Раньше повторная заявка с чужим instance_id выдавала свежий
        # claim-secret и затирала хэш прежнего: атакующий, знающий только
        # bootstrap-секрет, перехватывал заявку настоящей машины, оператор
        # одобрял «знакомое имя», а токен получал чужой. Ручное одобрение
        # переставало быть вторым фактором.
        #
        # Потерявший claim-secret воркер восстанавливается через оператора:
        # «Отклонить» → регистрация заново, либо ротация после одобрения.
        claim_secret: Optional[str] = None
        repositories.update_worker_fields(existing["worker_id"], fields, settings=settings)
        refreshed = repositories.get_worker(existing["worker_id"], settings=settings)
        return refreshed or existing, claim_secret, False

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
    claim_secret = auth.generate_claim_secret()
    repositories.update_worker_fields(
        worker["worker_id"],
        {
            "claim_secret_sha256": auth.hash_token(claim_secret),
            "claim_issued_at": time.time(),
        },
        settings=settings,
    )
    return (
        repositories.get_worker(worker["worker_id"], settings=settings) or worker,
        claim_secret,
        True,
    )


def claim_token(
    *, worker_id: str, claim_secret: str, settings: DistributedWorkersSettings
) -> tuple[dict[str, Any], str]:
    """Обменять одноразовый claim-secret на постоянный worker-token.

    Условия: регистрация одобрена, claim-secret совпадает, ранее не
    использовался. Токен возвращается ЕДИНСТВЕННЫЙ раз; центр хранит только
    его sha256, поэтому потерянный токен нельзя извлечь обратно из БД —
    только выпустить новый ротацией.
    """
    token = auth.generate_token()
    # Проверка и погашение — в ОДНОЙ транзакции. Раздельно они давали гонку:
    # два одновременных /claim с одним секретом проходили проверку оба и
    # выпускали ДВА живых токена, второй из которых никто не отслеживал.
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM workers WHERE worker_id = ?", (worker_id,)
        ).fetchone()
        if row is None:
            raise ClaimRejected("Воркер не найден.")
        worker = dict(row)
        status = worker.get("registration_status")
        if status == RegistrationStatus.REJECTED.value:
            raise ClaimRejected("Заявка отклонена оператором.")
        if status == RegistrationStatus.REVOKED.value:
            raise ClaimRejected("Доступ воркера отозван.")
        if status != RegistrationStatus.APPROVED.value:
            raise ClaimRejected("Регистрация ещё не одобрена оператором.")
        if worker.get("claim_used_at") is not None:
            raise ClaimRejected(
                "Claim уже использован. Токен выдаётся один раз; "
                "если он утерян — оператор выполняет ротацию."
            )
        stored = worker.get("claim_secret_sha256")
        if not stored or not auth.constant_time_equals(
            auth.hash_token((claim_secret or "").strip()), stored
        ):
            raise ClaimRejected("Claim-secret неверен.")

        now = time.time()
        conn.execute(
            "INSERT INTO worker_tokens (token_id, worker_id, token_sha256, label,"
            " created_at) VALUES (?, ?, ?, ?, ?)",
            (repositories.new_id("tok"), worker_id, auth.hash_token(token),
             "claim", now),
        )
        conn.execute(
            # Хэш claim-secret стирается: он больше не нужен и не должен
            # оставаться в базе.
            "UPDATE workers SET claim_used_at = ?, claim_secret_sha256 = NULL,"
            " updated_at = ? WHERE worker_id = ?",
            (now, now, worker_id),
        )
    return repositories.get_worker(worker_id, settings=settings) or worker, token


def reject_worker(
    *, worker_id: str, settings: DistributedWorkersSettings
) -> dict[str, Any]:
    """Отклонить заявку: claim-secret обесценивается, токен не выдаётся."""
    worker = repositories.get_worker(worker_id, settings=settings)
    if worker is None:
        raise RegistrationConflict("Воркер не найден.")
    repositories.update_worker_fields(
        worker_id,
        {
            "registration_status": RegistrationStatus.REJECTED.value,
            "worker_state": WorkerState.REVOKED.value,
            "claim_secret_sha256": None,
            "rejected_at": time.time(),
        },
        settings=settings,
    )
    return repositories.get_worker(worker_id, settings=settings) or {}


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
