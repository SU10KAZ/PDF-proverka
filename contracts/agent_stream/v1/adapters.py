"""Pure HTTP/domain ↔ Agent Stream v1 adapters.

This module has no socket code.  It validates the bounded control-plane shape
and lets the existing polling transport and a future gateway share domain
semantics without leaking generated protobuf objects into business services.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from google.protobuf.timestamp_pb2 import Timestamp

from contracts.agent_stream.v1 import agent_stream_pb2 as stream_pb
from contracts.agent_stream.v1 import common_pb2 as common_pb


PROTOCOL_MAJOR = 1
MAX_CONTROL_MESSAGE_BYTES = 1024 * 1024
MAX_CANONICAL_JSON_BYTES = 256 * 1024
MAX_EVENTS_PER_BATCH = 256
MAX_SAFE_STRING_BYTES = 4096

_FORBIDDEN_KEY_PARTS = (
    "password",
    "api_key",
    "apikey",
    "oauth_token",
    "access_token",
    "refresh_token",
    "worker_token",
    "execution_token",
    "registration_token",
    "claim_secret",
    "private_key",
    "auth_url",
    "device_code",
)

# CanonicalJson is an escape hatch only for already-authoritative bounded
# domain schemas.  Reject obvious executable/admin shapes at the adapter
# boundary as defense in depth; downstream Pydantic/domain validation remains
# mandatory and may be stricter.  Exact matching deliberately leaves safe
# identifiers such as `command_id` and event metadata untouched.
_FORBIDDEN_EXECUTION_KEYS = frozenset(
    {
        "command",
        "shell_command",
        "run_shell",
        "exec",
        "eval",
        "argv",
        "executable",
        "script",
        "python_code",
        "source_code",
        "cwd",
        "env",
        "environment",
        "hook",
        "install_package",
        "edit_file",
        "restart_service",
    }
)


class ContractViolation(ValueError):
    """A control-plane payload violates the v1 application contract."""


def timestamp_from_epoch(value: float | int | None) -> Timestamp:
    stamp = Timestamp()
    if value is not None:
        stamp.FromDatetime(datetime.fromtimestamp(float(value), tz=timezone.utc))
    return stamp


def epoch_from_timestamp(value: Timestamp) -> float:
    return value.ToDatetime(tzinfo=timezone.utc).timestamp()


def _assert_safe(value: Any, *, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).lower()
            if any(part in key for part in _FORBIDDEN_KEY_PARTS):
                raise ContractViolation(f"secret-bearing field is forbidden: {path}.{raw_key}")
            if key in _FORBIDDEN_EXECUTION_KEYS:
                raise ContractViolation(
                    f"executable/admin field is forbidden: {path}.{raw_key}"
                )
            _assert_safe(child, path=f"{path}.{raw_key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _assert_safe(child, path=f"{path}[{index}]")
    elif isinstance(value, str) and len(value.encode("utf-8")) > MAX_SAFE_STRING_BYTES:
        raise ContractViolation(f"string exceeds control-plane bound: {path}")


def canonical_json_message(
    value: Mapping[str, Any] | list[Any], *, schema: str, schema_version: int
) -> common_pb.CanonicalJson:
    _assert_safe(value)
    try:
        encoded = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ContractViolation(f"{schema} is not canonical JSON") from exc
    if len(encoded) > MAX_CANONICAL_JSON_BYTES:
        raise ContractViolation(
            f"{schema} JSON is {len(encoded)} bytes; limit is {MAX_CANONICAL_JSON_BYTES}"
        )
    return common_pb.CanonicalJson(
        schema=schema,
        schema_version=schema_version,
        canonical_json=encoded,
        sha256=hashlib.sha256(encoded).hexdigest(),
    )


def canonical_json_value(message: common_pb.CanonicalJson) -> Any:
    if len(message.canonical_json) > MAX_CANONICAL_JSON_BYTES:
        raise ContractViolation("canonical JSON exceeds v1 bound")
    actual = hashlib.sha256(message.canonical_json).hexdigest()
    if message.sha256 and actual != message.sha256:
        raise ContractViolation("canonical JSON sha256 mismatch")
    value = json.loads(message.canonical_json.decode("utf-8"))
    _assert_safe(value)
    return value


def negotiate_protocol(supported: Iterable[int]) -> int:
    versions = {int(item) for item in supported}
    if PROTOCOL_MAJOR not in versions:
        raise ContractViolation("unsupported protocol major")
    return PROTOCOL_MAJOR


def resolve_connection_epoch(current_epoch: int | None, offered_epoch: int) -> str:
    """Return the v1 duplicate-stream decision without touching connection state."""
    if offered_epoch < 1:
        raise ContractViolation("connection_epoch must be positive")
    if current_epoch is None:
        return "accept"
    if offered_epoch > current_epoch:
        return "supersede_old"
    raise ContractViolation("stale or duplicate connection_epoch")


def _provider_availability(snapshot: Mapping[str, Any]) -> int:
    auth = str(snapshot.get("auth_state") or "").lower()
    state = str(snapshot.get("quota_state") or snapshot.get("status") or "").lower()
    if auth in {"logged_out", "expired", "error"} or state == "auth_required":
        return common_pb.PROVIDER_AVAILABILITY_ACTION_REQUIRED
    if state in {"error", "policy_blocked"}:
        return common_pb.PROVIDER_AVAILABILITY_UNAVAILABLE
    if state in {"limited", "cooldown", "low", "stale"}:
        return common_pb.PROVIDER_AVAILABILITY_DEGRADED
    if auth == "logged_in" or state == "ready":
        return common_pb.PROVIDER_AVAILABILITY_AVAILABLE
    return common_pb.PROVIDER_AVAILABILITY_UNSPECIFIED


def _gb_from_bytes(value: int | float | None) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return 0.0
    return round(float(value) / (1024 ** 3), 2)


def _bytes_from_gb(value: Any) -> int:
    number = float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0
    if number <= 0:
        return 0
    return int(number * (1024 ** 3))


def _provider_snapshot_to_proto(item: Mapping[str, Any]) -> common_pb.ProviderCapabilitySnapshot:
    quota = item.get("quota") if isinstance(item.get("quota"), Mapping) else {}
    remaining = quota.get("estimated_remaining_pct")
    remaining_milli = 0
    raw_supported = bool(quota.get("raw_remaining_supported"))
    if raw_supported and isinstance(remaining, (int, float)) and not isinstance(remaining, bool):
        remaining_milli = max(0, min(100_000, int(round(float(remaining) * 1000))))
    observed = item.get("observed_at") or quota.get("observed_at")
    reset_at = quota.get("next_reset_at")
    return common_pb.ProviderCapabilitySnapshot(
        provider=str(item.get("provider") or ""),
        capabilities=sorted(str(x) for x in (item.get("capabilities") or [])),
        availability=_provider_availability(item),
        safe_status=str(quota.get("quota_state") or item.get("quota_state") or item.get("status") or "unknown")[:64],
        account_group_id=str(item.get("account_group_id") or "")[:64],
        account_kind=str(item.get("account_kind") or "")[:64],
        model_report_supported=bool(item.get("model_report_supported", False)),
        # ПРИНЯТЫЙ ОСТАТОЧНЫЙ ДЕФЕКТ 12I.3. Честное значение для «ещё не
        # опрашивали» — `INSTALL_NOT_OBSERVED`, и оно определено в
        # `audit_worker/providers/identity.py`. На проводе его нет намеренно:
        # работающий шлюз ui-real-16c533a7 приводит состояние к закрытому
        # списку (`installed/missing/broken`) с умолчанием `missing`, то есть
        # перепишет честное значение обратно в ложь, а перекатывать шлюз ради
        # одного стартового окна решено НЕ БУДЕТ (решение заказчика 18.08).
        #
        # Поэтому здесь сохраняется ИСХОДНОЕ умолчание, совместимое с
        # работающим шлюзом. Дефект, который действительно лечится в 12I.3, —
        # другой: заглушка ЗАЩЁЛКИВАЛАСЬ на всё соединение. Обнаружение
        # изменений теперь смотрит на состояние провайдеров
        # (`provider_status_digest`), поэтому первый же завершившийся опрос
        # доезжает до центра в том же соединении, и заглушка живёт секунды.
        installation_status=str(item.get("installation_status") or "missing")[:32],
        auth_state=str(item.get("auth_state") or "unknown")[:32],
        policy_state=str(item.get("policy_state") or "allowed")[:32],
        inference_allowed=bool(item.get("inference_allowed")),
        credential_present=bool(item.get("credential_present")),
        observed_at=timestamp_from_epoch(observed),
        quota_source=str(quota.get("source") or item.get("quota_source") or "unavailable")[:64],
        quota_confidence=str(quota.get("confidence") or item.get("quota_confidence") or "none")[:16],
        remaining_pct_milli=remaining_milli,
        next_reset_at=timestamp_from_epoch(reset_at),
        raw_remaining_supported=raw_supported,
        cli_version=str(item.get("cli_version") or "")[:64],
    )


#: Поля проводного снимка, которые меняются САМИ ПО СЕБЕ на каждом такте.
#: `observed_at` — это время сборки heartbeat, а не время наблюдения; включив
#: его в отпечаток, мы отправляли бы CapabilitiesChanged раз в 30 секунд
#: круглосуточно.
_VOLATILE_WIRE_FIELDS = ("observed_at",)


def provider_status_digest(snapshots: Any) -> str:
    """Отпечаток НАБЛЮДАЕМОГО состояния провайдеров.

    Считается по ТОЙ ЖЕ проекции, что реально уезжает в `CapabilitySnapshot`,
    минус отметка времени сборки. Это принципиально: домашний словарь снимка
    содержит поля, которых на проводе нет вовсе, и одно из них —
    `quota.detail` — на просроченном снимке пересобирается с ВОЗРАСТОМ внутри
    («снимок устарел (N с назад)»). Отпечаток по словарю менялся бы каждые
    30 секунд навсегда; отпечаток по проводу — только когда меняется то, что
    центр действительно видит.

    Повторный УСПЕШНЫЙ опрос с теми же значениями отпечаток не меняет — и это
    требование, а не упущение: центру нечего показывать заново.
    """
    if not isinstance(snapshots, (list, tuple)):
        return ""
    parts: list[tuple[str, bytes]] = []
    for item in snapshots:
        if not isinstance(item, Mapping) or not item.get("provider"):
            continue
        proto = _provider_snapshot_to_proto(item)
        stable = common_pb.ProviderCapabilitySnapshot()
        stable.CopyFrom(proto)
        for field in _VOLATILE_WIRE_FIELDS:
            stable.ClearField(field)
        parts.append((stable.provider, stable.SerializeToString(deterministic=True)))
    parts.sort()
    digest = hashlib.sha256()
    for name, blob in parts:
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(blob)
        digest.update(b"\n")
    return digest.hexdigest()


def provider_capability_to_center(item: common_pb.ProviderCapabilitySnapshot) -> dict[str, Any]:
    remaining = None
    if item.raw_remaining_supported and item.remaining_pct_milli:
        remaining = round(item.remaining_pct_milli / 1000.0, 1)
    observed = epoch_from_timestamp(item.observed_at) if item.HasField("observed_at") else None
    reset = epoch_from_timestamp(item.next_reset_at) if item.HasField("next_reset_at") else None
    quota = {
        "provider": item.provider,
        "quota_state": item.safe_status or "unknown",
        "observed_at": observed,
        "source": item.quota_source or "unavailable",
        "confidence": item.quota_confidence or "none",
        "estimated_remaining_pct": remaining,
        "raw_remaining_supported": bool(item.raw_remaining_supported),
        "next_reset_at": reset,
    }
    return {
        "provider": item.provider,
        # Приёмная сторона: пустое поле — это ОТСУТСТВИЕ сведений, а не
        # доказанное отсутствие CLI. Симметрично отправляющей стороне.
        # Замечание о развёртывании: эту функцию исполняет ШЛЮЗ из своего
        # дерева релиза, поэтому правка вступит в силу только с его будущей
        # выкаткой. Ради неё шлюз перекатывать не нужно: воркер с 12I.3 шлёт
        # непустую строку, и текущий шлюз ретранслирует её как есть.
        "installation_status": item.installation_status or "not_observed",
        "auth_state": item.auth_state or "unknown",
        "policy_state": item.policy_state or "allowed",
        "inference_allowed": bool(item.inference_allowed),
        "credential_present": bool(item.credential_present),
        "observed_at": observed,
        "cli_version": item.cli_version or None,
        "account_group_id": item.account_group_id or None,
        "quota": quota,
    }


#: Какие провайдеры обязаны РАБОТАТЬ для каждого профиля маршрутизации.
#: Не «настроены» и не «авторизованы» — именно работать: профиль, чей провайдер
#: отказывает, приведёт задание к падению на первом же вызове.
ROUTING_PRESET_PROVIDERS: dict[str, tuple[str, ...]] = {
    "claude_gpt_codex": ("claude",),
    "codex_exec": ("codex",),
}

#: Состояния квоты, означающие ДОКАЗАННУЮ непригодность провайдера, а не
#: временную неизвестность. `policy_blocked` попадает сюда потому, что воркер
#: ставит его в двух случаях: запрет нашей политики и отказ провайдера в
#: доступе учётной записи. Оба означают «сейчас работать нельзя».
_UNUSABLE_QUOTA_STATES: frozenset[str] = frozenset({"policy_blocked"})


def usable_routing_compatibility(
    declared: Iterable[Any], snapshots_by_provider: Mapping[str, Mapping[str, Any]]
) -> list[str]:
    """Оставить только те профили, чьи провайдеры действительно применимы.

    Зачем фильтр вообще. Список профилей — это ОБЕЩАНИЕ воркера центру: «такие
    задания я потяну». Обещание, выданное по конфигурации, ничего не знает о
    том, что провайдер отказывает прямо сейчас, и центр честно отправит сюда
    Claude-задание, которое упадёт на первом вызове. Дешевле не обещать.

    Фильтр консервативен: профиль убирается только при ДОКАЗАННОЙ непригодности
    (провайдер сообщил отказ) либо при отсутствующем CLI. «Ещё не опрашивали» и
    «лимит неизвестен» профиль не снимают — незнание не повод отказываться от
    работы.
    """
    out: list[str] = []
    for item in declared:
        name = str(item)
        required = ROUTING_PRESET_PROVIDERS.get(name, ())
        blocked = False
        for provider in required:
            snapshot = snapshots_by_provider.get(provider)
            if not isinstance(snapshot, Mapping):
                continue
            quota = snapshot.get("quota")
            state = str((quota or {}).get("quota_state") or "") if isinstance(quota, Mapping) else ""
            if state in _UNUSABLE_QUOTA_STATES:
                blocked = True
                break
            if str(snapshot.get("policy_state") or "allowed") != "allowed":
                blocked = True
                break
            if str(snapshot.get("installation_status") or "") == "broken":
                blocked = True
                break
        if not blocked:
            out.append(name)
    return out


def capabilities_from_domain(
    capabilities: Mapping[str, Any],
    *,
    provider_snapshots: Iterable[Mapping[str, Any]] = (),
    accepting_jobs: bool = True,
) -> common_pb.CapabilitySnapshot:
    _assert_safe(capabilities)
    provider_caps = capabilities.get("provider_capabilities") or {}
    by_name = {
        str(item.get("provider") or ""): item
        for item in provider_snapshots
        if isinstance(item, Mapping) and item.get("provider")
    }
    # Список ПОЛНЫЙ, включая провайдеров без живого снимка: по нему
    # `capabilities_to_domain` восстанавливает карту `provider_capabilities`, и
    # выбросив ненаблюдённых, мы сообщили бы центру, что воркер не умеет
    # ничего. Синтезированная запись получает стартовую заглушку `missing` —
    # см. принятый остаточный дефект в `_provider_snapshot_to_proto`.
    names = set(str(name) for name in provider_caps)
    names.update(by_name)
    providers = []
    for name in sorted(names):
        snap = by_name.get(name, {})
        snap = dict(snap)
        snap.setdefault("provider", name)
        snap.setdefault("capabilities", sorted(str(item) for item in provider_caps.get(name, [])))
        providers.append(_provider_snapshot_to_proto(snap))
    semantic = {
        "job_types": list(capabilities.get("job_types") or []),
        "compressions": list(capabilities.get("compressions") or []),
        "routing_compatibility": list(capabilities.get("routing_compatibility") or []),
        "provider_capabilities": provider_caps,
        "provider_policy_version": capabilities.get("provider_policy_version", 0),
        "provider_policy_sha256": capabilities.get("provider_policy_sha256", ""),
        "routing_plan_v1": bool(capabilities.get("routing_plan_v1", False)),
    }
    digest = canonical_json_message(
        semantic, schema="audit_worker.capabilities", schema_version=1
    ).sha256
    return common_pb.CapabilitySnapshot(
        revision=str(capabilities.get("capabilities_revision") or digest),
        sha256=digest,
        provider_policy_version=int(capabilities.get("provider_policy_version") or 0),
        provider_policy_sha256=str(capabilities.get("provider_policy_sha256") or ""),
        job_types=[str(item) for item in capabilities.get("job_types") or []],
        compressions=[str(item) for item in capabilities.get("compressions") or []],
        routing_compatibility=usable_routing_compatibility(
            capabilities.get("routing_compatibility") or [], by_name
        ),
        providers=providers,
        routing_plan_v1=bool(capabilities.get("routing_plan_v1", False)),
        accepting_jobs=accepting_jobs,
        max_verified_slots=int(capabilities.get("max_verified_slots") or 0),
        max_package_bytes=int(capabilities.get("max_package_bytes") or 0),
    )


def capabilities_to_domain(message: common_pb.CapabilitySnapshot) -> dict[str, Any]:
    return {
        "job_types": list(message.job_types),
        "compressions": list(message.compressions),
        "routing_compatibility": list(message.routing_compatibility),
        "provider_policy_version": message.provider_policy_version,
        "provider_policy_sha256": message.provider_policy_sha256,
        "provider_capabilities": {
            item.provider: list(item.capabilities) for item in message.providers
        },
        "routing_plan_v1": message.routing_plan_v1,
        "max_verified_slots": message.max_verified_slots,
        "max_package_bytes": message.max_package_bytes,
    }


def _attempt_from_domain(item: Mapping[str, Any]) -> common_pb.AttemptRef:
    state_name = str(item.get("state") or "").upper()
    state = getattr(common_pb, f"JOB_STATE_{state_name}", common_pb.JOB_STATE_UNSPECIFIED)
    return common_pb.AttemptRef(
        job_id=str(item.get("job_id") or ""),
        attempt_id=str(item.get("attempt_id") or ""),
        attempt_number=int(item.get("attempt_no") or item.get("attempt_number") or 0),
        assignment_generation=int(item.get("assignment_generation") or 0),
        state=state,
        stage_id=str(item.get("stage") or item.get("stage_id") or ""),
        last_written_event_sequence=int(
            item.get("last_event_seq") or item.get("last_written_seq") or 0
        ),
        last_acked_event_sequence=int(item.get("last_acked_seq") or 0),
        started_at=timestamp_from_epoch(item.get("started_at")),
        result_ready=bool(item.get("result_ready", False)),
    )


def heartbeat_from_http(
    payload: Mapping[str, Any], *, worker_id: str, connection_id: str
) -> stream_pb.Heartbeat:
    disk = payload.get("disk") if isinstance(payload.get("disk"), Mapping) else {}
    executor = (
        payload.get("executor")
        if isinstance(payload.get("executor"), Mapping)
        else {}
    )
    resource = payload.get("resource_snapshot") or {}
    ram = resource.get("ram") if isinstance(resource.get("ram"), Mapping) else {}
    cpu = resource.get("cpu") if isinstance(resource.get("cpu"), Mapping) else {}
    gpu = resource.get("gpu") if isinstance(resource.get("gpu"), Mapping) else {}
    sampled_at = resource.get("at") if isinstance(resource, Mapping) else None
    worker_state = getattr(
        common_pb,
        "WORKER_STATE_" + str(payload.get("worker_state") or "").upper(),
        common_pb.WORKER_STATE_UNSPECIFIED,
    )
    max_slots = int(payload.get("configured_max_slots") or 0)
    free_slots = int(payload.get("calculated_free_slots") or 0)
    # Resource pressure can reduce calculated_free_slots without occupying an
    # Executor slot. Encoding `max-free` as active work invented attempts and
    # made Gateway reconciliation disagree with the durable local DB.
    active_slots = max(
        int(payload.get("locally_reserved_slots") or 0),
        int(payload.get("active_local_jobs") or 0),
        int(payload.get("running_processes") or 0),
        len(payload.get("active_jobs") or []),
    )
    accepting = (
        free_slots > 0
        and str(payload.get("worker_state") or "")
        not in {"draining", "drained", "revoked", "degraded"}
    )
    capability_stub = {
        "provider_capabilities": {},
        "job_types": [],
        "compressions": [],
    }
    capability_message = capabilities_from_domain(
        capability_stub,
        provider_snapshots=[
            item for item in payload.get("providers") or [] if isinstance(item, Mapping)
        ],
        accepting_jobs=str(payload.get("worker_state") or "") not in {"draining", "drained", "revoked"},
    )
    return stream_pb.Heartbeat(
        worker_id=worker_id,
        connection_id=connection_id,
        observed_at=timestamp_from_epoch(payload.get("sent_at")),
        worker_state=worker_state,
        active_slots=max(0, min(max_slots, active_slots)),
        max_slots=max_slots,
        active_attempts=[
            _attempt_from_domain(item) for item in payload.get("active_jobs") or []
        ],
        resources=common_pb.ResourceSummary(
            disk_total_bytes=int(disk.get("total_bytes") or 0),
            disk_free_bytes=int(disk.get("free_bytes") or 0),
            jobs_bytes=int(disk.get("jobs_bytes") or 0),
            unconfirmed_results_bytes=int(disk.get("unconfirmed_results_bytes") or 0),
            running_processes=int(payload.get("running_processes") or 0),
            active_local_jobs=int(payload.get("active_local_jobs") or 0),
            disk_level=str(disk.get("level") or ""),
            executor_status=str(executor.get("status") or ""),
            sampled_at=timestamp_from_epoch(sampled_at),
            cpu_utilization_pct=float(cpu.get("utilization_pct") or 0.0),
            ram_used_pct=float(ram.get("used_pct") or 0.0),
            ram_total_bytes=_bytes_from_gb(ram.get("total_gb")),
            ram_available_bytes=_bytes_from_gb(ram.get("available_gb")),
            gpu_utilization_pct=float(gpu.get("utilization_pct") or 0.0),
            vram_used_bytes=_bytes_from_gb(gpu.get("used_gb")),
            vram_total_bytes=_bytes_from_gb(gpu.get("total_gb")),
            cpu_cores=int(cpu.get("cores") or 0),
            cpu_load1=float(cpu.get("la1") or 0.0),
            cpu_load5=float(cpu.get("la5") or 0.0),
        ),
        capabilities_revision=capability_message.revision,
        capabilities_sha256=capability_message.sha256,
        capabilities_changed=False,
        accepting_jobs=accepting,
    )


def heartbeat_to_http(message: stream_pb.Heartbeat, *, instance_id: str) -> dict[str, Any]:
    max_slots = int(message.max_slots)
    return {
        "instance_id": instance_id,
        "sent_at": epoch_from_timestamp(message.observed_at),
        "worker_state": common_pb.WorkerState.Name(message.worker_state)
        .removeprefix("WORKER_STATE_")
        .lower(),
        "configured_max_slots": max_slots,
        "calculated_free_slots": max(0, max_slots - int(message.active_slots)),
        "active_jobs": [
            {
                "job_id": item.job_id,
                "attempt_id": item.attempt_id,
                "project_id": "",
                "stage": item.stage_id,
                "last_event_seq": item.last_written_event_sequence,
                "started_at": epoch_from_timestamp(item.started_at)
                if item.HasField("started_at")
                else None,
            }
            for item in message.active_attempts
        ],
        "resource_snapshot": {
            "at": epoch_from_timestamp(message.resources.sampled_at),
            "ram": {
                "total_gb": _gb_from_bytes(message.resources.ram_total_bytes),
                "available_gb": _gb_from_bytes(message.resources.ram_available_bytes),
                "used_pct": message.resources.ram_used_pct or None,
            },
            "cpu": {
                "cores": message.resources.cpu_cores or None,
                "la1": message.resources.cpu_load1 or None,
                "la5": message.resources.cpu_load5 or None,
                "utilization_pct": message.resources.cpu_utilization_pct or None,
            },
            "gpu": {
                "utilization_pct": message.resources.gpu_utilization_pct or None,
                "used_gb": _gb_from_bytes(message.resources.vram_used_bytes),
                "total_gb": _gb_from_bytes(message.resources.vram_total_bytes),
                "source": "nvidia-smi" if message.resources.vram_total_bytes else "unavailable",
            },
        },
        "warnings": [],
        "executor": {"status": message.resources.executor_status},
        "disk": {
            "total_bytes": message.resources.disk_total_bytes,
            "free_bytes": message.resources.disk_free_bytes,
            "jobs_bytes": message.resources.jobs_bytes,
            "unconfirmed_results_bytes": message.resources.unconfirmed_results_bytes,
            "level": message.resources.disk_level,
        },
        "active_local_jobs": message.resources.active_local_jobs,
        "running_processes": message.resources.running_processes,
    }


def package_descriptor_from_http(
    package: Mapping[str, Any], *, direction: int, chunk_size_bytes: int = 0
) -> common_pb.PackageTransferDescriptor:
    # HTTP `url` is intentionally ignored. The stream carries only an opaque id.
    return common_pb.PackageTransferDescriptor(
        transfer_id=str(package.get("package_id") or package.get("upload_id") or ""),
        direction=direction,
        protocol=common_pb.PACKAGE_TRANSFER_PROTOCOL_HTTPS_RESUMABLE_V1,
        package_type=str(package.get("package_type") or ""),
        size_bytes=int(package.get("size_bytes") or package.get("expected_size") or 0),
        sha256=str(package.get("sha256") or package.get("expected_hash") or ""),
        tree_hash=str(package.get("tree_hash") or ""),
        manifest_hash=str(package.get("manifest_hash") or ""),
        manifest_version=int(package.get("manifest_version") or 0),
        compression=str(package.get("compression") or ""),
        chunk_size_bytes=chunk_size_bytes,
    )


def job_offer_from_http(
    assignment: Mapping[str, Any], *, priority: int = 0, required_slots: int = 1
) -> stream_pb.JobOffer:
    params = assignment.get("params") or {}
    if not isinstance(params, Mapping):
        raise ContractViolation("job params must be an object")
    routing = params.get("routing_plan") if isinstance(params, Mapping) else None
    routing = routing if isinstance(routing, Mapping) else {}
    route_message = common_pb.RoutingPlanReference()
    if routing:
        canonical = canonical_json_message(
            routing,
            schema="audit_routing.plan",
            schema_version=int(routing.get("schema_version") or 1),
        )
        route_message.CopyFrom(
            common_pb.RoutingPlanReference(
                routing_plan_id=str(routing.get("routing_plan_id") or ""),
                schema_version=int(routing.get("schema_version") or 1),
                routing_plan_hash=str(routing.get("routing_plan_hash") or canonical.sha256),
                canonical_plan=canonical,
            )
        )
    requirements = []
    requirement = params.get("provider_requirement")
    if isinstance(requirement, Mapping):
        requirements.append(
            common_pb.ProviderRequirement(
                provider=str(requirement.get("provider") or ""),
                capability=str(requirement.get("capability") or ""),
                allowed_stages=[str(x) for x in requirement.get("allowed_stages") or []],
                max_inferences=int(requirement.get("max_inferences") or 0),
            )
        )
    assigned_at = float(assignment.get("assigned_at") or 0)
    ttl = int(assignment.get("assign_ttl_sec") or 0)
    return stream_pb.JobOffer(
        job_id=str(assignment.get("job_id") or ""),
        attempt_id=str(assignment.get("attempt_id") or ""),
        attempt_number=int(assignment.get("attempt_no") or 0),
        assignment_generation=int(assignment.get("assignment_generation") or 1),
        assigned_worker_id=str(assignment.get("worker_id") or ""),
        project_id=str(assignment.get("project_id") or ""),
        version_id=str(assignment.get("version_id") or ""),
        job_type=str(assignment.get("job_type") or ""),
        job_params=canonical_json_message(
            dict(params), schema="audit_worker.job_params", schema_version=1
        ),
        routing_plan=route_message,
        expected_execution_revision=str(params.get("pipeline_revision") or ""),
        source_package=package_descriptor_from_http(
            assignment.get("package") or {},
            direction=common_pb.PACKAGE_DIRECTION_CENTER_TO_AGENT,
        ),
        provider_requirements=requirements,
        created_at=timestamp_from_epoch(assigned_at),
        offer_expires_at=timestamp_from_epoch(assigned_at + ttl),
        priority=priority,
        required_slots=required_slots,
        event_start_sequence=int(assignment.get("event_start_seq") or 1),
    )


def job_offer_to_domain(message: stream_pb.JobOffer) -> dict[str, Any]:
    return {
        "job_id": message.job_id,
        "attempt_id": message.attempt_id,
        "attempt_no": message.attempt_number,
        "assignment_generation": message.assignment_generation,
        "worker_id": message.assigned_worker_id,
        "project_id": message.project_id,
        "version_id": message.version_id or None,
        "job_type": message.job_type,
        "params": canonical_json_value(message.job_params),
        "package": {
            "package_id": message.source_package.transfer_id,
            "package_type": message.source_package.package_type,
            "size_bytes": message.source_package.size_bytes,
            "sha256": message.source_package.sha256,
            "compression": message.source_package.compression,
            "manifest_version": message.source_package.manifest_version,
        },
        "event_start_seq": message.event_start_sequence,
    }


def job_accept_from_http(
    payload: Mapping[str, Any], *, job_id: str, worker_id: str,
    routing_plan_hash: str, execution_revision: str,
) -> stream_pb.JobAccept:
    verified = payload.get("source_verified") or {}
    return stream_pb.JobAccept(
        job_id=job_id,
        attempt_id=str(payload.get("attempt_id") or ""),
        worker_id=worker_id,
        routing_plan_hash=routing_plan_hash,
        execution_revision=execution_revision,
        accepted_at=timestamp_from_epoch(payload.get("accepted_at")),
        source_sha256_verified=bool(verified.get("sha256_ok", False)),
        source_manifest_version=int(verified.get("manifest_version") or 0),
        planned_stages=[str(item) for item in payload.get("planned_stages") or []],
    )


def job_decline_from_http(
    payload: Mapping[str, Any], *, job_id: str, worker_id: str
) -> stream_pb.JobDecline:
    reason_name = str(payload.get("reason_code") or "other").upper()
    reason = getattr(
        stream_pb,
        "JOB_DECLINE_REASON_" + reason_name,
        stream_pb.JOB_DECLINE_REASON_OTHER,
    )
    return stream_pb.JobDecline(
        job_id=job_id,
        attempt_id=str(payload.get("attempt_id") or ""),
        worker_id=worker_id,
        reason=reason,
        safe_detail=str(payload.get("reason") or "")[:500],
        declined_at=timestamp_from_epoch(payload.get("declined_at")),
    )


def progress_from_http(payload: Mapping[str, Any]) -> stream_pb.ProgressUpdate:
    status_name = str(payload.get("status") or "running").upper()
    status = getattr(
        stream_pb,
        "PROGRESS_STATUS_" + status_name,
        stream_pb.PROGRESS_STATUS_UNSPECIFIED,
    )
    current = int(payload.get("current") or 0)
    total = int(payload.get("total") or 0)
    message = stream_pb.ProgressUpdate(
        job_id=str(payload.get("job_id") or ""),
        attempt_id=str(payload.get("attempt_id") or ""),
        stage_id=str(payload.get("stage_id") or ""),
        status=status,
        current=current,
        total=total,
        safe_message=str(payload.get("message") or "")[:500],
        observed_at=timestamp_from_epoch(payload.get("observed_at")),
    )
    if payload.get("action_id") is not None:
        message.action_id = str(payload["action_id"])
    if payload.get("percent") is not None:
        message.percent = float(payload["percent"])
    return message


def progress_to_domain(message: stream_pb.ProgressUpdate) -> dict[str, Any]:
    result: dict[str, Any] = {
        "job_id": message.job_id,
        "attempt_id": message.attempt_id,
        "stage_id": message.stage_id,
        "status": stream_pb.ProgressStatus.Name(message.status)
        .removeprefix("PROGRESS_STATUS_")
        .lower(),
        "current": message.current,
        "total": message.total,
        "message": message.safe_message,
        "observed_at": epoch_from_timestamp(message.observed_at),
    }
    if message.HasField("action_id"):
        result["action_id"] = message.action_id
    if message.HasField("percent"):
        result["percent"] = message.percent
    return result


def event_batch_from_http(payload: Mapping[str, Any], *, worker_id: str) -> stream_pb.EventBatch:
    events = list(payload.get("events") or [])
    if len(events) > MAX_EVENTS_PER_BATCH:
        raise ContractViolation("event batch exceeds v1 count limit")
    proto_events = []
    for event in events:
        event_name = str(event.get("event_type") or "").upper()
        event_type = getattr(
            stream_pb,
            "WORKER_EVENT_TYPE_" + event_name,
            stream_pb.WORKER_EVENT_TYPE_UNSPECIFIED,
        )
        proto_events.append(
            stream_pb.WorkerEvent(
                sequence=int(event.get("seq") or 0),
                event_id=str(event.get("event_id") or ""),
                event_type=event_type,
                occurred_at=timestamp_from_epoch(event.get("occurred_at")),
                schema_version=int(event.get("schema_version") or 1),
                safe_payload=canonical_json_message(
                    event.get("payload") or {},
                    schema="audit_worker.event_payload",
                    schema_version=int(event.get("schema_version") or 1),
                ),
            )
        )
    first = int(payload.get("first_seq") or 0)
    if proto_events and [event.sequence for event in proto_events] != list(
        range(first, first + len(proto_events))
    ):
        raise ContractViolation("EventOutbox sequence must be contiguous within a batch")
    return stream_pb.EventBatch(
        worker_id=worker_id,
        job_id=str(payload.get("job_id") or ""),
        attempt_id=str(payload.get("attempt_id") or ""),
        first_sequence=first,
        events=proto_events,
    )


def event_batch_to_http(message: stream_pb.EventBatch) -> dict[str, Any]:
    events = []
    for event in message.events:
        name = stream_pb.WorkerEventType.Name(event.event_type)
        events.append(
            {
                "seq": event.sequence,
                "event_id": event.event_id,
                "event_type": name.removeprefix("WORKER_EVENT_TYPE_").lower(),
                "occurred_at": epoch_from_timestamp(event.occurred_at),
                "schema_version": event.schema_version,
                "payload": canonical_json_value(event.safe_payload),
            }
        )
    return {
        "job_id": message.job_id,
        "attempt_id": message.attempt_id,
        "first_seq": message.first_sequence,
        "count": len(events),
        "events": events,
    }


def event_ack_from_http(
    response: Mapping[str, Any], *, job_id: str, attempt_id: str
) -> stream_pb.EventAck:
    return stream_pb.EventAck(
        job_id=job_id,
        attempt_id=attempt_id,
        highest_contiguous_sequence=int(response.get("last_seen_seq") or 0),
        accepted=int(response.get("accepted") or 0),
        skipped_duplicates=int(response.get("skipped_duplicates") or 0),
    )


def event_ack_to_http(message: stream_pb.EventAck) -> dict[str, Any]:
    return {
        "last_seen_seq": message.highest_contiguous_sequence,
        "accepted": message.accepted,
        "skipped_duplicates": message.skipped_duplicates,
        "replayed": bool(message.skipped_duplicates),
    }


def cancel_command_from_http(command: Mapping[str, Any]) -> stream_pb.CancelCommand:
    payload = command.get("payload") or {}
    return stream_pb.CancelCommand(
        command_id=str(command.get("command_id") or ""),
        job_id=str(command.get("job_id") or payload.get("job_id") or ""),
        attempt_id=str(command.get("attempt_id") or payload.get("attempt_id") or ""),
        safe_reason=str(payload.get("reason") or "")[:500],
        requested_at=timestamp_from_epoch(command.get("created_at")),
        deadline=timestamp_from_epoch(command.get("expires_at")),
    )


def cancel_ack_from_http(
    response: Mapping[str, Any], *, command_id: str, job_id: str, attempt_id: str
) -> stream_pb.CancelAck:
    stage_name = str(response.get("stage") or response.get("status") or "received").upper()
    stage = getattr(
        stream_pb,
        "CANCEL_ACK_STAGE_" + stage_name,
        stream_pb.CANCEL_ACK_STAGE_UNSPECIFIED,
    )
    return stream_pb.CancelAck(
        command_id=command_id,
        job_id=job_id,
        attempt_id=attempt_id,
        stage=stage,
        safe_detail=str(response.get("detail") or "")[:500],
        acknowledged_at=timestamp_from_epoch(response.get("acknowledged_at")),
    )


def result_ready_from_domain(payload: Mapping[str, Any]) -> stream_pb.ResultReady:
    return stream_pb.ResultReady(
        job_id=str(payload.get("job_id") or ""),
        attempt_id=str(payload.get("attempt_id") or ""),
        result_package=package_descriptor_from_http(
            payload.get("package") or payload,
            direction=common_pb.PACKAGE_DIRECTION_AGENT_TO_CENTER,
            chunk_size_bytes=int(payload.get("chunk_size_bytes") or 0),
        ),
        routing_plan_hash=str(payload.get("routing_plan_hash") or ""),
        execution_revision=str(payload.get("pipeline_revision") or ""),
        stage_status_summary=canonical_json_message(
            payload.get("stage_status_summary") or {},
            schema="audit_worker.stage_status_summary",
            schema_version=1,
        ),
        provider_action_ledger_summary=canonical_json_message(
            payload.get("provider_action_ledger_summary") or {},
            schema="audit_worker.provider_action_ledger_summary",
            schema_version=1,
        ),
        ready_at=timestamp_from_epoch(payload.get("ready_at")),
    )


def result_ready_to_domain(message: stream_pb.ResultReady) -> dict[str, Any]:
    package = message.result_package
    return {
        "job_id": message.job_id,
        "attempt_id": message.attempt_id,
        "package": {
            "upload_id": package.transfer_id,
            "package_type": package.package_type,
            "expected_size": package.size_bytes,
            "expected_hash": package.sha256,
            "tree_hash": package.tree_hash,
            "manifest_hash": package.manifest_hash,
            "manifest_version": package.manifest_version,
            "compression": package.compression,
        },
        "routing_plan_hash": message.routing_plan_hash,
        "pipeline_revision": message.execution_revision,
        "stage_status_summary": canonical_json_value(message.stage_status_summary),
        "provider_action_ledger_summary": canonical_json_value(
            message.provider_action_ledger_summary
        ),
        "ready_at": epoch_from_timestamp(message.ready_at),
    }


def result_ack_from_http(
    response: Mapping[str, Any], *, job_id: str, attempt_id: str, result_sha256: str
) -> stream_pb.ResultAck:
    retention_until = response.get("retention_until")
    accepted_at = float(response.get("server_time") or 0)
    return stream_pb.ResultAck(
        job_id=job_id,
        attempt_id=attempt_id,
        result_sha256=result_sha256,
        validation_status=stream_pb.RESULT_VALIDATION_STATUS_ACCEPTED,
        accepted_at=timestamp_from_epoch(accepted_at),
        retention_starts_at=timestamp_from_epoch(accepted_at),
        retention_until=timestamp_from_epoch(retention_until),
    )


def result_ack_to_http(message: stream_pb.ResultAck) -> dict[str, Any]:
    return {
        "state": "completed"
        if message.validation_status == stream_pb.RESULT_VALIDATION_STATUS_ACCEPTED
        else "superseded_result_received",
        "validation": {
            "status": stream_pb.ResultValidationStatus.Name(message.validation_status)
            .removeprefix("RESULT_VALIDATION_STATUS_")
            .lower(),
            "result_sha256": message.result_sha256,
        },
        "server_time": epoch_from_timestamp(message.accepted_at),
        "retention_until": epoch_from_timestamp(message.retention_until)
        if message.HasField("retention_until")
        else None,
    }


def validate_control_message(message: Any) -> int:
    encoded = message.SerializeToString()
    if len(encoded) > MAX_CONTROL_MESSAGE_BYTES:
        raise ContractViolation("control message exceeds v1 size limit")
    return len(encoded)
