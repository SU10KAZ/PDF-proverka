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
    names = set(str(name) for name in provider_caps)
    names.update(by_name)
    providers = []
    for name in sorted(names):
        snap = by_name.get(name, {})
        providers.append(
            common_pb.ProviderCapabilitySnapshot(
                provider=name,
                capabilities=sorted(str(item) for item in provider_caps.get(name, [])),
                availability=_provider_availability(snap),
                safe_status=str(snap.get("quota_state") or snap.get("status") or "unknown")[:64],
                account_group_id=str(snap.get("account_group_id") or "")[:64],
                account_kind=str(snap.get("account_kind") or "")[:64],
                model_report_supported=bool(snap.get("model_report_supported", False)),
            )
        )
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
        routing_compatibility=[
            str(item) for item in capabilities.get("routing_compatibility") or []
        ],
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
        "resource_snapshot": {"at": epoch_from_timestamp(message.resources.sampled_at)},
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
