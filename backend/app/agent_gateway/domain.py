"""Proto ↔ existing distributed-worker domain service adapter.

No HTTP loopback calls and no duplicate scheduler/state machine live here.
"""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Optional

from backend.app.models.distributed_workers import JobState, WorkerCommandType
from backend.app.services.distributed_workers import (
    attempt_service,
    database,
    event_service,
    gateway_repository,
    job_service,
    package_service,
    provider_accounts,
    registration_service,
    repositories,
    worker_registry,
)
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings
from contracts.agent_stream.v1 import adapters
from contracts.agent_stream.v1 import agent_stream_pb2 as stream_pb
from contracts.agent_stream.v1 import common_pb2 as common_pb


class DomainViolation(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: int = common_pb.ERROR_CODE_PROTOCOL_VIOLATION,
        retryable: bool = False,
        close_stream: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.close_stream = close_stream


def _epoch(stamp) -> float:
    if not stamp.ByteSize():
        return time.time()
    return float(stamp.seconds) + float(stamp.nanos) / 1_000_000_000


def _payload(job: dict[str, Any]) -> dict[str, Any]:
    raw = job.get("payload") or "{}"
    return json.loads(raw) if isinstance(raw, str) else dict(raw)


def _routing_hash(job: dict[str, Any]) -> str:
    route = (_payload(job).get("params") or {}).get("routing_plan") or {}
    return str(route.get("routing_plan_hash") or "")


class GatewayDomainAdapter:
    def __init__(self, settings: DistributedWorkersSettings) -> None:
        self.settings = settings

    async def accept_hello(
        self, hello: stream_pb.AgentHello, *, connection_id: str, protocol_version: int
    ) -> tuple[dict[str, Any], Optional[str]]:
        try:
            worker, old_connection = await database.run_db(
                gateway_repository.accept_connection,
                worker_id=hello.worker_id,
                instance_id=hello.worker_instance_id,
                connection_id=connection_id,
                connection_epoch=hello.connection_epoch,
                protocol_version=protocol_version,
                settings=self.settings,
            )
        except gateway_repository.GatewayConnectionRejected as exc:
            raise DomainViolation(
                str(exc), code=common_pb.ERROR_CODE_STALE_CONNECTION, close_stream=True
            ) from exc

        capabilities = adapters.capabilities_to_domain(hello.capabilities)
        capabilities["providers"] = sorted(capabilities.get("provider_capabilities", {}))
        try:
            await database.run_db(
                registration_service.update_registration,
                worker_id=hello.worker_id,
                instance_id=hello.worker_instance_id,
                worker_version=hello.worker_software_version,
                protocol_version=protocol_version,
                pipeline_revision=hello.execution_revision or None,
                capabilities=capabilities,
                settings=self.settings,
            )
        except (sqlite3.Error, OSError):
            raise
        except Exception as exc:
            raise DomainViolation("capabilities/revision update rejected") from exc
        await self._record_provider_snapshots(hello.worker_id, hello.capabilities)
        return worker, old_connection

    async def _record_provider_snapshots(
        self, worker_id: str, capabilities: common_pb.CapabilitySnapshot
    ) -> None:
        snapshots = [
            {
                "provider": item.provider,
                "status": common_pb.ProviderAvailability.Name(item.availability)
                .removeprefix("PROVIDER_AVAILABILITY_")
                .lower(),
                "quota_state": item.safe_status,
                "account_group_id": item.account_group_id,
                "account_kind": item.account_kind,
                "model_report_supported": item.model_report_supported,
            }
            for item in capabilities.providers
        ]
        if snapshots:
            await database.run_db(
                provider_accounts.record_worker_providers,
                worker_id=worker_id,
                snapshots=snapshots,
                settings=self.settings,
            )

    async def center_hello(self, *, connection_id: str) -> stream_pb.CenterHello:
        cursors = []
        # worker-scoped cursors are filled by the service after session creation.
        return stream_pb.CenterHello(
            accepted_protocol_version=1,
            connection_id=connection_id,
            server_time=adapters.timestamp_from_epoch(time.time()),
            heartbeat_interval={"seconds": 30},
            offer_lease_duration={"seconds": int(job_service.ASSIGN_TTL_SEC)},
            max_control_message_bytes=adapters.MAX_CONTROL_MESSAGE_BYTES,
            max_events_per_batch=adapters.MAX_EVENTS_PER_BATCH,
            max_unacked_event_window=1024,
            resume_cursors=cursors,
            duplicate_connection_policy=stream_pb.DUPLICATE_CONNECTION_POLICY_NEWER_EPOCH_SUPERSEDES,
        )

    async def resume_cursors(self, worker_id: str) -> list[common_pb.EventCursor]:
        rows = await database.run_db(
            repositories.cursors_for_worker, worker_id, settings=self.settings
        )
        return [
            common_pb.EventCursor(
                job_id=row["job_id"],
                attempt_id=row["attempt_id"],
                highest_contiguous_sequence=int(row["last_seen_seq"]),
            )
            for row in rows
        ]

    async def heartbeat(
        self, worker_id: str, instance_id: str, message: stream_pb.Heartbeat
    ) -> None:
        payload = adapters.heartbeat_to_http(message, instance_id=instance_id)
        await database.run_db(
            worker_registry.record_heartbeat,
            worker_id=worker_id,
            instance_id=instance_id,
            worker_state=payload["worker_state"],
            configured_max_slots=payload["configured_max_slots"],
            calculated_free_slots=payload["calculated_free_slots"],
            active_jobs=payload["active_jobs"],
            resource_snapshot=payload["resource_snapshot"],
            warnings=payload["warnings"],
            executor=payload["executor"],
            disk=payload["disk"],
            max_verified_slots=message.max_slots,
            active_local_jobs=payload["active_local_jobs"],
            running_processes=payload["running_processes"],
            locally_reserved_slots=message.active_slots,
            settings=self.settings,
        )

    async def capabilities_changed(
        self, worker_id: str, instance_id: str, message: stream_pb.CapabilitiesChanged
    ) -> None:
        worker = await database.run_db(
            repositories.get_worker, worker_id, settings=self.settings
        )
        if worker is None:
            raise DomainViolation("worker not found")
        caps = adapters.capabilities_to_domain(message.capabilities)
        caps["providers"] = sorted(caps.get("provider_capabilities", {}))
        await database.run_db(
            registration_service.update_registration,
            worker_id=worker_id,
            instance_id=instance_id,
            worker_version=str(worker.get("worker_version") or ""),
            protocol_version=1,
            pipeline_revision=worker.get("pipeline_revision"),
            capabilities=caps,
            settings=self.settings,
        )
        await self._record_provider_snapshots(worker_id, message.capabilities)

    async def recover_expired_offers(self) -> int:
        return await database.run_db(
            gateway_repository.recover_expired_offers, settings=self.settings
        )

    async def claim_offer(
        self,
        *,
        worker_id: str,
        connection_id: str,
        free_slots: int,
        busy_slots: int,
        offer_timeout_sec: float,
    ) -> Optional[stream_pb.JobOffer]:
        if free_slots <= 0:
            return None
        expires_at = time.time() + offer_timeout_sec
        try:
            job = await database.run_db(
                repositories.claim_next_job_for_worker,
                worker_id,
                worker_free_hint=free_slots,
                worker_busy_hint=busy_slots,
                transport_mode="grpc_stream",
                gateway_offer={
                    "connection_id": connection_id,
                    "expires_at": expires_at,
                },
                settings=self.settings,
            )
        except repositories.SlotLimitReached:
            return None
        if job is None:
            return None
        return self._job_offer(job, expires_at=expires_at)

    async def outstanding_offers(self, worker_id: str) -> list[stream_pb.JobOffer]:
        rows = await database.run_db(
            gateway_repository.pending_offers, worker_id, settings=self.settings
        )
        result = []
        for offer in rows:
            attempt = await database.run_db(
                repositories.get_attempt, offer["attempt_id"], settings=self.settings
            )
            if attempt is not None:
                result.append(self._job_offer(attempt, expires_at=float(offer["expires_at"])))
        return result

    def _job_offer(self, job: dict[str, Any], *, expires_at: float) -> stream_pb.JobOffer:
        archive = job_service.source_package_path(job, settings=self.settings)
        if archive is None:
            raise DomainViolation("source package not found", retryable=True)
        manifest_path = archive.parent / package_service.MANIFEST_NAME
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        params = job_service.assignment_params(job, _payload(job)).model_dump(mode="json")
        assignment = {
            "job_id": job["job_id"],
            "attempt_id": job["attempt_id"],
            "attempt_no": int(job.get("attempt_no") or 1),
            "assignment_generation": int(job.get("assignment_generation") or 1),
            "worker_id": job["assigned_worker_id"],
            "assigned_at": float(job.get("assigned_at") or time.time()),
            "assign_ttl_sec": max(1, int(expires_at - time.time())),
            "job_type": job["job_type"],
            "project_id": job["project_id"],
            "version_id": job.get("version_id"),
            "params": params,
            "package": {
                "package_id": manifest["package_id"],
                "package_type": "source",
                "size_bytes": int(manifest["archive"]["compressed_bytes"]),
                "sha256": manifest["archive"]["sha256"],
                "compression": manifest["compression"],
                "manifest_version": int(manifest["manifest_version"]),
            },
            "event_start_seq": 1,
        }
        offer = adapters.job_offer_from_http(assignment)
        offer.offer_expires_at.CopyFrom(adapters.timestamp_from_epoch(expires_at))
        return offer

    async def mark_offer_delivered(self, attempt_id: str) -> None:
        await database.run_db(
            gateway_repository.mark_offer_delivered, attempt_id, settings=self.settings
        )

    async def accept_job(self, worker_id: str, message: stream_pb.JobAccept) -> dict[str, Any]:
        attempt = await self._attempt(message.job_id, message.attempt_id, worker_id)
        if not message.source_sha256_verified:
            raise DomainViolation("source package was not verified")
        expected_route = _routing_hash(attempt)
        if expected_route and message.routing_plan_hash != expected_route:
            raise DomainViolation("routing plan hash mismatch", code=common_pb.ERROR_CODE_REVISION_MISMATCH)
        expected_revision = str(attempt.get("pipeline_revision") or "")
        if expected_revision and message.execution_revision != expected_revision:
            raise DomainViolation("execution revision mismatch", code=common_pb.ERROR_CODE_REVISION_MISMATCH)
        if attempt["state"] == JobState.ACCEPTED_BY_WORKER.value:
            await database.run_db(
                gateway_repository.mark_offer_accepted, message.attempt_id, settings=self.settings
            )
            return attempt
        if attempt["state"] == JobState.SOURCE_UPLOADING.value:
            attempt = await database.run_db(
                job_service.transition,
                attempt_id=message.attempt_id,
                to_state=JobState.SOURCE_READY,
                actor="worker",
                reason="gRPC source descriptor/hash verified",
                settings=self.settings,
            )
        try:
            attempt = await database.run_db(
                job_service.transition,
                attempt_id=message.attempt_id,
                to_state=JobState.ACCEPTED_BY_WORKER,
                actor="worker",
                reason="gRPC JobAccept",
                settings=self.settings,
            )
        except job_service.IllegalTransition as exc:
            raise DomainViolation(str(exc), code=common_pb.ERROR_CODE_JOB_CONFLICT) from exc
        await database.run_db(
            gateway_repository.mark_offer_accepted, message.attempt_id, settings=self.settings
        )
        return attempt

    async def decline_job(self, worker_id: str, message: stream_pb.JobDecline) -> None:
        await self._attempt(message.job_id, message.attempt_id, worker_id)
        temporary = message.reason in {
            stream_pb.JOB_DECLINE_REASON_NO_SLOT,
            stream_pb.JOB_DECLINE_REASON_WORKER_DRAINING,
            stream_pb.JOB_DECLINE_REASON_PROVIDER_UNAVAILABLE,
        }
        reason = stream_pb.JobDeclineReason.Name(message.reason).lower()
        attempt = await database.run_db(
            gateway_repository.decline_offer,
            attempt_id=message.attempt_id,
            worker_id=worker_id,
            reason=reason,
            requeue=temporary,
            settings=self.settings,
        )
        if not temporary and attempt["state"] == JobState.SOURCE_UPLOADING.value:
            await database.run_db(
                job_service.transition,
                attempt_id=message.attempt_id,
                to_state=JobState.FAILED,
                actor="worker",
                reason=f"gRPC JobDecline: {reason}",
                fields={"error": json.dumps({"code": "rejected_by_worker", "message": message.safe_detail[:500]})},
                settings=self.settings,
            )

    async def progress(self, worker_id: str, message: stream_pb.ProgressUpdate) -> None:
        await self._attempt(message.job_id, message.attempt_id, worker_id)
        snapshot = adapters.progress_to_domain(message)
        await database.run_db(
            gateway_repository.record_progress,
            attempt_id=message.attempt_id,
            worker_id=worker_id,
            snapshot=snapshot,
            settings=self.settings,
        )

    async def event_batch(self, worker_id: str, message: stream_pb.EventBatch) -> stream_pb.EventAck:
        attempt = await self._attempt(message.job_id, message.attempt_id, worker_id)
        payload = adapters.event_batch_to_http(message)
        try:
            result = await database.run_db(
                event_service.ingest_batch,
                job=attempt,
                worker_id=worker_id,
                first_seq=payload["first_seq"],
                events=payload["events"],
                settings=self.settings,
            )
        except event_service.SequenceGap as exc:
            raise DomainViolation(
                f"event sequence gap; expected {exc.expected_seq}",
                code=common_pb.ERROR_CODE_JOB_CONFLICT,
                retryable=True,
            ) from exc
        return adapters.event_ack_from_http(
            result, job_id=message.job_id, attempt_id=message.attempt_id
        )

    async def job_status(self, worker_id: str, message: stream_pb.JobStatusUpdate) -> None:
        attempt = await self._attempt(message.job_id, message.attempt_id, worker_id)
        state_name = common_pb.JobState.Name(message.state).removeprefix("JOB_STATE_").lower()
        if not state_name or state_name == "unspecified":
            raise DomainViolation("job state unspecified")
        target = JobState(state_name)
        if attempt["state"] == target.value:
            return
        try:
            await database.run_db(
                job_service.transition,
                attempt_id=message.attempt_id,
                to_state=target,
                actor="worker",
                reason="gRPC JobStatusUpdate",
                settings=self.settings,
            )
        except (ValueError, job_service.IllegalTransition) as exc:
            raise DomainViolation("invalid job state transition", code=common_pb.ERROR_CODE_JOB_CONFLICT) from exc

    async def pending_cancel_commands(self, worker_id: str) -> list[stream_pb.CancelCommand]:
        commands = await database.run_db(
            repositories.pending_commands,
            worker_id,
            mark_delivered=True,
            settings=self.settings,
        )
        result = []
        for command in commands:
            if command.get("command_type") != WorkerCommandType.CANCEL_ATTEMPT.value:
                continue
            command["payload"] = json.loads(command.get("payload") or "{}")
            result.append(adapters.cancel_command_from_http(command))
        return result

    async def cancel_ack(self, worker_id: str, message: stream_pb.CancelAck) -> None:
        command = await database.run_db(
            repositories.get_command, message.command_id, settings=self.settings
        )
        if command is None or command.get("worker_id") != worker_id:
            raise DomainViolation("cancel command identity mismatch")
        outcome_by_stage = {
            stream_pb.CANCEL_ACK_STAGE_CANCELLED: "cancelled",
            stream_pb.CANCEL_ACK_STAGE_ALREADY_FINISHED: "already_completed",
            stream_pb.CANCEL_ACK_STAGE_NOT_FOUND: "not_running_locally",
            stream_pb.CANCEL_ACK_STAGE_REJECTED: "ownership_mismatch",
            stream_pb.CANCEL_ACK_STAGE_RECEIVED: "received",
            stream_pb.CANCEL_ACK_STAGE_CANCEL_IN_PROGRESS: "in_progress",
        }
        outcome = outcome_by_stage.get(message.stage)
        if outcome is None:
            raise DomainViolation("cancel ACK stage unspecified")
        result = {"status": "ok", "detail": {"outcome": outcome, "message": message.safe_detail[:500]}}
        try:
            await database.run_db(
                repositories.ack_command, message.command_id, result, settings=self.settings
            )
        except repositories.CommandAckConflict as exc:
            raise DomainViolation(str(exc), code=common_pb.ERROR_CODE_JOB_CONFLICT) from exc
        await database.run_db(
            attempt_service.apply_cancel_ack,
            command=command,
            result=result,
            settings=self.settings,
        )

    async def result_ready(
        self, worker_id: str, message: stream_pb.ResultReady
    ) -> stream_pb.ResultAck | stream_pb.ResultRejected | None:
        if message.result_package.ByteSize() == 0 or not message.result_package.transfer_id:
            raise DomainViolation("result transfer descriptor missing")
        attempt = await self._attempt(message.job_id, message.attempt_id, worker_id)
        upload = await database.run_db(
            repositories.get_upload_session,
            message.result_package.transfer_id,
            settings=self.settings,
        )
        if upload is None:
            return stream_pb.ResultRejected(
                job_id=message.job_id,
                attempt_id=message.attempt_id,
                result_sha256=message.result_package.sha256,
                reason=stream_pb.RESULT_REJECT_REASON_UNEXPECTED_RESULT,
                safe_detail="unknown HTTPS upload transfer",
                retryable=True,
                rejected_at=adapters.timestamp_from_epoch(time.time()),
            )
        if (
            upload.get("job_id") != message.job_id
            or upload.get("attempt_id") != message.attempt_id
            or str(upload.get("expected_hash") or "") != message.result_package.sha256
        ):
            return stream_pb.ResultRejected(
                job_id=message.job_id,
                attempt_id=message.attempt_id,
                result_sha256=message.result_package.sha256,
                reason=stream_pb.RESULT_REJECT_REASON_UNEXPECTED_RESULT,
                safe_detail="HTTPS transfer identity mismatch",
                retryable=False,
                rejected_at=adapters.timestamp_from_epoch(time.time()),
            )
        expected_route = _routing_hash(attempt)
        if expected_route and message.routing_plan_hash != expected_route:
            return stream_pb.ResultRejected(
                job_id=message.job_id,
                attempt_id=message.attempt_id,
                result_sha256=message.result_package.sha256,
                reason=stream_pb.RESULT_REJECT_REASON_ROUTING_PLAN_HASH_MISMATCH,
                safe_detail="routing plan hash mismatch",
                retryable=False,
                rejected_at=adapters.timestamp_from_epoch(time.time()),
            )
        expected_revision = str(attempt.get("pipeline_revision") or "")
        if expected_revision and message.execution_revision != expected_revision:
            return stream_pb.ResultRejected(
                job_id=message.job_id,
                attempt_id=message.attempt_id,
                result_sha256=message.result_package.sha256,
                reason=stream_pb.RESULT_REJECT_REASON_REVISION_MISMATCH,
                safe_detail="execution revision mismatch",
                retryable=False,
                rejected_at=adapters.timestamp_from_epoch(time.time()),
            )
        await database.run_db(
            gateway_repository.record_result_ready,
            worker_id=worker_id,
            job_id=message.job_id,
            attempt_id=message.attempt_id,
            transfer_id=message.result_package.transfer_id,
            result_sha256=message.result_package.sha256,
            routing_plan_hash=message.routing_plan_hash,
            execution_revision=message.execution_revision,
            ready_at=_epoch(message.ready_at),
            settings=self.settings,
        )
        return self._result_outcome(attempt, result_sha256=message.result_package.sha256)

    async def pending_result_outcomes(
        self, worker_id: str
    ) -> list[stream_pb.ResultAck | stream_pb.ResultRejected]:
        rows = await database.run_db(
            gateway_repository.pending_result_notifications,
            worker_id,
            settings=self.settings,
        )
        result = []
        for row in rows:
            outcome = self._result_outcome(row, result_sha256=row["result_sha256"])
            if outcome is not None:
                result.append(outcome)
        return result

    def _result_outcome(
        self, attempt: dict[str, Any], *, result_sha256: str
    ) -> stream_pb.ResultAck | stream_pb.ResultRejected | None:
        state = str(attempt.get("state") or attempt.get("execution_state") or "")
        if state in {JobState.COMPLETED.value, JobState.SUPERSEDED_RESULT_RECEIVED.value} and attempt.get("retention_until"):
            validation = (
                stream_pb.RESULT_VALIDATION_STATUS_ACCEPTED
                if state == JobState.COMPLETED.value
                else stream_pb.RESULT_VALIDATION_STATUS_STORED_UNPUBLISHED
            )
            accepted = float(attempt.get("validated_at") or time.time())
            return stream_pb.ResultAck(
                job_id=attempt["job_id"],
                attempt_id=attempt["attempt_id"],
                result_sha256=str(attempt.get("result_package_hash") or result_sha256),
                validation_status=validation,
                accepted_at=adapters.timestamp_from_epoch(accepted),
                retention_starts_at=adapters.timestamp_from_epoch(accepted),
                retention_until=adapters.timestamp_from_epoch(float(attempt["retention_until"])),
            )
        if state == JobState.FAILED.value and attempt.get("error"):
            return stream_pb.ResultRejected(
                job_id=attempt["job_id"],
                attempt_id=attempt["attempt_id"],
                result_sha256=result_sha256,
                reason=stream_pb.RESULT_REJECT_REASON_OTHER,
                safe_detail="central result validation rejected",
                retryable=False,
                rejected_at=adapters.timestamp_from_epoch(time.time()),
            )
        return None

    async def _attempt(self, job_id: str, attempt_id: str, worker_id: str) -> dict[str, Any]:
        if not job_id or not attempt_id:
            raise DomainViolation("job/attempt identity missing")
        attempt = await database.run_db(
            repositories.get_attempt, attempt_id, settings=self.settings
        )
        if attempt is None or attempt.get("job_id") != job_id:
            raise DomainViolation("attempt not found", code=common_pb.ERROR_CODE_JOB_CONFLICT)
        if attempt.get("assigned_worker_id") != worker_id:
            raise DomainViolation("attempt belongs to another worker", code=common_pb.ERROR_CODE_UNAUTHORIZED)
        return attempt
