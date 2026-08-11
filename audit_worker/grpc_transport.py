"""Worker-side Agent Stream v1 control transport.

The class deliberately presents the small CenterClient surface consumed by
WorkerAgent. Package bytes are delegated to the same verified HTTPS client;
only control messages use the long-lived gRPC stream.
"""
from __future__ import annotations

import queue
import re
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

import grpc

from audit_worker import PROTOCOL_VERSION, __version__
from audit_worker.client import CenterError, SequenceGapError, backoff_delays
from audit_worker.local_store import (
    LocalJobStore, WorkerStateStore, atomic_write_json, read_json,
)
from contracts.agent_stream.v1 import adapters
from contracts.agent_stream.v1 import agent_stream_pb2 as stream_pb
from contracts.agent_stream.v1 import agent_stream_pb2_grpc as stream_grpc
from contracts.agent_stream.v1 import common_pb2 as common_pb


class GrpcTransportError(RuntimeError):
    pass


class FatalGrpcTransportError(GrpcTransportError):
    pass


@dataclass
class _Outbound:
    kind: str
    message: Any
    correlation_id: str = ""


class GrpcStreamControlTransport:
    """Explicit, reconnecting gRPC control plane with durable client fencing."""

    def __init__(
        self,
        *,
        target: str,
        data_client: Any,
        state_store: WorkerStateStore,
        jobs: LocalJobStore,
        worker_id: str,
        instance_id: str,
        config: Any,
        build_heartbeat: Callable[[], dict[str, Any]],
    ) -> None:
        self.target = target
        self.data = data_client
        self.state_store = state_store
        self.jobs = jobs
        self.worker_id = worker_id
        self.instance_id = instance_id
        self.config = config
        self.build_heartbeat = build_heartbeat
        self._critical: queue.Queue[_Outbound] = queue.Queue(
            maxsize=int(config.grpc_outbound_queue_max)
        )
        self._offers: queue.Queue[dict[str, Any]] = queue.Queue(
            maxsize=max(2, int(config.max_slots) * 2)
        )
        self._commands: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=128)
        self._waiters: dict[str, queue.Queue[Any]] = {}
        self._waiters_lock = threading.Lock()
        self._latest_heartbeat: Any = None
        self._latest_capabilities: Any = None
        self._last_capabilities_sha = ""
        self._heartbeat_lock = threading.Lock()
        self._wake = threading.Event()
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._start_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._channel: grpc.Channel | None = None
        self._connection_id = ""
        self._stream_sequence = 0
        self._center_stream_sequence = 0
        self._sequence_lock = threading.Lock()
        self._assignments: dict[str, dict[str, Any]] = {}
        self._uploads: dict[str, dict[str, Any]] = {}
        self._cancel_identity: dict[str, dict[str, str]] = {}
        self._seen_offers: set[str] = set()
        self._accepting_jobs = True
        self._fatal_error: BaseException | None = None

    def _ensure_started(self) -> None:
        if self._fatal_error is not None:
            raise FatalGrpcTransportError(str(self._fatal_error)) from self._fatal_error
        with self._start_lock:
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._connection_loop, name="grpc-control", daemon=True
                )
                self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._channel is not None:
            self._channel.close()
        if self._thread is not None:
            self._thread.join(timeout=10)
        self.data.set_control_context(connection_id=None)
        self.data.close()

    def _connection_loop(self) -> None:
        delays = backoff_delays()
        while not self._stop.is_set():
            epoch = self.state_store.reserve_connection_epoch()
            self._ready.clear()
            self._connection_id = ""
            self._seen_offers.clear()
            self.data.set_control_context(connection_id=None)
            with self._sequence_lock:
                self._stream_sequence = 0
                self._center_stream_sequence = 0
            try:
                # 12C permits plaintext gRPC only on loopback; config validation
                # rejects every external target. HTTPS package TLS remains on.
                self._channel = grpc.insecure_channel(
                    self.target,
                    options=(
                        ("grpc.max_receive_message_length", 1024 * 1024),
                        ("grpc.max_send_message_length", 1024 * 1024),
                    ),
                )
                grpc.channel_ready_future(self._channel).result(
                    timeout=float(self.config.grpc_connect_timeout_sec)
                )
                stub = stream_grpc.AgentStreamServiceStub(self._channel)
                responses = stub.Connect(self._request_iterator(epoch), wait_for_ready=False)
                for response in responses:
                    if self._stop.is_set():
                        break
                    self._handle_response(response)
                if not self._stop.is_set():
                    raise GrpcTransportError("Agent Gateway stream ended")
                delays = backoff_delays()
            except FatalGrpcTransportError as exc:
                self._fatal_error = exc
                self._fail_waiters(exc)
                return
            except Exception as exc:  # noqa: BLE001 - reconnect is the contract
                self._fail_waiters(exc)
                if not self._stop.wait(next(delays)):
                    continue
            finally:
                self._ready.clear()
                self.data.set_control_context(connection_id=None)
                if self._channel is not None:
                    self._channel.close()
                    self._channel = None

    def _request_iterator(self, epoch: int):
        yield self._envelope(
            "hello", self._hello(epoch), correlation_id="hello_" + uuid.uuid4().hex
        )
        while not self._stop.is_set() and not self._ready.wait(0.1):
            pass
        while not self._stop.is_set() and self._ready.is_set():
            try:
                item = self._critical.get_nowait()
            except queue.Empty:
                item = None
            if item is not None:
                yield self._envelope(
                    item.kind, item.message, correlation_id=item.correlation_id
                )
                continue
            with self._heartbeat_lock:
                capabilities = self._latest_capabilities
                self._latest_capabilities = None
                heartbeat = self._latest_heartbeat
                self._latest_heartbeat = None
            if capabilities is not None:
                yield self._envelope("capabilities_changed", capabilities)
                continue
            if heartbeat is not None:
                heartbeat.connection_id = self._connection_id
                yield self._envelope("heartbeat", heartbeat)
                continue
            self._wake.wait(0.25)
            self._wake.clear()

    def _envelope(self, kind: str, payload: Any, *, correlation_id: str = ""):
        with self._sequence_lock:
            self._stream_sequence += 1
            sequence = self._stream_sequence
        message = stream_pb.AgentToCenter(
            protocol_version=PROTOCOL_VERSION,
            message_id="amsg_" + uuid.uuid4().hex,
            worker_id=self.worker_id,
            connection_id=self._connection_id,
            sent_at=adapters.timestamp_from_epoch(time.time()),
            stream_sequence=sequence,
            correlation_id=correlation_id,
        )
        getattr(message, kind).CopyFrom(payload)
        adapters.validate_control_message(message)
        return message

    def _hello(self, epoch: int) -> stream_pb.AgentHello:
        heartbeat = self.build_heartbeat()
        active_by_attempt = {
            str(item.get("attempt_id") or ""): dict(item)
            for item in heartbeat.get("active_jobs") or []
            if item.get("attempt_id")
        }
        # A freshly restarted Agent has not yet rebuilt its in-memory observer
        # map when the reconnect starts. Durable job metadata is therefore part
        # of Hello, otherwise an actually running Executor would be reported as
        # an empty session precisely during the recovery window.
        for meta in self.jobs.active():
            active_by_attempt.setdefault(
                meta["attempt_id"],
                {
                    "job_id": meta["job_id"],
                    "attempt_id": meta["attempt_id"],
                    "state": meta.get("local_state"),
                    "stage": meta.get("local_state"),
                    "started_at": meta.get("started_at"),
                    "result_ready": bool(meta.get("result_hash")),
                },
            )
        active = list(active_by_attempt.values())
        cursors = []
        for meta in self.jobs.iter_all():
            ack = read_json(
                self.jobs.job_dir(meta["job_id"], meta["attempt_id"])
                / "events" / "ack.json",
                {},
            ) or {}
            cursors.append(
                common_pb.EventCursor(
                    job_id=meta["job_id"],
                    attempt_id=meta["attempt_id"],
                    highest_contiguous_sequence=int(ack.get("last_acked_seq") or 0),
                )
            )
        return stream_pb.AgentHello(
            worker_id=self.worker_id,
            worker_instance_id=self.instance_id,
            supported_protocol_versions=list(self.config.grpc_protocol_versions),
            worker_software_version=__version__,
            execution_revision=str(self.config.pipeline_revision or ""),
            bootstrap_version=str(
                (self.config.extra_capabilities or {}).get("bootstrap_version") or "unknown"
            ),
            capabilities=adapters.capabilities_from_domain(
                self.config.capabilities(),
                provider_snapshots=heartbeat.get("providers") or [],
                accepting_jobs=heartbeat.get("worker_state") not in {"draining", "drained"},
            ),
            max_slots=int(self.config.max_slots),
            active_attempts=[adapters._attempt_from_domain(item) for item in active],
            event_cursors=cursors,
            connection_epoch=epoch,
            connection_nonce="nonce_" + uuid.uuid4().hex,
        )

    def _handle_response(self, response: stream_pb.CenterToAgent) -> None:
        adapters.validate_control_message(response)
        if response.protocol_version != PROTOCOL_VERSION:
            raise FatalGrpcTransportError("Center response protocol version changed")
        if response.worker_id != self.worker_id:
            raise FatalGrpcTransportError("Center response worker identity changed")
        with self._sequence_lock:
            if response.stream_sequence <= self._center_stream_sequence:
                raise FatalGrpcTransportError("Center stream sequence is not increasing")
            self._center_stream_sequence = response.stream_sequence
        kind = response.WhichOneof("payload")
        if kind == "hello":
            if response.hello.accepted_protocol_version != PROTOCOL_VERSION:
                raise GrpcTransportError("gateway selected unsupported protocol")
            self._connection_id = response.hello.connection_id
            required_revision = response.hello.required_execution_revision
            revision_ok = not (
                required_revision
                and required_revision != str(self.config.pipeline_revision or "")
            )
            required_policy_version = int(response.hello.required_policy_version or 0)
            required_policy_sha = response.hello.required_policy_sha256
            local_policy_version = int(
                (self.config.extra_capabilities or {}).get("provider_policy_version") or 0
            )
            local_policy_sha = str(
                (self.config.extra_capabilities or {}).get("provider_policy_sha256") or ""
            )
            policy_ok = not (
                (required_policy_version and required_policy_version != local_policy_version)
                or (required_policy_sha and required_policy_sha != local_policy_sha)
            )
            self._accepting_jobs = revision_ok and policy_ok
            for cursor in response.hello.resume_cursors:
                path = (
                    self.jobs.job_dir(cursor.job_id, cursor.attempt_id)
                    / "events" / "ack.json"
                )
                current = read_json(path, {}) or {}
                acknowledged = max(
                    int(current.get("last_acked_seq") or 0),
                    int(cursor.highest_contiguous_sequence or 0),
                )
                atomic_write_json(path, {"last_acked_seq": acknowledged})
            self.data.set_control_context(connection_id=self._connection_id)
            self._ready.set()
            self._wake.set()
            return
        if response.connection_id != self._connection_id:
            raise GrpcTransportError("stale gateway connection response")
        if kind == "job_offer":
            assignment = adapters.job_offer_to_domain(response.job_offer)
            assignment["execution_token"] = ""
            self._assignments[assignment["attempt_id"]] = assignment
            if assignment["attempt_id"] in self._seen_offers:
                return
            self._seen_offers.add(assignment["attempt_id"])
            try:
                self._offers.put_nowait(assignment)
            except queue.Full as exc:
                raise GrpcTransportError("bounded JobOffer queue is full") from exc
        elif kind == "cancel":
            command = {
                "command_id": response.cancel.command_id,
                "command_type": "cancel_attempt",
                "job_id": response.cancel.job_id,
                "attempt_id": response.cancel.attempt_id,
                "payload": {
                    "job_id": response.cancel.job_id,
                    "attempt_id": response.cancel.attempt_id,
                    "reason": response.cancel.safe_reason,
                },
            }
            self._cancel_identity[response.cancel.command_id] = {
                "job_id": response.cancel.job_id,
                "attempt_id": response.cancel.attempt_id,
            }
            try:
                self._commands.put_nowait(command)
            except queue.Full as exc:
                raise GrpcTransportError("bounded CancelCommand queue is full") from exc
        elif kind in {"event_ack", "result_ack", "result_rejected", "error"}:
            with self._waiters_lock:
                waiter = self._waiters.get(response.correlation_id)
            if waiter is not None:
                waiter.put(response)
        if kind == "error" and not response.error.retryable:
            raise FatalGrpcTransportError(response.error.safe_message)

    def _fail_waiters(self, exc: BaseException) -> None:
        with self._waiters_lock:
            waiters = list(self._waiters.values())
        for waiter in waiters:
            try:
                waiter.put_nowait(exc)
            except queue.Full:
                pass

    def _send(self, kind: str, message: Any, *, wait_response: bool = False) -> Any:
        self._ensure_started()
        if not self._ready.wait(float(self.config.grpc_connect_timeout_sec)):
            raise GrpcTransportError("Agent Gateway is not ready")
        correlation = ("corr_" + uuid.uuid4().hex) if wait_response else ""
        waiter: queue.Queue[Any] | None = queue.Queue(maxsize=1) if wait_response else None
        if waiter is not None:
            with self._waiters_lock:
                self._waiters[correlation] = waiter
        try:
            self._critical.put(
                _Outbound(kind, message, correlation),
                timeout=float(self.config.request_timeout_sec),
            )
            self._wake.set()
            if waiter is None:
                return {}
            outcome = waiter.get(timeout=float(self.config.request_timeout_sec))
            if isinstance(outcome, BaseException):
                raise GrpcTransportError(str(outcome)) from outcome
            return outcome
        finally:
            if waiter is not None:
                with self._waiters_lock:
                    self._waiters.pop(correlation, None)

    # CenterClient-compatible control surface ---------------------------------
    def heartbeat(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._ensure_started()
        heartbeat = adapters.heartbeat_from_http(
            payload, worker_id=self.worker_id, connection_id=self._connection_id
        )
        heartbeat.accepting_jobs = heartbeat.accepting_jobs and self._accepting_jobs
        capability = adapters.capabilities_from_domain(
            self.config.capabilities(),
            provider_snapshots=payload.get("providers") or [],
            accepting_jobs=heartbeat.accepting_jobs,
        )
        with self._heartbeat_lock:
            if self._last_capabilities_sha and capability.sha256 != self._last_capabilities_sha:
                self._latest_capabilities = stream_pb.CapabilitiesChanged(
                    capabilities=capability
                )
            self._last_capabilities_sha = capability.sha256
            self._latest_heartbeat = heartbeat  # coalesce: newest observation wins
        self._wake.set()
        return {}

    def next_job(self, payload: dict[str, Any], **_: Any) -> dict[str, Any] | None:
        self._ensure_started()
        if not self._ready.wait(float(self.config.grpc_connect_timeout_sec)):
            raise GrpcTransportError("Agent Gateway is not ready")
        if not self._accepting_jobs:
            return None
        try:
            return self._offers.get(timeout=max(0.1, float(payload.get("wait_sec") or 1)))
        except queue.Empty:
            return None

    def accept_job(self, job_id: str, payload: dict[str, Any], _token: str) -> dict[str, Any]:
        assignment = self._assignments.get(str(payload.get("attempt_id") or ""), {})
        params = assignment.get("params") or {}
        routing = params.get("routing_plan") or {}
        message = adapters.job_accept_from_http(
            payload,
            job_id=job_id,
            worker_id=self.worker_id,
            routing_plan_hash=str(routing.get("routing_plan_hash") or ""),
            execution_revision=str(self.config.pipeline_revision or ""),
        )
        return self._send("job_accept", message)

    def reject_job(self, job_id: str, payload: dict[str, Any], _token: str) -> dict[str, Any]:
        return self._send(
            "job_decline",
            adapters.job_decline_from_http(payload, job_id=job_id, worker_id=self.worker_id),
        )

    def post_events(
        self, job_id: str, attempt_id: str, first_seq: int,
        events: list[dict[str, Any]], _token: str,
    ) -> dict[str, Any]:
        response = self._send(
            "event_batch",
            adapters.event_batch_from_http(
                {"job_id": job_id, "attempt_id": attempt_id,
                 "first_seq": first_seq, "events": events},
                worker_id=self.worker_id,
            ),
            wait_response=True,
        )
        if response.WhichOneof("payload") == "error":
            match = re.search(r"(?:expected|sequence)\D+(\d+)", response.error.safe_message)
            if match:
                raise SequenceGapError(int(match.group(1)), {"detail": response.error.safe_message})
            raise CenterError(409, response.error.safe_message)
        return adapters.event_ack_to_http(response.event_ack)

    def get_commands(self) -> dict[str, Any]:
        commands = []
        while True:
            try:
                commands.append(self._commands.get_nowait())
            except queue.Empty:
                break
        return {"commands": commands}

    def ack_command(self, command_id: str, result: dict[str, Any]) -> dict[str, Any]:
        detail = result.get("detail") or {}
        outcome = str(detail.get("outcome") or "")
        if result.get("status") != "ok":
            stage = stream_pb.CANCEL_ACK_STAGE_REJECTED
        elif "already" in outcome and "completed" in outcome:
            stage = stream_pb.CANCEL_ACK_STAGE_ALREADY_FINISHED
        elif "not_running" in outcome:
            stage = stream_pb.CANCEL_ACK_STAGE_NOT_FOUND
        else:
            stage = stream_pb.CANCEL_ACK_STAGE_CANCELLED
        # command identity is retained in a small side map when delivered.
        identity = self._cancel_identity.get(command_id, {})
        message = stream_pb.CancelAck(
            command_id=command_id,
            job_id=str(identity.get("job_id") or ""),
            attempt_id=str(identity.get("attempt_id") or ""),
            stage=stage,
            safe_detail=outcome[:500],
            acknowledged_at=adapters.timestamp_from_epoch(time.time()),
        )
        response = self._send("cancel_ack", message)
        self._cancel_identity.pop(command_id, None)
        return response

    def reconcile(self, _payload: dict[str, Any]) -> dict[str, Any]:
        # AgentHello carries real active attempts and cursors. 12B has no
        # request/response reconcile message; emitting HTTP control here would
        # violate single-owner transport semantics.
        self._ensure_started()
        return {"jobs": [], "transport": "grpc_stream"}

    # HTTPS package plane ------------------------------------------------------
    def download_source(self, job_id: str, dest: Any, _token: str, **kwargs: Any) -> int:
        assignment = next(
            (x for x in self._assignments.values() if x.get("job_id") == job_id), {}
        )
        return self.data.download_source(
            job_id, dest, "", transfer_id=str((assignment.get("package") or {}).get("package_id") or ""),
            **kwargs,
        )

    def create_upload(self, payload: dict[str, Any], _token: str) -> dict[str, Any]:
        response = self.data.create_upload(payload, "")
        self._uploads[response["upload_id"]] = dict(payload)
        return response

    def get_upload(self, upload_id: str) -> dict[str, Any]:
        return self.data.get_upload(upload_id)

    def put_chunk(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        kwargs["execution_token"] = ""
        return self.data.put_chunk(*args, **kwargs)

    def complete_upload(self, upload_id: str, payload: dict[str, Any], _token: str) -> dict[str, Any]:
        # HTTPS validates and stores the bytes first. Retention authority is
        # intentionally the subsequent stream ResultAck, never this HTTP reply.
        self.data.complete_upload(upload_id, payload, "")
        meta = self._uploads.get(upload_id, {})
        assignment = self._assignments.get(str(payload.get("attempt_id") or ""), {})
        params = assignment.get("params") or {}
        routing = params.get("routing_plan") or {}
        response = self._send(
            "result_ready",
            adapters.result_ready_from_domain(
                {
                    "job_id": payload.get("job_id"),
                    "attempt_id": payload.get("attempt_id"),
                    "upload_id": upload_id,
                    "package_type": "result",
                    "expected_size": meta.get("expected_size") or payload.get("total_size"),
                    "expected_hash": meta.get("expected_hash") or payload.get("sha256"),
                    "compression": meta.get("compression") or "gzip",
                    "manifest_version": meta.get("manifest_version") or 1,
                    "routing_plan_hash": routing.get("routing_plan_hash") or "",
                    "pipeline_revision": self.config.pipeline_revision or "",
                    "ready_at": time.time(),
                }
            ),
            wait_response=True,
        )
        kind = response.WhichOneof("payload")
        if kind == "result_rejected":
            raise CenterError(422, response.result_rejected.safe_detail)
        if kind == "error":
            raise CenterError(409, response.error.safe_message)
        return adapters.result_ack_to_http(response.result_ack)

    def update_registration(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise GrpcTransportError("registration update is not part of Agent Stream v1")
