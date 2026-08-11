"""Async implementation of AgentStreamService.Connect."""
from __future__ import annotations

import asyncio
import logging
import math
import re
import time
import uuid
from contextlib import suppress
from typing import AsyncIterator

from backend.app.agent_gateway.config import GatewayConfig
from backend.app.agent_gateway.domain import DomainViolation, GatewayDomainAdapter
from backend.app.agent_gateway.metrics import GatewayMetrics
from backend.app.agent_gateway.registry import GatewayConnectionRegistry, GatewaySession
from backend.app.services.distributed_workers import database, gateway_repository
from contracts.agent_stream.v1 import adapters
from contracts.agent_stream.v1 import agent_stream_pb2 as stream_pb
from contracts.agent_stream.v1 import agent_stream_pb2_grpc as stream_grpc
from contracts.agent_stream.v1 import common_pb2 as common_pb


logger = logging.getLogger("agent-gateway")
_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_CLOCK_SKEW_SEC = 30 * 86400


class AgentStreamService(stream_grpc.AgentStreamServiceServicer):
    def __init__(
        self,
        *,
        config: GatewayConfig,
        domain: GatewayDomainAdapter,
        registry: GatewayConnectionRegistry,
        metrics: GatewayMetrics,
    ) -> None:
        self.config = config
        self.domain = domain
        self.registry = registry
        self.metrics = metrics
        self.draining = False

    async def Connect(self, request_iterator, context) -> AsyncIterator[stream_pb.CenterToAgent]:
        session: GatewaySession | None = None
        reader: asyncio.Task | None = None
        poller: asyncio.Task | None = None
        try:
            try:
                first = await request_iterator.__anext__()
            except StopAsyncIteration:
                self.metrics.inc("protocol_errors", labels={"reason": "missing_hello"})
                return
            if first.ByteSize() > self.config.max_inbound_message_bytes:
                yield self._bare_error(
                    first,
                    common_pb.ERROR_CODE_MESSAGE_TOO_LARGE,
                    "first control message too large",
                )
                return
            if first.WhichOneof("payload") != "hello":
                self.metrics.inc("protocol_errors", labels={"reason": "hello_required"})
                yield self._bare_error(
                    first, common_pb.ERROR_CODE_PROTOCOL_VIOLATION, "AgentHello must be first"
                )
                return
            if not _ID_RE.fullmatch(first.message_id) or first.stream_sequence < 1:
                yield self._bare_error(
                    first, common_pb.ERROR_CODE_INVALID_MESSAGE, "invalid hello envelope"
                )
                return
            hello = first.hello
            if not _ID_RE.fullmatch(hello.worker_id) or hello.worker_id != first.worker_id:
                self.metrics.inc("connection_rejects", labels={"reason": "identity"})
                yield self._bare_error(
                    first, common_pb.ERROR_CODE_UNAUTHORIZED, "invalid worker identity"
                )
                return
            if not _ID_RE.fullmatch(hello.worker_instance_id):
                yield self._bare_error(
                    first, common_pb.ERROR_CODE_UNAUTHORIZED, "invalid worker instance identity"
                )
                return
            try:
                version = adapters.negotiate_protocol(hello.supported_protocol_versions)
            except adapters.ContractViolation:
                self.metrics.inc("connection_rejects", labels={"reason": "version"})
                yield self._bare_error(
                    first,
                    common_pb.ERROR_CODE_PROTOCOL_VERSION_UNSUPPORTED,
                    "no supported protocol major",
                )
                return
            if first.protocol_version not in (0, version):
                yield self._bare_error(
                    first,
                    common_pb.ERROR_CODE_PROTOCOL_VERSION_UNSUPPORTED,
                    "envelope protocol version mismatch",
                )
                return
            connection_id = "gconn_" + uuid.uuid4().hex
            try:
                _, old_connection_id = await self.domain.accept_hello(
                    hello, connection_id=connection_id, protocol_version=version
                )
            except DomainViolation as exc:
                self.metrics.inc("connection_rejects", labels={"reason": "fence"})
                yield self._bare_error(first, exc.code, str(exc))
                return

            session = GatewaySession(
                worker_id=hello.worker_id,
                instance_id=hello.worker_instance_id,
                connection_id=connection_id,
                connection_epoch=int(hello.connection_epoch),
                protocol_version=version,
                max_slots=max(1, int(hello.max_slots or 1)),
                active_slots=len(hello.active_attempts),
                last_stream_sequence=int(first.stream_sequence),
                outbound=asyncio.Queue(maxsize=self.config.max_outbound_queue),
            )
            old = await self.registry.register(session)
            if old is not None:
                with suppress(asyncio.QueueFull):
                    old.enqueue(
                        self._response(
                            old,
                            error=common_pb.ErrorStatus(
                                code=common_pb.ERROR_CODE_STALE_CONNECTION,
                                safe_message="connection superseded by newer epoch",
                                retryable=True,
                            ),
                        )
                    )
                # Set closing only after the terminal fencing envelope has
                # entered the old stream's bounded queue.
                old.closing.set()
            if old_connection_id:
                logger.info(
                    "connection superseded",
                    extra={"worker_id": session.worker_id, "connection_id": old_connection_id},
                )
            hello_response = await self.domain.center_hello(connection_id=connection_id)
            hello_response.max_control_message_bytes = min(
                hello_response.max_control_message_bytes,
                self.config.max_outbound_message_bytes,
            )
            hello_response.max_events_per_batch = self.config.max_event_batch_count
            hello_response.max_unacked_event_window = self.config.max_unacked_event_window
            hello_response.resume_cursors.extend(
                await self.domain.resume_cursors(session.worker_id)
            )
            session.enqueue(self._response(session, correlation_id=first.correlation_id, hello=hello_response))
            self.metrics.inc("connections_total", labels={"protocol_version": version})
            logger.info(
                "stream ready",
                extra={"worker_id": session.worker_id, "connection_id": connection_id},
            )

            reader = asyncio.create_task(self._read_loop(request_iterator, session))
            poller = asyncio.create_task(self._outbound_loop(session))
            while True:
                if session.closing.is_set() and session.outbound.empty():
                    break
                if reader.done() and session.outbound.empty():
                    break
                try:
                    outgoing = await asyncio.wait_for(session.outbound.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue
                if outgoing.ByteSize() > self.config.max_outbound_message_bytes:
                    self.metrics.inc("protocol_errors", labels={"reason": "outbound_size"})
                    session.closing.set()
                    continue
                yield outgoing
        finally:
            for task in (reader, poller):
                if task is not None:
                    task.cancel()
            for task in (reader, poller):
                if task is not None:
                    with suppress(asyncio.CancelledError, Exception):
                        await task
            if session is not None:
                await database.run_db(
                    gateway_repository.disconnect_connection,
                    session.worker_id,
                    session.connection_id,
                    settings=self.domain.settings,
                )
                await self.registry.unregister(session)
                self.metrics.inc("stream_disconnects")
                logger.info(
                    "stream disconnected",
                    extra={"worker_id": session.worker_id, "connection_id": session.connection_id},
                )

    async def _read_loop(self, request_iterator, session: GatewaySession) -> None:
        try:
            async for envelope in request_iterator:
                if session.closing.is_set():
                    return
                if envelope.ByteSize() > self.config.max_inbound_message_bytes:
                    await self._error(session, envelope, common_pb.ERROR_CODE_MESSAGE_TOO_LARGE, "control message too large", close=True)
                    return
                if envelope.worker_id != session.worker_id:
                    await self._error(session, envelope, common_pb.ERROR_CODE_UNAUTHORIZED, "worker identity changed", close=True)
                    return
                if envelope.connection_id and envelope.connection_id != session.connection_id:
                    await self._error(session, envelope, common_pb.ERROR_CODE_STALE_CONNECTION, "connection identity mismatch", close=True)
                    return
                if envelope.protocol_version != session.protocol_version:
                    await self._error(session, envelope, common_pb.ERROR_CODE_PROTOCOL_VERSION_UNSUPPORTED, "protocol version changed", close=True)
                    return
                if envelope.stream_sequence <= session.last_stream_sequence:
                    await self._error(session, envelope, common_pb.ERROR_CODE_PROTOCOL_VIOLATION, "stream sequence is not increasing", close=True)
                    return
                session.last_stream_sequence = envelope.stream_sequence
                session.last_message_at = time.time()
                alive = await database.run_db(
                    gateway_repository.touch_connection,
                    session.worker_id,
                    session.connection_id,
                    settings=self.domain.settings,
                )
                if not alive:
                    await self._error(session, envelope, common_pb.ERROR_CODE_STALE_CONNECTION, "connection was superseded", close=True)
                    return
                try:
                    self._validate_message(session, envelope)
                    await self._dispatch(session, envelope)
                except DomainViolation as exc:
                    if "event sequence gap" in str(exc):
                        self.metrics.inc("event_gap_count")
                    await self._error(session, envelope, exc.code, str(exc), retryable=exc.retryable, close=exc.close_stream)
                    if exc.close_stream:
                        return
                except (ValueError, adapters.ContractViolation) as exc:
                    await self._error(session, envelope, common_pb.ERROR_CODE_INVALID_MESSAGE, str(exc)[:300])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "inbound dispatcher failed",
                extra={"worker_id": session.worker_id, "connection_id": session.connection_id},
            )
            await self._error(session, None, common_pb.ERROR_CODE_INTERNAL_SAFE, "inbound dispatcher failure", retryable=True, close=True)

    async def _dispatch(self, session: GatewaySession, envelope: stream_pb.AgentToCenter) -> None:
        kind = envelope.WhichOneof("payload")
        if kind in (None, "hello"):
            raise DomainViolation("message type not allowed after handshake", close_stream=True)
        if kind == "heartbeat":
            if envelope.heartbeat.worker_id != session.worker_id:
                raise DomainViolation("heartbeat worker identity mismatch", close_stream=True)
            await self.domain.heartbeat(session.worker_id, session.instance_id, envelope.heartbeat)
            session.last_heartbeat_at = time.time()
            session.active_slots = int(envelope.heartbeat.active_slots)
            session.max_slots = max(1, int(envelope.heartbeat.max_slots or session.max_slots))
            session.accepting_jobs = bool(envelope.heartbeat.accepting_jobs)
            await database.run_db(
                gateway_repository.touch_connection,
                session.worker_id,
                session.connection_id,
                heartbeat=True,
                settings=self.domain.settings,
            )
            self.metrics.inc("heartbeats_total")
        elif kind == "capabilities_changed":
            await self.domain.capabilities_changed(session.worker_id, session.instance_id, envelope.capabilities_changed)
        elif kind == "job_accept":
            if envelope.job_accept.worker_id != session.worker_id:
                raise DomainViolation("JobAccept worker identity mismatch", close_stream=True)
            await self.domain.accept_job(session.worker_id, envelope.job_accept)
            session.sent_offers.discard(envelope.job_accept.attempt_id)
            self.metrics.inc("job_accepts_total")
        elif kind == "job_decline":
            if envelope.job_decline.worker_id != session.worker_id:
                raise DomainViolation("JobDecline worker identity mismatch", close_stream=True)
            if envelope.job_decline.reason == stream_pb.JOB_DECLINE_REASON_UNSPECIFIED:
                raise DomainViolation("job decline reason unspecified")
            await self.domain.decline_job(session.worker_id, envelope.job_decline)
            session.sent_offers.discard(envelope.job_decline.attempt_id)
            self.metrics.inc("job_declines_total", labels={"reason": envelope.job_decline.reason})
        elif kind == "progress":
            await self.domain.progress(session.worker_id, envelope.progress)
        elif kind == "event_batch":
            if envelope.event_batch.worker_id != session.worker_id:
                raise DomainViolation("EventBatch worker identity mismatch", close_stream=True)
            if len(envelope.event_batch.events) > self.config.max_event_batch_count:
                raise DomainViolation("event batch exceeds configured count", code=common_pb.ERROR_CODE_MESSAGE_TOO_LARGE)
            ack = await self.domain.event_batch(session.worker_id, envelope.event_batch)
            self.metrics.inc("event_batches_total")
            if ack.skipped_duplicates:
                self.metrics.inc("event_duplicates_total", value=ack.skipped_duplicates)
            session.enqueue(self._response(session, correlation_id=envelope.correlation_id, event_ack=ack))
        elif kind == "job_status":
            await self.domain.job_status(session.worker_id, envelope.job_status)
        elif kind == "cancel_ack":
            await self.domain.cancel_ack(session.worker_id, envelope.cancel_ack)
            session.sent_commands.discard(envelope.cancel_ack.command_id)
        elif kind == "result_ready":
            outcome = await self.domain.result_ready(session.worker_id, envelope.result_ready)
            self.metrics.inc("result_ready_total")
            if isinstance(outcome, stream_pb.ResultAck):
                session.enqueue(self._response(session, correlation_id=envelope.correlation_id, result_ack=outcome))
                session.sent_results.add(outcome.attempt_id + ":" + outcome.result_sha256)
                self.metrics.inc("result_ack_total", labels={"result": outcome.validation_status})
            elif isinstance(outcome, stream_pb.ResultRejected):
                session.enqueue(self._response(session, correlation_id=envelope.correlation_id, result_rejected=outcome))
                self.metrics.inc("result_reject_total", labels={"reason": outcome.reason})
        elif kind == "error":
            logger.warning(
                "safe agent error",
                extra={"worker_id": session.worker_id, "connection_id": session.connection_id, "code": envelope.error.code},
            )
        else:
            raise DomainViolation("unsupported message type", close_stream=True)

    def _validate_message(
        self, session: GatewaySession, envelope: stream_pb.AgentToCenter
    ) -> None:
        """Semantic constraints which generated protobuf classes cannot express."""
        if not _ID_RE.fullmatch(envelope.message_id):
            raise DomainViolation("invalid message_id")
        if len(envelope.correlation_id.encode("utf-8")) > 256:
            raise DomainViolation("correlation_id exceeds bound")
        if envelope.HasField("sent_at"):
            sent_at = envelope.sent_at.seconds + envelope.sent_at.nanos / 1_000_000_000
            if abs(time.time() - sent_at) > _MAX_CLOCK_SKEW_SEC:
                raise DomainViolation("message timestamp outside allowed skew")
        kind = envelope.WhichOneof("payload")
        message = getattr(envelope, kind) if kind else None
        for field in ("job_id", "attempt_id"):
            if message is not None and hasattr(message, field):
                value = str(getattr(message, field))
                if not _ID_RE.fullmatch(value):
                    raise DomainViolation(f"invalid {field}")
        if kind == "progress":
            if not _ID_RE.fullmatch(message.stage_id):
                raise DomainViolation("invalid progress stage_id")
            if message.status == stream_pb.PROGRESS_STATUS_UNSPECIFIED:
                raise DomainViolation("progress status unspecified")
            if len(message.safe_message.encode("utf-8")) > adapters.MAX_SAFE_STRING_BYTES:
                raise DomainViolation(
                    "safe message exceeds bound",
                    code=common_pb.ERROR_CODE_MESSAGE_TOO_LARGE,
                )
            if message.total and message.current > message.total:
                raise DomainViolation("progress current exceeds total")
            if message.HasField("percent") and (
                not math.isfinite(message.percent) or not 0 <= message.percent <= 100
            ):
                raise DomainViolation("invalid progress percent")
        elif kind == "heartbeat":
            if message.max_slots < 1:
                raise DomainViolation("heartbeat max_slots must be positive")
            if message.worker_state == common_pb.WORKER_STATE_UNSPECIFIED:
                raise DomainViolation("heartbeat worker state unspecified")
        elif kind == "event_batch":
            if message.first_sequence < 1 or not message.events:
                raise DomainViolation("invalid event batch sequence/count")
            for event in message.events:
                if not _ID_RE.fullmatch(event.event_id):
                    raise DomainViolation("invalid event_id")
                if event.event_type == stream_pb.WORKER_EVENT_TYPE_UNSPECIFIED:
                    raise DomainViolation("event type unspecified")
                if event.schema_version < 1:
                    raise DomainViolation("event schema version missing")
        elif kind == "job_status" and message.state == common_pb.JOB_STATE_UNSPECIFIED:
            raise DomainViolation("job state unspecified")
        elif kind == "job_decline":
            if len(message.safe_detail.encode("utf-8")) > adapters.MAX_SAFE_STRING_BYTES:
                raise DomainViolation("decline detail exceeds bound")
        elif kind == "cancel_ack":
            if not _ID_RE.fullmatch(message.command_id):
                raise DomainViolation("invalid command_id")
            if message.stage == stream_pb.CANCEL_ACK_STAGE_UNSPECIFIED:
                raise DomainViolation("cancel ACK stage unspecified")
        elif kind == "result_ready":
            package = message.result_package
            if not _ID_RE.fullmatch(package.transfer_id):
                raise DomainViolation("invalid result transfer_id")
            if package.direction != common_pb.PACKAGE_DIRECTION_AGENT_TO_CENTER:
                raise DomainViolation("invalid result transfer direction")
            if package.protocol != common_pb.PACKAGE_TRANSFER_PROTOCOL_HTTPS_RESUMABLE_V1:
                raise DomainViolation("invalid result transfer protocol")
            if package.package_type != "result" or package.size_bytes < 1:
                raise DomainViolation("invalid result package descriptor")
            self._validate_sha256(package.sha256, "result sha256")
            self._validate_sha256(message.routing_plan_hash, "routing plan hash", optional=True)
        elif kind == "job_accept":
            self._validate_sha256(message.routing_plan_hash, "routing plan hash", optional=True)

    @staticmethod
    def _validate_sha256(value: str, label: str, *, optional: bool = False) -> None:
        if optional and not value:
            return
        if not _SHA256_RE.fullmatch(value):
            raise DomainViolation(f"invalid {label}")

    async def _outbound_loop(self, session: GatewaySession) -> None:
        while not session.closing.is_set():
            await asyncio.sleep(self.config.offer_poll_interval_sec)
            now = time.time()
            durable = await database.run_db(
                gateway_repository.get_transport_session,
                session.worker_id,
                settings=self.domain.settings,
            )
            if durable is None or durable.get("active_connection_id") != session.connection_id:
                session.closing.set()
                return
            if now - session.last_heartbeat_at > self.config.heartbeat_timeout_sec:
                await self._error(session, None, common_pb.ERROR_CODE_STALE_CONNECTION, "heartbeat timeout", retryable=True, close=True)
                return
            if now - session.last_message_at > self.config.idle_timeout_sec:
                await self._error(session, None, common_pb.ERROR_CODE_STALE_CONNECTION, "connection idle timeout", retryable=True, close=True)
                return
            try:
                await self.domain.recover_expired_offers()
                for offer in await self.domain.outstanding_offers(session.worker_id):
                    if offer.attempt_id not in session.sent_offers:
                        self._enqueue(session, self._response(session, job_offer=offer))
                        session.sent_offers.add(offer.attempt_id)
                        await self.domain.mark_offer_delivered(offer.attempt_id)
                        self.metrics.inc("job_offers_total")
                if session.free_slots > 0 and not self.draining:
                    offer = await self.domain.claim_offer(
                        worker_id=session.worker_id,
                        connection_id=session.connection_id,
                        free_slots=session.free_slots,
                        busy_slots=session.active_slots,
                        offer_timeout_sec=self.config.offer_timeout_sec,
                    )
                    if offer is not None:
                        self._enqueue(session, self._response(session, job_offer=offer))
                        session.sent_offers.add(offer.attempt_id)
                        await self.domain.mark_offer_delivered(offer.attempt_id)
                        self.metrics.inc("job_offers_total")
                for command in await self.domain.pending_cancel_commands(session.worker_id):
                    if command.command_id not in session.sent_commands:
                        self._enqueue(session, self._response(session, cancel=command))
                        session.sent_commands.add(command.command_id)
                        self.metrics.inc("cancel_commands_total")
                for outcome in await self.domain.pending_result_outcomes(session.worker_id):
                    key = outcome.attempt_id + ":" + outcome.result_sha256
                    if key in session.sent_results:
                        continue
                    if isinstance(outcome, stream_pb.ResultAck):
                        self._enqueue(session, self._response(session, result_ack=outcome))
                        self.metrics.inc("result_ack_total", labels={"result": outcome.validation_status})
                    else:
                        self._enqueue(session, self._response(session, result_rejected=outcome))
                        self.metrics.inc("result_reject_total", labels={"reason": outcome.reason})
                    session.sent_results.add(key)
            except asyncio.QueueFull:
                self.metrics.inc("queue_rejections")
                session.closing.set()
                return
            except Exception:
                logger.exception(
                    "outbound scheduler adapter failed",
                    extra={"worker_id": session.worker_id, "connection_id": session.connection_id},
                )
                await self._error(session, None, common_pb.ERROR_CODE_INTERNAL_SAFE, "outbound adapter failure", retryable=True)

    def _enqueue(self, session: GatewaySession, message: stream_pb.CenterToAgent) -> None:
        session.enqueue(message)

    async def _error(
        self,
        session: GatewaySession,
        request: stream_pb.AgentToCenter | None,
        code: int,
        safe_message: str,
        *,
        retryable: bool = False,
        close: bool = False,
    ) -> None:
        self.metrics.inc("protocol_errors", labels={"reason": code})
        message = self._response(
            session,
            correlation_id=(request.correlation_id if request is not None else ""),
            error=common_pb.ErrorStatus(
                code=code,
                safe_message=safe_message[:300],
                retryable=retryable,
                correlation_id=(request.correlation_id if request is not None else ""),
            ),
        )
        try:
            session.enqueue(message)
        except asyncio.QueueFull:
            self.metrics.inc("queue_rejections")
        if close:
            session.closing.set()

    def _response(self, session: GatewaySession, *, correlation_id: str = "", **payload) -> stream_pb.CenterToAgent:
        session.outbound_sequence += 1
        return stream_pb.CenterToAgent(
            protocol_version=session.protocol_version,
            message_id="gmsg_" + uuid.uuid4().hex,
            worker_id=session.worker_id,
            connection_id=session.connection_id,
            sent_at=adapters.timestamp_from_epoch(time.time()),
            stream_sequence=session.outbound_sequence,
            correlation_id=correlation_id,
            **payload,
        )

    def _bare_error(self, request: stream_pb.AgentToCenter, code: int, message: str) -> stream_pb.CenterToAgent:
        return stream_pb.CenterToAgent(
            protocol_version=1,
            message_id="gmsg_" + uuid.uuid4().hex,
            worker_id=request.worker_id,
            sent_at=adapters.timestamp_from_epoch(time.time()),
            correlation_id=request.correlation_id,
            error=common_pb.ErrorStatus(code=code, safe_message=message[:300], retryable=False),
        )

    async def drain(self) -> None:
        self.draining = True
        await self.registry.drain()
