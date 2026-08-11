from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from pathlib import Path

import grpc
import pytest
import pytest_asyncio
from grpc_health.v1 import health_pb2, health_pb2_grpc

from backend.app.agent_gateway.config import GatewayConfig, GatewayConfigError
from backend.app.agent_gateway.metrics import GatewayMetrics
from backend.app.agent_gateway.server import GatewayServer, SERVICE_NAME
from backend.app.models.distributed_workers import (
    JobState,
    TestJobParams as WorkerTestJobParams,
    WorkerCommandType,
)
from backend.app.services.distributed_workers import (
    database,
    gateway_repository,
    job_service,
    repositories,
)
from backend.app.services.distributed_workers.settings import get_settings
from contracts.agent_stream.v1 import adapters
from contracts.agent_stream.v1 import agent_stream_pb2 as stream_pb
from contracts.agent_stream.v1 import agent_stream_pb2_grpc as stream_grpc
from contracts.agent_stream.v1 import common_pb2 as common_pb


SHA = "a" * 64


@pytest.fixture()
def gateway_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", "test-secret-" + "x" * 24)
    database.reset_state_for_tests()
    settings = get_settings()
    database.ensure_ready(settings)
    yield settings
    database.reset_state_for_tests()


def make_worker(settings, *, instance_id=None, slots=2):
    instance_id = instance_id or "fake-instance-" + uuid.uuid4().hex[:8]
    worker = repositories.create_worker(
        display_name="fake gateway worker",
        instance_id=instance_id,
        worker_version="12b.test",
        protocol_version=1,
        pipeline_revision="rev-test",
        capabilities={
            "job_types": ["test_pipeline_v1"],
            "compressions": ["gzip"],
            "provider_capabilities": {"codex": ["cheap_review"]},
        },
        configured_max_slots=slots,
        settings=settings,
    )
    repositories.update_worker_fields(
        worker["worker_id"],
        {
            "registration_status": "approved",
            "worker_state": "idle",
            "configured_max_slots": slots,
            "max_verified_slots": slots,
            "calculated_free_slots": slots,
        },
        settings=settings,
    )
    return repositories.get_worker(worker["worker_id"], settings=settings)


def make_job(settings, worker_id, *, label=None):
    return job_service.create_test_job(
        worker_id=worker_id,
        project_id="project-" + uuid.uuid4().hex[:10],
        version_id=None,
        params=WorkerTestJobParams(label=label or "gateway", steps=2, step_seconds=0),
        actor="operator:test",
        settings=settings,
    )


def capabilities(revision="caps-1"):
    return common_pb.CapabilitySnapshot(
        revision=revision,
        sha256="caps-sha",
        provider_policy_version=1,
        provider_policy_sha256="policy-sha",
        job_types=["test_pipeline_v1"],
        compressions=["gzip"],
        providers=[
            common_pb.ProviderCapabilitySnapshot(
                provider="codex",
                capabilities=["cheap_review"],
                availability=common_pb.PROVIDER_AVAILABILITY_AVAILABLE,
                safe_status="ready",
            )
        ],
        accepting_jobs=True,
        max_verified_slots=2,
    )


class FakeAgent:
    def __init__(self, worker, port):
        self.worker = worker
        self.channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        self.call = stream_grpc.AgentStreamServiceStub(self.channel).Connect()
        self.sequence = 0
        self.connection_id = ""

    async def connect(self, epoch=1, *, versions=(1,), max_slots=2, active_attempts=()):
        self.sequence = 1
        await self.call.write(
            stream_pb.AgentToCenter(
                protocol_version=1,
                message_id="hello-" + uuid.uuid4().hex,
                worker_id=self.worker["worker_id"],
                stream_sequence=self.sequence,
                hello=stream_pb.AgentHello(
                    worker_id=self.worker["worker_id"],
                    worker_instance_id=self.worker["instance_id"],
                    supported_protocol_versions=list(versions),
                    worker_software_version="12b.test",
                    execution_revision="rev-test",
                    capabilities=capabilities(),
                    max_slots=max_slots,
                    active_attempts=list(active_attempts),
                    connection_epoch=epoch,
                    connection_nonce="nonce-" + uuid.uuid4().hex,
                ),
            )
        )
        response = await asyncio.wait_for(self.call.read(), 3)
        if response.WhichOneof("payload") == "hello":
            self.connection_id = response.connection_id
        return response

    async def send(self, field, message, *, correlation_id=""):
        self.sequence += 1
        envelope = stream_pb.AgentToCenter(
            protocol_version=1,
            message_id="msg-" + uuid.uuid4().hex,
            worker_id=self.worker["worker_id"],
            connection_id=self.connection_id,
            stream_sequence=self.sequence,
            correlation_id=correlation_id,
            **{field: message},
        )
        await self.call.write(envelope)

    async def read(self, timeout=3):
        return await asyncio.wait_for(self.call.read(), timeout)

    async def read_until(self, kind, timeout=3):
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError(kind)
            item = await self.read(remaining)
            if item is grpc.aio.EOF:
                raise EOFError(kind)
            if item.WhichOneof("payload") == kind:
                return item

    async def close(self):
        try:
            if not self.call.done():
                await self.call.done_writing()
                while await asyncio.wait_for(self.call.read(), 1) is not grpc.aio.EOF:
                    pass
        except (asyncio.CancelledError, asyncio.TimeoutError, grpc.aio.AioRpcError):
            self.call.cancel()
        try:
            await self.call.code()
        except asyncio.CancelledError:
            pass
        await self.channel.close()
        # Let the server-side async generator observe cancellation before the
        # per-test event loop is torn down.
        await asyncio.sleep(0)


@pytest_asyncio.fixture()
async def running_gateway(gateway_env):
    config = GatewayConfig(
        heartbeat_timeout_sec=10,
        idle_timeout_sec=15,
        offer_timeout_sec=1.0,
        offer_poll_interval_sec=0.02,
        graceful_shutdown_sec=0.2,
    ).validated()
    server = GatewayServer(config, worker_settings=gateway_env)
    port = await server.start()
    assert server.config.host == "127.0.0.1" and port != 8443
    yield server, port
    await server.stop()


def test_a_gateway_imports_and_generated_service_stub():
    assert GatewayServer and stream_grpc.AgentStreamServiceStub
    assert stream_pb.DESCRIPTOR.services_by_name["AgentStreamService"]


@pytest.mark.parametrize(
    "config",
    [
        GatewayConfig(host="0.0.0.0"),
        GatewayConfig(host="8.8.8.8"),
        GatewayConfig(host="127.0.0.1", port=8443),
        GatewayConfig(host="127.0.0.1", environment="production"),
    ],
)
def test_b_d_insecure_public_or_production_bind_rejected(config):
    with pytest.raises(GatewayConfigError):
        config.validated()


@pytest.mark.parametrize("host", ["127.0.0.1", "::1"])
def test_c_insecure_loopback_allowed(host):
    assert GatewayConfig(host=host).validated().host == host


def test_config_bounds_and_mtls_deferred():
    with pytest.raises(GatewayConfigError):
        GatewayConfig(max_outbound_queue=0).validated()
    with pytest.raises(GatewayConfigError, match="12D"):
        GatewayConfig(security_mode="mtls").validated()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("AGENT_GATEWAY_PROTOCOL_VERSIONS", "one"),
        ("AGENT_GATEWAY_HEALTH_ENABLED", "perhaps"),
        ("AGENT_GATEWAY_METRICS_ENABLED", "maybe"),
    ],
)
def test_config_environment_boundary_is_typed_and_fail_closed(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(GatewayConfigError):
        GatewayConfig.from_env()


@pytest.mark.parametrize(
    "config",
    [
        GatewayConfig(environment="prodution"),
        GatewayConfig(health_enabled=False),
        GatewayConfig(metrics_enabled=False),
        GatewayConfig(reflection_enabled=True),
        GatewayConfig(log_level="TRACE"),
    ],
)
def test_config_rejects_unsupported_or_incomplete_runtime_modes(config):
    with pytest.raises(GatewayConfigError):
        config.validated()


@pytest.mark.asyncio
async def test_e_g_real_local_socket_hello_and_version_negotiation(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    agent = FakeAgent(worker, port)
    response = await agent.connect(epoch=1)
    assert response.WhichOneof("payload") == "hello"
    assert response.hello.accepted_protocol_version == 1
    assert response.hello.duplicate_connection_policy == stream_pb.DUPLICATE_CONNECTION_POLICY_NEWER_EPOCH_SUPERSEDES
    await agent.close()


@pytest.mark.asyncio
async def test_f_hello_required_first(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    call = stream_grpc.AgentStreamServiceStub(channel).Connect()
    await call.write(stream_pb.AgentToCenter(protocol_version=1, worker_id=worker["worker_id"], stream_sequence=1, heartbeat=stream_pb.Heartbeat(worker_id=worker["worker_id"])))
    response = await asyncio.wait_for(call.read(), 3)
    assert response.error.code == common_pb.ERROR_CODE_PROTOCOL_VIOLATION
    call.cancel()
    await channel.close()


@pytest.mark.asyncio
async def test_h_unsupported_version_rejected(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    agent = FakeAgent(worker, port)
    response = await agent.connect(epoch=1, versions=(2,))
    assert response.error.code == common_pb.ERROR_CODE_PROTOCOL_VERSION_UNSUPPORTED
    await agent.close()


@pytest.mark.asyncio
async def test_i_worker_identity_rejected(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    agent = FakeAgent(worker, port)
    agent.worker = {**worker, "worker_id": "unknown-worker"}
    response = await agent.connect(epoch=1)
    assert response.error.code in {common_pb.ERROR_CODE_UNAUTHORIZED, common_pb.ERROR_CODE_STALE_CONNECTION}
    await agent.close()


@pytest.mark.asyncio
async def test_j_n_epoch_persists_and_rejects_equal_lower_after_restart(gateway_env):
    worker = make_worker(gateway_env)
    first_server = GatewayServer(GatewayConfig(), worker_settings=gateway_env)
    port = await first_server.start()
    first = FakeAgent(worker, port)
    assert (await first.connect(epoch=10)).WhichOneof("payload") == "hello"
    await first.close()
    await first_server.stop(0)
    second_server = GatewayServer(GatewayConfig(), worker_settings=gateway_env)
    port2 = await second_server.start()
    for epoch in (10, 9):
        agent = FakeAgent(worker, port2)
        assert (await agent.connect(epoch=epoch)).error.code == common_pb.ERROR_CODE_STALE_CONNECTION
        await agent.close()
    newer = FakeAgent(worker, port2)
    assert (await newer.connect(epoch=11)).WhichOneof("payload") == "hello"
    session = gateway_repository.get_transport_session(worker["worker_id"], settings=gateway_env)
    assert session["last_connection_epoch"] == 11
    await newer.close()
    await second_server.stop(0)


@pytest.mark.asyncio
async def test_k_greater_epoch_supersedes_old_stream(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    old = FakeAgent(worker, port)
    await old.connect(epoch=20)
    new = FakeAgent(worker, port)
    assert (await new.connect(epoch=21)).WhichOneof("payload") == "hello"
    response = await old.read_until("error")
    assert response.error.code == common_pb.ERROR_CODE_STALE_CONNECTION
    await old.close()
    await new.close()


@pytest.mark.asyncio
async def test_o_center_hello_limits_and_resume_cursor(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    repositories.apply_event_batch(job_id=job["job_id"], attempt_id=job["attempt_id"], worker_id=worker["worker_id"], events=[], advance_to=7, settings=gateway_env)
    agent = FakeAgent(worker, port)
    response = await agent.connect(epoch=1)
    assert response.hello.max_control_message_bytes == 1024 * 1024
    assert response.hello.resume_cursors[0].highest_contiguous_sequence == 7
    await agent.close()


@pytest.mark.asyncio
async def test_p_q_heartbeat_and_capabilities_use_existing_domain(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    await agent.send("heartbeat", stream_pb.Heartbeat(worker_id=worker["worker_id"], connection_id=agent.connection_id, observed_at=adapters.timestamp_from_epoch(time.time()), worker_state=common_pb.WORKER_STATE_BUSY, active_slots=1, max_slots=2, active_attempts=[], resources=common_pb.ResourceSummary(executor_status="online"), accepting_jobs=True))
    await asyncio.sleep(0.1)
    fresh = repositories.get_worker(worker["worker_id"], settings=gateway_env)
    assert fresh["worker_state"] == "busy" and fresh["connection_status"] == "online"
    await agent.send("capabilities_changed", stream_pb.CapabilitiesChanged(capabilities=capabilities("caps-2")))
    await asyncio.sleep(0.1)
    fresh = repositories.get_worker(worker["worker_id"], settings=gateway_env)
    assert json.loads(fresh["capabilities"])["job_types"] == ["test_pipeline_v1"]
    await agent.close()


@pytest.mark.asyncio
async def test_r_u_scheduler_atomic_offer_and_lost_delivery_recovery(running_gateway, gateway_env):
    server, port = running_gateway
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    offer_response = await agent.read_until("job_offer")
    offer = offer_response.job_offer
    stored = repositories.get_attempt(job["attempt_id"], settings=gateway_env)
    assert stored["state"] == "source_uploading"
    offer_row = gateway_repository.pending_offers(worker["worker_id"], settings=gateway_env)[0]
    assert offer_row["attempt_id"] == offer.attempt_id and offer_row["delivered_at"]
    # Disconnect before accept: expiry recovers the same authoritative attempt.
    await agent.close()
    await asyncio.sleep(1.1)
    assert gateway_repository.recover_expired_offers(settings=gateway_env) == 1
    assert repositories.get_attempt(job["attempt_id"], settings=gateway_env)["state"] == "assigned"


@pytest.mark.asyncio
async def test_v_x_job_accept_duplicate_and_wrong_worker(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    other = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    offer = (await agent.read_until("job_offer")).job_offer
    accept = stream_pb.JobAccept(job_id=offer.job_id, attempt_id=offer.attempt_id, worker_id=worker["worker_id"], execution_revision="", accepted_at=adapters.timestamp_from_epoch(time.time()), source_sha256_verified=True, source_manifest_version=1)
    await agent.send("job_accept", accept)
    await asyncio.sleep(0.1)
    assert repositories.get_attempt(job["attempt_id"], settings=gateway_env)["state"] == "accepted_by_worker"
    await agent.send("job_accept", accept)
    await asyncio.sleep(0.1)
    assert repositories.get_attempt(job["attempt_id"], settings=gateway_env)["state"] == "accepted_by_worker"
    attacker = FakeAgent(other, port)
    await attacker.connect(epoch=1)
    await attacker.send("job_accept", stream_pb.JobAccept(job_id=job["job_id"], attempt_id=job["attempt_id"], worker_id=other["worker_id"], source_sha256_verified=True))
    error = await attacker.read_until("error")
    assert error.error.code == common_pb.ERROR_CODE_UNAUTHORIZED
    await attacker.close()
    await agent.close()


@pytest.mark.asyncio
async def test_y_z_job_decline_typed_and_requeues_temporary(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    offer = (await agent.read_until("job_offer")).job_offer
    await agent.send("job_decline", stream_pb.JobDecline(job_id=offer.job_id, attempt_id=offer.attempt_id, worker_id=worker["worker_id"], reason=stream_pb.JOB_DECLINE_REASON_NO_SLOT, safe_detail="temporary"))
    await asyncio.sleep(0.1)
    assert repositories.get_attempt(job["attempt_id"], settings=gateway_env)["state"] in {"assigned", "source_uploading"}
    await agent.close()


@pytest.mark.asyncio
async def test_aa_no_slot_means_no_offer(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env, slots=1)
    make_job(gateway_env, worker["worker_id"])
    active = common_pb.AttemptRef(job_id="active", attempt_id="active-attempt")
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1, max_slots=1, active_attempts=(active,))
    with pytest.raises(asyncio.TimeoutError):
        await agent.read(0.2)
    await agent.close()


@pytest.mark.asyncio
async def test_ab_af_event_ingest_duplicate_gap_ack_and_resume(running_gateway, gateway_env):
    server, port = running_gateway
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    await agent.read_until("job_offer")
    batch = adapters.event_batch_from_http({"job_id": job["job_id"], "attempt_id": job["attempt_id"], "first_seq": 1, "events": [{"seq": 1, "event_id": "e1", "event_type": "resource_warning", "occurred_at": time.time(), "payload": {"code": "disk"}}]}, worker_id=worker["worker_id"])
    await agent.send("event_batch", batch, correlation_id="events-1")
    ack = (await agent.read_until("event_ack")).event_ack
    assert ack.highest_contiguous_sequence == 1 and ack.accepted == 1
    await agent.send("event_batch", batch)
    duplicate = (await agent.read_until("event_ack")).event_ack
    assert duplicate.skipped_duplicates == 1 and duplicate.highest_contiguous_sequence == 1
    gap = adapters.event_batch_from_http({"job_id": job["job_id"], "attempt_id": job["attempt_id"], "first_seq": 3, "events": [{"seq": 3, "event_id": "e3", "event_type": "resource_warning", "occurred_at": time.time(), "payload": {}}]}, worker_id=worker["worker_id"])
    await agent.send("event_batch", gap)
    error = await agent.read_until("error")
    assert error.error.retryable and "expected 2" in error.error.safe_message
    await agent.close()
    reconnect = FakeAgent(worker, port)
    hello = await reconnect.connect(epoch=2)
    assert hello.hello.resume_cursors[0].highest_contiguous_sequence == 1
    assert server.metrics.snapshot()["event_duplicates_total"] >= 1
    assert server.metrics.snapshot()["event_gap_count"] >= 1
    await reconnect.close()


@pytest.mark.asyncio
async def test_ag_ah_progress_and_invalid_state_transition(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    await agent.read_until("job_offer")
    await agent.send("progress", adapters.progress_from_http({"job_id": "job_wrong", "attempt_id": job["attempt_id"], "stage_id": "stage", "status": "running", "current": 1, "total": 2, "observed_at": time.time()}))
    wrong_job = await agent.read_until("error")
    assert wrong_job.error.code == common_pb.ERROR_CODE_JOB_CONFLICT
    await agent.send("progress", adapters.progress_from_http({"job_id": job["job_id"], "attempt_id": job["attempt_id"], "stage_id": "stage", "status": "running", "current": 1, "total": 2, "observed_at": time.time()}))
    await asyncio.sleep(0.1)
    assert json.loads(repositories.get_attempt(job["attempt_id"], settings=gateway_env)["progress_snapshot"])["current"] == 1
    await agent.send("job_status", stream_pb.JobStatusUpdate(job_id=job["job_id"], attempt_id=job["attempt_id"], state=common_pb.JOB_STATE_COMPLETED))
    error = await agent.read_until("error")
    assert error.error.code == common_pb.ERROR_CODE_JOB_CONFLICT
    await agent.close()


@pytest.mark.asyncio
async def test_malformed_semantics_are_typed_and_do_not_crash_gateway(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    await agent.read_until("job_offer")

    await agent.send(
        "progress",
        stream_pb.ProgressUpdate(
            job_id=job["job_id"],
            attempt_id=job["attempt_id"],
            stage_id="stage",
            status=999,
        ),
    )
    assert (await agent.read_until("error")).error.code == common_pb.ERROR_CODE_INVALID_MESSAGE

    await agent.send(
        "progress",
        stream_pb.ProgressUpdate(
            job_id=job["job_id"],
            attempt_id=job["attempt_id"],
            stage_id="stage",
            status=stream_pb.PROGRESS_STATUS_RUNNING,
            safe_message="x" * (adapters.MAX_SAFE_STRING_BYTES + 1),
        ),
    )
    assert (await agent.read_until("error")).error.code == common_pb.ERROR_CODE_MESSAGE_TOO_LARGE

    agent.sequence -= 1
    await agent.send(
        "heartbeat",
        stream_pb.Heartbeat(worker_id=worker["worker_id"]),
    )
    duplicate = await agent.read_until("error")
    assert duplicate.error.code == common_pb.ERROR_CODE_PROTOCOL_VIOLATION
    await agent.close()


@pytest.mark.asyncio
async def test_ai_al_cancel_delivery_duplicate_ack_and_restart(running_gateway, gateway_env):
    server, port = running_gateway
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    repositories.update_attempt_fields(job["attempt_id"], {"state": "cancel_requested"}, settings=gateway_env)
    command = repositories.enqueue_command(worker_id=worker["worker_id"], command_type=WorkerCommandType.CANCEL_ATTEMPT.value, payload={"job_id": job["job_id"], "attempt_id": job["attempt_id"], "reason": "operator", "grace_period_sec": 10}, idempotency_key="cancel-test", job_id=job["job_id"], attempt_id=job["attempt_id"], settings=gateway_env)
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    cancel = (await agent.read_until("cancel")).cancel
    assert cancel.command_id == command["command_id"]
    await agent.close()
    reconnect = FakeAgent(worker, port)
    await reconnect.connect(epoch=2)
    assert (await reconnect.read_until("cancel")).cancel.command_id == command["command_id"]
    ack = stream_pb.CancelAck(command_id=command["command_id"], job_id=job["job_id"], attempt_id=job["attempt_id"], stage=stream_pb.CANCEL_ACK_STAGE_CANCELLED, acknowledged_at=adapters.timestamp_from_epoch(time.time()))
    await reconnect.send("cancel_ack", ack)
    await asyncio.sleep(0.1)
    assert repositories.get_attempt(job["attempt_id"], settings=gateway_env)["state"] == "cancelled"
    await reconnect.close()


def _create_upload(settings, job, upload_id="upload-test", sha=SHA):
    return repositories.create_upload_session(upload_id=upload_id, job_id=job["job_id"], attempt_id=job["attempt_id"], package_type="result", expected_size=100, chunk_size=100, expected_hash=sha, ttl_sec=3600, settings=settings)


@pytest.mark.asyncio
async def test_am_as_result_ready_data_plane_auth_and_validation_gate(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    other = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    _create_upload(gateway_env, job)
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    await agent.read_until("job_offer")
    ready = stream_pb.ResultReady(job_id=job["job_id"], attempt_id=job["attempt_id"], result_package=common_pb.PackageTransferDescriptor(transfer_id="upload-test", direction=common_pb.PACKAGE_DIRECTION_AGENT_TO_CENTER, protocol=common_pb.PACKAGE_TRANSFER_PROTOCOL_HTTPS_RESUMABLE_V1, package_type="result", size_bytes=100, sha256=SHA), execution_revision="rev-test", stage_status_summary=adapters.canonical_json_message({}, schema="stages", schema_version=1), provider_action_ledger_summary=adapters.canonical_json_message({}, schema="ledger", schema_version=1), ready_at=adapters.timestamp_from_epoch(time.time()))
    await agent.send("result_ready", ready)
    # No ACK before central validation.
    with pytest.raises(asyncio.TimeoutError):
        await agent.read(0.2)
    assert gateway_repository.authorize_transfer(transfer_id="upload-test", worker_id=worker["worker_id"], job_id=job["job_id"], attempt_id=job["attempt_id"], direction="agent_to_center", settings=gateway_env)
    assert not gateway_repository.authorize_transfer(transfer_id="upload-test", worker_id=other["worker_id"], job_id=job["job_id"], attempt_id=job["attempt_id"], direction="agent_to_center", settings=gateway_env)
    await agent.close()


@pytest.mark.asyncio
async def test_aq_av_real_validation_lost_ack_resends_and_retention_persisted(
    running_gateway, gateway_env, tmp_path
):
    from audit_worker import package_io

    _, port = running_gateway
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    job_dir = tmp_path / "fake-agent-job"
    (job_dir / "result").mkdir(parents=True)
    (job_dir / "work").mkdir(parents=True)
    (job_dir / "result" / "summary.json").write_text('{"status":"ok"}', encoding="utf-8")
    (job_dir / "result" / "run_log.txt").write_text("ok\n", encoding="utf-8")
    archive = tmp_path / "fake-result.tar.gz"
    manifest = package_io.build_result_package(
        dest_path=archive,
        job_dir=job_dir,
        job_id=job["job_id"],
        attempt_id=job["attempt_id"],
        project_id=job["project_id"],
        version_id=job.get("version_id"),
        worker_id=worker["worker_id"],
        worker_version="12b.test",
        protocol_version=1,
        manifest_version=1,
        source_package_hash="sha256:" + SHA,
        exit_code=0,
    )
    result_sha = manifest["archive"]["sha256"]
    _create_upload(gateway_env, job, sha=result_sha)
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    offer = (await agent.read_until("job_offer")).job_offer
    await agent.send(
        "job_accept",
        stream_pb.JobAccept(
            job_id=job["job_id"], attempt_id=job["attempt_id"],
            worker_id=worker["worker_id"], source_sha256_verified=True,
            source_manifest_version=1,
        ),
    )
    for state in (
        common_pb.JOB_STATE_RUNNING,
        common_pb.JOB_STATE_COMPLETED_LOCALLY,
        common_pb.JOB_STATE_RESULT_UPLOADING,
        common_pb.JOB_STATE_RESULT_RECEIVED,
    ):
        await agent.send(
            "job_status",
            stream_pb.JobStatusUpdate(
                job_id=offer.job_id, attempt_id=offer.attempt_id, state=state
            ),
        )
    await asyncio.sleep(0.1)
    assert repositories.get_attempt(job["attempt_id"], settings=gateway_env)["state"] == "result_received"
    ready = stream_pb.ResultReady(job_id=job["job_id"], attempt_id=job["attempt_id"], result_package=common_pb.PackageTransferDescriptor(transfer_id="upload-test", direction=common_pb.PACKAGE_DIRECTION_AGENT_TO_CENTER, protocol=common_pb.PACKAGE_TRANSFER_PROTOCOL_HTTPS_RESUMABLE_V1, package_type="result", size_bytes=archive.stat().st_size, sha256=result_sha), execution_revision="rev-test", stage_status_summary=adapters.canonical_json_message({}, schema="s", schema_version=1), provider_action_ledger_summary=adapters.canonical_json_message({}, schema="l", schema_version=1), ready_at=adapters.timestamp_from_epoch(time.time()))
    await agent.send("result_ready", ready)
    await asyncio.sleep(0.1)
    assert await running_gateway[0].domain.pending_result_outcomes(worker["worker_id"]) == []
    updated, report = job_service.finalize_result(
        job=repositories.get_attempt(job["attempt_id"], settings=gateway_env),
        archive=archive,
        expected_hash=result_sha,
        expected_size=archive.stat().st_size,
        settings=gateway_env,
    )
    assert report.ok and updated["state"] == "completed"
    retention = float(updated["retention_until"])
    await agent.close()  # persisted acceptance, ACK lost before delivery
    reconnect = FakeAgent(worker, port)
    await reconnect.connect(epoch=2)
    ack = (await reconnect.read_until("result_ack")).result_ack
    ack_retention = ack.retention_until.seconds + ack.retention_until.nanos / 1_000_000_000
    assert ack.result_sha256 == result_sha and ack_retention == pytest.approx(retention, abs=0.01)
    await reconnect.close()


@pytest.mark.asyncio
async def test_wrong_result_route_and_unknown_transfer_rejected(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    await agent.read_until("job_offer")
    ready = stream_pb.ResultReady(job_id=job["job_id"], attempt_id=job["attempt_id"], result_package=common_pb.PackageTransferDescriptor(transfer_id="missing", direction=common_pb.PACKAGE_DIRECTION_AGENT_TO_CENTER, protocol=common_pb.PACKAGE_TRANSFER_PROTOCOL_HTTPS_RESUMABLE_V1, package_type="result", size_bytes=100, sha256=SHA), execution_revision="rev-test", ready_at=adapters.timestamp_from_epoch(time.time()))
    await agent.send("result_ready", ready)
    rejected = (await agent.read_until("result_rejected")).result_rejected
    assert rejected.reason == stream_pb.RESULT_REJECT_REASON_UNEXPECTED_RESULT
    await agent.close()


@pytest.mark.asyncio
async def test_aw_ax_stream_loss_and_gateway_restart_do_not_fail_attempt(gateway_env):
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    server = GatewayServer(GatewayConfig(), worker_settings=gateway_env)
    port = await server.start()
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    await agent.read_until("job_offer")
    before = repositories.get_attempt(job["attempt_id"], settings=gateway_env)["state"]
    await agent.close()
    await server.stop(0)
    after = repositories.get_attempt(job["attempt_id"], settings=gateway_env)["state"]
    assert before == after == "source_uploading"


def test_ba_bb_bounded_queues_and_event_batch_guard():
    config = GatewayConfig(max_outbound_queue=3, max_event_batch_count=2).validated()
    assert config.max_outbound_queue == 3
    payload = {"job_id": "j", "attempt_id": "a", "first_seq": 1, "events": [{"seq": n, "event_id": f"e{n}", "event_type": "resource_warning", "occurred_at": time.time(), "payload": {}} for n in range(1, 4)]}
    batch = adapters.event_batch_from_http(payload, worker_id="w")
    assert len(batch.events) == 3  # gateway's negotiated bound is independently lower


@pytest.mark.asyncio
async def test_bc_oversized_message_rejected(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    huge = "x" * (1024 * 1024)
    await agent.send("progress", stream_pb.ProgressUpdate(job_id="j", attempt_id="a", stage_id="s", safe_message=huge))
    try:
        error = await agent.read_until("error")
    except grpc.aio.AioRpcError as exc:
        assert exc.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    else:
        assert error.error.code == common_pb.ERROR_CODE_MESSAGE_TOO_LARGE
    await agent.close()


@pytest.mark.asyncio
async def test_bd_be_graceful_shutdown_and_health(running_gateway, gateway_env):
    server, port = running_gateway
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = health_pb2_grpc.HealthStub(channel)
    response = await stub.Check(health_pb2.HealthCheckRequest(service=SERVICE_NAME))
    assert response.status == health_pb2.HealthCheckResponse.SERVING
    worker = make_worker(gateway_env)
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    await server.service.drain()
    assert server.service.draining
    await agent.close()
    await channel.close()


def test_bf_bg_metrics_have_required_counters_and_reject_worker_labels():
    metrics = GatewayMetrics()
    metrics.inc("connections_total", labels={"protocol_version": 1})
    with pytest.raises(ValueError, match="high-cardinality"):
        metrics.inc("connections_total", labels={"worker_id": "w"})
    assert metrics.snapshot()["connections_total{protocol_version=1}"] == 1


def test_bh_bj_safe_logging_has_no_full_payload_or_secret_fields():
    source = Path("backend/app/agent_gateway/service.py").read_text(encoding="utf-8").lower()
    assert "logger.info(envelope" not in source and "logger.info(message" not in source
    for secret in ("api_key", "private_key", "execution_token", "worker_token", "claim_secret"):
        assert secret not in source


def test_bi_proto_descriptor_has_no_credentials_or_shell():
    forbidden = ("password", "api_key", "access_token", "private_key", "execution_token", "shell", "argv")
    for descriptor in (common_pb.DESCRIPTOR, stream_pb.DESCRIPTOR):
        for message in descriptor.message_types_by_name.values():
            for field in message.fields:
                assert not any(item in field.name.lower() for item in forbidden)


def test_bk_bm_polling_unchanged_and_transport_ownership_fences_duplicate(gateway_env):
    worker = make_worker(gateway_env)
    make_job(gateway_env, worker["worker_id"])
    gateway_repository.accept_connection(worker_id=worker["worker_id"], instance_id=worker["instance_id"], connection_id="c", connection_epoch=1, protocol_version=1, settings=gateway_env)
    with pytest.raises(repositories.TransportOwnershipConflict):
        repositories.claim_next_job_for_worker(worker["worker_id"], transport_mode="polling", settings=gateway_env)
    claimed = repositories.claim_next_job_for_worker(worker["worker_id"], transport_mode="grpc_stream", gateway_offer={"connection_id": "c", "expires_at": time.time() + 30}, settings=gateway_env)
    assert claimed is not None
    assert "grpc" not in Path("requirements-worker.txt").read_text(encoding="utf-8").lower()


@pytest.mark.asyncio
async def test_bn_real_test_listener_never_uses_8443(running_gateway):
    server, port = running_gateway
    assert server.config.host == "127.0.0.1" and port != 8443


@pytest.mark.asyncio
async def test_stress_20_concurrent_fake_workers(running_gateway, gateway_env):
    server, port = running_gateway
    workers = [make_worker(gateway_env, instance_id=f"stress-instance-{i:02d}") for i in range(20)]
    jobs = {worker["worker_id"]: make_job(gateway_env, worker["worker_id"]) for worker in workers}

    async def connect_and_heartbeat(worker):
        agent = FakeAgent(worker, port)
        assert (await agent.connect(epoch=1)).WhichOneof("payload") == "hello"
        offer = (await agent.read_until("job_offer")).job_offer
        assert offer.attempt_id == jobs[worker["worker_id"]]["attempt_id"]
        await agent.send("heartbeat", stream_pb.Heartbeat(worker_id=worker["worker_id"], connection_id=agent.connection_id, observed_at=adapters.timestamp_from_epoch(time.time()), worker_state=common_pb.WORKER_STATE_IDLE, max_slots=2, accepting_jobs=True))
        event = adapters.event_batch_from_http(
            {
                "job_id": offer.job_id,
                "attempt_id": offer.attempt_id,
                "first_seq": 1,
                "events": [{"seq": 1, "event_id": "stress-event", "event_type": "resource_warning", "occurred_at": time.time(), "payload": {}}],
            },
            worker_id=worker["worker_id"],
        )
        await agent.send("event_batch", event)
        assert (await agent.read_until("event_ack")).event_ack.highest_contiguous_sequence == 1
        return agent

    agents = await asyncio.gather(*(connect_and_heartbeat(worker) for worker in workers))
    await asyncio.sleep(0.1)
    assert await server.registry.count() == 20
    assert server.metrics.snapshot()["heartbeats_total"] == 20
    await asyncio.gather(*(agent.close() for agent in agents))


@pytest.mark.asyncio
async def test_backpressure_many_event_batches_remain_durable(running_gateway, gateway_env):
    _, port = running_gateway
    worker = make_worker(gateway_env)
    job = make_job(gateway_env, worker["worker_id"])
    agent = FakeAgent(worker, port)
    await agent.connect(epoch=1)
    await agent.read_until("job_offer")
    for seq in range(1, 51):
        batch = adapters.event_batch_from_http({"job_id": job["job_id"], "attempt_id": job["attempt_id"], "first_seq": seq, "events": [{"seq": seq, "event_id": f"event-{seq}", "event_type": "resource_warning", "occurred_at": time.time(), "payload": {"n": seq}}]}, worker_id=worker["worker_id"])
        await agent.send("event_batch", batch)
    for seq in range(1, 51):
        ack = (await agent.read_until("event_ack")).event_ack
        assert ack.highest_contiguous_sequence == seq
    assert repositories.get_cursor(job["job_id"], job["attempt_id"], settings=gateway_env) == 50
    await agent.close()


def test_bo_no_provider_inference_or_production_deploy_surface():
    gateway_source = "\n".join(path.read_text(encoding="utf-8") for path in Path("backend/app/agent_gateway").glob("*.py"))
    assert "ProviderAdapter" not in gateway_source and "inference(" not in gateway_source
    assert not Path("/etc/systemd/system/agent-gateway.service").exists()
