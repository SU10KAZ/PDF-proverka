from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path

import httpx
import pytest

grpc = pytest.importorskip("grpc")

from audit_worker.client import CenterClient
from audit_worker.config import (
    InsecureTransportError, WorkerConfig, validate_control_transport,
)
from audit_worker.event_outbox import EventOutbox
from audit_worker.grpc_transport import (
    WORKER_METRIC_NAMES, FatalGrpcTransportError, GrpcStreamControlTransport,
    GrpcTransportError,
)
from audit_worker.local_store import LocalJobStore, WorkerStateStore, atomic_write_json
from backend.app.agent_gateway.config import GatewayConfig
from backend.app.agent_gateway.server import GatewayServer
from backend.app.services.distributed_workers import database
from backend.app.services.distributed_workers.settings import get_settings
from contracts.agent_stream.v1 import adapters as stream_adapters
from contracts.agent_stream.v1 import agent_stream_pb2 as stream_pb
from contracts.agent_stream.v1 import common_pb2 as common_pb
from tests.distributed_workers_helpers import (
    ADMIN_USER, SyncASGITransport, enable_portal_roles, make_center_app,
    session_cookie,
)


BOOTSTRAP = "test-bootstrap-secret-12c-0123456789abcdef"


def _config(root: Path, **changes) -> WorkerConfig:
    values = dict(
        dispatcher_url="https://center.invalid",
        root=root,
        display_name="12c-agent",
        provider_gate_enabled=False,
        max_slots=2,
        pipeline_revision="rev-test",
        grpc_target="127.0.0.1:12345",
    )
    values.update(changes)
    return WorkerConfig(**values)


def _heartbeat(active=None):
    return {
        "sent_at": time.time(),
        "worker_state": "busy" if active else "idle",
        "configured_max_slots": 2,
        "calculated_free_slots": 1 if active else 2,
        "active_jobs": active or [],
        "resource_snapshot": {},
        "disk": {},
        "executor": {"status": "online"},
        "providers": [],
    }


class _NullData:
    def __init__(self):
        self.connection_id = None

    def set_control_context(self, *, connection_id=None):
        self.connection_id = connection_id

    def close(self):
        pass


def _transport(tmp_path: Path, *, heartbeat=None, queue_max=128):
    config = _config(
        tmp_path,
        control_transport="grpc",
        grpc_outbound_queue_max=queue_max,
    )
    config.ensure_dirs()
    return GrpcStreamControlTransport(
        target=config.grpc_target,
        data_client=_NullData(),
        state_store=WorkerStateStore(config.state_path, config.token_path),
        jobs=LocalJobStore(config.jobs_dir),
        worker_id="wrk_0123456789abcdef",
        instance_id="inst_0123456789abcdef",
        config=config,
        build_heartbeat=lambda: heartbeat or _heartbeat(),
    )


def test_a_transport_config_polling_default(tmp_path):
    assert _config(tmp_path).control_transport == "polling"


def test_b_grpc_config_is_explicit_and_local(tmp_path):
    config = _config(tmp_path, control_transport="grpc")
    validate_control_transport(config)
    assert config.grpc_target == "127.0.0.1:12345"
    assert config.grpc_reconnect_min_delay_sec <= config.grpc_reconnect_max_delay_sec
    assert 0 <= config.grpc_reconnect_jitter <= 1
    assert config.grpc_max_send_message_bytes == 1024 * 1024
    assert config.grpc_max_receive_message_bytes == 1024 * 1024


def test_b_data_plane_defaults_to_dispatcher_and_can_be_isolated(tmp_path, monkeypatch):
    """Package bytes can use an isolated HTTPS origin without changing control."""
    from audit_worker.agent import WorkerAgent
    from audit_worker.config import load_config

    monkeypatch.setenv("AUDIT_WORKER_DISPATCHER_URL", "https://control.example")
    default = load_config(str(tmp_path / "default"))
    assert default.data_plane_base_url is None

    monkeypatch.setenv(
        "AUDIT_WORKER_DATA_PLANE_BASE_URL", "https://data.example:9443/"
    )
    isolated = load_config(str(tmp_path / "isolated"))
    assert isolated.data_plane_base_url == "https://data.example:9443"

    isolated.ensure_dirs()
    agent = WorkerAgent(
        isolated,
        {
            "worker_id": "wrk_0123456789abcdef",
            "instance_id": "inst_0123456789abcdef",
            "token": "test-token",
        },
    )
    try:
        assert agent.client.base_url == "https://data.example:9443"
    finally:
        agent.shutdown()


def test_b_external_data_plane_rejects_non_https(tmp_path):
    from audit_worker.config import (
        InsecureTransportError,
        WorkerConfig,
        validate_transport_security,
    )

    config = WorkerConfig(
        dispatcher_url="https://control.example",
        data_plane_base_url="http://data.example:9443",
        root=tmp_path,
        display_name="data-plane-security",
    )
    with pytest.raises(InsecureTransportError, match="DATA_PLANE_BASE_URL"):
        validate_transport_security(config)


def test_c_d_polling_and_grpc_are_single_owner_modes(tmp_path):
    assert _config(tmp_path).control_transport == "polling"
    assert _config(tmp_path, control_transport="grpc").control_transport == "grpc"


def test_e_f_g_durable_epoch_initial_increment_restart(tmp_path):
    config = _config(tmp_path)
    store = WorkerStateStore(config.state_path, config.token_path)
    assert store.reserve_connection_epoch() == 1
    assert store.reserve_connection_epoch() == 2
    restarted = WorkerStateStore(config.state_path, config.token_path)
    assert restarted.reserve_connection_epoch() == 3


def test_epoch_reservation_is_thread_safe(tmp_path):
    store = WorkerStateStore(tmp_path / "state.json", tmp_path / "token")
    found = []
    threads = [threading.Thread(target=lambda: found.append(store.reserve_connection_epoch()))
               for _ in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(found) == list(range(1, 13))


def test_pre_dispatch_crash_recovers_once_and_late_offer_never_recreates(tmp_path):
    from audit_worker.agent import WorkerAgent

    config = _config(tmp_path)
    config.ensure_dirs()
    assignment = {
        "job_id": "job_0123456789abcdef",
        "attempt_id": "att_0123456789abcdef",
        "job_type": "test_pipeline_v1",
        "project_id": "project-12c",
        "params": {"label": "pre-dispatch"},
        "package": {
            "package_id": "pkg_0123456789abcdef",
            "package_type": "source",
            "size_bytes": 100,
            "sha256": "a" * 64,
            "compression": "gzip",
            "manifest_version": 1,
        },
        "execution_token": "",
    }
    agent = WorkerAgent(config, {
        "worker_id": "wrk_0123456789abcdef",
        "instance_id": "inst_0123456789abcdef",
        "token": "test-token",
    })
    agent.jobs.create(assignment)
    agent.jobs.update(
        assignment["job_id"], assignment["attempt_id"], local_state="verified"
    )
    created_at = agent.jobs.load(
        assignment["job_id"], assignment["attempt_id"]
    )["created_at"]
    calls = {"accept": 0, "dispatch": 0, "download": 0}
    agent.client.accept_job = lambda *_args, **_kwargs: calls.__setitem__(
        "accept", calls["accept"] + 1
    ) or {}
    agent._dispatch_and_wait = lambda *_args, **_kwargs: calls.__setitem__(
        "dispatch", calls["dispatch"] + 1
    ) or {"ok": False, "reason": "test-stop"}
    agent._download_and_verify = lambda *_args, **_kwargs: calls.__setitem__(
        "download", calls["download"] + 1
    )
    agent._flush_outbox = lambda *_args, **_kwargs: None

    try:
        agent._resume_pre_dispatch_attempts()
        agent._job_threads[assignment["attempt_id"]].join(5)
        assert calls == {"accept": 1, "dispatch": 1, "download": 0}
        assert agent.jobs.load(
            assignment["job_id"], assignment["attempt_id"]
        )["local_state"] == "accepted"

        agent.jobs.update(
            assignment["job_id"], assignment["attempt_id"], local_state="finished"
        )
        agent._start_job_thread(assignment)
        assert calls == {"accept": 2, "dispatch": 1, "download": 0}
        assert agent.jobs.load(
            assignment["job_id"], assignment["attempt_id"]
        )["created_at"] == created_at
    finally:
        agent.shutdown()


def test_h_i_j_agent_hello_uses_real_metadata_attempts_and_cursors(tmp_path):
    active = [{
        "job_id": "job_0123456789abcdef",
        "attempt_id": "att_0123456789abcdef",
        "stage": "running",
        "last_event_seq": 7,
        "last_acked_seq": 5,
    }]
    transport = _transport(tmp_path, heartbeat=_heartbeat(active))
    transport.jobs.create({
        "job_id": active[0]["job_id"], "attempt_id": active[0]["attempt_id"],
        "job_type": "test_pipeline_v1", "execution_token": "",
    })
    events = transport.jobs.job_dir(active[0]["job_id"], active[0]["attempt_id"]) / "events"
    atomic_write_json(events / "cursor.json", {"last_written_seq": 7})
    atomic_write_json(events / "ack.json", {"last_acked_seq": 5})
    hello = transport._hello(9)
    assert hello.worker_id == transport.worker_id
    assert hello.connection_epoch == 9
    assert hello.active_attempts[0].attempt_id == active[0]["attempt_id"]
    assert hello.event_cursors[0].highest_contiguous_sequence == 5
    assert "test_pipeline_v1" in hello.capabilities.job_types


def test_k_l_center_hello_parsing_binds_https_context(tmp_path):
    transport = _transport(tmp_path)
    response = stream_pb.CenterToAgent(
        protocol_version=1,
        stream_sequence=1,
        worker_id=transport.worker_id,
        connection_id="gconn_0123456789abcdef",
        hello=stream_pb.CenterHello(
            accepted_protocol_version=1,
            connection_id="gconn_0123456789abcdef",
            heartbeat_interval={"seconds": 7},
            max_control_message_bytes=65536,
        ),
    )
    transport._handle_response(response)
    assert transport._ready.is_set()
    assert transport.data.connection_id == "gconn_0123456789abcdef"
    assert transport.heartbeat_interval_sec == 7
    assert transport._max_control_message_bytes == 65536
    assert transport.metrics_snapshot()["grpc_connect_success"] == 1
    assert set(transport.metrics_snapshot()) == set(WORKER_METRIC_NAMES)


def test_center_resume_cursor_rewinds_tail_and_rejects_impossible_ack(tmp_path):
    transport = _transport(tmp_path)
    job_id = "job_0123456789abcdef"
    attempt_id = "att_0123456789abcdef"
    transport.jobs.create({
        "job_id": job_id, "attempt_id": attempt_id,
        "job_type": "test_pipeline_v1", "execution_token": "",
    })
    events = transport.jobs.job_dir(job_id, attempt_id) / "events"
    outbox = EventOutbox(events)
    for index in range(1, 8):
        assert outbox.append("stage_progress", {"index": index}) == index
    outbox.ack(5)

    transport._handle_response(stream_pb.CenterToAgent(
        protocol_version=1,
        stream_sequence=1,
        worker_id=transport.worker_id,
        connection_id="gconn_0123456789abcdef",
        hello=stream_pb.CenterHello(
            accepted_protocol_version=1,
            connection_id="gconn_0123456789abcdef",
            resume_cursors=[{
                "job_id": job_id,
                "attempt_id": attempt_id,
                "highest_contiguous_sequence": 3,
            }],
        ),
    ))
    outbox.reload()
    assert outbox.last_acked_seq == 3
    assert [item["seq"] for item in outbox.pending_batch()] == [4, 5, 6, 7]

    with pytest.raises(FatalGrpcTransportError, match="exceeds"):
        transport._handle_response(stream_pb.CenterToAgent(
            protocol_version=1,
            stream_sequence=2,
            worker_id=transport.worker_id,
            connection_id="gconn_0123456789abcdef",
            hello=stream_pb.CenterHello(
                accepted_protocol_version=1,
                connection_id="gconn_0123456789abcdef",
                resume_cursors=[{
                    "job_id": job_id,
                    "attempt_id": attempt_id,
                    "highest_contiguous_sequence": 8,
                }],
            ),
        ))


def test_duplicate_offer_reaches_agent_core_for_idempotent_response(tmp_path):
    transport = _transport(tmp_path)
    transport._connection_id = "gconn_0123456789abcdef"
    offer = stream_adapters.job_offer_from_http({
        "job_id": "job_0123456789abcdef",
        "attempt_id": "att_0123456789abcdef",
        "attempt_no": 1,
        "assignment_generation": 1,
        "worker_id": transport.worker_id,
        "assigned_at": time.time(),
        "assign_ttl_sec": 30,
        "job_type": "test_pipeline_v1",
        "project_id": "project-12c",
        "params": {},
        "package": {
            "package_id": "pkg_0123456789abcdef",
            "package_type": "source",
            "size_bytes": 100,
            "sha256": "a" * 64,
            "compression": "gzip",
            "manifest_version": 1,
        },
    })
    for sequence in (1, 2):
        transport._handle_response(stream_pb.CenterToAgent(
            protocol_version=1,
            stream_sequence=sequence,
            worker_id=transport.worker_id,
            connection_id=transport._connection_id,
            job_offer=offer,
        ))
    assert transport._offers.get_nowait()["attempt_id"] == offer.attempt_id
    assert transport._offers.get_nowait()["attempt_id"] == offer.attempt_id


def test_protocol_version_rejection_reconnects_instead_of_stopping(tmp_path):
    transport = _transport(tmp_path)
    transport._connection_id = "gconn_0123456789abcdef"
    with pytest.raises(GrpcTransportError) as raised:
        transport._handle_response(stream_pb.CenterToAgent(
            protocol_version=1,
            stream_sequence=1,
            worker_id=transport.worker_id,
            connection_id=transport._connection_id,
            error={
                "code": common_pb.ERROR_CODE_PROTOCOL_VERSION_UNSUPPORTED,
                "safe_message": "no common version",
                "retryable": False,
            },
        ))
    assert not isinstance(raised.value, FatalGrpcTransportError)


def test_source_transfer_is_selected_by_exact_attempt(tmp_path):
    transport = _transport(tmp_path)
    captured = {}
    transport.data.download_source = lambda *_args, **kwargs: captured.update(kwargs) or 1
    job_id = "job_0123456789abcdef"
    transport._assignments = {
        "att_old_0123456789": {
            "job_id": job_id,
            "attempt_id": "att_old_0123456789",
            "assignment_generation": 1,
            "package": {"package_id": "pkg_old_0123456789"},
        },
        "att_new_0123456789": {
            "job_id": job_id,
            "attempt_id": "att_new_0123456789",
            "assignment_generation": 2,
            "package": {"package_id": "pkg_new_0123456789"},
        },
    }
    transport.download_source(
        job_id, tmp_path / "source.tar", "", attempt_id="att_new_0123456789"
    )
    assert captured["transfer_id"] == "pkg_new_0123456789"


def test_p_ba_heartbeat_is_coalesced_to_latest(tmp_path):
    transport = _transport(tmp_path)
    transport.heartbeat({**_heartbeat(), "sent_at": 1.0})
    transport.heartbeat({**_heartbeat(), "sent_at": 2.0})
    with transport._heartbeat_lock:
        assert transport._latest_heartbeat.observed_at.seconds == 2


def test_q_bb_capabilities_change_is_coalesced(tmp_path):
    transport = _transport(tmp_path)
    transport.heartbeat(_heartbeat())
    transport.config.extra_capabilities["provider_policy_version"] = 2
    transport.heartbeat(_heartbeat())
    with transport._heartbeat_lock:
        changed = transport._latest_capabilities
    assert changed is not None


def test_az_bounded_critical_queue(tmp_path):
    transport = _transport(tmp_path, queue_max=8)
    assert transport._critical.maxsize == 8


def test_ab_ac_negotiated_event_batch_bound_and_ack(tmp_path):
    transport = _transport(tmp_path)
    transport._max_events_per_batch = 17
    captured = {}

    def send(_kind, message, **_kwargs):
        captured["count"] = len(message.events)
        return stream_pb.CenterToAgent(
            protocol_version=1,
            stream_sequence=1,
            worker_id=transport.worker_id,
            connection_id="gconn_0123456789abcdef",
            event_ack=stream_pb.EventAck(
                job_id=message.job_id,
                attempt_id=message.attempt_id,
                highest_contiguous_sequence=17,
                accepted=17,
            ),
        )

    transport._send = send
    events = [
        {
            "seq": index,
            "event_id": f"ev_{index:016d}",
            "event_type": "stage_progress",
            "occurred_at": time.time(),
            "schema_version": 1,
            "payload": {"index": index},
        }
        for index in range(1, 301)
    ]
    response = transport.post_events(
        "job_0123456789abcdef", "att_0123456789abcdef", 1, events, ""
    )
    assert captured["count"] == 17
    assert response["last_seen_seq"] == 17


def test_ai_cancel_ack_waits_for_replayed_command_identity(tmp_path):
    transport = _transport(tmp_path)
    with pytest.raises(GrpcTransportError, match="identity"):
        transport.ack_command(
            "cmd_0123456789abcdef",
            {"status": "ok", "detail": {"outcome": "cancelled"}},
        )


def test_x_https_source_uses_opaque_transfer_and_active_stream_headers(tmp_path):
    captured = {}

    def handler(request: httpx.Request):
        captured.update(request.headers)
        return httpx.Response(200, content=b"package", request=request)

    client = CenterClient(
        "https://center.invalid", token="worker-token",
        worker_id="wrk_0123456789abcdef", instance_id="inst_0123456789abcdef",
        transport=httpx.MockTransport(handler),
    )
    client.set_control_context(connection_id="gconn_0123456789abcdef")
    destination = tmp_path / "source.tar"
    client.download_source(
        "job_0123456789abcdef", destination, "",
        transfer_id="pkg_0123456789abcdef",
    )
    assert captured["x-agent-stream-connection-id"].startswith("gconn_")
    assert captured["x-package-transfer-id"].startswith("pkg_")
    assert "x-execution-token" not in captured
    assert destination.read_bytes() == b"package"


@pytest.mark.parametrize("target", ["0.0.0.0:1", "8.8.8.8:443", "176.12.77.31:8443"])
def test_bi_insecure_public_target_rejected(tmp_path, target):
    with pytest.raises(InsecureTransportError, match="loopback"):
        validate_control_transport(
            _config(tmp_path, control_transport="grpc", grpc_target=target)
        )


def test_bj_local_insecure_allowed(tmp_path):
    validate_control_transport(
        _config(tmp_path, control_transport="grpc", grpc_target="localhost:50051")
    )


def test_bk_bm_production_defaults_and_zero_inference(tmp_path):
    config = _config(tmp_path)
    assert config.control_transport == "polling"
    assert config.verify_tls is True
    assert config.allow_real_llm is False
    assert config.allow_real_provider_probe is False
    assert config.pipeline_provider_bridge_enabled is False


def test_bl_bootstrap_transport_contract_defaults_polling_and_guards_grpc():
    from backend.app.services.worker_bootstrap.models import BootstrapRequest

    base = {
        "host": "test-worker.example",
        "ssh_user": "auditworker",
        "ssh_auth_ref": "ssh-ref",
        "expected_host_fingerprint": "SHA256:" + "A" * 32,
        "install_root": "/home/auditworker/audit-worker",
        "center_url": "https://center.example",
        "display_name": "12C test worker",
    }
    assert BootstrapRequest(**base).transport_mode == "polling"
    grpc_request = BootstrapRequest(
        **base,
        transport_mode="grpc_stream",
        gateway_target="127.0.0.1:50051",
        protocol_versions=[1],
    )
    assert grpc_request.gateway_security_mode == "test_insecure"
    with pytest.raises(ValueError, match="loopback"):
        BootstrapRequest(
            **base,
            transport_mode="grpc_stream",
            gateway_target="176.12.77.31:8443",
        )


class _GatewayThread:
    def __init__(self, settings, *, port=0):
        self.settings = settings
        self.requested_port = port
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.ready = threading.Event()
        self.server = None
        self.port = None
        self.error = None

    def _run(self):
        asyncio.set_event_loop(self.loop)

        async def start():
            try:
                self.server = GatewayServer(
                    GatewayConfig(
                        port=self.requested_port,
                        offer_poll_interval_sec=0.02,
                        heartbeat_timeout_sec=30,
                        idle_timeout_sec=40,
                        graceful_shutdown_sec=0.1,
                    ),
                    worker_settings=self.settings,
                )
                self.port = await self.server.start()
            except BaseException as exc:  # pragma: no cover - surfaced in caller
                self.error = exc
            finally:
                self.ready.set()

        self.loop.run_until_complete(start())
        if self.error is None:
            self.loop.run_forever()

    def start(self):
        self.thread.start()
        assert self.ready.wait(10)
        if self.error:
            raise self.error
        return self.port

    def stop(self):
        if self.server is not None:
            future = asyncio.run_coroutine_threadsafe(self.server.stop(0), self.loop)
            future.result(10)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(10)


def _wait_until(predicate, timeout=15, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


@pytest.fixture()
def grpc_e2e_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", BOOTSTRAP)
    monkeypatch.setenv("DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES", "4096")
    enable_portal_roles(monkeypatch)
    database.reset_state_for_tests()
    app = make_center_app()
    settings = get_settings()
    database.ensure_ready(settings)
    transport = SyncASGITransport(app)
    from backend.app.core import portal_auth

    admin = httpx.Client(
        transport=transport, base_url="http://center",
        headers={"X-Requested-With": "audit-workers"},
    )
    admin.cookies.set(portal_auth.get_settings().cookie_name, session_cookie(ADMIN_USER))
    yield tmp_path, settings, transport, admin
    admin.close()
    database.reset_state_for_tests()


def _registered_grpc_agent(env, port, *, slots=1):
    tmp_path, _settings, transport, admin = env
    from audit_worker.agent import WorkerAgent
    from audit_worker.registration import ensure_registered

    config = WorkerConfig(
        dispatcher_url="http://center",
        root=tmp_path / ("worker-" + str(slots)),
        display_name="12C real Agent",
        heartbeat_interval_sec=5,
        poll_wait_sec=1,
        event_flush_interval_sec=0.1,
        max_slots=slots,
        provider_gate_enabled=False,
        allow_insecure_localhost=True,
        transport=transport,
        control_transport="grpc",
        grpc_target=f"127.0.0.1:{port}",
        grpc_connect_timeout_sec=5,
        grpc_heartbeat_interval_override_sec=5,
        pipeline_revision=None,
    )
    config.ensure_dirs()
    identity = ensure_registered(config, bootstrap_secret=BOOTSTRAP)
    response = admin.post(
        f"/api/workers/{identity['worker_id']}/approve",
        json={"display_name": "12C real Agent", "configured_max_slots": slots},
    )
    assert response.status_code == 200, response.text
    identity = ensure_registered(config)
    agent = WorkerAgent(config, identity)
    # The host running this suite may intentionally have swap pressure. E2E
    # proves protocol/runtime behavior with a deterministic isolated capacity
    # snapshot; ResourceMonitor's real gates have their own tests.
    agent.monitor.snapshot = lambda **_: {
        "at": time.time(),
        "slots": {
            "calculated_free": slots,
            "binding_constraint": "e2e",
        },
    }
    return config, agent, admin, identity


def test_idle_stream_for_one_minute_has_no_reconnect_or_busy_loop(grpc_e2e_env):
    """A healthy idle stream stays on one epoch while periodic heartbeats flow."""
    gateway = _GatewayThread(grpc_e2e_env[1])
    port = gateway.start()
    config, agent, _admin, _identity = _registered_grpc_agent(grpc_e2e_env, port)

    try:
        agent.heartbeat.start()
        assert _wait_until(lambda: agent.client._ready.is_set(), 10)
        epoch = json.loads(config.state_path.read_text())["connection_epoch"]
        time.sleep(60)
        metrics = gateway.server.metrics.snapshot()
        assert agent.client._ready.is_set()
        assert json.loads(config.state_path.read_text())["connection_epoch"] == epoch
        assert metrics.get("connections_total{protocol_version=1}") == 1
        assert metrics.get("stream_disconnects", 0) == 0
        assert metrics.get("heartbeats_total", 0) >= 10
    finally:
        agent.shutdown()
        gateway.stop()


def test_local_real_agent_gateway_executor_eventoutbox_https_resultack(
    grpc_e2e_env,
):
    """Required local 12C vertical slice: real Agent, Gateway and Executor."""
    gateway = _GatewayThread(grpc_e2e_env[1])
    port = gateway.start()
    config, agent, admin, identity = _registered_grpc_agent(grpc_e2e_env, port)
    from tests.test_distributed_workers_e2e import running_executor

    created = admin.post(
        "/api/workers/jobs",
        json={
            "worker_id": identity["worker_id"],
            "project_id": "12c-local-real",
            "params": {"label": "12c", "steps": 3, "step_seconds": 0.01},
        },
    )
    assert created.status_code == 200, created.text
    job_id = created.json()["job"]["job_id"]
    try:
        agent._start_sender()
        with running_executor(config, max_jobs=1):
            agent.heartbeat.beat_once()
            deadline = time.time() + 15
            assignment = None
            while assignment is None and time.time() < deadline:
                assignment = agent.poller.poll(free_slots=1, compressions=["gzip"])
            if assignment is None:
                from backend.app.services.distributed_workers import (
                    gateway_repository, repositories, slots,
                )
                print("12C-E2E-DIAG", gateway.server.metrics.snapshot())
                print("12C-E2E-JOB", admin.get(f"/api/workers/jobs/{job_id}").json()["job"])
                worker = repositories.get_worker(identity["worker_id"], settings=grpc_e2e_env[1])
                print("12C-E2E-WORKER", worker)
                print("12C-E2E-LIMIT", slots.effective_limit(worker, protocol_version=1))
                print("12C-E2E-SESSION", gateway_repository.get_transport_session(
                    identity["worker_id"], settings=grpc_e2e_env[1]
                ))
            assert assignment is not None, {
                "metrics": gateway.server.metrics.snapshot(),
                "center_state": agent.center_state,
                "grpc_ready": agent.client._ready.is_set(),
                "connection_id": agent.client._connection_id,
                "job": admin.get(f"/api/workers/jobs/{job_id}").json(),
            }
            outcome = agent.execute_job(assignment)
        assert outcome["ok"], outcome
        final = admin.get(f"/api/workers/jobs/{job_id}").json()["job"]
        assert final["state"] == "completed"
        assert final["retention_until"] is not None
        meta = agent.jobs.load(job_id, assignment["attempt_id"])
        assert meta["retention_until"] is not None
        assert meta["execution_token"] in (None, "")
    finally:
        agent.shutdown()
        gateway.stop()


def test_network_loss_executor_survives_reconnect_replay_and_result_ack(grpc_e2e_env):
    gateway = _GatewayThread(grpc_e2e_env[1])
    port = gateway.start()
    config, agent, admin, identity = _registered_grpc_agent(grpc_e2e_env, port)
    from tests.test_distributed_workers_e2e import running_executor

    created = admin.post(
        "/api/workers/jobs",
        json={"worker_id": identity["worker_id"], "project_id": "12c-loss",
              "params": {"label": "loss", "steps": 40, "step_seconds": 0.03}},
    ).json()["job"]
    first_epoch = 0
    replacement = None
    try:
        agent._start_sender()
        with running_executor(config, max_jobs=1):
            agent.heartbeat.beat_once()
            assignment = _wait_until(
                lambda: agent.poller.poll(free_slots=1, compressions=["gzip"]), 15
            )
            assert assignment
            outcome = {}
            runner = threading.Thread(
                target=lambda: outcome.update(agent.execute_job(assignment)), daemon=True
            )
            runner.start()
            assert _wait_until(
                lambda: (agent.db.queue_item(assignment["attempt_id"]) or {}).get("state")
                == "running",
                10,
            )
            first_epoch = json.loads(config.state_path.read_text())["connection_epoch"]
            gateway.stop()
            # The Executor owns the process; loss of the stream cannot cancel it.
            assert _wait_until(
                lambda: (agent.db.queue_item(assignment["attempt_id"]) or {}).get("state")
                in {"running", "finished"},
                3,
            )
            replacement = _GatewayThread(grpc_e2e_env[1], port=port)
            replacement.start()
            assert _wait_until(lambda: agent.client._ready.is_set(), 15)
            agent.heartbeat.beat_once()
            runner.join(15)
            assert not runner.is_alive()
            # If ResultReady was in the loss window, durable local result is retried.
            assert _wait_until(
                lambda: (
                    agent._deliver_pending_results()
                    or (agent.jobs.load(created["job_id"], assignment["attempt_id"]) or {})
                    .get("retention_until")
                ),
                15,
                0.2,
            )
        assert json.loads(config.state_path.read_text())["connection_epoch"] > first_epoch
        final = admin.get(f"/api/workers/jobs/{created['job_id']}").json()["job"]
        assert final["state"] == "completed"
        events = admin.get(f"/api/workers/jobs/{created['job_id']}/events").json()["events"]
        seqs = [item["sequence"] for item in events]
        assert len(seqs) == len(set(seqs))
    finally:
        agent.shutdown()
        if replacement is not None:
            replacement.stop()
        elif gateway.thread.is_alive():
            gateway.stop()


def test_agent_restart_adopts_live_executor_higher_epoch_no_duplicate(grpc_e2e_env):
    gateway = _GatewayThread(grpc_e2e_env[1])
    port = gateway.start()
    config, agent1, admin, identity = _registered_grpc_agent(grpc_e2e_env, port)
    from audit_worker.agent import WorkerAgent
    from tests.test_distributed_workers_e2e import running_executor

    created = admin.post(
        "/api/workers/jobs",
        json={"worker_id": identity["worker_id"], "project_id": "12c-agent-restart",
              "params": {"label": "restart", "steps": 50, "step_seconds": 0.03}},
    ).json()["job"]
    agent2 = None
    try:
        with running_executor(config, max_jobs=1):
            agent1.heartbeat.beat_once()
            assignment = _wait_until(
                lambda: agent1.poller.poll(free_slots=1, compressions=["gzip"]), 15
            )
            assert assignment
            observer = threading.Thread(target=agent1.execute_job, args=(assignment,), daemon=True)
            observer.start()
            assert _wait_until(
                lambda: (agent1.db.queue_item(assignment["attempt_id"]) or {}).get("state")
                == "running", 10
            )
            process_before = agent1.db.process_row(assignment["attempt_id"])
            epoch_before = json.loads(config.state_path.read_text())["connection_epoch"]
            agent1.shutdown()
            observer.join(10)
            assert (agent1.db.queue_item(assignment["attempt_id"]) or {}).get("state") in {
                "running", "finished"
            }

            from audit_worker.registration import ensure_registered

            identity2 = ensure_registered(config)
            assert identity2["instance_id"] != identity["instance_id"]
            agent2 = WorkerAgent(config, identity2)
            agent2.monitor.snapshot = lambda **_: {
                "at": time.time(),
                "slots": {"calculated_free": 0, "binding_constraint": "active"},
            }
            agent2.heartbeat.beat_once()
            agent2._startup_reconcile()
            assert _wait_until(
                lambda: (agent2.db.queue_item(assignment["attempt_id"]) or {}).get("state")
                == "finished", 15
            )
            process_after = agent2.db.process_row(assignment["attempt_id"])
            assert process_after["pid"] == process_before["pid"]
            assert json.loads(config.state_path.read_text())["connection_epoch"] > epoch_before
            assert _wait_until(
                lambda: (
                    agent2._deliver_pending_results()
                    or (agent2.jobs.load(created["job_id"], assignment["attempt_id"]) or {})
                    .get("retention_until")
                ), 15, 0.2
            )
        assert admin.get(f"/api/workers/jobs/{created['job_id']}").json()["job"]["state"] == "completed"
    finally:
        if agent2 is not None:
            agent2.shutdown()
        elif not agent1._stop.is_set():
            agent1.shutdown()
        gateway.stop()


def test_multi_slot_two_real_attempts_one_stream_no_third(grpc_e2e_env):
    gateway = _GatewayThread(grpc_e2e_env[1])
    port = gateway.start()
    config, agent, admin, identity = _registered_grpc_agent(grpc_e2e_env, port, slots=2)
    from tests.test_distributed_workers_e2e import running_executor

    jobs = []
    for index in range(3):
        jobs.append(admin.post(
            "/api/workers/jobs",
            json={"worker_id": identity["worker_id"], "project_id": f"12c-slot-{index}",
                  "params": {"label": f"slot-{index}", "steps": 10,
                             "step_seconds": 0.02}},
        ).json()["job"])
    try:
        agent._start_sender()
        with running_executor(config, max_jobs=2):
            agent.heartbeat.beat_once()
            first = _wait_until(
                lambda: agent.poller.poll(free_slots=2, busy_slots=0,
                                          compressions=["gzip"]), 15
            )
            second = _wait_until(
                lambda: agent.poller.poll(free_slots=1, busy_slots=1,
                                          compressions=["gzip"]), 15
            )
            assert first and second and first["attempt_id"] != second["attempt_id"]
            assert agent.poller.poll(
                free_slots=0, busy_slots=2, compressions=["gzip"]
            ) is None
            outcomes = [{}, {}]
            threads = [
                threading.Thread(
                    target=lambda i=i, item=item: outcomes[i].update(agent.execute_job(item)),
                    daemon=True,
                )
                for i, item in enumerate((first, second))
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(30)
            assert all(item.get("ok") for item in outcomes), outcomes
        states = [admin.get(f"/api/workers/jobs/{item['job_id']}").json()["job"]["state"]
                  for item in jobs]
        assert states.count("completed") == 2
        # Once one result is ACKed the Gateway may lease the next offer, but
        # it must never become a third concurrent Executor attempt.
        assert states[2] in {"assigned", "source_uploading"}
        assert agent.db.process_row(jobs[2]["attempt_id"]) is None
        assert gateway.server.metrics.snapshot().get("active_connections") == 1
    finally:
        agent.shutdown()
        gateway.stop()


def test_controlled_grpc_to_polling_switch_requires_disconnected_stream(grpc_e2e_env):
    gateway = _GatewayThread(grpc_e2e_env[1])
    port = gateway.start()
    config, agent, admin, identity = _registered_grpc_agent(grpc_e2e_env, port)
    from audit_worker.registration import ensure_registered
    from backend.app.services.distributed_workers import gateway_repository

    try:
        agent.heartbeat.beat_once()
        assert _wait_until(lambda: agent.client._ready.is_set(), 10)
        created = admin.post(
            "/api/workers/jobs",
            json={"worker_id": identity["worker_id"], "project_id": "12c-switch",
                  "params": {"label": "switch", "steps": 2, "step_seconds": 0.01}},
        )
        assert created.status_code == 200, created.text
        agent.shutdown()
        assert _wait_until(
            lambda: not (
                gateway_repository.get_transport_session(
                    identity["worker_id"], settings=grpc_e2e_env[1]
                ) or {}
            ).get("active_connection_id"),
            10,
        )
        polling_identity = ensure_registered(config)
        client = CenterClient(
            "http://center",
            token=polling_identity["token"],
            worker_id=polling_identity["worker_id"],
            instance_id=polling_identity["instance_id"],
            transport=grpc_e2e_env[2],
        )
        try:
            assignment = client.next_job(
                {"free_slots": 1, "busy_slots": 0,
                 "accepts": {"compressions": ["gzip"]}, "wait_sec": 0,
                 "executor_status": "online"}
            )
        finally:
            client.close()
        assert assignment is not None
        session = gateway_repository.get_transport_session(
            identity["worker_id"], settings=grpc_e2e_env[1]
        )
        assert session["transport_mode"] == "polling"
    finally:
        if not agent._stop.is_set():
            agent.shutdown()
        gateway.stop()
