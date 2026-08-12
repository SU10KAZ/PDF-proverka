"""12E reliability seams: durable diagnostics and safe recovery observability.

The process-level Gateway/Agent chaos cases reuse the real isolated grpcio
vertical-slice tests in ``test_agent_grpc_client_12c``.  These tests cover the
new 12E operator surface and the race between a recovery epoch and a persisted
diagnostic update.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

import httpx

from audit_worker import agent as agent_module
from audit_worker.agent import WorkerAgent
from audit_worker.client import CenterClient
from audit_worker.__main__ import main
from audit_worker.config import WorkerConfig
from audit_worker.diagnostics import collect_worker_diagnostics
from audit_worker.event_outbox import EventOutbox
from audit_worker.grpc_transport import (
    FatalGrpcTransportError,
    GrpcStreamControlTransport,
    grpc_failure_reason_code,
)
from audit_worker.local_store import LocalJobStore, WorkerStateStore
from contracts.agent_stream.v1 import agent_stream_pb2 as stream_pb


class _NullData:
    def __init__(self) -> None:
        self.connection_id = None

    def set_control_context(self, *, connection_id=None) -> None:
        self.connection_id = connection_id

    def close(self) -> None:
        return None


def _config(root: Path, *, transport: str = "polling") -> WorkerConfig:
    return WorkerConfig(
        dispatcher_url="https://center.invalid",
        root=root,
        display_name="12e-chaos",
        provider_gate_enabled=False,
        control_transport=transport,
        grpc_target="localhost:12345" if transport == "grpc" else None,
        grpc_security_mode="test_insecure",
    )


def test_12e_state_epoch_and_runtime_diagnostics_are_atomic(tmp_path):
    store = WorkerStateStore(tmp_path / "worker_state.json", tmp_path / "token")
    epochs: list[int] = []

    def reserve() -> None:
        epochs.append(store.reserve_connection_epoch())

    def update(index: int) -> None:
        store.update_runtime_diagnostics(
            grpc_connection_state="disconnected",
            gateway_status="unavailable",
            last_disconnect_reason="GRPC_UNAVAILABLE",
            last_disconnect_at=float(index),
        )

    threads = [threading.Thread(target=reserve) for _ in range(12)]
    threads += [threading.Thread(target=update, args=(index,)) for index in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    state = store.load()
    assert sorted(epochs) == list(range(1, 13))
    assert state["connection_epoch"] == 12
    assert state["runtime_diagnostics"]["last_disconnect_reason"] == "GRPC_UNAVAILABLE"


def test_12e_doctor_is_offline_safe_and_never_prints_token(tmp_path, capsys):
    config = _config(tmp_path)
    config.ensure_dirs()
    state = WorkerStateStore(config.state_path, config.token_path)
    state.save({"worker_id": "wrk_0123456789abcdef", "connection_epoch": 7})
    state.write_token("not-for-diagnostics")
    state.update_runtime_diagnostics(
        grpc_connection_state="disconnected",
        gateway_status="unavailable",
        last_disconnect_reason="GRPC_UNAVAILABLE",
        last_disconnect_at=11.0,
    )
    jobs = LocalJobStore(config.jobs_dir)
    job_id, attempt_id = "job_0123456789abcdef", "att_0123456789abcdef"
    jobs.create({"job_id": job_id, "attempt_id": attempt_id, "job_type": "test_pipeline_v1"})
    outbox = EventOutbox(jobs.job_dir(job_id, attempt_id) / "events")
    for index in range(3):
        assert outbox.append("stage_progress", {"index": index}) == index + 1
    outbox.ack(1)
    jobs.update(job_id, attempt_id, result_hash="a" * 64, local_state="completed_locally")

    report = collect_worker_diagnostics(config)
    assert report["transport_mode"] == "polling"
    assert report["connection_epoch"] == 7
    assert report["outbox_pending_count"] == 2
    assert report["pending_result_count"] == 1
    assert report["last_disconnect_reason"] == "GRPC_UNAVAILABLE"
    assert "not-for-diagnostics" not in json.dumps(report)

    assert main(["doctor", "--root", str(tmp_path)]) == 0
    output = capsys.readouterr().out
    assert '"outbox_pending_count": 2' in output
    assert "not-for-diagnostics" not in output


def test_12e_doctor_does_not_create_or_migrate_a_missing_worker_root(tmp_path, capsys):
    root = tmp_path / "missing-worker-root"

    assert main(["doctor", "--root", str(root)]) == 0

    report = json.loads(capsys.readouterr().out)
    assert report["local_db_status"] == "absent"
    assert not root.exists()


def test_12e_gateway_hello_persists_diagnostic_connection_state(tmp_path):
    config = _config(tmp_path, transport="grpc")
    config.ensure_dirs()
    store = WorkerStateStore(config.state_path, config.token_path)
    transport = GrpcStreamControlTransport(
        target="localhost:12345",
        data_client=_NullData(),
        state_store=store,
        jobs=LocalJobStore(config.jobs_dir),
        worker_id="wrk_0123456789abcdef",
        instance_id="inst_0123456789abcdef",
        config=config,
        build_heartbeat=lambda: {"active_jobs": [], "providers": []},
    )
    transport._handle_response(stream_pb.CenterToAgent(
        protocol_version=1,
        stream_sequence=1,
        worker_id=transport.worker_id,
        connection_id="gconn_0123456789abcdef",
        hello=stream_pb.CenterHello(
            accepted_protocol_version=1,
            connection_id="gconn_0123456789abcdef",
        ),
    ))
    runtime = store.load()["runtime_diagnostics"]
    assert runtime["grpc_connection_state"] == "connected"
    assert runtime["gateway_status"] == "ready"
    assert runtime["worker_accepting_jobs"] is True
    assert runtime["last_connected_at"] > 0


def test_12e_typed_disconnect_reasons_are_stable():
    assert grpc_failure_reason_code(FatalGrpcTransportError("bad protocol")) == "PROTOCOL_MISMATCH"
    assert grpc_failure_reason_code(RuntimeError("TLS certificate verify failed")) == "TLS_FAILED"
    assert grpc_failure_reason_code(RuntimeError("stream ended")) == "GRPC_UNAVAILABLE"
    assert grpc_failure_reason_code(RuntimeError("unclassified")) == "UNKNOWN"


def test_12e_stale_request_iterator_cannot_consume_reconnected_outbox_item(tmp_path):
    """C02 regression: only the current connection epoch can drain control work."""
    config = _config(tmp_path, transport="grpc")
    transport = GrpcStreamControlTransport(
        target="localhost:12345",
        data_client=_NullData(),
        state_store=WorkerStateStore(config.state_path, config.token_path),
        jobs=LocalJobStore(config.jobs_dir),
        worker_id="wrk_0123456789abcdef",
        instance_id="inst_0123456789abcdef",
        config=config,
        build_heartbeat=lambda: {"active_jobs": [], "providers": []},
    )
    with transport._sequence_lock:
        transport._active_request_epoch = 2

    assert not transport._is_active_request_epoch(1)
    assert transport._is_active_request_epoch(2)
    transport._deactivate_request_epoch(2)
    assert not transport._is_active_request_epoch(2)


def test_c14_interrupted_source_body_resumes_from_durable_part(tmp_path, monkeypatch):
    boundary = 1024 * 1024
    payload = b"a" * boundary + b"def"
    requests: list[httpx.Request] = []

    class BrokenBody(httpx.SyncByteStream):
        def __iter__(self):
            yield payload[:boundary]
            raise httpx.ReadError("simulated mid-source reset")

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(200, stream=BrokenBody(), request=request)
        assert request.headers["range"] == f"bytes={boundary}-"
        return httpx.Response(
            206,
            content=payload[boundary:],
            headers={
                "Content-Range": f"bytes {boundary}-{len(payload) - 1}/{len(payload)}"
            },
            request=request,
        )

    client = CenterClient(
        "https://center.invalid",
        transport=httpx.MockTransport(handler),
    )

    class Jobs:
        updates: list[dict] = []

        def update(self, _job_id, _attempt_id, **fields):
            self.updates.append(fields)

    class Outbox:
        events: list[tuple[str, dict]] = []

        def append(self, event_type, event):
            self.events.append((event_type, event))

    worker = WorkerAgent.__new__(WorkerAgent)
    worker.client = client
    worker.jobs = Jobs()
    worker._stop = threading.Event()
    monkeypatch.setattr(agent_module, "backoff_delays", lambda: iter((0.0,)))
    monkeypatch.setattr(
        agent_module.package_io,
        "verify_and_unpack",
        lambda **_kwargs: {
            "files": 1,
            "bytes": len(payload),
            "manifest": {"manifest_version": 1},
        },
    )
    job_dir = tmp_path / "job"
    context = {
        "job_id": "job_0123456789abcdef",
        "attempt_id": "att_0123456789abcdef",
        "execution_token": "token",
        "outbox": Outbox(),
    }
    assignment = {
        "job_type": "test_pipeline_v1",
        "package": {
            "package_id": "pkg_0123456789abcdef",
            "compression": "gzip",
            "sha256": "a" * 64,
        },
    }

    worker._download_and_verify(assignment, context, job_dir)

    archive = job_dir / "source" / "pkg_0123456789abcdef.tar.gz"
    assert archive.read_bytes() == payload
    assert len(requests) == 2
    assert worker.jobs.updates[-1]["local_state"] == "verified"
    assert context["outbox"].events[-1][0] == "source_verified"
