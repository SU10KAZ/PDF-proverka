"""12E reliability seams: durable diagnostics and safe recovery observability.

The process-level Gateway/Agent chaos cases reuse the real isolated grpcio
vertical-slice tests in ``test_agent_grpc_client_12c``.  These tests cover the
new 12E operator surface and the race between a recovery epoch and a persisted
diagnostic update.
"""
from __future__ import annotations

import json
import queue
import random
import threading
from pathlib import Path

import httpx
import pytest

from audit_worker import agent as agent_module
from audit_worker import client as client_module
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
from audit_worker import grpc_transport as grpc_transport_module
from audit_worker.local_store import LocalJobStore, WorkerStateStore
from audit_worker import uploader
from audit_worker.uploader import UploadFailed, upload_result
from contracts.agent_stream.v1 import adapters
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
    assert (
        grpc_failure_reason_code(RuntimeError("CENTER_DB_UNAVAILABLE"))
        == "CENTER_DB_UNAVAILABLE"
    )
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


def test_c34_twelve_grpc_failures_never_fall_back_to_polling(tmp_path, monkeypatch):
    """C34: repeated stream failure keeps one explicit gRPC owner."""
    class DataPlane(_NullData):
        polling_calls = 0

        def next_job(self, *_args, **_kwargs):
            self.polling_calls += 1
            raise AssertionError("automatic polling fallback was invoked")

    class FailedChannel:
        def close(self):
            return None

    class FailedReady:
        def __init__(self, transport):
            self.transport = transport

        def result(self, timeout):
            if self.transport._connection_attempts >= 12:
                self.transport._stop.set()
            raise RuntimeError("simulated GRPC_UNAVAILABLE")

    config = _config(tmp_path, transport="grpc")
    config.grpc_reconnect_min_delay_sec = 0.0
    config.grpc_reconnect_max_delay_sec = 0.0
    config.grpc_reconnect_jitter = 0.0
    data = DataPlane()
    transport = GrpcStreamControlTransport(
        target="localhost:12345",
        data_client=data,
        state_store=WorkerStateStore(config.state_path, config.token_path),
        jobs=LocalJobStore(config.jobs_dir),
        worker_id="wrk_0123456789abcdef",
        instance_id="inst_0123456789abcdef",
        config=config,
        build_heartbeat=lambda: {"active_jobs": [], "providers": []},
    )
    monkeypatch.setattr(transport, "_open_channel", lambda: FailedChannel())
    monkeypatch.setattr(
        grpc_transport_module.grpc,
        "channel_ready_future",
        lambda _channel: FailedReady(transport),
    )

    transport._connection_loop()

    assert transport._connection_attempts == 12
    assert transport.metrics_snapshot()["grpc_connect_attempts"] == 12
    assert transport.metrics_snapshot()["grpc_reconnects"] == 11
    assert data.polling_calls == 0
    state = transport.state_store.load()["runtime_diagnostics"]
    assert state["grpc_connection_state"] == "disconnected"
    assert state["last_disconnect_reason"] == "GRPC_UNAVAILABLE"


def test_c36_twenty_reconnect_candidates_use_bounded_jitter(monkeypatch):
    """C36: a reconnect herd does not share one deterministic retry instant."""
    monkeypatch.setattr(client_module, "random", random.Random(12036))
    candidates = [
        client_module.backoff_delays(start=1.0, cap=30.0, jitter=0.2)
        for _ in range(20)
    ]
    first = [next(delays) for delays in candidates]
    second = [next(delays) for delays in candidates]
    third = [next(delays) for delays in candidates]

    assert all(0.8 <= value <= 1.2 for value in first)
    assert all(1.6 <= value <= 2.4 for value in second)
    assert all(3.2 <= value <= 4.8 for value in third)
    assert len({round(value, 3) for value in first}) >= 15
    assert len({round(value, 3) for value in second}) >= 15
    assert len({round(value, 3) for value in third}) >= 15


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


def test_c16_result_upload_resumes_same_session_after_data_plane_interruption(
    tmp_path, monkeypatch
):
    archive = tmp_path / "result.tar.gz"
    archive.write_bytes(b"abcdefgh")

    class Client:
        received: set[int] = set()
        create_calls = 0
        chunk_calls: list[int] = []
        outage = True

        def create_upload(self, _payload, _token):
            self.create_calls += 1
            return {
                "upload_id": "upl_0123456789abcdef",
                "chunk_size": 4,
                "chunks_total": 2,
                "received_chunks": sorted(self.received),
            }

        def put_chunk(self, _upload_id, idx, _data, _sha, execution_token=""):
            self.chunk_calls.append(idx)
            if idx == 1 and self.outage:
                raise httpx.ConnectError("isolated data plane interrupted")
            self.received.add(idx)
            return {"status": "accepted"}

        def complete_upload(
            self, _upload_id, _payload, _token, *,
            routing_plan_hash="", pipeline_revision="",
        ):
            assert self.received == {0, 1}
            # 12I.2: маршрут и ревизия доезжают до транспорта из СОХРАНЁННЫХ
            # метаданных попытки. Здесь их не передают, и это правильный
            # контроль: аргументы обязаны быть необязательными, иначе досылка
            # старых попыток сломалась бы на сигнатуре.
            self.completed_with = {
                "routing_plan_hash": routing_plan_hash,
                "pipeline_revision": pipeline_revision,
            }
            return {"state": "completed", "retention_until": 1234.0}

    client = Client()
    monkeypatch.setattr(uploader.time, "sleep", lambda _seconds: None)
    arguments = {
        "client": client,
        "job_id": "job_0123456789abcdef",
        "attempt_id": "att_0123456789abcdef",
        "archive": archive,
        "execution_token": "token",
        "uploads_dir": tmp_path / "uploads",
    }

    with pytest.raises(UploadFailed):
        upload_result(**arguments)
    assert client.received == {0}
    assert client.chunk_calls == [0, 1, 1, 1]

    client.outage = False
    result = upload_result(**arguments)

    assert result["retention_until"] == 1234.0
    assert client.completed_with == {"routing_plan_hash": "", "pipeline_revision": ""}
    assert client.create_calls == 2
    assert client.chunk_calls == [0, 1, 1, 1, 1]


def test_c41_duplicate_result_ack_never_blocks_grpc_reader(tmp_path):
    transport = GrpcStreamControlTransport(
        target="localhost:12345",
        data_client=_NullData(),
        state_store=WorkerStateStore(tmp_path / "state.json", tmp_path / "token"),
        jobs=LocalJobStore(tmp_path / "jobs"),
        worker_id="wrk_0123456789abcdef",
        instance_id="inst_0123456789abcdef",
        config=_config(tmp_path, transport="grpc"),
        build_heartbeat=lambda: {"active_jobs": [], "providers": []},
    )
    transport._connection_id = "gconn_0123456789abcdef"
    correlation = "corr_0123456789abcdef"
    waiter: queue.Queue = queue.Queue(maxsize=1)
    transport._waiters[correlation] = waiter

    def response(sequence: int) -> stream_pb.CenterToAgent:
        return stream_pb.CenterToAgent(
            protocol_version=1,
            message_id=f"msg_{sequence:016d}",
            correlation_id=correlation,
            worker_id=transport.worker_id,
            connection_id=transport._connection_id,
            stream_sequence=sequence,
            result_ack=stream_pb.ResultAck(
                job_id="job_0123456789abcdef",
                attempt_id="att_0123456789abcdef",
                result_sha256="a" * 64,
                retention_until=adapters.timestamp_from_epoch(1234.0),
            ),
        )

    transport._handle_response(response(1))
    transport._handle_response(response(2))

    accepted = waiter.get_nowait()
    assert accepted.result_ack.retention_until.seconds == 1234
    assert waiter.empty()
    assert transport._center_stream_sequence == 2
