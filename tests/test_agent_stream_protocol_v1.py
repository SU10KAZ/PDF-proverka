from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
from google.protobuf import descriptor_pb2

from backend.app.models.distributed_workers import JobState, WorkerEventType, WorkerState
from contracts.agent_stream.v1 import adapters
from contracts.agent_stream.v1 import agent_stream_pb2 as stream_pb
from contracts.agent_stream.v1 import common_pb2 as common_pb


ROOT = Path(__file__).resolve().parents[1]
GOLDEN = json.loads(
    (ROOT / "tests/fixtures/agent_stream_v1_golden.json").read_text(encoding="utf-8")
)
SNAPSHOT = json.loads(
    (ROOT / "contracts/agent_stream/v1/descriptor_snapshot.json").read_text(
        encoding="utf-8"
    )
)
DESCRIPTOR_PATH = ROOT / "contracts/agent_stream/v1/agent_stream_v1.desc"
SHA = "a" * 64


def _roundtrip(message):
    restored = type(message)()
    restored.ParseFromString(message.SerializeToString())
    return restored


def _capabilities():
    return {
        "capabilities_revision": "caps-7",
        "provider_policy_version": 4,
        "provider_policy_sha256": "policy-sha",
        "job_types": ["audit_pipeline_v1", "test_pipeline_v1"],
        "compressions": ["gzip"],
        "routing_compatibility": ["routing_plan_v1"],
        "provider_capabilities": {
            "claude": ["strong_audit"],
            "codex": ["cheap_review"],
            "openrouter": ["visual_reasoning"],
        },
        "routing_plan_v1": True,
        "max_verified_slots": 2,
        "max_package_bytes": 400_000_000,
    }


def _job_assignment():
    routing = {
        "schema_version": 1,
        "routing_plan_id": "route-1",
        "routing_plan_hash": "route-sha",
        "actions": [],
    }
    return {
        "job_id": "job-test",
        "attempt_id": "attempt-test",
        "attempt_no": 2,
        "assignment_generation": 3,
        "worker_id": "w-test-12a",
        "assigned_at": 1_700_000_000.0,
        "assign_ttl_sec": 60,
        "job_type": "audit_pipeline_v1",
        "project_id": "project-test",
        "version_id": "version-test",
        # Transport credential is deliberately not represented in protobuf.
        "execution_token": "not-forwarded",
        "params": {
            "pipeline_revision": "rev-test",
            "routing_plan": routing,
            "provider_requirement": {
                "provider": "codex",
                "capability": "cheap_review",
                "allowed_stages": ["findings_review"],
                "max_inferences": 0,
            },
        },
        "package": {
            "package_id": "source-test",
            "package_type": "source",
            "url": "https://center.invalid/secret-bearing-runtime-url",
            "size_bytes": 300_000_000,
            "sha256": SHA,
            "compression": "gzip",
            "manifest_version": 1,
        },
        "event_start_seq": 1,
    }


def _event_payload(first=41):
    return {
        "job_id": "job-test",
        "attempt_id": "attempt-test",
        "first_seq": first,
        "count": 2,
        "events": [
            {
                "seq": first,
                "event_id": "event-1",
                "event_type": "stage_started",
                "occurred_at": 1_700_000_001.0,
                "schema_version": 1,
                "payload": {"stage": "crop_blocks"},
            },
            {
                "seq": first + 1,
                "event_id": "event-2",
                "event_type": "stage_progress",
                "occurred_at": 1_700_000_002.0,
                "schema_version": 1,
                "payload": {"current": 1, "total": 3},
            },
        ],
    }


def test_a_proto_files_compile_and_descriptor_is_reproducible(tmp_path):
    output = tmp_path / "contract.desc"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{ROOT}",
            f"--descriptor_set_out={output}",
            "--include_imports",
            "contracts/agent_stream/v1/common.proto",
            "contracts/agent_stream/v1/agent_stream.proto",
        ],
        cwd=ROOT,
        check=True,
    )
    assert output.read_bytes() == DESCRIPTOR_PATH.read_bytes()


def test_b_generated_python_and_12b_grpc_stub_are_available():
    assert common_pb.DESCRIPTOR.package == "auditmanager.agent_stream.v1"
    assert stream_pb.DESCRIPTOR.services_by_name["AgentStreamService"]
    stub_path = ROOT / "contracts/agent_stream/v1/agent_stream_pb2_grpc.py"
    assert stub_path.is_file()
    from contracts.agent_stream.v1 import agent_stream_pb2_grpc as stream_grpc

    assert stream_grpc.AgentStreamServiceStub
    assert stream_grpc.AgentStreamServiceServicer


def test_c_package_and_service_are_versioned_bidi_v1():
    service = stream_pb.DESCRIPTOR.services_by_name["AgentStreamService"]
    method = service.methods_by_name["Connect"]
    assert service.full_name == "auditmanager.agent_stream.v1.AgentStreamService"
    assert method.client_streaming and method.server_streaming


def test_d_agent_envelope_oneof_roundtrip():
    envelope = stream_pb.AgentToCenter(
        protocol_version=1,
        message_id="m-1",
        worker_id="w-1",
        connection_id="c-1",
        stream_sequence=8,
        heartbeat=stream_pb.Heartbeat(worker_id="w-1"),
    )
    restored = _roundtrip(envelope)
    assert restored.WhichOneof("payload") == "heartbeat"
    assert restored.stream_sequence == 8


def test_e_center_envelope_oneof_roundtrip():
    envelope = stream_pb.CenterToAgent(
        protocol_version=1,
        message_id="m-2",
        worker_id="w-1",
        connection_id="c-1",
        stream_sequence=9,
        event_ack=stream_pb.EventAck(highest_contiguous_sequence=42),
    )
    restored = _roundtrip(envelope)
    assert restored.WhichOneof("payload") == "event_ack"
    assert restored.event_ack.highest_contiguous_sequence == 42


@pytest.mark.parametrize("fixture_name", ["agent_hello", "center_hello"])
def test_f_g_hello_golden_roundtrip(fixture_name):
    fixture = GOLDEN[fixture_name]
    cls = stream_pb.AgentHello if fixture_name == "agent_hello" else stream_pb.CenterHello
    message = cls(**fixture)
    restored = _roundtrip(message)
    for key, value in fixture.items():
        assert getattr(restored, key) == value


def test_h_i_capabilities_roundtrip_preserves_all_providers():
    providers = [
        {"provider": "claude", "auth_state": "logged_in", "quota_state": "ready"},
        {"provider": "codex", "auth_state": "logged_in", "quota_state": "limited"},
        {"provider": "openrouter", "auth_state": "logged_out", "quota_state": "ready"},
    ]
    proto = adapters.capabilities_from_domain(_capabilities(), provider_snapshots=providers)
    restored = adapters.capabilities_to_domain(_roundtrip(proto))
    assert restored["provider_capabilities"] == _capabilities()["provider_capabilities"]
    assert restored["provider_policy_version"] == 4
    assert [item.provider for item in proto.providers] == ["claude", "codex", "openrouter"]
    assert proto.providers[2].availability == common_pb.PROVIDER_AVAILABILITY_ACTION_REQUIRED


def test_j_k_job_offer_roundtrip_preserves_routing_identity_and_domain_fields():
    source = _job_assignment()
    proto = adapters.job_offer_from_http(source)
    restored = adapters.job_offer_to_domain(_roundtrip(proto))
    assert proto.routing_plan.routing_plan_hash == "route-sha"
    assert adapters.canonical_json_value(proto.routing_plan.canonical_plan) == source["params"]["routing_plan"]
    for field in ("job_id", "attempt_id", "attempt_no", "project_id", "version_id", "job_type", "event_start_seq"):
        assert restored[field] == source[field]
    assert restored["params"] == source["params"]
    assert restored["package"]["package_id"] == source["package"]["package_id"]
    assert "url" not in restored["package"]


def test_l_job_accept_http_mapping():
    accept = adapters.job_accept_from_http(
        {
            "attempt_id": "attempt-test",
            "accepted_at": 1_700_000_010.0,
            "source_verified": {"sha256_ok": True, "manifest_version": 1},
            "planned_stages": ["crop_blocks"],
        },
        job_id="job-test",
        worker_id="w-test-12a",
        routing_plan_hash="route-sha",
        execution_revision="rev-test",
    )
    assert accept.source_sha256_verified
    assert accept.routing_plan_hash == "route-sha"
    assert accept.planned_stages == ["crop_blocks"]


def test_m_job_decline_has_typed_reason_and_safe_detail():
    decline = adapters.job_decline_from_http(
        {
            "attempt_id": "attempt-test",
            "reason_code": "provider_unavailable",
            "reason": "quota unavailable",
            "declined_at": 1_700_000_011.0,
        },
        job_id="job-test",
        worker_id="w-test-12a",
    )
    assert decline.reason == stream_pb.JOB_DECLINE_REASON_PROVIDER_UNAVAILABLE
    assert decline.safe_detail == "quota unavailable"


def test_n_heartbeat_http_parity_and_multiple_active_attempts():
    payload = {
        "instance_id": "instance-test",
        "sent_at": 1_700_000_020.0,
        "worker_state": "busy",
        "configured_max_slots": 2,
        "calculated_free_slots": 0,
        "active_jobs": [
            {"job_id": "j-1", "attempt_id": "a-1", "project_id": "p", "stage": "s1", "last_event_seq": 5, "started_at": 1_700_000_000.0},
            {"job_id": "j-2", "attempt_id": "a-2", "project_id": "p", "stage": "s2", "last_event_seq": 7, "started_at": 1_700_000_005.0},
        ],
        "resource_snapshot": {"at": 1_700_000_019.0},
        "executor": {"status": "online"},
        "disk": {"total_bytes": 1000, "free_bytes": 400, "jobs_bytes": 500, "unconfirmed_results_bytes": 100, "level": "ok"},
        "active_local_jobs": 2,
        "running_processes": 2,
        "providers": [],
    }
    proto = adapters.heartbeat_from_http(payload, worker_id="w-test", connection_id="c-test")
    restored = adapters.heartbeat_to_http(_roundtrip(proto), instance_id="instance-test")
    assert restored["worker_state"] == payload["worker_state"]
    assert restored["configured_max_slots"] == 2
    assert restored["calculated_free_slots"] == 0
    assert [(x["job_id"], x["attempt_id"], x["last_event_seq"]) for x in restored["active_jobs"]] == [("j-1", "a-1", 5), ("j-2", "a-2", 7)]


def test_o_progress_is_typed_observational_and_roundtrips():
    payload = {"job_id": "j", "attempt_id": "a", "stage_id": "crop_blocks", "action_id": "act", "status": "running", "current": 2, "total": 4, "percent": 50.0, "message": "half", "observed_at": 1_700_000_030.0}
    restored = adapters.progress_to_domain(_roundtrip(adapters.progress_from_http(payload)))
    assert restored == payload


def test_p_event_batch_requires_contiguous_outbox_sequence_and_roundtrips():
    payload = _event_payload()
    proto = adapters.event_batch_from_http(payload, worker_id="w-test")
    restored = adapters.event_batch_to_http(_roundtrip(proto))
    assert restored == payload
    broken = _event_payload()
    broken["events"][1]["seq"] = 99
    with pytest.raises(adapters.ContractViolation, match="contiguous"):
        adapters.event_batch_from_http(broken, worker_id="w-test")


def test_q_r_event_ack_contiguous_cursor_and_duplicate_semantics():
    response = {"last_seen_seq": 42, "accepted": 0, "skipped_duplicates": 2, "replayed": True}
    proto = adapters.event_ack_from_http(response, job_id="job-test", attempt_id="attempt-test")
    restored = adapters.event_ack_to_http(_roundtrip(proto))
    assert restored == response
    assert proto.highest_contiguous_sequence == 42


def test_s_cancel_command_http_parity_and_identity():
    command = {"command_id": "cmd-1", "job_id": "j", "attempt_id": "a", "created_at": 1_700_000_040.0, "expires_at": 1_700_000_070.0, "payload": {"reason": "operator", "grace_period_sec": 30}}
    proto = adapters.cancel_command_from_http(command)
    assert (proto.command_id, proto.job_id, proto.attempt_id) == ("cmd-1", "j", "a")
    assert proto.safe_reason == "operator"


@pytest.mark.parametrize("stage", ["received", "cancel_in_progress", "cancelled", "already_finished", "not_found", "rejected"])
def test_t_cancel_ack_stages_are_typed(stage):
    ack = adapters.cancel_ack_from_http(
        {"stage": stage, "acknowledged_at": 1_700_000_041.0},
        command_id="cmd-1", job_id="j", attempt_id="a",
    )
    assert stream_pb.CancelAckStage.Name(ack.stage) == "CANCEL_ACK_STAGE_" + stage.upper()


def test_u_result_ready_metadata_parity_has_no_package_bytes():
    payload = {"job_id": "j", "attempt_id": "a", "upload_id": "u", "package_type": "result", "expected_size": 300_000_000, "expected_hash": SHA, "tree_hash": "tree", "manifest_hash": "manifest", "manifest_version": 1, "compression": "gzip", "chunk_size_bytes": 8_000_000, "routing_plan_hash": "route", "pipeline_revision": "rev", "stage_status_summary": {"crop_blocks": "completed"}, "provider_action_ledger_summary": {"actions": 0}, "ready_at": 1_700_000_050.0}
    proto = adapters.result_ready_from_domain(payload)
    restored = adapters.result_ready_to_domain(_roundtrip(proto))
    assert restored["package"]["expected_size"] == payload["expected_size"]
    assert restored["package"]["expected_hash"] == SHA
    assert restored["routing_plan_hash"] == "route"
    assert len(proto.SerializeToString()) < 2048


def test_v_result_ack_starts_retention_only_after_validated_acceptance():
    response = {"state": "completed", "validation": {"ok": True}, "server_time": 1_700_000_060.0, "retention_until": 1_702_592_060.0}
    proto = adapters.result_ack_from_http(response, job_id="j", attempt_id="a", result_sha256=SHA)
    restored = adapters.result_ack_to_http(_roundtrip(proto))
    assert proto.validation_status == stream_pb.RESULT_VALIDATION_STATUS_ACCEPTED
    assert restored["state"] == "completed"
    assert restored["retention_until"] == response["retention_until"]


def test_w_result_rejected_reason_is_typed():
    rejection = stream_pb.ResultRejected(job_id="j", attempt_id="a", result_sha256=SHA, reason=stream_pb.RESULT_REJECT_REASON_MANIFEST_INVALID, safe_detail="invalid manifest", retryable=False)
    assert _roundtrip(rejection).reason == stream_pb.RESULT_REJECT_REASON_MANIFEST_INVALID


def test_x_package_descriptor_contains_metadata_not_bytes_or_url():
    descriptor = common_pb.PackageTransferDescriptor.DESCRIPTOR
    assert {field.name for field in descriptor.fields}.isdisjoint({"bytes", "content", "data", "url", "endpoint", "credentials"})
    proto = adapters.package_descriptor_from_http({"package_id": "p", "url": "https://secret.invalid", "package_bytes": b"PDF", "size_bytes": 3, "sha256": SHA}, direction=common_pb.PACKAGE_DIRECTION_CENTER_TO_AGENT)
    assert proto.transfer_id == "p"
    assert "secret.invalid" not in str(proto)


def test_y_large_content_cannot_enter_control_plane():
    with pytest.raises(adapters.ContractViolation, match="limit|bound"):
        adapters.canonical_json_message({"document": "x" * (adapters.MAX_CANONICAL_JSON_BYTES + 1)}, schema="bad", schema_version=1)
    with pytest.raises(adapters.ContractViolation, match="canonical JSON"):
        adapters.canonical_json_message({"pdf": b"%PDF"}, schema="bad", schema_version=1)


def test_z_no_credential_fields_in_proto_descriptors():
    forbidden = ("password", "api_key", "apikey", "oauth", "access_token", "refresh_token", "worker_token", "execution_token", "registration_token", "claim_secret", "private_key", "auth_url", "device_code")
    for file_descriptor in (common_pb.DESCRIPTOR, stream_pb.DESCRIPTOR):
        for message in file_descriptor.message_types_by_name.values():
            for field in message.fields:
                assert not any(part in field.name.lower() for part in forbidden), (message.full_name, field.name)


def test_aa_http_heartbeat_parity_uses_current_domain_enums():
    assert {value.value for value in WorkerState} == {common_pb.WorkerState.Name(number).removeprefix("WORKER_STATE_").lower() for number in common_pb.WorkerState.values() if number}


def test_ab_http_job_parity_excludes_transport_auth_only():
    source = _job_assignment()
    restored = adapters.job_offer_to_domain(adapters.job_offer_from_http(source))
    assert "execution_token" not in restored
    assert restored["params"] == source["params"]
    assert restored["package"]["sha256"] == source["package"]["sha256"]


def test_ac_http_event_type_parity_is_exact():
    proto_names = {stream_pb.WorkerEventType.Name(number).removeprefix("WORKER_EVENT_TYPE_").lower() for number in stream_pb.WorkerEventType.values() if number}
    assert proto_names == {value.value for value in WorkerEventType}


def test_ad_ae_af_http_cancel_result_and_ack_parity():
    cancel = adapters.cancel_command_from_http({"command_id": "c", "job_id": "j", "attempt_id": "a", "payload": {"reason": "r"}})
    result = adapters.result_ready_from_domain({"job_id": "j", "attempt_id": "a", "upload_id": "u", "expected_hash": SHA})
    ack = adapters.result_ack_from_http({"server_time": 100.0, "retention_until": 200.0}, job_id="j", attempt_id="a", result_sha256=SHA)
    assert (cancel.job_id, result.job_id, ack.job_id) == ("j", "j", "j")
    assert (cancel.attempt_id, result.attempt_id, ack.attempt_id) == ("a", "a", "a")


def test_ag_reconnect_resume_metadata_has_per_attempt_contiguous_cursor():
    hello = stream_pb.AgentHello(worker_id="w", connection_epoch=8, active_attempts=[common_pb.AttemptRef(job_id="j", attempt_id="a", last_written_event_sequence=50, last_acked_event_sequence=42)], event_cursors=[common_pb.EventCursor(job_id="j", attempt_id="a", highest_contiguous_sequence=42)])
    center = stream_pb.CenterHello(resume_cursors=[common_pb.EventCursor(job_id="j", attempt_id="a", highest_contiguous_sequence=42)])
    assert _roundtrip(hello).event_cursors[0].highest_contiguous_sequence == 42
    assert _roundtrip(center).resume_cursors[0].highest_contiguous_sequence == 42


def test_ah_hello_supports_multiple_active_attempts():
    hello = stream_pb.AgentHello(active_attempts=[common_pb.AttemptRef(job_id="j1", attempt_id="a1"), common_pb.AttemptRef(job_id="j2", attempt_id="a2")], max_slots=2)
    assert len(_roundtrip(hello).active_attempts) == 2


def test_ai_duplicate_connection_policy_is_newer_epoch_supersedes():
    assert adapters.resolve_connection_epoch(None, 1) == "accept"
    assert adapters.resolve_connection_epoch(7, 8) == "supersede_old"
    with pytest.raises(adapters.ContractViolation, match="stale"):
        adapters.resolve_connection_epoch(8, 8)
    assert stream_pb.DUPLICATE_CONNECTION_POLICY_NEWER_EPOCH_SUPERSEDES == 1


def test_aj_protocol_negotiation_rejects_unknown_major():
    assert adapters.negotiate_protocol([1]) == 1
    with pytest.raises(adapters.ContractViolation, match="unsupported"):
        adapters.negotiate_protocol([2, 3])


def test_ak_all_contract_enums_have_unspecified_zero():
    files = (common_pb.DESCRIPTOR, stream_pb.DESCRIPTOR)
    for file_descriptor in files:
        for enum in file_descriptor.enum_types_by_name.values():
            assert enum.values[0].number == 0
            assert enum.values[0].name.endswith("_UNSPECIFIED")


def _contract_file_protos():
    descriptor_set = descriptor_pb2.FileDescriptorSet()
    descriptor_set.ParseFromString(DESCRIPTOR_PATH.read_bytes())
    return [item for item in descriptor_set.file if item.package == "auditmanager.agent_stream.v1"]


def test_al_messages_reserve_field_numbers_for_evolution():
    messages = [message for file in _contract_file_protos() for message in file.message_type]
    assert messages
    assert all(message.reserved_range for message in messages)


def test_am_control_message_and_event_count_guards():
    envelope = stream_pb.AgentToCenter(message_id="x" * adapters.MAX_CONTROL_MESSAGE_BYTES)
    with pytest.raises(adapters.ContractViolation, match="size"):
        adapters.validate_control_message(envelope)
    payload = _event_payload()
    payload["events"] = payload["events"] * 129
    with pytest.raises(adapters.ContractViolation, match="count"):
        adapters.event_batch_from_http(payload, worker_id="w")


def test_an_no_arbitrary_shell_or_admin_command_message():
    names = {message.name.lower() for file in _contract_file_protos() for message in file.message_type}
    forbidden = {"runshellcommand", "shellcommand", "execcommand", "evalcommand", "installpackage", "editarbitraryfile", "restartservice"}
    assert names.isdisjoint(forbidden)
    proto_text = (ROOT / "contracts/agent_stream/v1/agent_stream.proto").read_text(encoding="utf-8").lower()
    assert "run_shell" not in proto_text and "exec(" not in proto_text and "eval(" not in proto_text


def test_ao_descriptor_snapshot_critical_numbers_and_service_shape():
    assert SNAPSHOT["package"] == stream_pb.DESCRIPTOR.package
    service = stream_pb.DESCRIPTOR.services_by_name[SNAPSHOT["service"]]
    method = service.methods_by_name[SNAPSHOT["rpc"]]
    assert method.client_streaming == SNAPSHOT["client_streaming"]
    assert method.server_streaming == SNAPSHOT["server_streaming"]
    for message_name, expected in SNAPSHOT["critical_fields"].items():
        descriptor = stream_pb.DESCRIPTOR.message_types_by_name.get(message_name) or common_pb.DESCRIPTOR.message_types_by_name[message_name]
        assert {name: descriptor.fields_by_name[name].number for name in expected} == expected


def test_job_state_proto_mapping_is_exact_current_domain_model():
    proto_names = {common_pb.JobState.Name(number).removeprefix("JOB_STATE_").lower() for number in common_pb.JobState.values() if number}
    assert proto_names == {value.value for value in JobState}


def test_canonical_json_is_stable_hash_verified_and_secret_safe():
    first = adapters.canonical_json_message({"b": 2, "a": 1}, schema="test", schema_version=1)
    second = adapters.canonical_json_message({"a": 1, "b": 2}, schema="test", schema_version=1)
    assert first.canonical_json == second.canonical_json
    assert first.sha256 == hashlib.sha256(first.canonical_json).hexdigest()
    damaged = common_pb.CanonicalJson(schema="test", schema_version=1, canonical_json=b"{}", sha256="bad")
    with pytest.raises(adapters.ContractViolation, match="mismatch"):
        adapters.canonical_json_value(damaged)
    with pytest.raises(adapters.ContractViolation, match="secret-bearing"):
        adapters.canonical_json_message({"api_key": "forbidden"}, schema="test", schema_version=1)
    with pytest.raises(adapters.ContractViolation, match="executable/admin"):
        adapters.canonical_json_message(
            {"command": "echo must-not-cross-control-plane"},
            schema="audit_worker.job_params",
            schema_version=1,
        )
    # Exact forbidden-key matching does not reject the typed business identity.
    assert adapters.canonical_json_message(
        {"command_id": "cancel-1"}, schema="audit_worker.event_payload", schema_version=1
    )


def test_polling_runtime_requirements_and_transport_are_unchanged():
    for filename in ("requirements.txt", "requirements-worker.txt"):
        text = (ROOT / filename).read_text(encoding="utf-8").lower()
        assert "grpcio" not in text and "grpc_tools" not in text
    agent = (ROOT / "audit_worker/agent.py").read_text(encoding="utf-8")
    assert "AgentStreamService" not in agent and "grpc." not in agent
    assert ":8443" not in agent


def test_descriptor_set_digest_is_pinned():
    # Обновлено 19.08.2026 вместе с расширением ProviderCapabilitySnapshot
    # (окна лимита 20, код причины 21, стабильность источника 22, признак
    # наличия остатка 23). Дайджест прикреплён намеренно: он ловит СЛУЧАЙНОЕ
    # изменение провода, поэтому меняться обязан только вместе с осознанной
    # правкой контракта и её тестами.
    assert hashlib.sha256(DESCRIPTOR_PATH.read_bytes()).hexdigest() == "f7e4bbe6887c4f394859d66fc4a9584958b4d551327ac01a392150ec505642ea"

def test_heartbeat_roundtrip_preserves_resource_telemetry():
    from contracts.agent_stream.v1 import adapters

    payload = {
        "instance_id": "inst",
        "worker_state": "idle",
        "configured_max_slots": 1,
        "calculated_free_slots": 1,
        "active_jobs": [],
        "resource_snapshot": {
            "at": 100.0,
            "ram": {"total_gb": 32, "available_gb": 16, "used_pct": 50.0},
            "cpu": {"cores": 8, "la1": 0.5, "la5": 0.4, "utilization_pct": 12.0},
            "gpu": {"utilization_pct": 3.0, "used_gb": 1.0, "total_gb": 8.0},
        },
        "providers": [{
            "provider": "codex",
            "installation_status": "installed",
            "auth_state": "logged_in",
            "policy_state": "allowed",
            "inference_allowed": True,
            "credential_present": True,
            "observed_at": 100.0,
            "quota": {
                "provider": "codex",
                "quota_state": "ready",
                "observed_at": 100.0,
                "source": "official_app_server_rpc",
                "confidence": "high",
                "estimated_remaining_pct": 55.0,
                "raw_remaining_supported": True,
            },
        }],
        "disk": {"level": "ok"},
        "executor": {"status": "online"},
    }
    message = adapters.heartbeat_from_http(payload, worker_id="wrk", connection_id="conn")
    roundtrip = adapters.heartbeat_to_http(message, instance_id="inst")
    snap = roundtrip["resource_snapshot"]
    assert snap["cpu"]["utilization_pct"] == 12.0
    assert snap["ram"]["used_pct"] == 50.0
    assert snap["gpu"]["utilization_pct"] == 3.0

