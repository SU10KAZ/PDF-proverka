"""11K one-click bootstrap gates A–BG (all provider calls are fake/zero-inference)."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.app.services.distributed_workers import database, registration_service, repositories
from backend.app.services.distributed_workers.settings import get_settings
from backend.app.services.worker_bootstrap import store
from backend.app.services.worker_bootstrap.manager import BootstrapManager
from backend.app.services.worker_bootstrap.models import (
    BootstrapOperation,
    BootstrapRequest,
)
from backend.app.services.worker_bootstrap.remote import ActionRequired, RemoteFailure
from backend.app.services.worker_bootstrap.security import HostKeyMismatch, redact


TEST_REGISTRATION_SECRET = "wbt_TEST_REGISTRATION_TOKEN_VALUE_123456"
TEST_OPENROUTER_SECRET = "sk-or-v1-TEST-OPENROUTER-DO-NOT-USE-123456789"
TEST_SSH_PASSWORD = "SshPassword-TEST-123456"


@pytest.fixture()
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", "legacy-test-secret-0123456789")
    database.reset_state_for_tests()
    st = get_settings()
    database.ensure_ready(st)
    yield st
    database.reset_state_for_tests()


@pytest.fixture()
def bundle(tmp_path):
    archive = tmp_path / "worker.tar.gz"
    archive.write_bytes(b"deterministic-test-bundle")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest = tmp_path / "worker.manifest.json"
    manifest.write_text(
        json.dumps({"release": "release-11k", "archive_sha256": digest}),
        encoding="utf-8",
    )
    return archive, digest


def request_for(bundle, **overrides):
    archive, digest = bundle
    values = {
        "host": "worker.example.test",
        "port": 22,
        "ssh_user": "auditworker",
        "ssh_auth_ref": "agent",
        "expected_host_fingerprint": "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "install_root": "/srv/audit-worker-test",
        "center_url": "https://center.example.test",
        "display_name": "Clean test worker",
        "max_slots": 1,
        "providers": [],
        "bundle_path": str(archive),
        "bundle_sha256": digest,
    }
    values.update(overrides)
    return BootstrapRequest.model_validate(values)


class FakeCleanRemote:
    """In-memory foreign host. It proves installer logic, not a real VPS."""

    def __init__(self, request, session_id, settings, *, scenario=None):
        self.request = request
        self.session_id = session_id
        self.settings = settings
        self.scenario = scenario if scenario is not None else {}
        self.calls = self.scenario.setdefault("calls", [])
        self.files = self.scenario.setdefault("files", {})
        self.release = self.scenario.get("release")
        self.claim_secret = None
        self.worker_id = self.scenario.get("worker_id")

    def enroll(self):
        self.calls.append("enroll")
        failure = self.scenario.get("enroll_failure")
        if failure:
            raise failure
        return {"strict_host_key_checking": True, "host_key": self.request.expected_host_fingerprint}

    def preflight(self):
        self.calls.append("preflight")
        failure = self.scenario.get("preflight_failure")
        if failure:
            raise failure
        return {"os_id": "debian", "cpu": "4", "ram_mb": "8192", "disk_mb": "50000", "tls_center": "ok"}

    def deploy_release(self, *, archive, manifest):
        self.calls.append("deploy")
        if self.scenario.get("deploy_failure"):
            raise RemoteFailure("self_test_failed", "fake update failed")
        previous = self.release
        self.release = "release-11k"
        self.scenario["release"] = self.release
        return {"release_id": self.release, "previous_release_id": previous, "tree_hash": "f" * 64}

    def configure(self):
        self.calls.append("configure")
        if self.scenario.get("configure_failure"):
            raise RemoteFailure("configuration_failed", "fake configuration failed")
        self.files["worker.env"] = {"center": self.request.center_url, "mode": "0600"}
        return {
            "configured": True,
            "config_mode": "0600",
            "policy_version": 1,
            "policy_sha256": "a" * 64,
            "pipeline_revision": "release-11k",
        }

    def install_services(self):
        self.calls.append("install_services")
        suffix = hashlib.sha256(self.request.install_root.encode()).hexdigest()[:8]
        units = [f"audit-worker-{suffix}-agent.service", f"audit-worker-{suffix}-executor.service"]
        self.files["units"] = units
        return {"installed": True, "enabled": True, "units": units}

    def provider_status(self):
        self.calls.append("provider_status")
        waiting = list(self.scenario.get("provider_action_required", []))
        missing = list(self.scenario.get("provider_missing", []))
        return {
            "providers": {provider: "action_required" for provider in waiting},
            "missing": missing,
            "action_required": waiting,
            "compatible_presets": [] if waiting else ["claude_gpt_codex", "full_codex"],
        }

    def install_provider_cli(self, provider):
        self.calls.append("install_cli:" + provider)
        self.scenario["provider_missing"].remove(provider)
        return {"provider": provider, "installed": True, "version": "pinned"}

    def register(self, registration_token):
        self.calls.append("register")
        if self.worker_id and self.files.get("runtime_token_hash"):
            # Real audit_worker detects its persisted worker token and updates
            # registration instead of creating/claiming another identity.
            return {"worker_id": self.worker_id, "token_stored": True}
        store.consume_registration_token(
            registration_token,
            instance_id=self.request.bootstrap_instance_id,
            settings=self.settings,
        )
        capabilities = {
            "job_types": ["test_pipeline_v1", "audit_pipeline_v1"],
            "compressions": ["gzip"],
            "providers": [provider.value for provider in self.request.providers],
            "routing_compatibility": (
                ["claude_gpt_codex", "codex_exec"]
                if {provider.value for provider in self.request.providers}
                == {"claude", "codex", "openrouter"}
                else []
            ),
            "routing_plan_v1": True,
            "bootstrap_version": "1.0.0",
            "provider_policy_version": 1,
            "provider_policy_sha256": "a" * 64,
            "provider_capabilities": {
                provider.value: ["strong_audit"]
                for provider in self.request.providers
            },
        }
        capabilities.update(self.scenario.get("capabilities_override", {}))
        worker, self.claim_secret, _created = registration_service.register_worker(
            instance_id=self.request.bootstrap_instance_id,
            display_name_hint=self.request.display_name,
            worker_version="11k-test",
            protocol_version=self.settings.protocol_version,
            pipeline_revision=self.scenario.get(
                "registration_pipeline_revision", "release-11k"
            ),
            capabilities=capabilities,
            configured_max_slots_hint=self.request.max_slots,
            settings=self.settings,
        )
        self.worker_id = worker["worker_id"]
        self.scenario["worker_id"] = self.worker_id
        return {"worker_id": self.worker_id, "token_stored": False}

    def claim(self):
        self.calls.append("claim")
        if self.worker_id and self.files.get("runtime_token_hash"):
            return {"worker_id": self.worker_id, "token_stored": True}
        _worker, token = registration_service.claim_token(
            worker_id=self.worker_id,
            claim_secret=self.claim_secret,
            settings=self.settings,
        )
        self.files["runtime_token_hash"] = hashlib.sha256(token.encode()).hexdigest()
        return {"worker_id": self.worker_id, "token_stored": True}

    def start_services(self):
        self.calls.append("start_services")
        if self.scenario.get("delay_heartbeat"):
            return {"started": True}
        from backend.app.services.distributed_workers import worker_registry

        worker_registry.record_heartbeat(
            worker_id=self.worker_id,
            instance_id=self.request.bootstrap_instance_id,
            worker_state="idle",
            configured_max_slots=self.request.max_slots,
            calculated_free_slots=self.request.max_slots,
            active_jobs=[],
            resource_snapshot={"at": time.time()},
            warnings=[],
            settings=self.settings,
        )
        return {"started": True}

    def health(self):
        self.calls.append("health")
        return {
            "release": self.release,
            "units": [
                {"UNIT": "agent", "STATE": "active"},
                {"UNIT": "executor", "STATE": "active"},
            ],
            "outbound_https": True,
            "inbound_runtime_ports": [],
            "fake_job": "acknowledged",
            "heartbeat_revision": self.release,
            "capabilities_advertised": True,
            "token": "present",
        }

    def rollback(self, release_id):
        self.calls.append("rollback")
        previous = self.release
        self.release = release_id or self.scenario.get("previous_release", "old-release")
        self.scenario["release"] = self.release
        return {"release_id": self.release, "previous_release_id": previous}

    def uninstall(self):
        self.calls.append("uninstall")
        return {"uninstalled": True, "data_preserved": True, "provider_auth_preserved": True}


def manager_for(settings, tmp_path, scenario):
    instances = []

    def factory(request, session_id, st):
        remote = FakeCleanRemote(request, session_id, st, scenario=scenario)
        instances.append(remote)
        return remote

    def selftest(worker_id, session_id, st):
        scenario.setdefault("calls", []).append("network_selftest")
        return {
            "job_id": "job_fake_bootstrap",
            "state": "completed",
            "protocol": "outbound_https",
            "real_provider_calls": 0,
        }

    return BootstrapManager(
        settings=settings,
        remote_factory=factory,
        selftest_runner=selftest,
        repo_root=tmp_path,
    ), instances


def test_a_b_c_d_preflight_failures_are_before_mutation(settings, bundle, tmp_path):
    """A unsupported OS, B disk, C SSH, D host mismatch: deploy never starts."""
    for failure in (
        RemoteFailure("unsupported_os", "Ubuntu/Debian required"),
        RemoteFailure("not_enough_disk", "disk < 6000"),
        RemoteFailure("ssh_auth_failed", "permission denied"),
        HostKeyMismatch("host key mismatch"),
    ):
        scenario = {"preflight_failure": failure}
        if isinstance(failure, HostKeyMismatch):
            scenario = {"enroll_failure": failure}
        manager, _ = manager_for(settings, tmp_path, scenario)
        session = manager.create(operation=BootstrapOperation.INSTALL, request=request_for(bundle))
        result = manager.run(session["session_id"])
        assert result["state"] == "failed"
        assert "deploy" not in scenario["calls"]


def test_e_k_m_to_r_ak_to_an_bg_clean_install_and_repeat_is_idempotent(
    settings, bundle, tmp_path
):
    """E,K,M–R,AK–AN,BG: full fake clean install reaches READY once."""
    scenario = {}
    manager, _instances = manager_for(settings, tmp_path, scenario)
    request = request_for(bundle)
    session = manager.create(
        operation=BootstrapOperation.INSTALL,
        request=request,
        idempotency_key="clean-host-1",
    )
    result = manager.run(session["session_id"])
    assert result["state"] == "succeeded"
    assert result["step"] == "ready"
    assert result["result"]["health"]["fake_job"] == "acknowledged"
    assert result["result"]["network_selftest"]["state"] == "completed"
    assert result["result"]["network_selftest"]["real_provider_calls"] == 0
    assert result["result"]["health"]["inbound_runtime_ports"] == []
    assert result["result"]["health"]["outbound_https"] is True
    assert result["result"]["health"]["capabilities_advertised"] is True
    assert result["result"]["health"]["heartbeat_revision"] == "release-11k"
    assert len(repositories.list_workers(settings=settings)) == 1
    again = manager.create(
        operation=BootstrapOperation.INSTALL,
        request=request,
        idempotency_key="clean-host-1",
    )
    assert again["session_id"] == session["session_id"]
    assert manager.run(session["session_id"])["state"] == "succeeded"
    assert scenario["calls"].count("deploy") == 1
    assert len(repositories.list_workers(settings=settings)) == 1


def test_h_i_j_registration_token_ttl_scope_and_replay(settings, bundle):
    session = store.create_session(
        operation=BootstrapOperation.INSTALL,
        request=request_for(bundle, bootstrap_instance_id="inst_boot_expected"),
        idempotency_key=None,
        settings=settings,
    )
    token = store.issue_registration_token(
        session["session_id"],
        expected_instance_id="inst_boot_expected",
        ttl_sec=30,
        settings=settings,
        now=100.0,
    )
    with pytest.raises(store.RegistrationTokenRejected):
        store.consume_registration_token(
            token, instance_id="inst_boot_wrong", settings=settings, now=101.0
        )
    assert store.consume_registration_token(
        token, instance_id="inst_boot_expected", settings=settings, now=101.0
    ) == session["session_id"]
    with pytest.raises(store.RegistrationTokenRejected):
        store.consume_registration_token(
            token, instance_id="inst_boot_expected", settings=settings, now=102.0
        )
    expired = store.issue_registration_token(
        session["session_id"], expected_instance_id="inst_boot_expected",
        ttl_sec=30, settings=settings, now=200.0,
    )
    with pytest.raises(store.RegistrationTokenRejected):
        store.consume_registration_token(
            expired, instance_id="inst_boot_expected", settings=settings, now=231.0
        )


def test_k_fresh_sessions_reuse_stable_installation_identity(settings, bundle):
    request = request_for(bundle)
    first = store.create_session(
        operation=BootstrapOperation.INSTALL,
        request=request,
        idempotency_key=None,
        settings=settings,
    )
    second = store.create_session(
        operation=BootstrapOperation.INSTALL,
        request=request,
        idempotency_key=None,
        settings=settings,
    )
    assert first["session_id"] != second["session_id"]
    assert first["request"]["bootstrap_instance_id"] == second["request"][
        "bootstrap_instance_id"
    ]


def test_l_center_url_requires_clean_https(bundle):
    with pytest.raises(ValidationError):
        request_for(bundle, center_url="http://center.example.test")
    with pytest.raises(ValidationError):
        request_for(bundle, center_url="https://user:password@center.example.test/?api_key=secret")


def test_o_ay_unit_names_are_per_root(bundle):
    from backend.app.services.worker_bootstrap.remote import bootstrap_units_for_root

    first = bootstrap_units_for_root("/srv/a/audit-worker")
    second = bootstrap_units_for_root("/srv/b/audit-worker")
    assert first != second
    assert not set(first) & set(second)


def test_s_to_aa_ao_at_au_provider_action_resume_skips_install(
    settings, bundle, tmp_path
):
    """S–AA,AO,AT,AU: provider action persists and resume is continuation."""
    scenario = {"provider_action_required": ["claude", "codex"]}
    manager, _ = manager_for(settings, tmp_path, scenario)
    session = manager.create(
        operation=BootstrapOperation.INSTALL,
        request=request_for(bundle, providers=["claude", "codex"]),
    )
    waiting = manager.run(session["session_id"])
    assert waiting["state"] == "action_required"
    assert waiting["step"] == "provider_auth"
    assert scenario["calls"].count("deploy") == 1
    scenario["provider_action_required"] = []
    ready = manager.run(session["session_id"])
    assert ready["state"] == "succeeded"
    assert scenario["calls"].count("deploy") == 1
    assert manager.run(session["session_id"])["state"] == "succeeded"
    assert scenario["calls"].count("deploy") == 1


def test_replace_temporary_center_url_resumes_same_session_without_reinstall(
    settings, bundle, tmp_path
):
    """A dead Quick Tunnel can be replaced without losing bootstrap evidence."""
    scenario = {"provider_action_required": ["claude"]}
    manager, _ = manager_for(settings, tmp_path, scenario)
    session = manager.create(
        operation=BootstrapOperation.INSTALL,
        request=request_for(bundle, providers=["claude"]),
    )
    waiting = manager.run(session["session_id"])
    assert waiting["state"] == "action_required"
    instance_id = waiting["request"]["bootstrap_instance_id"]

    updated = manager.update_center_url(
        session["session_id"], "https://replacement.trycloudflare.com/"
    )
    assert updated["session_id"] == session["session_id"]
    assert updated["state"] == "queued"
    assert updated["step"] == "center_url_updated"
    assert updated["request"]["center_url"] == (
        "https://replacement.trycloudflare.com"
    )
    assert updated["request"]["bootstrap_instance_id"] == instance_id
    assert "configured" not in updated["result"]
    assert updated["result"]["services"]["installed"] is True

    scenario["provider_action_required"] = []
    ready = manager.run(session["session_id"])
    assert ready["state"] == "succeeded"
    assert scenario["files"]["worker.env"]["center"] == (
        "https://replacement.trycloudflare.com"
    )
    assert scenario["calls"].count("deploy") == 1
    assert scenario["calls"].count("configure") == 2
    assert scenario["calls"].count("install_services") == 1
    assert scenario["calls"].count("provider_status") == 2


def test_t_y_pinned_cli_install_path(settings, bundle, tmp_path):
    scenario = {"provider_missing": ["claude", "codex"]}
    manager, _ = manager_for(settings, tmp_path, scenario)
    session = manager.create(
        operation=BootstrapOperation.INSTALL,
        request=request_for(
            bundle,
            providers=["claude", "codex"],
            provider_setup="install_missing",
        ),
    )
    result = manager.run(session["session_id"])
    assert result["state"] == "succeeded"
    assert "install_cli:claude" in scenario["calls"]
    assert "install_cli:codex" in scenario["calls"]


def test_aj_al_approved_policy_covers_exact_presets(settings, bundle, tmp_path):
    scenario = {}
    manager, _ = manager_for(settings, tmp_path, scenario)
    session = manager.create(
        operation=BootstrapOperation.INSTALL,
        request=request_for(bundle, providers=["claude", "codex", "openrouter"]),
    )
    result = manager.run(session["session_id"])
    assert result["result"]["providers"]["compatible_presets"] == [
        "claude_gpt_codex",
        "codex_exec",
    ]
    policy = json.loads(
        (Path(__file__).parents[1] / "audit_worker/provider_policy.approved.json").read_text()
    )
    assert policy["policy_version"] == 1
    assert {"claude", "codex", "openrouter"} <= set(policy)


def test_ready_rejects_policy_hash_or_capability_mismatch(
    settings, bundle, tmp_path
):
    for override, expected in (
        ({"provider_policy_sha256": "b" * 64}, "provider_policy_hash_mismatch"),
        ({"provider_capabilities": {}}, "provider_capabilities_missing"),
    ):
        scenario = {"capabilities_override": override}
        manager, _ = manager_for(settings, tmp_path, scenario)
        session = manager.create(
            operation=BootstrapOperation.INSTALL,
            request=request_for(
                bundle,
                host=f"worker-{expected}.test",
                providers=["claude"],
            ),
        )
        result = manager.run(session["session_id"])
        assert result["state"] == "failed"
        assert result["error_code"] == expected


def test_ab_to_ae_openrouter_local_action_and_no_center_secret(
    settings, bundle, tmp_path
):
    scenario = {"provider_action_required": ["openrouter"]}
    manager, _ = manager_for(settings, tmp_path, scenario)
    session = manager.create(
        operation=BootstrapOperation.INSTALL,
        request=request_for(bundle, providers=["openrouter"]),
    )
    waiting = manager.run(session["session_id"])
    assert waiting["state"] == "action_required"
    serialized = json.dumps(waiting, ensure_ascii=False)
    assert TEST_OPENROUTER_SECRET not in serialized
    scenario["provider_action_required"] = []  # secure remote fixture completed
    ready = manager.run(session["session_id"])
    assert ready["state"] == "succeeded"


def test_af_to_ai_ah_az_bb_bc_adversarial_secrets_absent_everywhere(
    settings, bundle, tmp_path
):
    request = request_for(bundle)
    session = store.create_session(
        operation=BootstrapOperation.INSTALL,
        request=request,
        idempotency_key=None,
        settings=settings,
    )
    token = store.issue_registration_token(
        session["session_id"],
        expected_instance_id=session["request"]["bootstrap_instance_id"],
        ttl_sec=60,
        settings=settings,
    )
    store.transition(
        session["session_id"],
        state=__import__("backend.app.services.worker_bootstrap.models", fromlist=["BootstrapState"]).BootstrapState.FAILED,
        step="failed",
        code="synthetic",
        detail={
            "password": TEST_SSH_PASSWORD,
            "message": "Authorization: Bearer " + token,
            "api_key": TEST_OPENROUTER_SECRET,
        },
        settings=settings,
    )
    db_bytes = settings.db_path.read_bytes()
    for secret in (TEST_SSH_PASSWORD, TEST_OPENROUTER_SECRET, token, token[8:20]):
        assert secret.encode() not in db_bytes
    public = json.dumps(store.get_session(session["session_id"], settings=settings))
    assert TEST_SSH_PASSWORD not in public
    assert TEST_OPENROUTER_SECRET not in public
    assert token not in public
    assert "[REDACTED]" in public


def test_ap_aq_ar_as_ax_repair_update_uninstall_preserve_owned_data(
    settings, bundle, tmp_path
):
    auth_hash = hashlib.sha256(b"fake-provider-auth").hexdigest()
    scenario = {"files": {"provider_auth_hash": auth_hash, "jobs": ["job-1"], "unrelated.service": "active"}}
    manager, _ = manager_for(settings, tmp_path, scenario)
    for operation in (BootstrapOperation.REPAIR, BootstrapOperation.UPDATE):
        session = manager.create(operation=operation, request=request_for(bundle))
        assert manager.run(session["session_id"])["state"] == "succeeded"
        assert scenario["files"]["provider_auth_hash"] == auth_hash
        assert scenario["files"]["jobs"] == ["job-1"]
        assert scenario["files"]["unrelated.service"] == "active"
    uninstall = manager.create(operation=BootstrapOperation.UNINSTALL, request=request_for(bundle))
    result = manager.run(uninstall["session_id"])
    assert result["result"]["provider_auth_preserved"] is True
    assert result["result"]["deregistered"] is True
    assert result["result"]["registration_status"] == "revoked"
    assert scenario["files"]["unrelated.service"] == "active"


def test_f_g_old_revision_update_and_failed_update_rolls_back(settings, bundle, tmp_path):
    scenario = {"release": "old-release", "configure_failure": True}
    manager, _ = manager_for(settings, tmp_path, scenario)
    session = manager.create(operation=BootstrapOperation.UPDATE, request=request_for(bundle))
    result = manager.run(session["session_id"])
    assert result["state"] == "failed"
    assert "rolling_back" in [event["step"] for event in result["events"]]
    assert scenario["release"] == "old-release"
    scenario["configure_failure"] = False
    assert manager.run(session["session_id"])["state"] == "succeeded"
    assert scenario["release"] == "release-11k"


def test_av_aw_paths_fail_closed(bundle):
    for path in (
        "../../tmp/worker", "/", "/etc", "/home", "/srv", "/usr",
        "/srv/worker with space", "/srv/worker%instance", "relative/worker",
        "/home/auditworker",
    ):
        with pytest.raises(ValidationError):
            request_for(bundle, install_root=path)


def test_ba_progress_events_are_ordered(settings, bundle, tmp_path):
    scenario = {}
    manager, _ = manager_for(settings, tmp_path, scenario)
    session = manager.create(operation=BootstrapOperation.INSTALL, request=request_for(bundle))
    result = manager.run(session["session_id"])
    steps = [event["step"] for event in result["events"]]
    expected = ["created", "ssh_enrollment", "preflight", "release_install", "self_test", "configuration", "providers", "registration", "starting", "network_self_test", "ready"]
    positions = [steps.index(step) for step in expected]
    assert positions == sorted(positions)


def test_bd_bundle_build_is_deterministic(tmp_path):
    """BD/BE: deterministic helper produces identical gzip bytes/tree hash."""
    from scripts.deploy_audit_worker import tree_hash, write_bundle_archive

    source = tmp_path / "src"
    source.mkdir()
    (source / "b.txt").write_text("beta", encoding="utf-8")
    (source / "a.txt").write_text("alpha", encoding="utf-8")
    files = [source / "a.txt", source / "b.txt"]
    first = tmp_path / "one.tar.gz"
    second = tmp_path / "two.tar.gz"
    manifest = b'{"release":"test"}'
    write_bundle_archive(source, files, first, manifest)
    write_bundle_archive(source, list(reversed(files)), second, manifest)
    assert first.read_bytes() == second.read_bytes()
    assert tree_hash(source, files) == tree_hash(source, list(reversed(files)))


def test_security_source_has_no_hostkey_bypass_or_production_host():
    root = Path(__file__).resolve().parents[1]
    sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (root / "backend/app/services/worker_bootstrap").glob("*.py")
    )
    assert "StrictHostKeyChecking=no" not in sources
    assert "176.12.77.31" not in sources
    assert "/home/coder" not in sources


def test_redactor_handles_url_token_password_and_oauth_like_values():
    value = {
        "password": TEST_SSH_PASSWORD,
        "api_key": TEST_OPENROUTER_SECRET,
        "message": "Bearer oauth_TEST_1234567890",
    }
    clean = json.dumps(redact(value))
    assert TEST_SSH_PASSWORD not in clean
    assert TEST_OPENROUTER_SECRET not in clean
    assert "oauth_TEST_1234567890" not in clean


def test_registration_stdin_flag_is_allowed_but_secret_value_is_not_argv():
    from backend.app.services.worker_bootstrap.security import secret_free_argv

    secret_free_argv(["python", "-m", "audit_worker", "register", "--bootstrap-secret-stdin"])
    with pytest.raises(ValueError):
        secret_free_argv(["python", "register", TEST_REGISTRATION_SECRET])


def test_host_enrollment_persists_only_the_fingerprint_that_was_verified(
    tmp_path, monkeypatch
):
    from backend.app.services.worker_bootstrap.security import enroll_known_host

    trusted = b"host.test ssh-ed25519 TRUSTED"
    untrusted = b"host.test ssh-rsa UNTRUSTED"
    expected = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

    def fake_run(argv, **kwargs):
        if argv[0] == "ssh-keyscan":
            return subprocess.CompletedProcess(argv, 0, trusted + b"\n" + untrusted + b"\n", b"")
        line = Path(argv[2]).read_bytes()
        fingerprint = expected if b"ssh-ed25519" in line else "SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
        return subprocess.CompletedProcess(argv, 0, f"256 {fingerprint} host (ED25519)\n", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    known_hosts = tmp_path / "known_hosts"
    assert enroll_known_host(
        host="host.test",
        port=22,
        expected_fingerprint=expected,
        known_hosts=known_hosts,
    ) == expected
    assert known_hosts.read_bytes() == trusted + b"\n"


def test_deploy_adapter_converts_cli_exit_to_typed_remote_failure(monkeypatch):
    from backend.app.services.worker_bootstrap.remote import _BootstrapDeployRemote

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 7, "", "safe failure"),
    )
    remote = _BootstrapDeployRemote(host="host.test", user="worker", root="/srv/audit-worker")
    with pytest.raises(RemoteFailure, match="safe failure"):
        remote.run("false")


def test_rollback_snapshots_and_restores_release_config(
    settings, bundle, monkeypatch
):
    from backend.app.services.worker_bootstrap.remote import SSHBootstrapRemote

    remote = SSHBootstrapRemote(
        request=request_for(bundle), session_id="wbs_snapshot", settings=settings
    )
    scripts = []

    def capture(script, **kwargs):
        scripts.append(script)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(remote, "_run", capture)
    remote._snapshot_configuration("release-old")
    remote._restore_configuration("release-old")
    combined = "\n".join(scripts)
    assert "config/releases/$rel" in combined
    assert "worker.env.absent" in combined
    assert "provider_policy.json.absent" in combined
    assert "install -m 0600" in combined


def test_codex_status_uses_success_exit_code_when_message_is_on_stderr(
    settings, bundle, tmp_path, monkeypatch
):
    """Codex 0.147 reports a valid ChatGPT login on stderr with exit code 0."""
    from backend.app.services.worker_bootstrap.remote import SSHBootstrapRemote

    home = tmp_path / "worker-home"
    codex = home / ".local" / "bin" / "codex"
    codex.parent.mkdir(parents=True)
    codex.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = --version ]; then echo 'codex-cli 0.147.0'; exit 0; fi\n"
        "if [ \"$1 $2\" = 'login status' ]; then "
        "echo 'Logged in using ChatGPT' >&2; exit 0; fi\n"
        "exit 2\n",
        encoding="utf-8",
    )
    codex.chmod(0o755)
    remote = SSHBootstrapRemote(
        request=request_for(bundle, providers=["codex"]),
        session_id="wbs_codex_status_stderr",
        settings=settings,
    )

    def run_locally(script, **_kwargs):
        return subprocess.run(
            ["bash"],
            input=script,
            capture_output=True,
            text=True,
            env={**os.environ, "HOME": str(home)},
            check=False,
        )

    monkeypatch.setattr(remote, "_run", run_locally)
    result = remote.provider_status()
    assert result["providers"]["codex_auth"] == "ready"
    assert result["action_required"] == []


def test_ready_waits_for_fresh_post_start_heartbeat(
    settings, bundle, tmp_path, monkeypatch
):
    """Registration's online row is not evidence that Agent has started."""
    from backend.app.services.distributed_workers import worker_registry
    from backend.app.services.worker_bootstrap import manager as manager_module

    scenario = {
        "registration_pipeline_revision": "stale-registration-revision",
        "delay_heartbeat": True,
    }
    manager, _ = manager_for(settings, tmp_path, scenario)
    sleep_calls = []

    def deliver_fresh_heartbeat(_seconds):
        sleep_calls.append(1)
        worker_id = scenario["worker_id"]
        worker = repositories.get_worker(worker_id, settings=settings)
        assert worker is not None
        repositories.update_worker_fields(
            worker_id,
            {"pipeline_revision": "release-11k"},
            settings=settings,
        )
        worker_registry.record_heartbeat(
            worker_id=worker_id,
            instance_id=worker["instance_id"],
            worker_state="idle",
            configured_max_slots=1,
            calculated_free_slots=1,
            active_jobs=[],
            resource_snapshot={"at": time.time()},
            warnings=[],
            settings=settings,
        )

    monkeypatch.setattr(manager_module.time, "sleep", deliver_fresh_heartbeat)
    session = manager.create(
        operation=BootstrapOperation.INSTALL,
        request=request_for(bundle),
    )
    result = manager.run(session["session_id"])
    assert result["state"] == "succeeded"
    assert result["step"] == "ready"
    assert sleep_calls == [1]


def test_runtime_selftest_assigns_job_with_center_role(settings, monkeypatch):
    from backend.app.services.distributed_workers import job_service
    from backend.app.services.worker_bootstrap.manager import _default_runtime_selftest

    captured = {}

    def create_test_job(**kwargs):
        captured.update(kwargs)
        return {"job_id": "job_bootstrap_selftest", "state": "assigned"}

    monkeypatch.setattr(job_service, "create_test_job", create_test_job)
    monkeypatch.setattr(
        repositories,
        "get_job",
        lambda *_args, **_kwargs: {
            "job_id": "job_bootstrap_selftest",
            "state": "completed",
        },
    )
    result = _default_runtime_selftest(
        "wrk_bootstrap_test", "wbs_same_session", settings
    )
    assert captured["actor"] == "center:bootstrap:wbs_same_session"
    assert captured["resume_existing"] is True
    assert result["state"] == "completed"
    assert result["real_provider_calls"] == 0


def test_zero_inference_test_job_resumes_orphan_created_before_assignment(
    settings,
):
    from backend.app.models.distributed_workers import TestJobParams
    from backend.app.services.distributed_workers import job_service

    capabilities = {
        "job_types": ["test_pipeline_v1"],
        "compressions": ["gzip"],
    }
    worker, _claim_secret, _created = registration_service.register_worker(
        instance_id="inst_boot_selftest_resume",
        display_name_hint="Bootstrap selftest worker",
        worker_version="11k-test",
        protocol_version=settings.protocol_version,
        pipeline_revision="release-11k",
        capabilities=capabilities,
        configured_max_slots_hint=1,
        settings=settings,
    )
    registration_service.approve_worker(
        worker_id=worker["worker_id"],
        display_name="Bootstrap selftest worker",
        configured_max_slots=1,
        settings=settings,
    )
    repositories.set_worker_intake(
        worker["worker_id"], enabled=True, actor="center:bootstrap:test",
        reason="bootstrap selftest fixture", settings=settings,
    )
    orphan = repositories.create_job(
        job_type="test_pipeline_v1",
        project_id="bootstrap-selftest-resume-contract",
        version_id=None,
        payload={"params": {"label": "bootstrap-11k"}},
        display_name="bootstrap-selftest-resume-contract",
        created_by="bootstrap:old-contract",
        settings=settings,
    )
    original_manifest = job_service.build_source_package(
        job=orphan,
        params=TestJobParams(
            label="bootstrap-11k", steps=3, step_seconds=0.05, result_bytes=4096
        ),
        compression="gzip",
        settings=settings,
    )
    source_dir = job_service.source_package_path(orphan, settings=settings).parent
    original_files = sorted(path.name for path in source_dir.iterdir())

    resumed = job_service.create_test_job(
        worker_id=worker["worker_id"],
        project_id="bootstrap-selftest-resume-contract",
        version_id=None,
        params=TestJobParams(
            label="bootstrap-11k", steps=3, step_seconds=0.05, result_bytes=4096
        ),
        actor="center:bootstrap:wbs_same_session",
        settings=settings,
        resume_existing=True,
    )
    assert resumed["job_id"] == orphan["job_id"]
    assert resumed["state"] == "assigned"
    assert resumed["assigned_worker_id"] == worker["worker_id"]
    assert resumed["source_package_hash"] == original_manifest["archive"]["sha256"]
    assert sorted(path.name for path in source_dir.iterdir()) == original_files


def test_provider_metadata_is_advertised_without_secret(monkeypatch, tmp_path):
    from audit_worker.config import load_config

    monkeypatch.setenv("AUDIT_WORKER_ROOT", str(tmp_path / "worker"))
    monkeypatch.setenv("AUDIT_WORKER_PROVIDER_POLICY_VERSION", "1")
    monkeypatch.setenv("AUDIT_WORKER_PROVIDER_POLICY_SHA256", "a" * 64)
    monkeypatch.setenv(
        "AUDIT_WORKER_ROUTING_COMPATIBILITY", "claude_gpt_codex,codex_exec,invalid"
    )
    capabilities = load_config(require_dispatcher=False).capabilities()
    assert capabilities["provider_policy_sha256"] == "a" * 64
    assert capabilities["routing_compatibility"] == [
        "claude_gpt_codex",
        "codex_exec",
    ]


def test_action_required_uses_integrated_resume_command():
    instructions = BootstrapManager._provider_instructions(
        "wbs_test_session", ["claude", "openrouter"]
    )
    assert instructions == {
        "claude": (
            "python3 scripts/audit_worker_bootstrap.py provider-auth "
            "wbs_test_session claude"
        ),
        "openrouter": (
            "python3 scripts/audit_worker_bootstrap.py provider-auth "
            "wbs_test_session openrouter"
        ),
    }


def test_invalid_api_payload_does_not_echo_rejected_secrets():
    from fastapi import HTTPException
    from backend.app.api.routers.worker_bootstrap import _validated_payload

    raw = {
        "operation": "install",
        "request": {
            "center_url": "https://user:URL_PASSWORD_TEST@center.test/?api_key=QUERY_SECRET_TEST",
            "ssh_password": TEST_SSH_PASSWORD,
        },
    }
    with pytest.raises(HTTPException) as caught:
        _validated_payload(raw)
    body = json.dumps(caught.value.detail)
    assert "URL_PASSWORD_TEST" not in body
    assert "QUERY_SECRET_TEST" not in body
    assert TEST_SSH_PASSWORD not in body
