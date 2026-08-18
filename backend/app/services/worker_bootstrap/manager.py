"""Resumable state machine shared by bootstrap CLI and HTTP API."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

from backend.app.services.distributed_workers import database, registration_service, repositories
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersSettings,
    get_settings,
)
from scripts import deploy_audit_worker as deploy

from . import store
from . import BOOTSTRAP_VERSION
from .models import BootstrapOperation, BootstrapRequest, BootstrapState
from .remote import ActionRequired, BootstrapRemote, RemoteFailure, SSHBootstrapRemote


RemoteFactory = Callable[[BootstrapRequest, str, DistributedWorkersSettings], BootstrapRemote]
SelfTestRunner = Callable[[str, str, DistributedWorkersSettings], dict[str, Any]]
CertificateEnroller = Callable[..., Any]


def _default_certificate_enroller(
    *, worker_id: str, instance_id: str, csr_pem: bytes, request_id: str,
    settings: DistributedWorkersSettings,
):
    """Use the protected issuer socket; bootstrap never reads the CA key."""
    import os
    from backend.app.security.issuer_rpc import UnixSocketEnrollmentAuthority

    socket_path = Path(os.environ.get("AUDIT_WORKER_ISSUER_SOCKET", ""))
    if not str(socket_path) or not socket_path.is_socket():
        raise RemoteFailure(
            "certificate_issuer_unavailable",
            "protected Worker certificate issuer is not configured",
        )
    return UnixSocketEnrollmentAuthority(socket_path).enroll(
        worker_id=worker_id, instance_id=instance_id,
        csr_pem=csr_pem, request_id=request_id,
    )


def _default_remote(
    request: BootstrapRequest, session_id: str, settings: DistributedWorkersSettings
) -> BootstrapRemote:
    return SSHBootstrapRemote(request=request, session_id=session_id, settings=settings)


def _default_runtime_selftest(
    worker_id: str, session_id: str, settings: DistributedWorkersSettings
) -> dict[str, Any]:
    """Submit a real protocol test job and wait for center ACK/completion.

    `test_pipeline_v1` has a closed scalar payload and cannot call a model.
    It exercises poll → accept → executor → EventOutbox → upload → ACK.
    """
    from backend.app.models.distributed_workers import JobState, TestJobParams
    from backend.app.services.distributed_workers import job_service

    job = job_service.create_test_job(
        worker_id=worker_id,
        project_id="bootstrap-selftest-" + session_id[-16:],
        version_id=None,
        params=TestJobParams(
            label="bootstrap-11k", steps=3, step_seconds=0.05, result_bytes=4096
        ),
        # Assignment is a center-owned state transition.  Keep bootstrap in
        # the actor identity for the audit trail while retaining the `center`
        # role required by the job state machine.
        actor="center:bootstrap:" + session_id,
        settings=settings,
        resume_existing=True,
    )
    deadline = time.monotonic() + 180.0
    last_state = job.get("state")
    while time.monotonic() < deadline:
        current = repositories.get_job(job["job_id"], settings=settings)
        last_state = current.get("state") if current else None
        if last_state == JobState.COMPLETED.value:
            return {
                "job_id": job["job_id"],
                "state": last_state,
                "protocol": "outbound_https",
                "real_provider_calls": 0,
            }
        if last_state in {JobState.FAILED.value, JobState.CANCELLED.value}:
            break
        time.sleep(1.0)
    raise RemoteFailure(
        "self_test_failed",
        f"bootstrap test job не завершён: state={last_state}",
    )


class BootstrapManager:
    """One source of truth for install/update/repair/status lifecycle."""

    def __init__(
        self,
        *,
        settings: DistributedWorkersSettings | None = None,
        remote_factory: RemoteFactory = _default_remote,
        selftest_runner: SelfTestRunner = _default_runtime_selftest,
        certificate_enroller: CertificateEnroller = _default_certificate_enroller,
        repo_root: Path | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.settings.require_enabled()
        self.remote_factory = remote_factory
        self.selftest_runner = selftest_runner
        self.certificate_enroller = certificate_enroller
        self.repo_root = (repo_root or Path(__file__).resolve().parents[4]).resolve()

    def create(
        self,
        *,
        operation: BootstrapOperation,
        request: BootstrapRequest,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return store.create_session(
            operation=operation,
            request=request,
            idempotency_key=idempotency_key,
            settings=self.settings,
        )

    def get(self, session_id: str) -> dict[str, Any]:
        return store.get_session(session_id, settings=self.settings)

    def list(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return store.list_sessions(limit=limit, settings=self.settings)

    def update_center_url(self, session_id: str, center_url: str) -> dict[str, Any]:
        """Pin a replacement endpoint to this session and require reconfigure."""
        return store.update_center_url(
            session_id, center_url=center_url, settings=self.settings
        )

    def run(self, session_id: str) -> dict[str, Any]:
        """Execute or resume. Repeating it is safe and advances from evidence.

        Remote steps themselves are idempotent. A crash after completing a
        remote step but before persisting the transition therefore repeats the
        same operation without creating a second release/service/worker.
        """
        session = self.get(session_id)
        if session["state"] == BootstrapState.SUCCEEDED.value:
            return session
        request = BootstrapRequest.model_validate(session["request"])
        remote = self.remote_factory(request, session_id, self.settings)
        operation = BootstrapOperation(session["operation"])
        completed = dict(session.get("result") or {})
        deployed_this_run = False
        release: dict[str, Any] | None = None
        try:
            self._mark(session_id, "ssh_enrollment")
            enrolled = remote.enroll()
            self._mark(session_id, "preflight", result={"ssh": enrolled})
            preflight = remote.preflight()

            if operation in {BootstrapOperation.STATUS, BootstrapOperation.VALIDATE}:
                health = remote.health()
                providers = remote.provider_status()
                center = None
                known_worker_id = self._known_worker_id(request, session)
                if known_worker_id:
                    row = repositories.get_worker(
                        known_worker_id, settings=self.settings
                    )
                    if row:
                        center = {
                            "worker_id": row.get("worker_id"),
                            "registration_status": row.get("registration_status"),
                            "connection_status": row.get("connection_status"),
                            "last_seen_at": row.get("last_seen_at"),
                            "pipeline_revision": row.get("pipeline_revision"),
                        }
                return self._success(
                    session_id,
                    "validated" if operation == BootstrapOperation.VALIDATE else "status_complete",
                    {
                        "preflight": preflight,
                        "health": health,
                        "providers": providers,
                        "center": center,
                        "no_inference": True,
                    },
                )
            if operation == BootstrapOperation.ROLLBACK:
                rolled = remote.rollback(request.release_id)
                return self._success(session_id, "rollback_complete", rolled, fields=rolled)
            if operation == BootstrapOperation.UNINSTALL:
                worker_id = self._known_worker_id(request, session)
                result = remote.uninstall()
                if worker_id:
                    revoked = registration_service.revoke_worker(
                        worker_id=worker_id, settings=self.settings
                    )
                    result.update(
                        {
                            "worker_id": worker_id,
                            "deregistered": True,
                            "registration_status": revoked["registration_status"],
                        }
                    )
                else:
                    result.update(
                        {"deregistered": False, "deregister_reason": "worker_not_known"}
                    )
                return self._success(session_id, "uninstall_complete", result)
            if operation == BootstrapOperation.DEREGISTER:
                worker_id = self._known_worker_id(request, session)
                if not worker_id:
                    raise RemoteFailure("worker_id_missing", "сессия не содержит worker_id")
                from backend.app.services.distributed_workers import registration_service as reg
                database_call = reg.revoke_worker(
                    worker_id=worker_id, settings=self.settings
                )
                return self._success(
                    session_id,
                    "deregister_complete",
                    {"worker_id": worker_id, "registration_status": database_call["registration_status"]},
                )

            release = completed.get("release")
            if not isinstance(release, dict) or not release.get("release_id"):
                bundle = self._bundle(session_id, request)
                self._mark(session_id, "release_install", result={"preflight": preflight})
                release = remote.deploy_release(archive=bundle[0], manifest=bundle[1])
                deployed_this_run = True
                self._mark(
                    session_id,
                    "self_test",
                    result={"release": release, "core_selftest": "passed"},
                    fields=release,
                )
                self._mark(session_id, "configuration")
            configured = completed.get("configured")
            if not isinstance(configured, dict):
                configured = remote.configure()
            services = completed.get("services")
            if not isinstance(services, dict):
                services = remote.install_services()

            self._mark(session_id, "providers", result={"configured": configured, "services": services})
            providers = remote.provider_status()
            cli_changes = list(providers.get("missing", [])) + list(
                providers.get("incompatible", [])
            )
            if cli_changes and request.provider_setup == "install_missing":
                installs = []
                for provider in cli_changes:
                    installs.append(remote.install_provider_cli(provider))
                providers = remote.provider_status()
                providers["installs"] = installs
            if providers.get("missing") or providers.get("incompatible"):
                broken = list(providers.get("missing", [])) + list(
                    providers.get("incompatible", [])
                )
                raise ActionRequired(
                    "provider_cli_required",
                    "отсутствуют/несовместимы CLI: " + ", ".join(broken),
                )
            if providers.get("action_required"):
                waiting = list(providers["action_required"])
                action_codes = {
                    "claude": "claude_login_required",
                    "codex": "codex_login_required",
                    "openrouter": "openrouter_secret_required",
                }
                action_code = action_codes.get(waiting[0], "provider_auth_required") if len(waiting) == 1 else "provider_auth_required"
                return store.transition(
                    session_id,
                    state=BootstrapState.ACTION_REQUIRED,
                    step=(f"waiting_{waiting[0]}_auth" if len(waiting) == 1 and waiting[0] != "openrouter" else ("waiting_openrouter_secret" if waiting == ["openrouter"] else "provider_auth")),
                    code=action_code,
                    detail={
                        "providers": waiting,
                        "resume": f"bootstrap resume {session_id}",
                        "instructions": self._provider_instructions(
                            session_id, providers["action_required"]
                        ),
                    },
                    result_patch={"providers": providers},
                    fields={"error_code": None, "error_detail": None},
                    settings=self.settings,
                )
            requested_provider_names = {item.value for item in request.providers}
            providers["compatible_presets"] = (
                ["claude_gpt_codex", "codex_exec"]
                if {"claude", "codex", "openrouter"} <= requested_provider_names
                else []
            )

            self._mark(session_id, "registration", result={"providers": providers})
            instance_id = str(request.bootstrap_instance_id)
            registration_token = store.issue_registration_token(
                session_id,
                expected_instance_id=instance_id,
                ttl_sec=300,
                settings=self.settings,
            )
            # Plain registration_token lives only on this stack and remote
            # stdin. It is never included in transition/result/exception.
            registered = remote.register(registration_token)
            registration_token = ""  # shorten lifetime and prevent accidental reuse
            store.invalidate_registration_tokens(
                session_id, settings=self.settings
            )
            worker_id = str(registered.get("worker_id") or "")
            if not worker_id:
                raise RemoteFailure("registration_response_invalid", "worker_id отсутствует")
            registration_service.approve_worker(
                worker_id=worker_id,
                display_name=request.display_name,
                configured_max_slots=request.max_slots,
                settings=self.settings,
            )
            if request.gateway_security_mode == "mtls":
                self._mark(session_id, "certificate_enrollment")
                csr_pem = remote.prepare_mtls_csr(worker_id)
                issued_identity = self.certificate_enroller(
                    worker_id=worker_id,
                    instance_id=instance_id,
                    csr_pem=csr_pem,
                    request_id="bootstrap-cert-" + session_id,
                    settings=self.settings,
                )
                installed_identity = remote.install_mtls_identity(
                    issued_identity.certificate_chain_pem,
                    issued_identity.trust_chain_pem,
                )
                self._mark(
                    session_id,
                    "certificate_enrolled",
                    result={
                        "mtls_identity": {
                            "serial_hex": issued_identity.serial_hex,
                            "fingerprint_sha256": issued_identity.fingerprint_sha256,
                            "not_after": issued_identity.not_after,
                            "installed": bool(installed_identity.get("installed")),
                            "private_key_origin": "worker",
                        }
                    },
                )
            claimed = remote.claim()
            if not claimed.get("token_stored"):
                raise RemoteFailure("worker_claim_failed", "worker token не сохранён на VPS")
            # start_services deliberately runs only after claim; at this point
            # starting enabled units cannot race registration.
            self._mark(session_id, "starting")
            startup_started_at = time.time()
            remote.start_services()
            health = remote.health()
            units = health.get("units") or []
            if len(units) < 2 or any(unit.get("STATE") != "active" for unit in units):
                raise RemoteFailure("service_failed", "Agent/Executor не перешли в active")
            if health.get("release") not in {release.get("release_id"), None}:
                raise RemoteFailure("execution_revision_mismatch", "current release не совпадает с установленным")
            # Agent sends heartbeat immediately, but allow a bounded startup
            # window on slower machines. This is the difference between
            # `systemctl active` and a worker actually visible to the center.
            center_worker = None
            deadline = time.monotonic() + 45.0
            while time.monotonic() < deadline:
                center_worker = repositories.get_worker(worker_id, settings=self.settings)
                heartbeat_at = float(
                    (center_worker or {}).get("last_seen_at") or 0.0
                )
                if (
                    center_worker
                    and center_worker.get("connection_status") == "online"
                    and heartbeat_at >= startup_started_at
                ):
                    break
                time.sleep(1.0)
            heartbeat_at = float((center_worker or {}).get("last_seen_at") or 0.0)
            if (
                not center_worker
                or center_worker.get("connection_status") != "online"
                or heartbeat_at < startup_started_at
            ):
                raise RemoteFailure("heartbeat_missing", "Agent active, но heartbeat не пришёл на center")
            if center_worker.get("pipeline_revision") != configured.get("pipeline_revision"):
                raise RemoteFailure("execution_revision_mismatch", "heartbeat revision не совпала с worker.env")
            try:
                advertised = json.loads(center_worker.get("capabilities") or "{}")
            except ValueError as exc:
                raise RemoteFailure("capabilities_invalid", "center не разобрал capabilities") from exc
            if not advertised.get("routing_plan_v1"):
                raise RemoteFailure("capabilities_missing", "routing-plan capability не объявлена")
            if advertised.get("bootstrap_version") != BOOTSTRAP_VERSION:
                raise RemoteFailure(
                    "bootstrap_version_mismatch",
                    "heartbeat bootstrap version не совпала",
                )
            if advertised.get("provider_policy_version") != 1:
                raise RemoteFailure(
                    "provider_policy_version_mismatch",
                    "heartbeat policy version не совпала",
                )
            if advertised.get("provider_policy_sha256") != configured.get(
                "policy_sha256"
            ):
                raise RemoteFailure(
                    "provider_policy_hash_mismatch",
                    "heartbeat policy SHA-256 не совпал",
                )
            advertised_providers = advertised.get("provider_capabilities") or {}
            missing_capabilities = sorted(
                provider
                for provider in requested_provider_names
                if not advertised_providers.get(provider)
            )
            if missing_capabilities:
                raise RemoteFailure(
                    "provider_capabilities_missing",
                    "heartbeat не объявил capabilities: "
                    + ", ".join(missing_capabilities),
                )
            advertised_presets = set(advertised.get("routing_compatibility") or [])
            if not set(providers["compatible_presets"]) <= advertised_presets:
                raise RemoteFailure(
                    "routing_compatibility_mismatch",
                    "heartbeat не объявил рассчитанные presets",
                )
            self._mark(session_id, "network_self_test")
            network_selftest = self.selftest_runner(
                worker_id, session_id, self.settings
            )
            return self._success(
                session_id,
                "ready",
                {
                    "worker_id": worker_id,
                    "release": release,
                    "providers": providers,
                    "health": health,
                    "capabilities": advertised,
                    "network_selftest": network_selftest,
                    "no_inference": True,
                    "bootstrap_version": BOOTSTRAP_VERSION,
                    "policy_version": 1,
                },
                fields={"worker_id": worker_id, **release},
            )
        except ActionRequired as exc:
            return store.transition(
                session_id,
                state=BootstrapState.ACTION_REQUIRED,
                step="action_required",
                code=exc.code,
                detail={"message": exc.detail, "resume": f"bootstrap resume {session_id}"},
                fields={"error_code": exc.code, "error_detail": exc.detail},
                settings=self.settings,
            )
        except Exception as exc:  # noqa: BLE001 — state machine records typed failure
            code = exc.code if isinstance(exc, RemoteFailure) else "bootstrap_internal_error"
            detail = exc.detail if isinstance(exc, RemoteFailure) else f"{type(exc).__name__}: {exc}"
            previous = release.get("previous_release_id") if isinstance(release, dict) else None
            if deployed_this_run and previous:
                try:
                    store.transition(
                        session_id,
                        state=BootstrapState.RUNNING,
                        step="rolling_back",
                        code=code,
                        detail={"target_release": previous},
                        settings=self.settings,
                    )
                    rolled = remote.rollback(str(previous))
                    store.transition(
                        session_id,
                        state=BootstrapState.RUNNING,
                        step="rollback_complete",
                        result_patch={
                            "rolled_back_release": rolled.get("release_id"),
                            "release": {},
                            "configured": None,
                            "services": None,
                        },
                        fields={"release_id": rolled.get("release_id")},
                        settings=self.settings,
                    )
                    detail += f"; automatic rollback -> {rolled.get('release_id')}"
                except Exception as rollback_exc:  # noqa: BLE001
                    detail += f"; rollback failed: {type(rollback_exc).__name__}"
            return store.transition(
                session_id,
                state=BootstrapState.FAILED,
                step="failed",
                code=code,
                detail={"message": detail},
                fields={"error_code": code, "error_detail": detail},
                settings=self.settings,
            )

    def _bundle(self, session_id: str, request: BootstrapRequest) -> tuple[Path, Path]:
        if request.bundle_path:
            archive = Path(request.bundle_path).resolve()
            manifest = archive.with_name(archive.name.replace(".tar.gz", ".manifest.json"))
            if not archive.is_file() or not manifest.is_file():
                raise RemoteFailure("bundle_missing", "bundle или manifest не найден")
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            if request.bundle_sha256 and payload.get("archive_sha256") != request.bundle_sha256:
                raise RemoteFailure("bundle_hash_mismatch", "request SHA-256 не совпадает с manifest")
            return archive, manifest
        out = self.settings.data_dir / "bootstrap_bundles" / session_id
        out.mkdir(parents=True, exist_ok=True)
        revision = deploy._git(self.repo_root, "rev-parse", "HEAD")
        dirty = deploy._git(
            self.repo_root, "status", "--porcelain", "--untracked-files=no"
        )
        if not revision or dirty:
            raise RemoteFailure(
                "bundle_source_not_immutable",
                "automatic bundle build requires a clean immutable Git HEAD",
            )
        try:
            built = deploy.build_artifact(
                self.repo_root,
                out,
                pipeline_revision=request.release_id or revision,
                source_commit=revision,
            )
        except SystemExit as exc:
            raise RemoteFailure("bundle_build_failed", str(exc)) from exc
        return built.archive, built.manifest_path

    def _mark(
        self,
        session_id: str,
        step: str,
        *,
        result: dict[str, Any] | None = None,
        fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return store.transition(
            session_id,
            state=BootstrapState.RUNNING,
            step=step,
            result_patch=result,
            fields=fields,
            settings=self.settings,
        )

    def _success(
        self,
        session_id: str,
        step: str,
        result: dict[str, Any],
        *,
        fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        clean_fields = dict(fields or {})
        clean_fields.update({"error_code": None, "error_detail": None})
        return store.transition(
            session_id,
            state=BootstrapState.SUCCEEDED,
            step=step,
            result_patch=result,
            fields=clean_fields,
            settings=self.settings,
        )

    def _known_worker_id(
        self, request: BootstrapRequest, session: dict[str, Any]
    ) -> str | None:
        explicit = request.worker_id or session.get("worker_id")
        if explicit:
            return str(explicit)
        if request.bootstrap_instance_id:
            row = repositories.find_worker_by_instance(
                request.bootstrap_instance_id, settings=self.settings
            )
            if row:
                return str(row["worker_id"])
        return None

    @staticmethod
    def _provider_instructions(
        session_id: str, providers: list[str]
    ) -> dict[str, str]:
        # The CLI reuses the enrolled known_hosts file, attaches a transient
        # TTY, and resumes this same state machine after a successful action.
        return {
            provider: (
                "python3 scripts/audit_worker_bootstrap.py provider-auth "
                f"{session_id} {provider}"
            )
            for provider in providers
        }
