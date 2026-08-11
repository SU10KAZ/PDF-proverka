"""Strict-host-key SSH implementation of the bootstrap remote contract."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

from backend.app.services.distributed_workers.settings import DistributedWorkersSettings
from scripts import deploy_audit_worker as deploy

from .models import BootstrapRequest
from .security import enroll_known_host, redact, secret_free_argv


MIN_CPU = 2
MIN_DISK_MB = 6000


def bootstrap_units_for_root(root: str) -> tuple[str, str]:
    """Collision-free units: readable basename plus hash of the full root."""
    normalized = str(Path(root))
    slug = re.sub(r"[^a-z0-9-]+", "-", Path(normalized).name.lower()).strip("-")
    slug = (slug or "worker")[:32]
    suffix = hashlib.sha256(normalized.encode()).hexdigest()[:10]
    prefix = f"audit-worker-{slug}-{suffix}"
    return f"{prefix}-agent.service", f"{prefix}-executor.service"


class RemoteFailure(RuntimeError):
    def __init__(self, code: str, detail: str, *, retryable: bool = False):
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.retryable = retryable


class ActionRequired(RemoteFailure):
    pass


class _BootstrapDeployRemote(deploy.Remote):
    """Adapter that keeps deploy helper failures inside the state machine.

    The standalone deployment CLI intentionally raises ``SystemExit``. That
    is correct at a shell boundary but would bypass ``except Exception`` in a
    persistent bootstrap session and leave it forever RUNNING.
    """

    def run(self, script: str, *, timeout: int = 600, check: bool = True):
        argv = ["ssh", *self.ssh_opts, self.target, "bash -s"]
        secret_free_argv(argv)
        result = subprocess.run(
            argv, input=script, capture_output=True, text=True, timeout=timeout
        )
        if check and result.returncode:
            raise RemoteFailure(
                "remote_command_failed",
                str(redact((result.stderr or result.stdout)[-2000:])),
                retryable=result.returncode == 255,
            )
        return result


class BootstrapRemote(Protocol):
    def enroll(self) -> dict[str, Any]: ...
    def preflight(self) -> dict[str, Any]: ...
    def deploy_release(self, *, archive: Path, manifest: Path) -> dict[str, Any]: ...
    def configure(self) -> dict[str, Any]: ...
    def install_services(self) -> dict[str, Any]: ...
    def start_services(self) -> dict[str, Any]: ...
    def provider_status(self) -> dict[str, Any]: ...
    def install_provider_cli(self, provider: str) -> dict[str, Any]: ...
    def register(self, registration_token: str) -> dict[str, Any]: ...
    def claim(self) -> dict[str, Any]: ...
    def health(self) -> dict[str, Any]: ...
    def rollback(self, release_id: str | None) -> dict[str, Any]: ...
    def uninstall(self) -> dict[str, Any]: ...


def resolve_ssh_identity(reference: str) -> Path | None:
    """Разрешить opaque ref через central mapping; `agent` не требует файла."""
    if reference == "agent":
        return None
    mapping_path = os.environ.get("AUDIT_WORKER_SSH_AUTH_REFS_FILE", "").strip()
    if not mapping_path:
        raise RemoteFailure("ssh_auth_ref_unknown", "не настроен central SSH auth resolver")
    mapping = json.loads(Path(mapping_path).read_text(encoding="utf-8"))
    raw = mapping.get(reference)
    if not isinstance(raw, str) or not raw:
        raise RemoteFailure("ssh_auth_ref_unknown", "SSH auth reference не найден")
    key = Path(raw).expanduser().resolve()
    if not key.is_file():
        raise RemoteFailure("ssh_identity_missing", "файл SSH identity не найден")
    mode = stat.S_IMODE(key.stat().st_mode)
    if mode & 0o077:
        raise RemoteFailure("ssh_identity_permissions", "SSH identity должен иметь mode 0600")
    return key


@dataclass
class SSHBootstrapRemote:
    request: BootstrapRequest
    session_id: str
    settings: DistributedWorkersSettings

    def __post_init__(self) -> None:
        self.known_hosts = self.settings.data_dir / "bootstrap_known_hosts" / self.session_id
        self.identity = resolve_ssh_identity(self.request.ssh_auth_ref)

    @property
    def target(self) -> str:
        return f"{self.request.ssh_user}@{self.request.host}"

    def _ssh_options(self) -> list[str]:
        options = [
            "-F", "/dev/null",
            "-p", str(self.request.port),
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=15",
            "-o", "StrictHostKeyChecking=yes",
            "-o", f"UserKnownHostsFile={self.known_hosts}",
        ]
        if self.identity is not None:
            options.extend(["-i", str(self.identity), "-o", "IdentitiesOnly=yes"])
        return options

    def _deploy_remote(self) -> deploy.Remote:
        # deploy.Remote использует те же options и уже реализует проверенный
        # atomic release contract. Порт scp задаётся отдельно ниже.
        return _BootstrapDeployRemote(
            host=self.request.host,
            user=self.request.ssh_user,
            root=self.request.install_root,
            ssh_opts=tuple(self._ssh_options()),
        )

    def _run(self, script: str, *, timeout: int = 600, check: bool = True):
        argv = ["ssh", *self._ssh_options(), self.target, "bash -s"]
        secret_free_argv(argv)
        result = subprocess.run(
            argv, input=script, text=True, capture_output=True, timeout=timeout
        )
        if check and result.returncode:
            lowered = (result.stderr or "").lower()
            if result.returncode == 255:
                code = (
                    "ssh_auth_failed"
                    if "permission denied" in lowered or "authentication" in lowered
                    else "ssh_unreachable"
                )
            else:
                code = "remote_command_failed"
            raise RemoteFailure(
                code,
                str(redact((result.stderr or result.stdout)[-2000:])),
                retryable=result.returncode == 255,
            )
        return result

    def _copy(self, local: Path, remote_path: str, *, timeout: int = 1800) -> None:
        # scp использует -P, не ssh -p.
        opts = self._ssh_options()
        converted: list[str] = []
        i = 0
        while i < len(opts):
            if opts[i] == "-p" and i + 1 < len(opts):
                converted.extend(["-P", opts[i + 1]])
                i += 2
            else:
                converted.append(opts[i])
                i += 1
        argv = ["scp", *converted, str(local), f"{self.target}:{remote_path}"]
        secret_free_argv(argv)
        result = subprocess.run(argv, text=True, capture_output=True, timeout=timeout)
        if result.returncode:
            raise RemoteFailure("transfer_failed", str(redact(result.stderr[-2000:])), retryable=True)

    def enroll(self) -> dict[str, Any]:
        try:
            fingerprint = enroll_known_host(
                host=self.request.host,
                port=self.request.port,
                expected_fingerprint=self.request.expected_host_fingerprint,
                known_hosts=self.known_hosts,
            )
        except Exception as exc:
            from .security import HostKeyMismatch

            if isinstance(exc, HostKeyMismatch):
                raise RemoteFailure("host_key_mismatch", str(exc)) from exc
            raise RemoteFailure("ssh_unreachable", f"host-key enrollment failed: {type(exc).__name__}") from exc
        return {"host_key": fingerprint, "strict_host_key_checking": True}

    def preflight(self) -> dict[str, Any]:
        root = shlex.quote(self.request.install_root)
        center = shlex.quote(self.request.center_url)
        center_host = shlex.quote(str(urlsplit(self.request.center_url).hostname))
        result = self._run(
            f"""set +e
root={root}
probe="$root"
while [ ! -e "$probe" ] && [ "$probe" != / ]; do probe=$(dirname "$probe"); done
. /etc/os-release
echo HOSTNAME=$(hostname)
echo OS_ID=$ID
echo OS_VERSION=$VERSION_ID
echo ARCH=$(uname -m)
echo USER=$(id -un)
echo UID=$(id -u)
echo CPU=$(getconf _NPROCESSORS_ONLN)
echo RAM_MB=$(awk '/MemTotal:/ {{print int($2/1024)}}' /proc/meminfo)
echo RAM_AVAILABLE_MB=$(awk '/MemAvailable:/ {{print int($2/1024)}}' /proc/meminfo)
echo DISK_MB=$(df -Pm "$probe" | awk 'NR==2 {{print $4}}')
echo DISK_TOTAL_MB=$(df -Pm "$probe" | awk 'NR==2 {{print $2}}')
if command -v python3 >/dev/null; then echo PYTHON=$(python3 -c 'import sys;print(".".join(map(str,sys.version_info[:3])))'); else echo PYTHON=absent; fi
for tool in python3 tar sha256sum curl systemctl; do command -v "$tool" >/dev/null || echo MISSING=$tool; done
for tool in node git wget zstd; do command -v "$tool" >/dev/null && echo OPTIONAL_$tool=present || echo OPTIONAL_$tool=absent; done
for provider in claude codex; do
  p="$HOME/.local/bin/$provider"; [ -x "$p" ] || p=$(command -v "$provider" 2>/dev/null || true)
  [ -x "$p" ] && echo "EXISTING_${{provider}}=present" || echo "EXISTING_${{provider}}=absent"
done
getent ahosts {center_host} >/dev/null 2>&1; echo DNS_RC=$?
tls=$(curl --silent --show-error --head --max-time 20 -w '\nTLS_VERIFY=%{{ssl_verify_result}}\nHTTP_CODE=%{{http_code}}' {center} 2>&1)
curl_rc=$?
printf '%s\n' "$tls" | tail -3
echo CURL_RC=$curl_rc
date_header=$(printf '%s\n' "$tls" | awk 'BEGIN{{IGNORECASE=1}} /^date:/{{sub(/^[^:]*:[ ]*/,""); sub(/\r$/,""); print; exit}}')
server_epoch=$(date -d "$date_header" +%s 2>/dev/null)
local_epoch=$(date +%s)
if [ -n "$server_epoch" ]; then skew=$((local_epoch-server_epoch)); [ "$skew" -lt 0 ] && skew=$((-skew)); echo CLOCK_SKEW_SEC=$skew; else echo CLOCK_SKEW_SEC=unknown; fi
XDG_RUNTIME_DIR=/run/user/$(id -u) systemctl --user show-environment >/dev/null 2>&1; echo USER_SYSTEMD_RC=$?
sudo -n -l /usr/bin/loginctl enable-linger "$(id -un)" >/dev/null 2>&1; echo SUDO_LINGER_RC=$?
loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null | sed 's/^/LINGER=/'
command -v ss >/dev/null && echo LISTEN_COUNT=$(ss -tlnH | wc -l) || echo LISTEN_COUNT=unknown
if [ -L "$root/current" ]; then echo EXISTING_RELEASE=$(basename "$(readlink -f "$root/current")"); else echo EXISTING_RELEASE=none; fi
for unit in {" ".join(shlex.quote(unit) for unit in bootstrap_units_for_root(self.request.install_root))}; do
  state=$(XDG_RUNTIME_DIR=/run/user/$(id -u) systemctl --user is-active "$unit" 2>/dev/null || true)
  echo "EXISTING_UNIT=$unit:${{state:-absent}}"
done
""",
            timeout=60,
            check=False,
        )
        if result.returncode == 255:
            lowered = result.stderr.lower()
            code = "ssh_auth_failed" if "permission denied" in lowered else "ssh_unreachable"
            raise RemoteFailure(code, str(redact(result.stderr[-1500:])), retryable=True)
        values: dict[str, Any] = {"missing": [], "existing_units": []}
        for line in result.stdout.splitlines():
            key, sep, value = line.partition("=")
            if not sep:
                continue
            if key == "MISSING":
                values["missing"].append(value)
            elif key == "EXISTING_UNIT":
                values["existing_units"].append(value)
            else:
                values[key.lower()] = value
        warnings: list[str] = []
        if values["missing"]:
            raise RemoteFailure("required_tool_missing", "missing tools: " + ", ".join(values["missing"]))
        if values.get("os_id") not in {"ubuntu", "debian"}:
            raise RemoteFailure("unsupported_os", "поддерживаются Ubuntu/Debian")
        if values.get("arch") not in {"x86_64", "aarch64"}:
            raise RemoteFailure("unsupported_arch", "поддерживаются x86_64/aarch64")
        if int(values.get("cpu", 0)) < MIN_CPU:
            raise RemoteFailure("not_enough_cpu", f"cpu < {MIN_CPU}")
        if int(values.get("disk_mb", 0)) < MIN_DISK_MB:
            raise RemoteFailure("not_enough_disk", f"disk free < {MIN_DISK_MB} MB")
        version = tuple(int(part) for part in str(values.get("python", "0")).split(".")[:2])
        if version < (3, 10):
            raise RemoteFailure("python_unsupported", "python < 3.10")
        if values.get("dns_rc") != "0":
            raise RemoteFailure("center_dns_failed", "DNS имени центра не работает")
        if values.get("curl_rc") != "0" or values.get("tls_verify") != "0":
            raise RemoteFailure("tls_failed", "HTTPS/TLS центра не подтверждён")
        if values.get("user_systemd_rc") != "0":
            raise RemoteFailure("user_systemd_unavailable", "systemd --user недоступен для SSH user")
        if values.get("uid") == "0":
            raise RemoteFailure("root_worker_forbidden", "укажите non-root SSH user для provider runtime")
        if values.get("linger") != "yes" and values.get("sudo_linger_rc") != "0":
            raise RemoteFailure(
                "sudo_required",
                "для autostart нужен linger; SSH user не имеет passwordless sudo "
                "ровно для /usr/bin/loginctl enable-linger <user>",
            )
        skew = values.get("clock_skew_sec")
        if skew not in {None, "unknown"} and int(skew) > 300:
            raise RemoteFailure("clock_skew", "расхождение часов с HTTPS center > 300 секунд")
        if skew == "unknown":
            warnings.append("center Date header отсутствует: clock skew не измерен")
        # Эти числа не выдаются за универсальный hard limit. Это documented
        # pilot profile (§9/technical design): hard gates выше доказаны smoke,
        # а профиль 4 CPU / 8 GiB / 100 GiB отображается предупреждением.
        if int(values.get("cpu", 0)) < 4:
            warnings.append("ниже pilot profile: CPU < 4")
        if int(values.get("ram_mb", 0)) < 8192:
            warnings.append("ниже pilot profile: RAM < 8 GiB")
        if int(values.get("disk_total_mb", 0)) < 100 * 1024:
            warnings.append("ниже pilot profile: total disk < 100 GiB")
        values["warnings"] = warnings
        return values

    def deploy_release(self, *, archive: Path, manifest: Path) -> dict[str, Any]:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        release = str(payload.get("release") or "")
        revision = str(payload.get("pipeline_revision") or "")
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", release):
            raise RemoteFailure("release_manifest_invalid", "unsafe release id")
        if not re.fullmatch(r"[A-Za-z0-9._:-]{1,200}", revision):
            raise RemoteFailure("release_manifest_invalid", "unsafe pipeline revision")
        actual = hashlib.sha256(archive.read_bytes()).hexdigest()
        if actual != payload.get("archive_sha256"):
            raise RemoteFailure("bundle_hash_mismatch", "локальный archive SHA-256 не совпал")
        problems = deploy.verify_artifact(archive, manifest)
        if problems:
            raise RemoteFailure("bundle_verification_failed", "; ".join(problems[:5]))
        remote = self._deploy_remote()
        deploy.remote_bootstrap_layout(remote)
        previous = deploy.remote_current_release(remote)
        if previous:
            self._snapshot_configuration(previous)
        self._copy(archive, f"{self.request.install_root}/incoming/{archive.name}")
        self._copy(manifest, f"{self.request.install_root}/incoming/{manifest.name}")
        deploy.remote_install_release(remote, archive.name, manifest.name, release, actual)
        deploy.remote_sync_venv(remote, release)
        deploy.remote_selftest(remote, release)
        deploy.remote_switch_current(remote, release)
        return {"release_id": release, "previous_release_id": previous or None, "archive_sha256": actual}

    def configure(self) -> dict[str, Any]:
        root = self.request.install_root
        from . import BOOTSTRAP_VERSION

        manifest_result = self._run(
            f"""set -eu
root={shlex.quote(root)}
python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print("PIPELINE_REVISION=" + str(d["pipeline_revision"])); print("RELEASE_ID=" + str(d["release"]))' "$root/current/MANIFEST.deploy.json"
sha256sum "$root/current/audit_worker/provider_policy.approved.json" | awk '{{print "POLICY_SHA256=" $1}}'
"""
        )
        manifest_values = dict(
            line.split("=", 1) for line in manifest_result.stdout.splitlines() if "=" in line
        )
        pipeline_revision = manifest_values.get("PIPELINE_REVISION", "").strip()
        release_id = manifest_values.get("RELEASE_ID", "").strip()
        if not pipeline_revision or not release_id:
            raise RemoteFailure(
                "release_manifest_invalid",
                "pipeline_revision/release отсутствуют",
            )
        values = {
            "PYTHONPATH": f"{root}/current",
            "AUDIT_WORKER_ROOT": f"{root}/data",
            "AUDIT_WORKER_DISPATCHER_URL": self.request.center_url,
            "AUDIT_WORKER_NAME": self.request.display_name,
            "AUDIT_WORKER_MAX_SLOTS": str(self.request.max_slots),
            "AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS": "1",
            "AUDIT_WORKER_PIPELINE_ROOT": f"{root}/current",
            "AUDIT_WORKER_PIPELINE_PYTHON": f"{root}/venv/bin/python",
            "AUDIT_WORKER_PIPELINE_REVISION": pipeline_revision,
            "AUDIT_WORKER_AUDIT_PIPELINE_ENABLED": "true",
            "AUDIT_WORKER_BOOTSTRAP_INSTANCE_ID": str(self.request.bootstrap_instance_id),
            "AUDIT_WORKER_BOOTSTRAP_VERSION": BOOTSTRAP_VERSION,
            "AUDIT_WORKER_PROVIDER_POLICY_VERSION": "1",
            "AUDIT_WORKER_PROVIDER_POLICY_SHA256": manifest_values.get(
                "POLICY_SHA256", ""
            ),
            "AUDIT_WORKER_ROUTING_COMPATIBILITY": (
                "claude_gpt_codex,codex_exec"
                if {"claude", "codex", "openrouter"}
                <= {provider.value for provider in self.request.providers}
                else ""
            ),
            "AUDIT_WORKER_ALLOW_REAL_LLM": "true",
            "AUDIT_WORKER_PIPELINE_PROVIDER_ENABLED": "true",
            "AUDIT_WORKER_PIPELINE_PROVIDER_AUTO_GRANT_ENABLED": "true",
            "AUDIT_WORKER_PIPELINE_PROVIDER_MAX_INFERENCES": "256",
            "AUDIT_WORKER_PIPELINE_PROVIDER_GRANT_TTL_SEC": "21600",
            "AUDIT_WORKER_PROVIDER_CLAUDE_AUTH_MODE": "ambient_user",
            "AUDIT_WORKER_PROVIDER_CODEX_AUTH_MODE": "ambient_user",
            "AUDIT_WORKER_PROVIDER_OPENROUTER_AUTH_MODE": "isolated_provider_home",
            "AUDIT_WORKER_RETENTION_ENABLED": "true",
            "AUDIT_WORKER_RETENTION_DELETE_ENABLED": "false",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONUNBUFFERED": "1",
        }
        content = "".join(
            f"{key}={shlex.quote(str(value))}\n" for key, value in values.items()
        )
        encoded = __import__("base64").b64encode(content.encode()).decode()
        result = self._run(
            f"""set -euo pipefail
root={shlex.quote(root)}
umask 077
mkdir -p "$root/config"
policy_source="$root/current/audit_worker/provider_policy.approved.json"
test -f "$policy_source"
install -m 0600 "$policy_source" "$root/data/provider_policy.json.new"
mv -f "$root/data/provider_policy.json.new" "$root/data/provider_policy.json"
printf %s {shlex.quote(encoded)} | base64 -d > "$root/config/worker.env.new"
chmod 600 "$root/config/worker.env.new"
mv -f "$root/config/worker.env.new" "$root/config/worker.env"
snapshot="$root/config/releases/{release_id}"
mkdir -p "$snapshot"
install -m 0600 "$root/config/worker.env" "$snapshot/worker.env.new"
mv -f "$snapshot/worker.env.new" "$snapshot/worker.env"
install -m 0600 "$root/data/provider_policy.json" "$snapshot/provider_policy.json.new"
mv -f "$snapshot/provider_policy.json.new" "$snapshot/provider_policy.json"
echo CONFIG_OK
"""
        )
        policy_hash = manifest_values.get("POLICY_SHA256", "")
        return {
            "configured": "CONFIG_OK" in result.stdout,
            "config_mode": "0600",
            "pipeline_revision": pipeline_revision,
            "bootstrap_version": BOOTSTRAP_VERSION,
            "policy_version": 1,
            "policy_sha256": policy_hash,
        }

    def install_services(self) -> dict[str, Any]:
        root = self.request.install_root
        agent_unit, executor_unit = bootstrap_units_for_root(root)
        unit_dir = "$HOME/.config/systemd/user"
        common = (
            "[Service]\nType=simple\nWorkingDirectory=" + root + "\n"
            "EnvironmentFile=" + root + "/config/worker.env\n"
        )
        units = {
            agent_unit: (
                "[Unit]\nDescription=audit-worker outbound HTTPS agent\n"
                "After=network-online.target\nWants=network-online.target\n\n"
                + common
                + f"ExecStart={root}/venv/bin/python -m audit_worker agent --root {root}/data\n"
                + f"StandardOutput=append:{root}/logs/agent.log\nStandardError=append:{root}/logs/agent.log\n"
                + "Restart=always\nRestartSec=5\nKillMode=process\nTimeoutStopSec=30\n"
                + "NoNewPrivileges=true\nProtectSystem=full\nProtectKernelTunables=true\n"
                + "RestrictSUIDSGID=true\nLockPersonality=true\n\n[Install]\nWantedBy=default.target\n"
            ),
            executor_unit: (
                "[Unit]\nDescription=audit-worker local executor\n\n"
                + common
                + f"ExecStart={root}/venv/bin/python -m audit_worker executor --root {root}/data\n"
                + f"StandardOutput=append:{root}/logs/executor.log\nStandardError=append:{root}/logs/executor.log\n"
                + "Restart=always\nRestartSec=5\nKillMode=process\nTimeoutStopSec=60\n"
                + "NoNewPrivileges=true\nProtectSystem=full\nProtectKernelTunables=true\n"
                + "RestrictSUIDSGID=true\nLockPersonality=true\n\n[Install]\nWantedBy=default.target\n"
            ),
        }
        commands = [
            "set -euo pipefail",
            "export XDG_RUNTIME_DIR=/run/user/$(id -u)",
            "if [ \"$(loginctl show-user \"$(id -un)\" -p Linger --value 2>/dev/null)\" != yes ]; then sudo -n /usr/bin/loginctl enable-linger \"$(id -un)\"; fi",
            f"mkdir -p {unit_dir} {shlex.quote(root + '/logs')}",
        ]
        import base64
        for name, content in units.items():
            encoded = base64.b64encode(content.encode()).decode()
            commands.append(f"printf %s {shlex.quote(encoded)} | base64 -d > {unit_dir}/{shlex.quote(name)}")
        commands.extend(
            [
                "systemctl --user daemon-reload",
                f"systemctl --user enable {shlex.quote(agent_unit)} {shlex.quote(executor_unit)} >/dev/null",
                "echo SERVICES_INSTALLED",
            ]
        )
        result = self._run("\n".join(commands))
        return {"units": [agent_unit, executor_unit], "installed": "SERVICES_INSTALLED" in result.stdout}

    def start_services(self) -> dict[str, Any]:
        units = bootstrap_units_for_root(self.request.install_root)
        output = deploy.remote_restart_units(self._deploy_remote(), units)
        return {"units": list(units), "output": output[-1000:]}

    def provider_status(self) -> dict[str, Any]:
        requested = {str(provider.value) for provider in self.request.providers}
        script = ["set +e"]
        for name in sorted(requested & {"claude", "codex"}):
            script.append(f"p=$HOME/.local/bin/{name}; [ -x \"$p\" ] || p=$(command -v {name} 2>/dev/null || true); [ -x \"$p\" ] && echo {name}=present || echo {name}=missing")
            script.append(f"[ -x \"$p\" ] && \"$p\" --version 2>/dev/null | head -1 | sed 's/^/{name}_version=/' || true")
            if name == "claude":
                script.append(
                    "[ -x \"$p\" ] && timeout 40 \"$p\" auth status 2>/dev/null "
                    "| grep -q '\"loggedIn\"[[:space:]]*:[[:space:]]*true' "
                    "&& echo claude_auth=ready || echo claude_auth=action_required"
                )
            else:
                # `codex login status` defines authentication through its exit
                # code.  Current Codex releases print the human-readable
                # confirmation to stderr, so grepping stdout produces a false
                # negative even though the command returned success.
                script.append(
                    'if [ -x "$p" ] && timeout 40 "$p" login status '
                    '>/dev/null 2>&1; then echo codex_auth=ready; '
                    'else echo codex_auth=action_required; fi'
                )
        if "openrouter" in requested:
            credential = shlex.quote(self.request.install_root + "/data/providers/openrouter/home/.openrouter/credentials.json")
            script.append(
                "python3 -c 'import os,stat,sys; s=os.lstat(sys.argv[1]); "
                "ok=stat.S_ISREG(s.st_mode) and s.st_uid==os.getuid() and "
                "(stat.S_IMODE(s.st_mode)&~0o600)==0 and 0<s.st_size<=4096; "
                "raise SystemExit(0 if ok else 1)' "
                f"{credential} 2>/dev/null && echo openrouter=present || "
                "echo openrouter=action_required"
            )
        result = self._run("\n".join(script), check=False)
        status: dict[str, str] = {}
        for line in result.stdout.splitlines():
            key, sep, value = line.partition("=")
            if sep:
                status[key] = value
        missing = [name for name in requested if status.get(name) == "missing"]
        pins = {"claude": "2.1.220", "codex": "0.147.0"}
        incompatible = [
            name for name in requested & set(pins)
            if status.get(name) == "present" and pins[name] not in status.get(name + "_version", "")
        ]
        auth_required = [
            name for name in requested
            if status.get(name) == "action_required"
            or (name in {"claude", "codex"} and status.get(name + "_auth") != "ready")
        ]
        return {
            "providers": status,
            "missing": missing,
            "incompatible": incompatible,
            "action_required": auth_required,
            "pinned_versions": pins,
        }

    def interactive_provider_auth(self, provider: str) -> int:
        """Attach operator TTY directly to VPS; center never receives output.

        In particular OpenRouter's key exists only in terminal input and the
        remote 0600 file. No pipe through the bootstrap process is involved.
        """
        if provider == "claude":
            command = (
                "p=$HOME/.local/bin/claude; [ -x \"$p\" ] || "
                "p=$(command -v claude); exec \"$p\" auth login --claudeai"
            )
        elif provider == "codex":
            command = (
                "p=$HOME/.local/bin/codex; [ -x \"$p\" ] || "
                "p=$(command -v codex); exec \"$p\" login --device-auth"
            )
        elif provider == "openrouter":
            path = self.request.install_root + "/data/providers/openrouter/home/.openrouter/credentials.json"
            command = (
                f"umask 077; mkdir -p {shlex.quote(str(Path(path).parent))}; "
                "read -rsp 'OpenRouter key: ' key; printf '\\n'; "
                f"printf %s \"$key\" > {shlex.quote(path)}; chmod 600 {shlex.quote(path)}; "
                "unset key; echo 'OpenRouter credential stored'"
            )
        else:
            raise ValueError("неизвестный provider")
        argv = ["ssh", "-t", *self._ssh_options(), self.target, command]
        secret_free_argv(argv)
        return subprocess.run(argv, check=False).returncode

    def install_provider_cli(self, provider: str) -> dict[str, Any]:
        """Установить заранее скачанный, pinned и хэшированный standalone CLI.

        Bootstrap не исполняет `curl | sh` и не знает слова `latest`. Артефакты
        находятся в центральном inventory JSON и проверяются дважды: до scp и
        после него. Provider auth homes не затрагиваются.
        """
        pins = {"claude": "2.1.220", "codex": "0.147.0"}
        if provider not in pins:
            raise ActionRequired("provider_secret_required", "OpenRouter требует интерактивный ввод на VPS")
        inventory_path = os.environ.get("AUDIT_WORKER_CLI_ARTIFACTS_FILE", "").strip()
        if not inventory_path:
            raise ActionRequired("cli_artifact_required", f"не настроен pinned artifact для {provider}")
        inventory = json.loads(Path(inventory_path).read_text(encoding="utf-8"))
        spec = inventory.get(provider) or {}
        if spec.get("version") != pins[provider]:
            raise RemoteFailure("cli_version_unpinned", f"{provider}: разрешена версия {pins[provider]}")
        artifact = Path(str(spec.get("path", ""))).resolve()
        expected = str(spec.get("sha256", ""))
        if not artifact.is_file() or len(expected) != 64:
            raise RemoteFailure("cli_artifact_invalid", f"{provider}: artifact отсутствует или не имеет SHA-256")
        actual = hashlib.sha256(artifact.read_bytes()).hexdigest()
        if actual != expected:
            raise RemoteFailure("cli_artifact_hash_mismatch", f"{provider}: central SHA-256 не совпал")
        incoming = f"{self.request.install_root}/incoming/{provider}-{pins[provider]}"
        self._copy(artifact, incoming)
        self._run(
            f"""set -euo pipefail
artifact={shlex.quote(incoming)}
actual=$(sha256sum "$artifact" | awk '{{print $1}}')
[ "$actual" = {shlex.quote(expected)} ] || exit 42
mkdir -p "$HOME/.local/bin"
install -m 0755 "$artifact" "$HOME/.local/bin/{provider}"
"$HOME/.local/bin/{provider}" --version >/dev/null
echo CLI_INSTALL_OK
"""
        )
        return {"provider": provider, "version": pins[provider], "sha256": expected, "installed": True}

    def _register_command(self, *, with_secret: bool, secret: str = "") -> dict[str, Any]:
        root = self.request.install_root
        remote_cmd = (
            f"set -a; . {shlex.quote(root)}/config/worker.env; set +a; "
            f"cd {shlex.quote(root)}/current; "
            f"PYTHONPATH={shlex.quote(root)}/current {shlex.quote(root)}/venv/bin/python "
            f"-m audit_worker register --root {shlex.quote(root)}/data"
            + (" --bootstrap-secret-stdin" if with_secret else "")
        )
        argv = ["ssh", *self._ssh_options(), self.target, remote_cmd]
        secret_free_argv(argv)
        result = subprocess.run(
            argv,
            input=(secret + "\n") if with_secret else None,
            text=True,
            capture_output=True,
            timeout=120,
        )
        if result.returncode:
            raise RemoteFailure("registration_failed", str(redact(result.stderr[-2000:])))
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RemoteFailure("registration_response_invalid", "worker register не вернул JSON") from exc

    def register(self, registration_token: str) -> dict[str, Any]:
        return self._register_command(with_secret=True, secret=registration_token)

    def claim(self) -> dict[str, Any]:
        return self._register_command(with_secret=False)

    def health(self) -> dict[str, Any]:
        remote = self._deploy_remote()
        units = bootstrap_units_for_root(self.request.install_root)
        return deploy.remote_health(remote, units)

    def rollback(self, release_id: str | None) -> dict[str, Any]:
        remote = self._deploy_remote()
        releases = deploy.remote_list_releases(remote)
        current = deploy.remote_current_release(remote)
        target = release_id
        if not target:
            candidates = [item for item in sorted(releases) if item != current]
            if not candidates:
                raise RemoteFailure("rollback_target_missing", "нет другого установленного релиза")
            target = candidates[-1]
        if target not in releases:
            raise RemoteFailure("rollback_target_missing", "указанный релиз не установлен")
        self._restore_configuration(target)
        deploy.remote_switch_current(remote, target)
        deploy.remote_restart_units(remote, bootstrap_units_for_root(self.request.install_root))
        return {"release_id": target, "previous_release_id": current or None}

    def _snapshot_configuration(self, release_id: str) -> None:
        """Preserve non-provider-secret config before an update overwrites it."""
        self._run(
            f"""set -euo pipefail
root={shlex.quote(self.request.install_root)}
rel={shlex.quote(release_id)}
snapshot="$root/config/releases/$rel"
mkdir -p "$snapshot"
if [ -f "$root/config/worker.env" ]; then
  [ -f "$snapshot/worker.env" ] || install -m 0600 "$root/config/worker.env" "$snapshot/worker.env"
elif [ ! -f "$snapshot/worker.env" ]; then
  touch "$snapshot/worker.env.absent"
fi
if [ -f "$root/data/provider_policy.json" ]; then
  [ -f "$snapshot/provider_policy.json" ] || install -m 0600 "$root/data/provider_policy.json" "$snapshot/provider_policy.json"
elif [ ! -f "$snapshot/provider_policy.json" ]; then
  touch "$snapshot/provider_policy.json.absent"
fi
echo CONFIG_SNAPSHOT_OK
"""
        )

    def _restore_configuration(self, release_id: str) -> None:
        self._run(
            f"""set -euo pipefail
root={shlex.quote(self.request.install_root)}
rel={shlex.quote(release_id)}
snapshot="$root/config/releases/$rel"
if [ -f "$snapshot/worker.env" ]; then
  install -m 0600 "$snapshot/worker.env" "$root/config/worker.env.new"
  mv -f "$root/config/worker.env.new" "$root/config/worker.env"
elif [ -f "$snapshot/worker.env.absent" ]; then
  rm -f "$root/config/worker.env"
else
  exit 44
fi
if [ -f "$snapshot/provider_policy.json" ]; then
  install -m 0600 "$snapshot/provider_policy.json" "$root/data/provider_policy.json.new"
  mv -f "$root/data/provider_policy.json.new" "$root/data/provider_policy.json"
elif [ -f "$snapshot/provider_policy.json.absent" ]; then
  rm -f "$root/data/provider_policy.json"
else
  exit 45
fi
echo CONFIG_RESTORE_OK
"""
        )

    def uninstall(self) -> dict[str, Any]:
        root = shlex.quote(self.request.install_root)
        units = bootstrap_units_for_root(self.request.install_root)
        joined = " ".join(shlex.quote(unit) for unit in units)
        # data и provider auth намеренно сохраняются: операция обратима. Полное
        # удаление данных — отдельное явно разрушительное действие, не 11K.
        self._run(
            f"""set -euo pipefail
export XDG_RUNTIME_DIR=/run/user/$(id -u)
for unit in {joined}; do
  systemctl --user disable --now "$unit" >/dev/null 2>&1 || true
  rm -f "$HOME/.config/systemd/user/$unit"
done
systemctl --user daemon-reload
root={root}
backup="$root/.uninstalled-{self.session_id}"
mkdir -p "$backup"
for item in app current venv config incoming; do
  [ ! -e "$root/$item" ] || mv "$root/$item" "$backup/$item"
done
echo UNINSTALL_OK
"""
        )
        return {"uninstalled": True, "data_preserved": True, "provider_auth_preserved": True}
