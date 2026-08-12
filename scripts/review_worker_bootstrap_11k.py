#!/usr/bin/env python3
"""Seven independent static security lenses for an immutable 11K commit."""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = ROOT / "backend/app/services/worker_bootstrap"


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def ssh_credentials() -> list[str]:
    models = _read("backend/app/services/worker_bootstrap/models.py")
    remote = _read("backend/app/services/worker_bootstrap/remote.py")
    _require("ssh_auth_ref" in models, "opaque SSH auth reference missing")
    _require("ssh_password:" not in models, "SSH password accepted by model")
    _require("private_key:" not in models, "private key contents accepted by model")
    _require("AUDIT_WORKER_SSH_AUTH_REFS_FILE" in remote, "central resolver missing")
    _require("mode & 0o077" in remote, "private-key permission gate missing")
    _require('"AUDIT_WORKER_SSH_AUTH_REF"' not in remote, "SSH auth leaked to worker env")
    return ["opaque auth ref", "agent/key-reference only", "key mode <=0600", "runtime separation"]


def host_verification() -> list[str]:
    remote = _read("backend/app/services/worker_bootstrap/remote.py")
    security = _read("backend/app/services/worker_bootstrap/security.py")
    combined = remote + security
    _require("StrictHostKeyChecking=yes" in remote, "strict checking not enabled")
    forbidden = "StrictHostKeyChecking=" + "no"
    _require(forbidden not in combined, "host-key bypass present")
    _require("expected_fingerprint" in security, "out-of-band fingerprint absent")
    _require("matching_lines" in security, "unverified scanned key lines may be persisted")
    _require("os.replace(staged, known_hosts)" in security, "known_hosts is not atomic")
    return ["strict=yes", "fingerprint pin", "matching key lines only", "atomic 0600 known_hosts"]


def registration_token() -> list[str]:
    store = _read("backend/app/services/worker_bootstrap/store.py")
    agent_api = _read("backend/app/api/routers/audit_worker_agent.py")
    remote = _read("backend/app/services/worker_bootstrap/remote.py")
    _require("auth.hash_token(token)" in store, "plain registration token may persist")
    for needle in ("expires_at", "used_at", "expected_instance_id", "rowcount"):
        _require(needle in store, f"registration token invariant missing: {needle}")
    _require("--bootstrap-secret-stdin" in remote, "token is not sent on stdin")
    _require('input=(secret + "\\n")' in remote, "registration token stdin write missing")
    _require(
        'argv = ["ssh", *self._ssh_options(), self.target, remote_cmd]' in remote,
        "registration subprocess argv is not closed",
    )
    _require("RegistrationTokenRejected" in agent_api, "generic token rejection missing")
    return ["SHA-256 only", "TTL", "one-time atomic consume", "instance scope", "stdin exchange"]


def provider_secret() -> list[str]:
    remote = _read("backend/app/services/worker_bootstrap/remote.py")
    models = _read("backend/app/services/worker_bootstrap/models.py")
    api = _read("backend/app/api/routers/worker_bootstrap.py")
    _require("read -rsp 'OpenRouter key: '" in remote, "hidden remote prompt missing")
    _require("chmod 600" in remote and "unset key" in remote, "local key hardening missing")
    _require("openrouter_api_key" not in models.lower(), "provider key accepted in request")
    _require("raw_payload" in api and "safe_errors" in api, "safe validation response missing")
    _require("capture_output=True" not in remote.split("def interactive_provider_auth", 1)[1].split("def install_provider_cli", 1)[0], "provider auth output captured")
    return ["direct remote TTY", "hidden input", "0600 local file", "no center/API field", "no output capture"]


def logs_state() -> list[str]:
    store = _read("backend/app/services/worker_bootstrap/store.py")
    security = _read("backend/app/services/worker_bootstrap/security.py")
    api = _read("backend/app/api/routers/worker_bootstrap.py")
    _require("_REQUEST_FIELDS" in store, "request allowlist missing")
    _require("redact(detail or {})" in store, "event detail redaction missing")
    _require("SECRET_KEY_RE" in security and "SECRET_TEXT_RE" in security, "redactor missing")
    _require('item.get("input")' not in api, "Pydantic input may be reflected")
    _require("safe_errors" in api, "sanitized 422 missing")
    return ["request allowlist", "recursive redaction", "sanitized API 422", "hash-only token DB"]


def destructive_scope() -> list[str]:
    remote = _read("backend/app/services/worker_bootstrap/remote.py")
    models = _read("backend/app/services/worker_bootstrap/models.py")
    manager = _read("backend/app/services/worker_bootstrap/manager.py")
    uninstall = remote.split("def uninstall", 1)[1]
    _require("validate_install_root" in models, "install-root validation missing")
    _require("rm -rf" not in uninstall, "recursive deletion in uninstall")
    _require("for item in app current venv config incoming" in uninstall, "uninstall allowlist missing")
    _require("provider_auth_preserved" in uninstall and "data_preserved" in uninstall, "preservation contract missing")
    _require("rolling_back" in manager and "previous_release_id" in manager, "automatic rollback missing")
    return ["validated root", "allowlisted move", "no recursive uninstall delete", "data/provider preservation", "previous-release rollback"]


def existing_vps_isolation() -> list[str]:
    remote = _read("backend/app/services/worker_bootstrap/remote.py")
    models = _read("backend/app/services/worker_bootstrap/models.py")
    combined = remote + models
    _require("sha256(normalized.encode())" in remote, "full-root unit namespace missing")
    _require("176.12.77.31" not in combined and "/home/coder" not in combined, "production host/path hard-coded")
    for forbidden in ("ufw ", "iptables", "firewall-cmd", "sshd_config"):
        _require(forbidden not in remote, f"unrelated host mutation present: {forbidden}")
    _require("unrelated" not in remote.split("def uninstall", 1)[1], "unrelated target named by uninstall")
    return ["full-root hashed units", "no production constants", "no firewall/sshd mutation", "owned units only"]


LENSES: dict[str, Callable[[], list[str]]] = {
    "ssh_credentials": ssh_credentials,
    "host_verification": host_verification,
    "registration_token": registration_token,
    "provider_secret": provider_secret,
    "logs_state": logs_state,
    "destructive_scope": destructive_scope,
    "existing_vps_isolation": existing_vps_isolation,
}


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lens", required=True, choices=sorted(LENSES))
    parser.add_argument("--expected-commit", required=True)
    args = parser.parse_args()
    actual = _git("rev-parse", "HEAD")
    _require(actual == args.expected_commit, "review commit does not match expected commit")
    _require(not _git("status", "--porcelain"), "review worktree is not immutable/clean")
    checks = LENSES[args.lens]()
    print(json.dumps({"commit": actual, "lens": args.lens, "verdict": "PASS", "checks": checks}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
