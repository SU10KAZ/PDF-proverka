from __future__ import annotations

import re
from pathlib import Path

from backend.app.services.worker_bootstrap.remote import SSHBootstrapRemote
from backend.app.services.worker_bootstrap import manager as bootstrap_manager
from scripts import deploy_audit_worker as deploy


class _RecordingRemote:
    root = "/home/auditworker_11l/audit-worker-11l"

    def __init__(self) -> None:
        self.script = ""

    def run(self, script: str, **_kwargs):
        self.script = script
        return type("Result", (), {"stdout": "INSTALL_OK\n"})()


def test_remote_tree_verifier_heredoc_is_valid_python() -> None:
    remote = _RecordingRemote()
    deploy.remote_install_release(
        remote,
        "bundle.tar.gz",
        "bundle.manifest.json",
        "release-11l",
        "a" * 64,
    )
    match = re.search(
        r"<<'TREE_VERIFY_PY'\n(?P<source>.*?)\nTREE_VERIFY_PY\n",
        remote.script,
        flags=re.DOTALL,
    )
    assert match is not None
    compile(match.group("source"), "<tree-verify>", "exec")
    assert 'digest.update(b"\\n")' in match.group("source")


def test_preflight_accepts_least_privilege_linger_sudo() -> None:
    source = Path(SSHBootstrapRemote.preflight.__code__.co_filename).read_text(
        encoding="utf-8"
    )
    assert 'sudo -n -l /usr/bin/loginctl enable-linger "$(id -un)"' in source
    assert "sudo -n true" not in source
    assert "sudo -n /usr/bin/loginctl enable-linger" in source


def test_provider_action_required_clears_stale_failure_fields() -> None:
    source = Path(bootstrap_manager.__file__).read_text(encoding="utf-8")
    provider_transition = source.split(
        'if providers.get("action_required"):', 1
    )[1].split("requested_provider_names", 1)[0]
    assert 'fields={"error_code": None, "error_detail": None}' in provider_transition
