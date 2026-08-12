"""Process-scoped, synchronized chaos helpers for 12E.

The harness launches the real Gateway module in a fresh interpreter, backed
only by the isolated test DB supplied by the caller.  A multiprocessing child
would inherit (or race with) grpcio/SQLite thread state from pytest and is not
a credible service-restart model.  This is intentionally not a shell script:
each lifecycle transition has a concrete process/readiness observation.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_loopback_listener(process: subprocess.Popen[Any], port: int) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"isolated Gateway exited during startup ({process.returncode})")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    process.terminate()
    process.wait(timeout=5)
    raise RuntimeError("isolated Gateway did not bind its loopback port")


@dataclass
class GatewayProcess:
    """An independently killable, clean-process isolated Gateway."""

    port: int
    process: subprocess.Popen[Any]

    @classmethod
    def start(cls, *, worker_settings: Any, port: int | None = None) -> "GatewayProcess":
        """Launch the actual module against the caller's isolated settings."""
        selected = port or free_loopback_port()
        data_dir = str(worker_settings.data_dir)
        configured = os.environ.get("DISTRIBUTED_WORKERS_DATA_DIR", "")
        if Path(configured).resolve() != Path(data_dir).resolve():
            raise RuntimeError("12E harness settings are not the isolated environment settings")
        env = dict(os.environ)
        env.update(
            {
                "PYTHONUNBUFFERED": "1",
                "AGENT_GATEWAY_HOST": "127.0.0.1",
                "AGENT_GATEWAY_PORT": str(selected),
                "AGENT_GATEWAY_ENVIRONMENT": "test",
                "AGENT_GATEWAY_SECURITY_MODE": "test_insecure",
                "AGENT_GATEWAY_OFFER_POLL_SEC": "0.02",
                "AGENT_GATEWAY_HEARTBEAT_TIMEOUT_SEC": "30",
                "AGENT_GATEWAY_IDLE_TIMEOUT_SEC": "40",
                "AGENT_GATEWAY_SHUTDOWN_SEC": "0.1",
            }
        )
        process = subprocess.Popen(  # noqa: S603 - fixed module and controlled env
            [sys.executable, "-m", "backend.app.agent_gateway"],
            cwd=str(Path(__file__).resolve().parents[1]),
            env=env,
            stdin=subprocess.DEVNULL,
            # Test subprocess diagnostics are safe (Gateway redacts control
            # payloads) and make a crash/restart fault actionable in CI.
            stdout=None,
            stderr=None,
        )
        _wait_for_loopback_listener(process, selected)
        return cls(port=selected, process=process)

    def sigkill(self) -> None:
        """Hard process death: no Gateway drain or application cleanup runs."""
        self.process.kill()
        self.process.wait(timeout=10)
        if self.process.poll() is None:
            raise RuntimeError("isolated Gateway survived SIGKILL")

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
