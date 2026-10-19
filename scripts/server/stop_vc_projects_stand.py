#!/usr/bin/env python3
"""Stop a setsid-launched stand and its workers, without matching other servers.

The stand launcher writes the session leader PID to run/stand.pid. Validate
that leader before signalling its private process group; a stale PID file must
never stop an unrelated process. Linux /proc and pidfds are required.
"""
from __future__ import annotations

import argparse
import os
import signal
import time
from pathlib import Path


def _process(pid: int) -> tuple[str, int, int, int] | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return fields[0], int(fields[2]), int(fields[3]), int(fields[19])
    except FileNotFoundError:
        return None


def _members(group: int) -> dict[int, tuple[str, int, int, int]]:
    members = {}
    for path in Path("/proc").iterdir():
        if path.name.isdecimal():
            pid = int(path.name)
            state = _process(pid)
            if state and state[0] != "Z" and state[1:3] == (group, group):
                members[pid] = state
    return members


def _wait(group: int, timeout: float) -> dict[int, tuple[str, int, int, int]]:
    deadline = time.monotonic() + timeout
    while members := _members(group):
        if time.monotonic() >= deadline:
            return members
        time.sleep(0.05)
    return {}


def _signal_members(members: dict[int, tuple[str, int, int, int]], sig: int) -> None:
    for child, observed in members.items():
        try:
            fd = os.pidfd_open(child)
        except ProcessLookupError:
            continue
        try:
            current = _process(child)
            if current and current[1:] == observed[1:]:
                signal.pidfd_send_signal(fd, sig)
        except ProcessLookupError:
            pass
        finally:
            os.close(fd)


def _owned_process(pid: int, stand: Path) -> list[bytes]:
    proc = Path(f"/proc/{pid}")
    if proc.stat().st_uid != os.getuid() or proc.joinpath("cwd").resolve() != stand / "code":
        raise RuntimeError("PID file belongs to another process; no signal sent")
    return proc.joinpath("cmdline").read_bytes().split(b"\0")


def stop(stand: Path, timeout: float = 10) -> list[int]:
    stand = stand.resolve()
    pidfile = stand / "run/stand.pid"
    try:
        original = pidfile.read_text()
    except FileNotFoundError:
        return []
    pid = int(original.strip())
    if pid <= 1 or pid == os.getpgrp():
        raise RuntimeError("Refusing an unsafe process group")
    leader = _process(pid)
    members = _members(pid)
    if leader is None and not members:
        if pidfile.read_text() == original:
            pidfile.unlink()
        return []
    if leader is not None and leader[1:3] != (pid, pid):
        raise RuntimeError("PID file does not identify a live private session leader")
    if any(Path(f"/proc/{child}").stat().st_uid != os.getuid() for child in members):
        raise RuntimeError("Process group contains another user's process")
    if leader is not None and leader[0] != "Z":
        argv = _owned_process(pid, stand)
        expected = [b"-m", b"uvicorn", b"stand_app:app", b"--app-dir", os.fsencode(stand / "app")]
        if argv[1:6] != expected:
            raise RuntimeError("PID file belongs to another process; no signal sent")
        # All workers receive TERM even if uvicorn exits before its callbacks.
        current = _process(pid)
        if current is None or current[1:] != leader[1:] or pidfile.read_text() != original:
            raise RuntimeError("Stand identity changed during shutdown; no signal sent")
        os.killpg(pid, signal.SIGTERM)
    else:
        # A previous parent-only stop may already have removed the server. Only
        # accept known multiprocessing helpers in this stand's private session.
        for child in members:
            argv = _owned_process(child, stand)
            try:
                code = argv[argv.index(b"-c") + 1]
            except (ValueError, IndexError):
                code = b""
            if not code.startswith((b"from multiprocessing.spawn import spawn_main;",
                                    b"from multiprocessing.resource_tracker import main;")):
                raise RuntimeError("Unrecognized orphan process; no signal sent")
        if pidfile.read_text() != original:
            raise RuntimeError("PID file changed; no signal sent")
        _signal_members(members, signal.SIGTERM)
    _signal_members(_wait(pid, timeout), signal.SIGKILL)
    if _wait(pid, 3):
        raise RuntimeError("Stand still has live processes after SIGKILL")
    if pidfile.read_text() == original:
        pidfile.unlink()
    return sorted(members)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stand", type=Path)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    if args.timeout < 0:
        parser.error("--timeout must not be negative")
    try:
        stopped = stop(args.stand, args.timeout)
    except (OSError, ValueError, RuntimeError) as exc:
        parser.exit(1, f"Stand shutdown refused/failed: {exc}\n")
    print(f"Stand stopped; process group members: {stopped}" if stopped else "Stand is not running")


if __name__ == "__main__":
    main()
