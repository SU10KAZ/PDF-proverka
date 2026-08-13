#!/usr/bin/env python3
"""Operator CLI for one-click worker bootstrap (same manager as HTTP API)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.worker_bootstrap.manager import BootstrapManager
from backend.app.services.worker_bootstrap.models import BootstrapOperation, BootstrapRequest
from backend.app.services.worker_bootstrap.remote import SSHBootstrapRemote


def _print(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def _load_spec(path: str) -> BootstrapRequest:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return BootstrapRequest.model_validate(raw)


def cmd_operation(args: argparse.Namespace) -> int:
    manager = BootstrapManager()
    session = manager.create(
        operation=BootstrapOperation(args.command),
        request=_load_spec(args.spec),
        idempotency_key=args.idempotency_key,
    )
    if not args.no_run:
        session = manager.run(session["session_id"])
    _print(session)
    return 0 if session["state"] in {"succeeded", "action_required", "queued"} else 1


def cmd_resume(args: argparse.Namespace) -> int:
    session = BootstrapManager().run(args.session_id)
    _print(session)
    return 0 if session["state"] in {"succeeded", "action_required"} else 1


def cmd_status(args: argparse.Namespace) -> int:
    manager = BootstrapManager()
    if args.session_id:
        _print(manager.get(args.session_id))
    else:
        _print(manager.list(limit=args.limit))
    return 0


def cmd_update_center_url(args: argparse.Namespace) -> int:
    session = BootstrapManager().update_center_url(args.session_id, args.center_url)
    _print(session)
    return 0


def cmd_provider_auth(args: argparse.Namespace) -> int:
    manager = BootstrapManager()
    session = manager.get(args.session_id)
    request = BootstrapRequest.model_validate(session["request"])
    remote = SSHBootstrapRemote(request, session["session_id"], manager.settings)
    # enrollment is repeated to reject a changed key before attaching the TTY.
    remote.enroll()
    return_code = remote.interactive_provider_auth(args.provider)
    if return_code:
        return return_code
    resumed = manager.run(args.session_id)
    _print(resumed)
    return 0 if resumed["state"] in {"succeeded", "action_required"} else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="audit-worker-bootstrap")
    sub = parser.add_subparsers(dest="command", required=True)
    for operation in BootstrapOperation:
        command = sub.add_parser(operation.value)
        command.add_argument("--spec", required=True, help="JSON spec; secret values are forbidden")
        command.add_argument("--idempotency-key")
        command.add_argument("--no-run", action="store_true")
        command.set_defaults(func=cmd_operation)
    resume = sub.add_parser("resume")
    resume.add_argument("session_id")
    resume.set_defaults(func=cmd_resume)
    update_center = sub.add_parser("update-center-url")
    update_center.add_argument("session_id")
    update_center.add_argument("center_url")
    update_center.set_defaults(func=cmd_update_center_url)
    status = sub.add_parser("session-status")
    status.add_argument("session_id", nargs="?")
    status.add_argument("--limit", type=int, default=100)
    status.set_defaults(func=cmd_status)
    auth = sub.add_parser("provider-auth")
    auth.add_argument("session_id")
    auth.add_argument("provider", choices=["claude", "codex", "openrouter"])
    auth.set_defaults(func=cmd_provider_auth)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
