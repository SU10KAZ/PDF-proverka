#!/usr/bin/env python3
"""Fail closed when a development mutation starts from the production Git root."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


DEFAULT_PRODUCTION_ROOT = Path("/home/coder/projects/PDF-proverka")
PRODUCTION_ROOT_ENV = "AUDITMANAGER_PRODUCTION_GIT_ROOT"


def resolve_git_toplevel(cwd: Path) -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        raise RuntimeError("git_toplevel_unavailable")
    return Path(completed.stdout.strip()).resolve()


def mutation_allowed(git_toplevel: Path, production_root: Path) -> bool:
    return git_toplevel.resolve() != production_root.resolve()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Guard autonomous development against production-root mutations."
    )
    parser.add_argument("--intent", choices=("read", "mutate"), default="mutate")
    parser.add_argument("--cwd", type=Path, default=Path.cwd())
    parser.add_argument(
        "--production-root",
        type=Path,
        default=Path(os.environ.get(PRODUCTION_ROOT_ENV, DEFAULT_PRODUCTION_ROOT)),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        git_toplevel = resolve_git_toplevel(args.cwd)
    except RuntimeError:
        print("DEVELOPMENT_WORKTREE_GUARD=BLOCKED git_toplevel_unavailable", file=sys.stderr)
        return 3

    if args.intent == "read":
        print(f"DEVELOPMENT_WORKTREE_GUARD=PASS intent=read git_toplevel={git_toplevel}")
        return 0

    if not mutation_allowed(git_toplevel, args.production_root):
        print(
            "DEVELOPMENT_WORKTREE_GUARD=BLOCKED intent=mutate "
            f"production_root={args.production_root.resolve()} "
            "create/use a separate Git worktree before changing files",
            file=sys.stderr,
        )
        return 2

    print(f"DEVELOPMENT_WORKTREE_GUARD=PASS intent=mutate git_toplevel={git_toplevel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
