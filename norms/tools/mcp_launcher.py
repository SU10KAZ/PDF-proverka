#!/usr/bin/env python3
"""Запустить release-owned MCP server через Python общего norms runtime."""
from __future__ import annotations

import os
import sys
from pathlib import Path


def main() -> int:
    code_tools = Path(__file__).resolve().parent
    default_runtime_tools = code_tools
    for parent in code_tools.parents:
        if parent.name == "releases":
            default_runtime_tools = parent.parent / "shared" / "norms" / "tools"
            break
    runtime_tools = Path(
        os.environ.get("NORMS_TOOLS_PATH", str(default_runtime_tools))
    ).expanduser()
    python = Path(
        os.environ.get(
            "NORMS_MCP_PYTHON",
            str(runtime_tools / "venv" / "bin" / "python"),
        )
    ).expanduser()
    server = code_tools / "mcp_server.py"
    if not python.is_file():
        print(f"norms MCP python not found: {python}", file=sys.stderr)
        return 78
    if not server.is_file():
        print(f"norms MCP server not found: {server}", file=sys.stderr)
        return 78
    env = dict(os.environ)
    env.setdefault("NORMS_TOOLS_PATH", str(runtime_tools))
    env.setdefault("NORMS_MCP_PYTHON", str(python))
    os.execve(str(python), [str(python), str(server)], env)
    return 70  # pragma: no cover — успешный execve не возвращается


if __name__ == "__main__":
    raise SystemExit(main())
