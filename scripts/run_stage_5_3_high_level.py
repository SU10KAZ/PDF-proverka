#!/usr/bin/env python3
"""Run additive Stage 5.3 synthesis for existing Stage 5 comparison pairs."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison import store  # noqa: E402


async def _run(session_id: str, pair_ids: list[str], allow_ai: bool) -> int:
    failed = 0
    for pair_id in pair_ids:
        try:
            artifact = await store.run_high_level_project_changes(
                session_id, pair_id, allow_ai=allow_ai,
            )
            print(json.dumps({
                "session_id": session_id, "pair_id": pair_id,
                "status": artifact.get("status"), "summary": artifact.get("summary"),
                "artifact": str(
                    store.paths_mod.high_level_project_changes_path(session_id, pair_id)
                ),
            }, ensure_ascii=False))
        except Exception as exc:  # noqa: BLE001 - batch reports every pair
            failed += 1
            print(json.dumps({
                "session_id": session_id, "pair_id": pair_id,
                "error": f"{type(exc).__name__}:{exc}",
            }, ensure_ascii=False))
    return failed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True)
    parser.add_argument("--pair", action="append", required=True)
    parser.add_argument("--no-ai", action="store_true")
    args = parser.parse_args()
    return 1 if asyncio.run(_run(args.session, args.pair, not args.no_ai)) else 0


if __name__ == "__main__":
    raise SystemExit(main())
