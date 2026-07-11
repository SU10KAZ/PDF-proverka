"""Legacy CLI alias for local block-context preparation.

The historical command name is retained for operator scripts, but it no longer
loads a model or performs network OCR.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from backend.app.pipeline.stages.block_context.builder import build_block_context
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    STAGE02_BLOCKS_DIRNAME,
    gemma_output_root,
)


async def _run(project_dir: Path) -> dict:
    output_dir = gemma_output_root(project_dir)
    return await build_block_context(
        project_dir,
        output_dir=output_dir,
        blocks_index_path=output_dir / STAGE02_BLOCKS_DIRNAME / "index.json",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Собрать локальный контекст блоков")
    parser.add_argument("project_dir", type=Path)
    parser.add_argument("--force", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--parallelism", help=argparse.SUPPRESS)
    parser.add_argument("--model", help=argparse.SUPPRESS)
    parser.add_argument("--timeout", help=argparse.SUPPRESS)
    args = parser.parse_args()
    summary = asyncio.run(_run(args.project_dir.resolve()))
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("status") in {"ok", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
