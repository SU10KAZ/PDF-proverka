"""
run_stage02_findings_only_gpt54.py
----------------------------------
CLI-обёртка над qwen_findings_only.run_findings_only_for_project.

Production-готовый stage 02 в режиме findings-only через GPT-5.4 (low, OpenRouter).
Для каждого блока: PNG + qwen-описание + extended categories → {"findings": [...]} → адаптация.
Перезаписывает _output/02_blocks_analysis.json (с бэкапом в .classic.bak.json).

Использование:
  python scripts/run_stage02_findings_only_gpt54.py "projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.1-К1К2 (2).pdf"
  python scripts/run_stage02_findings_only_gpt54.py <proj> --blocks bid1,bid2
  python scripts/run_stage02_findings_only_gpt54.py <proj> --dry-run
  python scripts/run_stage02_findings_only_gpt54.py <proj> --reasoning-effort medium
  python scripts/run_stage02_findings_only_gpt54.py <proj> --no-extended-prompt
  python scripts/run_stage02_findings_only_gpt54.py <proj> --parallelism 5

ENV: OPENROUTER_API_KEY (из .env).
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env")
sys.path.insert(0, str(_ROOT))

from qwen_findings_only import (  # noqa: E402
    DEFAULT_EFFORT, DEFAULT_MAX_TOKENS, DEFAULT_MODEL, DEFAULT_PARALLELISM,
    FindingsOnlyError, check_prerequisites, get_enrichment,
    load_categories_for_section, run_findings_only_for_project,
)


def _print_progress(event: dict) -> None:
    t = event.get("type")
    if t == "started":
        print(
            f"[plan] blocks={event['blocks_total']}  model={event['model']}  "
            f"effort={event['reasoning_effort']}  extended={event['extended_prompt']}  "
            f"section={event['section']}  enrichment={event['enrichment_sources']}  "
            f"skipped(no enrichment)={event['skipped_no_enrichment']}"
        )
    elif t == "block_done":
        status = "OK" if event.get("ok") else "FAIL"
        err = f" err={event.get('error', '')[:80]}" if not event.get("ok") and event.get("error") else ""
        print(
            f"  [{status}] {event['block_id']} page={event['page']} "
            f"t={(event.get('elapsed_ms') or 0)/1000:.1f}s "
            f"in={event.get('input_tokens')} out={event.get('output_tokens')} "
            f"reason={event.get('reasoning_tokens')} findings={event.get('findings')} "
            f"[{event['completed']}/{event['total']}]" + err,
            flush=True,
        )
    elif t == "block_skip":
        print(f"  [SKIP] {event['block_id']} reason={event['reason']}", flush=True)


async def run(args) -> int:
    project_dir = Path(args.project).resolve()
    if not project_dir.exists():
        print(f"[error] project dir not found: {project_dir}", file=sys.stderr)
        return 2

    if args.dry_run:
        check = check_prerequisites(project_dir)
        print(f"[plan] project={project_dir.name}  blocks={check['blocks_total']}  "
              f"with_enrichment={check['with_enrichment']}  ok={check['ok']}")
        for r in check["reasons"]:
            print(f"  - {r}")
        return 0 if check["ok"] else 2

    try:
        result = await run_findings_only_for_project(
            project_dir,
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            extended_prompt=not args.no_extended_prompt,
            max_tokens=args.max_tokens,
            parallelism=args.parallelism,
            blocks_filter=[s.strip() for s in args.blocks.split(",") if s.strip()] if args.blocks else None,
            on_progress=_print_progress,
        )
    except FindingsOnlyError as e:
        print(f"[error] {e}", file=sys.stderr)
        return 2

    summary = result["summary"]
    totals = summary["totals"]
    if result["run_dir"] is not None:
        print(f"[run] log dir: {result['run_dir'].relative_to(project_dir)}")
    target = project_dir / "_output" / "02_blocks_analysis.json"
    bak = target.with_suffix(".classic.bak.json")
    if bak.exists() and bak.stat().st_mtime > target.stat().st_mtime - 3600:
        print(f"[backup] {bak.name} saved")
    print(f"[write] {target.relative_to(project_dir)}")

    print(
        f"\n[done] blocks={summary['blocks_ok']}/{summary['blocks_total']} ok  "
        f"failed={summary['blocks_failed']}  skipped(no enrichment)={summary['blocks_skipped_no_enrichment']}  "
        f"findings={totals['findings']}  in={totals['input_tokens']} out={totals['output_tokens']} "
        f"reason={totals['reasoning_tokens']}  ~${totals['estimated_cost_usd_total']:.3f}  "
        f"wall={summary['wall_clock_s']}s"
    )
    return 0 if summary["blocks_failed"] == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage 02 findings-only через GPT-5.4 + qwen-enrichment")
    parser.add_argument("project", type=str, help="путь к проекту (projects/<...>/<...>.pdf)")
    parser.add_argument("--blocks", type=str, default=None, help="только эти block_id, через запятую")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    parser.add_argument("--reasoning-effort", type=str, default=DEFAULT_EFFORT,
                        choices=["minimal", "low", "medium", "high", ""])
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    parser.add_argument("--parallelism", type=int, default=DEFAULT_PARALLELISM)
    parser.add_argument("--no-extended-prompt", action="store_true",
                        help="не подключать prompts/disciplines/<SECTION>/finding_categories.md")
    parser.add_argument("--dry-run", action="store_true",
                        help="только проверка пререквизитов и enrichment-sources")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
