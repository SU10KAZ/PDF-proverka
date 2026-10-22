#!/usr/bin/env python3
"""Replay Stage 02 (text analysis) with another model on expert-reviewed documents.

The production prompt is built exactly as the live stage builds it for Codex
(``prompt_builder.build_text_analysis_message_sets``) from an isolated copy of
the version; only the model call differs. Results are written in the replay
format of ``run_stage01_leg_replacement_replay.py`` (one pseudo block ``TEXT``
per document), so its ``score`` / ``report`` subcommands work unchanged.

Subcommands:

* ``select`` — documents whose reviewed findings came from the text stage in
  runs after the OpenRouter leg was switched off; references are the
  expert decisions on those text-sourced findings.
* ``run``    — one variant over the selection; ``prepare`` of the Stage 01
  replay script builds the workspace from the same selection file.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from backend.scripts import run_stage01_leg_replacement_replay as replay

TEXT_BLOCK = "TEXT"
VARIANTS: dict[str, dict[str, Any]] = {
    # Production: Astra through Codex with the stage's default effort.
    "prod_astra": {"transport": "codex", "model": "codex/gpt-6-astra", "effort": None},
    "sonnet_medium": {"transport": "claude", "model": "claude-sonnet-5-5", "effort": "medium"},
    "opus_medium": {"transport": "claude", "model": "claude-opus-5-5", "effort": "medium"},
}


def cmd_select(args: argparse.Namespace) -> None:
    rows = [json.loads(line) for line in args.phase0.read_text(encoding="utf-8").splitlines() if line.strip()]

    def era(row: dict[str, Any]) -> str:
        legs = set(row["run_legs"])
        if "openai/gpt-5.4" in legs:
            return "gpt"
        return "opus" if "claude-opus-5-5" in legs else ("codex" if legs else "none")

    by_doc: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if era(row) in ("opus", "codex") and row["has_text_source"] and row["text_status"] != "text_mismatch":
            by_doc[(row["object"], row["discipline"], row["document"], row["version"], row["run"])].append(row)
    pool = []
    for key, items in by_doc.items():
        accepted = sum(1 for r in items if r["decision"] == "accepted")
        if accepted < args.min_accepted:
            continue
        version_dir = replay._version_dir(*key[:4])
        run_dir = version_dir / "03_analysis" / "runs" / key[4]
        if not (run_dir / "document_graph.json").is_file() or not (version_dir / "02_work").is_dir():
            continue
        pool.append((key[1], (key, -accepted, replay._stable_rank(*key))))
    pool.sort(key=lambda item: item[1][1:])
    picked = replay._round_robin(pool, args.documents)
    producers, blocks = {}, []
    for key, _, _ in picked:
        obj, discipline, document, version, run = key
        name = "|".join(key)
        version_dir = replay._version_dir(obj, discipline, document, version)
        producers[name] = {
            "object": obj, "discipline": discipline, "document": document, "version": version,
            "stamp": run,
            "version_dir": str(version_dir.relative_to(replay.DATA_ROOT)),
            "run_dir": str((version_dir / "03_analysis" / "runs" / run).relative_to(replay.DATA_ROOT)),
        }
        blocks.append({
            "producer": name, "block_id": TEXT_BLOCK, "page": None, "set": "B_TEXT",
            "references": [replay._reference(r) for r in by_doc[key]],
        })
    selection = {
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "rule": f"text-sourced reviewed findings, post-GPT runs, >= {args.min_accepted} accepted, round-robin by discipline",
        "producers": producers,
        "blocks": blocks,
        "stats": {
            "documents": len(blocks),
            "disciplines": dict(Counter(p["discipline"] for p in producers.values())),
            "refs": dict(Counter(r["decision"] for b in blocks for r in b["references"])),
        },
    }
    replay._write_json(args.out, selection)
    print(json.dumps(selection["stats"], ensure_ascii=False))


async def _call(variant: dict[str, Any], messages: list[dict], project_id: str):
    if variant["transport"] == "codex":
        from backend.app.services.llm.codex_runner import run_codex_json_messages

        return await run_codex_json_messages(
            messages, timeout=1800, stage="text_analysis", project_id=project_id, model=variant["model"],
        )
    from backend.app.services.llm.claude_json_runner import run_claude_json_messages

    return await run_claude_json_messages(
        messages, timeout=1800, stage="text_analysis", project_id=project_id,
        model=variant["model"], reasoning_effort=variant["effort"],
    )


async def _run_one(args, variant, key, info, semaphore) -> None:
    from backend.app.pipeline.stages.prepare import prompt_builder
    from backend.app.pipeline.stages.text_analysis.md_chunker import CODEX_TEXT_INPUT_BUDGET
    from backend.app.services.common import audit_scope

    out_path = args.out / args.variant / f"rep{args.rep}" / f"{replay._stable_rank(key)[:16]}.json"
    if out_path.is_file() and replay._block_ok(((replay._load_json(out_path) or {}).get("blocks") or {}).get(TEXT_BLOCK) or {}):
        return
    ws = args.workspace / "objects"
    version_dir = replay._version_dir(info["object"], info["discipline"], info["document"], info["version"], ws)
    run_dir = version_dir / "03_analysis" / "runs" / Path(info["run_dir"]).name
    project_info = replay._load_json(version_dir / "01_input" / "project_info.json") or replay._load_json(
        run_dir / "project_info.json") or {}
    project_id = f"text-replay-{replay._stable_rank(key)[:12]}"
    with audit_scope.bind_audit_scope(
        output_dir=run_dir, version_dir=version_dir, project_id=project_id, version_id=info["version"],
    ):
        message_sets, meta = prompt_builder.build_text_analysis_message_sets(
            project_info, project_id, budget=CODEX_TEXT_INPUT_BUDGET,
        )
    record: dict[str, Any] = {
        "producer": key, "variant": args.variant, "rep": args.rep, "model": variant["model"],
        "effort": variant["effort"], "prompt_mode": meta.get("mode"), "prompt_chars": meta.get("chars"),
    }
    if len(message_sets) != 1:
        record["blocks"] = {TEXT_BLOCK: {"analysis_status": "skipped_chunked", "findings": []}}
        replay._write_json(out_path, record)
        print(f"[skip] {key}: chunked prompt ({len(message_sets)} parts)", flush=True)
        return
    async with semaphore:
        started = time.time()
        for attempt in (1, 2):
            result = await _call(variant, message_sets[0], project_id)
            if isinstance(result.json_data, dict) and not result.is_error:
                break
    findings = (result.json_data or {}).get("text_findings") if isinstance(result.json_data, dict) else None
    ok = isinstance(findings, list)
    record.update({
        "elapsed_s": round(time.time() - started, 1),
        "attempts": attempt,
        "input_tokens": result.input_tokens,
        "cached_input_tokens": result.cached_tokens,
        "cache_write_input_tokens": getattr(result, "cache_write_tokens", None),
        "output_tokens": result.output_tokens,
        "error": None if ok else (result.error_message or "text_findings_missing"),
        "blocks": {TEXT_BLOCK: {
            "analysis_status": "analyzed" if ok else "failed",
            "findings": findings or [],
        }},
    })
    replay._write_json(out_path, record)
    print(
        f"[{args.variant}] {key} ok={ok} findings={len(findings or [])} "
        f"in={result.input_tokens} out={result.output_tokens} t={record['elapsed_s']}s",
        flush=True,
    )
    if not ok and "usage limit" in str(result.error_message).lower():
        raise SystemExit("subscription limit hit")


async def _run(args: argparse.Namespace) -> None:
    selection = replay._load_json(args.selection)
    variant = VARIANTS[args.variant]
    semaphore = asyncio.Semaphore(args.parallelism)
    await asyncio.gather(*(
        _run_one(args, variant, key, info, semaphore)
        for key, info in sorted(selection["producers"].items())
    ))


def cmd_run(args: argparse.Namespace) -> None:
    if os.environ.get(replay.ENV_READY) != "1":
        env = replay._live_env()
        env.update({
            replay.ENV_READY: "1",
            "AUDIT_DISABLE_DOTENV": "1",
            "AUDIT_APP_DATA_DIR": str((args.workspace / "appdata").resolve()),
            "PYTHONPATH": str(replay.ROOT),
            "PATH": os.environ.get("PATH", env.get("PATH", "")),
            "LEG_REPLAY_DATA_ROOT": str(replay.DATA_ROOT),
        })
        for guard in ("AUDIT_OPENROUTER_ENABLED", "PAID_API_ENABLED"):
            if str(env.get(guard, "")).strip().lower() in {"1", "true", "yes", "on"}:
                raise SystemExit(f"refusing to replay: live {guard} is on")
        os.chdir(replay.ROOT)
        os.execve(sys.executable, [sys.executable, "-m", "backend.scripts.run_stage02_text_model_replay", *sys.argv[1:]], env)
    asyncio.run(_run(args))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("select")
    p.add_argument("--phase0", type=Path, required=True, help="findings.jsonl of research_openrouter_leg_history")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--documents", type=int, default=30)
    p.add_argument("--min-accepted", type=int, default=3)
    p.set_defaults(func=cmd_select)

    p = sub.add_parser("run")
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--workspace", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--variant", choices=sorted(VARIANTS), required=True)
    p.add_argument("--rep", type=int, default=1)
    p.add_argument("--parallelism", type=int, default=3)
    p.set_defaults(func=cmd_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
