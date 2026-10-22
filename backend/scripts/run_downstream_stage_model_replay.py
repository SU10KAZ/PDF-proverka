#!/usr/bin/env python3
"""Replay stages 03 (merge), OPT and CF (critic) with another model.

Each stage runs through its production entry point in ``claude_runner`` on an
isolated copy of the reviewed run (``03_analysis/latest``); the model is chosen
the way the portal chooses it (``STAGE_MODEL_CONFIG``), Claude effort comes from
``AUDIT_CLAUDE_EFFORT``. Expert decisions are joined afterwards:

* merge  — findings of the new ``03_findings.json`` in the Stage 01 replay
  result format, scored by its ``score`` / ``report`` against finding decisions;
* opt    — items of the new ``optimization.json``, same format, against
  optimization decisions;
* critic — verdicts of ``optimization_review.json`` on the production items
  (input = ``optimization_pre_review.json``), compared by item id directly.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

from backend.scripts import run_stage01_leg_replacement_replay as replay

STAGES = {
    "merge": ("findings_merge", "run_findings_merge", "03_findings.json"),
    "opt": ("optimization", "run_optimization", "optimization.json"),
    "critic": ("optimization_critic", "run_optimization_critic", "optimization_review.json"),
}
MODELS = {"astra": "codex/gpt-6-astra", "sonnet": "claude-sonnet-5-5", "opus": "claude-opus-5-5"}
# Large artefacts the three stages never write; linked instead of copied.
LINKED = {"blocks_stage02_100", "blocks", "_stage01_findings_only_runs", "_stage02_findings_only_runs"}


def _item_text(item: dict[str, Any]) -> str:
    parts = [item.get(k) for k in ("problem", "description", "current", "proposed", "title", "finding")]
    return " | ".join(str(p).strip() for p in parts if p)[:1500]


def cmd_select(args: argparse.Namespace) -> None:
    producers, merge_blocks, opt_blocks = {}, [], []
    for spec in args.documents:
        obj, discipline, document, version = spec.split("|")
        version_dir = replay._version_dir(obj, discipline, document, version)
        latest = version_dir / "03_analysis" / "latest"
        decisions = (replay._load_json(version_dir / "04_review" / "expert_review.json") or {}).get("decisions") or []
        findings = {f.get("id"): f for f in (replay._load_json(latest / "03_findings.json") or {}).get("findings") or []}
        items = {i.get("id"): i for i in (replay._load_json(latest / "optimization.json") or {}).get("items") or []}
        key = f"{spec}|latest"
        producers[key] = {
            "object": obj, "discipline": discipline, "document": document, "version": version, "stamp": "latest",
            "version_dir": str(version_dir.relative_to(replay.DATA_ROOT)),
            "run_dir": str(latest.relative_to(replay.DATA_ROOT)),
        }

        def refs(kind: str, pool: dict) -> list[dict[str, Any]]:
            out = []
            for d in decisions:
                if d.get("item_type", "finding") == kind and d.get("decision") in ("accepted", "rejected") \
                        and d.get("item_id") in pool:
                    item = pool[d["item_id"]]
                    out.append({"finding_id": d["item_id"], "decision": d["decision"], "origin": kind,
                                "severity": item.get("severity"), "problem": _item_text(item), "description": "",
                                "rejection_reason": d.get("rejection_reason") or ""})
            return out

        merge_blocks.append({"producer": key, "block_id": "DOC", "page": None, "set": "B_DOC",
                             "references": refs("finding", findings)})
        opt_blocks.append({"producer": key, "block_id": "DOC", "page": None, "set": "B_DOC",
                           "references": refs("optimization", items)})
    for name, blocks in (("merge", merge_blocks), ("opt", opt_blocks)):
        replay._write_json(args.out_dir / f"{name}_selection.json", {
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "producers": producers, "blocks": blocks,
            "stats": {"documents": len(blocks),
                      "refs": dict(Counter(r["decision"] for b in blocks for r in b["references"]))},
        })
        print(name, dict(Counter(r["decision"] for b in blocks for r in b["references"])))


def _variant_run_dir(args, info: dict[str, Any]) -> Path:
    # ``prepare`` of the Stage 01 replay puts every run under 03_analysis/runs/.
    src = replay._version_dir(
        info["object"], info["discipline"], info["document"], info["version"], args.workspace / "objects"
    ) / "03_analysis" / "runs" / Path(info["run_dir"]).name
    dst = args.workspace / "variants" / args.stage / args.variant / replay._stable_rank(info["run_dir"])[:12]
    if not dst.exists():
        dst.mkdir(parents=True)
        for item in src.iterdir():
            if item.name in LINKED:
                (dst / item.name).symlink_to(item.resolve())
            elif item.is_dir():
                shutil.copytree(item, dst / item.name, symlinks=True)
            else:
                shutil.copy2(item, dst / item.name)
        if args.stage == "critic":
            # The critic reviewed the optimiser's output before the corrector ran.
            shutil.copy2(dst / "optimization_pre_review.json", dst / "optimization.json")
    return dst


async def _run_one(args, key: str, info: dict[str, Any], semaphore) -> None:
    from backend.app.services.llm import claude_runner

    stage_key, func_name, artifact = STAGES[args.stage]
    out_path = args.out / args.stage / args.variant / "rep1" / f"{replay._stable_rank(key)[:16]}.json"
    if out_path.is_file():
        return
    run_dir = _variant_run_dir(args, info)
    version_dir = args.workspace / "objects" / Path(info["version_dir"]).relative_to("projects_v2/objects")
    project_info = replay._load_json(version_dir / "01_input" / "project_info.json") or replay._load_json(
        run_dir / "project_info.json") or {}
    project_id = f"stage-replay-{replay._stable_rank(key)[:12]}"
    (run_dir / artifact).unlink(missing_ok=True) if args.stage != "critic" else None
    kwargs = {"output_dir": run_dir, "version_dir": version_dir, "version_id": info["version"]}
    if args.stage == "opt":
        kwargs.update(model_override=MODELS[args.variant], reasoning_effort_override="medium")
    async with semaphore:
        started = time.time()
        exit_code, _text, result = await getattr(claude_runner, func_name)(project_info, project_id, **kwargs)
    data = replay._load_json(run_dir / artifact) or {}
    record: dict[str, Any] = {
        "producer": key, "stage": args.stage, "variant": args.variant, "exit_code": exit_code,
        "elapsed_s": round(time.time() - started, 1),
        "input_tokens": getattr(result, "input_tokens", 0), "output_tokens": getattr(result, "output_tokens", 0),
        "cost_usd": getattr(result, "cost_usd", None),
    }
    if args.stage == "critic":
        record["reviews"] = [{"item_id": r.get("item_id"), "verdict": r.get("verdict")} for r in data.get("reviews") or []]
        ok = bool(record["reviews"])
    else:
        items = data.get("findings") if args.stage == "merge" else data.get("items")
        ok = isinstance(items, list)
        record["blocks"] = {"DOC": {"analysis_status": "analyzed" if ok else "failed",
                                    "findings": [{"finding": _item_text(i)} for i in items or []]}}
    replay._write_json(out_path, record)
    print(f"[{args.stage}/{args.variant}] {key} ok={ok} exit={exit_code} t={record['elapsed_s']}s "
          f"in={record['input_tokens']} out={record['output_tokens']}", flush=True)


async def _run(args: argparse.Namespace) -> None:
    from backend.app.core import config

    stage_key = STAGES[args.stage][0]
    config.STAGE_MODEL_CONFIG[stage_key] = MODELS[args.variant]
    selection = replay._load_json(args.selection)
    semaphore = asyncio.Semaphore(args.parallelism)
    await asyncio.gather(*(_run_one(args, k, i, semaphore) for k, i in sorted(selection["producers"].items())))


def cmd_run(args: argparse.Namespace) -> None:
    if os.environ.get(replay.ENV_READY) != "1":
        env = replay._live_env()
        env.update({
            replay.ENV_READY: "1", "AUDIT_DISABLE_DOTENV": "1",
            "AUDIT_APP_DATA_DIR": str((args.workspace / "appdata").resolve()),
            "AUDIT_CLAUDE_EFFORT": "medium",
            "PYTHONPATH": str(replay.ROOT), "PATH": os.environ.get("PATH", env.get("PATH", "")),
            "LEG_REPLAY_DATA_ROOT": str(replay.DATA_ROOT),
        })
        for guard in ("AUDIT_OPENROUTER_ENABLED", "PAID_API_ENABLED"):
            if str(env.get(guard, "")).strip().lower() in {"1", "true", "yes", "on"}:
                raise SystemExit(f"refusing to replay: live {guard} is on")
        os.chdir(replay.ROOT)
        os.execve(sys.executable, [sys.executable, "-m", "backend.scripts.run_downstream_stage_model_replay", *sys.argv[1:]], env)
    asyncio.run(_run(args))


def cmd_critic_report(args: argparse.Namespace) -> None:
    selection = replay._load_json(args.selection)
    decisions = {(b["producer"], r["finding_id"]): r["decision"] for b in selection["blocks"] for r in b["references"]}
    report = {}
    for path in sorted((args.out / "critic").glob("*/rep1")):
        counts = Counter()
        for f in path.glob("*.json"):
            rec = replay._load_json(f) or {}
            for review in rec.get("reviews") or []:
                decision = decisions.get((rec["producer"], review["item_id"]))
                if decision:
                    counts[f"{decision}_{'pass' if review.get('verdict') == 'pass' else 'flagged'}"] += 1
        report[path.parent.name] = dict(counts)
    replay._write_json(args.out / "critic_report.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=1))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("select")
    p.add_argument("--documents", nargs="+", required=True, help="object|discipline|document|version")
    p.add_argument("--out-dir", type=Path, required=True)
    p.set_defaults(func=cmd_select)
    p = sub.add_parser("run")
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--workspace", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--stage", choices=sorted(STAGES), required=True)
    p.add_argument("--variant", choices=sorted(MODELS), required=True)
    p.add_argument("--parallelism", type=int, default=3)
    p.set_defaults(func=cmd_run)
    p = sub.add_parser("critic-report")
    p.add_argument("--selection", type=Path, required=True, help="opt_selection.json")
    p.add_argument("--out", type=Path, required=True)
    p.set_defaults(func=cmd_critic_report)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
