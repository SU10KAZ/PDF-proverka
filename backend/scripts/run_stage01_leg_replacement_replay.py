#!/usr/bin/env python3
"""Replay Stage 01 on a fixed block set for the OpenRouter-leg replacement study.

Subcommands:

* ``select``  — build the benchmark set from Phase 0 rows
  (``research_openrouter_leg_history.py``): B_T = source blocks of
  expert-accepted findings that only the OpenRouter leg found; B_R = blocks with
  only rejected OpenRouter-only findings; B_Q = unreviewed "quiet" blocks.
* ``prepare`` — copy each producer run plus its version inputs into an isolated
  workspace that mirrors the ``projects_v2`` layout (the block router derives the
  discipline and the PDF location from the path).
* ``run``     — replay a variant on the workspace through the production entry
  point ``run_findings_only_for_project``. Nothing is written to ``projects_v2``
  or to the live app data dir: the process re-executes itself with the live
  backend's environment (read from ``/proc/<pid>/environ``), an isolated
  ``AUDIT_APP_DATA_DIR`` and ``AUDIT_DISABLE_DOTENV=1``.

Expert decisions are never shown to a model; they are joined afterwards.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
# The checkout has no projects_v2: the live data lives in the service's data
# checkout (/srv/auditmanager/data). Selection paths stay relative to it.
DATA_ROOT = Path(os.environ.get("LEG_REPLAY_DATA_ROOT") or ROOT)
OBJECTS_DIR = DATA_ROOT / "projects_v2" / "objects"
LIVE_SERVICE = "auditmanager-backend.service"
ENV_READY = "LEG_REPLAY_ENV_READY"

RUN_COPY_EXCLUDE = {
    "_stage01_findings_only_runs",
    "_stage02_findings_only_runs",
    "_stage02_paid_response_cache",
    "_optimization_ensemble",
    "critic_v2_assisted_round2",
    "audit_trail",
}
APP_DATA_EXCLUDE = {
    "human_mapping_fixtures",
    "section_optimization",
    "section_optimization_pipeline",
    "project_change_preview_snapshot",
    "project_change_preview_snapshot_v3",
}
MAX_SOURCE_BLOCKS_PER_TARGET = 3

# Variants of the replay. ``baseline`` is the current production topology:
# ``ensemble/gpt-codex`` with OpenRouter off resolves to Astra + Sol legs and
# the Sol judge with gap search (TWO_MODEL_NO_OPENROUTER).
#
# Candidates run as one extra leg through the production single-model Codex
# path (``call_codex_for_block``): the same system prompt, block context, image
# and response schema. ``transport: claude`` swaps only the transport underneath
# — ``run_codex_json_messages`` → ``run_claude_json_messages`` (same signature,
# base64 image, ``--system-prompt`` without the agent wrapper) — so Claude sees
# byte-identical messages. ``prompt_profile`` swaps the production system prompt
# for one of the shadow profiles. OpenRouter is not involved on any path.
VARIANTS: dict[str, dict[str, Any]] = {
    "baseline": {"model": "ensemble/gpt-codex", "effort": "low", "env": {}},
    # codex/gpt-5.4 (the weights of the lost leg) is rejected by Codex with a
    # ChatGPT account (HTTP 400 "model is not supported"), so the "does effort
    # compensate the agent wrapper" question is tested on Sol instead.
    "k1_sol_medium": {"model": "codex/gpt-5.6-sol", "effort": "medium", "env": {}},
    "k2_sonnet_medium": {
        "model": "codex/claude-sonnet-5-5", "effort": "medium", "env": {}, "transport": "claude",
    },
    "k3_opus_medium": {
        "model": "codex/claude-opus-5-5", "effort": "medium", "env": {}, "transport": "claude",
    },
    "k3_opus_low": {
        "model": "codex/claude-opus-5-5", "effort": "low", "env": {}, "transport": "claude",
    },
    # The integrated production topology: Astra + Sol + optional Claude leg
    # with the three-way judge (STAGE01_CLAUDE_LEG_ENABLED).
    "integrated_claude_leg": {
        "model": "ensemble/gpt-codex", "effort": "low",
        "env": {"STAGE01_CLAUDE_LEG_ENABLED": "true"},
    },
    "k4_astra_v3_medium": {
        "model": "codex/gpt-6-astra", "effort": "medium", "env": {},
        "prompt_profile": "astra_shadow_v3",
    },
}
CLAUDE_VIA_CODEX_PREFIX = "codex/claude-"

# Claude-only Stage 01 study (21.10.2026): every Claude model × effort as the
# single leg — ``c1_<model>_<effort>``, the same payload as the Codex legs.
CLAUDE_MODELS = {"haiku": "claude-haiku-5-5", "sonnet": "claude-sonnet-5-5", "opus": "claude-opus-5-5"}
CLAUDE_EFFORTS = ("low", "medium", "high", "xhigh", "max")
for _short, _model in CLAUDE_MODELS.items():
    for _effort in CLAUDE_EFFORTS:
        VARIANTS[f"c1_{_short}_{_effort}"] = {
            "model": f"codex/{_model}", "effort": _effort, "env": {}, "transport": "claude",
        }

# Claude-only ensembles: the production topology (two legs, judge with gap
# search) with every call routed to Claude. ``legs`` maps the production leg
# model to the Claude (model, effort) that replaces it; the optional Opus leg
# is off so the block sees exactly two legs and one judge.
CLAUDE_ENSEMBLE_ENV = {"STAGE01_CLAUDE_LEG_ENABLED": "false"}
ASTRA, SOL = "codex/gpt-6-astra", "codex/gpt-5.6-sol"


def _claude_ensemble(leg_a: str, leg_b: str, judge: str) -> dict[str, Any]:
    def spec(value: str) -> tuple[str, str]:
        short, effort = value.split("_")
        return CLAUDE_MODELS[short], effort

    return {
        "model": "ensemble/gpt-codex", "effort": "low", "env": dict(CLAUDE_ENSEMBLE_ENV),
        "transport": "claude_ensemble",
        "legs": {ASTRA: spec(leg_a), SOL: spec(leg_b)}, "judge": spec(judge),
    }


VARIANTS.update({
    "e2_opus_medium_sonnet_medium__opus_medium": _claude_ensemble("opus_medium", "sonnet_medium", "opus_medium"),
    "e2_opus_medium_x2__opus_medium": _claude_ensemble("opus_medium", "opus_medium", "opus_medium"),
    "e2_opus_medium_x2__sonnet_medium": _claude_ensemble("opus_medium", "opus_medium", "sonnet_medium"),
    "e2_opus_high_sonnet_high__opus_medium": _claude_ensemble("opus_high", "sonnet_high", "opus_medium"),
})


def _install_claude_ensemble(variant: dict[str, Any]) -> None:
    from backend.app.services.llm import claude_json_runner, codex_runner

    legs = variant["legs"]
    judge_model, judge_effort = variant["judge"]

    async def routed(messages, *, model=None, stage="", reasoning_effort=None, **kwargs):
        if stage == "block_analysis_dual_review":
            target, effort = judge_model, judge_effort
        elif str(model) in legs:
            target, effort = legs[str(model)]
        else:
            raise RuntimeError(f"claude-only ensemble: unexpected Codex call {model!r} ({stage})")
        return await claude_json_runner.run_claude_json_messages(
            messages, model=target, stage=stage, reasoning_effort=effort, **kwargs
        )

    codex_runner.run_codex_json_messages = routed


# Codex-only study (23.10.2026): every Codex model × effort as the single leg —
# ``x1_<model>_<effort>`` — and the production topology with Codex legs/judge.
CODEX_MODELS = {"sol61": "codex/gpt-6.1-sol", "sol6": "codex/gpt-6-sol", "astra": "codex/gpt-6-astra"}
CODEX_EFFORTS = ("low", "medium", "high", "xhigh")
for _short, _model in CODEX_MODELS.items():
    for _effort in CODEX_EFFORTS:
        VARIANTS[f"x1_{_short}_{_effort}"] = {"model": _model, "effort": _effort, "env": {}}
# The production Codex-only selector itself (smoke of the real stage path).
VARIANTS["codex_only_mode"] = {"model": "ensemble/codex-only", "effort": "low", "env": {}}


def _codex_ensemble(leg_a: str, leg_b: str, judge: str) -> dict[str, Any]:
    def spec(value: str) -> tuple[str, str]:
        short, effort = value.rsplit("_", 1)
        return CODEX_MODELS[short], effort

    return {
        "model": "ensemble/gpt-codex", "effort": "low", "env": dict(CLAUDE_ENSEMBLE_ENV),
        "transport": "codex_ensemble",
        "legs": {ASTRA: spec(leg_a), SOL: spec(leg_b)}, "judge": spec(judge),
    }


def codex_ensemble_variant(name: str) -> dict[str, Any] | None:
    """``x2_<legA>__<legB>__<judge>``, each part ``<model>_<effort>``."""
    if not name.startswith("x2_"):
        return None
    parts = name[3:].split("__")
    if len(parts) != 3:
        return None
    return _codex_ensemble(*parts)


def _install_codex_ensemble(variant: dict[str, Any]) -> None:
    from backend.app.services.llm import codex_runner

    original = codex_runner.run_codex_json_messages
    legs = variant["legs"]
    judge_model, judge_effort = variant["judge"]

    async def routed(messages, *, model=None, stage="", reasoning_effort=None, **kwargs):
        if stage == "block_analysis_dual_review":
            target, effort = judge_model, judge_effort
        elif str(model) in legs:
            target, effort = legs[str(model)]
        else:
            raise RuntimeError(f"codex-only ensemble: unexpected call {model!r} ({stage})")
        return await original(messages, model=target, stage=stage, reasoning_effort=effort, **kwargs)

    codex_runner.run_codex_json_messages = routed


def install_no_claude_guard() -> None:
    """REPLAY_NO_CLAUDE=1: any Claude call aborts the replay (Claude quota is off-limits)."""
    if os.environ.get("REPLAY_NO_CLAUDE") != "1":
        return
    from backend.app.services.llm import claude_json_runner, claude_runner

    async def forbidden(*_args, **_kwargs):
        raise SystemExit("REPLAY_NO_CLAUDE=1: Claude call attempted")

    # ``_run_cli`` dispatches both providers: block it only for non-Codex models.
    original_cli = claude_runner._run_cli

    async def codex_only_cli(*args, model=None, **kwargs):
        from backend.app.core.config import is_codex_model

        if not is_codex_model(str(model or "")):
            raise SystemExit(f"REPLAY_NO_CLAUDE=1: Claude call attempted ({model!r})")
        return await original_cli(*args, model=model, **kwargs)

    claude_json_runner.run_claude_json_messages = forbidden
    claude_runner._run_cli = codex_only_cli


def _install_variant_hooks(variant: dict[str, Any]) -> None:
    """Apply the transport / prompt swaps of a candidate inside this process only."""
    install_no_claude_guard()
    if variant.get("transport") == "codex_ensemble":
        _install_codex_ensemble(variant)
    if variant.get("transport") == "claude_ensemble":
        _install_claude_ensemble(variant)
    if variant.get("transport") == "claude":
        from backend.app.services.llm import claude_json_runner, codex_runner

        original = codex_runner.run_codex_json_messages

        async def routed(messages, *, model=None, **kwargs):
            name = str(model or "")
            if name.startswith(CLAUDE_VIA_CODEX_PREFIX):
                return await claude_json_runner.run_claude_json_messages(
                    messages, model=name.split("/", 1)[1], **kwargs
                )
            return await original(messages, model=model, **kwargs)

        codex_runner.run_codex_json_messages = routed

    profile = variant.get("prompt_profile")
    if profile:
        from backend.app.pipeline.stages.block_analysis import gemma_findings_only as stage

        original_prompt = stage.build_system_prompt

        def build_with_profile(section, *args, **kwargs):
            if kwargs.get("prompt_profile", stage.SYSTEM_PROMPT_PROFILE_PRODUCTION) == (
                stage.SYSTEM_PROMPT_PROFILE_PRODUCTION
            ):
                kwargs["prompt_profile"] = profile
            return original_prompt(section, *args, **kwargs)

        stage.build_system_prompt = build_with_profile


# ─── helpers ────────────────────────────────────────────────────────────────


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _stable_rank(*parts: str) -> str:
    return hashlib.sha256("::".join(parts).encode("utf-8")).hexdigest()


def _producer_key(row: dict[str, Any]) -> str:
    return "|".join(
        (row["object"], row["discipline"], row["document"], row["version"], row["stage01_stamp"])
    )


def _version_dir(obj: str, discipline: str, document: str, version: str, base: Path = OBJECTS_DIR) -> Path:
    return base / obj / "disciplines" / discipline / "documents" / document / "versions" / version


def _producer_run_dir(version_dir: Path, stamp: str) -> Path | None:
    raws = sorted(version_dir.glob(f"03_analysis/runs/*/_stage01_findings_only_runs/{stamp}"))
    return raws[0].parents[1] if raws else None


def _reference(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "finding_id": row["finding_id"],
        "decision": row["decision"],
        "origin": row["origin"],
        "severity": row.get("severity"),
        "problem": row.get("problem") or "",
        "description": row.get("description") or "",
        "rejection_reason": row.get("rejection_reason") or "",
    }


# ─── select ─────────────────────────────────────────────────────────────────


def _replayable(run_dir: Path) -> bool:
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
        STAGE02_BLOCKS_DIRNAME,
        crop_index_matches_policy,
        stage02_crop_policy,
        validate_block_context_summary,
    )

    index = run_dir / STAGE02_BLOCKS_DIRNAME / "index.json"
    return (
        index.is_file()
        and crop_index_matches_policy(index, stage02_crop_policy())
        and (run_dir / "document_graph.json").is_file()
        and bool(validate_block_context_summary(run_dir).get("valid"))
    )


def _block_pages(run_dir: Path) -> dict[str, Any]:
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
        STAGE02_BLOCKS_DIRNAME,
    )

    index = _load_json(run_dir / STAGE02_BLOCKS_DIRNAME / "index.json") or {}
    return {
        str(b.get("block_id")): b.get("page")
        for b in index.get("blocks") or []
        if isinstance(b, dict)
    }


def _round_robin(pool: list[tuple[str, Any]], limit: int) -> list[Any]:
    """Pick ``limit`` items cycling over groups (discipline) in stable order."""
    groups: dict[str, list[Any]] = defaultdict(list)
    for group, item in pool:
        groups[group].append(item)
    order = sorted(groups)
    picked: list[Any] = []
    while len(picked) < limit and any(groups[g] for g in order):
        for group in order:
            if groups[group] and len(picked) < limit:
                picked.append(groups[group].pop(0))
    return picked


def cmd_select(args: argparse.Namespace) -> None:
    rows = [
        json.loads(line)
        for line in (args.phase0 / "findings.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    rows = [r for r in rows if r["gpt_run"] and r["text_status"] != "text_mismatch"]
    by_producer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_producer[_producer_key(row)].append(row)

    producers: dict[str, dict[str, Any]] = {}
    replayable_cache: dict[str, bool] = {}

    def producer_info(key: str) -> dict[str, Any] | None:
        if key in producers:
            return producers[key]
        if key in replayable_cache and not replayable_cache[key]:
            return None
        obj, discipline, document, version, stamp = key.split("|")
        version_dir = _version_dir(obj, discipline, document, version)
        run_dir = _producer_run_dir(version_dir, stamp)
        ok = run_dir is not None and _replayable(run_dir)
        replayable_cache[key] = ok
        if not ok:
            return None
        producers[key] = {
            "object": obj,
            "discipline": discipline,
            "document": document,
            "version": version,
            "stamp": stamp,
            "version_dir": str(version_dir.relative_to(ROOT)),
            "run_dir": str(run_dir.relative_to(ROOT)),
            "pages": _block_pages(run_dir),
        }
        return producers[key]

    def refs_for(key: str, block_id: str) -> list[dict[str, Any]]:
        return [
            _reference(r)
            for r in by_producer[key]
            if block_id in r["source_block_ids"]
        ]

    blocks: dict[tuple[str, str], dict[str, Any]] = {}
    skipped_targets = Counter()

    # B_T: source blocks of accepted OpenRouter-only findings.
    targets = [
        r for r in rows
        if r["origin"] == "gpt_only" and r["codex_healthy_on_sources"] and r["decision"] == "accepted"
    ]
    for row in targets:
        key = _producer_key(row)
        info = producer_info(key)
        if info is None:
            skipped_targets["not_replayable"] += 1
            continue
        for block_id in row["source_block_ids"][:MAX_SOURCE_BLOCKS_PER_TARGET]:
            if block_id not in info["pages"]:
                skipped_targets["block_not_in_index"] += 1
                continue
            blocks.setdefault((key, block_id), {"set": "B_T"})

    # B_R: blocks whose references are only rejected and include an
    # OpenRouter-only rejection. Prefer producers already in B_T (cheaper
    # copies), then fill by discipline round-robin.
    candidates_r: list[tuple[str, tuple[str, str]]] = []
    for key, items in by_producer.items():
        per_block: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in items:
            for block_id in r["source_block_ids"]:
                per_block[block_id].append(r)
        for block_id, refs in per_block.items():
            if (key, block_id) in blocks:
                continue
            if any(r["decision"] == "accepted" for r in refs):
                continue
            if not any(r["origin"] == "gpt_only" and r["codex_healthy_on_sources"] for r in refs):
                continue
            candidates_r.append((key, (key, block_id)))
    in_bt = {key for key, _ in blocks}
    candidates_r.sort(key=lambda kv: (kv[0] not in in_bt, _stable_rank(*kv[1])))
    pool_r = []
    for key, item in candidates_r:
        info = producer_info(key)
        if info is None or item[1] not in info["pages"]:
            continue
        pool_r.append((info["discipline"], item))
    for item in _round_robin(pool_r, args.rejected_blocks):
        blocks[item] = {"set": "B_R"}

    # B_Q: blocks of B_T producers that no reviewed finding references.
    pool_q = []
    for key in sorted(in_bt):
        info = producers[key]
        referenced = {b for r in by_producer[key] for b in r["source_block_ids"]}
        for block_id in sorted(info["pages"], key=lambda b: _stable_rank(key, b)):
            if block_id not in referenced and (key, block_id) not in blocks:
                pool_q.append((info["discipline"], (key, block_id)))
    for item in _round_robin(pool_q, args.quiet_blocks):
        blocks[item] = {"set": "B_Q"}

    selected = []
    for (key, block_id), meta in sorted(blocks.items()):
        selected.append(
            {
                "producer": key,
                "block_id": block_id,
                "page": producers[key]["pages"].get(block_id),
                "set": meta["set"],
                "references": refs_for(key, block_id),
            }
        )
    used = {b["producer"] for b in selected}
    selection = {
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "phase0": str(args.phase0),
        "producers": {
            k: {kk: vv for kk, vv in v.items() if kk != "pages"}
            for k, v in producers.items()
            if k in used
        },
        "blocks": selected,
        "stats": {
            "blocks_by_set": dict(Counter(b["set"] for b in selected)),
            "producers": len(used),
            "targets_total": len(targets),
            "targets_covered": sum(
                1
                for r in targets
                if any(
                    (_producer_key(r), b) in blocks
                    for b in r["source_block_ids"][:MAX_SOURCE_BLOCKS_PER_TARGET]
                )
            ),
            "skipped_targets": dict(skipped_targets),
            "rejected_refs_in_b_r": sum(
                1 for b in selected if b["set"] == "B_R" for r in b["references"]
                if r["decision"] == "rejected" and r["origin"] == "gpt_only"
            ),
        },
    }
    _write_json(args.out, selection)
    print(json.dumps(selection["stats"], ensure_ascii=False, indent=2))


# ─── prepare ────────────────────────────────────────────────────────────────


def _copy_tree(src: Path, dst: Path, exclude: set[str]) -> None:
    if dst.exists():
        return
    shutil.copytree(
        src,
        dst,
        symlinks=False,
        ignore=lambda _d, names: [n for n in names if n in exclude],
    )


def _restore_crops(run_dir: Path, block_ids: list[str]) -> Counter:
    """Put the selected blocks' PNGs back into the workspace crop folder.

    Finished runs get their crops evicted into ``.evicted/`` (see
    ``docs/block_crop_lifecycle.md``). The original file is exactly what the
    OpenRouter leg saw, so it is preferred; only if it was purged does the
    standard ladder re-render it from the PDF (network off).
    """
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
        STAGE02_BLOCKS_DIRNAME,
    )
    from backend.app.services.common.block_crop_store import hydrate_blocks_dir

    blocks_dir = run_dir / STAGE02_BLOCKS_DIRNAME
    index = _load_json(blocks_dir / "index.json") or {}
    files = {
        str(b.get("block_id")): str(b.get("file") or "")
        for b in index.get("blocks") or []
        if isinstance(b, dict)
    }
    stats: Counter = Counter()
    missing = []
    for block_id in block_ids:
        name = files.get(block_id)
        if not name:
            stats["not_in_index"] += 1
            continue
        target = blocks_dir / name
        if target.is_file():
            stats["present"] += 1
            continue
        evicted = blocks_dir / ".evicted" / name
        if evicted.is_file():
            shutil.copy2(evicted, target)
            stats["from_evicted"] += 1
        else:
            missing.append(block_id)
    if missing:
        report = hydrate_blocks_dir(
            blocks_dir, block_ids=missing, reason="leg-replay", allow_network=False
        )
        restored = [b for b in missing if (blocks_dir / files[b]).is_file()]
        stats["rerendered"] += len(restored)
        stats["unrecoverable"] += len(missing) - len(restored)
        del report
    return stats


def cmd_prepare(args: argparse.Namespace) -> None:
    selection = _load_json(args.selection)
    ws_objects = args.workspace / "objects"
    blocks_by_producer: dict[str, list[str]] = defaultdict(list)
    for block in selection["blocks"]:
        blocks_by_producer[block["producer"]].append(block["block_id"])
    crop_stats: Counter = Counter()
    for key, info in selection["producers"].items():
        src_version = DATA_ROOT / info["version_dir"]
        src_run = DATA_ROOT / info["run_dir"]
        dst_version = _version_dir(
            info["object"], info["discipline"], info["document"], info["version"], ws_objects
        )
        dst_run = dst_version / "03_analysis" / "runs" / src_run.name
        dst_version.mkdir(parents=True, exist_ok=True)
        for meta in (
            OBJECTS_DIR / info["object"] / "object.json",
            src_version.parents[1] / "document.json",
        ):
            target = dst_version.parents[1] / meta.name if meta.name == "document.json" else (
                ws_objects / info["object"] / meta.name
            )
            if meta.is_file() and not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(meta, target)
        for item in src_version.iterdir():
            if item.is_file() and not (dst_version / item.name).exists():
                shutil.copy2(item, dst_version / item.name)
        for sub in ("01_input", "02_work"):
            if (src_version / sub).is_dir():
                _copy_tree(src_version / sub, dst_version / sub, set())
        _copy_tree(src_run, dst_run, RUN_COPY_EXCLUDE)
        stats = _restore_crops(dst_run, blocks_by_producer[key])
        crop_stats.update(stats)
        print(f"[prepare] {key} crops={dict(stats)}")
    print(f"[prepare] crops total {dict(crop_stats)}")

    app_data = args.workspace / "appdata"
    live_data = Path(_live_env().get("AUDIT_APP_DATA_DIR") or ROOT / "backend" / "app" / "data")
    _copy_tree(live_data, app_data, APP_DATA_EXCLUDE)
    size = subprocess.run(["du", "-sh", str(args.workspace)], capture_output=True, text=True).stdout
    print(f"[prepare] workspace {size.strip()}")


# ─── run ────────────────────────────────────────────────────────────────────


def _live_env() -> dict[str, str]:
    pid = subprocess.run(
        ["systemctl", "show", "-p", "MainPID", "--value", LIVE_SERVICE],
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not pid or pid == "0":
        raise SystemExit(f"{LIVE_SERVICE} is not running: cannot mirror its environment")
    raw = Path(f"/proc/{pid}/environ").read_bytes()
    env = {}
    for item in raw.split(b"\0"):
        if b"=" in item:
            name, value = item.split(b"=", 1)
            env[name.decode()] = value.decode(errors="replace")
    return env


def _reexec_with_live_env(args: argparse.Namespace, extra_env: dict[str, str]) -> None:
    env = _live_env()
    env.update(
        {
            ENV_READY: "1",
            "AUDIT_DISABLE_DOTENV": "1",
            "AUDIT_APP_DATA_DIR": str((args.workspace / "appdata").resolve()),
            # A transient leg failure must not throw away a whole document:
            # failed blocks are recorded and retried by a later --resume pass.
            "STAGE01_ABORT_ON_LEG_FAILURE_ENABLED": "false",
            "PYTHONPATH": str(ROOT),
            "PATH": os.environ.get("PATH", env.get("PATH", "")),
            "LEG_REPLAY_DATA_ROOT": str(DATA_ROOT),
            "REPLAY_NO_CLAUDE": os.environ.get("REPLAY_NO_CLAUDE", ""),
        }
    )
    env.update(extra_env)
    for guard in ("AUDIT_OPENROUTER_ENABLED", "PAID_API_ENABLED"):
        if str(env.get(guard, "")).strip().lower() in {"1", "true", "yes", "on"}:
            raise SystemExit(f"refusing to replay: live {guard} is on")
    os.chdir(ROOT)
    os.execve(
        sys.executable,
        [sys.executable, "-m", "backend.scripts.run_stage01_leg_replacement_replay", *sys.argv[1:]],
        env,
    )


LIMIT_MARKERS = ("usage limit", "rate limit", "rate_limit", "quota")


def _raw_errors_hit_limit(raw_dir: Path) -> bool:
    """True when any leg of the raw run reports a subscription/rate limit."""
    if not raw_dir.is_dir():
        return False
    for path in raw_dir.glob("block_*.json"):
        result = (_load_json(path) or {}).get("result") or {}
        errors = [result.get("error")] + [
            (item.get("result") or {}).get("error")
            for item in result.get("detector_results") or []
        ]
        text = " ".join(str(e) for e in errors if e).lower()
        if any(marker in text for marker in LIMIT_MARKERS):
            return True
    return False


def _block_ok(block: dict[str, Any]) -> bool:
    # Single-model runs leave detectors_failed empty on failure; the status is
    # the only reliable signal for both topologies.
    return block.get("analysis_status") == "analyzed" and not block.get("detectors_failed")


def _block_result(block: dict[str, Any]) -> dict[str, Any]:
    keep = (
        "block_id", "page", "detectors_ok", "detectors_failed", "analysis_status",
        "findings", "deferred_findings", "evidence_gate", "dual_review",
    )
    return {k: block.get(k) for k in keep}


async def _run_variant(args: argparse.Namespace, variant: dict[str, Any]) -> None:
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
        run_findings_only_for_project,
    )

    _install_variant_hooks(variant)
    selection = _load_json(args.selection)
    blocks_by_producer: dict[str, list[str]] = defaultdict(list)
    for block in selection["blocks"]:
        if args.sets and block["set"] not in args.sets:
            continue
        blocks_by_producer[block["producer"]].append(block["block_id"])
    out_dir = args.out / args.variant / f"rep{args.rep}"
    out_dir.mkdir(parents=True, exist_ok=True)
    log = (out_dir / "progress.log").open("a", encoding="utf-8")
    ws_objects = args.workspace / "objects"
    producers = sorted(blocks_by_producer)
    if args.limit_producers:
        producers = producers[: args.limit_producers]
    for n, key in enumerate(producers, 1):
        info = selection["producers"][key]
        slug = _stable_rank(key)[:16]
        result_path = out_dir / f"{slug}.json"
        wanted = blocks_by_producer[key]
        previous = _load_json(result_path) if result_path.is_file() else None
        done_ok = {
            bid for bid, b in (previous or {}).get("blocks", {}).items() if _block_ok(b)
        }
        todo = [b for b in wanted if b not in done_ok]
        if not todo:
            continue
        version_dir = _version_dir(
            info["object"], info["discipline"], info["document"], info["version"], ws_objects
        )
        run_dir = version_dir / "03_analysis" / "runs" / Path(info["run_dir"]).name
        started = time.time()
        try:
            result = await run_findings_only_for_project(
                version_dir,
                output_dir_override=run_dir,
                model=variant["model"],
                reasoning_effort=variant["effort"],
                parallelism=args.parallelism,
                timeout_s=args.timeout,
                blocks_filter=todo,
                write_target=False,
                write_run_log=True,
                project_id=f"leg-replay-{slug}",
                version_id=info["version"],
                job_id=f"leg-replay-{args.variant}-rep{args.rep}",
            )
        except Exception as exc:  # noqa: BLE001 — record and continue with next document
            line = f"[{n}/{len(producers)}] FAIL {key}: {type(exc).__name__}: {exc}"
            print(line, flush=True)
            log.write(line + "\n")
            log.flush()
            if "usage limit" in str(exc).lower() or "rate limit" in str(exc).lower():
                raise
            continue
        merged = dict((previous or {}).get("blocks", {}))
        for block in result["output_doc"].get("block_analyses") or []:
            merged[str(block.get("block_id"))] = _block_result(block)
        summary = result.get("summary") or {}
        _write_json(
            result_path,
            {
                "producer": key,
                "variant": args.variant,
                "rep": args.rep,
                "model": variant["model"],
                "effort": variant["effort"],
                "run_dir": str(result.get("run_dir") or ""),
                "summary_totals": summary.get("totals"),
                "blocks": merged,
            },
        )
        failed = [b for b in todo if not _block_ok(merged.get(b) or {})]
        line = (
            f"[{n}/{len(producers)}] {key} blocks={len(todo)} failed_legs={len(failed)} "
            f"t={time.time() - started:.0f}s totals={json.dumps(summary.get('totals'), ensure_ascii=False)}"
        )
        print(line, flush=True)
        log.write(line + "\n")
        log.flush()
        if _raw_errors_hit_limit(Path(str(result.get("run_dir") or ""))):
            raise SystemExit("subscription limit hit — stopping to protect live audits")


def cmd_run(args: argparse.Namespace) -> None:
    variant = VARIANTS.get(args.variant) or codex_ensemble_variant(args.variant)
    if os.environ.get(ENV_READY) != "1":
        _reexec_with_live_env(args, variant.get("env") or {})
    asyncio.run(_run_variant(args, variant))


# ─── score ──────────────────────────────────────────────────────────────────

JUDGE_SYSTEM = """Ты сверяешь замечания к проектной документации (строительство, жилые дома).
Тебе дают эталонные замечания (R1, R2, …) к одному фрагменту чертежа и список находок
(A1, A2, …), которые независимо сделала другая проверка на том же фрагменте.

Для каждого эталона определи, какие находки описывают ТОТ ЖЕ дефект:
- same_defect — тот же элемент (марка, позиция, обозначение, узел, помещение, линия) и то
  же противоречие или нарушение (что с чем не сходится). Формулировка, категория,
  рекомендация и степень подробности могут отличаться. Находка, которая шире эталона,
  но явно включает его дефект, тоже считается.
- partial — тот же элемент, но другое или более расплывчатое противоречие; либо тот же
  тип противоречия, но нельзя уверенно сказать, что речь о том же элементе.
- Всё остальное не указывай.

Не оценивай, верны ли замечания по существу, — только совпадение дефекта.
Отвечай строго по JSON-схеме."""

JUDGE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["matches"],
    "properties": {
        "matches": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["ref", "same_defect", "partial", "comment"],
                "properties": {
                    "ref": {"type": "string"},
                    "same_defect": {"type": "array", "items": {"type": "string"}},
                    "partial": {"type": "array", "items": {"type": "string"}},
                    "comment": {"type": "string"},
                },
            },
        }
    },
}


def _finding_text(finding: dict[str, Any]) -> str:
    parts = [
        str(finding.get("finding") or finding.get("problem") or "").strip(),
        str(finding.get("value_found") or "").strip(),
    ]
    return " | ".join(p for p in parts if p)[:1200]


def _load_runs(run_dirs: list[Path]) -> dict[str, dict[tuple[str, str], list[dict[str, Any]]]]:
    """run label -> (producer, block_id) -> findings (published + deferred)."""
    runs: dict[str, dict[tuple[str, str], list[dict[str, Any]]]] = {}
    for run_dir in run_dirs:
        label = f"{run_dir.parent.name}/{run_dir.name}"
        per_block: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for path in sorted(run_dir.glob("*.json")):
            data = _load_json(path) or {}
            producer = data.get("producer")
            for block_id, block in (data.get("blocks") or {}).items():
                findings = []
                for status, items in (
                    ("published", block.get("findings")),
                    ("deferred", block.get("deferred_findings")),
                ):
                    for item in items or []:
                        if isinstance(item, dict):
                            findings.append({**item, "_status": status})
                per_block[(producer, block_id)] = {"ok": _block_ok(block), "findings": findings}
        runs[label] = per_block
    return runs


async def _judge_block(
    refs: list[dict[str, Any]],
    findings: list[tuple[str, str]],
    *,
    model: str,
    effort: str,
    cache_dir: Path,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    ref_lines = [
        f"R{i + 1}: {(r['problem'] + ' ' + r['description']).strip()[:1500]}"
        for i, r in enumerate(refs)
    ]
    finding_lines = [f"{fid}: {text}" for fid, text in findings]
    user = (
        "Эталонные замечания:\n" + "\n".join(ref_lines)
        + "\n\nНаходки проверки:\n" + "\n".join(finding_lines)
    )
    key = _stable_rank(model, effort, JUDGE_SYSTEM, user)[:32]
    cache_path = cache_dir / f"{key}.json"
    cached = _load_json(cache_path)
    if cached is not None:
        return cached
    if model.startswith("codex/"):
        from backend.app.services.llm.codex_runner import run_codex_json_messages as run_judge
    else:
        install_no_claude_guard()
        from backend.app.services.llm.claude_json_runner import run_claude_json_messages as run_judge
    async with semaphore:
        result = await run_judge(
            [{"role": "system", "content": JUDGE_SYSTEM}, {"role": "user", "content": user}],
            timeout=600 if model.startswith("codex/") else 300,
            stage="leg_replay_judge",
            model=model,
            reasoning_effort=effort,
            output_schema=JUDGE_SCHEMA,
        )
    if result.is_error:
        if "usage limit" in str(result.error_message):
            raise SystemExit(f"judge stopped: {result.error_message[:200]}")
        return {"error": result.error_message}
    payload = {
        "matches": (result.json_data or {}).get("matches") or [],
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
    }
    _write_json(cache_path, payload)
    return payload


async def _score(args: argparse.Namespace) -> None:
    selection = _load_json(args.selection)
    runs = _load_runs(args.runs)
    cache_dir = args.out / "judge_cache"
    semaphore = asyncio.Semaphore(args.concurrency)
    tasks = []
    keys = []
    # One call per (run, block): the judge sees a single run's findings, so it
    # cannot compare variants, and the cache (keyed by prompt content) is reused
    # when further runs are scored later.
    for label, per_block in sorted(runs.items()):
        for block in selection["blocks"]:
            key = (block["producer"], block["block_id"])
            refs = block["references"]
            findings = (per_block.get(key) or {}).get("findings") or []
            if not refs or not findings:
                continue
            anon = {
                f"A{i + 1}": (f"{label}#{i}", label, _finding_text(finding))
                for i, finding in enumerate(findings)
            }
            keys.append((key, refs, anon))
            tasks.append(
                _judge_block(
                    refs,
                    [(aid, item[2]) for aid, item in anon.items()],
                    model=args.judge_model,
                    effort=args.judge_effort,
                    cache_dir=cache_dir,
                    semaphore=semaphore,
                )
            )
    print(f"[score] judge calls (incl. cached): {len(tasks)}", flush=True)
    results = await asyncio.gather(*tasks)
    judged = []
    for (key, refs, anon), result in zip(keys, results):
        for match in result.get("matches") or []:
            try:
                ref = refs[int(str(match.get("ref", "R0"))[1:]) - 1]
            except (ValueError, IndexError):
                continue
            for kind in ("same_defect", "partial"):
                for aid in match.get(kind) or []:
                    if aid in anon:
                        judged.append(
                            {
                                "producer": key[0],
                                "block_id": key[1],
                                "finding_id": ref["finding_id"],
                                "run": anon[aid][1],
                                "anon_key": anon[aid][0],
                                "kind": kind,
                            }
                        )
        if result.get("error"):
            judged.append({"producer": key[0], "block_id": key[1], "error": result["error"]})
    _write_json(args.out / "judged_matches.json", judged)
    print(f"[score] matches={sum(1 for j in judged if 'kind' in j)} errors={sum(1 for j in judged if 'error' in j)}")


def cmd_score(args: argparse.Namespace) -> None:
    asyncio.run(_score(args))


async def _novelty(args: argparse.Namespace) -> None:
    """Find candidate findings that neither production nor the experts have.

    The judge is reused with roles swapped: each candidate finding is a
    "reference", and the existing items on the block (baseline findings of all
    given baseline runs + expert-reviewed references) are the "findings". A
    candidate finding with no same_defect/partial counterpart is novel — it is
    what the extra leg would add to the report and what needs a validity check.
    """
    selection = _load_json(args.selection)
    candidate = _load_runs(args.candidate)
    baseline = _load_runs(args.baseline)
    cache_dir = args.out / "judge_cache"
    semaphore = asyncio.Semaphore(args.concurrency)
    tasks, keys = [], []
    for label, per_block in sorted(candidate.items()):
        for block in selection["blocks"]:
            key = (block["producer"], block["block_id"])
            cand = (per_block.get(key) or {}).get("findings") or []
            if not cand:
                continue
            existing = [
                _finding_text(f)
                for runs in baseline.values()
                for f in (runs.get(key) or {}).get("findings") or []
            ] + [
                (r["problem"] + " " + r["description"]).strip()[:1200]
                for r in block["references"]
            ]
            pseudo_refs = [
                {"problem": _finding_text(f), "description": "", "finding_id": str(i)}
                for i, f in enumerate(cand)
            ]
            if not existing:
                keys.append((label, key, cand, None))
                tasks.append(asyncio.sleep(0, result={"matches": []}))
                continue
            keys.append((label, key, cand, existing))
            tasks.append(
                _judge_block(
                    pseudo_refs,
                    [(f"A{i + 1}", text) for i, text in enumerate(existing)],
                    model=args.judge_model,
                    effort=args.judge_effort,
                    cache_dir=cache_dir,
                    semaphore=semaphore,
                )
            )
    print(f"[novelty] judge calls (incl. cached): {sum(1 for k in keys if k[3])}", flush=True)
    results = await asyncio.gather(*tasks)
    novel = []
    summary: dict[str, Counter] = defaultdict(Counter)
    block_sets = {(b["producer"], b["block_id"]): b["set"] for b in selection["blocks"]}
    for (label, key, cand, _existing), result in zip(keys, results):
        covered = set()
        for match in result.get("matches") or []:
            if match.get("same_defect") or match.get("partial"):
                try:
                    covered.add(int(str(match.get("ref", "R0"))[1:]) - 1)
                except ValueError:
                    continue
        for i, finding in enumerate(cand):
            summary[label]["candidate_findings"] += 1
            if i in covered:
                summary[label]["already_known"] += 1
                continue
            summary[label][f"novel_{block_sets[key]}"] += 1
            novel.append(
                {
                    "run": label,
                    "producer": key[0],
                    "block_id": key[1],
                    "set": block_sets[key],
                    "status": finding.get("_status"),
                    "severity": finding.get("severity"),
                    "category": finding.get("category"),
                    "text": _finding_text(finding),
                    "finding": finding.get("finding"),
                    "value_found": finding.get("value_found"),
                    "block_evidence": finding.get("block_evidence"),
                }
            )
    _write_json(args.out / "novel_findings.json", novel)
    print(json.dumps({k: dict(v) for k, v in summary.items()}, ensure_ascii=False, indent=1))


def cmd_novelty(args: argparse.Namespace) -> None:
    asyncio.run(_novelty(args))


VALIDITY_SYSTEM = """Ты — опытный эксперт по проверке проектной документации жилых зданий.
Тебе дают фрагмент чертежа (изображение) и одно замечание, которое к нему написала
автоматическая проверка. Оцени по изображению, подтверждается ли описанный дефект:
- confirmed — противоречие/нарушение действительно видно на фрагменте, значения в
  замечании соответствуют изображению;
- refuted — на изображении видно, что дефекта нет (значения другие, элемент на месте,
  противоречия нет), либо замечание основано на неверно прочитанном тексте;
- cannot_tell — для вывода нужен контекст, которого нет на фрагменте.
Отдельно оцени, принял бы такое замечание строгий эксперт заказчика (would_accept):
только реальный дефект, влияющий на строительство, стоимость или эксплуатацию;
оформительские мелочи без последствий эксперт отклоняет. Отвечай по JSON-схеме."""

VALIDITY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["verdict", "would_accept", "reason"],
    "properties": {
        "verdict": {"type": "string", "enum": ["confirmed", "refuted", "cannot_tell"]},
        "would_accept": {"type": "boolean"},
        "reason": {"type": "string"},
    },
}


def _block_png(workspace: Path, selection: dict[str, Any], producer: str, block_id: str) -> Path | None:
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
        STAGE02_BLOCKS_DIRNAME,
    )

    info = selection["producers"][producer]
    run_dir = _version_dir(
        info["object"], info["discipline"], info["document"], info["version"], workspace / "objects"
    ) / "03_analysis" / "runs" / Path(info["run_dir"]).name
    blocks_dir = run_dir / STAGE02_BLOCKS_DIRNAME
    index = _load_json(blocks_dir / "index.json") or {}
    for item in index.get("blocks") or []:
        if str(item.get("block_id")) == block_id:
            path = blocks_dir / str(item.get("file") or "")
            return path if path.is_file() else None
    return None


async def _validity(args: argparse.Namespace) -> None:
    from backend.app.services.llm.claude_json_runner import run_claude_json_messages

    selection = _load_json(args.selection)
    novel = _load_json(args.out / "novel_findings.json") or []
    cache_dir = args.out / "validity_cache"
    semaphore = asyncio.Semaphore(args.concurrency)

    async def judge(item: dict[str, Any]) -> dict[str, Any]:
        png = _block_png(args.workspace, selection, item["producer"], item["block_id"])
        user = f"Замечание: {item['text']}"
        key = _stable_rank(args.judge_model, VALIDITY_SYSTEM, user, str(png))[:32]
        cached = _load_json(cache_dir / f"{key}.json")
        if cached is not None:
            return cached
        if png is None:
            return {"verdict": "cannot_tell", "would_accept": False, "reason": "no image"}
        async with semaphore:
            result = await run_claude_json_messages(
                [{"role": "system", "content": VALIDITY_SYSTEM}, {"role": "user", "content": user}],
                timeout=300,
                stage="leg_replay_validity",
                model=args.judge_model,
                image_paths=[png],
                reasoning_effort=args.judge_effort,
                output_schema=VALIDITY_SCHEMA,
            )
        if result.is_error:
            if "usage limit" in str(result.error_message):
                raise SystemExit(f"validity stopped: {result.error_message[:200]}")
            return {"error": result.error_message}
        payload = dict(result.json_data or {})
        _write_json(cache_dir / f"{key}.json", payload)
        return payload

    verdicts = await asyncio.gather(*(judge(item) for item in novel))
    for item, verdict in zip(novel, verdicts):
        item["validity"] = verdict
    _write_json(args.out / "novel_findings.json", novel)
    summary: dict[str, Counter] = defaultdict(Counter)
    for item in novel:
        v = item.get("validity") or {}
        summary[item["run"]][v.get("verdict", "error")] += 1
        summary[item["run"]]["would_accept"] += int(bool(v.get("would_accept")))
        summary[item["run"]]["total"] += 1
    print(json.dumps({k: dict(v) for k, v in summary.items()}, ensure_ascii=False, indent=1))


def cmd_validity(args: argparse.Namespace) -> None:
    asyncio.run(_validity(args))


REVIEW_PAGE = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Слепая выборка находок</title>
<style>
:root{--bg:#fafaf7;--card:#fff;--ink:#1d1d1b;--muted:#6b6b66;--line:#e2e1db;--acc:#2f6f4f;--rej:#a33b2b;}
@media (prefers-color-scheme: dark){:root{--bg:#161615;--card:#1f1f1d;--ink:#ecebe6;--muted:#a19f97;--line:#34332f;--acc:#6fbf94;--rej:#e07a68;}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 system-ui,sans-serif}
main{max-width:1100px;margin:0 auto;padding:16px}
h1{font-size:20px;margin:8px 0}p.lead{color:var(--muted);margin:0 0 16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;margin:0 0 16px}
.card h2{font-size:15px;margin:0 0 8px;color:var(--muted);font-weight:600}
.card img{max-width:100%;border:1px solid var(--line);border-radius:6px;background:#fff;cursor:zoom-in}
.card img.zoom{max-width:none;cursor:zoom-out}
.imgwrap{overflow:auto;max-height:80vh;margin:0 0 10px}
.text{white-space:pre-wrap;margin:0 0 10px}
.choices{display:flex;gap:8px;flex-wrap:wrap}
.choices label{border:1px solid var(--line);border-radius:999px;padding:6px 12px;cursor:pointer}
.choices input{margin-right:6px}
textarea{width:100%;min-height:36px;margin-top:8px;background:var(--bg);color:var(--ink);border:1px solid var(--line);border-radius:6px;padding:6px}
.bar{position:sticky;bottom:0;background:var(--card);border-top:1px solid var(--line);padding:10px 16px;display:flex;gap:12px;align-items:center;justify-content:space-between}
button{background:var(--acc);color:#fff;border:0;border-radius:8px;padding:8px 14px;font-size:15px;cursor:pointer}
#out{color:var(--muted);font-size:13px}
</style></head><body><main>
<h1>Слепая выборка находок этапа «Блоки»</h1>
<p class="lead">__COUNT__ находок, источник скрыт. Для каждой отметьте, приняли бы вы её как
замечание к проекту. Клик по картинке — увеличить. Отметки сохраняются в этом браузере;
в конце нажмите «Скопировать результат» и вставьте его в чат.</p>
__CARDS__
</main>
<div class="bar"><span id="out">Отмечено: 0 / __COUNT__</span><button id="copy">Скопировать результат</button></div>
<script>
const KEY="leg-replay-review-v1";let st={};try{st=JSON.parse(localStorage.getItem(KEY)||"{}")}catch(e){}
function save(){try{localStorage.setItem(KEY,JSON.stringify(st))}catch(e){};
 document.getElementById("out").textContent="Отмечено: "+Object.values(st).filter(v=>v&&v.v).length+" / __COUNT__";}
document.querySelectorAll(".card").forEach(c=>{const n=c.dataset.n;
 c.querySelectorAll("input[type=radio]").forEach(r=>{if(st[n]&&st[n].v===r.value)r.checked=true;
  r.addEventListener("change",()=>{st[n]=Object.assign(st[n]||{}, {v:r.value});save();});});
 const t=c.querySelector("textarea");if(st[n]&&st[n].c)t.value=st[n].c;
 t.addEventListener("input",()=>{st[n]=Object.assign(st[n]||{}, {c:t.value});save();});
 const img=c.querySelector("img");img.addEventListener("click",()=>img.classList.toggle("zoom"));});
document.getElementById("copy").addEventListener("click",()=>{
 const lines=Object.keys(st).sort((a,b)=>a-b).map(n=>n+":"+(st[n].v||"")+(st[n].c?" ("+st[n].c.replace(/\\s+/g," ")+")":""));
 const txt=lines.join("\\n");navigator.clipboard.writeText(txt).then(()=>alert("Скопировано"),()=>prompt("Скопируйте:",txt));});
save();
</script></body></html>
"""


def cmd_review_sample(args: argparse.Namespace) -> None:
    """Blind engineer sample of novel findings: HTML cards + a separate key."""
    import base64
    import html
    import random

    selection = _load_json(args.selection)
    novel = _load_json(args.out / "novel_findings.json") or []
    quotas = dict(item.split("=") for item in args.quota)
    rng = random.Random(args.seed)
    picked = []
    for run, count in quotas.items():
        pool = [n for n in novel if n["run"] == run]
        rng.shuffle(pool)
        picked.extend(pool[: int(count)])
    rng.shuffle(picked)
    cards, key = [], []
    for n, item in enumerate(picked, 1):
        png = _block_png(args.workspace, selection, item["producer"], item["block_id"])
        if png is None:
            continue
        data = base64.b64encode(png.read_bytes()).decode()
        info = selection["producers"][item["producer"]]
        text = item.get("finding") or item["text"]
        if item.get("value_found"):
            text += "\n\nЗначения: " + str(item["value_found"])
        cards.append(
            f'<section class="card" data-n="{n}"><h2>№{n} · {html.escape(info["discipline"])} · '
            f'{html.escape(info["document"])} · стр. {html.escape(str(_block_page(selection, item)))} · '
            f'{html.escape(str(item.get("severity") or ""))}</h2>'
            f'<div class="imgwrap"><img alt="фрагмент чертежа" src="data:image/png;base64,{data}"></div>'
            f'<div class="text">{html.escape(text)}</div>'
            '<div class="choices">'
            f'<label><input type="radio" name="q{n}" value="+">Принял бы</label>'
            f'<label><input type="radio" name="q{n}" value="-">Отклонил бы</label>'
            f'<label><input type="radio" name="q{n}" value="?">Не могу оценить</label></div>'
            '<textarea placeholder="Комментарий (необязательно)"></textarea></section>'
        )
        key.append({"n": n, **item})
    page = REVIEW_PAGE.replace("__CARDS__", "\n".join(cards)).replace("__COUNT__", str(len(cards)))
    args.review_dir.mkdir(parents=True, exist_ok=True)
    (args.review_dir / "review.html").write_text(page, encoding="utf-8")
    _write_json(args.review_dir / "key.json", key)
    print(f"[review] {len(cards)} cards -> {args.review_dir / 'review.html'}")


def _block_page(selection: dict[str, Any], item: dict[str, Any]) -> Any:
    for block in selection["blocks"]:
        if block["producer"] == item["producer"] and block["block_id"] == item["block_id"]:
            return block.get("page")
    return "?"


def _call_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    """Every model call of one raw block result: legs, optional legs and the judge."""
    rows = []
    legs = result.get("detector_results") or [{"model": result.get("model"), "result": result}]
    legs = legs + list(result.get("optional_leg_results") or [])
    for leg in legs:
        res = leg.get("result") or {}
        rows.append({
            "model": str(res.get("model") or leg.get("model") or ""),
            "input": int(res.get("input_tokens") or 0),
            "cached": int(res.get("cached_input_tokens") or 0),
            "cache_write": res.get("cache_write_input_tokens"),
            "output": int(res.get("output_tokens") or 0),
            "elapsed_ms": int(res.get("elapsed_ms") or 0),
        })
    if result.get("dual_review_calls"):
        review = result.get("dual_review") or {}
        rows.append({
            "model": f"judge:{review.get('reviewer_model') or ''}",
            "input": int(result.get("dual_review_input_tokens") or 0),
            "cached": 0,
            "cache_write": None,
            "output": int(result.get("dual_review_output_tokens") or 0),
            "elapsed_ms": 0,
        })
    return rows


def _claude_cost(row: dict[str, Any], model: str) -> tuple[float, float]:
    """API-equivalent USD (low, high). Without a cache-write split the uncached
    input is priced as plain input (low) and as cache write (high)."""
    from backend.app.services.common.usage_service import _subscription_price

    price_in, price_out, price_cw, price_cr = _subscription_price(model)
    cached = row["cached"]
    if row["cache_write"] is not None:
        write = int(row["cache_write"])
        plain = max(0, row["input"] - cached - write)
        cost = (plain * price_in + write * price_cw + cached * price_cr + row["output"] * price_out) / 1e6
        return cost, cost
    uncached = max(0, row["input"] - cached)
    base = (cached * price_cr + row["output"] * price_out) / 1e6
    return base + uncached * price_in / 1e6, base + uncached * price_cw / 1e6


def cmd_costs(args: argparse.Namespace) -> None:
    """Claude quota, Codex tokens and time per block for each replay run.

    The judge of a Claude-only ensemble is labelled with the production Sol
    model; ``--judge-claude-model`` maps it to the Claude model it really was.
    """
    judge_models = dict(item.split("=", 1) for item in args.judge_claude_model or [])
    report: dict[str, Any] = {}
    for run_dir in args.runs:
        label = f"{run_dir.parent.name}/{run_dir.name}"
        variant = VARIANTS.get(run_dir.parent.name) or codex_ensemble_variant(run_dir.parent.name) or {}
        judge_model = judge_models.get(run_dir.parent.name) or (
            variant.get("judge", [None])[0] if variant.get("transport") == "claude_ensemble" else None
        )
        blocks = []
        for path in sorted(run_dir.glob("*.json")):
            data = _load_json(path) or {}
            # A resumed run writes a new raw folder; earlier blocks stay in
            # the sibling folders of the same model tag.
            last_raw = Path(str(data.get("run_dir") or "")) if data.get("run_dir") else None
            raw_dirs = []
            if last_raw is not None:
                tag = last_raw.name.split("__", 1)[-1]
                raw_dirs = [last_raw, *sorted(last_raw.parent.glob(f"*__{tag}"), reverse=True)]
            for block_id, block in (data.get("blocks") or {}).items():
                raw = next(
                    (_load_json(d / f"block_{block_id}.json") for d in raw_dirs
                     if (d / f"block_{block_id}.json").is_file()),
                    None,
                )
                if raw is None:
                    continue
                result = raw.get("result") or {}
                calls = _call_rows(result)
                claude_low = claude_high = 0.0
                codex_tokens = 0
                for row in calls:
                    model = row["model"]
                    if model.startswith("judge:"):
                        model = judge_model or model.removeprefix("judge:")
                    elif variant.get("transport") == "claude_ensemble" and model in variant["legs"]:
                        model = variant["legs"][model][0]
                    if "claude" in model:
                        low, high = _claude_cost(row, model)
                        claude_low += low
                        claude_high += high
                    else:
                        codex_tokens += row["input"] + row["output"]
                blocks.append({
                    "ok": _block_ok(block),
                    "elapsed_s": int(result.get("elapsed_ms") or 0) / 1000,
                    "calls": len(calls),
                    "claude_usd_low": claude_low,
                    "claude_usd_high": claude_high,
                    "codex_tokens": codex_tokens,
                    "published": len(block.get("findings") or []),
                    "deferred": len(block.get("deferred_findings") or []),
                })
        if not blocks:
            continue
        times = sorted(b["elapsed_s"] for b in blocks)
        n = len(blocks)
        usd = sum(b["claude_usd_high"] for b in blocks) / n
        report[label] = {
            "blocks": n,
            "failed": sum(1 for b in blocks if not b["ok"]),
            "calls_per_block": round(sum(b["calls"] for b in blocks) / n, 2),
            "block_s_median": times[n // 2],
            "block_s_p90": times[min(n - 1, int(n * 0.9))],
            "blocks_over_200s": sum(1 for t in times if t > 200),
            "claude_usd_per_block_low": round(sum(b["claude_usd_low"] for b in blocks) / n, 4),
            "claude_usd_per_block_high": round(usd, 4),
            "codex_tokens_per_block": round(sum(b["codex_tokens"] for b in blocks) / n),
            "week_share_median": round(usd * args.median_blocks / args.weekly_usd, 3),
            "week_share_peak": round(usd * args.peak_blocks / args.weekly_usd, 3),
            "findings_per_block": round(sum(b["published"] + b["deferred"] for b in blocks) / n, 2),
        }
    _write_json(args.out / "costs.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=1))


def cmd_report(args: argparse.Namespace) -> None:
    selection = _load_json(args.selection)
    runs = _load_runs(args.runs)
    judged = [j for j in _load_json(args.out / "judged_matches.json") or [] if "kind" in j]
    matched: dict[tuple[str, str, str], set[str]] = defaultdict(set)  # (producer, F-id, run) -> kinds
    for j in judged:
        matched[(j["producer"], j["finding_id"], j["run"])].add(j["kind"])

    refs: dict[tuple[str, str], dict[str, Any]] = {}
    ref_blocks: dict[tuple[str, str], set[str]] = defaultdict(set)
    block_set: dict[tuple[str, str], str] = {}
    for block in selection["blocks"]:
        block_set[(block["producer"], block["block_id"])] = block["set"]
        for ref in block["references"]:
            k = (block["producer"], ref["finding_id"])
            refs[k] = ref
            ref_blocks[k].add(block["set"])

    def group(ref: dict[str, Any]) -> str:
        if ref["origin"] == "gpt_only":
            return f"gpt_only_{ref['decision']}"
        return f"other_{ref['decision']}"

    labels = sorted(runs)
    variants = sorted({label.split("/")[0] for label in labels})
    report: dict[str, Any] = {"runs": {}, "unions": {}}

    def recall(run_labels: list[str]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        totals = Counter(group(r) for r in refs.values())
        for kind_set, name in ((("same_defect",), "strict"), (("same_defect", "partial"), "loose")):
            hit = Counter()
            for k, ref in refs.items():
                kinds = set().union(*(matched.get((k[0], k[1], lbl), set()) for lbl in run_labels))
                if kinds & set(kind_set):
                    hit[group(ref)] += 1
            out[name] = {
                g: {"hit": hit[g], "total": totals[g], "share": round(hit[g] / totals[g], 3) if totals[g] else None}
                for g in sorted(totals)
            }
        return out

    for label in labels:
        per_block = runs[label]
        counts = Counter()
        for key, set_name in block_set.items():
            entry = per_block.get(key)
            if entry is None:
                counts["missing_blocks"] += 1
                continue
            counts["blocks"] += 1
            counts["blocks_failed"] += int(not entry["ok"])
            for f in entry["findings"]:
                counts[f"{set_name}_{f['_status']}"] += 1
        report["runs"][label] = {"counts": dict(counts), "recall": recall([label])}
    for variant in variants:
        members = [lbl for lbl in labels if lbl.startswith(variant + "/")]
        if len(members) > 1:
            report["unions"][variant] = recall(members)

    # Loss L = expert-accepted OpenRouter-only findings that the production
    # baseline did not find (strict). Each candidate is scored on what it adds
    # on top of the baseline: recovered L, and other accepted / rejected refs
    # the baseline did not already produce.
    def hits(run_labels: list[str]) -> set[tuple[str, str]]:
        return {
            k for k in refs
            if any("same_defect" in matched.get((k[0], k[1], lbl), set()) for lbl in run_labels)
        }

    baseline_labels = [lbl for lbl in labels if lbl.startswith(args.baseline + "/")]
    if baseline_labels:
        base_hits = hits(baseline_labels)
        loss = {k for k, r in refs.items() if group(r) == "gpt_only_accepted"} - base_hits
        added: dict[str, Any] = {
            "baseline_runs": baseline_labels,
            "loss_size": len(loss),
            "targets_total": sum(1 for r in refs.values() if group(r) == "gpt_only_accepted"),
            "candidates": {},
        }
        candidate_sets = {lbl: [lbl] for lbl in labels if lbl not in baseline_labels}
        for variant in variants:
            members = [lbl for lbl in labels if lbl.startswith(variant + "/")]
            if variant != args.baseline and len(members) > 1:
                candidate_sets[f"{variant}/union"] = members
        for name, members in sorted(candidate_sets.items()):
            new = hits(members) - base_hits
            by_group = Counter(group(refs[k]) for k in new)
            added["candidates"][name] = {
                "loss_recovered": len(new & loss),
                "loss_recovered_share": round(len(new & loss) / len(loss), 3) if loss else None,
                "added_by_group": dict(by_group),
                "added_accepted": by_group["gpt_only_accepted"] + by_group["other_accepted"],
                "added_rejected": by_group["gpt_only_rejected"] + by_group["other_rejected"],
            }
        report["added_over_baseline"] = added
    _write_json(args.out / "report.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=1))


# ─── cli ────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("select")
    p.add_argument("--phase0", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--rejected-blocks", type=int, default=40)
    p.add_argument("--quiet-blocks", type=int, default=20)
    p.set_defaults(func=cmd_select)

    p = sub.add_parser("prepare")
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--workspace", type=Path, required=True)
    p.set_defaults(func=cmd_prepare)

    p = sub.add_parser("run")
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--workspace", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--variant", default="baseline",
                   help="one of VARIANTS or x2_<legA>__<legB>__<judge> (<model>_<effort>)")
    p.add_argument("--rep", type=int, default=1)
    p.add_argument("--parallelism", type=int, default=2)
    p.add_argument("--sets", nargs="*", default=None)
    p.add_argument("--limit-producers", type=int, default=0)
    p.add_argument("--timeout", type=int, default=200, help="per-call timeout, s (production: 200)")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("score")
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--runs", type=Path, nargs="+", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--judge-model", default="claude-sonnet-5-5")
    p.add_argument("--judge-effort", default="medium")
    p.add_argument("--concurrency", type=int, default=3)
    p.set_defaults(func=cmd_score)

    p = sub.add_parser("novelty")
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--candidate", type=Path, nargs="+", required=True)
    p.add_argument("--baseline", type=Path, nargs="+", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--judge-model", default="claude-sonnet-5-5")
    p.add_argument("--judge-effort", default="medium")
    p.add_argument("--concurrency", type=int, default=3)
    p.set_defaults(func=cmd_novelty)

    p = sub.add_parser("validity")
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--workspace", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--judge-model", default="claude-sonnet-5-5")
    p.add_argument("--judge-effort", default="medium")
    p.add_argument("--concurrency", type=int, default=3)
    p.set_defaults(func=cmd_validity)

    p = sub.add_parser("review-sample")
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--workspace", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--review-dir", type=Path, required=True)
    p.add_argument("--quota", nargs="+", required=True, help="run=count, e.g. k3_opus_medium/rep1=20")
    p.add_argument("--seed", type=int, default=20261011)
    p.set_defaults(func=cmd_review_sample)

    p = sub.add_parser("costs")
    p.add_argument("--runs", type=Path, nargs="+", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--judge-claude-model", nargs="*", help="variant=claude-model for judge labels")
    # Weekly limit calibrated by the subscription snapshot on 21.10.2026;
    # block volumes are the August–October production median and peak week.
    p.add_argument("--weekly-usd", type=float, default=2080.0)
    p.add_argument("--median-blocks", type=int, default=2150)
    p.add_argument("--peak-blocks", type=int, default=6900)
    p.set_defaults(func=cmd_costs)

    p = sub.add_parser("report")
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--runs", type=Path, nargs="+", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--baseline", default="baseline")
    p.set_defaults(func=cmd_report)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
