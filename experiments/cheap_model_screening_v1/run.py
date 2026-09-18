"""Eight-call cheap-model screening over frozen Pair A/B Change Miner inputs.

Preparation sees only model metadata and structural properties of the already
frozen source inputs.  Evaluation truth and Astra results are deliberately not
read until a verified candidate result freeze exists.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import itertools
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import time

DEPS = Path("/home/coder/auditmanager/corpus-audits/20260914_project_change_272/controlled_inference_f1_f4_f2_v3/runtime_deps")
sys.path.insert(0, str(DEPS))
import jsonschema


ROOT = Path("/home/coder/auditmanager/corpus-audits/20260914_project_change_272")
OUT = ROOT / "cheap_model_screening_v1"
PAIR_ROOTS = {
    "A": ROOT / "pair_a_ai_mapping_change_miner_v1",
    "B": ROOT / "pair_b_ai_mapping_change_miner_v1",
}
MODEL_CACHE = Path("/home/coder/.codex/models_cache.json")
MODEL = "gpt-5.6-terra"
REASONING = "high"
SERVICE_TIER = "priority"
TIMEOUT_SECONDS = 1200
DISABLED = [
    "shell_tool", "unified_exec", "apps", "plugins", "remote_plugin",
    "memories", "multi_agent", "browser_use", "browser_use_external",
    "computer_use", "image_generation", "hooks", "shell_snapshot",
    "skill_search", "code_mode_host", "unbounded_connection_retries",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read(path: Path):
    return json.loads(path.read_text())


def write_new(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def safe_env() -> dict[str, str]:
    return {key: os.environ[key] for key in ["PATH", "LANG", "LC_ALL", "TZ"] if key in os.environ}


def source_call_dir(pair: str, group_id: str) -> Path:
    base = PAIR_ROOTS[pair] / ("miner_raw" if pair == "A" else "raw")
    return base / f"PASS_B_{group_id}"


def pair_a_structure() -> list[dict]:
    base = PAIR_ROOTS["A"]
    rows = []
    for model_input in sorted((base / "miner_inputs").glob("PASS_B_G*/MODEL_INPUT.json")):
        value = read(model_input)
        group = value["frozen_group"]
        pages = value["pages"]
        modalities = {block["modality"] for page in pages for block in page["blocks"]}
        table_count = sum(len(block["tables"]) for page in pages for block in page["blocks"])
        graphic_count = sum(block["modality"] == "GRAPHIC" for page in pages for block in page["blocks"])
        text_chars = sum(len(page["native_page_text"]) + sum(len(block["md_text"]) for block in page["blocks"])
                         for page in pages)
        group_id = group["map_group_id"]
        call = source_call_dir("A", group_id)
        rows.append({
            "pair": "A", "group_id": group_id,
            "old_page_count": len(group["old_pages"]), "new_page_count": len(group["new_pages"]),
            "page_count": len(pages), "prompt_bytes": (call / "prompt.txt").stat().st_size,
            "image_count": len(list(call.glob("image_*.png"))), "text_characters": text_chars,
            "table_count": table_count, "graphic_block_count": graphic_count,
            "modalities": sorted(modalities), "cross_modal_structural": len(modalities) > 1,
        })
    if len(rows) != 32:
        raise RuntimeError(f"Pair A structural inventory drift: {len(rows)} groups")
    return rows


def pair_b_structure() -> list[dict]:
    base = PAIR_ROOTS["B"]
    groups = read(base / "DOCUMENT_MAP.json")["groups"]
    rows = []
    for group in groups:
        page_rows = [read(base / "pages" / side / f"p{page:03d}.json")
                     for side in ["old", "new"] for page in group[f"{side}_pages"]]
        group_id = group["map_group_id"]
        call = source_call_dir("B", group_id)
        table_count = sum(len(page["tables"]) for page in page_rows)
        text_chars = sum(len(page["native"]) + len(page["ocr"]) for page in page_rows)
        rows.append({
            "pair": "B", "group_id": group_id,
            "old_page_count": len(group["old_pages"]), "new_page_count": len(group["new_pages"]),
            "page_count": len(page_rows), "prompt_bytes": (call / "prompt.txt").stat().st_size,
            "image_count": len(list(call.glob("image_*.png"))), "text_characters": text_chars,
            "table_count": table_count, "graphic_block_count": 0,
            "modalities": (["TABLE", "TEXT", "GRAPHIC"] if table_count else ["TEXT", "GRAPHIC"]),
            "cross_modal_structural": True,
            "graphic_proxy": "full-page rasters with low text density; no block-level graphic labels in Pair B input",
        })
    if len(rows) != 35:
        raise RuntimeError(f"Pair B structural inventory drift: {len(rows)} groups")
    return rows


def rank01(rows: list[dict], key: str) -> dict[str, float]:
    ordered = sorted(rows, key=lambda row: (row[key], row["group_id"]))
    denominator = max(1, len(ordered) - 1)
    return {row["group_id"]: index / denominator for index, row in enumerate(ordered)}


def choose_four(rows: list[dict]) -> list[dict]:
    """Choose structural extremes and modality coverage without semantic fields."""
    prompt_rank = rank01(rows, "prompt_bytes")
    table_rank = rank01(rows, "table_count")
    graphic_metric = "graphic_block_count" if any(row["graphic_block_count"] for row in rows) else "text_characters"
    graphic_rank = rank01(rows, graphic_metric)
    best = None
    for combo in itertools.combinations(rows, 4):
        ids = [row["group_id"] for row in combo]
        sizes = [prompt_rank[group_id] for group_id in ids]
        table = max(table_rank[group_id] for group_id in ids)
        if graphic_metric == "graphic_block_count":
            graphic = max(graphic_rank[group_id] for group_id in ids)
        else:
            # Less extracted text per raster is the only available Pair B graphic-heavy proxy.
            graphic = max(1 - graphic_rank[group_id] for group_id in ids)
        modality_union = set().union(*(set(row["modalities"]) for row in combo))
        cross_modal = sum(bool(row["cross_modal_structural"]) for row in combo) / 4
        score = ((max(sizes) - min(sizes)) * 4 + table * 2 + graphic * 2 +
                 len(modality_union) / 3 + cross_modal)
        tie = tuple(ids)
        candidate = (round(score, 12), tuple(reversed(tie)), combo)
        if best is None or candidate[:2] > best[:2]:
            best = candidate
    chosen = list(best[2])
    chosen.sort(key=lambda row: row["group_id"])
    return chosen


def candidate_catalog() -> dict:
    cache = read(MODEL_CACHE)
    locally_visible = {m["slug"]: m for m in cache["models"] if m.get("visibility") == "list"}
    specifications = {
        "gpt-5.6-sol": {"input_usd_per_mtok": 4.0, "cached_input_usd_per_mtok": 0.4,
                        "output_usd_per_mtok": 20.0, "positioning": "flagship professional work"},
        "gpt-5.6-terra": {"input_usd_per_mtok": 2.0, "cached_input_usd_per_mtok": 0.2,
                          "output_usd_per_mtok": 12.0, "positioning": "balance intelligence and cost"},
        "gpt-5.6-luna": {"input_usd_per_mtok": 0.2, "cached_input_usd_per_mtok": 0.02,
                         "output_usd_per_mtok": 1.2, "positioning": "cost-sensitive high-volume workloads"},
    }
    candidates = []
    for slug, prices in specifications.items():
        model = locally_visible.get(slug)
        if not model:
            continue
        candidates.append({
            "exact_model_identifier": slug,
            "available_reasoning_levels_current_environment": [x["effort"] for x in model["supported_reasoning_levels"]],
            "multimodal_support": {"text_input": True, "image_input": True, "text_output": True},
            "structured_output": True, "fresh_isolated_calls": True,
            "context_limit_tokens": 1_050_000, "max_output_tokens": 128_000,
            **prices,
            "why_potentially_cheaper": (f"Official token prices are below Astra's $10 input / $1 cached / $50 output per MTok; "
                                         f"the local Codex catalog lists {slug} for this subscription environment."),
        })
    astra = locally_visible.get("gpt-6-astra")
    fallback = {
        "exact_model_identifier": "gpt-6-astra",
        "available_lower_reasoning_configurations": [
            x["effort"] for x in astra["supported_reasoning_levels"] if x["effort"] in {"low", "medium", "high"}
        ] if astra else [],
        "multimodal_support": {"text_input": True, "image_input": True, "text_output": True},
        "structured_output": True, "fresh_isolated_calls": True,
        "context_limit_tokens": 1_050_000,
        "why_potentially_cheaper": "Same token price, but lower reasoning effort may consume less output/reasoning allowance; fallback only.",
    }
    return {
        "created_at": now(), "inference_calls_used_for_availability_check": 0,
        "local_model_catalog": str(MODEL_CACHE), "local_model_catalog_fetched_at": cache["fetched_at"],
        "official_source": "https://developers.openai.com/api/docs/models/compare",
        "official_source_checked_at": "2026-09-18",
        "availability_scope": "Local Codex model catalog proves listed availability; account quotas were not probed.",
        "candidates": candidates, "lower_reasoning_fallback": fallback,
        "selected": {"model": MODEL, "reasoning": REASONING, "service_tier": SERVICE_TIER,
                     "rationale": "Balanced cheaper model at high reasoning; lower quality risk than Luna for first engineering screening."},
        "excluded": {"openrouter": "PROHIBITED", "claude": "PROHIBITED", "gpt-6-astra_xhigh": "REFERENCE_ONLY_NO_NEW_CALLS"},
    }


def copy_frozen_inputs(sample: list[dict]) -> dict[str, dict]:
    hashes = {}
    for row in sample:
        sample_id = f"PAIR_{row['pair']}_{row['group_id']}"
        source = source_call_dir(row["pair"], row["group_id"])
        target = OUT / "candidate_inputs" / sample_id
        target.mkdir(parents=True)
        selected = [source / "prompt.txt", source / "schema.json", *sorted(source.glob("image_*.png"))]
        hashes[sample_id] = {}
        for path in selected:
            if not path.is_file():
                raise FileNotFoundError(path)
            shutil.copyfile(path, target / path.name)
            digest = sha(path)
            if sha(target / path.name) != digest:
                raise RuntimeError(f"Input copy drift: {sample_id}/{path.name}")
            hashes[sample_id][path.name] = digest
    return hashes


def prepare() -> None:
    if OUT.exists():
        raise FileExistsError(f"One screening run only: {OUT}")
    OUT.mkdir(parents=True)
    write_new(OUT / "AVAILABLE_MODEL_CANDIDATES.json", candidate_catalog())
    all_rows = pair_a_structure() + pair_b_structure()
    selected = choose_four([row for row in all_rows if row["pair"] == "A"]) + choose_four(
        [row for row in all_rows if row["pair"] == "B"])
    input_hashes = copy_frozen_inputs(selected)
    freeze = {
        "schema": "CHEAP_MODEL_SCREENING_SAMPLE_FREEZE/1", "frozen_at": now(),
        "selection_stage": "BEFORE_SOURCE_TRUTH_AND_REFERENCE_OUTPUT_ACCESS",
        "selection_basis": "structural properties only: page/image/table/graphic counts and input size",
        "source_truth_opened": False, "astra_outputs_opened": False,
        "pair_a_count": 4, "pair_b_count": 4, "total_groups": 8,
        "selected_model": MODEL, "selected_reasoning": REASONING, "service_tier": SERVICE_TIER,
        "groups": selected, "candidate_input_hashes": input_hashes,
        "prohibited_before_result_freeze": ["REAL15", "PROVEN10", "source-first evaluation", "expected changes", "Astra outputs"],
        "validation": "NOT OPENED", "final_holdout": "NOT OPENED",
    }
    write_new(OUT / "SCREENING_SAMPLE_FREEZE.json", freeze)
    print(json.dumps({"status": "PREPARED", "groups": [f"{r['pair']}:{r['group_id']}" for r in selected]}, ensure_ascii=False))


def sandbox_command(work: Path, command: list[str]) -> list[str]:
    cli = Path(shutil.which("codex") or "").resolve()
    auth = Path("/home/coder/.codex/auth.json")
    if not cli.is_file() or not auth.is_file() or not shutil.which("bwrap"):
        raise RuntimeError("Codex CLI/ChatGPT authentication or bubblewrap unavailable")
    args = ["bwrap", "--unshare-all", "--share-net", "--die-with-parent", "--new-session",
            "--ro-bind", "/usr", "/usr", "--symlink", "usr/bin", "/bin",
            "--symlink", "usr/lib", "/lib", "--symlink", "usr/lib64", "/lib64",
            "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
            "--dir", "/home/coder/.codex", "--ro-bind", str(auth), "/home/coder/.codex/auth.json",
            "--ro-bind", str(cli), "/opt/codex", "--bind", str(work.resolve()), "/work",
            "--chdir", "/work", "--setenv", "HOME", "/home/coder",
            "--setenv", "CODEX_HOME", "/home/coder/.codex"]
    for path in ["/etc/ssl", "/etc/resolv.conf", "/etc/hosts", "/etc/nsswitch.conf", "/etc/passwd"]:
        if Path(path).exists():
            args += ["--ro-bind", path, path]
    return args + command


def cli_command(image_names: list[str]) -> list[str]:
    command = ["/opt/codex", "exec", "--ephemeral", "--ignore-user-config", "--ignore-rules",
               "--skip-git-repo-check", "--sandbox", "read-only", "--json", "--color", "never",
               "--model", MODEL, "-c", 'model_provider="openai"',
               "-c", f'model_reasoning_effort="{REASONING}"',
               "-c", f'service_tier="{SERVICE_TIER}"', "-c", 'web_search="disabled"',
               "-c", "mcp_servers={}", "-c", "project_doc_max_bytes=0", "-c", 'approval_policy="never"',
               "--output-schema", "/work/schema.json", "--output-last-message", "/work/final.txt"]
    for feature in DISABLED:
        command += ["--disable", feature]
    for name in image_names:
        command += ["--image", f"/work/{name}"]
    return command + ["-"]


async def call_one(row: dict) -> dict:
    sample_id = f"PAIR_{row['pair']}_{row['group_id']}"
    source = OUT / "candidate_inputs" / sample_id
    target = OUT / "candidate_raw" / sample_id
    target.mkdir(parents=True, exist_ok=False)
    for path in [source / "prompt.txt", source / "schema.json", *sorted(source.glob("image_*.png"))]:
        shutil.copyfile(path, target / path.name)
    expected = read(OUT / "SCREENING_SAMPLE_FREEZE.json")["candidate_input_hashes"][sample_id]
    actual = {path.name: sha(path) for path in target.iterdir() if path.is_file()}
    if actual != expected:
        raise RuntimeError(f"Frozen input mismatch: {sample_id}")
    images = sorted(path.name for path in target.glob("image_*.png"))
    command = sandbox_command(target, cli_command(images))
    write_new(target / "INVOCATION.json", {
        "at": now(), "sample_id": sample_id, "model": MODEL, "reasoning": REASONING,
        "service_tier": SERVICE_TIER, "provider": "codex_chatgpt", "openrouter_calls": 0,
        "claude_calls": 0, "retries": 0, "tools_disabled": True, "fresh_ephemeral_context": True,
        "input_hashes": expected, "command": command,
    })
    started = time.monotonic()
    with (target / "raw.jsonl").open("wb") as raw, (target / "stderr.txt").open("wb") as err:
        process = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE,
            stdout=raw, stderr=err, env=safe_env(), start_new_session=True)
        try:
            await asyncio.wait_for(process.communicate((target / "prompt.txt").read_bytes()), TIMEOUT_SECONDS)
        except BaseException:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
            raise
    records = []
    for line in (target / "raw.jsonl").read_text().splitlines():
        try:
            records.append(json.loads(line))
        except ValueError:
            pass
    usage = [item["usage"] for item in records if item.get("type") == "turn.completed" and item.get("usage")]
    tools = [item for item in records if item.get("type", "").startswith("item.") and
             item.get("item", {}).get("type") not in {None, "agent_message", "reasoning", "error"}]
    receipt = {
        "at": now(), "sample_id": sample_id, "exit_code": process.returncode,
        "wall_time_seconds": time.monotonic() - started, "usage": usage, "tool_items": len(tools),
        "raw_sha256": sha(target / "raw.jsonl"), "retries": 0,
    }
    write_new(target / "RECEIPT.json", receipt)
    if process.returncode or len(usage) != 1 or tools:
        raise RuntimeError(f"Candidate call failed closed with no retry: {sample_id}")
    value = read(target / "final.txt")
    jsonschema.validate(value, read(target / "schema.json"))
    if value["map_group_id"] != row["group_id"]:
        raise RuntimeError(f"Group identity mismatch: {sample_id}")
    write_new(target / "parsed.json", value)
    print(json.dumps({"sample_id": sample_id, "status": "SUCCESS", "seconds": round(receipt["wall_time_seconds"])}), flush=True)
    return value


def verify_sample_freeze() -> dict:
    freeze = read(OUT / "SCREENING_SAMPLE_FREEZE.json")
    for sample_id, files in freeze["candidate_input_hashes"].items():
        for name, digest in files.items():
            if sha(OUT / "candidate_inputs" / sample_id / name) != digest:
                raise RuntimeError(f"Sample freeze drift: {sample_id}/{name}")
    return freeze


async def run_calls() -> None:
    freeze = verify_sample_freeze()
    if (OUT / "CANDIDATE_RESULT_FREEZE.json").exists():
        raise FileExistsError("Candidate result is already frozen")
    results = []
    # Sequential execution makes the exact eight-call budget and stop behavior explicit.
    for row in freeze["groups"]:
        results.append(await call_one(row))
    write_new(OUT / "CANDIDATE_RESULTS.json", {
        "model": MODEL, "reasoning": REASONING, "groups": results,
        "concrete_changes": [change for result in results for change in result["concrete_changes"]],
        "unresolved_hints": [hint for result in results for hint in result["unresolved_hints"]],
    })
    paths = [path for folder in [OUT / "candidate_inputs", OUT / "candidate_raw"]
             for path in folder.rglob("*") if path.is_file()]
    paths.append(OUT / "CANDIDATE_RESULTS.json")
    write_new(OUT / "CANDIDATE_RESULT_FREEZE.json", {
        "schema": "CANDIDATE_RESULT_FREEZE/1", "frozen_at": now(),
        "model": MODEL, "reasoning": REASONING, "model_calls": 8, "successful_calls": 8,
        "retries": 0, "astra_calls": 0, "openrouter_calls": 0, "claude_calls": 0,
        "source_truth_opened_before_freeze": False, "astra_outputs_opened_before_freeze": False,
        "hashes": {str(path.relative_to(OUT)): sha(path) for path in sorted(paths)},
        "validation": "NOT OPENED", "final_holdout": "NOT OPENED",
    })
    print("CANDIDATE_RESULT_FROZEN", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "run"])
    action = parser.parse_args().action
    if action == "prepare":
        prepare()
    else:
        asyncio.run(run_calls())


if __name__ == "__main__":
    main()
