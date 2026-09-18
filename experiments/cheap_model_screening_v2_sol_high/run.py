"""Run exactly eight Sol/high calls over the byte-identical V1 sample inputs.

This pre-freeze runner reads only the V1 sample freeze and its candidate_inputs
directory.  It never opens V1 candidate outputs, Astra outputs, or evaluation
truth.  There are no retries and no automatic evaluation or continuation.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import time


ROOT = Path("/home/coder/auditmanager/corpus-audits/20260914_project_change_272")
V1 = ROOT / "cheap_model_screening_v1"
OUT = ROOT / "cheap_model_screening_v2_sol_high"
MODEL_CACHE = Path("/home/coder/.codex/models_cache.json")
MODEL = "gpt-5.6-sol"
REASONING = "high"
SERVICE_TIER = "priority"
TIMEOUT_SECONDS = 1200
EXPECTED_GROUPS = ["PAIR_A_G002", "PAIR_A_G008", "PAIR_A_G024", "PAIR_A_G031",
                   "PAIR_B_G004", "PAIR_B_G017", "PAIR_B_G025", "PAIR_B_G035"]
DEPS = ROOT / "controlled_inference_f1_f4_f2_v3/runtime_deps"
sys.path.insert(0, str(DEPS))
import jsonschema


DISABLED = [
    "shell_tool", "unified_exec", "apps", "plugins", "remote_plugin",
    "memories", "multi_agent", "browser_use", "browser_use_external",
    "computer_use", "image_generation", "hooks", "shell_snapshot",
    "skill_search", "code_mode_host", "unbounded_connection_retries",
]


def now():
    return datetime.now(timezone.utc).isoformat()


def read(path):
    return json.loads(Path(path).read_text())


def write_new(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def safe_env():
    return {key: os.environ[key] for key in ["PATH", "LANG", "LC_ALL", "TZ"] if key in os.environ}


def local_model_record():
    cache = read(MODEL_CACHE)
    matches = [row for row in cache["models"] if row.get("slug") == MODEL and row.get("visibility") == "list"]
    if len(matches) != 1:
        raise RuntimeError(f"{MODEL} is not uniquely listed in the current local model catalog")
    model = matches[0]
    levels = [row["effort"] for row in model["supported_reasoning_levels"]]
    if REASONING not in levels:
        raise RuntimeError(f"{MODEL} does not list reasoning={REASONING}")
    return cache, model, levels


def prepare():
    if OUT.exists():
        raise FileExistsError(f"One screening run only: {OUT}")
    v1_freeze_path = V1 / "SCREENING_SAMPLE_FREEZE.json"
    v1_freeze = read(v1_freeze_path)
    group_ids = [f"PAIR_{row['pair']}_{row['group_id']}" for row in v1_freeze["groups"]]
    if group_ids != EXPECTED_GROUPS or v1_freeze["total_groups"] != 8:
        raise RuntimeError(f"V1 sample identity drift: {group_ids}")
    cache, model, levels = local_model_record()
    OUT.mkdir(parents=True)
    copied_hashes = {}
    equality_rows = []
    for sample_id in EXPECTED_GROUPS:
        source = V1 / "candidate_inputs" / sample_id
        target = OUT / "sol_inputs" / sample_id
        target.mkdir(parents=True)
        expected = v1_freeze["candidate_input_hashes"][sample_id]
        copied_hashes[sample_id] = {}
        for name, digest in expected.items():
            source_path = source / name
            if sha(source_path) != digest:
                raise RuntimeError(f"V1 frozen input drift: {sample_id}/{name}")
            shutil.copyfile(source_path, target / name)
            copied = sha(target / name)
            if copied != digest:
                raise RuntimeError(f"Sol input copy drift: {sample_id}/{name}")
            copied_hashes[sample_id][name] = copied
            equality_rows.append({"sample_id": sample_id, "file": name,
                                  "v1_hash": digest, "sol_input_hash": copied, "identical": True})
    write_new(OUT / "SAMPLE_INTEGRITY.json", {
        "schema": "CHEAP_MODEL_SCREENING_V2_SAMPLE_INTEGRITY/1", "created_at": now(),
        "status": "PASS", "sample_source": str(v1_freeze_path), "sample_source_sha256": sha(v1_freeze_path),
        "same_group_ids": True, "same_input_hashes": True, "same_input_file_count": len(equality_rows),
        "groups": EXPECTED_GROUPS, "v1_group_records": v1_freeze["groups"],
        "input_hashes": copied_hashes, "file_comparisons": equality_rows,
        "semantic_prompt_changed": False, "pages_changed": False, "text_blocks_changed": False,
        "tables_changed": False, "graphic_crops_changed": False, "coordinates_changed": False,
        "full_page_context_changed": False,
        "only_runtime_change": {"from": "gpt-5.6-terra/high", "to": f"{MODEL}/{REASONING}"},
        "model_availability": {"local_catalog": str(MODEL_CACHE), "fetched_at": cache["fetched_at"],
                               "slug": model["slug"], "visibility": model["visibility"],
                               "supported_reasoning_levels": levels},
        "official_model_source": "https://developers.openai.com/api/docs/models/gpt-5.6-sol",
        "availability_check_inference_calls": 0,
        "truth_opened": False, "astra_outputs_opened": False, "terra_outputs_opened": False,
        "validation": "NOT OPENED", "final_holdout": "NOT OPENED",
    })
    print(json.dumps({"status": "PREPARED", "groups": EXPECTED_GROUPS}, ensure_ascii=False))


def verify_integrity():
    integrity = read(OUT / "SAMPLE_INTEGRITY.json")
    if integrity["status"] != "PASS" or integrity["groups"] != EXPECTED_GROUPS:
        raise RuntimeError("Sample integrity is not PASS")
    for sample_id, files in integrity["input_hashes"].items():
        for name, digest in files.items():
            if sha(OUT / "sol_inputs" / sample_id / name) != digest:
                raise RuntimeError(f"Sol input hash drift: {sample_id}/{name}")
    return integrity


def sandbox_command(work, command):
    cli = Path(shutil.which("codex") or "").resolve()
    auth = Path("/home/coder/.codex/auth.json")
    if not cli.is_file() or not auth.is_file() or not shutil.which("bwrap"):
        raise RuntimeError("Codex CLI/ChatGPT authentication or bubblewrap unavailable")
    args = ["bwrap", "--unshare-all", "--share-net", "--die-with-parent", "--new-session",
            "--ro-bind", "/usr", "/usr", "--symlink", "usr/bin", "/bin",
            "--symlink", "usr/lib", "/lib", "--symlink", "usr/lib64", "/lib64",
            "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
            "--dir", "/home/coder/.codex", "--ro-bind", str(auth), "/home/coder/.codex/auth.json",
            "--ro-bind", str(cli), "/opt/codex", "--bind", str(Path(work).resolve()), "/work",
            "--chdir", "/work", "--setenv", "HOME", "/home/coder",
            "--setenv", "CODEX_HOME", "/home/coder/.codex"]
    for path in ["/etc/ssl", "/etc/resolv.conf", "/etc/hosts", "/etc/nsswitch.conf", "/etc/passwd"]:
        if Path(path).exists():
            args += ["--ro-bind", path, path]
    return args + command


def cli_command(images):
    command = ["/opt/codex", "exec", "--ephemeral", "--ignore-user-config", "--ignore-rules",
               "--skip-git-repo-check", "--sandbox", "read-only", "--json", "--color", "never",
               "--model", MODEL, "-c", 'model_provider="openai"',
               "-c", f'model_reasoning_effort="{REASONING}"',
               "-c", f'service_tier="{SERVICE_TIER}"', "-c", 'web_search="disabled"',
               "-c", "mcp_servers={}", "-c", "project_doc_max_bytes=0", "-c", 'approval_policy="never"',
               "--output-schema", "/work/schema.json", "--output-last-message", "/work/final.txt"]
    for feature in DISABLED:
        command += ["--disable", feature]
    for name in images:
        command += ["--image", f"/work/{name}"]
    return command + ["-"]


async def call_one(sample_id, frozen_hashes):
    source = OUT / "sol_inputs" / sample_id
    target = OUT / "sol_raw" / sample_id
    target.mkdir(parents=True, exist_ok=False)
    for path in [source / "prompt.txt", source / "schema.json", *sorted(source.glob("image_*.png"))]:
        shutil.copyfile(path, target / path.name)
    actual = {path.name: sha(path) for path in target.iterdir() if path.is_file()}
    if actual != frozen_hashes:
        raise RuntimeError(f"Invocation input mismatch: {sample_id}")
    images = sorted(path.name for path in target.glob("image_*.png"))
    command = sandbox_command(target, cli_command(images))
    write_new(target / "INVOCATION.json", {
        "at": now(), "sample_id": sample_id, "model": MODEL, "reasoning": REASONING,
        "service_tier": SERVICE_TIER, "provider": "codex_chatgpt", "fresh_ephemeral_context": True,
        "tools_disabled": True, "retries": 0, "astra_calls": 0, "terra_calls": 0,
        "openrouter_calls": 0, "claude_calls": 0, "input_hashes": frozen_hashes, "command": command,
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
    usage = [row["usage"] for row in records if row.get("type") == "turn.completed" and row.get("usage")]
    tool_items = [row for row in records if row.get("type", "").startswith("item.") and
                  row.get("item", {}).get("type") not in {None, "agent_message", "reasoning", "error"}]
    receipt = {"at": now(), "sample_id": sample_id, "exit_code": process.returncode,
               "wall_time_seconds": time.monotonic() - started, "usage": usage,
               "tool_items": len(tool_items), "raw_sha256": sha(target / "raw.jsonl"), "retries": 0}
    write_new(target / "RECEIPT.json", receipt)
    if process.returncode or len(usage) != 1 or tool_items:
        raise RuntimeError(f"Sol call failed closed with no retry: {sample_id}")
    value = read(target / "final.txt")
    jsonschema.validate(value, read(target / "schema.json"))
    expected_group = sample_id.rsplit("_", 1)[1]
    if value["map_group_id"] != expected_group:
        raise RuntimeError(f"Group identity mismatch: {sample_id}")
    write_new(target / "parsed.json", value)
    print(json.dumps({"sample_id": sample_id, "status": "SUCCESS", "seconds": round(receipt["wall_time_seconds"])}), flush=True)
    return value


async def run_calls():
    integrity = verify_integrity()
    if (OUT / "SOL_HIGH_RESULT_FREEZE.json").exists():
        raise FileExistsError("Sol/high result is already frozen")
    results = []
    for sample_id in EXPECTED_GROUPS:
        results.append(await call_one(sample_id, integrity["input_hashes"][sample_id]))
    write_new(OUT / "SOL_HIGH_RESULTS.json", {
        "model": MODEL, "reasoning": REASONING, "groups": results,
        "concrete_changes": [change for result in results for change in result["concrete_changes"]],
        "unresolved_hints": [hint for result in results for hint in result["unresolved_hints"]],
    })
    paths = [path for folder in [OUT / "sol_inputs", OUT / "sol_raw"]
             for path in folder.rglob("*") if path.is_file()]
    paths.append(OUT / "SOL_HIGH_RESULTS.json")
    write_new(OUT / "SOL_HIGH_RESULT_FREEZE.json", {
        "schema": "SOL_HIGH_RESULT_FREEZE/1", "frozen_at": now(),
        "model": MODEL, "reasoning": REASONING, "model_calls": 8, "successful_calls": 8,
        "retries": 0, "astra_calls": 0, "terra_calls": 0, "openrouter_calls": 0, "claude_calls": 0,
        "source_truth_opened_before_freeze": False, "astra_outputs_opened_before_freeze": False,
        "terra_outputs_opened_before_freeze": False, "no_truth_leakage": "PASS",
        "hashes": {str(path.relative_to(OUT)): sha(path) for path in sorted(paths)},
        "validation": "NOT OPENED", "final_holdout": "NOT OPENED",
    })
    print("SOL_HIGH_RESULT_FROZEN", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "run"])
    if parser.parse_args().action == "prepare":
        prepare()
    else:
        asyncio.run(run_calls())


if __name__ == "__main__":
    main()
