#!/usr/bin/env python3
"""Resume Pair A mining after the Codex turn/start transport limit.

For the three oversized frozen prompts, leading exact text chunks are injected
into fresh model-visible history and the final exact chunk starts the turn.
Concatenating the ordered user-text chunks is byte-for-byte identical to the
pre-Pair-B EXACT_PROMPT.txt.  No prompt characters, evidence, schema, image,
model, reasoning setting, or mining region change.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import time
from typing import Any


REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from experiments.project_change_272 import ai_first_semantic_mapping_projectchange_v3 as v3  # noqa: E402
from experiments.project_change_272 import ai_first_semantic_mapping_projectchange_v3_pair_a as pair_a  # noqa: E402


OUT = v3.OUT
CHUNK_CHARS = 800_000
OVERSIZED = {"A-R003", "A-R011", "A-R022"}


def command(target: Path) -> list[str]:
    cli = Path(shutil.which("codex") or "").resolve()
    auth = Path("/home/coder/.codex/auth.json")
    if not cli.is_file() or not auth.is_file() or not shutil.which("bwrap"):
        raise RuntimeError("Codex CLI/ChatGPT authentication or isolation unavailable")
    args = [
        "bwrap", "--unshare-all", "--share-net", "--die-with-parent", "--new-session",
        "--ro-bind", "/usr", "/usr", "--symlink", "usr/bin", "/bin",
        "--symlink", "usr/lib", "/lib", "--symlink", "usr/lib64", "/lib64",
        "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
        "--dir", "/home/coder/.codex", "--ro-bind", str(auth), "/home/coder/.codex/auth.json",
        "--ro-bind", str(cli), "/opt/codex", "--bind", str(target.resolve()), "/work",
        "--chdir", "/work", "--setenv", "HOME", "/home/coder",
        "--setenv", "CODEX_HOME", "/home/coder/.codex",
    ]
    for path in ("/etc/ssl", "/etc/resolv.conf", "/etc/hosts", "/etc/nsswitch.conf", "/etc/passwd"):
        if Path(path).exists():
            args += ["--ro-bind", path, path]
    app = [
        "/opt/codex", "app-server", "--stdio",
        "-c", 'model_provider="openai"',
        "-c", 'model_reasoning_effort="xhigh"',
        "-c", 'service_tier="priority"',
        "-c", 'web_search="disabled"',
        "-c", "mcp_servers={}",
        "-c", "project_doc_max_bytes=0",
        "-c", 'approval_policy="never"',
    ]
    for feature in (
        "shell_tool", "unified_exec", "apps", "plugins", "remote_plugin", "memories",
        "multi_agent", "browser_use", "browser_use_external", "computer_use",
        "image_generation", "hooks", "shell_snapshot", "skill_search",
        "code_mode_host", "unbounded_connection_retries",
    ):
        app += ["--disable", feature]
    return args + app


async def send(process: asyncio.subprocess.Process, value: dict[str, Any]) -> None:
    assert process.stdin is not None
    process.stdin.write((json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
    await process.stdin.drain()


async def read_message(process: asyncio.subprocess.Process, raw_handle: Any) -> dict[str, Any]:
    assert process.stdout is not None
    line = await asyncio.wait_for(process.stdout.readline(), 1800)
    if not line:
        raise RuntimeError("Codex App Server closed before turn completion")
    raw_handle.write(line)
    raw_handle.flush()
    return json.loads(line)


async def wait_response(process: asyncio.subprocess.Process, raw_handle: Any, request_id: int) -> dict[str, Any]:
    while True:
        message = await read_message(process, raw_handle)
        if message.get("id") == request_id:
            if "error" in message:
                raise RuntimeError(f"App Server request {request_id} failed: {message['error']}")
            return message["result"]


def usage_from_notification(message: dict[str, Any]) -> dict[str, int] | None:
    if message.get("method") != "thread/tokenUsage/updated":
        return None
    last = message["params"]["tokenUsage"]["last"]
    return {
        "input_tokens": last.get("inputTokens", 0),
        "cached_input_tokens": last.get("cachedInputTokens", 0),
        "cache_write_input_tokens": last.get("cacheWriteInputTokens", 0),
        "output_tokens": last.get("outputTokens", 0),
        "reasoning_output_tokens": last.get("reasoningOutputTokens", 0),
    }


async def injected_call(region: dict[str, Any]) -> dict[str, Any]:
    region_id = region["region_id"]
    base_call_id = f"PAIR_A_{region_id}"
    call_id = base_call_id + "_INJECT"
    target = OUT / "miner_raw" / call_id
    target.mkdir(parents=True, exist_ok=False)
    prepared = OUT / "miner_inputs_optimized" / base_call_id / "EXACT_PROMPT.txt"
    payload = prepared.read_text(encoding="utf-8")
    data, images, _ = v3.optimized_region_bundle("A", region)
    labels, names, seen = [], [], set()
    for row in images:
        source = Path(row["path"])
        digest = v3.sha256(source)
        if digest in seen:
            continue
        seen.add(digest)
        name = f"image_{len(names):03d}.png"
        shutil.copyfile(source, target / name)
        names.append(name)
        labels.append({**row["label"], "image": len(names)})
    rebuilt = v3.MINER_PROMPT + "\nIMAGES:\n" + json.dumps(labels, ensure_ascii=False) + "\nSOURCE DATA:\n" + json.dumps(data, ensure_ascii=False)
    if rebuilt != payload or v3.read_json(prepared.parent / "MODEL_INPUT.json") != data:
        raise RuntimeError(f"Prepared optimized input drift: {base_call_id}")
    chunks = [payload[start:start + CHUNK_CHARS] for start in range(0, len(payload), CHUNK_CHARS)]
    if len(chunks) < 2 or any(len(chunk) > CHUNK_CHARS for chunk in chunks) or "".join(chunks) != payload:
        raise RuntimeError(f"Invalid lossless transport split: {base_call_id}")
    shutil.copyfile(prepared, target / "prompt.txt")
    v3.write_new(target / "schema.json", v3.MINER_SCHEMA)
    transport = {
        "base_call_id": base_call_id,
        "call_id": call_id,
        "reason": "Codex turn/start rejects total user text >1048576 characters before inference",
        "transport": "codex_app_server_injected_history_plus_final_turn_chunk",
        "chunk_chars": [len(chunk) for chunk in chunks],
        "chunk_sha256": [__import__("hashlib").sha256(chunk.encode("utf-8")).hexdigest() for chunk in chunks],
        "concatenation_sha256": __import__("hashlib").sha256("".join(chunks).encode("utf-8")).hexdigest(),
        "frozen_prompt_sha256": v3.sha256(prepared),
        "lossless_concatenation_equal": "".join(chunks) == payload,
        "prompt_changed": False,
        "evidence_changed": False,
        "schema_changed": False,
        "openrouter": 0,
    }
    if transport["concatenation_sha256"] != transport["frozen_prompt_sha256"]:
        raise RuntimeError(f"Split transport hash mismatch: {base_call_id}")
    v3.write_new(target / "TRANSPORT.json", transport)
    app_command = command(target)
    input_hashes = {path.name: v3.sha256(path) for path in target.iterdir()}
    v3.write_new(target / "INVOCATION.json", {
        "call_id": call_id, "stage": "MINING", "pair": "A", "at": pair_a.now(),
        "provider": "codex_chatgpt", "model": v3.MODEL, "reasoning": v3.REASONING,
        "openrouter": 0, "command": app_command, "environment_keys": list(v3.safe_env()),
        "input_hashes": input_hashes,
    })
    started = time.monotonic()
    process = await asyncio.create_subprocess_exec(
        *app_command, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE, env=v3.safe_env(), start_new_session=True,
    )
    messages: list[dict[str, Any]] = []
    final_messages: list[str] = []
    usage: dict[str, int] | None = None
    completed: dict[str, Any] | None = None
    try:
        with (target / "raw.jsonl").open("wb") as raw:
            await send(process, {"method": "initialize", "id": 1, "params": {"clientInfo": {"name": "projectchange-v3-frozen", "version": "1.0"}, "capabilities": {"experimentalApi": True}}})
            await wait_response(process, raw, 1)
            await send(process, {"method": "initialized", "params": {}})
            await send(process, {"method": "thread/start", "id": 2, "params": {
                "model": v3.MODEL, "modelProvider": "openai", "cwd": "/work",
                "approvalPolicy": "never", "sandbox": "read-only", "ephemeral": True,
                "serviceTier": "priority", "experimentalRawEvents": False,
            }})
            thread = await wait_response(process, raw, 2)
            thread_id = thread["thread"]["id"]
            injected = [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": chunk}]}
                for chunk in chunks[:-1]
            ]
            await send(process, {"method": "thread/inject_items", "id": 3, "params": {
                "threadId": thread_id, "items": injected,
            }})
            await wait_response(process, raw, 3)
            inputs = [{"type": "text", "text": chunks[-1]}]
            inputs.extend({"type": "localImage", "path": f"/work/{name}"} for name in names)
            await send(process, {"method": "turn/start", "id": 4, "params": {
                "threadId": thread_id, "input": inputs, "model": v3.MODEL,
                "effort": v3.REASONING, "cwd": "/work", "approvalPolicy": "never",
                "serviceTierForTurn": "priority", "outputSchema": v3.MINER_SCHEMA,
            }})
            await wait_response(process, raw, 4)
            while completed is None:
                message = await read_message(process, raw)
                messages.append(message)
                new_usage = usage_from_notification(message)
                if new_usage is not None:
                    usage = new_usage
                if message.get("method") == "item/completed":
                    item = message["params"]["item"]
                    if item.get("type") == "agentMessage":
                        final_messages.append(item["text"])
                if message.get("method") == "turn/completed":
                    completed = message["params"]["turn"]
    except BaseException:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
        raise
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 10)
            except asyncio.TimeoutError:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
        assert process.stderr is not None
        (target / "stderr.txt").write_bytes(await process.stderr.read())
    status = completed.get("status") if completed else None
    error = completed.get("error") if completed else None
    if status != "completed" or error or usage is None or not final_messages:
        receipt = {
            "call_id": call_id, "stage": "MINING", "pair": "A", "at": pair_a.now(),
            "exit_code": process.returncode, "wall_time_seconds": time.monotonic() - started,
            "usage": [usage] if usage else [], "tool_items": 0,
            "raw_sha256": v3.sha256(target / "raw.jsonl"), "turn_status": status, "turn_error": error,
        }
        v3.write_new(target / "RECEIPT.json", receipt)
        raise RuntimeError(f"Split transport model call failed: {call_id}: {status}: {error}")
    final_text = final_messages[-1]
    v3.write_new(target / "final.txt", final_text)
    value = json.loads(final_text)
    v3.jsonschema.validate(value, v3.MINER_SCHEMA)
    v3.validate_miner("A", region, value)
    v3.write_new(target / "parsed.json", value)
    tool_types = {
        message["params"]["item"].get("type")
        for message in messages
        if message.get("method") == "item/completed"
        and message["params"]["item"].get("type") not in {"agentMessage", "reasoning", "userMessage"}
    }
    receipt = {
        "call_id": call_id, "stage": "MINING", "pair": "A", "at": pair_a.now(),
        "exit_code": 0, "wall_time_seconds": time.monotonic() - started,
        "usage": [usage], "tool_items": len(tool_types),
        "raw_sha256": v3.sha256(target / "raw.jsonl"), "turn_status": status,
        "transport": "lossless_text_injected_history_plus_final_turn_chunk",
    }
    v3.write_new(target / "RECEIPT.json", receipt)
    if tool_types:
        raise RuntimeError(f"Unexpected App Server tool items: {sorted(tool_types)}")
    for name, digest in input_hashes.items():
        if v3.sha256(target / name) != digest:
            raise RuntimeError(f"Inference input drift: {call_id}/{name}")
    print(json.dumps({"call": call_id, "status": "SUCCESS", "usage": usage}, ensure_ascii=False), flush=True)
    return value


def load_success(region: dict[str, Any]) -> dict[str, Any] | None:
    base = OUT / "miner_raw" / f"PAIR_A_{region['region_id']}"
    split = OUT / "miner_raw" / f"PAIR_A_{region['region_id']}_INJECT"
    for target in (base, split):
        parsed = target / "parsed.json"
        receipt = target / "RECEIPT.json"
        if parsed.exists() and receipt.exists() and len(v3.read_json(receipt).get("usage", [])) == 1:
            value = v3.read_json(parsed)
            v3.validate_miner("A", region, value)
            return value
    return None


def finalize(results: list[dict[str, Any]]) -> None:
    result_path = OUT / "PAIR_A_PROJECTCHANGE_MINER_RESULTS.json"
    v3.write_new(result_path, {
        "created_at": pair_a.now(), "model": v3.MODEL, "reasoning": v3.REASONING,
        "regions": results,
        "projectchanges": [change for region in results for change in region["projectchanges"]],
        "unresolved_hints": [hint for region in results for hint in region["unresolved_hints"]],
    })
    hashes = {
        "PAIR_A_PROJECTCHANGE_MINER_RESULTS.json": v3.sha256(result_path),
        "PAIR_A_STAGED_ARCHITECTURE_FREEZE.json": v3.sha256(OUT / "PAIR_A_STAGED_ARCHITECTURE_FREEZE.json"),
    }
    for path in sorted((OUT / "miner_raw").glob("PAIR_A*/*")):
        if path.is_file():
            hashes[str(path.relative_to(OUT))] = v3.sha256(path)
    actual = pair_a.usage_total(pair_a.stage_receipts("MINING"))
    successful_calls = sum(len(receipt.get("usage", [])) == 1 for receipt in pair_a.stage_receipts("MINING"))
    if actual > pair_a.HARD_CAP:
        raise RuntimeError("PAIR_A_TOKEN_BUDGET_REVIEW_REQUIRED")
    if successful_calls != 25:
        raise RuntimeError(f"Expected 25 successful Miner calls, got {successful_calls}")
    v3.write_new(OUT / "PAIR_A_PROJECTCHANGE_MINER_V3_FREEZE.json", {
        "frozen_at": pair_a.now(), "status": "PAIR_A_MINER_FROZEN", "model": v3.MODEL,
        "reasoning": v3.REASONING, "regions": 25, "calls": successful_calls,
        "transport_rejections_before_inference": 1,
        "lossless_injected_transport_regions": sorted(OVERSIZED),
        "projectchanges": len([c for r in results for c in r["projectchanges"]]),
        "unresolved_hints": len([h for r in results for h in r["unresolved_hints"]]),
        "actual_input_output": actual, "hard_cap": pair_a.HARD_CAP,
        "truth_opened": False, "hashes": hashes,
    })
    print("PAIR_A_PROJECTCHANGE_MINER_V3_FROZEN", flush=True)


async def resume() -> None:
    pair_a.verify_architecture()
    preflight = pair_a.optimized_preflight()
    calls = [row for row in preflight["calls"] if row["pair"] == "A"]
    mapping = v3.read_json(OUT / "PAIR_A_SEMANTIC_MAP.json")
    results = []
    for index, region in enumerate(mapping["regions"]):
        existing = load_success(region)
        if existing is not None:
            results.append(existing)
            continue
        actual = pair_a.usage_total(pair_a.stage_receipts("MINING"))
        projected_remaining = sum(row["projected_tokens_after"] for row in calls[index:]) + pair_a.FUTURE_DEDUPE_ALLOWANCE
        if actual + projected_remaining > pair_a.HARD_CAP:
            raise RuntimeError("PAIR_A_TOKEN_BUDGET_REVIEW_REQUIRED")
        if actual + projected_remaining > pair_a.SOFT_WARNING:
            print(json.dumps({"status": "PAIR_A_TOKEN_SOFT_WARNING", "actual": actual, "projected_remaining": projected_remaining, "projected_total": actual + projected_remaining}), flush=True)
        if region["region_id"] in OVERSIZED:
            value = await injected_call(region)
        else:
            data, images, _ = v3.optimized_region_bundle("A", region)
            call_id = f"PAIR_A_{region['region_id']}"
            value = await pair_a.call("MINING", call_id, v3.MINER_PROMPT, data, v3.MINER_SCHEMA, images, OUT / "miner_inputs_optimized" / call_id / "EXACT_PROMPT.txt")
            v3.validate_miner("A", region, value)
        results.append(value)
    finalize(results)


if __name__ == "__main__":
    try:
        asyncio.run(resume())
    except BaseException as exc:
        v3.write_new(OUT / f"PAIR_A_RECOVERY_STOP_{int(time.time())}.json", {
            "at": pair_a.now(), "status": "STOPPED_NO_AUTOMATIC_RETRY",
            "error": str(exc), "error_type": type(exc).__name__, "truth_opened": False,
        })
        raise
