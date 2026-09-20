#!/usr/bin/env python3
"""Model-free stand-in for ``claude -p --input-format stream-json``.

It never contacts a provider.  It refuses what the real CLI refuses
(stream-json input without stream-json output), reads the user message from
stdin and writes one JSON line to ``$FAKE_CLAUDE_LOG`` with what a model would
have seen: the text, the images decoded from base64 (as SHA256), the order of
the content blocks, the flags and the environment the transport set.  The
answer comes from ``$FAKE_CLAUDE_RESPONSE``.

``$FAKE_CLAUDE_MODE`` simulates what the real CLI can do behind the caller's
back or report as a failure:

``foreign_model``   the answer is given by another model (refusal fallback)
``foreign_usage``   the requested model is absent from ``modelUsage``
``dated_model``     the API reports the model id with a release date
``tools_leak``      the session has a tool besides ``StructuredOutput``
``no_structured``   success without ``structured_output``
``quota``           ``is_error`` result: usage limit reached
``limit_as_success`` ``is_error`` with ``subtype: success`` and a text no marker knows
``max_tokens``      ``is_error`` result: output token maximum exceeded
``effort_warning``  the CLI ignored ``--effort`` (stderr warning)
``no_result``       the stream ends without a ``result`` event
``slow``            sleeps until killed (cancel test)
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
import time


def flag(argv: list[str], name: str) -> str | None:
    return argv[argv.index(name) + 1] if name in argv and argv.index(name) + 1 < len(argv) else None


def emit(event: dict) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main() -> int:
    argv = sys.argv[1:]
    if "--version" in argv:
        print("9.9.9 (Fake Claude Code)")
        return 0
    if "--help" in argv:
        print("--json-schema --tools --setting-sources --strict-mcp-config --system-prompt --input-format "
              "--output-format (text, json, stream-json) --effort (low, medium, high, xhigh, max) "
              "--no-session-persistence --verbose --model")
        return 0
    mode = os.environ.get("FAKE_CLAUDE_MODE", "")
    if flag(argv, "--input-format") == "stream-json" and flag(argv, "--output-format") != "stream-json":
        sys.stderr.write("Error: --input-format=stream-json requires output-format=stream-json.\n")
        return 1
    model = flag(argv, "--model") or ""
    lines = [line for line in sys.stdin.read().split("\n") if line.strip()]
    message = json.loads(lines[0])["message"] if lines else {"content": []}
    blocks = message["content"]
    text_blocks = [b["text"] for b in blocks if b["type"] == "text"]
    images = [base64.b64decode(b["source"]["data"]) for b in blocks if b["type"] == "image"]
    with open(os.environ["FAKE_CLAUDE_LOG"], "a", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "argv": argv,
            "stdin_lines": len(lines),
            "stdin_is_ascii": all(ord(ch) < 128 for line in lines for ch in line),
            "role": message.get("role"),
            "block_types": [b["type"] for b in blocks],
            "text": text_blocks[-1] if text_blocks else "",
            "text_sha256": hashlib.sha256((text_blocks[-1] if text_blocks else "").encode("utf-8")).hexdigest(),
            "labels": text_blocks[:-1],
            "image_sha256": [hashlib.sha256(data).hexdigest() for data in images],
            "image_media_types": [b["source"]["media_type"] for b in blocks if b["type"] == "image"],
            "cwd": os.getcwd(),
            "env": {key: os.environ.get(key) for key in (
                "DISABLE_AUTO_COMPACT", "DISABLE_AUTOUPDATER", "CLAUDE_CODE_DISABLE_TERMINAL_TITLE",
                "CLAUDE_CODE_MAX_OUTPUT_TOKENS",
                "ANTHROPIC_API_KEY", "STAGE_COMPARISON_AI_RUN")},
        }, ensure_ascii=True) + "\n")  # ASCII: the text may carry U+2028, which splits lines
    if mode == "slow":
        time.sleep(600)
    if mode == "effort_warning":
        sys.stderr.write("Warning: Unknown --effort value 'xhigh' — ignoring it and using the default effort. "
                         "Valid values: low, medium, high.\n")
    tools = ["StructuredOutput"] + (["Bash"] if mode == "tools_leak" else [])
    emit({"type": "system", "subtype": "init", "session_id": "fake-session", "model": model,
          "claude_code_version": "9.9.9", "tools": tools, "mcp_servers": [], "apiKeySource": "none",
          "permissionMode": "default"})
    if mode == "no_result":
        sys.stdout.write("stream disconnected before completion\n")
        return 1
    answer = json.loads(open(os.environ["FAKE_CLAUDE_RESPONSE"], encoding="utf-8").read())
    answered_by = {"foreign_model": "claude-opus-4-8", "dated_model": f"{model}-20260801"}.get(mode, model)
    emit({"type": "assistant", "session_id": "fake-session", "message": {
        "model": answered_by, "role": "assistant",
        "content": [{"type": "thinking", "thinking": ""},
                    {"type": "tool_use", "id": "t1", "name": "StructuredOutput", "input": answer}]}})
    usage = {"input_tokens": 120, "cache_creation_input_tokens": 3000, "cache_read_input_tokens": 3100,
             "output_tokens": 450, "output_tokens_details": {"thinking_tokens": 300}}
    used_by = "claude-opus-4-8" if mode == "foreign_usage" else answered_by
    result = {"type": "result", "subtype": "success", "is_error": False, "num_turns": 2, "stop_reason": "end_turn",
              "duration_ms": 10, "duration_api_ms": 9, "total_cost_usd": 0.0, "session_id": "fake-session",
              "result": "", "usage": usage,
              "modelUsage": {used_by: {"inputTokens": 120, "outputTokens": 450, "cacheReadInputTokens": 3100,
                                       "cacheCreationInputTokens": 3000, "costUSD": 0.0,
                                       "contextWindow": 1000000, "maxOutputTokens": 128000,
                                       "provider": "firstParty"},
                             "claude-haiku-4-5-20251001": {"inputTokens": 8, "outputTokens": 1, "costUSD": 0.0,
                                                           "contextWindow": 200000, "maxOutputTokens": 32000}}}
    if mode == "quota":
        result.update(subtype="error_during_execution", is_error=True,
                      result="Claude usage limit reached. Your limit will reset at 5am.")
    elif mode == "limit_as_success":
        result.update(is_error=True, result="You've hit your limit · resets 3am")
    elif mode == "max_tokens":
        result.update(subtype="error_during_execution", is_error=True,
                      result="API Error: Claude's response exceeded the 128000 output token maximum.")
    elif mode != "no_structured":
        result["structured_output"] = answer
    emit(result)
    return 1 if result["is_error"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
