#!/usr/bin/env python3
"""Model-free stand-in for the Codex CLI (``exec`` and ``app-server --stdio``).

It never contacts a provider.  It enforces the real per-turn user-text limit
(1 048 576 characters, ``input_too_large``) and writes one JSON line per
session to ``$FAKE_CODEX_LOG`` with what a model would have seen: the ordered
injected history texts plus the turn text, and the images.  The answer comes
from ``$FAKE_CODEX_RESPONSE``.  ``$FAKE_CODEX_MODE``: ``reject_inject`` /
``turn_failed`` / ``tool_item`` simulate provider-side failures;
``exec_no_output`` makes ``exec`` answer only on stdout (no ``-o`` file).
``exec --json`` prints JSONL events, the last one ``turn.completed`` with usage.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys

LIMIT = 1_048_576


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def log(record: dict) -> None:
    with open(os.environ["FAKE_CODEX_LOG"], "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def response_text() -> str:
    with open(os.environ["FAKE_CODEX_RESPONSE"], encoding="utf-8") as handle:
        return handle.read()


def run_exec(argv: list[str]) -> int:
    out = argv[argv.index("-o") + 1]
    images: list[str] = []
    if "-i" in argv:
        start = argv.index("-i") + 1
        images = argv[start:argv.index("-", start)]
    prompt = sys.stdin.buffer.read().decode("utf-8")
    json_events = "--json" in argv
    log({"mode": "exec", "stdin_sha256": sha(prompt), "stdin_chars": len(prompt), "images": images,
         "output_schema": "--output-schema" in argv, "json_events": json_events})
    if len(prompt) > LIMIT:
        print(f"Error: turn/start: turn/start failed: Input exceeds the maximum length of {LIMIT} characters. "
              f'(code -32602), data: {{"input_error_code":"input_too_large","max_chars":{LIMIT},'
              f'"actual_chars":{len(prompt)}}}', file=sys.stderr)
        return 1
    answer = response_text()
    if os.environ.get("FAKE_CODEX_MODE", "") == "exec_no_output":
        print(answer)
    else:
        with open(out, "w", encoding="utf-8") as handle:
            handle.write(answer)
    if json_events:
        for event in ({"type": "thread.started", "thread_id": "fake-thread-1"}, {"type": "turn.started"},
                      {"type": "item.completed", "item": {"id": "item_0", "type": "agent_message", "text": answer}},
                      {"type": "turn.completed", "usage": {"input_tokens": len(prompt) // 7, "cached_input_tokens": 0,
                                                            "output_tokens": 10, "reasoning_output_tokens": 5}}):
            print(json.dumps(event, ensure_ascii=False))
    return 0


def run_app_server(argv: list[str]) -> int:
    mode = os.environ.get("FAKE_CODEX_MODE", "")
    history: list[str] = []
    record: dict = {"mode": "app-server", "argv": argv[1:], "requests": []}

    def send(message: dict) -> None:
        sys.stdout.write(json.dumps(message, ensure_ascii=False) + "\n")
        sys.stdout.flush()

    for raw in sys.stdin:
        message = json.loads(raw)
        method, request_id, params = message.get("method"), message.get("id"), message.get("params") or {}
        record["requests"].append(method)
        if method == "initialize":
            send({"id": request_id, "result": {"userAgent": "fake-codex/0.153.0"}})
        elif method == "thread/start":
            record["thread_start"] = {k: params.get(k) for k in ("model", "sandbox", "ephemeral", "approvalPolicy")}
            send({"id": request_id, "result": {"thread": {"id": "fake-thread-1", "ephemeral": True}}})
        elif method == "thread/inject_items":
            if mode == "reject_inject":
                send({"id": request_id, "error": {"code": -32600, "message": "inject rejected"}})
                continue
            for item in params["items"]:
                assert item["type"] == "message" and item["role"] == "user", item
                history.extend(c["text"] for c in item["content"] if c["type"] == "input_text")
            send({"id": request_id, "result": {}})
        elif method == "turn/start":
            texts = [i["text"] for i in params["input"] if i["type"] == "text"]
            images = [i["path"] for i in params["input"] if i["type"] == "localImage"]
            actual = sum(len(t) for t in texts)
            if actual > LIMIT:
                send({"id": request_id, "error": {"code": -32602, "message": f"Input exceeds the maximum length of {LIMIT} characters.",
                                                  "data": {"input_error_code": "input_too_large", "max_chars": LIMIT,
                                                           "actual_chars": actual}}})
                log(record | {"rejected_turn_chars": actual})
                continue
            visible = "".join(history) + "".join(texts)
            record.update({
                "history_items": len(history), "history_chars": [len(t) for t in history],
                "turn_text_chars": actual, "images": images,
                "model_visible_sha256": sha(visible), "model_visible_chars": len(visible),
                "turn_params": sorted(k for k in params if k != "input"),
                "effort": params.get("effort"), "cyberAccessProgram": params.get("cyberAccessProgram"),
                "output_schema": "outputSchema" in params,
            })
            log(record)
            send({"id": request_id, "result": {"turn": {"id": "turn-1", "status": "inProgress"}}})
            send({"method": "thread/tokenUsage/updated", "params": {"tokenUsage": {"last": {
                "inputTokens": len(visible) // 7, "cachedInputTokens": 0, "outputTokens": 10, "reasoningOutputTokens": 0}}}})
            if mode == "tool_item":
                send({"method": "item/completed", "params": {"item": {"type": "commandExecution", "id": "x"}}})
            if mode == "turn_failed":
                send({"method": "turn/completed", "params": {"turn": {"id": "turn-1", "status": "failed",
                                                                      "error": {"message": "usage limit reached"}}}})
                continue
            send({"method": "item/completed", "params": {"item": {"type": "agentMessage", "text": response_text()}}})
            send({"method": "turn/completed", "params": {"turn": {"id": "turn-1", "status": "completed", "error": None}}})
    return 0


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "exec":
        raise SystemExit(run_exec(args))
    if args and args[0] == "app-server":
        raise SystemExit(run_app_server(args))
    print("fake codex: unsupported invocation", args, file=sys.stderr)
    raise SystemExit(2)
