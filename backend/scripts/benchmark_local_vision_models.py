"""Benchmark local/ngrok vision models on the image-pair comparison task.

Reads:
  comparison/model_benchmarks/discovered_models.json
  comparison/model_benchmarks/dataset/cases.json

Writes (per run):
  comparison/model_benchmarks/runs/run_<UTC-ts>/
      results.json              — full structured results
      raw/<model>/<case>.txt    — raw chat-completion bodies
      summary.md                — quick human summary

Usage:
  python backend/scripts/benchmark_local_vision_models.py [--models id1,id2] [--cases case_001,case_002] [--max-tokens 1500]

Env:
  CHANDRA_BASE_URL, NGROK_AUTH_USER, NGROK_AUTH_PASS — required
  BENCHMARK_VISION_TIMEOUT_SEC                       — per-request timeout (default 600s)
  BENCHMARK_VISION_TOTAL_TIMEOUT_SEC                 — overall (default 18000s = 5h)
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
import ssl
import sys
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "comparison" / "model_benchmarks"
DISCOVERED = BENCH / "discovered_models.json"
CASES = BENCH / "dataset" / "cases.json"
RUNS = BENCH / "runs"

PROMPT = """Сравни два изображения проектной документации.
Первое изображение — предыдущая стадия или старая версия.
Второе изображение — новая стадия или новая версия.

Найди только значимые отличия, которые видны на изображениях:
- новые элементы;
- удалённые элементы;
- изменение размеров;
- изменение положения;
- изменение подписей;
- изменение числовых значений;
- изменение таблиц;
- изменение схем;
- изменение условных обозначений.

Не выдумывай отличия.
Не считай отличием небольшие артефакты качества изображения, шум, сжатие или незначительный сдвиг, если смысл не изменился.

Если значимых отличий не видно, так и напиши.

Верни только JSON:
{
  "has_significant_difference": true,
  "summary": "...",
  "differences": [
    {
      "type": "added|removed|changed|moved|text_changed|table_changed|unknown",
      "severity": "low|medium|high",
      "description": "...",
      "evidence": "что именно видно на изображениях"
    }
  ],
  "confidence": 0.0
}
"""


def utc_ts() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def b64_data_url(path: Path) -> str:
    return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode()


def build_payload(model: str, left: Path, right: Path, max_tokens: int) -> dict[str, Any]:
    return {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": 0.0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": PROMPT},
                    {"type": "image_url", "image_url": {"url": b64_data_url(left)}},
                    {"type": "image_url", "image_url": {"url": b64_data_url(right)}},
                ],
            },
        ],
    }


def chat_request(base_url: str, user: str, pwd: str, payload: dict[str, Any], timeout: int) -> tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    auth = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/v1/chat/completions",
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic " + auth,
            "ngrok-skip-browser-warning": "1",
        },
    )
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        return e.code, body
    except Exception as e:
        return 0, f"__transport_error__:{type(e).__name__}:{e}"


JSON_RE = re.compile(r"\{[\s\S]*\}")


def extract_message_text(body: str) -> tuple[str, str, dict[str, Any]]:
    """Return (content_text, source, raw_json_dict).

    source ∈ {content, reasoning_content, raw, error}
    """
    try:
        d = json.loads(body)
    except Exception:
        return body, "raw", {}
    try:
        choice = d["choices"][0]
        msg = choice.get("message", {})
        content = (msg.get("content") or "").strip()
        if content:
            return content, "content", d
        reasoning = (msg.get("reasoning_content") or "").strip()
        if reasoning:
            return reasoning, "reasoning_content", d
        return "", "empty", d
    except Exception:
        return body, "raw", d


def parse_diff_json(text: str) -> tuple[dict[str, Any] | None, bool]:
    """Try to extract the model's JSON. Returns (parsed, valid)."""
    if not text:
        return None, False
    # First try direct parse
    try:
        return json.loads(text), True
    except Exception:
        pass
    # Strip ```json ... ``` fences
    cleaned = re.sub(r"```(?:json)?", "", text).replace("```", "").strip()
    try:
        return json.loads(cleaned), True
    except Exception:
        pass
    # Fall back: largest brace block
    m = JSON_RE.search(cleaned)
    if m:
        try:
            return json.loads(m.group(0)), True
        except Exception:
            pass
    return None, False


def load_discovered() -> tuple[str, str, str, list[dict[str, Any]]]:
    d = json.loads(DISCOVERED.read_text(encoding="utf-8"))
    endpoint = d["endpoints"][0]
    base_url = endpoint["base_url"]
    user = os.environ.get(endpoint["auth_env"][0], "")
    pwd = os.environ.get(endpoint["auth_env"][1], "")
    if not (base_url and user and pwd):
        raise SystemExit("missing CHANDRA_BASE_URL / NGROK_AUTH_USER / NGROK_AUTH_PASS in env")
    models = [m for m in d["models"] if m.get("selected_for_benchmark")]
    return base_url, user, pwd, models


def lms_request(base_url: str, user: str, pwd: str, method: str, path: str,
                body: dict[str, Any] | None = None, timeout: int = 60) -> tuple[int, str]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    auth = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    headers = {
        "Authorization": "Basic " + auth,
        "ngrok-skip-browser-warning": "1",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}",
        data=data,
        method=method,
        headers=headers,
    )
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body_txt = ""
        try:
            body_txt = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        return e.code, body_txt
    except Exception as e:
        return 0, f"__transport_error__:{type(e).__name__}:{e}"


def list_loaded_models(base_url: str, user: str, pwd: str) -> list[dict[str, Any]]:
    code, body = lms_request(base_url, user, pwd, "GET", "/api/v1/models", timeout=30)
    if code != 200:
        return []
    try:
        d = json.loads(body)
    except Exception:
        return []
    loaded: list[dict[str, Any]] = []
    for m in d.get("models", []) or []:
        for inst in m.get("loaded_instances") or []:
            loaded.append({"model_key": m.get("key"), "instance_id": inst.get("id")})
    return loaded


def unload_all_lms(base_url: str, user: str, pwd: str) -> int:
    loaded = list_loaded_models(base_url, user, pwd)
    n = 0
    for it in loaded:
        c, _ = lms_request(base_url, user, pwd, "POST", "/api/v1/models/unload",
                           body={"instance_id": it["instance_id"]}, timeout=120)
        if c == 200:
            n += 1
    return n


def ensure_model_loaded(base_url: str, user: str, pwd: str, model: str,
                        context_length: int = 16000, max_wait: int = 600) -> tuple[bool, str]:
    """Unload anything else and load `model` explicitly. Returns (ok, message)."""
    # check if already loaded
    loaded = list_loaded_models(base_url, user, pwd)
    if any(it["model_key"] == model for it in loaded):
        return True, "already_loaded"
    # unload others
    if loaded:
        unloaded = unload_all_lms(base_url, user, pwd)
        time.sleep(2)
    body = {
        "model": model,
        "context_length": context_length,
        "flash_attention": True,
        "offload_kv_cache_to_gpu": True,
        "echo_load_config": True,
    }
    t0 = time.time()
    code, resp = lms_request(base_url, user, pwd, "POST", "/api/v1/models/load",
                             body=body, timeout=max_wait)
    elapsed = round(time.time() - t0, 1)
    if code != 200:
        return False, f"http {code} after {elapsed}s: {resp[:300]}"
    # double-check
    loaded = list_loaded_models(base_url, user, pwd)
    if any(it["model_key"] == model for it in loaded):
        return True, f"loaded in {elapsed}s"
    return False, f"not in loaded list after {elapsed}s: {resp[:300]}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", help="comma-separated subset of model ids")
    parser.add_argument("--cases", help="comma-separated subset of case ids")
    parser.add_argument("--max-tokens", type=int, default=1800)
    parser.add_argument("--run-suffix", default="", help="suffix appended to run dir name")
    parser.add_argument("--retry", type=int, default=1, help="retries on transport error")
    parser.add_argument("--explicit-load", action="store_true",
                        help="Use /api/v1/models/load to load each model before its cases")
    parser.add_argument("--load-context-length", type=int, default=16000)
    parser.add_argument("--load-wait-sec", type=int, default=600)
    parser.add_argument("--restore-model", default="",
                        help="model to load at the very end (e.g. chandra-ocr-2)")
    args = parser.parse_args()

    timeout = int(os.environ.get("BENCHMARK_VISION_TIMEOUT_SEC", "600"))
    total_timeout = int(os.environ.get("BENCHMARK_VISION_TOTAL_TIMEOUT_SEC", str(60 * 60 * 5)))

    base_url, user, pwd, models = load_discovered()
    if args.models:
        wanted = {x.strip() for x in args.models.split(",") if x.strip()}
        models = [m for m in models if m["id"] in wanted]
    cases_blob = json.loads(CASES.read_text(encoding="utf-8"))
    cases = cases_blob["cases"]
    if args.cases:
        wanted = {x.strip() for x in args.cases.split(",") if x.strip()}
        cases = [c for c in cases if c["id"] in wanted]

    run_name = f"run_{utc_ts()}{('-' + args.run_suffix) if args.run_suffix else ''}"
    run_dir = RUNS / run_name
    (run_dir / "raw").mkdir(parents=True, exist_ok=True)

    print(f"run dir: {run_dir}")
    print(f"models : {[m['id'] for m in models]}")
    print(f"cases  : {len(cases)} cases, timeout={timeout}s")

    results: dict[str, Any] = {
        "run": run_name,
        "started_at": dt.datetime.utcnow().isoformat() + "Z",
        "base_url": base_url,
        "model_ids": [m["id"] for m in models],
        "case_ids": [c["id"] for c in cases],
        "prompt_hash": str(abs(hash(PROMPT))),
        "results": [],
    }
    results_path = run_dir / "results.json"

    def flush() -> None:
        results_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    flush()

    overall_t0 = time.time()
    load_diagnostics: dict[str, dict[str, Any]] = {}
    for m in models:
        model_id = m["id"]
        safe_model = re.sub(r"[^A-Za-z0-9._-]", "_", model_id)
        raw_dir = run_dir / "raw" / safe_model
        raw_dir.mkdir(parents=True, exist_ok=True)

        if args.explicit_load:
            ok, msg = ensure_model_loaded(
                base_url, user, pwd, model_id,
                context_length=args.load_context_length,
                max_wait=args.load_wait_sec,
            )
            load_diagnostics[model_id] = {"ok": ok, "message": msg}
            print(f"  [LOAD] {model_id}: ok={ok} msg={msg}")
            results["load_diagnostics"] = load_diagnostics
            flush()
            if not ok:
                # record a skip row for each case
                for case in cases:
                    results["results"].append({
                        "model": model_id, "case_id": case["id"], "category": case["category"],
                        "status": 0, "ok": False, "duration_sec": 0, "attempts": 0,
                        "response_source": "load_failed", "finish_reason": None,
                        "usage": None, "text_chars": 0, "json_valid": False,
                        "diff_json": None, "raw_text_preview": "",
                        "error_preview": f"explicit_load_failed: {msg}",
                        "last_transport_error": "",
                    })
                flush()
                continue

        for case in cases:
            if time.time() - overall_t0 > total_timeout:
                print(f"[total-timeout] aborting after {int(time.time() - overall_t0)}s")
                flush()
                return 2

            case_id = case["id"]
            left = BENCH / "dataset" / case["left_image"]
            right = BENCH / "dataset" / case["right_image"]
            t0 = time.time()
            attempt = 0
            status = 0
            body = ""
            last_error = ""
            while attempt <= args.retry:
                attempt += 1
                payload = build_payload(model_id, left, right, args.max_tokens)
                status, body = chat_request(base_url, user, pwd, payload, timeout)
                if status == 200:
                    break
                if body.startswith("__transport_error__:") and attempt <= args.retry:
                    last_error = body
                    time.sleep(2)
                    continue
                if 500 <= status < 600 and attempt <= args.retry:
                    last_error = body[:400]
                    time.sleep(3)
                    continue
                break
            duration = round(time.time() - t0, 2)

            (raw_dir / f"{case_id}.txt").write_text(body, encoding="utf-8")

            text, source, parsed_chat = extract_message_text(body)
            diff_json, json_valid = parse_diff_json(text) if status == 200 else (None, False)

            finish_reason = None
            usage = None
            if isinstance(parsed_chat, dict) and parsed_chat:
                try:
                    finish_reason = parsed_chat["choices"][0].get("finish_reason")
                except Exception:
                    pass
                usage = parsed_chat.get("usage")

            ok = status == 200 and bool(text)
            entry = {
                "model": model_id,
                "case_id": case_id,
                "category": case["category"],
                "status": status,
                "ok": ok,
                "duration_sec": duration,
                "attempts": attempt,
                "response_source": source,
                "finish_reason": finish_reason,
                "usage": usage,
                "text_chars": len(text),
                "json_valid": bool(json_valid),
                "diff_json": diff_json,
                "raw_text_preview": text[:600],
                "error_preview": (body[:400] if status != 200 else ""),
                "last_transport_error": last_error,
            }
            results["results"].append(entry)
            flush()
            print(f"  [{model_id}] {case_id} status={status} ok={ok} dur={duration}s "
                  f"json_valid={json_valid} text={len(text)}ch src={source}")

    if args.restore_model:
        ok, msg = ensure_model_loaded(
            base_url, user, pwd, args.restore_model,
            context_length=args.load_context_length,
            max_wait=args.load_wait_sec,
        )
        print(f"  [RESTORE] {args.restore_model}: ok={ok} msg={msg}")
        results["restore_model"] = {"model": args.restore_model, "ok": ok, "message": msg}

    results["finished_at"] = dt.datetime.utcnow().isoformat() + "Z"
    results["wall_time_sec"] = round(time.time() - overall_t0, 2)
    flush()

    # Quick summary.md
    by_model: dict[str, list[dict[str, Any]]] = {}
    for r in results["results"]:
        by_model.setdefault(r["model"], []).append(r)
    lines = [f"# benchmark {run_name}", "", f"- cases: {len(cases)}", f"- models: {len(by_model)}",
             f"- wall_time: {results['wall_time_sec']}s", "", "| model | ok | json_valid | avg_dur_sec |",
             "|---|---|---|---|"]
    for mid, items in by_model.items():
        ok = sum(1 for it in items if it["ok"])
        jv = sum(1 for it in items if it["json_valid"])
        avg = round(sum(it["duration_sec"] for it in items) / len(items), 2)
        lines.append(f"| {mid} | {ok}/{len(items)} | {jv}/{len(items)} | {avg} |")
    (run_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"done: {results_path}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(3)
