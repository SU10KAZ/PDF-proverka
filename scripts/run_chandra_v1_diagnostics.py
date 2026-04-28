#!/usr/bin/env python3
"""Run controlled LM Studio /api/v1 Chandra diagnostics.

This is intentionally separate from production stage-02 code. It loads one
model at a time, disables reasoning where the native API allows it, and tests
small vision inputs before attempting larger ones.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.run_chandra_vision_model_eval import (  # noqa: E402
    _load_blocks_index,
    _prepare_images,
    _safe_model_dir,
    _ts,
)


DEFAULT_MODELS = [
    "google/gemma-4-31b",
    "qwen/qwen3.6-35b-a3b",
    "qwen/qwen3.6-27b",
]

DEFAULT_AUDIT_BLOCKS = [
    "3TFL-TNRF-7G6",
    "66NE-7DYY-GQN",
    "9DH3-MEUR-DR4",
    "947C-9UJT-RYU",
    "9PPJ-YP6U-HV6",
    "97AH-VUFP-LJJ",
    "4CFG-LM4Y-7H4",
    "66M4-Y69W-VCG",
    "6PPA-4DDX-6FR",
    "6R7N-7LRD-AUR",
    "7DJ9-EQQ3-QMK",
    "6P9T-Q7GT-N9J",
]


@dataclass
class ApiResult:
    ok: bool
    status_code: int | None
    elapsed_s: float
    error: str | None
    response: dict[str, Any] | None


def _save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def _client_config() -> tuple[str, tuple[str, str], dict[str, str]]:
    load_dotenv(ROOT / ".env")
    base = os.environ.get("CHANDRA_BASE_URL", "").rstrip("/")
    user = os.environ.get("NGROK_AUTH_USER", "")
    password = os.environ.get("NGROK_AUTH_PASS", "")
    missing = [name for name, value in {
        "CHANDRA_BASE_URL": base,
        "NGROK_AUTH_USER": user,
        "NGROK_AUTH_PASS": password,
    }.items() if not value]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")
    return base, (user, password), {"ngrok-skip-browser-warning": "true"}


def _request_json(
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    timeout: float,
) -> ApiResult:
    base, auth, base_headers = _client_config()
    headers = dict(base_headers)
    if json_body is not None:
        headers["content-type"] = "application/json"
    started = time.perf_counter()
    try:
        response = requests.request(
            method,
            f"{base}{path}",
            auth=auth,
            headers=headers,
            json=json_body,
            timeout=timeout,
        )
        elapsed = time.perf_counter() - started
        try:
            payload = response.json()
        except Exception:
            payload = {"raw_text": response.text[:4000]}
        return ApiResult(
            ok=200 <= response.status_code < 300 and not (isinstance(payload, dict) and payload.get("error")),
            status_code=response.status_code,
            elapsed_s=round(elapsed, 3),
            error=(payload.get("error") if isinstance(payload, dict) else None),
            response=payload,
        )
    except Exception as exc:
        elapsed = time.perf_counter() - started
        return ApiResult(
            ok=False,
            status_code=None,
            elapsed_s=round(elapsed, 3),
            error=f"{type(exc).__name__}: {exc}",
            response=None,
        )


def _list_models() -> list[dict[str, Any]]:
    result = _request_json("GET", "/api/v1/models", timeout=60)
    if not result.ok:
        raise RuntimeError(f"Failed to list models: {result.error or result.response}")
    return list((result.response or {}).get("models", []))


def _loaded_instances() -> list[tuple[str, str]]:
    instances: list[tuple[str, str]] = []
    for model in _list_models():
        key = model.get("key") or model.get("id") or ""
        for instance in model.get("loaded_instances") or []:
            instance_id = instance.get("id")
            if key and instance_id:
                instances.append((str(key), str(instance_id)))
    return instances


def _unload_all(exp_dir: Path, reason: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, instance_id in _loaded_instances():
        result = _request_json(
            "POST",
            "/api/v1/models/unload",
            json_body={"instance_id": instance_id},
            timeout=180,
        )
        row = {"reason": reason, "model": key, "instance_id": instance_id, **asdict(result)}
        rows.append(row)
    if rows:
        _save_json(exp_dir / "unload_events.json", rows)
    return rows


def _load_model(model: str, *, context_length: int, timeout: float) -> ApiResult:
    body = {
        "model": model,
        "context_length": context_length,
        "flash_attention": True,
        "offload_kv_cache_to_gpu": True,
        "echo_load_config": True,
    }
    return _request_json("POST", "/api/v1/models/load", json_body=body, timeout=timeout)


def _extract_output(response: dict[str, Any] | None) -> tuple[str, str]:
    if not isinstance(response, dict):
        return "", ""
    messages: list[str] = []
    reasoning: list[str] = []
    for item in response.get("output") or []:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "message":
            messages.append(str(item.get("content") or ""))
        elif item.get("type") == "reasoning":
            reasoning.append(str(item.get("content") or ""))
    return "\n".join(m for m in messages if m).strip(), "\n".join(r for r in reasoning if r).strip()


def _chat_text(model: str, timeout: float) -> dict[str, Any]:
    result = _request_json(
        "POST",
        "/api/v1/chat",
        json_body={
            "model": model,
            "input": "Напиши ровно одно слово: OK",
            "system_prompt": "Отвечай только финальным ответом. Не выводи рассуждения.",
            "temperature": 0,
            "max_output_tokens": 256,
            "reasoning": "off",
            "store": False,
        },
        timeout=timeout,
    )
    content, reasoning = _extract_output(result.response)
    return {"request": "text_smoke", **asdict(result), "content": content, "reasoning": reasoning}


def _data_url(path: Path) -> str:
    return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode()


def _chat_image(model: str, image_path: Path, prompt: str, timeout: float, max_output_tokens: int = 900) -> dict[str, Any]:
    result = _request_json(
        "POST",
        "/api/v1/chat",
        json_body={
            "model": model,
            "input": [
                {"type": "text", "content": prompt},
                {"type": "image", "data_url": _data_url(image_path)},
            ],
            "system_prompt": "Ты анализируешь строительные чертежи. Отвечай только финальным ответом, без рассуждений.",
            "temperature": 0,
            "max_output_tokens": max_output_tokens,
            "reasoning": "off",
            "store": False,
        },
        timeout=timeout,
    )
    content, reasoning = _extract_output(result.response)
    return {
        "request": "image_smoke",
        "image": str(image_path),
        "image_size_bytes": image_path.stat().st_size,
        **asdict(result),
        "content": content,
        "reasoning": reasoning,
    }


def _prepare_test_images(project_dir: Path, exp_dir: Path) -> dict[str, dict[str, Any]]:
    blocks = _load_blocks_index(project_dir)
    specs = [
        ("light_512", "A9M4-EJ7R-MKH", 512),
        ("light_1024", "A9M4-EJ7R-MKH", 1024),
        ("heavy_512", "3TFL-TNRF-7G6", 512),
    ]
    out: dict[str, dict[str, Any]] = {}
    for name, block_id, max_side in specs:
        subdir = exp_dir / "prepared_images" / name
        images = _prepare_images(
            project_dir=project_dir,
            exp_dir=subdir,
            blocks=blocks,
            block_ids=[block_id],
            native_crops=True,
            native_max_long_side=max_side,
        )
        image = images[0]
        out[name] = {
            "name": name,
            "block_id": block_id,
            "max_side": max_side,
            "image_path": str(image.image_file),
            "width": image.width,
            "height": image.height,
            "size_kb": image.size_kb,
            "label": image.label,
        }
    return out


def _prepare_audit_images(project_dir: Path, exp_dir: Path, block_ids: list[str], max_side: int) -> dict[str, dict[str, Any]]:
    blocks = _load_blocks_index(project_dir)
    images = _prepare_images(
        project_dir=project_dir,
        exp_dir=exp_dir / "audit_images",
        blocks=blocks,
        block_ids=block_ids,
        native_crops=True,
        native_max_long_side=max_side,
    )
    out: dict[str, dict[str, Any]] = {}
    for image in images:
        out[image.block_id] = {
            "name": image.block_id,
            "block_id": image.block_id,
            "max_side": max_side,
            "image_path": str(image.image_file),
            "width": image.width,
            "height": image.height,
            "size_kb": image.size_kb,
            "label": image.label,
        }
    return out


def _verdict(row: dict[str, Any]) -> str:
    if not row.get("ok"):
        return "fail"
    content = (row.get("content") or "").strip()
    if row.get("request") == "text_smoke":
        return "pass" if "OK" in content else "weak"
    if len(content) < 40:
        return "empty_or_too_short"
    lower = content.lower()
    if "черт" in lower or "план" in lower or "схем" in lower or "конструк" in lower:
        return "pass"
    return "weak"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-dir", required=True)
    ap.add_argument("--models", nargs="+", default=DEFAULT_MODELS)
    ap.add_argument("--context-length", type=int, default=8192)
    ap.add_argument("--load-timeout", type=float, default=900)
    ap.add_argument("--chat-timeout", type=float, default=480)
    ap.add_argument("--stop-after-first-working", action="store_true")
    ap.add_argument("--audit", action="store_true")
    ap.add_argument("--audit-max-side", type=int, default=1024)
    args = ap.parse_args()

    project_dir = Path(args.project_dir).resolve()
    exp_dir = project_dir / "_experiments" / "chandra_v1_diagnostics" / _ts()
    exp_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "project_dir": str(project_dir),
        "models": args.models,
        "context_length": args.context_length,
        "chat_api": "/api/v1/chat",
        "load_api": "/api/v1/models/load",
        "reasoning": "off",
    }
    _save_json(exp_dir / "manifest.json", manifest)

    images = _prepare_audit_images(project_dir, exp_dir, DEFAULT_AUDIT_BLOCKS, args.audit_max_side) if args.audit else _prepare_test_images(project_dir, exp_dir)
    _save_json(exp_dir / "prepared_images.json", images)

    rows: list[dict[str, Any]] = []
    _unload_all(exp_dir, "initial_cleanup")

    prompt = (
        "Кратко опиши, что видно на этом фрагменте строительного чертежа. "
        "Укажи тип схемы/плана, основные элементы и 2-3 читаемые детали. "
        "Если текст не читается, так и скажи."
    )

    try:
        for model in args.models:
            print(f"\n=== MODEL {model} ===", flush=True)
            model_dir = exp_dir / "models" / _safe_model_dir(model)
            model_dir.mkdir(parents=True, exist_ok=True)

            _unload_all(exp_dir, f"before_{model}")
            load_result = _load_model(model, context_length=args.context_length, timeout=args.load_timeout)
            load_row = {"model": model, "request": "load", **asdict(load_result)}
            rows.append(load_row)
            _save_json(model_dir / "load.json", load_row)
            print(f"load ok={load_result.ok} elapsed={load_result.elapsed_s}s error={load_result.error}", flush=True)
            if not load_result.ok:
                continue

            text_row = {"model": model, **_chat_text(model, timeout=args.chat_timeout)}
            text_row["verdict"] = _verdict(text_row)
            rows.append(text_row)
            _save_json(model_dir / "text_smoke.json", text_row)
            print(f"text {text_row['verdict']} elapsed={text_row['elapsed_s']}s content={text_row.get('content','')[:80]!r}", flush=True)
            if text_row["verdict"] == "fail":
                continue

            working = False
            image_names = list(images) if args.audit else ["light_512", "light_1024", "heavy_512"]
            for image_name in image_names:
                image_info = images[image_name]
                image_row = {
                    "model": model,
                    "image_name": image_name,
                    "block_id": image_info["block_id"],
                    "width": image_info["width"],
                    "height": image_info["height"],
                    **_chat_image(model, Path(image_info["image_path"]), prompt, timeout=args.chat_timeout),
                }
                image_row["verdict"] = _verdict(image_row)
                rows.append(image_row)
                _save_json(model_dir / f"{image_name}.json", image_row)
                print(
                    f"{image_name} {image_row['verdict']} elapsed={image_row['elapsed_s']}s "
                    f"content_len={len(image_row.get('content') or '')}",
                    flush=True,
                )
                if not args.audit and image_name == "light_512" and image_row["verdict"] == "fail":
                    break
                if image_row["verdict"] == "pass":
                    working = True

            if args.stop_after_first_working and working:
                break
    finally:
        _unload_all(exp_dir, "final_cleanup")

    _save_json(exp_dir / "results.json", rows)

    lines = [
        "# Chandra V1 Diagnostics",
        "",
        f"- Project: `{project_dir}`",
        f"- API: `/api/v1/chat` with `reasoning=off`",
        f"- Context length: `{args.context_length}`",
        "",
        "## Prepared Images",
        "",
        "| Name | Block | Size | File KB |",
        "|---|---|---:|---:|",
    ]
    for item in images.values():
        lines.append(f"| `{item['name']}` | `{item['block_id']}` | {item['width']}x{item['height']} | {item['size_kb']} |")
    lines.extend(["", "## Results", "", "| Model | Test | Verdict | OK | Elapsed s | Content excerpt |", "|---|---|---|---:|---:|---|"])
    for row in rows:
        if row.get("request") == "load":
            test = "load"
        elif row.get("request") == "text_smoke":
            test = "text"
        else:
            test = row.get("image_name", row.get("request", "unknown"))
        excerpt_value = row.get("content") or row.get("error") or ""
        if not isinstance(excerpt_value, str):
            excerpt_value = json.dumps(excerpt_value, ensure_ascii=False)
        excerpt = excerpt_value[:120].replace("\n", " ")
        lines.append(
            f"| `{row.get('model')}` | `{test}` | `{row.get('verdict', '')}` | "
            f"{row.get('ok')} | {row.get('elapsed_s')} | {excerpt} |"
        )
    (exp_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"\nArtifacts: {exp_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
