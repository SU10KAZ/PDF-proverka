"""Small local OpenAI-compatible vision client shared by offline tools.

The client accepts one PNG and one prompt.  It deliberately has no document
comparison prompt, diff schema, continuation protocol, jobs, or persistence.
"""
from __future__ import annotations

import base64
import io
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from backend.app.core.config import _normalize_local_base_url


_BLOCKED_EXTERNAL_HOSTS = {
    "openrouter.ai", "api.openrouter.ai", "api.openai.com", "openai.com",
    "anthropic.com", "api.anthropic.com", "generativelanguage.googleapis.com",
}


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.environ.get(name) or "").strip() or default)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float((os.environ.get(name) or "").strip() or default)
    except ValueError:
        return default


@dataclass(frozen=True)
class LocalVisionConfig:
    base_url: str
    model: str
    temperature: float
    max_tokens: int
    timeout_sec: int
    image_long_side: int
    auth: str
    basic_user: str
    basic_pass: str
    bearer_token: str


@dataclass
class LocalVisionResult:
    status: str
    model_used: str
    parsed: dict[str, Any] | None = None
    full_raw_response: str = ""
    raw_response_excerpt: str = ""
    error: str = ""


def load_local_vision_config() -> LocalVisionConfig:
    return LocalVisionConfig(
        base_url=_normalize_local_base_url(
            os.environ.get("EVIDENCE_LOCAL_VISION_BASE_URL")
            or os.environ.get("CHANDRA_BASE_URL")
            or os.environ.get("LMSTUDIO_BASE_URL", "")
        ),
        model=(os.environ.get("EVIDENCE_LOCAL_VISION_MODEL") or "").strip(),
        temperature=_env_float("EVIDENCE_LOCAL_VISION_TEMPERATURE", 0.0),
        max_tokens=max(1, _env_int("EVIDENCE_LOCAL_VISION_MAX_TOKENS", 6000)),
        timeout_sec=max(1, _env_int("EVIDENCE_LOCAL_VISION_TIMEOUT_SEC", 300)),
        image_long_side=max(64, _env_int("EVIDENCE_LOCAL_VISION_IMAGE_LONG_SIDE", 1100)),
        auth=(os.environ.get("EVIDENCE_LOCAL_VISION_AUTH") or "basic").strip().lower(),
        basic_user=os.environ.get("NGROK_AUTH_USER", ""),
        basic_pass=os.environ.get("NGROK_AUTH_PASS", ""),
        bearer_token=(
            os.environ.get("EVIDENCE_LOCAL_VISION_BEARER_TOKEN")
            or os.environ.get("CHANDRA_BEARER_TOKEN")
            or os.environ.get("LMSTUDIO_API_KEY", "")
        ),
    )


def _validate_config(cfg: LocalVisionConfig, model: str) -> str:
    if not cfg.base_url:
        return "missing_base_url"
    parsed = urlparse(cfg.base_url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or not host:
        return "invalid_base_url"
    if any(host == item or host.endswith("." + item) for item in _BLOCKED_EXTERNAL_HOSTS):
        return f"external_paid_host_blocked:{host}"
    if not model:
        return "missing_model"
    if cfg.auth == "basic" and not (cfg.basic_user and cfg.basic_pass):
        return "basic_auth_credentials_missing"
    if cfg.auth == "bearer" and not cfg.bearer_token:
        return "bearer_auth_credentials_missing"
    return ""


def _resize_png_to_long_side(path: Path, long_side: int) -> bytes:
    from PIL import Image

    raw = path.read_bytes()
    with Image.open(io.BytesIO(raw)) as image:
        image.load()
        current = max(image.size)
        if current <= long_side:
            return raw
        scale = long_side / float(current)
        size = (max(1, round(image.width * scale)), max(1, round(image.height * scale)))
        resized = image.convert("RGB").resize(size, Image.Resampling.LANCZOS)
        output = io.BytesIO()
        resized.save(output, format="PNG", optimize=True)
        return output.getvalue()


def _png_bytes_to_data_url(data: bytes) -> str:
    return "data:image/png;base64," + base64.b64encode(data).decode("ascii")


def _build_headers(cfg: LocalVisionConfig) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if cfg.auth == "bearer":
        headers["Authorization"] = f"Bearer {cfg.bearer_token}"
    elif cfg.auth == "basic":
        token = base64.b64encode(f"{cfg.basic_user}:{cfg.basic_pass}".encode()).decode()
        headers.update({"Authorization": f"Basic {token}", "ngrok-skip-browser-warning": "true"})
    return headers


def _json_object(text: str) -> dict[str, Any] | None:
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.I | re.S)
    try:
        value = json.loads(cleaned)
        return value if isinstance(value, dict) else None
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        for index, char in enumerate(cleaned):
            if char != "{":
                continue
            try:
                value, _ = decoder.raw_decode(cleaned[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                return value
    return None


async def snapshot_loaded_models(cfg: LocalVisionConfig | None = None) -> list[dict[str, Any]]:
    cfg = cfg or load_local_vision_config()
    if not cfg.base_url:
        return []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(f"{cfg.base_url}/api/v1/models", headers=_build_headers(cfg))
        response.raise_for_status()
        data = response.json()
    except Exception:
        return []
    loaded = []
    for model in data.get("models", []) if isinstance(data, dict) else []:
        for instance in model.get("loaded_instances") or []:
            loaded.append({
                "model_key": model.get("key"),
                "instance_id": instance.get("id"),
                "config": dict(instance.get("config") or {}),
            })
    return loaded


async def describe_image_local(
    image_path: str | Path,
    prompt: str,
    *,
    model: str | None = None,
    cfg: LocalVisionConfig | None = None,
) -> LocalVisionResult:
    cfg = cfg or load_local_vision_config()
    selected_model = (model or cfg.model).strip()
    error = _validate_config(cfg, selected_model)
    if error:
        return LocalVisionResult("provider_unavailable", selected_model, error=error)
    if not prompt.strip():
        return LocalVisionResult("error", selected_model, error="empty_prompt")
    try:
        image_url = _png_bytes_to_data_url(
            _resize_png_to_long_side(Path(image_path), cfg.image_long_side)
        )
        payload = {
            "model": selected_model,
            "max_tokens": cfg.max_tokens,
            "temperature": cfg.temperature,
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": image_url}},
            ]}],
        }
        async with httpx.AsyncClient(timeout=cfg.timeout_sec) as client:
            response = await client.post(
                f"{cfg.base_url}/v1/chat/completions",
                headers=_build_headers(cfg),
                json=payload,
            )
        response.raise_for_status()
        message = response.json()["choices"][0]["message"]
        text = str(message.get("content") or message.get("reasoning_content") or "").strip()
        parsed = _json_object(text)
        return LocalVisionResult(
            "done" if parsed is not None else "invalid_json",
            selected_model,
            parsed=parsed,
            full_raw_response=text,
            raw_response_excerpt=text[:4000],
            error="" if parsed is not None else "json_parse_failed",
        )
    except Exception as exc:
        return LocalVisionResult("error", selected_model, error=f"{type(exc).__name__}:{exc}")


__all__ = [
    "LocalVisionConfig", "LocalVisionResult", "load_local_vision_config",
    "describe_image_local", "snapshot_loaded_models",
]
