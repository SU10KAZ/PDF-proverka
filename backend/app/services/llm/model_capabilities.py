"""Возможности локальных моделей LM Studio (01.vibe) + статус через OpenAI /v1.

Новый удалённый сервер (bearer) отдаёт только OpenAI-совместимый слой:
`GET /v1/models` и `POST /v1/chat/completions`. Нативное управление LM Studio
(`/api/v0|v1/models`, `ws://…/llm`, `lms CLI`, локальный `nvidia-smi`/процессы)
на нём НЕдоступно — сервер сам решает, какую модель держать в памяти.

Этот модуль:
  * даёт карту возможностей моделей (vision/kind/reasoning), измеренную пробой;
  * строит честный статус страницы «Контроль моделей» из того, что реально
    отдаёт /v1 (список моделей + health-probe), без нативных заглушек.
"""
from __future__ import annotations

import os
import time
from typing import Any

import requests

from backend.app.core.config import ROOT_DIR, _normalize_local_base_url

# Возможности локальных моделей LM Studio 01.vibe — измерено пробой 2026-07-02.
# vision — измерено реальным vision-запросом; kind/reasoning — по id + поведению пробы.
# Новые (неизвестные) модели классифицируются эвристикой _classify_fallback().
MEASURED_CAPABILITIES: dict[str, dict[str, Any]] = {
    "agents-a1": {"vision": True, "kind": "vlm", "reasoning": True},
    "baidu.unlimited-ocr": {"vision": True, "kind": "ocr", "reasoning": False},
    "chandra-ocr-2": {"vision": True, "kind": "ocr", "reasoning": False},
    "gemma-4-12b-coder-fable5-composer2.5-v1": {"vision": False, "kind": "coder", "reasoning": False},
    "gemma-4-31b-it": {"vision": False, "kind": "llm", "reasoning": False},
    "google/gemma-4-12b": {"vision": True, "kind": "vlm", "reasoning": False},
    "google/gemma-4-26b-a4b": {"vision": True, "kind": "vlm", "reasoning": True},
    "google/gemma-4-31b": {"vision": True, "kind": "vlm", "reasoning": False},
    "google/gemma-4-31b-qat": {"vision": True, "kind": "vlm", "reasoning": False},
    "huihui-qwen3.5-35b-a3b-abliterated-i1": {"vision": False, "kind": "llm", "reasoning": False},
    "infinity-parser2-flash": {"vision": True, "kind": "ocr", "reasoning": False},
    "lift": {"vision": True, "kind": "vlm", "reasoning": True},
    "minimax/minimax-m2.7": {"vision": False, "kind": "llm", "reasoning": False},
    "nemotron-3.5-asr-streaming-0.6b": {"vision": False, "kind": "asr", "reasoning": False},
    "nuextract3": {"vision": True, "kind": "ocr", "reasoning": False},
    "paddleocr-vl-1.6": {"vision": False, "kind": "ocr", "reasoning": False},
    "qwen-agentworld-35b-a3b": {"vision": False, "kind": "llm", "reasoning": False},
    "qwen/qwen3.5-35b-a3b": {"vision": True, "kind": "vlm", "reasoning": True},
    "qwen/qwen3.5-9b": {"vision": True, "kind": "vlm", "reasoning": True},
    "qwen/qwen3.6-27b": {"vision": True, "kind": "vlm", "reasoning": True},
    "qwen/qwen3.6-35b-a3b": {"vision": True, "kind": "vlm", "reasoning": True},
    "qwen3-vl-reranker-8b": {"vision": True, "kind": "reranker", "reasoning": False},
    "qwen3.5-27b": {"vision": True, "kind": "vlm", "reasoning": True},
    "qwen3.5-27b-claude-4.6-opus-reasoning-distilled-v2": {"vision": True, "kind": "vlm", "reasoning": True},
    "qwen3.6-27b-mtp@q4_k_s": {"vision": True, "kind": "vlm", "reasoning": True},
    "qwen3.6-27b-mtp@q6_k": {"vision": True, "kind": "vlm", "reasoning": True},
    "qwen3.6-35b-a3b-mtp": {"vision": True, "kind": "vlm", "reasoning": True},
    "qwen3.6-35b-a3b-uncensored-hauhaucs-aggressive": {"vision": True, "kind": "vlm", "reasoning": True},
    "text-embedding-nomic-embed-text-v1.5": {"vision": False, "kind": "embedding", "reasoning": False},
    "text-embedding-qwen3-embedding-8b": {"vision": False, "kind": "embedding", "reasoning": False},
    "unlimited-ocr": {"vision": True, "kind": "ocr", "reasoning": False},
}


def _classify_fallback(model_id: str) -> dict[str, Any]:
    """Эвристика для моделей, которых нет в измеренной карте (новые id)."""
    m = model_id.lower()
    if "embed" in m:
        return {"vision": False, "kind": "embedding", "reasoning": False}
    if "asr" in m:
        return {"vision": False, "kind": "asr", "reasoning": False}
    if "reranker" in m:
        return {"vision": True, "kind": "reranker", "reasoning": False}
    if any(t in m for t in ("ocr", "parser", "nuextract", "paddle", "chandra", "unlimited")):
        return {"vision": True, "kind": "ocr", "reasoning": False}
    if "coder" in m or "composer" in m:
        return {"vision": False, "kind": "coder", "reasoning": False}
    reasoning = any(t in m for t in ("a4b", "qwen3.5", "qwen3.6", "reasoning", "distilled"))
    vision = any(t in m for t in ("gemma-4", "qwen3.5", "qwen3.6", "-vl", "gemma-3"))
    return {"vision": vision, "kind": "vlm" if vision else "unknown", "reasoning": reasoning and vision}


def capabilities_for(model_id: str) -> dict[str, Any]:
    caps = MEASURED_CAPABILITIES.get(model_id)
    if caps is not None:
        return {**caps, "measured": True}
    return {**_classify_fallback(model_id), "measured": False}


def _bearer_config() -> tuple[str, str, str]:
    """Возвращает (base_url без /v1, bearer-token, auth_mode)."""
    base = _normalize_local_base_url(
        os.environ.get("CHANDRA_BASE_URL") or os.environ.get("LMSTUDIO_BASE_URL", "")
    )
    token = os.environ.get("CHANDRA_BEARER_TOKEN") or os.environ.get("LMSTUDIO_API_KEY", "")
    mode = (os.environ.get("CHANDRA_AUTH_MODE", "basic") or "basic").strip().lower()
    return base, token, mode


def _headers(token: str, mode: str) -> dict[str, str]:
    h = {"ngrok-skip-browser-warning": "true"}
    if mode == "bearer" and token:
        h["Authorization"] = f"Bearer {token}"
    elif mode == "basic":
        user = os.environ.get("NGROK_AUTH_USER", "")
        pwd = os.environ.get("NGROK_AUTH_PASS", "")
        if user and pwd:
            import base64
            h["Authorization"] = "Basic " + base64.b64encode(f"{user}:{pwd}".encode()).decode()
    return h


def _pipeline_usage() -> dict[str, str]:
    """Какие модели использует пайплайн (id -> роль)."""
    usage: dict[str, str] = {}
    gemma = os.environ.get("CHANDRA_GEMMA_MODEL", "google/gemma-4-26b-a4b")
    usage[gemma] = "enrichment"
    g_model = os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "").strip()
    g_fb = os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_FALLBACK_MODEL", "").strip()
    if g_model:
        usage[g_model] = "graphic (сравнение стадий)"
    if g_fb:
        usage[g_fb] = usage.get(g_fb, "") and usage[g_fb] + ", graphic-fallback" or "graphic-fallback"
    return usage


def _fetch_native_state(base: str, token: str, mode: str) -> dict[str, dict[str, Any]]:
    """Загруженное состояние моделей через native `/api/v0/models` (bearer).

    ВАЖНО: native REST на 01.vibe РАБОТАЕТ с bearer (старый код бил его basic →
    401). Отдаёт поле `state` (loaded/not-loaded) + capabilities + контекст.
    Fail-soft: при любой ошибке возвращает {} (страница деградирует на /v1/models).
    Возвращает {model_id: {state, loaded, max_context, loaded_context, vision}}.
    """
    out: dict[str, dict[str, Any]] = {}
    try:
        resp = requests.get(
            f"{base}/api/v0/models", headers=_headers(token, mode), timeout=15
        )
        if resp.status_code != 200:
            return out
        for m in resp.json().get("data", []):
            if not isinstance(m, dict) or not m.get("id"):
                continue
            state = m.get("state")
            caps = m.get("capabilities") or []
            vision = ("vision" in caps) if isinstance(caps, (list, tuple)) else bool(
                isinstance(caps, dict) and caps.get("vision")
            )
            out[m["id"]] = {
                "state": state,
                "loaded": state in ("loaded", "loaded-idle", "loaded_idle"),
                "max_context": m.get("max_context_length"),
                "loaded_context": m.get("loaded_context_length"),
                "vision_native": vision,
                "quantization": m.get("quantization"),
            }
    except Exception:  # noqa: BLE001 — fail-soft, native опционален
        return {}
    return out


def get_remote_models_status() -> dict[str, Any]:
    """Честный статус для страницы «Контроль моделей» на удалённом bearer-сервере.

    Ходит ТОЛЬКО в OpenAI-слой `/v1/models`. Нативное управление помечается
    недоступным. Ресурсы host'а (RAM/CPU) — это audit-сервер, а НЕ LLM-хост.
    """
    base, token, mode = _bearer_config()
    endpoint = f"{base}/v1" if base else ""
    result: dict[str, Any] = {
        "ok": False,
        "endpoint": endpoint,
        "auth_mode": mode,
        "remote_bearer": mode == "bearer",
        "native_management_available": False,
        "native_management_note": (
            "Нативное управление (загрузка/выгрузка/reload, оценка памяти, GPU/VRAM, "
            "процессы LM Studio) недоступно на удалённом bearer-сервере — сервер сам "
            "управляет моделями в памяти. Доступен только OpenAI /v1."
        ),
        "models": [],
        "model_count": 0,
        "vision_count": 0,
        "health": {"alive": False, "latency_ms": None, "error": None},
        "pipeline_usage": _pipeline_usage(),
        "error": None,
    }
    if not base:
        result["error"] = "CHANDRA_BASE_URL / LMSTUDIO_BASE_URL не заданы"
        return result
    if mode != "bearer":
        result["error"] = f"CHANDRA_AUTH_MODE={mode} (ожидается bearer для 01.vibe)"

    started = time.perf_counter()
    try:
        resp = requests.get(
            f"{base}/v1/models", headers=_headers(token, mode), timeout=20
        )
        latency = round((time.perf_counter() - started) * 1000)
        if resp.status_code != 200:
            result["health"] = {"alive": False, "latency_ms": latency,
                                "error": f"HTTP {resp.status_code}"}
            result["error"] = f"/v1/models → HTTP {resp.status_code}"
            return result
        data = resp.json()
        ids = [m.get("id") for m in data.get("data", []) if isinstance(m, dict) and m.get("id")]
        usage = result["pipeline_usage"]
        # Native loaded-state (bearer) — что реально держится в VRAM прямо сейчас.
        native = _fetch_native_state(base, token, mode)
        models = []
        for mid in sorted(ids):
            caps = capabilities_for(mid)
            nst = native.get(mid) or {}
            models.append({
                "id": mid,
                "vision": caps["vision"],
                "kind": caps["kind"],
                "reasoning": caps["reasoning"],
                "measured": caps["measured"],
                "used_by": usage.get(mid),
                "loaded": bool(nst.get("loaded")),
                "state": nst.get("state"),
                "max_context": nst.get("max_context"),
                "loaded_context": nst.get("loaded_context"),
            })
        loaded_models = [m for m in models if m["loaded"]]
        result.update({
            "ok": True,
            "models": models,
            "model_count": len(models),
            "vision_count": sum(1 for m in models if m["vision"]),
            "loaded_models": loaded_models,
            "loaded_count": len(loaded_models),
            "native_state_available": bool(native),
            "health": {"alive": True, "latency_ms": latency, "error": None},
        })
        return result
    except Exception as exc:  # noqa: BLE001
        result["health"] = {
            "alive": False,
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "error": f"{type(exc).__name__}: {exc}",
        }
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result
