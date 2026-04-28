"""
lms_service.py
--------------
Удалённое управление LM Studio через ngrok+Basic Auth (WSS + monkey-patched SDK).

LM Studio Python SDK хардкодит ws:// и не поддерживает Basic Auth, поэтому
монкипатчим httpx_ws.aconnect_ws чтобы:
  - заменить ws:// → wss:// (ngrok-free отдаёт только HTTPS)
  - добавить заголовок Authorization: Basic <token>
  - добавить ngrok-skip-browser-warning

Через .env:
  CHANDRA_BASE_URL — https-URL ngrok (порт LM Studio)
  NGROK_AUTH_USER  — логин Basic Auth
  NGROK_AUTH_PASS  — пароль Basic Auth

Публичный API (синхронный, чтобы вызывать из FastAPI без хитростей):
  list_loaded() -> list[dict]      — текущие загруженные instance'ы
  list_downloaded() -> list[dict]  — все скачанные модели
  load_model(model_key, context_length, identifier?) -> dict
  unload_model(identifier) -> dict
  unload_all_for(model_key) -> int
"""
from __future__ import annotations

import base64
import os
from urllib.parse import urlparse
from typing import Optional

from dotenv import dotenv_values


# ─── Monkey-patch для wss + Basic Auth ────────────────────────────────────

_PATCHED = False


def _ensure_patched() -> None:
    global _PATCHED
    if _PATCHED:
        return

    cfg = dotenv_values(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
    user = cfg.get("NGROK_AUTH_USER") or os.environ.get("NGROK_AUTH_USER")
    pwd = cfg.get("NGROK_AUTH_PASS") or os.environ.get("NGROK_AUTH_PASS")
    if not user or not pwd:
        raise RuntimeError(
            "NGROK_AUTH_USER / NGROK_AUTH_PASS не заданы в .env — управление LM Studio недоступно"
        )
    auth = base64.b64encode(f"{user}:{pwd}".encode()).decode()

    import lmstudio._ws_impl as ws_impl
    import httpx_ws as _httpx_ws

    _orig_aconnect_ws = _httpx_ws.aconnect_ws

    class _AuthedConnect:
        def __init__(self, url, *args, **kwargs):
            if url.startswith("ws://"):
                url = "wss://" + url[5:]
            kwargs.setdefault("headers", {})
            kwargs["headers"]["Authorization"] = f"Basic {auth}"
            kwargs["headers"]["ngrok-skip-browser-warning"] = "true"
            self._cm = _orig_aconnect_ws(url, *args, **kwargs)

        async def __aenter__(self):
            return await self._cm.__aenter__()

        async def __aexit__(self, *args):
            return await self._cm.__aexit__(*args)

    _httpx_ws.aconnect_ws = _AuthedConnect
    ws_impl.aconnect_ws = _AuthedConnect
    _PATCHED = True


def _get_host() -> str:
    cfg = dotenv_values(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
    base_url = cfg.get("CHANDRA_BASE_URL") or os.environ.get("CHANDRA_BASE_URL")
    if not base_url:
        raise RuntimeError("CHANDRA_BASE_URL не задан в .env")
    return urlparse(base_url).netloc


def _client():
    _ensure_patched()
    import lmstudio as lms
    return lms.Client(_get_host())


# ─── Public API ────────────────────────────────────────────────────────────

def _rest_get(path: str, timeout: float = 8.0) -> dict:
    """Дешёвый REST-запрос к LM Studio API через Chandra ngrok (без SDK/WSS)."""
    import httpx
    cfg = dotenv_values(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
    base_url = (cfg.get("CHANDRA_BASE_URL") or os.environ.get("CHANDRA_BASE_URL") or "").rstrip("/")
    user = cfg.get("NGROK_AUTH_USER") or os.environ.get("NGROK_AUTH_USER")
    pwd = cfg.get("NGROK_AUTH_PASS") or os.environ.get("NGROK_AUTH_PASS")
    headers = {"ngrok-skip-browser-warning": "true"}
    auth = (user, pwd) if user and pwd else None
    with httpx.Client(timeout=timeout) as cli:
        r = cli.get(f"{base_url}{path}", headers=headers, auth=auth)
        r.raise_for_status()
        return r.json()


def list_loaded() -> list[dict]:
    """Список загруженных instance'ов через REST `/api/v0/models?state=loaded`.
    Намного быстрее SDK (REST ~1.2с против WSS ~13с) — никакого WS-handshake.
    """
    data = _rest_get("/api/v0/models")
    out: list[dict] = []
    for m in data.get("data", []):
        if (m.get("state") or "").lower() != "loaded":
            continue
        out.append({
            "identifier": m.get("id"),
            "type": m.get("type") or "llm",
            "context_length": m.get("loaded_context_length"),
        })
    return out


_LOADED_CACHE: dict = {"at": 0.0, "result": None}
_LOADED_TTL_S = 15.0


def list_loaded_cached() -> list[dict]:
    """Кешированный list_loaded (~15с TTL). Используется в health-poll, чтобы не дёргать ngrok."""
    import time
    now = time.monotonic()
    if _LOADED_CACHE["result"] is not None and (now - _LOADED_CACHE["at"]) < _LOADED_TTL_S:
        return _LOADED_CACHE["result"]
    result = list_loaded()
    _LOADED_CACHE["at"] = now
    _LOADED_CACHE["result"] = result
    return result


def invalidate_loaded_cache() -> None:
    """Сбросить кеш list_loaded после load/unload — чтобы UI сразу видел изменения."""
    _LOADED_CACHE["at"] = 0.0
    _LOADED_CACHE["result"] = None


def list_downloaded() -> list[dict]:
    """Список всех скачанных моделей (REST `/api/v0/models`)."""
    data = _rest_get("/api/v0/models")
    out: list[dict] = []
    for m in data.get("data", []):
        out.append({
            "id": m.get("id"),
            "type": m.get("type"),
            "publisher": m.get("publisher"),
            "arch": m.get("arch"),
            "quantization": m.get("quantization"),
            "state": m.get("state"),
            "max_context_length": m.get("max_context_length"),
            "loaded_context_length": m.get("loaded_context_length"),
            "capabilities": m.get("capabilities") or [],
        })
    return out


def load_model(
    model_key: str,
    *,
    context_length: int = 16384,
    identifier: Optional[str] = None,
    flash_attention: bool = True,
    keep_model_in_memory: bool = True,
    offload_kv_cache_to_gpu: bool = True,
    gpu_offload_ratio: float = 1.0,  # 1.0 = всё в GPU, 0.0 = всё в CPU
) -> dict:
    """Загрузить модель с заданным contextLength + оптимизациями.

    Параметры (LM Studio):
      flash_attention=True            — ускорение attention в 2-4× (Flash Attn)
      keep_model_in_memory=True       — не выгружать модель из RAM
      offload_kv_cache_to_gpu=True    — KV cache в VRAM (быстро) vs RAM (медленно)
      gpu_offload_ratio=1.0           — доля слоёв на GPU (1.0=все, 0.5=половина)
    """
    client = _client()
    config = {
        "contextLength": int(context_length),
        "flashAttention": bool(flash_attention),
        "keepModelInMemory": bool(keep_model_in_memory),
        "tryMmap": True,
        "useFp16ForKVCache": False,  # Q4 KV cache экономит VRAM при offload
    }
    # GPU offload — варианты названия в SDK разные между версиями
    if gpu_offload_ratio is not None:
        config["gpu"] = {
            "ratio": float(gpu_offload_ratio),
            "offloadKVCacheToGpu": bool(offload_kv_cache_to_gpu),
        }

    handle = client.llm.load_new_instance(
        model_key,
        instance_identifier=identifier,
        config=config,
    )
    try:
        ctx = handle.get_context_length()
    except Exception:
        ctx = context_length
    invalidate_loaded_cache()
    return {
        "identifier": handle.identifier,
        "context_length": ctx,
        "model_key": model_key,
        "flash_attention": flash_attention,
        "offload_kv_cache_to_gpu": offload_kv_cache_to_gpu,
        "gpu_offload_ratio": gpu_offload_ratio,
    }


def unload_model(identifier: str) -> dict:
    """Выгрузить конкретный instance по identifier."""
    client = _client()
    # Найти handle через list_loaded
    target = None
    for handle in client.llm.list_loaded():
        if handle.identifier == identifier:
            target = handle
            break
    if target is None:
        # Возможно embedding
        try:
            for handle in client.embedding.list_loaded():
                if handle.identifier == identifier:
                    target = handle
                    break
        except Exception:
            pass
    if target is None:
        return {"unloaded": False, "error": f"identifier '{identifier}' not found"}
    target.unload()
    invalidate_loaded_cache()
    return {"unloaded": True, "identifier": identifier}


def unload_all_for(model_key: str) -> int:
    """Выгрузить ВСЕ instance'ы модели. Возвращает количество выгруженных."""
    client = _client()
    count = 0
    for handle in client.llm.list_loaded():
        if handle.identifier == model_key or handle.identifier.startswith(f"{model_key}:"):
            try:
                handle.unload()
                count += 1
            except Exception:
                pass
    if count > 0:
        invalidate_loaded_cache()
    return count


_HEALTH_CACHE: dict = {"at": 0.0, "result": None}
_HEALTH_TTL_S = 20.0  # не чаще раза в 20 сек реальный пинг


def health_check_cached(model_id: Optional[str] = None) -> dict:
    """Кешированный health_check — не дрочит Qwen чаще раза в _HEALTH_TTL_S."""
    import time
    now = time.monotonic()
    if _HEALTH_CACHE["result"] is not None and (now - _HEALTH_CACHE["at"]) < _HEALTH_TTL_S:
        return _HEALTH_CACHE["result"]
    result = health_check(model_id)
    _HEALTH_CACHE["at"] = now
    _HEALTH_CACHE["result"] = result
    return result


def health_check(model_id: Optional[str] = None, timeout_s: float = 10.0) -> dict:
    """Ping загруженной модели — отправить минимальный запрос и замерить latency.

    Если model_id не указан — берём первую загруженную LLM.
    Возвращает {alive, model, latency_ms, error?}.
    """
    import time
    import httpx

    cfg = dotenv_values(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
    base_url = (cfg.get("CHANDRA_BASE_URL") or os.environ.get("CHANDRA_BASE_URL") or "").rstrip("/")
    user = cfg.get("NGROK_AUTH_USER") or os.environ.get("NGROK_AUTH_USER")
    pwd = cfg.get("NGROK_AUTH_PASS") or os.environ.get("NGROK_AUTH_PASS")
    headers = {"ngrok-skip-browser-warning": "true", "Content-Type": "application/json"}
    auth = (user, pwd) if user and pwd else None

    target = model_id
    if not target:
        try:
            loaded = list_loaded()
            if not loaded:
                return {"alive": False, "model": None, "latency_ms": None, "error": "Нет загруженных моделей"}
            target = loaded[0]["identifier"]
        except Exception as e:
            return {"alive": False, "model": None, "latency_ms": None, "error": f"list_loaded failed: {e}"}

    payload = {
        "model": target,
        "messages": [{"role": "user", "content": "ok"}],
        "max_tokens": 1,
        "temperature": 0,
    }
    started = time.monotonic()
    try:
        with httpx.Client(timeout=timeout_s) as cli:
            r = cli.post(f"{base_url}/v1/chat/completions", json=payload, headers=headers, auth=auth)
            latency_ms = int((time.monotonic() - started) * 1000)
            if r.status_code == 200:
                return {"alive": True, "model": target, "latency_ms": latency_ms}
            return {
                "alive": False, "model": target, "latency_ms": latency_ms,
                "error": f"HTTP {r.status_code}: {r.text[:200]}",
            }
    except Exception as e:
        latency_ms = int((time.monotonic() - started) * 1000)
        return {"alive": False, "model": target, "latency_ms": latency_ms, "error": str(e)[:200]}


def get_inflight_count() -> dict:
    """Сколько активных запросов сейчас на стороне webapp:
    - prepare-data jobs (Qwen enrichment)
    - audit jobs если используют local LLM (опционально)
    Не лезем в LM Studio — это локальный счётчик активности нашей системы.
    """
    try:
        from webapp.services.prepare_service import prepare_state
        prepare_running = sum(
            1 for it in prepare_state.queue_status.items
            if it.status == "running"
        )
        prepare_pending = sum(
            1 for it in prepare_state.queue_status.items
            if it.status == "pending"
        )
    except Exception:
        prepare_running = 0
        prepare_pending = 0

    return {
        "prepare_running": prepare_running,
        "prepare_pending": prepare_pending,
        "total_active": prepare_running + prepare_pending,
    }
