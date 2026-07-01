"""Local OpenAI-compatible vision LLM provider for stage-comparison graphic diff.

Provider key: ``local_openai_compatible``.

Используется только локальный LM Studio endpoint через ngrok + Basic Auth.
Никаких внешних платных API (openrouter / openai / anthropic / gemini) —
URL'ы внешних провайдеров явно отвергаются на этапе валидации конфигурации.

Конфигурация через env (см. .env.example):

  STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER=local_openai_compatible
  STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL=https://<ngrok>.ngrok-free.dev
  STAGE_COMPARISON_GRAPHIC_LLM_MODEL=qwen/qwen3.6-35b-a3b
  STAGE_COMPARISON_GRAPHIC_LLM_FALLBACK_MODEL=qwen3.6-35b-a3b-mtp
  STAGE_COMPARISON_GRAPHIC_LLM_TEMPERATURE=0.0
  STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS=6000
  STAGE_COMPARISON_GRAPHIC_LLM_MAX_CONTINUATIONS=2
  STAGE_COMPARISON_GRAPHIC_LLM_TIMEOUT_SEC=300
  STAGE_COMPARISON_GRAPHIC_LLM_IMAGE_LONG_SIDE=1100
  STAGE_COMPARISON_GRAPHIC_LLM_AUTH=basic
  STAGE_COMPARISON_GRAPHIC_LLM_ENABLE_MODEL_LOAD=true
  STAGE_COMPARISON_GRAPHIC_LLM_LOAD_CONTEXT_LENGTH=16000

Basic Auth: NGROK_AUTH_USER / NGROK_AUTH_PASS.

Provider бросает понятную ошибку, если env не сконфигурирован, и не падает
на пустых credentials. Внешние paid URL'ы заблокированы.
"""
from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import httpx

from backend.app.core.config import _normalize_local_base_url

logger = logging.getLogger(__name__)


# ─── Prompt (взят из benchmark — лучший результат) ───────────────────────

GRAPHIC_DIFF_LOCAL_PROMPT = """Сравни два изображения проектной документации.
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


# ─── Конфигурация ─────────────────────────────────────────────────────────


# Provider-имя. Используется в env и в response/store.
PROVIDER_NAME = "local_openai_compatible"

# Хосты, на которые мы НИКОГДА не должны ходить из этого provider'а.
# Это защита от того, чтобы кто-то случайно положил OpenRouter/OpenAI/Gemini
# URL в STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL.
_BLOCKED_EXTERNAL_HOSTS = {
    "openrouter.ai",
    "api.openrouter.ai",
    "generativelanguage.googleapis.com",
    "api.openai.com",
    "openai.com",
    "anthropic.com",
    "api.anthropic.com",
}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_csv(name: str, default: list[str]) -> list[str]:
    raw = os.environ.get(name)
    if raw is None:
        return list(default)
    return [part.strip() for part in raw.split(",") if part.strip()]


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass
class LocalGraphicLLMConfig:
    """Снимок конфигурации local graphic LLM."""

    provider: str
    base_url: str
    model: str
    fallback_model: str
    temperature: float
    max_tokens: int
    max_continuations: int
    timeout_sec: int
    image_long_side: int
    auth: str
    enable_model_load: bool
    load_context_length: int
    # Fast-profile параметры загрузки модели (benchmark 2026-06-05).
    # Тяжёлый ГРЩ-тайл на «медленном» инстансе шёл ~290s @ ~24 tok/s и падал в
    # ngrok ReadError; после clean reload c flash_attention + offload_kv_cache_to_gpu
    # тот же тайл прошёл за 29.3s @ ~230 tok/s. Поэтому ensure/load ВСЕГДА
    # поднимают модель с этим профилем, а diagnostics показывают fast_profile_ok.
    load_flash_attention: bool = True
    load_offload_kv_cache_to_gpu: bool = True
    load_parallel: int = 1
    # Streaming: длинный non-streaming ответ может простаивать до timeout без
    # единого байта и падать по ngrok read-timeout. stream=true гонит дельты
    # сразу, поэтому транспорт не считает соединение «висящим». Fallback на
    # non-streaming, если сервер/транспорт стрим не поддержали.
    stream_enabled: bool = True
    # Защита LM Studio: список model_key, которые НИКОГДА нельзя выгружать.
    # Если protected-модель пропадает после load/unload — её восстанавливаем
    # через /api/v1/models/load с config из pre-request snapshot.
    protect_models: list[str] = field(default_factory=list)
    # Если true — после успешного single graphic-diff запроса выгружать
    # primary/fallback (но не protected). Удобно держать chandra-ocr-2
    # активной для прочих OCR-задач, не оставляя qwen вечно loaded.
    unload_after_request: bool = False
    # Аналогично для batch job — выгружаем primary/fallback по завершении.
    unload_after_batch: bool = False
    basic_user: str = field(default="", repr=False)
    basic_pass: str = field(default="", repr=False)
    # Bearer-токен нового LM Studio (auth == "bearer"). Не логируется (repr=False).
    bearer_token: str = field(default="", repr=False)

    @property
    def is_active(self) -> bool:
        return self.provider == PROVIDER_NAME

    @property
    def auth_configured(self) -> bool:
        if self.auth == "bearer":
            return bool(self.bearer_token)
        if self.auth == "basic":
            return bool(self.basic_user and self.basic_pass)
        return True

    @property
    def base_url_present(self) -> bool:
        return bool((self.base_url or "").strip())


def load_local_graphic_llm_config() -> LocalGraphicLLMConfig:
    """Прочитать env. Без проверки доступности — это делает
    check_local_graphic_llm_available().
    """
    provider = (
        os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "existing").strip().lower()
        or "existing"
    )
    # base_url: свой env, fallback на общий CHANDRA_BASE_URL/LMSTUDIO_BASE_URL.
    # Нормализуем (обе формы — с /v1 и без — валидны).
    base_url = _normalize_local_base_url(
        os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL")
        or os.environ.get("CHANDRA_BASE_URL")
        or os.environ.get("LMSTUDIO_BASE_URL", "")
    )
    # Bearer-токен: свой env → общий CHANDRA_BEARER_TOKEN → LMSTUDIO_API_KEY (один токен).
    bearer_token = (
        os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_BEARER_TOKEN")
        or os.environ.get("CHANDRA_BEARER_TOKEN")
        or os.environ.get("LMSTUDIO_API_KEY", "")
    )
    return LocalGraphicLLMConfig(
        provider=provider,
        base_url=base_url,
        model=os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "").strip(),
        fallback_model=os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_FALLBACK_MODEL", "").strip(),
        temperature=_env_float("STAGE_COMPARISON_GRAPHIC_LLM_TEMPERATURE", 0.0),
        max_tokens=_env_int("STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS", 6000),
        max_continuations=max(0, _env_int("STAGE_COMPARISON_GRAPHIC_LLM_MAX_CONTINUATIONS", 2)),
        timeout_sec=_env_int("STAGE_COMPARISON_GRAPHIC_LLM_TIMEOUT_SEC", 300),
        image_long_side=_env_int("STAGE_COMPARISON_GRAPHIC_LLM_IMAGE_LONG_SIDE", 1100),
        auth=os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_AUTH", "basic").strip().lower() or "basic",
        enable_model_load=_env_bool("STAGE_COMPARISON_GRAPHIC_LLM_ENABLE_MODEL_LOAD", True),
        load_context_length=_env_int("STAGE_COMPARISON_GRAPHIC_LLM_LOAD_CONTEXT_LENGTH", 16000),
        load_flash_attention=_env_bool(
            "STAGE_COMPARISON_GRAPHIC_LLM_LOAD_FLASH_ATTENTION", True,
        ),
        load_offload_kv_cache_to_gpu=_env_bool(
            "STAGE_COMPARISON_GRAPHIC_LLM_LOAD_OFFLOAD_KV_CACHE_TO_GPU", True,
        ),
        load_parallel=max(1, _env_int("STAGE_COMPARISON_GRAPHIC_LLM_LOAD_PARALLEL", 1)),
        stream_enabled=_env_bool("STAGE_COMPARISON_GRAPHIC_LLM_STREAM", True),
        protect_models=_env_csv(
            "STAGE_COMPARISON_GRAPHIC_LLM_PROTECT_MODELS",
            ["chandra-ocr-2"],
        ),
        unload_after_request=_env_bool(
            "STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_REQUEST", False,
        ),
        unload_after_batch=_env_bool(
            "STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_BATCH", False,
        ),
        basic_user=os.environ.get("NGROK_AUTH_USER", ""),
        basic_pass=os.environ.get("NGROK_AUTH_PASS", ""),
        bearer_token=bearer_token,
    )


def _validate_base_url(base_url: str) -> Optional[str]:
    """None — ok, иначе строка с причиной отказа."""
    if not base_url:
        return "missing_base_url"
    try:
        parsed = urlparse(base_url)
    except ValueError:
        return "invalid_base_url"
    if parsed.scheme not in ("http", "https"):
        return "invalid_scheme"
    host = (parsed.hostname or "").lower()
    if not host:
        return "missing_host"
    for blocked in _BLOCKED_EXTERNAL_HOSTS:
        if host == blocked or host.endswith("." + blocked):
            return f"external_paid_host_blocked:{host}"
    return None


def check_local_graphic_llm_available(
    cfg: Optional[LocalGraphicLLMConfig] = None,
) -> tuple[bool, Optional[str]]:
    """Сконфигурирован ли provider настолько, чтобы можно было звать модель.

    Не проверяет, что endpoint реально живой — только корректность env.
    """
    cfg = cfg or load_local_graphic_llm_config()
    if not cfg.is_active:
        return False, f"provider_not_active:{cfg.provider}"
    if not cfg.model:
        return False, "missing_model"
    url_err = _validate_base_url(cfg.base_url)
    if url_err:
        return False, url_err
    if cfg.auth in ("basic", "bearer") and not cfg.auth_configured:
        return False, f"{cfg.auth}_auth_credentials_missing"
    return True, None


# ─── Image preprocessing ─────────────────────────────────────────────────


def _resize_png_to_long_side(path: Path, long_side: int) -> bytes:
    """Прочитать PNG. Если длинная сторона > long_side — уменьшить с
    сохранением пропорций. Маленькие картинки не растягиваем.
    Возвращает bytes (PNG).
    """
    from PIL import Image  # ленивый импорт, чтобы не тянуть PIL для config-only тестов

    raw = Path(path).read_bytes()
    if long_side <= 0:
        return raw
    try:
        with Image.open(io.BytesIO(raw)) as img:
            img.load()
            w, h = img.size
            current_long = max(w, h)
            if current_long <= long_side:
                return raw
            scale = long_side / float(current_long)
            new_size = (max(1, int(round(w * scale))), max(1, int(round(h * scale))))
            if img.mode not in ("RGB", "RGBA"):
                img = img.convert("RGB")
            resized = img.resize(new_size, Image.LANCZOS)
            buf = io.BytesIO()
            resized.save(buf, format="PNG", optimize=True)
            return buf.getvalue()
    except Exception as exc:
        logger.warning("image resize failed (%s); using original", exc)
        return raw


def _png_bytes_to_data_url(data: bytes) -> str:
    return "data:image/png;base64," + base64.b64encode(data).decode("ascii")


# ─── HTTP helpers (Basic Auth + ngrok header) ─────────────────────────────


def _build_headers(cfg: LocalGraphicLLMConfig) -> dict[str, str]:
    # bearer (новый сервер): Bearer <token>, без ngrok-заголовка. Токен не логируется.
    if cfg.auth == "bearer":
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if cfg.bearer_token:
            headers["Authorization"] = f"Bearer {cfg.bearer_token}"
        return headers
    # basic/legacy (ngrok): ngrok-skip + (для basic) Basic base64(user:pass)
    headers = {
        "Content-Type": "application/json",
        "ngrok-skip-browser-warning": "true",
    }
    if cfg.auth == "basic":
        token = base64.b64encode(
            f"{cfg.basic_user}:{cfg.basic_pass}".encode("utf-8")
        ).decode("ascii")
        headers["Authorization"] = f"Basic {token}"
    return headers


# ─── LM Studio model load ─────────────────────────────────────────────────


async def _list_loaded_models(cfg: LocalGraphicLLMConfig) -> list[dict[str, Any]]:
    """Прочитать /api/v1/models с base_url. Не падает, возвращает [] на ошибке.

    Возвращает каждый loaded instance с полным config: этого достаточно для
    последующего restore (context_length, eval_batch_size, parallel,
    flash_attention, offload_kv_cache_to_gpu).
    """
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                f"{cfg.base_url}/api/v1/models",
                headers=_build_headers(cfg),
            )
            if r.status_code != 200:
                return []
            data = r.json()
    except Exception as exc:
        logger.debug("list_loaded_models failed: %s", exc)
        return []
    if not isinstance(data, dict):
        return []
    loaded: list[dict[str, Any]] = []
    for m in data.get("models", []) or []:
        for inst in m.get("loaded_instances") or []:
            loaded.append({
                "model_key": m.get("key"),
                "instance_id": inst.get("id"),
                "config": dict(inst.get("config") or {}),
            })
    return loaded


async def _unload_instance(cfg: LocalGraphicLLMConfig, instance_id: str) -> tuple[bool, str]:
    """POST /api/v1/models/unload. Не падает, возвращает (ok, message)."""
    try:
        async with httpx.AsyncClient(timeout=180) as client:
            r = await client.post(
                f"{cfg.base_url}/api/v1/models/unload",
                headers=_build_headers(cfg),
                json={"instance_id": instance_id},
            )
    except Exception as exc:  # noqa: BLE001
        return False, f"http_error:{type(exc).__name__}:{exc}"
    if r.status_code != 200:
        return False, f"http_{r.status_code}:{(r.text or '')[:200]}"
    return True, "unloaded"


def _fast_profile_load_body(cfg: LocalGraphicLLMConfig, model: str) -> dict[str, Any]:
    """Тело /api/v1/models/load для нашего primary/fallback в fast-profile.

    Включает benchmark-проверенный профиль: ctx=load_context_length,
    flash_attention=true, offload_kv_cache_to_gpu=true, parallel=1.
    ``echo_load_config`` просит LM Studio вернуть фактический применённый конфиг.
    """
    return {
        "model": model,
        "context_length": int(cfg.load_context_length),
        "flash_attention": bool(cfg.load_flash_attention),
        "offload_kv_cache_to_gpu": bool(cfg.load_offload_kv_cache_to_gpu),
        "parallel": int(cfg.load_parallel),
        "echo_load_config": True,
    }


async def _load_model_with_config(
    cfg: LocalGraphicLLMConfig,
    model: str,
    snapshot_config: dict[str, Any],
) -> tuple[bool, str]:
    """POST /api/v1/models/load с конфигом из snapshot (для restore).

    Восстанавливаем protected-модель (chandra-ocr-2) ровно в том состоянии, в
    котором она была до того, как мы тронули LM Studio — поэтому профиль берём
    из snapshot, а не навязываем qwen fast-profile.
    """
    body: dict[str, Any] = {
        "model": model,
        "context_length": int(snapshot_config.get("context_length") or cfg.load_context_length),
        "flash_attention": bool(snapshot_config.get("flash_attention", True)),
        "offload_kv_cache_to_gpu": bool(snapshot_config.get("offload_kv_cache_to_gpu", True)),
        "echo_load_config": True,
    }
    if snapshot_config.get("parallel"):
        body["parallel"] = snapshot_config["parallel"]
    if snapshot_config.get("eval_batch_size"):
        body["eval_batch_size"] = snapshot_config["eval_batch_size"]
    try:
        async with httpx.AsyncClient(timeout=900) as client:
            r = await client.post(
                f"{cfg.base_url}/api/v1/models/load",
                headers=_build_headers(cfg),
                json=body,
            )
    except Exception as exc:  # noqa: BLE001
        return False, f"http_error:{type(exc).__name__}:{exc}"
    if r.status_code != 200:
        return False, f"http_{r.status_code}:{(r.text or '')[:200]}"
    return True, "restored"


async def _load_model(cfg: LocalGraphicLLMConfig, model: str) -> tuple[bool, str]:
    """POST /api/v1/models/load в fast-profile. Возвращает (ok, message)."""
    body = _fast_profile_load_body(cfg, model)
    try:
        async with httpx.AsyncClient(timeout=900) as client:
            r = await client.post(
                f"{cfg.base_url}/api/v1/models/load",
                headers=_build_headers(cfg),
                json=body,
            )
    except httpx.TimeoutException as exc:
        return False, f"timeout:{exc}"
    except Exception as exc:
        return False, f"http_error:{type(exc).__name__}:{exc}"
    if r.status_code != 200:
        return False, f"http_{r.status_code}:{(r.text or '')[:300]}"
    return True, "loaded"


async def snapshot_loaded_models(
    cfg: Optional[LocalGraphicLLMConfig] = None,
) -> list[dict[str, Any]]:
    """Безопасный read-only снимок loaded моделей для cleanup/restore.

    Возвращает то же, что и ``_list_loaded_models``, но это публичный API:
    caller может сохранить snapshot до load qwen и потом передать его в
    ``cleanup_local_graphic_llm``.
    """
    cfg = cfg or load_local_graphic_llm_config()
    return await _list_loaded_models(cfg)


def _provider_owned_model_keys(cfg: LocalGraphicLLMConfig) -> set[str]:
    """Какие model_key считаются 'нашими' (primary/fallback). Только их можно
    выгружать. protect_models исключены."""
    keys: set[str] = set()
    if cfg.model:
        keys.add(cfg.model)
    if cfg.fallback_model:
        keys.add(cfg.fallback_model)
    return keys - set(cfg.protect_models or [])


async def cleanup_local_graphic_llm(
    cfg: LocalGraphicLLMConfig,
    pre_snapshot: list[dict[str, Any]],
    *,
    scope: str,
) -> dict[str, Any]:
    """Cleanup после single request или batch.

    Шаги:
      1. Если scope='request' и cfg.unload_after_request=false → только проверка
         protect-models (без выгрузки).
      2. Если scope='batch' и cfg.unload_after_batch=false → только проверка.
      3. Иначе: выгрузить loaded instance'ы, принадлежащие провайдеру
         (primary/fallback), но НЕ из protect-list.
      4. Проверить, что protected models, бывшие в pre_snapshot, всё ещё loaded.
         Если что-то пропало — попробовать восстановить через /api/v1/models/load
         с config из snapshot.
      5. Вернуть {events, warnings, unloaded, restored, protected_kept}.

    Никогда не бросает исключений (caller отделяет UX от cleanup-ошибок).
    """
    assert scope in {"request", "batch"}, f"unknown cleanup scope: {scope}"
    result: dict[str, Any] = {
        "scope": scope,
        "events": [],
        "warnings": [],
        "unloaded": [],
        "restored": [],
        "protected_kept": [],
        "skipped_unload": False,
    }

    protect = set(cfg.protect_models or [])
    do_unload = (
        (scope == "request" and cfg.unload_after_request)
        or (scope == "batch" and cfg.unload_after_batch)
    )
    if not do_unload:
        result["skipped_unload"] = True
        result["events"].append(f"unload_skipped:{scope}")

    try:
        # 3) Unload provider-owned models, если включено
        if do_unload:
            owned = _provider_owned_model_keys(cfg)
            try:
                current = await _list_loaded_models(cfg)
            except Exception as exc:  # noqa: BLE001
                result["warnings"].append(f"list_loaded_failed_pre_unload:{exc}")
                current = []
            for inst in current:
                key = inst.get("model_key") or ""
                if key in protect:
                    continue
                if key not in owned:
                    continue
                ok, msg = await _unload_instance(cfg, inst.get("instance_id") or "")
                evt = f"unload:{key}:{msg}"
                result["events"].append(evt)
                if ok:
                    result["unloaded"].append(key)
                else:
                    result["warnings"].append(evt)

        # 4) Restore protected models, если они пропали
        try:
            now_loaded = await _list_loaded_models(cfg)
        except Exception as exc:  # noqa: BLE001
            result["warnings"].append(f"list_loaded_failed_post_unload:{exc}")
            now_loaded = []
        now_keys = {x.get("model_key") for x in now_loaded if x.get("model_key")}

        # Только те protected, которые были loaded ДО запроса — их и проверяем
        snapshot_protected = []
        for it in pre_snapshot or []:
            key = it.get("model_key")
            if key and key in protect:
                snapshot_protected.append(it)

        for it in snapshot_protected:
            key = it["model_key"]
            if key in now_keys:
                result["protected_kept"].append(key)
                continue
            # Пропала — пробуем восстановить
            ok, msg = await _load_model_with_config(cfg, key, it.get("config") or {})
            evt = f"restore:{key}:{msg}"
            result["events"].append(evt)
            if ok:
                result["restored"].append(key)
                # Перечитываем чтобы убедиться
                try:
                    re_loaded = await _list_loaded_models(cfg)
                    re_keys = {x.get("model_key") for x in re_loaded if x.get("model_key")}
                    if key in re_keys:
                        result["events"].append(f"restore_verified:{key}")
                    else:
                        result["warnings"].append(f"restore_not_in_loaded_list:{key}")
                except Exception as exc:  # noqa: BLE001
                    result["warnings"].append(f"restore_verify_failed:{key}:{exc}")
            else:
                result["warnings"].append(evt)

        return result
    except Exception as exc:  # noqa: BLE001
        logger.exception("cleanup_local_graphic_llm failed")
        result["warnings"].append(f"cleanup_unexpected:{type(exc).__name__}:{exc}")
        return result


def _loaded_context_length(instance: dict[str, Any]) -> Optional[int]:
    """Достать loaded context_length из одной записи _list_loaded_models.

    Возвращает None, если поле отсутствует (тогда caller трактует как
    backward-compat: не делать reload, чтобы не ломать legacy LM Studio).
    """
    cfg_section = instance.get("config") or {}
    for key in ("context_length", "loaded_context_length", "n_ctx"):
        val = cfg_section.get(key)
        if val is None:
            continue
        try:
            return int(val)
        except (TypeError, ValueError):
            continue
    return None


def _instance_profile_status(
    instance: dict[str, Any], cfg: LocalGraphicLLMConfig,
) -> dict[str, Any]:
    """Сверить loaded instance с fast-profile.

    ``fast_profile_ok`` / ``needs_reload`` ключуются на ТROUGHPUT-критичных
    факторах: ``ctx`` + ``flash_attention`` + ``offload_kv_cache_to_gpu``.
    Именно их clean reload поднял throughput с ~24 до ~230 tok/s (benchmark
    2026-06-05). ``parallel`` НЕ входит в эти проверки: тот же benchmark
    наблюдал быстрый профиль при `parallel=4`, поэтому форсить reload из-за
    `parallel != 1` значило бы зря перезагружать уже-быстрый инстанс.
    `parallel=1` всё равно ставится на СВЕЖЕЙ загрузке (`_fast_profile_load_body`)
    как консервативный дефолт под single-concurrency GRSH и просто
    отображается в диагностике (`parallel` / `parallel_ok`).

    Возвращает:
      * ``ctx`` / ``ctx_ok`` — loaded context_length и >= desired;
      * ``flash_attention`` / ``offload_kv_cache_to_gpu`` / ``parallel`` —
        фактически загруженные значения (None, если LM Studio их не отдаёт);
      * ``parallel_ok`` — parallel совпадает с desired (информационно);
      * ``fast_profile_ok`` — ctx+flash+offload ПОДТВЕРЖДЕНЫ (None → не
        подтверждено → False);
      * ``needs_reload`` — True только при ЯВНОМ противоречии ctx/flash/offload
        (поле отдано и не совпадает). Неизвестное поле (None) reload НЕ
        триггерит — backward-compat с LM Studio, которая может не эхоить config.
      * ``reasons`` — список причин reload (для логов/диагностики).
    """
    config = instance.get("config") or {}
    ctx = _loaded_context_length(instance)
    flash = config.get("flash_attention")
    offload = config.get("offload_kv_cache_to_gpu")
    parallel = config.get("parallel")
    desired_ctx = int(cfg.load_context_length)

    ctx_ok = bool(ctx is not None and ctx >= desired_ctx)
    # fast_profile_ok требует ПОДТВЕРЖДЁННОГО совпадения (None → не подтверждено)
    flash_ok = (flash is True) if cfg.load_flash_attention else (flash is False)
    offload_ok = (offload is True) if cfg.load_offload_kv_cache_to_gpu else (offload is False)
    try:
        parallel_ok = parallel is not None and int(parallel) == int(cfg.load_parallel)
    except (TypeError, ValueError):
        parallel_ok = False
    # parallel намеренно НЕ входит в fast_profile_ok (benchmark: parallel=4 был fast).
    fast_profile_ok = bool(ctx_ok and flash_ok and offload_ok)

    needs_reload = False
    reasons: list[str] = []
    if ctx is not None and ctx < desired_ctx:
        needs_reload = True
        reasons.append(f"ctx={ctx}<{desired_ctx}")
    if cfg.load_flash_attention and flash is False:
        needs_reload = True
        reasons.append("flash_attention=false")
    if cfg.load_offload_kv_cache_to_gpu and offload is False:
        needs_reload = True
        reasons.append("offload_kv_cache_to_gpu=false")

    return {
        "ctx": ctx,
        "ctx_ok": ctx_ok,
        "flash_attention": flash,
        "offload_kv_cache_to_gpu": offload,
        "parallel": parallel,
        "parallel_ok": parallel_ok,
        "fast_profile_ok": fast_profile_ok,
        "needs_reload": needs_reload,
        "reasons": reasons,
    }


async def ensure_lmstudio_model_loaded(
    model_name: str,
    *,
    cfg: Optional[LocalGraphicLLMConfig] = None,
    allow_fallback: bool = True,
) -> dict[str, Any]:
    """Убедиться, что нужная model загружена в LM Studio с требуемым ctx.

    Strict context check (2026-05-25): если model уже loaded, но
    ``config.context_length < cfg.load_context_length`` — этот instance
    некорректен (slop с дефолтным ctx=4096 в LM Studio). Тогда:

      1. Если модель в ``protect_models`` — НЕ выгружаем, возвращаем
         ``context_length_mismatch_protected`` (caller должен показать ошибку
         оператору; protected ребутить нельзя).
      2. Иначе — unload именно этот instance и load заново с
         ``cfg.load_context_length``.
      3. После reload верификация: если loaded ctx всё ещё < desired —
         возвращаем ``status=error, reason=context_length_mismatch``.

    Возвращает dict:

        {
          "ok": True/False,
          "model_used": "qwen/qwen3.6-35b-a3b" | fallback,
          "fallback_used": False,
          "endpoint_available": True/False,
          "messages": ["..."],
          # При context mismatch ошибке:
          "status": "error",
          "reason": "context_length_mismatch" | "context_length_mismatch_protected",
          "desired_ctx": 16000,
          "actual_ctx": 4096,
        }

    Если load endpoint недоступен — НЕ падаем, возвращаем
    endpoint_available=False; caller может попытаться сделать chat completion
    как есть (модель может быть уже загружена и JIT не понадобится).
    """
    cfg = cfg or load_local_graphic_llm_config()
    messages: list[str] = []
    desired_ctx = int(cfg.load_context_length)

    if not cfg.enable_model_load:
        return {
            "ok": True,
            "model_used": model_name,
            "fallback_used": False,
            "endpoint_available": False,
            "messages": ["model_load_disabled_via_env"],
        }

    loaded = await _list_loaded_models(cfg)
    if loaded:
        # endpoint available — посмотрим, есть ли уже нужная model
        same_model = [it for it in loaded if it.get("model_key") == model_name]
        if same_model:
            # Берём instance с наибольшим ctx и сверяем с fast-profile.
            best_inst = max(
                same_model,
                key=lambda it: (_loaded_context_length(it) or -1),
            )
            status = _instance_profile_status(best_inst, cfg)
            actual_ctx = status["ctx"]
            if not status["needs_reload"]:
                # fast-profile подтверждён ИЛИ поля неизвестны (backward-compat):
                # не трогаем уже загруженный инстанс.
                msg = (
                    "already_loaded:ctx_unknown"
                    if actual_ctx is None
                    else f"already_loaded:ctx={actual_ctx}"
                )
                return {
                    "ok": True,
                    "model_used": model_name,
                    "fallback_used": False,
                    "endpoint_available": True,
                    "messages": [msg],
                    "actual_ctx": actual_ctx,
                    "desired_ctx": desired_ctx,
                    "fast_profile_ok": status["fast_profile_ok"],
                }
            # Профиль не fast (ctx мал ИЛИ flash/offload/parallel не совпали):
            # нужен reload.
            protect = set(cfg.protect_models or [])
            if model_name in protect:
                ctx_is_reason = any(r.startswith("ctx=") for r in status["reasons"])
                return {
                    "ok": False,
                    "status": "error",
                    "reason": (
                        "context_length_mismatch_protected"
                        if ctx_is_reason
                        else "fast_profile_mismatch_protected"
                    ),
                    "model_used": model_name,
                    "fallback_used": False,
                    "endpoint_available": True,
                    "messages": [
                        f"profile_mismatch_protected:{','.join(status['reasons'])}",
                    ],
                    "desired_ctx": desired_ctx,
                    "actual_ctx": actual_ctx,
                    "fast_profile_ok": status["fast_profile_ok"],
                }
            # unload плохие instances этой модели
            for inst in same_model:
                inst_id = inst.get("instance_id") or ""
                if not inst_id:
                    continue
                u_ok, u_msg = await _unload_instance(cfg, inst_id)
                messages.append(f"unload_not_fast_profile:{model_name}:{u_msg}")
            messages.append(f"reload_reasons:{','.join(status['reasons'])}")
            # reload в fast-profile
            ok, msg = await _load_model(cfg, model_name)
            messages.append(f"reload_fast_profile:{msg}")
            if not ok:
                return {
                    "ok": False,
                    "status": "error",
                    "reason": "reload_failed",
                    "model_used": model_name,
                    "fallback_used": False,
                    "endpoint_available": True,
                    "messages": messages,
                    "desired_ctx": desired_ctx,
                    "actual_ctx": actual_ctx,
                    "fast_profile_ok": False,
                }
            # verify
            verify_loaded = await _list_loaded_models(cfg)
            verify_same = [it for it in verify_loaded if it.get("model_key") == model_name]
            verify_status = None
            if verify_same:
                verify_best = max(
                    verify_same,
                    key=lambda it: (_loaded_context_length(it) or -1),
                )
                verify_status = _instance_profile_status(verify_best, cfg)
            if verify_status is not None and not verify_status["needs_reload"]:
                messages.append(
                    f"verify_ok:ctx={verify_status['ctx']},"
                    f"fast_profile_ok={verify_status['fast_profile_ok']}"
                )
                return {
                    "ok": True,
                    "model_used": model_name,
                    "fallback_used": False,
                    "endpoint_available": True,
                    "messages": messages,
                    "desired_ctx": desired_ctx,
                    "actual_ctx": verify_status["ctx"],
                    "fast_profile_ok": verify_status["fast_profile_ok"],
                }
            # профиль по-прежнему не fast → жёсткая ошибка
            v_reasons = (
                verify_status["reasons"] if verify_status else ["model_not_loaded_after_reload"]
            )
            ctx_is_reason = any(r.startswith("ctx=") for r in v_reasons)
            return {
                "ok": False,
                "status": "error",
                "reason": (
                    "context_length_mismatch" if ctx_is_reason else "fast_profile_mismatch"
                ),
                "model_used": model_name,
                "fallback_used": False,
                "endpoint_available": True,
                "messages": messages + [f"verify_fail:{','.join(v_reasons)}"],
                "desired_ctx": desired_ctx,
                "actual_ctx": (verify_status["ctx"] if verify_status else actual_ctx),
                "fast_profile_ok": (
                    verify_status["fast_profile_ok"] if verify_status else False
                ),
            }
    # Попытаться загрузить primary
    ok, msg = await _load_model(cfg, model_name)
    messages.append(f"primary_load:{msg}")
    if ok:
        return {
            "ok": True,
            "model_used": model_name,
            "fallback_used": False,
            "endpoint_available": True,
            "messages": messages,
        }

    # Если primary не загрузилась — пробуем fallback (если есть)
    fallback = (cfg.fallback_model or "").strip()
    if allow_fallback and fallback and fallback != model_name:
        ok2, msg2 = await _load_model(cfg, fallback)
        messages.append(f"fallback_load:{msg2}")
        if ok2:
            return {
                "ok": True,
                "model_used": fallback,
                "fallback_used": True,
                "endpoint_available": True,
                "messages": messages,
            }

    # Load endpoint вообще не отвечает (мы не смогли получить список)? Не падаем —
    # пусть caller попробует chat completion напрямую.
    if not loaded:
        return {
            "ok": False,
            "model_used": model_name,
            "fallback_used": False,
            "endpoint_available": False,
            "messages": messages + ["load_endpoint_unreachable"],
        }
    return {
        "ok": False,
        "model_used": model_name,
        "fallback_used": False,
        "endpoint_available": True,
        "messages": messages,
    }


# ─── JSON parsing ─────────────────────────────────────────────────────────


_JSON_BLOCK_RE = re.compile(r"\{[\s\S]*\}")


def parse_diff_json(text: str) -> Optional[dict[str, Any]]:
    """Извлечь JSON из ответа модели. Возвращает dict или None."""
    if not text or not text.strip():
        return None
    s = text.strip()
    # 1) Прямой парс
    try:
        return json.loads(s)
    except Exception:
        pass
    # 2) Удалить markdown fences
    cleaned = re.sub(r"```(?:json)?", "", s).replace("```", "").strip()
    try:
        return json.loads(cleaned)
    except Exception:
        pass
    # 3) Найти крупный brace-блок
    m = _JSON_BLOCK_RE.search(cleaned)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    return None


# Маркеры self-truncation от модели. Качество salvage напрямую зависит от того,
# что мы режем строку именно по последнему многоточию (а не по случайной точке
# внутри числа или сокращения).
_ELLIPSIS_RE = re.compile(r"(?:…|\.\.\.)")


def salvage_partial_json(text: str) -> Optional[dict[str, Any]]:
    """Попытаться вытащить структурированные поля из оборванного JSON.

    Возвращаемый dict помечен ключом ``_salvaged: True``. Если salvage
    невозможен — None.

    Тактика:
      1. Найти крупный brace-блок (от первого `{` до конца);
      2. Отрезать всё после последнего многоточия `…`/`...`, найденного
         внутри JSON-значения, — модель почти всегда после этого
         останавливается;
      3. Откатиться к последней позиции, где можно безопасно закрыть
         текущую строку/массив/объект (то есть стереть незавершённую
         трейлинговую конструкцию);
      4. Сбалансировать `{}` и `[]` дописыванием закрывающих скобок;
      5. Попробовать `json.loads`. Если получилось — добавить
         ``_salvaged: True`` и вернуть.

    Salvage намеренно консервативный: он спасает summary/image_kind/
    список равных-уровней-выше элементов, а не дофантазирует поля.
    """
    if not text or not text.strip():
        return None
    s = text.strip()
    # markdown-fences off
    s = re.sub(r"```(?:json)?", "", s).replace("```", "").strip()

    # Локализуем основной JSON-блок: от первого `{` до конца.
    start = s.find("{")
    if start < 0:
        return None
    body = s[start:]

    # Отрезаем хвост по последнему многоточию — обычно именно там модель
    # самооборвалась.
    last_ell = None
    for m in _ELLIPSIS_RE.finditer(body):
        last_ell = m
    if last_ell is not None:
        body = body[: last_ell.start()]

    # Аккуратно откатываемся до последней «безопасной» точки:
    # удаляем недозакрытую трейлинговую строку и неполную последнюю запись.
    salvaged = _trim_to_last_safe_boundary(body)
    if salvaged is None:
        return None

    try:
        out = json.loads(salvaged)
    except Exception:
        return None
    if not isinstance(out, dict):
        return None
    out["_salvaged"] = True
    return out


def _trim_to_last_safe_boundary(body: str) -> Optional[str]:
    """Срезать недозакрытый хвост JSON и сбалансировать скобки.

    Идём по строке посимвольно. Для каждого `{`/`[` запоминаем тип контейнера
    и, для объектов, текущее «ожидание»: ключ → `:` → значение → `,`/`}`.
    Запоминаем «безопасную» позицию ТОЛЬКО после полностью завершённой
    `key: value` пары (внутри объекта) либо после полностью завершённого
    элемента массива.

    Затем срезаем висящие запятые/пробелы и дописываем закрывающие скобки
    в правильном порядке.
    """
    if not body:
        return None

    # Каждый элемент стека: ("{", expecting) или ("[", None).
    # expecting ∈ "key" | "colon" | "value" | "separator"
    stack: list[tuple[str, Optional[str]]] = []
    in_str = False
    escape = False
    last_safe = -1
    last_safe_stack: list[tuple[str, Optional[str]]] = []

    def _mark_safe(pos: int):
        nonlocal last_safe, last_safe_stack
        last_safe = pos
        last_safe_stack = [t for t in stack]

    def _on_value_complete(pos_after: int):
        """Значение завершено: внутри объекта переходим к 'separator',
        внутри массива — к 'separator'. На любом уровне — это безопасная точка.
        """
        if stack:
            kind, _ = stack[-1]
            stack[-1] = (kind, "separator")
            _mark_safe(pos_after)
        else:
            # значение на топ-уровне (редкость) — отметим.
            _mark_safe(pos_after)

    i = 0
    n = len(body)
    while i < n:
        c = body[i]

        if in_str:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_str = False
                # Конец строки. Что это было — ключ или значение?
                if stack:
                    kind, expecting = stack[-1]
                    if kind == "{":
                        if expecting == "key":
                            # это был ключ — следующее обязательно ':'
                            stack[-1] = ("{", "colon")
                            # НЕ помечаем как safe — мы посередине пары.
                        else:
                            # expecting == "value" → строка-значение
                            _on_value_complete(i + 1)
                    else:
                        # array — строка-значение
                        _on_value_complete(i + 1)
                else:
                    _mark_safe(i + 1)
            i += 1
            continue

        if c == '"':
            in_str = True
            i += 1
            continue
        if c == "{":
            stack.append(("{", "key"))
            i += 1
            continue
        if c == "[":
            stack.append(("[", None))
            i += 1
            continue
        if c == "}":
            if not stack or stack[-1][0] != "{":
                break
            stack.pop()
            _on_value_complete(i + 1)
            i += 1
            continue
        if c == "]":
            if not stack or stack[-1][0] != "[":
                break
            stack.pop()
            _on_value_complete(i + 1)
            i += 1
            continue
        if c == ":":
            if stack and stack[-1][0] == "{" and stack[-1][1] == "colon":
                stack[-1] = ("{", "value")
            i += 1
            continue
        if c == ",":
            if stack:
                kind, expecting = stack[-1]
                if kind == "{":
                    # после value/separator — снова ждём key
                    stack[-1] = ("{", "key")
                # array — ждём следующее значение
                _mark_safe(i)  # safe ПЕРЕД запятой
            i += 1
            continue
        if c.isdigit() or (c == "-" and i + 1 < n and body[i + 1].isdigit()):
            j = i + 1
            while j < n and body[j] in "0123456789.eE+-":
                j += 1
            _on_value_complete(j)
            i = j
            continue
        if c in "tfn":
            matched = False
            for token in ("true", "false", "null"):
                if body.startswith(token, i):
                    _on_value_complete(i + len(token))
                    i += len(token)
                    matched = True
                    break
            if not matched:
                # неизвестный токен — стоп
                break
            continue
        # пробелы, переводы строк — пропускаем
        i += 1

    if last_safe <= 0:
        return None

    head = body[:last_safe]
    # Срезать висящие запятые/пробелы.
    head = re.sub(r"[,\s]+$", "", head)
    # Закрыть оставшиеся открытые скобки в правильном порядке (LIFO).
    closers = "".join("}" if kind == "{" else "]" for kind, _ in reversed(last_safe_stack))
    return head + closers


# ─── Retry / transient error detection ─────────────────────────────────────


# Эти подстроки в теле HTTP-ответа LM Studio сигнализируют о transient-
# проблеме: модель выгрузилась JIT'ом, LM Studio внутренне отменил load,
# или схватил 500. Все они исправляются повторной попыткой через 5с.
_RETRYABLE_BODY_MARKERS = (
    "model unloaded",
    "operation canceled",
    "operation cancelled",
    "failed to load model",
    "model_not_loaded",
    "model is not loaded",
)


def _is_retryable_http(status_code: int, body_text: str) -> Optional[str]:
    """Вернуть короткую причину для retry или None.

    Используется как в Compare flow, так и в Describe flow.
    """
    if status_code >= 500:
        return f"http_{status_code}"
    if status_code == 400 and isinstance(body_text, str):
        low = body_text.lower()
        for marker in _RETRYABLE_BODY_MARKERS:
            if marker in low:
                return f"http_400:{marker}"
    return None


def _is_retryable_exception(exc: Exception) -> Optional[str]:
    """Сетевые/транспортные ошибки, которые имеет смысл пере-запросить."""
    if isinstance(exc, httpx.TimeoutException):
        return f"timeout:{type(exc).__name__}"
    if isinstance(exc, (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError, httpx.NetworkError)):
        return f"network:{type(exc).__name__}"
    return None


# ─── Transport-vs-content error classification (retry resilience 2026-06-06) ──
#
# Инцидент: на паре ПОС Qwen упал не из-за модели, а из-за транспорта — ngrok
# вернул HTML «tunnel not found» (HTTP 404) и httpx.ReadError. Такие сбои
# RETRYABLE: туннель/агент LM Studio временно отвалился, повтор через паузу
# обычно проходит. Их нельзя путать с content/model-ошибками (валидный 2xx, но
# контент не распарсился / не прошёл schema) — те повтором транспорта не лечатся.

# Маркеры HTML-страницы прокси/ngrok в теле ответа: запрос не дошёл до модели.
_TRANSPORT_HTML_MARKERS = (
    "ngrok",
    "err_ngrok",
    "tunnel not found",
    "tunnel-not-found",
    "<!doctype html",
    "<html",
    "bad gateway",
    "gateway time-out",
    "gateway timeout",
    "service unavailable",
    "502 bad gateway",
    "503 service",
    "504 gateway",
)

# HTTP-коды, которые на стеке «LM Studio за ngrok» практически всегда означают
# временную недоступность апстрима, а не валидную ошибку запроса:
#   404 — ngrok «tunnel not found» (агент офлайн);
#   408/425/429 — таймаут / too-early / rate-limit;
#   5xx — апстрим упал/перегружен (обрабатывается отдельно как >=500).
_RETRYABLE_TRANSPORT_STATUS = {404, 408, 425, 429}


def _looks_like_transport_html(body: Optional[str]) -> bool:
    """True, если тело ответа — HTML-страница ошибки прокси/ngrok, а не JSON от
    модели (т.е. запрос не дошёл до LLM → transport-сбой)."""
    if not body:
        return False
    low = body[:2000].lower()
    return any(m in low for m in _TRANSPORT_HTML_MARKERS)


def _parse_http_status_from_error(error: Optional[str]) -> int:
    """Достать числовой HTTP-код из строки вида 'http_404' / 'http_503:...'."""
    if not error or not error.startswith("http_"):
        return 0
    try:
        return int(error.split("_", 1)[1].split(":", 1)[0])
    except (ValueError, IndexError):
        return 0


def classify_describe_error(result: Optional["DescribeResult"]) -> str:
    """Классифицировать исход одного describe-вызова.

    Возвращает:
      * ``ok``        — done/partial (успех, включая salvage);
      * ``transport`` — RETRYABLE: сетевой/транспортный сбой (ngrok HTML 404,
        408/425/429, 5xx, ReadError/ConnectError/Timeout/RemoteProtocol, пустой
        ответ, HTML вместо JSON). Повтор после паузы имеет смысл;
      * ``content``   — NON-RETRYABLE: модель ответила (2xx), но контент не
        распарсился/не прошёл schema (invalid_json). Это путь salvage/fallback,
        а не транспортный повтор;
      * ``model``     — NON-RETRYABLE: валидная ошибка API запроса (4xx с
        осмысленным телом, например 400 bad request). Повтор не поможет.
    """
    if result is None:
        return "transport"
    st = (result.status or "").strip()
    if st in ("done", "partial"):
        return "ok"
    if st == "timeout":
        return "transport"
    body = getattr(result, "full_raw_response", "") or ""
    err = result.error or ""
    if st == "invalid_json":
        # 2xx, но контент не JSON. Транспортным считаем ТОЛЬКО если апстрим
        # подсунул HTML-страницу прокси вместо ответа модели (200 + ngrok page).
        return "transport" if _looks_like_transport_html(body) else "content"
    if st == "error":
        if err.startswith(("http_error:", "network:", "timeout")):
            return "transport"
        if err.startswith("http_"):
            code = _parse_http_status_from_error(err)
            if code in _RETRYABLE_TRANSPORT_STATUS or code >= 500:
                return "transport"
            if _looks_like_transport_html(body) or not body.strip():
                return "transport"
            return "model"  # настоящая 4xx с телом (например, 400 bad request)
        # Неизвестная строка ошибки: транспорт только при HTML/пустом теле.
        return "transport" if (_looks_like_transport_html(body) or not body.strip()) else "content"
    if st == "provider_unavailable":
        return "transport"
    return "content"


def _transport_retry_plan() -> tuple[int, list[float], float]:
    """(retries, backoff_seconds, jitter_frac) для transport-повторов из env.

    ``retries`` — число ПОВТОРОВ (итого попыток = 1 + retries). ``backoff`` —
    задержка перед каждым повтором по индексу (последнее значение тянется на
    хвост). ``jitter_frac`` — доля случайной добавки сверху [0..base*jitter].
    """
    retries = max(0, _env_int("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_RETRIES", 3))
    raw = os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_BACKOFF", "5,15,30,60")
    backoff: list[float] = []
    for tok in (raw or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            backoff.append(max(0.0, float(tok)))
        except ValueError:
            continue
    if not backoff:
        backoff = [5.0, 15.0, 30.0, 60.0]
    jitter = max(0.0, _env_float("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_JITTER", 0.25))
    return retries, backoff, jitter


def _transport_retry_delay(attempt_index: int, backoff: list[float], jitter: float) -> float:
    """Задержка перед повтором: backoff[attempt] + небольшой джиттер."""
    if not backoff:
        return 0.0
    base = backoff[min(attempt_index, len(backoff) - 1)]
    if jitter > 0 and base > 0:
        base = base + random.uniform(0.0, base * jitter)
    return base


def _build_summary_text(parsed: dict[str, Any]) -> str:
    """Собрать человекочитаемую summary из распарсенного JSON для UI."""
    if not isinstance(parsed, dict):
        return ""
    summary = (parsed.get("summary") or "").strip()
    diffs = parsed.get("differences") or []
    if isinstance(diffs, list) and diffs:
        lines: list[str] = []
        if summary:
            lines.append(summary)
        for d in diffs:
            if not isinstance(d, dict):
                continue
            t = (d.get("type") or "").strip() or "unknown"
            sev = (d.get("severity") or "").strip() or "?"
            desc = (d.get("description") or "").strip()
            ev = (d.get("evidence") or "").strip()
            entry = f"• [{t}/{sev}] {desc}"
            if ev:
                entry += f" ({ev})"
            lines.append(entry)
        return "\n".join(lines)
    return summary


# ─── Compare ──────────────────────────────────────────────────────────────


@dataclass
class CompareResult:
    """Результат одного сравнения двух картинок."""

    status: str                        # done | error | provider_unavailable | invalid_json | timeout
    provider: str = PROVIDER_NAME
    model: str = ""                    # запрошенная primary model
    model_used: str = ""               # фактически использованная (primary или fallback)
    fallback_used: bool = False
    has_significant_difference: Optional[bool] = None
    summary: str = ""                  # человекочитаемая текстовая summary
    differences: list[dict[str, Any]] = field(default_factory=list)
    confidence: Optional[float] = None
    parsed: Optional[dict[str, Any]] = None
    raw_response_excerpt: str = ""
    duration_sec: float = 0.0
    error: Optional[str] = None

    def to_entry_dict(self) -> dict[str, Any]:
        """Поля для сохранения в graphic_diffs.json через add_graphic_diff_result."""
        return {
            "provider": self.provider,
            "model": self.model,
            "model_used": self.model_used,
            "fallback_used": self.fallback_used,
            "has_significant_difference": self.has_significant_difference,
            "differences": self.differences,
            "confidence": self.confidence,
            "duration_sec": round(self.duration_sec, 3),
            "raw_response_excerpt": self.raw_response_excerpt,
        }


def _excerpt(text: str, max_chars: int = 1500) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "…"


async def compare_images_local(
    left_image_path: str | Path,
    right_image_path: str | Path,
    prompt: Optional[str] = None,
    *,
    model: Optional[str] = None,
    cfg: Optional[LocalGraphicLLMConfig] = None,
    model_used_hint: Optional[str] = None,
    fallback_used_hint: bool = False,
) -> CompareResult:
    """Сравнить две картинки через локальный OpenAI-compatible vision endpoint.

    Параметры:
      left_image_path  — путь к PNG предыдущей стадии
      right_image_path — путь к PNG новой стадии
      prompt           — кастомный prompt (default GRAPHIC_DIFF_LOCAL_PROMPT)
      model            — override primary модели (default — из конфига)
      cfg              — пред-загруженный config
      model_used_hint  — если caller уже сделал ensure_loaded и знает,
                          какая model реально загрузилась
      fallback_used_hint — указатель, что caller перешёл на fallback
    """
    cfg = cfg or load_local_graphic_llm_config()
    ok, reason = check_local_graphic_llm_available(cfg)
    if not ok:
        return CompareResult(
            status="provider_unavailable",
            provider=cfg.provider or PROVIDER_NAME,
            model=model or cfg.model,
            model_used=model_used_hint or "",
            fallback_used=fallback_used_hint,
            error=f"local_graphic_llm_unavailable:{reason}",
        )

    primary_model = (model or cfg.model).strip()
    use_model = (model_used_hint or primary_model).strip()
    use_prompt = (prompt or GRAPHIC_DIFF_LOCAL_PROMPT)

    # Preprocess images
    left_bytes = _resize_png_to_long_side(Path(left_image_path), cfg.image_long_side)
    right_bytes = _resize_png_to_long_side(Path(right_image_path), cfg.image_long_side)
    left_url = _png_bytes_to_data_url(left_bytes)
    right_url = _png_bytes_to_data_url(right_bytes)

    payload = {
        "model": use_model,
        "max_tokens": int(cfg.max_tokens),
        "temperature": float(cfg.temperature),
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": use_prompt},
                    {"type": "image_url", "image_url": {"url": left_url}},
                    {"type": "image_url", "image_url": {"url": right_url}},
                ],
            },
        ],
    }

    started = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=cfg.timeout_sec) as client:
            response = await client.post(
                f"{cfg.base_url}/v1/chat/completions",
                headers=_build_headers(cfg),
                json=payload,
            )
    except httpx.TimeoutException as exc:
        return CompareResult(
            status="timeout",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used_hint,
            duration_sec=time.monotonic() - started,
            error=f"timeout:{exc}",
        )
    except Exception as exc:  # noqa: BLE001
        return CompareResult(
            status="error",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used_hint,
            duration_sec=time.monotonic() - started,
            error=f"http_error:{type(exc).__name__}:{exc}",
        )

    duration = time.monotonic() - started

    body_text = response.text or ""
    try:
        data = response.json()
    except Exception:
        data = None

    if response.status_code >= 400:
        return CompareResult(
            status="error",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used_hint,
            duration_sec=duration,
            raw_response_excerpt=_excerpt(body_text),
            error=f"http_{response.status_code}",
        )

    # Извлечь content из choices[0].message
    content_text = ""
    if isinstance(data, dict):
        choices = data.get("choices") or []
        if choices and isinstance(choices[0], dict):
            msg = choices[0].get("message") or {}
            content = msg.get("content")
            if isinstance(content, str):
                content_text = content
            elif isinstance(content, list):
                # OpenAI multi-part content
                parts: list[str] = []
                for item in content:
                    if isinstance(item, dict) and isinstance(item.get("text"), str):
                        parts.append(item["text"])
                content_text = "\n".join(parts)
            if not content_text:
                reasoning = msg.get("reasoning_content")
                if isinstance(reasoning, str):
                    content_text = reasoning

    parsed = parse_diff_json(content_text)
    if parsed is None:
        return CompareResult(
            status="invalid_json",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used_hint,
            duration_sec=duration,
            raw_response_excerpt=_excerpt(content_text or body_text),
            error="json_parse_failed",
        )

    has_diff = parsed.get("has_significant_difference")
    if not isinstance(has_diff, bool):
        has_diff = None
    diffs = parsed.get("differences") if isinstance(parsed.get("differences"), list) else []
    confidence_raw = parsed.get("confidence")
    confidence: Optional[float]
    try:
        confidence = float(confidence_raw) if confidence_raw is not None else None
    except (TypeError, ValueError):
        confidence = None

    summary_text = _build_summary_text(parsed)

    return CompareResult(
        status="done",
        provider=cfg.provider,
        model=primary_model,
        model_used=use_model,
        fallback_used=fallback_used_hint,
        has_significant_difference=has_diff,
        summary=summary_text,
        differences=diffs,
        confidence=confidence,
        parsed=parsed,
        raw_response_excerpt=_excerpt(content_text),
        duration_sec=duration,
        error=None,
    )


# ─── Single-image description (для MD enrichment) ─────────────────────────


@dataclass
class DescribeResult:
    """Результат одного описания картинки локальной VLM.

    Не путать с `CompareResult` (тот возвращает diff двух картинок).

    Диагностические поля (помогают тюнить max_tokens/prompt без чтения raw'а):
      * ``finish_reason`` — finish_reason от первого chunk'а
        (``stop`` | ``length`` | ``error`` | None).
      * ``usage`` — usage от первого chunk'а
        (``prompt_tokens`` / ``completion_tokens`` / ``total_tokens``).
      * ``response_char_count`` — длина content из первого chunk'а в символах.
      * ``parse_error_detail`` — короткая категория сбоя парсера
        (``no_opening_brace`` | ``markdown_reasoning`` | ``truncated_json`` |
        ``empty_content`` | ``salvage_no_safe_boundary`` | None при успехе).
      * ``full_raw_response`` — full content_text от первого chunk'а, чтобы
        caller (например, enrich_side) мог записать его на диск целиком.
        В UI/JSON это поле НЕ пишется — оно только для caller-side persistence.
    """

    status: str  # done | error | provider_unavailable | invalid_json | timeout
    provider: str = PROVIDER_NAME
    model: str = ""
    model_used: str = ""
    fallback_used: bool = False
    parsed: Optional[dict[str, Any]] = None
    raw_response_excerpt: str = ""
    duration_sec: float = 0.0
    error: Optional[str] = None
    finish_reason: Optional[str] = None
    usage: Optional[dict[str, int]] = None
    response_char_count: int = 0
    parse_error_detail: Optional[str] = None
    full_raw_response: str = ""
    # Грубая категория сбоя для retry/validation: None|ok|transport|content|model.
    # Заполняется retry-слоем (см. ``classify_describe_error``). Транспортные
    # сбои (ngrok HTML 404, 5xx, ReadError…) → ``transport`` (retryable); ответы
    # модели, не прошедшие парс/schema → ``content``; валидные API-ошибки → ``model``.
    error_class: Optional[str] = None


def _classify_parse_error(content_text: str) -> str:
    """Короткая категория, почему `parse_diff_json` вернул None.

    Используется для диагностики: ``markdown_reasoning`` сразу подсказывает,
    что модель ушла в chain-of-thought; ``truncated_json`` — что не хватило
    max_tokens; ``empty_content`` — что модель вернула пустоту.
    """
    if not content_text:
        return "empty_content"
    stripped = content_text.strip()
    if not stripped:
        return "empty_content"
    # Markdown-reasoning от mtp обычно начинается с нумерованного списка
    # ("1. **Analyze the Request:**") или с заголовка "###" и не содержит `{`.
    if "{" not in stripped:
        # Похоже на markdown reasoning, если есть характерные маркеры.
        low = stripped[:400].lower()
        markdown_markers = (
            "**analyze",
            "** analyze",
            "1.  **",
            "**task:**",
            "**output format",
            "*   **",
            "- **",
        )
        if any(m in low for m in markdown_markers) or stripped.startswith(("#", "*", "-")):
            return "markdown_reasoning"
        return "no_opening_brace"
    # Есть `{`, но parse не справился — почти всегда обрыв на max_tokens.
    if _ELLIPSIS_RE.search(stripped):
        return "truncated_json"
    # `{` есть, но нет финальной `}` — также похоже на обрыв.
    if not stripped.rstrip().endswith("}"):
        return "truncated_json"
    return "malformed_json"


def _extract_usage(data: Any) -> dict[str, int]:
    """Извлечь {prompt_tokens, completion_tokens, total_tokens} из API-ответа.

    OpenAI-compatible API кладёт usage в data["usage"]. LM Studio тоже
    возвращает их. Если поля нет — пустой dict.
    """
    if not isinstance(data, dict):
        return {}
    usage = data.get("usage")
    if not isinstance(usage, dict):
        return {}
    out: dict[str, int] = {}
    for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
        val = usage.get(key)
        if isinstance(val, int):
            out[key] = val
        elif isinstance(val, float):
            out[key] = int(val)
    return out


def _extract_finish_reason(data: Any) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    choices = data.get("choices") or []
    if not choices or not isinstance(choices[0], dict):
        return None
    fr = choices[0].get("finish_reason")
    if isinstance(fr, str):
        return fr
    return None


def _extract_content_text_from_data(data: Any) -> str:
    """Достать content_text из non-streaming OpenAI-ответа.

    content может быть строкой, multi-part списком {type:text} или (fallback)
    reasoning_content.
    """
    content_text = ""
    if isinstance(data, dict):
        choices = data.get("choices") or []
        if choices and isinstance(choices[0], dict):
            msg = choices[0].get("message") or {}
            content = msg.get("content")
            if isinstance(content, str):
                content_text = content
            elif isinstance(content, list):
                parts: list[str] = []
                for item in content:
                    if isinstance(item, dict) and isinstance(item.get("text"), str):
                        parts.append(item["text"])
                content_text = "\n".join(parts)
            if not content_text:
                reasoning = msg.get("reasoning_content")
                if isinstance(reasoning, str):
                    content_text = reasoning
    return content_text


def _finalize_describe_result(
    *,
    content_text: str,
    finish_reason: Optional[str],
    usage: dict[str, int],
    body_text: str,
    status_code: int,
    cfg: LocalGraphicLLMConfig,
    use_model: str,
    primary_model: str,
    fallback_used: bool,
    duration: float,
) -> tuple[DescribeResult, str]:
    """Построить DescribeResult из извлечённого content (общая логика для
    streaming и non-streaming путей)."""
    if status_code >= 400:
        return DescribeResult(
            status="error",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used,
            duration_sec=duration,
            raw_response_excerpt=_excerpt(body_text),
            error=f"http_{status_code}",
            finish_reason=finish_reason,
            usage=usage or None,
            response_char_count=len(body_text or ""),
            parse_error_detail="http_error",
            full_raw_response=body_text or "",
        ), body_text

    response_chars = len(content_text or "")
    parsed = parse_diff_json(content_text)
    if parsed is None:
        return DescribeResult(
            status="invalid_json",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used,
            duration_sec=duration,
            raw_response_excerpt=_excerpt(content_text or body_text),
            error="json_parse_failed",
            finish_reason=finish_reason,
            usage=usage or None,
            response_char_count=response_chars,
            parse_error_detail=_classify_parse_error(content_text),
            full_raw_response=content_text or body_text or "",
        ), content_text or body_text

    return DescribeResult(
        status="done",
        provider=cfg.provider,
        model=primary_model,
        model_used=use_model,
        fallback_used=fallback_used,
        parsed=parsed,
        raw_response_excerpt=_excerpt(content_text),
        duration_sec=duration,
        error=None,
        finish_reason=finish_reason,
        usage=usage or None,
        response_char_count=response_chars,
        parse_error_detail=None,
        full_raw_response=content_text,
    ), content_text


async def _stream_chat_completion(
    cfg: LocalGraphicLLMConfig, payload: dict[str, Any],
) -> dict[str, Any]:
    """Streaming chat completion: гонит SSE-дельты и собирает полный content.

    Назначение — не дать длинному ответу простаивать до ngrok read-timeout без
    единого байта. Транспорт видит непрерывный поток дельт, поэтому соединение
    не считается «висящим» и не падает по ReadError.

    Возвращает dict:
      * ``ok`` — стрим дал годный результат (полный ИЛИ частичный, который
        стоит парсить/salvage'ить). False → caller делает fallback на
        non-streaming;
      * ``status_code`` / ``content_text`` / ``finish_reason`` / ``usage`` /
        ``raw_text`` / ``error`` / ``partial``.

    Логика fallback (`ok=False`):
      * HTTP >= 400 на открытии стрима (сервер отверг stream-запрос);
      * 0 SSE-строк И пустой content (сервер проигнорировал stream=true или
        транспорт не поддерживает `client.stream`) → пробуем non-streaming;
      * исключение ДО получения хоть какого-то content.

    Обрыв стрима ПОСЛЕ накопления части content → ``ok=True, partial=True``:
    отдаём то, что успели собрать (finish_reason=length), чтобы salvage спас.
    """
    content_parts: list[str] = []
    raw_lines: list[str] = []
    finish_reason: Optional[str] = None
    usage: dict[str, int] = {}
    status_code = 0
    saw_sse = False

    try:
        async with httpx.AsyncClient(timeout=cfg.timeout_sec) as client:
            async with client.stream(
                "POST",
                f"{cfg.base_url}/v1/chat/completions",
                headers=_build_headers(cfg),
                json=payload,
            ) as response:
                status_code = response.status_code
                if status_code >= 400:
                    try:
                        body = await response.aread()
                        body_text = body.decode("utf-8", errors="replace")
                    except Exception:  # noqa: BLE001
                        body_text = ""
                    return {
                        "ok": False, "status_code": status_code, "content_text": "",
                        "finish_reason": None, "usage": {}, "raw_text": body_text,
                        "error": f"http_{status_code}", "partial": False,
                    }
                async for line in response.aiter_lines():
                    raw_lines.append(line or "")
                    s = (line or "").strip()
                    if not s or not s.startswith("data:"):
                        continue
                    saw_sse = True
                    data_str = s[len("data:"):].strip()
                    if data_str == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data_str)
                    except Exception:  # noqa: BLE001
                        continue
                    if not isinstance(chunk, dict):
                        continue
                    choices = chunk.get("choices") or []
                    if choices and isinstance(choices[0], dict):
                        delta = choices[0].get("delta") or {}
                        piece = delta.get("content")
                        if isinstance(piece, str):
                            content_parts.append(piece)
                        else:
                            rc = delta.get("reasoning_content")
                            if isinstance(rc, str):
                                content_parts.append(rc)
                        fr = choices[0].get("finish_reason")
                        if isinstance(fr, str) and fr:
                            finish_reason = fr
                    u = _extract_usage(chunk)
                    if u:
                        usage = u
    except Exception as exc:  # noqa: BLE001
        content_text = "".join(content_parts)
        if content_text:
            # Стрим оборвался, но часть байтов мы получили — отдаём как partial,
            # salvage_partial_json спасёт обрезанный JSON.
            return {
                "ok": True, "status_code": status_code or 200, "content_text": content_text,
                "finish_reason": finish_reason or "length", "usage": usage,
                "raw_text": "\n".join(raw_lines),
                "error": f"stream_interrupted:{type(exc).__name__}", "partial": True,
            }
        return {
            "ok": False, "status_code": 0, "content_text": "", "finish_reason": None,
            "usage": {}, "raw_text": "", "error": f"stream_error:{type(exc).__name__}:{exc}",
            "partial": False,
        }

    content_text = "".join(content_parts)
    if not saw_sse and not content_text:
        # Сервер не стримил (проигнорировал stream=true / транспорт без SSE) —
        # сигналим fallback на non-streaming.
        return {
            "ok": False, "status_code": status_code or 200, "content_text": "",
            "finish_reason": finish_reason, "usage": usage,
            "raw_text": "\n".join(raw_lines), "error": "stream_unsupported", "partial": False,
        }
    return {
        "ok": True, "status_code": status_code or 200, "content_text": content_text,
        "finish_reason": finish_reason, "usage": usage, "raw_text": "\n".join(raw_lines),
        "error": None, "partial": False,
    }


async def _describe_image_once(
    *,
    img_url: str,
    prompt: str,
    cfg: LocalGraphicLLMConfig,
    use_model: str,
    primary_model: str,
    fallback_used: bool,
    stream: Optional[bool] = None,
) -> tuple[DescribeResult, str]:
    """Один вызов к LM Studio. Возвращает (DescribeResult, content_text).

    `content_text` — это полный текст ответа модели (для последующего
    salvage), а не обрезанный excerpt. Дублируется в
    ``DescribeResult.full_raw_response`` для удобства caller'а
    (например, ``enrich_side`` пишет его на диск целиком).

    ``stream`` — использовать ли streaming. None → берётся ``cfg.stream_enabled``
    (default True). На streaming-сбое (сервер не поддержал / транспорт упал без
    единого байта) — fallback на non-streaming с warning. Частично собранный
    стрим спасается salvage'ем на уровне ``_describe_with_retry_and_fallback``.
    """
    base_payload = {
        "model": use_model,
        "max_tokens": int(cfg.max_tokens),
        "temperature": float(cfg.temperature),
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": img_url}},
                ],
            },
        ],
    }

    effective_stream = cfg.stream_enabled if stream is None else bool(stream)

    if effective_stream:
        stream_payload = dict(base_payload)
        stream_payload["stream"] = True
        # OpenAI-compatible: попросить usage в финальном чанке (LM Studio может
        # игнорировать — usage остаётся диагностическим, не критичным).
        stream_payload["stream_options"] = {"include_usage": True}
        started = time.monotonic()
        outcome = await _stream_chat_completion(cfg, stream_payload)
        if outcome.get("ok"):
            duration = time.monotonic() - started
            return _finalize_describe_result(
                content_text=outcome["content_text"],
                finish_reason=outcome["finish_reason"],
                usage=outcome["usage"] or {},
                body_text=outcome["raw_text"],
                status_code=outcome["status_code"],
                cfg=cfg,
                use_model=use_model,
                primary_model=primary_model,
                fallback_used=fallback_used,
                duration=duration,
            )
        logger.warning(
            "_describe_image_once: streaming failed (%s); falling back to non-streaming",
            outcome.get("error"),
        )

    # ── non-streaming путь (исходный + fallback после streaming-сбоя) ──
    started = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=cfg.timeout_sec) as client:
            response = await client.post(
                f"{cfg.base_url}/v1/chat/completions",
                headers=_build_headers(cfg),
                json=base_payload,
            )
    except httpx.TimeoutException as exc:
        return DescribeResult(
            status="timeout",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used,
            duration_sec=time.monotonic() - started,
            error=f"timeout:{exc}",
        ), ""
    except Exception as exc:  # noqa: BLE001
        return DescribeResult(
            status="error",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used,
            duration_sec=time.monotonic() - started,
            error=f"http_error:{type(exc).__name__}:{exc}",
        ), ""

    duration = time.monotonic() - started
    body_text = response.text or ""
    try:
        data = response.json()
    except Exception:
        data = None

    finish_reason = _extract_finish_reason(data)
    usage = _extract_usage(data)
    content_text = "" if response.status_code >= 400 else _extract_content_text_from_data(data)

    return _finalize_describe_result(
        content_text=content_text,
        finish_reason=finish_reason,
        usage=usage,
        body_text=body_text,
        status_code=response.status_code,
        cfg=cfg,
        use_model=use_model,
        primary_model=primary_model,
        fallback_used=fallback_used,
        duration=duration,
    )


# Retry/fallback константы.
# LEGACY (2026-06-06): describe-путь больше их НЕ использует — transport-повторы
# теперь конфигурируются через ``_transport_retry_plan`` (env
# STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_RETRIES/_BACKOFF/_JITTER, default
# 3 повтора с backoff 5/15/30/60s + jitter). Оставлены для обратной совместимости.
DESCRIBE_TRANSIENT_RETRY_SLEEP_SEC = 5.0
DESCRIBE_TRANSIENT_RETRY_COUNT = 1   # legacy, не используется describe-путём


CONTINUATION_PROMPT_TEMPLATE = """Это ПРОДОЛЖЕНИЕ описания одного и того же изображения из проектной/рабочей документации в строительстве.

Предыдущий chunk закончился с пометкой next_chunk_hint:
"{hint}"

ЗАДАЧА:
Продолжи описание той же схемы/чертежа. Не повторяй уже описанное. Начни именно с того места, на которое указывает next_chunk_hint выше. Опиши столько новых элементов, сколько помещается в один валидный JSON.

ПРАВИЛА ВЫВОДА (строгие):

1. Возвращай ТОЛЬКО валидный, полностью закрытый JSON. Никакого markdown, никакого текста до или после JSON, никаких комментариев `//`.
2. ЗАПРЕЩЕНО использовать многоточие в любом виде: `…` (U+2026), `...`, `etc.`, `и т. д.`, `и т.п.`, `<…>`, `[...]`, `и др.`, `и тому подобное`.
3. ЗАПРЕЩЕНО обрывать ключи или значения JSON «на полуслове». Если строка не помещается полностью — сократи её формулировку, но всегда закрывай кавычку.
4. НЕ ограничивай себя «топ-10». Опиши столько элементов, сколько успеваешь корректно вместить.
5. Если ты понимаешь, что не помещаешься, заверши текущий объект, закрой все массивы и поставь:
   * `"continues": true`;
   * `"next_chunk_hint": "что осталось показать в следующем chunk'е"`;
   * `"coverage_notes": "что охвачено именно в этом chunk'е"`.
6. Если описание полностью завершено — поставь `"continues": false`.

ОБЯЗАТЕЛЬНЫЕ ПОЛЯ:

{{
"status": "done",
"continues": true|false,
"next_chunk_hint": "" или строка,
"coverage_notes": "" или строка (что охвачено именно в этом chunk'е)
}}

ОПЦИОНАЛЬНЫЕ ПОЛЯ (заполняй только те, по которым есть НОВЫЕ данные именно в этом chunk'е — не повторяй уже описанное):
"image_kind", "summary", "design_solutions", "materials", "equipment", "numeric_parameters", "requirements", "tables", "visible_text", "comparison_relevant_facts", "uncertainties", "scheme_analysis" (с её узлами/связями/последовательностями/контурами).

ФИНАЛЬНАЯ ПРОВЕРКА:

* ни одного `…` или `...` нигде;
* все строки закрыты двойной кавычкой;
* все массивы закрыты `]`;
* все объекты закрыты `}}`;
* `continues` и `coverage_notes` присутствуют;
* если `continues: true`, `next_chunk_hint` обязательно содержит осмысленный текст.

Никакого markdown вне JSON.
"""


def _build_continuation_prompt(hint: str) -> str:
    return CONTINUATION_PROMPT_TEMPLATE.format(hint=(hint or "").strip())


# ─── Merge of chunked image descriptions ─────────────────────────────────


# Ключи скаляров, которые мы берём из ПЕРВОГО chunk'а (характеризуют всё
# изображение целиком — kind, тип схемы, общий confidence). Не перезаписываются
# последующими chunk'ами.
_DESC_SCALAR_FIRST_WINS = ("image_kind", "confidence")

# Ключи списков из верхнего уровня JSON, которые надо объединять с дедупом.
# Соответствуют schema v4_compact prompt'а (см. md_image_enrichment.py).
_DESC_LIST_FIELDS = (
    "design_solutions",
    "materials",
    "equipment",
    "requirements",
    "tables",
    "visible_text",
    "comparison_relevant_facts",
    "uncertainties",
)

# Списки списков-словарей: дедуп по name+unit+context (для numeric) / id+label /
# from+to (для connections) / name (для circuits).
_DESC_DICT_LIST_FIELDS = ("numeric_parameters",)

# scheme_analysis.{...}
_SCHEME_LIST_FIELDS = (
    "sequence_summary",
    "comparison_relevant_scheme_facts",
    "uncertainties",
)
_SCHEME_DICT_LIST_FIELDS = ("nodes", "connections", "independent_circuits")
_SCHEME_SCALAR_FIRST_WINS = ("is_scheme", "scheme_type", "flow_medium")


def _norm_str(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def _dedup_strings(xs: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for x in xs:
        if not isinstance(x, str):
            # сохраняем не-строки как есть (без дедупа)
            out.append(x)
            continue
        key = _norm_str(x)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out


def _dict_key(d: dict, key_fields: tuple[str, ...]) -> str:
    parts = []
    for f in key_fields:
        v = d.get(f)
        if isinstance(v, (str, int, float, bool)):
            parts.append(_norm_str(v))
    if not any(parts):
        # fallback на JSON-сериализацию
        try:
            return json.dumps(d, sort_keys=True, ensure_ascii=False)
        except Exception:  # noqa: BLE001
            return repr(d)
    return "|".join(parts)


def _dedup_dicts(xs: list[Any], key_fields: tuple[str, ...]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for x in xs:
        if not isinstance(x, dict):
            out.append(x)
            continue
        key = _dict_key(x, key_fields)
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out


def _merge_string(base: Any, addition: Any) -> str:
    bs = (base or "").strip() if isinstance(base, str) else ""
    asx = (addition or "").strip() if isinstance(addition, str) else ""
    if not asx:
        return bs
    if not bs:
        return asx
    if _norm_str(bs) == _norm_str(asx):
        return bs
    return f"{bs}\n\n{asx}"


def _merge_image_descriptions(base: dict, addition: dict) -> dict:
    """Сжатый merge двух image-description JSON.

    `base` — итог предыдущих chunk'ов; `addition` — новый chunk.
    Возвращает новый dict; `base` не мутируется.

    Правила:
      * скалярные поля типа image_kind / confidence — берём из base (если есть);
      * summary, coverage_notes — конкатенация без дублей;
      * массивы строк / dict'ов на верхнем уровне — append + dedup;
      * scheme_analysis — рекурсивно: scalars first-wins, lists/dict-lists merge;
      * continues / next_chunk_hint — берутся из addition (это «последний chunk»);
      * status — "done", но если хоть один chunk был _salvaged, помечаем _salvaged=True.
    """
    if not isinstance(base, dict):
        base = {}
    if not isinstance(addition, dict):
        return dict(base)

    out: dict[str, Any] = dict(base)

    # scalar first-wins, но non-None wins over None: если salvage оборвал
    # base.image_kind на null literal или модель в первом chunk'е не успела
    # дописать поле — берём из addition, если у него есть осмысленное значение.
    for k in _DESC_SCALAR_FIRST_WINS:
        if out.get(k) is None and addition.get(k) is not None:
            out[k] = addition.get(k)

    # summary, coverage_notes — concat
    out["summary"] = _merge_string(out.get("summary"), addition.get("summary"))
    out["coverage_notes"] = _merge_string(out.get("coverage_notes"), addition.get("coverage_notes"))

    # status: "done" если оба done; если хоть один partial/salvaged — пометить
    if base.get("_salvaged") or addition.get("_salvaged"):
        out["_salvaged"] = True
    # status оставляем из addition если он "done", иначе из base
    if (addition.get("status") or "").lower() == "done":
        out["status"] = "done"
    elif (out.get("status") or "").lower() != "done":
        out["status"] = addition.get("status") or out.get("status")

    # continues / next_chunk_hint — из addition (это последний chunk)
    if "continues" in addition:
        out["continues"] = bool(addition.get("continues"))
    if "next_chunk_hint" in addition:
        out["next_chunk_hint"] = addition.get("next_chunk_hint") or ""

    # списки строк
    for k in _DESC_LIST_FIELDS:
        b = out.get(k) if isinstance(out.get(k), list) else []
        a = addition.get(k) if isinstance(addition.get(k), list) else []
        if not b and not a:
            continue
        out[k] = _dedup_strings(list(b) + list(a))

    # numeric_parameters и прочие списки dict'ов
    for k in _DESC_DICT_LIST_FIELDS:
        b = out.get(k) if isinstance(out.get(k), list) else []
        a = addition.get(k) if isinstance(addition.get(k), list) else []
        if not b and not a:
            continue
        out[k] = _dedup_dicts(list(b) + list(a), key_fields=("name", "unit", "context"))

    # scheme_analysis
    b_sc = out.get("scheme_analysis") if isinstance(out.get("scheme_analysis"), dict) else {}
    a_sc = addition.get("scheme_analysis") if isinstance(addition.get("scheme_analysis"), dict) else {}
    if b_sc or a_sc:
        merged_sc: dict[str, Any] = dict(b_sc)
        # Non-None wins over None — иначе truncated chunk1 с
        # scheme_analysis.is_scheme: null теряет уточнение из continuation.
        for k in _SCHEME_SCALAR_FIRST_WINS:
            if merged_sc.get(k) is None and a_sc.get(k) is not None:
                merged_sc[k] = a_sc.get(k)
        for k in _SCHEME_LIST_FIELDS:
            b = merged_sc.get(k) if isinstance(merged_sc.get(k), list) else []
            a = a_sc.get(k) if isinstance(a_sc.get(k), list) else []
            if b or a:
                merged_sc[k] = _dedup_strings(list(b) + list(a))
        for k in _SCHEME_DICT_LIST_FIELDS:
            b = merged_sc.get(k) if isinstance(merged_sc.get(k), list) else []
            a = a_sc.get(k) if isinstance(a_sc.get(k), list) else []
            if not b and not a:
                continue
            if k == "nodes":
                merged_sc[k] = _dedup_dicts(list(b) + list(a), key_fields=("id", "label", "visible_mark"))
            elif k == "connections":
                merged_sc[k] = _dedup_dicts(list(b) + list(a), key_fields=("from", "to", "direction", "line_label"))
            else:  # independent_circuits
                merged_sc[k] = _dedup_dicts(list(b) + list(a), key_fields=("name", "sequence"))
        # is_scheme: True если хотя бы один chunk пометил True
        if a_sc.get("is_scheme") is True:
            merged_sc["is_scheme"] = True
        out["scheme_analysis"] = merged_sc

    return out


def _content_signature(parsed: dict) -> tuple:
    """Грубая «подпись» содержимого для defensive no-progress detection.

    Возвращает tuple из размеров основных list-полей. Если после merge
    continuation'а подпись не изменилась — значит continuation не добавил
    ничего нового (модель повторила то, что уже было), и продолжать цикл
    бессмысленно — отдаём partial.
    """
    if not isinstance(parsed, dict):
        return ()
    sc = parsed.get("scheme_analysis") if isinstance(parsed.get("scheme_analysis"), dict) else {}
    return (
        len(parsed.get("design_solutions") or []),
        len(parsed.get("materials") or []),
        len(parsed.get("equipment") or []),
        len(parsed.get("numeric_parameters") or []),
        len(parsed.get("visible_text") or []),
        len(parsed.get("comparison_relevant_facts") or []),
        len(parsed.get("uncertainties") or []),
        len(sc.get("nodes") or []),
        len(sc.get("connections") or []),
        len(sc.get("sequence_summary") or []),
        len(sc.get("independent_circuits") or []),
        len(sc.get("comparison_relevant_scheme_facts") or []),
        len(parsed.get("summary") or ""),
    )


def _next_chunk_hint(parsed: dict) -> str:
    if not isinstance(parsed, dict):
        return ""
    v = parsed.get("next_chunk_hint")
    if not isinstance(v, str):
        return ""
    v = v.strip()
    # placeholder'ы от модели — не считаем валидным hint
    if v in ("", "<…>", "<...>", "...", "…"):
        return ""
    return v


def _wants_continuation(parsed: dict) -> bool:
    if not isinstance(parsed, dict):
        return False
    return bool(parsed.get("continues")) and bool(_next_chunk_hint(parsed))


def _should_try_fallback_after_invalid_json(parse_error_detail: Optional[str]) -> bool:
    """Решить, имеет ли смысл звать fallback после invalid_json primary'я.

    Эмпирически (benchmark 2026-05-26): fallback `qwen3.6-35b-a3b-mtp`
    стабильно срывается в markdown reasoning на больших prompt'ах. Если
    primary вернул *обрезанный JSON* (`truncated_json` / `malformed_json`),
    salvage_partial_json на нём почти всегда успешен — нет смысла тратить
    ещё 30s на fallback, который вернёт markdown без `{` и забьёт salvage
    более длинной нерелевантной строкой.

    Fallback оправдан только когда primary не дал ничего полезного:
      * `empty_content`            — модель вернула пусто;
      * `markdown_reasoning`       — primary тоже ушёл в reasoning;
      * `no_opening_brace`         — нет JSON-якоря, salvage бессмысленен;
      * `http_error`               — транспорт упал;
      * None                       — на всякий случай (defensive default).
    """
    if not parse_error_detail:
        return True
    detail = (parse_error_detail or "").lower()
    if detail in ("truncated_json", "malformed_json", "salvaged_from_invalid_json", "salvage_no_safe_boundary"):
        return False
    return True


def _pick_salvage_candidate(contents: list[tuple[str, str]]) -> tuple[str, str]:
    """Выбрать лучший content_text для salvage'а из list[(model_label, content)].

    Старый подход (max длины) ломался, когда mtp-fallback писал длинное
    markdown-reasoning, перекрывая обрезанный JSON primary'я. Новый
    приоритет:

      1. Среди тех, кто содержит `{` (есть хотя бы тень JSON), берём самый
         длинный — это даёт salvage'у больше материала для парсинга.
      2. Если ни в одном нет `{` — берём самый длинный как есть (terminal
         fallback; salvage всё равно вернёт None, но мы хотя бы запишем
         характеристики).
      3. Пустой пул — `("", "")`.
    """
    if not contents:
        return ("", "")
    with_brace = [(label, c) for label, c in contents if "{" in (c or "")]
    if with_brace:
        return max(with_brace, key=lambda x: len(x[1] or ""))
    return max(contents, key=lambda x: len(x[1] or ""))


async def _describe_with_retry_and_fallback(
    *,
    img_url: str,
    prompt: str,
    cfg: LocalGraphicLLMConfig,
    primary_model: str,
    fallback_used_hint: bool,
    allow_fallback: bool,
    pinned_model: Optional[str] = None,
    stream: Optional[bool] = None,
) -> DescribeResult:
    """retry + (conditional) fallback + salvage для одного chunk'а.

    Параметры:
      pinned_model: если задан, ходим ТОЛЬКО к этой модели (для
        continuation, чтобы не переключаться между primary и fallback
        посреди описания одной картинки).
      allow_fallback: разрешён ли fallback на cfg.fallback_model при
        invalid_json (по умолчанию True для первого chunk'а, False для
        continuation).

    Conditional fallback (2026-05-26):
      Fallback вызывается ТОЛЬКО если у primary нет шансов на salvage —
      см. ``_should_try_fallback_after_invalid_json``. Это убирает ~30s
      холостого вызова mtp на каждом обрезанном primary JSON и убирает
      загрязнение salvage-кандидата markdown reasoning'ом.
    """
    fallback_model = (cfg.fallback_model or "").strip()
    if pinned_model:
        candidates: list[tuple[str, bool]] = [(pinned_model, fallback_used_hint)]
        fallback_available = False
    else:
        candidates = [(primary_model, fallback_used_hint)]
        fallback_available = bool(
            allow_fallback and fallback_model and fallback_model.lower() != primary_model.lower()
        )

    last_result: Optional[DescribeResult] = None
    last_content_text = ""
    # Список (label, content) для умного выбора salvage candidate'а.
    salvage_pool: list[tuple[str, str]] = []

    cand_idx = 0
    while cand_idx < len(candidates):
        cand_model, is_fallback = candidates[cand_idx]
        result: Optional[DescribeResult] = None
        content_text: str = ""
        retries, backoff, jitter = _transport_retry_plan()
        for attempt in range(1 + retries):
            result, content_text = await _describe_image_once(
                img_url=img_url,
                prompt=prompt,
                cfg=cfg,
                use_model=cand_model,
                primary_model=primary_model,
                fallback_used=is_fallback,
                stream=stream,
            )

            if result.status == "done":
                if cand_idx > 0:
                    logger.info(
                        "describe_image_local: fallback model %s rescued after primary failure",
                        cand_model,
                    )
                return result

            # Классифицируем: транспортный сбой (ngrok 404/HTML, 5xx, ReadError,
            # timeout, пустой ответ) → RETRYABLE; content/model → нет.
            err_class = classify_describe_error(result)
            is_transport = err_class == "transport"
            body_for_log = getattr(result, "full_raw_response", "") or content_text or ""
            logger.warning(
                "describe attempt=%d/%d model=%s status=%s error=%s class=%s "
                "content_type=%s retryable=%s body_preview=%r",
                attempt + 1, retries + 1, cand_model, result.status,
                (result.error or "")[:80], err_class,
                "html" if _looks_like_transport_html(body_for_log) else "json/text",
                is_transport, body_for_log[:200],
            )

            if is_transport and attempt < retries:
                delay = _transport_retry_delay(attempt, backoff, jitter)
                logger.warning(
                    "describe_image_local: transient transport error (%s) on model %s "
                    "(attempt %d/%d), retrying after %.1fs",
                    result.error or result.status, cand_model,
                    attempt + 1, retries + 1, delay,
                )
                if delay > 0:
                    await asyncio.sleep(delay)
                continue
            break

        last_result = result
        last_content_text = content_text
        if result is not None and result.status == "invalid_json":
            salvage_pool.append((cand_model, content_text))
            # Решаем ДОБАВЛЯТЬ ли fallback в список кандидатов «по
            # потребности», а не заранее. Это избавляет от лишнего вызова
            # mtp, когда primary дал truncated_json — salvage и без mtp
            # отлично справится с обрезанным JSON.
            if (
                cand_idx == 0
                and fallback_available
                and _should_try_fallback_after_invalid_json(result.parse_error_detail)
            ):
                candidates.append((fallback_model, True))
                fallback_available = False
                logger.info(
                    "describe_image_local: primary returned invalid_json (%s), trying fallback model %s",
                    result.parse_error_detail or "json_parse_failed", fallback_model,
                )
                cand_idx += 1
                continue
            elif cand_idx == 0 and fallback_available:
                logger.info(
                    "describe_image_local: primary returned invalid_json (%s), "
                    "fallback skipped — primary content is salvage-friendly",
                    result.parse_error_detail or "json_parse_failed",
                )
                # fallback не зовём, идём прямо к salvage
                break
        break

    if last_result is not None and last_result.status == "invalid_json":
        # Берём содержимое с `{` максимальной длины (см. _pick_salvage_candidate).
        if not salvage_pool:
            salvage_pool = [(last_result.model_used or "?", last_content_text or "")]
        chosen_label, salvage_input = _pick_salvage_candidate(salvage_pool)
        salvaged = salvage_partial_json(salvage_input)
        if isinstance(salvaged, dict):
            logger.warning(
                "describe_image_local: salvaged partial JSON from %s (raw_len=%d, pool=%d)",
                chosen_label, len(salvage_input), len(salvage_pool),
            )
            return DescribeResult(
                status="partial",
                provider=cfg.provider,
                model=primary_model,
                model_used=last_result.model_used,
                fallback_used=last_result.fallback_used,
                parsed=salvaged,
                raw_response_excerpt=_excerpt(salvage_input),
                duration_sec=last_result.duration_sec,
                error="salvaged_partial_json",
                finish_reason=last_result.finish_reason,
                usage=last_result.usage,
                response_char_count=len(salvage_input or ""),
                parse_error_detail=(last_result.parse_error_detail or "salvaged_from_invalid_json"),
                full_raw_response=salvage_input or "",
            )
        # Salvage не справился — оставим parse_error_detail для диагностики
        last_result.parse_error_detail = (
            last_result.parse_error_detail or "salvage_no_safe_boundary"
        )

    if last_result is not None:
        # Финальный (исчерпавший повторы) результат: проставляем категорию,
        # чтобы caller/validation отличили транспортный сбой от content/model.
        if last_result.status not in ("done", "partial"):
            last_result.error_class = classify_describe_error(last_result)
        return last_result

    return DescribeResult(
        status="error",
        provider=cfg.provider,
        model=primary_model,
        model_used=primary_model,
        fallback_used=fallback_used_hint,
        error="no_candidate_attempted",
        error_class="transport",
    )


async def describe_image_local(
    image_path: str | Path,
    prompt: str,
    *,
    model: Optional[str] = None,
    cfg: Optional[LocalGraphicLLMConfig] = None,
    model_used_hint: Optional[str] = None,
    fallback_used_hint: bool = False,
    stream: Optional[bool] = None,
) -> DescribeResult:
    """Послать одну картинку + текстовый prompt в локальный OpenAI-compatible vision endpoint.

    Поведение:
      1. Используется primary `cfg.model`. На transient-ошибках (модель
         выгружена, http_500, timeout, network) — один retry через 5с.
      2. Если итог — `invalid_json` (`json_parse_failed`), пробуем fallback
         модель `cfg.fallback_model` (если задана и отличается). На ней
         тоже доступен retry.
      3. Если оба кандидата вернули `invalid_json` — пытаемся `salvage_partial_json`
         на полном тексте ответа. Salvaged-результат идёт как
         `status="partial"` с `parsed["_salvaged"] = True`.
      4. Если первый chunk вернул `continues: true` + `next_chunk_hint`,
         автоматически делаем до `cfg.max_continuations` дополнительных
         запросов к той же модели (без fallback) и merge'им результат.
      5. Любые иные status ('timeout', 'error') от последнего кандидата
         бросаются наружу как есть.

    Используется MD enrichment'ом для генерации structured-описания image/imagine
    блоков. Не делает diff — только описывает одно изображение.

    External paid hosts (OpenRouter / OpenAI / Gemini / Anthropic) явно
    запрещены через `_validate_base_url`.
    """
    cfg = cfg or load_local_graphic_llm_config()
    ok, reason = check_local_graphic_llm_available(cfg)
    if not ok:
        return DescribeResult(
            status="provider_unavailable",
            provider=cfg.provider or PROVIDER_NAME,
            model=model or cfg.model,
            model_used=model_used_hint or "",
            fallback_used=fallback_used_hint,
            error=f"local_graphic_llm_unavailable:{reason}",
        )

    primary_model = (model or cfg.model).strip()
    if not prompt or not prompt.strip():
        return DescribeResult(
            status="error",
            provider=cfg.provider,
            model=primary_model,
            model_used=primary_model,
            fallback_used=fallback_used_hint,
            error="empty_prompt",
        )

    img_bytes = _resize_png_to_long_side(Path(image_path), cfg.image_long_side)
    img_url = _png_bytes_to_data_url(img_bytes)

    # ── Chunk #1 — primary + fallback + salvage ─────────────────────────
    base_result = await _describe_with_retry_and_fallback(
        img_url=img_url,
        prompt=prompt,
        cfg=cfg,
        primary_model=primary_model,
        fallback_used_hint=fallback_used_hint,
        allow_fallback=True,
        pinned_model=None,
        stream=stream,
    )

    # Pass-through на non-done/partial: error/timeout/provider_unavailable.
    if base_result.status not in ("done", "partial") or not isinstance(base_result.parsed, dict):
        return base_result

    # ── Continuation loop ──────────────────────────────────────────────
    final_parsed: dict[str, Any] = dict(base_result.parsed)
    chunks_count = 1
    continued = False
    continuation_warnings: list[str] = []
    # #45: различаем salvage БАЗОВОГО chunk'а и salvage continuation-добора.
    # Только base-salvage понижает итог до partial; salvage continuation на чистом
    # done-base оставляет 'done' (база полна, добор лишь не дотянул лишние данные).
    base_salvaged = bool(base_result.status == "partial" or final_parsed.get("_salvaged"))
    continuation_salvaged = False
    used_model = base_result.model_used or primary_model

    # Аккумулируем usage / response_char_count по ВСЕМ чанкам (base +
    # continuation): continuation-вызовы реально тратят токены, а раньше в
    # итог шёл только usage первого chunk'а → недоучёт стоимости (reserc.md #46).
    def _sum_usage(acc: Any, add: Any) -> dict[str, Any]:
        out = dict(acc) if isinstance(acc, dict) else {}
        if isinstance(add, dict):
            for _k in ("prompt_tokens", "completion_tokens", "total_tokens"):
                _v = add.get(_k)
                if isinstance(_v, (int, float)):
                    out[_k] = (out.get(_k) or 0) + _v
        return out

    accum_usage = _sum_usage({}, base_result.usage)
    accum_char = base_result.response_char_count
    worst_finish = base_result.finish_reason

    # Если ответ был salvaged, поля `continues`/`next_chunk_hint`/`coverage_notes`
    # почти всегда отсутствуют в parsed (они должны были идти в самом конце JSON,
    # но генерация оборвалась раньше). Принудительно ставим continues=true и
    # синтезируем generic hint, чтобы continuation-цикл смог попросить добор.
    if base_result.status == "partial" and final_parsed.get("continues") is None:
        final_parsed["continues"] = True
        if not _next_chunk_hint(final_parsed):
            final_parsed["next_chunk_hint"] = (
                "Продолжи описание схемы/чертежа с того места, где обрыв. "
                "Допиши оставшиеся узлы, связи, числовые параметры и закрой "
                "scheme_analysis. В следующем chunk'е перечисли всё, что не вошло "
                "в первый chunk."
            )
        logger.info(
            "describe_image_local: salvaged chunk #1 had no `continues`, "
            "forcing continues=true to trigger continuation"
        )

    cap = max(0, int(cfg.max_continuations or 0))
    stop_reason = "continues_false"
    previous_hint: Optional[str] = None

    while True:
        if not _wants_continuation(final_parsed):
            stop_reason = "continues_false"
            break
        if chunks_count - 1 >= cap:
            stop_reason = "cap_reached"
            break

        hint = _next_chunk_hint(final_parsed)
        # Защита от «stuck» continuation: модель повторяет тот же hint, не
        # делая прогресса. Срабатывает на втором повторе hint'а подряд —
        # лучше отдать partial, чем впустую тратить ещё 2 запроса.
        if previous_hint is not None and _norm_str(hint) == _norm_str(previous_hint):
            warn = f"continuation_{chunks_count + 1}_skipped:hint_repeated"
            logger.warning("describe_image_local: %s", warn)
            continuation_warnings.append(warn)
            stop_reason = "hint_repeated"
            break

        cont_idx = chunks_count + 1
        logger.info(
            "describe_image_local: continuation %d/%d on model %s (hint=%r)",
            cont_idx, cap, used_model, hint[:80],
        )
        cont_prompt = _build_continuation_prompt(hint)
        cont_result = await _describe_with_retry_and_fallback(
            img_url=img_url,
            prompt=cont_prompt,
            cfg=cfg,
            primary_model=primary_model,
            fallback_used_hint=base_result.fallback_used,
            allow_fallback=False,
            pinned_model=used_model,
            stream=stream,
        )

        # Учитываем токены/символы КАЖДОГО continuation-вызова (даже неудачного —
        # запрос всё равно потрачен), finish_reason эскалируем до worst-case.
        accum_usage = _sum_usage(accum_usage, cont_result.usage)
        if isinstance(cont_result.response_char_count, int):
            accum_char = (accum_char or 0) + cont_result.response_char_count
        if cont_result.finish_reason == "length":
            worst_finish = "length"

        if cont_result.status not in ("done", "partial") or not isinstance(cont_result.parsed, dict):
            warn = f"continuation_{cont_idx}_failed:{cont_result.error or cont_result.status}"
            logger.warning("describe_image_local: %s", warn)
            continuation_warnings.append(warn)
            stop_reason = "continuation_failed"
            break

        if cont_result.status == "partial":
            warn = f"continuation_{cont_idx}_salvaged"
            logger.warning("describe_image_local: %s", warn)
            continuation_warnings.append(warn)
            continuation_salvaged = True

        # Защита от «no-op» continuation: merge должен реально что-то добавить
        # к base. Если ни одного нового узла/связи/строки не появилось —
        # модель ничего не описала, дальше просить бессмысленно.
        pre_signature = _content_signature(final_parsed)
        final_parsed = _merge_image_descriptions(final_parsed, cont_result.parsed)
        post_signature = _content_signature(final_parsed)
        if pre_signature == post_signature:
            warn = f"continuation_{cont_idx}_no_progress"
            logger.warning("describe_image_local: %s", warn)
            continuation_warnings.append(warn)
            stop_reason = "no_progress"
            chunks_count = cont_idx
            continued = True
            break

        previous_hint = hint
        chunks_count = cont_idx
        continued = True

    # Если уперлись в cap, но модель всё ещё хочет продолжать — отдельный warning.
    if stop_reason == "cap_reached" and bool(final_parsed.get("continues")):
        continuation_warnings.append(f"continuation_cap_reached:{cap}")

    # #45: continuation-only salvage не понижает чистый done-base — помечаем,
    # но не эскалируем статус.
    any_salvaged = base_salvaged or continuation_salvaged
    if continuation_salvaged and not base_salvaged:
        continuation_warnings.append("continuation_partial")

    # Финальная мета.
    final_parsed["chunks_count"] = chunks_count
    final_parsed["continued"] = continued
    if continuation_warnings:
        final_parsed["continuation_warnings"] = continuation_warnings
    if any_salvaged:
        final_parsed["_salvaged"] = True

    logger.info(
        "describe_image_local: done block in %d chunk(s), continued=%s, model=%s, "
        "stop=%s, warnings=%d, base_salvaged=%s, continuation_salvaged=%s",
        chunks_count, continued, used_model, stop_reason,
        len(continuation_warnings), base_salvaged, continuation_salvaged,
    )

    # #45: статус определяется ТОЛЬКО salvage базового chunk'а. Salvaged
    # continuation на чистом done-base → итог остаётся 'done' (+ continuation_partial).
    final_status = "partial" if base_salvaged else "done"
    final_error = "salvaged_partial_json" if base_salvaged else None

    return DescribeResult(
        status=final_status,
        provider=cfg.provider,
        model=primary_model,
        model_used=used_model,
        fallback_used=base_result.fallback_used,
        parsed=final_parsed,
        raw_response_excerpt=base_result.raw_response_excerpt,
        duration_sec=base_result.duration_sec,
        error=final_error,
        # Diagnostic поля АГРЕГИРОВАНЫ по base + continuation чанкам:
        # usage/response_char_count суммируются (continuation-вызовы тоже тратят
        # токены — иначе недоучёт стоимости), finish_reason — worst-case
        # ('length', если оборвался любой chunk). reserc.md #46.
        finish_reason=worst_finish,
        usage=accum_usage or None,
        response_char_count=accum_char,
        parse_error_detail=base_result.parse_error_detail,
        full_raw_response=base_result.full_raw_response,
    )


def config_info_for_endpoint(
    cfg: Optional[LocalGraphicLLMConfig] = None,
) -> dict[str, Any]:
    """Safe-вид конфига для GET /graphic-llm-config — без секретов.

    UI рендерит часть этих полей в config-panel'и (model, max_tokens,
    prompt_version, max_continuations), чтобы оператор видел РЕАЛЬНУЮ
    production-конфигурацию, а не догадывался по .env-сэмплам.
    """
    cfg = cfg or load_local_graphic_llm_config()
    available, reason = check_local_graphic_llm_available(cfg)
    # Импортируем поздно, чтобы не словить циклический импорт через
    # md_image_enrichment → graphic_llm_local при загрузке модуля.
    try:
        from . import md_image_enrichment as _mi
        prompt_version = _mi.PROMPT_VERSION
        compact_limits = dict(_mi.COMPACT_PROMPT_LIMITS)
    except (ImportError, AttributeError):
        prompt_version = "unknown"
        compact_limits = {}
    return {
        "provider": cfg.provider,
        "base_url_present": cfg.base_url_present,
        "model": cfg.model,
        "fallback_model": cfg.fallback_model,
        "auth": cfg.auth,
        "auth_configured": cfg.auth_configured,
        "model_load_enabled": cfg.enable_model_load,
        "available": available,
        "reason": reason,
        # Полезные read-only поля для UI (без секретов)
        "temperature": cfg.temperature,
        "max_tokens": cfg.max_tokens,
        "max_continuations": cfg.max_continuations,
        "timeout_sec": cfg.timeout_sec,
        "image_long_side": cfg.image_long_side,
        "load_context_length": cfg.load_context_length,
        # Desired fast-profile параметры загрузки (benchmark 2026-06-05).
        # Live-значения (что реально загружено) приходят из loaded_models_diagnostics.
        "load_flash_attention": cfg.load_flash_attention,
        "load_offload_kv_cache_to_gpu": cfg.load_offload_kv_cache_to_gpu,
        "load_parallel": cfg.load_parallel,
        "stream_enabled": cfg.stream_enabled,
        # Защита LM Studio моделей
        "protect_models": list(cfg.protect_models or []),
        "unload_after_request": cfg.unload_after_request,
        "unload_after_batch": cfg.unload_after_batch,
        # Production prompt info (для UI и для диагностики кеша).
        "prompt_version": prompt_version,
        "compact_prompt_limits": compact_limits,
        # Production architecture markers — чтобы UI/инженер видел, что
        # включена salvage-first + conditional-fallback политика 2026-05-26.
        "salvage_first_enabled": True,
        "conditional_fallback_enabled": True,
        "ctx_preload_mandatory": True,
    }


async def loaded_models_diagnostics(
    cfg: Optional[LocalGraphicLLMConfig] = None,
) -> dict[str, Any]:
    """Read-only диагностика LM Studio для GET /graphic-llm-config:
    какие модели загружены, какой у них ctx, OK ли primary.

    Никаких mutating операций. Никаких credentials в выводе. Если endpoint
    недоступен — endpoint_available=false, остальные поля пустые/None.
    """
    cfg = cfg or load_local_graphic_llm_config()
    desired = int(cfg.load_context_length)
    primary_key = (cfg.model or "").strip()
    protect = set(cfg.protect_models or [])

    out: dict[str, Any] = {
        "endpoint_available": False,
        "loaded_models": [],
        "desired_context_length": desired,
        "primary_loaded_ctx": None,
        "primary_context_ok": False,
        "primary_fast_profile_ok": False,
        # Desired fast-profile (для сравнения в UI/health-check)
        "desired_flash_attention": bool(cfg.load_flash_attention),
        "desired_offload_kv_cache_to_gpu": bool(cfg.load_offload_kv_cache_to_gpu),
        "desired_parallel": int(cfg.load_parallel),
        # Live snapshot ПЕРВИЧНОЙ модели — поля с именами как в задаче.
        "context_length": None,
        "flash_attention": None,
        "offload_kv_cache_to_gpu": None,
        "parallel": None,
        "parallel_ok": False,
        "ctx_ok": False,
        "fast_profile_ok": False,
    }

    # Если provider неактивен или auth не сконфигурирован — диагностика не имеет смысла.
    avail, _ = check_local_graphic_llm_available(cfg)
    if not avail:
        return out

    try:
        loaded = await _list_loaded_models(cfg)
    except Exception:  # noqa: BLE001
        return out

    out["endpoint_available"] = True

    # Группируем по model_key, выбирая instance с наибольшим ctx (чтобы достать
    # его полный config — flash/offload/parallel — для fast-profile проверки).
    by_key_best: dict[str, dict[str, Any]] = {}
    for it in loaded:
        key = it.get("model_key") or ""
        if not key:
            continue
        ctx = _loaded_context_length(it)
        prev = by_key_best.get(key)
        if prev is None or (ctx or -1) > (_loaded_context_length(prev) or -1):
            by_key_best[key] = it

    entries: list[dict[str, Any]] = []
    for key, inst in by_key_best.items():
        st = _instance_profile_status(inst, cfg)
        is_primary = bool(primary_key and key == primary_key)
        entry: dict[str, Any] = {
            "key": key,
            "ctx": st["ctx"],
            "is_primary": is_primary,
            "ctx_ok": st["ctx_ok"],
            "flash_attention": st["flash_attention"],
            "offload_kv_cache_to_gpu": st["offload_kv_cache_to_gpu"],
            "parallel": st["parallel"],
            "parallel_ok": st["parallel_ok"],
            "fast_profile_ok": st["fast_profile_ok"],
        }
        if key in protect:
            entry["protected"] = True
        entries.append(entry)
    # Сортируем: primary сверху, дальше protected, дальше по имени.
    entries.sort(key=lambda e: (
        0 if e.get("is_primary") else (1 if e.get("protected") else 2),
        str(e.get("key") or ""),
    ))
    out["loaded_models"] = entries

    if primary_key in by_key_best:
        pst = _instance_profile_status(by_key_best[primary_key], cfg)
        out["primary_loaded_ctx"] = pst["ctx"]
        out["primary_context_ok"] = pst["ctx_ok"]
        out["primary_fast_profile_ok"] = pst["fast_profile_ok"]
        # Live snapshot первичной модели (имена полей как в задаче)
        out["context_length"] = pst["ctx"]
        out["flash_attention"] = pst["flash_attention"]
        out["offload_kv_cache_to_gpu"] = pst["offload_kv_cache_to_gpu"]
        out["parallel"] = pst["parallel"]
        out["parallel_ok"] = pst["parallel_ok"]
        out["ctx_ok"] = pst["ctx_ok"]
        out["fast_profile_ok"] = pst["fast_profile_ok"]

    return out


async def _live_completion_probe(cfg: LocalGraphicLLMConfig) -> dict[str, Any]:
    """Короткий chat/completions без картинки. Возвращает {ok, reason, …}.

    Главная цель — отличить ЖИВОЙ JSON-ответ модели от HTML-страницы ngrok 404
    («tunnel not found») или иного transport-сбоя ДО запуска тяжёлого прогона.
    """
    payload = {
        "model": (cfg.model or "").strip(),
        "max_tokens": 8,
        "temperature": 0.0,
        "messages": [{"role": "user", "content": "ping"}],
    }
    try:
        async with httpx.AsyncClient(timeout=min(30, cfg.timeout_sec)) as client:
            r = await client.post(
                f"{cfg.base_url}/v1/chat/completions",
                headers=_build_headers(cfg),
                json=payload,
            )
    except httpx.TimeoutException as exc:
        return {"ok": False, "reason": f"timeout:{type(exc).__name__}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"network:{type(exc).__name__}"}
    body = r.text or ""
    if r.status_code >= 400:
        suffix = "_html" if _looks_like_transport_html(body) else ""
        return {"ok": False, "reason": f"http_{r.status_code}{suffix}", "status_code": r.status_code}
    if _looks_like_transport_html(body):
        return {"ok": False, "reason": "html_response", "status_code": r.status_code}
    try:
        data = r.json()
    except Exception:  # noqa: BLE001
        return {"ok": False, "reason": "non_json_response", "status_code": r.status_code}
    if not isinstance(data, dict) or "choices" not in data:
        return {"ok": False, "reason": "unexpected_response_shape", "status_code": r.status_code}
    return {"ok": True, "reason": "ok", "status_code": r.status_code}


async def probe_qwen_health(
    cfg: Optional[LocalGraphicLLMConfig] = None,
    *,
    do_live_test: bool = True,
    require_fast_profile: Optional[bool] = None,
) -> dict[str, Any]:
    """Read-only health-gate локального graphic-LLM перед запуском pipeline.

    Проверяет (без mutating-операций):
      * provider настроен и endpoint доступен;
      * есть хотя бы одна загруженная модель (``loaded_models`` не пуст);
      * primary ctx >= desired (``ctx_ok``);
      * ``fast_profile_ok`` (если fast-profile включён env-флагами и требуется);
      * (опц.) короткий live chat/completions — пришёл JSON, а не HTML/ngrok 404.

    Возвращает ``{ok, reason, details}``. Никогда не бросает — на любой
    непредвиденной ошибке отдаёт ``ok=False`` с понятной причиной.
    """
    try:
        cfg = cfg or load_local_graphic_llm_config()
        if require_fast_profile is None:
            require_fast_profile = bool(
                cfg.load_flash_attention or cfg.load_offload_kv_cache_to_gpu
            )
        details: dict[str, Any] = {}
        avail, areason = check_local_graphic_llm_available(cfg)
        if not avail:
            return {"ok": False, "reason": f"provider_unavailable:{areason}", "details": details}

        # Новый LM Studio: native management (/api/v1/models) отсутствует. При
        # enable_model_load=false модели предзагружены на сервере — не требуем
        # loaded_models-диагностику, полагаемся на live /v1/chat/completions
        # (единственный кросс-серверный сигнал живости).
        if not cfg.enable_model_load:
            details["model_load_enabled"] = False
            if do_live_test:
                live = await _live_completion_probe(cfg)
                details["live_test"] = live
                if not live.get("ok"):
                    return {"ok": False, "reason": f"live_test_failed:{live.get('reason')}",
                            "details": details}
            return {"ok": True, "reason": "ok", "details": details}

        try:
            diag = await loaded_models_diagnostics(cfg)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "reason": f"diagnostics_failed:{type(exc).__name__}",
                    "details": details}
        details.update({
            "endpoint_available": bool(diag.get("endpoint_available")),
            "loaded_models_count": len(diag.get("loaded_models") or []),
            "ctx_ok": bool(diag.get("ctx_ok")),
            "fast_profile_ok": bool(diag.get("fast_profile_ok")),
            "primary_loaded_ctx": diag.get("primary_loaded_ctx"),
            "require_fast_profile": bool(require_fast_profile),
        })
        if not diag.get("endpoint_available"):
            return {"ok": False, "reason": "endpoint_unavailable", "details": details}
        if not (diag.get("loaded_models") or []):
            return {"ok": False, "reason": "no_model_loaded", "details": details}
        if not diag.get("ctx_ok"):
            return {"ok": False, "reason": "ctx_below_desired", "details": details}
        if require_fast_profile and not diag.get("fast_profile_ok"):
            return {"ok": False, "reason": "fast_profile_not_loaded", "details": details}
        if do_live_test:
            live = await _live_completion_probe(cfg)
            details["live_test"] = live
            if not live.get("ok"):
                return {"ok": False, "reason": f"live_test_failed:{live.get('reason')}",
                        "details": details}
        return {"ok": True, "reason": "ok", "details": details}
    except Exception as exc:  # noqa: BLE001 — health gate must never raise
        return {"ok": False, "reason": f"probe_error:{type(exc).__name__}", "details": {}}


__all__ = [
    "PROVIDER_NAME",
    "GRAPHIC_DIFF_LOCAL_PROMPT",
    "LocalGraphicLLMConfig",
    "CompareResult",
    "load_local_graphic_llm_config",
    "check_local_graphic_llm_available",
    "ensure_lmstudio_model_loaded",
    "compare_images_local",
    "describe_image_local",
    "DescribeResult",
    "classify_describe_error",
    "parse_diff_json",
    "salvage_partial_json",
    "config_info_for_endpoint",
    "loaded_models_diagnostics",
    "probe_qwen_health",
    "snapshot_loaded_models",
    "cleanup_local_graphic_llm",
]
