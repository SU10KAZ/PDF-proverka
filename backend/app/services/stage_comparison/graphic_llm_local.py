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
  STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS=1800
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
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import httpx

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
    timeout_sec: int
    image_long_side: int
    auth: str
    enable_model_load: bool
    load_context_length: int
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

    @property
    def is_active(self) -> bool:
        return self.provider == PROVIDER_NAME

    @property
    def auth_configured(self) -> bool:
        if self.auth != "basic":
            return True
        return bool(self.basic_user and self.basic_pass)

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
    return LocalGraphicLLMConfig(
        provider=provider,
        base_url=os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", "").rstrip("/"),
        model=os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "").strip(),
        fallback_model=os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_FALLBACK_MODEL", "").strip(),
        temperature=_env_float("STAGE_COMPARISON_GRAPHIC_LLM_TEMPERATURE", 0.0),
        max_tokens=_env_int("STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS", 1800),
        timeout_sec=_env_int("STAGE_COMPARISON_GRAPHIC_LLM_TIMEOUT_SEC", 300),
        image_long_side=_env_int("STAGE_COMPARISON_GRAPHIC_LLM_IMAGE_LONG_SIDE", 1100),
        auth=os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_AUTH", "basic").strip().lower() or "basic",
        enable_model_load=_env_bool("STAGE_COMPARISON_GRAPHIC_LLM_ENABLE_MODEL_LOAD", True),
        load_context_length=_env_int("STAGE_COMPARISON_GRAPHIC_LLM_LOAD_CONTEXT_LENGTH", 16000),
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
    if cfg.auth == "basic" and not cfg.auth_configured:
        return False, "basic_auth_credentials_missing"
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
    headers: dict[str, str] = {
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


async def _load_model_with_config(
    cfg: LocalGraphicLLMConfig,
    model: str,
    snapshot_config: dict[str, Any],
) -> tuple[bool, str]:
    """POST /api/v1/models/load с конфигом из snapshot (для restore)."""
    body: dict[str, Any] = {
        "model": model,
        "context_length": int(snapshot_config.get("context_length") or cfg.load_context_length),
        "flash_attention": bool(snapshot_config.get("flash_attention", True)),
        "offload_kv_cache_to_gpu": bool(snapshot_config.get("offload_kv_cache_to_gpu", True)),
        "echo_load_config": True,
    }
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
    """POST /api/v1/models/load. Возвращает (ok, message)."""
    body = {
        "model": model,
        "context_length": int(cfg.load_context_length),
        "flash_attention": True,
        "offload_kv_cache_to_gpu": True,
        "echo_load_config": True,
    }
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
            # Берём первый instance: pick наиболее «жирный» ctx
            best_inst = max(
                same_model,
                key=lambda it: (_loaded_context_length(it) or -1),
            )
            actual_ctx = _loaded_context_length(best_inst)
            if actual_ctx is None:
                # ctx unknown — backward-compat: считаем OK, не трогаем.
                return {
                    "ok": True,
                    "model_used": model_name,
                    "fallback_used": False,
                    "endpoint_available": True,
                    "messages": ["already_loaded:ctx_unknown"],
                }
            if actual_ctx >= desired_ctx:
                return {
                    "ok": True,
                    "model_used": model_name,
                    "fallback_used": False,
                    "endpoint_available": True,
                    "messages": [f"already_loaded:ctx={actual_ctx}"],
                    "actual_ctx": actual_ctx,
                    "desired_ctx": desired_ctx,
                }
            # ctx mismatch: нужно reload
            protect = set(cfg.protect_models or [])
            if model_name in protect:
                return {
                    "ok": False,
                    "status": "error",
                    "reason": "context_length_mismatch_protected",
                    "model_used": model_name,
                    "fallback_used": False,
                    "endpoint_available": True,
                    "messages": [
                        f"ctx_mismatch_protected:actual={actual_ctx}<desired={desired_ctx}",
                    ],
                    "desired_ctx": desired_ctx,
                    "actual_ctx": actual_ctx,
                }
            # unload bad instances для этой модели
            for inst in same_model:
                inst_id = inst.get("instance_id") or ""
                if not inst_id:
                    continue
                u_ok, u_msg = await _unload_instance(cfg, inst_id)
                messages.append(f"unload_low_ctx:{model_name}:{u_msg}")
            # reload c desired ctx
            ok, msg = await _load_model(cfg, model_name)
            messages.append(f"reload_with_desired_ctx:{msg}")
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
                }
            # verify
            verify_loaded = await _list_loaded_models(cfg)
            verify_same = [it for it in verify_loaded if it.get("model_key") == model_name]
            verify_ctx = None
            if verify_same:
                verify_ctx = _loaded_context_length(max(
                    verify_same,
                    key=lambda it: (_loaded_context_length(it) or -1),
                ))
            if verify_ctx is not None and verify_ctx >= desired_ctx:
                messages.append(f"verify_ok:ctx={verify_ctx}")
                return {
                    "ok": True,
                    "model_used": model_name,
                    "fallback_used": False,
                    "endpoint_available": True,
                    "messages": messages,
                    "desired_ctx": desired_ctx,
                    "actual_ctx": verify_ctx,
                }
            # ctx по-прежнему мал/unknown → жёсткая ошибка
            return {
                "ok": False,
                "status": "error",
                "reason": "context_length_mismatch",
                "model_used": model_name,
                "fallback_used": False,
                "endpoint_available": True,
                "messages": messages + [f"verify_fail:ctx={verify_ctx}"],
                "desired_ctx": desired_ctx,
                "actual_ctx": verify_ctx if verify_ctx is not None else actual_ctx,
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


async def describe_image_local(
    image_path: str | Path,
    prompt: str,
    *,
    model: Optional[str] = None,
    cfg: Optional[LocalGraphicLLMConfig] = None,
    model_used_hint: Optional[str] = None,
    fallback_used_hint: bool = False,
) -> DescribeResult:
    """Послать одну картинку + текстовый prompt в локальный OpenAI-compatible vision endpoint.

    Используется MD enrichment'ом для генерации structured-описания image/imagine
    блоков (Qwen 35B). Не делает diff — только описывает одно изображение.

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
    use_model = (model_used_hint or primary_model).strip()
    if not prompt or not prompt.strip():
        return DescribeResult(
            status="error",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used_hint,
            error="empty_prompt",
        )

    img_bytes = _resize_png_to_long_side(Path(image_path), cfg.image_long_side)
    img_url = _png_bytes_to_data_url(img_bytes)

    payload = {
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

    started = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=cfg.timeout_sec) as client:
            response = await client.post(
                f"{cfg.base_url}/v1/chat/completions",
                headers=_build_headers(cfg),
                json=payload,
            )
    except httpx.TimeoutException as exc:
        return DescribeResult(
            status="timeout",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used_hint,
            duration_sec=time.monotonic() - started,
            error=f"timeout:{exc}",
        )
    except Exception as exc:  # noqa: BLE001
        return DescribeResult(
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
        return DescribeResult(
            status="error",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used_hint,
            duration_sec=duration,
            raw_response_excerpt=_excerpt(body_text),
            error=f"http_{response.status_code}",
        )

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

    parsed = parse_diff_json(content_text)
    if parsed is None:
        return DescribeResult(
            status="invalid_json",
            provider=cfg.provider,
            model=primary_model,
            model_used=use_model,
            fallback_used=fallback_used_hint,
            duration_sec=duration,
            raw_response_excerpt=_excerpt(content_text or body_text),
            error="json_parse_failed",
        )

    return DescribeResult(
        status="done",
        provider=cfg.provider,
        model=primary_model,
        model_used=use_model,
        fallback_used=fallback_used_hint,
        parsed=parsed,
        raw_response_excerpt=_excerpt(content_text),
        duration_sec=duration,
        error=None,
    )


def config_info_for_endpoint(
    cfg: Optional[LocalGraphicLLMConfig] = None,
) -> dict[str, Any]:
    """Safe-вид конфига для GET /graphic-llm-config — без секретов."""
    cfg = cfg or load_local_graphic_llm_config()
    available, reason = check_local_graphic_llm_available(cfg)
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
        "timeout_sec": cfg.timeout_sec,
        "image_long_side": cfg.image_long_side,
        "load_context_length": cfg.load_context_length,
        # Защита LM Studio моделей
        "protect_models": list(cfg.protect_models or []),
        "unload_after_request": cfg.unload_after_request,
        "unload_after_batch": cfg.unload_after_batch,
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

    # Группируем по model_key, выбирая наибольший ctx из загруженных instances.
    by_key: dict[str, int | None] = {}
    for it in loaded:
        key = it.get("model_key") or ""
        if not key:
            continue
        ctx = _loaded_context_length(it)
        prev = by_key.get(key)
        if prev is None or (ctx is not None and (prev is None or ctx > prev)):
            by_key[key] = ctx

    entries: list[dict[str, Any]] = []
    for key, ctx in by_key.items():
        is_primary = bool(primary_key and key == primary_key)
        ctx_ok = bool(ctx is not None and ctx >= desired)
        entry: dict[str, Any] = {
            "key": key,
            "ctx": ctx,
            "is_primary": is_primary,
            "ctx_ok": ctx_ok,
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

    if primary_key in by_key:
        out["primary_loaded_ctx"] = by_key[primary_key]
        out["primary_context_ok"] = bool(
            by_key[primary_key] is not None and by_key[primary_key] >= desired
        )

    return out


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
    "parse_diff_json",
    "config_info_for_endpoint",
    "loaded_models_diagnostics",
    "snapshot_loaded_models",
    "cleanup_local_graphic_llm",
]
