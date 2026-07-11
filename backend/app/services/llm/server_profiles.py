"""Профили LLM-серверов и переключение маршрутизации всего пайплайна.

Задача: дать оператору переключать ВЕСЬ алгоритм между двумя LLM-серверами —
старым (LM Studio за ngrok, Basic Auth, нативное управление) и новым
(01.vibe, Bearer, OpenAI /v1, сервер сам держит модели).

Механика переключения = переписать ключи маршрутизации в `.env` и рестартовать
backend (config.py запекает эндпоинт при импорте через `load_dotenv()`), поэтому
корректно перевести все точки резолва (текст-enrichment + графика) можно только
рестартом. Секреты (NGROK creds, bearer-токен) в `.env` держатся ОБА сразу и
профилем не трогаются — переключается только активный режим/URL.

Рецепт значений совпадает с задокументированным откатом в комментариях `.env`.
"""

from __future__ import annotations

import os
import re
import shutil
import urllib.request
import base64
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.app.core.config import ROOT_DIR

ENV_PATH = Path(ROOT_DIR) / ".env"

# URL-ы можно переопределить через env (ngrok-free URL может смениться при
# перезапуске туннеля — тогда достаточно выставить override без правки кода).
_NGROK_URL = os.environ.get(
    "SERVER_PROFILE_NGROK_BASE_URL", "https://louvred-madie-gigglier.ngrok-free.dev"
).rstrip("/")
_VIBE_URL = os.environ.get(
    "SERVER_PROFILE_VIBE_BASE_URL", "https://01.vibe.cloud-ip.cc"
).rstrip("/")

# Ключи маршрутизации, которые различают серверы. Оба профиля задают ОДИН и тот
# же набор ключей (инверсии друг друга) — чтобы переключение было полным.
PROFILES: dict[str, dict[str, Any]] = {
    "old_ngrok": {
        "id": "old_ngrok",
        "label": "Старый сервер (ngrok)",
        "short": "ngrok",
        "description": (
            "LM Studio за ngrok, Basic Auth, нативный транспорт. Поддерживает "
            "нативное управление моделями (reload/load)."
        ),
        "base_url": _NGROK_URL,
        "auth_mode": "basic",
        "env": {
            "CHANDRA_BASE_URL": _NGROK_URL,
            "CHANDRA_AUTH_MODE": "basic",
            "CHANDRA_CHAT_TRANSPORT": "native",
            "LMSTUDIO_BASE_URL": f"{_NGROK_URL}/v1",
            "STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL": _NGROK_URL,
            "STAGE_COMPARISON_GRAPHIC_LLM_AUTH": "basic",
            "STAGE_COMPARISON_GRAPHIC_LLM_ENABLE_MODEL_LOAD": "true",
        },
    },
    "new_vibe": {
        "id": "new_vibe",
        "label": "Новый сервер (01.vibe)",
        "short": "01.vibe",
        "description": (
            "Удалённый LM Studio, Bearer-токен, OpenAI /v1. Нативное управление "
            "недоступно — сервер сам держит модели в VRAM."
        ),
        "base_url": _VIBE_URL,
        "auth_mode": "bearer",
        "env": {
            "CHANDRA_BASE_URL": _VIBE_URL,
            "CHANDRA_AUTH_MODE": "bearer",
            "CHANDRA_CHAT_TRANSPORT": "openai_completions",
            "LMSTUDIO_BASE_URL": f"{_VIBE_URL}/v1",
            "STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL": _VIBE_URL,
            "STAGE_COMPARISON_GRAPHIC_LLM_AUTH": "bearer",
            "STAGE_COMPARISON_GRAPHIC_LLM_ENABLE_MODEL_LOAD": "false",
        },
    },
}

PROFILE_ORDER = ["old_ngrok", "new_vibe"]
_SECRET_HINT = re.compile(r"(TOKEN|KEY|PASS|SECRET)", re.I)


def _redact(key: str, val: str) -> str:
    return "***" if _SECRET_HINT.search(key) else val


def _current_base_url() -> str:
    """Живой эндпоинт текущего процесса (что реально маршрутизирует сейчас)."""
    raw = os.environ.get("CHANDRA_BASE_URL") or os.environ.get("LMSTUDIO_BASE_URL", "")
    return raw.rstrip("/").removesuffix("/v1").rstrip("/")


def active_profile_id() -> str | None:
    """Определить активный профиль по живому CHANDRA_BASE_URL процесса."""
    current = _current_base_url()
    for pid in PROFILE_ORDER:
        if PROFILES[pid]["base_url"].rstrip("/") == current:
            return pid
    return None


def list_profiles() -> dict[str, Any]:
    active = active_profile_id()
    return {
        "active_id": active,
        "current_base_url": _current_base_url(),
        "profiles": [
            {
                "id": p["id"],
                "label": p["label"],
                "short": p["short"],
                "description": p["description"],
                "base_url": p["base_url"],
                "auth_mode": p["auth_mode"],
                "active": p["id"] == active,
            }
            for p in (PROFILES[k] for k in PROFILE_ORDER)
        ],
    }


def _rewrite_env(target_env: dict[str, str]) -> list[dict[str, str]]:
    """Построчно переписать ключи в .env (backup + add-if-missing). Возвращает diff."""
    text = ENV_PATH.read_text()
    lines = text.splitlines(keepends=True)
    seen: dict[str, bool] = {k: False for k in target_env}
    diff: list[dict[str, str]] = []
    out: list[str] = []
    for ln in lines:
        m = re.match(r"^([A-Z0-9_]+)=(.*?)(\r?\n?)$", ln)
        if m and m.group(1) in target_env and not seen[m.group(1)]:
            key, old, eol = m.group(1), m.group(2), m.group(3)
            new = target_env[key]
            seen[key] = True
            if old != new:
                diff.append({"key": key, "old": _redact(key, old), "new": _redact(key, new)})
            out.append(f"{key}={new}{eol}")
        else:
            out.append(ln)
    for key, ok in seen.items():
        if not ok:
            new = target_env[key]
            diff.append({"key": key, "old": "(нет)", "new": _redact(key, new)})
            out.append(f"{key}={new}\n")

    backup = ENV_PATH.with_name(
        f".env.backup.before-profile-switch-{datetime.now():%Y%m%d-%H%M%S}"
    )
    shutil.copy2(ENV_PATH, backup)
    ENV_PATH.write_text("".join(out))
    return diff


def apply_profile(profile_id: str) -> dict[str, Any]:
    """Записать env-набор профиля в .env. НЕ рестартует backend (это делает роутер)."""
    if profile_id not in PROFILES:
        raise ValueError(f"Неизвестный профиль: {profile_id!r}")
    profile = PROFILES[profile_id]
    diff = _rewrite_env(profile["env"])
    return {
        "profile_id": profile_id,
        "label": profile["label"],
        "changed": diff,
        "changed_count": len([d for d in diff if d["old"] != d["new"]]),
    }


def _probe(base_url: str, auth_mode: str, timeout: float = 8.0) -> dict[str, Any]:
    """Быстрая проба /v1/models с корректной авторизацией под сервер."""
    url = base_url.rstrip("/") + "/v1/models"
    headers = {"ngrok-skip-browser-warning": "true"}
    if auth_mode == "bearer":
        token = os.environ.get("CHANDRA_BEARER_TOKEN") or os.environ.get("LMSTUDIO_API_KEY", "")
        if token:
            headers["Authorization"] = f"Bearer {token}"
    else:  # basic
        user = os.environ.get("NGROK_AUTH_USER", "")
        pwd = os.environ.get("NGROK_AUTH_PASS", "")
        if user or pwd:
            tok = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            headers["Authorization"] = f"Basic {tok}"
    started = datetime.now()
    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = json.loads(r.read() or b"{}")
        latency = int((datetime.now() - started).total_seconds() * 1000)
        count = len(body.get("data", []) or [])
        return {"alive": True, "http": 200, "model_count": count, "latency_ms": latency}
    except Exception as exc:  # noqa: BLE001
        latency = int((datetime.now() - started).total_seconds() * 1000)
        return {"alive": False, "error": f"{type(exc).__name__}: {str(exc)[:160]}", "latency_ms": latency}


def probe_all() -> dict[str, Any]:
    """Health обоих серверов — чтобы UI показал, какой жив, ДО переключения."""
    active = active_profile_id()
    result: dict[str, Any] = {"active_id": active, "probes": {}}
    for pid in PROFILE_ORDER:
        p = PROFILES[pid]
        result["probes"][pid] = _probe(p["base_url"], p["auth_mode"])
    return result
