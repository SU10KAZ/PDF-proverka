"""Удаление секретов из строк логов и событий.

Инвариант I-12 техпроекта уточнён проектным решением: редактируем при ЗАПИСИ
в outbox (то есть раньше, чем «перед отправкой»), потому что outbox — файл на
диске стороннего VPS, и хранить в нём секреты в открытом виде не менее опасно,
чем передавать. Центр прогоняет тот же редактор повторно — второй проход стоит
копейки.

Правило безопасного отказа: если редактор упал, строка НЕ пишется вовсе —
вместо неё уходит отметка о выброшенных байтах. Пропустить сырую строку
«на всякий случай» нельзя.

Модуль намеренно не имеет зависимостей от backend — он копируется в пакет
audit_worker как есть (см. audit_worker/redaction.py).
"""
from __future__ import annotations

import re
from typing import Iterable

# Имена переменных окружения, значения которых нельзя выпускать наружу.
SECRET_ENV_NAME_PATTERNS = (
    r"[A-Z0-9_]*TOKEN",
    r"[A-Z0-9_]*SECRET",
    r"[A-Z0-9_]*PASSWORD",
    r"[A-Z0-9_]*API_KEY",
    r"[A-Z0-9_]*_KEY",
    r"PORTAL_[A-Z0-9_]+",
    r"OPENROUTER_[A-Z0-9_]+",
    r"ANTHROPIC_[A-Z0-9_]+",
    r"CLAUDE_CODE_OAUTH_TOKEN",
    r"AWS_[A-Z0-9_]*",
)

_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(" + "|".join(SECRET_ENV_NAME_PATTERNS) + r")\s*[:=]\s*(\"[^\"]*\"|'[^']*'|\S+)"
)
# Схема заголовка — любая: Basic пропускался насквозь, а base64 «логин:пароль»
# читается тривиально.
_AUTH_HEADER_RE = re.compile(
    r"(?i)\b(authorization\s*:\s*)(bearer|basic|token|digest|apikey)?\s*\S+"
)
# Ключи в JSON и в repr словаря Python: кавычки бывают и одинарные.
_JSON_SECRET_RE = re.compile(
    r"(?i)([\"'])(token|secret|password|passwd|api_key|apikey|access_key|"
    r"worker_token|execution_token|claim_secret|bootstrap_secret|session)\1"
    r"\s*:\s*([\"'])[^\"']*\3"
)
# Явные форматы ключей провайдеров.
_KEYLIKE_RE = re.compile(
    r"\b(sk-ant-[A-Za-z0-9_\-]{8,}|sk-[A-Za-z0-9_\-]{20,}|ghp_[A-Za-z0-9]{20,}"
    r"|AIza[A-Za-z0-9_\-]{20,}|xox[abprs]-[A-Za-z0-9\-]{10,}"
    # JWT: три base64url-сегмента через точку.
    r"|eyJ[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{4,}\.[A-Za-z0-9_\-]{4,})\b"
)
# Наши собственные префиксы токенов.
_OWN_TOKEN_RE = re.compile(r"\b(wtk_|etk_|clm_)[A-Za-z0-9_\-]{8,}")
# Cookie целиком: разбирать её значения бессмысленно — они все чувствительные.
_COOKIE_RE = re.compile(r"(?i)\b(set-)?cookie\s*:\s*\S.*")
# Учётные данные внутри URL (scheme://user:pass@host).
_URL_CRED_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s/@:]+:[^\s/@]+@")
# Приватные ключи в PEM: вырезаем весь блок, а не только заголовок.
_PEM_RE = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
    re.DOTALL,
)
# Домашние пути чужой машины — не секрет, но лишняя информация о хосте.
_HOME_PATH_RE = re.compile(r"/home/[A-Za-z0-9._-]+/")

REDACTED = "<redacted>"


def _redact_once(text: str, extra_literals: Iterable[str]) -> str:
    out = _PEM_RE.sub("<redacted:private_key>", text)
    out = _AUTH_HEADER_RE.sub(
        lambda m: f"{m.group(1)}{(m.group(2) + ' ') if m.group(2) else ''}{REDACTED}", out
    )
    out = _COOKIE_RE.sub("Cookie: " + REDACTED, out)
    out = _URL_CRED_RE.sub(r"\1<redacted>:<redacted>@", out)
    out = _ASSIGNMENT_RE.sub(lambda m: f"{m.group(1)}=<redacted:{m.group(1)}>", out)
    out = _JSON_SECRET_RE.sub(
        lambda m: f"{m.group(1)}{m.group(2)}{m.group(1)}: {m.group(3)}{REDACTED}{m.group(3)}",
        out,
    )
    out = _KEYLIKE_RE.sub(REDACTED, out)
    out = _OWN_TOKEN_RE.sub(lambda m: m.group(1) + REDACTED, out)
    out = _HOME_PATH_RE.sub("~/", out)
    for literal in extra_literals:
        if literal and len(literal) >= 8 and literal in out:
            out = out.replace(literal, REDACTED)
    return out


def redact(text: str, *, extra_literals: Iterable[str] = ()) -> str:
    """Вычистить секреты. При любой внутренней ошибке — вернуть заглушку.

    `extra_literals` — конкретные значения, известные вызывающему (собственный
    токен воркера, execution token текущей попытки).
    """
    if not text:
        return text
    try:
        return _redact_once(text, extra_literals)
    except Exception:  # noqa: BLE001 — намеренно широкий: молча пропускать сырое нельзя
        return f"<redaction_failed: {len(text.encode('utf-8', 'ignore'))} bytes dropped>"


def redact_mapping(data: dict, *, extra_literals: Iterable[str] = ()) -> dict:
    """Рекурсивно очистить строковые значения словаря (payload события)."""
    try:
        return _redact_value(data, tuple(extra_literals))  # type: ignore[return-value]
    except Exception:  # noqa: BLE001
        return {"redaction_failed": True}


def _redact_value(value, extra: tuple):
    if isinstance(value, str):
        return redact(value, extra_literals=extra)
    if isinstance(value, dict):
        return {k: _redact_value(v, extra) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_value(v, extra) for v in value]
    return value
