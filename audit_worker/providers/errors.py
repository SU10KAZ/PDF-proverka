"""Классификация отказов провайдера (§27 задания).

Зачем отдельный модуль. Отказ провайдера и отказ воркера — разные новости, и
первый НЕ должен превращаться во второй. Агент обязан продолжать heartbeat,
исполнитель — работать, карточка VPS — оставаться online; «сломался» в этом
случае только провайдер, и говорить об этом нужно отдельной строкой.

Классификация намеренно не пытается быть умной: она смотрит на код возврата и
на закрытый набор признаков в тексте. Всё, что не опознано, попадает в
`unknown` — это честнее, чем натянуть чужую ошибку на знакомый код.
"""
from __future__ import annotations

import re
from typing import Any, Optional

ERR_AUTH_REQUIRED = "auth_required"
ERR_RATE_LIMITED = "rate_limited"
ERR_COOLDOWN = "cooldown"
ERR_NETWORK = "network_error"
ERR_PROVIDER_UNAVAILABLE = "provider_unavailable"
ERR_CLI_MISSING = "cli_missing"
ERR_INCOMPATIBLE_CLI = "incompatible_cli"
ERR_MALFORMED_STATUS = "malformed_status"
ERR_POLICY_BLOCKED = "policy_blocked"
ERR_TIMEOUT = "timeout"
ERR_UNKNOWN = "unknown"

PROVIDER_ERROR_CODES: tuple[str, ...] = (
    ERR_AUTH_REQUIRED,
    ERR_RATE_LIMITED,
    ERR_COOLDOWN,
    ERR_NETWORK,
    ERR_PROVIDER_UNAVAILABLE,
    ERR_CLI_MISSING,
    ERR_INCOMPATIBLE_CLI,
    ERR_MALFORMED_STATUS,
    ERR_POLICY_BLOCKED,
    ERR_TIMEOUT,
    ERR_UNKNOWN,
)

#: Признаки в тексте. Порядок ВАЖЕН: более специфичные идут раньше. «rate
#: limit» проверяется до «limit», иначе безобидное «context limit» уехало бы в
#: rate_limited и оператор увидел бы исчерпанную подписку там, где её нет.
_TEXT_PATTERNS: tuple[tuple[str, "re.Pattern[str]"], ...] = (
    (ERR_AUTH_REQUIRED, re.compile(
        r"(?i)\b(not logged in|log ?in required|login expired|unauthorized|"
        r"authentication required|authentication failed|invalid api key|"
        r"oauth[_ ]org[_ ]not[_ ]allowed|please run /login|run claude auth login|"
        r"account authentication required)\b"
    )),
    (ERR_RATE_LIMITED, re.compile(
        r"(?i)\b(rate[ _-]?limit(ed|s)?|usage limit reached|"
        r"limit reached[,.]? resets? at|429\b|too many requests|quota exceeded)\b"
    )),
    (ERR_COOLDOWN, re.compile(r"(?i)\b(cool ?down|try again in|retry after)\b")),
    (ERR_PROVIDER_UNAVAILABLE, re.compile(
        r"(?i)\b(overloaded|service unavailable|server error|internal error|"
        r"5\d\d\s+(error|status)|upstream (error|timeout))\b"
    )),
    (ERR_NETWORK, re.compile(
        r"(?i)\b(connection (refused|reset|closed)|network (error|unreachable)|"
        r"dns|getaddrinfo|temporary failure in name resolution|tls|certificate|"
        r"proxy error|econnrefused|enotfound|etimedout)\b"
    )),
    (ERR_POLICY_BLOCKED, re.compile(
        r"(?i)\b(not available in your (region|country)|"
        r"unsupported (region|country)|forbidden by policy)\b"
    )),
)


def classify_text(text: Optional[str]) -> Optional[str]:
    """Код по тексту сообщения. `None`, если ничего не опознано."""
    if not text:
        return None
    for code, pattern in _TEXT_PATTERNS:
        if pattern.search(text):
            return code
    return None


def classify_process_result(
    *,
    exit_code: Optional[int],
    stdout: str = "",
    stderr: str = "",
    timed_out: bool = False,
    executable_missing: bool = False,
) -> str:
    """Код по результату запуска CLI.

    Порядок проверок отражает надёжность признака: физическое отсутствие
    исполняемого файла и таймаут известны точно, текст — предположение, а голый
    ненулевой код возврата не говорит ничего, кроме «не получилось».
    """
    if executable_missing:
        return ERR_CLI_MISSING
    if timed_out:
        return ERR_TIMEOUT
    if exit_code == 127:
        return ERR_CLI_MISSING
    from_text = classify_text(stderr) or classify_text(stdout)
    if from_text:
        return from_text
    if exit_code == 0:
        return ERR_UNKNOWN
    return ERR_UNKNOWN


def classify_exception(exc: BaseException) -> str:
    """Код по исключению нашего собственного кода запуска."""
    import subprocess

    if isinstance(exc, FileNotFoundError):
        return ERR_CLI_MISSING
    if isinstance(exc, subprocess.TimeoutExpired):
        return ERR_TIMEOUT
    if isinstance(exc, PermissionError):
        return ERR_CLI_MISSING
    if isinstance(exc, (ValueError, TypeError, KeyError)):
        # Разбор ответа не удался: ответ есть, но он не той формы.
        return ERR_MALFORMED_STATUS
    if isinstance(exc, OSError):
        return ERR_NETWORK
    return classify_text(str(exc)) or ERR_UNKNOWN


def is_provider_fault(code: str) -> bool:
    """Отказ провайдера, а не воркера.

    Используется одним местом — расчётом предупреждений: провайдерская ошибка
    НЕ имеет права ни поменять состояние воркера, ни остановить задание.
    """
    return code in PROVIDER_ERROR_CODES


def error_message_ru(code: str, detail: Optional[str] = None) -> str:
    """Короткое русское объяснение для оператора."""
    base = {
        ERR_AUTH_REQUIRED: "требуется авторизация провайдера",
        ERR_RATE_LIMITED: "провайдер отказал по лимиту",
        ERR_COOLDOWN: "провайдер просит подождать",
        ERR_NETWORK: "сетевая ошибка при обращении к провайдеру",
        ERR_PROVIDER_UNAVAILABLE: "сервис провайдера недоступен",
        ERR_CLI_MISSING: "CLI провайдера не установлен",
        ERR_INCOMPATIBLE_CLI: "версия CLI несовместима",
        ERR_MALFORMED_STATUS: "ответ провайдера не разобран",
        ERR_POLICY_BLOCKED: "использование запрещено политикой",
        ERR_TIMEOUT: "провайдер не ответил вовремя",
        ERR_UNKNOWN: "неопознанный отказ провайдера",
    }.get(code, "неопознанный отказ провайдера")
    return f"{base}: {detail}" if detail else base


def summarize(payload: Any) -> str:
    """Строковое представление произвольного ответа — для classify_text."""
    if payload is None:
        return ""
    if isinstance(payload, str):
        return payload
    try:
        import json

        return json.dumps(payload, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(payload)
