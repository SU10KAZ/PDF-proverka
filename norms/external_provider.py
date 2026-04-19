"""Adapter к внешней нормативной базе Norms-main.

Единственный источник истины по статусам норм — status_index.json соседнего
проекта `/home/coder/projects/Norms`. Этот модуль только ЧИТАЕТ его,
ничего не пишет и ничего не копирует внутрь PDF-proverka.

WebSearch / WebFetch / интернет здесь запрещены концептуально: если нормы
нет в индексе, мы возвращаем found=False и направляем её в очередь на
ручное добавление (missing_norms_queue.json), а не пытаемся «угадать».

Публичный API:
    load_status_index(force_reload: bool = False) -> dict
    resolve_norm_status(raw_norm: str) -> dict

Схема возврата resolve_norm_status описана в его докстринге.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

# ─── Путь к внешней базе ──────────────────────────────────────────────────
# Переопределяется через env (удобно в тестах). В проде — абсолютный путь
# к соседнему проекту, как просил заказчик.
NORMS_STATUS_INDEX_PATH = Path(
    os.environ.get(
        "NORMS_STATUS_INDEX_PATH",
        "/home/coder/projects/Norms/tools/status_index.json",
    )
)


# ─── Определение семейства ────────────────────────────────────────────────
# Порядок важен: узкие шаблоны до широких.
_FAMILY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("ГОСТ Р", re.compile(r"^\s*ГОСТ\s+Р\b", re.IGNORECASE)),
    ("ГОСТ", re.compile(r"^\s*ГОСТ\b", re.IGNORECASE)),
    ("СНиП", re.compile(r"^\s*СНиП\b", re.IGNORECASE)),
    ("СП", re.compile(r"^\s*СП\s*\d", re.IGNORECASE)),
    ("ВСН", re.compile(r"^\s*ВСН\b", re.IGNORECASE)),
    ("МДС", re.compile(r"^\s*МДС\b", re.IGNORECASE)),
    ("РД", re.compile(r"^\s*РД\b", re.IGNORECASE)),
    ("ПУЭ", re.compile(r"^\s*(?:ПУЭ|ПЭУ)\b", re.IGNORECASE)),
    ("ПП РФ", re.compile(
        r"^\s*(?:Постановление\s+Правительства|ПП\s*РФ)\b", re.IGNORECASE)),
    ("ФЗ", re.compile(
        r"^\s*(?:Федеральный\s+закон|ФЗ\s+\d|\d+-ФЗ)\b", re.IGNORECASE)),
    ("СО", re.compile(r"^\s*СО\s+\d", re.IGNORECASE)),
]

# Те же семейства, что Norms-main маркирует supported. Важно: supported
# ≠ доступно в индексе. supported означает: мы умеем направить такую норму
# в intake queue и в дальнейшем либо добавить, либо пометить override.
_SUPPORTED_FAMILIES = {name for name, _ in _FAMILY_PATTERNS}


def _detect_family(code: str) -> str | None:
    if not code:
        return None
    s = str(code).strip()
    for name, rx in _FAMILY_PATTERNS:
        if rx.match(s):
            return name
    return None


# ─── Нормализация строки запроса ──────────────────────────────────────────
def _normalize_query(raw: str) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    # Схлопываем пробелы, убираем markdown
    s = s.replace("**", "").replace("*", "")
    s = re.sub(r"\s+", " ", s)
    # Убираем хвосты в скобках: (действует...), (ред. ...), (изм. ...),
    # (с изменениями...), (введ...), (утв...), (актуал...), (в ред...)
    s = re.sub(
        r"\s*\((?:действу|ред\.|изм\.|с изм|введ|утв|актуал|в ред)[^)]*\)",
        "", s, flags=re.IGNORECASE,
    )
    s = re.sub(r"\s+с\s+(?:[Ии]зменениями?|[Ии]зменением)\s*(?:№\s*[\d,\s\-–]+)?", "", s)
    s = re.sub(r"\s*ред\.\s*\d{2}\.\d{2}\.\d{4}", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.rstrip(".,;: ")
    return s


def _match_key(s: str) -> str:
    """Ключ для нечеткого матча: без пробелов, _/. унифицированы, lower."""
    if not s:
        return ""
    return re.sub(r"\s+", "", s).replace("_", ".").lower()


# ─── Кеш ──────────────────────────────────────────────────────────────────
_index_cache: dict | None = None
_lookup_cache: dict[str, str] | None = None       # match-key → canonical code
_alias_kind_cache: dict[str, str] | None = None   # match-key → "canonical"|"alias"
_by_code_cache: dict[str, dict] | None = None     # canonical code → entry


def load_status_index(force_reload: bool = False) -> dict:
    """Прочитать status_index.json из Norms-main и закешировать.

    Никогда не пишет обратно. При ошибке чтения возвращает пустой валидный
    каркас (чтобы вызывающий код мог честно увидеть "не покрыто").
    """
    global _index_cache, _lookup_cache, _alias_kind_cache, _by_code_cache
    if _index_cache is not None and not force_reload:
        return _index_cache

    path = NORMS_STATUS_INDEX_PATH
    if not path.exists():
        # Не роняем процесс — возвращаем пустой индекс. Все запросы вернут
        # not_found с resolution_reason=not_in_index.
        _index_cache = {"meta": {}, "norms": []}
        _lookup_cache = {}
        _alias_kind_cache = {}
        _by_code_cache = {}
        return _index_cache

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    _index_cache = data

    lookup: dict[str, str] = {}
    alias_kind: dict[str, str] = {}
    by_code: dict[str, dict] = {}
    for entry in data.get("norms", []):
        code = entry["code"]
        by_code[code] = entry
        canon_key = _match_key(code)
        if canon_key and canon_key not in lookup:
            lookup[canon_key] = code
            alias_kind[canon_key] = "canonical"
        for alias in entry.get("aliases") or []:
            ak = _match_key(alias)
            if not ak or ak == canon_key:
                continue
            lookup.setdefault(ak, code)
            alias_kind.setdefault(ak, "alias")

    _lookup_cache = lookup
    _alias_kind_cache = alias_kind
    _by_code_cache = by_code
    return data


def _reset_cache() -> None:
    """Тестовый хелпер — сбрасывает кеш."""
    global _index_cache, _lookup_cache, _alias_kind_cache, _by_code_cache
    _index_cache = None
    _lookup_cache = None
    _alias_kind_cache = None
    _by_code_cache = None


# ─── Core resolve ─────────────────────────────────────────────────────────
def _effective_status(doc_status: str | None, edition_status: str | None) -> str:
    """Свести (doc_status, edition_status) к единому статусу замечания.

    Mapping (см. норм-мэппинг из ТЗ):
        active + None/current  → active
        active + outdated      → outdated_edition
        replaced               → replaced
        cancelled              → cancelled
        иначе                  → unknown
    """
    if doc_status == "replaced":
        return "replaced"
    if doc_status == "cancelled":
        return "cancelled"
    if doc_status == "active":
        if edition_status == "outdated":
            return "outdated_edition"
        return "active"
    return "unknown"


def _resolve_in_index(normalized: str) -> tuple[str | None, str]:
    """Вернуть (canonical_code, kind) или (None, "none")."""
    load_status_index()
    assert _lookup_cache is not None
    assert _alias_kind_cache is not None

    key = _match_key(normalized)
    if not key:
        return None, "none"
    if key in _lookup_cache:
        return _lookup_cache[key], _alias_kind_cache.get(key, "canonical")
    # substring-fallback: аккуратный, минимизирует длину разницы.
    best: tuple[str, int] | None = None
    for k, code in _lookup_cache.items():
        if key in k or k in key:
            score = abs(len(k) - len(key))
            if best is None or score < best[1]:
                best = (code, score)
    if best:
        return best[0], "substring"
    return None, "none"


def _not_found(
    query: str,
    normalized: str,
    resolution_reason: str,
    family: str | None,
    supported_family: bool,
) -> dict:
    """Сформировать payload «не найдено» в edge-формате контракта."""
    return {
        "query": query,
        "normalized_query": normalized,
        "found": False,
        "matched_code": None,
        "status": "unknown",
        "doc_status": None,
        "edition_status": None,
        "authoritative": False,
        "resolution_reason": resolution_reason,
        "detected_family": family,
        "supported_family": supported_family,
        "needs_manual_addition": supported_family and resolution_reason == "not_in_index",
        "has_text": False,
        "replacement_doc": None,
        "current_version": None,
        "title": None,
        "file": None,
        "type": None,
        "year": None,
        "details": None,
        "source_url": None,
        "last_verified": None,
        "parse_confidence": None,
        "source": "not_found",
    }


def resolve_norm_status(raw_norm: str) -> dict:
    """Authoritative статус нормы из Norms-main status_index.

    Нормализует вход, ищет по canonical code и aliases, определяет семейство
    и возвращает стабильный payload.

    Ключевые поля:
      query, normalized_query, found, matched_code,
      status ∈ {active, outdated_edition, replaced, cancelled, unknown},
      doc_status, edition_status, authoritative,
      resolution_reason ∈ {exact, alias, manual_override,
                           not_in_index, unsupported_family, not_found},
      detected_family, supported_family, needs_manual_addition, has_text,
      replacement_doc, current_version, title, file, type, year,
      details, source_url, last_verified, parse_confidence, source.
    """
    original = "" if raw_norm is None else str(raw_norm)
    normalized = _normalize_query(original)

    # Пустой запрос → not_found без семейства.
    if not normalized:
        return _not_found(original, normalized, "not_found", None, False)

    try:
        load_status_index()
    except (OSError, json.JSONDecodeError):
        # Индекс нечитаем — честно фиксируем "не нашли". LLM сюда не зовём.
        return _not_found(
            original, normalized, "not_found",
            _detect_family(normalized), False,
        )

    matched_code, kind = _resolve_in_index(normalized)
    if matched_code is None:
        family = _detect_family(normalized)
        if family is None:
            return _not_found(original, normalized, "unsupported_family", None, False)
        return _not_found(original, normalized, "not_in_index", family, True)

    assert _by_code_cache is not None
    entry = _by_code_cache[matched_code]
    doc_status = entry.get("doc_status", "unknown")
    edition_status = entry.get("edition_status")
    eff = _effective_status(doc_status, edition_status)

    if entry.get("source") == "override_only":
        resolution_reason = "manual_override"
    elif kind == "canonical":
        resolution_reason = "exact"
    else:
        # alias или substring fallback — трактуем как alias-уровень.
        resolution_reason = "alias"

    family = _detect_family(entry.get("code", "")) or _detect_family(normalized)

    return {
        "query": original,
        "normalized_query": normalized,
        "found": True,
        "matched_code": entry["code"],
        "status": eff,
        "doc_status": doc_status,
        "edition_status": edition_status,
        "authoritative": bool(entry.get("authoritative", True)),
        "resolution_reason": resolution_reason,
        "detected_family": family,
        "supported_family": family in _SUPPORTED_FAMILIES,
        "needs_manual_addition": False,
        "has_text": bool(entry.get("has_text", entry.get("source") == "vault")),
        "replacement_doc": entry.get("replacement_doc"),
        "current_version": entry.get("current_version"),
        "title": entry.get("title"),
        "file": entry.get("file"),
        "type": entry.get("type"),
        "year": entry.get("year"),
        "details": entry.get("details"),
        "source_url": entry.get("source_url"),
        "last_verified": entry.get("last_verified"),
        "parse_confidence": entry.get("parse_confidence"),
        "source": entry.get("source", "vault"),
    }


__all__ = [
    "NORMS_STATUS_INDEX_PATH",
    "load_status_index",
    "resolve_norm_status",
]
