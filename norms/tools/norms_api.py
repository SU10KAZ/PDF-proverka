"""Authoritative machine-readable API над нормативной базой Norms.

Norms = единственный доверенный source of truth по статусам норм.
Никакого WebSearch. Никакого bootstrap/seed из других проектов.
Если норма не найдена — это честный сигнал «не покрыто», а не повод
что-то выдумывать.

Публичный API:
    load_status_index(force_reload: bool = False) -> dict
    detect_family(code: str) -> str | None
    is_supported_family(family: str | None) -> bool
    get_norm_status(code: str) -> dict
    get_paragraph(code: str, paragraph: str, max_lines: int = 50) -> dict
    semantic_search(query: str, top: int = 5, code_filter: str | None = None) -> list[dict]

Схема ответа get_norm_status см. докстринг функции.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

from parse_filename import normalize_user_code  # noqa: E402

HERE = Path(__file__).resolve().parent
VAULT = HERE.parent / "vault"
STATUS_INDEX_PATH = HERE / "status_index.json"


# ---------- cache ----------
_index_cache: dict | None = None
_lookup_cache: dict[str, str] | None = None  # match-key → canonical code
_alias_kind_cache: dict[str, str] | None = None  # match-key → "canonical" | "alias"
_by_code_cache: dict[str, dict] | None = None


# ---------- family detection ----------

_FAMILY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("ГОСТ Р", re.compile(r"^\s*ГОСТ\s+Р\b", re.IGNORECASE)),
    ("ГОСТ", re.compile(r"^\s*ГОСТ\b", re.IGNORECASE)),
    ("СНиП", re.compile(r"^\s*СНиП\b", re.IGNORECASE)),
    ("СП", re.compile(r"^\s*СП\s*\d", re.IGNORECASE)),
    ("ВСН", re.compile(r"^\s*ВСН\b", re.IGNORECASE)),
    ("МДС", re.compile(r"^\s*МДС\b", re.IGNORECASE)),
    ("РД", re.compile(r"^\s*РД\b", re.IGNORECASE)),
    ("ПУЭ", re.compile(r"^\s*(?:ПУЭ|ПЭУ)\b", re.IGNORECASE)),
    ("ПП РФ", re.compile(r"^\s*(?:Постановление\s+Правительства|ПП\s*РФ)\b", re.IGNORECASE)),
    ("ФЗ", re.compile(r"^\s*(?:Федеральный\s+закон|ФЗ\s+\d|\d+-ФЗ)\b", re.IGNORECASE)),
    ("СО", re.compile(r"^\s*СО\s+\d", re.IGNORECASE)),
]

# Все из _FAMILY_PATTERNS поддерживаются в смысле:
# мы узнаём семейство → можем направить в intake queue и дальше либо
# добавить файл в vault, либо сделать manual override.
_SUPPORTED_FAMILIES = {name for name, _ in _FAMILY_PATTERNS}

# Для каких семейств есть структурированный parse MD-файлов (hosted in vault).
_VAULT_HOSTED_FAMILIES = {
    "ГОСТ", "ГОСТ Р", "СП", "СНиП", "ВСН", "МДС", "РД", "ПУЭ", "ФЗ",
}


def detect_family(code: str) -> str | None:
    """Определить семейство нормы по началу строки.

    Возвращает одно из _FAMILY_PATTERNS или None, если ничего не распознано.
    """
    if not code:
        return None
    s = str(code).strip()
    for name, regex in _FAMILY_PATTERNS:
        if regex.match(s):
            return name
    return None


def is_supported_family(family: str | None) -> bool:
    return family in _SUPPORTED_FAMILIES


def is_vault_hosted_family(family: str | None) -> bool:
    return family in _VAULT_HOSTED_FAMILIES


# ---------- нормализация ----------


def _normalize_query(raw: str) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    return re.sub(r"\s+", " ", s)


def _match_key(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", "", s).replace("_", ".").lower()


# ---------- effective status ----------


def effective_status(doc_status: str | None, edition_status: str | None) -> str:
    if doc_status == "replaced":
        return "replaced"
    if doc_status == "cancelled":
        return "cancelled"
    if doc_status == "active":
        if edition_status == "outdated":
            return "outdated_edition"
        return "active"
    return "unknown"


# ---------- load ----------


def load_status_index(force_reload: bool = False) -> dict:
    """Загрузить и закэшировать status_index.json."""
    global _index_cache, _lookup_cache, _alias_kind_cache, _by_code_cache
    if _index_cache is not None and not force_reload:
        return _index_cache

    if not STATUS_INDEX_PATH.exists():
        raise FileNotFoundError(
            f"status_index.json не найден: {STATUS_INDEX_PATH}. "
            f"Запустите: python3 {HERE}/build_status_index.py"
        )

    data = json.loads(STATUS_INDEX_PATH.read_text(encoding="utf-8"))
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
    global _index_cache, _lookup_cache, _alias_kind_cache, _by_code_cache
    _index_cache = None
    _lookup_cache = None
    _alias_kind_cache = None
    _by_code_cache = None


# ---------- resolve ----------


def _resolve(query: str) -> tuple[str | None, str]:
    """(canonical_code | None, match_kind) где match_kind ∈ {exact, alias, substring}."""
    load_status_index()
    assert _lookup_cache is not None
    assert _alias_kind_cache is not None
    key = _match_key(query)
    if not key:
        return None, "none"
    if key in _lookup_cache:
        kind = _alias_kind_cache.get(key, "alias")
        return _lookup_cache[key], kind
    # fallback: substring (нужно осторожно — может захватить чужую запись)
    best: tuple[str, int] | None = None
    for k, code in _lookup_cache.items():
        if key in k or k in key:
            score = abs(len(k) - len(key))
            if best is None or score < best[1]:
                best = (code, score)
    if best:
        return best[0], "substring"
    return None, "none"


# ---------- get_norm_status ----------


def _not_found_payload(
    original: str,
    normalized: str,
    resolution_reason: str,
    detected_family: str | None,
    supported_family: bool,
    error: str | None = None,
) -> dict:
    out = {
        "query": original,
        "normalized_query": normalized,
        "found": False,
        "matched_code": None,
        "status": "unknown",
        "doc_status": None,
        "edition_status": None,
        "authoritative": False,
        "resolution_reason": resolution_reason,
        "detected_family": detected_family,
        "supported_family": supported_family,
        "needs_manual_addition": supported_family,
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
    if error:
        out["error"] = error
    return out


def get_norm_status(code: str) -> dict:
    """Вернуть authoritative статус нормы по «грязному» вводу.

    Формат ответа:
      {
        "query":               <исходная строка>,
        "normalized_query":    <trimmed+collapsed>,
        "found":               bool,
        "matched_code":        str | null,
        "status":              "active|outdated_edition|replaced|cancelled|unknown",
        "doc_status":          "active|replaced|cancelled|unknown|null",
        "edition_status":      "current|outdated|unknown|null",
        "authoritative":       bool,
        "resolution_reason":   "exact|alias|manual_override|not_in_index|
                                unsupported_family|not_found",
        "detected_family":     str | null,
        "supported_family":    bool,
        "needs_manual_addition": bool,
        "replacement_doc":     str | null,
        "current_version":     str | null,
        "title":               str | null,
        "file":                str | null,
        "type":                str | null,
        "year":                int | null,
        "details":             str | null,
        "source_url":          str | null,
        "last_verified":       str | null,
        "parse_confidence":    "high|low|null",
        "source":              "vault|override_only|not_found",
      }
    """
    original = "" if code is None else str(code)
    normalized = _normalize_query(original)

    try:
        load_status_index()
    except FileNotFoundError as e:
        return _not_found_payload(
            original, normalized, "not_found",
            detect_family(normalized), False, error=str(e),
        )

    if not normalized:
        return _not_found_payload(original, normalized, "not_found", None, False)

    matched_code, kind = _resolve(normalized)
    if matched_code is None:
        family = detect_family(normalized)
        if family is None:
            return _not_found_payload(original, normalized, "unsupported_family", None, False)
        return _not_found_payload(original, normalized, "not_in_index", family, True)

    assert _by_code_cache is not None
    entry = _by_code_cache[matched_code]
    doc_status = entry.get("doc_status", "unknown")
    edition_status = entry.get("edition_status")
    eff = effective_status(doc_status, edition_status)

    if entry.get("source") == "override_only":
        resolution_reason = "manual_override"
    elif kind == "canonical":
        resolution_reason = "exact"
    else:
        resolution_reason = "alias"  # alias + substring-fallback оба считаем alias-level

    family = detect_family(entry["code"]) or detect_family(normalized)

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
        "supported_family": is_supported_family(family),
        "needs_manual_addition": False,
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


# ---------- get_paragraph ----------


def get_paragraph(code: str, paragraph: str, max_lines: int = 50) -> dict:
    """Структурированный поиск пункта. Никогда не поднимает исключение.

    Формат ответа:
      {
        "query_code":        <исходная строка>,
        "matched_code":      str | null,
        "paragraph":         <запрошенный номер>,
        "found":             bool,
        "text":              str | null,
        "file":              str | null,
        "line":              int | null,
        "status":            "active|outdated_edition|replaced|cancelled|unknown",
        "doc_status":        str | null,
        "edition_status":    str | null,
        "authoritative":     bool,
        "has_text":          bool,
        "resolution_reason": "exact|alias|manual_override|no_document_text|
                              paragraph_not_found|not_in_index|unsupported_family|not_found",
        "replacement_doc":   str | null,
        "truncated":         bool,
      }
    """
    from find_paragraph import find_file, find_paragraph as _find

    original_code = "" if code is None else str(code)
    paragraph_str = "" if paragraph is None else str(paragraph).strip()

    status = get_norm_status(original_code)
    matched_code = status.get("matched_code")
    entry_file = status.get("file")

    base = {
        "query_code": original_code,
        "matched_code": matched_code,
        "paragraph": paragraph_str,
        "found": False,
        "text": None,
        "file": entry_file,
        "line": None,
        "status": status.get("status", "unknown"),
        "doc_status": status.get("doc_status"),
        "edition_status": status.get("edition_status"),
        "authoritative": status.get("authoritative", False),
        "has_text": False,
        "resolution_reason": status.get("resolution_reason", "not_found"),
        "replacement_doc": status.get("replacement_doc"),
        "truncated": False,
    }

    # Нет матча вообще — honest fail.
    if matched_code is None:
        return base

    # Override-only / source != vault → текста нет.
    if status.get("source") != "vault" or not entry_file:
        base["resolution_reason"] = "no_document_text"
        return base

    file_path = VAULT / entry_file
    if not file_path.exists():
        file_path = find_file(matched_code)
    if file_path is None or not file_path.exists():
        base["resolution_reason"] = "no_document_text"
        return base

    base["has_text"] = True
    base["file"] = file_path.name

    if not paragraph_str:
        base["resolution_reason"] = "paragraph_not_found"
        return base

    try:
        hit = _find(file_path, paragraph_str)
    except Exception:
        hit = None

    if hit is None:
        base["resolution_reason"] = "paragraph_not_found"
        return base

    line_num, lines = hit
    truncated = False
    if max_lines and len(lines) > max_lines:
        lines = lines[:max_lines]
        truncated = True
    base["found"] = True
    base["text"] = "\n".join(lines)
    base["line"] = line_num
    base["truncated"] = truncated
    # Сохраняем исходный resolution_reason (exact/alias/manual_override) — это
    # статус резолва кода, не пункта. 'paragraph_not_found' только если миссис.
    return base


# ---------- semantic_search ----------


def semantic_search(
    query: str,
    top: int = 5,
    code_filter: str | None = None,
) -> list[dict]:
    """Семантический поиск по пунктам норм.

    Никогда не поднимает исключение наружу: если эмбеддинги недоступны
    или модель не грузится — возвращает пустой list.
    """
    if not query or not str(query).strip():
        return []
    try:
        from search import search as _search

        cf = code_filter if code_filter else None
        result = _search(query, top, cf)
        return list(result or [])
    except SystemExit:
        # search.py делает sys.exit(1) при отсутствии .npz — не роняем процесс.
        return []
    except Exception as e:  # pragma: no cover
        print(f"[norms_api.semantic_search] fallback empty: {e}", file=sys.stderr)
        return []


__all__ = [
    "load_status_index",
    "detect_family",
    "is_supported_family",
    "is_vault_hosted_family",
    "effective_status",
    "get_norm_status",
    "get_paragraph",
    "semantic_search",
]
