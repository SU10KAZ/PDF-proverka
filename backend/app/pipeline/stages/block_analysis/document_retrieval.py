"""Deterministic document-level context retrieval for Stage 01 findings.

The block model normally sees one crop and one sheet.  That is insufficient for
marks whose legend, schedule or detail lives on another sheet.  This module uses
only the already extracted ``document_graph.json`` text layer: no network, no
embedding model and no second LLM call.  The returned receipt makes an empty
search distinguishable from a search that was never performed.
"""
from __future__ import annotations

import re
from typing import Any


_WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9][A-Za-zА-Яа-яЁё0-9._/\-]{2,}")
_MARK_RE = re.compile(
    r"(?<![A-Za-zА-Яа-яЁё0-9])(?:[A-Za-zА-Яа-яЁё]{1,5}[\-–]?)?\d{1,4}"
    r"(?:[./\-–][A-Za-zА-Яа-яЁё0-9]{1,8})+(?![A-Za-zА-Яа-яЁё0-9])"
    r"|(?<![A-Za-zА-Яа-яЁё0-9])[A-Za-zА-Яа-яЁё]{1,5}[\-–]?\d{1,4}"
    r"(?![A-Za-zА-Яа-яЁё0-9])"
)
_STOP = {
    "блок", "лист", "листа", "страница", "страницы", "проект", "проекте",
    "текст", "точный", "контекст", "схема", "план", "разрез", "узел",
    "section", "sheet", "page", "block", "findings", "задача", "чертежа",
    "эталонная", "текстовая", "разметка", "источник", "метод", "краткий",
    "результат", "узлов", "видов", "физическая", "структура", "document.pdf",
    "cad-геометрия", "неподтверждённые", "отношения", "добавляются",
}

_DISCIPLINE_TARGET_TERMS = {
    "EOM": (
        "QF", "отходящая линия", "нагрузка", "трансформатор тока", "Ip",
        "Iкз", "характеристика", "СПЗ", "ПЭСПЗ", "огнестойкость",
        "спецификация",
    ),
    "KJ": (
        "марка детали", "позиция", "ведомость", "спецификация", "диаметр",
        "длина", "шаг", "количество", "масса", "разрез",
    ),
    "KM": (
        "марка детали", "позиция", "ведомость", "спецификация", "сечение",
        "длина", "количество", "масса", "узел",
    ),
    "AR": (
        "название листа", "этаж", "помещение", "экспликация", "дверь",
        "проём", "развёртка", "ведомость",
    ),
    "AI": (
        "название листа", "этаж", "помещение", "экспликация", "дверь",
        "проём", "развёртка", "ведомость",
    ),
    "TX": (
        "название листа", "этаж", "помещение", "экспликация", "оборудование",
        "план", "разрез", "ведомость",
    ),
    "OV": (
        "система", "узел", "деталь", "оборудование", "DN", "диаметр",
        "диапазон", "расход", "отметка",
    ),
    "VK": (
        "система", "узел", "деталь", "оборудование", "DN", "диаметр",
        "диапазон", "расход", "отметка",
    ),
}


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).casefold()


def extract_query_terms(text: str, *, max_terms: int = 24) -> list[str]:
    """Prefer drawing marks, then uncommon words from the local block context."""
    source = text or ""
    terms: list[str] = []
    seen: set[str] = set()
    for raw in list(_MARK_RE.findall(source)) + list(_WORD_RE.findall(source)):
        term = raw.strip(".,;:()[]{}\"'«»")
        key = _norm(term)
        if not key or key in seen or key in _STOP or len(key) < 3:
            continue
        # Plain long numbers and common dimensions are weak global anchors.
        if key.isdigit():
            continue
        if key.startswith("blk_") or re.fullmatch(r"[0-9a-f]{24,}", key):
            continue
        seen.add(key)
        terms.append(term)
        if len(terms) >= max_terms:
            break
    return terms


def _page_number(page: dict[str, Any]) -> int:
    raw = page.get("page")
    if raw is None:
        raw = int(page.get("page_index") or 0) + 1
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def retrieve_document_context(
    graph: dict[str, Any],
    query_text: str,
    current_page: int,
    *,
    max_hits: int = 6,
    max_chars: int = 12000,
    max_query_terms: int = 24,
) -> tuple[str, dict[str, Any]]:
    """Return relevant text from other sheets plus an auditable search receipt."""
    terms = extract_query_terms(query_text, max_terms=max_query_terms)
    normalized_terms = [_norm(term) for term in terms]
    candidates: list[tuple[float, int, str, list[str]]] = []

    for page in graph.get("pages") or []:
        if not isinstance(page, dict):
            continue
        page_no = _page_number(page)
        if page_no == int(current_page or 0):
            continue
        sheet_name = str(page.get("sheet_name") or page.get("sheet_no") or "").strip()
        for tb in page.get("text_blocks") or []:
            if not isinstance(tb, dict):
                continue
            text = re.sub(r"\s+", " ", str(tb.get("text") or "")).strip()
            if len(text) < 20:
                continue
            lowered = _norm(text)
            matched = [term for term, key in zip(terms, normalized_terms) if key in lowered]
            if not matched:
                continue
            mark_matches = sum(1 for term in matched if _MARK_RE.fullmatch(term))
            if mark_matches == 0 and len(matched) < 2:
                continue
            # Several independent overlaps beat a single generic word; drawing
            # marks receive a strong bonus because they are stable cross-sheet keys.
            score = mark_matches * 8.0 + len(matched) * 2.0
            score += min(len(text), 1200) / 1200.0
            label = f"стр. PDF {page_no}" + (f", лист {sheet_name}" if sheet_name else "")
            candidates.append((score, page_no, f"[{label}] {text[:2400]}", matched))

    candidates.sort(key=lambda item: (-item[0], item[1], item[2]))
    selected: list[tuple[float, int, str, list[str]]] = []
    seen_text: set[str] = set()
    used = 0
    for candidate in candidates:
        key = _norm(candidate[2])
        if key in seen_text:
            continue
        snippet_len = len(candidate[2]) + 2
        if selected and used + snippet_len > max_chars:
            continue
        seen_text.add(key)
        selected.append(candidate)
        used += snippet_len
        if len(selected) >= max_hits:
            break

    receipt = {
        "scope": "all_document_vector_text_other_pages",
        "query_terms": terms,
        "candidate_hits": len(candidates),
        "selected_hits": len(selected),
        "selected_pages": sorted({item[1] for item in selected}),
        "status": "hits" if selected else ("no_hits" if terms else "no_query_terms"),
    }
    body = "\n\n".join(item[2] for item in selected)
    header = (
        "## Поиск связанного контекста по всему документу\n"
        f"Область поиска: все остальные листы; статус: {receipt['status']}; "
        f"совпадений выбрано: {len(selected)}.\n"
        f"Ключи поиска: {', '.join(terms) if terms else '(нет надёжных ключей)'}.\n"
    )
    if body:
        return header + body, receipt
    return header + "Связанный текст на других листах не найден.", receipt


def retrieve_targeted_document_context(
    graph: dict[str, Any],
    query_text: str,
    current_page: int,
    *,
    discipline: str,
    max_hits: int = 8,
    max_chars: int = 16000,
) -> tuple[str, dict[str, Any]]:
    """Retrieve cross-sheet evidence for the Astra v3 shadow experiment.

    Exact marks from ``query_text`` remain the strongest anchors.  The bounded
    discipline vocabulary adds likely counterpart tables/labels when the local
    package contains few usable marks.  Production continues to call
    :func:`retrieve_document_context` with its unchanged defaults.
    """
    code = str(discipline or "").strip().upper()
    target_terms = _DISCIPLINE_TARGET_TERMS.get(code, ())
    expanded_query = "\n".join(
        part for part in (" ".join(target_terms), query_text) if part
    )
    text, receipt = retrieve_document_context(
        graph,
        expanded_query,
        current_page,
        max_hits=max_hits,
        max_chars=max_chars,
        max_query_terms=48,
    )
    receipt.update(
        {
            "profile": "discipline_targeted_v3",
            "discipline": code,
            "discipline_terms": list(target_terms),
        }
    )
    return text, receipt
