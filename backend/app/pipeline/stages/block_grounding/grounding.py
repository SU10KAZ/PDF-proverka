"""Ядро Value Grounding (Phase 1) — чистые функции, без I/O.

Идея: векторный текст-слой блока (pdfplumber) — источник истины для значений.
Сверяем то, что прочитала gemma (ocr_text), с вектором и фиксируем глифовые ошибки
(доказанный класс: «В40» прочитано как «В4.0» → занижение класса бетона в 10 раз).

Главная функция: ground_block(gemma_text, pdfplumber_text, *, words=None) -> dict
Возвращает: {grounded_values, corrections, value_source, value_confidence, vector_usable}.
"""
from __future__ import annotations

import re
from typing import Optional

# Минимум символов текст-слоя, чтобы доверять вектору как эталону.
MIN_VECTOR_CHARS = 30
# Доля «мусорных» символов, выше которой вектор считаем грязным (не доверяем).
MAX_GARBLED_RATIO = 0.4

# Валидные классы бетона по ГОСТ 26633 (числовая часть, канон).
_VALID_CLASSES = {"3.5", "5", "7.5", "10", "12.5", "15", "20", "25", "30", "35", "40", "45", "50", "55", "60"}
# Класс бетона: кириллическая В или латинская B + 1-2 цифры (+ опц. дробь).
_CONCRETE_RE = re.compile(r"(?<![A-Za-zА-Яа-я0-9])[ВB]\s*(\d{1,2})(?:\s*[.,]\s*(\d))?")
# Отметка уровня: +12.500 / -2,850
_LEVEL_RE = re.compile(r"[+\-]\d{1,3}[.,]\d{2,3}")
# Числовой токен (>=2 знака), пробелы внутри числа склеиваем отдельно.
_NUM_RE = re.compile(r"\d{2,}")


def _strip_num_spaces(text: str) -> str:
    """«24 775» → «24775» (числа с тысячными пробелами склеиваем)."""
    return re.sub(r"(?<=\d)[  ](?=\d)", "", text or "")


def _garbled_ratio(text: str) -> float:
    """Грубая оценка «мусорности» текст-слоя: доля не-алфанумерик/не-пунктуации."""
    if not text:
        return 1.0
    good = sum(1 for c in text if c.isalnum() or c.isspace() or c in ".,;:+-×x/()«»\"'")
    return 1.0 - good / len(text)


def vector_usable(pdfplumber_text: str) -> bool:
    """Можно ли доверять вектору как эталону."""
    t = (pdfplumber_text or "").strip()
    return len(t) >= MIN_VECTOR_CHARS and _garbled_ratio(t) <= MAX_GARBLED_RATIO


def concrete_class_canon(raw_int: str, frac: Optional[str]) -> Optional[str]:
    """Каноническая форма ВАЛИДНОГО класса бетона (или None, если не класс).

    «В40» (int=40) → «В40». «В4.0» (int=4, frac=0) → «В40» (нет десятичных классов с .0).
    «В3.5» (int=3, frac=5) → «В3.5» (валидная дробь). «B01»/«B1» → None (не класс бетона).
    Латиница B канонизируется в кириллицу В. Возвращает None для всего, что не в _VALID_CLASSES.
    """
    g1 = raw_int.lstrip("0") or "0"
    if frac is None:
        num = g1
    elif frac == "0":
        num = f"{g1}0"               # 4 + .0 → «40» (10×-опечатка в обратную сторону)
    else:
        num = f"{g1}.{frac}"         # 3 + .5 → «3.5»
    if num in _VALID_CLASSES:
        return f"В{num}"
    # «4.0» само по себе не валидно, но склейка «40» — да (страховка)
    collapsed = num.replace(".", "")
    if "." in num and num.endswith(".0") and collapsed in _VALID_CLASSES:
        return f"В{collapsed}"
    return None


def extract_concrete_classes(text: str) -> set[str]:
    """Множество ВАЛИДНЫХ канонических классов бетона из текста."""
    out = set()
    for m in _CONCRETE_RE.finditer(text or ""):
        c = concrete_class_canon(m.group(1), m.group(2))
        if c:
            out.add(c)
    return out


# «В{d}.0» / «В{d},0» — невозможный класс (нет десятичных .0 по ГОСТ 26633) → всегда В{d}0.
_DOT_ZERO_RE = re.compile(r"(?<![A-Za-zА-Яа-я0-9])([ВB]\s*\d\s*[.,]\s*0)(?!\d)")


def domain_class_corrections(gemma_text: str) -> list[dict]:
    """Доменная коррекция БЕЗ эталона: «В4.0»→«В40» (класса В{N}.0 не существует).

    Высокая точность: десятичных .0-классов бетона по ГОСТ 26633 нет, поэтому «В4.0» —
    однозначно misread «В40» (10×-занижение). Срабатывает на любом блоке, вектор не нужен.
    """
    out: list[dict] = []
    seen: set[str] = set()
    for m in _DOT_ZERO_RE.finditer(gemma_text or ""):
        raw = m.group(1)
        digit = re.search(r"\d", raw).group(0)
        collapsed = f"{digit}0"
        if collapsed in _VALID_CLASSES and raw not in seen:
            seen.add(raw)
            out.append({
                "field": "concrete_class",
                "gemma_value": raw.replace(" ", ""),
                "grounded_value": f"В{collapsed}",
                "scope": "domain_rule",
                "reason": "класс В{N}.0 не существует по ГОСТ 26633 → В{N}0 (10×-занижение)",
            })
    return out


def _deceptive_forms(canonical: str) -> set[str]:
    """Обманчивые dot-формы 2-значного класса (реальная 10×-ошибка чтения).

    «В40» → {«В4.0», «В4,0», «B4.0», «B4,0»}. «В35» → {«В3.5», …}.
    Латиница/пробелы БЕЗ точки сюда не входят — они не меняют значение, не ошибка.
    """
    m = re.fullmatch(r"В(\d)(\d)", canonical)
    if not m:
        return set()
    a, b = m.group(1), m.group(2)
    return {f"В{a}.{b}", f"В{a},{b}", f"B{a}.{b}", f"B{a},{b}"}


def _text_has(text: str, token: str) -> bool:
    return token.replace(" ", "") in (text or "").replace(" ", "")


def extract_salient(text: str) -> dict:
    """Значимые значения из текста (для grounded_values / recall)."""
    t = _strip_num_spaces(text or "")
    return {
        "concrete_classes": sorted(extract_concrete_classes(text or "")),
        "level_marks": sorted(set(_LEVEL_RE.findall(text or ""))),
        "numbers": sorted(set(_NUM_RE.findall(t))),
    }


def ground_block(gemma_text: str, pdfplumber_text: str, *,
                 doc_classes: Optional[set] = None, words: Optional[list] = None) -> dict:
    """Сверить значения gemma с векторным эталоном.

    Эталон классов бетона = вектор-слой блока ∪ ``doc_classes`` (классы из вектора ДРУГИХ
    блоков того же документа). Это ловит «В4.0» на CAD-блоке без своего вектор-слоя, если
    «В40» есть в спецификации документа. corrections — только dot-обман (10×-занижение).
    """
    gemma_text = gemma_text or ""
    usable = vector_usable(pdfplumber_text)
    v = extract_salient(pdfplumber_text) if usable else {"concrete_classes": [], "level_marks": [], "numbers": []}
    g = extract_salient(gemma_text)

    # Авторитетные классы: блок + документ (для кросс-блочной коррекции «В4.0»→«В40»).
    auth_classes = set(v["concrete_classes"]) | set(doc_classes or ())

    corrections: list[dict] = []
    seen_corr: set = set()

    # (A) Доменное правило (эталон не нужен): «В4.0»→«В40» — невозможный класс по ГОСТ.
    for c in domain_class_corrections(gemma_text):
        key = (c["gemma_value"], c["grounded_value"])
        if key not in seen_corr:
            seen_corr.add(key)
            corrections.append(c)

    # (B) Эталонный dot-обман (вектор блока/документа): подтверждает/расширяет правило (A).
    # Латиница/пробелы без точки не трогаем (то же значение, не ошибка).
    for cls in sorted(auth_classes):
        deceptive = sorted(f for f in _deceptive_forms(cls) if _text_has(gemma_text, f))
        if deceptive and not _text_has(gemma_text, cls):
            key = (deceptive[0], cls)
            if key in seen_corr:
                continue
            seen_corr.add(key)
            in_block = cls in v["concrete_classes"]
            corrections.append({
                "field": "concrete_class",
                "gemma_value": deceptive[0],
                "grounded_value": cls,
                "scope": "block" if in_block else "document",
                "reason": "dot-обман класса, подтверждён вектором (10×-занижение)",
            })

    # recall чисел эталона в gemma (диагностика полноты)
    v_nums = set(v["numbers"])
    g_nums = set(g["numbers"])
    num_recall = round(len(v_nums & g_nums) / len(v_nums), 3) if v_nums else None

    grounded_values = {
        "concrete_classes": v["concrete_classes"],
        "level_marks": v["level_marks"],
        "numbers_count": len(v_nums),
    } if usable else {}

    return {
        "vector_usable": usable,
        "value_source": "vector" if usable else "gemma_only",
        "value_confidence": "high" if usable else "low",
        "grounded_values": grounded_values,
        "corrections": corrections,
        "gemma_number_recall_vs_vector": num_recall,
    }
