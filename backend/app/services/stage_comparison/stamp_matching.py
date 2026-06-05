"""Stamp / sheet-name based page matching for stage comparison.

Задача: предложить выравнивание страниц (page_alignment) между старой и новой
стадией по ИМЕНИ листа из штампа, а не по геометрии страницы.

Почему отдельно от `suggest_alignment` (fingerprint):
    `suggest_alignment` в store.py матчит страницы по fingerprint'у (соотношение
    сторон, число блоков, первые 300 символов текста) с маленьким окном
    lookahead=4 — то есть предполагает, что страницы почти не сдвинулись. Но
    между стадиями лист может уехать далеко (схема ГРЩ на стр.21 старой стадии и
    на стр.56 новой). Локальный greedy такое не находит.

    Имя листа в штампе («Наименование листа») — гораздо более устойчивый
    идентификатор. Здесь матч ГЛОБАЛЬНЫЙ по имени: одинаковые имена находят
    друг друга независимо от смещения страниц.

Источник имени листа:
    1. MD-штамп (Chandra OCR пишет `## СТРАНИЦА N` + `**Лист:**` +
       `**Наименование листа:**`) — основной путь, переиспользуем
       `build_fact_index` из evidence_first_fallback.
    2. Фолбэк: если у страницы имя пустое, можно подмешать текст-слой блоков
       страницы (pdfplumber_text / ocr_text из result.json) как слабый
       текст-сигнатуру (`extra_text_by_page`). Это «фолбэк на текст-слой
       block-PDF», но офлайн — без сетевых вызовов.

Модуль чистый и тестируемый: на вход — строки MD и (опционально) словарь
page→text. Никакого I/O и сети. I/O живёт в store.suggest_alignment_by_stamp.
"""
from __future__ import annotations

import math
import os
import re
import unicodedata
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Optional

from .evidence_first_fallback import build_fact_index


# ─── Тюнинг (env override, безопасные дефолты) ─────────────────────────────

def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


# Минимальный score нечёткого совпадения имени листа (взвешенная косинусная
# близость токенов с IDF-весами внутри пары).
STAMP_MATCH_MIN_SCORE = _env_float("STAGE_COMPARISON_STAMP_MATCH_MIN_SCORE", 0.55)
# Более строгий порог для фолбэка по тексту-слою (он шумнее имени).
STAMP_FALLBACK_MIN_SCORE = _env_float("STAGE_COMPARISON_STAMP_FALLBACK_MIN_SCORE", 0.75)
# Минимальный отрыв лучшего кандидата от второго по score. Если множество
# правых листов имеют ~равный score (типично для имён с общим бойлерплейт-
# префиксом «Часть 1. …»), матч НЕОДНОЗНАЧЕН → не предлагаем (precision > recall).
STAMP_MATCH_MIN_MARGIN = _env_float("STAGE_COMPARISON_STAMP_MATCH_MIN_MARGIN", 0.07)
# Длина текст-сигнатуры из текст-слоя для слабого фолбэка.
_FALLBACK_TEXT_LEN = 120


# ─── Нормализация имени листа ──────────────────────────────────────────────

_PAREN_FROM_RE = re.compile(r"\(\s*из\s*\d+\s*\)")
_SHEET_WORD_RE = re.compile(r"\bлист\b\s*№?\s*\d*")
_PAGE_WORD_RE = re.compile(r"\bстр\.?\b\s*\d*")
_NONE_RE = re.compile(r"\bnone\b")
_NON_ALNUM_RE = re.compile(r"[^0-9a-zа-я]+")
_WS_RE = re.compile(r"\s+")


def normalize_sheet_name(s: str) -> str:
    """Нормализовать имя листа для сравнения между стадиями.

    NFKC + ё→е + lower, срезаем «(из N)», «лист N», «стр. N», «none», любую
    пунктуацию → пробел, схлопываем пробелы. Никогда не падает.
    """
    s = (s or "").strip()
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("ё", "е").replace("Ё", "Е").lower()
    s = _PAREN_FROM_RE.sub(" ", s)
    s = _SHEET_WORD_RE.sub(" ", s)
    s = _PAGE_WORD_RE.sub(" ", s)
    s = _NONE_RE.sub(" ", s)
    s = _NON_ALNUM_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def _tokens(norm_name: str) -> list[str]:
    return [t for t in (norm_name or "").split(" ") if len(t) >= 2]


def _build_idf(names: list[str]) -> dict[str, float]:
    """IDF-веса токенов внутри пары: частые токены (бойлерплейт «часть»,
    «электроснабжение») получают малый вес, редкие/распознающие («вру», «грщ»,
    «молниезащита», номер этажа) — большой. Сглаженный, всегда > 0.
    """
    n = max(1, len(names))
    df: Counter = Counter()
    for nm in names:
        for t in set(_tokens(nm)):
            df[t] += 1
    return {t: math.log((n + 1.0) / (c + 0.5)) for t, c in df.items()}


def _weighted_sim(a: str, b: str, idf: dict[str, float]) -> float:
    """Взвешенная косинусная близость множеств токенов (бинарные векторы,
    веса = IDF). Разделяет имена с общим длинным префиксом, но разным «хвостом»:
    общий префикс из частых токенов почти не повышает score.
    """
    ta = set(_tokens(a))
    tb = set(_tokens(b))
    if not ta or not tb:
        return 0.0
    if ta == tb:
        return 1.0
    inter = ta & tb
    num = sum(idf.get(t, 1.0) ** 2 for t in inter)
    da = math.sqrt(sum(idf.get(t, 1.0) ** 2 for t in ta))
    db = math.sqrt(sum(idf.get(t, 1.0) ** 2 for t in tb))
    if da == 0.0 or db == 0.0:
        return 0.0
    return num / (da * db)


# ─── Запись о листе на одной стороне ───────────────────────────────────────

@dataclass
class SheetRec:
    page: int                 # номер PDF-страницы (= ## СТРАНИЦА N)
    sheet_no: str             # из **Лист:**
    sheet_name: str           # из **Наименование листа:** (или inherited/fallback)
    norm_name: str            # нормализованное имя для матчинга
    section_class: str        # pz | architectural | structural | ...
    is_graphic: bool          # есть ли image-блоки на странице
    name_source: str          # md | inherited | text_layer | none


def build_sheet_index(
    md: str,
    *,
    extra_text_by_page: Optional[dict[int, str]] = None,
) -> list[SheetRec]:
    """Распарсить MD стороны в список SheetRec (по PDF-страницам).

    Forward-fill: страницы-продолжения многостраничного листа (есть `**Лист:**`,
    но нет `**Наименование листа:**`) наследуют имя предыдущего именованного
    листа — так многостраничная «Текстовая часть» матчится по имени в порядке
    появления, а не рассыпается на безымянные слоты.

    extra_text_by_page: опциональный текст-слой по страницам (pdfplumber_text /
    ocr_text). Используется ТОЛЬКО для страниц без имени листа как слабая
    текст-сигнатура (фолбэк), нормализованная так же, как имя.
    """
    pages = build_fact_index("x", md or "").pages
    extra = extra_text_by_page or {}
    recs: list[SheetRec] = []
    last_name = ""
    for pr in sorted(pages, key=lambda p: p.page):
        raw_name = (pr.sheet_name or "").strip()
        norm = normalize_sheet_name(raw_name)
        if norm:
            last_name = norm
            source = "md"
            display = raw_name
        elif pr.sheet_no:
            # Страница-продолжение листа → наследуем имя.
            norm = last_name
            source = "inherited" if last_name else "none"
            display = ""
        else:
            norm = ""
            source = "none"
            display = ""
        # Фолбэк по тексту-слою для всё ещё безымянных страниц.
        if not norm and pr.page in extra:
            sig = normalize_sheet_name((extra.get(pr.page) or "")[:_FALLBACK_TEXT_LEN])
            if sig:
                norm = sig
                source = "text_layer"
        recs.append(SheetRec(
            page=pr.page,
            sheet_no=pr.sheet_no,
            sheet_name=display or raw_name,
            norm_name=norm,
            section_class=pr.section_class,
            is_graphic=bool(pr.image_block_ids),
            name_source=source,
        ))
    return recs


# ─── Матчинг ───────────────────────────────────────────────────────────────

def _matched_item(slot: int, left: SheetRec, right: SheetRec,
                  score: float, match_type: str) -> dict:
    note = f"{(left.sheet_name or right.sheet_name or '').strip()[:60]} · {match_type} {score:.2f}"
    return {
        "slot": slot,
        "left_page": left.page,
        "right_page": right.page,
        "mode": "manual",
        "note": note.strip(" ·"),
        # display-only поля для UI (validate() их отбросит при сохранении)
        "match": True,
        "match_type": match_type,
        "score": round(score, 3),
        "left_sheet_name": left.sheet_name,
        "right_sheet_name": right.sheet_name,
        "left_sheet_no": left.sheet_no,
        "right_sheet_no": right.sheet_no,
        "is_graphic": bool(left.is_graphic or right.is_graphic),
        "needs_review": match_type in ("fuzzy_name", "text_layer", "llm_semantic"),
    }


def _one_sided_item(slot: int, rec: SheetRec, side: str) -> dict:
    return {
        "slot": slot,
        "left_page": rec.page if side == "left" else None,
        "right_page": rec.page if side == "right" else None,
        "mode": "manual",
        "note": f"{side}_only",
        "match": False,
        "match_type": f"{side}_only",
        "score": 0.0,
        "left_sheet_name": rec.sheet_name if side == "left" else "",
        "right_sheet_name": rec.sheet_name if side == "right" else "",
        "is_graphic": bool(rec.is_graphic),
        "needs_review": False,
    }


def match_sheet_indexes(
    left: list[SheetRec],
    right: list[SheetRec],
    *,
    min_score: float = STAMP_MATCH_MIN_SCORE,
    fallback_min_score: float = STAMP_FALLBACK_MIN_SCORE,
    llm_match_fn=None,
) -> dict:
    """Сопоставить листы двух сторон по имени и собрать карту page_alignment.

    Проходы:
      1. exact: одинаковое нормализованное имя; дубликаты (многостраничные
         листы, повторяющиеся планы) — в порядке появления (1-й↔1-й, 2-й↔2-й).
      2. fuzzy: остаток — лучший взвешенный косинус ≥ порога
         (для text_layer-имён — более строгий fallback_min_score).
      3. [опц.] LLM-семантика: если передан `llm_match_fn`, он смотрит на
         НЕсматченный остаток обеих сторон и предлагает пары «это один и тот же
         лист» (например «Однолинейная расчетная схема ГРЩ» == «Однолинейная
         схема ГРЩ»). Возвращает [(left_page, right_page, score, match_type)].
         Пары проходят те же инварианты: каждый page не более одного раза.
      4. остаток → left_only / right_only.

    Сборка items: слоты в порядке левых страниц; right-only вставляются по
    возрастанию их номера так, чтобы сматченные листы стояли НАПРОТИВ друг
    друга (в одном слоте).
    """
    matches: dict[int, tuple[int, float, str]] = {}   # left_page -> (right_page, score, type)
    used_right: set[int] = set()
    left_by_page = {r.page: r for r in left}
    right_by_page = {r.page: r for r in right}

    # Pass 1 — exact normalized name, дубликаты в порядке появления.
    right_name_q: dict[str, deque[int]] = defaultdict(deque)
    for r in sorted(right, key=lambda x: x.page):
        if r.norm_name:
            right_name_q[r.norm_name].append(r.page)
    for l in sorted(left, key=lambda x: x.page):
        if l.norm_name and right_name_q.get(l.norm_name):
            rp = right_name_q[l.norm_name].popleft()
            mtype = "exact_name" if l.name_source != "text_layer" else "text_layer"
            matches[l.page] = (rp, 1.0, mtype)
            used_right.add(rp)

    # IDF-веса по всем именам пары (разделяет общий бойлерплейт-префикс).
    idf = _build_idf([r.norm_name for r in left if r.norm_name]
                     + [r.norm_name for r in right if r.norm_name])

    # Pass 2 — fuzzy для остатка: взвешенный косинус + margin-гейт.
    # Неоднозначные (много почти равных кандидатов) НЕ матчим — precision > recall.
    rem_left = [l for l in sorted(left, key=lambda x: x.page)
                if l.page not in matches and l.norm_name]
    lcount = max(1, len(left))
    rcount = max(1, len(right))
    for l in rem_left:
        # Ожидаемая правая позиция (пропорционально), для tie-break при равенстве.
        expected_rp = l.page / lcount * rcount
        scored: list[tuple[float, float, int]] = []  # (score, -|rp-expected|, page)
        for r in sorted(right, key=lambda x: x.page):
            if r.page in used_right or not r.norm_name:
                continue
            sc = _weighted_sim(l.norm_name, r.norm_name, idf)
            if sc <= 0.0:
                continue
            scored.append((sc, -abs(r.page - expected_rp), r.page))
        if not scored:
            continue
        scored.sort(reverse=True)
        best_sc, _, best_page = scored[0]
        second_sc = scored[1][0] if len(scored) > 1 else 0.0
        is_text = (l.name_source == "text_layer"
                   or right_by_page[best_page].name_source == "text_layer")
        threshold = fallback_min_score if is_text else min_score
        if best_sc < threshold:
            continue
        # Margin-гейт: лучший должен заметно опережать второго (иначе слипшийся
        # набор похожих имён → неоднозначно → не предлагаем).
        if (best_sc - second_sc) < STAMP_MATCH_MIN_MARGIN and best_sc < 0.999:
            continue
        mtype = "text_layer" if is_text else "fuzzy_name"
        matches[l.page] = (best_page, best_sc, mtype)
        used_right.add(best_page)

    # Pass 3 — опциональное LLM-семантическое доматчивание остатка.
    # Детерминированные совпадения не трогаем; LLM работает только по тому, что
    # осталось непарным с обеих сторон. Инварианты (page ≤ 1 раза) проверяем
    # здесь, не доверяя ответу модели.
    llm_match_count = 0
    if llm_match_fn is not None:
        rem_left = [l for l in sorted(left, key=lambda x: x.page)
                    if l.page not in matches]
        rem_right = [r for r in sorted(right, key=lambda x: x.page)
                     if r.page not in used_right]
        if rem_left and rem_right:
            try:
                proposals = llm_match_fn(rem_left, rem_right) or []
            except Exception:  # fail-soft — LLM не должен валить матчинг
                proposals = []
            for item in proposals:
                try:
                    lp, rp, score, mtype = item
                    lp = int(lp)
                    rp = int(rp)
                    score = float(score)
                except (TypeError, ValueError):
                    continue
                if lp in matches or rp in used_right:
                    continue
                if lp not in left_by_page or rp not in right_by_page:
                    continue
                matches[lp] = (rp, max(0.0, min(1.0, score)),
                               str(mtype or "llm_semantic"))
                used_right.add(rp)
                llm_match_count += 1

    # Сборка items — left в порядке страниц, right-only по возрастанию.
    right_only_pages = sorted(r.page for r in right if r.page not in used_right)
    items: list[dict] = []
    slot = 0
    ri = 0
    for l in sorted(left, key=lambda x: x.page):
        if l.page in matches:
            rp, score, mtype = matches[l.page]
            while ri < len(right_only_pages) and right_only_pages[ri] < rp:
                slot += 1
                items.append(_one_sided_item(slot, right_by_page[right_only_pages[ri]], "right"))
                ri += 1
            slot += 1
            items.append(_matched_item(slot, l, right_by_page[rp], score, mtype))
        else:
            slot += 1
            items.append(_one_sided_item(slot, l, "left"))
    while ri < len(right_only_pages):
        slot += 1
        items.append(_one_sided_item(slot, right_by_page[right_only_pages[ri]], "right"))
        ri += 1

    matched_items = [it for it in items if it.get("match")]
    scores = [it["score"] for it in matched_items]
    confidence = round(sum(scores) / len(scores), 3) if scores else 0.0

    warnings: list[str] = []
    if not left or not right:
        warnings.append("one_side_empty")
    if not any(r.norm_name for r in left) or not any(r.norm_name for r in right):
        warnings.append("no_sheet_names_found")
    if not matched_items:
        warnings.append("no_matches")

    return {
        "method": "stamp",
        "suggested_items": items,
        "confidence": confidence,
        "warnings": warnings,
        "matched_count": len(matched_items),
        "llm_match_count": llm_match_count,
        "left_only_count": sum(1 for it in items if it["match_type"] == "left_only"),
        "right_only_count": sum(1 for it in items if it["match_type"] == "right_only"),
        "left_page_count": len(left),
        "right_page_count": len(right),
    }


__all__ = [
    "SheetRec",
    "normalize_sheet_name",
    "build_sheet_index",
    "match_sheet_indexes",
    "STAMP_MATCH_MIN_SCORE",
    "STAMP_FALLBACK_MIN_SCORE",
]
