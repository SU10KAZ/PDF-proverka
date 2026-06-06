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

import inspect
import math
import os
import re
import unicodedata
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
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


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(raw)
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
# Сколько безопасных правых кандидатов держать на один левый лист (для
# mutual-best и для LLM-adjudication).
STAMP_CANDIDATE_TOPK = _env_int("STAGE_COMPARISON_STAMP_CANDIDATE_TOPK", 3)
# Нижний порог score, при котором пара становится КАНДИДАТОМ (для LLM или
# диагностики отклонений). Ниже auto-accept порога: слабые-но-правдоподобные
# пары не матчатся автоматически, а уходят на adjudication/ручной матч.
STAMP_LLM_CANDIDATE_MIN_SCORE = _env_float(
    "STAGE_COMPARISON_STAMP_LLM_CANDIDATE_MIN_SCORE", 0.20)
# Длина текст-сигнатуры из текст-слоя для слабого фолбэка.
_FALLBACK_TEXT_LEN = 120

# ─── Веса составного score (Stage 6) ───────────────────────────────────────
# Имя — основа; признаки дают bonus/penalty. order/позиция страницы в score НЕ
# входит (только tie-break при равных score).
_KIND_BONUS = 0.10
_KIND_PENALTY = 0.15
_SYSTEM_BONUS = 0.10
_EQUIPMENT_BONUS = 0.15
_FLOOR_BONUS = 0.08
_BUILDING_BONUS = 0.08
_TEXT_SIGNATURE_WEIGHT = 0.15


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


# ─── Слабое извлечение заголовка листа из СОДЕРЖИМОГО (derived/weak title) ──
# Когда штамп не читается / имя обрезано / отсутствует, заголовок листа часто
# виден в самом тексте страницы (Календарный план, Содержание тома, …). Берём
# его как weak-сигнал: для матчинга он менее надёжен, чем штамп, но позволяет
# распознать листы, которые иначе остаются безымянными. (regex по
# нормализованному тексту, канонический человекочитаемый заголовок.)
_KNOWN_SHEET_TITLE_PATTERNS = [
    (re.compile(r"календарн\w*\s+план"), "Календарный план"),
    (re.compile(r"график\s+производства\s+работ"), "График производства работ"),
    (re.compile(r"проект\s+организации\s+строительств\w*"), "Проект организации строительства"),
    (re.compile(r"строительн\w*\s+генеральн\w*\s+план"), "Строительный генеральный план"),
    (re.compile(r"стройгенплан\w*"), "Строительный генеральный план"),
    (re.compile(r"содержани\w*\s+тома"), "Содержание тома"),
    (re.compile(r"пояснительн\w*\s+записк\w*"), "Пояснительная записка"),
    (re.compile(r"текстов\w*\s+часть"), "Текстовая часть"),
    (re.compile(r"общие\s+данные"), "Общие данные"),
    (re.compile(r"ведомость\s+объемов\s+работ"), "Ведомость объёмов работ"),
    (re.compile(r"ведомость"), "Ведомость"),
    (re.compile(r"спецификаци\w*"), "Спецификация"),
]
# Сколько символов содержимого сканировать (заголовок — В САМОМ ВЕРХУ листа;
# узкое окно отсекает случайные упоминания «…в календарный план добавлено…» в
# теле листа «Разрешение на корректировку»).
_DERIVED_TITLE_SCAN_CHARS = 300
# Строки штамп-метаданных MD — вырезаем перед derived-извлечением, чтобы НЕ
# подхватить как «заголовок из содержимого» само штамп-имя листа (иначе
# «Общие данные система» дал бы спурьёзный derived «Общие данные»).
_META_LINE_RE = re.compile(
    r"^\s*\*\*\s*(?:Лист|Наименование\s+листа|Штамп|Шифр|Стадия|Обозначение)\b.*$",
    re.MULTILINE | re.IGNORECASE)


def _derive_title_from_text(text: str, *, exclude_norms: Optional[set] = None) -> str:
    """Слабое извлечение заголовка листа из текста страницы.

    Возвращает канонический человекочитаемый заголовок, если в первых
    ~`_DERIVED_TITLE_SCAN_CHARS` символах найден один из известных заголовков
    листов, иначе "". Берём САМОЕ РАННЕЕ вхождение. `exclude_norms` — множество
    нормализованных имён, которые игнорировать (обычно собственное штамп-имя
    листа: лист «Проект организации строительства» с блоком «Календарный план»
    должен дать derived «Календарный план», а не своё же имя). Никогда не падает.
    """
    if not text:
        return ""
    # Убрать штамп-метаданные (имя листа и т.п.) — derived только из содержимого.
    content = _META_LINE_RE.sub(" ", text)
    norm = normalize_sheet_name(content[:_DERIVED_TITLE_SCAN_CHARS])
    if not norm:
        return ""
    excl = exclude_norms or set()
    best, best_pos = "", None
    for rx, canon in _KNOWN_SHEET_TITLE_PATTERNS:
        if normalize_sheet_name(canon) in excl:
            continue
        m = rx.search(norm)
        if m and (best_pos is None or m.start() < best_pos):
            best, best_pos = canon, m.start()
    return best


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
    name_source: str          # md | inherited | derived_title | text_layer | none
    derived_name: str = ""    # weak-заголовок из содержимого (Календарный план…)


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
        # Weak-заголовок из содержимого страницы (body + текст-слой). Считаем
        # ВСЕГДА — даже когда есть штамп-имя: лист может иметь имя «Проект
        # организации строительства», но содержать блок «Календарный план».
        # Своё же имя листа исключаем, чтобы derived был именно из содержимого.
        # derived НЕ становится norm_name (иначе попал бы в exact-проход на полной
        # уверенности) — он используется только в weak derived-проходе, для
        # divergence-сигнала позиционного выравнивания и для отображения.
        _excl = {norm} if norm else set()
        derived = _derive_title_from_text(pr.body or "", exclude_norms=_excl)
        if not derived and pr.page in extra:
            derived = _derive_title_from_text(extra.get(pr.page) or "", exclude_norms=_excl)
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
            derived_name=derived,
        ))
    return recs


# ─── Каноникализация имени (Stage 3) ───────────────────────────────────────

# Безопасные алиасы: приводят близкие формулировки к единому виду. Намеренно
# консервативны — НЕ сливают разные виды схем (расчетная/принципиальная/
# структурная), только снимают служебные/избыточные слова. Ключи в
# нормализованной форме (lower, без пунктуации).
SAFE_ALIASES = {
    "однолинейная расчетная схема": "однолинейная схема",
    "расчетная однолинейная схема": "однолинейная схема",
    "принципиальная однолинейная схема": "однолинейная схема",
    "план расположения": "план",
    "план размещения": "план",
    "общие данные по рабочим чертежам": "общие данные",
    "общие данные рабочих чертежей": "общие данные",
    "спецификация оборудования изделий и материалов": "спецификация",
    "спецификация оборудования изделий материалов": "спецификация",
    "спецификация оборудования": "спецификация",
}

# Длиннее — раньше, чтобы префиксная замена не съела более длинную фразу.
_ALIAS_ITEMS = sorted(SAFE_ALIASES.items(), key=lambda kv: -len(kv[0]))


def canonicalize_sheet_name(normalized_name: str) -> str:
    """Второй слой нормализации: применить безопасные алиасы.

    Вход — уже нормализованное имя (после :func:`normalize_sheet_name`). Возвращает
    каноническую форму. Никогда не падает, идемпотентна для уже-канонических имён.
    """
    s = (normalized_name or "").strip()
    if not s:
        return ""
    for src, dst in _ALIAS_ITEMS:
        if src in s:
            s = s.replace(src, dst)
    return _WS_RE.sub(" ", s).strip()


# ─── Извлечение признаков листа (Stage 2) ──────────────────────────────────

# Системные «семьи» оборудования (нормализованные, без номера). MAIN — те,
# что НЕ должны путаться между собой (ВРУ vs ГРЩ vs ЩО) и используются в
# hard-gate. WEAK — слабые дисциплинарные/прочие теги (только bonus в score).
MAIN_SYSTEM_FAMILIES = {"вру", "грщ", "гру", "що", "щр", "авр", "рп", "ппу"}
_WEAK_SYSTEM_TAGS = {"эом", "эс", "сс", "апс", "соуэ", "вк", "ов", "вп", "qf", "qs"}
_ALL_SYSTEM_FAMILIES = MAIN_SYSTEM_FAMILIES | _WEAK_SYSTEM_TAGS

# Семьи, для которых ловим конкретный номер (ВРУ-1, ЩО-2, QF1, ГРЩ-1).
_EQUIP_FAMILIES = ["вру", "грщ", "гру", "що", "щр", "щк", "рп", "qf", "qs"]
_EQUIP_RE = re.compile(
    r"(" + "|".join(_EQUIP_FAMILIES) + r")\s*[-–—.]?\s*"
    r"(\d+(?:[.,]\d+)?)\s*(кв|квт|ква|вт|а|в)?",
    re.IGNORECASE,
)

# Виды листа (sheet_kind). Порядок проверки важен: специфичные раньше общих.
_KIND_RULES = [
    ("текстовая_часть", ("текстов",)),
    ("общие_данные", ("общие данны", "общих данны", "общие указани", "общих указани")),
    ("спецификация", ("специфика",)),
    ("ведомость", ("ведомост",)),
    ("узел", ("узел", "узлы", "узлов")),
    ("разрез", ("разрез", "сечени")),
    ("фасад", ("фасад",)),
    ("схема", ("схем",)),
    ("план", ("план", "планировк")),
]


def _extract_sheet_kind(low: str) -> Optional[str]:
    """Определить вид листа по lowered-имени. None если непонятно."""
    s = low or ""
    for kind, markers in _KIND_RULES:
        if any(m in s for m in markers):
            return kind
    return None


def _extract_system_tokens(norm_name: str) -> set:
    """Системные семьи (вру/грщ/що/…), номер отброшен."""
    out: set = set()
    for tok in (norm_name or "").split(" "):
        if not tok:
            continue
        for fam in _ALL_SYSTEM_FAMILIES:
            if tok == fam or (tok.startswith(fam) and tok[len(fam):].isdigit()):
                out.add(fam)
                break
    return out


def _extract_equipment_ids(low: str) -> set:
    """ID оборудования с номером: «вру-1», «що-2», «qf-1», «грщ-1».

    Номера, за которыми идёт единица напряжения/мощности (0,4кВ, 10кВ), —
    это рейтинг, а не индекс единицы; такие НЕ берём (иначе «ГРЩ-0,4кВ» дал бы
    ложный equipment-конфликт).
    """
    out: set = set()
    for m in _EQUIP_RE.finditer(low or ""):
        fam = m.group(1).lower()
        num = m.group(2).replace(",", ".")
        unit = (m.group(3) or "").lower()
        if unit:  # это номинал (кВ/А/кВт …), а не номер единицы
            continue
        out.add(f"{fam}-{num}")
    return out


# Этажи / уровни.
_WORD_FLOORS = {
    "подвал": "подвал", "паркинг": "паркинг", "кровл": "кровля",
    "цокол": "цоколь",
}
_FLOOR_NUM_RE = re.compile(r"(минус\s*|-)?(\d+)\s*(?:[-–]?\s*[а-я]{0,3}\s*)?этаж", re.IGNORECASE)
_FLOOR_NUM_RE2 = re.compile(r"этаж\w*\s*(минус\s*|-)?(\d+)", re.IGNORECASE)
_WORD_FLOOR_NUM = {"первого": 1, "первый": 1, "второго": 2, "второй": 2,
                   "третьего": 3, "третий": 3, "четвертого": 4, "четвертый": 4,
                   "пятого": 5, "пятый": 5}


def _extract_floor_tokens(low: str) -> set:
    """Этажи/уровни: «этаж:1», «этаж:-2», «подвал», «кровля», «технический»."""
    s = low or ""
    out: set = set()
    for sub, tok in _WORD_FLOORS.items():
        if sub in s:
            out.add(tok)
    if "технич" in s and "этаж" in s or "техэтаж" in s:
        out.add("технический")
    for rx in (_FLOOR_NUM_RE, _FLOOR_NUM_RE2):
        for m in rx.finditer(s):
            sign = -1 if (m.group(1) or "").strip() else 1
            try:
                out.add(f"этаж:{sign * int(m.group(2))}")
            except (TypeError, ValueError):
                continue
    for word, n in _WORD_FLOOR_NUM.items():
        if f"{word} этаж" in s or f"{word}го этаж" in s:
            out.add(f"этаж:{n}")
    return out


_BUILDING_RES = [
    ("корпус", re.compile(r"корпус[аы]?\s*([0-9][0-9.,\s]*|[а-я])\b", re.IGNORECASE)),
    ("секция", re.compile(r"секци[яюийе]?\s*([0-9]+|[а-я])\b", re.IGNORECASE)),
    ("блок", re.compile(r"блок[\s\-]+([0-9]+|[а-я])\b", re.IGNORECASE)),
]


def _extract_building_tokens(low: str) -> set:
    """Корпус/секция/блок: «корпус:1», «секция:2», «блок:а» (с namespace)."""
    s = low or ""
    out: set = set()
    for kind, rx in _BUILDING_RES:
        for m in rx.finditer(s):
            raw = m.group(1)
            nums = re.findall(r"\d+(?:\.\d+)?", raw)
            if nums:
                for n in nums:
                    out.add(f"{kind}:{n}")
            else:
                val = raw.strip().lower()
                if val:
                    out.add(f"{kind}:{val}")
    return out


# ─── Многостраничные листы (multipart / multisheet) ────────────────────────
#
# Один логический лист может в одной версии занимать 1 страницу, а в другой —
# несколько (начало / продолжение / конец). `sheet_group_key` — имя БЕЗ
# part-маркеров; `multipart_role` — роль части; `multipart_index` — номер части.
#
# Важно: маркеры с числом («часть 2», «ч 2») распознаются ТОЛЬКО при наличии
# числа, поэтому обычное имя «Текстовая часть» (без числа) НЕ ломается.

# normalize_sheet_name уже срезает «лист N», «стр. N», «(из N)». Здесь снимаем
# остаток: начало/продолжение/конец/окончание/часть N/ч N/из N.
_MULTIPART_STRIP_RES = [
    re.compile(r"\bначал[оа]\b"),
    re.compile(r"\b(?:продолжение|продолжени[яе]|продолж|прод)\b"),
    re.compile(r"\b(?:конец|окончание|оконч)\b"),
    re.compile(r"\bчасть\s*\d+\b"),
    re.compile(r"\bч\s*\d+\b"),
    re.compile(r"\bиз\s*\d+\b"),
]
_RE_PART_START = re.compile(r"\bначал[оа]\b")
_RE_PART_CONT = re.compile(r"\b(?:продолжение|продолжени[яе]|продолж|прод)\b")
_RE_PART_END = re.compile(r"\b(?:конец|окончание|оконч)\b")
_RE_PART_NUM = re.compile(r"\b(?:часть|ч)\s*(\d+)\b")
_RE_PART_TOTAL = re.compile(r"\bиз\s*(\d+)\b")


def extract_multipart(norm_name: str) -> tuple[str, Optional[str], Optional[int]]:
    """Разобрать имя на (group_key, role, index).

    role ∈ {None('single'), 'start', 'continuation', 'end'}.
    group_key — нормализованное имя без part-маркеров (затем каноникализуется
    вызывающим). Никогда не падает.
    """
    s = (norm_name or "").strip()
    if not s:
        return "", None, None
    total = None
    mt = _RE_PART_TOTAL.search(s)
    if mt:
        try:
            total = int(mt.group(1))
        except (TypeError, ValueError):
            total = None
    index = None
    mn = _RE_PART_NUM.search(s)
    if mn:
        try:
            index = int(mn.group(1))
        except (TypeError, ValueError):
            index = None

    role: Optional[str] = None
    if _RE_PART_START.search(s):
        role = "start"
        index = index or 1
    elif _RE_PART_END.search(s):
        role = "end"
    elif _RE_PART_CONT.search(s):
        role = "continuation"
    elif index is not None:
        if index == 1:
            role = "start"
        elif total is not None and index == total:
            role = "end"
        else:
            role = "continuation"

    base = s
    for rx in _MULTIPART_STRIP_RES:
        base = rx.sub(" ", base)
    base = _WS_RE.sub(" ", base).strip()
    return base, role, index


@dataclass
class SheetFeatures:
    """Расширенное описание листа для безопасного матчинга."""
    page: int
    sheet_no_raw: Optional[str]
    sheet_name_raw: Optional[str]
    normalized_name: str
    canonical_name: str
    source: str                  # md | inherited | derived_title | text_layer | none
    is_text_layer_fallback: bool
    derived_name: str
    is_derived_name: bool
    sheet_kind: Optional[str]
    system_tokens: set
    equipment_ids: set
    floor_tokens: set
    building_tokens: set
    canonical_tokens: set
    text_signature: set
    # Многостраничные листы (multipart):
    sheet_group_key: str = ""
    multipart_role: Optional[str] = None     # None/single | start | continuation | end
    multipart_index: Optional[int] = None


def extract_sheet_features(rec: SheetRec) -> SheetFeatures:
    """Построить :class:`SheetFeatures` из :class:`SheetRec`."""
    raw = (rec.sheet_name or "").strip()
    norm = rec.norm_name or ""
    # Для text_layer-фолбэка реального имени нет — признаки берём из текст-сигнатуры.
    low = (raw or norm).lower().replace("ё", "е")
    canon = canonicalize_sheet_name(norm)
    canon_tokens = set(_tokens(canon))
    base, role, index = extract_multipart(norm)
    group_key = canonicalize_sheet_name(base)
    return SheetFeatures(
        page=rec.page,
        sheet_no_raw=(rec.sheet_no or None),
        sheet_name_raw=(raw or None),
        normalized_name=norm,
        canonical_name=canon,
        source=rec.name_source,
        is_text_layer_fallback=(rec.name_source == "text_layer"),
        derived_name=(rec.derived_name or ""),
        is_derived_name=(rec.name_source == "derived_title"),
        sheet_kind=_extract_sheet_kind(low),
        system_tokens=_extract_system_tokens(norm),
        equipment_ids=_extract_equipment_ids(low),
        floor_tokens=_extract_floor_tokens(low),
        building_tokens=_extract_building_tokens(low),
        canonical_tokens=canon_tokens,
        text_signature=canon_tokens,
        sheet_group_key=group_key,
        multipart_role=role,
        multipart_index=index,
    )


# ─── Hard-gates (Stage 4) ──────────────────────────────────────────────────

_KIND_HARD_PAIRS = {
    frozenset({"план", "спецификация"}),
    frozenset({"план", "ведомость"}),
}


def _group_equipment(ids: set) -> dict:
    """{«вру-1»,«вру-2»} → {«вру»: {«1»,«2»}}."""
    out: dict = defaultdict(set)
    for eid in ids:
        fam, _, num = eid.rpartition("-")
        if fam and num:
            out[fam].add(num)
    return out


def _group_building(tokens: set) -> dict:
    """{«корпус:1»} → {«корпус»: {«1»}}."""
    out: dict = defaultdict(set)
    for t in tokens:
        kind, _, val = t.partition(":")
        if kind and val:
            out[kind].add(val)
    return out


def get_hard_conflict(left: SheetFeatures, right: SheetFeatures) -> Optional[str]:
    """Вернуть причину жёсткого конфликта пары листов или None.

    Запреты (precision > recall): разные единицы одного типа (ВРУ-1 vs ВРУ-2),
    разные основные системы (ГРЩ vs ВРУ), разные этажи, разные корпуса/секции/
    блоки, несовместимые виды листа (план↔спецификация/ведомость; схема↔
    спецификация без общего оборудования).
    """
    # 1. Одна семья оборудования, но непересекающиеся номера → разные единицы.
    lg = _group_equipment(left.equipment_ids)
    rg = _group_equipment(right.equipment_ids)
    for fam in set(lg) & set(rg):
        if lg[fam].isdisjoint(rg[fam]):
            return f"equipment_conflict:{fam}"
    # 2. Основные системы присутствуют с обеих сторон, но не пересекаются.
    lmain = left.system_tokens & MAIN_SYSTEM_FAMILIES
    rmain = right.system_tokens & MAIN_SYSTEM_FAMILIES
    if lmain and rmain and lmain.isdisjoint(rmain):
        return "system_conflict"
    # 3. Разные этажи.
    if left.floor_tokens and right.floor_tokens and left.floor_tokens.isdisjoint(right.floor_tokens):
        return "floor_conflict"
    # 4. Разные корпус/секция/блок (в пределах одного kind).
    lb = _group_building(left.building_tokens)
    rb = _group_building(right.building_tokens)
    for kind in set(lb) & set(rb):
        if lb[kind].isdisjoint(rb[kind]):
            return "building_conflict"
    # 5. Несовместимые виды листа.
    lk, rk = left.sheet_kind, right.sheet_kind
    if lk and rk and lk != rk:
        pair = frozenset({lk, rk})
        if pair in _KIND_HARD_PAIRS:
            return "kind_conflict"
        if pair == frozenset({"схема", "спецификация"}) and not (left.equipment_ids & right.equipment_ids):
            return "kind_conflict"
    return None


# ─── Составной score (Stage 6) ─────────────────────────────────────────────

def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if not inter:
        return 0.0
    return inter / float(len(a | b))


def _composite_score(lf: SheetFeatures, rf: SheetFeatures,
                     idf: dict[str, float]) -> tuple[float, dict]:
    """Составной score пары + разбор (positive/negative evidence).

    Имя — основа; признаки дают bonus/penalty. order/позиция страницы НЕ влияет
    на score (используется лишь как tie-break в матчере). Слабую по имени пару
    структурные бонусы не «вытаскивают»: нужна непустая близость имени ИЛИ общее
    оборудование.
    """
    name_sim = _weighted_sim(lf.canonical_name, rf.canonical_name, idf)
    eq_shared = lf.equipment_ids & rf.equipment_ids
    if name_sim <= 0.0 and not eq_shared:
        return 0.0, {"name_sim": 0.0, "positive": [], "negative": []}

    score = name_sim
    positive: list[str] = []
    negative: list[str] = []

    if lf.sheet_kind and rf.sheet_kind:
        if lf.sheet_kind == rf.sheet_kind:
            score += _KIND_BONUS
            positive.append(f"вид:{lf.sheet_kind}")
        else:
            score -= _KIND_PENALTY
            negative.append(f"вид:{lf.sheet_kind}≠{rf.sheet_kind}")

    sys_shared = lf.system_tokens & rf.system_tokens
    if sys_shared:
        score += _SYSTEM_BONUS
        positive.append("система:" + ",".join(sorted(sys_shared)))
    else:
        only = (lf.system_tokens ^ rf.system_tokens) & MAIN_SYSTEM_FAMILIES
        if only:
            negative.append("система:" + ",".join(sorted(only)))

    if eq_shared:
        score += _EQUIPMENT_BONUS
        positive.append("оборуд:" + ",".join(sorted(eq_shared)))

    fl_shared = lf.floor_tokens & rf.floor_tokens
    if fl_shared:
        score += _FLOOR_BONUS
        positive.append("этаж:" + ",".join(sorted(fl_shared)))

    bl_shared = lf.building_tokens & rf.building_tokens
    if bl_shared:
        score += _BUILDING_BONUS
        positive.append("корпус:" + ",".join(sorted(bl_shared)))

    if lf.is_text_layer_fallback or rf.is_text_layer_fallback:
        score += _TEXT_SIGNATURE_WEIGHT * _jaccard(lf.text_signature, rf.text_signature)

    score = max(0.0, min(1.0, score))
    return score, {"name_sim": round(name_sim, 3),
                   "positive": positive, "negative": negative}


# ─── Multipart group alignment (Pass 1.5) ──────────────────────────────────

def _align_multipart_parts(
    lparts: list[tuple[int, Optional[str], Optional[int]]],
    rparts: list[tuple[int, Optional[str], Optional[int]]],
) -> tuple[list[tuple[int, int]], list[int], list[int]]:
    """Сопоставить части одного логического листа по ролям/порядку.

    parts: список (page, role, index), порядок — по странице (= порядок в
    документе). Возвращает (pairs[(lpage,rpage)], left_extra_pages,
    right_extra_pages). Одна страница используется не более одного раза.

    Логика (role-aware, precision > recall):
      1. start↔start (роль 'start' либо single/None как якорь начала);
      2. end↔end;
      3. остальные части — позиционно (по порядку страниц);
      4. лишние части → односторонние.
    """
    L = sorted(lparts, key=lambda t: t[0])
    R = sorted(rparts, key=lambda t: t[0])
    used_l: set[int] = set()
    used_r: set[int] = set()
    pairs: list[tuple[int, int]] = []

    def _first(parts, used, pred):
        for p in parts:
            if p[0] not in used and pred(p):
                return p
        return None

    def _last(parts, used, pred):
        for p in reversed(parts):
            if p[0] not in used and pred(p):
                return p
        return None

    is_start = lambda p: p[1] in ("start", None)   # noqa: E731 — single = якорь начала
    is_end = lambda p: p[1] == "end"               # noqa: E731

    ls = _first(L, used_l, is_start)
    rs = _first(R, used_r, is_start)
    if ls and rs:
        pairs.append((ls[0], rs[0]))
        used_l.add(ls[0])
        used_r.add(rs[0])

    le = _last(L, used_l, is_end)
    re_ = _last(R, used_r, is_end)
    if le and re_:
        pairs.append((le[0], re_[0]))
        used_l.add(le[0])
        used_r.add(re_[0])

    rem_l = [p for p in L if p[0] not in used_l]
    rem_r = [p for p in R if p[0] not in used_r]
    for a, b in zip(rem_l, rem_r):
        pairs.append((a[0], b[0]))
        used_l.add(a[0])
        used_r.add(b[0])

    left_extra = [p[0] for p in L if p[0] not in used_l]
    right_extra = [p[0] for p in R if p[0] not in used_r]
    pairs.sort(key=lambda x: x[0])
    return pairs, left_extra, right_extra


# ─── Матчинг ───────────────────────────────────────────────────────────────

def _matched_item(slot: int, left: SheetRec, right: SheetRec,
                  score: float, match_type: str, *,
                  needs_review: Optional[bool] = None,
                  reason: str = "", positive_evidence: Optional[list] = None,
                  negative_evidence: Optional[list] = None,
                  risk_flags: Optional[list] = None,
                  confidence: Optional[float] = None,
                  diag: Optional[dict] = None) -> dict:
    note = f"{(left.sheet_name or right.sheet_name or '').strip()[:60]} · {match_type} {score:.2f}"
    if needs_review is None:
        needs_review = match_type in ("fuzzy_name", "fuzzy_structural",
                                      "text_layer", "llm_semantic",
                                      "derived_name_match")
    conf = score if confidence is None else confidence
    item = {
        "slot": slot,
        "left_page": left.page,
        "right_page": right.page,
        "mode": "manual",
        "note": note.strip(" ·"),
        # display-only поля для UI (validate() их отбросит при сохранении)
        "match": True,
        "match_type": match_type,
        "score": round(score, 3),
        "confidence": round(conf, 3),
        "left_sheet_name": left.sheet_name,
        "right_sheet_name": right.sheet_name,
        "left_sheet_no": left.sheet_no,
        "right_sheet_no": right.sheet_no,
        "left_derived_sheet_name": left.derived_name or "",
        "right_derived_sheet_name": right.derived_name or "",
        "left_name_source": left.name_source,
        "right_name_source": right.name_source,
        "is_graphic": bool(left.is_graphic or right.is_graphic),
        "needs_review": bool(needs_review),
        "reason": reason or "",
        "positive_evidence": list(positive_evidence or []),
        "negative_evidence": list(negative_evidence or []),
        "risk_flags": list(risk_flags or []),
    }
    if diag:
        item["match_diag"] = diag
    return item


def _one_sided_item(slot: int, rec: SheetRec, side: str, *,
                    match_type: Optional[str] = None, reason: str = "",
                    positive_evidence: Optional[list] = None) -> dict:
    return {
        "slot": slot,
        "left_page": rec.page if side == "left" else None,
        "right_page": rec.page if side == "right" else None,
        "mode": "manual",
        "note": match_type or f"{side}_only",
        "match": False,
        "match_type": match_type or f"{side}_only",
        "score": 0.0,
        "left_sheet_name": rec.sheet_name if side == "left" else "",
        "right_sheet_name": rec.sheet_name if side == "right" else "",
        "left_derived_sheet_name": rec.derived_name or "" if side == "left" else "",
        "right_derived_sheet_name": rec.derived_name or "" if side == "right" else "",
        "left_name_source": rec.name_source if side == "left" else "",
        "right_name_source": rec.name_source if side == "right" else "",
        "is_graphic": bool(rec.is_graphic),
        "needs_review": False,
        "reason": reason or "",
        "positive_evidence": list(positive_evidence or []),
        "negative_evidence": [],
        "risk_flags": [],
    }


# ─── Позиционное выравнивание нераспознанных начальных/межанкорных листов ───

POSITIONAL_ALIGN_TYPE = "positional_alignment"
POSITIONAL_ALIGN_RISK = "unconfirmed_alignment"
POSITIONAL_ALIGN_REASON = (
    "Нераспознанные начальные листы выровнены по порядку, чтобы не "
    "смещать дальнейшее сопоставление.")


def _positional_item(left_item: dict, right_item: dict) -> dict:
    """Собрать строку позиционного выравнивания из пары односторонних строк.

    Это НЕ уверенное сопоставление по штампу: страницы просто стоят напротив
    друг друга по порядку (титульные/вводные листы без рамок). `match=False`,
    поэтому такая строка не попадает в `matched_count`. Обе стороны заполнены.
    """
    return {
        "slot": 0,
        "left_page": left_item.get("left_page"),
        "right_page": right_item.get("right_page"),
        "mode": "manual",
        "note": "позиционно",
        # display-only поля (validate() их отбросит при сохранении)
        "match": False,
        "match_type": POSITIONAL_ALIGN_TYPE,
        "score": 0.0,
        "confidence": 0.0,
        "left_sheet_name": left_item.get("left_sheet_name") or "",
        "right_sheet_name": right_item.get("right_sheet_name") or "",
        "left_sheet_no": left_item.get("left_sheet_no"),
        "right_sheet_no": right_item.get("right_sheet_no"),
        "left_derived_sheet_name": left_item.get("left_derived_sheet_name") or "",
        "right_derived_sheet_name": right_item.get("right_derived_sheet_name") or "",
        "left_name_source": left_item.get("left_name_source") or "",
        "right_name_source": right_item.get("right_name_source") or "",
        "is_graphic": bool(left_item.get("is_graphic") or right_item.get("is_graphic")),
        "needs_review": True,
        "reason": POSITIONAL_ALIGN_REASON,
        "positive_evidence": [],
        "negative_evidence": [],
        "risk_flags": [POSITIONAL_ALIGN_RISK],
    }


def _side_name_signal(item: dict, side: str) -> tuple[bool, str]:
    """Есть ли у односторонней строки ОСМЫСЛЕННОЕ ШТАМП-имя листа (md/inherited).

    Только штамп-имя. Derived-заголовки из содержимого НЕ считаются осмысленными
    для позиционного выравнивания: титульные/обложечные листы часто выводят имя
    РАЗДЕЛА/ТОМА из шапки («Раздел 7 Проект организации строительства»), и это
    всё ещё front-matter, который надо выравнивать позиционно, а не divergence.
    Безымянные (в т.ч. обложки с derived-именем) → (False, "").
    """
    src = item.get(f"{side}_name_source") or ""
    nm = (item.get(f"{side}_sheet_name") or "").strip()
    if src in ("md", "inherited") and nm:
        return True, normalize_sheet_name(nm)
    return False, ""


def _positional_compatible(left_item: dict, right_item: dict) -> bool:
    """Можно ли позиционно поставить эти листы напротив (без divergence).

    Divergence (несовместимо, zip останавливаем) — когда у ОДНОЙ стороны уже
    начался осмысленный блок (штамп-имя или derived-заголовок), а у другой нет
    (`lm != rm`). Это прямое требование: «если с одной стороны осмысленный блок,
    а с другой нет — зип остановить».

    Совместимо, когда обе стороны без имени (титульные/вводные без рамок) ИЛИ обе
    осмысленные (front-matter с разными подписями между стадиями — например
    «Обложка тома» ↔ «Лист регистрации» — выравниваем позиционно, не выдавая за
    уверенный матч). Одинаковые имена сюда не попадают: их забрал exact-проход.
    """
    lm, _ = _side_name_signal(left_item, "left")
    rm, _ = _side_name_signal(right_item, "right")
    return lm == rm


def _apply_positional_alignment(items: list[dict]) -> list[dict]:
    """Выровнять позиционно нераспознанный ВЕДУЩИЙ прогон (до первого anchor'а).

    Проблема: титульные/вводные листы без рамок не матчатся по штампу, и сборка
    раскладывает их как `1→∅ / ∅→1 / 2→∅ / ∅→2`, смещая ВСЮ дальнейшую карту.
    До первого уверенного anchor'а ставим непарные листы напротив друг друга
    позиционно (`positional_alignment`, `unconfirmed_alignment`) — не уверенный
    матч, но карта не съезжает.

    Умная остановка по смыслу (а не механический zip всего прогона):
    зипуем начальную часть, пока страницы выглядят как стартовые/безымянные ИЛИ
    имеют одинаковый заголовок; как только начинается divergence (у одной стороны
    осмысленный блок, у другой нет, либо разные имена) — STOP, остаток остаётся
    односторонним (`_positional_compatible`). Дополнительно — boundary-buffer:
    при СИЛЬНОМ перекосе длин (короткий прогон полностью съеден прямо перед
    далёким anchor'ом) последнюю безымянную пару не зипуем (нет доказательств).

    Только ВЕДУЩИЙ прогон (намеренно консервативно):
      * межанкорные прогоны НЕ трогаем (на реальных данных зипуют далёкие
        несвязанные листы L37↔R3); trailing — реально добавленные/удалённые;
      * нет anchor'а / он первый → нечего выравнивать «до»;
      * непарный прогон только с одной стороны → positional НЕ выдумываем;
      * каждая страница участвует не более одного раза.

    Хвост: остаток правой стороны идёт ПЕРЕД остатком левой, чтобы непарный
    правый лист не «висел» отдельным блоком под левыми титульниками.
    """
    first_anchor = next((i for i, it in enumerate(items) if it.get("match")), None)
    if first_anchor is None or first_anchor == 0:
        return items

    lead = items[:first_anchor]
    if any(it.get("match_type") not in ("left_only", "right_only") for it in lead):
        return items  # неожиданный тип в начале → safety-выход

    lefts = [it for it in lead if it["match_type"] == "left_only"]
    rights = [it for it in lead if it["match_type"] == "right_only"]
    if not lefts or not rights:
        return items  # односторонний ведущий прогон — positional не нужен

    # Зипуем по порядку, пока совместимо; на первой divergence — стоп.
    k = 0
    limit = min(len(lefts), len(rights))
    while k < limit and _positional_compatible(lefts[k], rights[k]):
        k += 1
    if k == 0:
        return items  # сразу divergence — ничего не выравниваем

    # Boundary-buffer (precision > recall): когда ведущие прогоны СИЛЬНО неравны
    # и короткий прогон ПОЛНОСТЬЮ съеден зипом прямо перед далёким anchor'ом, его
    # последнюю БЕЗЫМЯННУЮ страницу не приклеиваем к длинной стороне без
    # доказательств — расхождение длин означает, что где-то перед anchor'ом
    # вставлены лишние листы, и соответствие последней пары не подтверждено.
    # Пример: left_run=1..6, right_run=1..3, anchor 7↔4 → 1↔1, 2↔2 (НЕ 3↔3).
    if k >= 2 and len(lefts) != len(rights) and k == limit:
        a = items[first_anchor]
        offset = abs((a.get("left_page") or 0) - (a.get("right_page") or 0))
        last_lm, _ = _side_name_signal(lefts[k - 1], "left")
        last_rm, _ = _side_name_signal(rights[k - 1], "right")
        if offset >= 2 and not last_lm and not last_rm:
            k -= 1  # последнюю безымянную пару короткого прогона не зипуем

    new_lead: list[dict] = [_positional_item(lefts[x], rights[x]) for x in range(k)]
    new_lead.extend(rights[k:])    # остаток справа — раньше, чтобы не «висел» ниже
    new_lead.extend(lefts[k:])     # остаток слева — left_only как есть

    out = new_lead + items[first_anchor:]
    for idx, it in enumerate(out):
        it["slot"] = idx + 1
    return out


def _call_llm_match_fn(fn, rem_left, rem_right, tasks):
    """Вызвать LLM-fn совместимо: новый fn принимает 3-й arg (tasks/candidates),
    старый — только (rem_left, rem_right). Определяем по сигнатуре."""
    accepts_tasks = False
    try:
        params = inspect.signature(fn).parameters
        accepts_tasks = (len(params) >= 3) or any(
            p.kind == inspect.Parameter.VAR_POSITIONAL for p in params.values())
    except (TypeError, ValueError):
        accepts_tasks = False
    if accepts_tasks:
        return fn(rem_left, rem_right, tasks)
    return fn(rem_left, rem_right)


def match_sheet_indexes(
    left: list[SheetRec],
    right: list[SheetRec],
    *,
    min_score: float = STAMP_MATCH_MIN_SCORE,
    fallback_min_score: float = STAMP_FALLBACK_MIN_SCORE,
    llm_match_fn=None,
) -> dict:
    """Сопоставить листы двух сторон по имени/признакам и собрать page_alignment.

    Проходы (precision > recall — лучше оставить лист непарным, чем ошибиться):
      1.  exact: одинаковое нормализованное имя; дубликаты — в порядке появления.
      1b. exact_canonical: совпадение после :func:`canonicalize_sheet_name`
          (снятие служебных слов — «расчетная», «расположения» …).
      1.5 multipart: один логический лист, разбитый на части (начало/продолжение/
          конец) — сопоставление по `sheet_group_key` + ролям; лишние части →
          односторонние `multipart_continuation`.
      2.  fuzzy: матрица кандидатов left×right на составном score (имя + признаки),
          приём только при mutual-best + threshold + margin + отсутствии
          hard-конфликта.
      3.  [опц.] LLM-adjudication: для непарного остатка строим top-k безопасных
          кандидатов и просим LLM выбрать ОДИН из них (или null). Выбор вне
          кандидатов / с hard-конфликтом / уже занятый — игнорируется. LLM не
          может перебить детерминированные пары и НЕ создаёт multipart-группы.
      4.  остаток → left_only / right_only.

    Сборка items: слоты в порядке левых страниц; right-only вставляются по
    возрастанию их номера так, чтобы сматченные листы стояли НАПРОТИВ друг друга.
    """
    matches: dict[int, dict] = {}     # left_page -> match payload
    used_right: set[int] = set()
    rejected: list[dict] = []         # hard-gate отклонения (диагностика)
    # multipart-части, ставшие односторонними (не матч, но и не fuzzy/LLM-кандидаты):
    consumed_left: set[int] = set()
    consumed_right: set[int] = set()
    multipart_one_sided: dict[tuple[str, int], dict] = {}  # (side, page) -> {reason, group_key}
    multipart_count = 0
    left_by_page = {r.page: r for r in left}
    right_by_page = {r.page: r for r in right}

    # Признаки листов (Stage 2).
    lf_by_page = {r.page: extract_sheet_features(r) for r in left}
    rf_by_page = {r.page: extract_sheet_features(r) for r in right}

    # Дубликаты имён на стороне → risk flag.
    dup_left = {n for n, c in Counter(r.norm_name for r in left if r.norm_name).items() if c > 1}
    dup_right = {n for n, c in Counter(r.norm_name for r in right if r.norm_name).items() if c > 1}

    def _risk(lp: int, rp: int, extra: Optional[list] = None) -> list:
        flags = list(extra or [])
        lf, rf = lf_by_page[lp], rf_by_page[rp]
        if lf.normalized_name in dup_left or rf.normalized_name in dup_right:
            flags.append("duplicate_sheet_name")
        if lf.is_text_layer_fallback or rf.is_text_layer_fallback:
            flags.append("text_layer_fallback")
        if lf.is_derived_name or rf.is_derived_name:
            flags.append("derived_name")
        return list(dict.fromkeys(flags))

    # Pass 1 — exact normalized name, дубликаты в порядке появления.
    right_name_q: dict[str, deque[int]] = defaultdict(deque)
    for r in sorted(right, key=lambda x: x.page):
        if r.norm_name:
            right_name_q[r.norm_name].append(r.page)
    for l in sorted(left, key=lambda x: x.page):
        if l.norm_name and right_name_q.get(l.norm_name):
            rp = right_name_q[l.norm_name].popleft()
            mtype = "exact_name" if l.name_source != "text_layer" else "text_layer"
            matches[l.page] = {"rp": rp, "score": 1.0, "mtype": mtype,
                               "reason": "точное совпадение имени листа",
                               "positive": [l.sheet_name or l.norm_name],
                               "negative": [], "risk": _risk(l.page, rp),
                               "confidence": 1.0}
            used_right.add(rp)

    # Pass 1b — exact canonical name (после безопасных алиасов).
    right_canon_q: dict[str, deque[int]] = defaultdict(deque)
    for r in sorted(right, key=lambda x: x.page):
        if r.page in used_right:
            continue
        cn = rf_by_page[r.page].canonical_name
        if cn:
            right_canon_q[cn].append(r.page)
    for l in sorted(left, key=lambda x: x.page):
        if l.page in matches:
            continue
        cn = lf_by_page[l.page].canonical_name
        if not cn or not right_canon_q.get(cn):
            continue
        rp = right_canon_q[cn][0]
        if get_hard_conflict(lf_by_page[l.page], rf_by_page[rp]):
            continue  # каноническое совпадение, но конфликт признаков → пропуск
        right_canon_q[cn].popleft()
        is_text = (l.name_source == "text_layer"
                   or right_by_page[rp].name_source == "text_layer")
        mtype = "text_layer" if is_text else "exact_canonical_name"
        matches[l.page] = {"rp": rp, "score": 0.95, "mtype": mtype,
                           "reason": "каноническое совпадение (сняты служебные слова)",
                           "positive": [lf_by_page[l.page].canonical_name],
                           "negative": [], "risk": _risk(l.page, rp),
                           "confidence": 0.95}
        used_right.add(rp)

    # ─── Pass 1.5 — multipart/group match ──────────────────────────────────
    # Один логический лист, разбитый на части (начало/продолжение/конец), в одной
    # версии может быть одной страницей, в другой — несколькими. Группируем
    # ОСТАТОК (после exact/canonical) по `sheet_group_key` и сопоставляем части
    # по ролям. Срабатывает только при реальном multipart-сигнале (>1 части на
    # стороне ИЛИ явная роль) — чистые 1↔1 уже разобраны canonical-проходом.
    lg: dict[str, list[int]] = defaultdict(list)
    rg: dict[str, list[int]] = defaultdict(list)
    for l in sorted(left, key=lambda x: x.page):
        if l.page in matches:
            continue
        gk = lf_by_page[l.page].sheet_group_key
        if gk and len(gk) >= 2:
            lg[gk].append(l.page)
    for r in sorted(right, key=lambda x: x.page):
        if r.page in used_right:
            continue
        gk = rf_by_page[r.page].sheet_group_key
        if gk and len(gk) >= 2:
            rg[gk].append(r.page)

    for gk in [k for k in lg if k in rg]:
        L = lg[gk]
        R = rg[gk]
        roles_present = any(lf_by_page[p].multipart_role for p in L) or \
            any(rf_by_page[p].multipart_role for p in R)
        if len(L) <= 1 and len(R) <= 1 and not roles_present:
            continue  # 1↔1 без ролей → отдать fuzzy (canonical уже не сматчил)
        lparts = [(p, lf_by_page[p].multipart_role, lf_by_page[p].multipart_index) for p in L]
        rparts = [(p, rf_by_page[p].multipart_role, rf_by_page[p].multipart_index) for p in R]
        pairs, left_extra, right_extra = _align_multipart_parts(lparts, rparts)
        # hard-gate каждую под-пару; конфликт где-либо → пропускаем всю группу.
        conflict = None
        for lp, rp in pairs:
            c = get_hard_conflict(lf_by_page[lp], rf_by_page[rp])
            if c:
                conflict = c
                if len(rejected) < 60:
                    rejected.append({"left_page": lp, "right_page": rp,
                                     "rejected_reason": c, "score": 1.0})
                break
        if conflict or not pairs:
            continue
        for i, (lp, rp) in enumerate(pairs):
            mtype = "exact_multipart_group" if i == 0 else "multipart_group"
            matches[lp] = {
                "rp": rp, "score": 0.98, "mtype": mtype,
                "reason": (f"Один логический лист разбит на несколько страниц: "
                           f"{gk} начало/продолжение/конец."),
                "positive": [f"sheet_group_key: {gk}", "multipart: start/continuation/end"],
                "negative": [], "risk": _risk(lp, rp), "confidence": 0.98,
                "diag": {"multipart": True, "group_key": gk,
                         "left_role": lf_by_page[lp].multipart_role,
                         "right_role": rf_by_page[rp].multipart_role},
            }
            used_right.add(rp)
            multipart_count += 1
        cont_reason = (f"Продолжение многостраничного листа {gk}; "
                       f"на другой стороне отдельной страницы нет.")
        for lp in left_extra:
            consumed_left.add(lp)
            multipart_one_sided[("left", lp)] = {"reason": cont_reason, "group_key": gk}
        for rp in right_extra:
            consumed_right.add(rp)
            multipart_one_sided[("right", rp)] = {"reason": cont_reason, "group_key": gk}

    # IDF-веса по каноническим именам пары (разделяет общий бойлерплейт-префикс).
    idf = _build_idf([lf_by_page[r.page].canonical_name for r in left
                      if lf_by_page[r.page].canonical_name]
                     + [rf_by_page[r.page].canonical_name for r in right
                        if rf_by_page[r.page].canonical_name])

    # ─── Матрица кандидатов (Stage 5): остаток после exact/canonical/multipart ──
    # consumed_* — multipart-части, ставшие односторонними: их НЕ матчим повторно.
    rem_left_pages = [l.page for l in sorted(left, key=lambda x: x.page)
                      if l.page not in matches and l.page not in consumed_left]
    rem_right_pages = [r.page for r in sorted(right, key=lambda x: x.page)
                       if r.page not in used_right and r.page not in consumed_right]
    lcount = max(1, len(left))
    rcount = max(1, len(right))
    expected_rp = {lp: lp / lcount * rcount for lp in rem_left_pages}
    cand: dict[tuple[int, int], tuple[float, dict]] = {}
    for lp in rem_left_pages:
        lf = lf_by_page[lp]
        for rp in rem_right_pages:
            rf = rf_by_page[rp]
            conflict = get_hard_conflict(lf, rf)
            sc, bd = _composite_score(lf, rf, idf)
            if conflict:
                # Записываем «соблазнительные» (похожие по имени) конфликты —
                # penalty может занизить composite, поэтому смотрим и на name_sim.
                attract = max(sc, bd.get("name_sim", 0.0))
                if attract >= STAMP_LLM_CANDIDATE_MIN_SCORE and len(rejected) < 60:
                    rejected.append({"left_page": lp, "right_page": rp,
                                     "rejected_reason": conflict,
                                     "score": round(attract, 3)})
                continue
            if sc > 0.0:
                cand[(lp, rp)] = (sc, bd)

    # best/second по строке (left) и по столбцу (right) для mutual-best.
    def _top2(pairs: list[tuple[float, float, int]]):
        pairs.sort(reverse=True)
        best = pairs[0]
        second = pairs[1][0] if len(pairs) > 1 else 0.0
        return best, second

    best_for_left: dict[int, tuple] = {}
    for lp in rem_left_pages:
        scored = [(sc, -abs(rp - expected_rp[lp]), rp)
                  for (l2, rp), (sc, _bd) in cand.items() if l2 == lp]
        if scored:
            (bsc, _, brp), ssc = _top2(scored)
            best_for_left[lp] = (brp, bsc, ssc)
    best_for_right: dict[int, tuple] = {}
    for rp in rem_right_pages:
        scored = [(sc, -abs(lp - expected_rp.get(lp, lp)), lp)
                  for (lp, r2), (sc, _bd) in cand.items() if r2 == rp]
        if scored:
            (bsc, _, blp), ssc = _top2(scored)
            best_for_right[rp] = (blp, bsc, ssc)

    # Pass 2 — fuzzy приём только при ВЗАИМНОМ лучшем + threshold + margin.
    for lp in rem_left_pages:
        if lp not in best_for_left:
            continue
        rp, sc, sec_l = best_for_left[lp]
        if rp in used_right:
            continue
        b_lp, rsc, sec_r = best_for_right.get(rp, (None, 0.0, 0.0))
        if b_lp != lp:
            continue  # не взаимно-лучшая пара → не предлагаем
        lf, rf = lf_by_page[lp], rf_by_page[rp]
        is_text = lf.is_text_layer_fallback or rf.is_text_layer_fallback
        threshold = fallback_min_score if is_text else min_score
        if sc < threshold:
            continue
        if (sc - sec_l) < STAMP_MATCH_MIN_MARGIN and sc < 0.999:
            continue
        if (rsc - sec_r) < STAMP_MATCH_MIN_MARGIN and rsc < 0.999:
            continue
        bd = cand[(lp, rp)][1]
        risk = []
        if (sc - sec_l) < STAMP_MATCH_MIN_MARGIN * 2:
            risk.append("low_margin")
        mtype = "text_layer" if is_text else (
            "fuzzy_structural" if bd.get("positive") else "fuzzy_name")
        reason = ("совпали: " + ", ".join(bd["positive"][:3])) if bd.get("positive") \
            else "близкое имя листа"
        matches[lp] = {"rp": rp, "score": sc, "mtype": mtype, "reason": reason,
                       "positive": bd.get("positive", []),
                       "negative": bd.get("negative", []),
                       "risk": _risk(lp, rp, risk), "confidence": sc,
                       "diag": {"score": round(sc, 3), "best_score": round(sc, 3),
                                "second_best_score": round(sec_l, 3),
                                "margin": round(sc - sec_l, 3), "mutual_best": True,
                                "match_type": mtype, "name_sim": bd.get("name_sim")}}
        used_right.add(rp)

    # ─── Pass 2.6 — derived/weak title match (Календарный план и т.п.) ──────
    # Лист может НЕ матчиться по штампу (имя обрезано/иное), но содержать
    # известный заголовок в теле страницы (left «Проект организации
    # строительства» содержит блок «Календарный план» ↔ right «Календарный
    # план»). Сопоставляем ОСТАТОК по derived-заголовку: ключ = нормализованный
    # derived_name слева == norm_name ИЛИ derived_name справа. Слабый матч:
    # низкая уверенность, risk «derived_name», hard-gate соблюдается.
    derived_match_count = 0
    right_derived_q: dict[str, deque[int]] = defaultdict(deque)
    for r in sorted(right, key=lambda x: x.page):
        if r.page in used_right:
            continue
        for key in {normalize_sheet_name(rf_by_page[r.page].derived_name),
                    rf_by_page[r.page].normalized_name}:
            if key and len(key) >= 4:
                right_derived_q[key].append(r.page)
    for l in sorted(left, key=lambda x: x.page):
        if l.page in matches:
            continue
        dk = normalize_sheet_name(lf_by_page[l.page].derived_name)
        if not dk or len(dk) < 4:
            continue
        # выбрать первый свободный правый кандидат по этому ключу
        # ВНИМАНИЕ: не переиспользовать имя `cand` — это матрица кандидатов
        # (dict (lp,rp)->(score,bd)), нужная Pass 3 (LLM adjudication) ниже.
        rp = None
        while right_derived_q.get(dk):
            cand_page = right_derived_q[dk].popleft()
            if cand_page not in used_right:
                rp = cand_page
                break
        if rp is None:
            continue
        if get_hard_conflict(lf_by_page[l.page], rf_by_page[rp]):
            continue
        matches[l.page] = {
            "rp": rp, "score": 0.6, "mtype": "derived_name_match",
            "reason": f"совпадение по заголовку из содержимого: {lf_by_page[l.page].derived_name}",
            "positive": [f"derived: {lf_by_page[l.page].derived_name}"],
            "negative": [], "risk": _risk(l.page, rp, ["derived_name"]),
            "confidence": 0.6}
        used_right.add(rp)
        derived_match_count += 1

    # ─── Pass 3 — LLM adjudication по безопасным кандидатам (Stage 7) ───
    llm_match_count = 0
    if llm_match_fn is not None:
        rem_left2 = [left_by_page[lp] for lp in rem_left_pages if lp not in matches]
        rem_right2 = [right_by_page[rp] for rp in rem_right_pages if rp not in used_right]
        tasks: list[dict] = []
        allowed: dict[int, set] = {}
        for l in rem_left2:
            lp = l.page
            lf = lf_by_page[lp]
            cands = []
            for r in rem_right2:
                rp = r.page
                key = (lp, rp)
                if key not in cand:
                    continue
                sc = cand[key][0]
                if sc < STAMP_LLM_CANDIDATE_MIN_SCORE:
                    continue
                cands.append((sc, rp, r))
            if not cands:
                continue
            cands.sort(key=lambda x: (-x[0], abs(x[1] - expected_rp.get(lp, lp))))
            cands = cands[:max(1, STAMP_CANDIDATE_TOPK)]
            allowed[lp] = {rp for _sc, rp, _r in cands}
            tasks.append({
                "left_page": lp,
                "left_name": l.sheet_name or lf.normalized_name,
                "left_kind": lf.sheet_kind,
                "left_systems": sorted(lf.system_tokens),
                "candidates": [
                    {"new_page": rp, "name": r.sheet_name or rf_by_page[rp].normalized_name,
                     "deterministic_score": round(sc, 3),
                     "kind": rf_by_page[rp].sheet_kind,
                     "systems": sorted(rf_by_page[rp].system_tokens)}
                    for sc, rp, r in cands
                ],
            })
        if tasks:
            try:
                proposals = _call_llm_match_fn(llm_match_fn, rem_left2, rem_right2, tasks) or []
            except Exception:  # fail-soft — LLM не должен валить матчинг
                proposals = []
            for item in proposals:
                try:
                    lp, rp, score, mtype = item
                    lp, rp, score = int(lp), int(rp), float(score)
                except (TypeError, ValueError):
                    continue
                # Инварианты: внутри кандидатов, page свободен, нет hard-конфликта.
                if lp in matches or rp in used_right:
                    continue
                if lp not in left_by_page or rp not in right_by_page:
                    continue
                if rp not in allowed.get(lp, set()):
                    continue
                if get_hard_conflict(lf_by_page[lp], rf_by_page[rp]):
                    continue
                matches[lp] = {"rp": rp, "score": max(0.0, min(1.0, score)),
                               "mtype": "llm_semantic",
                               "reason": "ИИ: один и тот же лист по смыслу",
                               "positive": [], "negative": [],
                               "risk": _risk(lp, rp, ["llm_semantic"]),
                               "confidence": max(0.0, min(1.0, score))}
                used_right.add(rp)
                llm_match_count += 1

    # Сборка items — left в порядке страниц, right-only по возрастанию.
    def _make_one_sided(slot_no: int, rec: SheetRec, side: str) -> dict:
        mp = multipart_one_sided.get((side, rec.page))
        if mp:
            return _one_sided_item(
                slot_no, rec, side, match_type="multipart_continuation",
                reason=mp["reason"],
                positive_evidence=[f"sheet_group_key: {mp['group_key']}"])
        return _one_sided_item(slot_no, rec, side)

    right_only_pages = sorted(r.page for r in right if r.page not in used_right)
    items: list[dict] = []
    slot = 0
    ri = 0
    for l in sorted(left, key=lambda x: x.page):
        if l.page in matches:
            m = matches[l.page]
            rp = m["rp"]
            while ri < len(right_only_pages) and right_only_pages[ri] < rp:
                slot += 1
                items.append(_make_one_sided(slot, right_by_page[right_only_pages[ri]], "right"))
                ri += 1
            slot += 1
            items.append(_matched_item(
                slot, l, right_by_page[rp], m["score"], m["mtype"],
                reason=m.get("reason", ""), positive_evidence=m.get("positive"),
                negative_evidence=m.get("negative"), risk_flags=m.get("risk"),
                confidence=m.get("confidence"), diag=m.get("diag")))
        else:
            slot += 1
            items.append(_make_one_sided(slot, l, "left"))
    while ri < len(right_only_pages):
        slot += 1
        items.append(_make_one_sided(slot, right_by_page[right_only_pages[ri]], "right"))
        ri += 1

    # Позиционно выровнять нераспознанный ВЕДУЩИЙ прогон (титульные/вводные листы
    # без рамок), чтобы они не смещали всю карту пустыми вставками (не уверенный
    # матч; межанкорные/trailing прогоны намеренно не трогаем).
    items = _apply_positional_alignment(items)

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
        "derived_match_count": derived_match_count,
        "multipart_match_count": multipart_count,
        "multipart_continuation_count": sum(
            1 for it in items if it["match_type"] == "multipart_continuation"),
        "positional_alignment_count": sum(
            1 for it in items if it["match_type"] == POSITIONAL_ALIGN_TYPE),
        "left_only_count": sum(1 for it in items if it["match_type"] == "left_only"),
        "right_only_count": sum(1 for it in items if it["match_type"] == "right_only"),
        "left_page_count": len(left),
        "right_page_count": len(right),
        "rejected_count": len(rejected),
        "rejected": rejected,
    }


__all__ = [
    "SheetRec",
    "SheetFeatures",
    "normalize_sheet_name",
    "canonicalize_sheet_name",
    "extract_multipart",
    "build_sheet_index",
    "extract_sheet_features",
    "get_hard_conflict",
    "match_sheet_indexes",
    "MAIN_SYSTEM_FAMILIES",
    "SAFE_ALIASES",
    "STAMP_MATCH_MIN_SCORE",
    "STAMP_FALLBACK_MIN_SCORE",
    "STAMP_MATCH_MIN_MARGIN",
    "POSITIONAL_ALIGN_TYPE",
    "POSITIONAL_ALIGN_RISK",
    "POSITIONAL_ALIGN_REASON",
]
