"""Детерминированное сопоставление листов по строке идентификации из штампа.

Открытие, ради которого написан этот модуль: строка вида
«Корпуса 1, 2. План 3 этажа» присутствует в ТЕКСТОВОМ СЛОЕ обоих документов
пары, в прямо сопоставимом виде. Продовый sheet_matcher её не использует —
он кладёт sheet_number из штампа в запись и больше к нему не обращается, а
сигнал `title` заполняет названием ТОМА («Часть 1. Архитектурные решения.
Планы»), одинаковым у 20 из 21 левой страницы. Отсюда score 0.27-0.71 и
жадное 1:1-назначение по возрастанию номера левой страницы.

Здесь — ноль моделей, ноль токенов: регулярное выражение по правому нижнему
блоку страницы плюс точное сравнение ключа (корпуса, тип листа, этаж).

Только чтение PDF.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

import fitz  # PyMuPDF

# «Корпуса 1, 2. План 3 этажа. М 1_200.»  /  «Корпус 4. План кровли. 12.25»
_STAMP = re.compile(
    r"(?P<buildings>корпус\w*\s*[\d][\d,.\s]*)\.\s*"
    r"план\s+(?P<what>кровли|[\d\-]+\s*этаж\w*|первого\s+подземного\s+этажа|типового\s+этажа)",
    re.I,
)
# запасной вариант: лист без указания корпуса («План первого подземного этажа»)
_STAMP_NO_BUILDING = re.compile(
    r"план\s+(?P<what>кровли|[\d\-]+\s*этаж\w*|первого\s+подземного\s+этажа|типового\s+этажа)",
    re.I,
)


@dataclass(frozen=True)
class SheetKey:
    buildings: tuple[str, ...]
    kind: str            # ROOF | FLOOR | UNDERGROUND | TYPICAL
    floors: tuple[str, ...]
    raw: str

    def matches(self, other: "SheetKey") -> bool:
        return (self.kind == other.kind
                and self.floors == other.floors
                and self.buildings == other.buildings)


def _parse_buildings(s: str | None) -> tuple[str, ...]:
    return tuple(sorted(re.findall(r"\d+(?:\.\d+)?", s or "")))


def _parse_what(s: str) -> tuple[str, tuple[str, ...]]:
    s = s.lower()
    if "кровли" in s:
        return "ROOF", ()
    if "подземного" in s:
        return "UNDERGROUND", ()
    if "типового" in s:
        return "TYPICAL", ()
    nums = re.findall(r"\d+", s)
    if len(nums) == 2:            # «План 3-15 этажей» — диапазон
        return "FLOOR", tuple(str(n) for n in range(int(nums[0]), int(nums[1]) + 1))
    return "FLOOR", tuple(nums)


def parse_stamp(text: str) -> SheetKey | None:
    flat = re.sub(r"\s+", " ", text)
    m = _STAMP.search(flat)
    if m:
        kind, floors = _parse_what(m.group("what"))
        return SheetKey(_parse_buildings(m.group("buildings")), kind, floors, m.group(0).strip())
    m = _STAMP_NO_BUILDING.search(flat)
    if m:
        kind, floors = _parse_what(m.group("what"))
        return SheetKey((), kind, floors, m.group(0).strip())
    return None


def sheet_keys(pdf_path: str, pages: list[int] | None = None) -> dict[int, SheetKey]:
    """Ключ листа для каждой страницы. Ищем в блоках, а не в сыром тексте:
    блок сохраняет соседство строк штампа, разорванных переносами."""
    out: dict[int, SheetKey] = {}
    doc = fitz.open(pdf_path)
    try:
        todo = pages or range(1, len(doc) + 1)
        for pg in todo:
            if pg < 1 or pg > len(doc):
                continue
            best: SheetKey | None = None
            for block in doc[pg - 1].get_text("blocks"):
                key = parse_stamp(block[4].replace("\n", " "))
                if key and (best is None or len(key.raw) > len(best.raw)):
                    best = key
            if best:
                out[pg] = best
    finally:
        doc.close()
    return out


def match(left: dict[int, SheetKey], right: dict[int, SheetKey]) -> dict:
    """Строгое сопоставление: пара только при полном совпадении ключа.

    Неоднозначность (несколько правых страниц с одним ключом) не разрешается
    молча — она попадает в ambiguous и становится вопросом, а не догадкой.
    """
    pairs, ambiguous = [], []
    used_right: set[int] = set()
    for lp in sorted(left):
        cands = [rp for rp in sorted(right) if left[lp].matches(right[rp]) and rp not in used_right]
        if len(cands) == 1:
            used_right.add(cands[0])
            pairs.append({"left_page": lp, "right_page": cands[0],
                          "key": left[lp].raw, "basis": "STAMP_EXACT"})
        elif len(cands) > 1:
            ambiguous.append({"left_page": lp, "candidates": cands, "key": left[lp].raw})
    return {
        "pairs": pairs,
        "ambiguous": ambiguous,
        "unmatched_left": [p for p in sorted(left) if p not in {x["left_page"] for x in pairs}
                           and p not in {x["left_page"] for x in ambiguous}],
        "unmatched_right": [p for p in sorted(right) if p not in used_right],
        "no_stamp_left": [], "no_stamp_right": [],
    }
