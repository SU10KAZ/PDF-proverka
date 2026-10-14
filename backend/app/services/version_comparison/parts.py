"""Разделение листа 1 → N (план §5 шаг 2, этап 2).

Два вида, оба — по штампам (обозначение документа и название листа):

* ``floors`` — типовой лист размножен по этажам: «План 2-4 этажей» →
  «План 2 этажа», «План 3 этажа», «План 4 этажа». Каждая новая страница —
  весь исходный лист со своими номерами квартир; сравнивается весь лист.
* ``split`` — лист разделён на части: «Схема ВРУ-1» → «Схема ВРУ-1. Начало»
  и «… Окончание». Часть сравнивается только в своей области исходного
  листа; по прогону считается покрытие исходного листа частями.

Текстовое вхождение для этого не годится: у поэтажных планов разные номера
квартир, а разделённую схему часто ещё и перерабатывают.
"""
from __future__ import annotations

import re
from typing import Any

_FLOORS = re.compile(r"(\d+(?:\s*(?:[-–,]|и)\s*\d+)*)\s*(?:-?(?:го|й|м|х))?\s*этаж\w*", re.I)


def _norm(t: str) -> str:
    return " ".join(str(t or "").lower().replace("ё", "е").split()).strip(" .")


def floors(title: str) -> tuple[str, frozenset[int]] | None:
    """«План 2-4 этажей. Сети» → ("план # этаж. сети", {2, 3, 4})."""
    t = _norm(title)
    m = _FLOORS.search(t)
    if not m:
        return None
    nums: set[int] = set()
    for part in re.split(r"\s*(?:,|и)\s*", m.group(1)):
        r = re.match(r"^(\d+)\s*[-–]\s*(\d+)$", part.strip())
        if r and 0 <= int(r.group(2)) - int(r.group(1)) <= 60:
            nums.update(range(int(r.group(1)), int(r.group(2)) + 1))
        elif part.strip().isdigit():
            nums.add(int(part.strip()))
    if not nums:
        return None
    base = (t[:m.start()] + "# этаж" + t[m.end():]).strip()
    return base, frozenset(nums)


def _doc(rec: dict[str, Any]) -> str:
    return re.sub(r"\s+", "", str((rec.get("stamp") or {}).get("designation") or "")).upper()


def _title(rec: dict[str, Any]) -> str:
    return _norm((rec.get("stamp") or {}).get("title"))


def find(pages_a: list[dict[str, Any]], pages_b: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Связи «часть листа» A → B: [{a_page, b_page, kind}]."""
    titles_b = {_title(r) for r in pages_b if _title(r)}
    out: list[dict[str, Any]] = []
    for ra in pages_a:
        ta = _title(ra)
        if len(ta) < 6 or ta in titles_b:
            continue      # лист продолжается под тем же названием — не разделение
        fa = floors(ta)
        if fa and len(fa[1]) >= 2:
            for rb in pages_b:
                fb = floors(_title(rb))
                if fb and fb[0] == fa[0] and fb[1] < fa[1] and _doc(rb) == _doc(ra):
                    out.append({"a_page": ra["page"], "b_page": rb["page"], "kind": "floors",
                                "a_title": (ra.get("stamp") or {}).get("title"), "b_title": (rb.get("stamp") or {}).get("title")})
        prefixed = [rb for rb in pages_b if _title(rb).startswith(ta) and len(_title(rb)) > len(ta) + 2
                    and _doc(rb) == _doc(ra)]
        if len(prefixed) >= 2:
            for rb in prefixed:
                out.append({"a_page": ra["page"], "b_page": rb["page"], "kind": "split",
                            "a_title": (ra.get("stamp") or {}).get("title"), "b_title": (rb.get("stamp") or {}).get("title")})
    return out
