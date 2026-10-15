"""Контекст записей из исходников, а не из желаемого ответа (план §4.3, §5.2).

* строка и окрестность — слова профиля страницы (текстовый слой без аннотаций и
  SHX-надписи) в полосе строки и в окне вокруг рамки;
* позиция СО — разбор спецификации версии (``spec_<версия>.json`` профиля):
  наименование, марка, единица, количество, поставщик, примечание;
* межлистовой контекст — марки из изменённых записей и их окрестности, найденные
  на других страницах **этих двух PDF** (другие версии и проекты не ищутся).
"""
from __future__ import annotations

import re
from typing import Any

from backend.app.services.version_comparison import paths, profile, spec

from . import geometry as geo
from . import ids, settings

ROW_HALF_WIDTH_PT = 420.0
NEAR_DX_PT, NEAR_DY_PT = 120.0, 30.0
MAX_TEXT = 700
SPEC_FIELDS = (("name", "наименование"), ("mark", "тип, марка"), ("code", "код"), ("supplier", "поставщик"),
               ("unit", "единица"), ("qty", "количество"), ("note", "примечание"), ("pos", "позиция"))
_HOMO = str.maketrans("ABCEHKMOPTXaceopxy", "АВСЕНКМОРТХасеорху")
MARK = re.compile(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё0-9]*(?:[-./][A-Za-zА-Яа-яЁё0-9]+)+")
MARK_MIN_LEN = 4
MARK_MAX_COMMON = 25


class Pages:
    """Слова страниц версий прогона (кэш на время сборки одного пакета)."""

    def __init__(self, object_id: str, sha_a: str, sha_b: str):
        self.object_id = object_id
        self.sha = {"A": sha_a, "B": sha_b}
        self._words: dict[tuple[str, int], list[list[Any]]] = {}
        self._profiles: dict[str, dict[str, Any] | None] = {}
        self._spec: dict[str, dict[str, Any] | None] = {}
        self._index: dict[str, dict[str, list[tuple[int, list[Any]]]]] = {}

    def profile(self, side: str) -> dict[str, Any] | None:
        if side not in self._profiles:
            self._profiles[side] = profile.load(self.object_id, self.sha[side])
        return self._profiles[side]

    def page_rec(self, side: str, page: int | None) -> dict[str, Any] | None:
        prof = self.profile(side)
        if not prof or not page or page > len(prof["pages"]):
            return None
        return prof["pages"][page - 1]

    def words(self, side: str, page: int) -> list[list[Any]]:
        key = (side, page)
        if key not in self._words:
            try:
                self._words[key] = profile.page_words(self.object_id, self.sha[side], page)
            except OSError:
                self._words[key] = []
        return self._words[key]

    def spec(self, side: str) -> dict[str, Any] | None:
        if side not in self._spec:
            self._spec[side] = spec.load(str(paths.profile_dir(self.object_id, self.sha[side])))
        return self._spec[side]

    def index(self, side: str) -> dict[str, list[tuple[int, list[Any]]]]:
        """Нормализованное слово → [(страница, слово)] по всем страницам версии."""
        if side not in self._index:
            idx: dict[str, list[tuple[int, list[Any]]]] = {}
            for page in range(1, self.page_count(side) + 1):
                for w in self.words(side, page):
                    idx.setdefault(norm_mark(str(w[4])), []).append((page, w))
            self._index[side] = idx
        return self._index[side]

    def page_count(self, side: str) -> int:
        prof = self.profile(side)
        return len(prof["pages"]) if prof else 0


def _vertical(r: list[float], text: str) -> bool:
    return len(text) > 1 and (r[3] - r[1]) > 1.5 * (r[2] - r[0])


def _lines(words: list[list[Any]], vertical: bool) -> str:
    """Слова в порядке чтения, строками."""
    if not words:
        return ""
    if vertical:
        key = lambda w: (round((w[0] + w[2]) / 2 / 6.0), -(w[1] + w[3]) / 2)  # noqa: E731
    else:
        key = lambda w: (round((w[1] + w[3]) / 2 / 4.0), w[0])  # noqa: E731
    lines: list[list[str]] = []
    last = None
    for w in sorted(words, key=key):
        k = key(w)[0]
        if k != last:
            lines.append([])
            last = k
        lines[-1].append(str(w[4]))
    text = "\n".join(" ".join(line) for line in lines)
    return text if len(text) <= MAX_TEXT else text[:MAX_TEXT] + " …"


def row_and_near(words: list[list[Any]], box: list[float], text: str = "") -> dict[str, Any]:
    """Строка (полоса по высоте записи) и окрестность (окно вокруг рамки)."""
    vertical = _vertical(box, text)
    if vertical:
        cx0, cx1 = box[0] - 0.3 * (box[2] - box[0]), box[2] + 0.3 * (box[2] - box[0])
        row = [w for w in words if cx0 <= (w[0] + w[2]) / 2 <= cx1
               and box[1] - ROW_HALF_WIDTH_PT <= (w[1] + w[3]) / 2 <= box[3] + ROW_HALF_WIDTH_PT]
        near_box = geo.expand(box, NEAR_DY_PT, NEAR_DX_PT)
    else:
        cy0, cy1 = box[1] - 0.3 * (box[3] - box[1]), box[3] + 0.3 * (box[3] - box[1])
        row = [w for w in words if cy0 <= (w[1] + w[3]) / 2 <= cy1
               and box[0] - ROW_HALF_WIDTH_PT <= (w[0] + w[2]) / 2 <= box[2] + ROW_HALF_WIDTH_PT]
        near_box = geo.expand(box, NEAR_DX_PT, NEAR_DY_PT)
    near = [w for w in words if geo.intersects(near_box, w[:4])]
    return {"row": _lines(row, vertical), "near": _lines(near, vertical), "vertical": vertical,
            "near_box": geo.rounded(near_box)}


def spec_item_at(pages: Pages, side: str, page: int | None, box: list[float] | None) -> dict[str, Any] | None:
    """Позиция СО, строка которой на этой странице пересекает рамку."""
    if not page or not box:
        return None
    data = pages.spec(side)
    if not data:
        return None
    best, best_area = None, 0.0
    for it in data.get("items") or []:
        for row in it.get("rows") or []:
            if int(row[0]) != int(page):
                continue
            r = row[1:5]
            ix = min(r[2], box[2]) - max(r[0], box[0])
            iy = min(r[3], box[3]) - max(r[1], box[1])
            if ix > 0 and iy > 0 and ix * iy > best_area:
                best, best_area = it, ix * iy
    return best


def spec_fields(it: dict[str, Any] | None) -> dict[str, str]:
    if not it:
        return {}
    out = {}
    for key, _label in SPEC_FIELDS:
        value = it.get(key) or (it.get("parent_" + key) if key in ("mark", "code", "supplier") else "")
        if value:
            out[key] = " ".join(str(value).split())
    if it.get("section"):
        out["section"] = " ".join(str(it["section"]).split())
    return out


def norm_mark(text: str) -> str:
    return text.strip(" -.,;:()").translate(_HOMO).replace(",", ".").casefold()


def marks(text: str) -> list[str]:
    out = []
    for m in MARK.finditer(text or ""):
        token = m.group(0).strip(" -.,;:")
        if len(token) >= MARK_MIN_LEN and re.search(r"\d", token) and re.search(r"[A-Za-zА-Яа-яЁё]{2}", token):
            out.append(token)
    return out


def xrefs(pages: Pages, mark_list: list[str], skip: dict[str, int | None], limit: int) -> list[dict[str, Any]]:
    """Совпадения марок на других страницах обеих версий и в позициях СО."""
    hits: list[dict[str, Any]] = []
    seen: set[tuple] = set()
    for mark in mark_list:
        key = norm_mark(mark)
        found: list[dict[str, Any]] = []
        for side in ("B", "A"):
            sd = pages.spec(side)
            for it in (sd or {}).get("items") or []:
                blob = " ".join(str(it.get(k) or "") for k in ("mark", "note", "name", "pos"))
                if any(norm_mark(t) == key for t in re.split(r"[\s,;]+", blob)):
                    row = (it.get("rows") or [[it.get("page")] + [0, 0, 0, 0]])[0]
                    if int(row[0]) == skip.get(side):
                        continue
                    found.append({"mark": mark, "side": side, "page": int(row[0]), "bbox": geo.rounded(row[1:5]),
                                  "method": f"поиск марки «{mark}» в позициях СО", "kind": "spec_position",
                                  "fields": spec_fields(it)})
            for page, w in pages.index(side).get(key, []):
                if page == skip.get(side):
                    continue
                near = row_and_near(pages.words(side, page), w[:4], str(w[4]))
                found.append({"mark": mark, "side": side, "page": page, "bbox": geo.rounded(w[:4]),
                              "method": f"поиск марки «{mark}» в тексте страниц", "kind": "page_text",
                              "text": near["row"]})
        if not found or len(found) > MARK_MAX_COMMON:
            continue   # нет совпадений или марка слишком частая — шум
        found.sort(key=lambda h: (h["kind"] != "spec_position", h["side"] != "B", h["page"]))
        for h in found[:limit]:
            sig = (key, h["side"], h["page"], h["kind"])     # одна страница — одно совпадение на марку
            if sig in seen:
                continue
            seen.add(sig)
            hits.append(h)
    for n, h in enumerate(hits, 1):
        h["id"] = ids.local("xref", str(n))
    return hits


def page_label(pages: Pages, side: str, page: int) -> str:
    rec = pages.page_rec(side, page) or {}
    st = rec.get("stamp") or {}
    parts = []
    if st.get("sheet_no"):
        parts.append(f"лист {st['sheet_no']}")
    if st.get("title"):
        parts.append(str(st["title"]))
    return ", ".join(parts)


def max_xref_marks() -> int:
    return 30


def xref_limit() -> int:
    return settings.xref_hits()
