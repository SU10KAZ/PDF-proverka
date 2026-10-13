"""Bounded, exhaustive native-text windows for an oversized single page pair.

Only the raw PDF text is windowed. Structured blocks, tables, crops and full-page
images remain intact in every request. Windows partition the OLD×NEW text space;
no prefix/tail is discarded and sources on disk are never modified.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
from typing import Any, Callable

VERSION = "projectchange_v3_native_text_windows/1"
MAX_PARTS = 256
KEY = "native_text_windows"


class NativeTextPortionError(ValueError):
    pass


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def page_text(page: dict[str, Any], region: dict[str, Any]) -> dict[str, Any]:
    """Return a request-only page copy with a checked text window, if selected."""
    windows = region.get(KEY)
    if windows is None:
        return page
    matches = [w for w in windows if (w["side"], w["physical_page"]) ==
               (page["side"], page["physical_page"])]
    if len(matches) != 1:
        raise NativeTextPortionError("для страницы не задано единственное окно текста")
    window = matches[0]
    raw = page.get("native_page_text") or ""
    start, end = window["start"], window["end"]
    if (not isinstance(start, int) or not isinstance(end, int)
            or not 0 <= start <= end <= len(raw)
            or window.get("total") != len(raw) or window.get("sha256") != digest(raw)):
        raise NativeTextPortionError("окно текста не соответствует исходной странице")
    return {**page, "native_page_text": raw[start:end], "native_page_text_window": dict(window)}


def split_region(region: dict[str, Any], texts: dict[tuple[str, int], str],
                 fits: Callable[[dict[str, Any]], bool]) -> list[dict[str, Any]]:
    """Bisect text rectangles until all exact final requests fit; reject infeasible overhead."""
    if any(len(region[key]) != 1 for key in ("old_pages", "new_pages")):
        raise NativeTextPortionError("текстовые окна допустимы только для пары отдельных страниц")
    keys = [(side, int(region[side.lower() + "_pages"][0])) for side in ("OLD", "NEW")]
    raw = [texts[key] for key in keys]
    bounds = [(0, len(text)) for text in raw]
    # A continued window may need further subdivision under a stricter envelope.
    if region.get(KEY):
        by_key = {(w["side"], w["physical_page"]): w for w in region[KEY]}
        if set(by_key) != set(keys) or len(region[KEY]) != 2:
            raise NativeTextPortionError("неверные окна исходной области")
        bounds = []
        for key, text in zip(keys, raw, strict=True):
            w = by_key[key]
            page_text({"side": key[0], "physical_page": key[1], "native_page_text": text}, region)
            bounds.append((w["start"], w["end"]))

    def part(rect, index):
        result = deepcopy(region)
        result["region_id"] = f"{region['region_id']}.T{index}"
        result["scope"] = f"{region.get('scope', '')} (порция текста PDF {index})"
        result[KEY] = [{"side": key[0], "physical_page": key[1], "start": span[0], "end": span[1],
                        "total": len(text), "sha256": digest(text)}
                       for key, text, span in zip(keys, raw, rect, strict=True)]
        return result

    if not fits(part([(0, 0), (0, 0)], MAX_PARTS)):
        raise NativeTextPortionError(
            f"страницы OLD {region['old_pages']} и NEW {region['new_pages'][0]}: "
            "блоки и изображения не помещаются даже без текста PDF; данные не обрезаны")
    rectangles = []

    def visit(rect):
        if len(rectangles) >= MAX_PARTS:
            raise NativeTextPortionError(f"для пары страниц требуется более {MAX_PARTS} порций текста PDF")
        if fits(part(rect, MAX_PARTS)):
            rectangles.append(rect)
            return
        candidates = [i for i, (a, b) in enumerate(rect) if b - a > 1]
        if not candidates:
            raise NativeTextPortionError("минимальная порция текста PDF не помещается")
        side = max(candidates, key=lambda i: len(raw[i][rect[i][0]:rect[i][1]].encode("utf-8")))
        a, b = rect[side]
        middle = (a + b) // 2
        # Prefer a line boundary near the midpoint; no overlap or omitted separator.
        newline = raw[side].find("\n", middle, min(b - 1, middle + 1024))
        if newline >= 0:
            middle = newline + 1
        left, right = list(rect), list(rect)
        left[side], right[side] = (a, middle), (middle, b)
        visit(left)
        visit(right)

    visit(bounds)
    validate_coverage(bounds, rectangles)
    parts = [part(rect, i) for i, rect in enumerate(rectangles, 1)]
    if not all(fits(p) for p in parts):
        raise NativeTextPortionError("окончательный запрос Miner с порцией текста PDF не помещается")
    return parts


def validate_coverage(bounds, rectangles) -> None:
    """Check rectangular coverage without enumerating characters (empty text is one atom)."""
    def nonempty(span, source):
        a, b = span
        if a == b == source[0] == source[1]:
            return (a, a + 1)
        if b <= a:
            raise NativeTextPortionError("пустое окно непустого текста")
        return (a, b)

    old, new = [nonempty(span, span) for span in bounds]
    rects = [[nonempty(span, source) for span, source in zip(rect, bounds, strict=True)] for rect in rectangles]
    if any(not (old[0] <= a < b <= old[1] and new[0] <= c < d <= new[1])
           for (a, b), (c, d) in rects):
        raise NativeTextPortionError("окна текста выходят за исходную область")
    cuts = sorted({old[0], old[1]} | {x for rect in rects for x in rect[0]})
    for start, end in zip(cuts, cuts[1:]):
        spans = sorted(rect[1] for rect in rects if rect[0][0] <= start and end <= rect[0][1])
        cursor = new[0]
        for a, b in spans:
            if a != cursor:
                raise NativeTextPortionError("пропуск или повтор сочетания фрагментов OLD и NEW")
            cursor = b
        if cursor != new[1]:
            raise NativeTextPortionError("план не покрывает весь текст PDF")


WINDOW_NOTE = (
    "В этом запросе native_page_text дан только в явно указанных окнах символов [start,end). "
    "Все сочетания окон OLD и NEW обрабатываются отдельными запросами. Структурированные блоки, "
    "таблицы и изображения переданы полностью для выбранных страниц. Не считай отсутствие текста "
    "за пределами окна удалением или добавлением: используй двусторонние доказательства; "
    "неподтверждённое сопоставление оставляй unresolved_hint."
)
