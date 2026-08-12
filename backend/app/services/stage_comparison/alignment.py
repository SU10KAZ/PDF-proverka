"""Карта соответствия страниц (page_alignment) для пары PDF.

Структура файла page_alignment.json:
{
  "version": 1,
  "items": [
    {"slot": 1, "left_page": 1, "right_page": 1, "mode": "auto", "note": ""},
    ...
  ]
}

mode:
  auto    — создано автоматически из количества страниц
  uncertain — только совместный просмотр кандидатов; НЕ автоматическая пара
  manual  — изменено пользователем (включая insert/move)
  blank   — добавлен пустой лист (одна сторона null)

Главные операции:
  build_default(left_count, right_count) — начальная карта
  validate(items, left_count, right_count) — провалидировать, нормализовать
  insert_blank(items, slot, side) — добавить пустой лист
  move(items, slot, direction) — переместить вверх/вниз
"""
from __future__ import annotations

from typing import Any, Literal


ALIGNMENT_VERSION = 1


def _empty_item(slot: int) -> dict:
    return {"slot": slot, "left_page": None, "right_page": None, "mode": "blank", "note": ""}


def build_default(left_count: int, right_count: int) -> dict:
    """Базовая карта: 1↔1, 2↔2, …, остаток без пары через null."""
    left_count = max(0, int(left_count or 0))
    right_count = max(0, int(right_count or 0))
    items: list[dict] = []
    common = min(left_count, right_count)
    for i in range(1, common + 1):
        items.append({"slot": i, "left_page": i, "right_page": i, "mode": "auto", "note": ""})
    # Хвост слева (страницы, у которых нет пары справа)
    for i in range(common + 1, left_count + 1):
        items.append({"slot": len(items) + 1, "left_page": i, "right_page": None, "mode": "auto", "note": ""})
    # Хвост справа
    for i in range(common + 1, right_count + 1):
        items.append({"slot": len(items) + 1, "left_page": None, "right_page": i, "mode": "auto", "note": ""})
    # Если обеих сторон 0 — пустой items
    return {"version": ALIGNMENT_VERSION, "items": items}


def _normalize_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        n = int(value)
        if n < 1:
            return None
        return n
    except (TypeError, ValueError):
        return None


def _normalize_mode(mode: Any) -> str:
    s = str(mode or "").strip().lower()
    if s not in {"auto", "uncertain", "manual", "blank"}:
        return "manual"
    return s


def validate(items: list[dict], left_count: int, right_count: int) -> tuple[list[dict], list[str]]:
    """Провалидировать и нормализовать список items.

    Возвращает (items_normalized, errors). errors — список строк с проблемами;
    если непусто — items могут быть возвращены, но недостаточно консистентны.

    Правила:
      * каждый left_page ∈ {None, 1..left_count}
      * каждый right_page ∈ {None, 1..right_count}
      * страницы не должны повторяться внутри одной стороны
      * slot нормализуется в порядке передачи: 1..N
      * mode ∈ {auto, manual, blank}; если обе стороны null → blank
    """
    errors: list[str] = []
    seen_left: dict[int, int] = {}     # left_page → slot (original)
    seen_right: dict[int, int] = {}
    normalized: list[dict] = []

    for idx, raw in enumerate(items or []):
        if not isinstance(raw, dict):
            errors.append(f"item[{idx}] is not an object")
            continue
        lp = _normalize_int_or_none(raw.get("left_page"))
        rp = _normalize_int_or_none(raw.get("right_page"))
        if lp is not None and lp > left_count:
            errors.append(f"item[{idx}].left_page={lp} > left_count={left_count}")
            lp = None
        if rp is not None and rp > right_count:
            errors.append(f"item[{idx}].right_page={rp} > right_count={right_count}")
            rp = None
        if lp is not None:
            if lp in seen_left:
                errors.append(f"left_page={lp} used in slots {seen_left[lp]} and {idx + 1}")
            seen_left[lp] = idx + 1
        if rp is not None:
            if rp in seen_right:
                errors.append(f"right_page={rp} used in slots {seen_right[rp]} and {idx + 1}")
            seen_right[rp] = idx + 1
        mode = _normalize_mode(raw.get("mode"))
        if lp is None and rp is None:
            mode = "blank"
        note = str(raw.get("note") or "")
        normalized.append({
            "slot": idx + 1,
            "left_page": lp,
            "right_page": rp,
            "mode": mode,
            "note": note,
        })

    return normalized, errors


def insert_blank(items: list[dict], slot: int, side: Literal["left", "right"]) -> list[dict]:
    """Вставить пустой лист перед указанным slot.

    side="left" — слева пусто, справа возьмём текущий right_page строки (он
    «сдвинется» вверх). То есть это эквивалентно «слева добавили лист, в
    новой стадии его пока нет».

    Реализация: добавляем строку с одной стороной null. Правая сторона
    стартует тоже null — пользователь сам выбрит её при сохранении.
    """
    if slot < 1:
        slot = 1
    new_item: dict = {"slot": slot, "left_page": None, "right_page": None, "mode": "blank", "note": ""}
    # Сторона = заполняется null; противоположная — null до сохранения
    new_items = list(items or [])
    new_items.insert(slot - 1, new_item)
    # перенумеруем slot
    for i, it in enumerate(new_items):
        it["slot"] = i + 1
    return new_items


def _strip_trailing_blank_slots(items: list[dict]) -> list[dict]:
    """Срезать только хвостовые слоты, где обе стороны = None.

    Внутренние (left=None, right=None) сохраняются — они могут быть валидными
    пользовательскими «пустотами». Срезаем только хвост, который появился
    как побочный эффект padding'а после insert_blank_side / move_page_side.
    После среза перенумеровываем slot'ы.
    """
    new_items = list(items or [])
    while new_items and new_items[-1].get("left_page") is None and new_items[-1].get("right_page") is None:
        new_items.pop()
    for i, it in enumerate(new_items):
        it["slot"] = i + 1
    return new_items


def insert_blank_side(items: list[dict], slot: int, side: Literal["left", "right"]) -> list[dict]:
    """Вставить пустую строку только на одной стороне.

    Левая/правая «дорожки» страниц независимы. Например, при вставке пустого
    листа слева перед slot=2:

        Было:                       Стало:
        slot 1: L=1 / R=1           slot 1: L=1 / R=1
        slot 2: L=2 / R=2           slot 2: L=∅ / R=2
        slot 3: L=3 / R=3           slot 3: L=2 / R=3
                                    slot 4: L=3 / R=∅

    Левая дорожка сдвигается вниз, правая остаётся на месте. Если после сдвига
    появляется хвост из чистых null-null строк (одна сторона уже исчерпана,
    другая — нет), они сохраняются — это и есть «удалённый лист с другой стороны».
    """
    left_track = [it.get("left_page") for it in (items or [])]
    right_track = [it.get("right_page") for it in (items or [])]
    notes = [str(it.get("note") or "") for it in (items or [])]

    pos = max(0, min(int(slot) - 1, max(len(left_track), len(right_track))))
    if side == "left":
        left_track.insert(pos, None)
    elif side == "right":
        right_track.insert(pos, None)
    else:
        return list(items or [])

    n = max(len(left_track), len(right_track))
    left_track += [None] * (n - len(left_track))
    right_track += [None] * (n - len(right_track))
    notes += [""] * (n - len(notes))

    new_items: list[dict] = []
    for i in range(n):
        lp = left_track[i]
        rp = right_track[i]
        mode = "manual" if (lp is not None or rp is not None) else "blank"
        new_items.append({
            "slot": i + 1,
            "left_page": lp,
            "right_page": rp,
            "mode": mode,
            "note": notes[i],
        })
    return _strip_trailing_blank_slots(new_items)


def delete_page_side(
    items: list[dict],
    slot: int,
    side: Literal["left", "right"],
) -> list[dict]:
    """Удалить страницу одной стороны в указанном slot'е.

    Левая/правая «дорожки» страниц независимы — другая сторона не трогается.
    На выбранной стороне страница в slot'е убирается, а все более поздние
    страницы этой стороны поднимаются на одну позицию вверх. Пример:

        Было (delete slot=2 left):     Стало:
        slot 1: L=1 / R=1               slot 1: L=1 / R=1
        slot 2: L=2 / R=2               slot 2: L=3 / R=2
        slot 3: L=3 / R=3               slot 3: L=∅ / R=3

    Если страница на этой стороне уже null — операция бессмысленна, ValueError.
    """
    if side not in ("left", "right"):
        raise ValueError("side_must_be_left_or_right")
    idx = int(slot) - 1
    src = list(items or [])
    if idx < 0 or idx >= len(src):
        raise ValueError("invalid_slot")
    key = "left_page" if side == "left" else "right_page"
    if src[idx].get(key) is None:
        raise ValueError("nothing_to_delete_on_side")

    track = [it.get(key) for it in src]
    track.pop(idx)
    track.append(None)

    other_key = "right_page" if side == "left" else "left_page"
    other_track = [it.get(other_key) for it in src]
    notes = [str(it.get("note") or "") for it in src]

    new_items: list[dict] = []
    for i in range(len(src)):
        if side == "left":
            lp, rp = track[i], other_track[i]
        else:
            lp, rp = other_track[i], track[i]
        mode = "manual" if (lp is not None or rp is not None) else "blank"
        new_items.append({
            "slot": i + 1,
            "left_page": lp,
            "right_page": rp,
            "mode": mode,
            "note": notes[i],
        })
    return _strip_trailing_blank_slots(new_items)


def move_page_side(
    items: list[dict],
    slot: int,
    side: Literal["left", "right"],
    direction: Literal["up", "down"],
) -> list[dict]:
    """Поменять местами страницу указанной стороны между соседними slot'ами.

    В отличие от :func:`move`, другая сторона не трогается. Поэтому пара
    left/right может поменяться, появятся новые «расходящиеся» строки. Если
    страница на выбранной стороне = None, операция запрещена.
    """
    if side not in ("left", "right"):
        raise ValueError("side_must_be_left_or_right")
    if direction not in ("up", "down"):
        raise ValueError("direction_must_be_up_or_down")
    idx = int(slot) - 1
    src = list(items or [])
    if idx < 0 or idx >= len(src):
        raise ValueError("invalid_slot")
    other_idx = idx - 1 if direction == "up" else idx + 1
    if other_idx < 0 or other_idx >= len(src):
        raise ValueError("cannot_move_beyond_bounds")

    key = "left_page" if side == "left" else "right_page"
    if src[idx].get(key) is None:
        raise ValueError("nothing_to_move_on_side")

    new_items = [dict(it) for it in src]
    new_items[idx][key], new_items[other_idx][key] = (
        new_items[other_idx][key],
        new_items[idx][key],
    )
    for i, it in enumerate(new_items):
        it["slot"] = i + 1
        if it.get("left_page") is None and it.get("right_page") is None:
            it["mode"] = "blank"
        else:
            it["mode"] = "manual"
    return _strip_trailing_blank_slots(new_items)


def move(items: list[dict], slot: int, direction: Literal["up", "down"]) -> list[dict]:
    new_items = list(items or [])
    idx = slot - 1
    if idx < 0 or idx >= len(new_items):
        return new_items
    if direction == "up":
        if idx == 0:
            return new_items
        new_items[idx - 1], new_items[idx] = new_items[idx], new_items[idx - 1]
    elif direction == "down":
        if idx >= len(new_items) - 1:
            return new_items
        new_items[idx + 1], new_items[idx] = new_items[idx], new_items[idx + 1]
    for i, it in enumerate(new_items):
        it["slot"] = i + 1
        # любое явное движение помечает строку как manual (если не blank)
        if it.get("left_page") is None and it.get("right_page") is None:
            it["mode"] = "blank"
        else:
            it["mode"] = "manual"
    return new_items


def build_page_to_slot_maps(items: list[dict]) -> tuple[dict[int, int], dict[int, int]]:
    """Вернуть (left_page→slot, right_page→slot)."""
    left_map: dict[int, int] = {}
    right_map: dict[int, int] = {}
    for it in items or []:
        slot = int(it.get("slot") or 0)
        lp = it.get("left_page")
        rp = it.get("right_page")
        if lp is not None and slot:
            left_map[int(lp)] = slot
        if rp is not None and slot:
            right_map[int(rp)] = slot
    return left_map, right_map


def left_to_right_page(items: list[dict], left_page: int) -> int | None:
    """Найти right_page, соответствующий left_page по карте."""
    for it in items or []:
        if it.get("left_page") == left_page:
            return it.get("right_page")
    return None


def right_to_left_page(items: list[dict], right_page: int) -> int | None:
    for it in items or []:
        if it.get("right_page") == right_page:
            return it.get("left_page")
    return None


def slot_for_left_page(items: list[dict], left_page: int) -> int | None:
    for it in items or []:
        if it.get("left_page") == left_page:
            return int(it.get("slot") or 0) or None
    return None


def slot_for_right_page(items: list[dict], right_page: int) -> int | None:
    for it in items or []:
        if it.get("right_page") == right_page:
            return int(it.get("slot") or 0) or None
    return None


def compute_page_stats(items: list[dict]) -> dict:
    """Сколько листов сопоставлено / добавлено / удалено / переставлено."""
    matched = 0
    new_right = 0
    removed_left = 0
    reordered = 0
    for it in items or []:
        lp = it.get("left_page")
        rp = it.get("right_page")
        slot = int(it.get("slot") or 0)
        if lp is not None and rp is not None:
            matched += 1
            if lp != rp:
                reordered += 1
        elif lp is None and rp is not None:
            new_right += 1
        elif lp is not None and rp is None:
            removed_left += 1
    return {
        "matched_pages": matched,
        "new_right_pages": new_right,
        "removed_left_pages": removed_left,
        "reordered_pages": reordered,
    }


__all__ = [
    "ALIGNMENT_VERSION",
    "build_default",
    "validate",
    "insert_blank",
    "insert_blank_side",
    "move",
    "move_page_side",
    "build_page_to_slot_maps",
    "left_to_right_page",
    "right_to_left_page",
    "slot_for_left_page",
    "slot_for_right_page",
    "compute_page_stats",
]
