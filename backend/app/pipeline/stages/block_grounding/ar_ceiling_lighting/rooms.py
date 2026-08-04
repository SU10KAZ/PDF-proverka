"""Марки помещений, области помещений (сеточный flood-fill) и привязка
объектов к помещениям.

Марка ``6.<квартира>.<помещение>`` — семантика кода (tier 5): квартира
берётся из средней части. Область помещения восстанавливается заливкой
от центра марки с барьерами из слоёв стен/дверей/остекления/колонн и
условных границ A-AREA-OTLN. Fail-closed: протечка к чужой марке или
разлив за предел площади не даёт «примерно ту» область — состояние
merged/leaked, а привязки объектов в таком регионе не создаются.
"""
from __future__ import annotations

import re

from .legend import MARK_RE
from .spatial import OccupancyGrid

# Семейства CAD-слоёв, образующих физические и условные границы помещений.
# Это конвенция слоёв AIA/Revit-экспорта (WALL/DOOR/GLAZ/COLS/STRS) плюс
# русские категории Revit; не привязка к конкретному файлу.
BARRIER_LAYER_RE = re.compile(
    r"(WALL|GLAZ|DOOR|COLS|STRS|CWMG|CURT)"
    r"|Opening\s*Swing|Frame_Mullion|Panel|Ограждени|Балясин|Поручн|Стены",
    re.I,
)
AREA_BOUNDARY_RE = re.compile(r"AREA.*OTLN", re.I)
ROOM_MARK_LAYER_RE = re.compile(r"AREA.*IDEN", re.I)

GRID_CELL = 2.5
LEAK_AREA_SHARE = 0.30  # регион больше 30% площади плана = разлив


def find_room_marks(inv: dict, scope_of) -> tuple[list[dict], list[dict]]:
    """Марки помещений в block_scope. Возврат: (марки, отклонённые)."""
    marks = []
    rejected = []
    for t in inv["texts"]:
        text = t["text"].strip()
        m = MARK_RE.match(text)
        if not m:
            continue
        rec = {
            "mark": text,
            "building_part": m.group(1),
            "apartment": m.group(2),
            "room_suffix": int(m.group(3)),
            "bbox": t["bbox"],
            "center": t["center"],
            "layer": t["layer"],
            "tid": t["tid"],
        }
        scope = scope_of(t["bbox"])
        if scope != "block":
            rejected.append({**rec, "why": f"scope={scope}"})
            continue
        if not ROOM_MARK_LAYER_RE.search(t["layer"] or ""):
            # марка вне слоя марок помещений (например, образец в легенде,
            # заехавший в кроп) — фиксируем, но не считаем помещением
            rejected.append({**rec, "why": f"layer={t['layer']!r} не слой марок"})
            continue
        marks.append(rec)
    marks.sort(key=lambda r: (r["apartment"], r["room_suffix"]))
    return marks, rejected


def build_room_regions(inv: dict, cp, marks: list[dict]) -> dict:
    """Flood-fill областей помещений. Возврат:
    {"grid": OccupancyGrid, "regions": {mark: {...}}, "cell_owner": {(i,j): mark}}.
    """
    block = cp.block_rect
    grid = OccupancyGrid(block, cell=GRID_CELL)
    barrier_segments = 0
    for s in inv["segments"]:
        layer = s["layer"] or ""
        if BARRIER_LAYER_RE.search(layer) or AREA_BOUNDARY_RE.search(layer):
            grid.mark_segment(s["p1"], s["p2"])
            barrier_segments += 1

    plan_cells = grid.nx * grid.ny
    regions: dict[str, dict] = {}
    floods: dict[str, set] = {}
    for mark in marks:
        region, overflow = grid.flood(*mark["center"], cap=int(plan_cells * LEAK_AREA_SHARE))
        state = "resolved"
        if region is None:
            state = "seed_blocked"
        elif overflow or len(region) > plan_cells * LEAK_AREA_SHARE:
            state = "leaked"
        floods[mark["mark"]] = region or set()
        regions[mark["mark"]] = {
            "mark": mark["mark"],
            "state": state,
            "cells": len(region or ()),
            "bbox": grid.region_bbox(region) if region else None,
        }

    # протечки: регион достал до центра чужой марки → merged для обеих
    seed_cells = {m["mark"]: grid.cell_of(*m["center"]) for m in marks}
    for mark in marks:
        region = floods[mark["mark"]]
        if not region or regions[mark["mark"]]["state"] != "resolved":
            continue
        reached = [other for other, seed in seed_cells.items()
                   if other != mark["mark"] and seed in region]
        if reached:
            regions[mark["mark"]]["state"] = "merged"
            regions[mark["mark"]]["merged_with"] = sorted(reached)
            for other in reached:
                if regions.get(other, {}).get("state") == "resolved":
                    regions[other]["state"] = "merged"
                    regions[other].setdefault("merged_with", []).append(mark["mark"])

    cell_owner: dict[tuple[int, int], str] = {}
    cell_quality: dict[tuple[int, int], str] = {}
    ambiguous_cells: set[tuple[int, int]] = set()
    for mark in marks:
        if regions[mark["mark"]]["state"] != "resolved":
            continue
        for cell in floods[mark["mark"]]:
            if cell in cell_owner:
                ambiguous_cells.add(cell)
            else:
                cell_owner[cell] = mark["mark"]
                cell_quality[cell] = "strict"
    for cell in ambiguous_cells:
        cell_owner.pop(cell, None)
        cell_quality.pop(cell, None)

    _watershed_merged(regions, floods, seed_cells, cell_owner, cell_quality)

    return {"grid": grid, "regions": regions, "cell_owner": cell_owner,
            "cell_quality": cell_quality, "floods": floods,
            "barrier_segments": barrier_segments}


# ширина зоны неопределённости watershed: клеток по геодезике
WATERSHED_CONTESTED_CELLS = 8
WATERSHED_STRONG_CELLS = 16


def _watershed_merged(regions, floods, seed_cells, cell_owner, cell_quality) -> None:
    """Разрез слитых (открытая планировка) регионов геодезическим watershed.

    Слитый регион ПОЛНОСТЬЮ замкнут общим контуром стен (иначе он был бы
    leaked) — спорная только внутренняя граница между марками. Клетки в
    полосе неопределённости (разность геодезических расстояний до двух
    ближайших марок меньше порога) владельца НЕ получают: объект там
    остаётся неразрешённым, это не «ближайшая марка молча».
    """
    import collections as _c

    done: set[str] = set()
    for mark_name, region in regions.items():
        if region["state"] != "merged" or mark_name in done:
            continue
        group = {mark_name, *region.get("merged_with", ())}
        # транзитивное замыкание группы слияния
        changed = True
        while changed:
            changed = False
            for other, reg2 in regions.items():
                if other in group or reg2.get("state") != "merged":
                    continue
                if group & {other, *reg2.get("merged_with", ())} - {other} or \
                        any(m in group for m in reg2.get("merged_with", ())):
                    group.add(other)
                    changed = True
        done.update(group)
        area = set()
        for m in group:
            area |= floods.get(m, set())
        seeds = {m: seed_cells[m] for m in sorted(group) if seed_cells.get(m) in area}
        if len(seeds) < 2:
            continue
        # честные геодезические карты расстояний от КАЖДОЙ марки: полоса
        # неопределённости — это весь пояс, где d2 − d1 мал, а не только шов
        dist_maps: dict[str, dict[tuple[int, int], int]] = {}
        for m in sorted(seeds):
            dist = {seeds[m]: 0}
            queue = _c.deque([(seeds[m], 0)])
            while queue:
                cell, d = queue.popleft()
                for di, dj in ((0, 1), (0, -1), (1, 0), (-1, 0)):
                    nxt = (cell[0] + di, cell[1] + dj)
                    if nxt in area and nxt not in dist:
                        dist[nxt] = d + 1
                        queue.append((nxt, d + 1))
            dist_maps[m] = dist
        counts = _c.Counter()
        for cell in area:
            pairs = sorted((dist_maps[m][cell], m) for m in dist_maps if cell in dist_maps[m])
            if not pairs:
                continue
            d1, owner = pairs[0]
            margin = (pairs[1][0] - d1) if len(pairs) > 1 else 10 ** 6
            if margin < WATERSHED_CONTESTED_CELLS:
                continue  # полоса неопределённости — без владельца
            cell_owner[cell] = owner
            cell_quality[cell] = "watershed_strong" if margin >= WATERSHED_STRONG_CELLS else "watershed_weak"
            counts[owner] += 1
        for m in group:
            regions[m]["state"] = "open_plan_watershed"
            regions[m]["cells"] = counts.get(m, 0)
            regions[m]["watershed_group"] = sorted(group)


def room_of_point(room_data: dict, x: float, y: float) -> tuple[str | None, str | None]:
    """(марка помещения, качество привязки) для точки.

    Качество: 'strict' — однозначная заливка; 'watershed_strong'/'weak' —
    открытая планировка, разрез по доминированию; None — не привязано.
    """
    cell = room_data["grid"].cell_of(x, y)
    mark = room_data["cell_owner"].get(cell)
    return mark, (room_data["cell_quality"].get(cell) if mark else None)


def nearest_mark(marks: list[dict], x: float, y: float) -> tuple[str | None, float]:
    """Ближайшая марка (только для диагностики tier 2, не для основной привязки)."""
    best, dist = None, float("inf")
    for m in marks:
        d = (m["center"][0] - x) ** 2 + (m["center"][1] - y) ** 2
        if d < dist:
            best, dist = m["mark"], d
    return best, dist ** 0.5
