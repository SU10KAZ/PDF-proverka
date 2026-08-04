"""Размерные привязки устройств и серые линии центрирования.

Размер признаётся только полной конструкцией: размерная линия с засечками
45° на обоих концах + числовое значение рядом + подтверждённые концы
(ось устройства / стена). Голое число «200» без конструкции размером
не становится — остаётся в semantic_ledger.

Серые диагонали — линии размещения/центрирования светильников, НЕ
электрические связи: из них строятся только флаги centered_by_guides.
"""
from __future__ import annotations

import math

from .spatial import SpatialIndex, build_chains, line_intersection, seg_angle_deg

TICK_LEN = (0.5, 3.2)      # длина засечки, pt
TICK_ANGLE = (22.0, 68.0)  # наклон засечки к размерной линии, град
DIM_LINE_LEN = (2.0, 260.0)
VALUE_SEARCH = 10.0        # радиус поиска числа у размерной линии, pt
END_ATTACH_TOL = 2.6       # допуск совпадения оси устройства с концом, pt
WALL_ATTACH_TOL = 3.0

from .rooms import BARRIER_LAYER_RE  # noqa: E402  (общее определение границ)


def _seg_len(s):
    return math.hypot(s["p2"][0] - s["p1"][0], s["p2"][1] - s["p1"][1])


def _seg_bbox(s):
    return (min(s["p1"][0], s["p2"][0]), min(s["p1"][1], s["p2"][1]),
            max(s["p1"][0], s["p2"][0]), max(s["p1"][1], s["p2"][1]))


def detect_dimensions(inv: dict, scope_of, number_labels: list[dict],
                      devices: list[dict]) -> tuple[list[dict], set[str]]:
    """Полные размерные конструкции. Возврат: (dimensions, consumed_label_ids)."""
    segs = [s for s in inv["segments"] if s["kind"] == "l" and scope_of(_seg_bbox(s)) == "block"]

    ticks = []
    candidates = []
    walls = []
    for s in segs:
        length = _seg_len(s)
        ang = seg_angle_deg(s["p1"], s["p2"])
        if s["color_family"] == "black" and TICK_LEN[0] <= length <= TICK_LEN[1] \
                and (TICK_ANGLE[0] <= ang <= TICK_ANGLE[1] or TICK_ANGLE[0] <= 180 - ang <= TICK_ANGLE[1]):
            ticks.append(s)
        elif s["color_family"] == "black" and DIM_LINE_LEN[0] <= length <= DIM_LINE_LEN[1] \
                and (ang < 6 or ang > 174 or 84 < ang < 96):
            candidates.append(s)
        if BARRIER_LAYER_RE.search(s["layer"] or ""):
            walls.append(s)

    tick_index = SpatialIndex(cell=6.0)
    for i, t in enumerate(ticks):
        tick_index.insert(i, _seg_bbox(t))
    wall_index = SpatialIndex(cell=10.0)
    for i, w in enumerate(walls):
        wall_index.insert(i, _seg_bbox(w))

    def tick_at(point) -> bool:
        bb = (point[0] - 1.6, point[1] - 1.6, point[0] + 1.6, point[1] + 1.6)
        for i in tick_index.query(bb):
            t = ticks[i]
            mid = ((t["p1"][0] + t["p2"][0]) / 2, (t["p1"][1] + t["p2"][1]) / 2)
            if math.hypot(mid[0] - point[0], mid[1] - point[1]) <= 1.6:
                return True
        return False

    def wall_near(point) -> bool:
        bb = (point[0] - WALL_ATTACH_TOL, point[1] - WALL_ATTACH_TOL,
              point[0] + WALL_ATTACH_TOL, point[1] + WALL_ATTACH_TOL)
        for i in wall_index.query(bb):
            w = walls[i]
            if _point_to_segment(point, w["p1"], w["p2"]) <= WALL_ATTACH_TOL:
                return True
        return False

    # выносные линии: чёрные сегменты, которыми размер дотягивается до стены
    ext_candidates = [s for s in segs
                      if s["color_family"] == "black" and 2.0 <= _seg_len(s) <= 60.0]
    ext_index = SpatialIndex(cell=8.0)
    for i, s in enumerate(ext_candidates):
        ext_index.insert(i, _seg_bbox(s))

    def wall_via_extension(point, horizontal) -> bool:
        """Конец размерной линии → выносная линия ⊥ → её дальний конец у стены."""
        bb = (point[0] - 1.8, point[1] - 1.8, point[0] + 1.8, point[1] + 1.8)
        for i in ext_index.query(bb):
            s = ext_candidates[i]
            ang = seg_angle_deg(s["p1"], s["p2"])
            perp = (78 < ang < 102) if horizontal else (ang < 12 or ang > 168)
            if not perp:
                continue
            for near_end, far_end in ((s["p1"], s["p2"]), (s["p2"], s["p1"])):
                if math.hypot(near_end[0] - point[0], near_end[1] - point[1]) <= 1.8 \
                        and wall_near(far_end):
                    return True
        return False

    label_index = SpatialIndex(cell=12.0)
    for i, lab in enumerate(number_labels):
        label_index.insert(i, lab["bbox"])

    dimensions: list[dict] = []
    consumed: set[str] = set()
    for s in candidates:
        if not (tick_at(s["p1"]) and tick_at(s["p2"])):
            continue
        horizontal = abs(s["p2"][1] - s["p1"][1]) < abs(s["p2"][0] - s["p1"][0])
        mid = ((s["p1"][0] + s["p2"][0]) / 2, (s["p1"][1] + s["p2"][1]) / 2)
        gap_pt = _seg_len(s)
        # значение: целое число >= 2 знаков около линии (в разрыве/рядом/над)
        search = (min(s["p1"][0], s["p2"][0]) - VALUE_SEARCH, min(s["p1"][1], s["p2"][1]) - VALUE_SEARCH,
                  max(s["p1"][0], s["p2"][0]) + VALUE_SEARCH, max(s["p1"][1], s["p2"][1]) + VALUE_SEARCH)
        best_label = None
        best_dist = VALUE_SEARCH
        for i in label_index.query(search):
            lab = number_labels[i]
            if lab["label_id"] in consumed or len(lab["value"]) < 2:
                continue
            if lab.get("color_family") == "red":
                continue  # красные числа — подписи групп, не размерные значения
            d = _point_to_segment(lab["center"], s["p1"], s["p2"])
            if d < best_dist:
                best_dist = d
                best_label = lab
        if best_label is None:
            continue

        ends = []
        for point in (s["p1"], s["p2"]):
            end = {"point": (round(point[0], 2), round(point[1], 2)), "attached_to": "unresolved"}
            device = _device_on_axis(devices, point, horizontal)
            if device is not None:
                end.update({"attached_to": "device_axis", "device_id": device})
            elif wall_near(point) or wall_via_extension(point, horizontal):
                end["attached_to"] = "wall_or_opening"
            ends.append(end)

        dimensions.append({
            "dim_id": f"dim-{len(dimensions) + 1}",
            "value_mm": int(best_label["value"]),
            "label_id": best_label["label_id"],
            "gap_pt": round(gap_pt, 2),
            "line": {"p1": s["p1"], "p2": s["p2"], "sid": s["sid"], "layer": s["layer"]},
            "orientation": "horizontal" if horizontal else "vertical",
            "ends": ends,
            "center": (round(mid[0], 2), round(mid[1], 2)),
        })
        consumed.add(best_label["label_id"])

    _estimate_scale(dimensions)
    return dimensions, consumed


def _point_to_segment(p, a, b) -> float:
    ax, ay = a
    bx, by = b
    dx, dy = bx - ax, by - ay
    den = dx * dx + dy * dy
    if den < 1e-12:
        return math.hypot(p[0] - ax, p[1] - ay)
    t = max(0.0, min(1.0, ((p[0] - ax) * dx + (p[1] - ay) * dy) / den))
    return math.hypot(p[0] - (ax + t * dx), p[1] - (ay + t * dy))


def _device_on_axis(devices, point, horizontal) -> str | None:
    """Устройство, ось которого проходит через конец размерной линии.

    Для горизонтального размера сравнивается X оси устройства, для
    вертикального — Y; поперечное смещение (выносная линия) допускается.
    """
    best = None
    best_axis = END_ATTACH_TOL
    for dev in devices:
        cx, cy = dev["center"]
        axis_d = abs(cx - point[0]) if horizontal else abs(cy - point[1])
        cross_d = abs(cy - point[1]) if horizontal else abs(cx - point[0])
        if axis_d <= best_axis and cross_d <= 55.0:
            best_axis = axis_d
            best = dev["symbol_id"]
    return best


def _estimate_scale(dimensions: list[dict]) -> None:
    """Масштаб мм/pt медианой по конструкциям; выбросы > 5% → tier 2."""
    ratios = sorted(d["value_mm"] / d["gap_pt"] for d in dimensions if d["gap_pt"] > 0.5)
    if not ratios:
        return
    median = ratios[len(ratios) // 2]
    for d in dimensions:
        ratio = d["value_mm"] / d["gap_pt"] if d["gap_pt"] > 0.5 else 0.0
        d["scale_mm_per_pt"] = round(ratio, 3)
        d["scale_consistent"] = bool(ratio and abs(ratio / median - 1.0) <= 0.05)
    for d in dimensions:
        d["sheet_scale_mm_per_pt"] = round(median, 3)


# ------------------------------------------------------- линии центрирования

def detect_centering_guides(inv: dict, scope_of, lights: list[dict]) -> dict:
    """Пересечения серых диагональных цепочек ↔ центры световых точек."""
    gray = [s for s in inv["segments"]
            if s["kind"] == "l" and s["color_family"] == "gray"
            and scope_of(_seg_bbox(s)) == "block"]
    chains = build_chains(gray, min_len=14.0)
    diag = [c for c in chains if 12.0 <= c["angle"] <= 168.0 and not (84.0 <= c["angle"] <= 96.0)]

    index = SpatialIndex(cell=25.0)
    for i, c in enumerate(diag):
        index.insert(i, c["bbox"])

    confirmed = {}
    for light in lights:
        cx, cy = light["center"]
        near_ids = index.query((cx - 4, cy - 4, cx + 4, cy + 4))
        hits = []
        for i in near_ids:
            for j in near_ids:
                if j <= i:
                    continue
                a, b = diag[i], diag[j]
                if abs(a["angle"] - b["angle"]) < 8.0:
                    continue
                cross = line_intersection(a["p1"], a["p2"], b["p1"], b["p2"])
                if cross and math.hypot(cross[0] - cx, cross[1] - cy) <= 2.8:
                    hits.append((a["chain_id"], b["chain_id"]))
        if hits:
            confirmed[light["symbol_id"]] = hits[0]
    return {"chains_total": len(chains), "diag_chains": len(diag), "confirmed": confirmed}
