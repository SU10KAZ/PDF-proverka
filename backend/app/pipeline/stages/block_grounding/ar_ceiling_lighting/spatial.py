"""Пространственные примитивы пилота: grid-hash индекс, коллинеарные
цепочки штрихов, сеточный flood-fill областей помещений.

Без O(N²) по всем примитивам: любые запросы «что рядом» идут через
SpatialIndex (ячейка 6 pt), цепочки собираются стратификацией по
(угол, смещение линии), заливка — по клеточной сетке.
"""
from __future__ import annotations

import collections
import math


class SpatialIndex:
    """Grid-hash: bbox → ячейки; запросы кандидатов вокруг точки/бокса."""

    def __init__(self, cell: float = 6.0):
        self.cell = float(cell)
        self._cells: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
        self._boxes: dict[int, tuple[float, float, float, float]] = {}

    def _range(self, bbox):
        c = self.cell
        return (int(bbox[0] // c), int(bbox[1] // c), int(bbox[2] // c), int(bbox[3] // c))

    def insert(self, oid: int, bbox) -> None:
        self._boxes[oid] = tuple(bbox)
        i0, j0, i1, j1 = self._range(bbox)
        for i in range(i0, i1 + 1):
            for j in range(j0, j1 + 1):
                self._cells[(i, j)].append(oid)

    def query(self, bbox, pad: float = 0.0) -> list[int]:
        q = (bbox[0] - pad, bbox[1] - pad, bbox[2] + pad, bbox[3] + pad)
        i0, j0, i1, j1 = self._range(q)
        seen: set[int] = set()
        out: list[int] = []
        for i in range(i0, i1 + 1):
            for j in range(j0, j1 + 1):
                for oid in self._cells.get((i, j), ()):
                    if oid in seen:
                        continue
                    seen.add(oid)
                    b = self._boxes[oid]
                    if b[0] <= q[2] and b[2] >= q[0] and b[1] <= q[3] and b[3] >= q[1]:
                        out.append(oid)
        return sorted(out)


def bbox_gap(a, b) -> float:
    """Зазор между двумя bbox (0 при пересечении)."""
    dx = max(a[0] - b[2], b[0] - a[2], 0.0)
    dy = max(a[1] - b[3], b[1] - a[3], 0.0)
    return math.hypot(dx, dy)


def seg_angle_deg(p1, p2) -> float:
    """Угол сегмента в градусах, нормированный в [0, 180)."""
    ang = math.degrees(math.atan2(p2[1] - p1[1], p2[0] - p1[0])) % 180.0
    return ang


def build_chains(segments: list[dict], *, angle_tol: float = 3.0, offset_tol: float = 1.4,
                 max_gap: float = 10.0, min_len: float = 12.0) -> list[dict]:
    """Сборка коллинеарных цепочек из микроштрихов (замена мёртвого dashes).

    segments: [{sid, p1, p2, ...}]. Стратификация по (квантованный угол,
    квантованное смещение линии rho), внутри — сортировка вдоль направления
    и разрез по зазорам > max_gap. Возврат: цепочки с длиной >= min_len.
    """
    buckets: dict[tuple[int, int], list[tuple[float, float, dict]]] = collections.defaultdict(list)
    for seg in segments:
        p1, p2 = seg["p1"], seg["p2"]
        ang = seg_angle_deg(p1, p2)
        rad = math.radians(ang)
        nx, ny = -math.sin(rad), math.cos(rad)  # нормаль к направлению
        rho = nx * (p1[0] + p2[0]) / 2 + ny * (p1[1] + p2[1]) / 2
        for da in (0, 1):  # сегмент на границе углового кванта попадает в оба
            key = (int((ang + da * angle_tol / 2) // angle_tol) % int(180 // angle_tol),
                   int(rho // offset_tol))
            t1 = math.cos(rad) * p1[0] + math.sin(rad) * p1[1]
            t2 = math.cos(rad) * p2[0] + math.sin(rad) * p2[1]
            buckets[key].append((min(t1, t2), max(t1, t2), seg))

    chains: list[dict] = []
    used: set[int] = set()
    for key in sorted(buckets):
        row = sorted(buckets[key], key=lambda item: item[0])
        cur: list[tuple[float, float, dict]] = []
        for item in row:
            if item[2]["sid"] in used:
                continue
            if cur and item[0] - cur[-1][1] > max_gap:
                _flush_chain(chains, cur, used, min_len)
                cur = []
            cur.append(item)
        _flush_chain(chains, cur, used, min_len)
    for idx, chain in enumerate(chains):
        chain["chain_id"] = f"chain-{idx + 1}"
    return chains


def _flush_chain(chains, cur, used, min_len) -> None:
    if len(cur) < 2:
        return
    length = cur[-1][1] - cur[0][0]
    if length < min_len:
        return
    pts: list[tuple[float, float]] = []
    for _, _, seg in cur:
        pts.extend((seg["p1"], seg["p2"]))
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    start = min(pts, key=lambda p: (p[0], p[1]))
    end = max(pts, key=lambda p: (p[0], p[1]))
    for _, _, seg in cur:
        used.add(seg["sid"])
    chains.append({
        "p1": (round(start[0], 2), round(start[1], 2)),
        "p2": (round(end[0], 2), round(end[1], 2)),
        "bbox": (round(min(xs), 2), round(min(ys), 2), round(max(xs), 2), round(max(ys), 2)),
        "angle": round(seg_angle_deg(start, end), 2),
        "length": round(length, 2),
        "segment_ids": sorted(seg["sid"] for _, _, seg in cur),
    })


def line_intersection(a1, a2, b1, b2):
    """Пересечение отрезков (в пределах их габаритов + малый запас)."""
    d1x, d1y = a2[0] - a1[0], a2[1] - a1[1]
    d2x, d2y = b2[0] - b1[0], b2[1] - b1[1]
    den = d1x * d2y - d1y * d2x
    if abs(den) < 1e-9:
        return None
    t = ((b1[0] - a1[0]) * d2y - (b1[1] - a1[1]) * d2x) / den
    u = ((b1[0] - a1[0]) * d1y - (b1[1] - a1[1]) * d1x) / den
    if -0.05 <= t <= 1.05 and -0.05 <= u <= 1.05:
        return (a1[0] + t * d1x, a1[1] + t * d1y)
    return None


class OccupancyGrid:
    """Клеточная сетка барьеров и flood-fill областей помещений."""

    def __init__(self, bbox, cell: float = 2.5):
        self.cell = float(cell)
        self.x0, self.y0 = float(bbox[0]), float(bbox[1])
        self.nx = max(1, int(math.ceil((bbox[2] - bbox[0]) / cell)))
        self.ny = max(1, int(math.ceil((bbox[3] - bbox[1]) / cell)))
        self.blocked: set[tuple[int, int]] = set()

    def cell_of(self, x: float, y: float) -> tuple[int, int]:
        return (int((x - self.x0) // self.cell), int((y - self.y0) // self.cell))

    def in_range(self, ij) -> bool:
        return 0 <= ij[0] < self.nx and 0 <= ij[1] < self.ny

    def mark_segment(self, p1, p2) -> None:
        """Supercover-разметка: все клетки, которые пересекает отрезок."""
        x1, y1 = p1
        x2, y2 = p2
        steps = max(1, int(math.hypot(x2 - x1, y2 - y1) / (self.cell * 0.45)))
        for k in range(steps + 1):
            t = k / steps
            ij = self.cell_of(x1 + (x2 - x1) * t, y1 + (y2 - y1) * t)
            if self.in_range(ij):
                self.blocked.add(ij)

    def flood(self, x: float, y: float, *, cap: int = 80000):
        """BFS от точки; возврат (region_cells | None-если-перелив, overflow)."""
        seed = self.cell_of(x, y)
        if not self.in_range(seed) or seed in self.blocked:
            # марка стоит на границе/за пределами — пробуем соседние клетки
            for di, dj in ((0, 1), (0, -1), (1, 0), (-1, 0), (1, 1), (-1, -1), (1, -1), (-1, 1)):
                cand = (seed[0] + di, seed[1] + dj)
                if self.in_range(cand) and cand not in self.blocked:
                    seed = cand
                    break
            else:
                return None, False
        region: set[tuple[int, int]] = {seed}
        queue = collections.deque([seed])
        while queue:
            if len(region) > cap:
                return region, True
            i, j = queue.popleft()
            for di, dj in ((0, 1), (0, -1), (1, 0), (-1, 0)):
                nxt = (i + di, j + dj)
                if nxt in region or not self.in_range(nxt) or nxt in self.blocked:
                    continue
                region.add(nxt)
                queue.append(nxt)
        return region, False

    def region_bbox(self, region) -> tuple[float, float, float, float]:
        xs = [ij[0] for ij in region]
        ys = [ij[1] for ij in region]
        return (round(self.x0 + min(xs) * self.cell, 2), round(self.y0 + min(ys) * self.cell, 2),
                round(self.x0 + (max(xs) + 1) * self.cell, 2), round(self.y0 + (max(ys) + 1) * self.cell, 2))
