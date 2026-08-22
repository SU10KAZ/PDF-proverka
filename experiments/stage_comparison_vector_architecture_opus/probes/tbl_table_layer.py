"""tbl_table_layer — generic vector table reconstruction (Track B probe, research only).

No discipline knowledge. Input: a PDF page (optionally a region). Output: tables as
rows x columns of joined text, with merged cells and per-cell clipping flags.

Pipeline:
  1. flatten page.get_drawings() to segments (l / re / qu; curves ignored as non-rulings)
  2. keep axis-aligned segments, merge collinear ones into rulings
  3. bipartite H<->V intersection graph -> connected components -> table candidates
  4. per candidate: column/row boundaries, base cells, merge across missing borders
  5. assign text spans to cells by span centre, join spans preserving intra-span spaces

Run from repo root:  python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_table_layer <pdf> <page_index>
"""
from __future__ import annotations

import math
import sys
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

import fitz

# ---------------------------------------------------------------- tunables
AXIS_TOL = 0.6        # pt: max deviation for a segment to count as axis-aligned
CLUSTER_TOL = 1.6     # pt: two rulings with |coord| below this are the same grid line
JOIN_GAP = 2.5        # pt: collinear rulings separated by less than this are merged
MIN_RULING = 14.0     # pt: shorter merged rulings are not grid lines
BORDER_COVER = 0.75   # fraction of a cell edge that must be covered by a ruling


# ---------------------------------------------------------------- geometry
def flatten_segments(drawings: Sequence[dict],
                     matrix: "fitz.Matrix | None" = None
                     ) -> list[tuple[float, float, float, float]]:
    """Flatten drawing items to segments.

    `matrix` must be `page.rotation_matrix` on a /Rotate page: PyMuPDF returns text in
    rotated page space but vector drawings in unrotated space, so the two layers do not
    share a frame unless the drawings are mapped forward.
    """
    segs: list[tuple[float, float, float, float]] = []
    for path in drawings:
        for item in path.get("items") or []:
            kind = item[0]
            if kind == "l":
                p1, p2 = item[1], item[2]
                segs.append((p1.x, p1.y, p2.x, p2.y))
            elif kind == "re":
                r = item[1]
                x0, y0, x1, y1 = r.x0, r.y0, r.x1, r.y1
                segs.extend([(x0, y0, x1, y0), (x1, y0, x1, y1),
                             (x1, y1, x0, y1), (x0, y1, x0, y0)])
            elif kind == "qu":
                q = item[1]
                pts = [q.ul, q.ur, q.lr, q.ll]
                for i in range(4):
                    a, b = pts[i], pts[(i + 1) % 4]
                    segs.append((a.x, a.y, b.x, b.y))
            # 'c' (bezier) intentionally ignored: curves are not table rulings
    if matrix is not None:
        mapped = []
        for x0, y0, x1, y1 in segs:
            a = fitz.Point(x0, y0) * matrix
            b = fitz.Point(x1, y1) * matrix
            mapped.append((a.x, a.y, b.x, b.y))
        segs = mapped
    return segs


@dataclass
class Ruling:
    coord: float          # y for horizontal, x for vertical
    lo: float             # start along the ruling axis
    hi: float
    horizontal: bool

    @property
    def length(self) -> float:
        return self.hi - self.lo


def detect_rulings(segs: Sequence[tuple[float, float, float, float]],
                   min_ruling: float = MIN_RULING) -> tuple[list[Ruling], list[Ruling]]:
    """Merge axis-aligned collinear segments into rulings."""
    h_raw: list[tuple[float, float, float]] = []   # (y, x0, x1)
    v_raw: list[tuple[float, float, float]] = []   # (x, y0, y1)
    for x0, y0, x1, y1 in segs:
        dx, dy = abs(x1 - x0), abs(y1 - y0)
        if dy <= AXIS_TOL and dx > AXIS_TOL:
            h_raw.append(((y0 + y1) / 2.0, min(x0, x1), max(x0, x1)))
        elif dx <= AXIS_TOL and dy > AXIS_TOL:
            v_raw.append(((x0 + x1) / 2.0, min(y0, y1), max(y0, y1)))

    def merge(raw: list[tuple[float, float, float]], horizontal: bool) -> list[Ruling]:
        out: list[Ruling] = []
        raw = sorted(raw, key=lambda t: (t[0], t[1]))
        i = 0
        while i < len(raw):
            j = i
            base = raw[i][0]
            while j < len(raw) and raw[j][0] - base <= CLUSTER_TOL:
                j += 1
            group = raw[i:j]
            coord = sum(g[0] for g in group) / len(group)
            spans = sorted((g[1], g[2]) for g in group)
            cur_lo, cur_hi = spans[0]
            for lo, hi in spans[1:]:
                if lo <= cur_hi + JOIN_GAP:
                    cur_hi = max(cur_hi, hi)
                else:
                    out.append(Ruling(coord, cur_lo, cur_hi, horizontal))
                    cur_lo, cur_hi = lo, hi
            out.append(Ruling(coord, cur_lo, cur_hi, horizontal))
            i = j
        return [r for r in out if r.length >= min_ruling]

    return merge(h_raw, True), merge(v_raw, False)


def grid_lines(rulings: list[Ruling]) -> list[tuple[float, list[Ruling]]]:
    """Cluster rulings that share a coordinate into one grid line."""
    out: list[tuple[float, list[Ruling]]] = []
    rulings = sorted(rulings, key=lambda r: r.coord)
    i = 0
    while i < len(rulings):
        j = i
        base = rulings[i].coord
        while j < len(rulings) and rulings[j].coord - base <= CLUSTER_TOL:
            j += 1
        group = rulings[i:j]
        out.append((sum(r.coord for r in group) / len(group), group))
        i = j
    return out


def _covered(group: list[Ruling], lo: float, hi: float) -> float:
    """Fraction of [lo,hi] covered by the union of ruling spans in group."""
    if hi - lo <= 1e-6:
        return 1.0
    spans = sorted((max(r.lo, lo), min(r.hi, hi)) for r in group)
    total, cur_lo, cur_hi = 0.0, None, None
    for a, b in spans:
        if b <= a:
            continue
        if cur_lo is None:
            cur_lo, cur_hi = a, b
        elif a <= cur_hi + 0.5:
            cur_hi = max(cur_hi, b)
        else:
            total += cur_hi - cur_lo
            cur_lo, cur_hi = a, b
    if cur_lo is not None:
        total += cur_hi - cur_lo
    return total / (hi - lo)


def _spans_interval(group: list[Ruling], lo: float, hi: float,
                    tol: float = CLUSTER_TOL) -> bool:
    """True when one merged run of this grid line reaches both ends of [lo,hi].

    Stricter than a coverage fraction: a long ruling that misses the first and last
    few points of a very wide interval must not count as a border.
    """
    if hi - lo <= 1e-6:
        return True
    runs: list[tuple[float, float]] = []
    for r in sorted(group, key=lambda r: r.lo):
        if runs and r.lo <= runs[-1][1] + JOIN_GAP:
            runs[-1] = (runs[-1][0], max(runs[-1][1], r.hi))
        else:
            runs.append((r.lo, r.hi))
    return any(a <= lo + tol and b >= hi - tol for a, b in runs)

def _near(pairs: list[tuple[float, list[Ruling]]], value: float) -> float:
    return min((c for c, _ in pairs), key=lambda c: abs(c - value))

LOCAL_LINE_COVER = 0.15
EMPTY_BAND_SPLIT = 4.0   # a band this many times the median row height splits the region   # a line must cover this much of the table bbox to be a grid line


def _median(values: list[float]) -> float:
    vals = sorted(v for v in values if v > 0)
    if not vals:
        return 1.0
    return vals[len(vals) // 2]


def table_candidates(h_rulings: list[Ruling], v_rulings: list[Ruling]) -> list[dict[str, Any]]:
    """Row-band seeding.

    For every pair of adjacent horizontal grid lines only the vertical lines that
    actually cross that band are considered, so a table is never over-segmented by
    verticals belonging to an unrelated part of the sheet (title block, frame).
    A closed cell needs all four borders; connected closed cells form a table core.
    """
    hs = grid_lines(h_rulings)
    vs = grid_lines(v_rulings)
    if len(hs) < 2 or len(vs) < 2:
        return []
    ys = [c for c, _ in hs]

    closed: list[dict[str, Any]] = []   # {"band": i, "x0","x1","y0","y1"}
    for i in range(len(hs) - 1):
        y0, y1 = ys[i], ys[i + 1]
        if y1 - y0 < 3.0:
            continue
        crossing = [c for c, grp in vs if _spans_interval(grp, y0, y1)]
        for a, b in zip(crossing, crossing[1:]):
            if b - a < 3.0:
                continue
            if (_spans_interval(hs[i][1], a, b) and
                    _spans_interval(hs[i + 1][1], a, b)):
                closed.append({"band": i, "x0": a, "x1": b, "y0": y0, "y1": y1})
    if not closed:
        return []

    # connect closed cells that share an edge
    n = len(closed)
    parent = list(range(n))

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for a in range(n):
        ca = closed[a]
        for b in range(a + 1, n):
            cb = closed[b]
            share_x = min(ca["x1"], cb["x1"]) - max(ca["x0"], cb["x0"]) > 1.0
            share_y = min(ca["y1"], cb["y1"]) - max(ca["y0"], cb["y0"]) > 1.0
            touch_v = abs(ca["y1"] - cb["y0"]) < CLUSTER_TOL or abs(cb["y1"] - ca["y0"]) < CLUSTER_TOL
            touch_h = abs(ca["x1"] - cb["x0"]) < CLUSTER_TOL or abs(cb["x1"] - ca["x0"]) < CLUSTER_TOL
            if (share_x and touch_v) or (share_y and touch_h):
                union(a, b)

    comps: dict[int, list[dict[str, Any]]] = {}
    for a in range(n):
        comps.setdefault(find(a), []).append(closed[a])

    def expand(bx0, by0, bx1, by1, med_h, med_w):
        """Grow the seed over rows/columns whose separators are hidden by merged cells
        or were split, in the global line list, by unrelated linework."""
        for _ in range(400):
            grew = False
            below = [c for c, grp in hs if c > by1 + CLUSTER_TOL
                     and _spans_interval(grp, bx0, bx1)]
            if below and (below[0] - by1) <= 6 * med_h:
                y = below[0]
                if (_spans_interval(dict(vs_map)[_near(vs_map, bx0)], by1, y) and
                        _spans_interval(dict(vs_map)[_near(vs_map, bx1)], by1, y)):
                    by1 = y; grew = True
            above = [c for c, grp in hs if c < by0 - CLUSTER_TOL
                     and _spans_interval(grp, bx0, bx1)]
            if above and (by0 - above[-1]) <= 6 * med_h:
                y = above[-1]
                if (_spans_interval(dict(vs_map)[_near(vs_map, bx0)], y, by0) and
                        _spans_interval(dict(vs_map)[_near(vs_map, bx1)], y, by0)):
                    by0 = y; grew = True
            right = [c for c, grp in vs if c > bx1 + CLUSTER_TOL
                     and _spans_interval(grp, by0, by1)]
            if right and (right[0] - bx1) <= 6 * med_w:
                x = right[0]
                if (_spans_interval(dict(hs_map)[_near(hs_map, by0)], bx1, x) and
                        _spans_interval(dict(hs_map)[_near(hs_map, by1)], bx1, x)):
                    bx1 = x; grew = True
            left = [c for c, grp in vs if c < bx0 - CLUSTER_TOL
                    and _spans_interval(grp, by0, by1)]
            if left and (bx0 - left[-1]) <= 6 * med_w:
                x = left[-1]
                if (_spans_interval(dict(hs_map)[_near(hs_map, by0)], x, bx0) and
                        _spans_interval(dict(hs_map)[_near(hs_map, by1)], x, bx0)):
                    bx0 = x; grew = True
            if not grew:
                break
        return bx0, by0, bx1, by1

    hs_map = hs
    vs_map = vs

    out = []
    for members in comps.values():
        bx0 = min(m["x0"] for m in members); bx1 = max(m["x1"] for m in members)
        by0 = min(m["y0"] for m in members); by1 = max(m["y1"] for m in members)
        if len(members) < 2:
            continue
        med_h = _median([m["y1"] - m["y0"] for m in members])
        med_w = _median([m["x1"] - m["x0"] for m in members])
        bx0, by0, bx1, by1 = expand(bx0, by0, bx1, by1, med_h, med_w)
        sub_h = [(c, grp) for c, grp in hs
                 if by0 - CLUSTER_TOL <= c <= by1 + CLUSTER_TOL
                 and _covered(grp, bx0, bx1) >= LOCAL_LINE_COVER]
        sub_v = [(c, grp) for c, grp in vs
                 if bx0 - CLUSTER_TOL <= c <= bx1 + CLUSTER_TOL
                 and _covered(grp, by0, by1) >= LOCAL_LINE_COVER]
        if len(sub_h) < 2 or len(sub_v) < 2:
            continue
        n_rows = len(sub_h) - 1
        n_cols = len(sub_v) - 1
        if n_rows < 1 or n_cols < 1:
            continue
        if n_rows * n_cols < 2:
            continue
        out.append({
            "hs": sub_h, "vs": sub_v,
            "bbox": (bx0, by0, bx1, by1),
            "closed_cells": len(members),
        })
    out.sort(key=lambda t: -((t["bbox"][2] - t["bbox"][0]) * (t["bbox"][3] - t["bbox"][1])))
    kept = []
    for cand in out:
        bx = cand["bbox"]
        if any(k["bbox"][0] <= bx[0] + 1 and k["bbox"][1] <= bx[1] + 1 and
               k["bbox"][2] >= bx[2] - 1 and k["bbox"][3] >= bx[3] - 1 for k in kept):
            continue
        kept.append(cand)
    return kept


# ---------------------------------------------------------------- grid
@dataclass
class Cell:
    row: int
    col: int
    rowspan: int
    colspan: int
    rect: tuple[float, float, float, float]
    borders: int = 0
    text: str = ""
    span_count: int = 0
    clipped: bool = False


def build_table(cand: dict[str, Any]) -> dict[str, Any]:
    hs = cand["hs"]          # list of (y, [Ruling]) bounding the table's rows
    vs = cand["vs"]
    ys = [c for c, _ in hs]
    xs = [c for c, _ in vs]
    n_rows, n_cols = len(ys) - 1, len(xs) - 1
    if n_rows < 1 or n_cols < 1:
        return {"rows": 0, "cols": 0, "cells": [], "bbox": cand["bbox"]}

    parent: dict[Any, Any] = {}

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for i in range(n_rows):
        for j in range(n_cols):
            parent[(i, j)] = (i, j)

    for i in range(n_rows):
        for j in range(n_cols):
            r_ok = _spans_interval(vs[j + 1][1], ys[i], ys[i + 1])
            b_ok = _spans_interval(hs[i + 1][1], xs[j], xs[j + 1])
            if not r_ok and j + 1 < n_cols:
                union((i, j), (i, j + 1))
            if not b_ok and i + 1 < n_rows:
                union((i, j), (i + 1, j))

    regions: dict[Any, list[tuple[int, int]]] = {}
    for i in range(n_rows):
        for j in range(n_cols):
            regions.setdefault(find((i, j)), []).append((i, j))

    cells: list[Cell] = []
    for members in regions.values():
        r0 = min(m[0] for m in members); r1 = max(m[0] for m in members)
        c0 = min(m[1] for m in members); c1 = max(m[1] for m in members)
        rect = (xs[c0], ys[r0], xs[c1 + 1], ys[r1 + 1])
        borders = 0
        borders += 1 if _spans_interval(vs[c0][1], ys[r0], ys[r1 + 1]) else 0
        borders += 1 if _spans_interval(vs[c1 + 1][1], ys[r0], ys[r1 + 1]) else 0
        borders += 1 if _spans_interval(hs[r0][1], xs[c0], xs[c1 + 1]) else 0
        borders += 1 if _spans_interval(hs[r1 + 1][1], xs[c0], xs[c1 + 1]) else 0
        cells.append(Cell(r0, c0, r1 - r0 + 1, c1 - c0 + 1, rect, borders))

    # logical re-indexing: a base-grid row split by a neighbouring table's ruling
    # must not become an extra table row.
    tops = sorted({round(c.rect[1], 2) for c in cells})
    lefts = sorted({round(c.rect[0], 2) for c in cells})
    for c in cells:
        c.row = tops.index(round(c.rect[1], 2))
        c.col = lefts.index(round(c.rect[0], 2))
    cells.sort(key=lambda c: (c.row, c.col))
    return {"rows": len(tops), "cols": len(lefts), "cells": cells,
            "xs": xs, "ys": ys, "_hs": hs, "_vs": vs, "bbox": cand["bbox"],
            "closed_cells": cand.get("closed_cells")}


# ---------------------------------------------------------------- text
def page_spans(page: fitz.Page, clip: tuple[float, float, float, float] | None = None
               ) -> list[dict[str, Any]]:
    """Spans in *displayed* page space.

    PyMuPDF returns both text and drawings in unrotated space while `page.rect` and
    `get_pixmap` are in displayed space, so on a /Rotate page every span is mapped
    forward with `page.rotation_matrix` before use.
    """
    matrix = page.rotation_matrix if page.rotation else None
    if clip is not None:
        rect = fitz.Rect(*clip)
        if matrix is not None:
            rect = rect * page.derotation_matrix
        data = page.get_text("dict", clip=rect)
    else:
        data = page.get_text("dict")
    out = []
    line_id = 0
    for block in data.get("blocks") or []:
        if block.get("type") != 0:
            continue
        for line in block.get("lines") or []:
            direction = line.get("dir") or (1.0, 0.0)
            rot = math.degrees(math.atan2(float(direction[1]), float(direction[0])))
            for span in line.get("spans") or []:
                raw = str(span.get("text") or "")
                if not raw:
                    continue
                bbox = [float(v) for v in span["bbox"]]
                if matrix is not None:
                    r = fitz.Rect(*bbox) * matrix
                    r.normalize()
                    bbox = [r.x0, r.y0, r.x1, r.y1]
                out.append({
                    "raw": raw, "blank": not raw.strip(),
                    "bbox": bbox, "line": line_id, "rot": rot,
                    "cx": (bbox[0] + bbox[2]) / 2, "cy": (bbox[1] + bbox[3]) / 2,
                    "size": float(span.get("size") or 0.0),
                })
            line_id += 1
    return out


def fill_table_text(table: dict[str, Any], spans: Sequence[dict[str, Any]]) -> dict[str, Any]:
    buckets: dict[int, list[dict[str, Any]]] = {}
    for k, cell in enumerate(table["cells"]):
        buckets[k] = []
    for sp in spans:
        for k, cell in enumerate(table["cells"]):
            x0, y0, x1, y1 = cell.rect
            if x0 - 0.5 <= sp["cx"] <= x1 + 0.5 and y0 - 0.5 <= sp["cy"] <= y1 + 0.5:
                buckets[k].append(sp)
                break
    for k, cell in enumerate(table["cells"]):
        group = buckets[k]
        cell.span_count = sum(1 for s in group if not s["blank"])
        if not any(not s["blank"] for s in group):
            cell.text = ""
            continue
        lines: dict[int, list[dict[str, Any]]] = {}
        for sp in group:
            lines.setdefault(sp["line"], []).append(sp)
        parts = []
        for lid in sorted(lines, key=lambda l: (min(s["cy"] for s in lines[l]),
                                                min(s["bbox"][0] for s in lines[l]))):
            row = sorted(lines[lid], key=lambda s: s["bbox"][0])
            parts.append("".join(s["raw"] for s in row))
        cell.text = " ".join(" ".join(p.split()) for p in parts).strip()
    return table


def open_sides(table: dict[str, Any],
               region: tuple[float, float, float, float] | None = None,
               tol: float = 3.0) -> dict[str, bool]:
    """Which sides of the table have no complete outer border.

    A side is *open* when its outer ruling does not span the table, or when the table
    bbox coincides with the region boundary — i.e. the crop, not the drawing, ended
    the table.  Rows or columns can be missing beyond an open side, so a row-count
    difference there is not evidence of a design change.
    """
    xs, ys = table["xs"], table["ys"]
    hs, vs = table["_hs"], table["_vs"]
    res = {
        "top": not _spans_interval(hs[0][1], xs[0], xs[-1]),
        "bottom": not _spans_interval(hs[-1][1], xs[0], xs[-1]),
        "left": not _spans_interval(vs[0][1], ys[0], ys[-1]),
        "right": not _spans_interval(vs[-1][1], ys[0], ys[-1]),
    }
    if region is not None:
        bx0, by0, bx1, by1 = table["bbox"]
        res["top"] = res["top"] or abs(by0 - region[1]) <= tol
        res["bottom"] = res["bottom"] or abs(by1 - region[3]) <= tol
        res["left"] = res["left"] or abs(bx0 - region[0]) <= tol
        res["right"] = res["right"] or abs(bx1 - region[2]) <= tol
    return res


def mark_clipped(table: dict[str, Any], region: tuple[float, float, float, float],
                 margin: float = 1.0) -> dict[str, Any]:
    rx0, ry0, rx1, ry1 = region
    for cell in table["cells"]:
        x0, y0, x1, y1 = cell.rect
        cell.clipped = (x0 < rx0 - margin or y0 < ry0 - margin
                        or x1 > rx1 + margin or y1 > ry1 + margin)
    return table


# ---------------------------------------------------------------- serialisation
def table_matrix(table: dict[str, Any]) -> list[list[str]]:
    grid = [["" for _ in range(table["cols"])] for _ in range(table["rows"])]
    for cell in table["cells"]:
        grid[cell.row][cell.col] = cell.text
    return grid


def table_rows(table: dict[str, Any]) -> list[list[str]]:
    """Rows of joined text, one entry per resolved (possibly merged) cell."""
    rows: dict[int, list[Cell]] = {}
    for cell in table["cells"]:
        rows.setdefault(cell.row, []).append(cell)
    out = []
    for r in sorted(rows):
        out.append([c.text for c in sorted(rows[r], key=lambda c: c.col)])
    return out


def table_to_dict(table: dict[str, Any]) -> dict[str, Any]:
    return {
        "rows": table["rows"], "cols": table["cols"],
        "bbox": [round(v, 2) for v in table["bbox"]],
        "closed_cells": table.get("closed_cells"),
        "cells": [
            {"row": c.row, "col": c.col, "rowspan": c.rowspan, "colspan": c.colspan,
             "rect": [round(v, 2) for v in c.rect], "borders": c.borders,
             "text": c.text, "span_count": c.span_count, "clipped": c.clipped}
            for c in table["cells"]
        ],
    }


# ---------------------------------------------------------------- top level
def reconstruct(page: fitz.Page, drawings: Sequence[dict] | None = None,
                region: tuple[float, float, float, float] | None = None,
                overlap_min: float = 0.30,
                min_ruling: float = MIN_RULING,
                data_tables_only: bool = True,
                clip_to_region: bool = False) -> list[dict[str, Any]]:
    """Detect tables on the page; if region given, keep those overlapping it."""
    dr = drawings if drawings is not None else page.get_drawings()
    matrix = page.rotation_matrix if page.rotation else None
    segs = flatten_segments(dr, matrix)
    if clip_to_region and region is not None:
        segs = _clip_segments(segs, region)
    h, v = detect_rulings(segs, min_ruling=min_ruling)
    cands = table_candidates(h, v)
    if region is not None:
        kept = []
        for c in cands:
            bx0, by0, bx1, by1 = c["bbox"]
            ix0, iy0 = max(bx0, region[0]), max(by0, region[1])
            ix1, iy1 = min(bx1, region[2]), min(by1, region[3])
            inter = max(0.0, ix1 - ix0) * max(0.0, iy1 - iy0)
            area = max(1e-6, (bx1 - bx0) * (by1 - by0))
            if inter / area >= overlap_min:
                kept.append(c)
        cands = kept
    spans = page_spans(page, clip=region if clip_to_region else None)
    cands = _split_on_empty_bands(cands, spans)
    tables = []
    for c in cands:
        t = build_table(c)
        if t["rows"] < 1:
            continue
        fill_table_text(t, spans)
        if region is not None:
            mark_clipped(t, region)
        t["filled_cells"] = sum(1 for cell in t["cells"] if cell.text)
        t["open_sides"] = open_sides(t, region if clip_to_region else None)
        tables.append(t)
    if data_tables_only:
        tables = [t for t in tables if is_data_table(t)]
    return tables


def is_data_table(table: dict[str, Any], min_filled: int = 4,
                  min_fill_ratio: float = 0.15) -> bool:
    """Reject rulings that merely happen to form two boxes (frames, arrow heads)."""
    cells = table["cells"]
    if table["rows"] < 2 or table["cols"] < 2 or len(cells) < 4:
        return False
    filled = sum(1 for c in cells if c.text)
    return filled >= min_filled and filled / len(cells) >= min_fill_ratio


def _split_on_empty_bands(cands: list[dict[str, Any]],
                          spans: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Split a candidate wherever a tall band carries no text at all.

    Two unrelated tables that share the sheet frame (a specification above, the title
    block below) otherwise become one region and their columns interleave.
    """
    out: list[dict[str, Any]] = []
    for cand in cands:
        hs, vs = cand["hs"], cand["vs"]
        ys = [c for c, _ in hs]
        bx0, bx1 = vs[0][0], vs[-1][0]
        gaps = [ys[k + 1] - ys[k] for k in range(len(ys) - 1)]
        med = _median(gaps)
        cuts = []
        for k, g in enumerate(gaps):
            if g <= EMPTY_BAND_SPLIT * med:
                continue
            has_text = any(bx0 - 1 <= sp["cx"] <= bx1 + 1 and ys[k] <= sp["cy"] <= ys[k + 1]
                           and not sp["blank"] for sp in spans)
            if not has_text:
                cuts.append(k)
        if not cuts:
            out.append(cand)
            continue
        bounds = [0] + [c for c in cuts] + [c + 1 for c in cuts] + [len(gaps)]
        bounds = sorted(set(bounds))
        for a, b in zip(bounds, bounds[1:]):
            if b - a < 1:
                continue
            part_h = hs[a:b + 1]
            if len(part_h) < 2:
                continue
            py0, py1 = part_h[0][0], part_h[-1][0]
            part_v = [(c, grp) for c, grp in vs if _covered(grp, py0, py1) >= LOCAL_LINE_COVER]
            if len(part_v) < 2 or (len(part_h) - 1) * (len(part_v) - 1) < 2:
                continue
            out.append({"hs": part_h, "vs": part_v,
                        "bbox": (part_v[0][0], py0, part_v[-1][0], py1),
                        "closed_cells": cand.get("closed_cells")})
    return out


def _clip_segments(segs, region):
    """Cut axis-aligned segments at the region boundary (what a block crop really does)."""
    x0, y0, x1, y1 = region
    out = []
    for ax, ay, bx, by in segs:
        if abs(by - ay) <= AXIS_TOL:      # horizontal
            if not (y0 <= (ay + by) / 2 <= y1):
                continue
            lo, hi = max(min(ax, bx), x0), min(max(ax, bx), x1)
            if hi > lo:
                out.append((lo, ay, hi, by))
        elif abs(bx - ax) <= AXIS_TOL:    # vertical
            if not (x0 <= (ax + bx) / 2 <= x1):
                continue
            lo, hi = max(min(ay, by), y0), min(max(ay, by), y1)
            if hi > lo:
                out.append((ax, lo, bx, hi))
        else:
            if x0 <= ax <= x1 and y0 <= ay <= y1 and x0 <= bx <= x1 and y0 <= by <= y1:
                out.append((ax, ay, bx, by))
    return out


def _cli() -> None:
    pdf, page_index = sys.argv[1], int(sys.argv[2])
    doc = fitz.open(pdf)
    page = doc[page_index]
    tables = reconstruct(page)
    print(f"{len(tables)} table candidates on page {page_index}")
    for i, t in enumerate(tables):
        print(f"-- table {i}: {t['rows']}x{t['cols']} bbox={[round(v,1) for v in t['bbox']]} "
              f"cells={len(t['cells'])} closed={t['closed_cells']}")
        for row in table_rows(t)[:8]:
            print("   ", row)


if __name__ == "__main__":
    _cli()
