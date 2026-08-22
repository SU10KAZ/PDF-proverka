"""Probe `obj` — generic GRAPHICAL OBJECT candidate layer, proof of concept.

Research only.  Nothing here is production code and nothing outside
experiments/stage_comparison_vector_architecture_opus/ is touched.

Two independent things live in this module:

1. `extract_segments` — a *minimal* rotation-correct reader of `page.get_drawings()`
   + text spans, clipped to a block bbox.  It deliberately does NOT reuse Track A's
   extractor because Track A clips drawings/text with a rectangle taken from
   `page.rect` (the *displayed*, rotation-applied space) while `get_drawings()` and
   `get_text()` both return *unrotated* coordinates.  On a rotated page that makes the
   JSON describe a different region of the sheet than the diagnostic PNG.

2. `group_objects` — the generic object grouper under test.  No discipline semantics:
   no "автомат", no "дверь", no "камера".  Only geometry-derived signals.

Run:  python -m experiments.stage_comparison_vector_architecture_opus.probes.obj_run
"""

from __future__ import annotations

import hashlib
import math
import statistics
from typing import Any, Iterable, Sequence

import fitz

CURVE_STEPS = 6

_DRAW_CACHE: dict[tuple[str, int], Any] = {}


# --------------------------------------------------------------------------- geometry


def _clip_line(p0, p1, rect):
    """Liang-Barsky clip of a segment against [x0, y0, x1, y1]."""
    x0, y0, x1, y1 = rect
    px, py = p0
    qx, qy = p1
    dx, dy = qx - px, qy - py
    t0, t1 = 0.0, 1.0
    for p, q in ((-dx, px - x0), (dx, x1 - px), (-dy, py - y0), (dy, y1 - py)):
        if p == 0:
            if q < 0:
                return None
            continue
        t = q / p
        if p < 0:
            if t > t1:
                return None
            t0 = max(t0, t)
        else:
            if t < t0:
                return None
            t1 = min(t1, t)
    if t0 > t1:
        return None
    return ((px + t0 * dx, py + t0 * dy), (px + t1 * dx, py + t1 * dy))


def _pt(v):
    if isinstance(v, fitz.Point):
        return (float(v.x), float(v.y))
    return (float(v[0]), float(v[1]))


def _sample_cubic(p0, p1, p2, p3, steps=CURVE_STEPS):
    out = []
    for i in range(steps + 1):
        t = i / steps
        u = 1 - t
        x = u**3 * p0[0] + 3 * u**2 * t * p1[0] + 3 * u * t**2 * p2[0] + t**3 * p3[0]
        y = u**3 * p0[1] + 3 * u**2 * t * p1[1] + 3 * u * t**2 * p2[1] + t**3 * p3[1]
        out.append((x, y))
    return out


def _dist(a, b):
    return math.hypot(a[0] - b[0], a[1] - b[1])


# --------------------------------------------------------------------------- extraction


def _is_invisible(dwg) -> bool:
    """True when the path paints nothing a reader can see on white paper.

    Covers the classic CAD/BIM text-knockout box: a white-filled, unstroked rectangle
    emitted behind an edited label.  Also covers zero-opacity paint.
    """
    def _white(c):
        return c is not None and len(c) >= 3 and all(float(v) >= 0.98 for v in c[:3])
    fill = dwg.get("fill")
    color = dwg.get("color")
    fo = dwg.get("fill_opacity")
    so = dwg.get("stroke_opacity")
    if fo is not None and float(fo) <= 0.01 and (color is None or (so is not None and float(so) <= 0.01)):
        return True
    if color is None and _white(fill):
        return True
    if fill is None and _white(color):
        return True
    if _white(fill) and _white(color):
        return True
    return False


def extract_segments(pdf_path: str, page_index: int, bbox_norm: Sequence[float],
                     drop_invisible: bool = True) -> dict[str, Any]:
    """Return flat segments + text spans of one block, in DISPLAYED coordinates.

    `bbox_norm` is expressed against `page.rect` (the space a rendered crop / block
    index uses).  Drawings and text are read in the page's own unrotated space and
    then mapped forward with `page.rotation_matrix`, so everything ends up in the same
    frame as the diagnostic PNG regardless of /Rotate.
    """
    doc = fitz.open(pdf_path)
    page = doc[page_index]
    page_rotation = int(page.rotation)
    rd = page.rect
    disp = fitz.Rect(
        rd.x0 + bbox_norm[0] * rd.width,
        rd.y0 + bbox_norm[1] * rd.height,
        rd.x0 + bbox_norm[2] * rd.width,
        rd.y0 + bbox_norm[3] * rd.height,
    )
    derot = page.derotation_matrix
    rot = page.rotation_matrix
    clip_unrot = fitz.Rect(disp) * derot
    clip_unrot.normalize()

    key = (pdf_path, page_index)
    if key not in _DRAW_CACHE:
        _DRAW_CACHE[key] = page.get_drawings()
    drawings = _DRAW_CACHE[key]

    disp_rect = [disp.x0, disp.y0, disp.x1, disp.y1]
    segments: list[dict[str, Any]] = []

    def emit(a, b, path_index, style, closed_hint, op):
        A = fitz.Point(*a) * rot
        B = fitz.Point(*b) * rot
        clipped = _clip_line((A.x, A.y), (B.x, B.y), disp_rect)
        if clipped is None:
            return
        (sx, sy), (ex, ey) = clipped
        length = math.hypot(ex - sx, ey - sy)
        if length <= 1e-6:
            return
        segments.append(
            {
                "i": len(segments),
                "p0": (sx, sy),
                "p1": (ex, ey),
                "len": length,
                "path": path_index,
                "op": op,
                "closed": closed_hint,
                "w": style[0],
                "color": style[1],
                "fill": style[2],
            }
        )

    invisible_paths = 0
    invisible_items = 0
    for path_index, dwg in enumerate(drawings):
        r = dwg.get("rect")
        if r is None or not fitz.Rect(r).intersects(clip_unrot):
            continue
        if _is_invisible(dwg):
            invisible_paths += 1
            invisible_items += len(dwg.get("items") or ())
            if drop_invisible:
                continue
        style = (
            round(float(dwg.get("width") or 0.0), 3),
            tuple(round(float(c), 3) for c in (dwg.get("color") or ())) or None,
            tuple(round(float(c), 3) for c in (dwg.get("fill") or ())) or None,
        )
        closed_hint = bool(dwg.get("closePath"))
        for item in dwg.get("items") or []:
            op = item[0]
            if op == "l":
                emit(_pt(item[1]), _pt(item[2]), path_index, style, closed_hint, op)
            elif op == "re":
                rr = fitz.Rect(item[1])
                c = [(rr.x0, rr.y0), (rr.x1, rr.y0), (rr.x1, rr.y1), (rr.x0, rr.y1)]
                for k in range(4):
                    emit(c[k], c[(k + 1) % 4], path_index, style, True, op)
            elif op == "qu":
                q = item[1]
                c = [_pt(q.ul), _pt(q.ur), _pt(q.lr), _pt(q.ll)]
                for k in range(4):
                    emit(c[k], c[(k + 1) % 4], path_index, style, True, op)
            elif op == "c":
                pts = _sample_cubic(_pt(item[1]), _pt(item[2]), _pt(item[3]), _pt(item[4]))
                for k in range(len(pts) - 1):
                    emit(pts[k], pts[k + 1], path_index, style, closed_hint, op)

    texts: list[dict[str, Any]] = []
    td = page.get_text("dict", clip=clip_unrot)
    for blk in td.get("blocks") or []:
        if blk.get("type") != 0:
            continue
        for line in blk.get("lines") or []:
            for span in line.get("spans") or []:
                t = " ".join(str(span.get("text") or "").split())
                if not t:
                    continue
                bb = fitz.Rect(span["bbox"]) * rot
                bb.normalize()
                cx, cy = (bb.x0 + bb.x1) / 2, (bb.y0 + bb.y1) / 2
                if not (disp.x0 <= cx <= disp.x1 and disp.y0 <= cy <= disp.y1):
                    continue
                texts.append(
                    {
                        "text": t,
                        "bbox": [bb.x0, bb.y0, bb.x1, bb.y1],
                        "cx": cx,
                        "cy": cy,
                        "size": float(span.get("size") or 0.0),
                        "font": span.get("font") or "",
                    }
                )
    doc.close()
    return {
        "pdf": pdf_path,
        "page_index": page_index,
        "page_rotation": page_rotation,
        "disp_rect": disp_rect,
        "segments": segments,
        "texts": texts,
        "invisible_paths": invisible_paths,
        "invisible_items": invisible_items,
        "drop_invisible": drop_invisible,
    }


# --------------------------------------------------------------------------- scale


def characteristic_scale(block: dict[str, Any]) -> dict[str, float]:
    segs = block["segments"]
    texts = block["texts"]
    lens = sorted(s["len"] for s in segs)
    s_geom = statistics.median(lens) if lens else 1.0
    sizes = [t["size"] for t in texts if t["size"] > 0]
    s_text = statistics.median(sizes) if len(sizes) >= 5 else None
    return {
        "s_text": s_text or 0.0,
        "s_geom": s_geom,
        "S": s_text if s_text else s_geom,
        "n_seg": len(segs),
        "n_text": len(texts),
    }


# --------------------------------------------------------------------------- union-find


class _UF:
    def __init__(self, n):
        self.p = list(range(n))

    def find(self, a):
        while self.p[a] != a:
            self.p[a] = self.p[self.p[a]]
            a = self.p[a]
        return a

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def dash_runs(segs, S, p):
    """Generic dashed-line consolidation.

    A CAD exporter writes a dashed line as N short collinear equal-length segments.
    Grouping them by (direction, perpendicular offset) and chaining along the line
    recovers ONE linear object instead of N/6 phantom symbol clusters.  No discipline
    knowledge is used: only collinearity, equal length and regular gaps.
    """
    dash_max = p["dash_max"] * S
    gap_max = p["dash_gap"] * S
    ang_q = p["dash_ang_q"]
    rho_q = max(p["dash_rho_q"] * S, 1e-6)
    buckets: dict[tuple[int, int], list[int]] = {}
    for sgm in segs:
        if sgm["len"] > dash_max:
            continue
        dx = sgm["p1"][0] - sgm["p0"][0]
        dy = sgm["p1"][1] - sgm["p0"][1]
        L = sgm["len"]
        ux, uy = dx / L, dy / L
        if ux < 0 or (abs(ux) < 1e-9 and uy < 0):
            ux, uy = -ux, -uy
        nx, ny = -uy, ux
        rho = sgm["p0"][0] * nx + sgm["p0"][1] * ny
        ang = math.degrees(math.atan2(uy, ux)) % 180.0
        buckets.setdefault((int(round(ang / ang_q)), int(round(rho / rho_q))), []).append(sgm["i"])
    runs = []
    used: set[int] = set()
    for key, members in buckets.items():
        if len(members) < p["dash_min"]:
            continue
        proj = []
        for gi in members:
            sgm = segs[gi]
            dx = sgm["p1"][0] - sgm["p0"][0]
            dy = sgm["p1"][1] - sgm["p0"][1]
            L = sgm["len"]
            ux, uy = dx / L, dy / L
            if ux < 0 or (abs(ux) < 1e-9 and uy < 0):
                ux, uy = -ux, -uy
            t0 = sgm["p0"][0] * ux + sgm["p0"][1] * uy
            t1 = sgm["p1"][0] * ux + sgm["p1"][1] * uy
            proj.append((min(t0, t1), max(t0, t1), gi))
        proj.sort()
        chain = [proj[0]]
        gaps: list[float] = []
        def flush(ch, gs):
            if len(ch) < p["dash_min"]:
                return
            lens = [b - a for a, b, _ in ch]
            if statistics.mean(lens) <= 0:
                return
            if len(lens) > 1 and statistics.pstdev(lens) / statistics.mean(lens) > p["dash_len_cv"]:
                return
            if gs and statistics.mean(gs) > 0 and len(gs) > 1 and statistics.pstdev(gs) / statistics.mean(gs) > p["dash_gap_cv"]:
                return
            runs.append([gi for _a, _b, gi in ch])
        for cur in proj[1:]:
            gap = cur[0] - chain[-1][1]
            if gap <= gap_max:
                gaps.append(max(gap, 0.0))
                chain.append(cur)
            else:
                flush(chain, gaps)
                chain, gaps = [cur], []
        flush(chain, gaps)
    for run in runs:
        used.update(run)
    return runs, used


# --------------------------------------------------------------------------- grouping

DEFAULTS = dict(
    node_tol=0.05,     # endpoint snapping tolerance, in units of S
    k_long=6.0,        # segment longer than k_long * S is a "long run" inside a big component
    alpha=0.60,        # symbol-core merge radius = alpha * S
    diag_max=8.0,      # component/cluster diagonal <= diag_max * S -> symbol scale
    min_seg=2,         # minimum segments in a reported symbol candidate
    label_r=1.6,       # text within label_r * S of an object bbox becomes its label
    quant=0.25,        # motif quantisation, in units of S
    max_members=4000,  # safety cap when re-componentising a huge component
    dash_merge=True,   # consolidate dashed lines drawn as N separate short segments
    dash_max=2.5,      # dash candidate: length <= dash_max * S
    dash_gap=2.0,      # chain when the along-line gap <= dash_gap * S
    dash_min=4,        # a run needs at least this many dashes
    dash_ang_q=2.0,    # direction bucket, degrees
    dash_rho_q=0.15,   # perpendicular-offset bucket, in units of S
    dash_len_cv=0.25,  # dashes must be near-equal in length
    dash_gap_cv=0.40,  # gaps must be near-regular
)


def _seg_bbox(s, pad=0.0):
    x0 = min(s["p0"][0], s["p1"][0]) - pad
    x1 = max(s["p0"][0], s["p1"][0]) + pad
    y0 = min(s["p0"][1], s["p1"][1]) - pad
    y1 = max(s["p0"][1], s["p1"][1]) + pad
    return (x0, y0, x1, y1)


def _bbox_of(members, segs):
    xs = [c for gi in members for c in (segs[gi]["p0"][0], segs[gi]["p1"][0])]
    ys = [c for gi in members for c in (segs[gi]["p0"][1], segs[gi]["p1"][1])]
    return (min(xs), min(ys), max(xs), max(ys))


def _bbox_gap(a, b):
    dx = max(0.0, max(a[0] - b[2], b[0] - a[2]))
    dy = max(0.0, max(a[1] - b[3], b[1] - a[3]))
    return math.hypot(dx, dy)


def _node_ids(indices, segs, tol):
    """Snap endpoints to shared node ids inside a tolerance, using a grid."""
    cell = max(tol, 1e-6)
    grid: dict[tuple[int, int], list[tuple[float, float, int]]] = {}
    ids: dict[tuple[int, int], int] = {}
    nxt = 0
    for gi in indices:
        for k, pnt in enumerate((segs[gi]["p0"], segs[gi]["p1"])):
            i, j = int(pnt[0] // cell), int(pnt[1] // cell)
            found = None
            for di in (-1, 0, 1):
                for dj in (-1, 0, 1):
                    for (qx, qy, qid) in grid.get((i + di, j + dj), ()):
                        if math.hypot(qx - pnt[0], qy - pnt[1]) <= tol:
                            found = qid
                            break
                    if found is not None:
                        break
                if found is not None:
                    break
            if found is None:
                found = nxt
                nxt += 1
                grid.setdefault((i, j), []).append((pnt[0], pnt[1], found))
            ids[(gi, k)] = found
    return ids, nxt


def _components(indices, segs, tol):
    """Connected components of the endpoint graph over `indices`.

    Returns list of (members, n_nodes, n_edges).
    """
    if not indices:
        return []
    ids, n_nodes_total = _node_ids(indices, segs, tol)
    uf = _UF(n_nodes_total)
    for gi in indices:
        uf.union(ids[(gi, 0)], ids[(gi, 1)])
    buckets: dict[int, list[int]] = {}
    for gi in indices:
        buckets.setdefault(uf.find(ids[(gi, 0)]), []).append(gi)
    out = []
    for root, members in buckets.items():
        nodes = set()
        for gi in members:
            nodes.add(ids[(gi, 0)])
            nodes.add(ids[(gi, 1)])
        out.append((members, len(nodes), len(members)))
    return out


def _merge_cores(cores, segs, radius, diag_cap):
    """Merge nearby symbol cores, refusing merges that exceed the symbol scale."""
    boxes = [c["bbox"] for c in cores]
    cell = max(radius, 1e-6)
    grid: dict[tuple[int, int], list[int]] = {}
    for idx, b in enumerate(boxes):
        i0, i1 = int(math.floor((b[0] - radius) / cell)), int(math.floor((b[2] + radius) / cell))
        j0, j1 = int(math.floor((b[1] - radius) / cell)), int(math.floor((b[3] + radius) / cell))
        if (i1 - i0 + 1) * (j1 - j0 + 1) > 4000:
            grid.setdefault((i0, j0), []).append(idx)
            continue
        for i in range(i0, i1 + 1):
            for j in range(j0, j1 + 1):
                grid.setdefault((i, j), []).append(idx)
    uf = _UF(len(cores))
    cur = {i: boxes[i] for i in range(len(cores))}
    pairs = set()
    for (i, j), bucket in grid.items():
        neigh = []
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                neigh.extend(grid.get((i + di, j + dj), ()))
        for a in bucket:
            for b in neigh:
                if b <= a:
                    continue
                pairs.add((a, b))
    for a, b in sorted(pairs):
        ra, rb = uf.find(a), uf.find(b)
        if ra == rb:
            continue
        if _bbox_gap(cur[ra], cur[rb]) > radius:
            continue
        merged = (
            min(cur[ra][0], cur[rb][0]),
            min(cur[ra][1], cur[rb][1]),
            max(cur[ra][2], cur[rb][2]),
            max(cur[ra][3], cur[rb][3]),
        )
        if math.hypot(merged[2] - merged[0], merged[3] - merged[1]) > diag_cap:
            continue
        uf.union(a, b)
        cur[uf.find(a)] = merged
    out: dict[int, list[int]] = {}
    for i in range(len(cores)):
        out.setdefault(uf.find(i), []).append(i)
    return list(out.values())


def _motif_signature(members, segs, quant_abs):
    bb = _bbox_of(members, segs)
    items = []
    for gi in members:
        a = (round((segs[gi]["p0"][0] - bb[0]) / quant_abs), round((segs[gi]["p0"][1] - bb[1]) / quant_abs))
        b = (round((segs[gi]["p1"][0] - bb[0]) / quant_abs), round((segs[gi]["p1"][1] - bb[1]) / quant_abs))
        items.append(tuple(sorted((a, b))))
    items.sort()
    return hashlib.sha1(repr(items).encode()).hexdigest()[:12]


def group_objects(block: dict[str, Any], params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Generic graphical-object candidates.  No discipline semantics anywhere."""
    p = dict(DEFAULTS)
    if params:
        p.update(params)
    segs = block["segments"]
    sc = characteristic_scale(block)
    S = sc["S"] or 1.0
    tol = max(p["node_tol"] * S, 0.02)
    long_thr = p["k_long"] * S
    radius = p["alpha"] * S
    diag_max = p["diag_max"] * S
    quant_abs = max(p["quant"] * S, 1e-6)

    dash_objects: list[dict[str, Any]] = []
    dash_used: set[int] = set()
    if p["dash_merge"]:
        runs, dash_used = dash_runs(segs, S, p)
        for run in runs:
            bb = _bbox_of(run, segs)
            dash_objects.append({"class": "linear_object", "members": run, "bbox": bb,
                                 "diag": math.hypot(bb[2] - bb[0], bb[3] - bb[1]),
                                 "n_seg": len(run), "cycle": False, "dashed": True})

    all_idx = [s["i"] for s in segs if s["i"] not in dash_used]
    comps = _components(all_idx, segs, tol)

    cores: list[dict[str, Any]] = []
    objects: list[dict[str, Any]] = list(dash_objects)
    stats = {"components": len(comps), "small_components": 0, "closed_area": 0,
             "decomposed_components": 0, "long_runs": 0,
             "dash_runs": len(dash_objects), "dash_segments": len(dash_used)}

    for members, n_nodes, n_edges in comps:
        bb = _bbox_of(members, segs)
        diag = math.hypot(bb[2] - bb[0], bb[3] - bb[1])
        has_cycle = n_edges >= n_nodes
        if diag <= diag_max:
            stats["small_components"] += 1
            cores.append({"members": members, "bbox": bb, "cycle": has_cycle})
        elif has_cycle:
            stats["closed_area"] += 1
            objects.append({"class": "closed_area_object", "members": members, "bbox": bb,
                            "diag": diag, "n_seg": len(members), "cycle": True})
        else:
            stats["decomposed_components"] += 1
            long_m = [gi for gi in members if segs[gi]["len"] > long_thr]
            short_m = [gi for gi in members if segs[gi]["len"] <= long_thr]
            if long_m:
                stats["long_runs"] += 1
                for sub, _n, _e in _components(long_m, segs, tol):
                    sbb = _bbox_of(sub, segs)
                    objects.append({"class": "linear_object", "members": sub, "bbox": sbb,
                                    "diag": math.hypot(sbb[2] - sbb[0], sbb[3] - sbb[1]),
                                    "n_seg": len(sub), "cycle": False})
            if short_m and len(short_m) <= p["max_members"]:
                for sub, sn, se in _components(short_m, segs, tol):
                    sbb = _bbox_of(sub, segs)
                    sdiag = math.hypot(sbb[2] - sbb[0], sbb[3] - sbb[1])
                    if sdiag <= diag_max:
                        cores.append({"members": sub, "bbox": sbb, "cycle": se >= sn})
                    else:
                        objects.append({"class": "dense_region", "members": sub, "bbox": sbb,
                                        "diag": sdiag, "n_seg": len(sub), "cycle": se >= sn})
            elif short_m:
                sbb = _bbox_of(short_m, segs)
                objects.append({"class": "dense_region", "members": short_m, "bbox": sbb,
                                "diag": math.hypot(sbb[2] - sbb[0], sbb[3] - sbb[1]),
                                "n_seg": len(short_m), "cycle": False})

    # merge nearby cores into symbol candidates
    for group in _merge_cores(cores, segs, radius, diag_max):
        members = [gi for ci in group for gi in cores[ci]["members"]]
        bb = _bbox_of(members, segs)
        cls = "symbol_candidate" if len(members) >= p["min_seg"] else "stray"
        objects.append({"class": cls, "members": members, "bbox": bb,
                        "diag": math.hypot(bb[2] - bb[0], bb[3] - bb[1]),
                        "n_seg": len(members), "cores": len(group),
                        "cycle": any(cores[ci]["cycle"] for ci in group)})

    sig_counts: dict[str, int] = {}
    for o in objects:
        if o["class"] != "symbol_candidate":
            continue
        o["motif"] = _motif_signature(o["members"], segs, quant_abs)
        sig_counts[o["motif"]] = sig_counts.get(o["motif"], 0) + 1
    for o in objects:
        if o.get("motif") and sig_counts[o["motif"]] >= 2:
            o["repeated"] = True
            o["motif_count"] = sig_counts[o["motif"]]

    lr = p["label_r"] * S
    for o in objects:
        bb = o["bbox"]
        best = None
        for t in block["texts"]:
            g = _bbox_gap(bb, (t["bbox"][0], t["bbox"][1], t["bbox"][2], t["bbox"][3]))
            if g <= lr and (best is None or g < best[0]):
                best = (g, t["text"])
        o["label"] = best[1] if best else None

    counts: dict[str, int] = {}
    for o in objects:
        counts[o["class"]] = counts.get(o["class"], 0) + 1
    return {
        "scale": sc,
        "params": p,
        "long_threshold": long_thr,
        "radius": radius,
        "diag_max": diag_max,
        "stage_stats": stats,
        "objects": objects,
        "counts": counts,
        "motifs": sig_counts,
    }


# --------------------------------------------------------------------------- baseline signal


def endpoint_components(block: dict[str, Any], tol_rel: float = 0.0025) -> int:
    """Baseline: connected components of the endpoint graph at Track A's tolerance."""
    segs = block["segments"]
    r = block["disp_rect"]
    diag = math.hypot(r[2] - r[0], r[3] - r[1])
    tol = tol_rel * diag
    return len(_components([s["i"] for s in segs], segs, tol))


# --------------------------------------------------------- tolerant shape descriptor


def shape_descriptor(members, segs):
    """Translation- and scale-invariant descriptor of one object candidate.

    Exact motif hashes do not survive between two PDF exports of the same sheet
    (different page size => different absolute symbol size => different quantisation).
    This descriptor normalises the object into its own bbox, so the same symbol drawn
    5 % larger still lands in the same place.
    """
    bb = _bbox_of(members, segs)
    w = max(bb[2] - bb[0], 1e-6)
    h = max(bb[3] - bb[1], 1e-6)
    diag = math.hypot(w, h)
    ang = [0.0] * 6
    grid = [0.0] * 16
    total = 0.0
    for gi in members:
        s = segs[gi]
        L = s["len"]
        total += L
        a = math.degrees(math.atan2(s["p1"][1] - s["p0"][1], s["p1"][0] - s["p0"][0])) % 180.0
        ang[min(int(a / 30.0), 5)] += L
        mx = (s["p0"][0] + s["p1"][0]) / 2
        my = (s["p0"][1] + s["p1"][1]) / 2
        gx = min(int((mx - bb[0]) / w * 4), 3)
        gy = min(int((my - bb[1]) / h * 4), 3)
        grid[gy * 4 + gx] += L
    if total <= 0:
        total = 1.0
    vec = [w / (w + h), min(total / diag, 8.0) / 8.0]
    vec += [v / total for v in ang]
    vec += [v / total for v in grid]
    return {"vec": vec, "n_seg": len(members), "diag": diag, "bbox": bb, "total_len": total}


def descriptor_distance(a, b):
    return sum(abs(x - y) for x, y in zip(a["vec"], b["vec"]))


def cluster_by_descriptor(objs, eps=0.35, n_seg_ratio=1.6):
    """Greedy agglomerative motif classes: first object of a class is its exemplar."""
    classes: list[dict[str, Any]] = []
    for o in objs:
        d = o["desc"]
        best = None
        for ci, c in enumerate(classes):
            ex = c["exemplar"]
            r = max(d["n_seg"], ex["n_seg"]) / max(1, min(d["n_seg"], ex["n_seg"]))
            if r > n_seg_ratio:
                continue
            dist = descriptor_distance(d, ex)
            if dist <= eps and (best is None or dist < best[0]):
                best = (dist, ci)
        if best is None:
            classes.append({"exemplar": d, "members": [o]})
        else:
            classes[best[1]]["members"].append(o)
    return classes


def radius_sweep(block, radii_rel, params=None):
    """Cluster ALL segments at a range of proximity radii; report cluster counts.

    Answers: is there a natural, stable object scale, or does the count slide
    continuously from `one cluster per segment` to `one cluster per sheet`?
    """
    p = dict(DEFAULTS)
    if params:
        p.update(params)
    segs = block["segments"]
    S = characteristic_scale(block)["S"] or 1.0
    out = []
    tol = max(p["node_tol"] * S, 0.02)
    base = [{"members": m, "bbox": _bbox_of(m, segs), "cycle": False}
            for m, _n, _e in _components([s["i"] for s in segs], segs, tol)]
    out.append({"radius_rel_S": 0.0, "radius_pt": 0.0, "clusters": len(base),
                "note": "endpoint components, no proximity merging"})
    for rel in radii_rel:
        radius = rel * S
        groups = _merge_cores(base, segs, radius, float("inf"))
        out.append({"radius_rel_S": rel, "radius_pt": round(radius, 2), "clusters": len(groups)})
        if len(groups) <= 1:
            break
    return out
