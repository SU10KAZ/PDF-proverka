"""MODE 1 core — deterministic local graphic diff of two ALREADY PREPARED blocks.

Research only.  Nothing here is wired into production.

Coordinate contract (this is the part that has bitten every previous probe):

* prepared block record carries `page` (1-based), `page_index` (0-based) and
  `coords_norm` — a bbox in the **visual** page space (the same space
  `page.rect` / `page.get_pixmap(clip=...)` live in);
* `page.get_drawings()` / `page.get_text()` return **mediabox** coordinates,
  which differ from the visual space whenever `/Rotate` is non-zero;
* therefore everything is converted **into visual points** once, at extraction
  time (`pt = data_pt * page.rotation_matrix`), and every later stage works in
  visual PDF points only.  No normalized x/w, y/h anywhere — the anisotropy
  defect (O10 of the previous audit) cannot happen by construction.
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

import fitz
import numpy as np

CURVE_STEPS = 8
WHITE_EPS = 0.02


# --------------------------------------------------------------------------
# page cache
# --------------------------------------------------------------------------
class PageCache:
    """One `get_drawings()` per (pdf, page) for the whole process."""

    def __init__(self) -> None:
        self._docs: dict[str, fitz.Document] = {}
        self._pages: dict[tuple[str, int], dict[str, Any]] = {}
        self.stats = {"doc_opens": 0, "page_parses": 0, "hits": 0}

    def doc(self, pdf: str) -> fitz.Document:
        d = self._docs.get(pdf)
        if d is None:
            d = fitz.open(pdf)
            self._docs[pdf] = d
            self.stats["doc_opens"] += 1
        return d

    def page(self, pdf: str, page_index: int) -> dict[str, Any]:
        key = (pdf, page_index)
        got = self._pages.get(key)
        if got is not None:
            self.stats["hits"] += 1
            return got
        doc = self.doc(pdf)
        page = doc[page_index]
        rec = {
            "page": page,
            "rect": [page.rect.x0, page.rect.y0, page.rect.x1, page.rect.y1],
            "rotation": page.rotation,
            "rot_matrix": page.rotation_matrix,
            "drawings": page.get_drawings(extended=True),
            "n_images": len(page.get_images()),
        }
        self._pages[key] = rec
        self.stats["page_parses"] += 1
        return rec

    def close(self) -> None:
        for d in self._docs.values():
            d.close()
        self._docs.clear()
        self._pages.clear()


PAGES = PageCache()


# --------------------------------------------------------------------------
# geometry primitives
# --------------------------------------------------------------------------
@dataclass
class Block:
    pdf: str
    page_index: int
    block_id: str
    bbox_vis: list[float]          # visual PDF points [x0,y0,x1,y1]
    label: str = ""
    polygon_vis: list[list[float]] | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def width(self) -> float:
        return self.bbox_vis[2] - self.bbox_vis[0]

    @property
    def height(self) -> float:
        return self.bbox_vis[3] - self.bbox_vis[1]


def block_from_record(pdf: str, rec: dict[str, Any], page_rect: Sequence[float]) -> Block:
    cn = rec["coords_norm"]
    x0, y0, x1, y1 = page_rect
    w, h = x1 - x0, y1 - y0
    bbox = [x0 + cn[0] * w, y0 + cn[1] * h, x0 + cn[2] * w, y0 + cn[3] * h]
    poly = rec.get("polygon_norm")
    poly_vis = None
    if poly:
        poly_vis = [[x0 + p[0] * w, y0 + p[1] * h] for p in poly]
    return Block(
        pdf=pdf,
        page_index=int(rec["page_index"]),
        block_id=str(rec.get("id") or rec.get("block_id") or ""),
        bbox_vis=bbox,
        label=str(rec.get("ocr_label") or "")[:120],
        polygon_vis=poly_vis,
        meta={k: rec[k] for k in ("page", "source", "file", "size_kb") if k in rec},
    )


def _sample_cubic(p0, p1, p2, p3, steps=CURVE_STEPS):
    pts = []
    for i in range(steps + 1):
        t = i / steps
        mt = 1 - t
        pts.append((
            mt ** 3 * p0[0] + 3 * mt ** 2 * t * p1[0] + 3 * mt * t ** 2 * p2[0] + t ** 3 * p3[0],
            mt ** 3 * p0[1] + 3 * mt ** 2 * t * p1[1] + 3 * mt * t ** 2 * p2[1] + t ** 3 * p3[1],
        ))
    return pts


def _is_white(color) -> bool:
    if color is None:
        return False
    try:
        return all(float(c) >= 1.0 - WHITE_EPS for c in color)
    except Exception:
        return False


def _invisible(d: dict[str, Any]) -> bool:
    """White fill without stroke, or fully transparent paint — paints nothing."""
    typ = d.get("type")
    fill, stroke = d.get("fill"), d.get("color")
    fo = d.get("fill_opacity")
    so = d.get("stroke_opacity")
    if typ == "f":
        if _is_white(fill):
            return True
        if fo is not None and float(fo) <= 0.01:
            return True
        return False
    if typ == "s":
        if _is_white(stroke):
            return True
        if so is not None and float(so) <= 0.01:
            return True
        return False
    if typ == "fs":
        fill_invisible = _is_white(fill) or (fo is not None and float(fo) <= 0.01)
        stroke_invisible = _is_white(stroke) or (so is not None and float(so) <= 0.01)
        return bool(fill_invisible and stroke_invisible)
    return False


def _clip_seg(x0, y0, x1, y1, r):
    """Liang–Barsky clip of a segment to rect r=[X0,Y0,X1,Y1]."""
    dx, dy = x1 - x0, y1 - y0
    t0, t1 = 0.0, 1.0
    for p, q in ((-dx, x0 - r[0]), (dx, r[2] - x0), (-dy, y0 - r[1]), (dy, r[3] - y0)):
        if abs(p) < 1e-12:
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
    if t1 < t0:
        return None
    return (x0 + t0 * dx, y0 + t0 * dy, x0 + t1 * dx, y0 + t1 * dy)


def _rects_touch(a, b) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


def _path_polygons(d: dict[str, Any], rm) -> list[np.ndarray]:
    """Subpaths of a drawing entry as closed polygons in visual points."""
    out: list[np.ndarray] = []
    cur: list[tuple[float, float]] = []

    def flush():
        nonlocal cur
        if len(cur) >= 3:
            out.append(np.asarray(cur, dtype=np.float32))
        cur = []

    for item in d.get("items") or []:
        op = item[0]
        if op == "l":
            a = (float(item[1].x), float(item[1].y))
            b = (float(item[2].x), float(item[2].y))
            if cur and abs(cur[-1][0] - a[0]) < 1e-9 and abs(cur[-1][1] - a[1]) < 1e-9:
                cur.append(b)
            else:
                flush()
                cur = [a, b]
        elif op == "c":
            pts = _sample_cubic((item[1].x, item[1].y), (item[2].x, item[2].y),
                                (item[3].x, item[3].y), (item[4].x, item[4].y))
            if cur and abs(cur[-1][0] - pts[0][0]) < 1e-9 and abs(cur[-1][1] - pts[0][1]) < 1e-9:
                cur.extend(pts[1:])
            else:
                flush()
                cur = list(pts)
        elif op == "re":
            rr = item[1]
            flush()
            out.append(np.asarray([(rr.x0, rr.y0), (rr.x1, rr.y0), (rr.x1, rr.y1), (rr.x0, rr.y1)], dtype=np.float32))
        elif op == "qu":
            q = item[1]
            flush()
            out.append(np.asarray([(q.ul.x, q.ul.y), (q.ur.x, q.ur.y), (q.lr.x, q.lr.y), (q.ll.x, q.ll.y)], dtype=np.float32))
    flush()
    ma, mb, mc, md, me, mf = rm.a, rm.b, rm.c, rm.d, rm.e, rm.f
    res = []
    for poly in out:
        arr = poly.astype(np.float64)
        pv = np.empty_like(poly)
        pv[:, 0] = ma * arr[:, 0] + mc * arr[:, 1] + me
        pv[:, 1] = mb * arr[:, 0] + md * arr[:, 1] + mf
        res.append(pv)
    return res


def flatten_page(pdf: str, page_index: int, drop_invisible: bool = True,
                 apply_scissor: bool = True) -> dict[str, Any]:
    """Flatten the WHOLE page once into visual points; cached per page.

    Done page-wide on purpose: `page.get_drawings()[i]["rect"]` is not reliable
    (measured: 1726 of 764 250 paths on 30 corpus pages report a rect that does
    not contain their own items, one of them by 187 pt).  A per-block rect
    prefilter therefore silently drops real geometry, which shows up later as a
    phantom "removed element".  Here every path is flattened and the block is
    cut from the flattened page by coordinates.
    """
    prec = PAGES.page(pdf, page_index)
    key = (drop_invisible, apply_scissor)
    cached = prec.setdefault("_flat", {}).get(key)
    if cached is not None:
        return cached

    rm = prec["rot_matrix"]
    ma, mb, mc, md, me, mf = rm.a, rm.b, rm.c, rm.d, rm.e, rm.f

    segs: list[tuple[float, float, float, float]] = []
    widths: list[float] = []
    pathid: list[int] = []
    fills: list[dict[str, Any]] = []
    n_invisible = 0
    n_paths_seen = 0
    n_paths_kept = 0
    dropped_segments_invisible = 0

    clip_stack: list[tuple[int, list[np.ndarray]]] = []
    for di, d in enumerate(prec["drawings"]):
        lvl = int(d.get("level") or 0)
        while clip_stack and clip_stack[-1][0] >= lvl:
            clip_stack.pop()
        if d.get("type") == "clip":
            cp = _path_polygons(d, rm)
            if cp:
                clip_stack.append((lvl, cp))
            continue
        n_paths_seen += 1
        invisible = drop_invisible and _invisible(d)
        if invisible:
            n_invisible += 1
        sc = None
        scissor = d.get("scissor") if apply_scissor else None
        if scissor is not None:
            sv = fitz.Rect(scissor) * rm
            sc = [min(sv.x0, sv.x1), min(sv.y0, sv.y1), max(sv.x0, sv.x1), max(sv.y0, sv.y1)]
        w = float(d.get("width") or 0.0)
        subpaths: list[list[tuple[float, float]]] = []
        cur: list[tuple[float, float]] = []

        def flush():
            nonlocal cur
            if len(cur) > 1:
                subpaths.append(cur)
            cur = []

        for item in d.get("items") or []:
            op = item[0]
            if op == "l":
                a = (float(item[1].x), float(item[1].y))
                b = (float(item[2].x), float(item[2].y))
                if cur and abs(cur[-1][0] - a[0]) < 1e-9 and abs(cur[-1][1] - a[1]) < 1e-9:
                    cur.append(b)
                else:
                    flush()
                    cur = [a, b]
            elif op == "c":
                pts = _sample_cubic((item[1].x, item[1].y), (item[2].x, item[2].y),
                                    (item[3].x, item[3].y), (item[4].x, item[4].y))
                if cur and abs(cur[-1][0] - pts[0][0]) < 1e-9 and abs(cur[-1][1] - pts[0][1]) < 1e-9:
                    cur.extend(pts[1:])
                else:
                    flush()
                    cur = list(pts)
            elif op == "re":
                rr = item[1]
                flush()
                subpaths.append([(rr.x0, rr.y0), (rr.x1, rr.y0), (rr.x1, rr.y1), (rr.x0, rr.y1), (rr.x0, rr.y0)])
            elif op == "qu":
                q = item[1]
                flush()
                subpaths.append([(q.ul.x, q.ul.y), (q.ur.x, q.ur.y), (q.lr.x, q.lr.y), (q.ll.x, q.ll.y), (q.ul.x, q.ul.y)])
        flush()

        is_fill = d.get("type") in ("f", "fs") and d.get("fill") is not None and not _is_white(d.get("fill"))
        fill_group: list[np.ndarray] = []
        local = 0
        for sp in subpaths:
            arr = np.asarray(sp, dtype=np.float64)
            vx = ma * arr[:, 0] + mc * arr[:, 1] + me
            vy = mb * arr[:, 0] + md * arr[:, 1] + mf
            if sc is not None:
                np.clip(vx, sc[0], sc[2], out=vx)
                np.clip(vy, sc[1], sc[3], out=vy)
            if is_fill and not invisible and len(sp) >= 3:
                fill_group.append(np.stack([vx, vy], axis=1).astype(np.float32))
            if invisible:
                dropped_segments_invisible += max(0, len(sp) - 1)
                continue
            for i in range(len(vx) - 1):
                x0, y0, x1, y1 = float(vx[i]), float(vy[i]), float(vx[i + 1]), float(vy[i + 1])
                if math.hypot(x1 - x0, y1 - y0) < 1e-9:
                    if d.get("type") == "s":
                        x1, y1 = x0 + 1e-3, y0
                    else:
                        continue
                segs.append((x0, y0, x1, y1))
                widths.append(w)
                pathid.append(di)
                local += 1
        if fill_group and not invisible:
            fills.append({"polys": fill_group, "even_odd": bool(d.get("even_odd")),
                          "clips": [c for _, c in clip_stack] or None})
        if local:
            n_paths_kept += 1

    arr = np.asarray(segs, dtype=np.float32).reshape(-1, 4)
    fill_bbox = np.asarray([[min(float(pv[:, 0].min()) for pv in g["polys"]),
                             min(float(pv[:, 1].min()) for pv in g["polys"]),
                             max(float(pv[:, 0].max()) for pv in g["polys"]),
                             max(float(pv[:, 1].max()) for pv in g["polys"])] for g in fills],
                           dtype=np.float32).reshape(-1, 4)
    out = {
        "segments": arr,
        "widths": np.asarray(widths, dtype=np.float32),
        "path_id": np.asarray(pathid, dtype=np.int32),
        "fills": fills,
        "fill_bbox": fill_bbox,
        "n_paths_seen": n_paths_seen,
        "n_paths_kept": n_paths_kept,
        "n_invisible_paths": n_invisible,
        "segments_dropped_invisible": dropped_segments_invisible,
    }
    prec["_flat"][key] = out
    return out


def _clip_segments(segs: np.ndarray, rect) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Vectorised Liang–Barsky: keep segments intersecting rect, clipped to it."""
    if len(segs) == 0:
        return segs, np.zeros(0, dtype=bool), np.zeros(0, dtype=np.int64)
    x0, y0, x1, y1 = segs[:, 0].astype(np.float64), segs[:, 1].astype(np.float64), \
        segs[:, 2].astype(np.float64), segs[:, 3].astype(np.float64)
    dx, dy = x1 - x0, y1 - y0
    t0 = np.zeros(len(segs)); t1 = np.ones(len(segs))
    alive = np.ones(len(segs), dtype=bool)
    for p, q in ((-dx, x0 - rect[0]), (dx, rect[2] - x0), (-dy, y0 - rect[1]), (dy, rect[3] - y0)):
        par = np.abs(p) < 1e-12
        alive &= ~(par & (q < 0))
        with np.errstate(divide="ignore", invalid="ignore"):
            t = np.where(par, 0.0, q / np.where(par, 1.0, p))
        neg = (~par) & (p < 0)
        pos = (~par) & (p > 0)
        t0 = np.where(neg, np.maximum(t0, t), t0)
        t1 = np.where(pos, np.minimum(t1, t), t1)
    alive &= (t1 >= t0)
    idx = np.nonzero(alive)[0]
    if len(idx) == 0:
        return np.zeros((0, 4), np.float32), np.zeros(0, dtype=bool), idx
    a0, a1 = t0[idx], t1[idx]
    out = np.empty((len(idx), 4), dtype=np.float32)
    out[:, 0] = x0[idx] + a0 * dx[idx]
    out[:, 1] = y0[idx] + a0 * dy[idx]
    out[:, 2] = x0[idx] + a1 * dx[idx]
    out[:, 3] = y0[idx] + a1 * dy[idx]
    clipped = (a0 > 1e-9) | (a1 < 1 - 1e-9)
    return out, clipped, idx


def extract_ink(
    block: Block,
    margin_pt: float = 0.0,
    drop_invisible: bool = True,
    apply_scissor: bool = True,
) -> dict[str, Any]:
    """Visible vector geometry of a prepared block, cut from the flattened page."""
    prec = PAGES.page(block.pdf, block.page_index)
    flat = flatten_page(block.pdf, block.page_index, drop_invisible, apply_scissor)
    bx = block.bbox_vis
    rect = [bx[0] - margin_pt, bx[1] - margin_pt, bx[2] + margin_pt, bx[3] + margin_pt]
    segs, clipped, idx = _clip_segments(flat["segments"], rect)
    fb = flat["fill_bbox"]
    if len(fb):
        keep = ~((fb[:, 2] < rect[0]) | (fb[:, 0] > rect[2]) | (fb[:, 3] < rect[1]) | (fb[:, 1] > rect[3]))
        fills = [flat["fills"][i] for i in np.nonzero(keep)[0]]
    else:
        fills = []
    return {
        "segments": segs,
        "fills": fills,
        "widths": flat["widths"][idx] if len(idx) else np.zeros(0, np.float32),
        "path_id": flat["path_id"][idx] if len(idx) else np.zeros(0, np.int32),
        "clipped": clipped,
        "rect": rect,
        "bbox_vis": bx,
        "page_rotation": prec["rotation"],
        "page_rect": prec["rect"],
        "n_paths_seen": flat["n_paths_seen"],
        "n_paths_kept": flat["n_paths_kept"],
        "n_invisible_paths": flat["n_invisible_paths"],
        "segments_dropped_invisible": flat["segments_dropped_invisible"],
        "n_page_images": prec["n_images"],
    }


def ink_length(segs: np.ndarray) -> float:
    if len(segs) == 0:
        return 0.0
    d = np.hypot(segs[:, 2] - segs[:, 0], segs[:, 3] - segs[:, 1])
    return float(d.sum())


def text_spans(block: Block, margin_pt: float = 0.0) -> list[dict[str, Any]]:
    """Text spans of the block in visual points (used only as masks/anchors)."""
    prec = PAGES.page(block.pdf, block.page_index)
    page = prec["page"]
    rm = prec["rot_matrix"]
    bx = block.bbox_vis
    rect = fitz.Rect(bx[0] - margin_pt, bx[1] - margin_pt, bx[2] + margin_pt, bx[3] + margin_pt)
    data_rect = rect * page.derotation_matrix
    out = []
    data = page.get_text("dict", clip=data_rect)
    for b in data.get("blocks") or []:
        if b.get("type") != 0:
            continue
        for line in b.get("lines") or []:
            for span in line.get("spans") or []:
                t = str(span.get("text") or "").strip()
                if not t:
                    continue
                r = fitz.Rect(span["bbox"]) * rm
                out.append({
                    "text": t,
                    "bbox": [min(r.x0, r.x1), min(r.y0, r.y1), max(r.x0, r.x1), max(r.y0, r.y1)],
                    "size": float(span.get("size") or 0.0),
                    "font": span.get("font") or "",
                })
    return out


# --------------------------------------------------------------------------
# rasterization of vector ink into a physical grid
# --------------------------------------------------------------------------
def rasterize(
    segs: np.ndarray,
    widths: np.ndarray | None,
    frame: Sequence[float],
    cell_pt: float,
    min_width_pt: float = 0.0,
    fills: list | None = None,
) -> np.ndarray:
    """Binary ink mask of segments (and filled paths) inside `frame`."""
    import cv2

    x0, y0, x1, y1 = frame
    w = max(1, int(math.ceil((x1 - x0) / cell_pt)))
    h = max(1, int(math.ceil((y1 - y0) / cell_pt)))
    canvas = np.zeros((h, w), dtype=np.uint8)

    if fills:
        simple: list[np.ndarray] = []          # one subpath, no clip -> batch
        complex_groups: list[tuple] = []
        for grp in fills:
            polys = grp["polys"] if isinstance(grp, dict) else [grp]
            even_odd = grp.get("even_odd", True) if isinstance(grp, dict) else True
            clips = grp.get("clips") if isinstance(grp, dict) else None
            ipolys = []
            for pv in polys:
                q = np.empty((len(pv), 2), dtype=np.float64)
                q[:, 0] = (pv[:, 0] - x0) / cell_pt
                q[:, 1] = (pv[:, 1] - y0) / cell_pt
                ipolys.append(np.round(q).astype(np.int32))
            if not ipolys:
                continue
            if len(ipolys) == 1 and not clips:
                simple.append(ipolys[0])
            else:
                complex_groups.append((ipolys, even_odd, clips))
        for i in range(0, len(simple), 500):
            cv2.fillPoly(canvas, simple[i:i + 500], 1)
        for ipolys, even_odd, clips in complex_groups:
            allp = np.concatenate(ipolys, axis=0)
            bx0 = max(0, int(allp[:, 0].min()) - 1); by0 = max(0, int(allp[:, 1].min()) - 1)
            bx1 = min(w, int(allp[:, 0].max()) + 2); by1 = min(h, int(allp[:, 1].max()) + 2)
            if bx1 <= bx0 or by1 <= by0:
                continue
            sw, sh = bx1 - bx0, by1 - by0
            if sw * sh > 40_000_000:
                continue
            tmp = np.zeros((sh, sw), np.uint8)
            shift = np.asarray([bx0, by0], dtype=np.int32)
            if len(ipolys) == 1:
                cv2.fillPoly(tmp, [ipolys[0] - shift], 1)
            else:
                one = np.zeros((sh, sw), np.uint8)
                for ip in ipolys:              # even-odd: holes must stay holes
                    one[:] = 0
                    cv2.fillPoly(one, [ip - shift], 1)
                    np.bitwise_xor(tmp, one, out=tmp)
            if clips:
                one = np.zeros((sh, sw), np.uint8)
                for cpolys in clips:
                    one[:] = 0
                    ic = []
                    for pv in cpolys:
                        q = np.empty((len(pv), 2), dtype=np.float64)
                        q[:, 0] = (pv[:, 0] - x0) / cell_pt - bx0
                        q[:, 1] = (pv[:, 1] - y0) / cell_pt - by0
                        ic.append(np.round(q).astype(np.int32))
                    cv2.fillPoly(one, ic, 1)
                    np.bitwise_and(tmp, one, out=tmp)
            np.bitwise_or(canvas[by0:by1, bx0:bx1], tmp, out=canvas[by0:by1, bx0:bx1])

    if len(segs) == 0:
        return canvas

    pts = np.empty((len(segs), 2, 2), dtype=np.int32)
    pts[:, 0, 0] = np.round((segs[:, 0] - x0) / cell_pt)
    pts[:, 0, 1] = np.round((segs[:, 1] - y0) / cell_pt)
    pts[:, 1, 0] = np.round((segs[:, 2] - x0) / cell_pt)
    pts[:, 1, 1] = np.round((segs[:, 3] - y0) / cell_pt)
    if widths is None:
        thick = np.ones(len(segs), dtype=np.int32)
    else:
        thick = np.maximum(1, np.round(np.maximum(widths, min_width_pt) / cell_pt)).astype(np.int32)
    for t in np.unique(thick):
        idx = np.nonzero(thick == t)[0]
        sel = pts[idx]
        cv2.polylines(canvas, [sel[i] for i in range(len(sel))], False, 1, int(t), lineType=cv2.LINE_8)
    return canvas


def render_gray(block: Block, cell_pt: float, margin_pt: float = 0.0) -> np.ndarray:
    """Render the block region to grayscale at exactly `cell_pt` per pixel."""
    prec = PAGES.page(block.pdf, block.page_index)
    page = prec["page"]
    bx = block.bbox_vis
    rect = fitz.Rect(bx[0] - margin_pt, bx[1] - margin_pt, bx[2] + margin_pt, bx[3] + margin_pt)
    zoom = 1.0 / cell_pt
    pm = page.get_pixmap(clip=rect, matrix=fitz.Matrix(zoom, zoom), colorspace=fitz.csGRAY, alpha=False)
    return np.frombuffer(pm.samples, dtype=np.uint8).reshape(pm.height, pm.width).copy()


def sha_block(block: Block) -> str:
    h = hashlib.sha1()
    h.update(block.pdf.encode())
    h.update(str(block.page_index).encode())
    h.update(",".join(f"{v:.3f}" for v in block.bbox_vis).encode())
    return h.hexdigest()[:12]
