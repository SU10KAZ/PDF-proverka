# -*- coding: utf-8 -*-
"""VECTOR 0.3 · foundation module — the ONLY sanctioned way to read a prepared block.

Research code.  Nothing outside
``experiments/stage_comparison_vector_objects_v03_opus/`` is written or modified.

Why this module exists
----------------------
Track A (v0.1) and the v0.2 probes each grew their own extractor.  The two
disagreed on the /Rotate correction, which silently invalidated a benchmark
(0 of 217 094 points intersected on one pair).  Every v0.3 probe reads blocks
through *this* file and no other.

Contract implemented here is the real upstream contract found in the repo:

    projects_v2/objects/<OBJ>/disciplines/<DISC>/documents/<DOC>/versions/<VER>/
        02_work/result.json      pages[].blocks[] with coords_px + page px size
        02_work/document.pdf

Coordinate systems (three of them — keep them straight)
------------------------------------------------------
* ``px``      — pixel space of ``result.json`` (page_px_w × page_px_h), origin top-left.
                This is the space ``coords_px`` lives in.
* ``display`` — PDF points *after* /Rotate is applied.  ``page.rect`` is in this
                space and ``page.get_pixmap(clip=...)`` expects a clip in it.
                Production's ``crop_from_pdf`` computes its clip here.
* ``page``    — PDF points in the page's *own* (unrotated) space.  This is what
                ``page.get_drawings()`` and ``page.get_text()`` return.

``px`` → ``display`` is a pure scale (production formula).  ``display`` → ``page``
is ``page.derotation_matrix``; back is ``page.rotation_matrix``.

Everything this module *emits* (segments, text boxes, image boxes) is in
**display** points, i.e. the same orientation a human saw in the crop PNG.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import statistics
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import fitz  # PyMuPDF

__all__ = [
    "PreparedBlock",
    "Frame",
    "BlockExtract",
    "iter_prepared_blocks",
    "block_frame",
    "extract_block",
    "render_block",
    "normalize",
    "ink_verdict",
    "INK_RULES",
    "open_doc",
    "clear_caches",
]

CURVE_STEPS = 6
_WHITE_EPS = 0.98

# ---------------------------------------------------------------- caches (per process)

_DOC_CACHE: dict[str, fitz.Document] = {}
_RECT_CACHE: dict[str, list[tuple[float, float]]] = {}
_DRAW_CACHE: dict[tuple[str, int], list] = {}
_SHA_CACHE: dict[str, str] = {}
_PDF_ROT_CACHE: dict[tuple[str, int], int] = {}


def open_doc(pdf_path: str | Path) -> fitz.Document:
    """Cached ``fitz.open``.  Documents are read-only here; never saved."""
    key = str(pdf_path)
    doc = _DOC_CACHE.get(key)
    if doc is None or doc.is_closed:
        doc = fitz.open(key)
        _DOC_CACHE[key] = doc
    return doc


def clear_caches() -> None:
    for d in list(_DOC_CACHE.values()):
        try:
            d.close()
        except Exception:
            pass
    _DOC_CACHE.clear()
    _RECT_CACHE.clear()
    _DRAW_CACHE.clear()
    _PDF_ROT_CACHE.clear()


def _page_rects(pdf_path: str) -> list[tuple[float, float]]:
    """Cached (width, height) of every page rect (displayed space)."""
    if pdf_path in _RECT_CACHE:
        return _RECT_CACHE[pdf_path]
    try:
        doc = open_doc(pdf_path)
        rects = [(float(doc[i].rect.width), float(doc[i].rect.height)) for i in range(doc.page_count)]
    except Exception:
        rects = []
    _RECT_CACHE[pdf_path] = rects
    return rects


def pdf_sha256(pdf_path: str | Path, _limit: int = 0) -> str:
    key = str(pdf_path)
    if key in _SHA_CACHE:
        return _SHA_CACHE[key]
    h = hashlib.sha256()
    with open(key, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    _SHA_CACHE[key] = h.hexdigest()
    return _SHA_CACHE[key]


# ---------------------------------------------------------------- prepared block

@dataclass
class PreparedBlock:
    block_id: str
    page_number: int          # 1-based, as in result.json
    page_index: int           # 0-based index into the PDF — ALWAYS page_number - 1 (see below)
    page_index_field: Optional[int]   # the raw blocks[].page_index value from result.json
    page_index_conflict: bool         # True when that raw value != page_number - 1
    page_aspect_ok: Optional[bool]    # page px aspect (result.json) == PDF page rect aspect
    coords_px: tuple[float, float, float, float]
    coords_norm: Optional[tuple[float, float, float, float]]
    page_px_w: int
    page_px_h: int
    rotation: int             # /Rotate of the page (result.json, else the PDF)
    rotation_source: str      # "result_json" | "pdf"
    block_type: str
    shape_type: Optional[str]
    category_code: Optional[str]
    ocr_text: str
    crop_url: Optional[str]
    polygon_points: Optional[list[list[float]]]
    pdf_path: str
    result_json: str
    doc_id: str
    version: str
    discipline: str
    obj_id: str

    @property
    def key(self) -> str:
        return f"{self.doc_id}|{self.version}|{self.block_id}"

    def as_dict(self) -> dict:
        return asdict(self)


_PATH_RE = re.compile(
    r"objects/(?P<obj>[^/]+)/disciplines/(?P<disc>[^/]+)/documents/(?P<doc>[^/]+)/"
    r"versions/(?P<ver>[^/]+)/02_work/result\.json$"
)


def _provenance_from_path(result_json_path: str) -> dict[str, str]:
    m = _PATH_RE.search(str(result_json_path).replace(os.sep, "/"))
    if m:
        return {
            "obj_id": m.group("obj"),
            "discipline": m.group("disc"),
            "doc_id": m.group("doc"),
            "version": m.group("ver"),
        }
    p = Path(result_json_path)
    return {"obj_id": "?", "discipline": "?", "doc_id": p.parent.parent.name, "version": "?"}


def _as_list4(v) -> Optional[tuple[float, float, float, float]]:
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except Exception:
            return None
    if isinstance(v, (list, tuple)) and len(v) >= 4:
        try:
            return (float(v[0]), float(v[1]), float(v[2]), float(v[3]))
        except Exception:
            return None
    return None


def _as_points(v) -> Optional[list[list[float]]]:
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except Exception:
            try:
                v = json.loads(v.replace("'", '"'))
            except Exception:
                return None
    if isinstance(v, (list, tuple)) and v and isinstance(v[0], (list, tuple)):
        out = []
        for p in v:
            try:
                out.append([float(p[0]), float(p[1])])
            except Exception:
                return None
        return out
    return None


def _pdf_rotation(pdf_path: str, page_index: int) -> Optional[int]:
    key = (pdf_path, page_index)
    if key in _PDF_ROT_CACHE:
        return _PDF_ROT_CACHE[key]
    try:
        doc = open_doc(pdf_path)
        if page_index < 0 or page_index >= doc.page_count:
            return None
        rot = int(doc[page_index].rotation) % 360
    except Exception:
        return None
    _PDF_ROT_CACHE[key] = rot
    return rot


def iter_prepared_blocks(
    result_json_path: str | Path,
    *,
    graphic_only: bool = True,
    resolve_rotation_from_pdf: bool = True,
) -> list[PreparedBlock]:
    """Prepared blocks of one document version, per the upstream contract.

    ``graphic_only=True`` keeps only ``block_type == "image"`` — the definition of a
    prepared GRAPHIC block in the brief.  Rotation comes from ``pages[].rotation``
    when present (legacy files omit it) and otherwise from the PDF itself.
    """
    rj = Path(result_json_path)
    prov = _provenance_from_path(str(rj))
    pdf_path = str(rj.parent / "document.pdf")
    if not Path(pdf_path).exists():
        alt = str(rj.parent / (rj.stem + ".pdf"))
        pdf_path = alt if Path(alt).exists() else pdf_path

    with open(rj, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    out: list[PreparedBlock] = []
    for page in data.get("pages") or []:
        try:
            page_number = int(page.get("page_number"))
        except Exception:
            continue
        page_px_w = int(page.get("width") or 0)
        page_px_h = int(page.get("height") or 0)
        rot_raw = page.get("rotation")
        if rot_raw is None or rot_raw == "":
            rot = None
        else:
            try:
                rot = int(rot_raw) % 360
            except Exception:
                rot = None
        rot_src = "result_json" if rot is not None else "pdf"

        rects = _page_rects(pdf_path) if Path(pdf_path).exists() else []
        n_pdf_pages = len(rects)
        aspect_ok: Optional[bool] = None
        pi_check = page_number - 1
        if rects and 0 <= pi_check < n_pdf_pages and page_px_w and page_px_h:
            w, h = rects[pi_check]
            if h > 0:
                aspect_ok = abs((w / h) - (page_px_w / page_px_h)) / (page_px_w / page_px_h) <= 0.01

        for b in page.get("blocks") or []:
            btype = str(b.get("block_type") or "")
            if graphic_only and btype != "image":
                continue
            coords = _as_list4(b.get("coords_px"))
            if coords is None:
                continue
            # ---------------------------------------------------------------
            # page identity.  result.json carries blocks[].page_index, documented
            # 0-based.  Measured on the whole corpus (artifacts/fnd_page_index.json):
            # it disagrees with page_number-1 on 41.3 % of pages / 48.7 % of blocks,
            # and where the page aspect ratio can arbitrate, page_number-1 wins
            # 1485 pages to 0.  Production (crop_from_pdf) uses page_number-1.
            # Therefore page_index is IGNORED as a page pointer and only reported.
            # ---------------------------------------------------------------
            try:
                pi_field = int(b.get("page_index"))
            except Exception:
                pi_field = None
            page_index = page_number - 1
            if page_index < 0:
                continue
            if page_index >= n_pdf_pages > 0:
                continue
            r = rot
            if r is None and resolve_rotation_from_pdf:
                r = _pdf_rotation(pdf_path, page_index)
            out.append(
                PreparedBlock(
                    block_id=str(b.get("id") or ""),
                    page_number=page_number,
                    page_index=page_index,
                    page_index_field=pi_field,
                    page_index_conflict=(pi_field is not None and pi_field != page_index),
                    page_aspect_ok=aspect_ok,
                    coords_px=coords,
                    coords_norm=_as_list4(b.get("coords_norm")),
                    page_px_w=page_px_w,
                    page_px_h=page_px_h,
                    rotation=int(r or 0),
                    rotation_source=rot_src if rot is not None else ("pdf" if r is not None else "missing"),
                    block_type=btype,
                    shape_type=(b.get("shape_type") or None),
                    category_code=(b.get("category_code") or None),
                    ocr_text=str(b.get("ocr_text") or ""),
                    crop_url=(b.get("crop_url") or None),
                    polygon_points=_as_points(b.get("polygon_points")),
                    pdf_path=pdf_path,
                    result_json=str(rj),
                    doc_id=prov["doc_id"],
                    version=prov["version"],
                    discipline=prov["discipline"],
                    obj_id=prov["obj_id"],
                )
            )
    return out


# ---------------------------------------------------------------- frame

@dataclass
class Frame:
    clip_display: fitz.Rect       # region in the ROTATED (displayed) space  -> get_pixmap
    clip_page: fitz.Rect          # same region in the page's OWN space      -> get_drawings/get_text
    to_page: fitz.Matrix          # display -> page      (page.derotation_matrix)
    to_display: fitz.Matrix       # page    -> display   (page.rotation_matrix)
    rotation: int
    page_rect: fitz.Rect          # page.rect (displayed)
    page_index: int
    scale_x: float                # px -> display point scale
    scale_y: float
    clamped: bool                 # clip_display had to be clamped to the page
    out_of_page: bool             # coords_px reached outside the page before clamping

    @property
    def w(self) -> float:
        return float(self.clip_display.width)

    @property
    def h(self) -> float:
        return float(self.clip_display.height)


def block_frame(
    pdf_path: str | Path,
    page_index: int,
    coords_px: Sequence[float],
    page_px_w: float,
    page_px_h: float,
) -> Frame:
    """The production clip, plus its twin in the page's own coordinate system.

    ``clip_display`` reproduces ``crop_blocks/blocks.py::crop_from_pdf`` verbatim
    (scale by page.rect / result.json px size).  ``clip_page`` is that same physical
    region expressed where ``get_drawings()`` speaks.
    """
    doc = open_doc(pdf_path)
    page = doc[page_index]
    prect = page.rect
    if not page_px_w or not page_px_h:
        raise ValueError("page px size missing in result.json")
    scale_x = prect.width / float(page_px_w)
    scale_y = prect.height / float(page_px_h)
    x1, y1, x2, y2 = [float(v) for v in coords_px[:4]]
    clip = fitz.Rect(
        min(x1, x2) * scale_x, min(y1, y2) * scale_y,
        max(x1, x2) * scale_x, max(y1, y2) * scale_y,
    )
    out_of_page = not fitz.Rect(prect).contains(clip)
    clipped = fitz.Rect(clip) & prect
    clamped = bool(out_of_page and clipped.is_valid and not clipped.is_empty)
    if clipped.is_valid and not clipped.is_empty:
        clip_display = clipped
    else:
        clip_display = clip
    derot = page.derotation_matrix
    rot = page.rotation_matrix
    clip_page = fitz.Rect(clip_display) * derot
    clip_page.normalize()
    return Frame(
        clip_display=clip_display,
        clip_page=clip_page,
        to_page=derot,
        to_display=rot,
        rotation=int(page.rotation) % 360,
        page_rect=fitz.Rect(prect),
        page_index=page_index,
        scale_x=scale_x,
        scale_y=scale_y,
        clamped=clamped,
        out_of_page=out_of_page,
    )


def naive_frame_page_rect(
    pdf_path: str | Path,
    page_index: int,
    coords_px: Sequence[float],
    page_px_w: float,
    page_px_h: float,
) -> fitz.Rect:
    """DELIBERATELY WRONG control: the production clip fed straight to get_drawings().

    Kept only so probes can measure how bad the un-derotated path is (F1b).
    Never use it to produce data.
    """
    f = block_frame(pdf_path, page_index, coords_px, page_px_w, page_px_h)
    return fitz.Rect(f.clip_display)


# ---------------------------------------------------------------- ink rules

INK_RULES: dict[str, str] = {
    "outside_clip": "path bbox does not intersect the block clip — nothing of it is in the crop",
    "zero_opacity": "fill_opacity<=0.01 and stroke_opacity<=0.01 — the painter is fully transparent",
    "white_fill_no_stroke": "fill is white and there is no stroke colour — a knockout box: it "
                            "produces 4 phantom rectangle edges but no visible edge on white paper",
    "white_stroke_no_fill": "stroke is white and there is no fill — the stroke paints paper on paper",
    "white_fill_white_stroke": "both fill and stroke are white — same as above",
    "degenerate": "path rect has zero width and zero height",
}


def _rect_overlaps(a, b, eps: float = 1e-9) -> bool:
    """Tolerant rectangle overlap that is correct for DEGENERATE rectangles.

    ``fitz.Rect.intersects()`` returns False whenever *either* rectangle is
    empty, and the bbox of a purely horizontal or vertical single-line path is
    empty by construction (zero height or zero width).  Such paths are 45.1 % of
    all paths in this corpus, so using ``intersects()`` as a gate silently drops
    them (measured: median 35.6 % of stroke length per block, >50 % on 28.6 % of
    blocks).  Interval overlap on both axes is closed, so it handles the
    degenerate case correctly.
    """
    try:
        ax0, ay0, ax1, ay1 = float(a.x0), float(a.y0), float(a.x1), float(a.y1)
        bx0, by0, bx1, by1 = float(b.x0), float(b.y0), float(b.x1), float(b.y1)
    except Exception:
        return True  # unreadable bbox -> do not gate, the clipper decides
    if ax0 > ax1:
        ax0, ax1 = ax1, ax0
    if ay0 > ay1:
        ay0, ay1 = ay1, ay0
    if bx0 > bx1:
        bx0, bx1 = bx1, bx0
    if by0 > by1:
        by0, by1 = by1, by0
    return (ax0 <= bx1 + eps and bx0 <= ax1 + eps
            and ay0 <= by1 + eps and by0 <= ay1 + eps)


def _is_white(c) -> bool:
    return c is not None and len(c) >= 3 and all(float(v) >= _WHITE_EPS for v in c[:3])


def ink_verdict(dwg: dict, clip_page: Optional[fitz.Rect] = None) -> Optional[str]:
    """Return the ink rule that kills this path, or None if it paints something.

    Rules are *provable* (each one is checked empirically in ``fnd_ink.json`` by
    measuring the raster edge response along the path outline).  This is NOT a
    generic foreground/background filter — the brief forbids that.
    """
    r = dwg.get("rect")
    if r is not None:
        rr = fitz.Rect(r)
        if clip_page is not None and not _rect_overlaps(rr, clip_page):
            return "outside_clip"
        if rr.width <= 0 and rr.height <= 0:
            return "degenerate"
    fill = dwg.get("fill")
    color = dwg.get("color")
    fo = dwg.get("fill_opacity")
    so = dwg.get("stroke_opacity")
    fo_zero = fo is not None and float(fo) <= 0.01
    so_zero = so is not None and float(so) <= 0.01
    has_fill = fill is not None
    has_stroke = color is not None
    if (not has_fill or fo_zero) and (not has_stroke or so_zero) :
        if fo_zero or so_zero:
            return "zero_opacity"
    if has_fill and has_stroke and _is_white(fill) and _is_white(color):
        return "white_fill_white_stroke"
    if has_fill and not has_stroke and _is_white(fill):
        return "white_fill_no_stroke"
    if has_stroke and not has_fill and _is_white(color):
        return "white_stroke_no_fill"
    return None


# ---------------------------------------------------------------- geometry helpers

def _pt(v) -> tuple[float, float]:
    if isinstance(v, fitz.Point):
        return (float(v.x), float(v.y))
    return (float(v[0]), float(v[1]))


def _sample_cubic(p0, p1, p2, p3, steps=CURVE_STEPS):
    out = []
    for i in range(steps + 1):
        t = i / steps
        u = 1 - t
        out.append((
            u ** 3 * p0[0] + 3 * u ** 2 * t * p1[0] + 3 * u * t ** 2 * p2[0] + t ** 3 * p3[0],
            u ** 3 * p0[1] + 3 * u ** 2 * t * p1[1] + 3 * u * t ** 2 * p2[1] + t ** 3 * p3[1],
        ))
    return out


def _clip_line(p0, p1, rect) -> Optional[tuple[tuple[float, float], tuple[float, float], bool]]:
    """Liang-Barsky.  Returns (a, b, was_clipped)."""
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
    was = (t0 > 1e-9) or (t1 < 1 - 1e-9)
    return ((px + t0 * dx, py + t0 * dy), (px + t1 * dx, py + t1 * dy), was)


# ---------------------------------------------------------------- extract

@dataclass
class BlockExtract:
    segments: list[dict]                # display points, clipped to the block
    segments_raw_count: int             # segments produced before the ink filter
    inked_segments_count: int
    invisible_dropped: int              # segments removed by the ink filter
    invisible_by_rule: dict[str, int]
    paths_total: int
    paths_invisible: int
    paths_outside_clip: int
    texts: list[dict]                   # printed LINES (not spans), display points
    images: list[dict]                  # raster inserts, display points
    curves_flattened_count: int
    clipped_at_border_flags: dict[str, Any]
    quality: dict[str, Any]
    char_scale: dict[str, float]
    frame: dict[str, Any]
    provenance: dict[str, Any]

    @property
    def S(self) -> float:
        return float(self.char_scale.get("S") or 1.0)


_GARBLE_RE = re.compile(r"[�\x00-\x08\x0B\x0C\x0E-\x1F]")


def _garbled_ratio(text: str) -> float:
    if not text:
        return 0.0
    bad = len(_GARBLE_RE.findall(text))
    # CID / notdef style output from broken embedded fonts
    bad += text.count("�")
    return bad / max(1, len(text))


def extract_block(
    pdf_path: str | Path,
    page_index: int,
    coords_px: Sequence[float],
    page_px_w: float,
    page_px_h: float,
    *,
    drop_invisible: bool = True,
    curve_steps: int = CURVE_STEPS,
    keep_dropped_segments: bool = False,
    frame: Optional[Frame] = None,
    naive_rotation: bool = False,
) -> BlockExtract:
    """Read one prepared block: inked segments, printed lines, raster inserts.

    All emitted coordinates are **display** PDF points (the orientation of the crop
    PNG a human reviewed).  ``naive_rotation=True`` reproduces the known-bad path
    (production clip handed to ``get_drawings``/``get_text`` without derotation) and
    exists only so probes can measure the damage.
    """
    doc = open_doc(pdf_path)
    page = doc[page_index]
    fr = frame or block_frame(pdf_path, page_index, coords_px, page_px_w, page_px_h)

    if naive_rotation:
        read_rect = fitz.Rect(fr.clip_display)     # WRONG on /Rotate != 0
        fwd = fitz.Identity
    else:
        read_rect = fitz.Rect(fr.clip_page)
        fwd = fr.to_display

    out_rect_r = fitz.Rect(fr.clip_display) if not naive_rotation else fitz.Rect(fr.clip_display)
    out_rect = [out_rect_r.x0, out_rect_r.y0, out_rect_r.x1, out_rect_r.y1]

    key = (str(pdf_path), page_index)
    if key not in _DRAW_CACHE:
        _DRAW_CACHE[key] = page.get_drawings()
    drawings = _DRAW_CACHE[key]

    segments: list[dict] = []
    dropped: list[dict] = []
    raw_count = 0
    curves = 0
    invisible_by_rule: dict[str, int] = {}
    paths_invisible = 0
    paths_outside_clip = 0
    border_clipped = 0

    def emit(a, b, path_index, style, closed_hint, op, rule, sink):
        nonlocal raw_count, border_clipped
        A = fitz.Point(*a) * fwd
        B = fitz.Point(*b) * fwd
        res = _clip_line((A.x, A.y), (B.x, B.y), out_rect)
        if res is None:
            return
        (sx, sy), (ex, ey), was_clipped = res
        length = math.hypot(ex - sx, ey - sy)
        if length <= 1e-6:
            return
        raw_count += 1
        if was_clipped:
            border_clipped += 1
        sink.append({
            "i": len(sink),
            "p0": (sx, sy), "p1": (ex, ey),
            "len": length,
            "path": path_index,
            "op": op,
            "closed": closed_hint,
            "w": style[0], "color": style[1], "fill": style[2],
            "ink_rule": rule,
            "border": was_clipped,
        })

    for path_index, dwg in enumerate(drawings):
        r = dwg.get("rect")
        if r is not None and not _rect_overlaps(fitz.Rect(r), read_rect):
            paths_outside_clip += 1
            invisible_by_rule["outside_clip"] = invisible_by_rule.get("outside_clip", 0) + 1
            continue
        rule = ink_verdict(dwg, read_rect)
        if rule:
            paths_invisible += 1
            invisible_by_rule[rule] = invisible_by_rule.get(rule, 0) + 1
        sink = segments
        if rule and drop_invisible:
            sink = dropped
        style = (
            round(float(dwg.get("width") or 0.0), 3),
            tuple(round(float(c), 3) for c in (dwg.get("color") or ())) or None,
            tuple(round(float(c), 3) for c in (dwg.get("fill") or ())) or None,
        )
        closed_hint = bool(dwg.get("closePath"))
        for item in dwg.get("items") or []:
            op = item[0]
            if op == "l":
                emit(_pt(item[1]), _pt(item[2]), path_index, style, closed_hint, op, rule, sink)
            elif op == "re":
                rr = fitz.Rect(item[1])
                c = [(rr.x0, rr.y0), (rr.x1, rr.y0), (rr.x1, rr.y1), (rr.x0, rr.y1)]
                for k in range(4):
                    emit(c[k], c[(k + 1) % 4], path_index, style, True, op, rule, sink)
            elif op == "qu":
                q = item[1]
                c = [_pt(q.ul), _pt(q.ur), _pt(q.lr), _pt(q.ll)]
                for k in range(4):
                    emit(c[k], c[(k + 1) % 4], path_index, style, True, op, rule, sink)
            elif op == "c":
                curves += 1
                pts = _sample_cubic(_pt(item[1]), _pt(item[2]), _pt(item[3]), _pt(item[4]),
                                    steps=curve_steps)
                for k in range(len(pts) - 1):
                    emit(pts[k], pts[k + 1], path_index, style, closed_hint, op, rule, sink)

    for n, s in enumerate(segments):
        s["i"] = n

    # ---- printed LINES (spans merged per line, as the contract asks) -------------
    texts: list[dict] = []
    raw_spans = 0
    raw_chars = 0
    try:
        td = page.get_text("dict", clip=read_rect)
    except Exception:
        td = {"blocks": []}
    for blk in td.get("blocks") or []:
        if blk.get("type") != 0:
            continue
        for line in blk.get("lines") or []:
            parts, sizes, fonts = [], [], []
            bb = None
            for span in line.get("spans") or []:
                t = str(span.get("text") or "")
                raw_spans += 1
                raw_chars += len(t)
                if not t.strip():
                    continue
                parts.append(t)
                sizes.append(float(span.get("size") or 0.0))
                fonts.append(str(span.get("font") or ""))
                sb = fitz.Rect(span["bbox"])
                bb = sb if bb is None else (bb | sb)
            if not parts or bb is None:
                continue
            text = " ".join("".join(parts).split())
            if not text:
                continue
            dbb = fitz.Rect(bb) * fwd
            dbb.normalize()
            cx, cy = (dbb.x0 + dbb.x1) / 2, (dbb.y0 + dbb.y1) / 2
            if not (out_rect[0] <= cx <= out_rect[2] and out_rect[1] <= cy <= out_rect[3]):
                continue
            d = line.get("dir") or (1, 0)
            texts.append({
                "text": text,
                "bbox": [dbb.x0, dbb.y0, dbb.x1, dbb.y1],
                "cx": cx, "cy": cy,
                "size": statistics.median(sizes) if sizes else 0.0,
                "font": fonts[0] if fonts else "",
                "dir": [float(d[0]), float(d[1])],
                "n_spans": len(parts),
            })

    # ---- raster inserts ---------------------------------------------------------
    images: list[dict] = []
    try:
        infos = page.get_image_info(hashes=False, xrefs=True)
    except Exception:
        infos = []
    for info in infos or []:
        try:
            ib = fitz.Rect(info.get("bbox"))
        except Exception:
            continue
        if not _rect_overlaps(ib, read_rect):
            continue
        dbb = fitz.Rect(ib) * fwd
        dbb.normalize()
        inter = fitz.Rect(dbb) & out_rect_r
        area = dbb.get_area()
        w_px = int(info.get("width") or 0)
        h_px = int(info.get("height") or 0)
        dpi_x = (w_px / dbb.width * 72.0) if dbb.width > 0 else 0.0
        dpi_y = (h_px / dbb.height * 72.0) if dbb.height > 0 else 0.0
        images.append({
            "bbox": [dbb.x0, dbb.y0, dbb.x1, dbb.y1],
            "px": [w_px, h_px],
            "dpi": [round(dpi_x, 1), round(dpi_y, 1)],
            "bpc": info.get("bpc"),
            "cs": info.get("cs-name"),
            "xref": info.get("xref"),
            "coverage_of_block": (inter.get_area() / out_rect_r.get_area()) if out_rect_r.get_area() > 0 else 0.0,
            "clipped": bool(area > 0 and inter.get_area() < area - 1e-6),
        })

    # ---- characteristic scale ---------------------------------------------------
    lens = sorted(s["len"] for s in segments)
    s_geom = statistics.median(lens) if lens else 0.0
    sizes = [t["size"] for t in texts if t["size"] > 0]
    s_text = statistics.median(sizes) if len(sizes) >= 5 else None
    char_scale = {
        "s_text": float(s_text or 0.0),
        "s_geom": float(s_geom),
        "S": float(s_text if s_text else (s_geom if s_geom > 0 else 1.0)),
        "n_seg": len(segments),
        "n_text": len(texts),
    }

    # ---- quality ---------------------------------------------------------------
    all_text = " ".join(t["text"] for t in texts)
    raster_cov = max([im["coverage_of_block"] for im in images], default=0.0)
    quality = {
        "has_vector": len(segments) > 0,
        "raster_only": len(segments) == 0 and len(images) > 0,
        "empty": len(segments) == 0 and len(images) == 0 and len(texts) == 0,
        "text_in_curves": (len(texts) == 0 and curves >= 20),
        "no_text": len(texts) == 0,
        "garbled_ratio": round(_garbled_ratio(all_text), 5),
        "broken_text": _garbled_ratio(all_text) > 0.02,
        "raster_over_vector": bool(images and len(segments) > 0 and raster_cov > 0.10),
        "raster_coverage": round(raster_cov, 4),
        "n_images": len(images),
        "n_curves": curves,
        "invisible_share_segments": round(len(dropped) / max(1, len(dropped) + len(segments)), 5),
        "clamped_to_page": fr.clamped,
    }

    ex = BlockExtract(
        segments=segments,
        segments_raw_count=len(segments) + len(dropped),
        inked_segments_count=len(segments),
        invisible_dropped=len(dropped),
        invisible_by_rule=invisible_by_rule,
        paths_total=len(drawings),
        paths_invisible=paths_invisible,
        paths_outside_clip=paths_outside_clip,
        texts=texts,
        images=images,
        curves_flattened_count=curves,
        clipped_at_border_flags={
            "segments_touching_border": border_clipped,
            "share": round(border_clipped / max(1, len(segments) + len(dropped)), 5),
            "frame_clamped": fr.clamped,
            "frame_out_of_page": fr.out_of_page,
        },
        quality=quality,
        char_scale=char_scale,
        frame={
            "clip_display": [fr.clip_display.x0, fr.clip_display.y0, fr.clip_display.x1, fr.clip_display.y1],
            "clip_page": [fr.clip_page.x0, fr.clip_page.y0, fr.clip_page.x1, fr.clip_page.y1],
            "rotation": fr.rotation,
            "page_rect": [fr.page_rect.x0, fr.page_rect.y0, fr.page_rect.x1, fr.page_rect.y1],
            "scale_x": fr.scale_x, "scale_y": fr.scale_y,
        },
        provenance={
            "pdf": str(pdf_path),
            "pdf_sha256": pdf_sha256(pdf_path),
            "page_index": page_index,
            "coords_px": [float(v) for v in coords_px[:4]],
            "page_px": [float(page_px_w), float(page_px_h)],
            "rotation": fr.rotation,
            "naive_rotation": bool(naive_rotation),
            "drop_invisible": bool(drop_invisible),
            "curve_steps": curve_steps,
            "module": "v03_foundation",
        },
    )
    if keep_dropped_segments:
        ex.quality["dropped_segments"] = dropped
    return ex


# ---------------------------------------------------------------- render

def render_block(
    pdf_path: str | Path,
    page_index: int,
    coords_px: Sequence[float],
    page_px_w: float,
    page_px_h: float,
    *,
    dpi: int = 0,
    target_px: int = 0,
    min_long_side: int = 0,
    out_png: Optional[str | Path] = None,
    frame: Optional[Frame] = None,
    production_clip: bool = True,
):
    """Rasterise exactly the region production's ``crop_from_pdf`` rasterises.

    The scale ladder (``dpi`` vs ``target_px``, ``min_long_side`` floor, clamp to
    [0.5, 8.0]) is copied from ``crop_blocks/blocks.py::crop_from_pdf`` so the pixels
    can be compared byte-for-byte.
    """
    doc = open_doc(pdf_path)
    page = doc[page_index]
    if frame is not None and production_clip:
        clip = fitz.Rect(frame.clip_display)
    else:
        fr = block_frame(pdf_path, page_index, coords_px, page_px_w, page_px_h)
        clip = fitz.Rect(fr.clip_display)
    if production_clip:
        # production does NOT intersect with page.rect; reproduce that literally
        x1, y1, x2, y2 = [float(v) for v in coords_px[:4]]
        sx = page.rect.width / float(page_px_w)
        sy = page.rect.height / float(page_px_h)
        clip = fitz.Rect(x1 * sx, y1 * sy, x2 * sx, y2 * sy)
    long_side_pt = max(clip.width, clip.height)
    if long_side_pt < 1:
        raise ValueError("Нулевой размер блока")
    if dpi > 0:
        rs = dpi / 72.0
        if min_long_side > 0:
            rs = max(rs, min_long_side / long_side_pt)
    else:
        rs = (target_px or 1500) / long_side_pt
    rs = max(0.5, min(8.0, rs))
    pix = page.get_pixmap(matrix=fitz.Matrix(rs, rs), clip=clip, alpha=False)
    if out_png:
        Path(out_png).parent.mkdir(parents=True, exist_ok=True)
        pix.save(str(out_png))
    return pix


# ---------------------------------------------------------------- normalize

def normalize(segments: Iterable[dict], frame: Frame | dict, mode: str = "isotropic") -> list[dict]:
    """Map display-point segments into a comparable unit frame.

    ``isotropic`` (the only mode any probe may use for real work) divides BOTH axes
    by the block's long side and shifts by the clip origin.  Angles, aspect ratios and
    relative lengths survive.

    ``anisotropic`` is the v0.1 formula (x/w, y/h).  It is kept ONLY as a control:
    v0.2 proved it destroys everything smaller than 1 % of the block.

    ``points`` shifts to the clip origin and keeps PDF points — no scaling at all.
    """
    if isinstance(frame, Frame):
        x0, y0, x1, y1 = (frame.clip_display.x0, frame.clip_display.y0,
                          frame.clip_display.x1, frame.clip_display.y1)
    else:
        x0, y0, x1, y1 = frame["clip_display"]
    w = max(float(x1) - float(x0), 1e-9)
    h = max(float(y1) - float(y0), 1e-9)
    if mode == "isotropic":
        s = max(w, h)
        fx = fy = 1.0 / s
    elif mode == "anisotropic":
        fx, fy = 1.0 / w, 1.0 / h
    elif mode == "points":
        # No scaling at all: shift to the clip origin and stay in PDF points.
        # v0.2 found relations must be measured in points; F3 measures whether that
        # also holds for raw segment matching across versions.
        fx = fy = 1.0
    else:
        raise ValueError(f"unknown normalize mode {mode!r}")
    out = []
    for sg in segments:
        p0 = ((sg["p0"][0] - x0) * fx, (sg["p0"][1] - y0) * fy)
        p1 = ((sg["p1"][0] - x0) * fx, (sg["p1"][1] - y0) * fy)
        d = dict(sg)
        d["p0"] = p0
        d["p1"] = p1
        d["len"] = math.hypot(p1[0] - p0[0], p1[1] - p0[1])
        out.append(d)
    return out
