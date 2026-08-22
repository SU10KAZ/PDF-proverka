"""Shared machinery for the HATCH / BACKGROUND NOISE probe (Track B, prefix `hatchnoise`).

Research only.  Reads Track A's extractor but never modifies it.

Run helpers from repo root, e.g.
    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p1_features
"""
from __future__ import annotations

import collections
import hashlib
import json
import math
import os
import pickle
import time
from pathlib import Path
from typing import Any, Sequence

import fitz

from experiments.stage_comparison_vector_blocks import extractor as ta

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"
OUT = ART / "hatchnoise"
CACHE = Path(
    os.environ.get(
        "HATCHNOISE_CACHE",
        "/tmp/claude-1001/-home-coder-projects-PDF-proverka/7be66dd6-80e8-4c87-9aef-d5834ab15302/scratchpad/hatchnoise_cache",
    )
)

BIG_CAP = 10 ** 9

SS = "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/%s/02_work/document.pdf"
AR = "projects_v2/objects/256_Primavera_K14_Spartak/disciplines/AR/documents/СТ26_01-14-АР0-АС-1-РД_V1/versions/%s/02_work/document.pdf"
VK = "projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК.КВ-К4_V1/versions/%s/02_work/document.pdf"
EOM = "projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К4/versions/%s/02_work/document.pdf"
AR42 = "projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР4.2-К4/versions/%s/02_work/document.pdf"
OV11 = "projects_v2/objects/214_Alia_ASTERUS/disciplines/OV/documents/13АВ-РД-ОВ1.1-К4 V1/versions/%s/02_work/document.pdf"

# The four blocks named by the probe brief + two fresh dense blocks.
# bbox values for the four benchmark blocks are copied verbatim from
# experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json.
BLOCKS: dict[str, dict[str, Any]] = {
    "ar_plan": {
        "discipline": "AR",
        "source": "trackA benchmark pair ar_plan",
        "left": (AR % "v001", 8, [0.022951479766458626, 0.007126567844925884, 0.6217032413931951, 0.9908779931584949]),
        "right": (AR % "v002", 8, [0.023376623376623374, 0.00857835309203823, 0.6238317757009346, 0.9904689470224114]),
    },
    "ss_plan_dense": {
        "discipline": "SS",
        "source": "trackA benchmark pair ss_plan_dense",
        "left": (SS % "v002", 7, [0.009230971336364746, 0.0, 0.9994679689407349, 0.7758837086715362]),
        "right": (SS % "v003", 7, [0.009230971336364746, 0.0, 0.9994679689407349, 0.759651807472305]),
    },
    "ar_wall_sections": {
        "discipline": "AR",
        "source": "trackA benchmark pair ar_wall_sections",
        "left": (AR % "v001", 13, [0.41929848138691034, 0.3845496009122007, 0.843703803252251, 0.6755986316989738]),
        "right": (AR % "v002", 13, [0.4238317757009346, 0.38417331525068427, 0.8425790754257908, 0.6746297605893186]),
    },
    "vk_plan": {
        "discipline": "VK",
        "source": "trackA benchmark pair vk_plan",
        "left": (VK % "v001", 5, [0.05024154589371981, 0.009294320137693631, 0.6681159420289855, 0.848710601719198]),
        "right": (VK % "v002", 5, [0.04794520547945206, 0.009202453987730062, 0.6673058485139022, 0.8475460122699386]),
    },
    # fresh #1 — dense AR socket-layout plan; this PDF *does* carry CAD layer names,
    # which gives a discipline-free ground truth for background/hatch/furniture.
    "ar_layered_plan": {
        "discipline": "AR",
        "source": "fresh; chosen because page.get_drawings() exposes non-empty 'layer' for 100% of drawings",
        "left": (AR42 % "v001", 24, [0.02, 0.01, 0.87, 0.72]),
        "right": (AR42 % "v002", 24, [0.02, 0.01, 0.87, 0.72]),
    },
    # fresh #2 — dense OV node/detail sheet, heavy construction hatch, NO layer names.
    "ov_nodes_hatch": {
        "discipline": "OV",
        "source": "fresh; densest page of the document (71926 drawings), zero layer names",
        "left": (OV11 % "v001", 13, [0.015, 0.01, 0.86, 0.86]),
        "right": (OV11 % "v002", 13, [0.015, 0.01, 0.86, 0.86]),
    },
}

EXTRA_PAIRS = {
    "ss_scheme_text_changed": {
        "discipline": "SS",
        "left": (SS % "v002", 5, [0.030090421438217163, 0.012994349002838135, 0.9898177683353424, 0.3752070367336273]),
        "right": (SS % "v003", 5, [0.02961629629135132, 0.013081371784210205, 0.9908167719841003, 0.37837013602256775]),
    },
    "eom_singleline_changed": {
        "discipline": "EOM",
        "left": (EOM % "v001", 8, [0.04, 0.025, 0.245, 0.94]),
        "right": (EOM % "v002", 10, [0.0226, 0.0092, 0.2452, 0.6639]),
    },
    "vk_nodes": {
        "discipline": "VK",
        "left": (VK % "v001", 8, [0.022263450834879408, 0.0, 0.4550724637681159, 0.9886926003824092]),
        "right": (VK % "v002", 8, [0.019704433497536946, 0.0016871165644171779, 0.46929260450160774, 0.9980879541108987]),
    },
}


# --------------------------------------------------------------------------- extraction


def _key(pdf: str, page_index: int, bbox: Sequence[float]) -> str:
    raw = f"{pdf}|{page_index}|{[round(float(v), 6) for v in bbox]}|rot2"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _renormalize(primitive: dict[str, Any], matrix: Any, rect: Any) -> None:
    """Map raw geometry through `matrix` (unrotated -> rendered page space) and recompute
    the block-normalized fields against the rendered block rect.

    Needed because page.get_drawings()/get_text() report coordinates in the UNROTATED page
    box while bbox_norm is expressed on the rendered (rotated) page.
    """
    width = max(rect.x1 - rect.x0, 1e-9)
    height = max(rect.y1 - rect.y0, 1e-9)
    raw_segments = []
    normalized_segments = []
    for start, end in primitive["raw"]["segments"]:
        p0 = fitz.Point(start[0], start[1]) * matrix
        p1 = fitz.Point(end[0], end[1]) * matrix
        raw_segments.append([[ta._round(p0.x), ta._round(p0.y)], [ta._round(p1.x), ta._round(p1.y)]])
        normalized_segments.append([
            [ta._round((p0.x - rect.x0) / width), ta._round((p0.y - rect.y0) / height)],
            [ta._round((p1.x - rect.x0) / width), ta._round((p1.y - rect.y0) / height)],
        ])
    primitive["raw"]["segments"] = raw_segments
    primitive["normalized"]["segments"] = normalized_segments
    primitive["raw"]["bbox"] = ta._bbox([point for segment in raw_segments for point in segment])
    primitive["normalized"]["bbox"] = ta._bbox([point for segment in normalized_segments for point in segment])
    primitive["length_norm"] = ta._round(sum(ta._distance(*segment) for segment in normalized_segments))
    first, last = raw_segments[0][0], raw_segments[-1][1]
    primitive["angle_degrees"] = (
        ta._round(math.degrees(math.atan2(last[1] - first[1], last[0] - first[0])), 3)
        if first != last else None
    )


def load_primitives(pdf: str, page_index: int, bbox_norm: Sequence[float]) -> dict[str, Any]:
    """Uncapped Track-A primitives + text spans for one block, cached on disk.

    Unlike extractor.extract_block this de-rotates the block window first, so blocks on
    rotated pages describe the region a human actually sees (see hatchnoise_p0_rotation).
    """
    CACHE.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE / f"prim_{_key(pdf, page_index, bbox_norm)}.pkl"
    if cache_file.exists():
        with cache_file.open("rb") as handle:
            return pickle.load(handle)
    t0 = time.time()
    document = fitz.open(ROOT / pdf)
    page = document[page_index]
    rect_rot = fitz.Rect(
        bbox_norm[0] * page.rect.width,
        bbox_norm[1] * page.rect.height,
        bbox_norm[2] * page.rect.width,
        bbox_norm[3] * page.rect.height,
    )
    rotation = page.rotation % 360
    rect_source = fitz.Rect(rect_rot) * page.derotation_matrix if rotation else fitz.Rect(rect_rot)
    rect_source.normalize()
    block_rect = [rect_source.x0, rect_source.y0, rect_source.x1, rect_source.y1]
    primitives, extraction = ta._extract_primitives(page, block_rect, BIG_CAP, polygon_abs=None)
    texts = ta._extract_text(page, block_rect, None)
    if rotation:
        matrix = page.rotation_matrix
        width = max(rect_rot.x1 - rect_rot.x0, 1e-9)
        height = max(rect_rot.y1 - rect_rot.y0, 1e-9)
        for primitive in primitives:
            _renormalize(primitive, matrix, rect_rot)
        for span in texts:
            box = fitz.Rect(*span["bbox"]) * matrix
            box.normalize()
            span["bbox"] = [ta._round(v) for v in (box.x0, box.y0, box.x1, box.y1)]
            span["bbox_norm"] = [
                ta._round((box.x0 - rect_rot.x0) / width), ta._round((box.y0 - rect_rot.y0) / height),
                ta._round((box.x1 - rect_rot.x0) / width), ta._round((box.y1 - rect_rot.y0) / height),
            ]
            span["x_norm"] = ta._round((span["bbox_norm"][0] + span["bbox_norm"][2]) / 2)
            span["y_norm"] = ta._round((span["bbox_norm"][1] + span["bbox_norm"][3]) / 2)
    payload = {
        "pdf": pdf,
        "page_index": page_index,
        "bbox_norm": [float(v) for v in bbox_norm],
        "page_rotation": rotation,
        "block_rect_rendered": [rect_rot.x0, rect_rot.y0, rect_rot.x1, rect_rot.y1],
        "block_rect_source": block_rect,
        "page_size": [page.rect.width, page.rect.height],
        "primitives": primitives,
        "texts": texts,
        "extraction": extraction,
        "elapsed_s": round(time.time() - t0, 2),
    }
    document.close()
    with cache_file.open("wb") as handle:
        pickle.dump(payload, handle, protocol=4)
    return payload


# --------------------------------------------------------------------------- features

GRID = 64                    # local-neighbourhood grid over the block
ANGLE_BINS = 18              # 10 degrees per bin
TINY = 0.004                 # normalized length treated as "tiny segment"


def _luminance(color: Sequence[float] | None) -> float:
    if not color:
        return 0.0
    if len(color) >= 3:
        return 0.2126 * color[0] + 0.7152 * color[1] + 0.0722 * color[2]
    return float(color[0])


def segment_table(payload: dict[str, Any]) -> dict[str, Any]:
    """One row per normalized segment with every candidate discriminating feature."""
    primitives = payload["primitives"]
    texts = payload["texts"]

    # motif repetition (path shape fingerprint -> how many primitives share it)
    motif_of: list[str] = []
    motif_count: collections.Counter[str] = collections.Counter()
    for primitive in primitives:
        pattern = ta._primitive_pattern(primitive)
        motif_of.append(pattern)
        motif_count[pattern] += 1

    # closed contours usable for the enclosure test (skip block-sized frames)
    closed_boxes = []
    for primitive in primitives:
        if not primitive["closed"]:
            continue
        bbox = primitive["normalized"]["bbox"]
        area = max(bbox[2] - bbox[0], 0.0) * max(bbox[3] - bbox[1], 0.0)
        if 1e-6 < area < 0.30:
            closed_boxes.append(bbox)
    closed_cells: dict[tuple[int, int], list[list[float]]] = collections.defaultdict(list)
    for bbox in closed_boxes:
        x0 = max(0, min(GRID - 1, int(bbox[0] * GRID)))
        x1 = max(0, min(GRID - 1, int(bbox[2] * GRID)))
        y0 = max(0, min(GRID - 1, int(bbox[1] * GRID)))
        y1 = max(0, min(GRID - 1, int(bbox[3] * GRID)))
        if (x1 - x0 + 1) * (y1 - y0 + 1) > 400:
            continue
        for gx in range(x0, x1 + 1):
            for gy in range(y0, y1 + 1):
                closed_cells[(gx, gy)].append(bbox)

    # text coverage per cell
    text_cell: collections.Counter[tuple[int, int]] = collections.Counter()
    for span in texts:
        bbox = span["bbox_norm"]
        x0 = max(0, min(GRID - 1, int(bbox[0] * GRID)))
        x1 = max(0, min(GRID - 1, int(bbox[2] * GRID)))
        y0 = max(0, min(GRID - 1, int(bbox[1] * GRID)))
        y1 = max(0, min(GRID - 1, int(bbox[3] * GRID)))
        for gx in range(x0, x1 + 1):
            for gy in range(y0, y1 + 1):
                text_cell[(gx, gy)] += 1

    rows: list[dict[str, Any]] = []
    for index, primitive in enumerate(primitives):
        style = primitive["style"]
        path_type = str(style.get("path_type") or "")
        filled = "f" in path_type
        stroked = "s" in path_type
        width = float(style.get("stroke_width") or 0.0)
        stroke_lum = _luminance(style.get("stroke"))
        fill_lum = _luminance(style.get("fill"))
        colored = False
        for color in (style.get("stroke"), style.get("fill")):
            if color and len(color) >= 3 and max(color) - min(color) > 0.08:
                colored = True
        layer = str(style.get("layer") or "")
        motif = motif_of[index]
        motif_n = motif_count[motif]
        for segment in primitive["normalized"]["segments"]:
            (x0, y0), (x1, y1) = segment[0], segment[1]
            length = math.hypot(x1 - x0, y1 - y0)
            if length <= 1e-9:
                continue
            mx, my = (x0 + x1) / 2, (y0 + y1) / 2
            angle = math.degrees(math.atan2(y1 - y0, x1 - x0)) % 180.0
            cell = (max(0, min(GRID - 1, int(mx * GRID))), max(0, min(GRID - 1, int(my * GRID))))
            rows.append(
                {
                    "pi": index,
                    "pid": primitive["id"],
                    "ptype": primitive["type"],
                    "seg": [[x0, y0], [x1, y1]],
                    "mid": [mx, my],
                    "len": length,
                    "ang": angle,
                    "cell": cell,
                    "filled": filled,
                    "stroked": stroked,
                    "width": width,
                    "stroke_lum": stroke_lum,
                    "fill_lum": fill_lum,
                    "colored": colored,
                    "closed": bool(primitive["closed"]),
                    "prim_segs": primitive["segment_count"],
                    "prim_len": primitive["length_norm"],
                    "motif": motif,
                    "motif_n": motif_n,
                    "layer": layer,
                }
            )

    # ---- per-cell aggregates
    by_cell: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for row_index, row in enumerate(rows):
        by_cell[row["cell"]].append(row_index)

    cell_stats: dict[tuple[int, int], dict[str, float]] = {}
    for cell, members in by_cell.items():
        hist = [0] * ANGLE_BINS
        tiny = 0
        for row_index in members:
            row = rows[row_index]
            hist[min(ANGLE_BINS - 1, int(row["ang"] / (180.0 / ANGLE_BINS)))] += 1
            if row["len"] < TINY:
                tiny += 1
        total = float(len(members))
        entropy = 0.0
        for count in hist:
            if count:
                p = count / total
                entropy -= p * math.log2(p)
        cell_stats[cell] = {
            "density": len(members),
            "dominant_share": max(hist) / total,
            "orientation_entropy": entropy,
            "tiny_frac": tiny / total,
            "text_spans": text_cell.get(cell, 0),
        }

    for row in rows:
        stats = cell_stats[row["cell"]]
        row["cell_density"] = stats["density"]
        row["cell_dominant_share"] = stats["dominant_share"]
        row["cell_orientation_entropy"] = stats["orientation_entropy"]
        row["cell_tiny_frac"] = stats["tiny_frac"]
        row["cell_text_spans"] = stats["text_spans"]
        boxes = closed_cells.get(row["cell"], ())
        enclosed = False
        for bbox in boxes:
            if bbox[0] <= row["mid"][0] <= bbox[2] and bbox[1] <= row["mid"][1] <= bbox[3]:
                enclosed = True
                break
        row["enclosed"] = enclosed

    return {"rows": rows, "cell_stats": cell_stats, "n_primitives": len(primitives), "n_texts": len(texts)}


# --------------------------------------------------------------------------- rendering


def render_segments(
    rows: Sequence[dict[str, Any]],
    out_png: Path,
    *,
    width_px: int = 1600,
    aspect: float = 1.0,
    color: tuple[float, float, float] = (0, 0, 0),
    title: str = "",
) -> None:
    """Render a set of normalized segments into a PNG through PyMuPDF (offline, no deps)."""
    height_px = max(64, int(width_px * aspect))
    document = fitz.open()
    page = document.new_page(width=width_px, height=height_px)
    shape = page.new_shape()
    for index, row in enumerate(rows):
        (x0, y0), (x1, y1) = row["seg"]
        shape.draw_line(
            fitz.Point(x0 * width_px, y0 * height_px),
            fitz.Point(x1 * width_px, y1 * height_px),
        )
        if index % 4000 == 3999:
            shape.finish(color=color, width=0.4)
            shape.commit()
            shape = page.new_shape()
    shape.finish(color=color, width=0.4)
    shape.commit()
    if title:
        page.insert_text(fitz.Point(8, 16), title, fontsize=11, color=(0.8, 0, 0))
    out_png.parent.mkdir(parents=True, exist_ok=True)
    page.get_pixmap(matrix=fitz.Matrix(1, 1)).save(out_png)
    document.close()


def render_crop(pdf: str, page_index: int, bbox_norm: Sequence[float], out_png: Path, width_px: int = 1600) -> None:
    document = fitz.open(ROOT / pdf)
    page = document[page_index]
    rect = fitz.Rect(
        bbox_norm[0] * page.rect.width,
        bbox_norm[1] * page.rect.height,
        bbox_norm[2] * page.rect.width,
        bbox_norm[3] * page.rect.height,
    )
    zoom = width_px / rect.width
    out_png.parent.mkdir(parents=True, exist_ok=True)
    page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=rect).save(out_png)
    document.close()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
