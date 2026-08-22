#!/usr/bin/env python3
"""VV — Vision-Verification harness for VectorBlockDescription.

Research only (Track B / Opus).  Nothing here is production code.

The harness answers one question: *can a multimodal model check a deterministic
vector description against the raster crop of the same block, BEFORE any
comparison happens?*  To measure that we need corrupted descriptions with known
ground truth, so detection and false alarms can both be scored.

Public API
----------
``load_description(path)``           read a v0.1 or v0.2 description
``fact_sheet(description, ...)``     short list of picture-checkable claims
``mutate(description, kind, rng)``   corrupt a description, return ground truth
``render_crop(pdf, page_index, bbox_norm, out_png, zoom)``   raster crop
``crop_for(pair_id, side)``          reuse Track A's diagnostics PNG
``verify(crop_png, sheet, out_json)``real multimodal call (Claude Code CLI)
``materialize_case(case)``           rebuild a manifest case deterministically

Reproduction
------------
    cd /home/coder/projects/PDF-proverka
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_harness selftest
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_build_cases
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_smoke
"""
from __future__ import annotations

import argparse
import collections
import copy
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable, Sequence

ROOT = Path("/home/coder/projects/PDF-proverka")
EXP_DIR = ROOT / "experiments" / "stage_comparison_vector_architecture_opus"
ARTIFACTS = EXP_DIR / "artifacts"
TRACK_A = ROOT / "experiments" / "stage_comparison_vector_blocks"
TRACK_A_ART = TRACK_A / "artifacts"
DIAGNOSTICS = TRACK_A_ART / "diagnostics"
DESCRIPTIONS = TRACK_A_ART / "descriptions"
V02_DIR = ARTIFACTS / "v02"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.stage_comparison_vector_blocks import extractor as ex  # noqa: E402

CASES_JSON = ARTIFACTS / "vv_cases.json"
VERIFY_DIR = ARTIFACTS / "vv_verify"
CROP_DIR = ARTIFACTS / "vv_crops"

# The comparator's own segment cap; needed for the ss_plan_dense scope case.
COMPARATOR_SEGMENT_CAP = 12_000

MUTATION_KINDS = (
    "clean",
    "deleted_object",
    "wrong_count",
    "missing_labels",
    "wrong_topology",
    "broken_text",
    "capped_geometry",
)

# Control characters that PyMuPDF returns when /ToUnicode has no entry for a code
# (measured on vk_nodes left: 207 of 421 spans, alphabet below).
BROKEN_ALPHABET = [chr(c) for c in (0x04, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A)]
BROKEN_ALPHABET += list("!#$%&()*+-/")
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_CYR_RE = re.compile(r"[А-Яа-яЁё]")
_WORDY_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]{2,}")


# --------------------------------------------------------------------------- io


def load_description(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def description_path(pair_id: str, side: str) -> Path:
    return DESCRIPTIONS / pair_id / side / "vector_block.json"


def v02_path(pair_id: str, side: str) -> Path:
    return V02_DIR / pair_id / f"{side}.json"


def schema_of(description: dict[str, Any]) -> str:
    version = str(description.get("schema_version", ""))
    if "objects-research-v0.2" in version:
        return "v0.2"
    if "research-v0.1" in version:
        return "v0.1"
    raise ValueError(f"unknown schema_version: {version!r}")


# ------------------------------------------------------------- derived rebuild


def rebuild_derived(description: dict[str, Any]) -> dict[str, Any]:
    """Recompute every layer downstream of ``geometry.primitives`` + ``texts``.

    Uses Track A's own extractor functions, so a mutated description is exactly
    as internally consistent as one the extractor would have produced from the
    mutated geometry.  ``vv_harness selftest`` asserts that running this on an
    untouched description is a no-op on all 20 Track A blocks.
    """
    primitives = description["geometry"]["primitives"]
    texts = description["texts"]
    tolerance = description["topology"].get("tolerance_norm", 0.0025)
    topology = ex._topology(primitives, tolerance, ex.DEFAULT_TOPOLOGY_CAP)
    anchors = ex._anchors(texts, primitives)
    description["topology"] = topology
    description["anchors"] = anchors
    description["repeated_elements"] = ex._repeated_elements(primitives)
    description["hatch_like_structures"] = ex._hatch_like_structures(primitives)
    description["dimensions"] = [
        {
            "text_id": item["id"],
            "text": item["text"],
            "bbox_norm": item["bbox_norm"],
            "geometry_id": next(
                (a["geometry_id"] for a in anchors if a["text_id"] == item["id"]), None
            ),
            "classification": "dimension_or_engineering_value_candidate",
        }
        for item in texts
        if item["category"] == "engineering_value"
    ]
    description["labels"] = [
        {"text_id": item["id"], "text": item["text"], "bbox_norm": item["bbox_norm"]}
        for item in texts
        if item["category"] == "label"
    ]
    extraction = description["geometry"]["extraction"]
    if not primitives or topology["segments_total"] < 3:
        quality = "VECTOR_DATA_INSUFFICIENT"
    elif extraction["storage_capped"] or topology["segments_capped"]:
        quality = "LIMITED_CAPPED"
    elif topology["segments_total"] < 30:
        quality = "LIMITED"
    else:
        quality = "GOOD"
    description["vector_quality"] = quality
    description["quality_notes"] = [
        note
        for condition, note in (
            (extraction["storage_capped"], "Primitive storage cap reached; longest/salient paths retained."),
            (topology["segments_capped"], "Topology cap reached; graph uses the longest segments."),
            (not texts, "No usable vector text spans in the block."),
            (quality == "VECTOR_DATA_INSUFFICIENT", "Useful PDF vector geometry is absent or insufficient."),
        )
        if condition
    ]
    description["primitive_summary"] = ex._summary(primitives, texts, topology)
    description["structural_signature"] = ex._signatures(primitives, texts, topology)
    description["size_metrics"] = ex._size_metrics(description)
    return description


# ---------------------------------------------------------------- fact sheet


def _grid_cell_name(col: int, row: int) -> str:
    rows = ("top", "middle", "bottom")
    cols = ("left", "centre", "right")
    return f"{rows[row]}-{cols[col]}"


def _segment_midpoints(description: dict[str, Any]) -> list[tuple[float, float]]:
    points = []
    for primitive in description["geometry"]["primitives"]:
        for start, end in primitive["normalized"]["segments"]:
            points.append(((start[0] + end[0]) / 2.0, (start[1] + end[1]) / 2.0))
    return points


def _occupancy(description: dict[str, Any]) -> dict[str, Any]:
    counts: collections.Counter[tuple[int, int]] = collections.Counter()
    points = _segment_midpoints(description)
    for x, y in points:
        col = min(2, max(0, int(x * 3)))
        row = min(2, max(0, int(y * 3)))
        counts[(col, row)] += 1
    empty = [
        _grid_cell_name(c, r)
        for r in range(3)
        for c in range(3)
        if counts.get((c, r), 0) == 0
    ]
    total = max(1, sum(counts.values()))
    ranked = sorted(
        ((counts.get((c, r), 0), c, r) for r in range(3) for c in range(3)),
        key=lambda item: (-item[0], item[2], item[1]),
    )
    return {
        "empty_cells": empty,
        "total_segments": sum(counts.values()),
        "densest": {"cell": _grid_cell_name(ranked[0][1], ranked[0][2]),
                    "share": round(ranked[0][0] / total, 3)},
        "sparsest_nonempty": next(
            ({"cell": _grid_cell_name(c, r), "share": round(n / total, 3)}
             for n, c, r in reversed(ranked) if n > 0),
            None,
        ),
        "counts": {f"{c}{r}": counts.get((c, r), 0) for r in range(3) for c in range(3)},
    }


def _elements_by_third(description: dict[str, Any], min_segments: int = 4) -> dict[str, Any]:
    """Count the block's biggest connected elements per horizontal third.

    "Biggest" is exactly the set the description itself lists: ``topology.components``
    (largest first, capped at 50 by the extractor) with at least ``min_segments``
    segments.  A deleted element always leaves this count, which is why the claim
    exists — without it a deletion inside a busy block moves nothing but totals.
    """
    left = centre = right = 0
    counted = 0
    for component in description["topology"].get("components") or []:
        if component.get("segment_count", 0) < min_segments:
            continue
        bbox = component.get("bbox_norm") or [0, 0, 0, 0]
        cx = (bbox[0] + bbox[2]) / 2.0
        counted += 1
        if cx < 1 / 3:
            left += 1
        elif cx < 2 / 3:
            centre += 1
        else:
            right += 1
    return {"left": left, "centre": centre, "right": right, "counted": counted,
            "min_segments": min_segments,
            "components_truncated": bool(description["topology"].get("components_truncated")),
            "definition": "connected components listed in the description (largest first, "
                          "extractor caps the list at 50) with >= 4 segments"}


def _boundary_touch(description: dict[str, Any], epsilon: float = 0.004) -> list[str]:
    sides: set[str] = set()
    boxes: list[Sequence[float]] = [p["normalized"]["bbox"] for p in description["geometry"]["primitives"]]
    boxes += [t["bbox_norm"] for t in description["texts"]]
    for bbox in boxes:
        if bbox[0] <= epsilon:
            sides.add("left")
        if bbox[1] <= epsilon:
            sides.add("top")
        if bbox[2] >= 1.0 - epsilon:
            sides.add("right")
        if bbox[3] >= 1.0 - epsilon:
            sides.add("bottom")
    return sorted(sides)


def _short(text: str, limit: int = 16) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _visible(text: str) -> str:
    """Render control characters so a fact sheet stays printable."""
    return "".join(ch if ch.isprintable() else "·" for ch in text)


def _text_view(description: dict[str, Any]) -> dict[str, Any]:
    schema = schema_of(description)
    if schema == "v0.1":
        items = [
            {
                "text": t["text"],
                "size": t.get("font_size", 0.0),
                "category": t.get("category", "label"),
                "cx": t.get("x_norm", 0.0),
                "cy": t.get("y_norm", 0.0),
            }
            for t in description["texts"]
        ]
    else:
        items = [
            {
                "text": t["text"],
                "size": t.get("font_size", 0.0),
                "category": "numeric" if t.get("numeric") else "label",
                "cx": t.get("cx", 0.0),
                "cy": t.get("cy", 0.0),
            }
            for t in description["texts"]
        ]
    for item in items:
        item["broken"] = bool(_CTRL_RE.search(item["text"]))
    # C1 must quote the count the DESCRIPTION states, not len(list): a miscount
    # mutation lives exactly in that gap.
    stated = len(items)
    if schema == "v0.1":
        stated = int(description.get("primitive_summary", {}).get("text_items", len(items)))
    return {"items": items, "n": stated, "listed": len(items),
            "broken": sum(1 for i in items if i["broken"])}


def _claim(cid: str, kind: str, claim: str, value: Any = None) -> dict[str, Any]:
    return {"id": cid, "kind": kind, "claim": claim, "value": value}


def fact_sheet(
    description: dict[str, Any],
    *,
    disclose_limits: bool = True,
    max_chars: int = 1200,
) -> dict[str, Any]:
    """Turn a description into a SHORT list of individually checkable claims.

    ``disclose_limits`` controls only the two self-reported claims (extraction
    cap, extractor self-rating).  ``False`` models a pipeline that truncates
    silently — the case the brief calls "silently truncate at a cap".
    """
    schema = schema_of(description)
    claims: list[dict[str, Any]] = []
    tv = _text_view(description)

    if tv["n"] == 0:
        claims.append(_claim("C1", "text_count", "There is no vector text in this block at all.", 0))
    else:
        claims.append(_claim("C1", "text_count", f"The block contains {tv['n']} separate text strings.", tv["n"]))
        if tv["broken"]:
            claims.append(_claim(
                "C2", "text_readable",
                f"{tv['broken']} of the {tv['n']} text strings carry no readable letters (garbled).",
                tv["broken"]))
        else:
            claims.append(_claim("C2", "text_readable",
                                 f"All {tv['n']} text strings are readable words or numbers.", 0))
        prominent, seen = [], set()
        for item in sorted(tv["items"], key=lambda i: (-i["size"], i["cx"])):
            token = _visible(_short(item["text"]))
            if len(token) < 2 or token in seen:
                continue
            seen.add(token)
            prominent.append(token)
            if len(prominent) >= 5:
                break
        if prominent:
            claims.append(_claim("C3", "prominent_text",
                                 "The largest lettering includes: " + ", ".join(prominent) + ".",
                                 prominent))
        values, seen_v = [], set()
        for item in sorted(tv["items"], key=lambda i: (-i["size"], i["cx"])):
            if item["category"] not in ("numeric", "engineering_value") or item["broken"]:
                continue
            token = _visible(_short(item["text"], 12))
            if token in seen_v:
                continue
            seen_v.add(token)
            values.append(token)
            if len(values) >= 6:
                break
        if values:
            claims.append(_claim("C4", "values",
                                 "Numeric values that appear include: " + ", ".join(values) + ".",
                                 values))

    if schema == "v0.1":
        summary = description["primitive_summary"]
        topology = description["topology"]
        repeated = description["repeated_elements"]
        n_seg = summary["total_segment_count"]
        n_prim = summary["primitive_count"]
        if repeated:
            top = repeated[0]
            claims.append(_claim("C5", "repeat_family",
                                 f"The most repeated shape occurs {top['count']} times "
                                 f"({top['primitive_type']}, {top['segment_count']} strokes each).",
                                 top["count"]))
            claims.append(_claim("C6", "repeat_families",
                                 f"{len(repeated)} different shapes repeat two or more times.",
                                 len(repeated)))
        else:
            claims.append(_claim("C6", "repeat_families",
                                 "No shape in the block is drawn twice with the same outline.", 0))
        claims.append(_claim("C7", "components",
                             f"The linework forms {topology['connected_components']} separate "
                             f"connected networks (nothing joins them).",
                             topology["connected_components"]))
        claims.append(_claim("C8", "junctions",
                             f"There are {topology['branch_points']} junctions where 3 or more "
                             f"lines meet and {topology['closed_contours']} closed outlines.",
                             [topology["branch_points"], topology["closed_contours"]]))
        if n_seg >= 5000:
            claims.append(_claim("C9", "size",
                                 f"The drawing is dense: {n_prim} paths, about {n_seg // 1000}k line "
                                 f"segments in total.", n_seg))
        else:
            claims.append(_claim("C9", "size",
                                 f"The drawing holds {n_prim} paths and {n_seg} line segments in total.",
                                 n_seg))
        occ = _occupancy(description)
        if occ["empty_cells"]:
            claims.append(_claim("C10", "occupancy",
                                 "These parts of the block hold no linework at all: "
                                 + ", ".join(occ["empty_cells"]) + ".",
                                 occ["empty_cells"]))
        else:
            claims.append(_claim("C10", "occupancy",
                                 "Linework reaches every one of the nine parts of the block "
                                 "(top/middle/bottom × left/centre/right).", []))
        thirds = _elements_by_third(description)
        if thirds["counted"]:
            claims.append(_claim("C14", "elements_by_third",
                                 f"Big separate drawn elements by third of the block — left "
                                 f"{thirds['left']}, centre {thirds['centre']}, right {thirds['right']}.",
                                 thirds))
        sides = _boundary_touch(description)
        if sides:
            claims.append(_claim("C11", "boundary",
                                 "Content runs into the block edge on the " + "/".join(sides)
                                 + " side, so it may be cut off there.", sides))
        else:
            claims.append(_claim("C11", "boundary",
                                 "Nothing touches the block edges; the drawing sits fully inside.", []))
        if disclose_limits:
            extraction = description["geometry"]["extraction"]
            if extraction.get("storage_capped"):
                kept = extraction["storage_cap"]
                total = extraction["primitives_uncapped"]
                claims.append(_claim("C12", "cap",
                                     f"Geometry was truncated at a cap: only the {kept} longest of "
                                     f"{total} paths were kept.", [kept, total]))
            claims.append(_claim("C13", "self_rating",
                                 f"The extractor rates its own view of this block: "
                                 f"{description['vector_quality']}.",
                                 description["vector_quality"]))
    else:  # v0.2 objects layer
        stats = description["stats"]
        repeated = description.get("repeated") or []
        objects = description.get("objects") or []
        classes = collections.Counter(o.get("class") for o in objects)
        if repeated:
            top = max(repeated, key=lambda r: r["count"])
            claims.append(_claim("C5", "repeat_family",
                                 f"The most repeated symbol occurs {top['count']} times.", top["count"]))
        claims.append(_claim("C6", "repeat_families",
                             f"{stats.get('repeated_families', len(repeated))} different symbols "
                             f"repeat two or more times.", stats.get("repeated_families")))
        claims.append(_claim("C7", "objects",
                             f"The block was grouped into {len(objects)} objects: "
                             + ", ".join(f"{n} {k}" for k, n in classes.most_common(3)) + ".",
                             len(objects)))
        claims.append(_claim("C8", "linear",
                             f"{stats.get('linear_objects')} long runs (buses/branches) were found.",
                             stats.get("linear_objects")))
        claims.append(_claim("C9", "size",
                             f"The drawing holds {stats.get('stroke_count')} strokes.",
                             stats.get("stroke_count")))
        relations = description.get("relations") or []
        claims.append(_claim("C10", "relations",
                             f"{len(relations)} text labels were bound to a specific object.",
                             len(relations)))

    text = "\n".join(f"{c['id']}. {c['claim']}" for c in claims)
    dropped: list[str] = []
    # Trim from the least valuable end until the sheet fits.
    priority = ["C13", "C8", "C6", "C11", "C4", "C9", "C10", "C5", "C12", "C7", "C14",
                "C3", "C2", "C1"]
    while len(text) > max_chars and claims:
        for cid in priority:
            hit = next((c for c in claims if c["id"] == cid), None)
            if hit is not None and len(claims) > 4:
                claims.remove(hit)
                dropped.append(cid)
                break
        else:
            break
        text = "\n".join(f"{c['id']}. {c['claim']}" for c in claims)

    return {
        "block_id": description.get("block_id"),
        "schema": schema,
        "disclose_limits": disclose_limits,
        "claims": claims,
        "text": text,
        "characters": len(text),
        "dropped_for_length": dropped,
    }


def sheet_delta(before: dict[str, Any], after: dict[str, Any]) -> list[dict[str, Any]]:
    """Which claims a mutation actually changed in the fact sheet."""
    a = {c["id"]: c["claim"] for c in before["claims"]}
    b = {c["id"]: c["claim"] for c in after["claims"]}
    out = []
    for cid in sorted(set(a) | set(b)):
        if a.get(cid) != b.get(cid):
            out.append({"claim_id": cid, "before": a.get(cid), "after": b.get(cid)})
    return out


# ------------------------------------------------------------------ mutations


def _position_phrase(bbox: Sequence[float]) -> str:
    cx = (bbox[0] + bbox[2]) / 2.0
    cy = (bbox[1] + bbox[3]) / 2.0
    row = "top" if cy < 1 / 3 else ("middle" if cy < 2 / 3 else "bottom")
    col = "left" if cx < 1 / 3 else ("centre" if cx < 2 / 3 else "right")
    return f"{row}-{col}"


def _rebuild_primitive(primitive: dict[str, Any], keep: list[int]) -> dict[str, Any] | None:
    """Rebuild one primitive from a subset of its segments, extractor-style."""
    original_count = len(primitive["normalized"]["segments"])
    if not keep:
        return None
    raw = [primitive["raw"]["segments"][i] for i in keep]
    norm = [primitive["normalized"]["segments"][i] for i in keep]
    item_indexes = primitive.get("item_indexes") or []
    source_kinds = primitive.get("source_kinds") or []
    primitive = dict(primitive)
    primitive["raw"] = {"bbox": ex._bbox([pt for seg in raw for pt in seg]), "segments": raw}
    primitive["normalized"] = {"bbox": ex._bbox([pt for seg in norm for pt in seg]), "segments": norm}
    primitive["length"] = ex._round(sum(ex._distance(*seg) for seg in raw))
    primitive["length_norm"] = ex._round(sum(ex._distance(*seg) for seg in norm))
    first, last = raw[0][0], raw[-1][1]
    angle = math.degrees(math.atan2(last[1] - first[1], last[0] - first[0])) if first != last else None
    primitive["angle_degrees"] = ex._round(angle, 3) if angle is not None else None
    primitive["segment_count"] = len(raw)
    if len(item_indexes) == original_count:
        primitive["item_indexes"] = [item_indexes[i] for i in keep]
    if len(source_kinds) == original_count:
        primitive["source_kinds"] = [source_kinds[i] for i in keep]
    if len(keep) != original_count:
        # a path that lost segments is no longer a closed contour
        primitive["closed"] = False
    return primitive


def _renumber(primitives: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for index, primitive in enumerate(primitives, 1):
        primitive["id"] = f"primitive-{index}"
    return primitives


def _renumber_texts(texts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for index, text in enumerate(texts, 1):
        text["id"] = f"text-{index}"
    return texts


def _pick_deletion_region(description: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    """A spatially contiguous, visible element: prefer a topology component."""
    candidates = []
    for component in description["topology"].get("components") or []:
        bbox = component.get("bbox_norm")
        if not bbox:
            continue
        area = max(0.0, bbox[2] - bbox[0]) * max(0.0, bbox[3] - bbox[1])
        if 0.004 <= area <= 0.30 and component.get("segment_count", 0) >= 4:
            candidates.append((component, area))
    if candidates:
        candidates.sort(key=lambda item: -item[1])
        component, area = candidates[rng.randrange(0, min(len(candidates), 8))]
        return {"source": "topology_component", "component_id": component["id"],
                "bbox_norm": list(component["bbox_norm"]), "area_norm": round(area, 5)}
    # fallback: the densest cell of a 4x4 grid
    counts: collections.Counter[tuple[int, int]] = collections.Counter()
    for x, y in _segment_midpoints(description):
        counts[(min(3, int(x * 4)), min(3, int(y * 4)))] += 1
    if not counts:
        raise ValueError("block has no geometry to delete")
    (col, row), _ = counts.most_common(1)[0]
    return {"source": "grid_cell_4x4", "component_id": None,
            "bbox_norm": [col / 4, row / 4, (col + 1) / 4, (row + 1) / 4],
            "area_norm": 0.0625}


def _mutate_deleted_object(description: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    region = _pick_deletion_region(description, rng)
    pad = 0.004
    x0, y0, x1, y1 = region["bbox_norm"]
    x0, y0, x1, y1 = x0 - pad, y0 - pad, x1 + pad, y1 + pad

    def inside(px: float, py: float) -> bool:
        return x0 <= px <= x1 and y0 <= py <= y1

    kept_primitives: list[dict[str, Any]] = []
    removed_segments = 0
    removed_primitives = 0
    for primitive in description["geometry"]["primitives"]:
        keep = []
        for index, (start, end) in enumerate(primitive["normalized"]["segments"]):
            mx, my = (start[0] + end[0]) / 2.0, (start[1] + end[1]) / 2.0
            if inside(mx, my):
                removed_segments += 1
            else:
                keep.append(index)
        if len(keep) == len(primitive["normalized"]["segments"]):
            kept_primitives.append(primitive)
            continue
        rebuilt = _rebuild_primitive(primitive, keep)
        if rebuilt is None:
            removed_primitives += 1
        else:
            kept_primitives.append(rebuilt)
    removed_texts = [t for t in description["texts"]
                     if inside(t.get("x_norm", -9), t.get("y_norm", -9))]
    kept_texts = [t for t in description["texts"]
                  if not inside(t.get("x_norm", -9), t.get("y_norm", -9))]
    description["geometry"]["primitives"] = _renumber(kept_primitives)
    description["texts"] = _renumber_texts([dict(t) for t in kept_texts])
    return {
        "region": region,
        "region_padded_bbox_norm": [round(v, 5) for v in (x0, y0, x1, y1)],
        "where": _position_phrase(region["bbox_norm"]),
        "segments_removed": removed_segments,
        "primitives_fully_removed": removed_primitives,
        "texts_removed": len(removed_texts),
        "removed_text_sample": [_visible(t["text"]) for t in removed_texts[:8]],
    }


def _mutate_wrong_count(description: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    repeated = description.get("repeated_elements") or []
    if repeated:
        index = 0
        entry = repeated[index]
        true_count = entry["count"]
        delta = max(2, int(round(true_count * 0.4)))
        if rng.random() < 0.5 and true_count - delta >= 2:
            new_count = true_count - delta
        else:
            new_count = true_count + delta
        entry["count"] = new_count
        instances = entry.get("instances") or []
        if new_count < len(instances):
            entry["instances"] = instances[:new_count]
        elif instances and new_count > len(instances) and not entry.get("instances_truncated"):
            while len(entry["instances"]) < min(new_count, 100):
                entry["instances"].append(list(instances[rng.randrange(len(instances))]))
        target = {"field": "repeated_elements[0].count", "pattern_id": entry.get("pattern_id")}
    else:
        summary = description["primitive_summary"]
        true_count = summary["text_items"]
        delta = max(2, int(round(true_count * 0.4)))
        new_count = max(0, true_count - delta) if rng.random() < 0.5 else true_count + delta
        summary["text_items"] = new_count
        description["size_metrics"]["compact_payload"]["summary"]["text_items"] = new_count
        target = {"field": "primitive_summary.text_items", "pattern_id": None}
    return {"target": target, "true_count": true_count, "stated_count": new_count,
            "delta": new_count - true_count}


def _mutate_missing_labels(description: dict[str, Any], rng: random.Random, n: int = 3) -> dict[str, Any]:
    texts = description["texts"]
    readable = [t for t in texts
                if not _CTRL_RE.search(t["text"])
                and _WORDY_RE.search(t["text"])
                and len(t["text"].strip()) >= 2]
    # Use exactly the ordering the fact sheet's "largest lettering" claim uses, so
    # the dropped strings are the ones the sheet would otherwise have named: the
    # corruption is then detectable in principle, not only by counting.
    readable = sorted(readable, key=lambda t: (-t.get("font_size", 0.0), t.get("x_norm", 0.0)))
    chosen, seen = [], set()
    for item in readable:
        key = item["text"].strip()
        if key in seen:
            continue
        seen.add(key)
        chosen.append(item)
        if len(chosen) >= n:
            break
    ids = {t["id"] for t in chosen}
    description["texts"] = _renumber_texts([dict(t) for t in texts if t["id"] not in ids])
    return {
        "dropped": [
            {"text": _visible(t["text"]), "font_size": t.get("font_size"),
             "bbox_norm": t.get("bbox_norm"), "where": _position_phrase(t.get("bbox_norm", [0, 0, 0, 0]))}
            for t in chosen
        ],
        "dropped_count": len(chosen),
        "selection": "the sheet's own 'largest lettering' entries (letters/digits only)",
        "texts_before": len(texts),
        "texts_after": len(description["texts"]),
    }


def _mutate_wrong_topology(description: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    topology = description["topology"]
    before = {
        "connected_components": topology["connected_components"],
        "branch_points": topology["branch_points"],
        "endpoints": topology["endpoints"],
    }
    mode = "merged" if rng.random() < 0.5 else "split"
    if mode == "merged":
        components = max(1, int(round(before["connected_components"] * 0.15)))
        branch = int(round(before["branch_points"] * 1.6)) + 3
    else:
        components = int(round(before["connected_components"] * 3)) + 3
        branch = max(0, int(round(before["branch_points"] * 0.4)))
    if abs(components - before["connected_components"]) < 2:
        components = before["connected_components"] + 3
    if abs(branch - before["branch_points"]) < 2:
        branch = before["branch_points"] + 3
    topology["connected_components"] = components
    topology["branch_points"] = branch
    description["primitive_summary"]["connected_components"] = components
    compact = description["size_metrics"]["compact_payload"]
    compact["summary"]["connected_components"] = components
    compact["topology"]["connected_components"] = components
    compact["topology"]["branch_points"] = branch
    return {
        "mode": mode,
        "before": before,
        "after": {"connected_components": components, "branch_points": branch},
        "not_adjusted": ["topology.components list", "topology.degree_histogram",
                         "structural_signature.*"],
    }


def _break_string(text: str, table: dict[str, str], rng: random.Random) -> str:
    out = []
    for ch in text:
        if _CYR_RE.match(ch):
            if ch not in table:
                table[ch] = BROKEN_ALPHABET[rng.randrange(len(BROKEN_ALPHABET))]
            out.append(table[ch])
        else:
            out.append(ch)
    return "".join(out)


def _mutate_broken_text(description: dict[str, Any], rng: random.Random,
                        fraction: float = 0.6) -> dict[str, Any]:
    texts = description["texts"]
    targets = [t for t in texts if _CYR_RE.search(t["text"])]
    selection = "cyrillic_spans"
    if len(targets) < 3:
        # vk_nodes-style blocks carry no Cyrillic left to break; escalate to any
        # still-readable span so the mutation has something to corrupt.
        targets = [t for t in texts if not _CTRL_RE.search(t["text"]) and len(t["text"]) >= 2]
        selection = "readable_spans_fallback"
    chosen = targets[: max(1, int(round(len(targets) * fraction)))] if targets else []
    table: dict[str, str] = {}
    changed = []
    for item in chosen:
        before = item["text"]
        after = _break_string(before, table, rng)
        if after == before:
            after = "".join(BROKEN_ALPHABET[rng.randrange(len(BROKEN_ALPHABET))] for _ in before)
        item["text"] = after
        item["category"] = "engineering_value" if ex._ENGINEERING_RE.search(after) else (
            "numeric" if ex._VALUE_RE.fullmatch(after) else "label")
        changed.append({"before": before, "after": _visible(after)})
    return {
        "mimics": "incomplete /ToUnicode CMap (finding O8): Cyrillic codes fall back to raw bytes",
        "target_selection": selection,
        "texts_total": len(texts),
        "texts_broken": len(changed),
        "sample": changed[:8],
        "glyph_table_size": len(table),
    }


def _mutate_capped_geometry(description: dict[str, Any], rng: random.Random,
                            fraction: float = 0.15) -> dict[str, Any]:
    primitives = description["geometry"]["primitives"]
    before_count = len(primitives)
    before_segments = sum(p["segment_count"] for p in primitives)
    ordered_all = sorted(
        primitives,
        key=lambda item: (item["type"] not in {"line", "polyline"}, item["closed"], item["length_norm"]),
        reverse=True,
    )
    # The real cap counts primitives, but what it costs is SEGMENTS, so aim at a
    # segment budget: keep longest-first until `fraction` of the segments is used.
    budget = max(1.0, before_segments * fraction)
    cap, used = 0, 0
    for item in ordered_all:
        if cap and used + item["segment_count"] > budget:
            break
        used += item["segment_count"]
        cap += 1
    cap = max(1, min(cap, before_count - 1)) if before_count > 1 else 1
    ordered = ordered_all[:cap]
    kept_ids = {p["id"] for p in ordered}
    kept = [p for p in primitives if p["id"] in kept_ids]
    description["geometry"]["primitives"] = _renumber([dict(p) for p in kept])
    extraction = description["geometry"]["extraction"]
    extraction["storage_cap"] = cap
    extraction["primitives_uncapped"] = max(extraction.get("primitives_uncapped", before_count), before_count)
    extraction["storage_capped"] = True
    after_segments = sum(p["segment_count"] for p in description["geometry"]["primitives"])
    return {
        "mimics": "extractor.DEFAULT_STORAGE_CAP longest-first truncation (finding O11)",
        "primitives_before": before_count,
        "primitives_after": cap,
        "segments_before": before_segments,
        "segments_after": after_segments,
        "segment_fraction_kept": round(after_segments / max(1, before_segments), 4),
    }


def mutate(description: dict[str, Any], kind: str,
           rng: random.Random | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """Corrupt a copy of ``description``; return ``(mutated, ground_truth)``.

    Every mutation that changes geometry or texts goes through
    ``rebuild_derived``, so the mutated description is internally consistent —
    the verifier cannot detect it by arithmetic alone, only by looking.
    Mutations that model a *reporting* error (``wrong_count``,
    ``wrong_topology``) deliberately leave the geometry intact and are applied
    after the rebuild; ``ground_truth.not_adjusted`` names what stays stale.
    """
    if kind not in MUTATION_KINDS:
        raise ValueError(f"unknown mutation kind: {kind}")
    rng = rng or random.Random(0)
    mutated = copy.deepcopy(description)
    ground_truth: dict[str, Any] = {"kind": kind, "corrupted": kind != "clean"}

    if kind == "clean":
        ground_truth["detail"] = {"note": "control; description returned unchanged"}
        return mutated, ground_truth

    if schema_of(description) != "v0.1":
        raise NotImplementedError("mutations are implemented for the v0.1 schema only")

    if kind == "deleted_object":
        detail = _mutate_deleted_object(mutated, rng)
        rebuild_derived(mutated)
    elif kind == "missing_labels":
        detail = _mutate_missing_labels(mutated, rng)
        rebuild_derived(mutated)
    elif kind == "broken_text":
        detail = _mutate_broken_text(mutated, rng)
        rebuild_derived(mutated)
    elif kind == "capped_geometry":
        detail = _mutate_capped_geometry(mutated, rng)
        rebuild_derived(mutated)
    elif kind == "wrong_count":
        detail = _mutate_wrong_count(mutated, rng)
    elif kind == "wrong_topology":
        detail = _mutate_wrong_topology(mutated, rng)
    else:  # pragma: no cover
        raise AssertionError(kind)

    ground_truth["detail"] = detail
    return mutated, ground_truth


# ---------------------------------------------------------------------- crops


def render_crop(pdf: str | Path, page_index: int, bbox_norm: Sequence[float],
                out_png: str | Path, zoom: float = 1.35) -> Path:
    """Raster crop of a block, same recipe as Track A's diagnostics PNGs."""
    import fitz  # imported lazily so the module loads without PyMuPDF

    pdf = Path(pdf)
    if not pdf.is_absolute():
        pdf = ROOT / pdf
    out_png = Path(out_png)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    document = fitz.open(pdf)
    page = document[page_index]
    rect = fitz.Rect(
        bbox_norm[0] * page.rect.width,
        bbox_norm[1] * page.rect.height,
        bbox_norm[2] * page.rect.width,
        bbox_norm[3] * page.rect.height,
    )
    pixmap = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=rect, alpha=False)
    pixmap.save(out_png)
    document.close()
    return out_png


def crop_for(pair_id: str, side: str) -> Path:
    """Track A already rendered these; reuse rather than re-render."""
    path = DIAGNOSTICS / pair_id / f"{side}.png"
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def crop_for_description(description: dict[str, Any], out_png: str | Path,
                         zoom: float = 1.35) -> Path:
    source = description["source"]["pdf"] if schema_of(description) == "v0.1" else description["source"]["pdf"]
    page_index = description.get("page_index", description.get("source", {}).get("page_index"))
    bbox = description.get("bbox_norm_on_page") or description["source"]["bbox_norm"]
    return render_crop(source, int(page_index), bbox, out_png, zoom)


# --------------------------------------------------------------------- verify


PROMPT_TEMPLATE = """You are verifying a machine-generated description of ONE block cut out of an engineering drawing (Russian design documentation). Read the image file ./{png} with the Read tool. It is the raster rendering of exactly that block.

Below is a FACT SHEET: a short list of claims a program derived from the PDF's vector layer, without looking at the picture. Your only job is to check those claims against the picture.

FACT SHEET
{sheet}

RULES — follow them exactly:
- Do NOT re-describe the drawing. Do not produce your own inventory of what is in it.
- Do NOT invent coordinates and do NOT invent exact numbers. If a claim states a count you cannot count in the picture (hundreds of items, sub-millimetre strokes), do not guess a number — judge only whether the stated number is plausible for what you see, and say so.
- You may confirm a claim, reject a claim, or report that something plainly visible in the picture is absent from the fact sheet. Nothing else.
- "suspicious" is for a claim that contradicts the picture or is implausible for it. "missing" is for something clearly visible in the picture that no claim covers (name it in words, e.g. "a symbol in the lower-left corner", "the label QF3").
- Judge each claim by its id (C1, C2, ...).

Status rule:
- VERIFIED — every claim you could check holds, and nothing plainly visible is unaccounted for.
- PARTIAL — the claims that hold are usable, but a named gap exists (something missing, or one claim you cannot confirm).
- FAILED — at least one claim contradicts the picture, so the description is not a safe basis for comparing two versions of this drawing.

Answer with a single JSON object and nothing else:
{{"status": "VERIFIED"|"PARTIAL"|"FAILED", "verified": ["C1", ...], "missing": ["short phrase", ...], "suspicious": [{{"claim_id": "C4", "why": "short phrase"}}, ...], "confidence": "high"|"medium"|"low"}}
"""


def build_prompt(sheet: dict[str, Any], png_name: str = "crop.png") -> str:
    return PROMPT_TEMPLATE.format(png=png_name, sheet=sheet["text"])


_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def parse_verdict(result_text: str) -> dict[str, Any] | None:
    """Pull the answer JSON out of the model's text, fenced or not."""
    if not result_text:
        return None
    fenced = re.findall(r"```(?:json)?\s*(.*?)```", result_text, re.DOTALL)
    candidates = [block.strip() for block in fenced]
    candidates.append(result_text.strip())
    match = _JSON_RE.search(result_text)
    if match:
        candidates.append(match.group(0))
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            inner = _JSON_RE.search(candidate)
            if not inner:
                continue
            try:
                parsed = json.loads(inner.group(0))
            except Exception:
                continue
        if isinstance(parsed, dict) and "status" in parsed:
            return parsed
    return None


def _payload_tokens(usage: dict[str, Any] | None) -> int | None:
    if not usage:
        return None
    return int(usage.get("input_tokens", 0)) + int(usage.get("cache_creation_input_tokens", 0)) + \
        int(usage.get("output_tokens", 0))


def verify(crop_png: str | Path, sheet: dict[str, Any], out_json: str | Path | None = None,
           *, timeout: int = 300, retries: int = 1, model: str | None = None) -> dict[str, Any]:
    """Real multimodal call: Claude Code CLI reads the crop and checks the sheet."""
    crop_png = Path(crop_png)
    if not crop_png.exists():
        raise FileNotFoundError(crop_png)
    prompt = build_prompt(sheet)
    attempts: list[dict[str, Any]] = []
    record: dict[str, Any] = {
        "crop_png": str(crop_png),
        "crop_bytes": crop_png.stat().st_size,
        "fact_sheet_characters": sheet["characters"],
        "prompt_characters": len(prompt),
        "block_id": sheet.get("block_id"),
    }
    for attempt in range(retries + 1):
        workdir = Path(tempfile.mkdtemp(prefix="vv_verify_"))
        try:
            shutil.copy2(crop_png, workdir / "crop.png")
            cmd = ["claude", "-p", prompt, "--allowed-tools", "Read", "--output-format", "json"]
            if model:
                cmd += ["--model", model]
            started = time.time()
            with open(os.devnull, "rb") as devnull:
                proc = subprocess.run(cmd, cwd=workdir, stdin=devnull, capture_output=True,
                                      text=True, timeout=timeout)
            wall = time.time() - started
            envelope = None
            try:
                envelope = json.loads(proc.stdout)
            except Exception:
                pass
            result_text = (envelope or {}).get("result") if isinstance(envelope, dict) else None
            verdict = parse_verdict(result_text or proc.stdout)
            attempts.append({
                "attempt": attempt,
                "returncode": proc.returncode,
                "wall_seconds": round(wall, 2),
                "duration_ms": (envelope or {}).get("duration_ms") if isinstance(envelope, dict) else None,
                "usage_raw": (envelope or {}).get("usage") if isinstance(envelope, dict) else None,
                "total_cost_usd": (envelope or {}).get("total_cost_usd") if isinstance(envelope, dict) else None,
                "model_text": result_text,
                "stderr_tail": proc.stderr[-800:] if proc.stderr else "",
                "parsed": verdict,
            })
            if proc.returncode == 0 and verdict is not None:
                break
        except subprocess.TimeoutExpired:
            attempts.append({"attempt": attempt, "returncode": None, "error": "timeout",
                             "wall_seconds": timeout})
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    last = attempts[-1]
    usage = last.get("usage_raw")
    record.update({
        "attempts": attempts,
        "ok": bool(last.get("parsed")),
        "verdict": last.get("parsed"),
        "status": (last.get("parsed") or {}).get("status"),
        "usage_raw": usage,
        "usage_payload_attributable": _payload_tokens(usage),
        "usage_note": ("cache_read_input_tokens is dominated by the Claude Code system prompt "
                       "(~50k) and is NOT attributable to our payload; "
                       "usage_payload_attributable = input + cache_creation + output"),
        "duration_ms": last.get("duration_ms"),
        "wall_seconds": last.get("wall_seconds"),
        "prompt": prompt,
    })
    if out_json:
        out = Path(out_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return record


# ----------------------------------------------------------------- manifest io


def materialize_case(case: dict[str, Any], manifest: dict[str, Any] | None = None
                     ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Rebuild ``(description, fact_sheet, ground_truth)`` for one manifest case."""
    manifest = manifest or json.loads(CASES_JSON.read_text(encoding="utf-8"))
    block = manifest["blocks"][case["block"]]
    description = load_description(ROOT / block["description_path"])
    mutated, ground_truth = mutate(description, case["mutation"], random.Random(case["seed"]))
    sheet = fact_sheet(mutated, disclose_limits=case["disclose_limits"])
    return mutated, sheet, ground_truth


# ------------------------------------------------------------------- selftest


def _jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=dict))


def _selftest() -> int:
    failures = 0
    pairs = sorted(p.name for p in DESCRIPTIONS.iterdir() if p.is_dir())
    for pair in pairs:
        for side in ("left", "right"):
            path = description_path(pair, side)
            if not path.exists():
                continue
            original = load_description(path)
            rebuilt = rebuild_derived(copy.deepcopy(original))
            for field in ("topology", "anchors", "repeated_elements", "hatch_like_structures",
                          "dimensions", "labels", "primitive_summary", "structural_signature",
                          "vector_quality", "quality_notes", "size_metrics"):
                # compare after a JSON round-trip: the extractor returns tuples and
                # Counters where the stored file holds lists and plain dicts.
                if _jsonable(rebuilt[field]) != _jsonable(original[field]):
                    print(f"FAIL rebuild_derived {pair}/{side}: {field} differs")
                    failures += 1
            sheet = fact_sheet(original)
            if sheet["characters"] > 1200:
                print(f"FAIL fact_sheet too long {pair}/{side}: {sheet['characters']}")
                failures += 1
            print(f"ok {pair}/{side}: sheet {sheet['characters']} chars, "
                  f"{len(sheet['claims'])} claims, quality {original['vector_quality']}")
    print(f"selftest failures: {failures}")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["selftest", "sheet"], nargs="?", default="selftest")
    parser.add_argument("--pair")
    parser.add_argument("--side", default="left")
    parser.add_argument("--mutation", default="clean")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--no-disclose", action="store_true")
    args = parser.parse_args()
    if args.command == "selftest":
        sys.exit(1 if _selftest() else 0)
    description = load_description(description_path(args.pair, args.side))
    mutated, ground_truth = mutate(description, args.mutation, random.Random(args.seed))
    sheet = fact_sheet(mutated, disclose_limits=not args.no_disclose)
    print(json.dumps({"ground_truth": ground_truth, "sheet": sheet}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
