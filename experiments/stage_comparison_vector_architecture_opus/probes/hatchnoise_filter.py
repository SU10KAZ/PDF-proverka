"""Discipline-free foreground/background filter, decided per PRIMITIVE (whole PDF path).

Deciding per *segment* is unsafe: a flattened arc is 6-24 micro segments and any
"tiny segment" rule deletes every curve in the drawing (measured in P2 v1:
469 649 of 505 213 vk_plan segments were shorter than 0.0008 of the block, and the
door swings / sanitary fixtures vanished).  So each rule below votes on the path.

Rules (each independently switchable so its own selectivity can be measured):

  P1 hatch_family        short simple stroke that belongs to a locally repeated
                         parallel same-style family
  P2 tiny_repeated_motif small path whose shape fingerprint repeats many times
  P3 light_stroke        light grey stroke (screened underlay)
  P4 dust                path whose total length is below a floor
"""
from __future__ import annotations

import collections
import math
from typing import Any, Sequence

DEFAULTS = {
    "angle_bucket_deg": 5.0,
    "length_decades": 6.0,
    "max_hatch_length": 0.06,
    "max_hatch_segments": 3,
    "min_family": 24,
    "support_radius": 0.02,
    "min_support": 5,
    "tiny_motif_max_len": 0.02,
    "tiny_motif_min_count": 12,
    "light_stroke_luminance": 0.62,
    "dust_length": 0.0015,
}
RULES = ("P1", "P2", "P3", "P4")


def primitive_view(rows: Sequence[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[int, list[int]]]:
    """Collapse the segment table into one record per primitive."""
    by_primitive: dict[int, list[int]] = collections.defaultdict(list)
    for index, row in enumerate(rows):
        by_primitive[row["pi"]].append(index)
    records = []
    for pi, indexes in by_primitive.items():
        first = rows[indexes[0]]
        xs = [rows[i]["mid"][0] for i in indexes]
        ys = [rows[i]["mid"][1] for i in indexes]
        total_length = sum(rows[i]["len"] for i in indexes)
        angles = [rows[i]["ang"] for i in indexes]
        records.append({
            "pi": pi,
            "rows": indexes,
            "n_seg": len(indexes),
            "length": total_length,
            "mid": [sum(xs) / len(xs), sum(ys) / len(ys)],
            "angle": angles[0],
            "width": first["width"],
            "stroke_lum": first["stroke_lum"],
            "filled": first["filled"],
            "closed": first["closed"],
            "motif": first["motif"],
            "layer": first["layer"],
        })
    return records, by_primitive


def _family_key(record: dict[str, Any], params: dict[str, float]) -> tuple:
    angle = record["angle"] % 180.0
    step = params["angle_bucket_deg"]
    angle_bucket = int(round(angle / step)) % int(round(180.0 / step))
    length_bucket = int(round(math.log10(max(record["length"], 1e-6)) * params["length_decades"]))
    return (angle_bucket, length_bucket, round(record["width"], 2), round(record["stroke_lum"], 2), record["filled"])


def classify(rows: Sequence[dict[str, Any]], params: dict[str, float] | None = None,
             rules: Sequence[str] = RULES) -> tuple[list[set[str]], list[dict[str, Any]], list[set[str]]]:
    """Return (per-segment flags, primitive records, per-primitive flags)."""
    settings = dict(DEFAULTS)
    if params:
        settings.update(params)
    records, _ = primitive_view(rows)
    flags: list[set[str]] = [set() for _ in records]

    if "P1" in rules:
        families: dict[tuple, list[int]] = collections.defaultdict(list)
        for index, record in enumerate(records):
            if record["length"] > settings["max_hatch_length"] or record["n_seg"] > settings["max_hatch_segments"]:
                continue
            families[_family_key(record, settings)].append(index)
        radius = settings["support_radius"]
        for members in families.values():
            if len(members) < settings["min_family"]:
                continue
            cells: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
            for index in members:
                mx, my = records[index]["mid"]
                cells[(int(mx / radius), int(my / radius))].append(index)
            for index in members:
                mx, my = records[index]["mid"]
                cx, cy = int(mx / radius), int(my / radius)
                support = 0
                for gx in range(cx - 1, cx + 2):
                    for gy in range(cy - 1, cy + 2):
                        for other in cells.get((gx, gy), ()):
                            if other == index:
                                continue
                            ox, oy = records[other]["mid"]
                            if (ox - mx) ** 2 + (oy - my) ** 2 <= radius * radius:
                                support += 1
                                if support >= settings["min_support"]:
                                    break
                        if support >= settings["min_support"]:
                            break
                    if support >= settings["min_support"]:
                        break
                if support >= settings["min_support"]:
                    flags[index].add("P1")

    if "P2" in rules:
        motif_members: dict[str, list[int]] = collections.defaultdict(list)
        for index, record in enumerate(records):
            motif_members[record["motif"]].append(index)
        for members in motif_members.values():
            if len(members) < settings["tiny_motif_min_count"]:
                continue
            if max(records[i]["length"] for i in members) > settings["tiny_motif_max_len"]:
                continue
            for index in members:
                flags[index].add("P2")

    if "P3" in rules:
        for index, record in enumerate(records):
            if not record["filled"] and record["stroke_lum"] >= settings["light_stroke_luminance"]:
                flags[index].add("P3")

    if "P4" in rules:
        for index, record in enumerate(records):
            if record["length"] < settings["dust_length"]:
                flags[index].add("P4")

    segment_flags: list[set[str]] = [set() for _ in rows]
    for index, record in enumerate(records):
        if flags[index]:
            for row_index in record["rows"]:
                segment_flags[row_index] = flags[index]
    return segment_flags, records, flags
