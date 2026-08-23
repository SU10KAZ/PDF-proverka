# -*- coding: utf-8 -*-
"""CNS-3 — deterministic taxonomy rules over cns_features.jsonl.

Every rule is a threshold on a measured feature: no learning, no randomness,
same input -> same class.  Order matters, first match wins.

Thresholds were fixed on the 64-block eye-labelled DEV set
(artifacts/cns_eye_labels_R.json) and then applied unchanged to the hold-out
set H and to the rare-class set T.
"""
from __future__ import annotations

CLASSES = ["empty", "stamp", "raster", "curved_text", "vector_raster_mix",
           "table", "legend_notes", "drawing"]

RULES_DOC = {
    "R1_empty": "n_seg==0 and n_text==0 and n_images==0 — nothing was extracted at all",
    "R2_stamp_upstream": "category_code=='stamp' — upstream already labelled it",
    "R3_raster": "raster_only, or largest raster covers >=50% of the block and n_seg<=200",
    "R4_curved_text": "n_text==0 and n_curves>=20 — no text layer but many Bezier paths: glyphs converted to outlines",
    "R5_stamp_geom": "no category_code: flat (aspect>=1.8), small (page_area_frac<=0.10), ruled grid "
                     "(n_rule_v>=3, len_share_on_rulings>=0.50), text-filled (text_area_share>=0.09, n_text>=5), axis-aligned (share_axis>=0.70)",
    "R6_mix_raster": "raster_coverage>=0.15 next to a vector layer — the block is part bitmap",
    "R7_table": "len_share_on_rulings>=0.55 and text_area_share>=0.09 and n_text>=8 and share_axis>=0.90 "
                "— the ink IS the ruling grid and every cell carries text",
    "R8_legend_notes": "text-dominant, geometry sparse: seg_per_text<=6, text_area_share>=0.08, "
                       "ink_density<=0.030, n_text>=5, len_share_on_rulings<0.50",
    "R9_drawing": "everything else that has a vector layer",
}


def derived(r: dict) -> dict:
    n_text = r.get("n_text", 0)
    n_seg = r.get("n_seg", 0)
    return {
        "seg_per_text": n_seg / max(1, n_text),
        "grid_cells": max(0, r.get("n_rule_h", 0) - 1) * max(0, r.get("n_rule_v", 0) - 1),
    }


def classify_v1(r: dict) -> tuple[str, str]:
    if "error" in r:
        return "error", "R0_error"
    d = derived(r)
    n_seg = r.get("n_seg", 0)
    n_text = r.get("n_text", 0)
    rc = r.get("raster_coverage", 0.0)
    rul = r.get("len_share_on_rulings", 0.0)

    if n_seg == 0 and n_text == 0 and r.get("n_images", 0) == 0:
        return "empty", "R1_empty"
    if r.get("category_code") == "stamp":
        return "stamp", "R2_stamp_upstream"
    if r.get("raster_only") or (rc >= 0.50 and n_seg <= 200):
        return "raster", "R3_raster"
    if n_text == 0 and r.get("n_curves", 0) >= 20:
        return "curved_text", "R4_curved_text"
    if (r.get("aspect", 0.0) >= 1.8 and r.get("page_area_frac", 1.0) <= 0.10
            and r.get("n_rule_v", 0) >= 3 and rul >= 0.50
            and r.get("text_area_share", 0.0) >= 0.09 and n_text >= 5
            and r.get("share_axis", 0.0) >= 0.70):
        return "stamp", "R5_stamp_geom"
    if rc >= 0.15:
        return "vector_raster_mix", "R6_mix_raster"
    if (rul >= 0.55 and r.get("text_area_share", 0.0) >= 0.09 and n_text >= 8
            and r.get("share_axis", 0.0) >= 0.90):
        return "table", "R7_table"
    if (n_text >= 5 and d["seg_per_text"] <= 6.0
            and r.get("text_area_share", 0.0) >= 0.08
            and r.get("ink_density", 0.0) <= 0.030 and rul < 0.50):
        return "legend_notes", "R8_legend_notes"
    return "drawing", "R9_drawing"


# ---------------------------------------------------------------------------
# v2 — two rules of v1 were falsified on the targeted set T (30 blocks, eye):
#   * R5_stamp_geom  precision 0/4: it fired on wide ruled TABLES
#     (экспликация помещений, ведомость деталей), never on a real stamp that
#     upstream had missed.  Removed; stamps are taken from category_code only.
#   * R4_curved_text precision 1/4: "no text layer + many Beziers" also matches
#     ordinary textless drawings full of arcs.  A page-level check separates
#     them: on the one true outlined-text block the WHOLE PDF page carried
#     2 text lines, on the three false ones 44/95/95 (artifacts/cns_curvetext.json).
#   * R7_table share_axis lowered 0.90 -> 0.85 so T23 (a legend table with a
#     couple of slanted leader lines) lands in table instead of drawing.
# v2 needs r["page_text_lines"] (number of text lines on the whole PDF page).
# ---------------------------------------------------------------------------

PAGE_TEXT_LINES_EMPTY = 5


def classify(r: dict) -> tuple[str, str]:
    if "error" in r:
        return "error", "R0_error"
    d = derived(r)
    n_seg = r.get("n_seg", 0)
    n_text = r.get("n_text", 0)
    rc = r.get("raster_coverage", 0.0)
    rul = r.get("len_share_on_rulings", 0.0)

    if n_seg == 0 and n_text == 0 and r.get("n_images", 0) == 0:
        return "empty", "R1_empty"
    if r.get("category_code") == "stamp":
        return "stamp", "R2_stamp_upstream"
    if r.get("raster_only") or (rc >= 0.50 and n_seg <= 200):
        return "raster", "R3_raster"
    if (n_text == 0 and r.get("n_curves", 0) >= 20
            and r.get("page_text_lines", 0) <= PAGE_TEXT_LINES_EMPTY):
        return "curved_text", "R4_curved_text_v2"
    if rc >= 0.15:
        return "vector_raster_mix", "R6_mix_raster"
    if (rul >= 0.55 and r.get("text_area_share", 0.0) >= 0.09 and n_text >= 8
            and r.get("share_axis", 0.0) >= 0.85):
        return "table", "R7_table_v2"
    if (n_text >= 5 and d["seg_per_text"] <= 6.0
            and r.get("text_area_share", 0.0) >= 0.08
            and r.get("ink_density", 0.0) <= 0.030 and rul < 0.50):
        return "legend_notes", "R8_legend_notes"
    return "drawing", "R9_drawing"
