#!/usr/bin/env python3
"""USEFULNESS: 22 hand-picked spans, true referent read off the rendered crop by a human,
compared with what the deterministic relations actually recover.

Crops used (rendered by txgeo_render_crops.py, never read by the detectors):
  artifacts/txgeo_crops/ss_simple_node_left.png      (rendered ad hoc from the Track A description)
  artifacts/txgeo_crops/fresh_kj_sections_left.png
  artifacts/txgeo_crops/fresh_ar_lintels_left.png
  artifacts/txgeo_crops/fresh_ar_legend_left.png

    python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_usefulness
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
REL = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/txgeo_relations/line"
ART = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

MM_PER_PT = 25.4 / 72.0

# block, text, occurrence, human-read true referent, relation type that SHOULD carry it,
# true printed value (for dimensions) and the drawing scale read off the sheet
GROUND_TRUTH = [
    ("ss_simple_node", 'Кабель"витаяпара",кат.5е, 4пары', 0,
     "the incoming twisted-pair cable line entering the block from the left", "leader", None, None),
    ("ss_simple_node", "КР", 0, "the large outer rectangle (кроссовое помещение)", "contour_caption", None, None),
    ("ss_simple_node", "БГЗ", 0, "the inner rectangle (блок грозозащиты)", "contour_caption", None, None),
    ("ss_simple_node", "IN", 0, "interior of the БГЗ rectangle, input side", "enclosure_tight", None, None),
    ("ss_simple_node", "RJ-45 (PoE)", 0, "interior of the ВК rectangle (видеокамера)", "enclosure_tight", None, None),
    ("ss_simple_node", "кОСПД3.1/ОСПД6.3", 0, "the horizontal cable line running to the left edge", "leader", None, None),
    ("fresh_kj_sections", "Засыпка", 0, "the керамзит backfill hatch in section 1-1 (two branches)", "leader", None, None),
    ("fresh_kj_sections", "2650", 0, "distance between axes П.Т and П.С in section 1-1", "dimension_interval", 2650, 50),
    ("fresh_kj_sections", "1370", 0, "vertical distance between marks -0,430 and -1,800 in section 1-1", "dimension_interval", 1370, 50),
    ("fresh_kj_sections", "1370", 1, "vertical distance between marks -0,430 and -1,800 in section 3-3", "dimension_interval", 1370, 50),
    ("fresh_kj_sections", "300", 0, "wall thickness at the bottom of section 3-3", "dimension_interval", 300, 50),
    ("fresh_kj_sections", "250", 0, "slab thickness at the top of section 3-3", "dimension_interval", 250, 50),
    ("fresh_kj_sections", "П.Т", 0, "the axis line the circle mark hangs on", "symbol_cluster", None, None),
    ("fresh_kj_sections", "-0,430", 0, "the level line with the elevation arrow in section 1-1", "elevation_mark(MISSING TYPE)", None, None),
    ("fresh_kj_sections", "400", 1, "bottom vertical dimension in section 3-3", "dimension_interval", 400, 50),
    ("fresh_ar_lintels", "40", 0, "the 40 mm leg of the уголок 40x4 in the ПР-11 section", "dimension_interval", 40, 10),
    ("fresh_ar_lintels", "200", 0, "the 200 mm width of the ПР-11 lintel section", "dimension_interval", 200, 10),
    ("fresh_ar_lintels", "Уголок40х4, 2шт.", 0, "the angle steel drawn in the ПР-11 section, reached by an arrow leader", "leader", None, None),
    ("fresh_ar_lintels", "ПР-11", 0, "table cell in column «Марка перемычки», row 1", "grid_cell", None, None),
    ("fresh_ar_lintels", "Анкер", 0, "the anchor symbol in the ПР-12 wall view, reached by an arrow leader", "leader", None, None),
    ("fresh_ar_legend", "С-10,кирпичные стены- 120мм", 0, "the hatch swatch immediately to the left in the same legend row", "band_association", None, None),
    ("fresh_ar_legend", "+4.500", 0, "the boxed elevation symbol drawn around the text", "symbol_cluster", None, None),
]


def main() -> None:
    cache: dict[str, list] = {}
    rows = []
    for block, text, occ, referent, should, value, scale in GROUND_TRUTH:
        if block not in cache:
            cache[block] = json.loads((REL / block / "left.json").read_text(encoding="utf-8"))["units"]
        matches = [u for u in cache[block] if u["text"].strip() == text]
        if occ >= len(matches):
            rows.append({"block": block, "text": text, "state": "TEXT_NOT_FOUND"})
            continue
        u = matches[occ]
        fired = [k for k, v in u["relations"].items()
                 if v.get("hit") and k not in ("nearest_geometry", "text_alignment", "enclosure_loose")]
        row = {
            "block": block, "text": text, "occurrence": occ,
            "human_referent": referent,
            "relation_that_should_carry_it": should,
            "detector_primary": u["primary"],
            "relations_fired": fired,
            "type_fired": should in fired,
        }
        if value is not None:
            rel = u["relations"].get("dimension_interval", {})
            if rel.get("hit"):
                mm = rel["measured_len_pt"] * MM_PER_PT * scale
                row["printed_value_mm"] = value
                row["measured_mm_at_sheet_scale"] = round(mm, 1)
                row["value_error_pct"] = round(abs(mm / value - 1.0) * 100, 1)
                row["verdict"] = "RECOVERED" if row["value_error_pct"] <= 2 else "WRONG_VALUE"
                row["ticks_in_reach"] = rel.get("ticks_in_reach")
                row["line_candidates"] = rel.get("candidates")
            else:
                row["printed_value_mm"] = value
                row["verdict"] = "NOT_RECOVERED"
        else:
            row["verdict"] = "RECOVERED" if u["primary"] == should else (
                "PARTIAL" if row["type_fired"] or u["primary"] in {"leader", "symbol_cluster", "enclosure_tight",
                                                                   "contour_caption", "band_association"} else "NOT_RECOVERED")
        rows.append(row)

    summary = {}
    for r in rows:
        summary[r.get("verdict", "?")] = summary.get(r.get("verdict", "?"), 0) + 1
    out = {"n": len(rows), "summary": summary, "rows": rows}
    (ART / "txgeo_usefulness.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    for r in rows:
        extra = ""
        if "measured_mm_at_sheet_scale" in r:
            extra = f"  printed={r['printed_value_mm']}mm measured={r['measured_mm_at_sheet_scale']}mm err={r['value_error_pct']}% ticks={r['ticks_in_reach']} lines={r['line_candidates']}"
        print(f"{r['block'][:18]:19s} {r['text'][:30]:32s} want={r['relation_that_should_carry_it'][:22]:24s} "
              f"got={r['detector_primary']:20s} {r['verdict']:14s}{extra}")


if __name__ == "__main__":
    main()
