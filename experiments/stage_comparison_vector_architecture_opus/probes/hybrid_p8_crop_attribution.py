#!/usr/bin/env python3
"""Probe HYBRID-8: how many "changes" are crop artefacts, and can that be decided
deterministically without Vision?

For every text string the comparator reports as added (right-only) or removed
(left-only), ask the OTHER document's PAGE (not block) whether the string exists
at all, and whether it sits outside the compared block bbox. If it does, the
"change" is a crop-window difference, not a design change.

Uses the page text layer only — no OCR, no Vision, no model.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p8_crop_attribution
"""
from __future__ import annotations

import json
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
TA = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts"
OUT = Path(__file__).resolve().parents[1] / "artifacts"


def intersects(a, b, eps: float = 0.5) -> bool:
    return not (a[2] < b[0] - eps or a[0] > b[2] + eps or a[3] < b[1] - eps or a[1] > b[3] + eps)


def page_spans(pdf: str, page_index: int):
    doc = fitz.open(ROOT / pdf)
    page = doc[page_index]
    rows = []
    for blk in page.get_text("dict")["blocks"]:
        for line in blk.get("lines", []):
            for span in line.get("spans", []):
                rows.append((span["text"], tuple(span["bbox"])))
    rect = (page.rect.width, page.rect.height)
    doc.close()
    return rows, rect


def main() -> None:
    pairs = json.loads((TA / "block_pairs.json").read_text("utf-8"))["pairs"]
    res = {}
    for p in pairs:
        pid = p["pair_id"]
        c = json.loads((TA / "comparisons" / pid / "comparison.json").read_text("utf-8"))
        added, removed = c["text"]["added"], c["text"]["removed"]
        out = {"left": [], "right": []}
        for side, other, strings in (("right", "left", added), ("left", "right", removed)):
            if not strings:
                continue
            spans, (pw, ph) = page_spans(p[other]["pdf"], p[other]["page_index"])
            own = p[side]["bbox_norm"]      # crop that DID contain the string
            oth = p[other]["bbox_norm"]     # crop that did not
            rect_oth = (oth[0] * pw, oth[1] * ph, oth[2] * pw, oth[3] * ph)
            rect_own = (own[0] * pw, own[1] * ph, own[2] * pw, own[3] * ph)
            attributable = []
            for st in strings:
                needle = st.strip()
                if len(needle) < 3:
                    continue  # too short to identify uniquely; never attributed
                inside_oth = band_hit = None
                for text, bb in spans:
                    if needle not in text:
                        continue
                    # PyMuPDF get_text(clip=...) keeps spans that INTERSECT the rect,
                    # so intersection is the right containment test here.
                    in_oth = intersects(bb, rect_oth)
                    in_own = intersects(bb, rect_own)
                    if in_oth:
                        inside_oth = inside_oth or bb
                    elif in_own:
                        band_hit = band_hit or bb   # in the crop-difference band
                # crop artefact: the string lives in the strip one crop keeps and the
                # other drops, and appears nowhere inside the other crop
                if band_hit and not inside_oth:
                    attributable.append([st, [round(v, 1) for v in band_hit],
                                         [round(v, 1) for v in rect_oth], [round(v, 1) for v in rect_own]])
            out[side] = attributable
        n_events = len(added) + len(removed)
        n_attr = len(out["right"]) + len(out["left"])
        res[pid] = {
            "events": n_events,
            "crop_attributable": n_attr,
            "pct": round(100.0 * n_attr / max(n_events, 1), 1),
            "note": "attributed only when the string occurs on the other page inside THIS side's crop rect but outside the OTHER side's crop rect, and nowhere inside the other side's crop rect; strings <3 chars never attributed",
            "bbox_delta_pt": None,
            "examples": (out["right"] + out["left"])[:4],
            "attributable_strings_right_only": [row[0] for row in out["right"]],
            "attributable_strings_left_only": [row[0] for row in out["left"]],
        }
        # block bbox delta in page points, both sides
        spansL, (pwL, phL) = page_spans(p["left"]["pdf"], p["left"]["page_index"])
        bl, br = p["left"]["bbox_norm"], p["right"]["bbox_norm"]
        res[pid]["bbox_delta_pt"] = [round((br[i] - bl[i]) * (pwL if i % 2 == 0 else phL), 2) for i in range(4)]
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "hybrid_crop_attribution.json").write_text(json.dumps(res, ensure_ascii=False, indent=2) + "\n", "utf-8")
    print(f"{'pair':24s} {'events':>7} {'crop_attr':>10} {'pct':>6}  bbox_delta_pt [x0,y0,x1,y1]")
    for k, v in res.items():
        print(f"{k:24s} {v['events']:>7} {v['crop_attributable']:>10} {v['pct']:>6}  {v['bbox_delta_pt']}")
    print()
    for k in ("ss_table_graphic", "eom_singleline_changed", "ss_scheme_text_changed"):
        print("==", k, json.dumps(res[k].get("examples", []), ensure_ascii=False)[:600])


if __name__ == "__main__":
    main()
