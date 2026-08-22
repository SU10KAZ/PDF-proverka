#!/usr/bin/env python3
"""Probe HYBRID-11: how often would the hybrid Vision triggers fire, and what would
they cost, on the 10-pair benchmark?

Every trigger is a deterministic predicate over Track A's own artifacts. The crop
cost uses the measured formula from HYBRID-5b:
    tokens = min(1.2014 * ceil(w/32)*ceil(h/32) + 48.67, 3051)

    python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p11_trigger_budget
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
TA = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts"
OUT = Path(__file__).resolve().parents[1] / "artifacts"
SLOPE, INTERCEPT, CEIL = 1.2014, 48.67, 3051
ZOOM = 2.0


def img_tokens(w: float, h: float) -> int:
    return int(round(min(SLOPE * math.ceil(w / 32) * math.ceil(h / 32) + INTERCEPT, CEIL)))


def crop_px(pdf: str, page_index: int, bbox_norm, sub=(0.0, 0.0, 1.0, 1.0), zoom: float = ZOOM):
    doc = fitz.open(ROOT / pdf)
    page = doc[page_index]
    pw, ph = page.rect.width, page.rect.height
    doc.close()
    x0, y0 = bbox_norm[0] * pw, bbox_norm[1] * ph
    w, h = (bbox_norm[2] - bbox_norm[0]) * pw, (bbox_norm[3] - bbox_norm[1]) * ph
    return ((sub[2] - sub[0]) * w * zoom, (sub[3] - sub[1]) * h * zoom)


def main() -> None:
    pairs = json.loads((TA / "block_pairs.json").read_text("utf-8"))["pairs"]
    attrib = json.loads((OUT / "hybrid_crop_attribution.json").read_text("utf-8"))
    bound = json.loads((OUT / "hybrid_boundary_evidence.json").read_text("utf-8"))
    rows = {}
    for p in pairs:
        pid = p["pair_id"]
        c = json.loads((TA / "comparisons" / pid / "comparison.json").read_text("utf-8"))
        lq = c["text"]["left_layer_quality"]["status"]
        rq = c["text"]["right_layer_quality"]["status"]
        capped = any(t.get("capped") for t in c["geometry"]["tolerance_experiment"])
        triggers = []
        if "UNDECODABLE" in (lq, rq):
            triggers.append("T1_UNDECODABLE_TEXT")
        if c["geometry"]["encoding_rewrite_suspected"]:
            triggers.append("T4_ENCODING_REWRITE")
        if capped and c["geometry"]["similarity"] < 0.99:
            triggers.append("T5_CAPPED_AND_DISSIMILAR")
        if bound[pid]["prefix_truncation_pairs"] > 0 and attrib[pid]["crop_attributable"] == 0:
            triggers.append("T3_TRUNCATION_UNRESOLVED")
        # cost: one tight crop per side, quarter of the block, zoom 2
        wl, hl = crop_px(p["left"]["pdf"], p["left"]["page_index"], p["left"]["bbox_norm"], (0.0, 0.0, 0.5, 0.5))
        wr, hr = crop_px(p["right"]["pdf"], p["right"]["page_index"], p["right"]["bbox_norm"], (0.0, 0.0, 0.5, 0.5))
        per_task = img_tokens(wl, hl) + img_tokens(wr, hr)
        rows[pid] = {
            "triggers": triggers,
            "n_triggers": len(triggers),
            "quarter_block_crop_px_left": [round(wl), round(hl)],
            "quarter_block_crop_px_right": [round(wr), round(hr)],
            "tokens_per_two_sided_task_zoom2": per_task,
            "vision_tokens_if_all_triggers_fire": per_task * len(triggers),
            "track_a_vector_payload_tokens": None,
        }
    p1 = __import__(
        "experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p1_prompt_composition",
        fromlist=["x"],
    )
    ta = {pl["pair_id"]: p1.tok(pl) for pl in p1.build_payloads()}
    for pid in rows:
        rows[pid]["track_a_vector_payload_tokens"] = ta.get(pid)
    summary = {
        "pairs_with_at_least_one_trigger": sum(1 for v in rows.values() if v["n_triggers"]),
        "total_triggers": sum(v["n_triggers"] for v in rows.values()),
        "total_vision_tokens_if_all_fire": sum(v["vision_tokens_if_all_triggers_fire"] for v in rows.values()),
        "formula": f"min({SLOPE}*ceil(w/32)*ceil(h/32)+{INTERCEPT}, {CEIL}) per image, zoom {ZOOM}",
    }
    (OUT / "hybrid_trigger_budget.json").write_text(
        json.dumps({"per_pair": rows, "summary": summary}, ensure_ascii=False, indent=2) + "\n", "utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    for k, v in rows.items():
        print(f"{k:24s} {v['n_triggers']} {str(v['triggers']):70s} crop2x {v['tokens_per_two_sided_task_zoom2']:>5} "
              f"trackA {v['track_a_vector_payload_tokens']}")


if __name__ == "__main__":
    main()
