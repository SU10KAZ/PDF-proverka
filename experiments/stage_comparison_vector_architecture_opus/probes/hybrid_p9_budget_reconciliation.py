#!/usr/bin/env python3
"""Probe HYBRID-9: reconcile Track A's 70,631 (vector) vs 38,069 (vision) tokens.

Uses the measured image-cost formula from HYBRID-5/5b and the measured codex
system-prompt baseline, and tokenises the two prompts and outputs with o200k_base.

    <venv>/bin/python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p9_budget_reconciliation
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import tiktoken
from PIL import Image

ENC = tiktoken.get_encoding("o200k_base")
ROOT = Path(__file__).resolve().parents[3]
AI = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts" / "ai_experiment"
DIAG = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts" / "diagnostics"
OUT = Path(__file__).resolve().parents[1] / "artifacts"

PAIR_IDS = ("ss_scheme_text_changed", "ss_table_graphic", "ar_plan", "vk_nodes", "eom_singleline_changed")
SLOPE, INTERCEPT, CEIL = 1.2014, 48.67, 3051


def img_tokens(w: int, h: int) -> float:
    return min(SLOPE * math.ceil(w / 32) * math.ceil(h / 32) + INTERCEPT, CEIL)


def main() -> None:
    formula = json.loads((OUT / "hybrid_image_token_formula.json").read_text("utf-8"))
    measured = {r["label"].split(":", 1)[-1]: r["measured"] for r in formula["validation_rows"]}

    vec_prompt = (AI / "vector_prompt.txt").read_text("utf-8")
    vis_prompt = (AI / "vision_prompt.txt").read_text("utf-8")
    vec_out = (AI / "vector_output.json").read_text("utf-8")
    vis_out = (AI / "vision_output.json").read_text("utf-8")
    meta = json.loads((AI / "invocation_metadata.json").read_text("utf-8"))

    images, img_total, img_bytes = [], 0.0, 0
    for pid in PAIR_IDS:
        for side in ("left", "right"):
            p = DIAG / pid / f"{side}.png"
            w, h = Image.open(p).size
            key = f"{pid}/{side}.png"
            t = measured.get(key, img_tokens(w, h))
            source = "measured" if key in measured else "formula"
            images.append({"image": key, "w": w, "h": h, "tokens": round(t), "source": source,
                           "png_bytes": p.stat().st_size})
            img_total += t
            img_bytes += p.stat().st_size

    base = json.loads((OUT / "hybrid_vision_crop_cost.json").read_text("utf-8"))["baseline_used"]
    vec_p, vis_p = len(ENC.encode(vec_prompt)), len(ENC.encode(vis_prompt))
    vec_o, vis_o = len(ENC.encode(vec_out)), len(ENC.encode(vis_out))
    res = {
        "codex_harness_baseline_tokens_measured": base,
        "vector": {
            "reported_total": meta["vector"]["stderr_tail"].strip().splitlines()[-1].replace(" ", ""),
            "reported_total_int": 70631,
            "prompt_bytes_utf8": len(vec_prompt.encode()),
            "prompt_tokens": vec_p,
            "bytes_per_token": round(len(vec_prompt.encode()) / vec_p, 2),
            "output_tokens": vec_o,
            "harness_baseline": base,
            "accounted": base + vec_p + vec_o,
            "residual_reasoning_and_overhead": 70631 - (base + vec_p + vec_o),
            "prompt_share_of_bill_pct": round(100.0 * vec_p / 70631, 1),
        },
        "vision": {
            "reported_total_int": 38069,
            "png_bytes_total": img_bytes,
            "image_tokens_total": round(img_total),
            "bytes_per_token": round(img_bytes / img_total, 1),
            "prompt_tokens": vis_p,
            "output_tokens": vis_o,
            "harness_baseline": base,
            "accounted": round(base + vis_p + vis_o + img_total),
            "residual_reasoning_and_overhead": round(38069 - (base + vis_p + vis_o + img_total)),
            "image_share_of_bill_pct": round(100.0 * img_total / 38069, 1),
            "images": images,
        },
    }
    res["byte_efficiency_ratio_image_over_text"] = round(
        res["vision"]["bytes_per_token"] / res["vector"]["bytes_per_token"], 1
    )
    res["gap_reported"] = 70631 - 38069
    res["gap_explained_by_prompt_minus_images"] = round(vec_p - img_total)
    (OUT / "hybrid_token_budget_reconciliation.json").write_text(
        json.dumps(res, ensure_ascii=False, indent=2) + "\n", "utf-8"
    )
    print(json.dumps({k: v for k, v in res.items() if k != "vision"}, ensure_ascii=False, indent=2))
    print(json.dumps({k: v for k, v in res["vision"].items() if k != "images"}, ensure_ascii=False, indent=2))
    for i in images:
        print(i)


if __name__ == "__main__":
    main()
