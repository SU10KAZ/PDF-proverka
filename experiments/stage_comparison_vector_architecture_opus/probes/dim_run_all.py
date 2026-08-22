#!/usr/bin/env python3
"""dim_* probe: run the dimension detector over every block, all ablations.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_run_all \
        --cache-dir <dir> --out-dir <dir>
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes.dim_detect import detect

VARIANTS = ["A_nearest_line", "B_side_convention", "C_scale_arbitration"]
MODELS = ["raw_endpoint", "slash_object"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--blocks", default="experiments/stage_comparison_vector_architecture_opus/probes/dim_blocks.json")
    ap.add_argument("--cache-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--summary", required=True)
    a = ap.parse_args()
    pairs = json.loads(Path(a.blocks).read_text(encoding="utf-8"))["pairs"]
    Path(a.out_dir).mkdir(parents=True, exist_ok=True)
    rows = []
    for p in pairs:
        for side in ("left", "right"):
            cache = json.loads(Path(f"{a.cache_dir}/{p['pair_id']}__{side}.json").read_text(encoding="utf-8"))
            for model in MODELS:
                for variant in VARIANTS:
                    t0 = time.time()
                    res = detect(cache, variant=variant, terminator_model=model)
                    dt = time.time() - t0
                    out = Path(a.out_dir) / f"{p['pair_id']}__{side}__{model}__{variant}.json"
                    out.write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
                    det = [r for r in res["dimensions"] if r.get("detected")]
                    ok = [r for r in det if r.get("scale_ok")]
                    rows.append({
                        "pair": p["pair_id"], "side": side, "model": model, "variant": variant,
                        "texts_total": res["texts_total"],
                        "numeric_candidates": len(res["dimensions"]),
                        "detected": len(det), "scale_ok": len(ok),
                        "scale": res["scale"].get("scale_mm_per_pt"),
                        "implied_scale_1_to": res["scale"].get("implied_drawing_scale_1_to"),
                        "seconds": round(dt, 2),
                    })
                    print(rows[-1])
    Path(a.summary).write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
