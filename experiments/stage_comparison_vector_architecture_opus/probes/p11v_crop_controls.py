#!/usr/bin/env python3
"""Verification control for FMC claim F3 (crop mismatch).

Decomposes the reported STRUCTURE_CHANGED on `fmc_crop_mismatch_same_sheet` into
(a) identical bbox, (b) pure translation with EQUAL size, (c) pure concentric scale,
(d) the probe's actual bbox pair.
"""
from __future__ import annotations
import json, time
from pathlib import Path
from experiments.stage_comparison_vector_blocks.comparator import compare_descriptions
from experiments.stage_comparison_vector_blocks.extractor import extract_block

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"
PDF = ROOT / "projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К3/versions/v002/02_work/document.pdf"
PAGE = 21
L = [0.10, 0.25, 0.42, 0.80]

def shift(b, dx, dy):
    return [b[0] + dx, b[1] + dy, b[2] + dx, b[3] + dy]

def scale_about_center(b, f):
    cx, cy = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
    w, h = (b[2] - b[0]) * f / 2, (b[3] - b[1]) * f / 2
    return [cx - w, cy - h, cx + w, cy + h]

CASES = {
    "a_identical_bbox":      (L, L),
    "b_pure_translation":    (L, shift(L, 0.06, 0.04)),               # same size 0.320x0.550
    "b2_small_translation":  (L, shift(L, 0.01, 0.01)),
    "c_concentric_scale_91": (L, scale_about_center(L, 0.906)),        # 0.320 -> 0.290 width, same centre
    "c2_concentric_scale_99":(L, scale_about_center(L, 0.99)),
    "d_probe_actual":        (L, [0.16, 0.29, 0.45, 0.83]),
}

def main() -> None:
    cache: dict[tuple, dict] = {}
    def desc(b):
        k = tuple(round(v, 6) for v in b)
        if k not in cache:
            t0 = time.perf_counter()
            cache[k] = extract_block(PDF, page_index=PAGE, bbox_norm=list(b), block_id="ctl_" + "_".join(f"{v:.3f}" for v in b))
            print(f"  extracted {k} in {time.perf_counter()-t0:.1f}s", flush=True)
        return cache[k]
    out = []
    for name, (lb, rb) in CASES.items():
        dl, dr = desc(lb), desc(rb)
        c = compare_descriptions(dl, dr)
        out.append({
            "case": name, "left_bbox": lb, "right_bbox": rb,
            "status": c["status"],
            "geometry": round(c["geometry"]["similarity"], 4),
            "text": round(c["text"]["effective_similarity"], 4),
            "topology": round(c["topology"]["similarity"], 4),
            "left_texts": len(dl["texts"]), "right_texts": len(dr["texts"]),
            "left_segments": dl["topology"]["segments_total"], "right_segments": dr["topology"]["segments_total"],
        })
        print(json.dumps(out[-1], ensure_ascii=False), flush=True)
    (ART / "p11_newcases_verify_crop_controls.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

if __name__ == "__main__":
    main()
