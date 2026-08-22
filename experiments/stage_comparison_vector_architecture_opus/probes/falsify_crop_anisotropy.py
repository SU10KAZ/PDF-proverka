"""falsify_ probe, attack B: v0.1 normalization is ANISOTROPIC.

extractor._norm_point divides x by the block width and y by the block height
independently, so the description is invariant only to a change of block position
and to a scale change that keeps the block ASPECT RATIO. Between two design
stages the block bbox is produced by a detector and will never have exactly the
same aspect; the schema nevertheless claims
`normalization_removes: [page position, uniform presentation scale]`.

This probe takes ONE real block from ONE real PDF page and compares it against
itself under three families of bbox perturbation:
  * shift   - the window moves by d of its width (content changes at the edges)
  * iso     - the window grows by factor f in both directions
  * aniso   - the window grows by factor f in width only (aspect ratio changes)
and reports the v0.1 geometry similarity and status for each.

Run:
  python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_crop_anisotropy
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from experiments.stage_comparison_vector_blocks import comparator, extractor

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"

BLOCKS = [
    {
        "name": "vk_plan_left",
        "pdf": "projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК.КВ-К4_V1/versions/v001/02_work/document.pdf",
        "page_index": 5,
        "bbox_norm": [0.05016317963600159, 0.009301990270614624, 0.6681880056858063, 0.848704606294632],
    },
    {
        "name": "ss_scheme_left",
        "pdf": "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v002/02_work/document.pdf",
        "page_index": 5,
        "bbox_norm": [0.030090421438217163, 0.012994349002838135, 0.9898177683353424, 0.3752070367336273],
    },
]


def perturb(bbox, kind, amount):
    x0, y0, x1, y1 = bbox
    w, h = x1 - x0, y1 - y0
    if kind == "shift":
        d = amount * w
        return [x0 + d, y0, min(1.0, x1 + d), y1]
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    if kind == "iso":
        nw, nh = w * amount, h * amount
    elif kind == "aniso_w":
        nw, nh = w * amount, h
    else:
        nw, nh = w, h * amount
    return [
        max(0.0, cx - nw / 2), max(0.0, cy - nh / 2),
        min(1.0, cx + nw / 2), min(1.0, cy + nh / 2),
    ]


def main() -> None:
    rows = []
    for block in BLOCKS:
        base = extractor.extract_block(
            ROOT / block["pdf"],
            page_index=block["page_index"],
            bbox_norm=block["bbox_norm"],
            block_id=block["name"] + "_base",
        )
        for kind, amounts in (
            ("shift", (0.002, 0.005, 0.01, 0.02, 0.05)),
            ("iso", (1.005, 1.01, 1.02, 1.05, 1.10)),
            ("aniso_w", (1.005, 1.01, 1.02, 1.05, 1.10)),
            ("aniso_h", (1.005, 1.01, 1.02, 1.05, 1.10)),
        ):
            for amount in amounts:
                bbox = perturb(block["bbox_norm"], kind, amount)
                started = time.time()
                other = extractor.extract_block(
                    ROOT / block["pdf"],
                    page_index=block["page_index"],
                    bbox_norm=bbox,
                    block_id=f"{block['name']}_{kind}_{amount}",
                )
                cmp_ = comparator.compare_descriptions(base, other)
                aspect_base = (block["bbox_norm"][2] - block["bbox_norm"][0]) / (
                    block["bbox_norm"][3] - block["bbox_norm"][1]
                )
                aspect_other = (bbox[2] - bbox[0]) / (bbox[3] - bbox[1])
                rows.append(
                    {
                        "block": block["name"],
                        "perturbation": kind,
                        "amount": amount,
                        "aspect_ratio_change": round(aspect_other / aspect_base, 4),
                        "segments": [
                            base["primitive_summary"]["total_segment_count"],
                            other["primitive_summary"]["total_segment_count"],
                        ],
                        "status": cmp_["status"],
                        "geometry_similarity": cmp_["geometry"]["similarity"],
                        "left_coverage": cmp_["geometry"]["left_coverage"],
                        "topology_similarity": cmp_["topology"]["similarity"],
                        "text_similarity": cmp_["text"]["effective_similarity"],
                        "elapsed_s": round(time.time() - started, 1),
                    }
                )
                print(
                    "%-16s %-9s %-6s aspect x%.4f  segs %6d->%6d  %-22s geom=%.4f topo=%.3f"
                    % (block["name"], kind, amount, rows[-1]["aspect_ratio_change"],
                       rows[-1]["segments"][0], rows[-1]["segments"][1],
                       cmp_["status"], cmp_["geometry"]["similarity"], cmp_["topology"]["similarity"]),
                    flush=True,
                )
    (ART / "falsify_crop_anisotropy.json").write_text(
        json.dumps({"rows": rows}, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print("wrote", ART / "falsify_crop_anisotropy.json")


if __name__ == "__main__":
    main()
