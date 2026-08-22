"""Probe `obj` — can a generic grouper recover COMPOSITE objects (repeated wall
sections) as opposed to symbol-scale objects?

`ar_wall_sections` contains four drawings titled «Сечение 3-3», «Сечение 4»,
«Сечение 5», «Сечение 6».  A human sees four objects.  This script clusters the
whole block at a range of radii and renders the resulting cluster boxes so the
recovery can be checked by eye and by number.

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/obj_sections.py
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent.parent.parent
ART = HERE.parent / "artifacts"
DIAG = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/diagnostics"
import obj_poc as P  # noqa: E402

PAIRS = {p["pair_id"]: p for p in json.loads(
    (ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json").read_text())["pairs"]}


def run(pair_id, radii, min_seg=20):
    e = PAIRS[pair_id]["left"]
    blk = P.extract_segments(str(ROOT / e["pdf"]), e["page_index"], e["bbox_norm"])
    segs = blk["segments"]
    S = P.characteristic_scale(blk)["S"]
    tol = max(0.05 * S, 0.02)
    base = [{"members": m, "bbox": P._bbox_of(m, segs), "cycle": False}
            for m, _n, _e in P._components([s["i"] for s in segs], segs, tol)]
    rows = []
    for rel in radii:
        groups = P._merge_cores(base, segs, rel * S, float("inf"))
        boxes = []
        for g in groups:
            members = [gi for ci in g for gi in base[ci]["members"]]
            bb = P._bbox_of(members, segs)
            boxes.append((len(members), bb))
        big = [b for b in boxes if b[0] >= min_seg]
        rows.append({"radius_rel_S": rel, "clusters": len(boxes),
                     "clusters_with_ge_%d_segments" % min_seg: len(big)})
        render(pair_id, blk, big, ART / "obj_sections" / f"{pair_id}_r{rel}.png")
        print(pair_id, "r=", rel, "clusters", len(boxes), "big", len(big), flush=True)
    return {"S_pt": round(S, 2), "segments": len(segs), "rows": rows}


def render(pair_id, blk, boxes, out_path):
    try:
        from PIL import Image, ImageDraw
    except Exception:
        return
    src = DIAG / pair_id / "left.png"
    if not src.exists():
        return
    im = Image.open(src).convert("RGB")
    d = blk["disp_rect"]
    sx = im.width / (d[2] - d[0])
    sy = im.height / (d[3] - d[1])
    dr = ImageDraw.Draw(im)
    for _n, bb in boxes:
        dr.rectangle([(bb[0] - d[0]) * sx, (bb[1] - d[1]) * sy,
                      (bb[2] - d[0]) * sx, (bb[3] - d[1]) * sy], outline=(200, 0, 0), width=4)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    im.save(out_path)


if __name__ == "__main__":
    out = {"probe": "obj", "research_only": True,
           "ground_truth": {"ar_wall_sections": {"sections": 4,
                            "source": "titles «Сечение 3-3», «Сечение 4», «Сечение 5», «Сечение 6» "
                                      "on diagnostics/ar_wall_sections/left.png"}}}
    out["ar_wall_sections"] = run("ar_wall_sections", [2.5, 4.0, 6.0, 9.0, 14.0])
    (ART / "obj_sections.json").write_text(json.dumps(out, ensure_ascii=False, indent=1))
    print("wrote", ART / "obj_sections.json")
