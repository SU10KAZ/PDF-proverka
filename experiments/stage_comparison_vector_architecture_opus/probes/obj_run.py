"""Probe `obj` driver — run the generic object grouper over the Track A benchmark.

Usage (from repo root):
    python experiments/stage_comparison_vector_architecture_opus/probes/obj_run.py
Optional:  --pairs a,b,c   --no-overlays
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent.parent.parent
ART = HERE.parent / "artifacts"

import obj_poc as P  # noqa: E402

PAIRS_JSON = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json"
DIAG = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/diagnostics"

CLASS_COLOR = {
    "symbol_candidate": (220, 30, 30),
    "closed_area_object": (30, 90, 220),
    "linear_object": (170, 170, 170),
    "dense_region": (230, 140, 0),
    "stray": (0, 160, 60),
}


def overlay(pair_id: str, side: str, block: dict, result: dict, out_path: Path) -> bool:
    try:
        from PIL import Image, ImageDraw
    except Exception:
        return False
    src = DIAG / pair_id / f"{side}.png"
    if not src.exists():
        return False
    im = Image.open(src).convert("RGB")
    d = block["disp_rect"]
    sx = im.width / (d[2] - d[0])
    sy = im.height / (d[3] - d[1])
    draw = ImageDraw.Draw(im)
    for o in result["objects"]:
        if o["class"] == "linear_object":
            continue
        bb = o["bbox"]
        box = [(bb[0] - d[0]) * sx, (bb[1] - d[1]) * sy, (bb[2] - d[0]) * sx, (bb[3] - d[1]) * sy]
        box = [box[0] - 2, box[1] - 2, box[2] + 2, box[3] + 2]
        draw.rectangle(box, outline=CLASS_COLOR.get(o["class"], (0, 0, 0)), width=2)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    im.save(out_path)
    return True


def slim(o, keep_members=False):
    r = {k: v for k, v in o.items() if k != "members"}
    r["bbox"] = [round(v, 2) for v in o["bbox"]]
    r["diag"] = round(o["diag"], 2)
    if keep_members:
        r["members"] = o["members"]
    return r


def object_diff(left_res, right_res, S, pos_tol_rel=0.05):
    """Match objects across two sides by motif class, then by normalized position."""
    def key(o):
        return o.get("motif") or f"{o['class']}:{o['n_seg']}"

    lb = collections.defaultdict(list)
    rb = collections.defaultdict(list)
    for o in left_res["objects"]:
        if o["class"] in ("symbol_candidate", "closed_area_object"):
            lb[key(o)].append(o)
    for o in right_res["objects"]:
        if o["class"] in ("symbol_candidate", "closed_area_object"):
            rb[key(o)].append(o)
    rows = []
    for k in sorted(set(lb) | set(rb)):
        l, r = lb.get(k, []), rb.get(k, [])
        rows.append(
            {
                "class_key": k,
                "left": len(l),
                "right": len(r),
                "delta": len(r) - len(l),
                "example_label_left": next((o["label"] for o in l if o["label"]), None),
                "example_label_right": next((o["label"] for o in r if o["label"]), None),
                "n_seg": (l or r)[0]["n_seg"],
            }
        )
    rows.sort(key=lambda x: (-abs(x["delta"]), -max(x["left"], x["right"])))
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", default="")
    ap.add_argument("--no-overlays", action="store_true")
    ap.add_argument("--out", default="obj_objects.json")
    args = ap.parse_args()

    pairs = {p["pair_id"]: p for p in json.loads(PAIRS_JSON.read_text())["pairs"]}
    wanted = [x for x in args.pairs.split(",") if x] or list(pairs)

    report = {"probe": "obj", "research_only": True, "blocks": {}, "pairs": {}}
    per_block_objects = {}
    for pid in wanted:
        sides = {}
        for side in ("left", "right"):
            e = pairs[pid][side]
            t0 = time.time()
            block = P.extract_segments(str(ROOT / e["pdf"]), e["page_index"], e["bbox_norm"])
            t_ext = time.time() - t0
            t0 = time.time()
            res = P.group_objects(block)
            t_grp = time.time() - t0
            comps = P.endpoint_components(block)
            entry = {
                "pdf": e["pdf"],
                "page_index": e["page_index"],
                "page_rotation": block["page_rotation"],
                "segments": len(block["segments"]),
                "texts": len(block["texts"]),
                "invisible_paths_dropped": block["invisible_paths"],
                "invisible_items_dropped": block["invisible_items"],
                "S_points": round(res["scale"]["S"], 3),
                "S_source": "median_font_size" if res["scale"]["s_text"] else "median_segment_length",
                "counts": res["counts"],
                "motif_classes": len(res["motifs"]),
                "repeated_motifs": {k: v for k, v in res["motifs"].items() if v >= 2},
                "stage_stats": res["stage_stats"],
                "baseline_endpoint_components": comps,
                "seconds_extract": round(t_ext, 2),
                "seconds_group": round(t_grp, 2),
            }
            report["blocks"][f"{pid}/{side}"] = entry
            per_block_objects[f"{pid}/{side}"] = [slim(o) for o in res["objects"]]
            sides[side] = (block, res)
            if not args.no_overlays:
                overlay(pid, side, block, res, ART / "obj_overlays" / pid / f"{side}.png")
            print(f"{pid:24s} {side:5s} rot={block['page_rotation']:3d} segs={len(block['segments']):6d} "
                  f"invis={block['invisible_paths']:4d} S={res['scale']['S']:5.2f} "
                  f"comps={comps:5d} counts={res['counts']}", flush=True)
        report["pairs"][pid] = {
            "object_class_diff": object_diff(sides["left"][1], sides["right"][1], 1.0),
            "count_diff": {
                k: [sides["left"][1]["counts"].get(k, 0), sides["right"][1]["counts"].get(k, 0)]
                for k in set(sides["left"][1]["counts"]) | set(sides["right"][1]["counts"])
            },
        }
    ART.mkdir(parents=True, exist_ok=True)
    (ART / args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1))
    (ART / "obj_object_lists.json").write_text(json.dumps(per_block_objects, ensure_ascii=False))
    print("wrote", ART / args.out)


if __name__ == "__main__":
    main()
