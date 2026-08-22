"""Probe `obj` analysis — tolerant motif classes, cross-version object diff,
ground-truth scoring, and an object-scale sweep.

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/obj_analyze.py
"""

from __future__ import annotations

import collections
import json
import math
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent.parent.parent
ART = HERE.parent / "artifacts"

import obj_poc as P  # noqa: E402

PAIRS = {p["pair_id"]: p for p in json.loads(
    (ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json").read_text())["pairs"]}


def build(pair_id, side):
    e = PAIRS[pair_id][side]
    block = P.extract_segments(str(ROOT / e["pdf"]), e["page_index"], e["bbox_norm"])
    res = P.group_objects(block)
    segs = block["segments"]
    objs = []
    for o in res["objects"]:
        if o["class"] in ("symbol_candidate", "closed_area_object"):
            o["desc"] = P.shape_descriptor(o["members"], segs)
            objs.append(o)
    return block, res, objs


def label_of(o, block, S, radius_mult=2.5):
    """Nearest text within radius_mult * S of the object bbox."""
    bb = o["bbox"]
    best = None
    for t in block["texts"]:
        g = P._bbox_gap(bb, tuple(t["bbox"]))
        if g <= radius_mult * S and (best is None or g < best[0]):
            best = (g, t["text"])
    return best[1] if best else None


def cross_classes(objs_l, objs_r, eps=0.35):
    """Motif classes shared by both sides: cluster the union, then split by side."""
    tagged = [dict(o, _side="L") for o in objs_l] + [dict(o, _side="R") for o in objs_r]
    classes = P.cluster_by_descriptor(tagged, eps=eps)
    rows = []
    for ci, c in enumerate(classes):
        L = [m for m in c["members"] if m["_side"] == "L"]
        R = [m for m in c["members"] if m["_side"] == "R"]
        rows.append({
            "class": f"C{ci:02d}",
            "left": len(L), "right": len(R), "delta": len(R) - len(L),
            "n_seg_exemplar": c["exemplar"]["n_seg"],
            "diag_left_pt": round(sum(m["desc"]["diag"] for m in L) / len(L), 1) if L else None,
            "diag_right_pt": round(sum(m["desc"]["diag"] for m in R) / len(R), 1) if R else None,
            "labels_left": sorted({m.get("label2") for m in L if m.get("label2")})[:8],
            "labels_right": sorted({m.get("label2") for m in R if m.get("label2")})[:8],
        })
    rows.sort(key=lambda r: (-max(r["left"], r["right"]), -abs(r["delta"])))
    return rows


def gt_from_labels(block, pattern):
    return [t for t in block["texts"] if re.match(pattern, t["text"])]


def anchored_score(block, objs, pattern, dy_max_S=3.5, dx_max_S=2.0):
    """Precision/recall against label-anchored ground truth.

    Each text span matching `pattern` marks one real device.  A device is RECOVERED
    when exactly one object candidate sits within (dx_max, dy_max) of that label.
    """
    S = P.characteristic_scale(block)["S"] or 1.0
    gts = gt_from_labels(block, pattern)
    hits, multi, miss = 0, 0, 0
    used = set()
    for t in gts:
        cands = []
        for i, o in enumerate(objs):
            bb = o["bbox"]
            dx = max(0.0, max(bb[0] - t["bbox"][2], t["bbox"][0] - bb[2]))
            dy = max(0.0, max(bb[1] - t["bbox"][3], t["bbox"][1] - bb[3]))
            if dx <= dx_max_S * S and dy <= dy_max_S * S:
                cands.append((dy + dx, i))
        if not cands:
            miss += 1
            continue
        cands.sort()
        hits += 1
        used.add(cands[0][1])
        if len(cands) > 1:
            multi += 1
    return {"gt": len(gts), "recovered": hits, "missed": miss,
            "gt_with_multiple_candidates": multi,
            "objects_total": len(objs), "objects_matched": len(used),
            "recall": round(hits / len(gts), 3) if gts else None,
            "precision_vs_gt_set": round(len(used) / len(objs), 3) if objs else None}


def main():
    out = {"probe": "obj", "research_only": True}

    # ---------------------------------------------------------------- eom object diff
    bl, rl, ol = build("eom_singleline_changed", "left")
    br, rr, orr = build("eom_singleline_changed", "right")
    Sl = rl["scale"]["S"]; Sr = rr["scale"]["S"]
    for o in ol:
        o["label2"] = label_of(o, bl, Sl)
    for o in orr:
        o["label2"] = label_of(o, br, Sr)
    out["eom_object_class_table"] = cross_classes(ol, orr)
    out["eom_sides"] = {
        "left": {"S": round(Sl, 2), "segments": len(bl["segments"]), "objects": len(ol),
                 "counts": rl["counts"]},
        "right": {"S": round(Sr, 2), "segments": len(br["segments"]), "objects": len(orr),
                  "counts": rr["counts"]},
    }
    out["eom_device_label_score"] = {
        "left": anchored_score(bl, ol, r"^(QD|QF|Wh)\d"),
        "right": anchored_score(br, orr, r"^(QD|QF|Wh)\d"),
    }

    # ---------------------------------------------------------------- ss_scheme
    bl2, rl2, ol2 = build("ss_scheme_text_changed", "left")
    br2, rr2, or2 = build("ss_scheme_text_changed", "right")
    S2 = rl2["scale"]["S"]
    for o in ol2:
        o["label2"] = label_of(o, bl2, S2)
    for o in or2:
        o["label2"] = label_of(o, br2, S2)
    out["ss_scheme_object_class_table"] = cross_classes(ol2, or2)
    out["ss_scheme_camera_score"] = {
        "left": anchored_score(bl2, ol2, r"^ВК$"),
        "right": anchored_score(br2, or2, r"^ВК$"),
    }
    out["ss_scheme_ospd_score"] = {
        "left": anchored_score(bl2, ol2, r"^ОСПД$"),
        "right": anchored_score(br2, or2, r"^ОСПД$"),
    }

    # ---------------------------------------------------------------- ss_plan_dense
    bl3, rl3, ol3 = build("ss_plan_dense", "left")
    out["ss_plan_dense_camera_score"] = anchored_score(bl3, ol3, r"^ВК")
    out["ss_plan_dense_counts"] = rl3["counts"]

    # ---------------------------------------------------------------- scale sweep
    radii = [0.15, 0.3, 0.6, 1.0, 1.5, 2.5, 4.0, 6.0, 9.0, 14.0, 22.0, 35.0, 55.0]
    sweep = {}
    for pid in ("ss_simple_node", "ss_scheme_text_changed", "eom_singleline_changed",
                "ar_wall_sections"):
        e = PAIRS[pid]["left"]
        blk = P.extract_segments(str(ROOT / e["pdf"]), e["page_index"], e["bbox_norm"])
        sweep[pid] = P.radius_sweep(blk, radii)
        print("sweep", pid, [(r["radius_rel_S"], r["clusters"]) for r in sweep[pid]], flush=True)
    out["radius_sweep"] = sweep

    (ART / "obj_analysis.json").write_text(json.dumps(out, ensure_ascii=False, indent=1))
    print("wrote", ART / "obj_analysis.json")


if __name__ == "__main__":
    main()
