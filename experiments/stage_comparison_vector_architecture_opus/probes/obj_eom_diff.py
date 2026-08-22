"""Probe `obj` — explicit OBJECT-LEVEL diff of `eom_singleline_changed`
(13АВ-РД-ЭМ-К4 v001 page 9 vs v002 page 11), the only benchmark pair with a real
engineering change.

Three questions, each answered with a number:
  Q1  Do the object candidates recover the drawn devices?  (label-anchored GT)
  Q2  Does a decomposition-insensitive shape descriptor transfer between two
      different PDF exports?  (same symbol, 30 vs 11 segments)
  Q3  Does object-level matching produce «Количество аппаратов X → Y»?

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/obj_eom_diff.py
"""
from __future__ import annotations

import collections
import itertools
import json
import re
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent.parent.parent
ART = HERE.parent / "artifacts"
import obj_poc as P  # noqa: E402

PAIRS = {p["pair_id"]: p for p in json.loads(
    (ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json").read_text())["pairs"]}
DEVICE_TAG = re.compile(r"^(QD|QF|Wh)(\d+|n)$")


def load(pair_id, s):
    e = PAIRS[pair_id][s]
    blk = P.extract_segments(str(ROOT / e["pdf"]), e["page_index"], e["bbox_norm"])
    res = P.group_objects(blk)
    S = res["scale"]["S"]
    objs = [o for o in res["objects"] if o["class"] in ("symbol_candidate", "closed_area_object")]
    for i, o in enumerate(objs):
        o["oid"] = f"{s[0].upper()}{i:03d}"
        o["desc"] = P.shape_descriptor(o["members"], blk["segments"])
        cands = sorted((P._bbox_gap(o["bbox"], tuple(t["bbox"])), t["text"]) for t in blk["texts"])
        o["labels_near"] = [t for g, t in cands[:5] if g <= 2.5 * S]
        o["label"] = o["labels_near"][0] if o["labels_near"] else None
    return blk, res, objs, S


def device_count(objs, blk, S):
    """Label-anchored ground truth: every exact device tag (QD1, Whn, QF3 ...) marks one
    real device.  The device is RECOVERED when an object candidate sits within
    (2 S horizontally, 3.5 S vertically) of that tag.  Returns {tag: object id}."""
    got = {}
    for t in blk["texts"]:
        if not DEVICE_TAG.fullmatch(t["text"]):
            continue
        best = None
        for o in objs:
            bb = o["bbox"]
            dx = max(0.0, max(bb[0] - t["bbox"][2], t["bbox"][0] - bb[2]))
            dy = max(0.0, max(bb[1] - t["bbox"][3], t["bbox"][1] - bb[3]))
            if dx <= 2.0 * S and dy <= 3.5 * S and (best is None or dx + dy < best[0]):
                best = (dx + dy, o["oid"])
        if best:
            got[t["text"]] = best[1]
    return got


def greedy_match(ol, orr, eps):
    pairs = []
    for a in ol:
        for b in orr:
            d = P.descriptor_distance(a["desc"], b["desc"])
            if d <= eps:
                pairs.append((d, a["oid"], b["oid"]))
    pairs.sort()
    ul, ur, out = set(), set(), []
    for d, a, b in pairs:
        if a in ul or b in ur:
            continue
        ul.add(a)
        ur.add(b)
        out.append((round(d, 4), a, b))
    return out, ul, ur


def main():
    bl, rl, ol, Sl = load("eom_singleline_changed", "left")
    br, rr, orr, Sr = load("eom_singleline_changed", "right")
    out = {"probe": "obj", "research_only": True, "pair": "eom_singleline_changed"}
    out["sides"] = {
        "left": {"version": "v001", "page_index": 8, "page_rotation": bl["page_rotation"],
                 "segments": len(bl["segments"]), "S_pt": round(Sl, 2), "objects": len(ol),
                 "counts": rl["counts"]},
        "right": {"version": "v002", "page_index": 10, "page_rotation": br["page_rotation"],
                  "segments": len(br["segments"]), "S_pt": round(Sr, 2), "objects": len(orr),
                  "counts": rr["counts"]},
    }

    # --- Q1: device recovery
    dl, dr = device_count(ol, bl, Sl), device_count(orr, br, Sr)
    out["Q1_device_objects"] = {
        "left_tags": sorted(dl), "left_count": len(dl),
        "left_distinct_objects": len(set(dl.values())),
        "right_tags": sorted(dr), "right_count": len(dr),
        "right_distinct_objects": len(set(dr.values())),
        "ground_truth_left": 6, "ground_truth_right": 12,
        "ground_truth_source": "human count on diagnostics/eom_singleline_changed/{left,right}.png: "
                               "v001 draws 2 outgoing rows x (QD,Wh,QF); v002 draws 4 rows x (QD,Wh,QF)",
    }

    # --- terminal markers (the repeated square-with-X symbol)
    def markers(objs, S):
        return [o for o in objs if 1.2 <= o["desc"]["diag"] / S <= 1.6 and o["n_seg"] >= 6]
    ml, mr = markers(ol, Sl), markers(orr, Sr)
    out["Q1_terminal_markers"] = {
        "left_count": len(ml), "right_count": len(mr),
        "left_n_seg": sorted(collections.Counter(o["n_seg"] for o in ml).items()),
        "right_n_seg": sorted(collections.Counter(o["n_seg"] for o in mr).items()),
        "ground_truth_both_sides": 14,
        "ground_truth_source": "human count on the diagnostics PNGs: 4 rows x 3 phase markers + "
                               "1 at Шина N + 1 at Шина PE",
    }

    # --- Q2: descriptor transfer for the SAME symbol drawn with different segment counts
    ml30 = [o for o in ml if o["n_seg"] == max(c for c, _ in
            [(o2["n_seg"], 0) for o2 in ml])] or ml
    q2 = {}
    if ml and mr:
        cross = [P.descriptor_distance(a["desc"], b["desc"]) for a in ml for b in mr]
        wl = [P.descriptor_distance(a["desc"], b["desc"]) for a, b in itertools.combinations(ml, 2)]
        wr = [P.descriptor_distance(a["desc"], b["desc"]) for a, b in itertools.combinations(mr, 2)]
        other = [P.descriptor_distance(ml[0]["desc"], o["desc"]) for o in orr if o not in mr]
        q2 = {
            "left_segments_per_marker": sorted({o["n_seg"] for o in ml}),
            "right_segments_per_marker": sorted({o["n_seg"] for o in mr}),
            "cross_version_distance_median": round(statistics.median(cross), 4),
            "cross_version_distance_max": round(max(cross), 4),
            "within_left_max": round(max(wl), 4) if wl else None,
            "within_right_max": round(max(wr), 4) if wr else None,
            "distance_to_other_right_objects_min": round(min(other), 4) if other else None,
            "separation_ratio": round(min(other) / max(cross), 2) if other and max(cross) else None,
        }
    out["Q2_descriptor_transfer"] = q2

    # --- Q3: object-level matching and the resulting sentences
    match_rows = []
    for eps in (0.05, 0.1, 0.15, 0.25, 0.4, 0.8, 1.5):
        m, ul, ur = greedy_match(ol, orr, eps)
        match_rows.append({"eps": eps, "matched": len(m),
                           "unmatched_left_removed": len(ol) - len(ul),
                           "unmatched_right_added": len(orr) - len(ur)})
    out["Q3_object_matching_sweep"] = match_rows

    eps = 0.15
    m, ul, ur = greedy_match(ol, orr, eps)
    out["Q3_chosen_eps"] = eps
    byid_l = {o["oid"]: o for o in ol}
    byid_r = {o["oid"]: o for o in orr}
    added = [byid_r[o["oid"]] for o in orr if o["oid"] not in ur]
    removed = [byid_l[o["oid"]] for o in ol if o["oid"] not in ul]
    out["Q3_added_objects"] = [{"oid": o["oid"], "label": o["label"], "n_seg": o["n_seg"],
                                "diag_over_S": round(o["desc"]["diag"] / Sr, 2)} for o in added]
    out["Q3_removed_objects"] = [{"oid": o["oid"], "label": o["label"], "n_seg": o["n_seg"],
                                  "diag_over_S": round(o["desc"]["diag"] / Sl, 2)} for o in removed]

    # class-level census under the SAME descriptor (union clustering, no n_seg gate)
    tagged = [dict(o, _side="L") for o in ol] + [dict(o, _side="R") for o in orr]
    classes = P.cluster_by_descriptor(tagged, eps=0.15, n_seg_ratio=10 ** 9)
    rows = []
    for ci, c in enumerate(classes):
        L = [x for x in c["members"] if x["_side"] == "L"]
        R = [x for x in c["members"] if x["_side"] == "R"]
        rows.append({"class": f"K{ci:02d}", "left": len(L), "right": len(R),
                     "delta": len(R) - len(L),
                     "diag_over_S": round(c["exemplar"]["diag"] / (Sl if L else Sr), 2),
                     "n_seg_left": sorted({x["n_seg"] for x in L})[:4],
                     "n_seg_right": sorted({x["n_seg"] for x in R})[:4],
                     "labels_left": sorted({x["label"] for x in L if x["label"]})[:5],
                     "labels_right": sorted({x["label"] for x in R if x["label"]})[:5]})
    rows.sort(key=lambda r: (-max(r["left"], r["right"]), -abs(r["delta"])))
    out["Q3_class_census"] = rows

    out["sentences_ru"] = [
        f"Отходящих линий: {len(dl) // 3} → {len(dr) // 3}.",
        f"Аппаратов (QD/Wh/QF) на схеме: {len(dl)} → {len(dr)}.",
        f"Клеммных маркеров (повторяющийся символ): {len(ml)} → {len(mr)}.",
        f"Объектов-кандидатов всего: {len(ol)} → {len(orr)}; "
        f"сопоставлено по форме {len(m)}, добавлено {len(added)}, удалено {len(removed)}.",
    ]
    (ART / "obj_eom_object_diff.json").write_text(json.dumps(out, ensure_ascii=False, indent=1))
    for k in ("sides", "Q1_device_objects", "Q1_terminal_markers", "Q2_descriptor_transfer",
              "Q3_object_matching_sweep", "sentences_ru"):
        print(k, "=", json.dumps(out[k], ensure_ascii=False))
    print("class census (top 12):")
    for r in rows[:12]:
        print("  ", json.dumps(r, ensure_ascii=False))
    print("wrote", ART / "obj_eom_object_diff.json")


if __name__ == "__main__":
    main()
