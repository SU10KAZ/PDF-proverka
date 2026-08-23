# -*- coding: utf-8 -*-
"""F4 [REAL] — what does the family layer say on the 33 hand-labelled pairs?

One configuration only (the one F3 selects): shared characteristic scale S, interior
scope (crop-border objects dropped, `mine` M5), two-pass clustering.  For every pair we
publish the full "N -> M" ledger with bboxes, so it can be compared with the human
sentence stored in `mine_pairs.json` (`human_expected_ru`, `expected_changed_objects`).

Usage: fam_f4b_real.py [out.json]
"""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import grp_match as M
import fam_family as FAM

MAX_SEG = 130000
PAD_FRAC = 0.02


def _inside(bbox, reg, pad):
    return (bbox[0] >= reg[0] + pad and bbox[1] >= reg[1] + pad and
            bbox[2] <= reg[2] - pad and bbox[3] <= reg[3] - pad)


def one_pair(p):
    t0 = time.time()
    row = {k: p.get(k) for k in ("pair_id", "classes", "expected_verdict",
                                 "expected_changed_objects", "label_confidence",
                                 "discipline", "doc_id", "human_expected_ru")}
    a, b = p["side_a"], p["side_b"]
    exA = G.F.extract_block(str(G.ROOT / a["pdf"]), a["page_index"], a["coords_px"], *a["page_px"])
    exB = G.F.extract_block(str(G.ROOT / b["pdf"]), b["page_index"], b["coords_px"], *b["page_px"])
    if not exA.segments or not exB.segments:
        return dict(row, error="no vector geometry")
    if max(len(exA.segments), len(exB.segments)) > MAX_SEG:
        return dict(row, error=f"too big {len(exA.segments)}/{len(exB.segments)}")
    clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
    base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
    dx, dy, score = M.register(exA.segments, exB.segments, {(0.0, 0.0), base})
    regB = (clipB[0] + dx, clipB[1] + dy, clipB[2] + dx, clipB[3] + dy)
    reg = (max(clipA[0], regB[0]), max(clipA[1], regB[1]),
           min(clipA[2], regB[2]), min(clipA[3], regB[3]))
    pad = max(2.0, PAD_FRAC * min(reg[2] - reg[0], reg[3] - reg[1]))

    LA0 = G.layer_of(exA.segments, exA.texts)
    LB0 = G.layer_of(exB.segments, exB.texts)
    S = max(LA0.S, LB0.S)
    LA = G.layer_of(exA.segments, exA.texts, S_override=S)
    LB = G.layer_of(exB.segments, exB.texts, S_override=S)
    oa = [o for o in LA.objects if _inside(o["bbox"], reg, pad)]
    ob = [o for o in LB.objects
          if _inside([o["bbox"][0] + dx, o["bbox"][1] + dy,
                      o["bbox"][2] + dx, o["bbox"][3] + dy], reg, pad)]
    out = {}
    for scope, (xa, xb) in (("interior", (oa, ob)), ("all", (LA.objects, LB.objects))):
        FP = FAM.build_families_pair(xa, xb)
        rows2 = FAM.family_deltas(FP, min_family=2)
        out[scope] = {
            "n_obj_a": len(xa), "n_obj_b": len(xb),
            "n_families": len(FP.families),
            "n_repeated": sum(1 for f in FP.families if len(f["members"]) >= 2),
            "rows_min2": len(rows2),
            "rows_min3": len(FAM.family_deltas(FP, min_family=3)),
            "delta_sum": sum(abs(r["delta"]) for r in rows2),
            "delta_max": max([abs(r["delta"]) for r in rows2], default=0),
            "ledger": [{"n_a": r["n_a"], "n_b": r["n_b"], "delta": r["delta"],
                        "cls": r["cls"], "diag": r["diag_med"],
                        "n_seg_med": r["n_seg_med"], "bbox": r["bbox"]}
                       for r in rows2[:12]],
        }
    row.update({"n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
                "S_a": round(LA0.S, 3), "S_b": round(LB0.S, 3), "S": round(S, 3),
                "reg_offset": [round(dx, 3), round(dy, 3)], "reg_score": round(score, 4),
                "variants": out, "t_sec": round(time.time() - t0, 1)})
    return row


def _work(p):
    try:
        return one_pair(p)
    except Exception:
        return {"pair_id": p["pair_id"], "classes": p.get("classes"),
                "expected_verdict": p.get("expected_verdict"),
                "error": traceback.format_exc().splitlines()[-1]}


def main():
    import multiprocessing as mp
    out_path = sys.argv[1] if len(sys.argv) > 1 else str(G.ART / "fam_f4b_real.json")
    pairs = json.load(open(G.ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    print("pairs", len(pairs), flush=True)
    rows = []
    with mp.Pool(4, maxtasksperchild=2) as pool:
        for r in pool.imap_unordered(_work, pairs, chunksize=1):
            rows.append(r)
            v = (r.get("variants") or {}).get("interior", {})
            print(len(rows), r["pair_id"], r.get("expected_verdict"), r.get("error", ""),
                  "rows", v.get("rows_min2"), "/", v.get("n_repeated"), flush=True)
    json.dump({"config": "S=shared, scope=interior|all, mode=twopass", "pad_frac": PAD_FRAC,
               "pairs": rows}, open(out_path, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
