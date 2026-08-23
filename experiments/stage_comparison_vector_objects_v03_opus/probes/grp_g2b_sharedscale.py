# -*- coding: utf-8 -*-
"""G2b — would a SHARED characteristic scale remove the boundary churn?  [REAL]

Diagnosis from G3: the layer estimates S per block, mostly from the median text size.
The crop boundary decides how much text is inside the block, so the SAME drawing can
get two different S on two versions (measured: EOM-7fef43a3, identical 10 972 segments
on both sides, S = 1.54 vs 13.96, 1 635 objects vs 200).

Here every pair is grouped twice: once with each side's own S (production behaviour) and
once with a single shared S = max(S_a, S_b) — the "text-derived" value when one side has
text.  Everything else is unchanged.
Usage:  grp_g2b_sharedscale.py
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import grp_match as M
import grp_g2_churn as g2

MAX_SEG = 60000


def main():
    real = json.load(open(G.ART / "grp_boundary_churn_real.json", encoding="utf-8"))["pairs"]
    pairs = json.load(open(G.ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    by_id = {p["pair_id"]: p for p in pairs}
    out = []
    for r in real:
        if "error" in r or r.get("reg_score", 0) < 0.90:
            continue
        if max(r["n_seg_a"], r["n_seg_b"]) > MAX_SEG:
            continue
        p = by_id[r["pair_id"]]
        t0 = time.time()
        exA = G.F.extract_block(str(G.ROOT / p["side_a"]["pdf"]), p["side_a"]["page_index"],
                                p["side_a"]["coords_px"], *p["side_a"]["page_px"])
        exB = G.F.extract_block(str(G.ROOT / p["side_b"]["pdf"]), p["side_b"]["page_index"],
                                p["side_b"]["coords_px"], *p["side_b"]["page_px"])
        off = tuple(r["reg_offset"])
        row = {"pair_id": r["pair_id"], "classes": r["classes"], "expected": r["expected"]}
        for tag, kw in (("own", None), ("shared", "S")):
            if tag == "own":
                LA = G.layer_of(exA.segments, exA.texts)
                LB = G.layer_of(exB.segments, exB.texts)
            else:
                sa = G.layer_of(exA.segments, exA.texts).S
                sb = G.layer_of(exB.segments, exB.texts).S
                S = max(sa, sb)
                LA = G.layer_of(exA.segments, exA.texts, S_override=S)
                LB = G.layer_of(exB.segments, exB.texts, S_override=S)
                row["S_shared"] = round(S, 3)
                row["S_a"], row["S_b"] = round(sa, 3), round(sb, 3)
            g2.mark_border(LA, exA.frame["clip_display"])
            g2.mark_border(LB, exB.frame["clip_display"])
            rows = M.churn_rows(LA, exA.segments, LB, exB.segments, off)
            row[tag] = {"churn": {k: round(v, 5) for k, v in M.classify(rows).items()},
                        "n_obj_a": len(LA.objects), "n_obj_b": len(LB.objects)}
        row["delta_1to1"] = round(row["shared"]["churn"]["one_to_one"] -
                                  row["own"]["churn"]["one_to_one"], 5)
        row["t_sec"] = round(time.time() - t0, 1)
        print(row["pair_id"], row["own"]["churn"]["one_to_one"], "->",
              row["shared"]["churn"]["one_to_one"], "S", row.get("S_a"), row.get("S_b"), flush=True)
        out.append(row)
    import statistics
    d = [r["delta_1to1"] for r in out]
    summary = {"n_pairs": len(out),
               "one_to_one_median_own": round(statistics.median([r["own"]["churn"]["one_to_one"] for r in out]), 5),
               "one_to_one_median_shared": round(statistics.median([r["shared"]["churn"]["one_to_one"] for r in out]), 5),
               "delta_median": round(statistics.median(d), 5),
               "delta_mean": round(statistics.mean(d), 5),
               "pairs_improved": sum(1 for x in d if x > 1e-6),
               "pairs_worsened": sum(1 for x in d if x < -1e-6),
               "pairs_with_different_S": sum(1 for r in out if abs(r["S_a"] - r["S_b"]) > 0.05 * max(r["S_a"], r["S_b"]))}
    json.dump({"summary": summary, "pairs": out},
              open(G.ART / "grp_shared_scale.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
