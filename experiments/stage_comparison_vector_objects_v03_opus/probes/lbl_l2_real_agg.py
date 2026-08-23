# -*- coding: utf-8 -*-
"""Aggregate the REAL arm of the L2 ablation."""
from __future__ import annotations
import json, statistics, sys
from collections import defaultdict
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lbl_common as L

MODES = ("geom", "geom_pos", "geom_pos_label")


def med(v):
    return round(statistics.median(v), 4) if v else None


def main():
    d = json.load(open(L.ART / "lbl_l2_real.json", encoding="utf-8"))
    rs_all = [r for r in d["pairs"] if "registered/geom_pos" in r]
    # a pair with NO strict 1:1 ink reference cannot score a matcher: excluded,
    # counted separately (otherwise 0/0 silently becomes 0.0 and drags the mean)
    rs = [r for r in rs_all if r["n_ref_1to1"] > 0]
    no_ref = [(r["pair_id"], r["n_obj_a"], r["n_ref_absent"]) for r in rs_all
              if r["n_ref_1to1"] == 0]
    out = {"n_pairs": len(d["pairs"]), "n_used": len(rs), "n_with_ink_reference": len(rs),
           "excluded_no_ink_reference": no_ref,
           "skipped": [(r["pair_id"], r.get("skip") or r.get("error"))
                       for r in d["pairs"] if "registered/geom_pos" not in r],
           "ref_cover": L.summarise([r["ref_cover"] for r in rs]),
           "unique_label_share_a": L.summarise([r["unique_label_share_a"] for r in rs]),
           "by_cond": {}, "pooled_by_cond": {}, "by_label_band": {}, "per_pair": []}
    for cond in ("registered", "raw"):
        for m in MODES:
            k = f"{cond}/{m}"
            acc = [r[k]["acc_on_ref"] for r in rs if k in r]
            fr = [r[k]["false_removed_share"] for r in rs if k in r]
            wr = [r[k]["wrong_on_ref"] for r in rs if k in r]
            fa = [r[k]["paired_though_ink_absent_share"] for r in rs
                  if k in r and r[k]["paired_though_ink_absent_share"] is not None]
            # pooled over objects, not over pairs
            n_ref = sum(r["n_ref_1to1"] for r in rs if k in r)
            n_ok = sum(round(r[k]["acc_on_ref"] * r["n_ref_1to1"]) for r in rs if k in r)
            out["by_cond"].setdefault(cond, {})[m] = {
                "n_pairs": len(acc),
                "median_acc_on_ref": med(acc), "mean_acc_on_ref": round(statistics.fmean(acc), 4),
                "p10_acc": round(statistics.quantiles(acc, n=10)[0], 4) if len(acc) > 3 else None,
                "median_false_removed_share": med(fr),
                "median_wrong_on_ref": med(wr), "total_wrong_on_ref": sum(wr),
                "median_paired_though_ink_absent_share": med(fa),
            }
            out["pooled_by_cond"].setdefault(cond, {})[m] = {
                "n_ref_objects": n_ref, "n_correct": n_ok,
                "acc_pooled": round(n_ok / max(n_ref, 1), 5)}
    # split by how much label there is on side A
    for r in rs:
        band = ("none" if r["unique_label_share_a"] == 0 else
                ("low" if r["unique_label_share_a"] < 0.10 else "some"))
        out["per_pair"].append({
            "pair_id": r["pair_id"], "discipline": r["discipline"],
            "n_obj_a": r["n_obj_a"], "n_ref_1to1": r["n_ref_1to1"],
            "ref_cover": r["ref_cover"], "band": band,
            "unique_label_share_a": r["unique_label_share_a"],
            "acc_geom": r["registered/geom"]["acc_on_ref"],
            "acc_geom_pos": r["registered/geom_pos"]["acc_on_ref"],
            "acc_geom_pos_label": r["registered/geom_pos_label"]["acc_on_ref"],
            "acc_raw_geom_pos": r["raw/geom_pos"]["acc_on_ref"],
            "acc_raw_geom_pos_label": r["raw/geom_pos_label"]["acc_on_ref"]})
    for band in ("none", "low", "some"):
        sel = [p for p in out["per_pair"] if p["band"] == band]
        if not sel:
            continue
        out["by_label_band"][band] = {
            "n_pairs": len(sel),
            "median_acc_geom": med([p["acc_geom"] for p in sel]),
            "median_acc_geom_pos": med([p["acc_geom_pos"] for p in sel]),
            "median_acc_geom_pos_label": med([p["acc_geom_pos_label"] for p in sel]),
            "delta_label": round((med([p["acc_geom_pos_label"] for p in sel]) or 0)
                                 - (med([p["acc_geom_pos"] for p in sel]) or 0), 4),
            "pooled_ref_objects": sum(p["n_ref_1to1"] for p in sel)}
    json.dump(out, open(L.ART / "lbl_l2_real_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("pairs used", len(rs), "of", len(d["pairs"]))
    print("ref_cover", out["ref_cover"])
    for cond, v in out["by_cond"].items():
        for m, s in v.items():
            p = out["pooled_by_cond"][cond][m]
            print(f"  {cond:11s} {m:15s} med_acc={s['median_acc_on_ref']} "
                  f"mean={s['mean_acc_on_ref']} pooled={p['acc_pooled']} "
                  f"({p['n_correct']}/{p['n_ref_objects']}) wrong={s['total_wrong_on_ref']} "
                  f"fr={s['median_false_removed_share']}")
    print("bands:", json.dumps(out["by_label_band"], ensure_ascii=False))


if __name__ == "__main__":
    main()
