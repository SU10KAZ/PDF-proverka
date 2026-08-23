# -*- coding: utf-8 -*-
"""Aggregate the L2 ablation: how much does each information mode add?"""
from __future__ import annotations
import json, sys, statistics
from collections import defaultdict
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lbl_common as L

MODES = ("geom", "geom_pos", "geom_pos_label")
KEYS = ("top1", "precision", "recall", "false_removed_share", "false_added_share")


def med(v):
    return round(statistics.median(v), 4) if v else None


def main():
    d = json.load(open(L.ART / "lbl_l2_ablation.json", encoding="utf-8"))
    cs = d["carriers"]
    ok = [c for c in cs if not c.get("error")]
    out = {"n_carriers": len(cs), "n_used": len(ok),
           "errors": [(c["carrier"]["block_id"], c["error"]) for c in cs if c.get("error")],
           "match_params": d["match_params"], "by_cf": {}, "by_label_availability": {}}
    # label availability of the carriers
    ul = [c["side_a"]["unique_label_share"] for c in ok]
    out["carrier_unique_label_share"] = L.summarise(ul)
    acc = defaultdict(lambda: defaultdict(list))
    accs = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for c in ok:
        band = ("none" if c["side_a"]["unique_label_share"] == 0 else
                ("low" if c["side_a"]["unique_label_share"] < 0.10 else "some"))
        for r in c["rows"]:
            if "skip" in r:
                continue
            for cond in ("registered", "raw"):
                for m in MODES:
                    k = f"{cond}/{m}"
                    if k not in r:
                        continue
                    for key in KEYS:
                        v = r[k].get(key)
                        if v is not None:
                            acc[(r["cf"], cond, m)][key].append(v)
                            accs[band][(r["cf"], cond, m)][key].append(v)
    for (cf, cond, m), vals in sorted(acc.items()):
        out["by_cf"].setdefault(cf, {}).setdefault(cond, {})[m] = \
            {k: med(v) for k, v in vals.items()} | {"n": len(vals.get("recall", []))}
    for band, a in accs.items():
        for (cf, cond, m), vals in sorted(a.items()):
            out["by_label_availability"].setdefault(band, {}).setdefault(cf, {}) \
                .setdefault(cond, {})[m] = {k: med(v) for k, v in vals.items()} | \
                {"n": len(vals.get("recall", []))}
    # "what label adds" table
    delta = {}
    for cf in out["by_cf"]:
        for cond in out["by_cf"][cf]:
            g = out["by_cf"][cf][cond].get("geom_pos", {})
            l = out["by_cf"][cf][cond].get("geom_pos_label", {})
            g0 = out["by_cf"][cf][cond].get("geom", {})
            if g and l:
                delta.setdefault(cf, {})[cond] = {
                    "recall_geom": g0.get("recall"), "recall_geom_pos": g.get("recall"),
                    "recall_geom_pos_label": l.get("recall"),
                    "delta_label_recall": round((l.get("recall") or 0) - (g.get("recall") or 0), 4),
                    "prec_geom": g0.get("precision"), "prec_geom_pos": g.get("precision"),
                    "prec_geom_pos_label": l.get("precision"),
                    "delta_label_precision": round((l.get("precision") or 0) - (g.get("precision") or 0), 4),
                    "top1_geom": g0.get("top1"), "top1_geom_pos": g.get("top1"),
                    "top1_geom_pos_label": l.get("top1"),
                    "n": l.get("n")}
    out["what_label_adds"] = delta
    json.dump(out, open(L.ART / "lbl_l2_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("carriers", len(ok), "of", len(cs), " unique_label_share:", out["carrier_unique_label_share"])
    hdr = f"{'cf':28s} {'cond':11s} | top1 g/gp/gpl        | recall g/gp/gpl      | prec g/gp/gpl        | n"
    print(hdr)
    for cf, cc in delta.items():
        for cond, v in cc.items():
            print(f"{cf:28s} {cond:11s} | "
                  f"{v['top1_geom']!s:6s} {v['top1_geom_pos']!s:6s} {v['top1_geom_pos_label']!s:6s} | "
                  f"{v['recall_geom']!s:6s} {v['recall_geom_pos']!s:6s} {v['recall_geom_pos_label']!s:6s} | "
                  f"{v['prec_geom']!s:6s} {v['prec_geom_pos']!s:6s} {v['prec_geom_pos_label']!s:6s} | {v['n']}")


if __name__ == "__main__":
    main()
