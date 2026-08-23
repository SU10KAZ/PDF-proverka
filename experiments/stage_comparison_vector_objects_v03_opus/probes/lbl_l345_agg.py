# -*- coding: utf-8 -*-
"""Aggregate L3 (no-label mode), L4 (§7 false text dependence), L5 (binding stability)."""
from __future__ import annotations
import json, statistics, sys
from collections import defaultdict
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lbl_common as L


def med(v):
    return round(statistics.median(v), 4) if v else None


def agg_l3():
    d = json.load(open(L.ART / "lbl_l3_nolabel.json", encoding="utf-8"))
    cs = [c for c in d["carriers"] if not c.get("error")]
    pops = defaultdict(list)
    for c in cs:
        pops[c["side_a"]["population"]].append(c)
    out = {"n_carriers": len(d["carriers"]), "n_used": len(cs),
           "population_sizes": {k: len(v) for k, v in pops.items()},
           "errors": [(c["carrier"]["block_id"], c["error"]) for c in d["carriers"] if c.get("error")],
           "by_population": {}, "by_population_and_cf": {}}
    for pop, cc in pops.items():
        for mode in ("geom_pos", "geom_pos_label"):
            hits, sp, ent, on = [], [], [], []
            per_cf = defaultdict(lambda: {"hit": [], "sp": []})
            for c in cc:
                for r in c["rows"]:
                    if "skip" in r or mode not in r:
                        continue
                    hits.append(1 if r[mode]["hit"] else 0)
                    sp.append(r[mode]["spurious"])
                    on.append(r[mode]["on_target"])
                    ent.append(r[mode]["n_entries"])
                    per_cf[r["cf"]]["hit"].append(1 if r[mode]["hit"] else 0)
                    per_cf[r["cf"]]["sp"].append(r[mode]["spurious"])
            n = len(hits)
            out["by_population"].setdefault(pop, {})[mode] = {
                "n_instances": n,
                "hit_rate": round(sum(hits) / n, 4) if n else None,
                "median_spurious": med(sp), "mean_spurious": round(statistics.fmean(sp), 3) if sp else None,
                "share_zero_spurious": round(sum(1 for x in sp if x == 0) / n, 4) if n else None,
                "precision_entries": round(sum(on) / max(sum(ent), 1), 4),
                "median_entries": med(ent)}
            for cf, v in per_cf.items():
                out["by_population_and_cf"].setdefault(pop, {}).setdefault(cf, {})[mode] = {
                    "n": len(v["hit"]),
                    "hit_rate": round(sum(v["hit"]) / len(v["hit"]), 4) if v["hit"] else None,
                    "median_spurious": med(v["sp"])}
    json.dump(out, open(L.ART / "lbl_l3_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("== L3 populations:", out["population_sizes"])
    for pop, v in sorted(out["by_population"].items()):
        for mode, s in v.items():
            print(f"  {pop:5s} {mode:15s} n={s['n_instances']:3d} hit={s['hit_rate']} "
                  f"med_spurious={s['median_spurious']} zero_sp={s['share_zero_spurious']} "
                  f"prec_entries={s['precision_entries']}")
    return out


def agg_l4():
    d = json.load(open(L.ART / "lbl_l4_d3.json", encoding="utf-8"))
    cs = [c for c in d["carriers"] if not c.get("error")]
    out = {"n_carriers": len(d["carriers"]), "n_used": len(cs),
           "errors": [(c["carrier"]["block_id"], c["error"]) for c in d["carriers"] if c.get("error")],
           "by_cf": {}}
    per = defaultdict(lambda: defaultdict(list))
    for c in cs:
        for r in c["rows"]:
            if "skip" in r:
                continue
            cf = r["cf"]
            for use in ("anchor", "evidence"):
                k = f"label_{use}"
                if k not in r:
                    continue
                v = r[k]
                per[(cf, use)]["fp_verdict"].append(1 if v["verdict_plain"] == "GRAPHIC_CHANGE" else 0)
                per[(cf, use)]["n_entries"].append(v["n_entries_plain"])
                per[(cf, use)]["mispairs"].append(v["mispairs"])
                per[(cf, use)]["precision"].append(v["precision"])
                per[(cf, use)]["recall"].append(v["recall"])
                per[(cf, use)]["label_changed"].append(v["label_census"]["changed"])
                per[(cf, use)]["label_changed_share"].append(
                    v["label_census"]["changed_share_of_labelled"])
                per[(cf, use)]["renamed_entries"].append(v["n_renamed_entries"])
                per[(cf, use)]["fp_verdict_lblev"].append(
                    1 if v["verdict_label_as_evidence"] == "GRAPHIC_CHANGE" else 0)
            if "geom_pos" in r:
                v = r["geom_pos"]
                per[(cf, "no_label")]["fp_verdict"].append(
                    1 if v["verdict_plain"] == "GRAPHIC_CHANGE" else 0)
                per[(cf, "no_label")]["n_entries"].append(v["n_entries_plain"])
                per[(cf, "no_label")]["precision"].append(v["precision"])
                per[(cf, "no_label")]["recall"].append(v["recall"])
    for (cf, use), v in sorted(per.items()):
        n = len(v["fp_verdict"])
        out["by_cf"].setdefault(cf, {})[use] = {
            "n": n,
            "share_GRAPHIC_CHANGE": round(sum(v["fp_verdict"]) / n, 4) if n else None,
            "median_entries": med(v["n_entries"]),
            "max_entries": max(v["n_entries"]) if v["n_entries"] else None,
            "median_precision": med(v["precision"]), "median_recall": med(v["recall"]),
            "median_mispairs": med(v.get("mispairs", [])),
            "total_mispairs": sum(v.get("mispairs", [])),
            "median_labels_changed": med(v.get("label_changed", [])),
            "total_labels_changed": sum(v.get("label_changed", [])),
            "median_label_changed_share": med(v.get("label_changed_share", [])),
            "share_GRAPHIC_CHANGE_if_label_is_evidence":
                round(sum(v["fp_verdict_lblev"]) / n, 4) if v.get("fp_verdict_lblev") else None,
            "total_RENAMED_entries": sum(v.get("renamed_entries", [])),
        }
    json.dump(out, open(L.ART / "lbl_l4_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("== L4 (n carriers used:", len(cs), ")")
    for cf, v in sorted(out["by_cf"].items()):
        for use, s in sorted(v.items()):
            print(f"  {cf:26s} {use:9s} n={s['n']:3d} FP_verdict={s['share_GRAPHIC_CHANGE']} "
                  f"med_entries={s['median_entries']} mispairs={s['total_mispairs']} "
                  f"lbl_changed={s['total_labels_changed']} "
                  f"FP_if_label_evidence={s['share_GRAPHIC_CHANGE_if_label_is_evidence']}")
    return out


def agg_l5():
    out = {}
    p = L.ART / "lbl_l5_real.json"
    if p.exists():
        d = json.load(open(p, encoding="utf-8"))
        rs = [r for r in d["pairs"] if "binding" in r]
        tot = defaultdict(int)
        for r in rs:
            for k, v in r["binding"].items():
                tot[k] += v
        both = tot["both_same"] + tot["both_diff"]
        n11 = sum(tot.values())
        same_shares = [r["same_label_share"] for r in rs if r["same_label_share"] is not None]
        out["real"] = {
            "n_pairs_measured": len(rs),
            "n_pairs_skipped": len(d["pairs"]) - len(rs),
            "skipped": [(r["pair_id"], r.get("skip") or r.get("error")) for r in d["pairs"]
                        if "binding" not in r],
            "n_1to1_objects": n11, "totals": dict(tot),
            "same_label_share_pooled": round(tot["both_same"] / both, 5) if both else None,
            "same_label_share_median_over_pairs": med(same_shares),
            "share_of_1to1_with_label_on_both_sides": round(both / max(n11, 1), 5),
            "share_of_1to1_with_no_label_at_all": round(tot["neither"] / max(n11, 1), 5),
            "share_of_1to1_one_sided_label": round((tot["a_only"] + tot["b_only"]) / max(n11, 1), 5),
            "rebind_to_another_a_mark": sum(r.get("rebind_to_another_a_mark", 0) for r in rs),
            "per_pair": [{"pair_id": r["pair_id"], "n_1to1": r["n_1to1"],
                          "both_labelled": r["n_both_labelled"],
                          "same_label_share": r["same_label_share"],
                          "no_label_share": r["no_label_share"],
                          "expected": r["expected"],
                          "diff_examples": r["diff_examples"][:4]} for r in rs],
        }
    p = L.ART / "lbl_l5_cf.json"
    if p.exists():
        d = json.load(open(p, encoding="utf-8"))
        per = defaultdict(lambda: defaultdict(int))
        shares = defaultdict(list)
        for c in d["carriers"]:
            for r in c.get("rows", []):
                if "binding" not in r:
                    continue
                for k, v in r["binding"].items():
                    per[r["cf"]][k] += v
                if r["same_label_share"] is not None:
                    shares[r["cf"]].append(r["same_label_share"])
        out["cf"] = {}
        for cf, t in per.items():
            both = t["both_same"] + t["both_diff"]
            n = sum(t.values())
            out["cf"][cf] = {"n_gt_pairs": n, "totals": dict(t),
                             "same_label_share_pooled": round(t["both_same"] / both, 5) if both else None,
                             "same_label_share_median": med(shares[cf]),
                             "label_lost_share": round(t["a_only"] / max(n, 1), 5),
                             "n_blocks": len(shares[cf])}
    p = L.ART / "lbl_l5_radius.json"
    if p.exists():
        d = json.load(open(p, encoding="utf-8"))
        rs = [c for c in d["carriers"] if "n_obj" in c]
        s = lambda k: sum(c[k] for c in rs)
        out["radius"] = {
            "n_blocks": len(rs), "n_obj": s("n_obj"),
            "labelled_share_1.0": round(s("labelled_1.0") / max(s("n_obj"), 1), 4),
            "labelled_share_1.6": round(s("labelled_1.6") / max(s("n_obj"), 1), 4),
            "labelled_share_2.5": round(s("labelled_2.5") / max(s("n_obj"), 1), 4),
            "mark_changes_1.0_to_1.6": round(
                s("diff_mark_1.0_vs_1.6") / max(s("same_mark_1.0_vs_1.6") + s("diff_mark_1.0_vs_1.6"), 1), 4),
            "mark_changes_1.6_to_2.5": round(
                s("diff_mark_1.6_vs_2.5") / max(s("same_mark_1.6_vs_2.5") + s("diff_mark_1.6_vs_2.5"), 1), 4),
        }
    json.dump(out, open(L.ART / "lbl_l5_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("== L5")
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk not in ("per_pair", "skipped")}
                      for k, v in out.items()}, ensure_ascii=False, indent=1))
    return out


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("all", "l3"):
        try: agg_l3()
        except FileNotFoundError as e: print("l3 missing", e)
    if which in ("all", "l4"):
        try: agg_l4()
        except FileNotFoundError as e: print("l4 missing", e)
    if which in ("all", "l5"):
        agg_l5()
