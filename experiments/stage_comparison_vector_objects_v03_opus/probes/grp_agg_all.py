# -*- coding: utf-8 -*-
"""Aggregate G2 (real + CF density), G4, G5, G6 into their artifacts."""
from __future__ import annotations
import json, statistics, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G

def med(v):
    return round(statistics.median(v), 5) if v else None

def pq(v, q):
    return round(G.pct(v, q), 5) if v else None

def load(tag):
    rows = []
    for f in sorted((G.ART / "grp_runs").glob(f"{tag}_*.jsonl")):
        for line in open(f, encoding="utf-8"):
            if line.strip():
                rows.append(json.loads(line))
    return rows


# ---------------------------------------------------------------- G2
def g2():
    real = json.load(open(G.ART / "grp_boundary_churn_real.json", encoding="utf-8"))
    pairs = real["pairs"]
    ok = [p for p in pairs if "error" not in p]
    good = [p for p in ok if p.get("reg_score", 0) >= 0.90]
    unchanged = [p for p in good if p["expected"] == "NO_GRAPHIC_CHANGE"
                 and "uncertain" not in p["classes"]]
    changed = [p for p in good if p["expected"] == "GRAPHIC_CHANGE"
               and "uncertain" not in p["classes"]]

    def summ(rows, key="churn_ab"):
        if not rows:
            return None
        o = [r[key]["one_to_one"] for r in rows]
        return {"n": len(rows), "one_to_one_median": med(o), "one_to_one_p10": pq(o, 0.10),
                "one_to_one_min": round(min(o), 5),
                "mean_by_kind": {k: round(statistics.mean([r[key][k] for r in rows]), 5)
                                 for k in ("one_to_one", "split", "merge", "mixed", "lost")},
                "pairs_perfect_1to1": sum(1 for x in o if x >= 0.999),
                "d_obj_abs_median": med([abs(r["d_obj"]) for r in rows]),
                "d_obj_rel_median": med([abs(r["d_obj"]) / max(1, r["n_obj_a"]) for r in rows])}

    # CF density curve, both the <=20k pass and the dense pass
    cf_rows = [r for r in load("g1") if not r.get("arc_ablation") and "rewrites" in r]
    cf_rows += [r for r in load("g1d") if not r.get("arc_ablation") and "rewrites" in r]
    dens = {}
    for r in cf_rows:
        for n in ("A6_round_0.1", "A6_round_0.25", "A5_order_shuffle", "A1_path_split",
                  "A4b_circle_to_chords5"):
            d = r["rewrites"].get(n)
            if not d or d.get("bite", 0) <= 0:
                continue
            dens.setdefault(n, {}).setdefault(r["bucket"], []).append(d["churn"]["one_to_one"])
    dens_out = {n: {b: {"n": len(v), "median": med(v), "p10": pq(v, 0.10),
                        "min": round(min(v), 5),
                        "share_below_0.9": round(sum(1 for x in v if x < 0.9) / len(v), 4)}
                    for b, v in sorted(bb.items())} for n, bb in dens.items()}
    # per block class
    dens_cls = {}
    for r in cf_rows:
        d = r["rewrites"].get("A6_round_0.25")
        if not d or d.get("bite", 0) <= 0:
            continue
        dens_cls.setdefault(r["cls"], []).append(d["churn"]["one_to_one"])
    # churn vs raw segment density (segments per 1000 pt^2 is unavailable here; use n_seg)
    curve = []
    for lo, hi in ((20, 200), (200, 500), (500, 1500), (1500, 5000), (5000, 15000),
                   (15000, 50000), (50000, 10**9)):
        v = [r["rewrites"]["A6_round_0.25"]["churn"]["one_to_one"] for r in cf_rows
             if lo <= r["n_seg"] < hi and r["rewrites"].get("A6_round_0.25", {}).get("bite", 0) > 0]
        if v:
            curve.append({"n_seg_range": [lo, hi], "n": len(v), "median": med(v),
                          "p10": pq(v, 0.10), "share_below_0.9": round(sum(1 for x in v if x < 0.9) / len(v), 4)})

    out = {
        "real": {"n_pairs_total": len(pairs), "n_ok": len(ok),
                 "n_registration_ok_0.90": len(good),
                 "registration_failed": [{"pair_id": p["pair_id"], "reg_score": p.get("reg_score"),
                                          "classes": p["classes"]}
                                         for p in ok if p.get("reg_score", 0) < 0.90],
                 "errors": [{"pair_id": p["pair_id"], "error": p["error"]} for p in pairs if "error" in p],
                 "unchanged_pairs": summ(unchanged),
                 "changed_pairs": summ(changed),
                 "unchanged_interior_only": summ(unchanged, "churn_ab_interior"),
                 "per_pair": [{"pair_id": p["pair_id"], "classes": p["classes"],
                               "expected": p["expected"], "reg_score": p["reg_score"],
                               "n_seg_a": p["n_seg_a"], "n_seg_b": p["n_seg_b"],
                               "n_obj_a": p["n_obj_a"], "n_obj_b": p["n_obj_b"],
                               "churn_ab": p["churn_ab"], "churn_ba": p["churn_ba"],
                               "churn_ab_interior": p["churn_ab_interior"],
                               "border_obj_share": p["border_obj_share"],
                               "unmatched_ink_share_a": p.get("unmatched_ink_share_a")}
                              for p in ok]},
        "counterfactual_density": {"by_bucket": dens_out,
                                   "by_block_class_A6_round_0.25":
                                       {c: {"n": len(v), "median": med(v), "p10": pq(v, 0.10)}
                                        for c, v in sorted(dens_cls.items())},
                                   "curve_A6_round_0.25": curve},
    }
    json.dump(out, open(G.ART / "grp_boundary_churn.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    return out


# ---------------------------------------------------------------- G4
def g4():
    rows = load("g4")
    census = None
    data = []
    for r in rows:
        if "ocg_census" in r:
            census = r["ocg_census"]
        elif "error" not in r:
            data.append(r)
    out = {"ocg_census_documents": census,
           "n_blocks": len(data),
           "join_sidecar_to_segments_ok_min": min((r["join_ok"] for r in data), default=None),
           "join_sidecar_to_segments_ok_median": med([r["join_ok"] for r in data]),
           "blocks_with_any_layer_name": sum(1 for r in data if r["n_layers"] > 0),
           "share_blocks_with_layer": round(sum(1 for r in data if r["n_layers"] > 0) /
                                            max(1, len(data)), 4),
           "styles_per_block_median": med([r["n_styles"] for r in data]),
           "object_count_style_on_vs_off": {
               "median_ratio": med([r["n_obj_style_on"] / max(1, r["n_obj_style_off"]) for r in data]),
               "blocks_more_objects_with_style": sum(1 for r in data
                                                     if r["n_obj_style_on"] > r["n_obj_style_off"]),
           }}
    per = {}
    for r in data:
        for name, d in (r.get("rewrites") or {}).items():
            per.setdefault(name, {"off": [], "on": [], "d": []})
            per[name]["off"].append(d["off"]["one_to_one"])
            per[name]["on"].append(d["on"]["one_to_one"])
            per[name]["d"].append(d["on"]["one_to_one"] - d["off"]["one_to_one"])
    out["churn_with_and_without_style"] = {
        n: {"n": len(v["off"]), "one_to_one_median_off": med(v["off"]),
            "one_to_one_median_on": med(v["on"]),
            "delta_median": med(v["d"]), "delta_mean": round(statistics.mean(v["d"]), 5),
            "blocks_improved": sum(1 for x in v["d"] if x > 1e-6),
            "blocks_worsened": sum(1 for x in v["d"] if x < -1e-6)}
        for n, v in sorted(per.items())}
    lp = [r["object_layer_purity_median"] for r in data if "object_layer_purity_median" in r]
    mix = [r["share_objects_mixing_layers"] for r in data if "share_objects_mixing_layers" in r]
    out["cad_layer_predicts_object_boundary"] = {
        "n_blocks_with_layers": len(lp), "purity_median": med(lp),
        "share_objects_mixing_layers_median": med(mix),
        "share_objects_mixing_layers_p90": pq(mix, 0.90)} if lp else \
        {"n_blocks_with_layers": 0, "note": "no optional-content layer reaches any block"}
    out["blocks"] = data
    json.dump(out, open(G.ART / "grp_style_layer.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    return out


# ---------------------------------------------------------------- G5
def g5():
    rows = [r for r in load("g5") if "error" not in r]
    params = {}
    for r in rows:
        for pname, series in r["sweeps"].items():
            base = None
            for row in series:
                params.setdefault(pname, {}).setdefault(str(row["v"]), {"n_obj": [], "o2o": [], "rel": []})
                params[pname][str(row["v"])]["n_obj"].append(row["n_obj"])
                params[pname][str(row["v"])]["o2o"].append(row["one_to_one"])
            # relative object count against the default value of this parameter
            import v03_objects as O
            dflt = str(O.DEFAULTS[pname])
            base = next((x["n_obj"] for x in series if str(x["v"]) == dflt), None)
            if base:
                for row in series:
                    params[pname][str(row["v"])]["rel"].append(row["n_obj"] / max(1, base))
    out = {"n_blocks": len(rows), "probe_rewrite": "A6_round_0.25",
           "note": "one parameter swept at a time, everything else at DEFAULTS",
           "sweeps": {}}
    for pname, vals in params.items():
        out["sweeps"][pname] = [
            {"value": v, "n": len(d["n_obj"]),
             "n_obj_median": med(d["n_obj"]),
             "n_obj_rel_to_default_median": med(d["rel"]),
             "churn_1to1_median": med(d["o2o"]),
             "churn_1to1_p10": pq(d["o2o"], 0.10)}
            for v, d in vals.items()]
    json.dump(out, open(G.ART / "grp_param_sweep.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    return out


# ---------------------------------------------------------------- G6
def g6():
    rows = [r for r in load("g6") if "error" not in r]
    err = [r for r in load("g6") if "error" in r]
    def by(keyfn):
        g = {}
        for r in rows:
            g.setdefault(keyfn(r), []).append(r)
        return {k: {"n": len(v),
                    "n_seg_median": med([x["n_seg"] for x in v]),
                    "t_build_median_s": med([x["t_build"] for x in v]),
                    "t_build_p90_s": pq([x["t_build"] for x in v], 0.90),
                    "t_build_max_s": round(max(x["t_build"] for x in v), 3),
                    "t_extract_median_s": med([x["t_extract"] for x in v]),
                    "us_per_segment_median": med([x["us_per_segment"] for x in v]),
                    "n_obj_median": med([x["n_obj"] for x in v]),
                    "bytes_full_median": med([x["bytes_full"] for x in v]),
                    "bytes_compact_median": med([x["bytes_compact"] for x in v]),
                    "tokens_full_median": med([x["tokens_full"] for x in v]),
                    "tokens_compact_median": med([x["tokens_compact"] for x in v]),
                    "tokens_compact_p90": pq([x["tokens_compact"] for x in v], 0.90)}
                for k, v in sorted(g.items())}
    out = {"n_blocks": len(rows), "n_errors": len(err),
           "errors": err[:10],
           "token_rule": "len(json_utf8)/4",
           "overall": {
               "t_build_median_s": med([r["t_build"] for r in rows]),
               "t_build_p90_s": pq([r["t_build"] for r in rows], 0.90),
               "t_build_max_s": round(max((r["t_build"] for r in rows), default=0), 3),
               "us_per_segment_median": med([r["us_per_segment"] for r in rows]),
               "us_per_segment_p90": pq([r["us_per_segment"] for r in rows], 0.90),
               "ink_coverage_min": min((r["ink_coverage"] for r in rows), default=None),
               "stray_len_share_median": med([r["stray_len_share"] for r in rows]),
               "stray_len_share_p90": pq([r["stray_len_share"] for r in rows], 0.90),
               "tokens_compact_median": med([r["tokens_compact"] for r in rows]),
               "tokens_compact_p90": pq([r["tokens_compact"] for r in rows], 0.90),
               "tokens_full_median": med([r["tokens_full"] for r in rows]),
           },
           "by_bucket": by(lambda r: r["bucket"]),
           "by_class": by(lambda r: r["cls"]),
           "blocks": rows}
    json.dump(out, open(G.ART / "grp_cost.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    return out


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    for name, fn in (("g2", g2), ("g4", g4), ("g5", g5), ("g6", g6)):
        if which in ("all", name):
            try:
                r = fn()
                print("==", name, json.dumps({k: v for k, v in r.items()
                                              if k not in ("blocks", "per_pair")},
                                             ensure_ascii=False)[:2500])
            except Exception as e:
                print("!!", name, repr(e))
