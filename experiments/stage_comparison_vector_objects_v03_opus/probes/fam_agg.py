# -*- coding: utf-8 -*-
"""Aggregate every fam artifact into artifacts/fam_summary.json + printable tables."""
from __future__ import annotations
import json, statistics, sys, collections
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G

ART = G.ART


def q(vals, p):
    return round(G.pct(vals, p), 5) if vals else None


def S(vals):
    if not vals:
        return None
    return {"n": len(vals), "median": round(statistics.median(vals), 5),
            "mean": round(statistics.mean(vals), 5), "p10": q(vals, .10),
            "p90": q(vals, .90), "min": round(min(vals), 5), "max": round(max(vals), 5)}


def f2():
    p = ART / "fam_f2_rewrite.jsonl"
    if not p.exists():
        return None
    rows = [json.loads(l) for l in open(p, encoding="utf-8")]
    per = collections.defaultdict(list)
    for r in rows:
        if r.get("error"):
            continue
        per[r["rewrite"]].append(r)
    out = {"n_blocks": len({r["block_id"] for r in rows}),
           "n_disciplines": len({r["discipline"] for r in rows}),
           "per_rewrite": {}}
    for k in sorted(per):
        rs = [r for r in per[k] if r.get("ari") is not None]
        bit = [r for r in per[k] if r.get("bite", 0) > 0 or k == "A0_identity"]
        ari = [r["ari"] for r in rs]
        same = [r["same_family_share"] for r in rs if r.get("same_family_share") is not None]
        lab = [r["labeled_share"] for r in rs]
        fd = [r["false_delta_rows"] for r in per[k] if "false_delta_rows" in r]
        fdi = [r["false_delta_ink_share"] for r in per[k] if "false_delta_ink_share" in r]
        nf = [(r["n_fam_b"] - r["n_fam"]) for r in per[k] if "n_fam_b" in r]
        out["per_rewrite"][k] = {
            "n": len(per[k]), "n_effective": len(bit),
            "ari": S(ari), "ari_eq1_share": round(sum(1 for a in ari if a > 0.99999) / max(len(ari), 1), 4),
            "same_family_share": S(same),
            "labeled_ink_share": S(lab),
            "false_delta_rows": S([float(x) for x in fd]),
            "false_delta_rows_zero_share": round(sum(1 for x in fd if x == 0) / max(len(fd), 1), 4),
            "false_delta_ink_share": S(fdi),
            "d_n_families": S([float(x) for x in nf]),
        }
    # by density bucket for the worst rewrite
    dens = collections.defaultdict(list)
    for r in per.get("A6_round_0.25", []):
        if r.get("ari") is None:
            continue
        n = r["n_seg"]
        b = ("<200" if n < 200 else "200-500" if n < 500 else "500-1500" if n < 1500 else
             "1500-5000" if n < 5000 else "5000-15000" if n < 15000 else ">=15000")
        dens[b].append(r["ari"])
    out["A6_round_0.25_by_density"] = {k: S(v) for k, v in dens.items()}
    return out


def f3():
    p = ART / "fam_f3_false.json"
    if not p.exists():
        return None
    d = json.load(open(p, encoding="utf-8"))
    ps = [x for x in d["pairs"] if not x.get("error")]
    out = {"n_pairs": len(ps), "n_error": len(d["pairs"]) - len(ps),
           "by_pop": dict(collections.Counter(x["pop"] for x in ps)), "variants": {}, "floor": {}}
    keys = sorted({k for x in ps for k in x.get("variants", {})})
    for k in keys:
        for pop in ("L", "Q", "ALL"):
            sub = [x for x in ps if pop == "ALL" or x["pop"] == pop]
            fr = [x["variants"][k]["false_rows_min2"] for x in sub if k in x["variants"]]
            f3_ = [x["variants"][k]["false_rows_min3"] for x in sub if k in x["variants"]]
            rep = [x["variants"][k]["n_repeated"] for x in sub if k in x["variants"]]
            out["variants"][f"{k}|{pop}"] = {
                "n_pairs": len(fr), "false_rows_total": sum(fr),
                "false_rows_median": statistics.median(fr) if fr else None,
                "false_rows_max": max(fr) if fr else None,
                "pairs_clean": sum(1 for x in fr if x == 0),
                "pairs_clean_share": round(sum(1 for x in fr if x == 0) / max(len(fr), 1), 4),
                "false_rows_min3_total": sum(f3_),
                "pairs_clean_min3_share": round(sum(1 for x in f3_ if x == 0) / max(len(f3_), 1), 4),
                "repeated_families_total": sum(rep),
                "false_per_repeated_family": round(sum(fr) / max(sum(rep), 1), 5),
            }
    floors = sorted({fl for x in ps for fl in x.get("floor_sweep", {})}, key=float)
    for fl in floors:
        fr = [x["floor_sweep"][fl]["false_rows_min2"] for x in ps if fl in x.get("floor_sweep", {})]
        f3_ = [x["floor_sweep"][fl]["false_rows_min3"] for x in ps if fl in x.get("floor_sweep", {})]
        rep = [x["floor_sweep"][fl]["n_repeated"] for x in ps if fl in x.get("floor_sweep", {})]
        blw = [x["floor_sweep"][fl]["n_below_floor"] for x in ps if fl in x.get("floor_sweep", {})]
        nob = [x["floor_sweep"][fl]["n_obj"] for x in ps if fl in x.get("floor_sweep", {})]
        out["floor"][fl] = {"n_pairs": len(fr), "false_rows_total": sum(fr),
                            "false_rows_max": max(fr) if fr else None,
                            "pairs_clean_share": round(sum(1 for x in fr if x == 0) / max(len(fr), 1), 4),
                            "false_rows_min3_total": sum(f3_),
                            "repeated_families_total": sum(rep),
                            "objects_dropped_share": round(sum(blw) / max(sum(nob), 1), 5)}
    k = "shared|interior|twopass"
    out["worst_pairs"] = sorted(
        [{"pair_id": x["pair_id"], "pop": x["pop"], "discipline": x["discipline"],
          "classes": x.get("classes"), "false_rows": x["variants"][k]["false_rows_min2"],
          "n_repeated": x["variants"][k]["n_repeated"],
          "top": x["variants"][k]["top"][:3]}
         for x in ps if k in x["variants"]],
        key=lambda r: -r["false_rows"])[:8]
    return out


def f4():
    p = ART / "fam_f4_cf.json"
    if not p.exists():
        return None
    rows = json.load(open(p, encoding="utf-8"))["rows"]
    ok = [r for r in rows if r.get("verdict")]
    out = {"n_rows": len(rows), "n_scored": len(ok),
           "n_blocks": len({r.get("block_id") for r in rows}),
           "n_disc": len({r.get("discipline") for r in rows if r.get("discipline")}),
           "skips": dict(collections.Counter(r["skip"][:40] for r in rows if r.get("skip"))),
           "by_sel_cf": {}, "by_family_size": {}, "by_area_bucket": {}}
    for (sel, cf), grp in sorted(collections.Counter().__class__().items() if False else
                                 _group(ok, lambda r: (r["sel"], r["cf"])).items()):
        v = collections.Counter(r["verdict"] for r in grp)
        ex = [r["n_extra_rows"] for r in grp]
        out["by_sel_cf"][f"{sel}|{cf}"] = {
            "n": len(grp), **{k: v.get(k, 0) for k in ("hit", "silent", "wrong")},
            "hit_share_of_published": round(v.get("hit", 0) / max(v.get("hit", 0) + v.get("wrong", 0), 1), 4),
            "published_share": round((v.get("hit", 0) + v.get("wrong", 0)) / max(len(grp), 1), 4),
            "extra_rows_median": statistics.median(ex) if ex else None,
            "extra_rows_total": sum(ex),
            "rank0_share": round(sum(1 for r in grp if r.get("rank_of_target") == 0) / max(len(grp), 1), 4)}
    for k, grp in sorted(_group([r for r in ok if r["sel"] != "any"],
                                lambda r: min(r.get("fam_size_a", 1), 9)).items()):
        v = collections.Counter(r["verdict"] for r in grp)
        out["by_family_size"][str(k)] = {"n": len(grp), **{x: v.get(x, 0) for x in ("hit", "silent", "wrong")}}
    for k, grp in sorted(_group([r for r in ok if r["sel"] != "any"], lambda r: r["bucket"]).items()):
        v = collections.Counter(r["verdict"] for r in grp)
        af = [r.get("obj_area_frac", 0) for r in grp]
        out["by_area_bucket"][k] = {"n": len(grp), **{x: v.get(x, 0) for x in ("hit", "silent", "wrong")},
                                    "area_frac_median": round(statistics.median(af), 6) if af else None}
    return out


def _group(rows, key):
    d = collections.defaultdict(list)
    for r in rows:
        d[key(r)].append(r)
    return d


def f4b():
    p = ART / "fam_f4b_real.json"
    if not p.exists():
        return None
    d = json.load(open(p, encoding="utf-8"))
    ps = d["pairs"]
    out = {"n_pairs": len(ps), "n_error": sum(1 for x in ps if x.get("error")), "rows": []}
    for x in ps:
        v = (x.get("variants") or {}).get("interior", {})
        out["rows"].append({
            "pair_id": x["pair_id"], "verdict": x.get("expected_verdict"),
            "classes": x.get("classes"), "expected_changed_objects": x.get("expected_changed_objects"),
            "error": x.get("error"),
            "rows_min2": v.get("rows_min2"), "rows_min3": v.get("rows_min3"),
            "delta_sum": v.get("delta_sum"), "delta_max": v.get("delta_max"),
            "n_repeated": v.get("n_repeated"),
            "ledger_top": (v.get("ledger") or [])[:4]})
    return out


def f5():
    import glob
    files = sorted(glob.glob(str(ART / "fam_f5_scope_s*.jsonl")))
    if not files:
        return None
    rows = []
    for fn in files:
        for line in open(fn, encoding="utf-8"):
            if line.strip():
                rows.append(json.loads(line))
    d = {"n_sampled": len(rows), "corpus_n": 43261}
    ok = [r for r in rows if "share_obj_in_repeated" in r]
    out = {"n_sampled": d["n_sampled"], "corpus_n": d["corpus_n"], "n_ok": len(ok),
           "n_error": sum(1 for r in rows if r.get("error")),
           "verdicts": dict(collections.Counter(r.get("verdict") or "ERR" for r in rows)),
           "verdict_share": {}, "by_class": {}, "by_discipline": {}}
    n = len(rows)
    for k, c in collections.Counter(r.get("verdict") or "ERR" for r in rows).items():
        out["verdict_share"][k] = round(c / n, 4)
    for k, grp in sorted(_group(rows, lambda r: r.get("cls") or "?").items()):
        c = collections.Counter(r.get("verdict") or "ERR" for r in grp)
        out["by_class"][k] = {"n": len(grp), **dict(c),
                              "usable_share": round(c.get("usable", 0) / len(grp), 4)}
    for k, grp in sorted(_group(rows, lambda r: r.get("discipline") or "?").items()):
        c = collections.Counter(r.get("verdict") or "ERR" for r in grp)
        out["by_discipline"][k] = {"n": len(grp), "usable_share": round(c.get("usable", 0) / len(grp), 4)}
    sh = [r["share_obj_in_repeated"] for r in ok]
    ink = [r["share_ink_in_repeated"] for r in ok]
    lf = [r["largest_family_share_obj"] for r in ok]
    out["share_objects_in_repeated"] = S(sh)
    out["share_ink_in_repeated"] = S(ink)
    out["largest_family_share_objects"] = S(lf)
    return out


def f7():
    p = ART / "fam_f7_gate.json"
    if not p.exists():
        return None
    d = json.load(open(p, encoding="utf-8"))
    rows = d["rows"]
    fal = [r for r in rows if r.get("label") == "FALSE"]
    tru = [r for r in rows if r.get("label") == "TRUE"]
    out = {"n_false": len(fal), "n_true": len(tru),
           "n_blocks": len({r.get("block_id") for r in rows}),
           "true_correct": sum(1 for r in tru if r.get("correct")),
           "feature_split": {}, "gates": {}, "sweeps": {}}
    for feat in ("n", "delta", "rel", "margin", "radius", "slack", "n_seg_med", "ink_share", "diag_med"):
        fv = [r[feat] for r in fal if r.get(feat) is not None]
        tv = [r[feat] for r in tru if r.get(feat) is not None]
        out["feature_split"][feat] = {"false": S([float(x) for x in fv]), "true": S([float(x) for x in tv])}
    # simple one-feature gates
    def gate(name, fn):
        kf = sum(1 for r in fal if fn(r))     # false rows killed
        kt = sum(1 for r in tru if fn(r) and r.get("correct"))
        out["gates"][name] = {"false_killed": kf, "false_killed_share": round(kf / max(len(fal), 1), 4),
                              "true_lost": kt,
                              "true_lost_share": round(kt / max(sum(1 for r in tru if r.get("correct")), 1), 4)}
    gate("n_seg_med<=2", lambda r: (r.get("n_seg_med") or 9) <= 2)
    gate("diag<2pt", lambda r: (r.get("diag_med") or 99) < 2.0)
    gate("diag<4pt", lambda r: (r.get("diag_med") or 99) < 4.0)
    gate("rel<0.2", lambda r: (r.get("rel") or 1) < 0.2)
    gate("n>=4", lambda r: (r.get("n") or 0) >= 4)
    gate("slack<0", lambda r: r.get("slack") is not None and r["slack"] < 0)
    gate("margin<eps", lambda r: r.get("margin") is not None and r["margin"] < 0.25)
    gate("ink_share<0.005", lambda r: (r.get("ink_share") or 1) < 0.005)
    sw = _group([r for r in rows if r.get("label") == "_SWEEP"], lambda r: (r["knob"], r["value"]))
    for (knob, val), grp in sorted(sw.items()):
        out["sweeps"][f"{knob}={val}"] = {
            "n_blocks": len(grp), "false_rows_total": sum(r["false_rows"] for r in grp),
            "repeated_total": sum(r["n_repeated"] for r in grp),
            "families_total": sum(r["n_families"] for r in grp)}
    return out


def f6():
    p = ART / "fam_f6_labels.json"
    if not p.exists():
        return None
    d = json.load(open(p, encoding="utf-8"))
    cells = d["cells"]
    c = collections.Counter(x["eye"] for x in cells)
    out = {"n_cells": len(cells), "n_blocks": len({x["block_id"] for x in cells}),
           "labels": dict(c),
           "share_real_elements": round(c.get("symbol", 0) / max(len(cells), 1), 4),
           "rules": {}}
    def ev(name, fn, universe=None):
        u = [x for x in cells if universe is None or x["eye"] in universe]
        tp = sum(1 for x in u if fn(x["feats"]) and x["eye"] == "symbol")
        fp = sum(1 for x in u if fn(x["feats"]) and x["eye"] != "symbol")
        fnn = sum(1 for x in u if not fn(x["feats"]) and x["eye"] == "symbol")
        out["rules"][name] = {"universe": "all" if universe is None else "/".join(universe),
                              "TP": tp, "FP": fp, "FN": fnn,
                              "precision": round(tp / max(tp + fp, 1), 4),
                              "recall": round(tp / max(tp + fnn, 1), 4)}
    ev("cls==symbol", lambda f: f["cls"] == "symbol")
    ev("occupied>=6", lambda f: f["occupied_cells"] >= 6)
    ev("occupied>=8", lambda f: f["occupied_cells"] >= 8)
    ev("cls==symbol&n_seg>=6", lambda f: f["cls"] == "symbol" and f["n_seg_med"] >= 6)
    ev("cycle>=0.9&dirconc<0.9 (v0.2-like)", lambda f: f["cycle_share"] >= 0.9 and f["dir_concentration"] < 0.9)
    ev("dir_conc<0.7 | symbol-vs-ruling", lambda f: f["dir_concentration"] < 0.7, ("symbol", "ruling"))
    ev("cls==symbol | symbol-vs-ruling", lambda f: f["cls"] == "symbol", ("symbol", "ruling"))
    return out


def f8():
    import glob
    out = {}
    pf = sorted(glob.glob(str(ART / "fam_f8_pairs_s*.jsonl")))
    cf = sorted(glob.glob(str(ART / "fam_f8_cf_s*.jsonl")))
    if not pf and not cf:
        return None
    P, C = [], []
    for fn in pf:
        for l in open(fn, encoding="utf-8"):
            if l.strip():
                P.append(json.loads(l))
    for fn in cf:
        for l in open(fn, encoding="utf-8"):
            if l.strip():
                C.append(json.loads(l))
    false_rows = [r for p in P for r in (p.get("rows") or [])]
    cf_rows = [r for c in C for r in (c.get("rows") or [])]
    true_rows = [r for r in cf_rows if r["label"] == "TRUE" and r.get("correct")]
    cf_false = [r for r in cf_rows if r["label"] == "FALSE"]
    out["n_pairs"] = len(P)
    out["n_pairs_err"] = sum(1 for p in P if p.get("error"))
    out["n_false_rows_real"] = len(false_rows)
    out["n_true_rows_cf"] = len(true_rows)
    out["n_false_rows_cf"] = len(cf_false)
    gates = {
        "none": lambda f: True,
        "cls==symbol": lambda f: f["cls"] == "symbol",
        "occupied>=6": lambda f: f["occupied_cells"] >= 6,
        "occupied>=8": lambda f: f["occupied_cells"] >= 8,
        "cls==symbol&occ>=6": lambda f: f["cls"] == "symbol" and f["occupied_cells"] >= 6,
        "cls==symbol&n_seg>=6": lambda f: f["cls"] == "symbol" and f["n_seg_med"] >= 6,
        "cycle>=0.9&dirconc<0.9": lambda f: f["cycle_share"] >= 0.9 and f["dir_concentration"] < 0.9,
        "n_seg>=4&occ>=6": lambda f: f["n_seg_med"] >= 4 and f["occupied_cells"] >= 6,
    }
    out["gates"] = {}
    for name, fn in gates.items():
        kept_false_real = sum(1 for r in false_rows if fn(r))
        kept_true = sum(1 for r in true_rows if fn(r))
        kept_false_cf = sum(1 for r in cf_false if fn(r))
        out["gates"][name] = {
            "false_rows_real_kept": kept_false_real,
            "false_rows_real_kept_share": round(kept_false_real / max(len(false_rows), 1), 4),
            "true_rows_kept": kept_true,
            "recall": round(kept_true / max(len(true_rows), 1), 4),
            "false_rows_cf_kept": kept_false_cf,
            "pairs_clean": sum(1 for p in P if not p.get("error") and
                               sum(1 for r in (p.get("rows") or []) if fn(r)) == 0),
        }
    out["n_pairs_ok"] = sum(1 for p in P if not p.get("error"))
    return out


def main():
    out = {"f0_contamination": (json.load(open(ART / "fam_f0_contam.json", encoding="utf-8"))["summary"]
                               if (ART / "fam_f0_contam.json").exists() else None),
           "f2_rewrite": f2(), "f3_false": f3(), "f4_cf": f4(), "f4_real": f4b(),
           "f5_scope": f5(), "f6_eye": f6(), "f7_gate": f7(), "f8_element_gate": f8()}
    json.dump(out, open(ART / "fam_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps({k: (v if k != "f4_real" else {"n": (v or {}).get("n_pairs")})
                      for k, v in out.items()}, ensure_ascii=False, indent=1)[:12000])


if __name__ == "__main__":
    main()
