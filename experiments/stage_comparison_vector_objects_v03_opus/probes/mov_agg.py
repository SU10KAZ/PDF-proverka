# -*- coding: utf-8 -*-
"""Aggregation for the `mov` probe: M2/M3 [CF] from mov_runs/cf_*.jsonl,
M4/M4b/M5 [REAL] from mov_runs/bench_*.jsonl, fallback_*.jsonl, border_*.jsonl.

    python probes/mov_agg.py           # writes artifacts/mov_*.json
"""
from __future__ import annotations
import glob, json, math, statistics as st
from collections import Counter, defaultdict
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"


def load(pat):
    out = []
    for f in sorted(glob.glob(str(ART / "mov_runs" / pat))):
        for l in open(f, encoding="utf-8"):
            if l.strip():
                out.append(json.loads(l))
    return out


def q(v, p):
    v = sorted(x for x in v if x is not None)
    if not v:
        return None
    k = (len(v) - 1) * p
    lo, hi = int(math.floor(k)), int(math.ceil(k))
    return round(v[lo] + (v[hi] - v[lo]) * (k - lo), 5)


def stats(v, name=""):
    v = [x for x in v if x is not None]
    if not v:
        return {"n": 0}
    return {"n": len(v), "median": q(v, .5), "p90": q(v, .9), "max": round(max(v), 5),
            "mean": round(sum(v) / len(v), 5)}


DENSITY = [(0, 500, "<500"), (500, 5000, "500-5k"), (5000, 20000, "5k-20k"),
           (20000, 10 ** 9, ">20k")]


def band(n):
    for lo, hi, name in DENSITY:
        if lo <= n < hi:
            return name
    return "?"


# ------------------------------------------------------------------ CF (M2 / M3)

def cf_report():
    rows = load("cf_*.jsonl")
    rep = {"n_rows": len(rows), "n_carriers": len({r["block_id"] for r in rows}),
           "n_disciplines": len({r["discipline"] for r in rows}),
           "n_ok": sum(1 for r in rows if "score" in r),
           "n_skipped": sum(1 for r in rows if "skipped" in r),
           "n_error": sum(1 for r in rows if "error" in r),
           "skip_reasons": Counter(r["skipped"] for r in rows if "skipped" in r).most_common(),
           "error_status": Counter(f"{r['tag']}|{r.get('status')}"
                                   for r in rows if "error" in r).most_common()}

    # ---- A: separation of "block transformed" from "object changed"
    fam = defaultdict(list)
    for r in rows:
        if "score" not in r:
            if "error" in r or "status" in r:
                fam[r["tag"]].append(r)
            continue
        fam[r["tag"]].append(r)
    tab = {}
    for tag, rs in sorted(fam.items()):
        ok = [r for r in rs if "score" in r]
        unavail = [r for r in rs if r.get("status") == "ALIGNMENT_UNAVAILABLE"]
        skipped = [r for r in rs if "skipped" in r]
        nf = [r["score"]["n_findings"] for r in ok]
        row = {"n": len(rs), "n_scored": len(ok), "n_unavailable": len(unavail),
               "n_skipped": len(skipped),
               "verdicts": Counter(r.get("verdict") for r in rs).most_common(),
               "n_findings": stats(nf),
               "clean": sum(1 for x in nf if x == 0),
               "clean_share": round(sum(1 for x in nf if x == 0) / len(nf), 4) if nf else None,
               "t_err_pt": stats([r.get("transform", {}).get("t_err_pt") for r in ok]),
               "s_err": stats([r.get("transform", {}).get("s_err") for r in ok]),
               "theta_ok": sum(1 for r in ok if r.get("transform", {}).get("theta_ok")),
               "sigma_pt": stats([(r.get("residual") or {}).get("sigma_pt") for r in ok]),
               "residual_median_pt": stats([(r.get("residual") or {}).get("median_pt") for r in ok]),
               "obj_moved": stats([(r.get("obj_counts") or {}).get("moved") for r in ok]),
               "obj_removed": stats([(r.get("obj_counts") or {}).get("removed") for r in ok]),
               "obj_added": stats([(r.get("obj_counts") or {}).get("added") for r in ok]),
               }
        if tag.startswith("C3") or tag.startswith("C3+"):
            row["detected"] = sum(1 for r in ok if r["score"].get("move_detected"))
            row["localised"] = sum(1 for r in ok if r["score"].get("localised"))
            row["truth_d_pt"] = stats([r["score"].get("truth_d_pt") for r in ok])
            row["vec_err_pt"] = stats([r["score"].get("move_vector_err_pt") for r in ok])
            row["fp_elsewhere"] = stats([r["score"].get("n_findings_elsewhere") for r in ok])
        if tag.startswith("C1"):
            row["localised"] = sum(1 for r in ok if r["score"].get("localised"))
            row["fp_elsewhere"] = stats([r["score"].get("n_findings_elsewhere") for r in ok])
        tab[tag] = row
    rep["by_tag"] = tab

    # ---- the sensitivity curve: detection vs delta (relative and absolute)
    c3 = [r for r in rows if r.get("cf_id") == "C3_move_object" and "score" in r
          and not r.get("chained")]
    curve = defaultdict(lambda: {"n": 0, "det": 0, "loc": 0, "d_pt": []})
    for r in c3:
        f = r["params"]["frac"]; b = r["params"]["bucket"]
        for key in ((f, b), (f, "all")):
            c = curve[key]
            c["n"] += 1
            c["det"] += bool(r["score"].get("move_detected"))
            c["loc"] += bool(r["score"].get("localised"))
            c["d_pt"].append(r["score"].get("truth_d_pt"))
    rep["curve_by_frac"] = [
        {"frac": k[0], "bucket": k[1], "n": v["n"],
         "detected": v["det"], "recall": round(v["det"] / v["n"], 4),
         "localised": v["loc"], "localised_share": round(v["loc"] / v["n"], 4),
         "delta_pt": stats(v["d_pt"])}
        for k, v in sorted(curve.items(), key=lambda kv: (kv[0][1], kv[0][0]))]

    # absolute-delta curve, the honest axis (tolerances live in PDF points)
    BINS = [(0, .25), (.25, .5), (.5, 1), (1, 2), (2, 4), (4, 8), (8, 16), (16, 32),
            (32, 64), (64, 1e9)]
    ab = defaultdict(lambda: {"n": 0, "det": 0, "loc": 0})
    for r in c3:
        d = r["score"].get("truth_d_pt")
        if d is None:
            continue
        for lo, hi in BINS:
            if lo <= d < hi:
                for key in ((lo, hi, "all"), (lo, hi, band(r["n_seg"]))):
                    ab[key]["n"] += 1
                    ab[key]["det"] += bool(r["score"].get("move_detected"))
                    ab[key]["loc"] += bool(r["score"].get("localised"))
                break
    rep["curve_by_pt"] = [
        {"lo_pt": k[0], "hi_pt": k[1], "band": k[2], "n": v["n"], "detected": v["det"],
         "recall": round(v["det"] / v["n"], 4), "localised_share": round(v["loc"] / v["n"], 4)}
        for k, v in sorted(ab.items(), key=lambda kv: (kv[0][2], kv[0][0]))]

    # ---- self-check of the explanation: is the floor really the matching tolerance?
    # If it is, recall must cross 50 % at delta / tol = 1 whatever tol happens to be.
    TB = [(0, .5), (.5, .9), (.9, 1.1), (1.1, 2), (2, 5), (5, 1e9)]
    tt = defaultdict(lambda: {"n": 0, "det": 0})
    for r in c3:
        d = r["score"].get("truth_d_pt")
        if d is None:
            continue
        tol = max(0.5, 0.05 * r["S"])
        x = d / tol
        for lo, hi in TB:
            if lo <= x < hi:
                tt[(lo, hi)]["n"] += 1
                tt[(lo, hi)]["det"] += bool(r["score"].get("move_detected"))
                break
    rep["curve_by_tol"] = [{"lo": k[0], "hi": k[1], "n": v["n"], "detected": v["det"],
                            "recall": round(v["det"] / max(1, v["n"]), 4)}
                           for k, v in sorted(tt.items())]
    tols = sorted(max(0.5, 0.05 * r["S"]) for r in c3)
    rep["tol_range"] = {"n": len(tols), "min": round(tols[0], 3), "max": round(tols[-1], 3),
                        "median": round(tols[len(tols) // 2], 3),
                        "share_above_0.5": round(sum(1 for t in tols if t > 0.5) / max(1, len(tols)), 4)}

    # ---- M3: block transform AND object move at once
    comb = [r for r in rows if r.get("chained") and "score" in r]
    cs = defaultdict(lambda: {"n": 0, "det": 0, "loc": 0, "terr": [], "serr": [], "fp": []})
    for r in comb:
        k = r["tag"].split("@", 1)[0] + "|" + r["tag"].split("@")[-1]
        c = cs[k]
        c["n"] += 1
        c["det"] += bool(r["score"].get("move_detected"))
        c["loc"] += bool(r["score"].get("localised"))
        c["terr"].append(r.get("transform", {}).get("t_err_pt"))
        c["serr"].append(r.get("transform", {}).get("s_err"))
        c["fp"].append(r["score"].get("n_findings_elsewhere"))
    rep["combo"] = {k: {"n": v["n"], "detected": v["det"],
                        "recall": round(v["det"] / v["n"], 4),
                        "localised_share": round(v["loc"] / v["n"], 4),
                        "fp_elsewhere": stats(v["fp"])} for k, v in sorted(cs.items())}
    # matched single-CF baseline for the same carriers/buckets
    rep["combo_rows"] = [{"tag": r["tag"], "block_id": r["block_id"], "disc": r["discipline"],
                          "n_seg": r["n_seg"], "status": r.get("status"),
                          "verdict": r.get("verdict"),
                          "truth_d_pt": r["score"].get("truth_d_pt"),
                          "detected": r["score"].get("move_detected"),
                          "localised": r["score"].get("localised"),
                          "fp": r["score"].get("n_findings_elsewhere"),
                          "t_err_pt": r.get("transform", {}).get("t_err_pt"),
                          "s_err": r.get("transform", {}).get("s_err")} for r in comb]

    # ---- false positives on the classes that must stay silent
    silent = [r for r in rows if "score" in r and r["expected"] == "NO_GRAPHIC_CHANGE"]
    rep["silent_classes"] = {
        "n": len(silent),
        "n_with_findings": sum(1 for r in silent if r["score"]["n_findings"] > 0),
        "fp_rate": round(sum(1 for r in silent if r["score"]["n_findings"] > 0) / max(1, len(silent)), 4),
        "by_class": {c: {"n": sum(1 for r in silent if r["tag"].startswith(c)),
                         "fp": sum(1 for r in silent if r["tag"].startswith(c)
                                   and r["score"]["n_findings"] > 0)}
                     for c in ("A1", "A5", "A6", "B1", "B2", "B3", "B4", "B5", "D1", "D3")},
        "worst": sorted([{"tag": r["tag"], "block": r["block_id"], "disc": r["discipline"],
                          "n_seg": r["n_seg"], "nf": r["score"]["n_findings"],
                          "lost": r["score"]["lost_ink_share"],
                          "new": r["score"]["new_ink_share"],
                          "moved": r["score"]["moved_ink_share"]}
                         for r in silent if r["score"]["n_findings"] > 0],
                        key=lambda x: -(x["lost"] or 0) - (x["new"] or 0))[:25]}

    # ---- M2 confusion matrix: truth class -> what the system said
    FLOOR = 0.5      # the strict ink tolerance, in PDF points

    def sys_class(r):
        if r.get("status") == "ALIGNMENT_UNAVAILABLE" or "score" not in r:
            return "UNKNOWN"
        sc = r["score"]
        if sc["n_findings"] == 0:
            return "SILENT(block transform / no change)"
        if sc.get("move_detected"):
            return "MOVED_OBJECT at the right place"
        if sc.get("localised"):
            return "change at the right place, not named a move"
        if sc.get("localised") is None:
            return "findings (no truth bbox)"
        return "findings ONLY elsewhere"

    def truth_class(r):
        t = r["tag"]
        if t.startswith(("B1", "B2", "B3", "B4", "B5")):
            return "B: whole block transformed"
        if t.startswith(("A1", "A5", "A6")):
            return "A: representation rewritten"
        if t.startswith(("D1", "D3")):
            return "D: text only"
        if t.startswith("C3+"):
            d = (r.get("score") or {}).get("truth_d_pt")
            return "C3+B: block AND object moved" if (d or 0) >= FLOOR else "C3+B: below tolerance"
        if t.startswith("C3"):
            d = (r.get("score") or {}).get("truth_d_pt")
            if d is None:
                return "C3: unknown delta"
            return "C3: one object moved >= 0.5 pt" if d >= FLOOR else "C3: one object moved < 0.5 pt"
        if t.startswith("C1"):
            return "C1: one object removed"
        return "other"

    cm2 = defaultdict(Counter)
    for r in rows:
        if "skipped" in r:
            continue
        cm2[truth_class(r)][sys_class(r)] += 1
    rep["confusion_M2"] = {k: dict(v) for k, v in sorted(cm2.items())}
    rep["block_transformed_flag"] = {
        t: {"n": sum(1 for r in rows if r["tag"].startswith(t) and "score" in r),
            "flagged": sum(1 for r in rows if r["tag"].startswith(t) and "score" in r
                           and r["score"].get("block_transformed"))}
        for t in ("A1", "A5", "A6", "B1", "B2", "B3", "B4", "B5", "C1", "C3_", "D1", "D3")}

    # ---- M5: when alignment is impossible
    un = [r for r in rows if r.get("status") == "ALIGNMENT_UNAVAILABLE"]
    rep["unavailable"] = {
        "n": len(un), "share": round(len(un) / max(1, len(rows)), 4),
        "by_tag": Counter(r["tag"] for r in un).most_common(),
        "by_band": Counter(band(r["n_seg"]) for r in un).most_common(),
        "rows": [{"tag": r["tag"], "block": r["block_id"], "disc": r["discipline"],
                  "n_seg": r["n_seg"], "n_obj": r.get("n_obj"),
                  "reason": (r.get("estimate") or {}).get("reason")} for r in un][:60]}
    amb = [r for r in rows if r.get("status") == "ALIGNMENT_AMBIGUOUS"]
    rep["ambiguous"] = {"n": len(amb), "by_tag": Counter(r["tag"] for r in amb).most_common(12)}
    return rep


# ------------------------------------------------------------------ REAL (M4 / M4b / M5)

def real_report():
    bench = load("bench_*.jsonl")
    fb = load("fallback_*.jsonl")
    bd = load("border_*.jsonl")
    rep = {"n_bench": len(bench), "n_fallback": len(fb), "n_border": len(bd)}

    def transform_stats(rows, label):
        ok = [r for r in rows if r.get("status") in ("ALIGNED", "ALIGNMENT_AMBIGUOUS")]
        t = [r["t_norm_pt"] for r in ok]
        s = [r["s_dev"] for r in ok]
        return {"label": label, "n": len(rows), "n_aligned": len(ok),
                "t_norm_pt": stats(t), "s_dev": stats(s),
                "n_theta_nonzero": sum(1 for r in ok if r.get("theta")),
                "share_t_gt_0.3pt": round(sum(1 for x in t if x > 0.3) / len(t), 4) if t else None,
                "share_t_gt_1pt": round(sum(1 for x in t if x > 1.0) / len(t), 4) if t else None,
                "share_t_gt_3pt": round(sum(1 for x in t if x > 3.0) / len(t), 4) if t else None,
                "share_s_gt_0.002": round(sum(1 for x in s if x > 0.002) / len(s), 4) if s else None,
                "share_s_gt_0.01": round(sum(1 for x in s if x > 0.01) / len(s), 4) if s else None,
                }
    rep["transform_bench"] = transform_stats(bench, "benchmark 33 [REAL cross-revision]")
    rep["transform_fallback"] = transform_stats(fb, "fallback R<->R (pd) [REAL]")
    rep["transform_border_sample"] = transform_stats(bd, "random cross-revision sample [REAL]")

    # verdict vs the human label
    vt = []
    for r in bench:
        vt.append({"pair_id": r["pair_id"], "classes": r.get("classes"),
                   "expected": r.get("expected"), "status": r.get("status"),
                   "verdict": r.get("verdict"), "n_findings": r.get("n_findings"),
                   "n_border_entries": r.get("n_border_entries"),
                   "t_norm_pt": r.get("t_norm_pt"), "s_dev": r.get("s_dev"),
                   "theta": r.get("theta"), "n_seg": [r.get("n_seg_a"), r.get("n_seg_b")],
                   "obj_counts": r.get("obj_counts"),
                   "ink": ((r.get("ink") or {}).get("strict") or {}),
                   "error": r.get("error")})
    rep["bench_rows"] = vt
    lab = [r for r in bench if r.get("expected") in ("GRAPHIC_CHANGE", "NO_GRAPHIC_CHANGE")
           and r.get("status") in ("ALIGNED", "ALIGNMENT_AMBIGUOUS")]
    cm = Counter()
    for r in lab:
        got = "GRAPHIC_CHANGE" if r.get("n_findings", 0) > 0 else "NO_GRAPHIC_CHANGE"
        cm[(r["expected"], got)] += 1
    rep["bench_confusion"] = {f"{k[0]}->{k[1]}": v for k, v in cm.items()}
    rep["bench_unaligned"] = [{"pair_id": r["pair_id"], "status": r.get("status"),
                               "reason": r.get("reason"), "classes": r.get("classes"),
                               "n_seg": [r.get("n_seg_a"), r.get("n_seg_b")],
                               "error": r.get("error")}
                              for r in bench if r.get("status") not in ("ALIGNED", "ALIGNMENT_AMBIGUOUS")]

    # block_moved class: what does the algorithm say
    rep["block_moved"] = [{"pair_id": r["pair_id"], "classes": r.get("classes"),
                           "transform": r.get("transform"), "t_norm_pt": r.get("t_norm_pt"),
                           "s_dev": r.get("s_dev"), "theta": r.get("theta"),
                           "verdict": r.get("verdict"), "n_findings": r.get("n_findings"),
                           "expected": r.get("expected"),
                           "human": r.get("human"),
                           "residual": r.get("residual"),
                           "naive": {k: {kk: vv for kk, vv in v.items()
                                         if kk in ("unmatched_ink_share_a", "dt_from_anchor_pt",
                                                   "transform")}
                                     for k, v in (r.get("naive") or {}).items()},
                           "anchor_unmatched_a": ((r.get("ink") or {}).get("strict") or {}).get("unmatched_ink_share_a")}
                          for r in bench if "block_moved" in (r.get("classes") or [])]

    # anchor vs bbox alignment (M1 evidence)
    comp = []
    for r in bench + fb:
        n = r.get("naive") or {}
        a = ((r.get("ink") or {}).get("strict") or {}).get("unmatched_ink_share_a")
        if a is None:
            continue
        row = {"pair_id": r["pair_id"], "source": r["source"], "anchor": a,
               "t_norm_pt": r.get("t_norm_pt")}
        for k, v in n.items():
            row[k] = v.get("unmatched_ink_share_a")
            row[k + "_dt"] = v.get("dt_from_anchor_pt")
        comp.append(row)
    rep["anchor_vs_bbox"] = {
        "rows": comp,
        "anchor": stats([c["anchor"] for c in comp]),
        "bbox_org": stats([c.get("bbox_org") for c in comp]),
        "bbox_fit": stats([c.get("bbox_fit") for c in comp]),
        "n_anchor_better_than_org": sum(1 for c in comp if c.get("bbox_org") is not None
                                        and c["anchor"] < c["bbox_org"] - 1e-9),
        "n_anchor_worse_than_org": sum(1 for c in comp if c.get("bbox_org") is not None
                                       and c["anchor"] > c["bbox_org"] + 1e-9),
        "n_anchor_better_than_fit": sum(1 for c in comp if c.get("bbox_fit") is not None
                                        and c["anchor"] < c["bbox_fit"] - 1e-9),
        "n_anchor_worse_than_fit": sum(1 for c in comp if c.get("bbox_fit") is not None
                                       and c["anchor"] > c["bbox_fit"] + 1e-9),
        "dt_org_pt": stats([c.get("bbox_org_dt") for c in comp]),
        "dt_fit_pt": stats([c.get("bbox_fit_dt") for c in comp]),
    }

    # ---- M4b: crop-border attribution on the larger sample
    grp = defaultdict(list)
    for r in bd:
        grp[r.get("group", "?")].append(r)
    m4b = {}
    for g, rs in grp.items():
        ok = [r for r in rs if r.get("status") in ("ALIGNED", "ALIGNMENT_AMBIGUOUS")]
        m4b[g] = {
            "n": len(rs), "n_aligned": len(ok),
            "n_unaligned": len(rs) - len(ok),
            "unaligned_status": Counter(r.get("status") for r in rs
                                        if r.get("status") not in ("ALIGNED", "ALIGNMENT_AMBIGUOUS")).most_common(),
            "n_findings": stats([r.get("n_findings") for r in ok]),
            "share_with_findings": round(sum(1 for r in ok if (r.get("n_findings") or 0) > 0)
                                         / max(1, len(ok)), 4),
            "share_with_moved": round(sum(1 for r in ok if "MOVED_INK" in (r.get("finding_types") or []))
                                      / max(1, len(ok)), 4),
            "n_border_entries": stats([r.get("n_border_entries") for r in ok]),
            "border_ink_share_a": stats([((r.get("ink") or {}).get("strict") or {}).get("border_ink_share_a") for r in ok]),
            "lost_ink_share_a": stats([((r.get("ink") or {}).get("strict") or {}).get("lost_ink_share_a") for r in ok]),
            "new_ink_share_b": stats([((r.get("ink") or {}).get("strict") or {}).get("new_ink_share_b") for r in ok]),
            "moved_ink_share_a": stats([((r.get("ink") or {}).get("strict") or {}).get("moved_ink_share_a") for r in ok]),
            "t_norm_pt": stats([r.get("t_norm_pt") for r in ok]),
        }
    rep["m4b_border"] = m4b
    rep["m4b_border_rows"] = [{"pair_id": r["pair_id"], "group": r.get("group"),
                               "disc": r.get("discipline"), "is_stamp": r.get("is_stamp"),
                               "status": r.get("status"), "verdict": r.get("verdict"),
                               "n_findings": r.get("n_findings"),
                               "types": r.get("finding_types"),
                               "n_border_entries": r.get("n_border_entries"),
                               "t_norm_pt": r.get("t_norm_pt"),
                               "n_seg": [r.get("n_seg_a"), r.get("n_seg_b")],
                               "ink": ((r.get("ink") or {}).get("strict") or {}),
                               "error": r.get("error")} for r in bd]

    # ---- M5 on real data
    allr = bench + fb + bd
    rep["m5_real"] = {
        "n": len(allr),
        "by_status": Counter(r.get("status") for r in allr).most_common(),
        "by_source": {src: Counter(r.get("status") or "ERROR" for r in allr
                                   if r.get("source") == src).most_common()
                      for src in ("benchmark", "fallback_RR", "border_sample")},
        "unavailable_reasons": Counter((r.get("estimate") or {}).get("reason")
                                       for r in allr
                                       if r.get("status") == "ALIGNMENT_UNAVAILABLE").most_common(),
        "n_error": sum(1 for r in allr if "error" in r),
        "errors": [{"pair_id": r.get("pair_id"), "err": r.get("error")}
                   for r in allr if "error" in r][:20],
    }
    # free-rotation diagnostic: how far from a multiple of 90 deg reality actually is
    al = [r for r in allr if r.get("status") == "ALIGNED"]
    th = [abs((r.get("estimate") or {}).get("theta_free_deg") or 0.0) for r in al]
    rep["theta_free"] = {"n": len(th), "median": q(th, .5), "p90": q(th, .9),
                         "max": round(max(th), 4) if th else None,
                         "n_nonzero": sum(1 for x in th if x > 0),
                         "n_above_0.5deg": sum(1 for x in th if x > 0.5),
                         "worst": sorted([{"pair_id": r["pair_id"],
                                           "theta_free_deg": (r.get("estimate") or {}).get("theta_free_deg"),
                                           "s_free": (r.get("estimate") or {}).get("s_free")}
                                          for r in al],
                                         key=lambda x: -abs(x["theta_free_deg"] or 0))[:8]}
    rep["fallback_rows"] = [{k: v for k, v in r.items()
                             if k not in ("findings_top", "border_top", "naive", "ink")}
                            for r in fb]
    return rep


# ------------------------------------------------------------------ M4b ablation [CF]

def b4b_report():
    rows = load("b4b_*.jsonl")
    ok = [r for r in rows if "v1" in r]
    rep = {"n_rows": len(rows), "n_scored": len(ok),
           "n_carriers": len({r.get("block_id") for r in rows}),
           "border_seg_share": stats([r.get("border_seg_share") for r in rows]),
           "skipped": Counter(r.get("skipped") for r in rows if "skipped" in r).most_common(),
           "unavailable": sum(1 for r in rows if r.get("status") == "ALIGNMENT_UNAVAILABLE")}
    pads = ["pad0.0", "pad0.5", "pad1.0", "pad2.0", "pad4.0"]
    tab = {}
    for tag in sorted({r["tag"] for r in ok}):
        rs = [r for r in ok if r["tag"] == tag]
        exp = rs[0].get("expected")
        e = {"n": len(rs), "expected": exp,
             "v1_n_findings": stats([r["v1"]["pad0.0"]["n"] for r in rs]),
             "v1_clean": sum(1 for r in rs if r["v1"]["pad0.0"]["n"] == 0)}
        if exp == "GRAPHIC_CHANGE":
            e["v1_on_truth"] = sum(1 for r in rs if r["v1"]["pad0.0"]["n_on_truth"] > 0)
            e["v1_elsewhere"] = stats([r["v1"]["pad0.0"]["n_elsewhere"] for r in rs])
        for pd in pads:
            e[f"v2_{pd}_clean"] = sum(1 for r in rs if r["v2"][pd]["n"] == 0)
            e[f"v2_{pd}_n"] = stats([r["v2"][pd]["n"] for r in rs])
            if exp == "GRAPHIC_CHANGE":
                e[f"v2_{pd}_on_truth"] = sum(1 for r in rs if r["v2"][pd]["n_on_truth"] > 0)
        tab[tag] = e
    rep["by_tag"] = tab
    # headline: false positives on the silent classes vs true positives kept
    sil = [r for r in ok if r["expected"] == "NO_GRAPHIC_CHANGE"]
    tru = [r for r in ok if r["expected"] == "GRAPHIC_CHANGE"]
    head = {"n_silent": len(sil), "n_true": len(tru),
            "v1_fp_rate": round(sum(1 for r in sil if r["v1"]["pad0.0"]["n"] > 0) / max(1, len(sil)), 4),
            "v1_tp_rate": round(sum(1 for r in tru if r["v1"]["pad0.0"]["n_on_truth"] > 0) / max(1, len(tru)), 4)}
    for pd in pads:
        head[f"v2_{pd}_fp_rate"] = round(sum(1 for r in sil if r["v2"][pd]["n"] > 0) / max(1, len(sil)), 4)
        head[f"v2_{pd}_tp_rate"] = round(sum(1 for r in tru if r["v2"][pd]["n_on_truth"] > 0) / max(1, len(tru)), 4)
    rep["headline"] = head
    # how many FALSE "an object moved" the crop frame produces, and how many survive v2
    rep["false_moved"] = {}
    for cls in ("B1", "B2", "B3", "B4", "B5"):
        rs = [r for r in ok if r["tag"].startswith(cls)]
        if not rs:
            continue
        rep["false_moved"][cls] = {
            "n": len(rs),
            "v1_instances_with_MOVED": sum(1 for r in rs if "MOVED_INK" in r["v1"]["pad0.0"]["types"]),
            "v2_instances_with_MOVED": sum(1 for r in rs if "MOVED_INK" in r["v2"]["pad0.0"]["types"]),
            "v1_findings_total": sum(r["v1"]["pad0.0"]["n"] for r in rs),
            "v2_findings_total": sum(r["v2"]["pad0.0"]["n"] for r in rs),
        }
    rep["by_class"] = {}
    for cls in ("B1", "B3", "B4", "C1", "C2", "C3"):
        rs = [r for r in ok if r["tag"].startswith(cls)]
        if not rs:
            continue
        exp = rs[0]["expected"]
        d = {"n": len(rs), "expected": exp,
             "v1": round(sum(1 for r in rs if r["v1"]["pad0.0"]["n"] > 0) / len(rs), 4)}
        for pd in pads:
            d[f"v2_{pd}"] = round(sum(1 for r in rs if r["v2"][pd]["n"] > 0) / len(rs), 4)
            if exp == "GRAPHIC_CHANGE":
                d[f"v2_{pd}_on_truth"] = round(sum(1 for r in rs if r["v2"][pd]["n_on_truth"] > 0) / len(rs), 4)
        if exp == "GRAPHIC_CHANGE":
            d["v1_on_truth"] = round(sum(1 for r in rs if r["v1"]["pad0.0"]["n_on_truth"] > 0) / len(rs), 4)
        rep["by_class"][cls] = d
    return rep


def region_report():
    rows = load("region_*.jsonl")
    ok = [r for r in rows if "comparable_share_a" in r]
    out = {"n": len(rows), "n_ok": len(ok),
           "comparable_share_a": stats([r["comparable_share_a"] for r in ok]),
           "comparable_share_b": stats([r["comparable_share_b"] for r in ok]),
           "share_below_0.95": round(sum(1 for r in ok if min(r["comparable_share_a"], r["comparable_share_b"]) < 0.95) / max(1, len(ok)), 4),
           "share_below_0.80": round(sum(1 for r in ok if min(r["comparable_share_a"], r["comparable_share_b"]) < 0.80) / max(1, len(ok)), 4),
           "rows": sorted(ok, key=lambda r: min(r["comparable_share_a"], r["comparable_share_b"]))}
    return out


# ------------------------------------------------------------------ numbers quoted in prose

def prose_numbers():
    import math
    rows = load("cf_*.jsonl")
    ok = [r for r in rows if "score" in r]
    FLOOR = 0.5
    c3 = [r for r in ok if r.get("cf_id") == "C3_move_object" and not r.get("chained")
          and (r["score"].get("truth_d_pt") or 0) >= FLOOR]
    sil = [r for r in c3 if r["score"]["n_findings"] == 0]
    loc = [r for r in c3 if r["score"]["n_findings"] > 0
           and not r["score"].get("move_detected") and r["score"].get("localised")]
    def sig(r):
        sc = r["score"]
        return (sc.get("moved_ink_share") or 0) + (sc.get("lost_ink_share") or 0) + (sc.get("new_ink_share") or 0)
    sg = [ (r.get("residual") or {}).get("sigma_pt") for r in ok
           if r["tag"].startswith(("B1", "B2", "B5", "A1", "A5")) and r.get("residual")]
    sg = [x for x in sg if x is not None]
    tb = defaultdict(list)
    for r in ok:
        tb[band(r["n_seg"])].append(r["t_sec"])
    xs = [(math.log(r["n_seg"]), math.log(r["t_sec"])) for r in ok if r["t_sec"] > 0.05]
    n = len(xs)
    mx = sum(a for a, _ in xs) / n; my = sum(b for _, b in xs) / n
    k = sum((a - mx) * (b - my) for a, b in xs) / sum((a - mx) ** 2 for a, _ in xs)
    return {
        "c3_above_floor": len(c3),
        "n_silent": len(sil),
        "silent_tiny": sum(1 for r in sil if r["params"].get("bucket") == "tiny"),
        "silent_dense": sum(1 for r in sil if r["n_seg"] > 20000),
        "silent_with_signal": sum(1 for r in sil if sig(r) > 0),
        "silent_below_pub_threshold": sum(1 for r in sil if 0 < sig(r) <= 1e-4),
        "n_localised_not_move": len(loc),
        "localised_as_add_remove": sum(1 for r in loc
                                       if set(r["score"]["types"]) == {"ADDED_INK", "REMOVED_INK"}),
        "align_sigma": stats(sg),
        "align_sigma_n": len(sg),
        "time_by_band": {b: {"n": len(v), "median": q(v, .5), "p90": q(v, .9),
                             "max": round(max(v), 1)} for b, v in tb.items()},
        "time_exponent": round(k, 3), "time_fit_n": n,
        **object_vs_ink(),
    }


def object_vs_ink():
    """How often the OBJECT-level bookkeeping claims a change the ink evidence denies."""
    rows = load("bench_*.jsonl") + load("border_*.jsonl") + load("fallback_*.jsonl")
    ok = [r for r in rows if r.get("status") == "ALIGNED" and r.get("n_findings") is not None
          and r.get("obj_counts")]
    quiet = [r for r in ok if r["n_findings"] == 0]
    v = [r["obj_counts"]["removed"] + r["obj_counts"]["added"] + r["obj_counts"]["moved"]
         for r in quiet]
    bad = sum(1 for x in v if x > 0)
    return {"oi_aligned": len(ok), "oi_quiet": len(quiet), "oi_bad": bad,
            "oi_share": round(bad / max(1, len(quiet)), 4),
            "oi_p90": q(v, .9), "oi_max": max(v) if v else 0}


def main():
    cf = cf_report()
    json.dump(cf, open(ART / "mov_cf_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("cf rows", cf["n_rows"], "carriers", cf["n_carriers"])
    rr = real_report()
    json.dump(rr, open(ART / "mov_real_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("real", rr["n_bench"], rr["n_fallback"], rr["n_border"])
    bb = b4b_report()
    json.dump(bb, open(ART / "mov_borderrule.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("b4b", bb["n_scored"], bb.get("headline"))
    pr = prose_numbers()
    json.dump(pr, open(ART / "mov_prose.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("prose", pr["n_silent"], pr["time_exponent"])
    rg = region_report()
    json.dump(rg, open(ART / "mov_region.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("region", rg["n_ok"])


if __name__ == "__main__":
    main()
