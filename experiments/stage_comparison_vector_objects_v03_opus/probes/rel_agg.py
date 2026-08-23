# -*- coding: utf-8 -*-
"""Aggregates every rel measurement into artifacts/rel_type_scorecard.json + rel_scorecard.md.

Four measures per type, exactly as BRIEF §8 asks, each with its source marked [CF]/[REAL]:
  1 stability   — share of relations that survive on the other side (raw and, because the
                  gate-fix lesson forbids a one-sided metric, conditional on both endpoints
                  having found a partner);
  2 matching    — top-1 object matching accuracy, descriptor alone vs descriptor + this type;
  3 addressing  — share of known changes for which the type yields a usable address;
  4 false rate  — graded by eye on the rendered relations.
"""
from __future__ import annotations
import json, glob, statistics as st
from pathlib import Path

ART = Path(__file__).resolve().parent.parent / "artifacts"
TY = ["CONNECTED_TO", "PART_OF", "INSIDE", "CONTAINS", "ADJACENT",
      "ALIGNED", "LEADER_TO", "LABEL_ANCHOR", "REPEATED_WITH"]
HARD = ["A3_curve_resample_down", "A3_curve_resample_up", "A6_round_0.1",
        "A6_round_0.25", "A4_circle_to_bezier", "A4b_circle_to_chords5"]


def load(pat):
    out = []
    for f in sorted(glob.glob(str(ART / pat))):
        out += json.load(open(f, encoding="utf-8"))
    return out


def med(v):
    return round(st.median(v), 4) if v else None


def p90(v):
    v = sorted(v)
    return round(v[int(0.9 * (len(v) - 1))], 4) if v else None


def main():
    sc = {t: {} for t in TY}

    # ---- 1 stability [CF] ------------------------------------------------------
    cf = load("rel_r1_cf_*.json")
    per = {t: {"raw": [], "cond": [], "n": 0} for t in TY}
    for r in cf:
        for run in r["runs"]:
            if run.get("cf") not in HARD or "surv" not in run:
                continue
            for t, d in run["surv"].items():
                if d["n"] == 0:
                    continue
                per[t]["raw"].append(d["survived"] / d["n"])
                per[t]["n"] += d["n"]
                if d["endpoints_ok"]:
                    per[t]["cond"].append(d["survived_of_endpoints_ok"] / d["endpoints_ok"])
    for t in TY:
        sc[t]["stability_cf"] = {
            "raw_median": med(per[t]["raw"]), "cond_median": med(per[t]["cond"]),
            "raw_p10": round(sorted(per[t]["raw"])[int(0.1 * (len(per[t]["raw"]) - 1))], 4)
            if per[t]["raw"] else None,
            "edges": per[t]["n"], "observations": len(per[t]["raw"]),
            "source": "[CF] 59 real blocks x 6 adversarial rewrites (A3/A4/A6)"}

    # ---- 1 stability [REAL] ----------------------------------------------------
    real = json.load(open(ART / "rel_r1_real.json", encoding="utf-8"))
    for t in TY:
        for tag, want in (("real_unchanged", "NO_GRAPHIC_CHANGE"),
                          ("real_changed", "GRAPHIC_CHANGE")):
            raw, cond, n = [], [], 0
            for r in real:
                if r.get("error") or r["expected"] != want:
                    continue
                d = r["surv"].get(t)
                if not d or d["n"] == 0:
                    continue
                raw.append(d["survived"] / d["n"]); n += d["n"]
                if d["endpoints_ok"]:
                    cond.append(d["survived_of_endpoints_ok"] / d["endpoints_ok"])
            sc[t]["stability_" + tag] = {"raw_median": med(raw), "cond_median": med(cond),
                                         "edges": n, "pairs": len(raw),
                                         "source": "[REAL] mine benchmark pairs"}

    # ---- 1b stability under a 10 % crop, points vs fraction [REAL geometry] -----
    r5 = load("rel_r5_*.json")
    for t in TY:
        vals = {}
        for reg in ("points", "fraction"):
            raw, cond = [], []
            for r in r5:
                d = (r.get(f"crop_{reg}") or {}).get("surv", {}).get(t)
                if not d or d["n"] == 0:
                    continue
                raw.append(d["survived"] / d["n"])
                if d["endpoints_ok"]:
                    cond.append(d["survived_of_endpoints_ok"] / d["endpoints_ok"])
            vals[reg] = {"raw_median": med(raw), "cond_median": med(cond), "blocks": len(raw)}
        sc[t]["crop10_tolerance_regime"] = vals

    # ---- 2 usefulness for matching [CF] ----------------------------------------
    r2 = [r for r in load("rel_r2_*.json") if not r.get("error")]
    for t in TY:
        out = {}
        for regime in ("noposition", "position"):
            base, with_t, allt = [], [], []
            for r in r2:
                for run in r["runs"]:
                    a = run.get("abl", {}).get(regime)
                    if not a or not a["n"]:
                        continue
                    base.append(a["top1"]["desc_only"])
                    with_t.append(a["top1"][t])
                    allt.append(a["top1"]["ALL"])
            out[regime] = {"desc_only": round(st.mean(base), 4) if base else None,
                           "desc_plus_type": round(st.mean(with_t), 4) if with_t else None,
                           "lift": round(st.mean(with_t) - st.mean(base), 4) if base else None,
                           "all_types": round(st.mean(allt), 4) if allt else None,
                           "observations": len(base)}
        sc[t]["matching_cf"] = out

    # ---- 3 usefulness for explaining change [CF] -------------------------------
    r3 = [r for r in load("rel_r3_*.json") if not r.get("error")]
    tot = 0
    hits = {t: {"any": 0, "stable": 0, "unique": 0, "usable": 0} for t in TY}
    byfam = {}
    for r in r3:
        for run in r["runs"]:
            if "addr" not in run:
                continue
            tot += 1
            fam = run["cf"]
            byfam.setdefault(fam, {"n": 0, **{t: 0 for t in TY}})
            byfam[fam]["n"] += 1
            for t in TY:
                d = run["addr"][t]
                for k in ("any", "stable", "unique", "usable"):
                    if d[k]:
                        hits[t][k] += 1
                if d["usable"]:
                    byfam[fam][t] += 1
    for t in TY:
        sc[t]["addressing_cf"] = {k: round(v / tot, 4) for k, v in hits[t].items()}
        sc[t]["addressing_cf"]["events"] = tot
        sc[t]["addressing_by_family"] = {f: round(byfam[f][t] / byfam[f]["n"], 4) for f in byfam}

    # ---- 4 false relation rate [eye] -------------------------------------------
    g = json.load(open(ART / "rel_r4_grades.json", encoding="utf-8"))
    for t in TY:
        gr = g["grades"].get(t, {})
        T = sum(1 for v in gr.values() if v == "T")
        F = sum(1 for v in gr.values() if v == "F")
        U = sum(1 for v in gr.values() if v == "U")
        sc[t]["false_rate_eye"] = {
            "graded": len(gr), "true": T, "false": F, "unclear": U,
            "false_rate_resolvable": round(F / (T + F), 4) if T + F else None,
            "false_rate_worst_case": round((F + U) / len(gr), 4) if gr else None}
        gg = g["guarded_grades"].get(t)
        if isinstance(gg, dict):
            T2 = sum(1 for v in gg.values() if v == "T")
            F2 = sum(1 for v in gg.values() if v == "F")
            sc[t]["false_rate_eye_guarded"] = {
                "graded": len(gg), "true": T2, "false": F2,
                "false_rate_resolvable": round(F2 / (T2 + F2), 4) if T2 + F2 else None}

    # ---- cost + locality --------------------------------------------------------
    for t in TY:
        v = sorted(r["counts"][t] for r in r5 if not r.get("error"))
        sc[t]["edges_per_block"] = {"median": v[len(v) // 2], "p90": v[int(0.9 * (len(v) - 1))],
                                    "max": v[-1], "blocks": len(v)}
    r9 = [r for r in load("rel_r9_*.json") if not r.get("error")]
    for t in TY:
        m, ms, g10, gd = [], [], [], []
        for r in r9:
            d = r["gaps"].get(t)
            if not d:
                continue
            m.append(d["med"]); ms.append(d["med"] / max(r["S"], 1e-6))
            g10.append(d["share_gt_10S"]); gd.append(d["share_gt_0.1diag"])
        sc[t]["locality"] = {"gap_median_pt": med(m), "gap_median_in_S": med(ms),
                             "share_edges_over_10S": med(g10),
                             "share_edges_over_0.1_block_diag": med(gd), "blocks": len(m)}

    # ---- the rectangle->circle defect ------------------------------------------
    r6 = [r for r in load("rel_r6_*.json") if not r.get("error")]
    defect = {}
    for t in ("INSIDE", "CONTAINS", "PART_OF", "ADJACENT"):
        totn = sum(r["bbox_rel_total"][t] for r in r6)
        lost = sum(r["bbox_rel_only_defective"][t] for r in r6)
        vals = [r["bbox_rel_share_defective"][t] for r in r6
                if r["bbox_rel_share_defective"].get(t) is not None]
        defect[t] = {"micro_share_only_defective": round(lost / totn, 4) if totn else None,
                     "macro_median": med(vals), "edges": totn, "blocks": len(vals)}
        sc[t]["arc_defect_share"] = defect[t]
    summary = {
        "blocks": len(r6),
        "share_objects_with_inflated_bbox_median": med([r["share_obj_inflated"] for r in r6]),
        "share_objects_with_inflated_bbox_p90": p90([r["share_obj_inflated"] for r in r6]),
        "blocks_affected": sum(1 for r in r6 if r["n_obj_inflated"] > 0),
        "bbox_area_inflation_median": med([r["inflation_med"] for r in r6 if r["inflation_med"]]),
        "closed_arc_prims_from_<=4_segments": sum(r["closed_arc_le4seg"] for r in r6),
        "arc_prims_total": sum(r["arc_prims"] for r in r6),
        "arc_prims_with_guard": sum(r["arc_prims_guard"] for r in r6),
        "relations_defective_vs_guarded": defect}

    # ---- guard effect on the verdicts (R8) --------------------------------------
    r8 = load("rel_r8_*.json")
    guard = {}
    for side in ("off", "on"):
        stab = {t: [] for t in TY}
        addr = {t: 0 for t in TY}
        n_addr = 0
        for pair in r8:
            row = pair[side]
            if row.get("error"):
                continue
            for s in row["stab"]:
                for t, d in (s.get("surv") or {}).items():
                    if d["n"]:
                        stab[t].append(d["survived"] / d["n"])
            for a in row["addr"]:
                if "addr" not in a:
                    continue
                for t in TY:
                    if a["addr"][t]["usable"]:
                        addr[t] += 1
                n_addr += 1
        guard[side] = {"stability_median": {t: med(stab[t]) for t in TY},
                       "usable_address": {t: (round(addr[t] / n_addr, 4) if n_addr else None)
                                          for t in TY},
                       "address_events": n_addr,
                       "carriers": sum(1 for p in r8 if not p[side].get("error"))}
    json.dump({"types": sc, "arc_defect": summary, "guard_effect": guard},
              open(ART / "rel_type_scorecard.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("wrote rel_type_scorecard.json; addressing events", tot,
          "; r6 blocks", len(r6), "; r8 carriers", guard["off"]["carriers"])


if __name__ == "__main__":
    main()
