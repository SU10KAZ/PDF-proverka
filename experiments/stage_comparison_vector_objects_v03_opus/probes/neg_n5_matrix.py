# -*- coding: utf-8 -*-
"""N5 — the FALSE GRAPHIC CHANGE matrix over every negative control."""
from __future__ import annotations
import json, statistics, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N   # noqa: E402

ART = N.ART
FLAV = ("ink", "count", "object_id", "churn", "label")


def agg(rows, key="shared"):
    """rows: list of dicts with rows[i][key]['ledger'] -> flavour counts."""
    out = {"n": len(rows)}
    for f in FLAV:
        vals = [r[key]["ledger"][f] for r in rows]
        out[f] = {
            "fp_rate": round(sum(1 for v in vals if v > 0) / max(1, len(vals)), 4),
            "mean_per_block": round(statistics.fmean(vals), 3) if vals else None,
            "median": N.med(vals), "p90": N.pct(vals, 0.9), "max": max(vals) if vals else None,
        }
    ib = [r[key]["ledger"]["ink_border"] for r in rows]
    out["ink_border"] = {"fp_rate": round(sum(1 for v in ib if v > 0) / max(1, len(ib)), 4),
                         "mean_per_block": round(statistics.fmean(ib), 3) if ib else None}
    return out


def load(name):
    p = ART / name
    return json.load(open(p, encoding="utf-8")) if p.exists() else None



GUARD_LOST = 0.90   # registration failed: this share of A's ink found no partner at all


def power_rows(matrix):
    """N6: the same comparator, same config, same carriers — but a REAL change.
    Without this the zeros above are not evidence."""
    n6 = load("neg_n6_power.json")
    if not n6:
        return
    groups = {}
    for r in n6["rows"]:
        groups.setdefault(r["variant"], []).append(r)
    for k, v in sorted(groups.items()):
        ink = [r["res"]["ledger"]["ink"] for r in v]
        matrix[f"POWER/{k}"] = {
            "n": len(v), "disciplines": len({r["discipline"] for r in v}),
            "expected": "GRAPHIC_CHANGE",
            "recall": round(sum(1 for x in ink if x > 0) / max(1, len(ink)), 4),
            "records_median": N.med(ink),
            "records_mean": round(statistics.fmean(ink), 2),
            "obj_area_frac_median": N.med([r["obj_area_frac"] for r in v
                                           if r.get("obj_area_frac") is not None]),
        }
    sw = {}
    for k in sorted({k for r in n6["rows"] for k in r["sweep"]}):
        vals = [r["sweep"][k]["n"] for r in n6["rows"]]
        sw[k] = {"recall": round(sum(1 for x in vals if x > 0) / max(1, len(vals)), 4),
                 "mean_records": round(statistics.fmean(vals), 2)}
    matrix["_POWER_SWEEP"] = sw


def dim_runmin(matrix):
    n4b = load("neg_n4b_dimsweep.json")
    if not n4b:
        return
    out = {}
    for rm in n4b["run_min_grid"]:
        k = f"run_min_S={rm}"
        d6 = [r["sweep"][k]["D6_dim_value_only"]["n_retiled"] for r in n4b["rows"]]
        d6a = [r["sweep"][k]["D6_dim_value_only"]["n_retiled"]
               + r["sweep"][k]["D6_dim_value_only"]["n_only_a"]
               + r["sweep"][k]["D6_dim_value_only"]["n_only_b"] for r in n4b["rows"]]
        d7 = [r["sweep"][k]["D7_dim_geometry"]["n_retiled"] for r in n4b["rows"]]
        nb = [r["sweep"][k]["n_chains_base"] for r in n4b["rows"]]
        out[k] = {"n": len(n4b["rows"]),
                  "D6_retiled_fp_rate": round(sum(1 for x in d6 if x > 0) / max(1, len(d6)), 4),
                  "D6_any_record_fp_rate": round(sum(1 for x in d6a if x > 0) / max(1, len(d6a)), 4),
                  "D7_retiled_recall": round(sum(1 for x in d7 if x > 0) / max(1, len(d7)), 4),
                  "chains_per_block_median": N.med(nb)}
    matrix["_DIM_RUNMIN_SWEEP"] = out


def guard_rows(real_matrix, pairs):
    sil = [p for p in pairs if p["expected_verdict"] == "NO_GRAPHIC_CHANGE"]
    chg = [p for p in pairs if p["expected_verdict"] == "GRAPHIC_CHANGE"]
    silc = [p for p in sil if p["res"]["lost_share"] < GUARD_LOST]
    chgc = [p for p in chg if p["res"]["lost_share"] < GUARD_LOST]
    real_matrix["_REGISTRATION_GUARD"] = {
        "rule": f"lost_share >= {GUARD_LOST} -> NOT_COMPARABLE (registration failed)",
        "silent_total": len(sil), "silent_comparable": len(silc),
        "silent_excluded": [p["pair_id"] for p in sil if p["res"]["lost_share"] >= GUARD_LOST],
        "changed_total": len(chg), "changed_comparable": len(chgc),
        "changed_excluded": [p["pair_id"] for p in chg if p["res"]["lost_share"] >= GUARD_LOST],
        "fp_rate_before": round(sum(1 for p in sil if p["n_entries_inner"] > 0) / max(1, len(sil)), 4),
        "fp_rate_after": round(sum(1 for p in silc if p["n_entries_inner"] > 0) / max(1, len(silc)), 4),
        "recall_before": round(sum(1 for p in chg if p["n_entries_inner"] > 0) / max(1, len(chg)), 4),
        "recall_after": round(sum(1 for p in chgc if p["n_entries_inner"] > 0) / max(1, len(chgc)), 4),
    }


def main():
    matrix, notes = {}, []
    n1 = load("neg_n1_text.json")
    if n1:
        groups: dict[str, list] = {}
        for r in n1["rows"]:
            groups.setdefault(r["variant"], []).append(r)
        for k, v in sorted(groups.items()):
            matrix[f"TEXT_ONLY/{k}"] = {"shared_scale": agg(v, "shared"),
                                        "own_scale": agg(v, "own_scale"),
                                        "geometry_identical_all": all(x["geometry_identical"] for x in v),
                                        "disciplines": len({x["discipline"] for x in v})}
    n2 = load("neg_n2_table.json")
    if n2:
        groups = {}
        for r in n2["d45"]:
            groups.setdefault(r["cf_id"], []).append(r)
        for k, v in sorted(groups.items()):
            matrix[f"TABLE_ONLY/{k}"] = {"shared_scale": agg(v, "shared"),
                                         "own_scale": agg(v, "own_scale"),
                                         "geometry_identical_all": all(x["geometry_identical"] for x in v),
                                         "disciplines": len({x["discipline"] for x in v})}
        rowg: dict[str, list] = {}
        for r in n2["row_edits"]:
            rowg.setdefault(r["cf_id"], []).append(r)
        for k, v in sorted(rowg.items()):
            ink = [r["res"]["ledger"]["ink"] for r in v]
            inkb = [r["res"]["ledger"]["ink_border"] for r in v]
            matrix[f"TABLE_ROW/{k}"] = {
                "n": len(v), "disciplines": len({r["discipline"] for r in v}),
                "detected_rate": round(sum(1 for x in ink if x > 0) / max(1, len(ink)), 4),
                "ink_records": {"median": N.med(ink), "mean": round(statistics.fmean(ink), 2),
                                "p90": N.pct(ink, 0.9), "max": max(ink)},
                "ink_border_records": {"median": N.med(inkb), "mean": round(statistics.fmean(inkb), 2)},
                "obj_delta": {"median": N.med([r["res"]["n_obj_b"] - r["n_obj_before"] for r in v])},
                "note": "expected GRAPHIC_CHANGE by construction; the question is at what cost",
            }
    n4 = load("neg_n4_dims.json")
    if n4:
        groups = {}
        for r in n4["rows"]:
            groups.setdefault(r["cf_id"], []).append(r)
        for k, v in sorted(groups.items()):
            ink = [r["res"]["ledger"]["ink"] for r in v]
            ret = [r["dim"]["n_retiled"] for r in v]
            allr = [r["dim"]["n_records"] for r in v]
            matrix[f"DIM/{k}"] = {
                "n": len(v), "disciplines": len({r["discipline"] for r in v}),
                "expected": v[0]["expected"],
                "ink_fire_rate": round(sum(1 for x in ink if x > 0) / max(1, len(ink)), 4),
                "ink_records_mean": round(statistics.fmean(ink), 3),
                "chain_retiled_rate": round(sum(1 for x in ret if x > 0) / max(1, len(ret)), 4),
                "chain_records_mean": round(statistics.fmean(allr), 3),
                "chain_retiled_mean": round(statistics.fmean(ret), 3),
            }
        # sensitivity sweep: D7 recall vs everything else
        sweep_keys = sorted({k for r in n4["rows"] for k in r["sweep"]})
        sw = {}
        for k in sweep_keys:
            d6 = [r["sweep"][k]["n"] for r in n4["rows"] if r["cf_id"] == "D6_dim_value_only"]
            d7 = [r["sweep"][k]["n"] for r in n4["rows"] if r["cf_id"] == "D7_dim_geometry"]
            sw[k] = {"D6_fp_rate": round(sum(1 for x in d6 if x > 0) / max(1, len(d6)), 4),
                     "D7_recall": round(sum(1 for x in d7 if x > 0) / max(1, len(d7)), 4),
                     "n6": len(d6), "n7": len(d7)}
        matrix["_DIM_SWEEP"] = sw
    n3 = load("neg_n3_curves.json")
    if n3:
        c = [r for r in n3["cf"] if "D9_as_control" in r]
        if c:
            matrix["CURVES/D9_text_to_curves_control"] = {
                "n": len(c),
                "fp_rate": round(sum(1 for r in c
                                     if r["D9_as_control"]["verdict_raw"] == "GRAPHIC_CHANGE") / len(c), 4),
                "records_median": N.med([r["D9_as_control"]["n_entries_raw"] for r in c]),
                "records_mean": round(statistics.fmean([r["D9_as_control"]["n_entries_raw"] for r in c]), 1),
                "fp_rate_after_glyph_filter": round(
                    sum(1 for r in c if r["D9_as_control"]["verdict_filtered"] == "GRAPHIC_CHANGE") / len(c), 4),
                "records_median_after_filter": N.med([r["D9_as_control"]["n_entries_filtered"] for r in c]),
            }
        e = [r for r in n3["cf"] if "curve_text_edit" in r]
        if e:
            matrix["CURVES/NC1_text_edit_on_curves"] = {
                "n": len(e),
                "fp_rate": round(sum(1 for r in e
                                     if r["curve_text_edit"]["verdict_raw"] == "GRAPHIC_CHANGE") / len(e), 4),
                "records_median": N.med([r["curve_text_edit"]["n_entries_raw"] for r in e]),
                "records_mean": round(statistics.fmean([r["curve_text_edit"]["n_entries_raw"] for r in e]), 1),
                "fp_rate_after_glyph_filter": round(
                    sum(1 for r in e if r["curve_text_edit"]["verdict_filtered"] == "GRAPHIC_CHANGE") / len(e), 4),
                "records_median_after_filter": N.med([r["curve_text_edit"]["n_entries_filtered"] for r in e]),
                "records_mean_after_filter": round(statistics.fmean(
                    [r["curve_text_edit"]["n_entries_filtered"] for r in e]), 2),
            }
        er = [r for r in n3["real"] if "curve_text_edit" in r]
        if er:
            matrix["CURVES/NC1_text_edit_on_REAL_curve_blocks"] = {
                "n": len(er),
                "fp_rate": round(sum(1 for r in er
                                     if r["curve_text_edit"]["verdict_raw"] == "GRAPHIC_CHANGE") / len(er), 4),
                "records_median": N.med([r["curve_text_edit"]["n_entries_raw"] for r in er]),
                "fp_rate_after_glyph_filter": round(
                    sum(1 for r in er if r["curve_text_edit"]["verdict_filtered"] == "GRAPHIC_CHANGE") / len(er), 4),
                "records_median_after_filter": N.med([r["curve_text_edit"]["n_entries_filtered"] for r in er]),
            }

    # ---------------- real pairs -------------------------------------------------
    rp = load("neg_real_pairs.json")
    real_matrix = {}
    if rp:
        buckets: dict[str, list] = {}
        for p in rp["pairs"]:
            if p["expected_verdict"] != "NO_GRAPHIC_CHANGE":
                continue
            for cl in p["classes"]:
                buckets.setdefault(cl, []).append(p)
            buckets.setdefault("ALL_SILENT", []).append(p)
        for k, v in sorted(buckets.items()):
            inner = [p["n_entries_inner"] for p in v]
            border = [p["n_entries_border"] for p in v]
            cnt = [p["res"]["ledger"]["count"] for p in v]
            oid = [p["res"]["ledger"]["object_id"] for p in v]
            ch = [p["res"]["ledger"]["churn"] for p in v]
            cntn = [p["own_scale"]["ledger"]["count"] for p in v]
            real_matrix[k] = {
                "n": len(v), "pairs": [p["pair_id"] for p in v],
                "ink_fp_rate": round(sum(1 for x in inner if x > 0) / len(v), 4),
                "ink_mean_per_block": round(statistics.fmean(inner), 3),
                "ink_median": N.med(inner), "ink_max": max(inner),
                "ink_border_mean": round(statistics.fmean(border), 3),
                "count_mean": round(statistics.fmean(cnt), 2),
                "count_mean_own_scale": round(statistics.fmean(cntn), 2),
                "object_id_mean": round(statistics.fmean(oid), 2),
                "churn_mean": round(statistics.fmean(ch), 2),
                "dim_retiled_mean": round(statistics.fmean([p["dim"]["n_retiled"] for p in v]), 2),
                "dim_records_mean": round(statistics.fmean([p["dim"]["n_records"] for p in v]), 2),
            }
        # sensitivity: false records on silent real pairs vs threshold
        sil = buckets.get("ALL_SILENT", [])
        sw = {}
        for k in sorted({k for p in sil for k in p["sweep"]}):
            v = [p["sweep"][k]["n"] for p in sil]
            sw[k] = {"fp_rate": round(sum(1 for x in v if x > 0) / max(1, len(v)), 4),
                     "mean_records": round(statistics.fmean(v), 3), "max": max(v)}
        real_matrix["_SWEEP_ON_SILENT_PAIRS"] = sw
        # recall side: the 15 pairs that really changed
        chg = [p for p in rp["pairs"] if p["expected_verdict"] == "GRAPHIC_CHANGE"]
        if chg:
            det = [p for p in chg if p["n_entries_inner"] > 0]
            real_matrix["_CHANGED_PAIRS_RECALL"] = {
                "n": len(chg), "detected": len(det),
                "recall": round(len(det) / len(chg), 4),
                "missed": [p["pair_id"] for p in chg if p["n_entries_inner"] == 0],
                "records_median": N.med([p["n_entries_inner"] for p in chg]),
            }
            sw2 = {}
            for k in sorted({k for p in chg for k in p["sweep"]}):
                v = [p["sweep"][k]["n"] for p in chg]
                sw2[k] = {"recall": round(sum(1 for x in v if x > 0) / len(v), 4),
                          "mean_records": round(statistics.fmean(v), 2)}
            real_matrix["_SWEEP_ON_CHANGED_PAIRS"] = sw2

    power_rows(matrix)
    dim_runmin(matrix)
    if rp:
        guard_rows(real_matrix, rp["pairs"])

    N.dump("neg_matrix.json", {"schema": "neg-matrix-1",
                               "counterfactual": matrix, "real": real_matrix,
                               "notes": notes})


if __name__ == "__main__":
    main()
