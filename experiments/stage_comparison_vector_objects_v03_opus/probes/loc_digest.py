# -*- coding: utf-8 -*-
"""Print every number the report cites, in report order (for the final refresh pass)."""
import json
from pathlib import Path
ART = Path(__file__).resolve().parents[1] / "artifacts"
J = lambda n: json.load(open(ART / n, encoding="utf-8"))

s = J("loc_sensitivity.json")
print("== provenance", s["provenance"])
print("== by_density_and_size (none)")
for k, v in s["by_density_and_size"]["none"].items():
    print(f"   {k:18s} n={v['n']:4d} L2={v['L2_localised']:.2f} L4={v['L4_right_object']:.2f}")
print("== by_area_share (none)")
for c in s["by_area_share"]["none"]:
    print("   ", c)
print("== by_area_share per density (none)")
for k, v in s["by_area_share_and_density_noise_none"].items():
    print("   ", k, [(c["area_frac"], c["n"], c["L2"]) for c in v])
print("== blind zone", json.dumps(s["scalar_blind_zone"], ensure_ascii=False))
print("== by_cf_id (none)")
for k, v in s["by_cf_id"]["none"].items():
    print(f"   {k:22s} n={v['n']:3d} L2={v['L2']:.3f} L3={v['L3']:.3f} L4={v['L4']:.3f} "
          f"counts_blind={v['counts_blind']:.3f} med_sim={v['median_sim']}")
print("== by_cf_id (round025)")
for k, v in s["by_cf_id"]["round025"].items():
    print(f"   {k:22s} n={v['n']:3d} L2={v['L2']:.3f} L3={v['L3']:.3f} L4={v['L4']:.3f}")

v = J("loc_vision_join.json")
for nz in ("none", "round025"):
    print("== vision", nz, json.dumps(v[nz], ensure_ascii=False))

d = J("loc_dilution.json")
print("== dilution", d["n_rows"], "rows", d["n_carriers"], "carriers", d["summary"])
for c in d["by_frame_growth"]["none"]:
    print("   ", {k: c[k] for k in ("frame_area_over_target", "n", "median_n_seg_frame",
                                    "median_deleted_ink_frac", "median_ink_similarity",
                                    "scalar_999_calls_identical", "ledger_L2",
                                    "median_false_records", "median_t_sec")})

sc = J("loc_samecounts.json")
print("== samecounts strict", sc["strict_counters_identical"])
for k, val in sc["per"].items():
    if k.endswith("|none") or k.endswith("|round025"):
        print(f"   {k:34s}", {kk: val[kk] for kk in ("n", "counters_identical_measured",
                                                     "counts_verdict_blind", "scalar_999_blind",
                                                     "ledger_L2", "ledger_L3", "ledger_L4",
                                                     "median_records")})

r = J("loc_real_summary.json")
print("== real baselines", r["baselines_pair_level"], "n_pos", r["n_pos"], "n_neg", r["n_neg"])
print("== real roc interior")
for c in r["pair_level_roc_interior_records"]:
    print("   ", c)
print("== real roc all")
for c in r["pair_level_roc_all_records"][:1] + r["pair_level_roc_all_records"][-4:]:
    print("   ", c)

h = J("loc_hybrid_roc.json")
for k in h:
    if isinstance(h[k], dict) and "curve" in h[k]:
        print("== hybrid", k, "n_pos", h[k]["n_cf_positives"])
        for c in h[k]["curve"]:
            print("   ", c)

b = J("loc_border_rule.json")
print("== border rule (thin=20, frac=0.5)")
for x in b["grid"]:
    if x["thin_pt"] == 20.0 and x["along_frac"] == 0.5:
        print("   ", x)

c = J("loc_cost.json")
print("== cost", c["fit"], c["extrapolation"])
for t in c["by_block_size"]:
    print("   ", t)

try:
    a = J("loc_arms.json")
    print("== arms")
    print(json.dumps(a, ensure_ascii=False, indent=1))
except FileNotFoundError:
    pass
print("== desc bug", {k: v for k, v in J("loc_desc_bug.json").items() if k != "per_block"})
print("== c4 tolerance", J("loc_c4_tolerance.json")["summary"])
print("== c4 vacuity", J("loc_c4_vacuity.json")["summary"])
