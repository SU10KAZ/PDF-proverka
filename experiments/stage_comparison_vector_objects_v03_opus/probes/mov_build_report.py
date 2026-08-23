# -*- coding: utf-8 -*-
"""Assemble artifacts/mov_FINDINGS.md from probes/mov_report.tmpl.md + the aggregated JSON.
Every number in the report therefore comes from an artifact, never from prose.

    python probes/mov_build_report.py
"""
from __future__ import annotations
import json, re, subprocess, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
CF = json.load(open(ART / "mov_cf_summary.json", encoding="utf-8"))
RE_ = json.load(open(ART / "mov_real_summary.json", encoding="utf-8"))
BR = json.load(open(ART / "mov_borderrule.json", encoding="utf-8"))
RG = json.load(open(ART / "mov_region.json", encoding="utf-8"))
UNI = json.load(open(ART / "mov_border_universe.json", encoding="utf-8"))
PR = json.load(open(ART / "mov_prose.json", encoding="utf-8"))

tables = subprocess.run([sys.executable, str(HERE / "mov_tables.py")],
                        capture_output=True, text=True, check=True).stdout
blocks = {}
cur = None
buf = []
for line in tables.splitlines():
    m = re.match(r"^### \[(T[\w]+)\]", line)
    if m:
        if cur:
            blocks[cur] = "\n".join(buf).strip()
        cur = m.group(1)
        buf = [line]
    elif cur:
        buf.append(line)
if cur:
    blocks[cur] = "\n".join(buf).strip()
(ART / "mov_tables.md").write_text(tables, encoding="utf-8")


def by(tag_prefix, field, sub=None):
    tot = n = 0
    for tag, v in CF["by_tag"].items():
        if tag.startswith(tag_prefix):
            x = v.get(field)
            if sub and isinstance(x, dict):
                x = x.get(sub)
            if isinstance(x, (int, float)):
                tot += x
            n += v.get("n_scored", 0)
    return tot, n


def cnt(pref, key):
    return sum(v[key] for t, v in CF["block_transformed_flag"].items() if t.startswith(pref))


fpc = CF["silent_classes"]["by_class"]
fpb_n = sum(fpc[k]["n"] for k in ("B1", "B2", "B5"))
fpb_f = sum(fpc[k]["fp"] for k in ("B1", "B2", "B5"))
fpj_n = sum(fpc[k]["n"] for k in ("B3", "B4"))
fpj_f = sum(fpc[k]["fp"] for k in ("B3", "B4"))
curve = {(r["lo_pt"], r["hi_pt"]): r for r in CF["curve_by_pt"] if r["band"] == "all"}
below = sum(r["n"] for (lo, hi), r in curve.items() if hi <= 0.5)
first = curve[(0.5, 1)]["recall"]
combo = CF["combo"]
a = RE_["anchor_vs_bbox"]
tb = RE_["transform_bench"]
bd = RE_["transform_border_sample"]
b1 = CF["by_tag"].get("B1_translate@0.02", {})
V = {
    "CF_rows": CF["n_rows"],
    "B1_n": sum(v["n_scored"] for t, v in CF["by_tag"].items() if t.startswith("B1")),
    "B1_ok001": sum(1 for _ in []) or None,
    "B2_terr": CF["by_tag"].get("B2_scale@1.05", {}).get("t_err_pt", {}).get("median"),
    "B5_theta": cnt("B5", "flagged"),
    "B5_n": cnt("B5", "n"),
    "AVB_n": a["anchor"]["n"],
    "AVB_org": a["bbox_org"]["median"],
    "AVB_anchor": a["anchor"]["median"],
    "AVB_better": a["n_anchor_better_than_org"],
    "AVB_worse": a["n_anchor_worse_than_org"],
    "AVB_dt": a["dt_org_pt"]["median"],
    "CURVE_below": below,
    "CURVE_first": f"{first:.3f}",
    "FPB_n": fpb_n, "FPB_fp": fpb_f, "FPB_rate": f"{fpb_f/max(1,fpb_n):.3f}",
    "FPBJ_rate": f"{fpj_f/max(1,fpj_n):.3f}",
    "BT_B1": f"{cnt('B1','flagged')}/{cnt('B1','n')} B1",
    "BT_B2": f"{cnt('B2','flagged')}/{cnt('B2','n')} B2",
    "BT_B5": f"{cnt('B5','flagged')}/{cnt('B5','n')} B5",
    "M3_lo": f"{min(v['recall'] for v in combo.values()):.3f}" if combo else "?",
    "M3_hi": f"{max(v['recall'] for v in combo.values()):.3f}" if combo else "?",
    "M3_n": sum(v["n"] for v in combo.values()),
    "BR_v1fp": f"{BR['headline']['v1_fp_rate']:.4f}",
    "BR_v2fp": f"{BR['headline']['v2_pad0.0_fp_rate']:.4f}",
    "BR_v1tp": f"{BR['headline']['v1_tp_rate']:.4f}",
    "BR_v2tp": f"{BR['headline']['v2_pad0.0_tp_rate']:.4f}",
    "TR_bench_t1": f"{tb['share_t_gt_1pt']:.3f}" if tb["share_t_gt_1pt"] is not None else "—",
    "TR_bench_max": tb["t_norm_pt"].get("max"),
    "TR_bench_s": f"{tb['share_s_gt_0.01']:.3f}" if tb["share_s_gt_0.01"] is not None else "—",
    "TR_bd_t1": f"{bd['share_t_gt_1pt']:.3f}" if bd["share_t_gt_1pt"] is not None else "—",
    "RG_95": f"{RG['share_below_0.95']:.3f}",
    "RG_80": f"{RG['share_below_0.80']:.3f}",
    "UN_n": CF["unavailable"]["n"],
    "UN_share": f"{CF['unavailable']['share']:.4f}",
    "BD_universe": UNI["n_pairs_with_big_components"],
    "BD_border_only": UNI["n_border_only"] if UNI["n_border_only"] else "—",
    "BD_border_share": f"{UNI.get('border_only_share', 0):.3f}",
    "P_silent": PR["n_silent"], "P_c3n": PR["c3_above_floor"],
    "P_silent_tiny": PR["silent_tiny"], "P_silent_dense": PR["silent_dense"],
    "P_sig": PR["silent_with_signal"], "P_below": PR["silent_below_pub_threshold"],
    "P_loc_n": PR["n_localised_not_move"], "P_loc_ar": PR["localised_as_add_remove"],
    "P_sigma_max": PR["align_sigma"]["max"], "P_sigma_n": PR["align_sigma_n"],
    "P_exp": PR["time_exponent"], "P_fitn": PR["time_fit_n"],
    "P_t500": PR["time_by_band"]["<500"]["median"],
    "P_t5k": PR["time_by_band"]["500-5k"]["median"],
    "P_t20k": PR["time_by_band"]["5k-20k"]["median"],
    "P_t20kp": PR["time_by_band"][">20k"]["median"],
    "P_tmax": PR["time_by_band"][">20k"]["max"],
    "OI_quiet": PR["oi_quiet"], "OI_bad": PR["oi_bad"],
    "OI_share": f"{PR['oi_share']:.3f}", "OI_p90": PR["oi_p90"], "OI_max": PR["oi_max"],
}
_bd = dict(RE_["m5_real"]["by_source"].get("border_sample", []))
_un = _bd.get("ALIGNMENT_UNAVAILABLE", 0)
_try = sum(v for k, v in _bd.items() if k != "SKIPPED_TOO_DENSE")
V["M5_bd_un"] = _un
V["M5_bd_try"] = _try
V["M5_bd_rate"] = f"{_un/max(1,_try):.3f}"
V["M5_bd_dense"] = _bd.get("SKIPPED_TOO_DENSE", 0)
V["BD_sampled"] = sum(_bd.values())
V["CF_err"] = CF["n_error"]
V["CF_carr"] = CF["n_carriers"]
PH = json.load(open(ART / "mov_phase.json", encoding="utf-8"))
_a = PH["agg"]
V["PH_n"] = _a["0.0"]["n"]
V["PH_small"] = "0.01"
V["PH_c001"] = _a["0.01"]["churn_median"]
V["PH_n001"] = _a["0.01"]["n"] - _a["0.01"]["n_zero"]
V["PH_c01"] = _a["0.1"]["churn_median"]
V["PH_c3"] = _a["3.0"]["churn_median"]
V["PH_max"] = max(_a[k]["churn_max"] for k in _a)
V["PH_zero05"] = _a["0.5"]["n_zero"]
_cm = RE_["bench_confusion"]
V["CM_tp"] = _cm.get("GRAPHIC_CHANGE->GRAPHIC_CHANGE", 0)
V["CM_fn"] = _cm.get("GRAPHIC_CHANGE->NO_GRAPHIC_CHANGE", 0)
V["CM_fp"] = _cm.get("NO_GRAPHIC_CHANGE->GRAPHIC_CHANGE", 0)
V["CM_pos"] = V["CM_tp"] + V["CM_fn"]
_tf = RE_["theta_free"]
V["TF_n"] = _tf["n"]; V["TF_med"] = _tf["median"]; V["TF_p90"] = _tf["p90"]
V["TF_max"] = _tf["max"]; V["TF_nz"] = _tf["n_nonzero"]
_c3 = BR["by_class"].get("C3", {})
V["BRC3_v1"] = _c3.get("v1_on_truth"); V["BRC3_v2"] = _c3.get("v2_pad0.0_on_truth")
V["BRC3_n"] = _c3.get("n")
_fm = BR.get("false_moved") or {}
V["FM_b3_v1"] = _fm.get("B3", {}).get("v1_instances_with_MOVED")
V["FM_b3_n"] = _fm.get("B3", {}).get("n")
V["FM_b3_v1n"] = _fm.get("B3", {}).get("v1_findings_total")
V["FM_b4_v1n"] = _fm.get("B4", {}).get("v1_findings_total")
V["BR_carr"] = BR["n_carriers"]; V["BR_n"] = BR["n_scored"]
V["BR_c2n"] = (BR["by_class"].get("C2") or {}).get("n", "—")
try:
    UR = json.load(open(ART / "mov_unavail_reasons.json", encoding="utf-8"))
    _rr = [r for r in UR["rows"] if r.get("inlier_ratio") is not None]
    V["UR_reasons"] = ", ".join(f"`{k}` — {v}" for k, v in UR["agg"]["by_reason"])
    V["UR_inl_lo"] = min(r["inliers"] for r in _rr) if _rr else "?"
    V["UR_inl_hi"] = max(r["inliers"] for r in _rr) if _rr else "?"
    V["UR_ratio_lo"] = min(r["inlier_ratio"] for r in _rr) if _rr else "?"
    V["UR_ratio_hi"] = max(r["inlier_ratio"] for r in _rr) if _rr else "?"
    V["UR_objratio"] = UR["agg"].get("obj_ratio_median")
    V["UR_n"] = UR["agg"]["n"]
    V["UR_lowratio"] = dict(UR["agg"]["by_reason"]).get("low_inlier_ratio", 0)
    V["UR_b2"] = dict(UR["agg"]["by_tag"]).get("B2_scale@1.2", 0)
except FileNotFoundError:
    pass
# B1 exact-recovery count needs the raw rows
import glob
b1rows = []
for f in sorted(glob.glob(str(ART / "mov_runs" / "cf_*.jsonl"))):
    for l in open(f, encoding="utf-8"):
        if '"B1_translate' in l:
            r = json.loads(l)
            if r["tag"].startswith("B1_translate") and (r.get("transform") or {}).get("t_err_pt") is not None:
                b1rows.append(r["transform"]["t_err_pt"])
V["B1_n"] = len(b1rows)
V["B1_ok001"] = sum(1 for x in b1rows if x <= 0.01)

txt = (HERE / "mov_report.tmpl.md").read_text(encoding="utf-8")
for k, v in V.items():
    txt = txt.replace("{{" + k + "}}", str(v))
for k, v in blocks.items():
    # inside the report the table title is a bold line, not a heading: the surrounding
    # prose already carries the section structure
    v = re.sub(r"^### (\[T[\w]+\] .*)$", r"**\1**", v, count=1, flags=re.M)
    txt = txt.replace(f"<!--{k}-->", v)
left = re.findall(r"\{\{(\w+)\}\}|<!--(T\w+)-->", txt)
if left:
    print("UNFILLED:", left, file=sys.stderr)
(ART / "mov_FINDINGS.md").write_text(txt, encoding="utf-8")
print("wrote", ART / "mov_FINDINGS.md", len(txt), "chars")
