# -*- coding: utf-8 -*-
"""Aggregate cf_runs.json -> cf_manifest.json (the CF set) + cf_selfcheck.json (checks)."""
from __future__ import annotations
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"


def q(vals, p):
    if not vals:
        return None
    v = sorted(vals)
    k = min(len(v) - 1, max(0, int(round(p * (len(v) - 1)))))
    return v[k]


def summ(vals, nd=6):
    vals = [v for v in vals if v is not None]
    if not vals:
        return {"n": 0}
    return {"n": len(vals), "median": round(st.median(vals), nd), "mean": round(sum(vals) / len(vals), nd),
            "p90": round(q(vals, 0.9), nd), "max": round(max(vals), nd), "min": round(min(vals), nd)}


def main():
    runs = json.load(open(ART / "cf_runs.json", encoding="utf-8"))
    carriers, cfs = [], []
    cov = defaultdict(lambda: {"instances": 0, "carriers": set(), "disciplines": set(),
                               "buckets": defaultdict(int), "size_buckets": defaultdict(int),
                               "skips": defaultdict(int), "errors": defaultdict(int)})
    fid = {"with_text": [], "no_text": []}
    rows = []
    for r in runs["runs"]:
        c = r["carrier"]
        if r.get("error"):
            carriers.append({**{k: c[k] for k in ("block_id", "doc_id", "version", "discipline",
                                                  "page_number", "cls", "n_seg", "n_text", "bucket")},
                             "error": r["error"]})
            continue
        b = r.get("block", {})
        carriers.append({**{k: c[k] for k in ("block_id", "doc_id", "version", "discipline",
                                              "page_number", "cls", "bucket")}, **b})
        for k in fid:
            f = (r.get("fidelity") or {}).get(k)
            if f:
                fid[k].append(f)
        for x in r["instances"]:
            cid = x["cf_id"]
            e = cov[cid]
            if x["status"] == "skip":
                e["skips"][x.get("reason", "?")[:80]] += 1
                continue
            if x["status"] == "error":
                e["errors"][x.get("reason", "?")[:120]] += 1
                continue
            man = x["manifest"]
            e["instances"] += 1
            e["carriers"].add(c["block_id"])
            e["disciplines"].add(c["discipline"])
            e["buckets"][c["bucket"]] += 1
            sb = (man.get("params") or {}).get("size_bucket")
            if sb:
                e["size_buckets"][sb] += 1
            chk = x.get("check") or {}
            cfs.append({"selfcheck": {"visible_on_raster": (None if chk.get("diff_px") is None
                                                            else chk.get("diff_px") > 0),
                                      "diff_frac": chk.get("diff_frac"),
                                      "localised": chk.get("localised"),
                                      "frac_diff_inside_expected_bbox":
                                          chk.get("frac_diff_inside_expected_bbox")},
                        "carrier": {"block_id": c["block_id"], "doc_id": c["doc_id"],
                                    "version": c["version"], "discipline": c["discipline"],
                                    "page_number": c["page_number"], "cls": c["cls"],
                                    "bucket": c["bucket"], "n_seg": b.get("n_seg"),
                                    "n_obj": b.get("n_obj"), "S_pt": b.get("S_pt")},
                        "tag": x["tag"], "kwargs": x["kwargs"], "manifest": man})
            pr = (man.get("page_rewrite") or {})
            rows.append({"cf_id": cid,
                         "page_rewrite": ({"rect_equal": pr.get("rect_equal"),
                                           "src_rot": pr.get("src_rot"),
                                           "out_rot": pr.get("out_rot")} if pr else None),
                         "n_seg_after": man["changed_primitives"].get("n_after"),
                         "n_seg_before": man["changed_primitives"].get("n_before"), "tag": x["tag"], "cls": man["cf_class"],
                         "discipline": c["discipline"], "bucket": c["bucket"],
                         "block_id": c["block_id"], "n_seg": b.get("n_seg"),
                         "size_bucket": sb,
                         "delta": man.get("delta"),
                         "expected": man["expected_verdict"],
                         "check": x.get("check"), "check_pdf": x.get("check_pdf"),
                         "check_locality": x.get("check_locality"),
                         "geometry": x.get("geometry")})

    coverage = {}
    for cid, e in sorted(cov.items()):
        coverage[cid] = {"instances": e["instances"], "n_carriers": len(e["carriers"]),
                         "n_disciplines": len(e["disciplines"]),
                         "disciplines": sorted(e["disciplines"]),
                         "buckets": dict(e["buckets"]), "size_buckets": dict(e["size_buckets"]),
                         "skips": dict(e["skips"]), "errors": dict(e["errors"]),
                         "publishable": len(e["carriers"]) >= 15 and len(e["disciplines"]) >= 5}
    man_out = {"schema": "v03-cf-manifest-1", "seed": runs["seed"], "target_px": runs["target_px"],
               "n_carriers": len(carriers), "n_counterfactuals": len(cfs),
               "coverage": coverage, "carriers": carriers, "counterfactuals": cfs}
    p = ART / "cf_manifest.json"
    json.dump(man_out, open(p, "w", encoding="utf-8"), ensure_ascii=False)
    print("cf_manifest.json", round(p.stat().st_size / 1e6, 1), "MB", len(cfs), "counterfactuals")

    # ---------------- self-check aggregation ----------------
    def by_cf(pred, val):
        out = {}
        for cid in sorted({r["cf_id"] for r in rows if pred(r)}):
            vs = [val(r) for r in rows if r["cf_id"] == cid and pred(r)]
            vs = [v for v in vs if v is not None]
            out[cid] = summ(vs)
        return out

    A_rows = [r for r in rows if r["cls"] == "A"]
    B_rows = [r for r in rows if r["cls"] == "B"]
    C_rows = [r for r in rows if r["cls"] == "C"]
    D_rows = [r for r in rows if r["cls"] == "D"]

    def dfrac(r):
        c = r.get("check") or {}
        return c.get("diff_frac")

    check = {
        "renderer_fidelity": {
            k: {"n": len(v),
                "ref_covered_by_mine": summ([x["ref_covered_by_mine"] for x in v], 4),
                "mine_covered_by_ref": summ([x["mine_covered_by_ref"] for x in v], 4),
                "iou": summ([x["iou"] for x in v], 4)}
            for k, v in fid.items()},
        "A_picture_invariance": by_cf(lambda r: r["cls"] == "A", dfrac),
        "A_ink_iou": by_cf(lambda r: r["cls"] == "A", lambda r: (r.get("check") or {}).get("ink_iou")),
        "A_geometry_exact_share": {
            cid: {"n": sum(1 for r in A_rows if r["cf_id"] == cid and r.get("geometry")),
                  "identical": sum(1 for r in A_rows if r["cf_id"] == cid
                                   and (r.get("geometry") or {}).get("identical"))}
            for cid in sorted({r["cf_id"] for r in A_rows})},
        "A_pagelevel_tolerant": by_cf(lambda r: r["cls"] == "A",
                                      lambda r: (r.get("check") or {}).get("diff_frac_1px_tolerant")),
        "B_picture_after_compensation": by_cf(lambda r: r["cls"] == "B", dfrac),
        "B_ink_iou": by_cf(lambda r: r["cls"] == "B", lambda r: (r.get("check") or {}).get("ink_iou")),
        "B5_pdf_level": {
            "strict": summ([(r.get("check_pdf") or {}).get("diff_frac")
                            for r in B_rows if r["cf_id"] == "B5_rotate_page"]),
            "tolerant_1px": summ([(r.get("check_pdf") or {}).get("diff_frac_1px_tolerant")
                                  for r in B_rows if r["cf_id"] == "B5_rotate_page"]),
            "geometry_identical": sum(1 for r in B_rows if r["cf_id"] == "B5_rotate_page"
                                      and ((r.get("check") or {}).get("geometry_match") or {}).get("identical")),
            "n": sum(1 for r in B_rows if r["cf_id"] == "B5_rotate_page")},
        "C_locality": {
            cid: {"n": sum(1 for r in C_rows if r["cf_id"] == cid),
                  "localised": sum(1 for r in C_rows if r["cf_id"] == cid
                                   and (r.get("check") or {}).get("localised") is True),
                  "invisible": sum(1 for r in C_rows if r["cf_id"] == cid
                                   and (r.get("check") or {}).get("diff_px") == 0),
                  "frac_inside": summ([(r.get("check") or {}).get("frac_diff_inside_expected_bbox")
                                       for r in C_rows if r["cf_id"] == cid], 4),
                  "diff_frac": summ([dfrac(r) for r in C_rows if r["cf_id"] == cid])}
            for cid in sorted({r["cf_id"] for r in C_rows})},
        "D_geometry_identical": {
            cid: {"n": sum(1 for r in D_rows if r["cf_id"] == cid),
                  "identical": sum(1 for r in D_rows if r["cf_id"] == cid
                                   and (r.get("check") or {}).get("identical") is True),
                  "text_lines_changed": summ([(r.get("check") or {}).get("text_lines_changed")
                                              for r in D_rows if r["cf_id"] == cid], 2)}
            for cid in sorted({r["cf_id"] for r in D_rows})},
        "D7_locality": {
            "n": sum(1 for r in D_rows if r["cf_id"] == "D7_dim_geometry"),
            "localised": sum(1 for r in D_rows if r["cf_id"] == "D7_dim_geometry"
                             and (r.get("check_locality") or {}).get("localised") is True),
            "frac_inside": summ([(r.get("check_locality") or {}).get("frac_diff_inside_expected_bbox")
                                 for r in D_rows if r["cf_id"] == "D7_dim_geometry"], 4)},
        "D9_page_level": {
            "diff_frac": summ([(r.get("check") or {}).get("diff_frac")
                               for r in D_rows if r["cf_id"] == "D9_text_to_curves"]),
            "diff_frac_1px_tolerant": summ([(r.get("check") or {}).get("diff_frac_1px_tolerant")
                                            for r in D_rows if r["cf_id"] == "D9_text_to_curves"]),
            "text_after": summ([(r.get("check") or {}).get("n_text_after")
                                for r in D_rows if r["cf_id"] == "D9_text_to_curves"], 2),
            "geom_match_0.05_frac_of_a": summ([((r.get("check") or {}).get("geometry_match_tol_0.05") or {}).get("frac_of_a")
                                               for r in D_rows if r["cf_id"] == "D9_text_to_curves"], 4)},
    }
    # C sensitivity curve: C3 by size bucket x delta
    curve = defaultdict(list)
    for r in C_rows:
        if r["cf_id"] != "C3_move_object":
            continue
        d = (r.get("delta") or {}).get("d_pt")
        c = r.get("check") or {}
        curve[(r["size_bucket"], r["tag"].split("@")[-1])].append(
            {"d_pt": d, "d_over_S": (r.get("delta") or {}).get("d_over_S"),
             "diff_frac": c.get("diff_frac"), "diff_px": c.get("diff_px"),
             "ink_a": c.get("ink_a"), "n_seg": r["n_seg"]})
    check["C3_sensitivity"] = {
        f"{k[0]}@{k[1]}": {"n": len(v),
                           "diff_frac": summ([x["diff_frac"] for x in v]),
                           "diff_px": summ([x["diff_px"] for x in v], 1),
                           "share_diff_below_1e4": round(sum(1 for x in v if (x["diff_frac"] or 0) < 1e-4) / len(v), 3),
                           "share_invisible": round(sum(1 for x in v if (x["diff_px"] or 0) == 0) / len(v), 3),
                           "d_over_S": summ([x["d_over_S"] for x in v], 3)}
        for k, v in sorted(curve.items())}
    # C1/C2 by size bucket: how big must an object be to be seen at all
    for cid in ("C1_remove_object", "C2_add_object", "C6_reshape_object"):
        agg = defaultdict(list)
        for r in C_rows:
            if r["cf_id"] != cid:
                continue
            agg[r["size_bucket"]].append(r)
        check[f"{cid}_by_size"] = {
            k: {"n": len(v),
                "diff_frac": summ([dfrac(r) for r in v]),
                "share_invisible": round(sum(1 for r in v if (r.get("check") or {}).get("diff_px") == 0) / len(v), 3),
                "ink_share_of_block": summ([(r.get("check") or {}).get("ink_diff_frac_of_a") for r in v], 5)}
            for k, v in sorted(agg.items())}
    # page-level rewrite properties (does the tool preserve the page box and /Rotate?)
    pl = defaultdict(list)
    for r in rows:
        if r.get("page_rewrite"):
            pl[r["cf_id"]].append(r)
    check["page_rewrite_properties"] = {
        cid: {"n": len(v),
              "rect_preserved": sum(1 for r in v if r["page_rewrite"]["rect_equal"]),
              "rotation_changed": sum(1 for r in v if r["page_rewrite"]["src_rot"] != r["page_rewrite"]["out_rot"]),
              "seg_ratio_after_over_before": summ([(r["n_seg_after"] / r["n_seg_before"])
                                                   for r in v if r.get("n_seg_before")], 4),
              "seg_identical_count": sum(1 for r in v if r.get("n_seg_after") == r.get("n_seg_before"))}
        for cid, v in sorted(pl.items())}
    # visibility curve: does the raster show a change at all, by touched-object area share
    vis = []
    for e in cfs:
        m = e["manifest"]
        if m["cf_class"] != "C" or not m.get("touched_objects"):
            continue
        sc = e["selfcheck"]
        if sc.get("visible_on_raster") is None:
            continue
        vis.append({"cf_id": m["cf_id"],
                    "area_frac": m["touched_objects"][0]["area_frac_of_block"],
                    "n_seg_obj": m["touched_objects"][0]["n_seg"],
                    "visible": bool(sc["visible_on_raster"]),
                    "diff_frac": sc.get("diff_frac"),
                    "localised": sc.get("localised")})
    bins = [(0, 1e-4), (1e-4, 3e-4), (3e-4, 1e-3), (1e-3, 3e-3), (3e-3, 1e-2),
            (1e-2, 3e-2), (3e-2, 1.0)]
    curve = {}
    for lo, hi in bins:
        sel = [v for v in vis if lo <= v["area_frac"] < hi]
        if not sel:
            continue
        curve[f"{lo:g}..{hi:g}"] = {
            "n": len(sel),
            "share_visible": round(sum(1 for v in sel if v["visible"]) / len(sel), 4),
            "share_localised": round(sum(1 for v in sel if v["localised"]) / len(sel), 4),
            "diff_frac": summ([v["diff_frac"] for v in sel]),
            "obj_n_seg": summ([v["n_seg_obj"] for v in sel], 1)}
    check["visibility_by_object_area"] = curve
    check["localised_among_visible"] = {
        cid: {"visible": sum(1 for r in C_rows if r["cf_id"] == cid
                             and (r.get("check") or {}).get("diff_px", 0) > 0),
              "localised": sum(1 for r in C_rows if r["cf_id"] == cid
                               and (r.get("check") or {}).get("localised") is True)}
        for cid in sorted({r["cf_id"] for r in C_rows})}
    json.dump(check, open(ART / "cf_selfcheck.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    with open(ART / "cf_selfcheck_rows.jsonl", "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    print("cf_selfcheck.json written;", len(rows), "instance rows")
    npub = sum(1 for v in coverage.values() if v["publishable"])
    print("publishable cf ids:", npub, "of", len(coverage))


if __name__ == "__main__":
    main()
