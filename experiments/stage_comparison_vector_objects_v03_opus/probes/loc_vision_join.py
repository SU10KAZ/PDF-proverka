# -*- coding: utf-8 -*-
"""Does the ledger see what the PICTURE cannot show?

The `cf` probe rendered every class-C counterfactual at 1 100 px and recorded how many
pixels changed (`cf_selfcheck_rows.jsonl`).  Joining that to the ledger rows by
(block_id, instance tag) answers two questions no single arm can:

  * of the changes that move NO pixel at all, how many does the ledger still name?
    — this is the part of the corpus a Vision comparator cannot reach in principle;
  * of the ledger's misses, how many were invisible anyway?
    — a miss on an invisible change is a different kind of failure from a miss on a
      change a human would have seen.
"""
from __future__ import annotations
import glob
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
from loc_agg2 import merged  # noqa: E402
import loc_agg as A          # noqa: E402


def main():
    vis = {}
    for line in open(ART / "cf_selfcheck_rows.jsonl", encoding="utf-8"):
        r = json.loads(line)
        if r["cls"] != "C" or not r.get("check"):
            continue
        vis[(r["block_id"], r["tag"])] = r["check"]
    R, prov = merged()
    rows = [r for r in R if r.get("cf_class") == "C" and (r["block_id"], r["inst"]) in vis]
    out = {"provenance": prov, "n_joined": len(rows),
           "n_cf_rows_with_picture": len(vis)}
    for noise in ("none", "round025"):
        sel = [r for r in rows if r["noise"] == noise]
        inv = [r for r in sel if vis[(r["block_id"], r["inst"])]["diff_px"] == 0]
        tiny = [r for r in sel if 0 < vis[(r["block_id"], r["inst"])]["diff_frac"] <= 1e-4]
        seen = [r for r in sel if vis[(r["block_id"], r["inst"])]["diff_frac"] > 1e-4]
        miss = [r for r in sel if not r["score"]["L2_localised"]]
        out[noise] = {
            "n": len(sel),
            "picture_invisible": {
                "n": len(inv), "share": round(len(inv) / max(len(sel), 1), 4),
                "ledger_L2": A.rate([r["score"]["L2_localised"] for r in inv]),
                "ledger_L4": A.rate([r["score"]["L4_right_object"] for r in inv]),
                "median_touched_ink_pt": A.med([r["touched_ink_pt"] for r in inv])},
            "picture_barely_visible_le_1e-4": {
                "n": len(tiny),
                "ledger_L2": A.rate([r["score"]["L2_localised"] for r in tiny]),
                "ledger_L4": A.rate([r["score"]["L4_right_object"] for r in tiny])},
            "picture_visible_gt_1e-4": {
                "n": len(seen),
                "ledger_L2": A.rate([r["score"]["L2_localised"] for r in seen]),
                "ledger_L4": A.rate([r["score"]["L4_right_object"] for r in seen])},
            "ledger_misses": {
                "n": len(miss),
                "share_of_misses_that_are_picture_invisible":
                    A.rate([vis[(r["block_id"], r["inst"])]["diff_px"] == 0 for r in miss]),
                "share_of_misses_below_1e-4_of_pixels":
                    A.rate([vis[(r["block_id"], r["inst"])]["diff_frac"] <= 1e-4
                            for r in miss])},
        }
    json.dump(out, open(ART / "loc_vision_join.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(out, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
