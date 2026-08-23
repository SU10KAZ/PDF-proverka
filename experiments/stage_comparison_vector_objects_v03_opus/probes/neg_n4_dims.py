# -*- coding: utf-8 -*-
"""N4 — dimensions: number-only change (D6) must be silent, chain re-tiling (D7) must fire."""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N          # noqa: E402
import neg_dim as DM            # noqa: E402
import v03_counterfactual as CF # noqa: E402
import v03_objects as O         # noqa: E402

L_GRID = [0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0]


def run():
    t0 = time.time()
    rows, skips = [], []
    for i, c in enumerate(N.carriers()):
        key = N.carrier_key(c)
        try:
            ex = N.carrier_extract(c)
        except Exception as e:
            skips.append({"carrier": key, "reason": str(e)}); continue
        la = O.build_objects(ex)
        S = la.S
        ch_base = DM.chains(ex, S)
        base = {"carrier": key, "discipline": c["discipline"], "cls": c["cls"],
                "n_seg": len(ex.segments), "n_text": len(ex.texts), "S": round(S, 3),
                "n_chains": len(ch_base),
                "n_chains_multi_tick": sum(1 for x in ch_base if x["n_ticks"] >= 2)}
        for cid in ("D6_dim_value_only", "D7_dim_geometry"):
            try:
                ex2, man = CF.apply(ex, la, cid, key=key)
            except CF.CFNotApplicable as e:
                skips.append({"carrier": key, "cf": cid, "reason": str(e)}); continue
            except Exception as e:
                skips.append({"carrier": key, "cf": cid, "reason": f"ERR {e}"}); continue
            try:
                r = N.full_compare2(ex, ex2, shared_scale=True)
            except Exception as e:
                skips.append({"carrier": key, "cf": cid, "reason": f"CMP {e}"}); continue
            lay_a, lay_b, off, rws, cfg = r["_la"], r["_lb"], r["_off"], r["_rows"], r["_cfg"]
            sweep = {}
            for Lm in L_GRID:
                for us in (0.2, 0.35):
                    v = N.ledger_at(ex, ex2, lay_a, lay_b, off, Lm, u_share=us,
                                    L_min_S=0.0, rows=rws)
                    sweep[f"L{Lm}_u{us}"] = {"n": v["n_entries"], "nb": v["n_border_entries"],
                                             "verdict": v["verdict"]}
            ch2 = DM.chains(ex2, max(lay_a.S, lay_b.S))
            ch1 = DM.chains(ex, max(lay_a.S, lay_b.S))
            dim_recs = DM.compare(ch1, ch2, off=off, S=S)
            rows.append({**base, "cf_id": cid,
                         "expected": CF.CF_SPECS[cid]["expected"],
                         "manifest": {k: v for k, v in man.items()
                                      if k in ("params", "delta", "change_bbox_pt",
                                               "changed_primitives")},
                         "geometry_identical": N.geometry_identical(ex, ex2),
                         "res": {k: v for k, v in r.items() if not k.startswith("_")},
                         "sweep": sweep,
                         "dim": {"n_chains_a": len(ch1), "n_chains_b": len(ch2),
                                 "n_records": len(dim_recs),
                                 "n_retiled": sum(1 for x in dim_recs
                                                  if x["type"] == "DIM_CHAIN_RETILED"),
                                 "n_only_a": sum(1 for x in dim_recs if x["type"] == "DIM_RUN_ONLY_A"),
                                 "n_only_b": sum(1 for x in dim_recs if x["type"] == "DIM_RUN_ONLY_B"),
                                 "records": dim_recs[:6]}})
        print(f"[{i+1}/59] {key} chains={len(ch_base)}", flush=True)
    N.dump("neg_n4_dims.json", {"schema": "neg-n4-1", "rows": rows, "skips": skips,
                                "L_grid": L_GRID, "sec": round(time.time() - t0, 1)})


if __name__ == "__main__":
    run()
