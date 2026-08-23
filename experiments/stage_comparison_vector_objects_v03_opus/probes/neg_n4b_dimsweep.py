# -*- coding: utf-8 -*-
"""N4b — the dimension-chain channel is a THRESHOLD, and the threshold was wrong.

`neg_n4_dims.py` ran the chain detector at its default `run_min_S = 6.0`, i.e. a
dimension line has to be at least six characteristic scales long.  Half of the D7
carriers' dimension lines are shorter than that, so the channel could not fire on
them by construction.  This sweeps the one parameter that gates it and reports the
D6-false-positive / D7-recall pair at each setting, which is the only honest way to
say what the channel is worth.

Nothing else changes: same carriers, same D6/D7, same geometry.
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N          # noqa: E402
import neg_dim as DM            # noqa: E402
import v03_counterfactual as CF # noqa: E402
import v03_objects as O         # noqa: E402

RUN_MIN = [1.5, 2.0, 3.0, 4.0, 6.0, 8.0]
MAX_SEG = int(__import__("os").environ.get("NEG_N4B_MAX_SEG", "0")) or 10**9


def run(shard=0, of=1):
    t0 = time.time()
    rows, skips = [], []
    for i, c in enumerate(N.carriers()):
        if i % of != shard:
            continue
        key = N.carrier_key(c)
        try:
            ex = N.carrier_extract(c)
        except Exception as e:
            skips.append({"carrier": key, "reason": str(e)}); continue
        if len(ex.segments) > MAX_SEG:
            skips.append({"carrier": key, "reason": f"n_seg {len(ex.segments)} > cap {MAX_SEG}"})
            continue
        la = O.build_objects(ex)
        S = la.S
        made = {}
        for cid in ("D6_dim_value_only", "D7_dim_geometry"):
            try:
                made[cid] = CF.apply(ex, la, cid, key=key)
            except Exception as e:
                skips.append({"carrier": key, "cf": cid, "reason": str(e)})
        if len(made) < 2:
            continue
        base = {"carrier": key, "discipline": c["discipline"], "S": round(S, 3),
                "n_seg": len(ex.segments),
                "dim_line_len_pt": made["D7_dim_geometry"][1]["params"]["dim_line_len_pt"],
                "tick_len_pt": made["D7_dim_geometry"][1]["params"]["tick_len_pt"]}
        per = {}
        for rm in RUN_MIN:
            ch0 = DM.chains(ex, S, run_min_S=rm)
            cell = {"n_chains_base": len(ch0)}
            for cid, (ex2, man) in made.items():
                ch2 = DM.chains(ex2, S, run_min_S=rm)
                recs = DM.compare(ch0, ch2, off=(0.0, 0.0), S=S)
                cell[cid] = {
                    "n_chains": len(ch2),
                    "n_retiled": sum(1 for x in recs if x["type"] == "DIM_CHAIN_RETILED"),
                    "n_only_a": sum(1 for x in recs if x["type"] == "DIM_RUN_ONLY_A"),
                    "n_only_b": sum(1 for x in recs if x["type"] == "DIM_RUN_ONLY_B"),
                }
            per[f"run_min_S={rm}"] = cell
        rows.append({**base, "sweep": per})
        print(f"[{i+1}] {key} S={S:.2f} dimlen={base['dim_line_len_pt']:.1f}", flush=True)
    name = "neg_n4b_dimsweep.json" if of == 1 and MAX_SEG >= 10**9 else f"neg_runs/neg_n4b_{shard}of{of}_cap{MAX_SEG}.json"
    N.dump(name, {"schema": "neg-n4b-1", "run_min_grid": RUN_MIN, "rows": rows,
                  "skips": skips, "sec": round(time.time() - t0, 1)})


if __name__ == "__main__":
    a = sys.argv[1:]
    run(int(a[0]) if a else 0, int(a[1]) if len(a) > 1 else 1)
