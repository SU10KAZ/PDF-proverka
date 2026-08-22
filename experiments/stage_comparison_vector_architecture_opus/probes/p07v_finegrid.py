"""p07v — VERIFIER: does a FINER threshold grid find a universal safe setting that the
560-point grid of hatchnoise_p4_transfer missed?

Reuses hatchnoise_p4_transfer.primitive_scalars (same scalars, same ground truth) and
sweeps a much denser grid, including the large gaps the original grid left
(P1 21..40, P2 61..inf, P3 0.80..1.0, P4 0..0.0005).

    python -m experiments.stage_comparison_vector_architecture_opus.probes.p07v_finegrid
"""
from __future__ import annotations
import json, os, pickle, statistics
from pathlib import Path
import numpy as np

from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_p4_transfer as T

SCRATCH = Path("/tmp/claude-1001/-home-coder-projects-PDF-proverka/7be66dd6-80e8-4c87-9aef-d5834ab15302/scratchpad/p07v")
SCRATCH.mkdir(parents=True, exist_ok=True)
OUT = C.ART / "p07v_finegrid.json"

NAMES = list(T.GT_BLOCKS)

def scalars(name):
    f = SCRATCH / f"{name}.npz"
    if f.exists():
        return dict(np.load(f))
    payload = C.load_primitives(*T.GT_BLOCKS[name])
    rows = C.segment_table(payload)["rows"]
    recs = T.primitive_scalars(rows)
    del rows, payload
    gtmap = {"hatch": 1, "furniture": 1, "underlay": 1, "foreground": 2, "unlabelled": 0}
    d = {
        "support": np.array([r["support"] for r in recs], dtype=np.int32),
        "motif_n": np.array([r["motif_n"] for r in recs], dtype=np.int32),
        "lum": np.array([r["lum"] for r in recs], dtype=np.float32),
        "length": np.array([r["length"] for r in recs], dtype=np.float32),
        "seg": np.array([r["segments"] for r in recs], dtype=np.int64),
        "gt": np.array([gtmap[r["gt"]] for r in recs], dtype=np.int8),
    }
    np.savez_compressed(f, **d)
    return d

P1 = [2,3,4,5,6,8,10,12,16,20,25,30,35,41]        # support is capped at 40 -> 41 == off
P2 = [4,6,8,12,18,24,40,60,100,150,250,400,700,1200,2500,5000,10**9]
P3 = [0.40,0.50,0.55,0.62,0.70,0.75,0.80,0.85,0.90,0.95,0.99,2.0]
P4 = [0.0,0.00002,0.00005,0.0001,0.0002,0.0003,0.0005,0.001,0.0015,0.004]

def main():
    data = {n: scalars(n) for n in NAMES}
    bg_tot = {n: int(data[n]["seg"][data[n]["gt"]==1].sum()) for n in NAMES}
    fg_tot = {n: int(data[n]["seg"][data[n]["gt"]==2].sum()) for n in NAMES}
    print("bg/fg segment totals:", {n:(bg_tot[n],fg_tot[n]) for n in NAMES})

    # precompute boolean masks per axis value per block to keep the sweep cheap
    res = {n: {} for n in NAMES}
    for n in NAMES:
        d = data[n]
        m1 = {p: d["support"] >= p for p in P1}
        m2 = {p: d["motif_n"] >= p for p in P2}
        m3 = {p: d["lum"] >= p for p in P3}
        m4 = {p: d["length"] < p for p in P4}
        segb = d["seg"] * (d["gt"]==1)
        segf = d["seg"] * (d["gt"]==2)
        for a in P1:
            for b in P2:
                ab = m1[a] | m2[b]
                for c in P3:
                    abc = ab | m3[c]
                    for e in P4:
                        drop = abc | m4[e] if e > 0 else abc
                        res[n][(a,b,c,e)] = (int(segb[drop].sum()), int(segf[drop].sum()))
        print("swept", n, len(res[n]), flush=True)

    keys = list(res[NAMES[0]])
    rows = []
    for k in keys:
        bg = [res[n][k][0]/max(bg_tot[n],1) for n in NAMES]
        fg = [res[n][k][1]/max(fg_tot[n],1) for n in NAMES]
        rows.append({"setting": [k[0],k[1],k[2],k[3]], "max_fg": max(fg),
                     "mean_bg": statistics.mean(bg), "bg": [round(v,4) for v in bg],
                     "fg": [round(v,4) for v in fg]})
    out = {"probe":"p07v_finegrid","blocks":NAMES,"n_settings":len(rows),
           "bg_segments":bg_tot,"fg_segments":fg_tot,"universal":[]}
    for bound in (0.01,0.02,0.05,0.10,0.15,0.20):
        safe = sorted([r for r in rows if r["max_fg"]<=bound], key=lambda r:-r["mean_bg"])
        best = safe[0] if safe else None
        if best: best = dict(best, mean_bg=round(best["mean_bg"],4),
                             zeros=sum(1 for v in best["bg"] if v==0.0))
        out["universal"].append({"bound":bound,"n_safe":len(safe),"best":best})
        print(bound, len(safe), best, flush=True)
    # per-sheet oracle on the fine grid
    orc = {}
    for i,n in enumerate(NAMES):
        cand = [r for r in rows if r["fg"][i] <= 0.01]
        cand.sort(key=lambda r:-r["bg"][i])
        orc[n] = {"bg": cand[0]["bg"][i], "setting": cand[0]["setting"]} if cand else None
    out["oracle"] = orc
    out["oracle_mean"] = round(statistics.mean(v["bg"] for v in orc.values()),4)
    out["oracle_zeros"] = sum(1 for v in orc.values() if v["bg"]==0.0)
    print("oracle", json.dumps(orc, ensure_ascii=False), out["oracle_mean"], out["oracle_zeros"])
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

if __name__ == "__main__":
    main()
