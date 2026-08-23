# -*- coding: utf-8 -*-
"""G5 — parameter sensitivity.  Are the thresholds tuned, or does the answer survive?

For each parameter we sweep one value at a time (everything else at DEFAULTS) and
report, per block: object count and the class-A churn (A6_round_0.25, the rewrite that
bites everywhere).  A result that slides monotonically with a parameter has no natural
scale — v0.2 OBJ-5 found exactly that for naive proximity clustering (486->429->398->3->1).
Usage:  grp_g5_sweep.py <shard> <nshards>
"""
from __future__ import annotations
import json, random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import v03_objects as O

SEED = 20260823
SWEEPS = {
    "diag_max": [2.0, 4.0, 6.0, 8.0, 12.0, 20.0, 40.0, 1e9],   # HARD symbol-scale cap
    "alpha": [0.2, 0.4, 0.6, 0.9, 1.5, 3.0],                    # core merge radius
    "node_tol": [0.0, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5],        # endpoint welding
    "k_long": [2.0, 4.0, 6.0, 10.0, 20.0],                      # long-run threshold
    "dash_merge": [True, False],
    "dash_gap": [1.0, 2.0, 4.0],
    "dash_min": [3, 4, 6, 10],
    "arc_enable": [True, False],
    "scale_mode": ["auto", "text", "geom"],
}
PROBE_RW = "A6_round_0.25"


def run_block(rec):
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return None
    ex = G.extract(pb)
    if not ex.segments:
        return None
    segs0 = G.rw_identity(ex.segments, random.Random(SEED))
    segs1 = G.REWRITES[PROBE_RW](ex.segments, random.Random(SEED))
    out = {"block_id": rec["block_id"], "discipline": rec["discipline"], "cls": rec["cls"],
           "bucket": rec["bucket"], "n_seg": len(ex.segments), "sweeps": {}}
    for pname, values in SWEEPS.items():
        rows = []
        for v in values:
            kw = {pname: v}
            t0 = time.time()
            L0 = G.layer_of(segs0, ex.texts, **kw)
            L1 = G.layer_of(segs1, ex.texts, **kw)
            cl = G.classify_churn(G.churn_exact(L0, segs0, L1, segs1))
            rows.append({"v": v, "n_obj": len(L0.objects),
                         "counts": L0.counts(),
                         "one_to_one": round(cl["one_to_one"], 5),
                         "S": round(L0.S, 3),
                         "t": round(time.time() - t0, 3)})
        out["sweeps"][pname] = rows
    return out


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nsh = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for b in sample["blocks"] if 50 <= b["n_seg"] <= 12000]
    rng = random.Random(SEED)
    rng.shuffle(blocks)
    blocks = blocks[:96]
    blocks = [b for i, b in enumerate(blocks) if i % nsh == shard]
    outp = G.ART / f"grp_runs/g5_{shard}.jsonl"
    outp.parent.mkdir(exist_ok=True)
    with open(outp, "w", encoding="utf-8") as fh:
        for k, rec in enumerate(blocks):
            try:
                r = run_block(rec)
            except Exception as e:
                r = {"block_id": rec["block_id"], "error": repr(e)}
            if r:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                fh.flush()
            print(f"[{shard}] {k+1}/{len(blocks)}", flush=True)


if __name__ == "__main__":
    main()
