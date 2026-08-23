# -*- coding: utf-8 -*-
"""G1b — can the 25-number shape descriptor IDENTIFY the same object after a repack?

Retrieval test.  Ground truth = the ink partner (churn_exact, best_share >= 0.9).
Three rankers:
  * descriptor alone (all objects on the other side are candidates);
  * descriptor with a position gate (candidate centre within 3*S);
  * position alone (nearest centre).
Top-1 accuracy of each.  This is the number that decides whether the descriptor can
carry identity by itself or only as a tie-breaker next to geometry.
Usage:  grp_g1b_identity.py <shard> <nshards>
"""
from __future__ import annotations
import json, math, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import v03_objects as O

SEED = 20260823
RWS = ["A6_round_0.25", "A4b_circle_to_chords5", "A3_curve_resample_up"]


def retrieval(L0, segs0, L1, segs1, S, rng, cap=120):
    rows = G.churn_exact(L0, segs0, L1, segs1)
    gt = {r["o"]: r["partner"] for r in rows
          if r.get("partner") is not None and r["best_share"] >= 0.9}
    idx = [o for o in gt if L0.objects[o]["n_seg"] >= 3]
    if len(idx) > cap:
        idx = rng.sample(sorted(idx), cap)
    if not idx:
        return None
    hit_d = hit_dp = hit_p = 0
    for i in idx:
        a = L0.objects[i]
        best_d = best_dp = best_p = None
        for j, b in enumerate(L1.objects):
            dd = O.descriptor_distance(a["desc"], b["desc"])
            pp = math.hypot(a["cx"] - b["cx"], a["cy"] - b["cy"])
            if best_d is None or dd < best_d[0]:
                best_d = (dd, j)
            if best_p is None or pp < best_p[0]:
                best_p = (pp, j)
            if pp <= 3.0 * S and (best_dp is None or dd < best_dp[0]):
                best_dp = (dd, j)
        if best_d and best_d[1] == gt[i]:
            hit_d += 1
        if best_dp and best_dp[1] == gt[i]:
            hit_dp += 1
        if best_p and best_p[1] == gt[i]:
            hit_p += 1
    n = len(idx)
    return {"n": n, "top1_descriptor": hit_d / n, "top1_descriptor_pos_gate": hit_dp / n,
            "top1_position": hit_p / n}


def run_block(rec):
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return None
    ex = G.extract(pb)
    if len(ex.segments) < 50:
        return None
    segs0 = G.rw_identity(ex.segments, random.Random(SEED))
    L0 = G.layer_of(segs0, ex.texts)
    out = {"block_id": rec["block_id"], "discipline": rec["discipline"], "cls": rec["cls"],
           "bucket": rec["bucket"], "n_seg": len(ex.segments), "n_obj": len(L0.objects),
           "S": round(L0.S, 3), "res": {}}
    for name in RWS:
        segs1 = G.REWRITES[name](ex.segments, random.Random(SEED))
        if G.rewrite_bite(name, ex.segments, segs1) <= 0:
            continue
        L1 = G.layer_of(segs1, ex.texts)
        r = retrieval(L0, segs0, L1, segs1, L0.S, random.Random(SEED + 3))
        if r:
            out["res"][name] = r
    return out if out["res"] else None


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nsh = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for b in sample["blocks"] if 50 <= b["n_seg"] <= 8000]
    rng = random.Random(SEED)
    rng.shuffle(blocks)
    blocks = blocks[:80]
    blocks = [b for i, b in enumerate(blocks) if i % nsh == shard]
    outp = G.ART / f"grp_runs/g1b_{shard}.jsonl"
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
