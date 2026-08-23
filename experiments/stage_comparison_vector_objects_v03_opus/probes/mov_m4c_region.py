# -*- coding: utf-8 -*-
"""M4c — how much of each side is even COMPARABLE [REAL].

Reads the transforms the main run already estimated (mov_runs/bench_*.jsonl,
fallback_*.jsonl) and measures, per pair, the share of each side's ink length that
falls inside the frame intersection.  Ink outside it can never be called
added / removed / moved, so this is the honest denominator of the verdict.

    python probes/mov_m4c_region.py [--shard i --of k]
"""
from __future__ import annotations
import argparse, glob, json, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import mov_common as MC       # noqa: E402
import mov_align as MA        # noqa: E402
import mov_m4_real as R       # noqa: E402
import grp_common as G        # noqa: E402

ART = MC.ART


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--of", type=int, default=1)
    a = ap.parse_args()
    src = []
    for f in sorted(glob.glob(str(ART / "mov_runs" / "bench_*.jsonl"))) + \
             sorted(glob.glob(str(ART / "mov_runs" / "fallback_*.jsonl"))):
        for l in open(f, encoding="utf-8"):
            if l.strip():
                src.append(json.loads(l))
    pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    byid = {p["pair_id"]: p for p in pairs}
    out = []
    for i, r in enumerate(src):
        if i % a.of != a.shard:
            continue
        if r.get("status") not in ("ALIGNED", "ALIGNMENT_AMBIGUOUS"):
            continue
        t0 = time.time()
        row = {"pair_id": r["pair_id"], "source": r["source"],
               "classes": r.get("classes"), "expected": r.get("expected"),
               "frame_a": r["frame_a"], "frame_b": r["frame_b"]}
        try:
            if r["source"] == "benchmark":
                p = byid[r["pair_id"]]
                exA = R.side_extract(p["side_a"]); exB = R.side_extract(p["side_b"])
            else:
                pa = G.prepared_block(r["doc_a"], r["ver_a"], r["block_a"])
                pb = G.prepared_block(r["doc_b"], r["ver_b"], r["block_b"])
                exA, exB = G.extract(pa), G.extract(pb)
            g = r["transform"]
            T = MA.Sim(g["s"], g["theta"], g["tx"], g["ty"])
            fa, fb = MC.frame_of(exA), MC.frame_of(exB)
            inter = MA._frame_intersection(fa, fb, T)
            row["inter"] = [round(v, 2) for v in inter]
            row["comparable_share_a"] = R.comparable_share(exA.segments, T, inter)
            row["comparable_share_b"] = R.comparable_share(exB.segments, None, inter)
            row["area_share_a"] = round(max(0.0, inter[2] - inter[0]) * max(0.0, inter[3] - inter[1])
                                        / max(1e-9, (fa[2] - fa[0]) * (fa[3] - fa[1])), 4)
            row["area_share_b"] = round(max(0.0, inter[2] - inter[0]) * max(0.0, inter[3] - inter[1])
                                        / max(1e-9, (fb[2] - fb[0]) * (fb[3] - fb[1])), 4)
            row["n_seg"] = [len(exA.segments), len(exB.segments)]
        except Exception as e:
            row["error"] = repr(e)
        row["t_sec"] = round(time.time() - t0, 1)
        out.append(row)
        print(row["pair_id"], row.get("comparable_share_a"), row.get("comparable_share_b"),
              row.get("error", ""), flush=True)
    with open(ART / "mov_runs" / f"region_{a.shard}.jsonl", "w", encoding="utf-8") as fh:
        for r in out:
            fh.write(json.dumps(r, ensure_ascii=False, default=float) + "\n")


if __name__ == "__main__":
    main()
