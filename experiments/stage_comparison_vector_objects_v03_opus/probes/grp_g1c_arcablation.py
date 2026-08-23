# -*- coding: utf-8 -*-
"""G1c — does closing Bezier chains into arcs actually help the DESCRIPTOR?

Direct ablation on circle re-encoding, the v0.2 R12 case.  For every object we take the
descriptor distance to its own ink partner after the rewrite (drift).  Reported as the
mean over objects per block, then the median over blocks — the median over objects is
0.0 (most objects contain no circle at all) and hides the effect entirely.
Usage:  grp_g1c_arcablation.py <shard> <nshards>
"""
from __future__ import annotations
import json, random, statistics, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import v03_objects as O

SEED = 20260823
RWS = ["A4_circle_to_bezier", "A4b_circle_to_chords5", "A4c_circle_to_chords24"]


def drift(L0, segs0, L1, segs1, only_arcs=False):
    rows = G.churn_exact(L0, segs0, L1, segs1)
    d = []
    for r in rows:
        if r.get("partner") is None or r["best_share"] < 0.9:
            continue
        a = L0.objects[r["o"]]
        if only_arcs and a.get("arc_share", 0) <= 0.05:
            continue
        d.append(O.descriptor_distance(a["desc"], L1.objects[r["partner"]]["desc"]))
    return d


def run_block(rec):
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return None
    ex = G.extract(pb)
    if len(G._closed_circles(ex.segments)) < 3:
        return None
    segs0 = G.rw_identity(ex.segments, random.Random(SEED))
    out = {"block_id": rec["block_id"], "discipline": rec["discipline"], "cls": rec["cls"],
           "n_seg": len(ex.segments), "n_circles": len(G._closed_circles(ex.segments)), "res": {}}
    for name in RWS:
        segs1 = G.REWRITES[name](ex.segments, random.Random(SEED))
        if G.rewrite_bite(name, ex.segments, segs1) <= 0:
            continue
        r = {}
        for tag, on in (("arc_on", True), ("arc_off", False)):
            L0 = G.layer_of(segs0, ex.texts, arc_enable=on)
            L1 = G.layer_of(segs1, ex.texts, arc_enable=on)
            d_all = drift(L0, segs0, L1, segs1, only_arcs=False)
            d_arc = drift(L0, segs0, L1, segs1, only_arcs=True)
            r[tag] = {"n_obj": len(L0.objects), "n_matched": len(d_all),
                      "drift_mean": (round(statistics.mean(d_all), 5) if d_all else None),
                      "n_arc_obj": len(d_arc),
                      "drift_mean_arc_objects": (round(statistics.mean(d_arc), 5) if d_arc else None)}
        out["res"][name] = r
    return out if out["res"] else None


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nsh = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for b in sample["blocks"] if 100 <= b["n_seg"] <= 12000]
    rng = random.Random(SEED)
    rng.shuffle(blocks)
    blocks = [b for i, b in enumerate(blocks[:120]) if i % nsh == shard]
    outp = G.ART / f"grp_runs/g1c_{shard}.jsonl"
    outp.parent.mkdir(exist_ok=True)
    with open(outp, "w", encoding="utf-8") as fh:
        for k, rec in enumerate(blocks):
            try:
                r = run_block(rec)
            except Exception:
                r = None
            if r:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                fh.flush()
            print(f"[{shard}] {k+1}/{len(blocks)}", flush=True)


if __name__ == "__main__":
    main()
