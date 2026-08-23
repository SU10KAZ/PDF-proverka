# -*- coding: utf-8 -*-
"""G1 — stability of the object layer under representation repacking (class A) [CF].

For every rewrite we report, per block:
  * delta in object count,
  * share of objects whose BOUNDARY is byte-identical (same set of original segments),
  * ink-length-weighted churn split (1:1 / split / merge / mixed / lost),
  * share of objects whose descriptor moved further than the distance to the nearest
    OTHER object on the same side  (i.e. the object became confusable),
  * the rewrite's "bite" — how many segments it actually touched.  A rewrite with
    bite 0 is a no-op on that block and is excluded from the stability claim.
Usage:  grp_g1_repack.py <shard> <nshards> [max_seg]
"""
from __future__ import annotations
import json, math, random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import v03_objects as O

SEED = 20260823
import os
ORDER = ["A1_path_split", "A2_path_merge", "A3_curve_resample_down", "A3_curve_resample_up",
         "A4_circle_to_bezier", "A4b_circle_to_chords5", "A4c_circle_to_chords24",
         "A5_order_shuffle", "A6_round_0.01", "A6_round_0.1", "A6_round_0.25",
         "A6_round_0.5", "A8_lineweight"]
if os.environ.get("GRP_RW"):
    ORDER = os.environ["GRP_RW"].split(",")
MIN_SEG = int(os.environ.get("GRP_MIN_SEG", "0"))


def boundary_identity(layer_a, layer_b, segs_b):
    """Share of left objects whose exact original-segment set exists on the right."""
    right = set()
    for o in layer_b.objects:
        src = []
        for gi in o["segments"]:
            src.extend(segs_b[gi].get("src") or [gi])
        right.add(frozenset(src))
    hit = sum(1 for o in layer_a.objects if frozenset(o["segments"]) in right)
    return hit / max(1, len(layer_a.objects))


def descriptor_confusion(layer_a, rows, layer_b, sample_rng, cap=150):
    """Share of objects whose descriptor drift exceeds the distance to the nearest
    OTHER object on the left.  This is the number that decides whether the drift
    matters: a shift smaller than the gap to the next object is harmless."""
    objs = layer_a.objects
    if len(objs) < 2:
        return None, None, None, 0, None
    idx = list(range(len(objs)))
    if len(idx) > cap:
        idx = sample_rng.sample(idx, cap)
    by_o = {r["o"]: r for r in rows}
    n_bad = 0
    n_tot = 0
    n_degenerate = 0
    drifts = []
    nns = []
    for i in idx:
        r = by_o.get(i)
        if r is None or r.get("partner") is None:
            continue
        d_self = O.descriptor_distance(objs[i]["desc"], layer_b.objects[r["partner"]]["desc"])
        best_other = None
        for j in range(len(objs)):
            if j == i:
                continue
            d = O.descriptor_distance(objs[i]["desc"], objs[j]["desc"])
            if best_other is None or d < best_other:
                best_other = d
                if best_other == 0.0:
                    break
        drifts.append(d_self)
        if best_other is not None:
            nns.append(best_other)
        if best_other is None or best_other <= 1e-9:
            # an identical twin already exists on the same side: the descriptor cannot
            # separate them with or without the rewrite.  Counted, not blamed.
            n_degenerate += 1
            continue
        n_tot += 1
        if d_self > best_other:
            n_bad += 1
    import statistics as _st
    med_d = _st.median(drifts) if drifts else None
    med_nn = _st.median(nns) if nns else None
    if n_tot == 0:
        return None, med_d, 0, n_degenerate, med_nn
    return n_bad / n_tot, med_d, n_tot, n_degenerate, med_nn


def run_block(rec, arc_ablation=False):
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return None
    t0 = time.time()
    ex = G.extract(pb)
    t_extract = time.time() - t0
    if not ex.segments:
        return None
    rng0 = random.Random(SEED)
    segs0 = G.rw_identity(ex.segments, rng0)
    params = {"arc_enable": not arc_ablation}
    t0 = time.time()
    L0 = G.layer_of(segs0, ex.texts, **params)
    t_build = time.time() - t0
    out = {"block_id": rec["block_id"], "doc_id": rec["doc_id"], "version": rec["version"],
           "discipline": rec["discipline"], "cls": rec["cls"], "bucket": rec["bucket"],
           "n_seg": len(ex.segments), "n_text": len(ex.texts), "n_curves": ex.quality["n_curves"],
           "n_circles": len(G._closed_circles(ex.segments)),
           "S": round(L0.S, 3), "scale_source": L0.scale_source,
           "n_obj": len(L0.objects), "counts": L0.counts(),
           "ink_coverage": L0.stats["ink_coverage"],
           "t_extract": round(t_extract, 3), "t_build": round(t_build, 3),
           "arc_ablation": arc_ablation, "rewrites": {}}
    for name in ORDER:
        rng = random.Random(SEED)
        try:
            segs = G.REWRITES[name](ex.segments, rng)
        except Exception as e:
            out["rewrites"][name] = {"error": repr(e)}
            continue
        bite = G.rewrite_bite(name, ex.segments, segs)
        L = G.layer_of(segs, ex.texts, **params)
        rows = G.churn_exact(L0, segs0, L, segs)
        cl = G.classify_churn(rows)
        conf, drift, n_conf, n_deg, nn = descriptor_confusion(L0, rows, L, random.Random(SEED + 1))
        out["rewrites"][name] = {
            "bite": bite, "n_seg_out": len(segs), "n_obj": len(L.objects),
            "d_obj": len(L.objects) - len(L0.objects),
            "boundary_identical": round(boundary_identity(L0, L, segs), 5),
            "churn": {k: round(v, 5) for k, v in cl.items()},
            "desc_confused": (None if conf is None else round(conf, 5)),
            "desc_drift_median": (None if drift is None else round(drift, 5)),
            "n_desc": n_conf, "n_desc_degenerate": n_deg,
            "desc_nn_median": (None if nn is None else round(nn, 5)),
        }
    return out


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nsh = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    max_seg = int(sys.argv[3]) if len(sys.argv) > 3 else 20000
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for b in sample["blocks"] if MIN_SEG <= b["n_seg"] <= max_seg]
    blocks = [b for i, b in enumerate(blocks) if i % nsh == shard]
    outp = G.ART / f"grp_runs/{os.environ.get('GRP_TAG','g1')}_{shard}.jsonl"
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
            # arc ablation on the circle-recoding rewrites only, same blocks
            if r and r.get("n_circles", 0) >= 3:
                try:
                    r2 = run_block(rec, arc_ablation=True)
                    if r2:
                        fh.write(json.dumps(r2, ensure_ascii=False) + "\n")
                        fh.flush()
                except Exception:
                    pass
            print(f"[{shard}] {k+1}/{len(blocks)} {rec['block_id'][:12]} n_seg={rec['n_seg']}",
                  flush=True)


if __name__ == "__main__":
    main()
