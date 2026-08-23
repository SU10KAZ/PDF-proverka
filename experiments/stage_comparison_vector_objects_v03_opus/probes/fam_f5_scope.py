# -*- coding: utf-8 -*-
"""F5 — where is the notion of a FAMILY meaningless, and how much of the corpus is that?

Random (not hand-picked) sample of the census `cns_block_classes.jsonl`, so the shares
read directly as corpus shares.  Per block we ask the two opposite questions:

  * is there anything repeated at all?   share of objects that sit in a family of >= 2
  * is what repeats an ELEMENT or just BACKGROUND?  a block whose largest family is a
    third of all objects and whose members are single strokes is hatching / table ruling,
    not "12 sockets".

Also dumps per-family features for F6 (hatch vs symbol) so both use one measurement.
Usage: fam_f5_scope.py [n] [out_prefix]
"""
from __future__ import annotations
import json, math, random, statistics, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import fam_common as C
import fam_family as FAM

SEED = 20260823
MAX_SEG = 60000
TOP_FAM = 12


def fam_features(f, objects, S, block_ink):
    ms = f["members"]
    diags = [objects[m]["desc"]["diag"] for m in ms]
    vecs = [objects[m]["desc"]["vec"] for m in ms]
    cen = [sum(v[j] for v in vecs) / len(vecs) for j in range(len(vecs[0]))]
    cyc = sum(1 for m in ms if objects[m].get("cycle")) / len(ms)
    lab = sum(1 for m in ms if objects[m].get("label")) / len(ms)
    occ = sum(1 for j in range(8, 24) if cen[j] > 0.01)
    dirmax = max(cen[2:8])
    pts = [(objects[m]["cx"], objects[m]["cy"]) for m in ms]
    nn = []
    if len(pts) >= 2:
        for i, (x, y) in enumerate(pts):
            d = min(math.hypot(x - u, y - v) for k, (u, v) in enumerate(pts) if k != i)
            nn.append(d)
    med_d = statistics.median(diags)
    return {
        "n": len(ms), "cls": f["cls"],
        "n_seg_med": f["n_seg_med"],
        "diag_med": round(med_d, 3), "diag_over_S": round(med_d / max(S, 1e-9), 3),
        "elong": round(cen[1] * 8.0, 4),           # total stroke length / bbox diagonal
        "arc_share": round(cen[24], 4),
        "cycle_share": round(cyc, 4),
        "label_share": round(lab, 4),
        "occupied_cells": occ,                      # of the 4x4 occupancy grid
        "dir_concentration": round(dirmax, 4),
        "size_cv": round((statistics.pstdev(diags) / med_d) if med_d > 0 else 0.0, 4),
        "nn_cv": round((statistics.pstdev(nn) / statistics.median(nn))
                       if nn and statistics.median(nn) > 0 else 0.0, 4),
        "nn_med_over_diag": round((statistics.median(nn) / med_d) if nn and med_d > 0 else 0.0, 3),
        "ink_share": round(f["seg_len_sum"] / max(block_ink, 1e-9), 5),
        "radius_max": f["radius_max"],
        "bbox": f["bbox"],
    }


def one_block(b):
    pb = G.prepared_block(b["doc_id"], b["version"], b["block_id"])
    if pb is None:
        return {"block_id": b["block_id"], "skip": "no result.json"}
    t0 = time.time()
    ex = G.extract(pb)
    row = {"block_id": b["block_id"], "doc_id": b["doc_id"], "version": b["version"],
           "discipline": b["discipline"], "cls": b["cls"], "elig": b["elig"],
           "n_seg_census": b["n_seg"], "n_seg": len(ex.segments), "n_text": len(ex.texts)}
    if not ex.segments:
        return dict(row, verdict="no_vector")
    if len(ex.segments) > MAX_SEG:
        return dict(row, verdict="too_big_for_probe")
    L = G.layer_of(ex.segments, ex.texts)
    F = FAM.build_families(L)
    objs = L.objects
    block_ink = sum(o["seg_len"] for o in objs) or 1.0
    fams = sorted(range(len(F.families)), key=lambda i: -len(F.families[i]["members"]))
    feats = [fam_features(F.families[i], objs, L.S, block_ink) for i in fams[:TOP_FAM]]
    big = feats[0] if feats else None
    n_obj = len(objs)
    share_rep = F.stats["share_objects_in_repeated"]
    largest_share = (F.stats["largest_family"] / n_obj) if n_obj else 0.0
    # degenerate: one huge family of near-primitive members carrying real ink
    hatchy = bool(big and big["n"] >= 8 and largest_share >= 0.25 and
                  big["n_seg_med"] <= 2 and big["ink_share"] >= 0.05)
    if b["cls"] == "curved_text":
        verdict = "text_in_curves"
    elif share_rep < 0.20:
        verdict = "no_families"
    elif hatchy:
        verdict = "background_dominated"
    else:
        verdict = "usable"
    row.update({
        "n_obj": n_obj, "S": round(L.S, 3), "scale_src": L.scale_source,
        "n_fam": F.stats["n_families"], "n_rep": F.stats["n_repeated_families"],
        "share_obj_in_repeated": share_rep,
        "share_ink_in_repeated": F.stats["share_ink_in_repeated"],
        "largest_family": F.stats["largest_family"],
        "largest_family_share_obj": round(largest_share, 5),
        "largest_family_ink_share": big["ink_share"] if big else 0.0,
        "stray_share": round(sum(1 for o in objs if o["cls"] == "stray") / max(n_obj, 1), 4),
        "verdict": verdict, "families": feats,
        "t_sec": round(time.time() - t0, 2)})
    return row


def _work(b):
    try:
        return one_block(b)
    except Exception:
        return {"block_id": b["block_id"], "discipline": b.get("discipline"),
                "cls": b.get("cls"), "error": traceback.format_exc().splitlines()[-1]}


def sample_of(n):
    """Random corpus sample; blocks the probe cannot afford are dropped BEFORE
    extraction using the census segment count, and counted, so the denominator of
    every share stays the honest corpus denominator."""
    all_rows = [r for r in G.block_records()]
    rng = random.Random(SEED)
    rng.shuffle(all_rows)
    return all_rows[:n], len(all_rows)


def main():
    """Sharded, incremental: one process per shard, one JSONL line per block.
    mp.Pool deadlocked on this workload (workers frozen in futex with 0 CPU for
    50 min), so the probe does not use it.
    Usage: fam_f5_scope.py N out.jsonl [shard] [of]"""
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 400
    out = sys.argv[2] if len(sys.argv) > 2 else str(G.ART / "fam_f5_scope.jsonl")
    shard = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    of = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    sample, corpus_n = sample_of(n)
    mine = [b for i, b in enumerate(sample) if i % of == shard]
    print("shard", shard, "of", of, ":", len(mine), "blocks; corpus", corpus_n, flush=True)
    fh = open(out, "w", encoding="utf-8")
    for k, b in enumerate(mine):
        if b.get("n_seg", 0) > MAX_SEG:
            r = {"block_id": b["block_id"], "discipline": b.get("discipline"),
                 "cls": b.get("cls"), "n_seg_census": b.get("n_seg"),
                 "verdict": "too_big_for_probe"}
        else:
            r = _work(b)
        fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        fh.flush()
        if k % 10 == 0:
            print(shard, k, len(mine), r.get("verdict") or (r.get("error") or "")[:40], flush=True)
    fh.close()
    print("shard", shard, "done", flush=True)


if __name__ == "__main__":
    main()
