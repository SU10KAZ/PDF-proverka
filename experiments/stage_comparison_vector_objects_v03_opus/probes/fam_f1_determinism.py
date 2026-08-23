# -*- coding: utf-8 -*-
"""F1 — is the family partition a function of the CONTENT, or of the input order?

v0.2 P19: greedy leader clustering produced 50 changed clusters on a byte-identical
PDF; the two-pass produced 0.  Here the three modes are compared directly on the same
real blocks: the object list is permuted with a seeded shuffle (the ink is identical,
only the order differs) and the induced partition of the objects is compared.

    repeat        same input twice -> must be identical (pure determinism)
    permuted      shuffled input   -> identical iff the clustering is order free
Usage: fam_f1_determinism.py [n] [out.json] [shard] [of]
"""
from __future__ import annotations
import json, random, statistics, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import fam_family as FAM

SEED = 20260823
MAX_SEG = 60000
MODES = ("twopass", "greedy", "greedy_input")


def partition(F, order=None):
    """canonical partition: frozenset of frozensets of ORIGINAL object indices."""
    inv = order or list(range(len(F.obj_family)))
    d = {}
    for i, f in enumerate(F.obj_family):
        if f < 0:
            continue
        d.setdefault(f, []).append(inv[i])
    return frozenset(frozenset(v) for v in d.values())


def one(b):
    pb = G.prepared_block(b["doc_id"], b["version"], b["block_id"])
    if pb is None:
        return None
    ex = G.extract(pb)
    if not ex.segments or len(ex.segments) > MAX_SEG:
        return None
    L = G.layer_of(ex.segments, ex.texts)
    objs = L.objects
    if len(objs) < 4:
        return None
    rng = random.Random(SEED)
    perm = list(range(len(objs)))
    rng.shuffle(perm)
    shuffled = [objs[i] for i in perm]
    row = {"block_id": b["block_id"], "discipline": b["discipline"], "cls": b["cls"],
           "n_seg": len(ex.segments), "n_obj": len(objs)}
    for m in MODES:
        F1 = FAM.build_families(objs, mode=m)
        F2 = FAM.build_families(objs, mode=m)
        F3 = FAM.build_families(shuffled, mode=m)
        p1, p2, p3 = partition(F1), partition(F2), partition(F3, perm)
        lab1 = F1.obj_family
        lab3 = [0] * len(objs)
        for k, i in enumerate(perm):
            lab3[i] = F3.obj_family[k]
        row[m] = {"n_fam": len(F1.families), "repeat_identical": p1 == p2,
                  "permuted_identical": p1 == p3,
                  "ari_permuted": round(FAM.ari(lab1, lab3), 6),
                  "n_fam_permuted": len(F3.families)}
    return row


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 120
    out = sys.argv[2] if len(sys.argv) > 2 else str(G.ART / "fam_f1_determinism.json")
    shard = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    of = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    smp = [b for i, b in enumerate(
        json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))["blocks"][:n])
        if i % of == shard]
    rows = []
    for k, b in enumerate(smp):
        try:
            r = one(b)
        except Exception:
            r = {"block_id": b["block_id"], "error": traceback.format_exc().splitlines()[-1]}
        if r:
            rows.append(r)
        if k % 20 == 0:
            print(shard, k, len(smp), flush=True)
    summ = {}
    for m in MODES:
        rs = [r for r in rows if m in r]
        summ[m] = {"n": len(rs),
                   "repeat_identical": sum(1 for r in rs if r[m]["repeat_identical"]),
                   "permuted_identical": sum(1 for r in rs if r[m]["permuted_identical"]),
                   "permuted_identical_share": round(
                       sum(1 for r in rs if r[m]["permuted_identical"]) / max(len(rs), 1), 4),
                   "ari_permuted_median": round(statistics.median(
                       [r[m]["ari_permuted"] for r in rs]), 6) if rs else None,
                   "ari_permuted_min": round(min([r[m]["ari_permuted"] for r in rs]), 6) if rs else None,
                   "ari_eq1_share": round(sum(1 for r in rs if r[m]["ari_permuted"] > 0.99999)
                                          / max(len(rs), 1), 4)}
    json.dump({"seed": SEED, "summary": summ, "rows": rows},
              open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(summ, indent=1))


if __name__ == "__main__":
    main()
