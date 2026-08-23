# -*- coding: utf-8 -*-
"""R2 — USEFULNESS OF EACH RELATION TYPE FOR MATCHING OBJECTS (ablation).

`grp` G7 measured that the shape descriptor alone gets top-1 = 0.700 under
A6_round_0.25, and that POSITION alone gets 1.000.  Position is only decisive because
a rewrite does not move anything; the interesting question is the one position cannot
answer: **can the relations tell two identical motifs apart?**

So the ablation is run in two regimes, both reported:

  * `noposition` — candidates are ALL objects of the other side, ranked by descriptor
    distance alone.  This is the regime where a block full of identical valve symbols
    is genuinely ambiguous.
  * `position`   — candidates gated to 3*S around the object (the `grp` G7 gate).

The ablation itself is parameter-free: the rank key is
    (round(descriptor_distance, 3), relation_signature_distance)
so adding a relation type can only re-order objects the descriptor already calls a tie.
No weight is tuned; nothing to overfit.

Ground truth: exact segment provenance (counterfactual side) — never the thing measured.

Usage:  rel_r2_matching.py [shard nshards]
"""
from __future__ import annotations
import json, math, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import rel_common as C
import rel_relations as R
import v03_counterfactual as CF
import cf_build_set as CB

REWRITES = ["A6_round_0.25", "A6_round_0.1", "A4b_circle_to_chords5", "A1_path_split"]
MAX_OBJ = 1500          # matching is O(n^2) in the no-position regime; cap it honestly


def motif_code(o, eps=0.05):
    return (o["cls"], tuple(int(round(v / eps)) for v in o["desc"]["vec"]))


def signatures(layer, rels, texts):
    """Per object, per relation type: a multiset of neighbour features.

    A neighbour is described by WHAT IT IS (class + quantised shape + quantised size),
    never by its index — otherwise the signature would presuppose the matching.
    """
    sig: list[dict[str, list]] = [{} for _ in layer.objects]
    S = layer.S or 1.0
    for r in rels:
        t = r["type"]
        pairs = []
        if t == "LABEL_ANCHOR":
            pairs.append((r["a"], ("TXT", (r.get("text") or "").strip())))
        elif t == "LEADER_TO":
            f = ("LDR", (r.get("text") or "").strip(), layer.objects[r["b"]]["cls"])
            pairs.append((r["a"], f))
            pairs.append((r["b"], ("LDRT", (r.get("text") or "").strip())))
        else:
            for u, v in ((r["a"], r["b"]), (r["b"], r["a"])):
                if u is None or v is None:
                    continue
                ob = layer.objects[v]
                f = (ob["cls"],
                     tuple(int(round(x * 12)) for x in ob["desc"]["vec"][:8]),
                     int(round(math.log(max(ob["diag"], 1e-6)) / math.log(1.5))))
                pairs.append((u, f))
                if not r.get("sym"):
                    break
        for u, f in pairs:
            if u is None:
                continue
            sig[u].setdefault(t, []).append(f)
    return sig


def sig_dist(a, b):
    if not a and not b:
        return 0.0
    ca, cb = {}, {}
    for f in a:
        ca[f] = ca.get(f, 0) + 1
    for f in b:
        cb[f] = cb.get(f, 0) + 1
    inter = sum(min(ca.get(k, 0), cb.get(k, 0)) for k in set(ca) | set(cb))
    union = sum(max(ca.get(k, 0), cb.get(k, 0)) for k in set(ca) | set(cb))
    return 1.0 - inter / union if union else 0.0


def rank_all(layerA, layerB, sigA, sigB, gt, gate_S=None):
    """Top-1 accuracy for the descriptor alone and for descriptor+each relation type.

    The rank key is lexicographic — (rounded descriptor distance, signature distance) —
    so only the TIE SET at the minimal descriptor distance can be re-ordered by a
    relation.  Computing the tie set once and re-ranking it per type is exactly
    equivalent to the naive loop and is what makes the ablation affordable.
    """
    import numpy as np
    S = layerA.S or 1.0
    A = np.array([o["desc"]["vec"] for o in layerA.objects], dtype=np.float32)
    B = np.array([o["desc"]["vec"] for o in layerB.objects], dtype=np.float32)
    cxA = np.array([o["cx"] for o in layerA.objects], dtype=np.float32)
    cyA = np.array([o["cy"] for o in layerA.objects], dtype=np.float32)
    cxB = np.array([o["cx"] for o in layerB.objects], dtype=np.float32)
    cyB = np.array([o["cy"] for o in layerB.objects], dtype=np.float32)
    keys = ["desc_only"] + list(R.REL_TYPES) + ["ALL"]
    ok = {k: 0 for k in keys}
    tot = 0
    ties = 0
    for ia in range(len(layerA.objects)):
        ib_true = gt[ia]
        if ib_true < 0:
            continue
        tot += 1
        d = np.abs(B - A[ia]).sum(axis=1)
        if gate_S is not None:
            bad = (np.abs(cxB - cxA[ia]) > gate_S * S) | (np.abs(cyB - cyA[ia]) > gate_S * S)
            d = np.where(bad, np.inf, d)
        dr = np.round(d, 3)
        dmin = dr.min()
        if not np.isfinite(dmin):
            continue
        tie = np.flatnonzero(dr == dmin)
        ties += len(tie)
        ok["desc_only"] += int(tie[0] == ib_true)
        if len(tie) == 1:
            for k in keys[1:]:
                ok[k] += int(tie[0] == ib_true)
            continue
        for k in keys[1:]:
            types = R.REL_TYPES if k == "ALL" else [k]
            best, bi = None, None
            for ib in tie:
                sd = sum(sig_dist(sigA[ia].get(t, []), sigB[int(ib)].get(t, []))
                         for t in types)
                if best is None or sd < best:
                    best, bi = sd, int(ib)
            ok[k] += int(bi == ib_true)
    return {"n": tot, "mean_tie": round(ties / tot, 3) if tot else None,
            "top1": {k: (round(v / tot, 4) if tot else None) for k, v in ok.items()}}


def one_carrier(rec):
    row = {"carrier": rec, "runs": [], "error": None}
    try:
        pb = C.G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        ex = C.G.extract(pb)
        if not ex.segments:
            row["error"] = "no geometry"; return row
        L0 = C.O.build_objects(ex)
        if len(L0.objects) > MAX_OBJ or len(L0.objects) < 4:
            row["error"] = f"n_obj {len(L0.objects)} outside [4,{MAX_OBJ}]"; return row
        rel0 = R.build_relations(L0, ex)
        sig0 = signatures(L0, rel0, ex.texts)
        row["n_obj"] = len(L0.objects)
        row["n_seg"] = len(ex.segments)
        # how ambiguous is this block for the descriptor alone?
        codes: dict = {}
        for o in L0.objects:
            codes[motif_code(o)] = codes.get(motif_code(o), 0) + 1
        row["dup_motif_share"] = round(
            sum(v for v in codes.values() if v > 1) / max(len(L0.objects), 1), 4)
        for cf_id in REWRITES:
            try:
                ex2, man = CF.apply(ex, L0, cf_id)
            except Exception as e:
                row["runs"].append({"cf": cf_id, "skipped": repr(e)[:100]}); continue
            L2 = C.O.build_objects(ex2, S_override=L0.S)
            rel2 = R.build_relations(L2, ex2)
            sig2 = signatures(L2, rel2, ex2.texts)
            a2b, b2a, ov = C.match_by_provenance(L0, ex.segments, L2, ex2.segments)
            res = {"cf": cf_id, "n_obj_b": len(L2.objects),
                   "gt_pairs": sum(1 for v in a2b if v >= 0), "abl": {}}
            for regime, gate in (("noposition", None), ("position", 3.0)):
                res["abl"][regime] = rank_all(L0, L2, sig0, sig2, a2b, gate_S=gate)
            row["runs"].append(res)
    except Exception:
        row["error"] = traceback.format_exc()[-400:]
    return row


def main(shard, n):
    carriers = [r for i, r in enumerate(CB.pick_carriers()) if i % n == shard]
    out = []
    for k, rec in enumerate(carriers):
        t0 = time.time()
        r = one_carrier(rec)
        print(f"[{shard}] {k+1}/{len(carriers)} {rec['block_id']} "
              f"{r.get('n_obj')} obj {round(time.time()-t0,1)}s err={r['error'] is not None}",
              flush=True)
        out.append(r)
        C.F.clear_caches()
    json.dump(out, open(C.ART / f"rel_r2_{shard}.json", "w", encoding="utf-8"),
              ensure_ascii=False)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 0,
         int(sys.argv[2]) if len(sys.argv) > 2 else 1)
