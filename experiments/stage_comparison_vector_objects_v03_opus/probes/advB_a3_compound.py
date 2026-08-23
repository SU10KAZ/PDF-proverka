# -*- coding: utf-8 -*-
"""advB attack #3 — are counterfactuals systematically EASIER than real edits?

A real CAD revision moves several things at once: the frame of the prepared block,
the packaging of the paths, the text, and the geometry.  Every counterfactual of the
track moves exactly ONE.  This probe removes the SAME object twice from the same
carrier:

  arm SINGLE   — C1 only                     (the track's condition)
  arm COMPOUND — crop jitter 0.5 % + C1 + coordinate rounding 0.25 pt + text edit

and reports recall / localisation / false records for both.
"""
from __future__ import annotations
import json, math, os, random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import grp_match as M
import loc_common as L
import v03_objects as O
import v03_counterfactual as CF

SEED = 20260823


def _bbox_inter(a, b):
    return (max(0.0, min(a[2], b[2]) - max(a[0], b[0])) *
            max(0.0, min(a[3], b[3]) - max(a[1], b[1])))


def evaluate(exA, exB, change_bbox, pad=0.0):
    clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
    base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
    seeds = {(0.0, 0.0), base}
    dx, dy, score = M.register(exA.segments, exB.segments, seeds)
    LA, LB, meta = L.layers(exA, exB)
    led = L.ledger(exA, exB, off=(dx, dy), LA=LA, LB=LB, meta=meta)
    S = meta["S_shared"]
    thr = max(2 * S, 3.0)
    recs = [r for r in led["records"] if r["change_len"] >= thr and not r["at_boundary"]]
    hit = [r for r in recs if _bbox_inter(r["bbox_pt"], change_bbox) > 0]
    return {"off": [round(dx, 3), round(dy, 3)], "reg_score": round(score, 4),
            "S": round(S, 3), "thr": round(thr, 3),
            "n_rec": len(recs), "n_hit": len(hit), "n_false": len(recs) - len(hit),
            "localised": bool(hit),
            "named": bool(hit and (hit[0].get("objects_a") or hit[0].get("objects_b"))),
            "top_len": round(recs[0]["change_len"], 2) if recs else 0.0,
            "unmatched_share": round(1 - led["scalar"]["ink_similarity"], 6)}


def key_of(s):
    return (round(s["p0"][0], 3), round(s["p0"][1], 3), round(s["p1"][0], 3), round(s["p1"][1], 3))


def round_segments(segs, q):
    out = []
    for s in segs:
        t = dict(s)
        t["p0"] = (round(s["p0"][0] / q) * q, round(s["p0"][1] / q) * q)
        t["p1"] = (round(s["p1"][0] / q) * q, round(s["p1"][1] / q) * q)
        t["len"] = math.hypot(t["p1"][0] - t["p0"][0], t["p1"][1] - t["p0"][1])
        if t["len"] > 1e-9:
            out.append(t)
    for k, s in enumerate(out):
        s["i"] = k
    return out


def edit_texts(texts, rng, k=5):
    out = [dict(t) for t in texts]
    ix = [i for i in range(len(out)) if (out[i].get("text") or "").strip()]
    rng.shuffle(ix)
    for i in ix[:k]:
        out[i]["text"] = (out[i]["text"] or "") + "X"
    return out


def run_block(rec, bucket):
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return None
    exA = G.extract(pb)
    if len(exA.segments) < 50:
        return None
    objs = O.build_objects(exA)
    ex_single, man = CF.apply(exA, objs, "C1_remove_object", bucket=bucket)
    change_bbox = man["change_bbox_pt"]
    removed = set(man["changed_primitives"]["removed_segment_ix"])
    row = {"block_id": rec["block_id"], "discipline": rec["discipline"],
           "bucket": rec["bucket"], "n_seg": len(exA.segments), "obj_bucket": bucket,
           "obj_area_frac": man["touched_objects"][0].get("area_frac"),
           "n_seg_removed": len(removed), "change_bbox": change_bbox}
    row["single"] = evaluate(exA, ex_single, change_bbox)

    # ---- compound arms -----------------------------------------------------------
    # A real revision changes several things at once.  Every arm below adds ONE more
    # axis on top of the previous one, so the cost of each axis is separable.
    import advB_rw as R

    def build(scale_k, jitter, do_round, do_text, do_split, do_remove):
        ex_j = exA
        if jitter:
            ex_j, _ = CF.apply(exA, None, "B3_crop_jitter", frac=jitter)
        segs = ex_j.segments
        if do_remove:
            want = {key_of(exA.segments[i]) for i in removed}
            segs = [s for s in segs if key_of(s) not in want]
        segs = [dict(s) for s in segs]
        for k, s in enumerate(segs):
            s["i"] = k
        if do_split:
            segs = R._split_at([0.37], 0.5)(segs, random.Random(SEED))
        if scale_k and scale_k != 1.0:
            fr = ex_j.frame["clip_display"]
            cx, cy = (fr[0] + fr[2]) / 2, (fr[1] + fr[3]) / 2
            for s in segs:
                s["p0"] = (cx + (s["p0"][0] - cx) * scale_k, cy + (s["p0"][1] - cy) * scale_k)
                s["p1"] = (cx + (s["p1"][0] - cx) * scale_k, cy + (s["p1"][1] - cy) * scale_k)
                s["len"] = math.hypot(s["p1"][0] - s["p0"][0], s["p1"][1] - s["p0"][1])
        if do_round:
            segs = round_segments(segs, 0.25)
        texts = edit_texts(ex_j.texts, random.Random(SEED)) if do_text else ex_j.texts
        return CF._clone(ex_j, segments=segs, texts=texts, prov={"cf": "COMPOUND"})

    arms = {
        "A1_pack":        dict(scale_k=1.0,   jitter=0.005, do_round=True, do_text=True,  do_split=True,  do_remove=True),
        "A2_scale_0.2pct":dict(scale_k=1.002, jitter=0.005, do_round=True, do_text=True,  do_split=True,  do_remove=True),
        "A3_scale_0.5pct":dict(scale_k=1.005, jitter=0.005, do_round=True, do_text=True,  do_split=True,  do_remove=True),
        "A4_scale_1pct":  dict(scale_k=1.01,  jitter=0.005, do_round=True, do_text=True,  do_split=True,  do_remove=True),
        "N3_scale_0.5pct_NOEDIT": dict(scale_k=1.005, jitter=0.005, do_round=True, do_text=True, do_split=True, do_remove=False),
        "N4_scale_1pct_NOEDIT":   dict(scale_k=1.01,  jitter=0.005, do_round=True, do_text=True, do_split=True, do_remove=False),
    }
    for nm, kw in arms.items():
        try:
            exB = build(**kw)
            row[nm] = evaluate(exA, exB, change_bbox)
            row[nm]["expects_change"] = bool(kw["do_remove"])
        except Exception as e:
            row[nm] = {"error": repr(e)}
    return row


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nsh = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for b in sample["blocks"] if 100 <= b["n_seg"] <= 12000]
    if os.environ.get("ADVB_SHUFFLE"):
        random.Random(SEED).shuffle(blocks)
    blocks = [b for i, b in enumerate(blocks) if i % nsh == shard]
    outp = G.ART / f"advB/{os.environ.get('ADVB_TAG','cmp')}_{shard}.jsonl"
    outp.parent.mkdir(exist_ok=True, parents=True)
    with open(outp, "w", encoding="utf-8") as fh:
        for k, rec in enumerate(blocks):
            for bucket in ("tiny", "small", "large"):
                try:
                    r = run_block(rec, bucket)
                except Exception as e:
                    r = {"block_id": rec["block_id"], "obj_bucket": bucket, "error": repr(e)}
                if r:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n"); fh.flush()
            print(f"[{shard}] {k+1}/{len(blocks)} {rec['block_id'][:12]}", flush=True)


if __name__ == "__main__":
    main()
