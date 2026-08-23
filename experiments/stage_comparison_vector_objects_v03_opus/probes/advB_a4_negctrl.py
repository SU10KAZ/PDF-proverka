# -*- coding: utf-8 -*-
"""advB attack #4 — REAL pairs where the picture did NOT change and the object layer
says it did (and the reverse).

Two populations, both taken from the screening of the whole corpus
(`mine_align2.jsonl`, 3 940 matched pairs of prepared blocks):

  QUIET  — registered raster difference == 0 at equal physical scale
  LOUD   — registered raster difference >= 2 %

For every sampled pair this probe re-renders both sides ITSELF (independent raster
arbiter) and then measures four object-layer statements and one ink statement.
"""
from __future__ import annotations
import json, math, os, random, sys, time
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import grp_match as M
import loc_common as L
import v03_objects as O
import v03_counterfactual as CF

SEED = 20260823
ART = G.ART


def side(pdf, page_index, coords, page_px):
    return G.F.extract_block(str(G.ROOT / pdf), page_index, coords, page_px[0], page_px[1])


def run_pair(d, pop):
    exA = side(d["pdf_a"], d["page_index_a"], d["coords_a"], d["page_px_a"])
    exB = side(d["pdf_b"], d["page_index_b"], d["coords_b"], d["page_px_b"])
    if not exA.segments or not exB.segments:
        return {"skip": "no_vector"}
    clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
    base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
    dx, dy, score = M.register(exA.segments, exB.segments, {(0.0, 0.0), base})
    off = (dx, dy)
    # --- independent raster arbiter: both sides drawn in ONE frame ------------------
    fr = [clipA[0], clipA[1], clipA[2], clipA[3]]
    exB_sh = CF._clone(exB, segments=[{**s,
                                       "p0": (s["p0"][0] + dx, s["p0"][1] + dy),
                                       "p1": (s["p1"][0] + dx, s["p1"][1] + dy)}
                                      for s in exB.segments], frame=exA.frame)
    pa = CF.render_extract(exA, frame=fr, target_px=1100, draw_text=False)
    pb = CF.render_extract(exB_sh, frame=fr, target_px=1100, draw_text=False)
    iou = CF.ink_iou(pa, pb)
    # --- ink correspondence (segment level, no objects) -----------------------------
    fa, mla, tla = L.unmatched_mask(exA.segments, exB.segments, off, 0.8)
    fb, mlb, tlb = L.unmatched_mask(exB.segments, exA.segments, (-dx, -dy), 0.8)
    unmatched = (tla - mla) + (tlb - mlb)
    # --- object layer, SHARED characteristic scale ----------------------------------
    LA, LB, meta = L.layers(exA, exB)
    ids_a, ids_b = Counter(o["object_id"] for o in LA.objects), Counter(o["object_id"] for o in LB.objects)
    mism = sum((ids_a - ids_b).values()) + sum((ids_b - ids_a).values())
    rows = M.churn_rows(LA, exA.segments, LB, exB.segments, off, tol=0.8)
    cls = M.classify(rows)
    return {
        "pop": pop, "doc_id": d["doc_id"], "discipline": d["discipline"],
        "ver": [d["ver_a"], d["ver_b"]], "block_a": d["block_a"], "block_b": d["block_b"],
        "cat_a": d.get("cat_a"), "screen_diff": (d.get("align2") or {}).get("diff_frac_block"),
        "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
        "off": [round(dx, 3), round(dy, 3)], "reg_score": round(score, 4),
        "my_ink_iou": round(iou, 6),
        "unmatched_len": round(unmatched, 3),
        "unmatched_share": round(unmatched / max(tla + tlb, 1e-9), 8),
        "S_a": round(meta["S_a"], 3), "S_b": round(meta["S_b"], 3),
        "S_shared": round(meta["S_shared"], 3),
        "src_a": meta["src_a"], "src_b": meta["src_b"],
        "n_obj_a": len(LA.objects), "n_obj_b": len(LB.objects),
        "d_obj": len(LB.objects) - len(LA.objects),
        "objid_mismatch": mism,
        "objid_mismatch_share": round(mism / max(1, len(LA.objects) + len(LB.objects)), 4),
        "churn_1to1": round(cls.get("one_to_one", 0.0), 5),
        "cls_a": LA.counts(), "cls_b": LB.counts(),
    }


def main():
    shard = int(sys.argv[1]); nsh = int(sys.argv[2])
    quiet, loud = [], []
    for line in open(ART / "mine_align2.jsonl", encoding="utf-8"):
        d = json.loads(line)
        a2 = d.get("align2") or {}
        df = a2.get("diff_frac_block")
        if df is None or d.get("same_pdf"):
            continue
        if df == 0.0:
            quiet.append(d)
        elif df >= 0.02:
            loud.append(d)
    rng = random.Random(SEED)
    rng.shuffle(quiet); rng.shuffle(loud)
    quiet = [d for d in quiet if max(d["ink_a"], d["ink_b"]) > 0][:120]
    loud = loud[:120]
    tasks = [(d, "QUIET") for d in quiet] + [(d, "LOUD") for d in loud]
    tasks = [t for i, t in enumerate(tasks) if i % nsh == shard]
    outp = ART / f"advB/neg_{shard}.jsonl"
    outp.parent.mkdir(exist_ok=True, parents=True)
    with open(outp, "w", encoding="utf-8") as fh:
        for k, (d, pop) in enumerate(tasks):
            t0 = time.time()
            try:
                r = run_pair(d, pop)
            except Exception as e:
                r = {"pop": pop, "doc_id": d["doc_id"], "block_a": d["block_a"], "error": repr(e)}
            r["sec"] = round(time.time() - t0, 1)
            fh.write(json.dumps(r, ensure_ascii=False) + "\n"); fh.flush()
            print(f"[{shard}] {k+1}/{len(tasks)} {pop} {r.get('d_obj')} {r.get('sec')}s", flush=True)


if __name__ == "__main__":
    main()
