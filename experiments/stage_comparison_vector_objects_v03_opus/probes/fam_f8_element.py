# -*- coding: utf-8 -*-
"""F8 — does an ELEMENT gate (F6) actually remove the false "N -> M" rows (F3)
without removing the true ones (F4)?

F6 labelled 68 families by eye: only 10 are repeated ELEMENTS, 17 are table ruling,
5 hatching, 36 plain strokes / dash fragments.  So the family layer, left alone,
mostly talks about background geometry.  Here every published row carries the family
features F6 measured, on BOTH populations, so the gate is evaluated where it matters:

  mode pairs : real quiet pairs (F3 population) -> every row is FALSE
  mode cf    : C1/C2 on a member of a repeated family -> exactly one row is TRUE

Usage: fam_f8_element.py pairs|cf [shard] [of] [out.jsonl]
"""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import grp_match as M
import fam_family as FAM
import fam_f3_false as F3
import fam_f5_scope as F5
import v03_counterfactual as CF
import cf_build_set as CB

MAX_SEG = 60000
PAD = 0.02


def _feats(FP, fi, objects, S, ink):
    f = FP.families[fi]
    return F5.fam_features(f, objects, S, ink)


def row_of(FP, fi, objects, S, ink, sides_known=True):
    f = FP.families[fi]
    ft = _feats(FP, fi, objects, S, ink)
    return {"n_a": f.get("n_a", 0), "n_b": f.get("n_b", 0),
            "delta": f.get("n_b", 0) - f.get("n_a", 0),
            "cls": ft["cls"], "n_seg_med": ft["n_seg_med"], "diag_med": ft["diag_med"],
            "occupied_cells": ft["occupied_cells"], "cycle_share": ft["cycle_share"],
            "dir_concentration": ft["dir_concentration"], "elong": ft["elong"],
            "arc_share": ft["arc_share"], "ink_share": ft["ink_share"],
            "nn_med_over_diag": ft["nn_med_over_diag"], "size_cv": ft["size_cv"]}


def do_pairs(p):
    a, b = p["side_a"], p["side_b"]
    exA = G.F.extract_block(str(G.ROOT / a["pdf"]), a["page_index"], a["coords_px"], *a["page_px"])
    exB = G.F.extract_block(str(G.ROOT / b["pdf"]), b["page_index"], b["coords_px"], *b["page_px"])
    if not exA.segments or not exB.segments:
        return {"pair_id": p["pair_id"], "pop": p["pop"], "error": "no vector"}
    if max(len(exA.segments), len(exB.segments)) > MAX_SEG:
        return {"pair_id": p["pair_id"], "pop": p["pop"], "error": "too big"}
    clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
    dx, dy, sc = M.register(exA.segments, exB.segments,
                            {(0.0, 0.0), (clipA[0] - clipB[0], clipA[1] - clipB[1])})
    regB = (clipB[0] + dx, clipB[1] + dy, clipB[2] + dx, clipB[3] + dy)
    reg = (max(clipA[0], regB[0]), max(clipA[1], regB[1]),
           min(clipA[2], regB[2]), min(clipA[3], regB[3]))
    pad = max(2.0, PAD * min(reg[2] - reg[0], reg[3] - reg[1]))
    L0a = G.layer_of(exA.segments, exA.texts)
    L0b = G.layer_of(exB.segments, exB.texts)
    S = max(L0a.S, L0b.S)
    LA = G.layer_of(exA.segments, exA.texts, S_override=S)
    LB = G.layer_of(exB.segments, exB.texts, S_override=S)
    oa = [o for o in LA.objects if F3._inside(o["bbox"], reg, pad)]
    ob = [o for o in LB.objects
          if F3._inside([o["bbox"][0] + dx, o["bbox"][1] + dy,
                         o["bbox"][2] + dx, o["bbox"][3] + dy], reg, pad)]
    FP = FAM.build_families_pair(oa, ob)
    objs = FP.objects
    ink = sum(o["seg_len"] for o in objs) or 1.0
    rows = [dict(row_of(FP, r["family"], objs, S, ink), label="FALSE")
            for r in FAM.family_deltas(FP, min_family=2)]
    return {"pair_id": p["pair_id"], "pop": p["pop"], "discipline": p["discipline"],
            "classes": p.get("classes"), "n_obj_a": len(oa), "n_obj_b": len(ob),
            "n_repeated": sum(1 for f in FP.families if len(f["members"]) >= 2),
            "rows": rows}


def do_cf(b):
    out = []
    pb = G.prepared_block(b["doc_id"], b["version"], b["block_id"])
    if pb is None:
        return out
    ex = G.extract(pb)
    if not ex.segments or len(ex.segments) > MAX_SEG:
        return out
    LA = G.layer_of(ex.segments, ex.texts)
    FA = FAM.build_families(LA)
    size = [len(f["members"]) for f in FA.families]
    ix = [i for i in range(len(LA.objects))
          if FA.obj_family[i] >= 0 and size[FA.obj_family[i]] >= 2]
    if not ix:
        return out

    class P:
        objects = [LA.objects[i] for i in ix]
        S = LA.S
        scale_source = LA.scale_source
        params = LA.params
        stats = LA.stats
        prims = LA.prims
    for cf_id in ("C1_remove_object", "C2_add_object"):
        for bucket in ("tiny", "small", "large", None):
            try:
                ex2, man = CF.apply(ex, P, cf_id, **({"bucket": bucket} if bucket else {}))
            except Exception:
                continue
            LB = G.layer_of(ex2.segments, ex2.texts, S_override=LA.S)
            FP = FAM.build_families_pair(LA, LB)
            objs = FP.objects
            ink = sum(o["seg_len"] for o in objs) or 1.0
            oid = man["touched_objects"][0]["object_id"]
            ai = next((i for i, o in enumerate(LA.objects) if o["object_id"] == oid), None)
            if ai is None:
                continue
            fi = FP.obj_family[ai]
            if fi < 0:
                continue
            want = -1 if cf_id.startswith("C1") else 1
            tr = row_of(FP, fi, objs, LA.S, ink)
            tr.update(label="TRUE", correct=bool(tr["delta"] == want),
                      block_id=b["block_id"], discipline=b["discipline"],
                      cf=cf_id, bucket=bucket or "any")
            out.append(tr)
            for r in FAM.family_deltas(FP, min_family=2):
                if r["family"] == fi:
                    continue
                fr = row_of(FP, r["family"], objs, LA.S, ink)
                fr.update(label="FALSE", block_id=b["block_id"], discipline=b["discipline"],
                          cf=cf_id, bucket=bucket or "any")
                out.append(fr)
    return out


def main():
    mode = sys.argv[1]
    shard = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    of = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    out = sys.argv[4] if len(sys.argv) > 4 else str(G.ART / f"fam_f8_{mode}_s{shard}.jsonl")
    fh = open(out, "w", encoding="utf-8")
    if mode == "pairs":
        items = [p for i, p in enumerate(F3.load_population()) if i % of == shard]
    else:
        items = [b for i, b in enumerate(CB.pick_carriers()) if i % of == shard]
    print(mode, "shard", shard, "of", of, ":", len(items), flush=True)
    for k, it in enumerate(items):
        t0 = time.time()
        try:
            r = do_pairs(it) if mode == "pairs" else {"rows": do_cf(it),
                                                     "block_id": it["block_id"],
                                                     "discipline": it["discipline"]}
        except Exception:
            r = {"id": it.get("pair_id") or it.get("block_id"),
                 "error": traceback.format_exc().splitlines()[-1]}
        r["t_sec"] = round(time.time() - t0, 1)
        fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        fh.flush()
        print(shard, k, len(items), len(r.get("rows") or []), r.get("error", ""), r["t_sec"], flush=True)
    fh.close()


if __name__ == "__main__":
    main()
