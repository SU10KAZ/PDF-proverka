# -*- coding: utf-8 -*-
"""F7 — can a FALSE "N -> M" row be told from a TRUE one before it is published?

On the same real carriers we generate rows that are false by construction (class A
rewrite: the ink is identical, so every cardinality row is a lie) and rows that are
true by construction (C1 removes / C2 duplicates exactly one object).  Each row gets
the same features, so a gate can be fitted and its cost in recall measured.

Features per row (all computable at publish time, none of them ground truth):
    n           family size, max(n_a, n_b)
    delta       |n_b - n_a|
    rel         delta / n
    margin      L1 distance from this family's centroid to the nearest OTHER family
                centroid of the same class and size band  (small margin = the two
                families are neighbours and a member can drift across)
    radius      max L1 distance of a member from its own centroid (cluster tightness)
    slack       margin - 2*radius  (how far the family is from touching its neighbour)
    n_seg_med   median segment count of the members (1-2 = ruling / hatch strokes)
    ink_share   share of the block's ink in the family

Also sweeps eps_desc, size_tol and min_family, because a threshold that was not
swept is a threshold that was tuned.
Usage: fam_f7_gate.py [n_carriers] [out.json]
"""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import fam_common as C
import fam_family as FAM
import v03_counterfactual as CF
import cf_build_set as CB

FALSE_REWRITES = ["A4b_circle_to_chords5", "A6_round_0.1", "A6_round_0.25", "A1_path_split"]
EPS_SWEEP = [0.10, 0.18, 0.25, 0.35, 0.50]
STOL_SWEEP = [0.04, 0.08, 0.12, 0.20, 0.35]
FLOOR_SWEEP = [0.0, 0.5, 1.0, 2.0, 4.0, 8.0]


def _margins(FP):
    """nearest OTHER family centroid, same class band and size band."""
    import math
    fams = FP.families
    out = []
    for i, f in enumerate(fams):
        best = float("inf")
        li = math.log(max(f["diag_med"], 1e-6))
        for j, g in enumerate(fams):
            if i == j or g["class_key"] != f["class_key"]:
                continue
            lj = math.log(max(g["diag_med"], 1e-6))
            if abs(li - lj) > FP.params["size_tol"]:
                continue
            d = sum(abs(a - b) for a, b in zip(f["vec"], g["vec"]))
            if d < best:
                best = d
        out.append(best)
    return out


def _rowfeat(FP, margins, fi, block_ink):
    f = FP.families[fi]
    na, nb = f["n_a"], f["n_b"]
    n = max(na, nb)
    m = margins[fi]
    r = f["radius_max"]
    return {"n": n, "n_a": na, "n_b": nb, "delta": abs(nb - na),
            "rel": round(abs(nb - na) / max(n, 1), 4),
            "margin": round(m, 5) if m != float("inf") else None,
            "radius": round(r, 5), "slack": (round(m - 2 * r, 5) if m != float("inf") else None),
            "n_seg_med": f["n_seg_med"], "cls": f["cls"],
            "diag_med": f["diag_med"],
            "ink_share": round(f["seg_len_sum"] / max(block_ink, 1e-9), 5)}


def one_carrier(b):
    import random
    rows = []
    pb = G.prepared_block(b["doc_id"], b["version"], b["block_id"])
    if pb is None:
        return rows
    ex = G.extract(pb)
    if not ex.segments:
        return rows
    LA = G.layer_of(ex.segments, ex.texts)
    ink = sum(o["seg_len"] for o in LA.objects) or 1.0
    base = {"block_id": b["block_id"], "discipline": b["discipline"], "cls": b["cls"],
            "bucket": b["bucket"], "n_seg": len(ex.segments), "n_obj": len(LA.objects)}

    # ---------- FALSE rows: class A rewrites of the same ink -------------------
    for name in FALSE_REWRITES:
        segsB = C.REWRITES[name](ex.segments, random.Random(20260823))
        LB = G.layer_of(segsB, ex.texts, S_override=LA.S)
        FP = FAM.build_families_pair(LA, LB)
        mg = _margins(FP)
        for r in FAM.family_deltas(FP, min_family=2):
            rows.append(dict(base, label="FALSE", source=name,
                             **_rowfeat(FP, mg, r["family"], ink)))
        rows.append(dict(base, label="_TOTALS", source=name,
                         n_families=len(FP.families),
                         n_plain_rows=len(FAM.family_deltas(FP, 2)),
                         n_repeated=sum(1 for f in FP.families if len(f["members"]) >= 2)))
        for lf in (2.0, 4.0, 8.0):
            rows.append(dict(base, label="_SWEEP", source=name, knob="link_frac", value=lf,
                             false_rows=len(FAM.family_deltas_robust(FP, 2, link_frac=lf)),
                             n_repeated=sum(1 for f in FP.families if len(f["members"]) >= 2),
                             n_families=len(FP.families)))
        # ---- parameter sweeps, false-row count only
        if name == "A6_round_0.25":
            for e in EPS_SWEEP:
                FPs = FAM.build_families_pair(LA, LB, eps_desc=e)
                rows.append(dict(base, label="_SWEEP", source=name, knob="eps_desc", value=e,
                                 false_rows=len(FAM.family_deltas(FPs, 2)),
                                 n_repeated=sum(1 for f in FPs.families if len(f["members"]) >= 2),
                                 n_families=len(FPs.families)))
            for s in STOL_SWEEP:
                FPs = FAM.build_families_pair(LA, LB, size_tol=s)
                rows.append(dict(base, label="_SWEEP", source=name, knob="size_tol", value=s,
                                 false_rows=len(FAM.family_deltas(FPs, 2)),
                                 n_repeated=sum(1 for f in FPs.families if len(f["members"]) >= 2),
                                 n_families=len(FPs.families)))
            for s in FLOOR_SWEEP:
                FPs = FAM.build_families_pair(LA, LB, min_diag_pt=s)
                rows.append(dict(base, label="_SWEEP", source=name, knob="min_diag_pt", value=s,
                                 false_rows=len(FAM.family_deltas(FPs, 2)),
                                 n_repeated=sum(1 for f in FPs.families if len(f["members"]) >= 2),
                                 n_below=FPs.stats["n_below_floor"],
                                 n_families=len(FPs.families)))

    # ---------- TRUE rows: C1 / C2 -------------------------------------------
    for cf_id in ("C1_remove_object", "C2_add_object"):
        for bucket in ("tiny", "small", "large", None):
            try:
                ex2, man = CF.apply(ex, LA, cf_id, **({"bucket": bucket} if bucket else {}))
            except CF.CFNotApplicable:
                continue
            except Exception:
                continue
            LB = G.layer_of(ex2.segments, ex2.texts, S_override=LA.S)
            FP = FAM.build_families_pair(LA, LB)
            mg = _margins(FP)
            oid = man["touched_objects"][0]["object_id"]
            ai = next((i for i, o in enumerate(LA.objects) if o["object_id"] == oid), None)
            if ai is None:
                continue
            fi = FP.obj_family[ai]
            want = -1 if cf_id.startswith("C1") else 1
            got = FP.families[fi]["n_b"] - FP.families[fi]["n_a"]
            ft = _rowfeat(FP, mg, fi, ink)
            rob = {}
            for lf in (2.0, 4.0, 8.0):
                sf = FAM.super_family_of(FP, ai, link_frac=lf)
                rr = FAM.family_deltas_robust(FP, 2, link_frac=lf)
                if sf is None:
                    rob[str(lf)] = {"verdict": "silent", "rows": len(rr)}
                else:
                    sna, snb, nfam, members = sf
                    rob[str(lf)] = {"n_a": sna, "n_b": snb, "delta": snb - sna, "n_fam": nfam,
                                    "verdict": ("silent" if max(sna, snb) < 2 else
                                                "hit" if snb - sna == want else "wrong"),
                                    "rows": len(rr),
                                    "extra_rows": sum(1 for r in rr if fi not in r["families"])}
            rows.append(dict(base, label="TRUE", source=f"{cf_id}@{bucket or 'any'}",
                             correct=bool(got == want), got=got, want=want, robust=rob,
                             obj_area_frac=man["touched_objects"][0]["area_frac_of_block"],
                             obj_n_seg=man["touched_objects"][0]["n_seg"], **ft))
            for r in FAM.family_deltas(FP, min_family=2):
                if r["family"] == fi:
                    continue
                rows.append(dict(base, label="FALSE", source=f"{cf_id}@{bucket or 'any'}_extra",
                                 **_rowfeat(FP, mg, r["family"], ink)))
    return rows


def _work(b):
    try:
        return one_carrier(b)
    except Exception:
        return [{"block_id": b["block_id"], "error": traceback.format_exc().splitlines()[-1]}]


def main():
    import multiprocessing as mp
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    out = sys.argv[2] if len(sys.argv) > 2 else str(G.ART / "fam_f7_gate.json")
    carriers = CB.pick_carriers()[:n]
    print("carriers", len(carriers), flush=True)
    rows = []
    done = 0
    with mp.Pool(8, maxtasksperchild=2) as pool:
        for got in pool.imap_unordered(_work, carriers, chunksize=1):
            rows.extend(got)
            done += 1
            print(done, len(rows), flush=True)
    json.dump({"n_carriers": len(carriers), "false_rewrites": FALSE_REWRITES,
               "eps_sweep": EPS_SWEEP, "stol_sweep": STOL_SWEEP, "floor_sweep": FLOOR_SWEEP, "rows": rows},
              open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
