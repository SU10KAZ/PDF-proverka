# -*- coding: utf-8 -*-
"""F4 [CF] — does the family layer catch a TRUE change of cardinality?

C1_remove_object / C2_add_object on real prepared blocks.  The manifest names exactly
which object was removed / duplicated, so "the right delta in the right family" is
checkable without any labelling.

TWO selection modes, and the difference between them is itself the finding:

  any    the counterfactual may touch ANY object of the block (what `cf` does by
         default).  Most objects of a real drawing are unique, so the family layer
         is not entitled to say anything -> verdict `silent`.
  fam    the counterfactual is restricted to members of a REPEATED family (>= 2,
         and separately >= 3).  This is the "12 -> 11" / "12 -> 13" sentence of
         section 14 and the only place where a cardinality claim is legitimate.

Verdicts
    hit      the family holding the touched object reports delta -1 (C1) / +1 (C2)
    silent   that family has < min_family members, so no row is published at all
    wrong    the family reports some other delta
    extra    how many OTHER families claim a change on the same block (false rows)

Usage: fam_f4_true.py [n_carriers] [out.json]
"""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import fam_family as FAM
import v03_counterfactual as CF
import cf_build_set as CB

BUCKETS = ["tiny", "small", "large", None]


class _Proxy:
    """An ObjectLayer-like view over a subset of objects (indices stay original in
    each object's own `segments` list, which is all the C1/C2 code touches)."""
    def __init__(self, layer, objects):
        self.objects = objects
        for a in ("S", "scale_source", "params", "stats", "prims"):
            if hasattr(layer, a):
                setattr(self, a, getattr(layer, a))


def _repeated_members(F, layer, min_family):
    keep = []
    for f in F.families:
        if len(f["members"]) >= min_family:
            keep.extend(f["members"])
    return sorted(set(keep))


def one_carrier(b):
    out = []
    pb = G.prepared_block(b["doc_id"], b["version"], b["block_id"])
    if pb is None:
        return out
    ex = G.extract(pb)
    if not ex.segments:
        return out
    LA = G.layer_of(ex.segments, ex.texts)
    FA = FAM.build_families(LA)
    fam_of = FA.obj_family
    fam_size = [len(f["members"]) for f in FA.families]
    base = {"block_id": b["block_id"], "doc_id": b["doc_id"], "version": b["version"],
            "discipline": b["discipline"], "cls": b["cls"], "bucket": b["bucket"],
            "n_seg": len(ex.segments), "n_obj": len(LA.objects),
            "n_fam": FA.stats["n_families"], "n_rep": FA.stats["n_repeated_families"],
            "share_obj_in_repeated": FA.stats["share_objects_in_repeated"],
            "S": round(LA.S, 3)}

    selections = [("any", LA, BUCKETS)]
    for mf in (2, 3):
        ix = [i for i in range(len(LA.objects)) if fam_size[fam_of[i]] >= mf]
        if ix:
            selections.append((f"fam{mf}", _Proxy(LA, [LA.objects[i] for i in ix]), BUCKETS))

    for sel_name, sel_layer, buckets in selections:
        for cf_id in ("C1_remove_object", "C2_add_object"):
            for bucket in buckets:
                t0 = time.time()
                try:
                    ex2, man = CF.apply(ex, sel_layer, cf_id,
                                        **({"bucket": bucket} if bucket else {}))
                except CF.CFNotApplicable as e:
                    out.append(dict(base, sel=sel_name, cf=cf_id, bucket=bucket or "any",
                                    skip=str(e)[:60]))
                    continue
                except Exception:
                    out.append(dict(base, sel=sel_name, cf=cf_id, bucket=bucket or "any",
                                    error=traceback.format_exc().splitlines()[-1]))
                    continue
                LB = G.layer_of(ex2.segments, ex2.texts, S_override=LA.S)
                FP = FAM.build_families_pair(LA, LB)
                rows = FAM.family_deltas(FP, min_family=2)
                rec = man["touched_objects"][0]
                oid = rec["object_id"]
                ai = next((i for i, o in enumerate(LA.objects) if o["object_id"] == oid), None)
                if ai is None:
                    out.append(dict(base, sel=sel_name, cf=cf_id, bucket=bucket or "any",
                                    error="touched object not found"))
                    continue
                fi = FP.obj_family[ai]
                f = FP.families[fi]
                na, nb = f["n_a"], f["n_b"]
                want = -1 if cf_id == "C1_remove_object" else +1
                got = nb - na
                published = max(na, nb) >= 2
                verdict = ("silent" if not published else "hit" if got == want else "wrong")
                extra = [r for r in rows if r["family"] != fi]
                rob = {}
                for lf in (2.0, 4.0, 8.0):
                    sf = FAM.super_family_of(FP, ai, link_frac=lf)
                    rrows = FAM.family_deltas_robust(FP, min_family=2, link_frac=lf)
                    if sf is None:
                        rob[str(lf)] = {"verdict": "silent", "rows": len(rrows)}
                        continue
                    sna, snb, nfam, members = sf
                    spub = max(sna, snb) >= 2
                    sgot = snb - sna
                    rob[str(lf)] = {
                        "n_a": sna, "n_b": snb, "delta": sgot, "n_fam": nfam,
                        "verdict": ("silent" if not spub else "hit" if sgot == want else "wrong"),
                        "rows": len(rrows),
                        "extra_rows": sum(1 for r in rrows if fi not in r["families"])}
                out.append(dict(base, sel=sel_name, cf=cf_id,
                                bucket=man["params"].get("size_bucket", bucket or "any"),
                                obj_n_seg=rec["n_seg"], obj_cls=rec["cls"],
                                obj_diag=rec["diag_pt"], obj_area_frac=rec["area_frac_of_block"],
                                fam_size_a=fam_size[fam_of[ai]],
                                fam_n_a=na, fam_n_b=nb, fam_delta=got, want=want,
                                verdict=verdict, published=published,
                                n_rows=len(rows), n_extra_rows=len(extra),
                                extra_delta_sum=sum(abs(r["delta"]) for r in extra),
                                rank_of_target=next((k for k, r in enumerate(rows)
                                                     if r["family"] == fi), None),
                                n_obj_b=len(LB.objects), robust=rob,
                                t_sec=round(time.time() - t0, 2)))
    return out


def _work(b):
    try:
        return one_carrier(b)
    except Exception:
        return [{"block_id": b["block_id"], "error": traceback.format_exc().splitlines()[-1]}]


def main():
    import multiprocessing as mp
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    out_path = sys.argv[2] if len(sys.argv) > 2 else str(G.ART / "fam_f4_cf.json")
    carriers = CB.pick_carriers()[:n]
    print("carriers", len(carriers), flush=True)
    rows = []
    with mp.Pool(5, maxtasksperchild=2) as pool:
        for got in pool.imap_unordered(_work, carriers, chunksize=1):
            rows.extend(got)
            print(len(rows), (got[0].get("block_id") or "?")[:12],
                  [r.get("verdict") or (r.get("skip") or r.get("error", ""))[:10] for r in got][:6],
                  flush=True)
    json.dump({"n_carriers": len(carriers), "rows": rows},
              open(out_path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
