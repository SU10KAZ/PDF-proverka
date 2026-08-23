# -*- coding: utf-8 -*-
"""L3 [CF] — WHAT IS ACHIEVABLE WITHOUT ANY LABEL.

Blocks are split by the L1 census into three populations by the share of objects that
carry a UNIQUE designation within 1.6*S:
    none    0 %          (no text anchor exists at all)
    low     0 < x < 10 %
    some    >= 10 %

On each block the local-change counterfactuals C1 / C2 / C3 are applied, the ledger is
produced from the geometry+position matcher (and, where labels exist, from the
geometry+position+label matcher) and scored against the manifest bbox:

    hit          — the ledger names an object inside the manifest change bbox
    on_target    — ledger entries inside that bbox
    spurious     — ledger entries anywhere else  (false ADDED/REMOVED/MOVED)

Usage: lbl_l3_nolabel.py [workers]
"""
from __future__ import annotations
import json, math, sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lbl_common as L
import grp_common as G

MAX_SEG = 60000
MAX_OBJ = 4000
PLAN = [("C1_remove_object", {"bucket": "tiny"}), ("C1_remove_object", {"bucket": "small"}),
        ("C1_remove_object", {"bucket": "large"}),
        ("C2_add_object", {"bucket": "small"}),
        ("C3_move_object", {"bucket": "small", "frac": 0.005}),
        ("C3_move_object", {"bucket": "small", "frac": 0.02}),
        ("C6_reshape_object", {"bucket": "small"})]


def _inside(bb, box, pad):
    cx, cy = (bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2
    return (box[0] - pad <= cx <= box[2] + pad) and (box[1] - pad <= cy <= box[3] + pad)


def score_ledger(v, LA, LB, box, S):
    pad = 2.0 * S
    on = sp = 0
    for ia in v["entries"]["REMOVED_OBJECT"]:
        if _inside(LA.objects[ia]["bbox"], box, pad):
            on += 1
        else:
            sp += 1
    for ib in v["entries"]["ADDED_OBJECT"]:
        if _inside(LB.objects[ib]["bbox"], box, pad):
            on += 1
        else:
            sp += 1
    for ia, ib, _d in v["entries"]["MOVED_OBJECT"]:
        if _inside(LA.objects[ia]["bbox"], box, pad) or _inside(LB.objects[ib]["bbox"], box, pad):
            on += 1
        else:
            sp += 1
    return {"on_target": on, "spurious": sp, "hit": on > 0,
            "n_entries": on + sp}


def run_carrier(rec):
    import v03_counterfactual as C
    out = {"carrier": {k: rec[k] for k in ("doc_id", "version", "block_id",
                                           "discipline", "cls", "bucket", "n_seg")},
           "rows": []}
    try:
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        ex = G.extract(pb)
        if not ex.segments or len(ex.segments) > MAX_SEG:
            out["error"] = f"segments={len(ex.segments)}"; return out
        LA = G.layer_of(ex.segments, ex.texts)
        if not LA.objects or len(LA.objects) > MAX_OBJ:
            out["error"] = f"objects={len(LA.objects)}"; return out
        lab = L.object_labels(LA, ex.texts)
        uq = sum(1 for m, u in lab if m and u) / len(LA.objects)
        pop = "none" if uq == 0 else ("low" if uq < 0.10 else "some")
        out["side_a"] = {"n_obj": len(LA.objects), "n_text": len(ex.texts),
                         "unique_label_share": round(uq, 4), "population": pop,
                         "S": round(LA.S, 3)}
        for cf_id, kw in PLAN:
            tag = cf_id + "@" + "@".join(str(v) for v in kw.values())
            row = {"cf": tag}
            try:
                ex2, man = C.apply(ex, LA, cf_id, **kw)
            except C.CFNotApplicable as e:
                row["skip"] = str(e); out["rows"].append(row); continue
            box = man.get("change_bbox_pt")
            if not box:
                row["skip"] = "no change bbox"; out["rows"].append(row); continue
            LA2 = G.layer_of(ex.segments, ex.texts, S_override=LA.S)
            LB = G.layer_of(ex2.segments, ex2.texts, S_override=LA.S)
            lab_a = L.object_labels(LA2, ex.texts)
            lab_b = L.object_labels(LB, ex2.texts)
            to = (man.get("touched_objects") or [{}])[0]
            row.update({"area_frac": to.get("area_frac_of_block"),
                        "obj_n_seg": to.get("n_seg"), "obj_cls": to.get("cls"),
                        "n_obj_a": len(LA2.objects), "n_obj_b": len(LB.objects)})
            for mode in ("geom_pos", "geom_pos_label"):
                m = L.match_objects(LA2, LB, mode, LA.S, labels_a=lab_a, labels_b=lab_b)
                v = L.verdict(LA2, LB, m["pairs"], LA.S)
                row[mode] = score_ledger(v, LA2, LB, box, LA.S)
                row[mode]["verdict"] = v["verdict"]
            out["rows"].append(row)
    except Exception as exc:
        import traceback
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["tb"] = traceback.format_exc()[-600:]
    finally:
        G.F._DRAW_CACHE.clear(); G.F.clear_caches()
    return out


def main():
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    import cf_build_set as CB
    carriers = CB.pick_carriers()
    print(len(carriers), "carriers", flush=True)
    res = []
    with ProcessPoolExecutor(max_workers=workers) as exe:
        futs = [exe.submit(run_carrier, c) for c in carriers]
        for i, f in enumerate(as_completed(futs)):
            r = f.result(); res.append(r)
            print(f"  {i+1}/{len(carriers)} {r.get('error') or r['side_a']['population']}",
                  flush=True)
    json.dump({"plan": [p[0] + "@" + "@".join(str(v) for v in p[1].values()) for p in PLAN],
               "carriers": res},
              open(L.ART / "lbl_l3_nolabel.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("done")


if __name__ == "__main__":
    main()
