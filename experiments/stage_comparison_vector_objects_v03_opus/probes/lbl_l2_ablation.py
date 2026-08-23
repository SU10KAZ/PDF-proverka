# -*- coding: utf-8 -*-
"""L2 [CF] — MATCHING ABLATION on controlled counterfactuals, where the object
correspondence is known EXACTLY from segment provenance.

One matcher, three information modes:
    (a) geom            — shape descriptor + size only
    (b) geom_pos        — + position (PDF points, same page frame)
    (c) geom_pos_label  — + text label as an anchor

Two frame conditions, because that is what decides whether position is usable at all:
    registered   — the translation between the two sides is known/compensated
    raw          — it is not (crop boundary moved, block shifted)

Usage: lbl_l2_ablation.py [workers] [out_suffix]
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lbl_common as L
import grp_common as G

MAX_OBJ = 4000
MAX_SEG = 60000

PLAN = [
    ("A1_path_split", {}),
    ("A6_round_0.25", {}),
    ("A4b_circle_to_chords5", {}),
    ("B1_translate", {"frac": 0.02}),
    ("B1_translate", {"frac": 0.10}),
    ("B3_crop_jitter", {"frac": 0.02}),
    ("C1_remove_object", {"bucket": "small"}),
    ("C2_add_object", {"bucket": "small"}),
    ("C3_move_object", {"bucket": "small", "frac": 0.01}),
    ("C6_reshape_object", {"bucket": "small"}),
    ("D1_text_edit", {}),
]
MODES = ("geom", "geom_pos", "geom_pos_label")


def tag_of(cf_id, kw):
    return cf_id + ("@" + "@".join(str(v) for v in kw.values()) if kw else "")


def run_carrier(rec):
    import v03_counterfactual as C
    out = {"carrier": {k: rec[k] for k in ("doc_id", "version", "block_id",
                                           "discipline", "cls", "bucket", "n_seg")},
           "rows": [], "error": None}
    try:
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            out["error"] = "block not found"
            return out
        ex = G.extract(pb)
        if not ex.segments or len(ex.segments) > MAX_SEG:
            out["error"] = f"segments={len(ex.segments)}"
            return out
        LA = G.layer_of(ex.segments, ex.texts)
        if not LA.objects or len(LA.objects) > MAX_OBJ:
            out["error"] = f"objects={len(LA.objects)}"
            return out
        lab_a = L.object_labels(LA, ex.texts)
        n_unique = sum(1 for m, u in lab_a if m and u)
        out["side_a"] = {"n_obj": len(LA.objects), "S": round(LA.S, 3),
                         "scale_source": LA.scale_source, "n_text": len(ex.texts),
                         "n_unique_label_obj": n_unique,
                         "unique_label_share": round(n_unique / len(LA.objects), 4)}
        for cf_id, kw in PLAN:
            tag = tag_of(cf_id, kw)
            row = {"cf": tag}
            t0 = time.time()
            try:
                ex2, man = C.apply(ex, LA, cf_id, **kw)
            except C.CFNotApplicable as e:
                row["skip"] = str(e)
                out["rows"].append(row)
                continue
            # shared characteristic scale (G2-2b: per-side S is the top real failure)
            LB = G.layer_of(ex2.segments, ex2.texts, S_override=LA.S)
            LA2 = G.layer_of(ex.segments, ex.texts, S_override=LA.S)
            lab_a2 = L.object_labels(LA2, ex.texts)
            lab_b = L.object_labels(LB, ex2.texts)
            gt_ab, gt_ba = L.gt_from_provenance(LA2, ex.segments, LB, ex2.segments)
            row.update({"n_obj_a": len(LA2.objects), "n_obj_b": len(LB.objects),
                        "n_gt_pairs": sum(1 for v in gt_ab.values() if v is not None),
                        "true_removed": sum(1 for v in gt_ab.values() if v is None),
                        "true_added": sum(1 for v in gt_ba.values() if v is None),
                        "expected": man.get("expected_verdict"),
                        "t_build": round(time.time() - t0, 2)})
            for cond in ("registered", "raw"):
                off = (0.0, 0.0)
                if cond == "registered":
                    d = man.get("delta") or {}
                    off = (-float(d.get("dx_pt", 0.0)), -float(d.get("dy_pt", 0.0))) \
                        if man.get("cf_class") == "B" else (0.0, 0.0)
                for mode in MODES:
                    m = L.match_objects(LA2, LB, mode, LA.S, labels_a=lab_a2,
                                        labels_b=lab_b, off=off)
                    sc = L.score(m["pairs"], gt_ab, gt_ba, m["na"], m["nb"])
                    acc, ngt = L.top1_acc(m["top1"], gt_ab)
                    sc["top1"] = round(acc, 5) if acc is not None else None
                    row[f"{cond}/{mode}"] = sc
            out["rows"].append(row)
    except Exception as exc:
        import traceback
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["tb"] = traceback.format_exc()[-800:]
    finally:
        G.F._DRAW_CACHE.clear()
        G.F.clear_caches()
        try:
            import v03_counterfactual as C
            C.cleanup_scratch()
        except Exception:
            pass
    return out


def main():
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    suffix = sys.argv[2] if len(sys.argv) > 2 else ""
    import cf_build_set as CB
    carriers = CB.pick_carriers()
    if suffix == "smoke":
        carriers = carriers[:4]
    print(len(carriers), "carriers", flush=True)
    res = []
    with ProcessPoolExecutor(max_workers=workers) as exe:
        futs = [exe.submit(run_carrier, c) for c in carriers]
        for i, f in enumerate(as_completed(futs)):
            r = f.result()
            res.append(r)
            print(f"  {i+1}/{len(carriers)} {r['carrier']['block_id'][:12]} "
                  f"{r.get('error') or len(r['rows'])}", flush=True)
    out = L.ART / f"lbl_l2_ablation{'_'+suffix if suffix else ''}.json"
    json.dump({"plan": [tag_of(*p) for p in PLAN], "modes": list(MODES),
               "match_params": L.DEFAULT_MATCH, "max_obj": MAX_OBJ, "max_seg": MAX_SEG,
               "carriers": res}, open(out, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("->", out)


if __name__ == "__main__":
    main()
