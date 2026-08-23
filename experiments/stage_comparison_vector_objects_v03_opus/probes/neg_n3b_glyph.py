# -*- coding: utf-8 -*-
"""N3b — how good can a DETERMINISTIC 'this mark is a letter contour' test get?

`neg_n3_curves.py` measured one detector (glyph runs).  Its precision was high and
its recall was not, and the reason is mechanical: `gs -dNoOutputFonts` emits several
closed sub-contours per glyph (the bowl of an 'o', the dot of an 'i', an accent), and
only the outer ones line up into a baseline run.  So this file measures the natural
repair — after the runs are found, absorb every small mark that lies INSIDE a run's
band — and reports precision/recall for each variant against the same ground truth.

No compare is run here: this is the detector alone, which is what the task asks to
measure.  Ground truth = objects whose bbox is covered by a text line's box on the
pre-conversion side (the text layer is known exactly, because we converted it).
"""
from __future__ import annotations
import json, math, sys, time, traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N          # noqa: E402
import neg_glyph as GL          # noqa: E402
import neg_n3_curves as N3      # noqa: E402
import grp_common as G          # noqa: E402
import v03_counterfactual as CF # noqa: E402
import v03_objects as O         # noqa: E402

VARIANTS = {
    "run": {},
    "run_closed": {"closed_only": True},
    "run_absorb": {"absorb": True},
    "run_absorb_closed": {"absorb": True, "closed_only": True},
    "run_absorb_pad60": {"absorb": True, "absorb_pad": 0.60},
    "size_only": None,           # every small object -> the naive baseline
}


def size_flags(layer, frame, frac=0.02):
    x0, y0, x1, y1 = frame
    d = math.hypot(x1 - x0, y1 - y0)
    out = set()
    for i, o in enumerate(layer.objects):
        bb = o["bbox"]
        if max(bb[2] - bb[0], bb[3] - bb[1]) <= frac * d:
            out.add(i)
    return out


def detectors(layer, frame):
    res = {}
    for name, prm in VARIANTS.items():
        if prm is None:
            res[name] = (size_flags(layer, frame), {})
        else:
            res[name] = GL.glyph_flags(layer, frame, **prm)
    return res


def run(shard=0, of=1):
    t0 = time.time()
    cf_rows, real_rows, skips = [], [], []
    for i, c in enumerate(N.carriers()):
        if i % of != shard:
            continue
        key = N.carrier_key(c)
        try:
            ex = N.carrier_extract(c)
        except Exception as e:
            skips.append({"carrier": key, "reason": f"extract {e}"}); continue
        if not ex.texts:
            skips.append({"carrier": key, "reason": "no text layer"}); continue
        la = O.build_objects(ex)
        try:
            ex9, man = CF.apply(ex, la, "D9_text_to_curves", key=key)
        except Exception as e:
            skips.append({"carrier": key, "reason": f"D9 {e}"}); continue
        l9 = O.build_objects(ex9)
        gt, owner = N3.ground_truth(l9, ex.texts)
        fr = N._frame(ex9)
        row = {"carrier": key, "discipline": c["discipline"], "cls": c["cls"],
               "n_obj_before": len(la.objects), "n_obj_after": len(l9.objects),
               "obj_ratio": round(len(l9.objects) / max(1, len(la.objects)), 3),
               "n_gt": len(gt),
               "gt_share": round(len(gt) / max(1, len(l9.objects)), 4),
               "det": {}}
        for name, (det, diag) in detectors(l9, fr).items():
            row["det"][name] = {**N3.prf(det, gt),
                                "n_absorbed": diag.get("n_absorbed"),
                                "n_by_run": diag.get("n_by_run")}
        # what the false positives of the best variant cost in INK, not in count
        tot = sum(o["seg_len"] for o in l9.objects) or 1e-9
        det, _ = GL.glyph_flags(l9, fr, absorb=True)
        row["absorb_ink"] = {
            "flagged_ink_share": round(sum(l9.objects[i]["seg_len"] for i in det) / tot, 4),
            "gt_ink_share": round(sum(l9.objects[i]["seg_len"] for i in gt) / tot, 4),
            "fp_ink_share": round(sum(l9.objects[i]["seg_len"] for i in (det - gt)) / tot, 5),
            "fn_ink_share": round(sum(l9.objects[i]["seg_len"] for i in (gt - det)) / tot, 5)}
        cf_rows.append(row)
        print(f"[cf {i+1}] {key} gt={len(gt)}/{len(l9.objects)}", flush=True)

    real = [r for r in G.block_records() if r.get("cls") == "curved_text"]
    for j, r in enumerate(real):
        if j % of != shard:
            continue
        k = f"{r['doc_id']}|{r['version']}|{r['block_id']}"
        try:
            pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
            if pb is None:
                skips.append({"block": k, "reason": "no prepared block"}); continue
            ex = G.extract(pb)
        except Exception as e:
            skips.append({"block": k, "reason": f"extract {e}"}); continue
        lay = O.build_objects(ex)
        fr = N._frame(ex)
        tot = sum(o["seg_len"] for o in lay.objects) or 1e-9
        rr = {"block": k, "discipline": r["discipline"], "n_seg": len(ex.segments),
              "n_text": len(ex.texts), "n_obj": len(lay.objects), "det": {}}
        for name, (det, diag) in detectors(lay, fr).items():
            rr["det"][name] = {"n_flagged": len(det),
                               "share": round(len(det) / max(1, len(lay.objects)), 4),
                               "ink_share": round(sum(lay.objects[i]["seg_len"] for i in det) / tot, 4),
                               "n_absorbed": diag.get("n_absorbed")}
        real_rows.append(rr)
        print(f"[real {j+1}] {k} obj={len(lay.objects)}", flush=True)

    name = "neg_n3b_glyph.json" if of == 1 else f"neg_runs/neg_n3b_{shard}of{of}.json"
    N.dump(name, {"schema": "neg-n3b-1", "cf": cf_rows, "real": real_rows,
                  "skips": skips, "variants": list(VARIANTS),
                  "sec": round(time.time() - t0, 1)})


if __name__ == "__main__":
    a = sys.argv[1:]
    run(int(a[0]) if a else 0, int(a[1]) if len(a) > 1 else 1)
