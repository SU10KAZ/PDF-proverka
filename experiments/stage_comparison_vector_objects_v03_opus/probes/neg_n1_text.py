# -*- coding: utf-8 -*-
"""N1 — text-only negative controls.  Expected verdict is always NO_GRAPHIC_CHANGE."""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N          # noqa: E402
import neg_textcf as T          # noqa: E402
import v03_counterfactual as CF # noqa: E402
import v03_objects as O         # noqa: E402

CF_TEXT = ["D1_text_edit", "D2_text_move", "D3_label_rename", "D8_font_swap"]


def run():
    rows = []
    skips = []
    cs = N.carriers()
    for ci, c in enumerate(cs):
        key = N.carrier_key(c)
        try:
            ex = N.carrier_extract(c)
        except Exception as e:
            skips.append({"carrier": key, "cf": "*", "reason": f"extract failed: {e}"})
            continue
        la = O.build_objects(ex)
        base = {"carrier": key, "block_id": c["block_id"], "doc_id": c["doc_id"],
                "version": c["version"], "discipline": c["discipline"], "cls": c["cls"],
                "bucket": c["bucket"], "n_seg": len(ex.segments), "n_text": len(ex.texts),
                "n_obj": len(la.objects), "S": round(la.S, 4),
                "scale_source": la.scale_source}
        jobs = [(cid, {}) for cid in CF_TEXT] + list(T.VARIANTS)
        for cid, prm in jobs:
            t0 = time.time()
            try:
                if cid in CF_TEXT:
                    ex2, man = CF.apply(ex, la, cid, key=key, **prm)
                else:
                    ex2, man = T.apply(ex, cid, key, **prm)
            except CF.CFNotApplicable as e:
                skips.append({"carrier": key, "cf": cid, "params": prm, "reason": str(e)})
                continue
            except Exception as e:
                skips.append({"carrier": key, "cf": cid, "params": prm,
                              "reason": f"ERROR {e}", "tb": traceback.format_exc()[-400:]})
                continue
            gid = N.geometry_identical(ex, ex2)
            try:
                r_sh = N.full_compare2(ex, ex2, shared_scale=True, la=None, lb=None)
                r_no = N.full_compare2(ex, ex2, shared_scale=False)
            except Exception as e:
                skips.append({"carrier": key, "cf": cid, "reason": f"COMPARE ERROR {e}"})
                continue
            variant = cid + ("_" + "_".join(f"{k}{v}" for k, v in prm.items()) if prm else "")
            rows.append({**base, "cf_id": cid, "variant": variant, "params": prm,
                         "geometry_identical": gid,
                         "n_text_after": len(ex2.texts),
                         "shared": {k: v for k, v in r_sh.items() if not k.startswith("_")},
                         "own_scale": {k: v for k, v in r_no.items() if not k.startswith("_")},
                         "sec": round(time.time() - t0, 2)})
        print(f"[{ci+1}/{len(cs)}] {key} seg={len(ex.segments)} done", flush=True)
    N.dump("neg_n1_text.json", {"schema": "neg-n1-1", "n_rows": len(rows),
                                "n_skips": len(skips), "rows": rows, "skips": skips})


if __name__ == "__main__":
    run()
