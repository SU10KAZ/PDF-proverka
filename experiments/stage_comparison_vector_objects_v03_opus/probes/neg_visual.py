# -*- coding: utf-8 -*-
"""Eyeball material for probe `neg`: does each control do what it claims?"""
from __future__ import annotations
import json, math, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N          # noqa: E402
import neg_glyph as GL          # noqa: E402
import neg_tablecf as TB        # noqa: E402
import neg_n3_curves as N3      # noqa: E402
import grp_common as G          # noqa: E402
import v03_counterfactual as CF # noqa: E402
import v03_objects as O         # noqa: E402

OUT = N.ART / "neg_visual"
OUT.mkdir(parents=True, exist_ok=True)


def colour_by_flags(ex, layer, flags, hit=(1, 0, 0), miss=(0.55, 0.55, 0.55)):
    col = {}
    for oi, o in enumerate(layer.objects):
        c = hit if oi in flags else miss
        for g in o["segments"]:
            col[g] = c
    segs = []
    for i, s in enumerate(ex.segments):
        t = dict(s)
        t["color"] = col.get(i, miss)
        t["w"] = max(float(s.get("w") or 0.0), 0.6 if i in col and col[i] == hit else 0.3)
        segs.append(t)
    return CF._clone(ex, segments=segs)


def triptych(ex_a, ex_b, name, frame=None, px=1400):
    fr = frame or N._frame(ex_a)
    CF.render_extract(ex_a, frame=fr, target_px=px, out_png=OUT / f"{name}_A.png",
                      draw_text=False, force_black=True)
    CF.render_extract(ex_b, frame=fr, target_px=px, out_png=OUT / f"{name}_B.png",
                      draw_text=False, force_black=True)


def main():
    idx = []
    cs = N.carriers()

    # --- 1. table row insert: is it a row insert? -------------------------------
    n = 0
    for c in cs:
        if c["cls"] not in ("stamp", "table"):
            continue
        ex = N.carrier_extract(c)
        try:
            ex2, man = TB.apply(ex, "NR2_row_insert_shift", N.carrier_key(c), shift=True)
        except CF.CFNotApplicable:
            continue
        nm = f"NR2_{c['discipline']}_{c['block_id'][:8]}"
        triptych(ex, ex2, nm)
        idx.append({"kind": "table_row_insert", "name": nm, "carrier": N.carrier_key(c),
                    "manifest": man})
        n += 1
        if n >= 4:
            break

    # --- 2. glyph detector on real curves-only blocks ---------------------------
    real = [r for r in G.block_records() if r.get("cls") == "curved_text"]
    for r in real[:4]:
        pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None:
            continue
        ex = G.extract(pb)
        lay = O.build_objects(ex)
        det, diag = GL.glyph_flags(lay, N._frame(ex))
        nm = f"GLYPHreal_{r['discipline']}_{r['block_id'][:8]}"
        CF.render_extract(colour_by_flags(ex, lay, det), target_px=1600,
                          out_png=OUT / f"{nm}.png", draw_text=False)
        idx.append({"kind": "glyph_real", "name": nm,
                    "block": f"{r['doc_id']}|{r['version']}|{r['block_id']}",
                    "n_obj": len(lay.objects), "n_flagged": len(det), "diag": diag})

    # --- 3. glyph detector on a D9 counterfactual -------------------------------
    n = 0
    for c in cs:
        ex = N.carrier_extract(c)
        if len(ex.texts) < 8 or len(ex.segments) > 6000:
            continue
        la = O.build_objects(ex)
        try:
            ex9, man = CF.apply(ex, la, "D9_text_to_curves", key=N.carrier_key(c))
        except Exception:
            continue
        l9 = O.build_objects(ex9)
        gt, owner = N3.ground_truth(l9, ex.texts)
        det, diag = GL.glyph_flags(l9, N._frame(ex9))
        nm = f"GLYPHd9_{c['discipline']}_{c['block_id'][:8]}"
        CF.render_extract(colour_by_flags(ex9, l9, det), target_px=1600,
                          out_png=OUT / f"{nm}_det.png", draw_text=False)
        CF.render_extract(colour_by_flags(ex9, l9, gt, hit=(0, 0.45, 0.95)), target_px=1600,
                          out_png=OUT / f"{nm}_gt.png", draw_text=False)
        idx.append({"kind": "glyph_d9", "name": nm, "carrier": N.carrier_key(c),
                    "n_obj_before": len(la.objects), "n_obj_after": len(l9.objects),
                    "n_gt": len(gt), "n_det": len(det),
                    "prf": N3.prf(det, gt)})
        n += 1
        if n >= 4:
            break

    with open(OUT.parent / "neg_visual_index.json", "w", encoding="utf-8") as fh:
        json.dump(idx, fh, ensure_ascii=False, indent=1, default=str)
    print(f"[neg] {len(idx)} visuals in {OUT}")


if __name__ == "__main__":
    main()
