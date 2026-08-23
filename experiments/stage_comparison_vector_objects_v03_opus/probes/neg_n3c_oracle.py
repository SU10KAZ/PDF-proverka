# -*- coding: utf-8 -*-
"""N3c — is the false verdict on a curves-only sheet caused ENTIRELY by letters?

`neg_n3_curves.py` shows that D9 (text converted to outlines) makes the comparator say
GRAPHIC_CHANGE, and that a run-based glyph filter only halves the records.  That leaves
an ambiguity the report must not paper over: are the surviving records letters the
detector missed, or is `gs -dNoOutputFonts` also perturbing the real strokes?

This separates the two with an ORACLE filter.  The ground truth of "which object is a
letter" is known exactly (we converted the text ourselves, so we know every text line's
box).  Drop every ledger entry whose object is in that set and see what is left.  What
survives is, by construction, not a letter.

Capped by block size: the question is qualitative and does not need the densest blocks.
"""
from __future__ import annotations
import json, os, sys, time, traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N          # noqa: E402
import neg_glyph as GL          # noqa: E402
import neg_n3_curves as N3      # noqa: E402
import v03_counterfactual as CF # noqa: E402
import v03_objects as O         # noqa: E402

MAX_SEG = int(os.environ.get("NEG_N3C_MAX_SEG", "6000"))


def run(shard=0, of=1):
    t0 = time.time()
    rows, skips = [], []
    for i, c in enumerate(N.carriers()):
        if i % of != shard:
            continue
        key = N.carrier_key(c)
        try:
            ex = N.carrier_extract(c)
        except Exception as e:
            skips.append({"carrier": key, "reason": f"extract {e}"}); continue
        if len(ex.segments) > MAX_SEG:
            skips.append({"carrier": key, "reason": f"n_seg {len(ex.segments)} > cap"}); continue
        if len(ex.texts) < 3:
            skips.append({"carrier": key, "reason": "fewer than 3 text lines"}); continue
        try:
            la = O.build_objects(ex)
            ex9, man = CF.apply(ex, la, "D9_text_to_curves", key=key)
            l9 = O.build_objects(ex9)
        except Exception as e:
            skips.append({"carrier": key, "reason": f"D9 {e}"}); continue
        gt9, owner = N3.ground_truth(l9, ex.texts)      # letters on the converted side
        gt0, _ = N3.ground_truth(la, ex.texts)          # contamination on the original side
        try:
            r = N.full_compare2(ex, ex9, shared_scale=True)
        except Exception as e:
            skips.append({"carrier": key, "reason": f"CMP {e}",
                          "tb": traceback.format_exc()[-200:]}); continue
        lay_a, lay_b, off, rws, cfg = r["_la"], r["_lb"], r["_off"], r["_rows"], r["_cfg"]
        ents = N.ink_entry_list(ex, ex9, lay_a, lay_b, off, cfg, rws[0], rws[1])
        inner = [e for e in ents if not e["border"]]
        det_a, _ = GL.glyph_flags(lay_a, N._frame(ex), absorb=True)
        det_b, _ = GL.glyph_flags(lay_b, N._frame(ex9), absorb=True)
        def keep(pred_a, pred_b):
            return [e for e in inner
                    if not ((e["side"] == "A" and e["oi"] in pred_a)
                            or (e["side"] == "B" and e["oi"] in pred_b))]
        oracle = keep(gt0, gt9)
        heur = keep(det_a, det_b)
        rows.append({
            "carrier": key, "discipline": c["discipline"], "cls": c["cls"],
            "n_seg": len(ex.segments), "n_seg9": len(ex9.segments),
            "n_text": len(ex.texts),
            "n_obj_a": len(lay_a.objects), "n_obj_b": len(lay_b.objects),
            "n_gt_b": len(gt9), "n_gt_a": len(gt0),
            "records_raw": len(inner),
            "records_after_oracle": len(oracle),
            "records_after_heuristic": len(heur),
            "verdict_raw": "GRAPHIC_CHANGE" if inner else "NO_GRAPHIC_CHANGE",
            "verdict_oracle": "GRAPHIC_CHANGE" if oracle else "NO_GRAPHIC_CHANGE",
            "verdict_heuristic": "GRAPHIC_CHANGE" if heur else "NO_GRAPHIC_CHANGE",
            "survivors": sorted(oracle, key=lambda e: -e["unmatched_len_pt"])[:5],
        })
        print(f"[{i+1}] {key} raw={len(inner)} oracle={len(oracle)} heur={len(heur)}", flush=True)
    name = "neg_n3c_oracle.json" if of == 1 else f"neg_runs/neg_n3c_{shard}of{of}.json"
    N.dump(name, {"schema": "neg-n3c-1", "cap_n_seg": MAX_SEG, "rows": rows,
                  "skips": skips, "sec": round(time.time() - t0, 1)})


if __name__ == "__main__":
    a = sys.argv[1:]
    run(int(a[0]) if a else 0, int(a[1]) if len(a) > 1 else 1)
