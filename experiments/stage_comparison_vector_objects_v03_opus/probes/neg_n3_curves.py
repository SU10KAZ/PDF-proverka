# -*- coding: utf-8 -*-
"""N3 — text drawn as curves.  Do letter contours become graphical objects, and how
many false graphic records does editing such text produce?"""
from __future__ import annotations
import json, math, random, sys, time, traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N          # noqa: E402
import neg_glyph as GL          # noqa: E402
import grp_common as G          # noqa: E402
import v03_counterfactual as CF # noqa: E402
import v03_objects as O         # noqa: E402

ART = N.ART
SEED = 20260823


def _inflate(bb, k):
    h = bb[3] - bb[1]
    return [bb[0] - 0.25 * h, bb[1] - k * h, bb[2] + 0.25 * h, bb[3] + k * h]


def _cover(obj_bb, boxes):
    """max share of obj_bb area covered by a single box in `boxes`."""
    ax = max(obj_bb[2] - obj_bb[0], 1e-9)
    ay = max(obj_bb[3] - obj_bb[1], 1e-9)
    area = ax * ay
    best = 0.0
    bi = -1
    for i, b in enumerate(boxes):
        ix = min(obj_bb[2], b[2]) - max(obj_bb[0], b[0])
        iy = min(obj_bb[3], b[3]) - max(obj_bb[1], b[1])
        if ix <= 0 or iy <= 0:
            continue
        s = (ix * iy) / area
        if s > best:
            best, bi = s, i
    return best, bi


def ground_truth(layer, texts, thr=0.85):
    boxes = [_inflate(t["bbox"], 0.35) for t in texts]
    gt, owner = set(), {}
    for i, o in enumerate(layer.objects):
        s, bi = _cover(o["bbox"], boxes)
        if s >= thr:
            gt.add(i)
            owner[i] = bi
    return gt, owner


def prf(det, gt):
    tp = len(det & gt)
    return {"tp": tp, "fp": len(det - gt), "fn": len(gt - det),
            "precision": round(tp / max(1, len(det)), 4),
            "recall": round(tp / max(1, len(gt)), 4),
            "n_det": len(det), "n_gt": len(gt)}


def _swap_text_ink(ex, layer, gt, owner, key):
    """Simulate 'the text was edited' on a curves-only sheet: the glyph ink of one
    printed line is replaced by the glyph ink of another line, in place."""
    rng = random.Random(CF._seed_for("NC1_curve_text_edit", key))
    by_line: dict[int, list[int]] = {}
    for oi, li in owner.items():
        by_line.setdefault(li, []).append(oi)
    lines = [li for li, v in by_line.items() if len(v) >= 3]
    if len(lines) < 2:
        raise CF.CFNotApplicable("fewer than 2 curve-drawn text lines with >=3 marks")
    rng.shuffle(lines)
    la, lb = lines[0], lines[1]
    seg_a = [g for oi in by_line[la] for g in layer.objects[oi]["segments"]]
    seg_b = [g for oi in by_line[lb] for g in layer.objects[oi]["segments"]]
    drop = set(seg_a)
    bb_a = CF._bbox_of_segs([ex.segments[g] for g in seg_a])
    bb_b = CF._bbox_of_segs([ex.segments[g] for g in seg_b])
    dx, dy = bb_a[0] - bb_b[0], bb_a[1] - bb_b[1]
    segs = [dict(s) | {"src": [s["i"]]} for i, s in enumerate(ex.segments) if i not in drop]
    for g in seg_b:
        s = ex.segments[g]
        t = CF._mk_seg((s["p0"][0] + dx, s["p0"][1] + dy),
                       (s["p1"][0] + dx, s["p1"][1] + dy),
                       CF._seg_style(s), src=[], tag="NC1_glyph")
        segs.append(t)
    CF._renumber(segs)
    ex2 = CF._clone(ex, segments=segs, prov={"cf": "NC1_curve_text_edit"})
    man = {"cf_id": "NC1_curve_text_edit", "line_replaced": la, "line_source": lb,
           "n_seg_removed": len(seg_a), "n_seg_added": len(seg_b),
           "bbox_pt": [round(v, 3) for v in bb_a],
           "expected_verdict": "NO_GRAPHIC_CHANGE"}
    return ex2, man


def _filtered(entries, flags_a, flags_b):
    keep = [e for e in entries
            if not ((e["side"] == "A" and e["oi"] in flags_a)
                    or (e["side"] == "B" and e["oi"] in flags_b))]
    return keep


def analyse_pair(ex_a, ex_b, tag):
    r = N.full_compare2(ex_a, ex_b, shared_scale=True)
    la, lb, off, rows, cfg = r["_la"], r["_lb"], r["_off"], r["_rows"], r["_cfg"]
    ents = N.ink_entry_list(ex_a, ex_b, la, lb, off, cfg, rows[0], rows[1])
    fa, _ = GL.glyph_flags(la, N._frame(ex_a))
    fb, _ = GL.glyph_flags(lb, N._frame(ex_b))
    kept = _filtered(ents, fa, fb)
    inner = [e for e in ents if not e["border"]]
    kept_inner = [e for e in kept if not e["border"]]
    return {"tag": tag, "n_entries_raw": len(inner), "n_entries_filtered": len(kept_inner),
            "n_entries_raw_with_border": len(ents),
            "n_flagged_a": len(fa), "n_flagged_b": len(fb),
            "n_obj_a": len(la.objects), "n_obj_b": len(lb.objects),
            "verdict_raw": "GRAPHIC_CHANGE" if inner else "NO_GRAPHIC_CHANGE",
            "verdict_filtered": "GRAPHIC_CHANGE" if kept_inner else "NO_GRAPHIC_CHANGE",
            "ledger": r["ledger"]}


def run(shard=0, of=1):
    t0 = time.time()
    cf_rows, real_rows, skips = [], [], []

    # ---------------- D9 on the counterfactual carriers -------------------------
    for i, c in enumerate(N.carriers()):
        if i % of != shard:
            continue
        key = N.carrier_key(c)
        try:
            ex = N.carrier_extract(c)
        except Exception as e:
            skips.append({"carrier": key, "reason": f"extract {e}"}); continue
        if not ex.texts:
            skips.append({"carrier": key, "reason": "no text layer to convert"}); continue
        la = O.build_objects(ex)
        try:
            ex9, man = CF.apply(ex, la, "D9_text_to_curves", key=key)
        except Exception as e:
            skips.append({"carrier": key, "cf": "D9", "reason": str(e)}); continue
        l9 = O.build_objects(ex9)
        gt, owner = ground_truth(l9, ex.texts)
        gt0, _ = ground_truth(la, ex.texts)            # contamination of the truth
        det, diag = GL.glyph_flags(l9, N._frame(ex9))
        det_closed, _ = GL.glyph_flags(l9, N._frame(ex9), closed_only=True)
        small = {i for i, o in enumerate(l9.objects)
                 if max(o["bbox"][2] - o["bbox"][0], o["bbox"][3] - o["bbox"][1])
                 <= 0.02 * math.hypot(*(lambda f: (f[2] - f[0], f[3] - f[1]))(N._frame(ex9)))}
        row = {"carrier": key, "discipline": c["discipline"], "cls": c["cls"],
               "n_seg_before": len(ex.segments), "n_seg_after": len(ex9.segments),
               "seg_ratio": round(len(ex9.segments) / max(1, len(ex.segments)), 3),
               "n_text_before": len(ex.texts), "n_text_after": len(ex9.texts),
               "n_obj_before": len(la.objects), "n_obj_after": len(l9.objects),
               "obj_ratio": round(len(l9.objects) / max(1, len(la.objects)), 3),
               "gt_glyph_objects": len(gt),
               "gt_share_of_objects": round(len(gt) / max(1, len(l9.objects)), 4),
               "gt_contamination_before": len(gt0),
               "gt_contamination_share": round(len(gt0) / max(1, len(la.objects)), 4),
               "run_detector": prf(det, gt), "run_closed_detector": prf(det_closed, gt),
               "size_only_detector": prf(small, gt), "diag": diag}
        # D9 itself as a negative control (text->curves, picture unchanged)
        try:
            row["D9_as_control"] = analyse_pair(ex, ex9, "D9_control")
        except Exception as e:
            row["D9_as_control_error"] = str(e)
        # a text EDIT on the curves-only sheet
        try:
            ex9b, man2 = _swap_text_ink(ex9, l9, gt, owner, key)
            row["curve_text_edit"] = analyse_pair(ex9, ex9b, "NC1")
            row["curve_text_edit"]["manifest"] = man2
        except CF.CFNotApplicable as e:
            row["curve_text_edit_skip"] = str(e)
        except Exception as e:
            row["curve_text_edit_error"] = traceback.format_exc()[-300:]
        cf_rows.append(row)
        print(f"D9 {i+1}/59 {key} obj {len(la.objects)}->{len(l9.objects)}", flush=True)

    # ---------------- real curved-text blocks from the census -------------------
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
        det, diag = GL.glyph_flags(lay, N._frame(ex))
        rr = {"block": k, "discipline": r["discipline"], "n_seg": len(ex.segments),
              "n_text": len(ex.texts), "n_obj": len(lay.objects),
              "counts": lay.counts(), "n_flagged": len(det),
              "flagged_share": round(len(det) / max(1, len(lay.objects)), 4),
              "flagged_ink_share": round(
                  sum(lay.objects[i]["seg_len"] for i in det)
                  / max(1e-9, sum(o["seg_len"] for o in lay.objects)), 4),
              "diag": diag}
        # edit the "text" of a real curves-only block: swap two detected runs
        try:
            owner = {}
            runs = sorted(det)
            if len(runs) >= 6:
                # group flagged objects into rows by cy, use the run structure
                rows_by_y: dict[int, list[int]] = {}
                for i in runs:
                    o = lay.objects[i]
                    h = max(o["bbox"][3] - o["bbox"][1], 1e-6)
                    rows_by_y.setdefault(int(round(o["cy"] / max(h, 1e-6))), []).append(i)
                lines = [v for v in rows_by_y.values() if len(v) >= 3]
                if len(lines) >= 2:
                    for li, v in enumerate(lines):
                        for i in v:
                            owner[i] = li
                    ex2, man2 = _swap_text_ink(ex, lay, set(owner), owner, k)
                    rr["curve_text_edit"] = analyse_pair(ex, ex2, "NC1_real")
                    rr["curve_text_edit"]["manifest"] = man2
                else:
                    rr["curve_text_edit_skip"] = "fewer than 2 detected runs"
            else:
                rr["curve_text_edit_skip"] = f"only {len(runs)} flagged marks"
        except CF.CFNotApplicable as e:
            rr["curve_text_edit_skip"] = str(e)
        except Exception as e:
            rr["curve_text_edit_error"] = traceback.format_exc()[-300:]
        real_rows.append(rr)
        print(f"real {j+1}/{len(real)} {k} obj={len(lay.objects)} flagged={len(det)}", flush=True)

    name = "neg_n3_curves.json" if of == 1 else f"neg_runs/neg_n3_curves_{shard}of{of}.json"
    N.dump(name, {"schema": "neg-n3-1", "cf": cf_rows, "real": real_rows,
                  "skips": skips, "shard": [shard, of], "sec": round(time.time() - t0, 1)})


if __name__ == "__main__":
    a = sys.argv[1:]
    run(int(a[0]) if a else 0, int(a[1]) if len(a) > 1 else 1)
