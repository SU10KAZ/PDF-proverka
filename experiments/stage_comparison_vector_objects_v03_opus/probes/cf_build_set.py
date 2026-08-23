# -*- coding: utf-8 -*-
"""Build the counterfactual set on REAL prepared blocks + run the four self-checks.

    python probes/cf_build_set.py [--carriers N] [--workers K] [--out-suffix S]

Writes:
    artifacts/cf_manifest.json   — every counterfactual with its exact manifest
    artifacts/cf_selfcheck.json  — the four mandatory self-checks per instance
"""
from __future__ import annotations
import argparse
import json
import math
import os
import sys
import time
import traceback
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXP = HERE.parent
ART = EXP / "artifacts"
sys.path.insert(0, str(HERE))

SEED = 20260823
TARGET = 1100
MAX_SEG = 60000           # carriers above this are too slow for the raster self-check
MIN_SEG = 60


def pick_carriers(n_per_disc=4):
    """Stratified carriers: per discipline one sparse / medium / dense block + one table."""
    import random
    smp = json.load(open(ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    rng = random.Random(SEED)
    by = defaultdict(list)
    for r in smp:
        if not (MIN_SEG <= r["n_seg"] <= MAX_SEG):
            continue
        by[(r["discipline"], r["bucket"])].append(r)
    out = []
    for disc in sorted({d for d, _ in by}):
        for bucket in ("sparse", "medium", "dense", "very_dense"):
            rows = sorted(by.get((disc, bucket), []), key=lambda r: r["block_id"])
            if not rows:
                continue
            rng.shuffle(rows)
            # prefer a drawing for geometry classes, but keep tables in the pool
            rows.sort(key=lambda r: 0 if r["cls"] == "drawing" else 1)
            out.append(dict(rows[0], bucket=bucket))
            if bucket in ("medium", "dense"):
                tab = [r for r in rows if r["cls"] in ("stamp", "table")]
                if tab:
                    out.append(dict(tab[0], bucket=bucket))
    seen = set()
    uniq = []
    for r in out:
        k = (r["doc_id"], r["version"], r["block_id"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)
    return uniq


def plan():
    """(cf_id, kwargs, tag) instances applied to every carrier."""
    P = []
    for cf in ("A1_path_split", "A2_path_merge", "A3_curve_resample_down", "A3_curve_resample_up",
               "A4_circle_to_bezier", "A4b_circle_to_chords5", "A4c_circle_to_chords24",
               "A5_order_shuffle", "A6_round_0.01", "A6_round_0.1", "A6_round_0.25",
               "A6_round_0.5", "A8_lineweight"):
        P.append((cf, {}, cf))
    P.append(("A7_reexport_gs", {}, "A7_reexport_gs"))
    P.append(("A7_reexport_cairo", {}, "A7_reexport_cairo"))
    for f in (0.005, 0.02, 0.10):
        P.append(("B1_translate", {"frac": f}, f"B1_translate@{f}"))
    for k in (0.95, 1.05, 1.2):
        P.append(("B2_scale", {"k": k}, f"B2_scale@{k}"))
    for f in (0.0025, 0.005, 0.02, 0.05, 0.10):
        P.append(("B3_crop_jitter", {"frac": f}, f"B3_crop_jitter@{f}"))
    for f in (0.05, 0.14):
        P.append(("B4_aspect", {"frac": f}, f"B4_aspect@{f}"))
    for a in (90, 270):
        P.append(("B5_rotate_page", {"add": a}, f"B5_rotate_page@{a}"))
    for b in ("tiny", "small", "large"):
        P.append(("C1_remove_object", {"bucket": b}, f"C1_remove_object@{b}"))
        P.append(("C2_add_object", {"bucket": b}, f"C2_add_object@{b}"))
        P.append(("C6_reshape_object", {"bucket": b}, f"C6_reshape_object@{b}"))
        for f in (0.0025, 0.005, 0.01, 0.02, 0.05):
            P.append(("C3_move_object", {"bucket": b, "frac": f}, f"C3_move_object@{b}@{f}"))
    for cf in ("C4_swap_objects", "C5_swap_unlike", "C7_split_object", "C8_merge_objects",
               "C9_add_branch", "C10_remove_opening"):
        P.append((cf, {}, cf))
    for cf in ("D1_text_edit", "D2_text_move", "D3_label_rename", "D4_table_values",
               "D5_table_row_text", "D6_dim_value_only", "D7_dim_geometry", "D8_font_swap",
               "D9_text_to_curves"):
        P.append((cf, {}, cf))
    return P


def run_carrier(rec):
    import numpy as np
    import grp_common as G
    import v03_objects as O
    import v03_counterfactual as C
    import cf_check as K

    t0 = time.time()
    res = {"carrier": rec, "instances": [], "error": None}
    try:
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            res["error"] = "block not found"
            return res
        ex = G.extract(pb)
        if not ex.segments:
            res["error"] = "no vector geometry"
            return res
        ol = O.build_objects(ex)
        fr0 = C._frame_of(ex)
        base = C.render_extract(ex, frame=fr0, target_px=TARGET, draw_text=False)
        A0 = C.pix_to_bin(base)
        res["block"] = {"n_seg": len(ex.segments), "n_text": len(ex.texts),
                        "n_obj": len(ol.objects), "S_pt": round(ol.S, 4),
                        "scale_source": ol.scale_source,
                        "frame_pt": [round(v, 2) for v in fr0],
                        "counts": ol.counts(),
                        "base_ink_px": int(A0.sum()), "render_px": list(A0.shape)}
        res["fidelity"] = K.renderer_fidelity2(ex, target=TARGET)
        for cf_id, kw, tag in plan():
            row = {"cf_id": cf_id, "tag": tag, "kwargs": kw}
            t1 = time.time()
            try:
                ex2, man = C.apply(ex, ol, cf_id, **kw)
            except C.CFNotApplicable as e:
                row["status"] = "skip"
                row["reason"] = str(e)
                res["instances"].append(row)
                continue
            except Exception as e:                                  # noqa: BLE001
                row["status"] = "error"
                row["reason"] = f"{type(e).__name__}: {e}"
                res["instances"].append(row)
                continue
            row["status"] = "ok"
            row["manifest"] = man
            cls = man["cf_class"]
            try:
                if cf_id.startswith("A7_") or cf_id == "D9_text_to_curves":
                    row["check"] = K.check_page_level(ex, ex2, man)
                    if cf_id == "D9_text_to_curves":
                        row["check"]["geometry_match_tol_0.05"] = K.geometry_match(ex, ex2, tol=0.05)
                        row["check"]["n_text_before"] = len(ex.texts)
                        row["check"]["n_text_after"] = len(ex2.texts)
                elif cf_id == "B5_rotate_page":
                    row["check"] = K.check_B(ex, ex2, man)
                    row["check_pdf"] = K.check_page_level(ex, ex2, man, rotate_back=kw.get("add", 90))
                elif cls == "A":
                    row["check"] = K.check_A(ex, ex2, man, base_mask=A0)
                    row["geometry"] = K.geometry_match(ex, ex2, tol=1e-6)
                elif cls == "B":
                    row["check"] = K.check_B(ex, ex2, man)
                elif cls == "C":
                    row["check"] = K.check_C(ex, ex2, man, base_mask=A0)
                    row["geometry"] = K.geometry_match(ex, ex2, tol=1e-6)
                elif cf_id == "D7_dim_geometry":
                    row["check"] = K.check_D(ex, ex2, man)
                    row["check_locality"] = K.check_C(ex, ex2, man, base_mask=A0)
                else:
                    row["check"] = K.check_D(ex, ex2, man)
            except Exception as e:                                  # noqa: BLE001
                row["check"] = {"error": f"{type(e).__name__}: {e}"}
            row["sec"] = round(time.time() - t1, 3)
            res["instances"].append(row)
    except Exception as e:                                          # noqa: BLE001
        res["error"] = f"{type(e).__name__}: {e}"
        res["traceback"] = traceback.format_exc()[-1500:]
    finally:
        try:
            import v03_counterfactual as C2
            C2.cleanup_scratch()
        except Exception:
            pass
    res["sec"] = round(time.time() - t0, 2)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--carriers", type=int, default=0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out-suffix", default="")
    a = ap.parse_args()
    carriers = pick_carriers()
    if a.carriers:
        carriers = carriers[: a.carriers]
    print(f"carriers: {len(carriers)}  instances/carrier: {len(plan())}", flush=True)
    out = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=a.workers) as pool:
        futs = {pool.submit(run_carrier, r): r for r in carriers}
        for i, f in enumerate(as_completed(futs), 1):
            r = f.result()
            out.append(r)
            print(f"[{i}/{len(carriers)}] {r['carrier']['discipline']} "
                  f"{r['carrier']['block_id'][:12]} {r['carrier']['bucket']} "
                  f"ok={sum(1 for x in r['instances'] if x['status'] == 'ok')} "
                  f"skip={sum(1 for x in r['instances'] if x['status'] == 'skip')} "
                  f"err={sum(1 for x in r['instances'] if x['status'] == 'error')} "
                  f"{r.get('sec')}s {r.get('error') or ''}", flush=True)
    suf = a.out_suffix
    man = {"schema": "v03-cf-set-1", "seed": SEED, "target_px": TARGET,
           "n_carriers": len(carriers), "plan": [t for _, _, t in plan()],
           "runs": out, "wall_sec": round(time.time() - t0, 1)}
    p = ART / f"cf_runs{suf}.json"
    json.dump(man, open(p, "w", encoding="utf-8"), ensure_ascii=False)
    print("wrote", p, round(p.stat().st_size / 1e6, 1), "MB")


if __name__ == "__main__":
    main()
