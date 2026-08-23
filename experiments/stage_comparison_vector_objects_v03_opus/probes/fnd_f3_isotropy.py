# -*- coding: utf-8 -*-
"""F3 — isotropic vs anisotropic normalisation, on real prepared blocks.

Three measurements:

1. Real cross-version block pairs (same document, same sheet, IoU-matched) whose bbox
   aspect ratios differ.  Analytic angular error of a 45-degree segment under x/w,y/h.
2. Segment partner-finding rate on those real pairs, at 0.25 / 0.5 / 1 % tolerance,
   under both normalisations.
3. CONTENT-CONTROLLED variant: one real block, its own segments, normalised with its
   own bbox versus with a bbox carrying the partner's aspect ratio.  Content is
   bit-identical, so every mismatch is the normalisation and nothing else.
"""
from __future__ import annotations

import collections, json, math, os, random, sys, time
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402
import fitz  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
SEED = 20260823
TOLS = (0.0025, 0.005, 0.01)
MIN_AR_DIFF = 0.02      # >=2 % aspect-ratio difference
MIN_IOU = 0.60
MAX_PAIRS = 200
MAX_SEG = 40000


def iou(a, b):
    ix = max(0.0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    ua = (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter
    return inter / ua if ua > 0 else 0.0


def mine_pairs():
    idx = json.loads((ART / "fnd_corpus_index.json").read_text(encoding="utf-8"))
    by = collections.defaultdict(list)
    for x in idx["documents"]:
        if not x.get("pdf_exists") or x.get("error"):
            continue
        p = x["result_json"].split("/")
        by[(p[2], p[4], p[6])].append((p[8], x["result_json"]))
    pairs = []
    for key, vers in sorted(by.items()):
        if len(vers) < 2:
            continue
        vers.sort()
        (va, fa), (vb, fb) = vers[0], vers[-1]
        try:
            A = F.iter_prepared_blocks(fa, resolve_rotation_from_pdf=False)
            B = F.iter_prepared_blocks(fb, resolve_rotation_from_pdf=False)
        except Exception:
            continue
        bp = collections.defaultdict(list)
        for b in B:
            bp[b.page_number].append(b)
        used = set()
        for a in A:
            if not a.page_px_w or not a.page_px_h:
                continue
            an = [a.coords_px[0] / a.page_px_w, a.coords_px[1] / a.page_px_h,
                  a.coords_px[2] / a.page_px_w, a.coords_px[3] / a.page_px_h]
            best, bi = 0.0, None
            for b in bp.get(a.page_number, []):
                if b.block_id in used or not b.page_px_w or not b.page_px_h:
                    continue
                bn = [b.coords_px[0] / b.page_px_w, b.coords_px[1] / b.page_px_h,
                      b.coords_px[2] / b.page_px_w, b.coords_px[3] / b.page_px_h]
                v = iou(an, bn)
                if v > best:
                    best, bi = v, b
            if bi is None or best < MIN_IOU:
                continue
            wa, ha = a.coords_px[2] - a.coords_px[0], a.coords_px[3] - a.coords_px[1]
            wb, hb = bi.coords_px[2] - bi.coords_px[0], bi.coords_px[3] - bi.coords_px[1]
            if min(wa, ha, wb, hb) < 60:
                continue
            ar_a, ar_b = wa / ha, wb / hb
            rel = abs(math.log(ar_a / ar_b))
            if rel < math.log(1 + MIN_AR_DIFF):
                continue
            used.add(bi.block_id)
            pairs.append({"key": "/".join(key), "va": va, "vb": vb,
                          "a": a, "b": bi, "iou": best,
                          "ar_a": ar_a, "ar_b": ar_b, "ar_rel_log": rel})
        F.clear_caches()
    return pairs


def angle_error_45(ar_a, ar_b):
    """A 45-degree segment after x/w,y/h in each block: how far apart do the two land?"""
    th_a = math.degrees(math.atan2(1.0, ar_a))   # dy'/dx' = (1/h)/(1/w) = w/h = ar
    th_b = math.degrees(math.atan2(1.0, ar_b))
    return abs(th_a - th_b)


def seg_feats(segs):
    out = np.empty((len(segs), 4), dtype=np.float64)
    for i, s in enumerate(segs):
        (x0, y0), (x1, y1) = s["p0"], s["p1"]
        out[i] = ((x0 + x1) / 2, (y0 + y1) / 2, math.hypot(x1 - x0, y1 - y0),
                  math.degrees(math.atan2(y1 - y0, x1 - x0)) % 180.0)
    return out


def match_rate(FA, FB, tol, ang_tol=5.0):
    """Share of A-segments finding a B-partner: midpoint within tol, |dlen|<=tol, angle close."""
    if len(FA) == 0 or len(FB) == 0:
        return float("nan")
    cell = max(tol, 1e-9)
    grid = collections.defaultdict(list)
    for j in range(len(FB)):
        grid[(int(FB[j, 0] // cell), int(FB[j, 1] // cell))].append(j)
    hit = 0
    for i in range(len(FA)):
        gx, gy = int(FA[i, 0] // cell), int(FA[i, 1] // cell)
        found = False
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for j in grid.get((gx + dx, gy + dy), ()):
                    if abs(FA[i, 0] - FB[j, 0]) > tol or abs(FA[i, 1] - FB[j, 1]) > tol:
                        continue
                    if math.hypot(FA[i, 0] - FB[j, 0], FA[i, 1] - FB[j, 1]) > tol:
                        continue
                    if abs(FA[i, 2] - FB[j, 2]) > tol:
                        continue
                    da = abs(FA[i, 3] - FB[j, 3])
                    if min(da, 180 - da) > ang_tol:
                        continue
                    found = True
                    break
                if found:
                    break
            if found:
                break
        hit += found
    return hit / len(FA)


def main():
    rng = random.Random(SEED)
    t0 = time.time()
    pairs = mine_pairs()
    print(f"mined {len(pairs)} aspect-differing pairs in {time.time()-t0:.0f}s", flush=True)
    per_doc = collections.Counter()
    sel = []
    rng.shuffle(pairs)
    for p in pairs:
        if per_doc[p["key"]] >= 3:
            continue
        per_doc[p["key"]] += 1
        sel.append(p)
        if len(sel) >= MAX_PAIRS:
            break
    print(f"selected {len(sel)} pairs over {len(per_doc)} documents", flush=True)

    rows = []
    for i, p in enumerate(sel):
        a, b = p["a"], p["b"]
        row = {"key": p["key"], "va": p["va"], "vb": p["vb"], "iou_bbox": p["iou"],
               "block_a": a.block_id, "block_b": b.block_id, "page": a.page_number,
               "discipline": a.discipline, "rotation": a.rotation,
               "ar_a": p["ar_a"], "ar_b": p["ar_b"],
               "ar_rel_pct": 100 * (math.exp(p["ar_rel_log"]) - 1),
               "angle_error_45_aniso_deg": angle_error_45(p["ar_a"], p["ar_b"]),
               "angle_error_45_iso_deg": 0.0}
        try:
            fa = F.block_frame(a.pdf_path, a.page_index, a.coords_px, a.page_px_w, a.page_px_h)
            fb = F.block_frame(b.pdf_path, b.page_index, b.coords_px, b.page_px_w, b.page_px_h)
            ea = F.extract_block(a.pdf_path, a.page_index, a.coords_px, a.page_px_w, a.page_px_h, frame=fa)
            eb = F.extract_block(b.pdf_path, b.page_index, b.coords_px, b.page_px_w, b.page_px_h, frame=fb)
            row["n_seg_a"], row["n_seg_b"] = len(ea.segments), len(eb.segments)
            if len(ea.segments) < 20 or len(eb.segments) < 20 or \
               len(ea.segments) > MAX_SEG or len(eb.segments) > MAX_SEG:
                row["skipped"] = "segment count out of range"
                rows.append(row)
                F.clear_caches()
                continue
            long_a = max(fa.w, fa.h)
            for mode in ("isotropic", "anisotropic", "points"):
                FA = seg_feats(F.normalize(ea.segments, fa, mode))
                FB = seg_feats(F.normalize(eb.segments, fb, mode))
                for tol in TOLS:
                    t_eff = tol * long_a if mode == "points" else tol
                    row[f"match_{mode}_{tol}"] = match_rate(FA, FB, t_eff)
            # ---- content-controlled: same segments, partner's aspect ratio -------
            wa, ha = fa.w, fa.h
            area = wa * ha
            ar_b = p["ar_b"]
            wj = math.sqrt(area * ar_b)
            hj = area / wj
            cx, cy = (fa.clip_display.x0 + fa.clip_display.x1) / 2, (fa.clip_display.y0 + fa.clip_display.y1) / 2
            fj = {"clip_display": [cx - wj / 2, cy - hj / 2, cx + wj / 2, cy + hj / 2]}
            for mode in ("isotropic", "anisotropic", "points"):
                FA = seg_feats(F.normalize(ea.segments, fa, mode))
                FJ = seg_feats(F.normalize(ea.segments, fj, mode))
                for tol in TOLS:
                    t_eff = tol * long_a if mode == "points" else tol
                    row[f"ctrl_{mode}_{tol}"] = match_rate(FA, FJ, t_eff)
        except Exception as exc:
            row["error"] = f"{type(exc).__name__}: {exc}"
        rows.append(row)
        F.clear_caches()
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(sel)} {time.time()-t0:.0f}s", flush=True)

    ok = [r for r in rows if "error" not in r and "skipped" not in r and "match_isotropic_0.0025" in r]
    def med(k):
        v = [r[k] for r in ok if isinstance(r.get(k), float) and r[k] == r[k]]
        return float(np.median(v)) if v else None
    summary = {
        "n_pairs_mined": len(pairs), "n_pairs_selected": len(sel), "n_pairs_measured": len(ok),
        "min_aspect_diff": MIN_AR_DIFF, "min_bbox_iou": MIN_IOU, "seed": SEED,
        "median_aspect_diff_pct": float(np.median([r["ar_rel_pct"] for r in rows])),
        "p90_aspect_diff_pct": float(np.percentile([r["ar_rel_pct"] for r in rows], 90)),
        "angle_error_45_deg": {
            "anisotropic_median": float(np.median([r["angle_error_45_aniso_deg"] for r in rows])),
            "anisotropic_p90": float(np.percentile([r["angle_error_45_aniso_deg"] for r in rows], 90)),
            "anisotropic_max": float(np.max([r["angle_error_45_aniso_deg"] for r in rows])),
            "isotropic": 0.0,
            "note": "isotropic keeps a 45-degree segment at 45 degrees by construction",
        },
        "match_rate_real_pairs": {
            f"{mode}_{tol}": med(f"match_{mode}_{tol}")
            for mode in ("isotropic", "anisotropic", "points") for tol in TOLS},
        "match_rate_content_controlled": {
            f"{mode}_{tol}": med(f"ctrl_{mode}_{tol}")
            for mode in ("isotropic", "anisotropic", "points") for tol in TOLS},
        "n_pairs_by_mode_note": "points mode uses tol * long_side(A) in PDF points, so "
                                "the three modes carry the same physical tolerance",
    }
    (ART / "fnd_isotropy.json").write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
