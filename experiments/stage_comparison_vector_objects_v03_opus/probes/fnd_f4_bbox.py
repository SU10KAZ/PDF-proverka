# -*- coding: utf-8 -*-
"""F4 — does the bbox we clip actually describe the prepared block?

Three checks over the WHOLE corpus (no sampling):
  * coords_px (result.json) vs crop_px (blocks_stage02_100/index.json) — the two places
    production reads the block bbox from.  Any disagreement means the crop a human
    reviewed is not the region a probe reads.
  * region inside the page (coords_px within page_px, and clip inside page.rect).
  * shape_type == "polygon": a rectangular clip is wider than the real block.  Measured
    as polygon_area / bbox_area from polygon_points.
"""
from __future__ import annotations

import json, math, os, sys, time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")


def shoelace(pts):
    a = 0.0
    n = len(pts)
    for i in range(n):
        x0, y0 = pts[i]
        x1, y1 = pts[(i + 1) % n]
        a += x0 * y1 - x1 * y0
    return abs(a) / 2.0


def main():
    idx = json.loads((ART / "fnd_corpus_index.json").read_text(encoding="utf-8"))
    t0 = time.time()
    n_blocks = 0
    cnt = Counter()
    poly_ratios = []
    poly_by_disc = defaultdict(list)
    mismatch_examples = []
    oob_examples = []
    mismatch_mag = []
    idx_cover = Counter()
    by_disc_poly = Counter()
    by_disc_blocks = Counter()

    for k, doc in enumerate(idx["documents"]):
        rj = doc["result_json"]
        try:
            blocks = F.iter_prepared_blocks(rj, resolve_rotation_from_pdf=False)
        except Exception:
            cnt["doc_read_error"] += 1
            continue
        ver_dir = Path(rj).parents[1]
        ipath = ver_dir / "03_analysis/latest/blocks_stage02_100/index.json"
        crop_px = {}
        if ipath.exists():
            try:
                jd = json.loads(ipath.read_text(encoding="utf-8"))
                for e in jd.get("blocks") or []:
                    crop_px[str(e.get("block_id"))] = e.get("crop_px")
                cnt["docs_with_index"] += 1
            except Exception:
                cnt["index_read_error"] += 1
        for b in blocks:
            n_blocks += 1
            by_disc_blocks[b.discipline] += 1
            # --- index agreement ---
            cp = crop_px.get(b.block_id)
            if cp is None:
                idx_cover["not_in_index"] += 1
            else:
                idx_cover["in_index"] += 1
                same = (len(cp) >= 4 and all(abs(float(cp[i]) - float(b.coords_px[i])) < 0.5
                                             for i in range(4)))
                if same:
                    cnt["index_match"] += 1
                else:
                    cnt["index_mismatch"] += 1
                    d = max(abs(float(cp[i]) - float(b.coords_px[i])) for i in range(4)) if len(cp) >= 4 else -1
                    mismatch_mag.append(d)
                    if len(mismatch_examples) < 40:
                        mismatch_examples.append({
                            "block_id": b.block_id, "doc_id": b.doc_id, "version": b.version,
                            "discipline": b.discipline, "coords_px": list(b.coords_px),
                            "crop_px": cp, "max_abs_delta_px": d})
            # --- inside page ---
            x1, y1, x2, y2 = b.coords_px
            if b.page_px_w and b.page_px_h:
                if x1 < -0.5 or y1 < -0.5 or x2 > b.page_px_w + 0.5 or y2 > b.page_px_h + 0.5:
                    cnt["outside_page_px"] += 1
                    if len(oob_examples) < 40:
                        over = max(-x1, -y1, x2 - b.page_px_w, y2 - b.page_px_h)
                        oob_examples.append({"block_id": b.block_id, "doc_id": b.doc_id,
                                             "version": b.version, "coords_px": list(b.coords_px),
                                             "page_px": [b.page_px_w, b.page_px_h],
                                             "overflow_px": over})
                else:
                    cnt["inside_page_px"] += 1
            else:
                cnt["no_page_px"] += 1
            if x2 <= x1 or y2 <= y1:
                cnt["degenerate_bbox"] += 1
            # --- polygon ---
            if b.shape_type == "polygon":
                cnt["polygon"] += 1
                by_disc_poly[b.discipline] += 1
                if b.polygon_points and len(b.polygon_points) >= 3:
                    cnt["polygon_with_points"] += 1
                    bb = (x2 - x1) * (y2 - y1)
                    if bb > 0:
                        r = shoelace(b.polygon_points) / bb
                        poly_ratios.append(r)
                        poly_by_disc[b.discipline].append(r)
                else:
                    cnt["polygon_without_points"] += 1
            elif b.shape_type == "rectangle":
                cnt["rectangle"] += 1
            else:
                cnt["shape_other"] += 1
        if (k + 1) % 100 == 0:
            print(f"  {k+1}/{len(idx['documents'])} docs, {n_blocks} blocks, {time.time()-t0:.0f}s",
                  flush=True)

    pr = np.array(poly_ratios) if poly_ratios else np.array([])
    summary = {
        "n_graphic_blocks": n_blocks,
        "index_coverage": dict(idx_cover),
        "index_match": cnt["index_match"],
        "index_mismatch": cnt["index_mismatch"],
        "index_mismatch_share_of_covered": (cnt["index_mismatch"] /
                                            max(1, cnt["index_match"] + cnt["index_mismatch"])),
        "index_mismatch_max_abs_delta_px_median": float(np.median(mismatch_mag)) if mismatch_mag else None,
        "index_mismatch_max_abs_delta_px_p90": float(np.percentile(mismatch_mag, 90)) if mismatch_mag else None,
        "inside_page_px": cnt["inside_page_px"],
        "outside_page_px": cnt["outside_page_px"],
        "share_outside_page_px": cnt["outside_page_px"] / max(1, n_blocks),
        "no_page_px": cnt["no_page_px"],
        "degenerate_bbox": cnt["degenerate_bbox"],
        "shape": {"rectangle": cnt["rectangle"], "polygon": cnt["polygon"],
                  "other": cnt["shape_other"]},
        "share_polygon": cnt["polygon"] / max(1, n_blocks),
        "polygon_with_points": cnt["polygon_with_points"],
        "polygon_without_points": cnt["polygon_without_points"],
        "polygon_area_over_bbox": {
            "n": int(pr.size),
            "median": float(np.median(pr)) if pr.size else None,
            "mean": float(pr.mean()) if pr.size else None,
            "p10": float(np.percentile(pr, 10)) if pr.size else None,
            "p25": float(np.percentile(pr, 25)) if pr.size else None,
            "p90": float(np.percentile(pr, 90)) if pr.size else None,
            "share_below_0.9": float((pr < 0.9).mean()) if pr.size else None,
            "share_below_0.7": float((pr < 0.7).mean()) if pr.size else None,
            "share_below_0.5": float((pr < 0.5).mean()) if pr.size else None,
        },
        "polygon_share_by_discipline": {
            d: {"blocks": by_disc_blocks[d], "polygon": by_disc_poly.get(d, 0),
                "share": by_disc_poly.get(d, 0) / max(1, by_disc_blocks[d]),
                "median_area_ratio": float(np.median(poly_by_disc[d])) if poly_by_disc.get(d) else None}
            for d in sorted(by_disc_blocks)},
        "elapsed_s": round(time.time() - t0, 1),
    }
    (ART / "fnd_bbox_fidelity.json").write_text(json.dumps({
        "summary": summary,
        "index_mismatch_examples": mismatch_examples,
        "outside_page_examples": oob_examples,
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
