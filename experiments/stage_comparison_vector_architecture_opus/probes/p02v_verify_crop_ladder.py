"""p02v_ VERIFIER: independent re-run of the falsify_ (p02_counts) crop-size ladder
plus the negative control the original probe did not run.

Writes ONLY to artifacts/p02v_*.json.  Does not touch falsify_* artifacts.
"""
from __future__ import annotations
import json, random, sys, time
from pathlib import Path
import fitz
from experiments.stage_comparison_vector_blocks import comparator, extractor

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"

SOT_L = "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v001/02_work/document.pdf"
SOT_R = "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v002/02_work/document.pdf"
PAGE = 8
TIGHT = [0.18765959235661997, 0.19280929695823926, 0.2521670522661555, 0.2923199413194866]


def run_pair(lpdf, rpdf, page, bbox, tag):
    t0 = time.time()
    L = extractor.extract_block(ROOT / lpdf, page_index=page, bbox_norm=bbox, block_id=tag + "_L")
    R = extractor.extract_block(ROOT / rpdf, page_index=page, bbox_norm=bbox, block_id=tag + "_R")
    c = comparator.compare_descriptions(L, R)
    return {
        "tag": tag,
        "bbox": [round(v, 6) for v in bbox],
        "status": c["status"],
        "geom": round(c["geometry"]["similarity"], 6),
        "topo": round(c["topology"]["similarity"], 6),
        "tol": c["geometry"]["selected_tolerance"],
        "prims": [L["primitive_summary"]["primitive_count"], R["primitive_summary"]["primitive_count"]],
        "segs": [L["primitive_summary"]["total_segment_count"], R["primitive_summary"]["total_segment_count"]],
        "diffs": c["differences"][:6],
        "secs": round(time.time() - t0, 1),
    }


def grow(bbox, k):
    cx, cy = (bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2
    w, h = (bbox[2] - bbox[0]) * k / 2, (bbox[3] - bbox[1]) * k / 2
    return [max(0.0, cx - w), max(0.0, cy - h), min(1.0, cx + w), min(1.0, cy + h)]


def area_pct(bbox, pw, ph):
    return (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) * 100.0


def main():
    out = {"repro": [], "controls": [], "geometry": {}}
    doc = fitz.open(ROOT / SOT_L)
    pw, ph = doc[PAGE].rect.width, doc[PAGE].rect.height
    doc.close()

    # --- 1. reproduce the ladder ---
    ladders = {"tight": grow(TIGHT, 1.0), "x3": grow(TIGHT, 3.0), "x8": grow(TIGHT, 8.0),
               "x20": grow(TIGHT, 20.0), "wholepage": [0.001, 0.001, 0.999, 0.999]}
    for name, bb in ladders.items():
        r = run_pair(SOT_L, SOT_R, PAGE, bb, "repro_" + name)
        r["area_pct_of_page"] = round(area_pct(bb, pw, ph), 4)
        r["area_x_of_tight"] = round(area_pct(bb, pw, ph) / area_pct(ladders["tight"], pw, ph), 1)
        out["repro"].append(r)
        print(name, r["status"], r["geom"], "area x tight =", r["area_x_of_tight"], flush=True)

    # --- 2. NEGATIVE CONTROL: same-size crops on UNCHANGED parts of the same page pair ---
    # the raster diff found only 2 dirty regions; everything else is pixel-identical.
    dirty = [[0.1877, 0.1928, 0.2522, 0.2923], [0.1261, 0.1928, 0.1818, 0.2737]]

    def overlaps(bb):
        for d in dirty:
            if not (bb[2] < d[0] or bb[0] > d[2] or bb[3] < d[1] or bb[1] > d[3]):
                return True
        return False

    rnd = random.Random(20260823)
    tw, th = TIGHT[2] - TIGHT[0], TIGHT[3] - TIGHT[1]
    for scale, n in ((1.0, 12), (3.0, 6)):
        w, h = tw * scale, th * scale
        placed = 0
        tries = 0
        while placed < n and tries < 400:
            tries += 1
            x0 = rnd.uniform(0.02, max(0.02, 0.98 - w))
            y0 = rnd.uniform(0.02, max(0.02, 0.98 - h))
            bb = [x0, y0, min(1.0, x0 + w), min(1.0, y0 + h)]
            if overlaps(bb):
                continue
            r = run_pair(SOT_L, SOT_R, PAGE, bb, f"ctrl_x{scale:g}_{placed}")
            r["scale"] = scale
            if r["segs"][0] < 20:      # empty white area: not informative
                r["skipped_empty"] = True
            out["controls"].append(r)
            print("ctrl", scale, placed, r["status"], r["geom"], "segs", r["segs"], flush=True)
            placed += 1

    (ART / "p02v_crop_ladder_and_controls.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print("written")


if __name__ == "__main__":
    main()
