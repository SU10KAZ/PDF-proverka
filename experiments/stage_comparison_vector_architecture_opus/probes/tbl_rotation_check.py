"""tbl_rotation_check — do page.get_drawings() and page.get_text() share a coordinate frame?

PyMuPDF returns text in *rotated* page space (page.rect) but vector drawings in
*unrotated* space (mediabox/cropbox).  Any block extractor that clips both with the
same rectangle therefore reads geometry from a different part of a /Rotate page.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_rotation_check
"""
from __future__ import annotations

import glob
import json
import random
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"


def seg_points(drawings):
    pts = []
    for path in drawings:
        for item in path.get("items") or []:
            if item[0] == "l":
                pts.append((item[1].x, item[1].y))
                pts.append((item[2].x, item[2].y))
            elif item[0] == "re":
                r = item[1]
                pts.extend([(r.x0, r.y0), (r.x1, r.y1)])
    return pts


def main() -> None:
    report: dict = {}

    # --- 1. the exact block Track A used as one of its two change-recall pairs
    pairs = json.loads((ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json").read_text())["pairs"]
    pair = next(p for p in pairs if p["pair_id"] == "eom_singleline_changed")
    per_side = {}
    for side in ("left", "right"):
        doc = fitz.open(str(ROOT / pair[side]["pdf"]))
        page = doc[pair[side]["page_index"]]
        b = pair[side]["bbox_norm"]
        w, h = page.rect.width, page.rect.height
        displayed = fitz.Rect(b[0] * w, b[1] * h, b[2] * w, b[3] * h)   # what the crop PNG shows
        # PyMuPDF applies a clip in UNROTATED space, so passing `displayed` straight to
        # get_text/get_drawings samples this region of the sheet instead:
        sampled = fitz.Rect(displayed)
        if page.rotation:
            sampled = fitz.Rect(displayed) * page.rotation_matrix
            sampled.normalize()
        inter = fitz.Rect(displayed) & sampled
        inter_area = max(0.0, inter.width) * max(0.0, inter.height)
        union = displayed.get_area() + sampled.get_area() - inter_area
        dr = page.get_drawings()
        pts = seg_points(dr)
        m = page.rotation_matrix
        in_displayed = 0
        in_raw = 0
        for x, y in pts:
            if displayed.x0 <= x <= displayed.x1 and displayed.y0 <= y <= displayed.y1:
                in_raw += 1
            q = fitz.Point(x, y) * m
            if displayed.x0 <= q.x <= displayed.x1 and displayed.y0 <= q.y <= displayed.y1:
                in_displayed += 1
        raw_spans = page.get_text("dict", clip=displayed)
        n_raw = sum(len(l["spans"]) for bl in raw_spans["blocks"] if bl.get("type") == 0 for l in bl["lines"])
        fixed_clip = fitz.Rect(displayed) * page.derotation_matrix if page.rotation else displayed
        fixed_clip.normalize()
        fixed_spans = page.get_text("dict", clip=fixed_clip)
        n_fixed = sum(len(l["spans"]) for bl in fixed_spans["blocks"] if bl.get("type") == 0 for l in bl["lines"])
        raw_txt = {s["text"].strip() for bl in raw_spans["blocks"] if bl.get("type") == 0
                   for l in bl["lines"] for s in l["spans"] if s["text"].strip()}
        fixed_txt = {s["text"].strip() for bl in fixed_spans["blocks"] if bl.get("type") == 0
                     for l in bl["lines"] for s in l["spans"] if s["text"].strip()}
        per_side[side] = {
            "pdf": pair[side]["pdf"],
            "page_index": pair[side]["page_index"],
            "page_rotation": page.rotation,
            "page_rect": [round(w, 1), round(h, 1)],
            "mediabox": [round(page.mediabox.width, 1), round(page.mediabox.height, 1)],
            "displayed_block_rect": [round(v, 1) for v in displayed],
            "actually_sampled_rect_in_displayed_space": [round(v, 1) for v in sampled],
            "crop_region_iou": round(inter_area / union, 4) if union else None,
            "drawing_points_total": len(pts),
            "drawing_points_in_rect_AS_TRACK_A_DOES": in_raw,
            "drawing_points_in_rect_CORRECT": in_displayed,
            "text_spans_AS_TRACK_A_DOES": n_raw,
            "text_spans_CORRECT": n_fixed,
            "text_jaccard_track_a_vs_correct": round(
                len(raw_txt & fixed_txt) / max(1, len(raw_txt | fixed_txt)), 4),
            "seen_by_track_a_but_not_in_the_crop_image": sorted(raw_txt - fixed_txt),
            "visible_in_the_crop_image_but_missed_by_track_a": sorted(fixed_txt - raw_txt),
        }
        doc.close()
    report["eom_singleline_changed"] = per_side
    report["track_a_left_texts_in_description"] = 73
    report["track_a_right_texts_in_description"] = 299

    # --- 2. how common are rotated pages in the corpus
    pdfs = sorted(glob.glob(str(ROOT / "projects_v2/objects/*/disciplines/*/documents/*/versions/*/02_work/document.pdf")))
    random.seed(17)
    sample = random.sample(pdfs, min(120, len(pdfs)))
    rot_pages = 0
    total_pages = 0
    rot_docs = 0
    by_rot: dict[int, int] = {}
    for p in sample:
        try:
            doc = fitz.open(p)
        except Exception:
            continue
        doc_rot = False
        for page in doc:
            total_pages += 1
            by_rot[page.rotation] = by_rot.get(page.rotation, 0) + 1
            if page.rotation:
                rot_pages += 1
                doc_rot = True
        rot_docs += 1 if doc_rot else 0
        doc.close()
    report["corpus_rotation_sample"] = {
        "pdfs_sampled": len(sample),
        "pdfs_total_available": len(pdfs),
        "pages_scanned": total_pages,
        "pages_rotated": rot_pages,
        "pages_rotated_pct": round(100.0 * rot_pages / max(total_pages, 1), 2),
        "documents_with_any_rotated_page": rot_docs,
        "pages_by_rotation": dict(sorted(by_rot.items())),
    }

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "tbl_rotation_check.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    print(json.dumps(report, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
