"""falsify_ probe harness: run Track A v0.1 extractor+comparator on arbitrary
(pdf, page_index, bbox_norm) block pairs and render raster crops for human check.

Read-only with respect to Track A: imports its modules, edits nothing.

Usage (from repo root):
    python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_harness cases.json
where cases.json is {"cases": [{"case_id":..., "note":..., "left":{...}, "right":{...}}]}
and each side is {"pdf":..., "page_index":int, "bbox_norm":[x0,y0,x1,y1]}.

Outputs artifacts/falsify_cases/<case_id>/{left.png,right.png,comparison.json}
and a roll-up artifacts/falsify_case_results.json
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import fitz

from experiments.stage_comparison_vector_blocks import comparator, extractor

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"


def render_crop(pdf: str, page_index: int, bbox_norm, out_png: Path, dpi: int = 150) -> None:
    doc = fitz.open(ROOT / pdf if not Path(pdf).is_absolute() else pdf)
    page = doc[page_index]
    rect = fitz.Rect(
        bbox_norm[0] * page.rect.width,
        bbox_norm[1] * page.rect.height,
        bbox_norm[2] * page.rect.width,
        bbox_norm[3] * page.rect.height,
    )
    zoom = dpi / 72.0
    # keep the longest side under 2200 px
    longest = max(rect.width, rect.height) * zoom
    if longest > 2200:
        zoom *= 2200 / longest
    elif longest < 500:  # tiny symbol blocks must still be human-checkable
        zoom *= 500 / max(longest, 1e-6)
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=rect)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    pix.save(out_png)
    doc.close()


def describe(side: dict, block_id: str) -> dict:
    pdf = side["pdf"]
    path = ROOT / pdf if not Path(pdf).is_absolute() else Path(pdf)
    return extractor.extract_block(
        path,
        page_index=side["page_index"],
        bbox_norm=side["bbox_norm"],
        block_id=block_id,
    )


def compact_row(case_id: str, note: str, cmp_: dict, left: dict, right: dict) -> dict:
    return {
        "case_id": case_id,
        "note": note,
        "status": cmp_["status"],
        "geometry_similarity": cmp_["geometry"]["similarity"],
        "geometry_left_cov": cmp_["geometry"]["left_coverage"],
        "geometry_right_cov": cmp_["geometry"]["right_coverage"],
        "selected_tolerance": cmp_["geometry"]["selected_tolerance"],
        "text_effective_similarity": cmp_["text"]["effective_similarity"],
        "text_reliable": cmp_["text"]["reliable"],
        "topology_similarity": cmp_["topology"]["similarity"],
        "patterns_similarity": cmp_["repeated_patterns"].get("similarity"),
        "level3_signature_equal": cmp_["structural_signature_equal"],
        "level2_signature_equal": cmp_["normalized_signature_equal"],
        "counts_left": {
            "primitives": left["primitive_summary"]["primitive_count"],
            "segments": left["primitive_summary"]["total_segment_count"],
            "texts": left["primitive_summary"]["text_items"],
            "components": left["topology"]["connected_components"],
            "endpoints": left["topology"]["endpoints"],
            "branch_points": left["topology"]["branch_points"],
            "closed_contours": left["topology"]["closed_contours"],
            "repeated_motifs": len(left["repeated_elements"]),
            "quality": left["vector_quality"],
        },
        "counts_right": {
            "primitives": right["primitive_summary"]["primitive_count"],
            "segments": right["primitive_summary"]["total_segment_count"],
            "texts": right["primitive_summary"]["text_items"],
            "components": right["topology"]["connected_components"],
            "endpoints": right["topology"]["endpoints"],
            "branch_points": right["topology"]["branch_points"],
            "closed_contours": right["topology"]["closed_contours"],
            "repeated_motifs": len(right["repeated_elements"]),
            "quality": right["vector_quality"],
        },
        "differences": cmp_["differences"][:12],
    }


def run(cases_path: Path, out_name: str = "falsify_case_results.json") -> None:
    payload = json.loads(cases_path.read_text(encoding="utf-8"))
    rows = []
    for case in payload["cases"]:
        cid = case["case_id"]
        started = time.time()
        outdir = ART / "falsify_cases" / cid
        outdir.mkdir(parents=True, exist_ok=True)
        left = describe(case["left"], cid + "_L")
        right = describe(case["right"], cid + "_R")
        cmp_ = comparator.compare_descriptions(left, right)
        render_crop(case["left"]["pdf"], case["left"]["page_index"], case["left"]["bbox_norm"], outdir / "left.png")
        render_crop(case["right"]["pdf"], case["right"]["page_index"], case["right"]["bbox_norm"], outdir / "right.png")
        (outdir / "comparison.json").write_text(
            json.dumps(cmp_, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        (outdir / "left_texts.json").write_text(
            json.dumps([t["text"] for t in left["texts"]], ensure_ascii=False), encoding="utf-8"
        )
        (outdir / "right_texts.json").write_text(
            json.dumps([t["text"] for t in right["texts"]], ensure_ascii=False), encoding="utf-8"
        )
        row = compact_row(cid, case.get("note", ""), cmp_, left, right)
        row["elapsed_s"] = round(time.time() - started, 1)
        row["source"] = {"left": case["left"], "right": case["right"]}
        rows.append(row)
        print(
            "%-34s %-28s geom=%.4f text=%.3f topo=%.3f  %5.1fs"
            % (cid, row["status"], row["geometry_similarity"], row["text_effective_similarity"],
               row["topology_similarity"], row["elapsed_s"]),
            flush=True,
        )
    (ART / out_name).write_text(json.dumps({"cases": rows}, ensure_ascii=False, indent=1), encoding="utf-8")
    print("wrote", ART / out_name)


if __name__ == "__main__":
    run(Path(sys.argv[1]), sys.argv[2] if len(sys.argv) > 2 else "falsify_case_results.json")
