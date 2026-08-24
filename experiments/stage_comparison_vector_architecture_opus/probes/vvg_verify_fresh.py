"""VVG — run the ARM-1 vision verifier on the 14 fresh blocks extracted here.

These are real, unmutated descriptions of blocks that are NOT in the Track A benchmark,
so they are the out-of-sample label set for the gate.

Rotation: `vv_harness.render_crop` renders the DISPLAY frame while `extract_block`
reads the MEDIABOX frame (finding O13).  On a rotated page the two disagree, so this
script re-renders those crops with the page rotation cleared, which is the region the
description actually describes.

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_verify_fresh
"""
from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fitz

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments" / "stage_comparison_vector_architecture_opus" / "artifacts"
FRESH = ART / "vvg_fresh"
RUNS = ART / "vvg_runs"


def derotated_crop(pdf: Path, page_index: int, bbox_norm, out_png: Path, zoom: float = 1.35) -> dict:
    doc = fitz.open(pdf)
    page = doc[page_index]
    rotation = page.rotation
    rect = [bbox_norm[0] * page.rect.width, bbox_norm[1] * page.rect.height,
            bbox_norm[2] * page.rect.width, bbox_norm[3] * page.rect.height]
    if rotation:
        page.set_rotation(0)
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=fitz.Rect(*rect), alpha=False)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    pix.save(out_png)
    doc.close()
    return {"png": str(out_png), "page_rotation": rotation, "pixels": [pix.width, pix.height],
            "bytes": out_png.stat().st_size, "derotated": bool(rotation)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--only", nargs="*")
    args = ap.parse_args()

    RUNS.mkdir(parents=True, exist_ok=True)
    index = json.loads((ART / "vvg_fresh_index.json").read_text(encoding="utf-8"))["blocks"]
    crop_meta = {}
    jobs = []
    for block in index:
        if args.only and block["id"] not in args.only:
            continue
        out = RUNS / f"{block['id']}.json"
        if out.exists():
            print(f"skip {block['id']} (already run)")
            continue
        description = json.loads((ROOT / block["description"]).read_text(encoding="utf-8"))
        sheet = vv.fact_sheet(description, disclose_limits=True)
        png = FRESH / f"{block['id']}.rotfix.png"
        meta = derotated_crop(ROOT / block["pdf"], block["page_index"], block["bbox_norm"], png)
        crop_meta[block["id"]] = meta
        jobs.append((block, sheet, png, out))

    if crop_meta:
        (ART / "vvg_fresh_crops.json").write_text(
            json.dumps(crop_meta, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

    def run(job):
        block, sheet, png, out = job
        record = vv.verify(png, sheet, timeout=420, retries=1)
        record["block"] = block["id"]
        record["discipline"] = block["discipline"]
        record["family"] = "fresh_control"
        record["mutation"] = "clean"
        record["fact_sheet"] = sheet
        out.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"{block['id']}: {record.get('status')} "
              f"payload={record.get('usage_payload_attributable')} "
              f"{record.get('wall_seconds')}s", flush=True)
        return record

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        list(pool.map(run, jobs))
    print("done")


if __name__ == "__main__":
    main()
