"""VVG — raster-content signal: how much of the block is a bitmap the vector layer cannot see.

Zero model calls: `page.get_image_info()` plus a rectangle intersection.
Motivation: on the fresh blocks the verifier's FAILED verdicts and most of its
`missing[]` items point at content that has no vector representation at all —
scans, photographs, logos, handwritten signatures.

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_raster_signal
"""
from __future__ import annotations

import json
from pathlib import Path

import fitz

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments" / "stage_comparison_vector_architecture_opus" / "artifacts"
TRACK_A_DESC = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts" / "descriptions"


def raster_for(desc: dict) -> dict:
    doc = fitz.open(desc["source"]["pdf"])
    page = doc[desc["page_index"]]
    b = desc["bbox_norm_on_page"]
    rect = fitz.Rect(b[0] * page.rect.width, b[1] * page.rect.height,
                     b[2] * page.rect.width, b[3] * page.rect.height)
    area = rect.get_area() or 1.0
    covered = 0.0
    count = 0
    for info in page.get_image_info():
        ib = fitz.Rect(info["bbox"])
        inter = ib & rect
        if inter.get_area() > 0:
            count += 1
            covered += inter.get_area()
    doc.close()
    return {
        "raster_images_in_block": count,
        "raster_area_share": round(min(covered / area, 1.0), 6),
        "has_raster": bool(count > 0),
    }


def main() -> None:
    out = {}
    for pair_dir in sorted(TRACK_A_DESC.iterdir()):
        if not pair_dir.is_dir():
            continue
        for side in ("left", "right"):
            p = pair_dir / side / "vector_block.json"
            if p.exists():
                out[f"{pair_dir.name}:{side}"] = {"set": "track_a",
                                                  **raster_for(json.loads(p.read_text(encoding="utf-8")))}
    for block in json.loads((ART / "vvg_fresh_index.json").read_text(encoding="utf-8"))["blocks"]:
        d = json.loads((ROOT / block["description"]).read_text(encoding="utf-8"))
        out[block["id"]] = {"set": "fresh", **raster_for(d)}
    n = sum(1 for v in out.values() if v["has_raster"])
    summary = {"blocks": len(out), "blocks_with_raster": n,
               "share": round(n / len(out), 4),
               "track_a": sum(1 for v in out.values() if v["set"] == "track_a" and v["has_raster"]),
               "fresh": sum(1 for v in out.values() if v["set"] == "fresh" and v["has_raster"])}
    (ART / "vvg_raster_signal.json").write_text(
        json.dumps({"summary": summary, "blocks": out}, ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    for k, v in sorted(out.items(), key=lambda kv: -kv[1]["raster_area_share"]):
        if v["has_raster"]:
            print(f"  {k:<40} images={v['raster_images_in_block']:>3} area_share={v['raster_area_share']}")


if __name__ == "__main__":
    main()
