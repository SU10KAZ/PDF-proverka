#!/usr/bin/env python3
"""Extract five FRESH blocks (both versions) with Track A's unmodified extractor.

The blocks were chosen by eye from rendered pages; the bboxes are recorded here so the
measurement is reproducible.  Run from the repository root:

    python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_extract_fresh
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from experiments.stage_comparison_vector_blocks import extractor as EX

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/txgeo_fresh_descriptions"

AR = "projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.1-К2/versions/{v}/02_work/document.pdf"
KJ = "projects_v2/objects/214_Alia_ASTERUS/disciplines/KJ/documents/13АВ-РД-КЖ5.1-К1К2/versions/{v}/02_work/document.pdf"
OV = "projects_v2/objects/214_Alia_ASTERUS/disciplines/OV/documents/13АВ-РД-ОВ2-К2 V1/versions/{v}/02_work/document.pdf"

BLOCKS = [
    # id, discipline, what it is, pdf template, page, bbox_norm, versions
    ("fresh_ar_lintels", "AR", "Перемычки 2 этаж — detail sections with dimension lines and leaders",
     AR, 5, [0.639, 0.009, 0.807, 0.315], ("v001", "v002")),
    ("fresh_ar_legend", "AR", "Условные обозначения — legend: repeated symbol + text",
     AR, 5, [0.449, 0.768, 0.752, 0.917], ("v001", "v002")),
    ("fresh_kj_sections", "KJ", "Разрезы 1-1/2-2/3-3 опалубки — sections with dimensions and leaders",
     KJ, 9, [0.716, 0.319, 0.864, 0.649], ("v001", "v002")),
    ("fresh_kj_plan_part", "KJ", "Фрагмент плана опалубки — dimension chains and «по N» leaders",
     KJ, 9, [0.236, 0.256, 0.554, 0.560], ("v001", "v002")),
    ("fresh_ov_spec_table", "OV", "Характеристика систем вентиляции — pure table",
     OV, 5, [0.044, 0.008, 0.978, 0.753], ("v001", "v002")),
]


def main() -> None:
    manifest = []
    for block_id, disc, what, template, page, bbox, versions in BLOCKS:
        for side, version in zip(("left", "right"), versions):
            pdf = ROOT / template.format(v=version)
            started = time.time()
            desc = EX.extract_block(
                pdf, page_index=page, bbox_norm=bbox, block_id=f"{block_id}_{version}"
            )
            target = OUT / block_id / side / "vector_block.json"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(desc, ensure_ascii=False), encoding="utf-8")
            took = time.time() - started
            print(f"{block_id:22s} {side:6s} {version} texts={len(desc['texts']):5d} "
                  f"prims={len(desc['geometry']['primitives']):6d} q={desc['vector_quality']:22s} {took:6.2f}s",
                  flush=True)
            manifest.append({
                "block_id": block_id, "discipline": disc, "what": what, "side": side,
                "version": version, "pdf": str(pdf.relative_to(ROOT)), "page_index": page,
                "bbox_norm": bbox, "texts": len(desc["texts"]),
                "primitives": len(desc["geometry"]["primitives"]),
                "vector_quality": desc["vector_quality"], "extract_seconds": round(took, 2),
            })
    (OUT / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
