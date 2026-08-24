"""VVG — extract fresh blocks myself with Track A's extractor, and render their crops.

ARM 3, Track B (Opus). Research only.

Block selection reuses the bboxes mined by the sibling Track B agent `fmc`
(artifacts/fmc_pairs.json); the extraction itself is run here, from the PDF,
so the descriptions are mine.

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_fresh_extract
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from experiments.stage_comparison_vector_blocks import extractor as ex
from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments" / "stage_comparison_vector_architecture_opus" / "artifacts"
OUT = ART / "vvg_fresh"

WANTED = [
    "fmc_eom_text_as_paths",
    "fmc_kj_spec_table_reflow",
    "fmc_eom_room_schedule_values",
    "fmc_eom_tray_plan_geometry",
    "fmc_ov_raster_retile",
    "fmc_tx_raster_scan",
    "fmc_ss_a4_to_a3_reissue",
    "fmc_ov_block_split_widened",
    "fmc_gp_section_hatch_dims",
    "fmc_ar_hatch_sections",
    "fmc_vk_spec_positions",
    "fmc_km_broken_text_swap",
    "fmc_eom_rotated_labels",
    "fmc_eom_cable_table_values",
]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pairs = {p["pair_id"]: p for p in json.loads((ART / "fmc_pairs.json").read_text(encoding="utf-8"))["pairs"]}
    index = []
    for pid in WANTED:
        pair = pairs[pid]
        side = pair["left"]
        name = f"{pid}__left"
        desc_path = OUT / f"{name}.json"
        png_path = OUT / f"{name}.png"
        started = time.time()
        if desc_path.exists():
            description = json.loads(desc_path.read_text(encoding="utf-8"))
        else:
            description = ex.extract_block(
                ROOT / side["pdf"],
                page_index=side["page_index"],
                bbox_norm=side["bbox_norm"],
                block_id=name,
            )
            desc_path.write_text(json.dumps(description, ensure_ascii=False, default=str) + "\n",
                                 encoding="utf-8")
        if not png_path.exists():
            vv.render_crop(ROOT / side["pdf"], side["page_index"], side["bbox_norm"], png_path)
        took = time.time() - started
        row = {
            "id": name,
            "pair_id": pid,
            "discipline": pair["discipline"],
            "pdf": side["pdf"],
            "page_index": side["page_index"],
            "bbox_norm": side["bbox_norm"],
            "description": str(desc_path.relative_to(ROOT)),
            "crop_png": str(png_path.relative_to(ROOT)),
            "crop_bytes": png_path.stat().st_size,
            "segments": description["primitive_summary"]["total_segment_count"],
            "texts": description["primitive_summary"]["text_items"],
            "quality": description["vector_quality"],
            "extract_seconds": round(took, 2),
        }
        index.append(row)
        print(json.dumps(row, ensure_ascii=False))
    (ART / "vvg_fresh_index.json").write_text(
        json.dumps({"blocks": index}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {ART / 'vvg_fresh_index.json'} ({len(index)} blocks)")


if __name__ == "__main__":
    main()
