#!/usr/bin/env python3
"""signoise probe 5 — verdict stability under bbox perturbation.

Re-extracts the RIGHT block of three benchmark pairs with the crop bbox shifted by 0.5 % / 2 %
of the block size and with a 2 % scale change, then re-runs the Track A comparator against the
UNCHANGED left description and reports how the status and every similarity score move.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_05_perturbation

Writes artifacts/signoise_05_perturbation.{json,md}.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from experiments.stage_comparison_vector_blocks import comparator as C
from experiments.stage_comparison_vector_blocks import extractor as E

ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = Path(__file__).resolve().parents[1] / "artifacts"

TARGET_PAIRS = ("ss_simple_node", "ss_scheme_text_changed", "eom_singleline_changed", "ss_table_graphic")
# mode "cross": left(unchanged) vs right(perturbed crop)  -> stability of a real verdict
# mode "self":  left(unchanged) vs left(perturbed crop)   -> pure false-positive noise floor


def clamp(bbox: list[float]) -> list[float]:
    x0, y0, x1, y1 = bbox
    return [max(0.0, min(x0, 0.999)), max(0.0, min(y0, 0.999)),
            min(1.0, max(x1, 0.001)), min(1.0, max(y1, 0.001))]


def perturb(bbox: list[float], kind: str) -> list[float]:
    x0, y0, x1, y1 = bbox
    w, h = x1 - x0, y1 - y0
    if kind == "identity":
        return list(bbox)
    if kind.startswith("shift_"):
        fraction = float(kind.split("_")[1]) / 100.0
        dx, dy = w * fraction, h * fraction
        return clamp([x0 + dx, y0 + dy, x1 + dx, y1 + dy])
    if kind.startswith("scale_"):
        fraction = float(kind.split("_")[1]) / 100.0
        cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
        nw, nh = w * (1 + fraction) / 2, h * (1 + fraction) / 2
        return clamp([cx - nw, cy - nh, cx + nw, cy + nh])
    raise ValueError(kind)


PERTURBATIONS = ("identity", "shift_0.5", "shift_2", "scale_2", "scale_-2")


def summarise(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": result["status"],
        "geometry_similarity": result["geometry"]["similarity"],
        "selected_tolerance": result["geometry"]["selected_tolerance"],
        "left_coverage": result["geometry"]["left_coverage"],
        "right_coverage": result["geometry"]["right_coverage"],
        "text_similarity": result["text"]["effective_similarity"],
        "text_multiset_similarity": result["text"]["similarity"],
        "topology_similarity": result["topology"]["similarity"],
        "patterns_similarity": result["repeated_patterns"]["similarity"],
        "exact_sig_equal": result["exact_vector_signature_equal"],
        "normalized_sig_equal": result["normalized_signature_equal"],
        "structural_sig_equal": result["structural_signature_equal"],
        "differences": result["differences"],
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pairs = {p["pair_id"]: p for p in json.loads((BASE / "block_pairs.json").read_text())["pairs"]}
    results: dict[str, Any] = {}
    for mode in ("cross", "self"):
      for pair_id in TARGET_PAIRS:
        spec = pairs[pair_id]
        side = "right" if mode == "cross" else "left"
        left = json.loads((BASE / "descriptions" / pair_id / "left" / "vector_block.json").read_text())
        base_bbox = list(spec[side]["bbox_norm"])
        rows = {}
        for kind in PERTURBATIONS:
            bbox = perturb(base_bbox, kind)
            started = time.time()
            right = E.extract_block(
                ROOT / spec[side]["pdf"],
                page_index=spec[side]["page_index"],
                bbox_norm=bbox,
                block_id=spec[side]["block_id"] + "_" + kind,
            )
            extract_seconds = time.time() - started
            summary = summarise(C.compare_descriptions(left, right))
            summary["bbox_norm"] = [round(v, 6) for v in bbox]
            summary["extract_seconds"] = round(extract_seconds, 2)
            summary["right_primitive_count"] = right["primitive_summary"]["primitive_count"]
            summary["right_segment_count"] = right["primitive_summary"]["total_segment_count"]
            summary["right_text_items"] = right["primitive_summary"]["text_items"]
            summary["right_vector_quality"] = right["vector_quality"]
            rows[kind] = summary
            summary["difference_line_count"] = len(summary["differences"])
            print(f"{mode:5s} {pair_id:24s} {kind:10s} {summary['status']:30s} "
                  f"geo={summary['geometry_similarity']:.4f} txt={summary['text_similarity']:.4f} "
                  f"top={summary['topology_similarity']:.4f} ({extract_seconds:.1f}s)")
            del right
        results[f"{mode}/{pair_id}"] = {
            "mode": mode,
            "human_expected": spec["human_expected"] if mode == "cross" else "IDENTICAL (same PDF, same block)",
            "base_bbox_norm": [round(v, 6) for v in base_bbox],
            "perturbations": rows,
        }
        del left

    payload = {"probe": "signoise_05_perturbation", "research_only": True,
               "perturbations": list(PERTURBATIONS), "results": results}
    (OUT / "signoise_05_perturbation.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

    lines = [
        "# signoise probe 5 — verdict stability under 0.5 % / 2 % bbox perturbation",
        "",
        "Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_05_perturbation`",
        "",
        "The RIGHT block is re-extracted with a perturbed crop; LEFT is the unchanged Track A description.",
        "",
        "`cross` = left vs perturbed right (real pair). `self` = left vs perturbed LEFT, i.e. the same "
        "block of the same PDF compared with itself under crop jitter — the pure false-positive floor.",
        "",
        "| mode/pair | perturbation | status | diff lines | geometry | text | topology | patterns | prims | segs | texts |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for pair_id, block in results.items():
        for kind, row in block["perturbations"].items():
            lines.append(
                f"| {pair_id} | `{kind}` | **{row['status']}** | {row['difference_line_count']} | {row['geometry_similarity']:.4f} | "
                f"{row['text_similarity']:.4f} | {row['topology_similarity']:.4f} | "
                f"{row['patterns_similarity']:.4f} | {row['right_primitive_count']} | "
                f"{row['right_segment_count']} | {row['right_text_items']} |"
            )
    (OUT / "signoise_05_perturbation.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("written:", OUT / "signoise_05_perturbation.json")


if __name__ == "__main__":
    main()
