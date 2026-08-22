#!/usr/bin/env python3
"""FMC probe step 9 — run the Track A v0.1 extractor + comparator over the FMC failure-mode corpus.

Track A's own run_research.py writes into experiments/stage_comparison_vector_blocks/artifacts/,
which this audit must not touch, so this runner imports extract_block / compare_descriptions
directly and writes every artifact under the Track B directory instead.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_run_v01 [--only pair_id,...]
"""
from __future__ import annotations

import argparse
import json
import time
import traceback
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes.fmc_io import exists as read_exists, read_json
from experiments.stage_comparison_vector_blocks.comparator import compare_descriptions
from experiments.stage_comparison_vector_blocks.extractor import extract_block

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"
DESC = ART / "fmc_descriptions"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--reuse", action="store_true")
    a = ap.parse_args()
    manifest = json.loads((ART / "fmc_pairs.json").read_text(encoding="utf-8"))
    only = {x for x in a.only.split(",") if x}
    DESC.mkdir(parents=True, exist_ok=True)
    results = []
    for pair in manifest["pairs"]:
        pid = pair["pair_id"]
        if only and pid not in only:
            continue
        row = {
            "pair_id": pid,
            "discipline": pair["discipline"],
            "change_class": pair["change_class"],
            "human_expected": pair["human_expected"],
            "human_expected_ru": pair["human_expected_ru"],
        }
        sides = {}
        ok = True
        for name in ("left", "right"):
            side = pair[name]
            cache = DESC / f"{pid}_{name}.json"
            if a.reuse and read_exists(cache):
                sides[name] = read_json(cache)
                row[f"{name}_seconds"] = 0.0
                continue
            t0 = time.perf_counter()
            try:
                d = extract_block(
                    ROOT / side["pdf"],
                    page_index=side["page_index"],
                    bbox_norm=side["bbox_norm"],
                    block_id=side["block_id"],
                )
            except Exception as exc:  # pragma: no cover
                row[f"{name}_error"] = f"{type(exc).__name__}: {exc}"
                row["traceback"] = traceback.format_exc()[-800:]
                ok = False
                break
            row[f"{name}_seconds"] = round(time.perf_counter() - t0, 3)
            cache.write_text(json.dumps(d, ensure_ascii=False), encoding="utf-8")
            sides[name] = d
        if not ok:
            results.append(row)
            print(json.dumps(row, ensure_ascii=False)[:400])
            continue
        left, right = sides["left"], sides["right"]
        t0 = time.perf_counter()
        cmp = compare_descriptions(left, right)
        row["compare_seconds"] = round(time.perf_counter() - t0, 3)
        (DESC / f"{pid}_comparison.json").write_text(json.dumps(cmp, ensure_ascii=False), encoding="utf-8")
        row.update(
            {
                "status": cmp["status"],
                "geometry_similarity": round(cmp["geometry"]["similarity"], 4),
                "selected_tolerance": cmp["geometry"]["selected_tolerance"],
                "left_coverage": round(cmp["geometry"]["left_coverage"], 4),
                "right_coverage": round(cmp["geometry"]["right_coverage"], 4),
                "text_similarity": round(cmp["text"]["effective_similarity"], 4),
                "text_reliable": cmp["text"]["reliable"],
                "topology_similarity": round(cmp["topology"]["similarity"], 4),
                "left_quality": left["vector_quality"],
                "right_quality": right["vector_quality"],
                "left_primitives": left["primitive_summary"]["primitive_count"],
                "right_primitives": right["primitive_summary"]["primitive_count"],
                "left_segments": left["topology"]["segments_total"],
                "right_segments": right["topology"]["segments_total"],
                "left_texts": len(left["texts"]),
                "right_texts": len(right["texts"]),
                "left_hatch": len(left["hatch_like_structures"]),
                "right_hatch": len(right["hatch_like_structures"]),
                "differences": cmp["differences"],
                "n_differences": len(cmp["differences"]),
                "text_added": cmp["text"]["added"][:15],
                "text_removed": cmp["text"]["removed"][:15],
                "text_value_changes": cmp["text"]["value_changes"][:15],
                "l3_tokens": [
                    left["size_metrics"]["level_3_compact_description"]["estimated_tokens"],
                    right["size_metrics"]["level_3_compact_description"]["estimated_tokens"],
                ],
                "l0_tokens": [
                    left["size_metrics"]["level_0_raw_vector"]["estimated_tokens"],
                    right["size_metrics"]["level_0_raw_vector"]["estimated_tokens"],
                ],
            }
        )
        results.append(row)
        print(
            f"{pid:34} {row['status']:30} geom={row['geometry_similarity']:.3f} "
            f"txt={row['text_similarity']:.3f} topo={row['topology_similarity']:.3f} "
            f"({row['left_seconds']}s/{row['right_seconds']}s)"
        )
    out = ART / "fmc_v01_results.json"
    prev = []
    if a.only and out.is_file():
        prev = [r for r in json.loads(out.read_text(encoding="utf-8")) if r["pair_id"] not in only]
    out.write_text(json.dumps(prev + results, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"wrote {out} ({len(prev)+len(results)} rows)")


if __name__ == "__main__":
    main()
