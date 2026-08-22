#!/usr/bin/env python3
"""TCF probe 0 — inventory of stored topology blocks (Track A descriptions).

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_inventory
"""
from __future__ import annotations

import json
import pathlib

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_inventory.json")


def main() -> None:
    rows = []
    for pair_dir in sorted(ROOT.iterdir()):
        for side in ("left", "right"):
            path = pair_dir / side / "vector_block.json"
            if not path.exists():
                continue
            d = json.loads(path.read_text(encoding="utf-8"))
            t = d["topology"]
            rows.append(
                {
                    "pair": pair_dir.name,
                    "side": side,
                    "json_mb": round(path.stat().st_size / 1e6, 2),
                    "quality": d["vector_quality"],
                    "primitives": len(d["geometry"]["primitives"]),
                    "primitives_uncapped": d["geometry"]["extraction"]["primitives_uncapped"],
                    "storage_capped": d["geometry"]["extraction"]["storage_capped"],
                    "segments_total": t["segments_total"],
                    "segments_used": t["segments_used"],
                    "segments_capped": t["segments_capped"],
                    "node_count": t["node_count"],
                    "edge_count": t["edge_count"],
                    "components": t["connected_components"],
                    "endpoints": t["endpoints"],
                    "branch_points": t["branch_points"],
                    "t_junctions": t["t_junctions"],
                    "x_crossings": t["x_crossings_unconnected"],
                    "crossings_truncated": t["crossings_truncated"],
                    "closed_contours": t["closed_contours"],
                    "nested_contours": t["nested_contours"],
                    "texts": len(d["texts"]),
                }
            )
    OUT.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    hdr = ["pair", "side", "json_mb", "quality", "primitives", "segments_total", "segments_used",
           "segments_capped", "node_count", "edge_count", "components", "endpoints",
           "branch_points", "t_junctions", "x_crossings", "crossings_truncated",
           "closed_contours", "nested_contours"]
    print("\t".join(hdr))
    for r in rows:
        print("\t".join(str(r[k]) for k in hdr))


if __name__ == "__main__":
    main()
