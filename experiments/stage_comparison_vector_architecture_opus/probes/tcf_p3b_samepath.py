#!/usr/bin/env python3
"""TCF probe 3b — how many "X crossings" are internal to one drawn object?

`_topology` skips a crossing only when both segments carry the same `primitive_id`.
`_drawing_primitives` splits one PDF path into many primitives whenever the path is
not a pure polyline, so a single drawn object (a device symbol, a pipe fitting
outline, a hatch patch) yields several primitives and its self-intersections are
counted as "unconnected X crossings".  Sharing a `drawing_index` means sharing one
PDF path, i.e. one drawn object.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p3b_samepath
"""
from __future__ import annotations

import json
import pathlib

from experiments.stage_comparison_vector_architecture_opus.probes import tcf_topo

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p3b_same_path_crossings.json")


def main() -> None:
    out = {}
    for pair_dir in sorted(ROOT.iterdir()):
        d = json.loads((pair_dir / "left" / "vector_block.json").read_text(encoding="utf-8"))
        prims = {p["id"]: p for p in d["geometry"]["primitives"]}
        topo = tcf_topo.topology(d["geometry"]["primitives"], 0.0025, 8_000, keep_crossings=True)
        records = topo["_crossings"]
        same = sum(
            1
            for r in records
            if prims[r["primitive_ids"][0]]["drawing_index"] == prims[r["primitive_ids"][1]]["drawing_index"]
        )
        drawings = {p["drawing_index"] for p in d["geometry"]["primitives"]}
        out[pair_dir.name] = {
            "primitives": len(prims),
            "distinct_pdf_paths": len(drawings),
            "primitives_per_pdf_path": round(len(prims) / max(1, len(drawings)), 3),
            "drawings_intersecting_block": d["geometry"]["extraction"]["drawings_intersecting_block"],
            "crossings": len(records),
            "same_pdf_path_same_object": same,
            "share_same_pdf_path": round(same / len(records), 4) if records else None,
        }
        print(pair_dir.name, out[pair_dir.name], flush=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
