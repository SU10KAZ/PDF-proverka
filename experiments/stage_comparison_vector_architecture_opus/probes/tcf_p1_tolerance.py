#!/usr/bin/env python3
"""TCF probe 1 — tolerance sensitivity of extractor._topology.

Recomputes the topology of all 20 Track A block descriptions at five node-merge
tolerances and reports (a) how the nine counts the comparator averages move and
(b) what the comparator's topology similarity would be for the real left/right
pairs at each tolerance, plus the "tolerance artefact" similarity obtained by
comparing one and the same block against itself at two different tolerances.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p1_tolerance
"""
from __future__ import annotations

import json
import pathlib
import time

from experiments.stage_comparison_vector_blocks.comparator import _topology_diff
from experiments.stage_comparison_vector_architecture_opus.probes import tcf_topo

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p1_tolerance.json")
TOLERANCES = (0.0005, 0.001, 0.0025, 0.005, 0.01)
KEYS = tcf_topo.COMPARATOR_KEYS


def main() -> None:
    results: dict[str, dict[str, dict]] = {}
    started = time.time()
    for pair_dir in sorted(ROOT.iterdir()):
        pair = pair_dir.name
        results[pair] = {}
        for side in ("left", "right"):
            path = pair_dir / side / "vector_block.json"
            if not path.exists():
                continue
            description = json.loads(path.read_text(encoding="utf-8"))
            primitives = description["geometry"]["primitives"]
            results[pair][side] = {}
            for tolerance in TOLERANCES:
                t0 = time.time()
                topo = tcf_topo.topology(primitives, tolerance, 8_000)
                topo["_seconds"] = round(time.time() - t0, 2)
                topo.pop("_dropped_segment_lengths", None)
                results[pair][side][str(tolerance)] = topo
            print(f"{pair}/{side} done  {time.time()-started:.0f}s", flush=True)

    summary = []
    for pair, sides in results.items():
        if "left" not in sides or "right" not in sides:
            continue
        row = {"pair": pair, "pair_similarity_by_tolerance": {}, "self_similarity_vs_0.0025": {}}
        base_left = sides["left"]["0.0025"]
        for tolerance in TOLERANCES:
            key = str(tolerance)
            row["pair_similarity_by_tolerance"][key] = _topology_diff(
                sides["left"][key], sides["right"][key]
            )["similarity"]
            row["self_similarity_vs_0.0025"][key] = _topology_diff(base_left, sides["left"][key])[
                "similarity"
            ]
        sims = list(row["pair_similarity_by_tolerance"].values())
        row["pair_similarity_spread"] = round(max(sims) - min(sims), 6)
        row["min_self_similarity"] = min(row["self_similarity_vs_0.0025"].values())
        summary.append(row)

    payload = {
        "probe": "tcf_p1_tolerance",
        "tolerances": list(TOLERANCES),
        "comparator_keys": list(KEYS),
        "summary": summary,
        "raw": results,
    }
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

    print("\n== counts vs tolerance (left side) ==")
    print("pair\tkey\t" + "\t".join(str(t) for t in TOLERANCES) + "\tmax/min")
    for pair, sides in results.items():
        for key in KEYS:
            values = [sides["left"][str(t)][key] for t in TOLERANCES]
            lo = min(v for v in values) or 1
            print(f"{pair}\t{key}\t" + "\t".join(str(v) for v in values) + f"\t{max(values)/lo:.2f}")
    print("\n== comparator topology similarity ==")
    print("pair\t" + "\t".join(str(t) for t in TOLERANCES) + "\tspread\tself_min")
    for row in summary:
        print(
            row["pair"]
            + "\t"
            + "\t".join(f"{row['pair_similarity_by_tolerance'][str(t)]:.4f}" for t in TOLERANCES)
            + f"\t{row['pair_similarity_spread']:.4f}\t{row['min_self_similarity']:.4f}"
        )


if __name__ == "__main__":
    main()
