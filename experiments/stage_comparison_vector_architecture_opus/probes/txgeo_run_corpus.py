#!/usr/bin/env python3
"""Run the TXGEO relation detectors over a set of VectorBlockDescription files.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_run_corpus \
        --set trackA --unit-mode line
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import txgeo_relations as R

ROOT = Path(__file__).resolve().parents[3]
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/txgeo_relations"
FRESH = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/txgeo_fresh_descriptions"


def iter_descriptions(which: str):
    if which in ("trackA", "all"):
        for path in sorted(TRACK_A.glob("*/*/vector_block.json")):
            yield path.parent.parent.name, path.parent.name, path
    if which in ("fresh", "all"):
        for path in sorted(FRESH.glob("*/*/vector_block.json")):
            yield path.parent.parent.name, path.parent.name, path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", default="trackA", choices=["trackA", "fresh", "all"])
    ap.add_argument("--unit-mode", default="line", choices=["span", "line"])
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    for pair, side, path in iter_descriptions(args.set):
        target = OUT / args.unit_mode / pair / f"{side}.json"
        started = time.time()
        description = json.loads(path.read_text(encoding="utf-8"))
        result = R.analyse(description, unit_mode=args.unit_mode)
        result["pair_id"] = pair
        result["side"] = side
        result["source_description"] = str(path.relative_to(ROOT))
        result["runtime_s"] = round(time.time() - started, 2)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
        print(f"{pair:24s} {side:6s} {args.unit_mode:5s} units={result['counts']['units']:5d} "
              f"segs={result['counts']['segments']:6d} {result['runtime_s']:6.2f}s", flush=True)


if __name__ == "__main__":
    main()
