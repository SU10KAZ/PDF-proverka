"""Re-extract a Track A pair with a LARGER storage cap, to separate real motif-count
differences from artefacts of the 20 000-primitive cap.

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut <pair_id> [cap]
Writes artifacts/ptn/recut/<pair>/<side>/vector_block.json
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from experiments.stage_comparison_vector_blocks import extractor  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/ptn/recut"


def main() -> None:
    pair_id = sys.argv[1]
    cap = int(sys.argv[2]) if len(sys.argv) > 2 else 200_000
    pairs = json.load(open(ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json",
                           encoding="utf-8"))["pairs"]
    pair = next(p for p in pairs if p["pair_id"] == pair_id)
    for side in ("left", "right"):
        spec = pair[side]
        t0 = time.time()
        desc = extractor.extract_block(
            ROOT / spec["pdf"],
            page_index=spec["page_index"],
            bbox_norm=spec["bbox_norm"],
            block_id=spec["block_id"],
            storage_cap=cap,
            topology_cap=cap,
        )
        target = OUT / pair_id / side
        target.mkdir(parents=True, exist_ok=True)
        with open(target / "vector_block.json", "w", encoding="utf-8") as handle:
            json.dump(desc, handle, ensure_ascii=False)
        print(pair_id, side, "prims", len(desc["geometry"]["primitives"]),
              "uncapped", desc["geometry"]["extraction"]["primitives_uncapped"],
              "capped", desc["geometry"]["extraction"]["storage_capped"],
              f"{time.time() - t0:.1f}s", flush=True)


if __name__ == "__main__":
    main()
