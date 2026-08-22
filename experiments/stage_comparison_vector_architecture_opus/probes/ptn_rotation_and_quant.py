"""(a) How often is a motif instance a ROTATED/MIRRORED copy that S1 misses?
   (b) How brittle is the S1 hash to the quantisation step q?

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_rotation_and_quant
Writes artifacts/ptn_rotation_and_quant.json
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from experiments.stage_comparison_vector_architecture_opus.probes import ptn_motifs as M  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"
QS = (0.02, 0.035, 0.05, 0.08, 0.12, 0.2)


def main() -> None:
    result = {"rotation": {}, "quantisation": {}}
    for pair_dir in sorted(TRACK_A.iterdir()):
        if not pair_dir.is_dir():
            continue
        side = "left"
        path = pair_dir / side / "vector_block.json"
        if not path.exists():
            continue
        desc = M.load_description(path)
        bundle = M.build_motifs(desc, unit="cc_split")
        M.enrich_bundle(bundle, desc)
        motifs, segments = bundle["motifs"], bundle["segments"]

        # --- rotation ---
        s1 = [M._hash(("S1", M.geom_core(m, segments))) for m in motifs]
        s5 = [M._hash(("S5", M.geom_core_d4(m, segments))) for m in motifs]
        s5c = [M._hash(("S5c", M.geom_core_rot(m, segments))) for m in motifs]
        for name, sig in (("S5", s5), ("S5c", s5c)):
            classes: dict[str, set[str]] = defaultdict(set)
            inst: Counter = Counter()
            for a, b in zip(sig, s1):
                classes[a].add(b)
                inst[a] += 1
            multi = {k: v for k, v in classes.items() if len(v) > 1}
            result["rotation"].setdefault(pair_dir.name, {})[name] = {
                "motifs": len(motifs),
                "classes": len(classes),
                "classes_with_multiple_orientations": len(multi),
                "instances_in_such_classes": sum(inst[k] for k in multi),
                "max_orientations_in_one_class": max((len(v) for v in classes.values()), default=0),
                "share_instances_affected": round(sum(inst[k] for k in multi) / max(len(motifs), 1), 3),
            }

        # --- quantisation sweep ---
        rows = {}
        for q in QS:
            sig = [M._hash(("S1", M.geom_core(m, segments, q))) for m in motifs]
            counts = Counter(sig)
            rows[str(q)] = {
                "groups": len(counts),
                "groups_repeated": sum(1 for v in counts.values() if v >= 2),
                "instances_in_repeated": sum(v for v in counts.values() if v >= 2),
                "largest_group": max(counts.values()),
            }
        result["quantisation"][pair_dir.name] = rows
        print(pair_dir.name, {q: rows[q]["groups_repeated"] for q in rows}, flush=True)

    with open(OUT / "ptn_rotation_and_quant.json", "w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=1)
    print(OUT / "ptn_rotation_and_quant.json")


if __name__ == "__main__":
    main()
