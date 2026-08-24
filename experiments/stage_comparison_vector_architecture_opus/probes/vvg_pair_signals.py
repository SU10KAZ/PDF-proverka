"""VVG — pair-level gate signals (O10 anisotropy, O9 alignment residual).

These need BOTH sides, so they can only gate at comparison time, never at extraction time.

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_pair_signals
"""
from __future__ import annotations

import json
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vvg_signals as sg

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments" / "stage_comparison_vector_architecture_opus" / "artifacts"
TRACK_A_DESC = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts" / "descriptions"
FRESH = ART / "vvg_fresh"


def main() -> None:
    rows = []
    for pair_dir in sorted(TRACK_A_DESC.iterdir()):
        if not pair_dir.is_dir():
            continue
        l = pair_dir / "left" / "vector_block.json"
        r = pair_dir / "right" / "vector_block.json"
        if not (l.exists() and r.exists()):
            continue
        row = {"pair_id": pair_dir.name, "set": "track_a"}
        row.update(sg.pair_signals(sg.load_description(l), sg.load_description(r)))
        rows.append(row)
    index = json.loads((ART / "vvg_fresh_index.json").read_text(encoding="utf-8"))["blocks"]
    for block in index:
        pid = block["pair_id"]
        l, r = FRESH / f"{pid}__left.json", FRESH / f"{pid}__right.json"
        if not (l.exists() and r.exists()):
            continue
        row = {"pair_id": pid, "set": "fresh", "discipline": block["discipline"]}
        row.update(sg.pair_signals(sg.load_description(l), sg.load_description(r)))
        rows.append(row)
    (ART / "vvg_pair_signals.json").write_text(
        json.dumps({"rows": rows}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    hdr = ["pair_id", "set", "anisotropy", "anisotropy_excess", "align_shift_max",
           "align_scale_dev_max", "align_residual"]
    print(" | ".join(hdr))
    for row in rows:
        print(" | ".join(str(row.get(h, ""))[:24] for h in hdr))


if __name__ == "__main__":
    main()
