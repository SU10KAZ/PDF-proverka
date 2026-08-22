"""Probe `obj` — how much of the geometry in these PDFs paints nothing.

Measures, per benchmark block, the segment count with and without the
"invisible ink" filter (white-on-white fills, zero-opacity paint), and how the
left/right segment-count difference changes when those primitives are dropped.

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/obj_ink.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent.parent.parent
ART = HERE.parent / "artifacts"
import obj_poc as P  # noqa: E402

PAIRS = json.loads((ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json").read_text())["pairs"]

out = {"probe": "obj", "research_only": True, "pairs": {}}
for pr in PAIRS:
    row = {}
    for side in ("left", "right"):
        e = pr[side]
        raw = P.extract_segments(str(ROOT / e["pdf"]), e["page_index"], e["bbox_norm"], drop_invisible=False)
        ink = P.extract_segments(str(ROOT / e["pdf"]), e["page_index"], e["bbox_norm"], drop_invisible=True)
        row[side] = {
            "segments_all": len(raw["segments"]),
            "segments_inked": len(ink["segments"]),
            "invisible_paths": raw["invisible_paths"],
            "invisible_share": round(1 - len(ink["segments"]) / max(1, len(raw["segments"])), 4),
        }
    a, b = row["left"], row["right"]
    row["delta_all"] = abs(a["segments_all"] - b["segments_all"])
    row["delta_inked"] = abs(a["segments_inked"] - b["segments_inked"])
    row["rel_delta_all"] = round(row["delta_all"] / max(1, max(a["segments_all"], b["segments_all"])), 4)
    row["rel_delta_inked"] = round(row["delta_inked"] / max(1, max(a["segments_inked"], b["segments_inked"])), 4)
    out["pairs"][pr["pair_id"]] = row
    print(f"{pr['pair_id']:24s} all {a['segments_all']:6d}/{b['segments_all']:6d} "
          f"inked {a['segments_inked']:6d}/{b['segments_inked']:6d} "
          f"reldelta {row['rel_delta_all']:.4f} -> {row['rel_delta_inked']:.4f}", flush=True)

(ART / "obj_invisible_ink.json").write_text(json.dumps(out, ensure_ascii=False, indent=1))
print("wrote", ART / "obj_invisible_ink.json")
