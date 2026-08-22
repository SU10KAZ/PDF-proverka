"""P5 — is the background/foreground decision stable?

ar_plan and ar_wall_sections compare a PDF against a byte-identical copy of itself
(orchestrator finding O1); the only difference is a ~0.1 % crop jitter.  Any segment whose
keep/drop decision changes between the two sides is pure filter instability, not a change
in the drawing.  Segments are matched by their RAW page coordinates, which are identical
in identical PDFs.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p5_stability
"""
from __future__ import annotations

import collections
import hashlib
import json

from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_filter as F

OUT = C.ART / "hatchnoise_p5_stability.json"


def keyed_decisions(spec) -> dict[tuple, bool]:
    payload = C.load_primitives(*spec)
    rows = C.segment_table(payload)["rows"]
    flags, records, prim_flags = F.classify(rows)
    primitives = payload["primitives"]
    dropped_pi = {record["pi"] for record, flag in zip(records, prim_flags) if flag}
    out: dict[tuple, bool] = {}
    for index, primitive in enumerate(primitives):
        for start, end in primitive["raw"]["segments"]:
            key = (round(start[0], 2), round(start[1], 2), round(end[0], 2), round(end[1], 2))
            out[key] = index in dropped_pi
    return out


def main() -> None:
    results = []
    for name in ("ar_wall_sections", "ar_plan"):
        spec = C.BLOCKS[name]
        left = keyed_decisions(spec["left"])
        right = keyed_decisions(spec["right"])
        shared = set(left) & set(right)
        flips = sum(1 for key in shared if left[key] != right[key])
        only_left = len(left) - len(shared)
        only_right = len(right) - len(shared)
        results.append({
            "block": name,
            "left_pdf": spec["left"][0], "right_pdf": spec["right"][0],
            "note": "byte-identical PDFs (O1); only the crop box differs by ~0.1%",
            "segments_left": len(left), "segments_right": len(right),
            "segments_matched_by_raw_coordinates": len(shared),
            "decision_flips": flips,
            "decision_flip_frac_of_matched": round(flips / max(len(shared), 1), 5),
            "segments_only_in_left": only_left,
            "segments_only_in_right": only_right,
            "dropped_left_frac": round(sum(left.values()) / max(len(left), 1), 4),
            "dropped_right_frac": round(sum(right.values()) / max(len(right), 1), 4),
        })
        print(json.dumps(results[-1], ensure_ascii=False))
    C.write_json(OUT, {"probe": "hatchnoise_p5_stability", "blocks": results})


if __name__ == "__main__":
    main()
