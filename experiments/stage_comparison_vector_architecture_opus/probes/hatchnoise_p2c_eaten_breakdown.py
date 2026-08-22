"""P2c — exactly WHICH CAD layers the discipline-free filter eats.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2c_eaten_breakdown
"""
from __future__ import annotations

import collections
import json

from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_filter as F
from experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2_filter_render import gt_class

OUT = C.ART / "hatchnoise_p2c_eaten_breakdown.json"


def main() -> None:
    block = "ar_layered_plan"
    pdf, page_index, bbox = C.BLOCKS[block]["left"]
    payload = C.load_primitives(pdf, page_index, bbox)
    rows = C.segment_table(payload)["rows"]
    flags, records, prim_flags = F.classify(rows)

    per_layer = collections.defaultdict(lambda: {"primitives": 0, "dropped": 0, "segments": 0, "segments_dropped": 0})
    rules_per_layer = collections.defaultdict(collections.Counter)
    for record, flag in zip(records, prim_flags):
        stats = per_layer[record["layer"] or "<EMPTY>"]
        stats["primitives"] += 1
        stats["segments"] += record["n_seg"]
        if flag:
            stats["dropped"] += 1
            stats["segments_dropped"] += record["n_seg"]
            for rule in flag:
                rules_per_layer[record["layer"] or "<EMPTY>"][rule] += 1

    table = []
    for layer, stats in sorted(per_layer.items(), key=lambda kv: -kv[1]["segments_dropped"]):
        table.append({
            "layer": layer,
            "class": gt_class(layer),
            "primitives": stats["primitives"],
            "primitives_dropped": stats["dropped"],
            "primitives_dropped_frac": round(stats["dropped"] / max(stats["primitives"], 1), 4),
            "segments": stats["segments"],
            "segments_dropped": stats["segments_dropped"],
            "segments_dropped_frac": round(stats["segments_dropped"] / max(stats["segments"], 1), 4),
            "rules": dict(rules_per_layer[layer]),
        })
    payload_out = {
        "probe": "hatchnoise_p2c_eaten_breakdown",
        "block": block,
        "pdf": pdf,
        "page_index": page_index,
        "filter_defaults": F.DEFAULTS,
        "layers": table,
    }
    C.write_json(OUT, payload_out)
    print(f"{'segdrop':>8} {'segs':>8} {'frac':>6} {'class':11s} layer")
    for row in table[:20]:
        print(f"{row['segments_dropped']:8d} {row['segments']:8d} {row['segments_dropped_frac']:6.2f} {row['class']:11s} {row['layer']}  rules={row['rules']}")


if __name__ == "__main__":
    main()
