"""P2d — what ARE the repeated motifs that rule P2 (== Track A `repeated_elements`) removes?

If the removed motifs form a small alphabet of shapes, each repeated hundreds of times,
with heights clustered on a couple of discrete values, they are glyphs of outlined text,
not decoration.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2d_motif_identity
"""
from __future__ import annotations

import collections
import json
import statistics

from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_filter as F

OUT = C.ART / "hatchnoise_p2d_motif_identity.json"


def analyse(block: str) -> dict:
    pdf, page_index, bbox = C.BLOCKS[block]["left"]
    payload = C.load_primitives(pdf, page_index, bbox)
    primitives = payload["primitives"]
    rows = C.segment_table(payload)["rows"]
    _, records, prim_flags = F.classify(rows)

    dropped_by_p2 = [record for record, flag in zip(records, prim_flags) if "P2" in flag]
    motifs = collections.Counter(record["motif"] for record in dropped_by_p2)
    heights = []
    widths = []
    for record in dropped_by_p2:
        bb = primitives[record["pi"]]["raw"]["bbox"]
        heights.append(round(bb[3] - bb[1], 2))
        widths.append(round(bb[2] - bb[0], 2))
    height_hist = collections.Counter(round(h, 1) for h in heights)
    return {
        "block": block,
        "pdf": pdf,
        "page_index": page_index,
        "vector_text_spans_in_block": len(payload["texts"]),
        "primitives_total": len(records),
        "primitives_dropped_by_P2": len(dropped_by_p2),
        "distinct_motifs_dropped": len(motifs),
        "top_motif_counts": motifs.most_common(15),
        "median_bbox_height_pt": round(statistics.median(heights), 2) if heights else None,
        "bbox_height_top_values": height_hist.most_common(10),
        "share_of_dropped_in_top_3_heights": round(
            sum(count for _, count in height_hist.most_common(3)) / max(len(heights), 1), 4
        ),
        "share_of_dropped_in_top_60_motifs": round(
            sum(count for _, count in motifs.most_common(60)) / max(len(dropped_by_p2), 1), 4
        ),
    }


def main() -> None:
    results = [analyse(name) for name in ("ov_nodes_hatch", "ar_layered_plan", "ar_wall_sections")]
    C.write_json(OUT, {"probe": "hatchnoise_p2d_motif_identity", "blocks": results})
    for row in results:
        print(json.dumps({k: v for k, v in row.items() if k != "top_motif_counts"}, ensure_ascii=False))
        print("   top motifs:", row["top_motif_counts"][:6])


if __name__ == "__main__":
    main()
