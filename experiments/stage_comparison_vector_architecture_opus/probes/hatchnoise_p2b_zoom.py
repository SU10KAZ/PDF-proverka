"""P2b — zoomed keep/drop/eaten renders so a human can judge a small region at symbol scale.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2b_zoom BLOCK x0 y0 x1 y1 TAG
(coordinates are normalized INSIDE the block)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_filter as F
from experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2_filter_render import gt_class


def clip(rows, box):
    x0, y0, x1, y1 = box
    out = []
    for row in rows:
        mx, my = row["mid"]
        if x0 <= mx <= x1 and y0 <= my <= y1:
            copy = dict(row)
            copy["seg"] = [
                [(row["seg"][0][0] - x0) / (x1 - x0), (row["seg"][0][1] - y0) / (y1 - y0)],
                [(row["seg"][1][0] - x0) / (x1 - x0), (row["seg"][1][1] - y0) / (y1 - y0)],
            ]
            out.append(copy)
    return out


def main() -> None:
    block = sys.argv[1]
    box = [float(v) for v in sys.argv[2:6]]
    tag = sys.argv[6] if len(sys.argv) > 6 else "zoom"
    spec = C.BLOCKS[block]
    pdf, page_index, bbox = spec["left"]
    payload = C.load_primitives(pdf, page_index, bbox)
    rows = C.segment_table(payload)["rows"]
    flags, records, prim_flags = F.classify(rows)

    sub = clip(rows, box)
    sub_flags = [flags[i] for i, row in enumerate(rows)
                 if box[0] <= row["mid"][0] <= box[2] and box[1] <= row["mid"][1] <= box[3]]
    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]
    page_box = [
        bbox[0] + box[0] * width, bbox[1] + box[1] * height,
        bbox[0] + box[2] * width, bbox[1] + box[3] * height,
    ]
    aspect = ((box[3] - box[1]) * height * payload["page_size"][1]) / ((box[2] - box[0]) * width * payload["page_size"][0])
    out_dir = C.OUT / block
    C.render_crop(pdf, page_index, page_box, out_dir / f"{tag}_00_crop.png", width_px=1500)
    keep = [row for row, flag in zip(sub, sub_flags) if not flag]
    drop = [row for row, flag in zip(sub, sub_flags) if flag]
    C.render_segments(keep, out_dir / f"{tag}_02_keep.png", aspect=aspect, width_px=1500,
                      title=f"{block} {tag}: KEPT {len(keep)}/{len(sub)}")
    C.render_segments(drop, out_dir / f"{tag}_03_drop.png", aspect=aspect, color=(0.85, 0, 0), width_px=1500,
                      title=f"{block} {tag}: DROPPED {len(drop)}/{len(sub)}")
    classes = [gt_class(row["layer"]) for row in sub]
    eaten = [row for row, cls, flag in zip(sub, classes, sub_flags)
             if flag and cls == "foreground"]
    if eaten:
        C.render_segments(eaten, out_dir / f"{tag}_07_eaten.png", aspect=aspect, color=(0, 0.45, 0.9), width_px=1500,
                          title=f"{block} {tag}: CAD-layer FOREGROUND eaten {len(eaten)}")
    print(block, tag, "sub segments", len(sub), "kept", len(keep), "dropped", len(drop), "eaten_fg", len(eaten))


if __name__ == "__main__":
    main()
