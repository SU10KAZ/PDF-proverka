"""tbl_separation_and_size — (a) does the table detector separate the table region from the
engineering graphic inside one block, (b) how large is a table representation next to
Track A's Level-3 vector description.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_separation_and_size
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_architecture_opus.probes import tbl_table_layer as T  # noqa: E402

TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"


def table_markdown(tables) -> str:
    out = []
    for i, t in enumerate(tables):
        out.append(f"### Таблица {i + 1} ({t['rows']}×{t['cols']})")
        for row in T.table_rows(t):
            out.append("| " + " | ".join(row) + " |")
    return "\n".join(out)


def main() -> None:
    report: dict = {}
    pairs = json.loads((TRACK_A / "block_pairs.json").read_text())["pairs"]

    for pid in ("ss_table_graphic", "eom_singleline_changed"):
        pair = next(p for p in pairs if p["pair_id"] == pid)
        per_side = {}
        for side in ("left", "right"):
            info = pair[side]
            doc = fitz.open(str(ROOT / info["pdf"]))
            page = doc[info["page_index"]]
            w, h = page.rect.width, page.rect.height
            b = info["bbox_norm"]
            region = (b[0] * w, b[1] * h, b[2] * w, b[3] * h)
            drawings = page.get_drawings()
            tables = T.reconstruct(page, drawings=drawings, region=region)
            matrix = page.rotation_matrix if page.rotation else None
            segs = T.flatten_segments(drawings, matrix)

            def in_region(x, y):
                return region[0] <= x <= region[2] and region[1] <= y <= region[3]

            def in_any_table(x, y):
                for t in tables:
                    bb = t["bbox"]
                    if bb[0] <= x <= bb[2] and bb[1] <= y <= bb[3]:
                        return True
                return False

            spans = [s for s in T.page_spans(page) if not s["blank"] and in_region(s["cx"], s["cy"])]
            span_in_table = sum(1 for s in spans if in_any_table(s["cx"], s["cy"]))
            seg_mid = [((a + c) / 2, (bb + d) / 2) for a, bb, c, d in segs]
            seg_in_region = [p for p in seg_mid if in_region(*p)]
            seg_in_table = sum(1 for p in seg_in_region if in_any_table(*p))

            md = table_markdown(tables)
            desc_path = TRACK_A / f"descriptions/{pid}/{side}/vector_block.md"
            desc_json = TRACK_A / f"descriptions/{pid}/{side}/vector_block.json"
            per_side[side] = {
                "tables_detected": len(tables),
                "table_shapes": [[t["rows"], t["cols"]] for t in tables],
                "table_bboxes": [[round(v, 1) for v in t["bbox"]] for t in tables],
                "block_region": [round(v, 1) for v in region],
                "text_spans_in_block": len(spans),
                "text_spans_inside_a_table": span_in_table,
                "text_spans_outside_tables_graphic_part": len(spans) - span_in_table,
                "drawing_segments_in_block": len(seg_in_region),
                "drawing_segments_inside_a_table": seg_in_table,
                "drawing_segments_outside_tables_graphic_part": len(seg_in_region) - seg_in_table,
                "table_markdown_chars": len(md),
                "track_a_level3_md_chars": len(desc_path.read_text()) if desc_path.exists() else None,
                "track_a_json_chars": len(desc_json.read_text()) if desc_json.exists() else None,
            }
            (OUT / f"tbl_md_{pid}_{side}.md").write_text(md)
            doc.close()
        report[pid] = per_side

    (OUT / "tbl_separation_and_size.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    print(json.dumps(report, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
