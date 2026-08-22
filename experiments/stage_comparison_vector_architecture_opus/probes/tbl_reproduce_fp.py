"""tbl_reproduce_fp — reproduce Track A's ss_table_graphic Vision-vs-Vector false positive.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_reproduce_fp
Writes experiments/stage_comparison_vector_architecture_opus/artifacts/tbl_failure_reproduction.json
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"


def main() -> None:
    pairs = json.loads((TRACK_A / "block_pairs.json").read_text())["pairs"]
    pair = next(p for p in pairs if p["pair_id"] == "ss_table_graphic")
    comparison = json.loads((TRACK_A / "comparisons/ss_table_graphic/comparison.json").read_text())
    vector_ai = next(p for p in json.loads((TRACK_A / "ai_experiment/vector_output.json").read_text())["pairs"]
                     if p["pair_id"] == "ss_table_graphic")
    vision_ai = next(p for p in json.loads((TRACK_A / "ai_experiment/vision_output.json").read_text())["pairs"]
                     if p["pair_id"] == "ss_table_graphic")

    report: dict = {
        "pair_id": "ss_table_graphic",
        "track_a_vector_major_changes": vector_ai["major_changes"],
        "track_a_vision_major_changes": vision_ai["major_changes"],
        "track_a_differences": comparison["differences"],
    }

    crops = {}
    for side in ("left", "right"):
        doc = fitz.open(str(ROOT / pair[side]["pdf"]))
        page = doc[pair[side]["page_index"]]
        w, h = page.rect.width, page.rect.height
        b = pair[side]["bbox_norm"]
        crops[side] = {
            "pdf": pair[side]["pdf"],
            "page_index": pair[side]["page_index"],
            "page_size_pt": [round(w, 2), round(h, 2)],
            "bbox_norm": b,
            "crop_rect_pt": [round(b[0] * w, 2), round(b[1] * h, 2),
                             round(b[2] * w, 2), round(b[3] * h, 2)],
        }
        doc.close()
    report["crop_windows"] = crops
    lr, rr = crops["left"]["crop_rect_pt"], crops["right"]["crop_rect_pt"]
    report["crop_window_delta_pt"] = {
        "x0": round(rr[0] - lr[0], 2), "y0": round(rr[1] - lr[1], 2),
        "x1": round(rr[2] - lr[2], 2), "y1": round(rr[3] - lr[3], 2),
    }

    # the underlying documents, read WITHOUT the crop, on the same table lines
    keys = ("Монтажная", "Цилиндрическая", "Разъём", "Герметик", "гофрированная",
            "UTP", "Резиновая", "Винт", "Сальник")
    full_rows = {}
    for side in ("left", "right"):
        doc = fitz.open(str(ROOT / pair[side]["pdf"]))
        page = doc[pair[side]["page_index"]]
        lines = []
        for block in page.get_text("dict")["blocks"]:
            if block.get("type") != 0:
                continue
            for line in block["lines"]:
                txt = "".join(s["text"] for s in line["spans"]).strip()
                if any(k in txt for k in keys) and line["bbox"][0] > 800:
                    lines.append({"y": round(line["bbox"][1], 2),
                                  "x0": round(line["bbox"][0], 2),
                                  "x1": round(line["bbox"][2], 2),
                                  "text": txt})
        full_rows[side] = sorted(lines, key=lambda d: d["y"])
        doc.close()
    report["unclipped_table_lines"] = full_rows
    report["unclipped_identical"] = ([d["text"] for d in full_rows["left"]]
                                     == [d["text"] for d in full_rows["right"]])

    # which of Track A's text differences are explained by the crop edges alone
    left_desc = json.loads((TRACK_A / "descriptions/ss_table_graphic/left/vector_block.json").read_text())
    right_desc = json.loads((TRACK_A / "descriptions/ss_table_graphic/right/vector_block.json").read_text())
    lx1, ly0 = lr[2], lr[1]
    rx1, ry0 = rr[2], rr[1]
    def classify(desc, x_edge, y_edge, other_y_edge):
        out = {"touch_right_edge": [], "above_other_top_edge": [], "interior": []}
        for t in desc["texts"]:
            bb = t["bbox"]
            if bb[2] >= x_edge - 3.0:
                out["touch_right_edge"].append(t["text"])
            elif bb[3] < other_y_edge:
                out["above_other_top_edge"].append(t["text"])
            else:
                out["interior"].append(t["text"])
        return out
    report["left_span_classes"] = classify(left_desc, lx1, ly0, ly0)
    report["right_span_classes"] = classify(right_desc, rx1, ry0, ly0)

    lset = {t["text"] for t in left_desc["texts"]}
    rset = {t["text"] for t in right_desc["texts"]}
    only_left = sorted(lset - rset)
    only_right = sorted(rset - lset)
    edge_r = set(report["right_span_classes"]["touch_right_edge"])
    edge_l = set(report["left_span_classes"]["touch_right_edge"])
    top_r = set(report["right_span_classes"]["above_other_top_edge"])
    report["symmetric_text_difference"] = {
        "only_left": only_left,
        "only_right": only_right,
        "only_left_explained_by_right_crop_edge": sorted(
            t for t in only_left
            if t in edge_l or any(t.startswith(e) or e.startswith(t) for e in edge_r)),
        "only_right_explained_by_right_crop_edge": sorted(t for t in only_right if t in edge_r or t in edge_l),
        "only_right_explained_by_left_top_edge": sorted(t for t in only_right if t in top_r),
    }
    explained = set(report["symmetric_text_difference"]["only_left_explained_by_right_crop_edge"]) \
        | set(report["symmetric_text_difference"]["only_right_explained_by_right_crop_edge"]) \
        | set(report["symmetric_text_difference"]["only_right_explained_by_left_top_edge"])
    report["unexplained_text_difference"] = sorted((set(only_left) | set(only_right)) - explained)
    report["span_counts"] = {"left": len(left_desc["texts"]), "right": len(right_desc["texts"])}

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "tbl_failure_reproduction.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=1))
    print(json.dumps({k: report[k] for k in
                      ("crop_window_delta_pt", "unclipped_identical",
                       "symmetric_text_difference", "unexplained_text_difference",
                       "span_counts")}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
