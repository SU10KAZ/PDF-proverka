"""tbl_score — score the reconstructed tables against the crop-read ground truth.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_score
Reads artifacts/tbl_tables.json + artifacts/tbl_ground_truth.json,
writes artifacts/tbl_eval.json.
"""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

_WS = re.compile(r"\s+")
_PUNCT_SPACE = re.compile(r"\s+([,.;:)\]»])")
_SPACE_PUNCT = re.compile(r"([(\[«])\s+")


def canon(text: str, dehyphenate: bool = False) -> str:
    """Whitespace/typography-insensitive comparison key.

    Ground truth was transcribed by eye from a raster crop, so exact space runs and the
    exact dash/ellipsis glyph cannot be trusted; content and cell assignment can.
    """
    t = unicodedata.normalize("NFKC", text or "")
    t = t.replace("…", "...").replace("–", "-").replace("—", "-")
    t = t.replace("−", "-").replace(" ", " ")
    t = t.replace("×", "x").replace("х", "x").replace("х", "x")
    t = _WS.sub(" ", t).strip()
    t = _PUNCT_SPACE.sub(r"\1", t)
    t = _SPACE_PUNCT.sub(r"\1", t)
    if dehyphenate:
        t = re.sub(r"(\w)-\s+(\w)", r"\1\2", t)   # "Код обору- дования" -> "Кодоборудования"
    return t.lower()


def matrix_from_cells(table: dict) -> list[list[str]]:
    grid = [["" for _ in range(table["cols"])] for _ in range(table["rows"])]
    spans = [[0 for _ in range(table["cols"])] for _ in range(table["rows"])]
    for c in table["cells"]:
        if c["row"] < table["rows"] and c["col"] < table["cols"]:
            grid[c["row"]][c["col"]] = c["text"]
            spans[c["row"]][c["col"]] = c["span_count"]
    return grid, spans


def pick_table(entry: dict, key: str) -> dict | None:
    if "frame_mode_tables" in entry:
        tables = entry["frame_mode_tables"]
    else:
        tables = entry["tables"]
    if not tables:
        return None
    return tables[0]


def main() -> None:
    data = json.loads((OUT / "tbl_tables.json").read_text())
    gt_all = json.loads((OUT / "tbl_ground_truth.json").read_text())

    results = {}
    totals = {"gt_cells": 0, "correct": 0, "pred_cells": 0,
              "multi_span_gt": 0, "multi_span_correct": 0,
              "nonempty_gt": 0, "nonempty_correct": 0}

    for key, gt in gt_all.items():
        if key == "_method":
            continue
        if key.endswith("_left") or key.endswith("_right"):
            pair, side = key.rsplit("_", 1)
            entry = data["blocks"][pair][side]
            table = entry["frame_mode_tables"][0] if entry["frame_mode_tables"] else None
        else:
            entry = data["blocks"][key]
            idx = gt.get("table_index", 0)
            table = entry["tables"][idx] if len(entry["tables"]) > idx else None
        if table is None:
            results[key] = {"error": "no table reconstructed"}
            continue

        grid, spans = matrix_from_cells(table)
        n_rows = len(gt["rows"])
        n_cols = gt["cols"]
        correct = wrong = missing = 0
        correct_h = 0
        ne_ok_h = 0
        errors = []
        multi_gt = multi_ok = 0
        ne_gt = ne_ok = 0
        for i in range(n_rows):
            for j in range(n_cols):
                expected = gt["rows"][i][j] if j < len(gt["rows"][i]) else ""
                got = grid[i][j] if i < len(grid) and j < len(grid[i]) else None
                sc = spans[i][j] if i < len(spans) and j < len(spans[i]) else 0
                if got is None:
                    missing += 1
                    errors.append({"row": i, "col": j, "expected": expected, "got": "<no cell>"})
                    continue
                ok = canon(got) == canon(expected)
                ok_h = canon(got, True) == canon(expected, True)
                if expected.strip():
                    ne_gt += 1
                    ne_ok += 1 if ok else 0
                    ne_ok_h += 1 if ok_h else 0
                if sc >= 2:
                    multi_gt += 1
                    multi_ok += 1 if ok else 0
                correct_h += 1 if ok_h else 0
                if ok:
                    correct += 1
                else:
                    wrong += 1
                    errors.append({"row": i, "col": j, "expected": expected, "got": got})
        gt_cells = n_rows * n_cols
        results[key] = {
            "table_shape_detected": [table["rows"], table["cols"]],
            "gt_shape": [n_rows, n_cols],
            "gt_cells_scored": gt_cells,
            "correct": correct,
            "wrong": wrong,
            "missing": missing,
            "cell_accuracy": round(correct / gt_cells, 4) if gt_cells else None,
            "cell_accuracy_dehyphenated": round(correct_h / gt_cells, 4) if gt_cells else None,
            "nonempty_accuracy_dehyphenated": round(ne_ok_h / ne_gt, 4) if ne_gt else None,
            "nonempty_gt_cells": ne_gt,
            "nonempty_correct": ne_ok,
            "nonempty_accuracy": round(ne_ok / ne_gt, 4) if ne_gt else None,
            "multi_span_cells_in_scored_region": multi_gt,
            "multi_span_correct": multi_ok,
            "multi_span_join_accuracy": round(multi_ok / multi_gt, 4) if multi_gt else None,
            "errors": errors[:20],
        }
        totals["gt_cells"] += gt_cells
        totals["correct"] += correct
        totals["correct_dehyphenated"] = totals.get("correct_dehyphenated", 0) + correct_h
        totals["nonempty_correct_dehyphenated"] = totals.get("nonempty_correct_dehyphenated", 0) + ne_ok_h
        totals["multi_span_gt"] += multi_gt
        totals["multi_span_correct"] += multi_ok
        totals["nonempty_gt"] += ne_gt
        totals["nonempty_correct"] += ne_ok

    totals["cell_accuracy"] = round(totals["correct"] / max(1, totals["gt_cells"]), 4)
    totals["nonempty_accuracy"] = round(totals["nonempty_correct"] / max(1, totals["nonempty_gt"]), 4)
    totals["multi_span_join_accuracy"] = round(
        totals["multi_span_correct"] / max(1, totals["multi_span_gt"]), 4)
    totals["cell_accuracy_dehyphenated"] = round(
        totals.get("correct_dehyphenated", 0) / max(1, totals["gt_cells"]), 4)
    totals["nonempty_accuracy_dehyphenated"] = round(
        totals.get("nonempty_correct_dehyphenated", 0) / max(1, totals["nonempty_gt"]), 4)

    report = {"per_table": results, "totals": totals}
    (OUT / "tbl_eval.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    for k, v in results.items():
        if "error" in v:
            print(f"{k}: {v['error']}")
            continue
        print(f"{k}: shape {v['table_shape_detected']} gt {v['gt_shape']} "
              f"acc {v['cell_accuracy']} ({v['correct']}/{v['gt_cells_scored']}) "
              f"nonempty {v['nonempty_accuracy']} multi-span join {v['multi_span_join_accuracy']} "
              f"({v['multi_span_correct']}/{v['multi_span_cells_in_scored_region']})")
        for e in v["errors"][:4]:
            print(f"    r{e['row']}c{e['col']} exp={e['expected']!r} got={e['got']!r}")
    print("TOTALS", json.dumps(totals, ensure_ascii=False))


if __name__ == "__main__":
    main()
