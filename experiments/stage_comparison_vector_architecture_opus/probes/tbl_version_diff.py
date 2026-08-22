"""tbl_version_diff — run the table-level diff on real version pairs of specification /
cable-journal sheets, to see whether the sentences an expert needs are derivable.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_version_diff
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_architecture_opus.probes import tbl_table_layer as T  # noqa: E402
from experiments.stage_comparison_vector_architecture_opus.probes import tbl_table_diff as D  # noqa: E402

OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"
ALIA = ROOT / "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents"

CASES = [
    # (case id, doc, left version, right version, keyword identifying the sheet)
    ("sot_k7_specification", "13AB-РД-СОТ-К7 V1", "v002", "v003", "Спецификация оборудования"),
    ("sot_k7_cable_journal", "13AB-РД-СОТ-К7 V1", "v002", "v003", "Кабельный журнал"),
    ("aps_k3_specification", "13АВ-РД-АПЗ.АПС-К3 V1", "v001", "v002", "Спецификация оборудования"),
    ("kk_pa_specification", "13АВ-РД-КК-ПА", "v001", "v002", "Спецификация оборудования"),
]


def find_page(doc: fitz.Document, keyword: str) -> int | None:
    for i in range(len(doc)):
        if keyword.lower() in doc[i].get_text("text").lower():
            if i > 5:          # skip the contents sheet at the front of the album
                return i
    for i in range(len(doc)):
        if keyword.lower() in doc[i].get_text("text").lower():
            return i
    return None


def biggest(tables):
    body = [t for t in tables if t["rows"] >= 4 and t["cols"] >= 3]
    if not body:
        return None
    return max(body, key=lambda t: t["filled_cells"])


def main() -> None:
    report = {}
    for case, docname, lv, rv, keyword in CASES:
        entry = {"document": docname, "versions": [lv, rv], "keyword": keyword}
        sides = {}
        for tag, ver in (("left", lv), ("right", rv)):
            path = ALIA / docname / "versions" / ver / "02_work/document.pdf"
            if not path.exists():
                entry["error"] = f"missing {path}"
                break
            doc = fitz.open(str(path))
            pi = find_page(doc, keyword)
            if pi is None:
                entry["error"] = f"no page with {keyword!r} in {ver}"
                doc.close()
                break
            page = doc[pi]
            tables = T.reconstruct(page)
            t = biggest(tables)
            sides[tag] = {"page_index": pi, "table": t,
                          "shape": [t["rows"], t["cols"]] if t else None,
                          "filled": t["filled_cells"] if t else 0}
            doc.close()
        if "error" in entry or len(sides) != 2 or not all(s["table"] for s in sides.values()):
            report[case] = entry | {"status": "SKIPPED"}
            continue
        d = D.diff_tables(sides["left"]["table"], sides["right"]["table"],
                          left_label=lv, right_label=rv)
        dc = D.diff_tables(sides["left"]["table"], sides["right"]["table"],
                           left_label=lv, right_label=rv, align="content")
        entry.update({
            "content_alignment": {
                "verdict": dc["verdict"],
                "rows_added": len(dc["rows_added"]), "rows_removed": len(dc["rows_removed"]),
                "n_cell_changes": len(dc["cell_changes"]),
                "sentences": dc["sentences"][:25],
            },
            "left_page_index": sides["left"]["page_index"],
            "right_page_index": sides["right"]["page_index"],
            "left_shape": sides["left"]["shape"], "right_shape": sides["right"]["shape"],
            "verdict": d["verdict"], "row_alignment": d["row_alignment"],
            "rows_added": d["rows_added"], "rows_removed": d["rows_removed"],
            "n_cell_changes": len(d["cell_changes"]),
            "cell_changes": d["cell_changes"][:40],
            "sentences": d["sentences"][:40],
        })
        report[case] = entry

    (OUT / "tbl_version_diff.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    for case, e in report.items():
        print(f"\n=== {case}: {e.get('status') or e.get('verdict')} "
              f"{e.get('left_shape')} -> {e.get('right_shape')} "
              f"pages {e.get('left_page_index')}/{e.get('right_page_index')}")
        for s in (e.get("sentences") or [])[:8]:
            print("   *", s)
        ca = e.get("content_alignment")
        if ca:
            print(f"   -- content-aligned: {ca['verdict']} added={ca['rows_added']} "
                  f"removed={ca['rows_removed']} changes={ca['n_cell_changes']}")
            for s in ca["sentences"][:8]:
                print("      +", s)


if __name__ == "__main__":
    main()
