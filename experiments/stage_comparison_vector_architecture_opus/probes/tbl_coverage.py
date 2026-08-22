"""tbl_coverage — how much of a CAD sheet's text lives inside a ruled table?

Answers "is a table a first-class generic primitive or a discipline-profile concern"
with a corpus measurement rather than an argument.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_coverage [n_pages]
"""
from __future__ import annotations

import glob
import json
import random
import sys
import time
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_architecture_opus.probes import tbl_table_layer as T  # noqa: E402

OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"
BUDGET_S = 900.0


def main() -> None:
    n_pages = int(sys.argv[1]) if len(sys.argv) > 1 else 150
    pdfs = sorted(glob.glob(str(ROOT / "projects_v2/objects/*/disciplines/*/documents/*/versions/*/02_work/document.pdf")))
    random.seed(1729)
    random.shuffle(pdfs)

    rows = []
    started = time.time()
    capped = False
    for pdf in pdfs:
        if len(rows) >= n_pages:
            break
        if time.time() - started > BUDGET_S:
            capped = True
            break
        try:
            doc = fitz.open(pdf)
        except Exception:
            continue
        if len(doc) == 0:
            doc.close()
            continue
        idx = random.randrange(len(doc))
        page = doc[idx]
        try:
            t0 = time.time()
            drawings = page.get_drawings()
            t_dr = time.time() - t0
            t0 = time.time()
            tables = T.reconstruct(page, drawings=drawings)
            t_tab = time.time() - t0
            spans = T.page_spans(page)
        except Exception as exc:
            doc.close()
            rows.append({"pdf": pdf, "page": idx, "error": str(exc)[:120]})
            continue
        ink = [s for s in spans if not s["blank"]]
        in_table = 0
        for s in ink:
            for t in tables:
                if any(c.rect[0] <= s["cx"] <= c.rect[2] and c.rect[1] <= s["cy"] <= c.rect[3]
                       for c in t["cells"]):
                    in_table += 1
                    break
        disc = pdf.split("/disciplines/")[1].split("/")[0]
        rows.append({
            "pdf": str(Path(pdf).relative_to(ROOT)), "page": idx, "discipline": disc,
            "rotation": page.rotation,
            "tables": len(tables),
            "table_cells": sum(len(t["cells"]) for t in tables),
            "table_stats": [
                {"rows": t["rows"], "cols": t["cols"], "cells": len(t["cells"]),
                 "filled": t["filled_cells"],
                 "bbox_rel": [round(t["bbox"][0] / max(page.rect.width, 1), 3),
                              round(t["bbox"][1] / max(page.rect.height, 1), 3),
                              round(t["bbox"][2] / max(page.rect.width, 1), 3),
                              round(t["bbox"][3] / max(page.rect.height, 1), 3)]}
                for t in tables],
            "spans": len(ink),
            "spans_in_tables": in_table,
            "t_get_drawings": round(t_dr, 2), "t_reconstruct": round(t_tab, 2),
        })
        doc.close()

    ok = [r for r in rows if "error" not in r]
    with_tables = [r for r in ok if r["tables"] > 0]
    tot_spans = sum(r["spans"] for r in ok)
    tot_in = sum(r["spans_in_tables"] for r in ok)
    per_disc: dict[str, dict] = {}
    for r in ok:
        d = per_disc.setdefault(r["discipline"], {"pages": 0, "with_tables": 0, "spans": 0, "in_tables": 0})
        d["pages"] += 1
        d["with_tables"] += 1 if r["tables"] else 0
        d["spans"] += r["spans"]
        d["in_tables"] += r["spans_in_tables"]
    for d in per_disc.values():
        d["pages_with_tables_pct"] = round(100.0 * d["with_tables"] / max(1, d["pages"]), 1)
        d["spans_in_tables_pct"] = round(100.0 * d["in_tables"] / max(1, d["spans"]), 1)

    def is_body_table(t):
        """Bigger than a title block and not sitting in the bottom-right stamp corner."""
        big = t["rows"] >= 5 and t["cols"] >= 3 and t["filled"] >= 15
        x0, y0, _, _ = t["bbox_rel"]
        stamp_corner = x0 > 0.5 and y0 > 0.6
        return big and not stamp_corner

    body_pages = [r for r in ok if any(is_body_table(t) for t in r.get("table_stats", []))]
    summary = {
        "pages_with_body_table_not_title_block": len(body_pages),
        "pages_with_body_table_pct": round(100.0 * len(body_pages) / max(1, len(ok)), 1),
        "pages_scanned": len(ok),
        "pages_capped": capped,
        "errors": len(rows) - len(ok),
        "pages_with_at_least_one_table": len(with_tables),
        "pages_with_at_least_one_table_pct": round(100.0 * len(with_tables) / max(1, len(ok)), 1),
        "text_spans_total": tot_spans,
        "text_spans_inside_a_table_cell": tot_in,
        "text_spans_inside_a_table_cell_pct": round(100.0 * tot_in / max(1, tot_spans), 1),
        "median_t_get_drawings_s": sorted(r["t_get_drawings"] for r in ok)[len(ok) // 2] if ok else None,
        "median_t_reconstruct_s": sorted(r["t_reconstruct"] for r in ok)[len(ok) // 2] if ok else None,
        "max_t_reconstruct_s": max((r["t_reconstruct"] for r in ok), default=None),
        "per_discipline": dict(sorted(per_disc.items())),
    }
    (OUT / "tbl_coverage.json").write_text(json.dumps({"summary": summary, "pages": rows},
                                                      ensure_ascii=False, indent=1))
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
