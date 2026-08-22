"""tbl_scan_corpus — find table-bearing pages in corpus PDFs and size the detected tables.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_scan_corpus <pdf> [keyword] [max_pages]
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import fitz

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from experiments.stage_comparison_vector_architecture_opus.probes import tbl_table_layer as T  # noqa: E402


def main() -> None:
    pdf = sys.argv[1]
    keyword = sys.argv[2] if len(sys.argv) > 2 else ""
    max_pages = int(sys.argv[3]) if len(sys.argv) > 3 else 25
    doc = fitz.open(pdf)
    scanned = 0
    for i in range(len(doc)):
        page = doc[i]
        text = page.get_text("text")
        if keyword and keyword.lower() not in text.lower():
            continue
        scanned += 1
        if scanned > max_pages:
            break
        t0 = time.time()
        tables = T.reconstruct(page)
        dt = time.time() - t0
        if not tables:
            continue
        big = tables[0]
        filled = sum(1 for c in big["cells"] if c.text)
        print(f"page {i}: {len(tables)} tables; biggest {big['rows']}x{big['cols']} "
              f"cells={len(big['cells'])} filled={filled} "
              f"bbox={[round(v,1) for v in big['bbox']]} ({dt:.1f}s)")
    doc.close()


if __name__ == "__main__":
    main()
