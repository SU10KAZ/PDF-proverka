"""Probe: is a PDF layer / optional-content-group signal available at all?

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_layer_census

Writes artifacts/hatchnoise_layer_census.json
"""
from __future__ import annotations

import collections
import json
import random
import sys
import time
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parents[1] / "artifacts" / "hatchnoise_layer_census.json"


def main() -> None:
    pdfs = sorted(ROOT.glob("projects_v2/objects/*/disciplines/*/documents/*/versions/*/02_work/document.pdf"))
    ocg_docs = []
    stats = collections.Counter()
    errors = []
    t0 = time.time()
    for path in pdfs:
        try:
            doc = fitz.open(path)
        except Exception as exc:  # pragma: no cover - corpus robustness
            errors.append([str(path.relative_to(ROOT)), repr(exc)])
            continue
        try:
            ocgs = doc.get_ocgs() or {}
        except Exception as exc:
            errors.append([str(path.relative_to(ROOT)), "get_ocgs " + repr(exc)])
            ocgs = {}
        stats["pdfs"] += 1
        if ocgs:
            stats["pdfs_with_ocg"] += 1
            ocg_docs.append({
                "pdf": str(path.relative_to(ROOT)),
                "n_ocg": len(ocgs),
                "names": sorted({str(v.get("name")) for v in ocgs.values()})[:20],
            })
        doc.close()

    # per-page drawing['layer'] measurement on a random page sample
    random.seed(20260822)
    sample = random.sample(pdfs, min(40, len(pdfs)))
    page_rows = []
    layer_counter: collections.Counter[str] = collections.Counter()
    for path in sample:
        try:
            doc = fitz.open(path)
            if len(doc) == 0:
                doc.close()
                continue
            pidx = min(len(doc) - 1, max(0, len(doc) // 2))
            page = doc[pidx]
            drawings = page.get_drawings()
            non_empty = sum(1 for d in drawings if str(d.get("layer") or ""))
            layer_counter.update(str(d.get("layer") or "<EMPTY>") for d in drawings)
            page_rows.append({
                "pdf": str(path.relative_to(ROOT)),
                "page_index": pidx,
                "drawings": len(drawings),
                "drawings_with_layer": non_empty,
            })
            doc.close()
        except Exception as exc:
            errors.append([str(path.relative_to(ROOT)), "page " + repr(exc)])

    total_dr = sum(r["drawings"] for r in page_rows)
    total_lay = sum(r["drawings_with_layer"] for r in page_rows)
    payload = {
        "probe": "hatchnoise_layer_census",
        "elapsed_s": round(time.time() - t0, 1),
        "pymupdf": fitz.__doc__.splitlines()[0],
        "pdfs_scanned": stats["pdfs"],
        "pdfs_with_ocg": stats["pdfs_with_ocg"],
        "ocg_docs": ocg_docs[:20],
        "page_sample": len(page_rows),
        "sample_drawings_total": total_dr,
        "sample_drawings_with_non_empty_layer": total_lay,
        "sample_layer_values": layer_counter.most_common(10),
        "page_rows": page_rows,
        "errors": errors[:20],
        "n_errors": len(errors),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in payload.items() if k not in {"page_rows", "ocg_docs"}}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    sys.exit(main())
