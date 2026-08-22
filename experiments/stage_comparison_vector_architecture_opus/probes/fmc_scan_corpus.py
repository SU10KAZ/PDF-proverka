#!/usr/bin/env python3
"""FMC probe step 1 — cheap per-page descriptors for every document with 2+ versions.

Run from repository root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_scan_corpus

Writes artifacts/fmc_page_scan.json  (research only, reads nothing outside projects_v2).
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import re
import sys
import time
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parents[1] / "artifacts" / "fmc_page_scan.json"


def _docs() -> list[dict]:
    rows = []
    for doc in sorted((ROOT / "projects_v2" / "objects").glob("*/disciplines/*/documents/*")):
        vdir = doc / "versions"
        if not vdir.is_dir():
            continue
        pdfs = []
        for v in sorted(p for p in vdir.glob("v*") if p.is_dir()):
            pdf = v / "02_work" / "document.pdf"
            if pdf.is_file():
                pdfs.append((v.name, pdf))
        if len(pdfs) < 2:
            continue
        parts = doc.parts
        rows.append(
            {
                "object": parts[-5],
                "discipline": parts[-3],
                "document": doc.name,
                "versions": [
                    {
                        "version": n,
                        "pdf": str(p.relative_to(ROOT)),
                        "sha256": hashlib.sha256(p.read_bytes()).hexdigest()[:16],
                        "bytes": p.stat().st_size,
                    }
                    for n, p in pdfs
                ],
            }
        )
    return rows


_NUM = re.compile(r"\d+(?:[.,]\d+)?")


def _page_descriptor(page) -> dict:
    text = page.get_text("text")
    try:
        contents = page.read_contents()
    except Exception:
        contents = b""
    words = text.split()
    nums = _NUM.findall(text)
    return {
        "text_sha": hashlib.sha256(text.encode("utf-8")).hexdigest()[:16],
        "text_len": len(text),
        "n_words": len(words),
        "word_sha": hashlib.sha256(" ".join(sorted(words)).encode("utf-8")).hexdigest()[:16],
        "num_sha": hashlib.sha256(" ".join(sorted(nums)).encode("utf-8")).hexdigest()[:16],
        "n_nums": len(nums),
        "contents_len": len(contents),
        "contents_sha": hashlib.sha256(contents).hexdigest()[:16],
        "n_images": len(page.get_images(full=True)),
        "rect": [round(v, 2) for v in page.rect],
        "rotation": page.rotation,
    }


def _scan_pdf(rel: str) -> dict:
    t0 = time.perf_counter()
    doc = fitz.open(ROOT / rel)
    pages = [_page_descriptor(doc[i]) for i in range(doc.page_count)]
    doc.close()
    return {"pdf": rel, "page_count": len(pages), "pages": pages, "seconds": round(time.perf_counter() - t0, 3)}


def main() -> None:
    docs = _docs()
    rels = sorted({v["pdf"] for d in docs for v in d["versions"]})
    print(f"documents={len(docs)} distinct_pdfs={len(rels)}", file=sys.stderr)
    with mp.Pool(8) as pool:
        scans = pool.map(_scan_pdf, rels)
    by_pdf = {s["pdf"]: s for s in scans}
    OUT.write_text(
        json.dumps({"documents": docs, "scans": by_pdf}, ensure_ascii=False), encoding="utf-8"
    )
    print(f"wrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)", file=sys.stderr)


if __name__ == "__main__":
    main()
