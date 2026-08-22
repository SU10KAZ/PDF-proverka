#!/usr/bin/env python3
"""FMC probe step 2 — match pages across version steps and mine change candidates.

Run from repository root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_mine_candidates

Reads artifacts/fmc_page_scan.json, re-extracts per-page words/numbers/sheet labels,
matches pages between consecutive versions and classifies each matched page.
Writes artifacts/fmc_candidates.json.
"""
from __future__ import annotations

import collections
import json
import multiprocessing as mp
import re
import sys
import time
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"
SCAN = ART / "fmc_page_scan.json"
OUT = ART / "fmc_candidates.json"

_NUM = re.compile(r"-?\d+(?:[.,]\d+)?")
_WORD = re.compile(r"[^\W\d_]{2,}", re.UNICODE)


def _page_text_features(rel: str) -> dict:
    doc = fitz.open(ROOT / rel)
    pages = []
    for i in range(doc.page_count):
        page = doc[i]
        text = page.get_text("text")
        words = _WORD.findall(text.lower())
        nums = _NUM.findall(text)
        fonts = sorted({f[3] for f in page.get_fonts(full=True)})
        pages.append(
            {
                "words": words,
                "nums": nums,
                "n_chars": len(text),
                "n_repl": text.count("�"),
                "fonts": fonts,
                "rect": [round(v, 1) for v in page.rect],
                "n_images": len(page.get_images(full=True)),
            }
        )
    doc.close()
    return {"pdf": rel, "pages": pages}


def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _match_pages(pa: list[dict], pb: list[dict]) -> list[tuple[int, int, float]]:
    """Greedy best-Jaccard matching on word sets, ties broken by index proximity."""
    sa = [set(p["words"]) for p in pa]
    sb = [set(p["words"]) for p in pb]
    cand = []
    for i, a in enumerate(sa):
        for j, b in enumerate(sb):
            s = _jaccard(a, b)
            if s > 0.35:
                cand.append((s - 0.0005 * abs(i - j), i, j, s))
    cand.sort(reverse=True)
    used_a, used_b, out = set(), set(), []
    for _, i, j, s in cand:
        if i in used_a or j in used_b:
            continue
        used_a.add(i)
        used_b.add(j)
        out.append((i, j, s))
    return sorted(out)


def main() -> None:
    from .fmc_io import read_json
    scan = read_json(SCAN)
    docs = scan["documents"]
    scans = scan["scans"]
    steps = []
    for doc in docs:
        vs = doc["versions"]
        for a, b in zip(vs, vs[1:]):
            if a["sha256"] == b["sha256"]:
                continue
            steps.append((doc, a, b))
    rels = sorted({v["pdf"] for _, a, b in steps for v in (a, b)})
    print(f"steps={len(steps)} pdfs={len(rels)}", file=sys.stderr)
    t0 = time.perf_counter()
    with mp.Pool(8) as pool:
        feats = {f["pdf"]: f["pages"] for f in pool.map(_page_text_features, rels)}
    print(f"text features in {time.perf_counter()-t0:.1f}s", file=sys.stderr)

    results = []
    for doc, a, b in steps:
        pa, pb = feats[a["pdf"]], feats[b["pdf"]]
        da, db = scans[a["pdf"]]["pages"], scans[b["pdf"]]["pages"]
        matches = _match_pages(pa, pb)
        matched_a = {i for i, _, _ in matches}
        matched_b = {j for _, j, _ in matches}
        rows = []
        for i, j, sim in matches:
            wa, wb = collections.Counter(pa[i]["words"]), collections.Counter(pb[j]["words"])
            na, nb = collections.Counter(pa[i]["nums"]), collections.Counter(pb[j]["nums"])
            rows.append(
                {
                    "i": i,
                    "j": j,
                    "word_jaccard": round(sim, 4),
                    "word_added": sum((wb - wa).values()),
                    "word_removed": sum((wa - wb).values()),
                    "num_added": sum((nb - na).values()),
                    "num_removed": sum((na - nb).values()),
                    "text_identical": da[i]["text_sha"] == db[j]["text_sha"],
                    "contents_identical": da[i]["contents_sha"] == db[j]["contents_sha"],
                    "contents_len": [da[i]["contents_len"], db[j]["contents_len"]],
                    "rect_changed": pa[i]["rect"] != pb[j]["rect"],
                    "rect": [pa[i]["rect"], pb[j]["rect"]],
                    "index_shift": j - i,
                    "n_chars": [pa[i]["n_chars"], pb[j]["n_chars"]],
                    "n_repl": [pa[i]["n_repl"], pb[j]["n_repl"]],
                    "n_images": [pa[i]["n_images"], pb[j]["n_images"]],
                    "fonts_changed": pa[i]["fonts"] != pb[j]["fonts"],
                }
            )
        results.append(
            {
                "object": doc["object"],
                "discipline": doc["discipline"],
                "document": doc["document"],
                "left": {"version": a["version"], "pdf": a["pdf"], "page_count": len(pa)},
                "right": {"version": b["version"], "pdf": b["pdf"], "page_count": len(pb)},
                "matched": rows,
                "unmatched_left": sorted(set(range(len(pa))) - matched_a),
                "unmatched_right": sorted(set(range(len(pb))) - matched_b),
            }
        )
    OUT.write_text(json.dumps(results, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)", file=sys.stderr)


if __name__ == "__main__":
    main()
