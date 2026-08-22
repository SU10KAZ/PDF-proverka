#!/usr/bin/env python3
"""FMC probe step 6 — find pages whose extracted text is broken/undecodable.

Broken = the extracted string contains U+FFFD or Private-Use-Area code points, i.e. a font
whose glyphs PyMuPDF cannot map back to Unicode.  These pages are the failure mode "the vector
text layer exists but is not readable", which the whole VectorBlockDescription text layer depends on.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_find_broken_text
"""
from __future__ import annotations

import json
from pathlib import Path

import fitz

ART = Path(__file__).resolve().parents[1] / "artifacts"
ROOT = Path(__file__).resolve().parents[3]


def bad_chars(text: str) -> int:
    n = 0
    for ch in text:
        o = ord(ch)
        if o == 0xFFFD or 0xE000 <= o <= 0xF8FF or 0xF0000 <= o <= 0xFFFFD:
            n += 1
    return n


def main() -> None:
    from .fmc_io import read_json
    profile = read_json(ART / "fmc_page_profile.json")
    hits = []
    for pdf, pages in profile.items():
        doc = fitz.open(ROOT / pdf)
        for i, meta in enumerate(pages):
            if meta["n_chars"] < 10:
                continue
            text = doc[i].get_text("text")
            n = bad_chars(text)
            if n:
                hits.append({"pdf": pdf, "page_index": i, "bad_chars": n, "n_chars": meta["n_chars"]})
        doc.close()
    hits.sort(key=lambda h: -h["bad_chars"])
    (ART / "fmc_broken_text.json").write_text(json.dumps(hits, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"pages with undecodable code points: {len(hits)}")
    for h in hits[:25]:
        parts = h["pdf"].split("/")
        print(f"  bad={h['bad_chars']:4} chars={h['n_chars']:6} {parts[4]:4} {parts[6][:30]:30} {parts[8]} p{h['page_index']}")


if __name__ == "__main__":
    main()
