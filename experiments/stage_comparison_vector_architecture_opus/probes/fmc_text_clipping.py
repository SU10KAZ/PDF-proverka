#!/usr/bin/env python3
"""FMC probe step 10 — measure how often the v0.1 text layer TRUNCATES span strings at the block border.

extractor._extract_text calls page.get_text("dict", clip=block_rect); PyMuPDF drops the characters that
fall outside the clip, so a span crossing the block boundary arrives as a fragment
("4х(1х120)+1х70" -> "20)+1х70").  This probe compares, per block of the FMC corpus, the clipped span
strings against the full-page span strings of the same spans.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_text_clipping
"""
from __future__ import annotations

import collections
import json
import re
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"

_WS = re.compile(r"\s+")


def spans(page, clip=None):
    out = []
    d = page.get_text("dict", clip=clip)
    for b in d.get("blocks") or []:
        if b.get("type") != 0:
            continue
        for l in b.get("lines") or []:
            for s in l.get("spans") or []:
                t = _WS.sub(" ", str(s.get("text") or "")).strip()
                if t:
                    out.append((t, tuple(round(v, 1) for v in s["bbox"])))
    return out


def main() -> None:
    from .fmc_io import read_json
    manifest = read_json(ART / "fmc_pairs.json")
    rows = []
    docs: dict[str, fitz.Document] = {}
    for pair in manifest["pairs"]:
        for name in ("left", "right"):
            side = pair[name]
            if side["pdf"] not in docs:
                docs[side["pdf"]] = fitz.open(ROOT / side["pdf"])
            page = docs[side["pdf"]][side["page_index"]]
            bb = side["bbox_norm"]
            rect = fitz.Rect(bb[0] * page.rect.width, bb[1] * page.rect.height,
                             bb[2] * page.rect.width, bb[3] * page.rect.height)
            clipped = spans(page, rect)
            full = {b: t for t, b in spans(page)}
            # a clipped span is truncated when the same bbox on the full page carries a longer string
            truncated = []
            for t, b in clipped:
                ft = full.get(b)
                if ft is None:
                    # bbox itself was cropped by PyMuPDF -> find the full span containing this bbox
                    for fb, ftext in full.items():
                        if fb[1] - 0.6 <= b[1] and b[3] <= fb[3] + 0.6 and fb[0] - 0.6 <= b[0] and b[2] <= fb[2] + 0.6:
                            ft = ftext
                            break
                if ft is not None and ft != t and t in ft:
                    truncated.append({"clipped": t, "full": ft})
            rows.append(
                {
                    "pair_id": pair["pair_id"],
                    "side": name,
                    "spans_in_block": len(clipped),
                    "truncated_spans": len(truncated),
                    "truncated_frac": round(len(truncated) / len(clipped), 4) if clipped else 0.0,
                    "examples": truncated[:6],
                }
            )
    total = sum(r["spans_in_block"] for r in rows)
    trunc = sum(r["truncated_spans"] for r in rows)
    summary = {
        "blocks": len(rows),
        "spans_total": total,
        "spans_truncated": trunc,
        "truncated_share": round(trunc / total, 4) if total else 0.0,
        "blocks_with_truncation": sum(1 for r in rows if r["truncated_spans"]),
        "rows": rows,
    }
    (ART / "fmc_text_clipping.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"blocks={summary['blocks']} spans={total} truncated={trunc} ({summary['truncated_share']*100:.1f}%) "
          f"blocks_with_truncation={summary['blocks_with_truncation']}")
    worst = sorted(rows, key=lambda r: -r["truncated_spans"])[:8]
    for r in worst:
        ex = "; ".join(f"{e['clipped']!r}<-{e['full']!r}" for e in r["examples"][:2])
        print(f"  {r['pair_id']:34} {r['side']:5} {r['truncated_spans']:4}/{r['spans_in_block']:4}  {ex[:120]}")


if __name__ == "__main__":
    main()
