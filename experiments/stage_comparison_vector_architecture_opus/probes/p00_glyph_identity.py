#!/usr/bin/env python3
"""Orchestrator probe O8: make "undecodable" CAD text comparable without OCR or Vision.

Track A labels the VK text layer UNDECODABLE and drops it from the verdict, because the CAD
exporter writes a subset font whose /ToUnicode CMap covers only Latin, digits and punctuation.
Cyrillic codes have no entry, so PyMuPDF returns the raw byte and the span looks like control
characters.

For *comparison* we never need to read the word.  We need to know whether the left word is the
same word as the right word.  This probe rewrites every character code as a hash of the glyph
outline the embedded TrueType program actually draws, giving a canonical, document-independent
string.  Nothing here uses OCR, Vision, or a model.

Run from the repository root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.p00_glyph_identity
"""
from __future__ import annotations

import collections
import difflib
import hashlib
import io
import json
from pathlib import Path
from typing import Any

import fitz
from fontTools.pens.recordingPen import DecomposingRecordingPen
from fontTools.ttLib import TTFont

VK_V1 = (
    "projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/"
    "13АВ-РД-ВК.КВ-К4_V1/versions/v001/02_work/document.pdf"
)
VK_V2 = (
    "projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/"
    "13АВ-РД-ВК.КВ-К4_V1/versions/v002/02_work/document.pdf"
)

# page index and block bbox copied from Track A artifacts/block_pairs.json
PAIRS = {
    "vk_nodes": (
        (VK_V1, 8, [0.02226443588733673, 0.0, 0.45511864125728607, 0.9887108504772186]),
        (VK_V2, 8, [0.01965601965601966, 0.0017388552317053254, 0.46928746928746934, 0.9981029029988565]),
    ),
    "vk_node_plan": (
        (VK_V1, 11, [0.018328696489334106, 0.002146989107131958, 0.5161558747235082, 0.5867864828409218]),
        (VK_V2, 11, [0.017839640378952026, 0.003336876630783081, 0.5227995480454231, 0.5830469829391108]),
    ),
    "vk_plan": (
        (VK_V1, 5, [0.05016317963600159, 0.009301990270614624, 0.6681880056858063, 0.848704606294632]),
        (VK_V2, 5, [0.04789525270462036, 0.009159773588180542, 0.6673479080200195, 0.8476087749004364]),
    ),
}


def _glyph_signature(glyph_set: Any, name: str, units_per_em: float, advance: float) -> str:
    """Order-preserving hash of one glyph outline, normalised by the font's em square."""
    pen = DecomposingRecordingPen(glyph_set)
    glyph_set[name].draw(pen)

    def quantise(value: Any) -> Any:
        if isinstance(value, (int, float)):
            return round(float(value) / units_per_em, 3)
        if isinstance(value, (tuple, list)):
            return tuple(quantise(item) for item in value)
        return value

    payload = [(operator, quantise(points)) for operator, points in pen.value]
    payload.append(("advance", round(float(advance) / units_per_em, 3)))
    return hashlib.sha1(repr(payload).encode("utf-8")).hexdigest()[:10]


def glyph_tables(document: fitz.Document, page: fitz.Page) -> dict[str, dict[int, str]]:
    """font base name (subset tag stripped) -> {character code: glyph outline hash}."""
    tables: dict[str, dict[int, str]] = {}
    for entry in page.get_fonts(full=True):
        xref, base_name = entry[0], entry[3]
        try:
            _, extension, _, buffer = document.extract_font(xref)
        except Exception:
            continue
        if not buffer or extension != "ttf":
            continue
        try:
            font = TTFont(io.BytesIO(buffer))
            glyph_set = font.getGlyphSet()
            units_per_em = float(font["head"].unitsPerEm)
            metrics = font["hmtx"].metrics if "hmtx" in font else {}
        except Exception:
            continue
        # A CAD subset font exposes the raw character codes through the (1,0) Macintosh table, or
        # as 0xF000+code in the (3,0) symbol table.  fontTools.getBestCmap() picks (3,0) and
        # therefore resolves none of the codes 1..99 — which is the whole reason the text looked
        # undecodable in the first place.
        cmap: dict[int, str] = {}
        for table in font["cmap"].tables if "cmap" in font else []:
            if table.platformID == 1:
                cmap.update(table.cmap)
        if not cmap:
            for table in font["cmap"].tables if "cmap" in font else []:
                if table.platformID == 3 and table.platEncID == 0:
                    cmap.update({code & 0xFF: name for code, name in table.cmap.items()})
        codes: dict[int, str] = {}
        for code, glyph_name in cmap.items():
            if glyph_name not in glyph_set:
                continue
            advance = metrics.get(glyph_name, (0, 0))[0]
            try:
                codes[code] = _glyph_signature(glyph_set, glyph_name, units_per_em, advance)
            except Exception:
                continue
        tables[base_name.split("+", 1)[-1]] = codes
    return tables


def canonical_spans(pdf_path: str, page_index: int, bbox_norm: list[float]) -> list[dict[str, Any]]:
    document = fitz.open(pdf_path)
    page = document[page_index]
    tables = glyph_tables(document, page)
    rect = fitz.Rect(
        bbox_norm[0] * page.rect.width,
        bbox_norm[1] * page.rect.height,
        bbox_norm[2] * page.rect.width,
        bbox_norm[3] * page.rect.height,
    )
    rows: list[dict[str, Any]] = []
    for block in page.get_text("dict", clip=rect).get("blocks") or []:
        if block.get("type") != 0:
            continue
        for line in block.get("lines") or []:
            for span in line.get("spans") or []:
                text = str(span.get("text") or "")
                if not text.strip():
                    continue
                font = str(span.get("font") or "").split("+", 1)[-1]
                table = tables.get(font, {})
                pieces, resolved = [], True
                for character in text:
                    code = ord(character)
                    # Codes 1..31 are ordinary glyph codes in a CAD subset font, not control
                    # characters.  Only a real space is whitespace here.
                    if code == 32:
                        pieces.append(" ")
                        continue
                    signature = table.get(code)
                    if signature is None:
                        resolved = False
                        pieces.append(f"?{code:04x}")
                    else:
                        pieces.append(signature)
                bbox = [float(value) for value in span.get("bbox")]
                rows.append(
                    {
                        "text": text,
                        "font": font,
                        "broken": any(ord(c) < 32 and not c.isspace() for c in text),
                        "canonical": "|".join(pieces),
                        "resolved": resolved,
                        "x_norm": round(
                            ((bbox[0] + bbox[2]) / 2 - rect.x0) / max(rect.width, 1e-9), 5
                        ),
                        "y_norm": round(
                            ((bbox[1] + bbox[3]) / 2 - rect.y0) / max(rect.height, 1e-9), 5
                        ),
                    }
                )
    document.close()
    return rows


def _f1(left: collections.Counter, right: collections.Counter) -> float:
    if not sum(left.values()) and not sum(right.values()):
        return 1.0
    matches = sum((left & right).values())
    precision = matches / max(sum(right.values()), 1)
    recall = matches / max(sum(left.values()), 1)
    return round(2 * precision * recall / max(precision + recall, 1e-12), 4)


def _stream(rows: list[dict[str, Any]], key: str) -> str:
    ordered = sorted(rows, key=lambda item: (round(item["y_norm"], 2), item["x_norm"]))
    return " ".join(item[key] for item in ordered)


def main() -> None:
    output: dict[str, Any] = {}
    for pair_id, ((left_pdf, left_page, left_bbox), (right_pdf, right_page, right_bbox)) in PAIRS.items():
        left = canonical_spans(left_pdf, left_page, left_bbox)
        right = canonical_spans(right_pdf, right_page, right_bbox)
        broken_left = [row for row in left if row["broken"]]
        broken_right = [row for row in right if row["broken"]]

        record = {
            "left_spans": len(left),
            "right_spans": len(right),
            "left_broken_spans": len(broken_left),
            "right_broken_spans": len(broken_right),
            "left_broken_resolved_by_glyph": sum(1 for row in broken_left if row["resolved"]),
            "right_broken_resolved_by_glyph": sum(1 for row in broken_right if row["resolved"]),
            "all_spans": {
                "raw_multiset_f1": _f1(
                    collections.Counter(r["text"] for r in left),
                    collections.Counter(r["text"] for r in right),
                ),
                "glyph_multiset_f1": _f1(
                    collections.Counter(r["canonical"] for r in left),
                    collections.Counter(r["canonical"] for r in right),
                ),
                "raw_stream_similarity": round(
                    difflib.SequenceMatcher(
                        None, _stream(left, "text"), _stream(right, "text"), autojunk=False
                    ).ratio(),
                    4,
                ),
                "glyph_stream_similarity": round(
                    difflib.SequenceMatcher(
                        None, _stream(left, "canonical"), _stream(right, "canonical"), autojunk=False
                    ).ratio(),
                    4,
                ),
            },
            "broken_spans_only": {
                "raw_multiset_f1": _f1(
                    collections.Counter(r["text"] for r in broken_left),
                    collections.Counter(r["text"] for r in broken_right),
                ),
                "glyph_multiset_f1": _f1(
                    collections.Counter(r["canonical"] for r in broken_left),
                    collections.Counter(r["canonical"] for r in broken_right),
                ),
            },
        }
        output[pair_id] = record
        print(pair_id, json.dumps(record, ensure_ascii=False))

    destination = Path(
        "experiments/stage_comparison_vector_architecture_opus/artifacts/p00_glyph_identity.json"
    )
    destination.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print("written", destination)


if __name__ == "__main__":
    main()
