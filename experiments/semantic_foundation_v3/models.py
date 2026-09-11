"""Local evidence models. None of these functions assembles semantic sections/tables."""
from collections import defaultdict
from enum import Enum
import re

from . import frozen_v1 as v1

CAPTION_DISTANCE = 3  # Table start is one of the next three meaningful same-page lines.
CAPTION_LEXICON = re.compile(r"^(?:(?:продолжение|окончание)\s+)?(?:таблиц\w*|ведомост\w*|экспликац\w*|спецификац\w*|перечень)\b", re.I)
NUMBER_CHAIN = re.compile(r"^(\d+(?:\.\d+)*[.)]?)\s+\S")
BOLD = re.compile(r"^(?:\*\*.+\*\*|__.+__)$")


class HeadingModel:
    def __init__(self, ledger, pages):
        self.records = {}
        for i, line in enumerate(ledger.lines):
            md = pages.heading_info[i]
            clean = ledger.clean[i]
            numbered = NUMBER_CHAIN.match(clean)
            bold = bool(BOLD.fullmatch(line.text.strip()))
            evidence = set()
            if md:
                evidence.add("MD_HEADING")
            if bold and numbered:
                evidence.add("BOLD_NUMBERED_HEADING")
            if numbered and not md and not bold:
                evidence.add("NUMBER_CHAIN_CANDIDATE")
            if bold and not numbered:
                evidence.add("BOLD_CANDIDATE")
            letters = [c for c in clean if c.isalpha()]
            if len(letters) >= 3 and all(c.isupper() for c in letters) and not v1.TABLE_ROW_RE.match(line.text):
                evidence.add("ALL_CAPS_CANDIDATE")
            if not evidence:
                continue
            number = md["number"] if md else (numbered.group(1).rstrip(".)") if numbered else None)
            self.records[i] = {"tier": "STRONG" if md or (bold and numbered) else "CANDIDATE",
                               "number": number, "level": md["level"] if md else (number.count(".") + 1 if number else None),
                               "evidence_codes": sorted(evidence)}


class FurnitureModel:
    def __init__(self, ledger, headings):
        self.records = {}
        occurrences = defaultdict(list)
        norms = ledger.columns["normalized_text"]
        for number, ids in ledger.page_lines.items():
            edge = set(ids[:4] + ids[-4:])
            for i in ids:
                line = ledger.lines[i]
                if line.block_type in {"stamp", "title_block"}:
                    self.records[i] = ["STAMP_BLOCK"]
                if v1.TABLE_ROW_RE.match(line.text):
                    continue
                if i in set(ids[:1] + ids[-1:]) and re.fullmatch(r"\d{1,4}", line.text.strip()):
                    self.records.setdefault(i, []).append("EDGE_PAGE_NUMBER")
                heading = headings.records.get(i, {})
                if i in edge and 8 <= len(norms[i]) <= 220 and not heading.get("number"):
                    occurrences[norms[i]].append(i)
        threshold = max(3, int(len(ledger.pages) * 0.15))
        for ids in occurrences.values():
            if len({ledger.lines[i].page for i in ids}) >= threshold:
                for i in ids:
                    self.records.setdefault(i, []).append("REPEATED_EDGE_TEXT")


class CaptionModel:
    """The only caption decision point; heading evidence is already available."""
    def __init__(self, ledger, pages, headings, furniture):
        self.records = {}
        for number, ids in ledger.page_lines.items():
            if not pages.eligible(number):
                continue
            meaningful = [i for i in ids if i not in furniture.records]
            for offset, i in enumerate(meaningful):
                if not CAPTION_LEXICON.match(ledger.clean[i]):
                    continue
                target = None
                for j in meaningful[offset + 1:offset + 1 + CAPTION_DISTANCE]:
                    if v1.TABLE_ROW_RE.match(ledger.lines[j].text):
                        target = j
                        break
                    if headings.records.get(j, {}).get("tier") == "STRONG":
                        break
                if target is None:
                    continue
                strong = headings.records.get(i, {}).get("tier") == "STRONG"
                self.records[i] = {"decision": "REVIEW" if strong else "CAPTION", "table_start": target,
                                   "evidence_codes": ["CAPTION_LEXICON", "TABLE_WITHIN_3_MEANINGFUL_LINES"] +
                                   (["CAPTION_HEADING_CONFLICT"] if strong else [])}


class FirstRowKind(str, Enum):
    TEXTUAL = "TEXTUAL"
    MULTI_ROW = "MULTI_ROW"
    DATA_LIKE = "DATA_LIKE"
    NONE = "NONE"
    UNKNOWN = "UNKNOWN"


def first_row_model(lines):
    """Describe surface structure, never infer a semantic header from a separator.

    MULTI_ROW requires explicit HTML rowspan/colspan markup; empty cells alone
    do not prove a multi-row header. TEXTUAL means letters, not a proven header.
    """
    rows = [x for x in lines if not v1.TABLE_SEPARATOR_RE.match(x)]
    separators = [i for i, x in enumerate(lines) if v1.TABLE_SEPARATOR_RE.match(x)]
    if not rows:
        kind, codes, width = FirstRowKind.NONE, ["NO_CONTENT_ROW"], 0
    else:
        cells = [v1.strip_markup(c) for c in v1.parse_cells(rows[0])]
        width = len(cells)
        if re.search(r"\b(?:rowspan|colspan)\s*=", rows[0], re.I):
            kind, codes = FirstRowKind.MULTI_ROW, ["EXPLICIT_SPAN_MARKUP"]
        elif cells and (re.match(r"^\d", cells[0]) or (not cells[0] and any(cells))):
            kind, codes = FirstRowKind.DATA_LIKE, ["NUMERIC_OR_EMPTY_LEADING_CELL"]
        elif cells and all(c and any(x.isalpha() for x in c) for c in cells):
            kind, codes = FirstRowKind.TEXTUAL, ["TEXTUAL_CELLS_NOT_HEADER_PROOF"]
        else:
            kind, codes = FirstRowKind.UNKNOWN, ["FIRST_ROW_UNRESOLVED"]
    return {"kind": kind.value, "width": width, "content_row_count": len(rows),
            "separator_offsets": separators, "semantic_header_proven": False, "evidence_codes": codes}
