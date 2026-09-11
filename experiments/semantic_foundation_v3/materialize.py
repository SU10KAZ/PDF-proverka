"""Thin ownership assemblers over one ledger. No Table/Section V3 rule tuning."""
from collections import Counter

from . import frozen_v1 as v1
from .boundary import BoundaryDecision
from .ledger import LineLedger, PRODUCER_VERSION
from .models import HeadingModel, FurnitureModel, CaptionModel, first_row_model
from .page_model import PageModel


def assemble(ledger, pages, headings, furniture, captions, sheets):
    units, decisions, table_groups = [], [], []
    sheet_owners = {}
    for ref, sheet in enumerate(sheets["units"]):
        sheet_owners[sheet["source_pages"][0]] = len(units)
        units.append({"kind": "SHEET", "sheet_ref": ref, "status": "PROVEN"})
    current, open_heading, previous, previous_page = None, None, None, None
    current_table = None
    kinds, codes = ledger.columns["kind"], ledger.columns["evidence_codes"]
    pending_captions = {}
    for i, c in captions.records.items():
        if c["decision"] == "CAPTION":
            pending_captions.setdefault(c["table_start"], []).append(i)
    for number in sorted(pages.pages):
        if not pages.eligible(number):
            current, open_heading, previous, current_table = None, None, None, None
        for i in ledger.page_lines.get(number, []):
            line = ledger.lines[i]
            h, cap = headings.records.get(i), captions.records.get(i)
            if h:
                codes[i].extend(h["evidence_codes"])
            if cap:
                codes[i].extend(cap["evidence_codes"])
            if i in furniture.records:
                kinds[i] = "FURNITURE"
                codes[i].extend(furniture.records[i])
                ledger.claim(i, "FURNITURE")
                continue
            if not pages.eligible(number):
                kinds[i] = "EXCLUDED"
                codes[i].extend(pages.pages[number]["classification"]["reason_codes"])
                ledger.claim(i, sheet_owners.get(number, "EXCLUDED"))
                continue
            table_row = bool(v1.TABLE_ROW_RE.match(line.text))
            if table_row:
                kinds[i] = "TABLE_SEPARATOR" if v1.TABLE_SEPARATOR_RE.match(line.text) else "TABLE_ROW"
                codes[i].append("MARKDOWN_TABLE_STRUCTURE")
                if current_table is None or ledger.lines[table_groups[-1][-1]].page != number or ledger.lines[table_groups[-1][-1]].block_id != line.block_id:
                    current_table = len(units)
                    units.append({"kind": "TABLE_SEGMENT", "first_line": i, "last_line": i,
                                  "section_context": current if open_heading is not None else None,
                                  "status": "REVIEW", "caption_lines": pending_captions.get(i, [])})
                    table_groups.append([])
                    for caption in pending_captions.get(i, []):
                        ledger.claim(caption, current_table)
                table_groups[-1].append(i)
                units[current_table]["last_line"] = i
                ledger.claim(i, current_table)
                previous, previous_page = i, number
                continue
            current_table = None
            if cap and cap["decision"] == "CAPTION":
                kinds[i] = "CAPTION"
                # The following table owns it; a caption cannot open a section.
                continue
            page_break = previous_page is not None and number != previous_page
            boundary = None
            if h or current is None or page_break:
                join, split, observed = set(), set(), set()
                if h:
                    observed.update(h["evidence_codes"])
                    repeated = open_heading is not None and ledger.columns["normalized_text"][open_heading] == ledger.columns["normalized_text"][i]
                    if repeated:
                        observed.add("REPEATED_OPEN_HEADING_CANDIDATE_SAME")
                    elif h["tier"] == "STRONG":
                        split.add("STRONG_HEADING")
                    if cap:
                        join.add("POSITIONAL_CAPTION")
                        split.add("STRONG_HEADING")
                elif current is not None and page_break:
                    join.add("PAGE_TRANSPARENT_NO_HEADING")
                else:
                    observed.add("NO_OPEN_SECTION")
                boundary = BoundaryDecision.collect("SECTION", previous, i, join=join, split=split, observed=observed)
                decisions.append(boundary.artifact())
            if current is None or (boundary and boundary.decision != "SAME"):
                current = len(units)
                proven_heading = boundary and boundary.decision == "NEW" and boundary.basis == "PROVEN"
                open_heading = i if proven_heading else None
                units.append({"kind": "SECTION_FRAGMENT", "first_line": i, "last_line": i,
                              "heading_line": open_heading, "status": "PROVEN" if proven_heading else "REVIEW"})
            kinds[i] = "REVIEW" if cap or (h and (h["tier"] == "CANDIDATE" or open_heading != i)) else ("HEADING" if h else "TEXT")
            if kinds[i] == "TEXT":
                codes[i].append("ORDERED_SOURCE_TEXT")
            ledger.claim(i, current)
            units[current]["last_line"] = i
            previous, previous_page = i, number

    table_units = [u for u in units if u["kind"] == "TABLE_SEGMENT"]
    for u, ids in zip(table_units, table_groups):
        u["first_row"] = first_row_model([ledger.lines[i].text for i in ids])
        rows = [i for i in ids if kinds[i] != "TABLE_SEPARATOR"]
        u["first_content_line"] = rows[0] if rows else None
        u["last_content_line"] = rows[-1] if rows else None
    for left, right in zip(table_units, table_units[1:]):
        a, b = left["last_line"], right["first_line"]
        lp, rp = ledger.lines[a].page, ledger.lines[b].page
        observed = {"TABLE_ASSEMBLER_DEFERRED"}
        if rp == lp:
            observed.add("SAME_PAGE")
        if rp == lp + 1:
            observed.add("ADJACENT_PAGE")
        if left["first_row"]["width"] == right["first_row"]["width"]:
            observed.add("EQUAL_WIDTH")
        if ledger.columns["normalized_text"][left["first_line"]] == ledger.columns["normalized_text"][b]:
            observed.add("REPEATED_FIRST_ROW")
        if not any(kinds[j] not in {"FURNITURE", "CAPTION"} for j in range(a + 1, b)):
            observed.add("FURNITURE_TRANSPARENT_ADJACENCY")
        if right["caption_lines"]:
            observed.add("CAPTION_BEFORE_RIGHT")
        observed.add("RIGHT_FIRST_ROW_" + right["first_row"]["kind"])
        decisions.append(BoundaryDecision.collect("TABLE", a, b, observed=observed).artifact())
    for i in range(len(codes)):
        codes[i] = sorted(set(codes[i]))
    decisions.sort(key=lambda d: (d["right_anchor"], d["kind"]))
    return units, decisions


def materialize_document(document):
    ledger, raw = LineLedger.read(document)
    pages = PageModel(ledger, raw)
    headings = HeadingModel(ledger, pages)
    furniture = FurnitureModel(ledger, headings)
    captions = CaptionModel(ledger, pages, headings, furniture)
    sheets = pages.sheets(ledger)
    units, decisions = assemble(ledger, pages, headings, furniture, captions, sheets)
    quality = ledger.audit(units, raw)
    quality.update({"pages": len(pages.pages), "caption_heading_conflicts": sum(c["decision"] == "REVIEW" for c in captions.records.values()),
                    "boundary_distribution": dict(Counter(d["basis"] for d in decisions)),
                    "decision_distribution": dict(Counter(d["decision"] for d in decisions)),
                    "cache_counters": pages.counters})
    for key in ("duplicate_semantic_lines", "unowned_unexplained_lines", "invalid_owners", "lost_source_lines", "lost_source_blocks"):
        if quality[key]:
            raise ValueError(f"Ownership invariant failed: {key}={quality[key]}")
    return {"ledger": ledger.artifact(),
            "semantics": {"schema": "semantic-foundation.v3", "producer_version": PRODUCER_VERSION,
                          "units": units, "pages": pages.pages, "sheets_v1": sheets,
                          "headings": headings.records, "captions": captions.records},
            "decisions": {"schema": "boundary-decisions.v3", "producer_version": PRODUCER_VERSION,
                          "decisions": decisions}, "quality": quality}
