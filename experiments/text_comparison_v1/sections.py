"""Section V3 ownership assembler over the existing Foundation ledger and models.

PageModel routes non-text, but a page transition never opens a section. Weak
headings create an explicit ownership uncertainty interval, not a proven split.
"""
from collections import Counter
import re

from experiments.semantic_foundation_v3.ledger import LineLedger
from experiments.semantic_foundation_v3.page_model import PageModel
from experiments.semantic_foundation_v3.models import HeadingModel, FurnitureModel, CaptionModel
from experiments.semantic_foundation_v3.boundary import BoundaryDecision
from experiments.semantic_foundation_v3 import frozen_v1 as foundation

from . import PRODUCER_VERSION
from .common import digest, file_hash, norm


def materialize(document, *, heading_transform=None):
    for kind in ("work_md", "blocks"):
        receipt = document["artifacts"][kind]
        if file_hash(receipt["path"]) != receipt["sha256"]:
            raise ValueError(f"Pinned {kind} source changed")
    ledger, raw = LineLedger.read(document)
    pages = PageModel(ledger, raw)
    headings = HeadingModel(ledger, pages)
    # Offline research extension: reuse the same ledger and ownership assembler.
    # Default V1 behavior is unchanged; evidence adapters never replace parsing.
    adjustments = []
    if heading_transform is not None:
        adjustments = heading_transform(ledger, pages, headings) or []
    furniture = FurnitureModel(ledger, headings)
    captions = CaptionModel(ledger, pages, headings, furniture)
    sections, decisions, ownership, exclusions = [], [], [], []
    stack, current, previous, uncertainty = [], None, None, None
    keys = Counter()
    html_table = False

    def ref(i):
        return {"document_version": document["document_version"], **ledger.anchor(i, "LINE")}

    def external(i, kind, reasons):
        nonlocal previous
        row = {"line_id": i, "route": kind, "section_owner": None,
               "status": "EXCLUDED", "reason_codes": sorted(set(reasons)), "source_ref": ref(i)}
        ownership.append(row)
        ledger.columns["kind"][i] = "EXCLUDED"
        ledger.columns["evidence_codes"][i] = row["reason_codes"]
        ledger.claim(i, "EXCLUDED")
        exclusions.append(row)
        if current is not None and kind in {"TABLE", "GRAPHIC"}:
            target = sections[current]["table_refs" if kind == "TABLE" else "graphic_refs"]
            block = ledger.blocks[ledger.columns["block_ref"][i]]
            if not target or target[-1]["block_id"] != block["block_id"]:
                target.append({"token": f"[{kind}_REF]", "block_id": block["block_id"],
                               "page": block["page"], "source_ref": ref(i),
                               "association": "PRECEDING_SECTION_LOCAL_EVIDENCE_ONLY"})

    for i, line in enumerate(ledger.lines):
        text, clean = line.text, ledger.clean[i]
        block_type = line.block_type.casefold()
        if re.search(r"<table\b", text, re.I):
            html_table = True
        table = (html_table or block_type == "table" or bool(foundation.TABLE_ROW_RE.match(text))
                 or bool(re.search(r"<t[rdh]\b", text, re.I))
                 or (text.count("|") >= 3))
        if re.search(r"</table>", text, re.I):
            html_table = False
        if table:
            external(i, "TABLE", ["TABLE_STRUCTURE_OR_BLOCK"])
            continue
        if block_type in {"image", "graphic", "drawing", "sheet"} or re.match(r"\s*!\[", text):
            external(i, "GRAPHIC", ["GRAPHIC_BLOCK_OR_REFERENCE"])
            continue
        if i in furniture.records:
            external(i, "FURNITURE", furniture.records[i])
            continue
        if not pages.eligible(line.page):
            c = pages.pages[line.page]["classification"]
            external(i, "GRAPHIC" if c["unit_type"] == "SHEET" else "NON_NARRATIVE_PAGE",
                     c["reason_codes"] or ["PAGE_NOT_TEXT_ELIGIBLE"])
            continue
        cap, h = captions.records.get(i), headings.records.get(i)
        if cap:
            external(i, "TABLE", cap["evidence_codes"] + ["CAPTION_NOT_SECTION_TEXT"])
            if cap["decision"] == "REVIEW":
                decisions.append(BoundaryDecision.collect("SECTION", previous, i,
                    join=["POSITIONAL_CAPTION"], split=["STRONG_HEADING"],
                    observed=["EXTERNAL_REFERENCE_ONLY"]).artifact())
                uncertainty = "CAPTION_HEADING_CONFLICT"
            continue
        join, split, observed = set(), set(), set()
        repeated = None
        if h:
            observed.update(h["evidence_codes"])
            md = pages.heading_info.get(i)
            number = h["number"]
            title = md["title"] if md else re.sub(r"^\d+(?:\.\d+)*[.)]?\s+", "", clean).strip()
            normalized = norm(title)
            level = h["level"] or (len(stack) + 1)
            repeated = next((j for j in reversed(stack)
                if (sections[j]["section_number"] == number or number is None)
                and sections[j]["normalized_title"] == normalized), None)
            if repeated is not None:
                join.add("REPEATED_ACTIVE_HEADING")
            elif h["tier"] == "STRONG":
                split.add("EXPLICIT_HEADING_STRUCTURE")
            else:
                observed.add("WEAK_HEADING_UNPROMOTED")
                uncertainty = "WEAK_HEADING_UNRESOLVED"
        elif current is not None:
            join.add("ORDERED_NARRATIVE_CONTINUATION")
            if previous is not None and ledger.lines[previous].page != line.page:
                observed.add("PAGE_TRANSPARENT_FLOW")
        else:
            observed.add("NO_PROVEN_HEADING")
        boundary = BoundaryDecision.collect("SECTION", previous, i, join=join, split=split, observed=observed)
        # Store every narrative decision, so any DEV anchor interval can replay.
        decisions.append(boundary.artifact())
        is_heading = h and boundary.basis == "PROVEN"
        if boundary.decision == "NEW":
            while stack and sections[stack[-1]]["heading_level"] >= level:
                stack.pop()
            parent = sections[stack[-1]] if stack else None
            path = (parent["heading_path"][:] if parent else []) + [
                {"number": number, "title": title, "normalized_title": normalized}]
            key = "section_" + digest({"path": [{"number": p["number"], "title": p["normalized_title"]} for p in path]})[:24]
            keys[key] += 1
            current = len(sections)
            sections.append({"section_key": key, "instance_id": key + f"_{keys[key]}",
                "document_version": document["document_version"], "heading_path": path,
                "section_number": number, "section_title": title, "normalized_title": normalized,
                "heading_level": level, "parent_section": parent["section_key"] if parent else None,
                "ordered_text_blocks": [], "page_span": [], "source_refs": [],
                "table_refs": [], "graphic_refs": [], "status": "PROVEN", "review_reasons": [],
                "producer_version": PRODUCER_VERSION})
            stack.append(current)
            uncertainty = None
        elif repeated is not None:
            current = repeated
            stack = stack[:stack.index(repeated) + 1]
            uncertainty = None
        status = "PROVEN" if current is not None and uncertainty is None else "REVIEW"
        reasons = [uncertainty or "NO_PROVEN_HEADING"] if status == "REVIEW" else list(boundary.evidence_codes)
        owner = sections[current]["instance_id"] if current is not None else None
        ownership.append({"line_id": i, "route": "TEXT", "section_owner": owner if status == "PROVEN" else None,
                          "candidate_owner": owner if status == "REVIEW" else None,
                          "status": status, "reason_codes": reasons, "source_ref": ref(i)})
        ledger.columns["kind"][i] = "HEADING" if is_heading else "TEXT"
        ledger.columns["evidence_codes"][i] = reasons
        ledger.claim(i, current if status == "PROVEN" else "REVIEW")
        if current is not None:
            s = sections[current]
            s["source_refs"].append(ref(i))
            s["page_span"].append(line.page)
            if status == "REVIEW":
                s["status"] = "REVIEW"
                s["review_reasons"].extend(reasons)
            if not is_heading:
                blocks = s["ordered_text_blocks"]
                # Blank lines separate paragraphs on one page; page envelopes do not.
                new_block = (not blocks or (previous is not None and line.page == ledger.lines[previous].page
                            and line.markdown_line > ledger.lines[previous].markdown_line + 1)
                            or bool(re.match(r"\s*[-•]\s+", text)))
                if new_block:
                    blocks.append({"text": text, "source_refs": [ref(i)], "ownership_status": status})
                else:
                    blocks[-1]["text"] += " " + text
                    blocks[-1]["source_refs"].append(ref(i))
                    if status == "REVIEW":
                        blocks[-1]["ownership_status"] = "REVIEW"
        previous = i

    # Images with no Markdown text still exist in the ledger. Local refs are
    # attached only if their source position has a preceding section on that page.
    for block in ledger.blocks:
        if block["state"] != "NO_MARKDOWN_CONTENT":
            continue
        exclusions.append({"route": "GRAPHIC" if block["block_type"] in {"graphic", "image"} else "NO_TEXT",
                           "reason_codes": ["NO_MARKDOWN_CONTENT"], "block_id": block["block_id"], "page": block["page"]})
    for s in sections:
        s["page_span"] = sorted(set(s["page_span"]))
        s["review_reasons"] = sorted(set(s["review_reasons"]))
        if keys[s["section_key"]] > 1:
            s["status"] = "REVIEW"
            s["review_reasons"].append("NON_UNIQUE_SEMANTIC_KEY")
            for block in s["ordered_text_blocks"]:
                block["ownership_status"] = "REVIEW"
    ambiguous = {s["instance_id"] for s in sections if "NON_UNIQUE_SEMANTIC_KEY" in s["review_reasons"]}
    for r in ownership:
        if r["section_owner"] in ambiguous:
            r["candidate_owner"], r["section_owner"], r["status"] = r["section_owner"], None, "REVIEW"
            r["reason_codes"] = sorted(set(r["reason_codes"] + ["NON_UNIQUE_SEMANTIC_KEY"]))
            ledger.columns["owner"][r["line_id"]] = "REVIEW"
            ledger.columns["evidence_codes"][r["line_id"]] = r["reason_codes"]
    if len(ownership) != len(ledger.lines) or len({r["line_id"] for r in ownership}) != len(ledger.lines):
        raise ValueError("Line ownership partition failed")
    quality = {"source_lines": len(ledger.lines), "pages": len(pages.pages), "sections": len(sections),
               "narrative_lines": sum(r["route"] == "TEXT" for r in ownership),
               "proven_narrative_lines": sum(r["route"] == "TEXT" and r["status"] == "PROVEN" for r in ownership),
               "review_narrative_lines": sum(r["route"] == "TEXT" and r["status"] == "REVIEW" for r in ownership),
               "routes": dict(Counter(r["route"] for r in ownership)),
               "unexplained_unowned": sum(r["section_owner"] is None and not r["reason_codes"] for r in ownership),
               "duplicate_ownership": 0, "table_content_in_section_text": 0, "graphic_content_in_section_text": 0}
    return {"schema": "text-section-materialization.v3", "producer_version": PRODUCER_VERSION,
            "document_version": document["document_version"], "sections": sections, "ownership": ownership,
            "decisions": decisions, "exclusions": exclusions, "quality": quality,
            "ledger": ledger.artifact(), "page_model": pages.pages,
            **({"heading_adjustments": adjustments} if heading_transform is not None else {})}
