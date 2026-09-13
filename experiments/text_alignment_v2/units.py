"""Paragraph/list units from the existing ledger routing and section context."""
from collections import Counter
import re
from pathlib import Path

from experiments.text_comparison_v1.common import digest, file_hash
from experiments.text_safe_coverage.sections import materialize

MAX_LOCAL_CHARS = 6000
SENTENCE = re.compile(r"(?<=[.!?;])\s+(?=[А-ЯЁA-Z]|[-•]\s)")


def surface(text):
    # Do not strip underscores inside equipment identifiers or numeric punctuation.
    text = re.sub(r"\*{1,2}([^*]+)\*{1,2}", r"\1", text)
    text = re.sub(r"^\s*(?:[-•]\s+|#{1,6}\s+)", "", text)
    return re.sub(r"\s+", " ", text).strip()


def canonical_text(text):
    return surface(text).casefold().replace("ё", "е").replace("\u00a0", " ")


def lexical(text):
    return re.findall(r"[а-яёa-z]+|\d+(?:[.,]\d+)?", canonical_text(text))


def from_materialization(document, mat):
    receipt = document["artifacts"]["work_md"]
    if file_hash(receipt["path"]) != receipt["sha256"]:
        raise ValueError("Narrative source changed")
    lines = Path(receipt["path"]).read_text().splitlines()
    cols, blocks = mat["ledger"]["columns"], mat["ledger"]["blocks"]
    sections = {s["instance_id"]: s for s in mat["sections"]}
    headings = {}
    for s in mat["sections"]:
        for ref in s["source_refs"]:
            if cols["kind"][ref["line_id"]] == "HEADING":
                headings[ref["line_id"]] = s
    units, exclusions, pending, nearest = [], [], [], None

    def flush():
        nonlocal pending
        if not pending:
            return
        text, spans = "", []
        for row, raw in pending:
            start = len(text) + (1 if text else 0)
            text += (" " if text else "") + raw
            spans.append((start, len(text), row))
        # Natural sentence boundaries only, when a paragraph is too large for a
        # small local package. Unsplittable oversize units are retained as REVIEW.
        cuts = [0]
        if len(text) > 1800:
            cuts += [m.end() for m in SENTENCE.finditer(text)]
        cuts.append(len(text))
        parent = "paragraph_" + digest([document["document_version"], [r[0]["line_id"] for r in pending]])[:24]
        for a, b in zip(cuts, cuts[1:]):
            raw = text[a:b].strip()
            rows = [r for x, y, r in spans if x < b and y > a]
            if not raw or not rows:
                continue
            refs = [r["source_ref"] for r in rows]
            owner_ids = sorted({r.get("section_owner") or r.get("candidate_owner") for r in rows} - {None})
            contexts = [{"instance_id": i, "section_key": sections[i]["section_key"],
                         "title": sections[i]["section_title"], "heading_path": sections[i]["heading_path"],
                         "status": sections[i]["status"]} for i in owner_ids]
            nwords = len(re.findall(r"[а-яёa-z]{2,}", raw.casefold()))
            # Small labels remain traceable, but do not independently assert a
            # narrative engineering subject. No fixed token-window splitting.
            reasons = []
            if nwords < 5:
                reasons.append("SHORT_LABEL_OR_FRAGMENT")
            if len(raw) > MAX_LOCAL_CHARS:
                reasons.append("UNSPLITTABLE_LOCAL_BUDGET")
            if re.fullmatch(r"[\W\d_]+", raw):
                reasons.append("NUMBER_OR_PUNCTUATION_ONLY")
            uid = "unit_" + digest([document["document_version"], refs, [a,b], raw])[:24]
            units.append({"unit_id": uid, "document_version": document["document_version"],
                          "document_code": document["document_code"], "ordinal": len(units),
                          "kind": "SENTENCE_CLUSTER" if len(cuts)>2 else "PARAGRAPH_OR_LIST_ITEM",
                          "paragraph_id": parent, "paragraph_span": [a,b], "text": raw,
                          "source_refs": refs, "page_span": sorted({r["page"] for r in refs}),
                          "block_ids": sorted({r["block_id"] for r in refs}),
                          "section_context": contexts, "nearest_heading": nearest["section_title"] if nearest else None,
                          "section_ownership_proven": all(r["status"]=="PROVEN" for r in rows),
                          "eligibility": "REVIEW" if reasons else "ELIGIBLE", "review_reasons": reasons,
                          "external_refs": sorted({x["token"] for i in owner_ids for k in ("table_refs", "graphic_refs")
                                                   for x in sections[i][k]}),
                          "source_route": "TEXT"})
        pending = []

    for row in mat["ownership"]:
        i = row["line_id"]
        raw = lines[row["source_ref"]["markdown_line"]-1]
        if row["route"] != "TEXT" or cols["kind"][i] == "HEADING":
            flush()
            if i in headings:
                nearest = headings[i]
            exclusions.append({"line_id": i, "route": row["route"] if row["route"]!="TEXT" else "HEADING",
                               "source_ref": row["source_ref"]})
            continue
        if pending:
            prev = pending[-1][0]
            same_owner = (prev.get("section_owner") or prev.get("candidate_owner")) == (row.get("section_owner") or row.get("candidate_owner"))
            same_block = cols["block_ref"][prev["line_id"]] == cols["block_ref"][i]
            gap = row["source_ref"]["markdown_line"] - prev["source_ref"]["markdown_line"]
            if (not same_owner or not same_block or gap > 1 or re.match(r"^\s*(?:[-•]|\d+[)])\s+", raw)):
                flush()
        pending.append((row, raw))
    flush()
    expected = {r["line_id"] for r in mat["ownership"] if r["route"] == "TEXT" and cols["kind"][r["line_id"]] != "HEADING"}
    actual = {r["line_id"] for u in units for r in u["source_refs"]}
    if actual != expected:
        raise ValueError("Local narrative line coverage failed")
    return {"schema": "local-narrative-units.v2", "document_version": document["document_version"],
            "source_receipts": document["artifacts"], "foundation_quality": mat["quality"],
            "units": units, "exclusions": exclusions,
            "quality": {"units": len(units), "eligible": sum(u["eligibility"]=="ELIGIBLE" for u in units),
                        "represented_narrative_lines": len(actual), "lost_narrative_lines": 0,
                        "routes_excluded": dict(Counter(x["route"] for x in exclusions)),
                        "table_content_compared": False, "graphic_content_compared": False}}


def build(document):
    return from_materialization(document, materialize(document))
