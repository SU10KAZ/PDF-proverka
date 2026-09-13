"""Small evidence adapters over the unchanged Foundation source ledger."""
from collections import Counter
import re

from experiments.text_comparison_v1.sections import materialize as assemble
from experiments.text_comparison_v1.dev_score import predict as baseline_predict


def adapt_headings(ledger, pages, headings, approach):
    adjustments = []
    if approach in {"hierarchy", "layout"}:
        # Hypothesis A: retain explicit OCR Markdown hierarchy and corroborate
        # numbered candidates from sibling numbering. Never consult case IDs.
        numbers = {h["number"] for h in headings.records.values() if h["number"]}
        for i, h in headings.records.items():
            md = re.match(r"^(#{4,6})\s+", ledger.lines[i].text)
            if md:
                adjustments.append({"source_ref": ledger.anchor(i, "LINE"), "action": "MARKDOWN_LEVEL",
                                    "before": h["level"], "after": len(md[1]) - 2})
                h["level"] = len(md[1]) - 2
            n = h["number"]
            if approach == "hierarchy" and h["tier"] == "CANDIDATE" and n:
                parts = n.split(".")
                next_number = ".".join(parts[:-1] + [str(int(parts[-1]) + 1)])
                if next_number in numbers:
                    h["tier"] = "STRONG"
                    h["evidence_codes"].append("DOCUMENT_NUMBER_SEQUENCE")
                    adjustments.append({"source_ref": ledger.anchor(i, "LINE"), "action": "PROMOTE_NUMBER_SEQUENCE"})
        return adjustments
    if approach not in {"continuity", "structural"}:
        return adjustments
    # A numbered prose paragraph is not a heading merely because it starts
    # with a number. Require a long, punctuated assertion, without Markdown
    # heading or whole-line emphasis. Short labels remain unresolved.
    for i, h in list(headings.records.items()):
        clean = ledger.clean[i]
        if (h["evidence_codes"] == ["NUMBER_CHAIN_CANDIDATE"]
                and len(clean.split()) >= 18
                and re.search(r"[.;:]\s*$", clean)):
            del headings.records[i]
            adjustments.append({"source_ref": ledger.anchor(i, "LINE"),
                                "action": "NUMBERED_PROSE_NOT_HEADING", "original_heading": h,
                                "evidence_codes": ["PLAIN_NUMBER_CHAIN", "AT_LEAST_18_WORDS", "TERMINAL_PUNCTUATION"]})
    return adjustments


def materialize(document, *, approach="structural"):
    result = assemble(document, heading_transform=lambda l, p, h: adapt_headings(l, p, h, approach))
    result["producer_version"] = "text-safe-coverage-v1/" + approach
    result["boundary_certificates"] = []
    # Certainty of a local relation and uniqueness of a cross-version key are
    # separate. A repeated active heading certifies continuation of that
    # container; it does NOT repair direct ownership of an uncertain prefix.
    if approach in {"continuity", "structural"}:
        owners = {r["line_id"]: r for r in result["ownership"]}
        for d in result["decisions"]:
            if d["decision"] == "SAME" and "REPEATED_ACTIVE_HEADING" in d["evidence_codes"]:
                result["boundary_certificates"].append({
                    "left_anchor": d["left_anchor"], "right_anchor": d["right_anchor"],
                    "decision": "SAME", "scope": "ACTIVE_CONTAINER_CONTINUATION",
                    "reason": "EXACT_REPEATED_ACTIVE_HEADING", "direct_owner_proven": False})
            elif approach == "structural" and d["decision"] == "SAME" and d["left_anchor"] is not None:
                a, b = d["left_anchor"], d["right_anchor"]
                x, y = owners[a], owners[b]
                xowner = x.get("section_owner") or x.get("candidate_owner")
                yowner = y.get("section_owner") or y.get("candidate_owner")
                if (xowner and xowner == yowner and x["route"] == y["route"] == "TEXT"
                        and (x["status"] == "REVIEW" or y["status"] == "REVIEW")
                        and all(owners[i]["route"] in {"TEXT", "FURNITURE"} for i in range(a, b + 1))):
                    result["boundary_certificates"].append({
                        "left_anchor": a, "right_anchor": b, "decision": "SAME",
                        "scope": "LOCAL_NARRATIVE_CONTINUATION", "reason": "UNINTERRUPTED_NARRATIVE_FLOW",
                        "direct_owner_proven": False})
    result["quality"]["boundary_certificate_count"] = len(result["boundary_certificates"])
    return result


def predict(case, result):
    base = baseline_predict(case, result)
    if case["kind"] != "SECTION" or base["answer"] != "REVIEW":
        return base
    anchors = case["source_anchors"]
    owners = {r["line_id"]: r for r in result["ownership"]}
    for a in anchors:
        row = owners.get(a["line_id"])
        if (row is None or row["route"] != "TEXT" or any(
                row["source_ref"].get(k) != a[k] for k in
                ("page", "block_id", "markdown_line", "line_sha256"))):
            return base
    a, b = (x["line_id"] for x in anchors)
    edges = [d for d in result["decisions"] if a < d["right_anchor"] <= b]
    cert = next((c for c in result["boundary_certificates"] if c["right_anchor"] == b), None)
    if cert and cert["scope"] == "LOCAL_NARRATIVE_CONTINUATION" and any(
            owners[i]["route"] not in {"TEXT", "FURNITURE"} for i in range(a, b + 1)):
        return base
    if cert and edges and all(d["decision"] == "SAME" for d in edges):
        return {"answer": "YES", "reason": cert["reason"],
                "scope": cert["scope"], "direct_owner_proven": False}
    return base
