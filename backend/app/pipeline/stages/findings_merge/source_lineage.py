"""Recover candidate references as provenance, never as verified evidence.

A merge can change a claim while retaining its source ID. Therefore these
references must be reviewed before use as proof or as a severity justification.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any


def text_candidate_lineage(
    findings: list[dict[str, Any]],
    text_analysis: dict[str, Any],
    known_block_ids: set[str],
) -> int:
    """Attach a deterministic receipt for explicit T-ID links; return changes.

    Duplicate IDs are ambiguous even if their contents happen to match. Missing
    blocks stay visible as unresolved references. No page-wide evidence guesses.
    The receipt is rebuilt on each call so stale source links cannot persist.
    """
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    candidates = text_analysis.get("text_findings", [])
    if not isinstance(candidates, list):
        candidates = []
    for candidate in candidates:
        if isinstance(candidate, dict) and isinstance(candidate.get("id"), str):
            index[candidate["id"]].append(candidate)
    changed = 0
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        source_ids = finding.get("source_finding_ids", [])
        if not isinstance(source_ids, list):
            source_ids = []
        receipts = []
        for sid in dict.fromkeys(s for s in source_ids if isinstance(s, str)):
            if not sid.startswith("T-"):
                continue
            matches = index.get(sid, [])
            receipt: dict[str, Any] = {"source_finding_id": sid}
            if len(matches) != 1:
                receipt["status"] = "missing_candidate" if not matches else "ambiguous_candidate_id"
            else:
                candidate = matches[0]
                refs = candidate.get("related_block_ids", [])
                refs = list(dict.fromkeys(x for x in refs if isinstance(x, str) and x)) if isinstance(refs, list) else []
                receipt.update(
                    status="requires_semantic_review",
                    source_claim=candidate.get("finding"),
                    source_location=candidate.get("source"),
                    existing_block_ids=[x for x in refs if x in known_block_ids],
                    unresolved_block_ids=[x for x in refs if x not in known_block_ids],
                )
            receipts.append(receipt)
        if receipts:
            if finding.get("source_candidate_lineage") != receipts:
                finding["source_candidate_lineage"] = receipts
                changed += 1
        elif "source_candidate_lineage" in finding:
            del finding["source_candidate_lineage"]
            changed += 1
    return changed
