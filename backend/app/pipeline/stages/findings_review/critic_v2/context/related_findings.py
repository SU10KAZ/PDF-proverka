"""
related_findings.py
--------------------
Find related findings: same category, same sheet, or possible duplicates.

Read-only. Never modifies artifacts.
"""
from __future__ import annotations

import re
from typing import Optional

from .context_models import RelatedFinding

_MIN_JACCARD_DUPLICATE = 0.55
_MIN_JACCARD_RELATED = 0.30
_MAX_RELATED = 5


def _tokenize(text: str) -> set[str]:
    """Simple word tokenizer for Jaccard similarity."""
    return set(re.findall(r"\w{3,}", text.lower()))


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def get_related_findings(
    finding: dict,
    all_findings: list[dict],
    max_related: int = _MAX_RELATED,
) -> list[RelatedFinding]:
    """
    Find findings related to this one by category, sheet, or text similarity.

    Args:
        finding: the current finding to find neighbors for
        all_findings: all findings in the project (including this one)
        max_related: max results to return

    Returns:
        List of RelatedFinding, ordered by relevance
    """
    fid = str(finding.get("id") or "")
    category = str(finding.get("category") or "").lower()
    sheet = str(finding.get("sheet") or "").lower()
    page = finding.get("page")
    if isinstance(page, list):
        pages: set[int] = set()
        for p in page:
            try:
                pages.add(int(p))
            except (TypeError, ValueError):
                pass
    else:
        try:
            pages = {int(page)} if page is not None else set()
        except (TypeError, ValueError):
            pages = set()

    problem_text = (
        (finding.get("problem") or finding.get("title") or "") + " " +
        (finding.get("description") or "")
    ).strip()
    tokens_self = _tokenize(problem_text)

    results: list[RelatedFinding] = []
    seen_ids: set[str] = set()

    for other in all_findings:
        other_id = str(other.get("id") or "")
        if other_id == fid or other_id in seen_ids:
            continue

        other_cat = str(other.get("category") or "").lower()
        other_sheet = str(other.get("sheet") or "").lower()
        other_page = other.get("page")
        try:
            other_pages = {int(other_page)} if isinstance(other_page, (int, float)) else set()
            if isinstance(other_page, list):
                other_pages = {int(p) for p in other_page if p is not None}
        except (TypeError, ValueError):
            other_pages = set()

        other_text = (
            (other.get("problem") or other.get("title") or "") + " " +
            (other.get("description") or "")
        ).strip()
        tokens_other = _tokenize(other_text)
        j = _jaccard(tokens_self, tokens_other)

        sim_type: Optional[str] = None
        sim_score = 0.0

        if j >= _MIN_JACCARD_DUPLICATE:
            sim_type = "possible_duplicate"
            sim_score = j
        elif j >= _MIN_JACCARD_RELATED and category == other_cat:
            sim_type = "same_category"
            sim_score = j
        elif category and category == other_cat and (sheet and sheet == other_sheet):
            sim_type = "same_category"
            sim_score = 0.5
        elif pages and pages & other_pages:
            sim_type = "same_sheet"
            sim_score = 0.3

        if sim_type:
            other_title = (other.get("title") or other.get("problem") or "")[:80]
            results.append(RelatedFinding(
                finding_id=other_id,
                title=other_title,
                category=other.get("category") or "",
                sheet=other.get("sheet") or "",
                page=list(other_pages)[0] if other_pages else None,
                similarity_type=sim_type,
                similarity_score=sim_score,
            ))
            seen_ids.add(other_id)

    # Sort: duplicates first, then by score desc
    results.sort(key=lambda r: (0 if r.similarity_type == "possible_duplicate" else 1, -r.similarity_score))
    return results[:max_related]
