"""
critic_v2/deduplicator.py
--------------------------
Deterministic deduplication of findings.

No LLM, no embeddings. Uses normalized text + Jaccard similarity
on token overlap across title, description, impact_area and action.

A group of duplicates keeps the finding with the highest score
(or earliest ID as tiebreaker). Others get decision=merge.
"""
from __future__ import annotations

import re
import string
from itertools import combinations

from .models import NormalizedFinding

# Threshold above which two findings are considered duplicates
_JACCARD_THRESHOLD = 0.55

# Russian and English stop words to ignore during token comparison
_STOP_WORDS = {
    "в", "на", "не", "и", "с", "к", "по", "для", "от", "из", "за", "при",
    "что", "как", "это", "все", "или", "но", "же", "до", "то", "так", "уже",
    "а", "о", "об", "по", "при", "из", "без", "через", "над", "под",
    "the", "a", "an", "is", "are", "of", "in", "to", "and", "or", "not",
    "for", "with", "that", "this", "it", "be", "as", "at", "by",
}


def _tokenize(text: str) -> set[str]:
    """Lowercase, strip punctuation, remove stop words, return token set."""
    text = text.lower()
    text = text.translate(str.maketrans("", "", string.punctuation + "«»""''–—"))
    tokens = re.split(r"\s+", text.strip())
    return {t for t in tokens if t and t not in _STOP_WORDS and len(t) > 2}


def _signature(nf: NormalizedFinding) -> set[str]:
    """Combined token signature for similarity comparison."""
    parts = [nf.title, nf.description]
    if nf.impact_area:
        parts.append(nf.impact_area)
    if nf.action_required:
        parts.append(nf.action_required[:150])
    return _tokenize(" ".join(parts))


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _same_location(a: NormalizedFinding, b: NormalizedFinding) -> bool:
    """True if both findings point to the same sheet/page."""
    a_raw, b_raw = a.raw, b.raw
    # Sheet match
    a_sheet = str(a_raw.get("sheet", "")).strip().lower()
    b_sheet = str(b_raw.get("sheet", "")).strip().lower()
    if a_sheet and b_sheet and a_sheet == b_sheet:
        return True
    # Page match (scalar or list overlap)
    def pages(r):
        p = r.get("page")
        if p is None:
            return set()
        if isinstance(p, list):
            return set(p)
        return {p}
    return bool(pages(a_raw) & pages(b_raw))


def _key_tokens(nf: NormalizedFinding) -> set[str]:
    """Extract high-signal tokens: object names, norms, numbers, key nouns."""
    text = f"{nf.title} {nf.description}"
    # Keep norm-like tokens (цифры + буквы без пунктуации)
    tokens = set(re.findall(r"[а-яёa-z0-9]{3,}", text.lower()))
    tokens -= _STOP_WORDS
    return tokens


def _shared_evidence_blocks(a: NormalizedFinding, b: NormalizedFinding) -> int:
    """Count shared block_ids between two findings."""
    a_blocks = set(a.evidence_refs)
    b_blocks = set(b.evidence_refs)
    return len(a_blocks & b_blocks)


def _are_duplicates(a: NormalizedFinding, b: NormalizedFinding) -> bool:
    """Check whether two normalized findings are duplicates."""
    sig_a = _signature(a)
    sig_b = _signature(b)
    sim = _jaccard(sig_a, sig_b)

    if sim >= _JACCARD_THRESHOLD:
        return True

    # Lower threshold when they share location AND category
    if sim >= 0.30 and a.category == b.category and _same_location(a, b):
        return True

    # Shared evidence blocks + same category + same location → strong duplicate signal
    if a.category == b.category and _same_location(a, b):
        shared_blocks = _shared_evidence_blocks(a, b)
        if shared_blocks >= 1 and a.impact_area == b.impact_area:
            return True

    # Key-token overlap: same category + location + significant key-token overlap
    if a.category == b.category and _same_location(a, b):
        key_a = _key_tokens(a)
        key_b = _key_tokens(b)
        key_sim = _jaccard(key_a, key_b)
        if key_sim >= 0.25 and a.impact_area == b.impact_area:
            return True

    return False


def deduplicate(
    findings: list[NormalizedFinding],
    scores: dict[str, int],
) -> dict[str, str | None]:
    """
    Find duplicate groups and decide which is primary.

    Returns:
        dict mapping finding_id → merged_into (None for primary, id for duplicates).
    """
    n = len(findings)
    merged_into: dict[str, str | None] = {nf.finding_id: None for nf in findings}

    # Union-Find for grouping
    parent: dict[str, str] = {nf.finding_id: nf.finding_id for nf in findings}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: str, y: str) -> None:
        rx, ry = find(x), find(y)
        if rx != ry:
            # Keep higher-scoring finding as root
            sx = scores.get(rx, 0)
            sy = scores.get(ry, 0)
            if sy > sx or (sy == sx and ry < rx):
                parent[rx] = ry
            else:
                parent[ry] = rx

    # Compare all pairs
    for i in range(n):
        for j in range(i + 1, n):
            if _are_duplicates(findings[i], findings[j]):
                union(findings[i].finding_id, findings[j].finding_id)

    # Build groups
    groups: dict[str, list[str]] = {}
    for nf in findings:
        root = find(nf.finding_id)
        groups.setdefault(root, []).append(nf.finding_id)

    # For groups with >1 member: root is primary, rest get merged_into = root
    for root, members in groups.items():
        if len(members) > 1:
            for fid in members:
                if fid != root:
                    merged_into[fid] = root

    return merged_into
