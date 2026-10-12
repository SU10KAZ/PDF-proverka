"""Read-only, local content proposals inside a saved Mapper region.

These are navigation suggestions, never Human Mapping decisions or anchors.
No inference, writes, live-source reads, or all-to-all expansion. Graphic
descriptions cannot establish a graphic correspondence. Ambiguous repeated
text is deliberately left unresolved.
"""
from __future__ import annotations

import html
import re
from collections import defaultdict

VERSION = "content-links/1"


def _features(block):
    if block.get("type") not in {"TEXT", "TABLE"}:
        return None
    text = html.unescape(str(block.get("structured_md") or ""))
    text = re.sub(r"<[^>]*>", " ", text)
    tokens = re.findall(r"[+-]?\d+(?:[.,/]\d+)*|[\w]+(?:[.,/-][\w]+)*|[≤≥<>±=]", text.casefold(), re.UNICODE)
    # Short headings, stamps and table headers are not sufficient evidence.
    if len(tokens) < 24 or len(set(tokens)) < 16:
        return None
    shingles = {tuple(tokens[i:i + 4]) for i in range(len(tokens) - 3)}
    return tokens, shingles, " ".join(text.casefold().split())


def propose_region_links(region):
    """Conservative mutual best matches; scores are overlap, not confidence."""
    olds, news = region.get("old_blocks") or [], region.get("new_blocks") or []
    if len(olds) < 2 or len(news) < 2:
        return []  # Existing 1:1 / 1:N / N:1 membership links stay unchanged.
    features = {(side, b["id"]): _features(b)
                for side, blocks in (("OLD", olds), ("NEW", news)) for b in blocks}
    candidates = []
    by_old, by_new = defaultdict(list), defaultdict(list)
    for old in olds:
        a = features["OLD", old["id"]]
        if not a:
            continue
        for new in news:
            b = features["NEW", new["id"]]
            if not b or old["type"] != new["type"]:
                continue
            shared = a[1] & b[1]
            score = 2 * len(shared) / (len(a[1]) + len(b[1]))
            if score < .5:
                continue
            row = (score, old, new, a, b, shared)
            candidates.append(row)
            by_old[old["id"]].append(score)
            by_new[new["id"]].append(score)
    result = []
    for score, old, new, a, b, shared in candidates:
        if score < .8 or len(shared) < 12:
            continue
        # A tie or near-tie in either direction is unresolved, never broken by ID.
        def separated(scores):
            ordered = sorted(scores, reverse=True)
            return score == ordered[0] and (len(ordered) == 1 or score - ordered[1] >= .1)
        if not separated(by_old[old["id"]]) or not separated(by_new[new["id"]]):
            continue
        exact = a[2] == b[2]
        reason = ("Совпадает распознанный текст блоков внутри одной смысловой группы."
                  if exact else "Близкие текстовые фрагменты внутри одной смысловой группы; "
                  "среди её блоков нет равноценного кандидата в обоих направлениях.")
        result.append({"old_block_id": old["id"], "new_block_id": new["id"],
                       "method": "EXACT_TEXT" if exact else "TEXT_OVERLAP",
                       "reason": reason + " Соответствие предложено, требуется проверка.",
                       "shared_excerpt": " ".join(sorted(shared)[0])[:180]})
    return sorted(result, key=lambda r: (r["old_block_id"], r["new_block_id"]))


def propose_links(regions):
    return {r["id"]: propose_region_links(r) for r in regions}
