"""High-confidence, deterministic repair of wrong one-to-one sheet links.

The planner is deliberately narrower than the ordinary sheet matcher.  It is
only allowed to touch links that Stage 5 has already rejected by sheet purpose.
Stage 5.1 title proof remains authoritative; Stage 5.2 adds explainable content
proof from compact existing-text fingerprints.  No PDF pixels, OCR retry,
embeddings or model calls are involved here.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections import Counter
from difflib import SequenceMatcher
from typing import Any

from . import content_sheet_link_repair


VERSION = 1
KIND = "stage_comparison_sheet_link_repairs"
FUZZY_THRESHOLD = 0.94
FUZZY_MARGIN = 0.02
TITLE_EXACT = "TITLE_EXACT"
TITLE_MUTUAL_FUZZY = "TITLE_MUTUAL_FUZZY"

_TYPE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("single_line", re.compile(r"однолинейн")),
    ("plan", re.compile(r"\bплан\w*\b")),
    ("scheme", re.compile(r"\bсхем\w*\b")),
    ("node", re.compile(r"\bуз(?:ел|ла|лы|лов|ле)\b|\bдетал")),
    ("explication", re.compile(r"экспликац")),
    ("section", re.compile(r"\bразрез\w*\b")),
    ("facade", re.compile(r"\bфасад\w*\b")),
)
_GENERIC_RE = re.compile(
    r"^(?:архитектурные решения|общие данные|графическая часть|текстовая часть|"
    r"план|схема|узел|деталь|разрез|фасад|кладочные планы?)$"
)
_STOP_WORDS = {
    "архитектурные", "решения", "часть", "лист", "листа", "схема", "схемы",
    "план", "планы", "узел", "узла", "деталь", "расчетная", "расчетной",
    "общие", "данные", "этаж", "этажа", "по", "на", "для", "из", "и", "в",
}


def normalize_title(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower().replace("ё", "е")
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"[^a-zа-я0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _title(record: dict[str, Any]) -> str:
    return normalize_title(str(record.get("title") or ""))


def _is_specific(title: str) -> bool:
    tokens = title.split()
    return bool(
        title and len(tokens) >= 2 and not _GENERIC_RE.fullmatch(title)
        and not re.fullmatch(r"(?:страница|лист)\s+\d+(?:\s+из\s+\d+)?", title)
    )


def _purpose_type(title: str) -> str:
    return next((name for name, pattern in _TYPE_PATTERNS if pattern.search(title)), "")


def _subject_tokens(title: str) -> set[str]:
    return {
        token for token in title.split()
        if len(token) > 1 and token not in _STOP_WORDS
    }


def _fuzzy_compatible(left: str, right: str) -> bool:
    left_type, right_type = _purpose_type(left), _purpose_type(right)
    if not left_type or left_type != right_type:
        return False
    return bool(_subject_tokens(left) & _subject_tokens(right))


def _ranked_matches(
    source: tuple[int, str], candidates: list[tuple[int, str]],
) -> list[tuple[float, int]]:
    source_title = source[1]
    ranked = [
        (SequenceMatcher(None, source_title, candidate_title).ratio(), page)
        for page, candidate_title in candidates
        if _fuzzy_compatible(source_title, candidate_title)
    ]
    return sorted(ranked, key=lambda item: (-item[0], item[1]))


def _fuzzy_candidate(
    right: tuple[int, str], left_records: list[tuple[int, str]],
    right_records: list[tuple[int, str]],
) -> tuple[int, float] | None:
    ranked_left = _ranked_matches(right, left_records)
    if not ranked_left or ranked_left[0][0] < FUZZY_THRESHOLD:
        return None
    if len(ranked_left) > 1 and ranked_left[0][0] - ranked_left[1][0] < FUZZY_MARGIN:
        return None
    score, left_page = ranked_left[0]
    left = next(record for record in left_records if record[0] == left_page)
    ranked_right = _ranked_matches(left, right_records)
    if not ranked_right or ranked_right[0][1] != right[0]:
        return None
    if len(ranked_right) > 1 and ranked_right[0][0] - ranked_right[1][0] < FUZZY_MARGIN:
        return None
    return left_page, score


def _candidate_for_right(
    right: tuple[int, str], left_records: list[tuple[int, str]],
    right_records: list[tuple[int, str]], left_counts: Counter[str],
    right_counts: Counter[str],
) -> dict[str, Any] | None:
    right_page, right_title = right
    if not _is_specific(right_title):
        return None
    exact = [page for page, title in left_records if title == right_title]
    if len(exact) == 1 and left_counts[right_title] == 1 and right_counts[right_title] == 1:
        return {
            "left_page": exact[0], "right_page": right_page,
            "rule": TITLE_EXACT, "similarity": 1.0,
        }
    fuzzy = _fuzzy_candidate(right, left_records, right_records)
    if fuzzy is None:
        return None
    left_page, score = fuzzy
    return {
        "left_page": left_page, "right_page": right_page,
        "rule": TITLE_MUTUAL_FUZZY, "similarity": round(score, 6),
    }


def source_signature(
    links_payload: dict[str, Any], suggestions: dict[str, Any], problem_group_ids: set[str],
    source_groups: list[dict[str, Any]] | None = None,
) -> str:
    source = {
        "version": VERSION,
        "links": {
            "version": links_payload.get("version"),
            "pair_id": links_payload.get("pair_id"),
            "links": links_payload.get("links") or [],
            "unlinked_left_pages": links_payload.get("unlinked_left_pages") or [],
        },
        "left_sheet_index": suggestions.get("left_sheet_index") or [],
        "right_sheet_index": suggestions.get("right_sheet_index") or [],
        "problem_group_ids": sorted(problem_group_ids),
        "cross_sheet_evidence": [
            {
                "group_id": str(group.get("group_id") or ""),
                "source_group_sha256": str(group.get("source_group_sha256") or ""),
                "evidence": [
                    {
                        "evidence_id": str(item.get("evidence_id") or ""),
                        "source_status": str(item.get("source_status") or ""),
                        "right_pages": list(item.get("right_pages") or []),
                    }
                    for item in group.get("atomic_evidence") or []
                    if "FOUND_ON_OTHER_SHEET" in str(item.get("source_status") or "").upper()
                    or "FOUND_ON_OTHER_SHEET" in str(item.get("reason") or "").upper()
                ],
            }
            for group in source_groups or []
            if str(group.get("group_id") or "") in problem_group_ids
        ],
    }
    encoded = json.dumps(source, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _plan_title_repairs(
    links_payload: dict[str, Any], suggestions: dict[str, Any], problem_group_ids: set[str],
) -> dict[str, Any] | None:
    """Return an atomic replacement payload or ``None`` when proof is insufficient."""
    links = [dict(link) for link in links_payload.get("links") or []]
    left_index = [dict(item) for item in suggestions.get("left_sheet_index") or []]
    right_index = [dict(item) for item in suggestions.get("right_sheet_index") or []]
    left_records = [(int(item["pdf_page"]), _title(item)) for item in left_index if _title(item)]
    right_records = [(int(item["pdf_page"]), _title(item)) for item in right_index if _title(item)]
    if not links or not left_records or not right_records or not problem_group_ids:
        return None

    left_counts = Counter(title for _page, title in left_records)
    right_counts = Counter(title for _page, title in right_records)
    title_by_left = dict(left_records)
    title_by_right = dict(right_records)

    # Any duplicated page membership makes the component ambiguous even if
    # individual link arrays happen to be one-to-one.
    left_membership: Counter[int] = Counter()
    right_membership: Counter[int] = Counter()
    for link in links:
        left_membership.update(int(page) for page in link.get("left_pages") or [])
        right_membership.update(int(page) for page in link.get("right_pages") or [])

    unique_by_id: dict[str, dict[str, Any]] = {}
    current_by_left: dict[int, dict[str, Any]] = {}
    current_by_right: dict[int, dict[str, Any]] = {}
    for link in links:
        link_id = str(link.get("id") or "")
        left_pages = [int(page) for page in link.get("left_pages") or []]
        right_pages = [int(page) for page in link.get("right_pages") or []]
        if (
            len(left_pages) == len(right_pages) == 1
            and left_membership[left_pages[0]] == right_membership[right_pages[0]] == 1
        ):
            unique_by_id[link_id] = link
            current_by_left[left_pages[0]] = link
            current_by_right[right_pages[0]] = link

    raw_candidates: dict[int, dict[str, Any]] = {}
    problem_rights: set[int] = set()
    for group_id in sorted(problem_group_ids):
        link = unique_by_id.get(group_id)
        if link is None:
            continue
        current_left = int(link["left_pages"][0])
        right_page = int(link["right_pages"][0])
        problem_rights.add(right_page)
        right_title = title_by_right.get(right_page, "")
        candidate = _candidate_for_right(
            (right_page, right_title), left_records, right_records, left_counts, right_counts,
        )
        candidate_left = int((candidate or {}).get("left_page") or 0)
        candidate_is_safe = (
            left_membership[candidate_left] == 0 or candidate_left in current_by_left
        )
        if candidate and candidate_left != current_left and candidate_is_safe:
            raw_candidates[right_page] = candidate

    if not raw_candidates:
        return None
    # Two desired right pages may never claim one left page.
    claimed_left = Counter(int(item["left_page"]) for item in raw_candidates.values())
    raw_candidates = {
        right: item for right, item in raw_candidates.items()
        if claimed_left[int(item["left_page"])] == 1
    }

    # A chain may displace another current link only if that link is also a
    # rejected group and has its own proven destination.  This admits swaps and
    # longer cycles atomically while rejecting partial/destructive rewires.
    valid_rights: set[int] = set()
    for start in sorted(raw_candidates):
        chain: set[int] = set()
        cursor = start
        safe = True
        while cursor not in chain:
            chain.add(cursor)
            candidate = raw_candidates.get(cursor)
            if candidate is None:
                safe = False
                break
            occupied = current_by_left.get(int(candidate["left_page"]))
            if occupied is None:
                break
            occupied_right = int(occupied["right_pages"][0])
            if occupied_right == cursor:
                break
            if occupied_right not in problem_rights or occupied_right not in raw_candidates:
                safe = False
                break
            cursor = occupied_right
        if safe:
            valid_rights.update(chain)

    candidates = {right: raw_candidates[right] for right in sorted(valid_rights)}
    if not candidates:
        return None
    affected_ids = {
        str(current_by_right[right].get("id")) for right in candidates
        if right in current_by_right
    }
    before_links = [dict(link) for link in links if str(link.get("id")) in affected_ids]
    kept_links = [dict(link) for link in links if str(link.get("id")) not in affected_ids]
    after_links = []
    changes = []
    for right_page, candidate in candidates.items():
        left_page = int(candidate["left_page"])
        link = {
            "id": f"repair_{hashlib.sha256(f'{left_page}:{right_page}'.encode()).hexdigest()[:12]}",
            "left_pages": [left_page], "right_pages": [right_page],
            "source": "auto_repair", "confidence": "high",
            "reason": [candidate["rule"], "stage5_purpose_precheck_repair"],
        }
        after_links.append(link)
        old = current_by_right.get(right_page) or {}
        changes.append({
            "left_page_before": int((old.get("left_pages") or [0])[0]) or None,
            "left_page_after": left_page, "right_page": right_page,
            "left_title_before": title_by_left.get(int((old.get("left_pages") or [0])[0]), ""),
            "left_title_after": title_by_left.get(left_page, ""),
            "right_title": title_by_right.get(right_page, ""),
            "rule": candidate["rule"], "similarity": candidate["similarity"],
        })
    next_links = kept_links + after_links
    linked_left = {int(page) for link in next_links for page in link.get("left_pages") or []}
    formerly_linked = {int(page) for link in before_links for page in link.get("left_pages") or []}
    unlinked = {
        int(page) for page in links_payload.get("unlinked_left_pages") or []
    } | (formerly_linked - linked_left)
    unlinked -= linked_left
    after_payload = {
        "version": 1, "pair_id": links_payload.get("pair_id"),
        "links": next_links, "unlinked_left_pages": sorted(unlinked),
        "updated_at": None,
    }
    return {
        "source_signature": source_signature(links_payload, suggestions, problem_group_ids),
        "reason": (
            TITLE_EXACT
            if all(change["rule"] == TITLE_EXACT for change in changes)
            else TITLE_MUTUAL_FUZZY
        ),
        "confidence": "high", "before_links": before_links, "after_links": after_links,
        "changes": changes, "before_snapshot": links_payload, "after_snapshot": after_payload,
    }


def plan_repairs(
    links_payload: dict[str, Any], suggestions: dict[str, Any], problem_group_ids: set[str],
    *, source_groups: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Prefer Stage 5.1 title proof, then try conservative Stage 5.2 content proof."""
    title_plan = _plan_title_repairs(links_payload, suggestions, problem_group_ids)
    if title_plan is not None:
        return title_plan
    signature = source_signature(
        links_payload, suggestions, problem_group_ids, source_groups,
    )
    return content_sheet_link_repair.plan_content_repairs(
        links_payload,
        suggestions,
        problem_group_ids,
        source_groups=source_groups,
        source_signature=signature,
    )


def empty_artifact(pair_id: str) -> dict[str, Any]:
    return {"version": VERSION, "kind": KIND, "pair_id": pair_id, "repairs": []}


def public_view(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("version") != VERSION or payload.get("kind") != KIND:
        return {"version": VERSION, "kind": KIND, "repairs": [], "active_repairs": []}
    repairs = list(payload.get("repairs") or [])
    return {**payload, "repairs": repairs, "active_repairs": [item for item in repairs if item.get("status") == "applied"]}


__all__ = [
    "FUZZY_MARGIN", "FUZZY_THRESHOLD", "KIND", "VERSION", "empty_artifact",
    "TITLE_EXACT", "TITLE_MUTUAL_FUZZY", "normalize_title", "plan_repairs",
    "public_view", "source_signature",
]
