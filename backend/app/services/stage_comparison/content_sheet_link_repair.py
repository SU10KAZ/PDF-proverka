"""Deterministic content layer for conservative one-to-one sheet-link repair."""
from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from collections import Counter
from difflib import SequenceMatcher
from typing import Any

from .sheet_content_fingerprint import has_meaningful_content, purpose_terms


MIN_SCORE = 0.44
MIN_MARGIN = 0.14
MIN_REVERSE_MARGIN = 0.10
MIN_IMPROVEMENT = 0.18
MIN_COMPONENT_OVERLAP = 0.22

CONTENT_UNIQUE_ANCHORS = "CONTENT_UNIQUE_ANCHORS"
CONTENT_MUTUAL_BEST = "CONTENT_MUTUAL_BEST"
CONTENT_SWAP = "CONTENT_SWAP"
CONTENT_3_CYCLE = "CONTENT_3_CYCLE"

_FEATURES: tuple[tuple[str, str, float], ...] = (
    ("rare_term_overlap", "rare_terms", 0.25),
    ("designation_overlap", "unique_designations", 0.20),
    ("system_overlap", "system_names", 0.13),
    ("equipment_overlap", "equipment_codes", 0.16),
    ("structural_token_overlap", "structural_tokens", 0.16),
)
_STRONG_ANCHOR_KEYS = {
    "unique_designations", "system_names", "equipment_codes", "node_names",
}


def _normalize_title(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold().replace("ё", "е")
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"[^a-zа-я0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _fingerprint(record: dict[str, Any]) -> dict[str, Any] | None:
    value = record.get("content_fingerprint")
    return value if has_meaningful_content(value) else None


def _values(record: dict[str, Any], key: str) -> set[str]:
    fingerprint = _fingerprint(record) or {}
    return {str(value) for value in fingerprint.get(key) or [] if str(value)}


def _purpose(record: dict[str, Any]) -> set[str]:
    fingerprint = _fingerprint(record) or {}
    return {
        *(str(value) for value in fingerprint.get("purpose_terms") or [] if str(value)),
        *purpose_terms(str(record.get("title") or "")),
    }


def _compatible_purpose(left: set[str], right: set[str]) -> bool:
    if not left or not right or left & right:
        return True
    compatible = (
        {"single_line", "scheme"}, {"node", "section"},
        {"node", "specification"}, {"section", "specification"},
    )
    return any(bool(left & pair) and bool(right & pair) for pair in compatible)


def _plain_overlap(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / max(1, min(len(left), len(right)))


def _document_frequencies(records: list[dict[str, Any]]) -> dict[str, Counter[str]]:
    output: dict[str, Counter[str]] = {}
    for _component, key, _weight in _FEATURES:
        output[key] = Counter(
            value for record in records for value in _values(record, key)
        )
    for key in ("node_names", "section_names"):
        output[key] = Counter(
            value for record in records for value in _values(record, key)
        )
    return output


def _rarity_weight(
    value: str, left_df: dict[str, Counter[str]], right_df: dict[str, Counter[str]],
    key: str, left_count: int, right_count: int,
) -> float:
    return (
        1.0
        + math.log((left_count + 1) / (left_df[key][value] + 1))
        + math.log((right_count + 1) / (right_df[key][value] + 1))
    )


def _weighted_overlap(
    left: set[str], right: set[str], *, key: str,
    left_df: dict[str, Counter[str]], right_df: dict[str, Counter[str]],
    left_count: int, right_count: int,
) -> float:
    if not left or not right:
        return 0.0
    weights = {
        value: _rarity_weight(value, left_df, right_df, key, left_count, right_count)
        for value in left | right
    }
    overlap = sum(weights[value] for value in left & right)
    # Weighted Dice rewards shared rare anchors while penalising a candidate
    # that merely contains the source as a small generic subset.
    denominator = (
        sum(weights[value] for value in left)
        + sum(weights[value] for value in right)
    ) / 2
    return overlap / denominator if denominator else 0.0


def _unique_anchors(
    left: dict[str, Any], right: dict[str, Any], *,
    left_df: dict[str, Counter[str]], right_df: dict[str, Counter[str]],
) -> tuple[list[str], list[str], dict[str, list[str]]]:
    anchors: list[str] = []
    strong: list[str] = []
    matched: dict[str, list[str]] = {}
    keys = (
        "unique_designations", "system_names", "equipment_codes", "node_names",
        "section_names", "rare_terms", "structural_tokens",
    )
    for key in keys:
        common = sorted(_values(left, key) & _values(right, key))
        if common:
            matched[key] = common[:16]
        for value in common:
            if left_df.get(key, Counter())[value] != 1:
                continue
            if right_df.get(key, Counter())[value] != 1:
                continue
            if value not in anchors:
                anchors.append(value)
            if key in _STRONG_ANCHOR_KEYS and value not in strong:
                strong.append(value)
    return anchors[:16], strong[:16], matched


def _score_pair(
    left: dict[str, Any], right: dict[str, Any], *,
    left_df: dict[str, Counter[str]], right_df: dict[str, Counter[str]],
    left_count: int, right_count: int,
) -> dict[str, Any]:
    left_title = _normalize_title(str(left.get("title") or ""))
    right_title = _normalize_title(str(right.get("title") or ""))
    title_similarity = (
        SequenceMatcher(None, left_title, right_title).ratio()
        if left_title and right_title else 0.0
    )
    left_purpose, right_purpose = _purpose(left), _purpose(right)
    purpose_similarity = _plain_overlap(left_purpose, right_purpose)
    components: dict[str, float] = {
        "title_similarity": title_similarity,
        "purpose_similarity": purpose_similarity,
        "cross_sheet_confirmation": 0.0,
    }
    weighted_score = 0.04 * title_similarity
    available_weight = 0.04 if left_title or right_title else 0.0
    if left_purpose or right_purpose:
        weighted_score += 0.06 * purpose_similarity
        available_weight += 0.06
    independent = 0
    for component, key, weight in _FEATURES:
        left_values, right_values = _values(left, key), _values(right, key)
        value = _weighted_overlap(
            left_values, right_values, key=key, left_df=left_df, right_df=right_df,
            left_count=left_count, right_count=right_count,
        )
        components[component] = value
        if left_values or right_values:
            weighted_score += weight * value
            available_weight += weight
        if value >= MIN_COMPONENT_OVERLAP:
            independent += 1
    score = weighted_score / available_weight if available_weight else 0.0
    anchors, strong_anchors, matched = _unique_anchors(
        left, right, left_df=left_df, right_df=right_df,
    )
    return {
        "score": round(score, 6),
        "score_components": {key: round(value, 6) for key, value in components.items()},
        "unique_anchors": anchors,
        "strong_unique_anchors": strong_anchors,
        "matched_features": matched,
        "independent_content_components": independent,
        "purpose": {
            "left": sorted(left_purpose), "right": sorted(right_purpose),
            "compatible": _compatible_purpose(left_purpose, right_purpose),
        },
    }


def _cross_sheet_confirmation(
    source_group: dict[str, Any] | None, candidate_right: int,
) -> dict[str, Any]:
    evidence_ids: list[str] = []
    for evidence in (source_group or {}).get("atomic_evidence") or []:
        status = str(evidence.get("source_status") or "").upper()
        reason = str(evidence.get("reason") or "").upper()
        if "FOUND_ON_OTHER_SHEET" not in status and "FOUND_ON_OTHER_SHEET" not in reason:
            continue
        if candidate_right not in {int(page) for page in evidence.get("right_pages") or []}:
            continue
        evidence_ids.append(str(evidence.get("evidence_id") or ""))
    return {
        "confirmed": bool(evidence_ids),
        "candidate_right_page": candidate_right,
        "evidence_ids": [value for value in evidence_ids if value],
        "score": round(min(1.0, len(evidence_ids) / 3), 6),
    }


def assess_content_candidates(
    links_payload: dict[str, Any], suggestions: dict[str, Any],
    problem_group_ids: set[str], source_groups: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Rank every sheet on both sides and explain each conservative decision."""
    links = [dict(link) for link in links_payload.get("links") or []]
    left_records = [
        dict(item) for item in suggestions.get("left_sheet_index") or []
        if _fingerprint(item)
    ]
    right_records = [
        dict(item) for item in suggestions.get("right_sheet_index") or []
        if _fingerprint(item)
    ]
    if not left_records or not right_records:
        return []
    left_by_page = {int(item["pdf_page"]): item for item in left_records}
    right_by_page = {int(item["pdf_page"]): item for item in right_records}
    left_df, right_df = _document_frequencies(left_records), _document_frequencies(right_records)
    matrix: dict[tuple[int, int], dict[str, Any]] = {}
    for left_page, left in left_by_page.items():
        for right_page, right in right_by_page.items():
            matrix[(left_page, right_page)] = _score_pair(
                left, right, left_df=left_df, right_df=right_df,
                left_count=len(left_records), right_count=len(right_records),
            )

    left_membership: Counter[int] = Counter()
    right_membership: Counter[int] = Counter()
    for link in links:
        left_membership.update(int(page) for page in link.get("left_pages") or [])
        right_membership.update(int(page) for page in link.get("right_pages") or [])
    groups_by_id = {
        str(group.get("group_id") or ""): group for group in source_groups or []
    }
    assessments: list[dict[str, Any]] = []
    for link in links:
        group_id = str(link.get("id") or "")
        if group_id not in problem_group_ids:
            continue
        left_pages = [int(page) for page in link.get("left_pages") or []]
        right_pages = [int(page) for page in link.get("right_pages") or []]
        if len(left_pages) != 1 or len(right_pages) != 1:
            assessments.append({
                "group_id": group_id, "confidence": "LOW", "auto_repair": False,
                "decision_reasons": ["MANY_TO_MANY"],
            })
            continue
        left_page, current_right = left_pages[0], right_pages[0]
        if (
            left_membership[left_page] != 1 or right_membership[current_right] != 1
            or left_page not in left_by_page
        ):
            assessments.append({
                "group_id": group_id, "left_sheet": left_page,
                "right_sheet_before": current_right, "confidence": "LOW",
                "auto_repair": False, "decision_reasons": ["MANY_TO_MANY"],
            })
            continue
        ranked = sorted(
            (
                (matrix[(left_page, right_page)]["score"], right_page)
                for right_page in right_by_page
            ),
            key=lambda item: (-item[0], item[1]),
        )
        if not ranked:
            continue
        best_base_score, best_right = ranked[0]
        second_score = ranked[1][0] if len(ranked) > 1 else 0.0
        reverse_ranked = sorted(
            (
                (matrix[(candidate_left, best_right)]["score"], candidate_left)
                for candidate_left in left_by_page
            ),
            key=lambda item: (-item[0], item[1]),
        )
        reverse_best_left = reverse_ranked[0][1] if reverse_ranked else None
        reverse_second = reverse_ranked[1][0] if len(reverse_ranked) > 1 else 0.0
        reverse_margin = (
            reverse_ranked[0][0] - reverse_second if reverse_ranked else 0.0
        )
        current = matrix.get((left_page, current_right))
        current_score = float((current or {}).get("score") or 0.0)
        best = dict(matrix[(left_page, best_right)])
        cross = _cross_sheet_confirmation(groups_by_id.get(group_id), best_right)
        best["score_components"] = {
            **best["score_components"],
            "cross_sheet_confirmation": cross["score"],
        }
        # Cross-sheet evidence is deliberately only a small confirmation and
        # never participates in mutual-best selection or anchor gates.
        best_score = min(1.0, best_base_score + 0.03 * cross["score"])
        margin = best_base_score - second_score
        improvement = best_score - current_score
        current_purpose = (current or {}).get("purpose") or {
            "left": sorted(_purpose(left_by_page[left_page])), "right": [],
            "compatible": True,
        }
        candidate_purpose = best["purpose"]
        reasons: list[str] = []
        if best_right == current_right:
            reasons.append("CURRENT_LINK_BEST")
        if best_score < MIN_SCORE:
            reasons.append("LOW_SCORE")
        if margin < MIN_MARGIN or reverse_margin < MIN_REVERSE_MARGIN:
            reasons.append("LOW_MARGIN")
        if reverse_best_left != left_page:
            reasons.append("NON_MUTUAL_BEST")
        if not candidate_purpose["compatible"]:
            reasons.append("PURPOSE_CONFLICT")
        purpose_before_score = float((current or {}).get("score_components", {}).get(
            "purpose_similarity", 0.0
        ))
        purpose_after_score = float(best["score_components"].get("purpose_similarity", 0.0))
        if purpose_after_score + 1e-9 < purpose_before_score:
            reasons.append("PURPOSE_CONSISTENCY_WORSE")
        if improvement < MIN_IMPROVEMENT:
            reasons.append("NOT_MATERIALLY_BETTER")
        anchors = best["unique_anchors"]
        strong_anchors = best["strong_unique_anchors"]
        if (
            best["independent_content_components"] < 2
            or not (strong_anchors or len(anchors) >= 3)
        ):
            reasons.append("INSUFFICIENT_CONTENT_ANCHORS")
        auto_repair = not reasons
        confidence = "HIGH" if auto_repair else (
            "MEDIUM" if best_score >= 0.45 and reverse_best_left == left_page else "LOW"
        )
        rule = (
            CONTENT_UNIQUE_ANCHORS if len(strong_anchors) >= 2
            else CONTENT_MUTUAL_BEST
        )
        right_record = right_by_page[best_right]
        current_record = right_by_page.get(current_right) or {}
        left_record = left_by_page[left_page]
        assessments.append({
            "group_id": group_id,
            "left_sheet": left_page,
            "right_sheet_before": current_right,
            "right_sheet_after": best_right,
            "left_title": str(left_record.get("title") or ""),
            "old_right_title": str(current_record.get("title") or ""),
            "new_right_title": str(right_record.get("title") or ""),
            "rule": rule,
            "confidence": confidence,
            "auto_repair": auto_repair,
            "decision_reasons": reasons or ["HIGH_CONFIDENCE"],
            "content_evidence": best["matched_features"],
            "unique_anchors": anchors,
            "strong_unique_anchors": strong_anchors,
            "score_components": best["score_components"],
            "current_score": round(current_score, 6),
            "best_score": round(best_score, 6),
            "second_score": round(second_score, 6),
            "margin": round(margin, 6),
            "reverse_margin": round(reverse_margin, 6),
            "improvement_over_current": round(improvement, 6),
            "mutual_best": reverse_best_left == left_page,
            "purpose_before": current_purpose,
            "purpose_after": candidate_purpose,
            "cross_sheet_confirmation": cross,
            "source_signatures": {
                "left_fingerprint": (_fingerprint(left_record) or {}).get("source_sha256", ""),
                "old_right_fingerprint": (_fingerprint(current_record) or {}).get("source_sha256", ""),
                "new_right_fingerprint": (_fingerprint(right_record) or {}).get("source_sha256", ""),
            },
        })
    return assessments


def _safe_components(
    assessments: list[dict[str, Any]], links: list[dict[str, Any]],
) -> list[tuple[list[dict[str, Any]], str]]:
    high = {
        int(item["left_sheet"]): item for item in assessments
        if item.get("auto_repair") and item.get("confidence") == "HIGH"
    }
    current_by_left: dict[int, dict[str, Any]] = {}
    current_by_right: dict[int, dict[str, Any]] = {}
    left_membership: Counter[int] = Counter()
    right_membership: Counter[int] = Counter()
    for link in links:
        left_pages = [int(page) for page in link.get("left_pages") or []]
        right_pages = [int(page) for page in link.get("right_pages") or []]
        left_membership.update(left_pages)
        right_membership.update(right_pages)
        if len(left_pages) == len(right_pages) == 1:
            current_by_left[left_pages[0]] = link
            current_by_right[right_pages[0]] = link
    output: list[tuple[list[dict[str, Any]], str]] = []
    consumed: set[int] = set()
    for start in sorted(high):
        if start in consumed or left_membership[start] != 1:
            continue
        chain: list[int] = []
        cursor = start
        while cursor not in chain:
            item = high.get(cursor)
            if item is None:
                chain = []
                break
            chain.append(cursor)
            target_right = int(item["right_sheet_after"])
            if right_membership[target_right] > 1:
                chain = []
                break
            occupied = current_by_right.get(target_right)
            if occupied is None:
                if len(chain) == 1:
                    output.append(([item], item["rule"]))
                    consumed.add(start)
                chain = []
                break
            next_left = int(occupied["left_pages"][0])
            if next_left == start:
                if len(chain) == 2:
                    operation = CONTENT_SWAP
                elif len(chain) == 3:
                    operation = CONTENT_3_CYCLE
                else:
                    operation = ""
                if operation:
                    component = [high[left_page] for left_page in chain]
                    output.append((component, operation))
                    consumed.update(chain)
                chain = []
                break
            cursor = next_left
    return output


def plan_content_repairs(
    links_payload: dict[str, Any], suggestions: dict[str, Any],
    problem_group_ids: set[str], *, source_groups: list[dict[str, Any]] | None = None,
    source_signature: str,
) -> dict[str, Any] | None:
    assessments = assess_content_candidates(
        links_payload, suggestions, problem_group_ids, source_groups,
    )
    links = [dict(link) for link in links_payload.get("links") or []]
    components = _safe_components(assessments, links)
    accepted = [item for component, _operation in components for item in component]
    if not accepted:
        return None
    operation_by_left = {
        int(item["left_sheet"]): operation
        for component, operation in components for item in component
    }
    affected_left = {int(item["left_sheet"]) for item in accepted}
    before_links = [
        dict(link) for link in links
        if any(int(page) in affected_left for page in link.get("left_pages") or [])
    ]
    affected_ids = {str(link.get("id") or "") for link in before_links}
    kept_links = [dict(link) for link in links if str(link.get("id") or "") not in affected_ids]
    after_links: list[dict[str, Any]] = []
    changes: list[dict[str, Any]] = []
    for item in sorted(accepted, key=lambda value: int(value["left_sheet"])):
        left_page = int(item["left_sheet"])
        right_page = int(item["right_sheet_after"])
        operation = operation_by_left[left_page]
        link_reason = [item["rule"], "stage5_purpose_precheck_repair"]
        if operation not in link_reason:
            link_reason.append(operation)
        after_links.append({
            "id": f"repair_{hashlib.sha256(f'{left_page}:{right_page}'.encode()).hexdigest()[:12]}",
            "left_pages": [left_page], "right_pages": [right_page],
            "source": "auto_repair", "confidence": "high", "reason": link_reason,
        })
        changes.append({**item, "operation": operation})
    next_links = kept_links + after_links
    linked_left = {int(page) for link in next_links for page in link.get("left_pages") or []}
    unlinked = {
        int(page) for page in links_payload.get("unlinked_left_pages") or []
    } - linked_left
    after_payload = {
        "version": 1, "pair_id": links_payload.get("pair_id"),
        "links": next_links, "unlinked_left_pages": sorted(unlinked), "updated_at": None,
    }
    operations = {operation for _component, operation in components}
    reason = next(iter(operations)) if len(operations) == 1 else CONTENT_MUTUAL_BEST
    return {
        "source_signature": source_signature,
        "reason": reason,
        "confidence": "high",
        "before_links": before_links,
        "after_links": after_links,
        "changes": changes,
        "content_assessments": assessments,
        "before_snapshot": links_payload,
        "after_snapshot": after_payload,
    }


__all__ = [
    "CONTENT_3_CYCLE", "CONTENT_MUTUAL_BEST", "CONTENT_SWAP",
    "CONTENT_UNIQUE_ANCHORS", "MIN_COMPONENT_OVERLAP", "MIN_IMPROVEMENT",
    "MIN_MARGIN", "MIN_REVERSE_MARGIN", "MIN_SCORE", "assess_content_candidates",
    "plan_content_repairs",
]
