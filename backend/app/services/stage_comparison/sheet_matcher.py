"""Two-pass production Sheet Matcher for literal LEFT -> RIGHT comparison.

Pass 1 bounds the candidate set with inexpensive facts.  Pass 2 evaluates
only those candidates with richer functional, entity, topology and graphic
facts.  Titles and page numbers remain supporting evidence and can never, on
their own, create a high-confidence production relation.
"""
from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Iterable, Mapping

from .production_artifacts import (
    canonical_strings,
    content_signature,
    stable_id,
    utc_now,
)


KIND = "stage_comparison_sheet_relations"
SCHEMA_VERSION = "sheet-relations.v1"
ALGORITHM_VERSION = "production-sheet-matcher.v1"
DIRECTION = "LEFT_TO_RIGHT"
STATUSES = frozenset({"HIGH", "POSSIBLE", "NO_MATCH", "UNKNOWN"})
RELATION_TYPES = frozenset({"MATCHED", "SPLIT", "MERGED", "UNCERTAIN"})
DEFAULT_TOP_K = 5

_TOKEN_RE = re.compile(r"[a-zа-я0-9]+(?:[-./][a-zа-я0-9]+)*", re.I)
_CONTINUATION_RE = re.compile(
    r"\b(?:часть|начало|продолжение|продолж\.|окончание|лист\s+\d+\s+из\s+\d+)\b",
    re.I,
)


def _tokens(value: Any) -> list[str]:
    return canonical_strings(_TOKEN_RE.findall(str(value or "")))


def _facts(record: Mapping[str, Any], *keys: str) -> list[str]:
    output: list[Any] = []
    for key in keys:
        raw = record.get(key)
        if isinstance(raw, str):
            output.append(raw)
        elif isinstance(raw, Iterable) and not isinstance(raw, (bytes, Mapping)):
            output.extend(raw)
    return canonical_strings(output)


def _fingerprint(record: Mapping[str, Any]) -> Mapping[str, Any]:
    value = record.get("content_fingerprint")
    return value if isinstance(value, Mapping) else {}


def normalize_sheet(record: Mapping[str, Any], *, side: str) -> dict[str, Any]:
    """Normalize public sheet metadata and already-extracted compact facts."""
    page = record.get("page", record.get("pdf_page"))
    if not isinstance(page, int) or isinstance(page, bool) or page < 1:
        raise ValueError(f"{side} sheet page must be a positive integer")
    fingerprint = _fingerprint(record)
    title = " ".join(str(record.get("title") or "").split()) or None
    functional = canonical_strings([
        *_facts(record, "functional_content", "functional_roles", "functions"),
        *_facts(fingerprint, "purpose_terms", "system_names", "section_names"),
    ])
    entities = canonical_strings([
        *_facts(record, "main_entities", "text_entities", "entity_refs"),
        *_facts(fingerprint, "equipment_codes", "unique_designations", "node_names"),
    ])
    topology = canonical_strings([
        *_facts(record, "relationships", "topology", "topology_tokens"),
        *_facts(fingerprint, "structural_tokens"),
    ])
    graphic = _facts(record, "graphic_features", "light_graphic_features")
    sheet_types = canonical_strings([
        *_facts(record, "sheet_type", "sheet_types"),
        *_facts(fingerprint, "purpose_terms"),
    ])
    explicit_group = str(
        record.get("comparison_group_ref")
        or record.get("continuation_group_ref")
        or record.get("group_ref")
        or ""
    ).strip() or None
    continuation = bool(title and _CONTINUATION_RE.search(title))
    core_group = None
    if explicit_group:
        core_group = "explicit:" + explicit_group.casefold()
    elif continuation and (functional or entities):
        core_group = "continuation:" + content_signature({
            "functional": functional[:6],
            "entities": entities[:6],
        })[:16]
    return {
        "side": side,
        "page": page,
        "sheet_number": str(record.get("sheet_number") or "").strip() or None,
        "title": title,
        "title_tokens": _tokens(title),
        "functional": functional,
        "entities": entities,
        "topology": topology,
        "sheet_types": sheet_types,
        "graphic": graphic,
        "group_key": core_group,
        "source_ref": record.get("source_ref"),
    }


def _overlap(left: Iterable[str], right: Iterable[str]) -> float | None:
    left_set, right_set = set(left), set(right)
    if not left_set or not right_set:
        return None
    return len(left_set & right_set) / max(1, min(len(left_set), len(right_set)))


def _title_similarity(left: str | None, right: str | None) -> float | None:
    if not left or not right:
        return None
    return SequenceMatcher(None, left.casefold(), right.casefold(), autojunk=False).ratio()


def _weighted_score(values: list[tuple[float | None, float]]) -> float | None:
    available = [(value, weight) for value, weight in values if value is not None]
    if not available:
        return None
    numerator = sum(float(value) * weight for value, weight in available)
    denominator = sum(weight for _value, weight in available)
    return numerator / denominator if denominator else None


def _pass1(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    function = _overlap(left["functional"], right["functional"])
    entity = _overlap(left["entities"], right["entities"])
    sheet_type = _overlap(left["sheet_types"], right["sheet_types"])
    graphic = _overlap(left["graphic"], right["graphic"])
    title = _title_similarity(left["title"], right["title"])
    page_distance = 1.0 / (1.0 + abs(int(left["page"]) - int(right["page"])))
    score = _weighted_score([
        (function, 0.32),
        (entity, 0.30),
        (sheet_type, 0.13),
        (graphic, 0.10),
        (title, 0.12),
        (page_distance, 0.03),
    ])
    return {
        "right_page": right["page"],
        "score": round(score, 6) if score is not None else None,
        "signals": {
            "functional": function,
            "entities": entity,
            "sheet_type": sheet_type,
            "graphic": graphic,
            "title": title,
            "page_proximity": page_distance,
        },
    }


def _deep(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    signals = {
        "functional": _overlap(left["functional"], right["functional"]),
        "entities": _overlap(left["entities"], right["entities"]),
        "topology": _overlap(left["topology"], right["topology"]),
        "graphic": _overlap(left["graphic"], right["graphic"]),
        "sheet_type": _overlap(left["sheet_types"], right["sheet_types"]),
        "title": _title_similarity(left["title"], right["title"]),
        "page_proximity": 1.0 / (
            1.0 + abs(int(left["page"]) - int(right["page"]))
        ),
    }
    score = _weighted_score([
        (signals["functional"], 0.26),
        (signals["entities"], 0.25),
        (signals["topology"], 0.24),
        (signals["graphic"], 0.13),
        (signals["sheet_type"], 0.06),
        (signals["title"], 0.05),
        (signals["page_proximity"], 0.01),
    ])
    strong = sorted(
        key for key in ("functional", "entities", "topology", "graphic", "sheet_type")
        if signals[key] is not None and float(signals[key]) >= 0.6
    )
    if score is None:
        status = "UNKNOWN"
    elif score >= 0.70 and len(strong) >= 2:
        status = "HIGH"
    elif score >= 0.27:
        status = "POSSIBLE"
    else:
        status = "NO_MATCH"
    evidence = [
        {"feature": key, "score": round(float(value), 6)}
        for key, value in signals.items()
        if value is not None and float(value) > 0
    ]
    return {
        "right_page": right["page"],
        "score": round(score, 6) if score is not None else None,
        "status": status,
        "strong_signals": strong,
        "evidence": evidence,
    }


def _relation(
    left_pages: list[int],
    right_pages: list[int],
    candidates: list[Mapping[str, Any]],
    *,
    relation_type: str,
) -> dict[str, Any]:
    ordered_candidates = sorted(
        (dict(candidate) for candidate in candidates),
        key=lambda item: (
            -(item["score"] if item.get("score") is not None else -1),
            item["right_page"],
        ),
    )
    statuses = {str(candidate["status"]) for candidate in ordered_candidates}
    status = (
        "HIGH" if statuses == {"HIGH"}
        else "POSSIBLE" if statuses <= {"HIGH", "POSSIBLE"}
        else "UNKNOWN" if "UNKNOWN" in statuses
        else "NO_MATCH"
    )
    scores = [float(item["score"]) for item in ordered_candidates if item.get("score") is not None]
    confidence = round(min(scores), 6) if scores else None
    identity = {
        "direction": DIRECTION,
        "left_pages": sorted(left_pages),
        "right_pages": sorted(right_pages),
        "relation_type": relation_type,
    }
    return {
        "relation_id": stable_id("srel_", identity),
        "left_pages": sorted(left_pages),
        "right_pages": sorted(right_pages),
        "relation_type": relation_type,
        "status": status,
        "confidence": confidence,
        "evidence": [item for candidate in ordered_candidates for item in candidate["evidence"]],
        "candidate_pages": [item["right_page"] for item in ordered_candidates],
        "provenance": {
            "algorithm": ALGORITHM_VERSION,
            "title_is_primary": False,
            "ai_final_decision": False,
        },
    }


def match_sheets(
    left_sheets: Iterable[Mapping[str, Any]],
    right_sheets: Iterable[Mapping[str, Any]],
    *,
    top_k: int = DEFAULT_TOP_K,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Create deterministic 1:1, 1:N and N:1 production relations."""
    if not isinstance(top_k, int) or isinstance(top_k, bool) or top_k < 1 or top_k > 20:
        raise ValueError("top_k must be between 1 and 20")
    left = sorted(
        (normalize_sheet(item, side="LEFT") for item in left_sheets),
        key=lambda item: item["page"],
    )
    right = sorted(
        (normalize_sheet(item, side="RIGHT") for item in right_sheets),
        key=lambda item: item["page"],
    )
    if len({item["page"] for item in left}) != len(left):
        raise ValueError("duplicate LEFT page")
    if len({item["page"] for item in right}) != len(right):
        raise ValueError("duplicate RIGHT page")

    pass1: dict[int, list[dict[str, Any]]] = {}
    deep_by_pair: dict[tuple[int, int], dict[str, Any]] = {}
    right_by_page = {item["page"]: item for item in right}
    for left_item in left:
        quick = [_pass1(left_item, right_item) for right_item in right]
        quick.sort(key=lambda item: (
            -(item["score"] if item["score"] is not None else -1),
            item["right_page"],
        ))
        pass1[left_item["page"]] = quick[:top_k]
        for candidate in pass1[left_item["page"]]:
            right_item = right_by_page[candidate["right_page"]]
            deep_by_pair[(left_item["page"], right_item["page"])] = _deep(
                left_item, right_item
            )

    left_by_page = {item["page"]: item for item in left}
    relations: list[dict[str, Any]] = []
    consumed_left: set[int] = set()
    consumed_right: set[int] = set()

    # Explicit/continuation groups are the only safe automatic cardinality
    # expansion.  Similar titles alone never manufacture SPLIT or MERGED.
    group_keys = sorted({
        item["group_key"] for item in [*left, *right] if item.get("group_key")
    })
    for group_key in group_keys:
        left_group = [item for item in left if item.get("group_key") == group_key]
        right_group = [item for item in right if item.get("group_key") == group_key]
        if not left_group or not right_group or len(left_group) == len(right_group):
            continue
        candidates = []
        for left_item in left_group:
            ranked = [
                deep_by_pair[(left_item["page"], right_item["page"])]
                for right_item in right_group
                if (left_item["page"], right_item["page"]) in deep_by_pair
            ]
            ranked = [item for item in ranked if item["status"] in {"HIGH", "POSSIBLE"}]
            candidates.extend(ranked)
        if not candidates:
            continue
        left_pages = [item["page"] for item in left_group]
        right_pages = sorted({int(item["right_page"]) for item in candidates})
        if len(left_pages) == 1 and len(right_pages) > 1:
            relation_type = "SPLIT"
        elif len(left_pages) > 1 and len(right_pages) == 1:
            relation_type = "MERGED"
        else:
            relation_type = "UNCERTAIN"
        relations.append(_relation(left_pages, right_pages, candidates, relation_type=relation_type))
        consumed_left.update(left_pages)
        consumed_right.update(right_pages)

    for left_item in left:
        left_page = int(left_item["page"])
        if left_page in consumed_left:
            continue
        ranked = sorted(
            (
                value for (candidate_left, _right), value in deep_by_pair.items()
                if candidate_left == left_page and value["status"] in {"HIGH", "POSSIBLE"}
            ),
            key=lambda item: (
                -(item["score"] if item["score"] is not None else -1),
                item["right_page"],
            ),
        )
        available = [item for item in ranked if int(item["right_page"]) not in consumed_right]
        chosen = available[0] if available else (ranked[0] if ranked else None)
        if chosen is None:
            continue
        relation_type = "MATCHED" if int(chosen["right_page"]) not in consumed_right else "UNCERTAIN"
        relations.append(_relation([left_page], [int(chosen["right_page"])], [chosen], relation_type=relation_type))
        consumed_left.add(left_page)
        if relation_type == "MATCHED":
            consumed_right.add(int(chosen["right_page"]))

    input_signature = content_signature({
        "algorithm": ALGORITHM_VERSION,
        "left": left,
        "right": right,
        "top_k": top_k,
    })
    relations.sort(key=lambda item: (item["left_pages"], item["right_pages"], item["relation_id"]))
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "version": 1,
        "direction": DIRECTION,
        "input_signature": input_signature,
        "generated_at": generated_at or utc_now(),
        "relations": relations,
        "unmatched_left_pages": sorted(set(left_by_page) - consumed_left),
        "unmatched_right_pages": sorted(set(right_by_page) - consumed_right),
        "candidate_search": [
            {
                "left_page": left_page,
                "top_candidates": pass1[left_page],
                "deep_candidates": sorted(
                    [
                        value for (candidate_left, _right), value in deep_by_pair.items()
                        if candidate_left == left_page
                    ],
                    key=lambda item: (
                        -(item["score"] if item["score"] is not None else -1),
                        item["right_page"],
                    ),
                ),
            }
            for left_page in sorted(pass1)
        ],
        "diagnostics": {
            "pass1_pair_count": sum(len(items) for items in pass1.values()),
            "full_cartesian_pair_count": len(left) * len(right),
            "deep_pair_count": len(deep_by_pair),
            "top_k": top_k,
            "uses_model": False,
            "title_is_primary": False,
        },
    }


def page_selection_suggestions(
    selected_left_pages: Iterable[int],
    selected_right_pages: Iterable[int],
    sheet_relations: Mapping[str, Any],
) -> dict[str, Any]:
    """Return advisory matches without changing the user-selected PAGE scope."""
    left = sorted({int(page) for page in selected_left_pages})
    right = sorted({int(page) for page in selected_right_pages})
    if not left or not right or min([*left, *right]) < 1:
        raise ValueError("PAGE selection requires positive LEFT and RIGHT pages")
    suggestions = []
    for relation in sheet_relations.get("relations") or []:
        relation_left = sorted(int(page) for page in relation.get("left_pages") or [])
        relation_right = sorted(int(page) for page in relation.get("right_pages") or [])
        if not set(relation_left) & set(left):
            continue
        if relation_right == right and relation_left == left:
            continue
        suggestions.append({
            "suggestion_id": stable_id(
                "ssug_", left, right, relation.get("relation_id"),
            ),
            "selected_left_pages": left,
            "selected_right_pages": right,
            "suggested_left_pages": relation_left,
            "suggested_right_pages": relation_right,
            "relation_id": relation.get("relation_id"),
            "relation_type": relation.get("relation_type"),
            "status": relation.get("status"),
            "actions": ["COMPARE_ADDITIONALLY", "REPLACE", "ADD_TO_GROUP", "IGNORE"],
            "applied": False,
        })
    return {
        "kind": "stage_comparison_sheet_suggestions",
        "schema_version": "sheet-suggestions.v1",
        "direction": DIRECTION,
        "selected_scope": {"left_pages": left, "right_pages": right},
        "selection_preserved": True,
        "sheet_matcher_is_gate": False,
        "input_signature": content_signature({
            "selection": [left, right],
            "relations": sheet_relations.get("input_signature"),
        }),
        "suggestions": suggestions,
    }


__all__ = [
    "ALGORITHM_VERSION",
    "DIRECTION",
    "KIND",
    "RELATION_TYPES",
    "SCHEMA_VERSION",
    "STATUSES",
    "match_sheets",
    "normalize_sheet",
    "page_selection_suggestions",
]
