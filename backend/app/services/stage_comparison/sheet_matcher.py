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
ALGORITHM_VERSION = "production-sheet-matcher.v2"
DIRECTION = "LEFT_TO_RIGHT"
STATUSES = frozenset({"HIGH", "POSSIBLE", "NO_MATCH", "UNKNOWN"})
RELATION_TYPES = frozenset({"MATCHED", "SPLIT", "MERGED", "UNCERTAIN"})
DEFAULT_TOP_K = 5

_SUBSTANTIVE_FEATURES = (
    "functional",
    "entities",
    "topology",
    "graphic",
    "sheet_type",
)
_PRIMARY_FEATURES = frozenset({"functional", "entities", "topology"})

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
    explicit_group_key = None
    if explicit_group:
        explicit_group_key = "explicit:" + explicit_group.casefold()
        core_group = explicit_group_key
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
        "explicit_group_key": explicit_group_key,
        "continuation_hint": continuation,
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
        key for key in _SUBSTANTIVE_FEATURES
        if signals[key] is not None and float(signals[key]) >= 0.6
    )
    observed = sorted(key for key in _SUBSTANTIVE_FEATURES if signals[key] is not None)
    positive = sorted(
        key
        for key in _SUBSTANTIVE_FEATURES
        if signals[key] is not None and float(signals[key]) > 0
    )
    # Page proximity and title similarity are useful for candidate retrieval,
    # but they do not say whether two sheets have comparable content.  With no
    # substantive observations the honest result is UNKNOWN, even for adjacent
    # pages with identical titles.
    if not observed:
        status = "UNKNOWN"
        score = None
    elif not positive:
        status = "NO_MATCH"
        score = 0.0
    elif score >= 0.70 and len(strong) >= 2:
        status = "HIGH"
    elif score >= 0.27:
        status = "POSSIBLE"
    else:
        status = "NO_MATCH"
    evidence = [
        {"feature": key, "score": round(float(value), 6)}
        for key, value in signals.items()
        if value is not None
        and (key in _SUBSTANTIVE_FEATURES or float(value) > 0)
    ]
    return {
        "left_page": left["page"],
        "right_page": right["page"],
        "score": round(score, 6) if score is not None else None,
        "status": status,
        "strong_signals": strong,
        "substantive_observations": observed,
        "substantive_signals": positive,
        "cardinality_edge_supported": (
            len(strong) >= 2
            and bool(_PRIMARY_FEATURES & set(strong))
            and status in {"HIGH", "POSSIBLE"}
        ),
        "signals": signals,
        "evidence": evidence,
    }


def _sheet_fact_set(sheet: Mapping[str, Any]) -> set[tuple[str, str]]:
    """Return dimension-qualified facts used for aggregate cardinality proof."""
    facts: set[tuple[str, str]] = set()
    for feature, field in (
        ("functional", "functional"),
        ("entities", "entities"),
        ("topology", "topology"),
        ("graphic", "graphic"),
        ("sheet_type", "sheet_types"),
    ):
        facts.update((feature, str(value)) for value in sheet.get(field) or [])
    return facts


def _aggregate_cardinality_evidence(
    anchor: Mapping[str, Any],
    members: list[Mapping[str, Any]],
    candidates: list[Mapping[str, Any]],
    *,
    relation_type: str,
) -> dict[str, Any] | None:
    """Prove that several supported edges are complementary, not alternatives.

    A high score to two pages is insufficient to infer a split/merge: the two
    pages can simply be duplicate alternatives.  Every member must contribute
    at least one distinct fact also present on the aggregate sheet, and the
    union must cover most of the aggregate sheet facts.  Titles and page
    numbers deliberately do not participate in this proof.
    """
    if len(members) < 2 or len(candidates) != len(members):
        return None
    if not all(candidate.get("cardinality_edge_supported") for candidate in candidates):
        return None

    anchor_facts = _sheet_fact_set(anchor)
    if not anchor_facts:
        return None
    matched_by_member = [anchor_facts & _sheet_fact_set(member) for member in members]
    if any(not matched for matched in matched_by_member):
        return None

    distinct_by_member: list[set[tuple[str, str]]] = []
    for index, matched in enumerate(matched_by_member):
        other_facts = set().union(*(
            value for other_index, value in enumerate(matched_by_member)
            if other_index != index
        ))
        distinct_by_member.append(matched - other_facts)
    if any(not distinct for distinct in distinct_by_member):
        return None

    covered = set().union(*matched_by_member)
    coverage = len(covered) / len(anchor_facts)
    if coverage < 0.6:
        return None
    return {
        "kind": "AGGREGATE_CONTENT",
        "relation_type": relation_type,
        "coverage": round(coverage, 6),
        "covered_fact_count": len(covered),
        "anchor_fact_count": len(anchor_facts),
        "distinct_contributions": [
            {
                "page": int(member["page"]),
                "facts": [
                    {"feature": feature, "value": value}
                    for feature, value in sorted(distinct)
                ],
            }
            for member, distinct in zip(members, distinct_by_member)
        ],
        "title_used": False,
        "page_proximity_used": False,
    }


def _candidate_sort_key(candidate: Mapping[str, Any]) -> tuple[float, int, int]:
    score = candidate.get("score")
    return (
        -(float(score) if score is not None else -1.0),
        int(candidate.get("left_page") or 0),
        int(candidate.get("right_page") or 0),
    )


def _missing_candidate(*, left_page: int | None, right_page: int | None) -> dict[str, Any]:
    return {
        "left_page": left_page,
        "right_page": right_page,
        "score": None,
        "status": "UNKNOWN",
        "strong_signals": [],
        "substantive_observations": [],
        "substantive_signals": [],
        "cardinality_edge_supported": False,
        "signals": {},
        "evidence": [{"feature": "candidate_availability", "state": "ABSENT"}],
    }


def _relation(
    left_pages: list[int],
    right_pages: list[int],
    candidates: list[Mapping[str, Any]],
    *,
    relation_type: str,
    aggregate_evidence: Mapping[str, Any] | None = None,
    automatic_scope: bool = True,
) -> dict[str, Any]:
    ordered_candidates = sorted(
        (dict(candidate) for candidate in candidates),
        key=_candidate_sort_key,
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
    evidence = []
    candidate_edges = []
    supported_edges = []
    for candidate in ordered_candidates:
        left_page = candidate.get("left_page")
        right_page = candidate.get("right_page")
        for item in candidate.get("evidence") or []:
            evidence.append({
                **dict(item),
                "left_page": left_page,
                "right_page": right_page,
            })
        if left_page is not None and right_page is not None:
            edge = {
                "left_page": int(left_page),
                "right_page": int(right_page),
                "status": candidate.get("status"),
                "score": candidate.get("score"),
                "substantive_signals": list(candidate.get("substantive_signals") or []),
                "cardinality_edge_supported": bool(
                    candidate.get("cardinality_edge_supported")
                ),
            }
            candidate_edges.append(edge)
            if edge["cardinality_edge_supported"]:
                supported_edges.append(edge)
    if aggregate_evidence is not None:
        evidence.append(dict(aggregate_evidence))
    return {
        "relation_id": stable_id("srel_", identity),
        "left_pages": sorted(left_pages),
        "right_pages": sorted(right_pages),
        "relation_type": relation_type,
        "status": status,
        "confidence": confidence,
        "automatic_scope": automatic_scope,
        "evidence": evidence,
        "candidate_pages": sorted({
            int(item["right_page"])
            for item in ordered_candidates
            if item.get("right_page") is not None
        }),
        "candidate_edges": candidate_edges,
        "supported_edges": supported_edges,
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

    supported_by_left: dict[int, dict[int, dict[str, Any]]] = {}
    supported_by_right: dict[int, dict[int, dict[str, Any]]] = {}
    for (left_page, right_page), candidate in deep_by_pair.items():
        if not candidate.get("cardinality_edge_supported"):
            continue
        supported_by_left.setdefault(left_page, {})[right_page] = candidate
        supported_by_right.setdefault(right_page, {})[left_page] = candidate

    # An explicit group is a useful grouping assertion, but never evidence that
    # every page in the group belongs in an automatic relation.  Retain only
    # pages incident to an independently supported deep edge.
    group_keys = sorted({
        item["explicit_group_key"]
        for item in [*left, *right]
        if item.get("explicit_group_key")
    })
    for group_key in group_keys:
        left_group = [
            item for item in left if item.get("explicit_group_key") == group_key
        ]
        right_group = [
            item for item in right if item.get("explicit_group_key") == group_key
        ]
        if not left_group or not right_group or len(left_group) == len(right_group):
            continue
        group_left_pages = {int(item["page"]) for item in left_group}
        group_right_pages = {int(item["page"]) for item in right_group}
        edge_items = [
            candidate
            for (left_page, right_page), candidate in deep_by_pair.items()
            if left_page in group_left_pages
            and right_page in group_right_pages
            and candidate.get("cardinality_edge_supported")
        ]
        active_left_pages = sorted({int(item["left_page"]) for item in edge_items})
        active_right_pages = sorted({int(item["right_page"]) for item in edge_items})
        if not active_left_pages or not active_right_pages:
            continue
        if len(active_left_pages) == 1 and len(active_right_pages) > 1:
            relation_type = "SPLIT"
        elif len(active_left_pages) > 1 and len(active_right_pages) == 1:
            relation_type = "MERGED"
        else:
            # Equal-cardinality and many-to-many groups remain independent
            # candidate relations; grouping them would invent topology.
            continue
        relations.append(_relation(
            active_left_pages,
            active_right_pages,
            edge_items,
            relation_type=relation_type,
            aggregate_evidence={
                "kind": "EXPLICIT_GROUP_WITH_SUPPORTED_EDGES",
                "group_key": group_key,
                "included_edge_count": len(edge_items),
                "all_included_pages_have_supported_edge": True,
                "title_used": False,
                "page_proximity_used": False,
            },
        ))
        consumed_left.update(active_left_pages)
        consumed_right.update(active_right_pages)

    # Deep candidate graph can establish cardinality without an upstream group
    # reference.  The leaf constraint rejects ambiguous many-to-many graphs;
    # aggregate evidence then proves that every included page contributes a
    # distinct substantive fact instead of merely being an alternative match.
    proposals: list[dict[str, Any]] = []
    for left_item in left:
        left_page = int(left_item["page"])
        if left_page in consumed_left:
            continue
        edges = [
            candidate
            for right_page, candidate in supported_by_left.get(left_page, {}).items()
            if right_page not in consumed_right
            and {
                other_left
                for other_left in supported_by_right.get(right_page, {})
                if other_left not in consumed_left
            } == {left_page}
        ]
        edges.sort(key=_candidate_sort_key)
        members = [right_by_page[int(item["right_page"])] for item in edges]
        aggregate = _aggregate_cardinality_evidence(
            left_item, members, edges, relation_type="SPLIT"
        )
        if aggregate is not None:
            proposals.append({
                "relation_type": "SPLIT",
                "left_pages": [left_page],
                "right_pages": sorted(int(item["page"]) for item in members),
                "candidates": edges,
                "aggregate": aggregate,
            })

    for right_item in right:
        right_page = int(right_item["page"])
        if right_page in consumed_right:
            continue
        edges = [
            candidate
            for left_page, candidate in supported_by_right.get(right_page, {}).items()
            if left_page not in consumed_left
            and {
                other_right
                for other_right in supported_by_left.get(left_page, {})
                if other_right not in consumed_right
            } == {right_page}
        ]
        edges.sort(key=lambda item: int(item["left_page"]))
        members = [left_by_page[int(item["left_page"])] for item in edges]
        aggregate = _aggregate_cardinality_evidence(
            right_item, members, edges, relation_type="MERGED"
        )
        if aggregate is not None:
            proposals.append({
                "relation_type": "MERGED",
                "left_pages": sorted(int(item["page"]) for item in members),
                "right_pages": [right_page],
                "candidates": edges,
                "aggregate": aggregate,
            })

    proposals.sort(key=lambda item: (
        -float(item["aggregate"]["coverage"]),
        -min(float(candidate.get("score") or 0) for candidate in item["candidates"]),
        item["relation_type"],
        item["left_pages"],
        item["right_pages"],
    ))
    for proposal in proposals:
        if set(proposal["left_pages"]) & consumed_left:
            continue
        if set(proposal["right_pages"]) & consumed_right:
            continue
        relations.append(_relation(
            proposal["left_pages"],
            proposal["right_pages"],
            proposal["candidates"],
            relation_type=proposal["relation_type"],
            aggregate_evidence=proposal["aggregate"],
        ))
        consumed_left.update(proposal["left_pages"])
        consumed_right.update(proposal["right_pages"])

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
        chosen = available[0] if available else None
        if chosen is None:
            continue
        relations.append(_relation(
            [left_page],
            [int(chosen["right_page"])],
            [chosen],
            relation_type="MATCHED",
        ))
        consumed_left.add(left_page)
        consumed_right.add(int(chosen["right_page"]))

    # NO_MATCH and UNKNOWN are persisted as explicit reviewable relations, not
    # merely inferred from summary arrays.  One side is deliberately left out
    # of the automatic scope so downstream DOCUMENT orchestration cannot treat
    # an unresolved candidate as an approved page comparison.
    for left_item in left:
        left_page = int(left_item["page"])
        if left_page in consumed_left:
            continue
        ranked = sorted(
            (
                value for (candidate_left, _right), value in deep_by_pair.items()
                if candidate_left == left_page
            ),
            key=_candidate_sort_key,
        )
        chosen = ranked[0] if ranked else _missing_candidate(
            left_page=left_page, right_page=None
        )
        relations.append(_relation(
            [left_page],
            [],
            [chosen],
            relation_type="UNCERTAIN",
            automatic_scope=False,
        ))

    for right_item in right:
        right_page = int(right_item["page"])
        if right_page in consumed_right:
            continue
        ranked = sorted(
            (
                value for (_left, candidate_right), value in deep_by_pair.items()
                if candidate_right == right_page
            ),
            key=_candidate_sort_key,
        )
        chosen = ranked[0] if ranked else _missing_candidate(
            left_page=None, right_page=right_page
        )
        relations.append(_relation(
            [],
            [right_page],
            [chosen],
            relation_type="UNCERTAIN",
            automatic_scope=False,
        ))

    input_signature = content_signature({
        "algorithm": ALGORITHM_VERSION,
        "left": left,
        "right": right,
        "top_k": top_k,
    })
    relations.sort(key=lambda item: (
        0 if item["left_pages"] else 1,
        item["left_pages"],
        item["right_pages"],
        item["relation_id"],
    ))
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
