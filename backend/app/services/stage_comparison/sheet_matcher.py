"""Production Sheet Matcher for literal LEFT -> RIGHT comparison.

Identity comes first.  A sheet says which sheet it is in its stamp — «Корпуса
1, 2. План 3 этажа» — and two sheets carrying the same stamp key are the same
sheet, whatever their PDF page numbers are.  That is the primary evidence, and
a proven stamp key that *differs* is equally decisive in the other direction:
«План 3 этажа» is not «План 4 этажа», however similar their contents look.

Where no stamp parses, the two content passes still run: pass 1 bounds the
candidate set with inexpensive facts, pass 2 evaluates those candidates with
richer functional, entity, topology and graphic facts.  Titles and page
numbers remain supporting evidence and can never, on their own, create a
high-confidence production relation.

The final 1:1 step is a global maximum-weight assignment, not a greedy walk
down the LEFT page numbers.  Greedy let an early weak pair consume a RIGHT
page that a later, much stronger pair needed, and the strong pair then
vanished without a trace.  Nothing strong is allowed to vanish here: a
displaced high-confidence candidate becomes an explicit conflict and a
question, never silence.
"""
from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Iterable, Mapping

from .document_matching import maximum_weight_assignment
from .production_artifacts import (
    canonical_strings,
    content_signature,
    stable_id,
    utc_now,
)
from .sheet_identity import SheetIdentity, covers_floors, identity_from_dict


KIND = "stage_comparison_sheet_relations"
SCHEMA_VERSION = "sheet-relations.v1"
ALGORITHM_VERSION = "production-sheet-matcher.v3"
DIRECTION = "LEFT_TO_RIGHT"
STATUSES = frozenset({"HIGH", "POSSIBLE", "NO_MATCH", "UNKNOWN"})
RELATION_TYPES = frozenset({"MATCHED", "SPLIT", "MERGED", "UNCERTAIN"})
#: Which evidence created a relation.  Diagnostics only — never user-facing text.
PRIMARY_SOURCES = frozenset({
    "STAMP_EXACT",
    "STAMP_GROUP",
    "CONTENT",
    "USER_SELECTED",
})
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
    identity = identity_from_dict(record.get("sheet_identity"))
    if identity is not None and identity.page != page:
        raise ValueError(f"{side} sheet identity page mismatch")
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
        "identity": identity,
        "stamp_key": identity.stamp_key if identity else None,
    }


def sheet_label(sheet: Mapping[str, Any]) -> str:
    """What to call this page on a question card, in the project's own words."""
    identity: SheetIdentity | None = sheet.get("identity")
    if identity is not None and identity.raw_stamp_text:
        return identity.raw_stamp_text
    number = sheet.get("sheet_number")
    title = sheet.get("title")
    if number and title:
        return f"Лист {number} — {title}"
    if number:
        return f"Лист {number}"
    if title:
        return str(title)
    return f"Стр. {int(sheet['page'])}"


def _signature_view(sheet: Mapping[str, Any]) -> dict[str, Any]:
    """JSON-safe projection of a normalized sheet for the input signature."""
    identity: SheetIdentity | None = sheet.get("identity")
    return {
        **{key: value for key, value in sheet.items() if key != "identity"},
        "sheet_identity": identity.to_dict() if identity else None,
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


def _stamp_relation(
    left: Mapping[str, Any], right: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Classify two sheets by their stamps: SAME, COVERS, CONFLICT or UNKNOWN.

    A stamp is only decisive when both sides actually have one.  ``COVERS`` is
    the «План 3-15 этажей» over «План 7 этажа» case: related sheets, but not
    the same sheet, so it feeds the 1->N / N->1 machinery instead of a pair.
    """
    left_identity: SheetIdentity | None = left.get("identity")
    right_identity: SheetIdentity | None = right.get("identity")
    if left_identity is None or right_identity is None:
        return "UNKNOWN", {
            "left_stamp_key": left_identity.stamp_key if left_identity else None,
            "right_stamp_key": right_identity.stamp_key if right_identity else None,
        }
    evidence = {
        "left_stamp_key": left_identity.stamp_key,
        "right_stamp_key": right_identity.stamp_key,
        "left_stamp_text": left_identity.raw_stamp_text,
        "right_stamp_text": right_identity.raw_stamp_text,
    }
    if left_identity.matches(right_identity):
        return "SAME", evidence
    if covers_floors(right_identity, left_identity):
        return "COVERS", {**evidence, "container_side": "RIGHT"}
    if covers_floors(left_identity, right_identity):
        return "COVERS", {**evidence, "container_side": "LEFT"}
    return "CONFLICT", evidence


def _deep(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    stamp_relation, stamp_evidence = _stamp_relation(left, right)
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
    reason_codes: list[str] = []
    if stamp_relation == "CONFLICT":
        # Both sheets state which sheet they are, and they say different
        # things.  No amount of shared vocabulary makes «План 3 этажа» into
        # «План 4 этажа», so this pair is refused outright rather than left
        # to win on room names the two floors happen to share.
        status = "NO_MATCH"
        score = 0.0
        reason_codes.append("stamp_key_conflict")
    elif stamp_relation == "COVERS":
        reason_codes.append("stamp_floor_range_covers")
    elif stamp_relation == "SAME":
        reason_codes.append("stamp_key_exact")
    evidence = [
        {"feature": key, "score": round(float(value), 6)}
        for key, value in signals.items()
        if value is not None
        and (key in _SUBSTANTIVE_FEATURES or float(value) > 0)
    ]
    if stamp_relation != "UNKNOWN":
        evidence.append({"feature": "stamp_identity", "state": stamp_relation, **stamp_evidence})
    return {
        "left_page": left["page"],
        "right_page": right["page"],
        "score": round(score, 6) if score is not None else None,
        "status": status,
        "stamp_relation": stamp_relation,
        "stamp_evidence": stamp_evidence,
        "reason_codes": reason_codes,
        "strong_signals": strong,
        "substantive_observations": observed,
        "substantive_signals": positive,
        "cardinality_edge_supported": (
            len(strong) >= 2
            and bool(_PRIMARY_FEATURES & set(strong))
            and status in {"HIGH", "POSSIBLE"}
        ) or stamp_relation == "COVERS",
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
        "stamp_relation": "UNKNOWN",
        "stamp_evidence": {},
        "reason_codes": [],
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
    primary_source: str = "CONTENT",
    status_override: str | None = None,
    confidence_override: float | None = None,
    reason_codes: Iterable[str] = (),
    conflicting_evidence: Iterable[Mapping[str, Any]] = (),
    stamp_identity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if primary_source not in PRIMARY_SOURCES:
        raise ValueError("unsupported sheet relation primary source")
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
    if status_override is not None:
        status = status_override
    scores = [float(item["score"]) for item in ordered_candidates if item.get("score") is not None]
    confidence = round(min(scores), 6) if scores else None
    if confidence_override is not None:
        confidence = round(float(confidence_override), 6)
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
    if stamp_identity is not None:
        evidence.append({"kind": "STAMP_IDENTITY", **dict(stamp_identity)})
    all_reason_codes = sorted({
        *(str(code) for code in reason_codes),
        *(
            str(code)
            for candidate in ordered_candidates
            for code in candidate.get("reason_codes") or []
        ),
    })
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
        "primary_source": primary_source,
        "reason_codes": all_reason_codes,
        "conflicting_evidence": [dict(item) for item in conflicting_evidence],
        "provenance": {
            "algorithm": ALGORITHM_VERSION,
            "title_is_primary": False,
            "page_number_is_primary": False,
            "primary_source": primary_source,
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
        deep_pages = {int(candidate["right_page"]) for candidate in pass1[left_item["page"]]}
        # Pass 1 ranks by shared vocabulary, which is exactly what fails on
        # architectural sheets.  A stamp-related page must be evaluated even
        # when it did not make the cheap top-K cut.
        if left_item.get("stamp_key"):
            deep_pages.update(
                int(right_item["page"])
                for right_item in right
                if _stamp_relation(left_item, right_item)[0] in {"SAME", "COVERS"}
            )
        for right_page in sorted(deep_pages):
            deep_by_pair[(left_item["page"], right_page)] = _deep(
                left_item, right_by_page[right_page]
            )

    left_by_page = {item["page"]: item for item in left}
    relations: list[dict[str, Any]] = []
    consumed_left: set[int] = set()
    consumed_right: set[int] = set()

    def _edge(left_page: int, right_page: int) -> dict[str, Any]:
        return deep_by_pair.get((left_page, right_page)) or _missing_candidate(
            left_page=left_page, right_page=right_page
        )

    # ---- Stamp identity, before any content pass -------------------------
    # A page number is not a sheet identity; the stamp is.  Exactly one sheet
    # per side carrying one key is a proven pair.  Anything else stays a
    # question: picking the first candidate is what produced pairs like
    # «План 3 этажа» opposite «План 4 этажа».
    left_by_key: dict[str, list[Mapping[str, Any]]] = {}
    right_by_key: dict[str, list[Mapping[str, Any]]] = {}
    for item in left:
        if item.get("stamp_key"):
            left_by_key.setdefault(str(item["stamp_key"]), []).append(item)
    for item in right:
        if item.get("stamp_key"):
            right_by_key.setdefault(str(item["stamp_key"]), []).append(item)
    stamp_ambiguous_keys: list[dict[str, Any]] = []
    for stamp_key in sorted(set(left_by_key) & set(right_by_key)):
        left_group = left_by_key[stamp_key]
        right_group = right_by_key[stamp_key]
        left_pages = sorted(int(item["page"]) for item in left_group)
        right_pages = sorted(int(item["page"]) for item in right_group)
        identity_evidence = {
            "stamp_key": stamp_key,
            "left_stamp_text": left_group[0]["identity"].raw_stamp_text,
            "right_stamp_text": right_group[0]["identity"].raw_stamp_text,
            "sheet_kind": left_group[0]["identity"].sheet_kind,
            "buildings": list(left_group[0]["identity"].buildings),
            "floors": list(left_group[0]["identity"].floors),
            "title_used": False,
            "page_proximity_used": False,
        }
        if len(left_group) == 1 and len(right_group) == 1:
            relations.append(_relation(
                left_pages,
                right_pages,
                [_edge(left_pages[0], right_pages[0])],
                relation_type="MATCHED",
                primary_source="STAMP_EXACT",
                status_override="HIGH",
                confidence_override=1.0,
                reason_codes=["stamp_key_exact"],
                stamp_identity=identity_evidence,
            ))
        else:
            # Same key on several pages of one side is real ambiguity, and the
            # cheapest wrong answer is to hand the first candidate the page.
            stamp_ambiguous_keys.append({"stamp_key": stamp_key, "left_pages": left_pages, "right_pages": right_pages})
            relations.append(_relation(
                left_pages,
                right_pages,
                [
                    _edge(left_page, right_page)
                    for left_page in left_pages
                    for right_page in right_pages
                ],
                relation_type="UNCERTAIN",
                primary_source="STAMP_EXACT",
                status_override="UNKNOWN",
                automatic_scope=False,
                reason_codes=["stamp_key_ambiguous"],
                stamp_identity=identity_evidence,
            ))
        consumed_left.update(left_pages)
        consumed_right.update(right_pages)

    # «План 3-15 этажей» genuinely covers «План 7 этажа», but the two are not
    # the same sheet.  That is a cardinality candidate for the existing
    # 1->N / N->1 contract, offered at POSSIBLE so a human confirms the scope.
    stamp_group_relations = 0
    for container_side in ("RIGHT", "LEFT"):
        containers = right if container_side == "RIGHT" else left
        members = left if container_side == "RIGHT" else right
        consumed_container = consumed_right if container_side == "RIGHT" else consumed_left
        consumed_member = consumed_left if container_side == "RIGHT" else consumed_right
        for container in containers:
            container_identity: SheetIdentity | None = container.get("identity")
            if container_identity is None or int(container["page"]) in consumed_container:
                continue
            covered = [
                item for item in members
                if int(item["page"]) not in consumed_member
                and item.get("identity") is not None
                and covers_floors(container_identity, item["identity"])
            ]
            if len(covered) < 2:
                continue
            # Every member must be a distinct floor, and no other container may
            # claim them, or this is a guess about scope rather than a reading.
            floor_sets = [item["identity"].floor_set for item in covered]
            if len({frozenset(value) for value in floor_sets}) != len(floor_sets):
                continue
            rival_containers = [
                other for other in containers
                if other is not container
                and int(other["page"]) not in consumed_container
                and other.get("identity") is not None
                and any(covers_floors(other["identity"], item["identity"]) for item in covered)
            ]
            if rival_containers:
                continue
            member_pages = sorted(int(item["page"]) for item in covered)
            container_page = int(container["page"])
            if container_side == "RIGHT":
                relation_left, relation_right = member_pages, [container_page]
                relation_type = "MERGED"
                edges = [_edge(page, container_page) for page in member_pages]
            else:
                relation_left, relation_right = [container_page], member_pages
                relation_type = "SPLIT"
                edges = [_edge(container_page, page) for page in member_pages]
            relations.append(_relation(
                relation_left,
                relation_right,
                edges,
                relation_type=relation_type,
                primary_source="STAMP_GROUP",
                status_override="POSSIBLE",
                reason_codes=["stamp_floor_range_covers"],
                stamp_identity={
                    "stamp_key": container_identity.stamp_key,
                    "container_side": container_side,
                    "container_stamp_text": container_identity.raw_stamp_text,
                    "member_stamp_texts": [
                        item["identity"].raw_stamp_text for item in covered
                    ],
                    "title_used": False,
                    "page_proximity_used": False,
                },
            ))
            stamp_group_relations += 1
            consumed_container.add(container_page)
            consumed_member.update(member_pages)

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

    # ---- Content pass: one global assignment, never a greedy walk ---------
    # Walking LEFT pages in order and taking each page's best free candidate
    # let LEFT 34 -> RIGHT 3 (0.53) consume the page that LEFT 41 -> RIGHT 3
    # (0.71) needed, and the strongest relation of the whole run disappeared.
    open_left = [item for item in left if int(item["page"]) not in consumed_left]
    open_right = [item for item in right if int(item["page"]) not in consumed_right]
    assignable: dict[tuple[int, int], Mapping[str, Any]] = {
        (left_page, right_page): candidate
        for (left_page, right_page), candidate in deep_by_pair.items()
        if left_page not in consumed_left
        and right_page not in consumed_right
        and candidate["status"] in {"HIGH", "POSSIBLE"}
    }
    assigned_pairs: list[tuple[int, int]] = []
    if open_left and open_right and assignable:
        weights = [
            [
                float(assignable[(int(left_item["page"]), int(right_item["page"]))]["score"] or 0.0)
                if (int(left_item["page"]), int(right_item["page"])) in assignable
                else 0.0
                for right_item in open_right
            ]
            for left_item in open_left
        ]
        for left_index, right_index in maximum_weight_assignment(weights):
            if weights[left_index][right_index] <= 0.0:
                continue
            assigned_pairs.append((
                int(open_left[left_index]["page"]),
                int(open_right[right_index]["page"]),
            ))
    assigned_pairs.sort()
    assigned_by_left = {left_page: right_page for left_page, right_page in assigned_pairs}
    assigned_by_right = {right_page: left_page for left_page, right_page in assigned_pairs}

    # A strong candidate that lost its page to somebody else is not allowed to
    # disappear: it is attached to the winning relation as conflicting evidence
    # and, if it lost outright, it becomes its own reviewable relation below.
    displaced_high: list[dict[str, Any]] = []
    for (left_page, right_page), candidate in sorted(deep_by_pair.items()):
        if candidate["status"] != "HIGH":
            continue
        if assigned_by_left.get(left_page) == right_page:
            continue
        if left_page in consumed_left and right_page in consumed_right:
            continue
        displaced_high.append({
            "kind": "DISPLACED_HIGH_CANDIDATE",
            "left_page": left_page,
            "right_page": right_page,
            "score": candidate.get("score"),
            "reason_code": "high_candidate_displaced",
            "left_page_taken_by": assigned_by_left.get(left_page),
            "right_page_taken_by": assigned_by_right.get(right_page),
        })
    displaced_by_left: dict[int, list[dict[str, Any]]] = {}
    displaced_by_right: dict[int, list[dict[str, Any]]] = {}
    for item in displaced_high:
        displaced_by_left.setdefault(int(item["left_page"]), []).append(item)
        displaced_by_right.setdefault(int(item["right_page"]), []).append(item)

    for left_page, right_page in assigned_pairs:
        conflicts = [
            item for item in displaced_high
            if item["left_page"] == left_page or item["right_page"] == right_page
        ]
        relations.append(_relation(
            [left_page],
            [right_page],
            [deep_by_pair[(left_page, right_page)]],
            relation_type="MATCHED",
            primary_source="CONTENT",
            reason_codes=(
                ["displaced_high_candidate_present"] if conflicts else []
            ),
            conflicting_evidence=conflicts,
        ))
        consumed_left.add(left_page)
        consumed_right.add(right_page)

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
        conflicts = displaced_by_left.get(left_page, [])
        relations.append(_relation(
            [left_page],
            [],
            [chosen],
            relation_type="UNCERTAIN",
            automatic_scope=False,
            reason_codes=(
                ["high_candidate_displaced"] if conflicts else []
            ),
            conflicting_evidence=conflicts,
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
        conflicts = displaced_by_right.get(right_page, [])
        relations.append(_relation(
            [],
            [right_page],
            [chosen],
            relation_type="UNCERTAIN",
            automatic_scope=False,
            reason_codes=(
                ["high_candidate_displaced"] if conflicts else []
            ),
            conflicting_evidence=conflicts,
        ))

    input_signature = content_signature({
        "algorithm": ALGORITHM_VERSION,
        "left": [_signature_view(item) for item in left],
        "right": [_signature_view(item) for item in right],
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
        # Human-readable page labels travel with the relations so a question
        # card can say «Корпуса 1, 2. План 3 этажа» instead of «LEFT 29».
        "sheet_labels": {
            "LEFT": {str(item["page"]): sheet_label(item) for item in left},
            "RIGHT": {str(item["page"]): sheet_label(item) for item in right},
        },
        # Номер листа из штампа и страница PDF — разные числа: лист 7 может
        # лежать на 29-й странице файла. Номер публикуется отдельно, чтобы
        # интерфейсу не приходилось выковыривать его из названия.
        "sheet_numbers": {
            "LEFT": {
                str(item["page"]): item["sheet_number"]
                for item in left if item.get("sheet_number")
            },
            "RIGHT": {
                str(item["page"]): item["sheet_number"]
                for item in right if item.get("sheet_number")
            },
        },
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
            "page_number_is_primary": False,
            "left_pages_with_stamp_identity": sum(
                1 for item in left if item.get("stamp_key")
            ),
            "right_pages_with_stamp_identity": sum(
                1 for item in right if item.get("stamp_key")
            ),
            "stamp_exact_relations": sum(
                1 for item in relations if item.get("primary_source") == "STAMP_EXACT"
                and item["relation_type"] != "UNCERTAIN"
            ),
            "stamp_ambiguous_keys": stamp_ambiguous_keys,
            "stamp_group_relations": stamp_group_relations,
            "stamp_conflict_pairs": sum(
                1 for candidate in deep_by_pair.values()
                if candidate.get("stamp_relation") == "CONFLICT"
            ),
            "global_assignment_used": True,
            "greedy_assignment_used": False,
            "displaced_high_candidates": displaced_high,
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
    "PRIMARY_SOURCES",
    "KIND",
    "RELATION_TYPES",
    "SCHEMA_VERSION",
    "STATUSES",
    "match_sheets",
    "normalize_sheet",
    "sheet_label",
    "page_selection_suggestions",
]
