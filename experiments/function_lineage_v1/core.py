"""Function-first lineage matching over frozen Candidate Generator v4 data.

This module is deliberately isolated from the stage-comparison backend.  It
reads the frozen research inputs, assigns stable IDs to extracted functions
and fragments, and exposes only bounded, evidence-bearing lineage candidates
to a selector.  Physical sheet mappings are derived after function selection.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from experiments.ai_sheet_matcher.core import PROJECT_CONFIG, canonical_json, digest, stable_id
from experiments.candidate_v4.core import (
    ALGORITHM_VERSION as CANDIDATE_GENERATOR_VERSION,
    CandidateV4Dataset,
    build_candidate_v4_dataset,
)


ALGORITHM_VERSION = "function-lineage-matcher.v1"
RELATION_DOCUMENT_LINK = "DOCUMENT_LINK"
RELATION_FUNCTIONAL_ANALOGUE = "FUNCTIONAL_ANALOGUE"

DOCUMENT_ROLES = frozenset({
    "GRAPHIC_SHEET", "CHANGE_REGISTER", "TOC", "NOTE", "TABLE", "OTHER",
})
LINEAGE_RELATIONS = frozenset({
    "CONTINUED_1_TO_1",
    "SPLIT_1_TO_N",
    "MERGED_N_TO_1",
    "FUNCTION_DISTRIBUTED",
    "RENAMED_FUNCTION",
    "FUNCTION_EXPANDED",
    "FUNCTION_REDUCED",
    "FUNCTION_REMOVED",
    "NEW_FUNCTION",
    "NEED_MORE_EVIDENCE",
})
CONCRETE_RELATIONS = LINEAGE_RELATIONS - {
    "FUNCTION_REMOVED", "NEW_FUNCTION", "NEED_MORE_EVIDENCE",
}
SENTINELS = frozenset({"FUNCTION_REMOVED", "NEED_MORE_EVIDENCE"})
FUNCTIONAL_CHANNELS = (
    "FUNCTION",
    "OBJECT_ZONE",
    "SYSTEM",
    "CONSUMER",
    "EQUIPMENT_ROLE",
    "UPSTREAM_DOWNSTREAM",
    "TOPOLOGY",
    "ENTITY",
    "NEIGHBOR_CONTEXT",
)
SUPPORTING_CHANNEL = "DOCUMENT_CONTEXT"
PAGE_CANDIDATE_LIMIT = 8
GROUP_CANDIDATE_LIMIT = 8

PASSPORT_FIELDS = (
    "function_id", "source_sheet", "discipline", "function_class",
    "serviced_object", "corpus", "section", "zone", "floors", "systems",
    "consumers", "equipment_roles", "upstream", "downstream", "source_type",
    "receiver_type", "topology_role", "component_role",
    "neighboring_function_context", "document_role", "evidence_refs",
    "extraction_confidence", "contradictions",
)

_TOKEN_RE = re.compile(r"[a-zа-я0-9]+(?:[./_-][a-zа-я0-9]+)*", re.IGNORECASE)
_CORPUS_RE = re.compile(r"корпус", re.IGNORECASE)
_SECTION_RE = re.compile(r"секц", re.IGNORECASE)
_SCOPE_RE = re.compile(r"(?:корпус|секц(?:ия|ии|ий)?)\s*[№#]?\s*([0-9]+)(?:[.,][0-9]+)?", re.IGNORECASE)

_TOPOLOGY_ROLE = {
    "RISER_DISTRIBUTION": "VERTICAL_DISTRIBUTION",
    "PUMPING_PRESSURE": "PRESSURE_BOOST",
    "DOMESTIC_PRESSURE_BOOST": "DOMESTIC_PRESSURE_BOOST",
    "FIRE_PRESSURE_BOOST": "FIRE_PRESSURE_BOOST",
    "METERING": "METERING_NODE",
    "INCOMING_METERING": "INCOMING_METERING_NODE",
    "ELECTRICAL_DISTRIBUTION": "ELECTRICAL_DISTRIBUTION",
    "LOAD_CALCULATION": "CALCULATION",
    "WATER_DRAINAGE": "DRAINAGE_DISTRIBUTION",
    "WATER_SUPPLY": "WATER_DISTRIBUTION",
    "HOT_WATER": "HOT_WATER_DISTRIBUTION",
    "FIRE_WATER": "FIRE_WATER_DISTRIBUTION",
}


@dataclass
class FunctionLineageDataset:
    candidate_v4: CandidateV4Dataset
    function_passports: dict[str, dict[str, dict[str, Any]]]
    function_fragments: dict[str, dict[str, dict[str, Any]]]
    evidence_catalog: dict[str, dict[str, Any]]
    document_links: list[dict[str, Any]]
    candidates: dict[str, dict[str, Any]]
    tasks: list[dict[str, Any]]
    input_signature: str

    @property
    def project(self) -> str:
        return self.candidate_v4.base.project

    @property
    def pair_id(self) -> str:
        return self.candidate_v4.base.pair_id


def _unique(values: Iterable[Any], *, limit: int = 40) -> list[Any]:
    output: list[Any] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        cleaned = " ".join(str(value).split())
        key = cleaned.casefold()
        if cleaned and key not in seen:
            output.append(value if not isinstance(value, str) else cleaned)
            seen.add(key)
        if len(output) >= limit:
            break
    return output


def _tokens(values: Any) -> set[str]:
    if isinstance(values, Mapping):
        return set().union(*(_tokens(value) for value in values.values())) if values else set()
    if isinstance(values, (list, tuple, set)):
        return set().union(*(_tokens(value) for value in values)) if values else set()
    return {token.casefold() for token in _TOKEN_RE.findall(str(values or "")) if len(token) > 1}


def _jaccard(left: Any, right: Any) -> float:
    left_tokens, right_tokens = _tokens(left), _tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _document_role(raw: str) -> str:
    mapping = {
        "GRAPHIC_SHEET": "GRAPHIC_SHEET",
        "CHANGE_REGISTER": "CHANGE_REGISTER",
        "CONTENTS": "TOC",
        "TEXT_PAGE": "OTHER",
    }
    return mapping.get(raw, "OTHER")


def _field_refs(raw: Mapping[str, Any], sheet: Mapping[str, Any], source: str) -> list[str]:
    refs = list(raw.get("provenance", {}).get(source) or [])
    if not refs:
        refs = list(raw.get("evidence_refs") or sheet.get("text_evidence_references") or [])
    return sorted(set(str(value) for value in refs))


def _passport_v2(
    *, pair_id: str, raw: Mapping[str, Any], sheet: Mapping[str, Any], side: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Project a v4 extraction into a provenance-complete compact passport."""
    evidence_refs = sorted(set(
        str(value) for value in [
            *(raw.get("evidence_refs") or []),
            *(sheet.get("text_evidence_references") or []),
        ]
    ))
    source_sheet = {
        "side": side,
        "physical_page": int(sheet["physical_page"]),
        "graphic_sheet_number": sheet.get("graphic_sheet_number"),
        "document_version_id": sheet.get("document_version_id"),
    }
    objects = _unique(raw.get("serviced_object") or [])
    zones = _unique(raw.get("serviced_zone") or [])
    function_class = str(raw.get("function_class") or "GENERAL_DOCUMENT_FUNCTION")
    topology_role = _TOPOLOGY_ROLE.get(function_class, "GENERAL_FUNCTION")
    populated = sum(bool(value) for value in (
        function_class, objects, zones, sheet.get("floors"), raw.get("systems"),
        raw.get("consumers"), raw.get("equipment_roles"), raw.get("upstream"),
        raw.get("downstream"), sheet.get("topology_hints"),
    ))
    passport: dict[str, Any] = {
        "function_id": str(raw["function_id"]),
        "pair_id": pair_id,
        "side": side,
        "source_sheet": source_sheet,
        "discipline": sheet.get("discipline"),
        "function_class": function_class,
        "serviced_object": objects,
        "corpus": _unique(value for value in objects if _CORPUS_RE.search(str(value))),
        "section": _unique(value for value in objects if _SECTION_RE.search(str(value))),
        "zone": zones,
        "floors": _unique(sheet.get("floors") or [], limit=18),
        "systems": _unique(raw.get("systems") or [], limit=18),
        "consumers": _unique(raw.get("consumers") or [], limit=8),
        "equipment_roles": _unique(raw.get("equipment_roles") or [], limit=8),
        "upstream": _unique(raw.get("upstream") or [], limit=8),
        "downstream": _unique(raw.get("downstream") or [], limit=8),
        # v4 has literal upstream/downstream text, but no reliable categorical
        # source/receiver taxonomy.  Empty is an extraction state, not a fact.
        "source_type": [],
        "receiver_type": [],
        "topology_role": topology_role,
        "component_role": topology_role,
        "neighboring_function_context": [],
        "document_role": _document_role(str(sheet.get("_page_kind") or "")),
        "evidence_refs": evidence_refs,
        "extraction_confidence": round(populated / 10, 3),
        "contradictions": [],
    }
    provenance = {
        "function_id": [f"deterministic:{CANDIDATE_GENERATOR_VERSION}:function_id"],
        "source_sheet": _field_refs(raw, sheet, "function_class"),
        "discipline": list(sheet.get("provenance", {}).get("discipline") or evidence_refs),
        "function_class": _field_refs(raw, sheet, "function_class"),
        "serviced_object": _field_refs(raw, sheet, "serviced_object"),
        "corpus": _field_refs(raw, sheet, "serviced_object"),
        "section": _field_refs(raw, sheet, "serviced_object"),
        "zone": _field_refs(raw, sheet, "serviced_zone"),
        "floors": list(sheet.get("provenance", {}).get("floors") or evidence_refs),
        "systems": _field_refs(raw, sheet, "systems"),
        "consumers": _field_refs(raw, sheet, "consumers"),
        "equipment_roles": _field_refs(raw, sheet, "equipment_roles"),
        "upstream": _field_refs(raw, sheet, "upstream"),
        "downstream": _field_refs(raw, sheet, "downstream"),
        "source_type": [],
        "receiver_type": [],
        "topology_role": _field_refs(raw, sheet, "function_class"),
        "component_role": _field_refs(raw, sheet, "function_class"),
        "neighboring_function_context": [],
        "document_role": list(sheet.get("provenance", {}).get("document_subtype") or evidence_refs),
        "evidence_refs": evidence_refs,
        "extraction_confidence": evidence_refs,
        "contradictions": [],
    }
    passport["provenance"] = {field: provenance[field] for field in PASSPORT_FIELDS}
    snippets = _unique(raw.get("fragment_text") or [], limit=2)
    fragment_id = stable_id(
        "frag_", pair_id, side, source_sheet["physical_page"], passport["function_id"],
    )
    fragment = {
        "fragment_id": fragment_id,
        "function_id": passport["function_id"],
        "pair_id": pair_id,
        "side": side,
        "physical_page": source_sheet["physical_page"],
        "function_class": function_class,
        "component_role": topology_role,
        "document_role": passport["document_role"],
        "evidence_refs": evidence_refs,
        "evidence_snippets": [str(value)[:360] for value in snippets],
        "capacity_key": (
            f"RIGHT:{source_sheet['physical_page']}:{fragment_id}" if side == "RIGHT" else None
        ),
    }
    passport["function_fragment_ids"] = [fragment_id]
    return passport, fragment


def _extract_functions(
    base: CandidateV4Dataset, left_pages: Sequence[int], right_pages: Sequence[int],
) -> tuple[
    dict[str, dict[str, dict[str, Any]]],
    dict[str, dict[str, dict[str, Any]]],
    dict[str, dict[str, Any]],
]:
    passports: dict[str, dict[str, dict[str, Any]]] = {"LEFT": {}, "RIGHT": {}}
    fragments: dict[str, dict[str, dict[str, Any]]] = {"LEFT": {}, "RIGHT": {}}
    evidence: dict[str, dict[str, Any]] = {}
    for side, pages in (("LEFT", left_pages), ("RIGHT", right_pages)):
        for page in pages:
            sheet = base.sheet_passports[side][int(page)]
            evidence.update({str(key): dict(value) for key, value in sheet["evidence_catalog"].items()})
            page_ids: list[str] = []
            for raw in base.function_passports[side][int(page)]:
                passport, fragment = _passport_v2(
                    pair_id=base.base.pair_id, raw=raw, sheet=sheet, side=side,
                )
                passports[side][passport["function_id"]] = passport
                fragments[side][fragment["fragment_id"]] = fragment
                page_ids.append(passport["function_id"])
            for function_id in page_ids:
                neighbor_ids = [value for value in page_ids if value != function_id]
                passports[side][function_id]["neighboring_function_context"] = neighbor_ids
                passports[side][function_id]["provenance"]["neighboring_function_context"] = list(
                    passports[side][function_id]["evidence_refs"]
                )
    return passports, fragments, evidence


def _by_page(passports: Mapping[str, Mapping[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    output: dict[int, list[dict[str, Any]]] = {}
    for passport in passports.values():
        output.setdefault(int(passport["source_sheet"]["physical_page"]), []).append(dict(passport))
    for values in output.values():
        values.sort(key=lambda item: (item["function_class"], item["function_id"]))
    return output


def _fragments_by_function(
    fragments: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {str(fragment["function_id"]): dict(fragment) for fragment in fragments.values()}


def _primary_classes(functions: Sequence[Mapping[str, Any]]) -> list[str]:
    classes = {str(value["function_class"]) for value in functions}
    # The atomic fragment representing a sheet-level riser or metering detail
    # is preferable to its broad WATER_SUPPLY alias.  Distributed candidates
    # use their explicit component coverage instead of this projection.
    if "RISER_DISTRIBUTION" in classes:
        return ["RISER_DISTRIBUTION"]
    if "METERING" in classes:
        return ["METERING"]
    specific = [
        value for value in (
            "DOMESTIC_PRESSURE_BOOST", "FIRE_PRESSURE_BOOST", "INCOMING_METERING",
            "LOAD_CALCULATION", "ELECTRICAL_DISTRIBUTION", "WATER_DRAINAGE",
            "FIRE_WATER", "HOT_WATER", "WATER_SUPPLY",
        ) if value in classes
    ]
    return specific[:2] or sorted(classes)[:1]


def _scope_ids(functions: Sequence[Mapping[str, Any]]) -> set[str]:
    return {
        match
        for function in functions
        for value in [
            *(function.get("serviced_object") or []),
            *(function.get("corpus") or []),
            *(function.get("section") or []),
            *(function.get("zone") or []),
        ]
        for match in _SCOPE_RE.findall(str(value))
    }


def _scope_compatible(
    left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]],
) -> bool:
    left_scope, right_scope = _scope_ids(left), _scope_ids(right)
    return not left_scope or not right_scope or bool(left_scope & right_scope)


def _channel_scores(left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    def values(items: Sequence[Mapping[str, Any]], *fields: str) -> list[Any]:
        return [value for item in items for field in fields for value in (
            item.get(field) if isinstance(item.get(field), list) else [item.get(field)]
        ) if value]

    left_classes = {str(value["function_class"]) for value in left}
    right_classes = {str(value["function_class"]) for value in right}
    coverage = len(left_classes & right_classes) / len(left_classes) if left_classes else 0.0
    scores = {
        "FUNCTION": coverage,
        "OBJECT_ZONE": _jaccard(
            values(left, "serviced_object", "corpus", "section", "zone", "floors"),
            values(right, "serviced_object", "corpus", "section", "zone", "floors"),
        ),
        "SYSTEM": _jaccard(values(left, "systems"), values(right, "systems")),
        "CONSUMER": _jaccard(values(left, "consumers"), values(right, "consumers")),
        "EQUIPMENT_ROLE": _jaccard(values(left, "equipment_roles"), values(right, "equipment_roles")),
        "UPSTREAM_DOWNSTREAM": _jaccard(
            values(left, "upstream", "downstream"), values(right, "upstream", "downstream"),
        ),
        "TOPOLOGY": _jaccard(values(left, "topology_role"), values(right, "topology_role")),
        "ENTITY": _jaccard(
            values(left, "systems", "equipment_roles", "serviced_object"),
            values(right, "systems", "equipment_roles", "serviced_object"),
        ),
        "NEIGHBOR_CONTEXT": _jaccard(
            values(left, "neighboring_function_context"),
            values(right, "neighboring_function_context"),
        ),
    }
    return {key: round(float(scores[key]), 6) for key in FUNCTIONAL_CHANNELS}


def _functional_score(scores: Mapping[str, float]) -> float:
    weights = {
        "FUNCTION": 0.42,
        "OBJECT_ZONE": 0.17,
        "SYSTEM": 0.08,
        "CONSUMER": 0.05,
        "EQUIPMENT_ROLE": 0.08,
        "UPSTREAM_DOWNSTREAM": 0.07,
        "TOPOLOGY": 0.07,
        "ENTITY": 0.04,
        "NEIGHBOR_CONTEXT": 0.02,
    }
    return round(sum(scores[key] * weight for key, weight in weights.items()), 8)


def _component_map(
    left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]],
    fragments: Mapping[str, dict[str, Any]], classes: Sequence[str],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for function_class in classes:
        left_matches = [value for value in left if value["function_class"] == function_class]
        right_matches = [value for value in right if value["function_class"] == function_class]
        for left_function in left_matches:
            for right_function in right_matches:
                left_fragment = fragments[str(left_function["function_id"])]
                right_fragment = fragments[str(right_function["function_id"])]
                output.append({
                    "component_role": function_class,
                    "left_function_id": left_function["function_id"],
                    "left_fragment_id": left_fragment["fragment_id"],
                    "right_function_id": right_function["function_id"],
                    "right_fragment_id": right_fragment["fragment_id"],
                    "right_physical_page": right_function["source_sheet"]["physical_page"],
                    "capacity_key": right_fragment["capacity_key"],
                })
    return output


def _candidate(
    *, base: CandidateV4Dataset, source_id: str, relation_type: str,
    left_pages: Sequence[int], right_pages: Sequence[int],
    component_map: Sequence[Mapping[str, Any]], channel_scores: Mapping[str, float],
    evidence_refs: Sequence[str], source_kind: str, source_rank: int | None,
    source_reasons: Sequence[str],
) -> dict[str, Any]:
    identity = {
        "source": source_id,
        "relation": relation_type,
        "left_fragments": sorted(str(value["left_fragment_id"]) for value in component_map),
        "right_fragments": sorted(str(value["right_fragment_id"]) for value in component_map),
    }
    candidate_id = stable_id("lcand_", base.base.pair_id, identity)
    return {
        "candidate_id": candidate_id,
        "pair_id": base.base.pair_id,
        "project": base.base.project,
        "relation_namespace": RELATION_FUNCTIONAL_ANALOGUE,
        "relation_type": relation_type,
        "direction": "LEFT_TO_RIGHT",
        "left_pages": sorted(set(int(value) for value in left_pages)),
        "right_pages": sorted(set(int(value) for value in right_pages)),
        "left_function_ids": sorted(set(str(value["left_function_id"]) for value in component_map)),
        "right_function_ids": sorted(set(str(value["right_function_id"]) for value in component_map)),
        "left_fragment_ids": sorted(set(str(value["left_fragment_id"]) for value in component_map)),
        "right_fragment_ids": sorted(set(str(value["right_fragment_id"]) for value in component_map)),
        "right_capacity_keys": sorted(set(str(value["capacity_key"]) for value in component_map)),
        "component_map": [dict(value) for value in component_map],
        "retrieval_channels": [key for key in FUNCTIONAL_CHANNELS if channel_scores.get(key, 0) > 0],
        "channel_scores": dict(channel_scores),
        "functional_score": _functional_score(channel_scores),
        "document_context": {
            "channel": SUPPORTING_CHANNEL,
            "supporting_only": True,
            "included_in_functional_score": False,
        },
        "evidence_refs": sorted(set(str(value) for value in evidence_refs)),
        "source_candidate_id": source_id,
        "source_candidate_kind": source_kind,
        "source_rank": source_rank,
        "source_reasons": list(source_reasons),
        "generator_version": ALGORITHM_VERSION,
    }


def _lineage_candidates(
    base: CandidateV4Dataset,
    passports: Mapping[str, Mapping[str, Mapping[str, Any]]],
    fragments: Mapping[str, Mapping[str, Mapping[str, Any]]],
    focus_pages: Sequence[int],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    left_by_page = _by_page(passports["LEFT"])
    right_by_page = _by_page(passports["RIGHT"])
    fragments_by_function = {
        **_fragments_by_function(fragments["LEFT"]),
        **_fragments_by_function(fragments["RIGHT"]),
    }
    candidates: dict[str, dict[str, Any]] = {}
    task_candidates: dict[int, list[dict[str, Any]]] = {int(page): [] for page in focus_pages}

    for left_page in focus_pages:
        left_functions = left_by_page[int(left_page)]
        primary = _primary_classes(left_functions)
        for raw in base.candidate_sets[int(left_page)]["candidates"]:
            right_page = int(raw["right_physical_page"])
            right_functions = right_by_page.get(right_page, [])
            if not right_functions or any(value["document_role"] != "GRAPHIC_SHEET" for value in right_functions):
                continue
            if not _scope_compatible(left_functions, right_functions):
                continue
            component_map = _component_map(
                left_functions, right_functions, fragments_by_function, primary,
            )
            if not component_map:
                continue
            scores = _channel_scores(left_functions, right_functions)
            candidate = _candidate(
                base=base,
                source_id=str(raw["candidate_id"]),
                relation_type="CONTINUED_1_TO_1",
                left_pages=[int(left_page)],
                right_pages=[right_page],
                component_map=component_map,
                channel_scores=scores,
                evidence_refs=[
                    *left_functions[0]["evidence_refs"],
                    *right_functions[0]["evidence_refs"],
                ],
                source_kind="RIGHT_PAGE",
                source_rank=int(raw.get("rank") or 0),
                source_reasons=raw.get("which_channels_found") or [],
            )
            candidates[candidate["candidate_id"]] = candidate
            task_candidates[int(left_page)].append(candidate)

    relation_mapping = {
        "MATCH_1_TO_1": "CONTINUED_1_TO_1",
        "SPLIT_1_TO_N": "SPLIT_1_TO_N",
        "MERGED_N_TO_1": "MERGED_N_TO_1",
        "FUNCTION_DISTRIBUTED": "FUNCTION_DISTRIBUTED",
    }
    for raw in base.group_candidates:
        left_pages = [int(value) for value in raw.get("left_pages") or []]
        right_pages = [int(value) for value in raw.get("right_pages") or []]
        if not set(left_pages) & set(focus_pages):
            continue
        if raw.get("relation_type") == "MERGED_N_TO_1" and not set(left_pages) <= set(focus_pages):
            continue
        coverage = raw.get("component_coverage") or {}
        nested_coverage = bool(coverage) and all(
            isinstance(value, Mapping) for value in coverage.values()
        )
        coverage_rows: list[tuple[list[int], str, list[int]]] = []
        if nested_coverage:
            for left_page_key, component_values in coverage.items():
                for function_class, covered_pages in component_values.items():
                    coverage_rows.append((
                        [int(left_page_key)], str(function_class),
                        [int(value) for value in covered_pages],
                    ))
        else:
            for function_class, covered_pages in coverage.items():
                coverage_rows.append((
                    list(left_pages), str(function_class),
                    [int(value) for value in covered_pages],
                ))
        if not coverage_rows and raw.get("relation_type") == "MERGED_N_TO_1":
            coverage_rows = [
                (list(left_pages), str(function_class), list(right_pages))
                for function_class in raw.get("covered_functions") or []
            ]
        classes = sorted({function_class for _, function_class, _ in coverage_rows})
        left_functions = [value for page in left_pages for value in left_by_page.get(page, [])]
        right_functions = [value for page in right_pages for value in right_by_page.get(page, [])]
        if not left_functions or not right_functions:
            continue
        if any(value["document_role"] != "GRAPHIC_SHEET" for value in right_functions):
            continue
        specific_components = {
            "DOMESTIC_PRESSURE_BOOST", "FIRE_PRESSURE_BOOST", "INCOMING_METERING",
        } & {str(value["function_class"]) for value in left_functions}
        if len(specific_components) >= 2:
            coverage_rows = [
                value for value in coverage_rows if value[1] in specific_components
            ]
            classes = sorted(specific_components)
        if any(
            not _scope_compatible(
                left_functions,
                [
                    value for value in right_functions
                    if int(value["source_sheet"]["physical_page"]) == right_page
                ],
            )
            for right_page in right_pages
        ):
            continue
        if raw.get("relation_type") == "MERGED_N_TO_1" and any(
            not _scope_compatible(left_by_page.get(left_page, []), right_functions)
            for left_page in left_pages
        ):
            continue
        component_map: list[dict[str, Any]] = []
        for covered_left_pages, function_class, covered_right_pages in coverage_rows:
            allowed_pages = set(covered_right_pages)
            component_map.extend(_component_map(
                [
                    value for value in left_functions
                    if value["function_class"] == function_class
                    and int(value["source_sheet"]["physical_page"]) in covered_left_pages
                ],
                [
                    value for value in right_functions
                    if value["function_class"] == function_class
                    and int(value["source_sheet"]["physical_page"]) in allowed_pages
                ],
                fragments_by_function,
                [function_class],
            ))
        if not component_map:
            continue
        if specific_components and {
            str(value["component_role"]) for value in component_map
        } != specific_components:
            continue
        roles_by_page = {
            right_page: {
                str(value["component_role"])
                for value in component_map if int(value["right_physical_page"]) == right_page
            }
            for right_page in right_pages
        }
        all_roles = {str(value["component_role"]) for value in component_map}
        if (
            raw.get("relation_type") != "MERGED_N_TO_1"
            and len(right_pages) > 1
            and any(roles >= all_roles for roles in roles_by_page.values())
        ):
            # One member already covers the declared component set.  The other
            # pages are alternatives, not evidence of SPLIT/DISTRIBUTED.
            continue
        scores = _channel_scores(left_functions, right_functions)
        # Explicit component completeness is the defining signal for a group;
        # it comes from v4 extraction and is not a benchmark/reference label.
        component_completeness = len({value["component_role"] for value in component_map}) / max(1, len(classes))
        scores["FUNCTION"] = round(max(scores["FUNCTION"], component_completeness), 6)
        candidate = _candidate(
            base=base,
            source_id=str(raw["candidate_group_id"]),
            relation_type=relation_mapping.get(str(raw["relation_type"]), "FUNCTION_DISTRIBUTED"),
            left_pages=left_pages,
            right_pages=right_pages,
            component_map=component_map,
            channel_scores=scores,
            evidence_refs=raw.get("evidence_refs") or [],
            source_kind="RIGHT_FUNCTION_GROUP",
            source_rank=int(raw.get("group_rank") or 0),
            source_reasons=raw.get("why_group_exists") or [],
        )
        candidates[candidate["candidate_id"]] = candidate
        for left_page in set(left_pages) & set(focus_pages):
            task_candidates[int(left_page)].append(candidate)

    tasks = []
    for left_page in focus_pages:
        values = [
            value
            for value in {value["candidate_id"]: value for value in task_candidates[int(left_page)]}.values()
            if value["left_pages"] == [int(left_page)]
            or value["relation_type"] == "MERGED_N_TO_1"
        ]
        pages = sorted(
            (value for value in values if value["source_candidate_kind"] == "RIGHT_PAGE"),
            key=lambda value: (-value["functional_score"], value["source_rank"] or 999, value["candidate_id"]),
        )[:PAGE_CANDIDATE_LIMIT]
        groups = sorted(
            (value for value in values if value["source_candidate_kind"] == "RIGHT_FUNCTION_GROUP"),
            key=lambda value: (
                value["source_rank"] or 999,
                -len({row["component_role"] for row in value["component_map"]}),
                -value["functional_score"], value["candidate_id"],
            ),
        )[:GROUP_CANDIDATE_LIMIT]
        option_ids = [value["candidate_id"] for value in [*groups, *pages]]
        task_id = stable_id("ltask_", base.base.pair_id, int(left_page))
        tasks.append({
            "task_id": task_id,
            "pair_id": base.base.pair_id,
            "project": base.base.project,
            "left_physical_page": int(left_page),
            "left_function_ids": sorted(
                value["function_id"] for value in left_by_page[int(left_page)]
            ),
            "candidate_ids": option_ids,
            "allowed_outputs": [*option_ids, "FUNCTION_REMOVED", "NEED_MORE_EVIDENCE"],
        })
    exposed = {value for task in tasks for value in task["candidate_ids"]}
    return {key: value for key, value in candidates.items() if key in exposed}, tasks


def _document_links(base: CandidateV4Dataset, focus_pages: Sequence[int]) -> list[dict[str, Any]]:
    """Detect documentary correspondence independently from functional score."""
    output: list[dict[str, Any]] = []
    for left_page in focus_pages:
        left = base.sheet_passports["LEFT"][int(left_page)]
        number = str(left.get("graphic_sheet_number") or "").strip().casefold()
        if not number:
            continue
        for right_page, right in base.sheet_passports["RIGHT"].items():
            right_number = str(right.get("graphic_sheet_number") or "").strip().casefold()
            if right_number != number:
                continue
            role = _document_role(str(right.get("_page_kind") or ""))
            title_overlap = _jaccard(left.get("title"), right.get("title"))
            if role not in {"CHANGE_REGISTER", "TOC", "TABLE", "NOTE"} and title_overlap < 0.2:
                continue
            basis = "CHANGE_REGISTER_STAMP" if role == "CHANGE_REGISTER" else "SHEET_NUMBER_TITLE_STAMP"
            evidence_refs = sorted(set([
                *(left.get("provenance", {}).get("graphic_sheet_number") or []),
                *(right.get("provenance", {}).get("graphic_sheet_number") or []),
                *(right.get("toc_references") and [
                    value["evidence_ref"] for value in right["toc_references"]
                ] or []),
            ]))
            output.append({
                "document_link_id": stable_id(
                    "dlink_", base.base.pair_id, int(left_page), int(right_page), basis,
                ),
                "pair_id": base.base.pair_id,
                "project": base.base.project,
                "relation_namespace": RELATION_DOCUMENT_LINK,
                "left_physical_page": int(left_page),
                "right_physical_page": int(right_page),
                "right_document_role": role,
                "detector_sources": [basis],
                "evidence_refs": evidence_refs,
                "functional_score_contribution": 0,
                "generator_version": ALGORITHM_VERSION,
            })
    return sorted(output, key=lambda value: (value["left_physical_page"], value["right_physical_page"]))


def build_function_lineage_dataset(repo_root: Path, pair_id: str) -> FunctionLineageDataset:
    base = build_candidate_v4_dataset(repo_root, pair_id)
    focus_pages = [int(value) for value in PROJECT_CONFIG[pair_id]["focus_left_pages"]]
    right_pages = sorted({
        int(candidate["right_physical_page"])
        for page in focus_pages for candidate in base.candidate_sets[page]["candidates"]
    } | {
        int(page)
        for group in base.group_candidates
        if set(group.get("left_pages") or []) & set(focus_pages)
        for page in group.get("right_pages") or []
    })
    passports, fragments, evidence = _extract_functions(base, focus_pages, right_pages)
    candidates, tasks = _lineage_candidates(base, passports, fragments, focus_pages)
    document_links = _document_links(base, focus_pages)
    signature = digest({
        "algorithm": ALGORITHM_VERSION,
        "candidate_generator": base.input_signature,
        "passports": passports,
        "fragments": fragments,
        "document_links": document_links,
        "candidates": candidates,
        "tasks": tasks,
    })
    return FunctionLineageDataset(
        base, passports, fragments, evidence, document_links, candidates, tasks, signature,
    )


def _take(values: Any, limit: int = 2, char_limit: int = 120) -> Any:
    if isinstance(values, list):
        return [str(value)[:char_limit] for value in values[:limit]]
    return values


def _function_core(passport: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "function_id", "function_class", "serviced_object", "corpus", "section", "zone",
        "floors", "systems", "consumers", "equipment_roles", "upstream", "downstream",
        "topology_role", "component_role", "contradictions", "document_role",
    )
    return {
        **{field: _take(passport.get(field)) for field in fields},
        "source_sheet": passport["source_sheet"],
        "fragment_ids": passport["function_fragment_ids"],
    }


def build_selector_payload(dataset: FunctionLineageDataset) -> dict[str, Any]:
    referenced_functions = {
        function_id
        for candidate in dataset.candidates.values()
        for function_id in [*candidate["left_function_ids"], *candidate["right_function_ids"]]
    }
    function_cores = {
        function_id: _function_core(passport)
        for side in ("LEFT", "RIGHT")
        for function_id, passport in dataset.function_passports[side].items()
        if function_id in referenced_functions
    }
    candidates = [{
        key: (
            [{
                "component_role": row["component_role"],
                "left_fragment_id": row["left_fragment_id"],
                "right_fragment_id": row["right_fragment_id"],
                "right_physical_page": row["right_physical_page"],
            } for row in value]
            if key == "component_map" else value
        ) for key, value in candidate.items() if key in {
            "candidate_id", "relation_namespace", "relation_type", "direction",
            "left_pages", "right_pages", "left_function_ids", "right_function_ids",
            "left_fragment_ids", "right_fragment_ids", "component_map",
            "retrieval_channels", "document_context", "evidence_refs",
            "source_reasons",
        }
    } for candidate in dataset.candidates.values()]
    payload: dict[str, Any] = {
        "schema_version": "function-lineage-selector.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "candidate_generator": CANDIDATE_GENERATOR_VERSION,
        "project": dataset.project,
        "pair_id": dataset.pair_id,
        "relation_namespace": RELATION_FUNCTIONAL_ANALOGUE,
        "tasks": dataset.tasks,
        "function_cores": function_cores,
        "lineage_candidates": sorted(candidates, key=lambda value: value["candidate_id"]),
        "policy": {
            "select_only_candidate_ids": True,
            "document_links_are_separate": True,
            "document_context_is_supporting_only": True,
            "page_proximity_is_functional_signal": False,
            "invented_pages_functions_fragments_groups_evidence_forbidden": True,
            "function_removed_requires_exhaustive_absence_candidate": True,
            "new_sheet_is_not_new_function": True,
            "removed_sheet_is_not_removed_function": True,
        },
    }
    payload["payload_signature"] = digest(payload)
    return payload


def build_selector_prompt(dataset: FunctionLineageDataset) -> tuple[str, dict[str, Any]]:
    payload = build_selector_payload(dataset)
    prompt = "\n".join([
        "You are a bounded engineering FUNCTION LINEAGE selector.",
        "The objective is FUNCTIONAL_ANALOGUE, never documentary navigation.",
        "For every task choose exactly one listed candidate_id or NEED_MORE_EVIDENCE.",
        "Prefer exact object/zone, component role and source-to-receiver topology.",
        "A function may split or be distributed across several sheets. A physical RIGHT sheet may",
        "be reused when different right_fragment_ids are used. Never resolve by page proximity.",
        "CHANGE_REGISTER/TOC/document links are not functional analogues.",
        "Do not invent IDs, pages, functions, fragments, evidence, groups, or relations.",
        "FUNCTION_REMOVED is unavailable unless an exhaustive absence candidate is listed; none is implied by a missing sheet.",
        "Return only the JSON object required by the output schema.",
        "payload=" + canonical_json(payload),
    ])
    return prompt, payload


def output_schema(dataset: FunctionLineageDataset, payload_signature: str) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["payload_signature", "selections"],
        "properties": {
            "payload_signature": {"type": "string", "const": payload_signature},
            "selections": {
                "type": "array",
                "minItems": len(dataset.tasks),
                "maxItems": len(dataset.tasks),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["task_id", "candidate_id"],
                    "properties": {
                        "task_id": {"type": "string", "enum": [value["task_id"] for value in dataset.tasks]},
                        "candidate_id": {
                            "type": "string",
                            "enum": sorted({
                                output for task in dataset.tasks for output in task["allowed_outputs"]
                            }),
                        },
                    },
                },
            },
        },
    }


def verify_capacity(
    selections: Sequence[Mapping[str, Any]], candidates: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    """Reject incompatible fragment reuse while allowing physical-page reuse."""
    occupants: dict[str, str] = {}
    errors: list[str] = []
    for selection in selections:
        candidate_id = str(selection.get("candidate_id") or "")
        candidate = candidates.get(candidate_id)
        if not candidate:
            continue
        for key in candidate.get("right_capacity_keys") or []:
            previous = occupants.get(str(key))
            if previous is None or previous == candidate_id:
                occupants[str(key)] = candidate_id
            else:
                errors.append(f"FUNCTION_FRAGMENT_CONFLICT:{key}:{previous}:{candidate_id}")
    return sorted(set(errors))


def verify_selector_response(
    dataset: FunctionLineageDataset,
    payload_signature: str,
    response: Mapping[str, Any] | None,
) -> dict[str, Any]:
    task_by_id = {str(value["task_id"]): value for value in dataset.tasks}
    result: dict[str, Any] = {
        "ok": False,
        "global_errors": [],
        "task_results": {},
        "model_supplied_evidence_possible": False,
        "engineer_mapping_write_possible": False,
    }
    if not isinstance(response, Mapping):
        result["global_errors"] = ["MODEL_FAILURE"]
        return result
    if set(response) - {"payload_signature", "selections"}:
        result["global_errors"].append("UNKNOWN_RESPONSE_FIELD")
    if response.get("payload_signature") != payload_signature:
        result["global_errors"].append("PAYLOAD_SIGNATURE_MISMATCH")
    raw_selections = response.get("selections")
    if not isinstance(raw_selections, list):
        result["global_errors"].append("INVALID_SELECTIONS")
        raw_selections = []
    selections_by_task: dict[str, Mapping[str, Any]] = {}
    for raw in raw_selections:
        if not isinstance(raw, Mapping):
            result["global_errors"].append("INVALID_SELECTION")
            continue
        task_id = str(raw.get("task_id") or "")
        if task_id in selections_by_task:
            result["global_errors"].append("DUPLICATE_TASK")
        selections_by_task[task_id] = raw
    if set(selections_by_task) != set(task_by_id):
        result["global_errors"].append("TASK_SET_MISMATCH")

    concrete: list[dict[str, Any]] = []
    for task_id, task in task_by_id.items():
        raw = selections_by_task.get(task_id) or {}
        errors: list[str] = []
        extra_fields = set(raw) - {"task_id", "candidate_id"}
        if "evidence_refs" in extra_fields or "evidence" in extra_fields:
            errors.append("AI_INVENTED_EVIDENCE")
        if extra_fields & {"function_ids", "fragment_ids", "right_fragment_ids", "left_fragment_ids"}:
            errors.append("AI_INVENTED_FRAGMENT")
        if extra_fields:
            errors.append("UNKNOWN_SELECTION_FIELD")
        candidate_id = str(raw.get("candidate_id") or "")
        if candidate_id not in task["allowed_outputs"]:
            errors.append("CANDIDATE_ID_NOT_BOUNDED")
        if candidate_id == "FUNCTION_REMOVED":
            errors.append("FUNCTION_REMOVED_WITHOUT_EXHAUSTIVE_EVIDENCE")
        candidate = dataset.candidates.get(candidate_id)
        if candidate:
            if candidate["relation_namespace"] != RELATION_FUNCTIONAL_ANALOGUE:
                errors.append("RELATION_NAMESPACE_MIXED")
            if candidate["relation_type"] not in CONCRETE_RELATIONS:
                errors.append("RELATION_TYPE_NOT_ALLOWED")
            if candidate["direction"] != "LEFT_TO_RIGHT":
                errors.append("DIRECTION_NOT_LEFT_TO_RIGHT")
            for function_id in candidate["left_function_ids"]:
                if function_id not in dataset.function_passports["LEFT"]:
                    errors.append("LEFT_FUNCTION_NOT_FOUND")
            for function_id in candidate["right_function_ids"]:
                if function_id not in dataset.function_passports["RIGHT"]:
                    errors.append("RIGHT_FUNCTION_NOT_FOUND")
            for fragment_id in candidate["left_fragment_ids"]:
                if fragment_id not in dataset.function_fragments["LEFT"]:
                    errors.append("LEFT_FRAGMENT_NOT_FOUND")
            for fragment_id in candidate["right_fragment_ids"]:
                if fragment_id not in dataset.function_fragments["RIGHT"]:
                    errors.append("RIGHT_FRAGMENT_NOT_FOUND")
            for evidence_ref in candidate["evidence_refs"]:
                if evidence_ref not in dataset.evidence_catalog:
                    errors.append("EVIDENCE_NOT_FOUND")
        result["task_results"][task_id] = {
            "ok": not errors,
            "candidate_id": candidate_id,
            "errors": sorted(set(errors)),
        }
        if candidate:
            concrete.append({"task_id": task_id, "candidate_id": candidate_id})

    capacity_errors = verify_capacity(concrete, dataset.candidates)
    selected_by_page = {
        int(task_by_id[task_id]["left_physical_page"]): str(raw.get("candidate_id") or "")
        for task_id, raw in selections_by_task.items() if task_id in task_by_id
    }
    group_errors: list[str] = []
    for candidate_id in sorted({value["candidate_id"] for value in concrete}):
        candidate = dataset.candidates[candidate_id]
        if candidate["relation_type"] != "MERGED_N_TO_1":
            continue
        if any(selected_by_page.get(int(page)) != candidate_id for page in candidate["left_pages"]):
            group_errors.append(f"INCOMPLETE_MERGE_GROUP:{candidate_id}")
    result["global_errors"].extend([*capacity_errors, *group_errors])
    result["global_errors"] = sorted(set(result["global_errors"]))
    non_capacity_errors = [
        value for value in result["global_errors"]
        if not value.startswith("FUNCTION_FRAGMENT_CONFLICT:")
        and not value.startswith("INCOMPLETE_MERGE_GROUP:")
    ]
    if non_capacity_errors:
        for task_result in result["task_results"].values():
            task_result["ok"] = False
    for error in capacity_errors:
        for task_result in result["task_results"].values():
            if str(task_result.get("candidate_id") or "") in error:
                task_result["ok"] = False
    for error in group_errors:
        candidate_id = error.rsplit(":", 1)[-1]
        for task_result in result["task_results"].values():
            if task_result.get("candidate_id") == candidate_id:
                task_result["ok"] = False
    result["ok"] = not result["global_errors"] and all(
        value["ok"] for value in result["task_results"].values()
    )
    return result


def stable_consensus(
    dataset: FunctionLineageDataset, records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Require verified Pass A/B agreement in all three independent cold runs."""
    output: list[dict[str, Any]] = []
    for task in dataset.tasks:
        task_id = str(task["task_id"])
        observations: list[dict[str, Any]] = []
        for record in sorted(records, key=lambda value: (int(value["cold_run"]), str(value["pass_name"]))):
            verification = record.get("verification") or {}
            task_result = (verification.get("task_results") or {}).get(task_id) or {}
            observations.append({
                "cold_run": int(record["cold_run"]),
                "pass_name": str(record["pass_name"]),
                "model_ok": bool((record.get("model_call") or {}).get("ok")),
                "verified": bool(task_result.get("ok")),
                "candidate_id": task_result.get("candidate_id"),
            })
        choices = [value["candidate_id"] for value in observations if value["verified"]]
        stable = len(observations) == 6 and len(choices) == 6 and len(set(choices)) == 1
        selected = str(choices[0]) if stable else "NEED_MORE_EVIDENCE"
        candidate = dataset.candidates.get(selected)
        output.append({
            "task_id": task_id,
            "pair_id": dataset.pair_id,
            "project": dataset.project,
            "left_physical_page": task["left_physical_page"],
            "stable": stable,
            "selected_candidate_id": selected,
            "relation_namespace": RELATION_FUNCTIONAL_ANALOGUE,
            "relation_type": candidate["relation_type"] if candidate else "NEED_MORE_EVIDENCE",
            "left_function_ids": candidate["left_function_ids"] if candidate else task["left_function_ids"],
            "right_function_ids": candidate["right_function_ids"] if candidate else [],
            "left_fragment_ids": candidate["left_fragment_ids"] if candidate else [],
            "right_fragment_ids": candidate["right_fragment_ids"] if candidate else [],
            "left_pages": candidate["left_pages"] if candidate else [task["left_physical_page"]],
            "right_pages": candidate["right_pages"] if candidate else [],
            "right_capacity_keys": candidate["right_capacity_keys"] if candidate else [],
            "observations": observations,
            "materialization_allowed": False,
        })
    return output


def derive_sheet_map(decisions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Build a physical presentation only after stable function lineages exist."""
    unique: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        candidate_id = str(decision.get("selected_candidate_id") or "")
        if not decision.get("stable") or candidate_id in SENTINELS:
            continue
        unique.setdefault(candidate_id, {
            "lineage_id": stable_id("lineage_", decision["pair_id"], candidate_id),
            "candidate_id": candidate_id,
            "pair_id": decision["pair_id"],
            "project": decision["project"],
            "relation_type": decision["relation_type"],
            "left_pages": decision["left_pages"],
            "right_pages": decision["right_pages"],
            "left_function_ids": decision["left_function_ids"],
            "right_function_ids": decision["right_function_ids"],
            "right_fragment_ids": decision["right_fragment_ids"],
            "right_capacity_keys": decision["right_capacity_keys"],
        })
    relations = sorted(unique.values(), key=lambda value: (
        value["pair_id"], value["left_pages"], value["right_pages"], value["candidate_id"],
    ))
    reuse: list[dict[str, Any]] = []
    false_conflicts: list[dict[str, Any]] = []
    by_pair_page: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for relation in relations:
        for page in relation["right_pages"]:
            by_pair_page.setdefault((relation["pair_id"], int(page)), []).append(relation)
    for (pair_id, page), occupants in sorted(by_pair_page.items()):
        if len(occupants) < 2:
            continue
        keys = [set(value["right_capacity_keys"]) for value in occupants]
        disjoint = all(not keys[i] & keys[j] for i in range(len(keys)) for j in range(i + 1, len(keys)))
        row = {
            "pair_id": pair_id,
            "right_physical_page": page,
            "lineage_ids": [value["lineage_id"] for value in occupants],
            "right_fragment_ids": sorted({
                fragment for value in occupants for fragment in value["right_fragment_ids"]
            }),
            "function_level_compatible": disjoint,
        }
        reuse.append(row)
        if disjoint:
            false_conflicts.append(row)
    return {
        "kind": "derived_sheet_map",
        "schema_version": "derived-sheet-map.v1",
        "derivation": "FUNCTION_LINEAGE -> FUNCTION_FRAGMENTS -> PHYSICAL_SHEETS",
        "relations": relations,
        "right_sheet_reuse": reuse,
        "false_sheet_global_conflicts_avoided": false_conflicts,
    }


def prompt_character_count(dataset: FunctionLineageDataset) -> int:
    prompt, _ = build_selector_prompt(dataset)
    return len(prompt)


def serialized_dataset_summary(dataset: FunctionLineageDataset) -> str:
    return json.dumps({
        "project": dataset.project,
        "passports": sum(len(value) for value in dataset.function_passports.values()),
        "fragments": sum(len(value) for value in dataset.function_fragments.values()),
        "document_links": len(dataset.document_links),
        "candidates": len(dataset.candidates),
        "tasks": len(dataset.tasks),
        "signature": dataset.input_signature,
    }, ensure_ascii=False, sort_keys=True)
