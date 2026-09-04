"""Fail-closed Function Lineage Matcher v1 production shadow contour.

The contour consumes only compact sheet facts already prepared for the
production sheet matcher.  It never reads images, changes sheet scope, writes
human decisions, or materializes model output into the comparison result.

Three namespaces intentionally remain distinct:

* ``DOCUMENT_LINK`` is a projection of documentary sheet correspondence;
* ``FUNCTIONAL_ANALOGUE`` contains bounded candidates shown to the model;
* ``FUNCTION_LINEAGE`` contains only two-pass unanimous, verified decisions.
"""
from __future__ import annotations

import copy
import itertools
import re
import statistics
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .ai import gateway as ai_gateway
from .ai import settings as ai_settings
from .production_artifacts import canonical_json, content_signature, stable_id, utc_now
from .sheet_matcher import normalize_sheet


ALGORITHM_VERSION = "function-lineage-matcher.v1.2-deterministic"
SCHEMA_VERSION = "function-lineage-shadow.v1"
RELATION_DOCUMENT_LINK = "DOCUMENT_LINK"
RELATION_FUNCTIONAL_ANALOGUE = "FUNCTIONAL_ANALOGUE"
RELATION_FUNCTION_LINEAGE = "FUNCTION_LINEAGE"
FRAGMENT_OWNED_EVIDENCE = "FRAGMENT_OWNED_EVIDENCE"
SHEET_SHARED_EVIDENCE = "SHEET_SHARED_EVIDENCE"
DIRECTION = "LEFT_TO_RIGHT"
NEED_MORE_EVIDENCE = "NEED_MORE_EVIDENCE"
FUNCTION_REMOVED = "FUNCTION_REMOVED"
CONCRETE_RELATIONS = frozenset({
    "CONTINUED_1_TO_1",
    "SPLIT_1_TO_N",
    "MERGED_N_TO_1",
    "FUNCTION_DISTRIBUTED",
    "RENAMED_FUNCTION",
    "FUNCTION_EXPANDED",
    "FUNCTION_REDUCED",
})
COMPLEX_RELATIONS = frozenset({
    "SPLIT_1_TO_N", "MERGED_N_TO_1", "FUNCTION_DISTRIBUTED",
})
PASSPORT_FIELDS = (
    "source_sheet",
    "function_class",
    "function_evidence",
    "serviced_object",
    "building",
    "corpus",
    "section",
    "zone",
    "floors",
    "consumers",
    "systems",
    "equipment_roles",
    "upstream",
    "downstream",
    "stable_entities",
    "cross_sheet_functional_references",
    "topology_role",
    "component_role",
    "document_role",
    "neighboring_function_context",
    "contradictions",
    "evidence_refs",
)
SHEET_SHARED_FIELDS = frozenset({
    "source_sheet",
    "serviced_object",
    "building",
    "corpus",
    "section",
    "zone",
    "floors",
    "systems",
    "consumers",
    "equipment_roles",
    "document_role",
    "stable_entities",
    "cross_sheet_functional_references",
    "neighboring_function_context",
})
MAX_FACTS_PER_FIELD = 12
MAX_CANDIDATES_PER_TASK = 12
PER_CHANNEL_CANDIDATE_LIMIT = 1
GROUP_CANDIDATE_LIMIT = 8
GROUP_SOURCE_POOL = 4

FUNCTIONAL_CHANNELS = (
    "FUNCTION_CLASS",
    "FUNCTION_EVIDENCE",
    "SERVICED_OBJECT",
    "CORPUS_ZONE",
    "FLOORS",
    "CONSUMERS",
    "UPSTREAM_DOWNSTREAM",
    "SYSTEMS",
    "EQUIPMENT_ROLES",
    "DOCUMENT_ROLE",
    "STABLE_ENTITIES",
    "CROSS_SHEET_REFERENCE",
    "NEIGHBORING_FUNCTIONS",
)
SUPPORTING_CHANNELS = ("DOCUMENT_CONTEXT", "TITLE", "PAGE_PROXIMITY")

_CHANNEL_WEIGHTS = {
    "FUNCTION_CLASS": 0.31,
    "FUNCTION_EVIDENCE": 0.06,
    "SERVICED_OBJECT": 0.11,
    "CORPUS_ZONE": 0.15,
    "FLOORS": 0.05,
    "CONSUMERS": 0.05,
    "UPSTREAM_DOWNSTREAM": 0.07,
    "SYSTEMS": 0.06,
    "EQUIPMENT_ROLES": 0.05,
    "DOCUMENT_ROLE": 0.02,
    "STABLE_ENTITIES": 0.04,
    "CROSS_SHEET_REFERENCE": 0.02,
    "NEIGHBORING_FUNCTIONS": 0.01,
    "DOCUMENT_CONTEXT": 0.015,
    "TITLE": 0.01,
    "PAGE_PROXIMITY": 0.005,
}

_TOKEN_RE = re.compile(r"[a-zа-я0-9]+(?:[-./][a-zа-я0-9]+)*", re.I)
_FUNCTION_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("ELECTRICAL_DISTRIBUTION", ("вру", "грщ", "распределительн", "однолинейн", "электроснабжен")),
    ("LOAD_CALCULATION", ("расчет нагруз", "расчёт нагруз", "расчетный ток", "calculation")),
    ("LIGHTING", ("освещен", "светильник", "lighting")),
    ("GROUNDING_LIGHTNING", ("заземлен", "молниезащит", "уравнивани")),
    ("WATER_DRAINAGE", ("водоотведен", "канализац", "водосток", "сточн", "дренаж")),
    ("HOT_WATER", ("горяч", "т3", "т4")),
    ("FIRE_WATER", ("пожар", "впв", "в2.1", "в2.2")),
    ("WATER_SUPPLY", ("водоснабжен", "водопровод", "холодн", "хвс")),
    ("RISER_DISTRIBUTION", ("стояк", "квартир", "riser")),
    ("PUMPING_PRESSURE", ("насос", "повышен", "напор", "booster")),
    ("DOMESTIC_PRESSURE_BOOST", ("насосная хвс", "хозяйственно-питьевого", "domestic booster")),
    ("FIRE_PRESSURE_BOOST", ("насосная впв", "пожаротушен", "fire booster")),
    ("INCOMING_METERING", ("водомерный узел", "общедомовой водомер", "ввод в1")),
    ("METERING", ("водомер", "счетчик", "счётчик", "узел учета", "узел учёта")),
)

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
_COMPOSITE_ROLES = frozenset({
    "DOMESTIC_PRESSURE_BOOST", "FIRE_PRESSURE_BOOST", "INCOMING_METERING",
})
_SCOPE_RE = re.compile(
    r"\b(corpus|building|корпус|section|секци(?:я|и)|zone|зона)\s*[№#]?\s*([0-9]+(?:[.,][0-9]+)?)",
    re.IGNORECASE,
)


@dataclass
class FunctionLineageDataset:
    pair_id: str
    sheet_passports: dict[str, dict[int, dict[str, Any]]]
    function_passports: dict[str, dict[str, dict[str, Any]]]
    function_fragments: dict[str, dict[str, dict[str, Any]]]
    evidence_catalog: dict[str, dict[str, Any]]
    document_link_map: dict[str, Any]
    candidates: dict[str, dict[str, Any]]
    tasks: list[dict[str, Any]]
    input_signature: str


def _clean(value: Any) -> str:
    return " ".join(str(value or "").casefold().replace("ё", "е").split())


def _unique(values: Iterable[Any], *, limit: int = MAX_FACTS_PER_FIELD) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = " ".join(str(raw or "").split())
        key = _clean(value)
        if value and key not in seen:
            output.append(value)
            seen.add(key)
        if len(output) >= limit:
            break
    return output


def _tokens(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        return set().union(*(_tokens(item) for item in value.values())) if value else set()
    if isinstance(value, (list, tuple, set)):
        return set().union(*(_tokens(item) for item in value)) if value else set()
    return {
        token.casefold().replace("ё", "е")
        for token in _TOKEN_RE.findall(str(value or ""))
        if len(token) > 1
    }


def _jaccard(left: Any, right: Any) -> float:
    left_tokens, right_tokens = _tokens(left), _tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _document_role(sheet: Mapping[str, Any]) -> str:
    text = _clean(" ".join([
        str(sheet.get("title") or ""),
        *[str(value) for value in sheet.get("sheet_types") or []],
    ]))
    if "содержан" in text or "contents" in text:
        return "TOC"
    if "изменен" in text or "change register" in text:
        return "CHANGE_REGISTER"
    if any(value in text for value in ("спецификац", "ведомост", "таблиц", "schedule")):
        return "TABLE"
    if (
        sheet.get("functional")
        or sheet.get("entities")
        or sheet.get("topology")
        or sheet.get("stamp_key")
    ):
        return "GRAPHIC_SHEET"
    return "OTHER"


def _function_classes(sheet: Mapping[str, Any]) -> list[str]:
    source = sheet.get("function_lineage_source")
    if isinstance(source, Mapping):
        classes = [
            str(value.get("function_class") or "")
            for value in source.get("functions") or []
            if isinstance(value, Mapping) and value.get("function_class")
        ]
        if classes:
            return list(dict.fromkeys(classes))
    text = _clean(" ".join([
        str(sheet.get("title") or ""),
        *[str(value) for value in sheet.get("functional") or []],
        *[str(value) for value in sheet.get("entities") or []],
        *[str(value) for value in sheet.get("topology") or []],
    ]))
    classes = [
        name for name, needles in _FUNCTION_RULES
        if any(needle in text for needle in needles)
    ]
    # A generic fragment is still useful for bounded uncertainty.  It does not
    # claim that a documentary page has an engineering function: those pages
    # were removed by ``document_role`` above.
    return list(dict.fromkeys(classes)) or ["GENERAL_DOCUMENT_FUNCTION"]


def _sheet_passport(record: Mapping[str, Any], *, side: str) -> dict[str, Any]:
    normalized = normalize_sheet(record, side=side)
    identity = normalized.get("identity")
    identity_dict = identity.to_dict() if identity is not None else {}
    source = record.get("function_lineage_source")
    source_dict = copy.deepcopy(dict(source)) if isinstance(source, Mapping) else None
    return {
        "side": side,
        "physical_page": int(normalized["page"]),
        "sheet_number": normalized.get("sheet_number"),
        "title": normalized.get("title"),
        "functional": list(normalized.get("functional") or [])[:MAX_FACTS_PER_FIELD],
        "entities": list(normalized.get("entities") or [])[:MAX_FACTS_PER_FIELD],
        "topology": list(normalized.get("topology") or [])[:MAX_FACTS_PER_FIELD],
        "sheet_types": list(normalized.get("sheet_types") or [])[:MAX_FACTS_PER_FIELD],
        "buildings": list(identity_dict.get("buildings") or []),
        "floors": list(identity_dict.get("floors") or []),
        "elevation": identity_dict.get("elevation"),
        "document_role": (
            str(source_dict.get("document_role"))
            if source_dict and source_dict.get("document_role")
            else _document_role(normalized)
        ),
        "graphic_sheet_number": (
            source_dict.get("graphic_sheet_number")
            if source_dict else normalized.get("sheet_number")
        ),
        "function_lineage_source": source_dict,
    }


def _field_evidence(
    *,
    pair_id: str,
    side: str,
    page: int,
    field: str,
    value: Any,
    function_id: str,
    fragment_id: str,
    source: str,
) -> tuple[str, dict[str, Any]]:
    provenance_type = (
        SHEET_SHARED_EVIDENCE
        if field in SHEET_SHARED_FIELDS
        else FRAGMENT_OWNED_EVIDENCE
    )
    owner_identity = (
        None
        if provenance_type == SHEET_SHARED_EVIDENCE
        else {"function_id": function_id, "fragment_id": fragment_id}
    )
    evidence_id = stable_id(
        "flev_",
        pair_id,
        side,
        page,
        field,
        value,
        provenance_type,
        owner_identity,
    )
    return evidence_id, {
        "evidence_id": evidence_id,
        "side": side,
        "physical_page": page,
        "field": field,
        "content_signature": content_signature(value),
        "source": source,
        "provenance_type": provenance_type,
        "owner_function_id": (
            function_id if provenance_type == FRAGMENT_OWNED_EVIDENCE else None
        ),
        "owner_fragment_id": (
            fragment_id if provenance_type == FRAGMENT_OWNED_EVIDENCE else None
        ),
    }


def _passport_values(sheet: Mapping[str, Any], function_class: str) -> dict[str, Any]:
    source = sheet.get("function_lineage_source")
    source_dict = dict(source) if isinstance(source, Mapping) else {}
    buildings = _unique([
        *(source_dict.get("building") or []),
        *(f"Корпус {value}" for value in sheet.get("buildings") or []),
    ])
    objects = _unique([*(source_dict.get("serviced_object") or []), *buildings])
    systems = _unique(source_dict.get("systems") or sheet.get("functional") or [])
    entities = _unique(source_dict.get("stable_entities") or sheet.get("entities") or [])
    sections = _unique(source_dict.get("section") or [])
    if not sections and not source_dict:
        sections = _unique(
            value for value in systems
            if any(marker in _clean(value) for marker in ("план", "схем", "разрез", "section"))
        )
    zones = _unique([
        *(source_dict.get("zone") or []),
        *(f"Отметка {sheet['elevation']}" for _ in [0] if sheet.get("elevation")),
    ])
    source_sheet = {
        "side": sheet["side"],
        "physical_page": int(sheet["physical_page"]),
        "graphic_sheet_number": (
            source_dict.get("graphic_sheet_number")
            or sheet.get("graphic_sheet_number")
            or sheet.get("sheet_number")
        ),
        "title": source_dict.get("title") or sheet.get("title"),
    }
    topology_role = _TOPOLOGY_ROLE.get(function_class, "GENERAL_FUNCTION")
    function_evidence = _unique(
        snippet
        for value in source_dict.get("functions") or []
        if isinstance(value, Mapping) and value.get("function_class") == function_class
        for snippet in value.get("fragment_text") or []
    )

    def known(values: Iterable[Any]) -> list[str] | None:
        result = _unique(values)
        return result or None

    return {
        "source_sheet": source_sheet,
        "function_class": function_class,
        "function_evidence": function_evidence or None,
        "serviced_object": objects or None,
        "building": buildings or None,
        "corpus": known(source_dict.get("corpus") or buildings),
        "section": sections or None,
        "zone": zones or None,
        "floors": known([*(source_dict.get("floors") or []), *(sheet.get("floors") or [])]),
        "systems": systems or None,
        "consumers": known(source_dict.get("consumers") or []),
        "equipment_roles": known(source_dict.get("equipment_roles") or entities),
        # Direction is not inferred from an unordered topology token list.
        "upstream": known(source_dict.get("upstream") or []),
        "downstream": known(source_dict.get("downstream") or []),
        "stable_entities": entities or None,
        "cross_sheet_functional_references": known(
            source_dict.get("cross_sheet_functional_references") or []
        ),
        "topology_role": topology_role,
        "component_role": topology_role,
        "document_role": source_dict.get("document_role") or sheet["document_role"],
        "neighboring_function_context": None,
        "contradictions": [],
        "evidence_refs": [],
    }


def _build_functions(
    pair_id: str,
    sheets: Mapping[str, Mapping[int, Mapping[str, Any]]],
) -> tuple[
    dict[str, dict[str, dict[str, Any]]],
    dict[str, dict[str, dict[str, Any]]],
    dict[str, dict[str, Any]],
]:
    passports: dict[str, dict[str, dict[str, Any]]] = {"LEFT": {}, "RIGHT": {}}
    fragments: dict[str, dict[str, dict[str, Any]]] = {"LEFT": {}, "RIGHT": {}}
    evidence: dict[str, dict[str, Any]] = {}
    page_function_ids: dict[tuple[str, int], list[str]] = {}
    for side in ("LEFT", "RIGHT"):
        for page, sheet in sorted(sheets[side].items()):
            if sheet.get("document_role") not in {"GRAPHIC_SHEET", "TABLE"}:
                continue
            source = sheet.get("function_lineage_source")
            source_dict = dict(source) if isinstance(source, Mapping) else {}
            source_functions = {
                str(value.get("function_class")): dict(value)
                for value in source_dict.get("functions") or []
                if isinstance(value, Mapping) and value.get("function_class")
            }
            for index, function_class in enumerate(_function_classes(sheet), 1):
                function_id = stable_id(
                    "func_", pair_id, side, page, function_class, index
                )
                fragment_id = stable_id("frag_", pair_id, side, page, function_id)
                values = _passport_values(sheet, function_class)
                provenance: dict[str, list[str]] = {}
                owned_refs: list[str] = []
                for field in PASSPORT_FIELDS:
                    field_value = values.get(field)
                    if field in {"evidence_refs", "neighboring_function_context", "contradictions"}:
                        provenance[field] = []
                        continue
                    if field_value in (None, "", []):
                        provenance[field] = []
                        continue
                    evidence_id, item = _field_evidence(
                        pair_id=pair_id,
                        side=side,
                        page=page,
                        field=field,
                        value=field_value,
                        function_id=function_id,
                        fragment_id=fragment_id,
                        source=(
                            "DETERMINISTIC_MARKDOWN_FACTS"
                            if source_dict else "COMPACT_SHEET_PASSPORT"
                        ),
                    )
                    evidence[evidence_id] = item
                    provenance[field] = [evidence_id]
                    owned_refs.append(evidence_id)
                values["evidence_refs"] = sorted(set(owned_refs))
                provenance["evidence_refs"] = list(values["evidence_refs"])
                passport = {
                    "function_id": function_id,
                    "pair_id": pair_id,
                    "side": side,
                    **{field: copy.deepcopy(values[field]) for field in PASSPORT_FIELDS},
                    "function_fragment_ids": [fragment_id],
                    "provenance": provenance,
                }
                fragment = {
                    "fragment_id": fragment_id,
                    "function_id": function_id,
                    "pair_id": pair_id,
                    "side": side,
                    "physical_page": page,
                    "function_class": function_class,
                    "component_role": function_class,
                    "document_role": sheet["document_role"],
                    "evidence_refs": list(values["evidence_refs"]),
                    "evidence_snippets": [
                        str(value)[:480]
                        for value in (
                            source_functions.get(function_class, {}).get("fragment_text") or []
                        )[:8]
                    ],
                    "capacity_key": (
                        f"RIGHT:{page}:{fragment_id}" if side == "RIGHT" else None
                    ),
                }
                passports[side][function_id] = passport
                fragments[side][fragment_id] = fragment
                page_function_ids.setdefault((side, page), []).append(function_id)
    for (side, _page), function_ids in page_function_ids.items():
        for function_id in function_ids:
            neighbors = sorted(value for value in function_ids if value != function_id)
            passport = passports[side][function_id]
            passport["neighboring_function_context"] = neighbors
            if neighbors:
                fragment_id = str(passport["function_fragment_ids"][0])
                evidence_id, item = _field_evidence(
                    pair_id=pair_id,
                    side=side,
                    page=int(passport["source_sheet"]["physical_page"]),
                    field="neighboring_function_context",
                    value=neighbors,
                    function_id=function_id,
                    fragment_id=fragment_id,
                    source="DETERMINISTIC_FUNCTION_STRUCTURE",
                )
                evidence[evidence_id] = item
                passport["evidence_refs"] = sorted({
                    *passport["evidence_refs"], evidence_id,
                })
                passport["provenance"]["evidence_refs"] = list(
                    passport["evidence_refs"]
                )
                passport["provenance"]["neighboring_function_context"] = [
                    evidence_id
                ]
                fragments[side][fragment_id]["evidence_refs"] = list(
                    passport["evidence_refs"]
                )
            else:
                passport["provenance"]["neighboring_function_context"] = []
    return passports, fragments, evidence


def _build_document_link_map(pair_id: str, sheet_relations: Mapping[str, Any]) -> dict[str, Any]:
    links: list[dict[str, Any]] = []
    evidence_catalog: dict[str, dict[str, Any]] = {}
    for relation in sheet_relations.get("relations") or []:
        if not isinstance(relation, Mapping):
            continue
        left_pages = sorted({int(value) for value in relation.get("left_pages") or []})
        right_pages = sorted({int(value) for value in relation.get("right_pages") or []})
        if not left_pages or not right_pages:
            continue
        evidence_ref = stable_id(
            "dlev_", pair_id, relation.get("relation_id"), left_pages, right_pages
        )
        evidence_catalog[evidence_ref] = {
            "evidence_id": evidence_ref,
            "left_pages": left_pages,
            "right_pages": right_pages,
            "source": "PRODUCTION_SHEET_MATCHER",
            "source_relation_id": relation.get("relation_id"),
        }
        links.append({
            "document_link_id": stable_id(
                "dlink_", pair_id, relation.get("relation_id"), left_pages, right_pages
            ),
            "pair_id": pair_id,
            "relation_namespace": RELATION_DOCUMENT_LINK,
            "direction": DIRECTION,
            "relation_type": relation.get("relation_type"),
            "status": relation.get("status"),
            "left_pages": left_pages,
            "right_pages": right_pages,
            "source_relation_id": relation.get("relation_id"),
            "source": relation.get("primary_source") or "PRODUCTION_SHEET_MATCHER",
            "evidence_refs": [evidence_ref],
            "functional_score_contribution": 0,
        })
    links.sort(key=lambda value: (value["left_pages"], value["right_pages"], value["document_link_id"]))
    return {
        "kind": "document_link_map",
        "schema_version": "document-link-map.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "pair_id": pair_id,
        "relation_namespace": RELATION_DOCUMENT_LINK,
        "direction": DIRECTION,
        "links": links,
        "evidence_catalog": evidence_catalog,
    }


def _fragments_by_page(
    fragments: Mapping[str, Mapping[str, Mapping[str, Any]]], side: str,
) -> dict[int, list[dict[str, Any]]]:
    result: dict[int, list[dict[str, Any]]] = {}
    for value in fragments[side].values():
        result.setdefault(int(value["physical_page"]), []).append(dict(value))
    for values in result.values():
        values.sort(key=lambda item: (item["function_class"], item["fragment_id"]))
    return result


def _edge_candidates(sheet_relations: Mapping[str, Any]) -> dict[tuple[int, int], dict[str, Any]]:
    """Return optional Sheet Matcher context; never a retrieval allowlist."""
    result: dict[tuple[int, int], dict[str, Any]] = {}
    for search in sheet_relations.get("candidate_search") or []:
        if not isinstance(search, Mapping):
            continue
        left_page = int(search.get("left_page") or 0)
        if left_page < 1:
            continue
        for edge in search.get("deep_candidates") or []:
            if not isinstance(edge, Mapping) or str(edge.get("status") or "") == "NO_MATCH":
                continue
            right_page = int(edge.get("right_page") or 0)
            if right_page > 0:
                result[(left_page, right_page)] = dict(edge)
    for relation in sheet_relations.get("relations") or []:
        if not isinstance(relation, Mapping):
            continue
        for left_page in relation.get("left_pages") or []:
            for right_page in relation.get("right_pages") or []:
                pair = int(left_page), int(right_page)
                if pair not in result:
                    result[pair] = {
                        "left_page": pair[0], "right_page": pair[1],
                        "status": relation.get("status"),
                        "score": relation.get("confidence"),
                        "signals": {},
                        "source_relation_id": relation.get("relation_id"),
                    }
    return result


def _scope_ids(passport: Mapping[str, Any], *fields: str) -> set[str]:
    aliases = {
        "corpus": "CORPUS", "building": "CORPUS", "корпус": "CORPUS",
        "section": "SECTION", "секция": "SECTION", "секции": "SECTION",
        "zone": "ZONE", "зона": "ZONE",
    }
    return {
        f"{aliases.get(kind.casefold(), kind.casefold())}:{number.replace(',', '.')}"
        for field in fields
        for value in (
            passport.get(field)
            if isinstance(passport.get(field), list)
            else [passport.get(field)]
        )
        if value
        for kind, number in _SCOPE_RE.findall(str(value))
    }


def _explicit_scope_conflicts(
    left: Mapping[str, Any], right: Mapping[str, Any],
) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    for kind, scope_prefix, fields in (
        ("INCOMPATIBLE_CORPUS", "CORPUS:", ("serviced_object", "building", "corpus")),
        ("INCOMPATIBLE_SECTION", "SECTION:", ("serviced_object", "section")),
        ("INCOMPATIBLE_ZONE", "ZONE:", ("zone",)),
    ):
        left_ids = {
            value for value in _scope_ids(left, *fields)
            if value.startswith(scope_prefix)
        }
        right_ids = {
            value for value in _scope_ids(right, *fields)
            if value.startswith(scope_prefix)
        }
        if left_ids and right_ids and left_ids.isdisjoint(right_ids):
            conflicts.append({"kind": kind, "left": sorted(left_ids), "right": sorted(right_ids)})
    return conflicts


def _class_compatibility(left: str, right: str) -> float:
    if left == right:
        return 1.0
    if "GENERAL_DOCUMENT_FUNCTION" in {left, right}:
        return 0.2
    hierarchy = {
        "PUMPING_PRESSURE": {"DOMESTIC_PRESSURE_BOOST", "FIRE_PRESSURE_BOOST"},
        "METERING": {"INCOMING_METERING"},
        "WATER_SUPPLY": {
            "HOT_WATER", "FIRE_WATER", "RISER_DISTRIBUTION", "PUMPING_PRESSURE",
            "DOMESTIC_PRESSURE_BOOST", "FIRE_PRESSURE_BOOST", "INCOMING_METERING",
        },
    }
    for parent, children in hierarchy.items():
        if {left, right} <= {parent, *children} and (left == parent or right == parent):
            return 0.65
    return 0.0


def _overlap(left: Any, right: Any) -> float | None:
    if not _tokens(left) or not _tokens(right):
        return None
    return _jaccard(left, right)


def _numeric_score(value: Any) -> float | None:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(1.0, score))


def _neighbor_classes(
    passport: Mapping[str, Any], passports: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    return [
        str(passports[function_id].get("function_class"))
        for function_id in passport.get("neighboring_function_context") or []
        if function_id in passports
    ]


def _channel_scores(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    *,
    passports: Mapping[str, Mapping[str, Any]],
    edge: Mapping[str, Any] | None,
) -> dict[str, float | None]:
    left_sheet = left.get("source_sheet") or {}
    right_sheet = right.get("source_sheet") or {}
    scores: dict[str, float | None] = {
        "FUNCTION_CLASS": _class_compatibility(
            str(left.get("function_class")), str(right.get("function_class"))
        ),
        "FUNCTION_EVIDENCE": _overlap(
            left.get("function_evidence"), right.get("function_evidence")
        ),
        "SERVICED_OBJECT": _overlap(left.get("serviced_object"), right.get("serviced_object")),
        "CORPUS_ZONE": _overlap(
            [left.get("building"), left.get("corpus"), left.get("section"), left.get("zone")],
            [right.get("building"), right.get("corpus"), right.get("section"), right.get("zone")],
        ),
        "FLOORS": _overlap(left.get("floors"), right.get("floors")),
        "CONSUMERS": _overlap(left.get("consumers"), right.get("consumers")),
        "UPSTREAM_DOWNSTREAM": _overlap(
            [left.get("upstream"), left.get("downstream")],
            [right.get("upstream"), right.get("downstream")],
        ),
        "SYSTEMS": _overlap(left.get("systems"), right.get("systems")),
        "EQUIPMENT_ROLES": _overlap(left.get("equipment_roles"), right.get("equipment_roles")),
        "DOCUMENT_ROLE": (
            1.0
            if left.get("document_role") and left.get("document_role") == right.get("document_role")
            else (0.0 if left.get("document_role") and right.get("document_role") else None)
        ),
        "STABLE_ENTITIES": _overlap(left.get("stable_entities"), right.get("stable_entities")),
        "CROSS_SHEET_REFERENCE": _overlap(
            left.get("cross_sheet_functional_references"),
            right.get("cross_sheet_functional_references"),
        ),
        "NEIGHBORING_FUNCTIONS": _overlap(
            _neighbor_classes(left, passports), _neighbor_classes(right, passports)
        ),
        "DOCUMENT_CONTEXT": _numeric_score((edge or {}).get("score")),
        "TITLE": _overlap(left_sheet.get("title"), right_sheet.get("title")),
        "PAGE_PROXIMITY": 1.0 / (
            1.0
            + abs(
                int(left_sheet.get("physical_page") or 0)
                - int(right_sheet.get("physical_page") or 0)
            )
        ),
    }
    return {
        key: (round(float(value), 6) if value is not None else None)
        for key, value in scores.items()
    }


def _ranking_score(scores: Mapping[str, float | None]) -> float:
    return round(sum(
        float(scores[channel]) * weight
        for channel, weight in _CHANNEL_WEIGHTS.items()
        if scores.get(channel) is not None
    ), 8)


def _series_key(passport: Mapping[str, Any]) -> str:
    title = _clean((passport.get("source_sheet") or {}).get("title"))
    title = re.sub(
        r"\b(?:часть|лист|начало|продолжение|окончание)\s*[№#]?\s*\d*\b",
        " ",
        title,
    )
    return " ".join(sorted(_tokens(title)))


def _lineage_identity_tokens(passport: Mapping[str, Any]) -> set[str]:
    """Return title identifiers without treating generic title words as truth."""
    ignored = {
        "и", "в", "на", "для", "часть", "лист", "начало", "продолжение",
        "окончание", "однолинейная", "расчетная", "расчетный", "схема",
        "системы", "система", "план", "планы", "внутренние", "внутреннее",
    }
    return {
        token for token in _tokens((passport.get("source_sheet") or {}).get("title"))
        if token not in ignored and not token.isdigit()
    }


def _lineage_identity_score(
    left: Mapping[str, Any], right_values: Sequence[Mapping[str, Any]],
) -> float:
    left_tokens = _lineage_identity_tokens(left)
    if not left_tokens:
        return 0.0
    return max(
        (
            len(left_tokens & _lineage_identity_tokens(right)) / len(
                left_tokens | _lineage_identity_tokens(right)
            )
            for right in right_values
            if _lineage_identity_tokens(right)
        ),
        default=0.0,
    )


def _sheet_number_family(value: Any) -> str | None:
    match = re.fullmatch(r"(\d+)[.]\d+", _clean(value))
    return match.group(1) if match else None


def _related_sequence(passports: Sequence[Mapping[str, Any]]) -> bool:
    if not passports:
        return False
    series = [_series_key(value) for value in passports]
    if series[0] and len(set(series)) == 1:
        return True
    families = [
        _sheet_number_family((value.get("source_sheet") or {}).get("graphic_sheet_number"))
        for value in passports
    ]
    if families[0] and len(set(families)) == 1:
        return True
    entity_sets = [
        _tokens([value.get("stable_entities"), value.get("systems")])
        for value in passports
    ]
    if entity_sets and set.intersection(*entity_sets):
        return True
    return any(value.get("cross_sheet_functional_references") for value in passports)


def _make_candidate(
    *,
    pair_id: str,
    relation_type: str,
    components: Sequence[Mapping[str, Any]],
    source: Mapping[str, Any],
    passports: Mapping[str, Any],
) -> dict[str, Any]:
    component_map = [dict(value) for value in components]
    left_pages = sorted({int(value["left_physical_page"]) for value in component_map})
    right_pages = sorted({int(value["right_physical_page"]) for value in component_map})
    left_fragment_ids = sorted({str(value["left_fragment_id"]) for value in component_map})
    right_fragment_ids = sorted({str(value["right_fragment_id"]) for value in component_map})
    left_function_ids = sorted({str(value["left_function_id"]) for value in component_map})
    right_function_ids = sorted({str(value["right_function_id"]) for value in component_map})
    evidence_refs = sorted({
        str(ref)
        for function_id in [*left_function_ids, *right_function_ids]
        for ref in passports[function_id].get("evidence_refs") or []
    })
    identity = {
        "relation_type": relation_type,
        "left_fragment_ids": left_fragment_ids,
        "right_fragment_ids": right_fragment_ids,
    }
    channel_scores = (
        dict(source.get("channel_scores"))
        if isinstance(source.get("channel_scores"), Mapping) else {}
    )
    return {
        "candidate_id": stable_id("lcand_", pair_id, identity),
        "pair_id": pair_id,
        "relation_namespace": RELATION_FUNCTIONAL_ANALOGUE,
        "relation_type": relation_type,
        "direction": DIRECTION,
        "left_pages": left_pages,
        "right_pages": right_pages,
        "left_function_ids": left_function_ids,
        "right_function_ids": right_function_ids,
        "left_fragment_ids": left_fragment_ids,
        "right_fragment_ids": right_fragment_ids,
        "right_capacity_keys": sorted({
            str(value["capacity_key"]) for value in component_map
        }),
        "component_map": component_map,
        "evidence_refs": evidence_refs,
        "retrieval_channels": [
            channel for channel in FUNCTIONAL_CHANNELS
            if channel_scores.get(channel) not in (None, 0, 0.0, False, "")
        ],
        "supporting_channels": [
            channel for channel in SUPPORTING_CHANNELS
            if channel_scores.get(channel) not in (None, 0, 0.0, False, "")
        ],
        "channel_scores": channel_scores,
        "functional_score": source.get("functional_score"),
        "document_context": {
            "supporting_only": True,
            "included_in_functional_score": False,
            "source_relation_id": source.get("source_relation_id"),
            "sheet_matcher_edge_present": bool(source.get("sheet_matcher_edge_present")),
        },
        "source_kind": source.get("source_kind") or "FULL_RIGHT_CORPUS",
        "source_score": source.get("ranking_score"),
        "group_evidence": dict(source.get("group_evidence") or {}),
        "explicit_contradictions": list(source.get("explicit_contradictions") or []),
    }


def _build_candidates(
    pair_id: str,
    sheet_relations: Mapping[str, Any],
    passports: Mapping[str, Mapping[str, Mapping[str, Any]]],
    fragments: Mapping[str, Mapping[str, Mapping[str, Any]]],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    by_page = {
        "LEFT": _fragments_by_page(fragments, "LEFT"),
        "RIGHT": _fragments_by_page(fragments, "RIGHT"),
    }
    passport_lookup = {**passports["LEFT"], **passports["RIGHT"]}
    edges = _edge_candidates(sheet_relations)
    candidates: dict[str, dict[str, Any]] = {}
    single_options: dict[str, list[str]] = {fragment_id: [] for fragment_id in fragments["LEFT"]}
    group_options: dict[str, list[str]] = {fragment_id: [] for fragment_id in fragments["LEFT"]}
    full_single_rows: dict[str, list[dict[str, Any]]] = {}

    def component(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "component_role": left["component_role"],
            "left_function_id": left["function_id"],
            "left_fragment_id": left["fragment_id"],
            "left_physical_page": left["physical_page"],
            "right_function_id": right["function_id"],
            "right_fragment_id": right["fragment_id"],
            "right_physical_page": right["physical_page"],
            "capacity_key": right["capacity_key"],
        }

    # Every LEFT fragment searches the complete RIGHT function corpus.  Sheet
    # Matcher edges can add a small supporting score, but cannot admit or
    # exclude a candidate.
    all_right = [
        value for page in sorted(by_page["RIGHT"]) for value in by_page["RIGHT"][page]
    ]
    for left_page, left_fragments in sorted(by_page["LEFT"].items()):
        for left in left_fragments:
            left_passport = passport_lookup[str(left["function_id"])]
            rows: list[dict[str, Any]] = []
            for right in all_right:
                right_passport = passport_lookup[str(right["function_id"])]
                class_score = _class_compatibility(
                    str(left["function_class"]), str(right["function_class"])
                )
                if class_score <= 0:
                    continue
                contradictions = _explicit_scope_conflicts(left_passport, right_passport)
                if contradictions:
                    continue
                edge = edges.get((left_page, int(right["physical_page"])))
                scores = _channel_scores(
                    left_passport, right_passport, passports=passport_lookup, edge=edge
                )
                ranking_score = _ranking_score(scores)
                functional_score = round(sum(
                    float(scores[channel]) * _CHANNEL_WEIGHTS[channel]
                    for channel in FUNCTIONAL_CHANNELS
                    if scores.get(channel) is not None
                ), 8)
                candidate = _make_candidate(
                    pair_id=pair_id,
                    relation_type="CONTINUED_1_TO_1",
                    components=[component(left, right)],
                    source={
                        "channel_scores": scores,
                        "functional_score": functional_score,
                        "ranking_score": ranking_score,
                        "source_relation_id": (edge or {}).get("source_relation_id"),
                        "sheet_matcher_edge_present": edge is not None,
                        "source_kind": "FULL_RIGHT_FUNCTION_CORPUS",
                        "explicit_contradictions": contradictions,
                    },
                    passports=passport_lookup,
                )
                rows.append(candidate)
            rows.sort(key=lambda value: (
                -float(value.get("source_score") or 0.0),
                int(value["right_pages"][0]),
                value["right_fragment_ids"],
            ))
            full_single_rows[str(left["fragment_id"])] = rows
            # Preserve global leaders plus independent functional-channel
            # leaders.  Supporting title/page/document signals never receive
            # their own admission quota.
            selected: list[dict[str, Any]] = list(rows[:4])
            selected_ids = {value["candidate_id"] for value in selected}
            for channel in FUNCTIONAL_CHANNELS:
                channel_rows = sorted(
                    (
                        value for value in rows
                        if (value.get("channel_scores") or {}).get(channel) not in (None, 0, 0.0)
                    ),
                    key=lambda value: (
                        -float(value["channel_scores"][channel]),
                        -float(value.get("source_score") or 0.0),
                        int(value["right_pages"][0]),
                    ),
                )
                for value in channel_rows[:PER_CHANNEL_CANDIDATE_LIMIT]:
                    if value["candidate_id"] not in selected_ids:
                        selected.append(value)
                        selected_ids.add(value["candidate_id"])
                    if len(selected) >= MAX_CANDIDATES_PER_TASK:
                        break
                if len(selected) >= MAX_CANDIDATES_PER_TASK:
                    break
            for value in rows:
                if len(selected) >= MAX_CANDIDATES_PER_TASK:
                    break
                if value["candidate_id"] not in selected_ids:
                    selected.append(value)
                    selected_ids.add(value["candidate_id"])
            ordered = selected
            ordered.sort(key=lambda value: (
                -float(value.get("source_score") or 0.0),
                int(value["right_pages"][0]),
                value["candidate_id"],
            ))
            for rank, candidate in enumerate(ordered[:MAX_CANDIDATES_PER_TASK], 1):
                candidate["single_rank"] = rank
                candidates[candidate["candidate_id"]] = candidate
                single_options[str(left["fragment_id"])].append(candidate["candidate_id"])

    def aggregate_scores(values: Sequence[Mapping[str, Any]]) -> dict[str, float | None]:
        output: dict[str, float | None] = {}
        for channel in (*FUNCTIONAL_CHANNELS, *SUPPORTING_CHANNELS):
            available = [
                float(value["channel_scores"][channel])
                for value in values
                if (value.get("channel_scores") or {}).get(channel) is not None
            ]
            output[channel] = round(statistics.fmean(available), 6) if available else None
        return output

    def add_group(
        relation_type: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        source_kind: str,
        bonus: float = 0.0,
        group_evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        components = [
            dict(component_value)
            for row in rows for component_value in row.get("component_map") or []
        ]
        scores = aggregate_scores(rows)
        ranking_score = round(statistics.fmean(
            float(row.get("source_score") or 0.0) for row in rows
        ) + bonus, 8)
        candidate = _make_candidate(
            pair_id=pair_id,
            relation_type=relation_type,
            components=components,
            source={
                "channel_scores": scores,
                "functional_score": round(sum(
                    float(scores[channel]) * _CHANNEL_WEIGHTS[channel]
                    for channel in FUNCTIONAL_CHANNELS
                    if scores.get(channel) is not None
                ), 8),
                "ranking_score": ranking_score,
                "source_kind": source_kind,
                "group_evidence": dict(group_evidence or {}),
                "sheet_matcher_edge_present": any(
                    (row.get("document_context") or {}).get("sheet_matcher_edge_present")
                    for row in rows
                ),
            },
            passports=passport_lookup,
        )
        candidates[candidate["candidate_id"]] = candidate
        for fragment_id in candidate["left_fragment_ids"]:
            if candidate["candidate_id"] not in group_options[fragment_id]:
                group_options[fragment_id].append(candidate["candidate_id"])
        return candidate

    # Generic bounded set cover for composite engineering functions.  The
    # roles are ontology classes, not project/page special cases.
    for _left_page, left_values in sorted(by_page["LEFT"].items()):
        role_values = [
            value for value in left_values if value["function_class"] in _COMPOSITE_ROLES
        ]
        if len(role_values) < 2:
            continue
        choices: list[list[dict[str, Any]]] = []
        for left in sorted(role_values, key=lambda value: value["function_class"]):
            exact = [
                row for row in full_single_rows.get(str(left["fragment_id"]), [])
                if passport_lookup[str(row["right_function_ids"][0])]["function_class"]
                == left["function_class"]
            ][:GROUP_SOURCE_POOL]
            if not exact:
                choices = []
                break
            choices.append(exact)
        generated: list[dict[str, Any]] = []
        for rows in itertools.product(*choices) if choices else []:
            capacity = {
                key for row in rows for key in row.get("right_capacity_keys") or []
            }
            right_pages = {
                page for row in rows for page in row.get("right_pages") or []
            }
            if len(capacity) != len(rows) or len(right_pages) < 2:
                continue
            generated.append(add_group(
                "FUNCTION_DISTRIBUTED",
                rows,
                source_kind="BOUNDED_FUNCTION_SET_COVER",
                bonus=0.04 * len(right_pages),
                group_evidence={"complete_composite_role_cover": True},
            ))
        generated.sort(key=lambda value: (
            -float(value.get("source_score") or 0.0), value["right_pages"], value["candidate_id"]
        ))
        for value in generated[GROUP_CANDIDATE_LIMIT:]:
            for fragment_id in value["left_fragment_ids"]:
                group_options[fragment_id] = [
                    item for item in group_options[fragment_id] if item != value["candidate_id"]
                ]
            candidates.pop(value["candidate_id"], None)

    # One LEFT fragment can continue over an adjacent RIGHT sheet series even
    # when no Sheet Matcher edge exists for one or more members.
    for left_fragment_id, rows in full_single_rows.items():
        # The pool remains bounded, but is wider than the final task shortlist:
        # sparse continuation/table pages may be weak individually while their
        # explicit sheet family is strong group evidence.
        left_passport = passport_lookup[
            str(fragments["LEFT"][left_fragment_id]["function_id"])
        ]
        sequence_pools: list[tuple[str, list[dict[str, Any]]]] = [
            ("TOP_SCORE", rows[:18])
        ]
        structural_pools: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            right_passport = passport_lookup[str(row["right_function_ids"][0])]
            family = _sheet_number_family(
                (right_passport.get("source_sheet") or {}).get("graphic_sheet_number")
            )
            series = _series_key(right_passport)
            if family:
                structural_pools.setdefault(f"SHEET_FAMILY:{family}", []).append(row)
            if series:
                structural_pools.setdefault(f"TITLE_SERIES:{series}", []).append(row)
        for key, pool in sorted(structural_pools.items()):
            if len({int(row["right_pages"][0]) for row in pool}) >= 2:
                sequence_pools.append((key, pool[:18]))
        generated: list[dict[str, Any]] = []
        generated_ids: set[str] = set()
        for pool_kind, pool in sequence_pools:
            for size in (2, 3):
                for members in itertools.combinations(pool, size):
                    pages = sorted(int(row["right_pages"][0]) for row in members)
                    if len(set(pages)) != len(pages):
                        continue
                    if any(right != left + 1 for left, right in zip(pages, pages[1:])):
                        continue
                    right_passports = [
                        passport_lookup[str(row["right_function_ids"][0])]
                        for row in members
                    ]
                    if not _related_sequence(right_passports):
                        continue
                    series = [_series_key(value) for value in right_passports]
                    families = [
                        _sheet_number_family(
                            (value.get("source_sheet") or {}).get("graphic_sheet_number")
                        )
                        for value in right_passports
                    ]
                    structural_bonus = (
                        0.12 if families[0] and len(set(families)) == 1 else 0.0
                    ) + (
                        0.08 if series[0] and len(set(series)) == 1 else 0.0
                    )
                    identity_score = _lineage_identity_score(
                        left_passport, right_passports
                    )
                    candidate = add_group(
                        "SPLIT_1_TO_N", members,
                        source_kind="CROSS_SHEET_FUNCTION_SEQUENCE",
                        bonus=(
                            0.03 * size + structural_bonus + 0.08 * identity_score
                        ),
                        group_evidence={
                            "pool_kind": pool_kind,
                            "same_sheet_number_family": bool(
                                families[0] and len(set(families)) == 1
                            ),
                            "same_title_series": bool(
                                series[0] and len(set(series)) == 1
                            ),
                            "title_identity_score_supporting_only": round(
                                identity_score, 6
                            ),
                        },
                    )
                    if candidate["candidate_id"] not in generated_ids:
                        generated.append(candidate)
                        generated_ids.add(candidate["candidate_id"])
        generated.sort(key=lambda value: (
            -float(value.get("source_score") or 0.0), value["right_pages"], value["candidate_id"]
        ))
        for value in generated[GROUP_CANDIDATE_LIMIT:]:
            group_options[left_fragment_id] = [
                item for item in group_options[left_fragment_id] if item != value["candidate_id"]
            ]
            candidates.pop(value["candidate_id"], None)

    # Adjacent LEFT fragments independently retaining the same exact RIGHT
    # fragment form an N -> 1 candidate.  No page-global exclusivity is used.
    left_fragments_ordered = sorted(
        fragments["LEFT"].values(),
        key=lambda value: (int(value["physical_page"]), value["function_class"], value["fragment_id"]),
    )
    for left_a, left_b in itertools.combinations(left_fragments_ordered, 2):
        if int(left_b["physical_page"]) != int(left_a["physical_page"]) + 1:
            continue
        if left_a["function_class"] != left_b["function_class"]:
            continue
        left_passports = [
            passport_lookup[str(left_a["function_id"])],
            passport_lookup[str(left_b["function_id"])],
        ]
        if not _related_sequence(left_passports):
            continue
        rows_a = {
            row["right_fragment_ids"][0]: row
            for row in full_single_rows.get(str(left_a["fragment_id"]), [])[:MAX_CANDIDATES_PER_TASK]
        }
        rows_b = {
            row["right_fragment_ids"][0]: row
            for row in full_single_rows.get(str(left_b["fragment_id"]), [])[:MAX_CANDIDATES_PER_TASK]
        }
        shared = sorted(
            set(rows_a) & set(rows_b),
            key=lambda fragment_id: -statistics.fmean([
                float(rows_a[fragment_id].get("source_score") or 0.0),
                float(rows_b[fragment_id].get("source_score") or 0.0),
            ]),
        )
        for right_fragment_id in shared[:GROUP_CANDIDATE_LIMIT]:
            right_passport = passport_lookup[
                str(rows_a[right_fragment_id]["right_function_ids"][0])
            ]
            identity_score = max(
                _lineage_identity_score(left_passport, [right_passport])
                for left_passport in left_passports
            )
            lineage_bonus = 0.05 + 0.10 * identity_score
            add_group(
                "MERGED_N_TO_1",
                [rows_a[right_fragment_id], rows_b[right_fragment_id]],
                source_kind="SHARED_RIGHT_FUNCTION_FRAGMENT",
                bonus=lineage_bonus,
                group_evidence={
                    "same_left_series": _series_key(left_passports[0])
                    == _series_key(left_passports[1]),
                    "title_identity_score_supporting_only": round(identity_score, 6),
                },
            )

    tasks: list[dict[str, Any]] = []
    for fragment_id, fragment in sorted(
        fragments["LEFT"].items(),
        key=lambda item: (item[1]["physical_page"], item[1]["function_class"], item[0]),
    ):
        group_limit = 4
        all_group_ids = sorted(
            group_options.get(fragment_id) or [],
            key=lambda candidate_id: (
                -float(candidates[candidate_id].get("source_score") or 0.0),
                candidates[candidate_id]["right_pages"],
                candidate_id,
            ),
        )
        reserved_group_ids = []
        for relation_type in (
            "FUNCTION_DISTRIBUTED", "MERGED_N_TO_1", "SPLIT_1_TO_N",
        ):
            match = next((
                candidate_id for candidate_id in all_group_ids
                if candidates[candidate_id]["relation_type"] == relation_type
            ), None)
            if match is not None:
                reserved_group_ids.append(match)
        group_ids = list(dict.fromkeys([
            *reserved_group_ids, *all_group_ids,
        ]))[:group_limit]
        single_ids = single_options.get(fragment_id) or []
        single_limit = MAX_CANDIDATES_PER_TASK - len(group_ids)
        candidate_ids = list(dict.fromkeys([
            *group_ids, *single_ids[:single_limit],
        ]))
        candidate_ids.sort(key=lambda candidate_id: (
            -float(candidates[candidate_id].get("source_score") or 0.0),
            0 if candidates[candidate_id]["relation_type"] in COMPLEX_RELATIONS else 1,
            candidates[candidate_id]["right_pages"],
            candidate_id,
        ))
        candidate_ids = candidate_ids[:MAX_CANDIDATES_PER_TASK]
        task_id = stable_id("ltask_", pair_id, fragment_id)
        tasks.append({
            "task_id": task_id,
            "pair_id": pair_id,
            "left_physical_page": fragment["physical_page"],
            "left_function_id": fragment["function_id"],
            "left_fragment_id": fragment_id,
            "candidate_ids": candidate_ids,
            "candidate_ranks": {
                candidate_id: rank for rank, candidate_id in enumerate(candidate_ids, 1)
            },
            "allowed_outputs": [*candidate_ids, FUNCTION_REMOVED, NEED_MORE_EVIDENCE],
        })
    exposed = {candidate_id for task in tasks for candidate_id in task["candidate_ids"]}
    return {
        candidate_id: candidate for candidate_id, candidate in candidates.items()
        if candidate_id in exposed
    }, tasks


def build_dataset(
    *,
    pair_id: str,
    sheet_indexes: Mapping[str, Sequence[Mapping[str, Any]]],
    sheet_relations: Mapping[str, Any],
) -> FunctionLineageDataset:
    sheets: dict[str, dict[int, dict[str, Any]]] = {"LEFT": {}, "RIGHT": {}}
    for side, key in (("LEFT", "left"), ("RIGHT", "right")):
        for record in sheet_indexes.get(key) or []:
            passport = _sheet_passport(record, side=side)
            sheets[side][passport["physical_page"]] = passport
    passports, fragments, evidence = _build_functions(pair_id, sheets)
    document_link_map = _build_document_link_map(pair_id, sheet_relations)
    candidates, tasks = _build_candidates(
        pair_id, sheet_relations, passports, fragments
    )
    signature = content_signature({
        "algorithm_version": ALGORITHM_VERSION,
        "pair_id": pair_id,
        "sheet_passports": sheets,
        "function_passports": passports,
        "function_fragments": fragments,
        "document_link_map": document_link_map,
        "candidates": candidates,
        "tasks": tasks,
        "sheet_relations_signature": sheet_relations.get("input_signature"),
    })
    document_link_map["input_signature"] = signature
    return FunctionLineageDataset(
        pair_id=pair_id,
        sheet_passports=sheets,
        function_passports=passports,
        function_fragments=fragments,
        evidence_catalog=evidence,
        document_link_map=document_link_map,
        candidates=candidates,
        tasks=tasks,
        input_signature=signature,
    )


def deterministic_candidate_artifact(
    dataset: FunctionLineageDataset, *, run_id: str | None = None,
) -> dict[str, Any]:
    """Serialize the pre-selector state without making or implying model calls."""
    counts = [len(task.get("candidate_ids") or []) for task in dataset.tasks]
    ordered = sorted(counts)
    p95 = (
        float(ordered[max(0, (95 * len(ordered) + 99) // 100 - 1)])
        if ordered else 0.0
    )
    composite_pages = {
        int(passport["source_sheet"]["physical_page"])
        for passport in dataset.function_passports["LEFT"].values()
        if passport.get("function_class") in _COMPOSITE_ROLES
    }
    eligible_pages = {
        page for page in composite_pages
        if sum(
            passport.get("function_class") in _COMPOSITE_ROLES
            and int(passport["source_sheet"]["physical_page"]) == page
            for passport in dataset.function_passports["LEFT"].values()
        ) >= 2
    }
    distributed_pages = {
        int(page)
        for candidate in dataset.candidates.values()
        if candidate.get("relation_type") == "FUNCTION_DISTRIBUTED"
        for page in candidate.get("left_pages") or []
    }
    return {
        "kind": "function_lineage_deterministic_candidates",
        "schema_version": "function-lineage-deterministic-candidates.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "run_id": run_id,
        "pair_id": dataset.pair_id,
        "input_signature": dataset.input_signature,
        "relation_namespace": RELATION_FUNCTIONAL_ANALOGUE,
        "document_relation_namespace": RELATION_DOCUMENT_LINK,
        "selector_executed": False,
        "model_calls": 0,
        "materialization_applied": False,
        "sheet_passports": copy.deepcopy(dataset.sheet_passports),
        "function_passports": copy.deepcopy(dataset.function_passports),
        "function_fragments": copy.deepcopy(dataset.function_fragments),
        "functional_candidates": sorted(
            copy.deepcopy(list(dataset.candidates.values())),
            key=lambda value: value["candidate_id"],
        ),
        "candidate_tasks": copy.deepcopy(dataset.tasks),
        "evidence_catalog": copy.deepcopy(dataset.evidence_catalog),
        "document_links": copy.deepcopy(dataset.document_link_map["links"]),
        "diagnostics": {
            "candidate_count_median": statistics.median(counts) if counts else 0.0,
            "candidate_count_p95": p95,
            "search_failures": [
                task["task_id"] for task in dataset.tasks if not task.get("candidate_ids")
            ],
            "group_generation_failures": sorted(eligible_pages - distributed_pages),
            "page_global_exclusivity": False,
            "capacity_scope": "RIGHT_PHYSICAL_PAGE_PLUS_EXACT_FUNCTION_FRAGMENT_ID",
        },
    }


def build_selector_payload(dataset: FunctionLineageDataset) -> dict[str, Any]:
    function_cores = {
        function_id: {
            "function_id": function_id,
            "source_sheet": passport["source_sheet"],
            **{field: copy.deepcopy(passport[field]) for field in PASSPORT_FIELDS},
            "fragment_ids": list(passport["function_fragment_ids"]),
        }
        for side in ("LEFT", "RIGHT")
        for function_id, passport in dataset.function_passports[side].items()
    }
    candidates = [{
        key: copy.deepcopy(candidate[key])
        for key in (
            "candidate_id", "relation_namespace", "relation_type", "direction",
            "left_pages", "right_pages", "left_function_ids", "right_function_ids",
            "left_fragment_ids", "right_fragment_ids", "component_map",
            "retrieval_channels", "supporting_channels", "channel_scores",
            "functional_score", "source_kind", "source_score",
            "group_evidence", "document_context", "explicit_contradictions",
            "evidence_refs",
        )
    } for candidate in dataset.candidates.values()]
    exposed_evidence_refs = {
        str(ref)
        for candidate in dataset.candidates.values()
        for ref in candidate.get("evidence_refs") or []
    }
    evidence_provenance = {
        evidence_ref: {
            key: copy.deepcopy(item.get(key))
            for key in (
                "side",
                "physical_page",
                "field",
                "provenance_type",
                "owner_function_id",
                "owner_fragment_id",
            )
        }
        for evidence_ref in sorted(exposed_evidence_refs)
        for item in [dataset.evidence_catalog[evidence_ref]]
    }
    payload: dict[str, Any] = {
        "schema_version": "function-lineage-selector.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "pair_id": dataset.pair_id,
        "relation_namespace": RELATION_FUNCTIONAL_ANALOGUE,
        "tasks": dataset.tasks,
        "sheet_passports": dataset.sheet_passports,
        "function_passports": function_cores,
        "function_fragments": {
            side: list(dataset.function_fragments[side].values())
            for side in ("LEFT", "RIGHT")
        },
        "document_link_candidates": dataset.document_link_map["links"],
        "functional_candidates": sorted(candidates, key=lambda value: value["candidate_id"]),
        "evidence_provenance": evidence_provenance,
        "policy": {
            "select_only_candidate_ids": True,
            "document_link_is_not_functional_analogue": True,
            "page_proximity_is_only_a_weak_supporting_signal": True,
            "physical_right_page_can_be_reused_by_distinct_fragments": True,
            "invented_ids_or_evidence_forbidden": True,
            "same_page_fragment_evidence_is_not_transferable": True,
            "sheet_shared_evidence_requires_explicit_provenance": True,
            "function_removed_requires_exhaustive_evidence": True,
        },
    }
    payload["payload_signature"] = content_signature(payload)
    return payload


def build_selector_prompt(dataset: FunctionLineageDataset, pass_name: str) -> tuple[str, dict[str, Any]]:
    payload = build_selector_payload(dataset)
    prompt = "\n".join([
        f"Independent verification pass {pass_name}.",
        "You are a bounded engineering FUNCTION LINEAGE selector.",
        "For every task choose exactly one listed candidate_id or NEED_MORE_EVIDENCE.",
        "DOCUMENT_LINK is documentary navigation and is never a FUNCTIONAL_ANALOGUE.",
        "Use exact object/zone, function, component role and topology evidence.",
        "A RIGHT physical page may be reused only through distinct right_fragment_ids.",
        "SHEET_SHARED_EVIDENCE is limited sheet context; "
        "FRAGMENT_OWNED_EVIDENCE never transfers between fragments merely "
        "because they share a page.",
        "Do not invent pages, functions, fragments, groups, relations, or evidence.",
        "A missing physical sheet never proves FUNCTION_REMOVED.",
        "Return only the JSON object required by the output schema.",
        "payload=" + canonical_json(payload),
    ])
    return prompt, payload


def output_schema(dataset: FunctionLineageDataset, payload_signature: str) -> dict[str, Any]:
    outputs = sorted({
        value for task in dataset.tasks for value in task["allowed_outputs"]
    })
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
                        "task_id": {
                            "type": "string",
                            "enum": [value["task_id"] for value in dataset.tasks],
                        },
                        "candidate_id": {"type": "string", "enum": outputs},
                    },
                },
            },
        },
    }


def _mapping_rows(candidate: Mapping[str, Any]) -> set[tuple[str, str]]:
    """Exact LEFT -> RIGHT fragment pairs a candidate asserts."""
    return {
        (str(row["left_fragment_id"]), str(row["right_fragment_id"]))
        for row in candidate.get("component_map") or []
        if row.get("left_fragment_id") and row.get("right_fragment_id")
    }


def classify_group_derivability(
    candidate: Mapping[str, Any],
    singleton_candidates: Sequence[Mapping[str, Any]],
) -> tuple[str, list[str]]:
    """Classify whether a group candidate is the exact union of child mappings.

    ``EXACT_CHILD_UNION`` iff the group covers more than one distinct LEFT
    source fragment and every exact LEFT -> RIGHT mapping of the group is
    independently present as a complete ``CONTINUED_1_TO_1`` child candidate.
    Nothing here uses scores, ranks, pages or project names.
    """
    if str(candidate.get("relation_type")) not in COMPLEX_RELATIONS:
        return "UNKNOWN", []
    mappings = _mapping_rows(candidate)
    sources = {left for left, _right in mappings}
    if not mappings or not sources:
        return "UNKNOWN", []
    declared = sorted({str(value) for value in candidate.get("right_capacity_keys") or []})
    mapped = sorted({
        str(row.get("capacity_key"))
        for row in candidate.get("component_map") or []
        if row.get("capacity_key")
    })
    if declared != mapped:
        return "UNKNOWN", []
    if candidate.get("explicit_contradictions"):
        return "NON_DECOMPOSABLE_GROUP", []
    # SPLIT is one source fragment with several targets, not a composite scope.
    if len(sources) <= 1:
        return "NON_DECOMPOSABLE_GROUP", []
    index: dict[tuple[str, str], list[str]] = {}
    for singleton in singleton_candidates:
        if str(singleton.get("relation_type")) != "CONTINUED_1_TO_1":
            continue
        if singleton.get("explicit_contradictions"):
            continue
        rows = _mapping_rows(singleton)
        if len(rows) != 1:
            continue
        index.setdefault(next(iter(rows)), []).append(str(singleton["candidate_id"]))
    matched = {
        mapping: sorted(index[mapping])[0]
        for mapping in mappings if index.get(mapping)
    }
    child_ids = sorted(set(matched.values()))
    if set(matched) == mappings:
        return "EXACT_CHILD_UNION", child_ids
    if matched:
        return "PARTIAL_CHILD_UNION", child_ids
    return "NON_DECOMPOSABLE_GROUP", []


def group_derivability_index(
    candidates: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Classify every group candidate of a dataset deterministically."""
    singletons = [
        candidate for _candidate_id, candidate in sorted(candidates.items())
        if str(candidate.get("relation_type")) == "CONTINUED_1_TO_1"
    ]
    output: dict[str, dict[str, Any]] = {}
    for candidate_id, candidate in sorted(candidates.items()):
        if str(candidate.get("relation_type")) not in COMPLEX_RELATIONS:
            continue
        classification, child_ids = classify_group_derivability(candidate, singletons)
        output[str(candidate_id)] = {
            "classification": classification,
            "child_candidate_ids": child_ids,
            "derivable": classification == "EXACT_CHILD_UNION",
        }
    return output


def exact_child_union_licences(
    candidates: Mapping[str, Mapping[str, Any]],
    derivability: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[tuple[str, str], list[str]]:
    """Map an unordered child-candidate pair to certified exact-union parents.

    Two atomic child lineages of one certified exact union are parts of ONE
    composed mapping.  Selecting both is equivalent to selecting the parent
    group candidate, which is itself an allowed output, so their claims on the
    shared RIGHT fragment are co-ownership, not competition.
    """
    index = derivability if derivability is not None else group_derivability_index(candidates)
    licences: dict[tuple[str, str], list[str]] = {}
    for parent_id, row in sorted(index.items()):
        if row.get("classification") != "EXACT_CHILD_UNION":
            continue
        children = sorted({str(value) for value in row.get("child_candidate_ids") or []})
        for left, right in itertools.combinations(children, 2):
            licences.setdefault((left, right), []).append(str(parent_id))
    return {key: sorted(value) for key, value in licences.items()}


def _capacity_owner(candidate: Mapping[str, Any], key: str) -> frozenset[str]:
    """LEFT fragments a candidate asserts continue into one exact RIGHT key."""
    return frozenset(
        str(row["left_fragment_id"])
        for row in candidate.get("component_map") or []
        if str(row.get("capacity_key")) == str(key) and row.get("left_fragment_id")
    )


def capacity_compatibility(
    *,
    key: str,
    held_id: str,
    held: Mapping[str, Any],
    claim_id: str,
    claim: Mapping[str, Any],
    licences: Mapping[tuple[str, str], Sequence[str]],
) -> tuple[str, list[str]] | None:
    """Name the licence under which two claims may share one RIGHT fragment.

    Returns ``None`` when no licence is provable.  Missing structure is never
    a licence: unknown ownership stays fail-closed.
    """
    if held_id == claim_id:
        # A MERGED candidate is one lineage exposed to several LEFT tasks.
        return "SAME_LINEAGE", []
    held_owner = _capacity_owner(held, key)
    claim_owner = _capacity_owner(claim, key)
    if not held_owner or not claim_owner:
        return None
    if held_owner == claim_owner:
        return "SAME_ATOMIC_OWNERSHIP", []
    if held_owner < claim_owner or claim_owner < held_owner:
        # One claim is the composite that contains the other atomic mapping.
        return "DERIVED_COMPOSITE_OWNERSHIP", []
    if held_owner & claim_owner:
        return None
    parents = list(licences.get(tuple(sorted((held_id, claim_id))), ()))
    if parents:
        return "DERIVED_EXACT_CHILD_UNION", parents
    return None


CAPACITY_RESOLUTION = "GLOBAL_ORDER_INDEPENDENT_PAIRWISE"
CAPACITY_CONTESTED = "CAPACITY_CONTESTED"


def capacity_ownership(
    selections: Sequence[Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
    *,
    licences: Mapping[tuple[str, str], Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Fragment-level capacity accounting over lineage ownership.

    Capacity identity stays ``RIGHT physical_page + exact function_fragment_id``
    and is never page-global.  A fragment may carry several claims only when a
    deterministic licence proves they are one composed mapping.

    The accounting is a pure function of the *set* of claims.  Every distinct
    pair of claims on a key is examined, so the outcome cannot depend on shard
    boundaries, shard size, batch or task ordering, parallel scheduling or
    cold-run grouping.  A key whose claims are not pairwise compatible is
    contested as a whole: no winner is selected by score, rank, order,
    confidence or page.
    """
    resolved = (
        licences if licences is not None
        else exact_child_union_licences(candidates)
    )
    claimants: dict[str, set[str]] = {}
    for selection in selections:
        candidate_id = str(selection.get("candidate_id") or "")
        candidate = candidates.get(candidate_id)
        if candidate is None:
            continue
        for capacity_key in candidate.get("right_capacity_keys") or []:
            claimants.setdefault(str(capacity_key), set()).add(candidate_id)

    errors: set[str] = set()
    granted: set[str] = set()
    contested: set[str] = set()
    for key in sorted(claimants):
        ordered = sorted(claimants[key])
        conflicting = False
        for left, right in itertools.combinations(ordered, 2):
            licence = capacity_compatibility(
                key=key,
                held_id=left,
                held=candidates[left],
                claim_id=right,
                claim=candidates[right],
                licences=resolved,
            )
            if licence is None:
                conflicting = True
                errors.add(f"FUNCTION_FRAGMENT_CONFLICT:{key}:{left}:{right}")
                continue
            name, parents = licence
            if name == "SAME_LINEAGE":
                continue
            granted.add(canonical_json({
                "capacity_key": key,
                "licence": name,
                "candidate_ids": [left, right],
                "derived_from_candidate_ids": list(parents),
            }))
        if conflicting:
            contested.add(key)
    return {
        "errors": sorted(errors),
        "licences": sorted(granted),
        "contested_capacity_keys": sorted(contested),
        "contested_candidate_ids": sorted({
            candidate_id for key in contested for candidate_id in claimants[key]
        }),
        "capacity_identity": "RIGHT physical_page + exact function_fragment_id",
        "resolution": CAPACITY_RESOLUTION,
    }


def verify_capacity(
    selections: Sequence[Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
    *,
    licences: Mapping[tuple[str, str], Sequence[str]] | None = None,
) -> list[str]:
    """Allow page reuse, but reject reuse of one atomic RIGHT fragment."""
    return capacity_ownership(selections, candidates, licences=licences)["errors"]


def resolve_lineage_capacity(
    claims: Mapping[str, str],
    candidates: Mapping[str, Mapping[str, Any]],
    *,
    licences: Mapping[tuple[str, str], Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Resolve capacity once, globally, over the stable claims of a whole run.

    ``claims`` maps a task id to the candidate the task selected unanimously.
    Only stable claims belong here: a task that did not reach two-pass
    unanimity has nothing to publish and therefore cannot contest a fragment.

    A claim is published only when every capacity key it consumes is
    uncontested.  Publishing part of a claim would assert a relation the
    candidate does not declare, so a claim is withheld whole.
    """
    selections = [
        {"task_id": str(task_id), "candidate_id": str(candidate_id)}
        for task_id, candidate_id in sorted(claims.items())
        if str(candidate_id) not in {"", NEED_MORE_EVIDENCE, FUNCTION_REMOVED}
    ]
    ownership = capacity_ownership(selections, candidates, licences=licences)
    contested_keys = set(ownership["contested_capacity_keys"])
    published: dict[str, str] = {}
    withheld: dict[str, dict[str, Any]] = {}
    for selection in selections:
        candidate = candidates.get(selection["candidate_id"])
        if candidate is None:
            continue
        keys = {str(value) for value in candidate.get("right_capacity_keys") or []}
        blocked = sorted(keys & contested_keys)
        if blocked:
            withheld[selection["task_id"]] = {
                "candidate_id": selection["candidate_id"],
                "contested_capacity_keys": blocked,
                "reason_code": CAPACITY_CONTESTED,
            }
        else:
            published[selection["task_id"]] = selection["candidate_id"]
    return {
        **ownership,
        "stable_claim_count": len(selections),
        "published": published,
        "withheld": withheld,
        "published_count": len(published),
        "withheld_count": len(withheld),
    }


def verify_selector_response(
    dataset: FunctionLineageDataset,
    payload_signature: str,
    response: Mapping[str, Any] | None,
) -> dict[str, Any]:
    tasks = {str(value["task_id"]): value for value in dataset.tasks}
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
    by_task: dict[str, Mapping[str, Any]] = {}
    for raw in raw_selections:
        if not isinstance(raw, Mapping):
            result["global_errors"].append("INVALID_SELECTION")
            continue
        task_id = str(raw.get("task_id") or "")
        if task_id in by_task:
            result["global_errors"].append("DUPLICATE_TASK")
        by_task[task_id] = raw
    if set(by_task) != set(tasks):
        result["global_errors"].append("TASK_SET_MISMATCH")

    all_passports = {
        **dataset.function_passports["LEFT"],
        **dataset.function_passports["RIGHT"],
    }
    concrete: list[dict[str, str]] = []
    for task_id, task in tasks.items():
        raw = by_task.get(task_id) or {}
        errors: list[str] = []
        extra_fields = set(raw) - {"task_id", "candidate_id"}
        if extra_fields & {"evidence", "evidence_refs", "source_evidence"}:
            errors.append("AI_INVENTED_EVIDENCE")
        if extra_fields & {
            "function_ids", "fragment_ids", "left_fragment_ids", "right_fragment_ids",
        }:
            errors.append("AI_INVENTED_FRAGMENT")
        if extra_fields:
            errors.append("UNKNOWN_SELECTION_FIELD")
        candidate_id = str(raw.get("candidate_id") or "")
        if candidate_id not in task["allowed_outputs"]:
            errors.append("CANDIDATE_ID_NOT_BOUNDED")
        if candidate_id == FUNCTION_REMOVED:
            errors.append("FUNCTION_REMOVED_WITHOUT_EXHAUSTIVE_EVIDENCE")
        candidate = dataset.candidates.get(candidate_id)
        if candidate is not None:
            if candidate_id not in task["candidate_ids"]:
                errors.append("CANDIDATE_NOT_OWNED_BY_TASK")
            if candidate.get("relation_namespace") != RELATION_FUNCTIONAL_ANALOGUE:
                errors.append("RELATION_NAMESPACE_MIXED")
            if candidate.get("relation_type") not in CONCRETE_RELATIONS:
                errors.append("RELATION_TYPE_NOT_ALLOWED")
            if candidate.get("direction") != DIRECTION:
                errors.append("DIRECTION_NOT_LEFT_TO_RIGHT")
            for function_id in candidate.get("left_function_ids") or []:
                if function_id not in dataset.function_passports["LEFT"]:
                    errors.append("LEFT_FUNCTION_NOT_FOUND")
            for function_id in candidate.get("right_function_ids") or []:
                if function_id not in dataset.function_passports["RIGHT"]:
                    errors.append("RIGHT_FUNCTION_NOT_FOUND")
            for fragment_id in candidate.get("left_fragment_ids") or []:
                if fragment_id not in dataset.function_fragments["LEFT"]:
                    errors.append("LEFT_FRAGMENT_NOT_FOUND")
            for fragment_id in candidate.get("right_fragment_ids") or []:
                if fragment_id not in dataset.function_fragments["RIGHT"]:
                    errors.append("RIGHT_FRAGMENT_NOT_FOUND")
            allowed_fragment_ids = set(candidate.get("left_fragment_ids") or []) | set(
                candidate.get("right_fragment_ids") or []
            )
            allowed_function_ids = set(candidate.get("left_function_ids") or []) | set(
                candidate.get("right_function_ids") or []
            )
            allowed_pages = {
                ("LEFT", int(page)) for page in candidate.get("left_pages") or []
            } | {
                ("RIGHT", int(page)) for page in candidate.get("right_pages") or []
            }
            expected_refs = {
                str(ref)
                for function_id in [
                    *(candidate.get("left_function_ids") or []),
                    *(candidate.get("right_function_ids") or []),
                ]
                for ref in (all_passports.get(str(function_id)) or {}).get("evidence_refs") or []
            }
            for evidence_ref in candidate.get("evidence_refs") or []:
                evidence = dataset.evidence_catalog.get(str(evidence_ref))
                if evidence is None:
                    errors.append("EVIDENCE_NOT_FOUND")
                    continue
                if evidence_ref not in expected_refs:
                    errors.append("EVIDENCE_NOT_OWNED_BY_CANDIDATE")
                provenance_type = evidence.get("provenance_type")
                if provenance_type == FRAGMENT_OWNED_EVIDENCE:
                    if evidence.get("owner_function_id") not in allowed_function_ids:
                        errors.append("EVIDENCE_FUNCTION_OWNER_MISMATCH")
                    if evidence.get("owner_fragment_id") not in allowed_fragment_ids:
                        errors.append("EVIDENCE_FRAGMENT_OWNER_MISMATCH")
                elif provenance_type == SHEET_SHARED_EVIDENCE:
                    if evidence.get("field") not in SHEET_SHARED_FIELDS:
                        errors.append("SHEET_SHARED_EVIDENCE_FIELD_NOT_ALLOWED")
                    if evidence.get("owner_function_id") is not None:
                        errors.append("SHEET_SHARED_EVIDENCE_HAS_FUNCTION_OWNER")
                    if evidence.get("owner_fragment_id") is not None:
                        errors.append("SHEET_SHARED_EVIDENCE_HAS_FRAGMENT_OWNER")
                else:
                    errors.append("EVIDENCE_PROVENANCE_TYPE_INVALID")
                if (str(evidence.get("side")), int(evidence.get("physical_page") or 0)) not in allowed_pages:
                    errors.append("EVIDENCE_PAGE_OWNER_MISMATCH")
            if set(candidate.get("evidence_refs") or []) != expected_refs:
                errors.append("EVIDENCE_SET_INCOMPLETE")
            concrete.append({"task_id": task_id, "candidate_id": candidate_id})
        result["task_results"][task_id] = {
            "ok": not errors,
            "candidate_id": candidate_id,
            "errors": sorted(set(errors)),
        }

    # Capacity is deliberately NOT evaluated here.  A response is one shard of
    # one pass, so accounting capacity at this point would make a task's
    # outcome depend on which other tasks happened to share its batch.  It is
    # resolved once, globally, after two-pass consensus.
    result["capacity_scope"] = "DEFERRED_TO_GLOBAL_RESOLUTION"
    result["global_errors"] = sorted(set(result["global_errors"]))
    if result["global_errors"]:
        for task_result in result["task_results"].values():
            task_result["ok"] = False
    result["ok"] = not result["global_errors"] and all(
        value["ok"] for value in result["task_results"].values()
    )
    return result


def _usage_tokens(usage: Mapping[str, Any]) -> int:
    for key in ("total_tokens", "total_input_tokens"):
        value = usage.get(key)
        if isinstance(value, (int, float)):
            if key == "total_input_tokens" and isinstance(usage.get("output_tokens"), (int, float)):
                return int(value) + int(usage["output_tokens"])
            return int(value)
    return sum(
        int(usage.get(key) or 0)
        for key in ("input_tokens", "output_tokens", "cached_input_tokens")
    )


def _call_pass(
    dataset: FunctionLineageDataset,
    pass_name: str,
    *,
    cancel: Any = None,
    run_id: str = "",
) -> dict[str, Any]:
    prompt, payload = build_selector_prompt(dataset, pass_name)
    started = time.perf_counter()
    try:
        call = ai_gateway.call(
            ai_settings.CODEX_SESSION,
            prompt,
            model=ai_settings.analyst_model(),
            reasoning_level=ai_settings.analyst_effort(),
            schema=output_schema(dataset, str(payload["payload_signature"])),
            images=(),
            retries=0,
            cancel=cancel,
            run_id=run_id,
        )
        ok = bool(call.ok)
        parsed = call.parsed if ok else None
        duration_ms = int(call.duration_ms)
        usage = dict(call.usage or {})
        error_kind = str(call.error_kind or "") if not ok else ""
        attempts = int(call.attempts or 1)
    except Exception as exc:  # noqa: BLE001 - shadow must fail closed
        ok = False
        parsed = None
        duration_ms = max(0, int((time.perf_counter() - started) * 1000))
        usage = {}
        error_kind = type(exc).__name__
        attempts = 1
    verification = verify_selector_response(
        dataset, str(payload["payload_signature"]), parsed
    )
    return {
        "pass_name": pass_name,
        "mode": "TEXT_STRUCTURED",
        "model_call": {
            "ok": ok,
            "duration_ms": duration_ms,
            "usage": usage,
            "tokens": _usage_tokens(usage),
            "error_kind": error_kind,
            "attempts": attempts,
            "vision_used": False,
        },
        "verification": verification,
    }


def stable_consensus(
    dataset: FunctionLineageDataset,
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Require verified unanimity of exactly Pass A and Pass B."""
    decisions: list[dict[str, Any]] = []
    for task in dataset.tasks:
        task_id = str(task["task_id"])
        observations = []
        for record in records:
            task_result = (
                ((record.get("verification") or {}).get("task_results") or {}).get(task_id)
                or {}
            )
            observations.append({
                "pass_name": record.get("pass_name"),
                "model_ok": bool((record.get("model_call") or {}).get("ok")),
                "verified": bool(task_result.get("ok")),
                "candidate_id": task_result.get("candidate_id"),
            })
        choices = [value.get("candidate_id") for value in observations if value["verified"]]
        unanimous = (
            {str(value.get("pass_name")) for value in observations} == {"A", "B"}
            and len(choices) == 2
            and len(set(choices)) == 1
        )
        selected = str(choices[0]) if unanimous else NEED_MORE_EVIDENCE
        candidate = dataset.candidates.get(selected)
        decisions.append({
            "task_id": task_id,
            "left_physical_page": task["left_physical_page"],
            "left_function_id": task["left_function_id"],
            "left_fragment_id": task["left_fragment_id"],
            "stable": unanimous,
            "selected_candidate_id": selected,
            "relation_type": (
                candidate["relation_type"] if candidate is not None else NEED_MORE_EVIDENCE
            ),
            "observations": observations,
        })
    return decisions


def _lineages(
    dataset: FunctionLineageDataset,
    decisions: Sequence[Mapping[str, Any]],
    capacity: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    stable: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    analogues: dict[str, dict[str, Any]] = {}
    withheld = dict((capacity or {}).get("withheld") or {})
    for decision in decisions:
        candidate_id = str(decision.get("selected_candidate_id") or "")
        candidate = dataset.candidates.get(candidate_id)
        contested = withheld.get(str(decision["task_id"]))
        if contested is not None:
            unresolved.append({
                "task_id": decision["task_id"],
                "left_physical_page": decision["left_physical_page"],
                "left_function_id": decision["left_function_id"],
                "left_fragment_id": decision["left_fragment_id"],
                "relation_namespace": RELATION_FUNCTION_LINEAGE,
                "relation_type": NEED_MORE_EVIDENCE,
                "reason_code": CAPACITY_CONTESTED,
                "contested_capacity_keys": list(contested["contested_capacity_keys"]),
                "withheld_candidate_id": contested["candidate_id"],
                "observations": copy.deepcopy(decision.get("observations") or []),
            })
            continue
        if not decision.get("stable") or candidate is None:
            observations = decision.get("observations") or []
            verified_choices = [
                value.get("candidate_id") for value in observations if value.get("verified")
            ]
            reason = (
                NEED_MORE_EVIDENCE
                if len(verified_choices) == 2 and set(verified_choices) == {NEED_MORE_EVIDENCE}
                else "PASS_DISAGREEMENT"
                if len(verified_choices) == 2
                else "MODEL_OR_VERIFIER_FAILURE"
            )
            unresolved.append({
                "task_id": decision["task_id"],
                "left_physical_page": decision["left_physical_page"],
                "left_function_id": decision["left_function_id"],
                "left_fragment_id": decision["left_fragment_id"],
                "relation_namespace": RELATION_FUNCTION_LINEAGE,
                "relation_type": NEED_MORE_EVIDENCE,
                "reason_code": reason,
                "observations": copy.deepcopy(observations),
            })
            continue
        analogue = copy.deepcopy(candidate)
        analogue["materialization_allowed"] = False
        analogues.setdefault(candidate_id, analogue)
        stable.setdefault(candidate_id, {
            "lineage_id": stable_id("lineage_", dataset.pair_id, candidate_id),
            "candidate_id": candidate_id,
            "pair_id": dataset.pair_id,
            "relation_namespace": RELATION_FUNCTION_LINEAGE,
            "functional_relation_namespace": RELATION_FUNCTIONAL_ANALOGUE,
            "relation_type": candidate["relation_type"],
            "direction": DIRECTION,
            "left_pages": list(candidate["left_pages"]),
            "right_pages": list(candidate["right_pages"]),
            "left_function_ids": list(candidate["left_function_ids"]),
            "right_function_ids": list(candidate["right_function_ids"]),
            "left_fragment_ids": list(candidate["left_fragment_ids"]),
            "right_fragment_ids": list(candidate["right_fragment_ids"]),
            "right_capacity_keys": list(candidate["right_capacity_keys"]),
            "evidence_refs": list(candidate["evidence_refs"]),
            "materialization_allowed": False,
        })
    return (
        sorted(stable.values(), key=lambda value: value["lineage_id"]),
        sorted(unresolved, key=lambda value: value["task_id"]),
        sorted(analogues.values(), key=lambda value: value["candidate_id"]),
    )


def _engineer_disagreements(
    stable_lineages: Sequence[Mapping[str, Any]],
    manual_mappings: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    disagreements: list[dict[str, Any]] = []
    # Documentary human links are deliberately ignored: DOCUMENT_LINK and a
    # functional lineage are allowed to point to different sheets.
    functional = [
        value for value in manual_mappings
        if value.get("relation_namespace") in {
            RELATION_FUNCTIONAL_ANALOGUE, RELATION_FUNCTION_LINEAGE,
        }
    ]
    for lineage in stable_lineages:
        left_pages = sorted(int(value) for value in lineage.get("left_pages") or [])
        for manual in functional:
            manual_left = sorted(int(value) for value in manual.get("left_pages") or [])
            if manual_left != left_pages:
                continue
            manual_right = sorted(int(value) for value in manual.get("right_pages") or [])
            shadow_right = sorted(int(value) for value in lineage.get("right_pages") or [])
            if manual_right != shadow_right:
                disagreements.append({
                    "lineage_id": lineage["lineage_id"],
                    "manual_mapping_id": manual.get("mapping_id") or manual.get("id"),
                    "left_pages": left_pages,
                    "shadow_right_pages": shadow_right,
                    "manual_right_pages": manual_right,
                    "reason_code": "SHADOW_DIFFERS_FROM_MANUAL_FUNCTIONAL_DECISION",
                    "resolution": "HUMAN_MAPPING_PRESERVED",
                })
    return disagreements


def derive_sheet_map(
    *,
    pair_id: str,
    run_id: str,
    input_signature: str,
    stable_lineages: Sequence[Mapping[str, Any]],
    shadow_status: str,
) -> dict[str, Any]:
    relations = [{
        "derived_relation_id": stable_id("dsrel_", value["lineage_id"]),
        "lineage_id": value["lineage_id"],
        "relation_namespace": RELATION_FUNCTION_LINEAGE,
        "relation_type": value["relation_type"],
        "left_pages": list(value["left_pages"]),
        "right_pages": list(value["right_pages"]),
        "right_fragment_ids": list(value["right_fragment_ids"]),
        "right_capacity_keys": list(value["right_capacity_keys"]),
    } for value in stable_lineages]
    by_page: dict[int, list[dict[str, Any]]] = {}
    for relation in relations:
        for page in relation["right_pages"]:
            by_page.setdefault(int(page), []).append(relation)
    reuse: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for page, occupants in sorted(by_page.items()):
        if len(occupants) < 2:
            continue
        key_sets = [set(value["right_capacity_keys"]) for value in occupants]
        compatible = all(
            not key_sets[left] & key_sets[right]
            for left in range(len(key_sets))
            for right in range(left + 1, len(key_sets))
        )
        row = {
            "right_physical_page": page,
            "lineage_ids": [value["lineage_id"] for value in occupants],
            "right_fragment_ids": sorted({
                fragment_id for value in occupants for fragment_id in value["right_fragment_ids"]
            }),
            "function_level_compatible": compatible,
        }
        reuse.append(row)
        if not compatible:
            conflicts.append(row)
    return {
        "kind": "derived_sheet_map",
        "schema_version": "derived-sheet-map.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "run_id": run_id,
        "pair_id": pair_id,
        "input_signature": input_signature,
        "shadow_status": shadow_status,
        "derivation": "FUNCTION_LINEAGE -> FUNCTION_FRAGMENTS -> PHYSICAL_SHEETS",
        "relations": relations,
        "right_sheet_reuse": reuse,
        "function_level_conflicts": conflicts,
        "production_sheet_scope_unchanged": True,
        "materialization_applied": False,
    }


def _rejections(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for record in records:
        verification = record.get("verification") or {}
        for error in verification.get("global_errors") or []:
            output.append({"pass_name": record.get("pass_name"), "reason_code": error})
        for task_id, task_result in (verification.get("task_results") or {}).items():
            for error in task_result.get("errors") or []:
                output.append({
                    "pass_name": record.get("pass_name"),
                    "task_id": task_id,
                    "candidate_id": task_result.get("candidate_id"),
                    "reason_code": error,
                })
    return output


def run_shadow(
    *,
    pair_id: str,
    run_id: str,
    sheet_indexes: Mapping[str, Sequence[Mapping[str, Any]]],
    sheet_relations: Mapping[str, Any],
    manual_mappings: Sequence[Mapping[str, Any]] = (),
    cancel: Any = None,
) -> dict[str, dict[str, Any]]:
    """Run two TEXT passes and return three non-materializing artifacts."""
    started = time.perf_counter()
    generated_at = utc_now()
    dataset = build_dataset(
        pair_id=pair_id,
        sheet_indexes=sheet_indexes,
        sheet_relations=sheet_relations,
    )
    records = (
        [
            _call_pass(dataset, "A", cancel=cancel, run_id=run_id),
            _call_pass(dataset, "B", cancel=cancel, run_id=run_id),
        ]
        if dataset.tasks else []
    )
    decisions = stable_consensus(dataset, records)
    # One global, order-independent capacity resolution over the stable claims
    # of the whole run.  Nothing before this point consumes capacity.
    capacity = resolve_lineage_capacity(
        {
            str(value["task_id"]): str(value["selected_candidate_id"])
            for value in decisions if value.get("stable")
        },
        dataset.candidates,
    )
    stable_lineages, unresolved, analogues = _lineages(dataset, decisions, capacity)
    rejections = _rejections(records)
    capacity_errors = list(capacity["errors"])
    calls_ok = all((record.get("model_call") or {}).get("ok") for record in records)
    verifier_ok = all((record.get("verification") or {}).get("ok") for record in records)
    shadow_status = "COMPLETED" if calls_ok and verifier_ok else "FAILED"
    model_calls = len(records)
    tokens = sum(int((record.get("model_call") or {}).get("tokens") or 0) for record in records)
    runtime_ms = max(0, int((time.perf_counter() - started) * 1000))
    disagreements = _engineer_disagreements(stable_lineages, manual_mappings)
    function_map = {
        "kind": "function_lineage_map",
        "schema_version": SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "run_id": run_id,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "input_signature": dataset.input_signature,
        "relation_namespace": RELATION_FUNCTION_LINEAGE,
        "candidate_relation_namespace": RELATION_FUNCTIONAL_ANALOGUE,
        "document_relation_namespace": RELATION_DOCUMENT_LINK,
        "shadow_status": shadow_status,
        "stable_lineages": stable_lineages,
        "unresolved_lineages": unresolved,
        "document_links": copy.deepcopy(dataset.document_link_map["links"]),
        "functional_analogues": analogues,
        "distributed_relations": [
            value for value in stable_lineages
            if value["relation_type"] in COMPLEX_RELATIONS
        ],
        "function_level_conflicts": capacity_errors,
        "capacity_resolution": {
            "resolution": capacity["resolution"],
            "capacity_identity": capacity["capacity_identity"],
            "stage": "POST_CONSENSUS_GLOBAL",
            "stable_claims": capacity["stable_claim_count"],
            "published": capacity["published_count"],
            "withheld": capacity["withheld_count"],
            "contested_capacity_keys": list(capacity["contested_capacity_keys"]),
            "licences": list(capacity["licences"]),
        },
        "engineer_disagreements": disagreements,
        "unsupported_or_rejected": rejections,
        "model_calls": model_calls,
        "tokens": tokens,
        "runtime": {"duration_ms": runtime_ms},
        "stability": {
            "required_passes": ["A", "B"],
            "policy": "FULL_TWO_PASS_UNANIMITY",
            "stable": len(stable_lineages),
            "unresolved": len(unresolved),
        },
        "verifier_result": {
            "status": "PASSED" if verifier_ok else "FAILED",
            "passes": [{
                "pass_name": record["pass_name"],
                "model_ok": bool(record["model_call"]["ok"]),
                "verification_ok": bool(record["verification"]["ok"]),
                "global_errors": list(record["verification"].get("global_errors") or []),
            } for record in records],
            "fail_closed": True,
        },
        "sheet_passports": dataset.sheet_passports,
        "function_passports": dataset.function_passports,
        "function_fragments": dataset.function_fragments,
        "functional_candidates": sorted(
            dataset.candidates.values(), key=lambda value: value["candidate_id"]
        ),
        "candidate_tasks": copy.deepcopy(dataset.tasks),
        "evidence_catalog": dataset.evidence_catalog,
        "model_passes": [{
            "pass_name": record["pass_name"],
            "mode": record["mode"],
            "model_call": copy.deepcopy(record["model_call"]),
            "verification": copy.deepcopy(record["verification"]),
        } for record in records],
        "materialization": {
            "feature_flag": ai_settings.FUNCTION_LINEAGE_MATERIALIZATION_FEATURE_FLAG,
            "requested": ai_settings.function_lineage_materialization_enabled(),
            "implemented": False,
            "applied": False,
            "production_result_changed": False,
        },
        "human_priority": "HUMAN_MAPPING_OVER_SHADOW_AI",
        "vision_used": False,
    }
    document_map = {
        **dataset.document_link_map,
        "run_id": run_id,
        "generated_at": generated_at,
        "shadow_status": shadow_status,
    }
    derived = derive_sheet_map(
        pair_id=pair_id,
        run_id=run_id,
        input_signature=dataset.input_signature,
        stable_lineages=stable_lineages,
        shadow_status=shadow_status,
    )
    return {
        "document_link_map": document_map,
        "function_lineage_map": function_map,
        "derived_sheet_map": derived,
    }


def failure_artifacts(
    *, pair_id: str, run_id: str, reason_code: str,
) -> dict[str, dict[str, Any]]:
    """Return safe, path-free artifacts for an unexpected contour failure."""
    generated_at = utc_now()
    signature = content_signature({
        "algorithm_version": ALGORITHM_VERSION,
        "pair_id": pair_id,
        "run_id": run_id,
        "reason_code": reason_code,
    })
    document_map = {
        "kind": "document_link_map",
        "schema_version": "document-link-map.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "run_id": run_id,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "input_signature": signature,
        "relation_namespace": RELATION_DOCUMENT_LINK,
        "direction": DIRECTION,
        "shadow_status": "FAILED",
        "links": [],
        "evidence_catalog": {},
        "reason_code": reason_code,
    }
    function_map = {
        "kind": "function_lineage_map",
        "schema_version": SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "run_id": run_id,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "input_signature": signature,
        "relation_namespace": RELATION_FUNCTION_LINEAGE,
        "candidate_relation_namespace": RELATION_FUNCTIONAL_ANALOGUE,
        "document_relation_namespace": RELATION_DOCUMENT_LINK,
        "shadow_status": "FAILED",
        "stable_lineages": [],
        "unresolved_lineages": [],
        "document_links": [],
        "functional_analogues": [],
        "distributed_relations": [],
        "function_level_conflicts": [],
        "engineer_disagreements": [],
        "unsupported_or_rejected": [{"reason_code": reason_code}],
        "model_calls": 0,
        "tokens": 0,
        "runtime": {"duration_ms": 0},
        "stability": {"required_passes": ["A", "B"], "policy": "FULL_TWO_PASS_UNANIMITY"},
        "verifier_result": {"status": "FAILED", "fail_closed": True, "reason_code": reason_code},
        "sheet_passports": {"LEFT": {}, "RIGHT": {}},
        "function_passports": {"LEFT": {}, "RIGHT": {}},
        "function_fragments": {"LEFT": {}, "RIGHT": {}},
        "functional_candidates": [],
        "evidence_catalog": {},
        "model_passes": [],
        "materialization": {
            "feature_flag": ai_settings.FUNCTION_LINEAGE_MATERIALIZATION_FEATURE_FLAG,
            "requested": ai_settings.function_lineage_materialization_enabled(),
            "implemented": False,
            "applied": False,
            "production_result_changed": False,
        },
        "human_priority": "HUMAN_MAPPING_OVER_SHADOW_AI",
        "vision_used": False,
        "reason_code": reason_code,
    }
    return {
        "document_link_map": document_map,
        "function_lineage_map": function_map,
        "derived_sheet_map": derive_sheet_map(
            pair_id=pair_id,
            run_id=run_id,
            input_signature=signature,
            stable_lineages=[],
            shadow_status="FAILED",
        ) | {"reason_code": reason_code},
    }


__all__ = [
    "ALGORITHM_VERSION",
    "CONCRETE_RELATIONS",
    "DIRECTION",
    "FRAGMENT_OWNED_EVIDENCE",
    "FUNCTION_REMOVED",
    "NEED_MORE_EVIDENCE",
    "PASSPORT_FIELDS",
    "RELATION_DOCUMENT_LINK",
    "RELATION_FUNCTIONAL_ANALOGUE",
    "RELATION_FUNCTION_LINEAGE",
    "SHEET_SHARED_EVIDENCE",
    "SHEET_SHARED_FIELDS",
    "FunctionLineageDataset",
    "build_dataset",
    "CAPACITY_CONTESTED",
    "CAPACITY_RESOLUTION",
    "capacity_compatibility",
    "capacity_ownership",
    "classify_group_derivability",
    "exact_child_union_licences",
    "group_derivability_index",
    "deterministic_candidate_artifact",
    "build_selector_payload",
    "build_selector_prompt",
    "derive_sheet_map",
    "failure_artifacts",
    "output_schema",
    "resolve_lineage_capacity",
    "run_shadow",
    "stable_consensus",
    "verify_capacity",
    "verify_selector_response",
]
