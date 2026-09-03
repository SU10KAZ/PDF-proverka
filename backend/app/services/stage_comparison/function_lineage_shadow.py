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
import re
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .ai import gateway as ai_gateway
from .ai import settings as ai_settings
from .production_artifacts import canonical_json, content_signature, stable_id, utc_now
from .sheet_matcher import normalize_sheet


ALGORITHM_VERSION = "function-lineage-matcher.v1.1-shadow"
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
    "function_class",
    "serviced_object",
    "corpus",
    "section",
    "zone",
    "floors",
    "systems",
    "equipment_roles",
    "upstream",
    "downstream",
    "topology_role",
    "component_role",
    "document_role",
    "neighboring_function_context",
    "contradictions",
    "evidence_refs",
)
SHEET_SHARED_FIELDS = frozenset({
    "serviced_object",
    "corpus",
    "section",
    "zone",
    "floors",
    "systems",
    "consumers",
    "equipment_roles",
    "document_role",
})
MAX_FACTS_PER_FIELD = 12
MAX_CANDIDATES_PER_TASK = 12

_TOKEN_RE = re.compile(r"[a-zа-я0-9]+(?:[-./][a-zа-я0-9]+)*", re.I)
_FUNCTION_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("ELECTRICAL_DISTRIBUTION", ("вру", "грщ", "распределительн", "однолинейн", "электроснабжен")),
    ("LOAD_CALCULATION", ("расчет нагруз", "расчёт нагруз", "расчетный ток", "calculation")),
    ("LIGHTING", ("освещен", "светильник", "lighting")),
    ("GROUNDING_LIGHTNING", ("заземлен", "молниезащит", "уравнивани")),
    ("WATER_DRAINAGE", ("водоотведен", "канализац", "водосток", "сточн")),
    ("HOT_WATER", ("горяч", "т3", "т4")),
    ("FIRE_WATER", ("пожар", "впв", "в2.1", "в2.2")),
    ("WATER_SUPPLY", ("водоснабжен", "водопровод", "холодн", "хвс")),
    ("RISER_DISTRIBUTION", ("стояк", "квартир", "riser")),
    ("DOMESTIC_PRESSURE_BOOST", ("насосная хвс", "хозяйственно-питьевого", "domestic booster")),
    ("FIRE_PRESSURE_BOOST", ("насосная впв", "пожаротушен", "fire booster")),
    ("INCOMING_METERING", ("водомерный узел", "общедомовой водомер", "ввод в1")),
    ("METERING", ("водомер", "счетчик", "счётчик", "узел учета", "узел учёта")),
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
        "document_role": _document_role(normalized),
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
        "source": "COMPACT_SHEET_PASSPORT",
        "provenance_type": provenance_type,
        "owner_function_id": (
            function_id if provenance_type == FRAGMENT_OWNED_EVIDENCE else None
        ),
        "owner_fragment_id": (
            fragment_id if provenance_type == FRAGMENT_OWNED_EVIDENCE else None
        ),
    }


def _passport_values(sheet: Mapping[str, Any], function_class: str) -> dict[str, Any]:
    buildings = _unique(f"Корпус {value}" for value in sheet.get("buildings") or [])
    systems = _unique(sheet.get("functional") or [])
    entities = _unique(sheet.get("entities") or [])
    sections = _unique(
        value for value in systems
        if any(marker in _clean(value) for marker in ("план", "схем", "разрез", "section"))
    )
    zones = _unique([
        *(f"Отметка {sheet['elevation']}" for _ in [0] if sheet.get("elevation")),
    ])
    return {
        "function_class": function_class,
        "serviced_object": buildings,
        "corpus": buildings,
        "section": sections,
        "zone": zones,
        "floors": _unique(sheet.get("floors") or []),
        "systems": systems,
        "consumers": [],
        "equipment_roles": entities,
        # Direction is not inferred from an unordered topology token list.
        "upstream": [],
        "downstream": [],
        "topology_role": function_class,
        "component_role": function_class,
        "document_role": sheet["document_role"],
        "neighboring_function_context": [],
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
            if sheet.get("document_role") != "GRAPHIC_SHEET":
                continue
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
                    "source_sheet": {"side": side, "physical_page": page},
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
            passport["provenance"]["neighboring_function_context"] = list(
                passport["evidence_refs"]
            )
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


def _edge_candidates(sheet_relations: Mapping[str, Any]) -> dict[int, list[dict[str, Any]]]:
    result: dict[int, list[dict[str, Any]]] = {}
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
                result.setdefault(left_page, []).append(dict(edge))
    # Exact/group relations are also valid bounded retrieval sources when they
    # were established before a deep content candidate was needed.
    for relation in sheet_relations.get("relations") or []:
        if not isinstance(relation, Mapping):
            continue
        for left_page in relation.get("left_pages") or []:
            for right_page in relation.get("right_pages") or []:
                pair = (int(left_page), int(right_page))
                existing = {
                    (int(value.get("left_page") or pair[0]), int(value.get("right_page") or 0))
                    for value in result.get(pair[0], [])
                }
                if pair not in existing:
                    result.setdefault(pair[0], []).append({
                        "left_page": pair[0],
                        "right_page": pair[1],
                        "status": relation.get("status"),
                        "score": relation.get("confidence"),
                        "signals": {},
                        "source_relation_id": relation.get("relation_id"),
                    })
    for values in result.values():
        values.sort(key=lambda item: (
            -(float(item.get("score")) if item.get("score") is not None else -1.0),
            int(item.get("right_page") or 0),
        ))
        del values[MAX_CANDIDATES_PER_TASK:]
    return result


def _compatible(left: Mapping[str, Any], right: Mapping[str, Any], passports: Mapping[str, Any]) -> bool:
    if left["function_class"] == right["function_class"]:
        return True
    left_passport = passports[str(left["function_id"])]
    right_passport = passports[str(right["function_id"])]
    if "GENERAL_DOCUMENT_FUNCTION" not in {
        left["function_class"], right["function_class"],
    }:
        return False
    return _jaccard(
        [left_passport.get("systems"), left_passport.get("equipment_roles")],
        [right_passport.get("systems"), right_passport.get("equipment_roles")],
    ) >= 0.2


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
    signals = source.get("signals") if isinstance(source.get("signals"), Mapping) else {}
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
        "retrieval_channels": sorted(
            str(key).upper() for key, value in signals.items()
            if value not in (None, 0, 0.0, False, "") and key != "page_proximity"
        ),
        "document_context": {
            "supporting_only": True,
            "included_in_functional_score": False,
            "source_relation_id": source.get("source_relation_id"),
        },
        "source_score": source.get("score"),
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
    options: dict[str, set[str]] = {
        fragment_id: set() for fragment_id in fragments["LEFT"]
    }

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

    for left_page, left_fragments in sorted(by_page["LEFT"].items()):
        for edge in edges.get(left_page, []):
            right_page = int(edge["right_page"])
            for left in left_fragments:
                for right in by_page["RIGHT"].get(right_page, []):
                    if not _compatible(left, right, passport_lookup):
                        continue
                    candidate = _make_candidate(
                        pair_id=pair_id,
                        relation_type="CONTINUED_1_TO_1",
                        components=[component(left, right)],
                        source=edge,
                        passports=passport_lookup,
                    )
                    candidates[candidate["candidate_id"]] = candidate
                    options[left["fragment_id"]].add(candidate["candidate_id"])

    relation_mapping = {"SPLIT": "SPLIT_1_TO_N", "MERGED": "MERGED_N_TO_1"}
    for relation in sheet_relations.get("relations") or []:
        if not isinstance(relation, Mapping):
            continue
        relation_type = relation_mapping.get(str(relation.get("relation_type") or ""))
        left_pages = [int(value) for value in relation.get("left_pages") or []]
        right_pages = [int(value) for value in relation.get("right_pages") or []]
        if not relation_type or not left_pages or not right_pages:
            continue
        for function_class in sorted({
            value["function_class"]
            for page in left_pages for value in by_page["LEFT"].get(page, [])
        }):
            left_values = [
                value for page in left_pages for value in by_page["LEFT"].get(page, [])
                if value["function_class"] == function_class
            ]
            right_values = [
                value for page in right_pages for value in by_page["RIGHT"].get(page, [])
                if value["function_class"] == function_class
            ]
            components = [component(left, right) for left in left_values for right in right_values]
            if not components:
                continue
            candidate = _make_candidate(
                pair_id=pair_id,
                relation_type=relation_type,
                components=components,
                source={
                    "source_relation_id": relation.get("relation_id"),
                    "score": relation.get("confidence"),
                    "signals": {},
                },
                passports=passport_lookup,
            )
            candidates[candidate["candidate_id"]] = candidate
            for left in left_values:
                options[left["fragment_id"]].add(candidate["candidate_id"])

    # A distributed candidate is composed only when several independently
    # extracted LEFT fragments are covered on different RIGHT pages.
    for left_page, left_values in sorted(by_page["LEFT"].items()):
        if len(left_values) < 2:
            continue
        chosen: list[dict[str, Any]] = []
        for left in left_values:
            match = next((
                right
                for edge in edges.get(left_page, [])
                for right in by_page["RIGHT"].get(int(edge["right_page"]), [])
                if _compatible(left, right, passport_lookup)
            ), None)
            if match is not None:
                chosen.append(component(left, match))
        if len(chosen) < 2 or len({value["right_physical_page"] for value in chosen}) < 2:
            continue
        candidate = _make_candidate(
            pair_id=pair_id,
            relation_type="FUNCTION_DISTRIBUTED",
            components=chosen,
            source={"signals": {"function": 1}, "source_relation_id": None},
            passports=passport_lookup,
        )
        candidates[candidate["candidate_id"]] = candidate
        for left in left_values:
            if left["fragment_id"] in candidate["left_fragment_ids"]:
                options[left["fragment_id"]].add(candidate["candidate_id"])

    tasks: list[dict[str, Any]] = []
    for fragment_id, fragment in sorted(
        fragments["LEFT"].items(),
        key=lambda item: (item[1]["physical_page"], item[1]["function_class"], item[0]),
    ):
        candidate_ids = sorted(
            options.get(fragment_id) or [],
            key=lambda candidate_id: (
                0 if candidates[candidate_id]["relation_type"] in COMPLEX_RELATIONS else 1,
                -(float(candidates[candidate_id].get("source_score"))
                  if candidates[candidate_id].get("source_score") is not None else -1.0),
                candidate_id,
            ),
        )[:MAX_CANDIDATES_PER_TASK]
        task_id = stable_id("ltask_", pair_id, fragment_id)
        tasks.append({
            "task_id": task_id,
            "pair_id": pair_id,
            "left_physical_page": fragment["physical_page"],
            "left_function_id": fragment["function_id"],
            "left_fragment_id": fragment_id,
            "candidate_ids": candidate_ids,
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
            "retrieval_channels", "document_context", "evidence_refs",
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
            "page_proximity_is_not_a_functional_signal": True,
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


def verify_capacity(
    selections: Sequence[Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    """Allow page reuse, but reject reuse of one atomic RIGHT fragment."""
    occupants: dict[str, str] = {}
    errors: list[str] = []
    for selection in selections:
        candidate_id = str(selection.get("candidate_id") or "")
        candidate = candidates.get(candidate_id)
        if candidate is None:
            continue
        for capacity_key in candidate.get("right_capacity_keys") or []:
            key = str(capacity_key)
            previous = occupants.get(key)
            # A MERGED candidate is one lineage exposed to several LEFT tasks.
            if previous is None or previous == candidate_id:
                occupants[key] = candidate_id
            else:
                errors.append(
                    f"FUNCTION_FRAGMENT_CONFLICT:{key}:{previous}:{candidate_id}"
                )
    return sorted(set(errors))


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

    capacity_errors = verify_capacity(concrete, dataset.candidates)
    result["global_errors"].extend(capacity_errors)
    result["global_errors"] = sorted(set(result["global_errors"]))
    response_errors = [
        value for value in result["global_errors"]
        if not value.startswith("FUNCTION_FRAGMENT_CONFLICT:")
    ]
    if response_errors:
        for task_result in result["task_results"].values():
            task_result["ok"] = False
    for error in capacity_errors:
        for task_result in result["task_results"].values():
            if str(task_result.get("candidate_id") or "") in error:
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
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    stable: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    analogues: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        candidate_id = str(decision.get("selected_candidate_id") or "")
        candidate = dataset.candidates.get(candidate_id)
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
    stable_lineages, unresolved, analogues = _lineages(dataset, decisions)
    rejections = _rejections(records)
    capacity_errors = sorted({
        error
        for record in records
        for error in (record.get("verification") or {}).get("global_errors") or []
        if str(error).startswith("FUNCTION_FRAGMENT_CONFLICT:")
    })
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
    "build_selector_payload",
    "build_selector_prompt",
    "derive_sheet_map",
    "failure_artifacts",
    "output_schema",
    "run_shadow",
    "stable_consensus",
    "verify_capacity",
    "verify_selector_response",
]
