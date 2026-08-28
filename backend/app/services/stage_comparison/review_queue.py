"""Consolidated SHEET / ENTITY / CHANGE human-review questions.

Questions have stable semantic identities while each question also carries an
input signature for precise staleness checks.  Human answers are stored in a
separate versioned artifact and are applied as a small override layer to the
dependent relation; source analysis artifacts stay immutable and the pipeline
does not need to be rerun.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from copy import deepcopy
from typing import Any

from .production_artifacts import (
    canonical_json,
    content_signature,
    stable_id,
    utc_now,
)
from .unified_change_policy import (
    DIRECTIONS,
    DIMENSIONS,
    OUTCOMES,
    UNKNOWN_DIMENSION,
)


KIND = "stage_comparison_human_review_questions"
SCHEMA_VERSION = "human-review-queue.v3"
BUILDER_VERSION = "consolidated-human-review-queue-v3"
DECISIONS_KIND = "stage_comparison_human_decisions"
DECISIONS_SCHEMA_VERSION = "human-decisions.v2"
DECISIONS_BUILDER_VERSION = "human-decision-store-v2"
APPLICATION_KIND = "stage_comparison_human_decision_applications"
APPLICATION_SCHEMA_VERSION = "human-decision-applications.v2"
APPLICATION_VERSION = "human-decision-application-v2"
CHANGE_TYPED_RESOLUTION_VERSION = "change-typed-resolution.v1"
CATEGORIES = ("SHEET", "ENTITY", "CHANGE")
BASE_ANSWERS = ("YES", "NO", "OTHER", "UNSURE")

_VOLATILE_KEYS = frozenset(
    {"generated_at", "created_at", "updated_at", "timestamp", "stale"}
)
_TYPED_CHANGE_RESOLUTION_FIELDS = frozenset(
    {
        "dimension",
        "subject_ref",
        "project_entity_ref",
        "facet_ref",
        "direction",
        "outcome",
        "before_value",
        "after_value",
        "selected_change_ids",
    }
)
_TYPED_ATOM_RESOLUTION_FIELDS = (
    _TYPED_CHANGE_RESOLUTION_FIELDS - {"selected_change_ids"}
)
_TYPED_CONTESTED_RESOLUTION_FIELDS = frozenset({"selected_change_ids"})


def _stable_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _stable_payload(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in _VOLATILE_KEYS
        }
    if isinstance(value, (set, frozenset)):
        return sorted(
            (_stable_payload(item) for item in value), key=canonical_json
        )
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return [_stable_payload(item) for item in value]
    return value


def _artifact_signature(value: Mapping[str, Any] | None) -> str | None:
    if not isinstance(value, Mapping):
        return None
    direct = value.get("input_signature")
    if isinstance(direct, str) and direct:
        return direct
    provenance = value.get("provenance")
    if isinstance(provenance, Mapping):
        nested = provenance.get("input_signature")
        if isinstance(nested, str) and nested:
            return nested
    return content_signature(_stable_payload(value))


def _ref(item: Mapping[str, Any], *keys: str, prefix: str) -> str:
    for key in keys:
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return stable_id(prefix, _stable_payload(item))


def _option(code: str, label: str, **extra: Any) -> dict[str, Any]:
    return {"code": code, "label": label, **extra}


def _base_options() -> list[dict[str, Any]]:
    return [
        _option("YES", "Да"),
        _option("NO", "Нет"),
        _option("OTHER", "Другой вариант"),
        _option("UNSURE", "Не уверен"),
    ]


def _question(
    *,
    identity: Mapping[str, Any],
    category: str,
    question_type: str,
    prompt: str,
    options: Iterable[Mapping[str, Any]],
    dependencies: Iterable[Mapping[str, Any]],
    dependency_payload: Any,
    context: Mapping[str, Any],
) -> dict[str, Any]:
    if category not in CATEGORIES:
        raise ValueError("unsupported review category")
    options_list = [dict(item) for item in options]
    codes = [item.get("code") for item in options_list]
    if any(not isinstance(code, str) or not code for code in codes):
        raise ValueError("answer option code required")
    if len(codes) != len(set(codes)):
        raise ValueError("duplicate answer option code")
    dependency_list = sorted(
        (dict(item) for item in dependencies),
        key=lambda item: (str(item.get("kind")), str(item.get("ref"))),
    )
    question_id = stable_id(
        "hquestion_", category, question_type, _stable_payload(identity), length=24
    )
    question_input_signature = content_signature(
        {
            "builder": BUILDER_VERSION,
            "category": category,
            "question_type": question_type,
            "dependencies": dependency_list,
            "dependency_payload": _stable_payload(dependency_payload),
            "answer_codes": codes,
        }
    )
    return {
        "question_id": question_id,
        "category": category,
        "question_type": question_type,
        "prompt": prompt,
        "answer_options": options_list,
        "dependencies": dependency_list,
        "dependency_refs": [str(item["ref"]) for item in dependency_list],
        "context": dict(context),
        "input_signature": question_input_signature,
        "status": "PENDING",
    }


def _sheet_candidate_edges(
    relation: Mapping[str, Any],
) -> list[tuple[int, int]]:
    """Return only substantive ambiguous edges, never empty placeholders."""
    status = str(relation.get("status") or "UNKNOWN").upper()
    if status in {"HIGH", "NO_MATCH"}:
        return []
    output: set[tuple[int, int]] = set()
    for edge in relation.get("candidate_edges") or []:
        if not isinstance(edge, Mapping):
            continue
        edge_status = str(edge.get("status") or status).upper()
        substantive = list(edge.get("substantive_signals") or [])
        if edge_status != "POSSIBLE" or not substantive:
            continue
        left_page = int(edge.get("left_page") or 0)
        right_page = int(edge.get("right_page") or 0)
        if left_page > 0 and right_page > 0:
            output.add((left_page, right_page))
    # Synthetic/imported relations need not carry matcher diagnostics.  A
    # POSSIBLE relation with two explicit sides is itself actionable evidence.
    if status == "POSSIBLE" and not output:
        left_pages = {int(page) for page in relation.get("left_pages") or []}
        right_pages = {int(page) for page in relation.get("right_pages") or []}
        output.update(
            (left_page, right_page)
            for left_page in left_pages
            for right_page in right_pages
            if left_page > 0 and right_page > 0
        )
    return sorted(output)


def _sheet_candidate_components(
    relations: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Group POSSIBLE edges into stable bipartite candidate components."""
    records: list[tuple[Mapping[str, Any], str, list[tuple[int, int]]]] = []
    adjacency: dict[tuple[str, int], set[tuple[str, int]]] = defaultdict(set)
    for relation in relations:
        edges = _sheet_candidate_edges(relation)
        if not edges:
            continue
        relation_id = _ref(relation, "relation_id", prefix="srel_")
        records.append((relation, relation_id, edges))
        for left_page, right_page in edges:
            left_node, right_node = ("LEFT", left_page), ("RIGHT", right_page)
            adjacency[left_node].add(right_node)
            adjacency[right_node].add(left_node)

    components = []
    remaining = set(adjacency)
    while remaining:
        start = min(remaining)
        stack = [start]
        nodes: set[tuple[str, int]] = set()
        while stack:
            node = stack.pop()
            if node in nodes:
                continue
            nodes.add(node)
            remaining.discard(node)
            stack.extend(sorted(adjacency[node] - nodes, reverse=True))
        left_pages = sorted(page for side, page in nodes if side == "LEFT")
        right_pages = sorted(page for side, page in nodes if side == "RIGHT")
        component_records = [
            (relation, relation_id, edges)
            for relation, relation_id, edges in records
            if any(
                ("LEFT", left_page) in nodes and ("RIGHT", right_page) in nodes
                for left_page, right_page in edges
            )
        ]
        # Prefer an already two-sided automatic relation as the one relation
        # that a human answer may materialize.  Every other relation remains
        # traceable evidence for the grouped question.
        ranked = sorted(
            component_records,
            key=lambda item: (
                not bool(item[0].get("left_pages") and item[0].get("right_pages")),
                not bool(item[0].get("automatic_scope")),
                -float(item[0].get("confidence") or 0),
                item[1],
            ),
        )
        components.append({
            "left_pages": left_pages,
            "right_pages": right_pages,
            "records": component_records,
            "materialization_relation_id": ranked[0][1],
            "candidate_edges": sorted({
                edge for _relation, _relation_id, edges in component_records
                for edge in edges
            }),
        })
    return sorted(
        components,
        key=lambda item: (
            item["left_pages"], item["right_pages"],
            item["materialization_relation_id"],
        ),
    )


def _cardinality_relation_type(
    left_pages: Iterable[int], right_pages: Iterable[int],
) -> str:
    left, right = list(left_pages), list(right_pages)
    if len(left) == len(right) == 1:
        return "MATCHED"
    if len(left) == 1 and len(right) > 1:
        return "SPLIT"
    if len(left) > 1 and len(right) == 1:
        return "MERGED"
    return "UNCERTAIN"


def _sheet_questions(sheet_relations: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(sheet_relations, Mapping):
        return []
    relations = [
        relation for relation in sheet_relations.get("relations") or []
        if isinstance(relation, Mapping)
    ]
    questions = []
    for component in _sheet_candidate_components(relations):
        left_pages = component["left_pages"]
        right_pages = component["right_pages"]
        relation_type = _cardinality_relation_type(left_pages, right_pages)
        if relation_type == "SPLIT":
            question_type = "SHEET_SPLIT"
            prompt = (
                f"LEFT {left_pages[0]} соответствует группе RIGHT "
                f"{', '.join(map(str, right_pages))}?"
            )
            options = [_option("YES", "Да, вся группа 1→N")]
            options.extend(
                _option(
                    f"SELECT_RIGHT:{page}",
                    f"Только RIGHT {page}",
                    selected_right_pages=[page],
                )
                for page in right_pages
            )
            options.extend(_base_options()[1:])
        elif relation_type == "MERGED":
            question_type = "SHEET_MERGED"
            prompt = (
                f"Группа LEFT {', '.join(map(str, left_pages))} соответствует "
                f"RIGHT {right_pages[0]}?"
            )
            options = [_option("YES", "Да, вся группа N→1")]
            options.extend(
                _option(
                    f"SELECT_LEFT:{page}",
                    f"Только LEFT {page}",
                    selected_left_pages=[page],
                )
                for page in left_pages
            )
            options.extend(_base_options()[1:])
        elif relation_type == "MATCHED":
            question_type = "SHEET_RELATION"
            prompt = (
                f"Листы LEFT {left_pages[0]} и RIGHT {right_pages[0]} "
                "соответствуют?"
            )
            options = _base_options()
        else:
            # Many-to-many evidence is genuinely ambiguous.  It remains one
            # fail-closed question and can only be resolved by rejecting it or
            # supplying one explicit valid MATCHED/SPLIT/MERGED candidate.
            question_type = "SHEET_CANDIDATE_GROUP"
            prompt = (
                f"Как сопоставить группу LEFT {', '.join(map(str, left_pages))} "
                f"с RIGHT {', '.join(map(str, right_pages))}?"
            )
            options = [
                _option("NO", "Нет соответствия"),
                _option("OTHER", "Указать точное соответствие"),
                _option("UNSURE", "Не уверен"),
            ]
        records = component["records"]
        relation_ids = sorted(relation_id for _relation, relation_id, _edges in records)
        dependencies = [
            {
                "kind": "SHEET_RELATION",
                "artifact_kind": sheet_relations.get("kind"),
                "ref": relation_id,
            }
            for relation_id in relation_ids
        ]
        questions.append(_question(
            identity={"left_pages": left_pages, "right_pages": right_pages},
            category="SHEET",
            question_type=question_type,
            prompt=prompt,
            options=options,
            dependencies=dependencies,
            dependency_payload={
                "relations": [relation for relation, _relation_id, _edges in records],
                "candidate_edges": component["candidate_edges"],
            },
            context={
                "relation_id": component["materialization_relation_id"],
                "materialization_relation_id": component[
                    "materialization_relation_id"
                ],
                "candidate_relation_ids": relation_ids,
                "candidate_edges": [
                    {"left_page": left, "right_page": right}
                    for left, right in component["candidate_edges"]
                ],
                "grouped_candidate": True,
                "relation_type": relation_type,
                "left_pages": left_pages,
                "right_pages": right_pages,
                "automatic_status": "POSSIBLE",
            },
        ))
    return questions


def _entity_questions(entity_relations: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(entity_relations, Mapping):
        return []
    by_left: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for relation in entity_relations.get("relations") or []:
        if not isinstance(relation, Mapping):
            continue
        status = relation.get("relation", relation.get("status"))
        if status not in {"POSSIBLE_ENTITY", "UNKNOWN"}:
            continue
        if relation.get("review_required") is False:
            continue
        left_ref = str(relation.get("left_entity_ref") or "").strip()
        if not left_ref:
            left_ref = stable_id("left_entity_", _stable_payload(relation))
        by_left[left_ref].append(relation)

    questions: list[dict[str, Any]] = []
    for left_ref, raw_relations in sorted(by_left.items()):
        relations = sorted(
            raw_relations,
            key=lambda item: (
                int(item.get("candidate_rank") or 10**9),
                str(item.get("right_entity_ref") or ""),
                str(item.get("relation_id") or ""),
            ),
        )
        relation_ids = [
            _ref(item, "relation_id", prefix="erel_") for item in relations
        ]
        right_refs = [str(item.get("right_entity_ref") or "?") for item in relations]
        if len(relations) == 1:
            question_type = "ENTITY_IDENTITY"
            prompt = f"{left_ref} слева и {right_refs[0]} справа — один объект?"
            options = _base_options()
        else:
            question_type = "ENTITY_CANDIDATE_SELECTION"
            prompt = f"Какой объект справа соответствует {left_ref} слева?"
            options = [
                _option(
                    f"SELECT_RIGHT:{right_ref}",
                    f"RIGHT {right_ref}",
                    selected_right_entity_ref=right_ref,
                )
                for right_ref in right_refs
            ]
            options.extend(
                [
                    _option("NO", "Ни один"),
                    _option("OTHER", "Другой кандидат"),
                    _option("UNSURE", "Не уверен"),
                ]
            )
        dependencies = [
            {
                "kind": "ENTITY_RELATION",
                "artifact_kind": entity_relations.get("kind"),
                "ref": relation_id,
            }
            for relation_id in relation_ids
        ]
        context_relations = [
            {
                "relation_id": relation_id,
                "right_entity_ref": right_ref,
                "automatic_relation": relation.get(
                    "relation", relation.get("status")
                ),
                "confidence": relation.get("confidence"),
                "score": relation.get("score"),
            }
            for relation, relation_id, right_ref in zip(
                relations, relation_ids, right_refs
            )
        ]
        questions.append(
            _question(
                identity={"left_entity_ref": left_ref},
                category="ENTITY",
                question_type=question_type,
                prompt=prompt,
                options=options,
                dependencies=dependencies,
                dependency_payload=relations,
                context={
                    "left_entity_ref": left_ref,
                    "candidate_relations": context_relations,
                },
            )
        )
    return questions


def _nested_review_requirements(value: Any) -> list[dict[str, Any]]:
    """Find explicit review-question policy through synthesis wrappers.

    A production TEXT fact is wrapped first by Text Atom Builder and then by
    the synthesizer's review-evidence record.  Imported artifacts can contain
    one additional ``provenance`` wrapper.  Traverse the JSON-shaped payload
    instead of binding question policy to one fragile nesting depth.
    """
    output: dict[str, dict[str, Any]] = {}
    pending: list[tuple[Any, int]] = [(value, 0)]
    seen: set[int] = set()
    while pending:
        current, depth = pending.pop()
        if depth > 8 or not isinstance(current, (Mapping, list, tuple)):
            continue
        marker = id(current)
        if marker in seen:
            continue
        seen.add(marker)
        if isinstance(current, Mapping):
            requirement = current.get("review_requirement")
            if isinstance(requirement, Mapping):
                normalized = dict(requirement)
                output[content_signature(_stable_payload(normalized))] = normalized
            pending.extend(
                (nested, depth + 1)
                for key, nested in current.items()
                if key != "review_requirement"
            )
        else:
            pending.extend((nested, depth + 1) for nested in current)
    return [output[key] for key in sorted(output)]


def _non_actionable_change_review(
    item: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Describe an intentionally non-actionable per-atom review item.

    Suppression is opt-in and fail-closed: an explicit ``True`` at any nested
    provenance level wins over ``False`` and keeps the engineer question.
    Missing policy also keeps the historical question behavior.
    """
    requirements = _nested_review_requirements(item.get("provenance"))
    actionable = [
        value.get("per_atom_question_actionable")
        for value in requirements
        if isinstance(value.get("per_atom_question_actionable"), bool)
    ]
    if False not in actionable or True in actionable:
        return None
    suppressed_requirements = [
        value
        for value in requirements
        if value.get("per_atom_question_actionable") is False
    ]
    reason_codes = sorted({
        str(reason)
        for requirement in suppressed_requirements
        for reason in requirement.get("reason_codes") or []
        if isinstance(reason, str) and reason.strip()
    })
    review_ref = _ref(
        item, "review_evidence_id", "atom_id", prefix="review_evidence_"
    )
    only_sheet_relation = bool(suppressed_requirements) and all(
        requirement.get("only_upstream_relation_blocker") is True
        or {
            reason
            for reason in requirement.get("reason_codes") or []
            if isinstance(reason, str)
        }
        == {"sheet_relation_unconfirmed"}
        for requirement in suppressed_requirements
    )
    return {
        "review_evidence_id": review_ref,
        "atom_id": item.get("atom_id"),
        "reason_codes": reason_codes or ["non_actionable_upstream_review"],
        "only_upstream_relation_blocker": only_sheet_relation,
        "opposite_coverage_gap": (
            "opposite_side_structured_coverage_incomplete" in reason_codes
        ),
    }


def _change_question_plan(
    synthesis: Mapping[str, Any] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(synthesis, Mapping):
        return [], []
    questions: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    for item in synthesis.get("review_items") or []:
        if not isinstance(item, Mapping):
            continue
        suppression = _non_actionable_change_review(item)
        if suppression is not None:
            suppressed.append(suppression)
            continue
        review_ref = _ref(
            item, "review_evidence_id", "atom_id", prefix="review_evidence_"
        )
        dimension = str(item.get("dimension") or "UNKNOWN_DIMENSION")
        required_fields = []
        if dimension == UNKNOWN_DIMENSION:
            required_fields.append("dimension")
        project_ref = item.get("project_entity_ref")
        if not isinstance(project_ref, str) or not project_ref.strip():
            required_fields.append("project_entity_ref")
        provenance = item.get("provenance")
        source_outcome = (
            provenance.get("source_atom_outcome")
            if isinstance(provenance, Mapping)
            else None
        )
        if source_outcome == "REVIEW_REQUIRED":
            required_fields.append("outcome")
        questions.append(
            _question(
                identity={"review_evidence_id": review_ref},
                category="CHANGE",
                question_type="CHANGE_REVIEW_EVIDENCE",
                prompt=f"Подтвердить изменение {review_ref} ({dimension})?",
                options=_base_options(),
                dependencies=[
                    {
                        "kind": "SYNTHESIS_REVIEW_ITEM",
                        "artifact_kind": synthesis.get("kind"),
                        "ref": review_ref,
                    }
                ],
                dependency_payload=item,
                context={
                    "review_evidence_id": review_ref,
                    "atom_id": item.get("atom_id"),
                    "dimension": dimension,
                    "subject_ref": item.get("subject_ref"),
                    "project_entity_ref": item.get("project_entity_ref"),
                    "direction": item.get("direction"),
                    "outcome": item.get("outcome"),
                    "source_atom_outcome": source_outcome,
                    "reason_codes": sorted(item.get("reason_codes") or []),
                    "typed_resolution_contract": {
                        "version": CHANGE_TYPED_RESOLUTION_VERSION,
                        "generic_yes_allowed": not required_fields,
                        "required_fields": required_fields,
                        "accepted_fields": sorted(
                            _TYPED_ATOM_RESOLUTION_FIELDS
                        ),
                    },
                },
            )
        )
    for group in synthesis.get("contested_groups") or []:
        if not isinstance(group, Mapping):
            continue
        group_ref = _ref(group, "group_id", prefix="contest_")
        change_ids = sorted(str(value) for value in group.get("change_ids") or [])
        questions.append(
            _question(
                identity={"contested_group_id": group_ref},
                category="CHANGE",
                question_type="CHANGE_CONTESTED",
                prompt=f"Разрешить противоречие между изменениями {', '.join(change_ids)}?",
                options=_base_options(),
                dependencies=[
                    {
                        "kind": "SYNTHESIS_CONTESTED_GROUP",
                        "artifact_kind": synthesis.get("kind"),
                        "ref": group_ref,
                    }
                ],
                dependency_payload=group,
                context={
                    "contested_group_id": group_ref,
                    "change_ids": change_ids,
                    "reason_codes": sorted(group.get("reason_codes") or []),
                    "typed_resolution_contract": {
                        "version": CHANGE_TYPED_RESOLUTION_VERSION,
                        "generic_yes_allowed": False,
                        "required_fields": ["selected_change_ids"],
                        "accepted_fields": sorted(
                            _TYPED_CONTESTED_RESOLUTION_FIELDS
                        ),
                    },
                },
            )
        )
    suppressed.sort(key=lambda value: value["review_evidence_id"])
    return questions, suppressed


def _change_questions(synthesis: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    return _change_question_plan(synthesis)[0]


def _deduplicate(questions: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for raw in questions:
        question = dict(raw)
        question_id = str(question["question_id"])
        previous = found.get(question_id)
        if previous is None:
            found[question_id] = question
        elif previous["input_signature"] != question["input_signature"]:
            raise ValueError(f"conflicting duplicate question_id {question_id}")
    category_order = {category: index for index, category in enumerate(CATEGORIES)}
    return sorted(
        found.values(),
        key=lambda item: (category_order[item["category"]], item["question_id"]),
    )


def _decision_rows(value: Any) -> list[Mapping[str, Any]]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        rows = value.get("decisions") or []
    else:
        rows = value
    return [item for item in rows if isinstance(item, Mapping)]


def decision_is_stale(
    decision: Mapping[str, Any], question: Mapping[str, Any] | None
) -> bool:
    return (
        not isinstance(question, Mapping)
        or decision.get("question_id") != question.get("question_id")
        or decision.get("question_input_signature")
        != question.get("input_signature")
    )


def _normalized_selected_refs(value: Any) -> list[str]:
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes, Mapping)):
        return []
    return sorted(
        {
            str(item).strip()
            for item in value
            if isinstance(item, (str, int)) and str(item).strip()
        }
    )


def _explicit_entity_ref(decision: Mapping[str, Any]) -> str | None:
    candidate = decision.get("explicit_candidate")
    value = (
        candidate.get("right_entity_ref")
        if isinstance(candidate, Mapping)
        else None
    )
    return value.strip() if isinstance(value, str) and value.strip() else None


def _resolved_entity_candidate_ref(decision: Mapping[str, Any]) -> str | None:
    refs = set(_normalized_selected_refs(decision.get("selected_refs")))
    explicit_ref = _explicit_entity_ref(decision)
    if explicit_ref:
        refs.add(explicit_ref)
    return next(iter(refs)) if len(refs) == 1 else None


def _typed_resolution(decision: Mapping[str, Any]) -> dict[str, Any]:
    value = decision.get("typed_resolution")
    return dict(value) if isinstance(value, Mapping) else {}


def _accepted_typed_resolution_fields(
    question: Mapping[str, Any],
) -> frozenset[str]:
    """Return the fail-closed typed contract for this exact question kind."""
    if question.get("category") != "CHANGE":
        return frozenset()
    if question.get("question_type") == "CHANGE_REVIEW_EVIDENCE":
        return _TYPED_ATOM_RESOLUTION_FIELDS
    if question.get("question_type") == "CHANGE_CONTESTED":
        return _TYPED_CONTESTED_RESOLUTION_FIELDS
    return frozenset()


def _has_semantic_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (Mapping, list, tuple, set, frozenset)):
        return bool(value)
    return True


def _explicit_sheet_candidate_resolves(decision: Mapping[str, Any]) -> bool:
    candidate = decision.get("explicit_candidate")
    if not isinstance(candidate, Mapping):
        return False
    left_pages = candidate.get("left_pages")
    right_pages = candidate.get("right_pages")
    if not isinstance(left_pages, list) or not isinstance(right_pages, list):
        return False
    relation_type = candidate.get("relation_type")
    return (
        relation_type == "MATCHED"
        and len(left_pages) == len(right_pages) == 1
    ) or (
        relation_type == "SPLIT"
        and len(left_pages) == 1
        and len(right_pages) > 1
    ) or (
        relation_type == "MERGED"
        and len(left_pages) > 1
        and len(right_pages) == 1
    )


def _change_resolution_requirements(
    question: Mapping[str, Any], decision: Mapping[str, Any]
) -> list[str]:
    """Return unresolved typed fields; an empty list is safe to close."""
    context = question.get("context")
    context = context if isinstance(context, Mapping) else {}
    typed = _typed_resolution(decision)
    missing: list[str] = []
    accepted_fields = _accepted_typed_resolution_fields(question)
    if any(
        field not in accepted_fields and _has_semantic_value(value)
        for field, value in typed.items()
    ):
        missing.append("typed_resolution")
    if isinstance(decision.get("typed_resolution"), Mapping) and not any(
        _has_semantic_value(value) for value in typed.values()
    ):
        missing.append("typed_resolution")
    if "dimension" in typed and typed.get("dimension") not in DIMENSIONS:
        missing.append("dimension")
    if "direction" in typed and typed.get("direction") not in DIRECTIONS:
        missing.append("direction")
    if "outcome" in typed and (
        typed.get("outcome") not in OUTCOMES
        or typed.get("outcome") == "REVIEW_REQUIRED"
    ):
        missing.append("outcome")
    if question.get("question_type") == "CHANGE_CONTESTED":
        allowed = {
            str(value)
            for value in context.get("change_ids") or []
            if isinstance(value, str) and value
        }
        selected = set(
            _normalized_selected_refs(typed.get("selected_change_ids"))
            or _normalized_selected_refs(decision.get("selected_refs"))
        )
        if not selected or not selected < allowed:
            missing.append("selected_change_ids")
        return sorted(set(missing))

    dimension = context.get("dimension")
    if dimension == UNKNOWN_DIMENSION:
        resolved_dimension = typed.get("dimension")
        if resolved_dimension not in DIMENSIONS or resolved_dimension == UNKNOWN_DIMENSION:
            missing.append("dimension")
    project_ref = context.get("project_entity_ref")
    resolved_project = project_ref
    if not isinstance(project_ref, str) or not project_ref.strip():
        resolved_project = typed.get("project_entity_ref")
        if not isinstance(resolved_project, str) or not resolved_project.strip():
            missing.append("project_entity_ref")
    if context.get("source_atom_outcome") == "REVIEW_REQUIRED":
        resolved_outcome = typed.get("outcome")
        if resolved_outcome not in OUTCOMES or resolved_outcome == "REVIEW_REQUIRED":
            missing.append("outcome")
    subject_ref = context.get("subject_ref")
    if not isinstance(subject_ref, str) or not subject_ref.strip():
        resolved_subject = typed.get("subject_ref") or resolved_project
        if (
            "project_entity_ref" not in missing
            and (
                not isinstance(resolved_subject, str)
                or not resolved_subject.strip()
            )
        ):
            missing.append("subject_ref")
    return sorted(set(missing))


def _decision_resolves_question(
    decision: Mapping[str, Any], question: Mapping[str, Any] | None
) -> bool:
    if decision_is_stale(decision, question):
        return False
    assert isinstance(question, Mapping)
    answer = str(decision.get("answer") or "")
    category = question.get("category")
    if answer == "OTHER":
        if category == "ENTITY":
            return _resolved_entity_candidate_ref(decision) is not None
        if category == "SHEET":
            return _explicit_sheet_candidate_resolves(decision)
        if category == "CHANGE":
            if question.get("question_type") == "CHANGE_CONTESTED":
                return not _change_resolution_requirements(question, decision)
            return bool(_typed_resolution(decision)) and not (
                _change_resolution_requirements(question, decision)
            )
        return False
    if category == "CHANGE" and answer == "YES":
        return not _change_resolution_requirements(question, decision)
    return True


def build_review_queue(
    sheet_relations: Mapping[str, Any] | None = None,
    entity_relations: Mapping[str, Any] | None = None,
    synthesis: Mapping[str, Any] | None = None,
    *,
    human_decisions: Mapping[str, Any] | Iterable[Mapping[str, Any]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build one deduplicated queue and suppress unchanged resolved questions."""
    change_questions, suppressed_change_reviews = _change_question_plan(synthesis)
    all_questions = _deduplicate(
        [
            *_sheet_questions(sheet_relations),
            *_entity_questions(entity_relations),
            *change_questions,
        ]
    )
    questions_by_id = {item["question_id"]: item for item in all_questions}
    decisions = _decision_rows(human_decisions)
    resolved_ids = {
        str(decision.get("question_id"))
        for decision in decisions
        if _decision_resolves_question(
            decision, questions_by_id.get(str(decision.get("question_id")))
        )
    }
    stale_decision_ids = sorted(
        str(decision.get("decision_id") or decision.get("question_id") or "")
        for decision in decisions
        if decision_is_stale(
            decision, questions_by_id.get(str(decision.get("question_id")))
        )
    )
    unresolved_decision_ids = sorted(
        str(decision.get("decision_id") or decision.get("question_id") or "")
        for decision in decisions
        if not decision_is_stale(
            decision, questions_by_id.get(str(decision.get("question_id")))
        )
        and not _decision_resolves_question(
            decision, questions_by_id.get(str(decision.get("question_id")))
        )
    )
    pending = [
        question for question in all_questions if question["question_id"] not in resolved_ids
    ]
    category_counts = Counter(item["category"] for item in pending)
    source_signatures = {
        "sheet_relations": _artifact_signature(sheet_relations),
        "entity_relations": _artifact_signature(entity_relations),
        "synthesis": _artifact_signature(synthesis),
    }
    question_signatures = {
        item["question_id"]: item["input_signature"] for item in all_questions
    }
    # The queue input identity describes every generated question, not merely
    # the pending projection, so supplying decisions cannot make the source
    # analysis appear changed.
    input_signature = content_signature(
        {
            "builder": BUILDER_VERSION,
            "source_signatures": source_signatures,
            "question_signatures": question_signatures,
        }
    )
    counts = {
        "total": len(pending),
        "pending": len(pending),
        "resolved_unchanged": len(resolved_ids),
        "stale_decisions": len(stale_decision_ids),
        "unresolved_decisions": len(unresolved_decision_ids),
        "by_category": {
            category: category_counts.get(category, 0) for category in CATEGORIES
        },
    }
    # Convenience category counters are useful to API/UI clients and retain
    # the explicit A/B/C taxonomy in the persisted artifact.
    counts.update({category: category_counts.get(category, 0) for category in CATEGORIES})
    suppression_reasons = Counter(
        reason
        for item in suppressed_change_reviews
        for reason in item["reason_codes"]
    )
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "builder_version": BUILDER_VERSION,
        "version": 1,
        "input_signature": input_signature,
        "source_signatures": source_signatures,
        "generated_at": generated_at or utc_now(),
        "questions": pending,
        "question_signatures": question_signatures,
        "resolved_question_ids": sorted(resolved_ids),
        "counts": counts,
        "diagnostics": {
            "generated_questions": len(all_questions),
            "deduplicated_questions": len(all_questions),
            "suppressed_change_questions": len(suppressed_change_reviews),
            "suppressed_change_question_reasons": dict(
                sorted(suppression_reasons.items())
            ),
            "suppressed_change_review_item_refs": [
                item["review_evidence_id"] for item in suppressed_change_reviews
            ],
            "upstream_sheet_relation_review_items_suppressed": sum(
                item["only_upstream_relation_blocker"]
                for item in suppressed_change_reviews
            ),
            "opposite_coverage_gap_review_items_suppressed": sum(
                item["opposite_coverage_gap"]
                for item in suppressed_change_reviews
            ),
            "stale_decision_ids": stale_decision_ids,
            "unresolved_decision_ids": unresolved_decision_ids,
            "already_resolved_not_reasked": len(resolved_ids),
            "uses_model": False,
        },
    }


def review_queue_is_stale(
    queue: Mapping[str, Any] | None,
    sheet_relations: Mapping[str, Any] | None = None,
    entity_relations: Mapping[str, Any] | None = None,
    synthesis: Mapping[str, Any] | None = None,
) -> bool:
    if not isinstance(queue, Mapping):
        return True
    current = build_review_queue(
        sheet_relations, entity_relations, synthesis, generated_at="signature-only"
    )
    return (
        queue.get("kind") != KIND
        or queue.get("schema_version") != SCHEMA_VERSION
        or queue.get("input_signature") != current["input_signature"]
    )


def _answer_code(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        raw = value.get("answer", value.get("answer_code"))
        if isinstance(raw, str):
            return raw.strip()
    raise ValueError("human answer code required")


def _normalize_explicit_candidate(
    question: Mapping[str, Any], answer_payload: Mapping[str, Any]
) -> dict[str, Any] | None:
    raw = answer_payload.get("explicit_candidate")
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise ValueError("explicit_candidate must be an object")
    category = question.get("category")
    if category == "ENTITY":
        right_ref = raw.get(
            "right_entity_ref",
            raw.get("entity_ref", raw.get("candidate_ref")),
        )
        if not isinstance(right_ref, str) or not right_ref.strip():
            raise ValueError("explicit entity candidate right_entity_ref required")
        project_ref = raw.get("project_entity_ref")
        if project_ref is not None and (
            not isinstance(project_ref, str) or not project_ref.strip()
        ):
            raise ValueError(
                "explicit entity candidate project_entity_ref must be non-empty"
            )
        return {
            "right_entity_ref": right_ref.strip(),
            "project_entity_ref": (
                project_ref.strip() if isinstance(project_ref, str) else None
            ),
        }
    if category == "SHEET":
        def pages(key: str) -> list[int]:
            values = raw.get(key) or []
            if (
                not isinstance(values, Iterable)
                or isinstance(values, (str, bytes, Mapping))
            ):
                raise ValueError(f"explicit sheet candidate {key} must be an array")
            result = sorted(set(values))
            if any(
                not isinstance(page, int)
                or isinstance(page, bool)
                or page < 1
                for page in result
            ):
                raise ValueError(
                    f"explicit sheet candidate {key} must contain positive pages"
                )
            return result

        left_pages, right_pages = pages("left_pages"), pages("right_pages")
        if not left_pages or not right_pages:
            raise ValueError("explicit sheet candidate requires both sides")
        relation_type = raw.get("relation_type")
        if relation_type is None:
            relation_type = (
                "MATCHED"
                if len(left_pages) == len(right_pages) == 1
                else "SPLIT"
                if len(left_pages) == 1
                else "MERGED"
                if len(right_pages) == 1
                else "UNCERTAIN"
            )
        if relation_type not in {"MATCHED", "SPLIT", "MERGED"}:
            raise ValueError("explicit sheet candidate relation_type unsupported")
        cardinality_valid = (
            relation_type == "MATCHED"
            and len(left_pages) == len(right_pages) == 1
        ) or (
            relation_type == "SPLIT"
            and len(left_pages) == 1
            and len(right_pages) > 1
        ) or (
            relation_type == "MERGED"
            and len(left_pages) > 1
            and len(right_pages) == 1
        )
        if not cardinality_valid:
            raise ValueError(
                "explicit sheet candidate relation_type conflicts with cardinality"
            )
        return {
            "left_pages": left_pages,
            "right_pages": right_pages,
            "relation_type": relation_type,
        }
    raise ValueError("explicit_candidate is supported for ENTITY or SHEET")


def _normalize_typed_resolution(
    question: Mapping[str, Any], answer_payload: Mapping[str, Any]
) -> dict[str, Any] | None:
    raw = answer_payload.get("typed_resolution")
    if raw is None and isinstance(answer_payload.get("resolution"), Mapping):
        raw = answer_payload.get("resolution")
    if raw is None:
        return None
    accepted_fields = _accepted_typed_resolution_fields(question)
    if not isinstance(raw, Mapping) or not set(raw) <= accepted_fields:
        raise ValueError("typed_resolution has unsupported fields")
    result = deepcopy(dict(raw))
    for key in (
        "subject_ref",
        "project_entity_ref",
        "facet_ref",
        "direction",
        "outcome",
    ):
        value = result.get(key)
        if value is not None:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"typed_resolution.{key} must be non-empty")
            result[key] = value.strip()
    if "dimension" in result and result["dimension"] not in DIMENSIONS:
        raise ValueError("typed_resolution.dimension unsupported")
    if "direction" in result and result["direction"] not in DIRECTIONS:
        raise ValueError("typed_resolution.direction unsupported")
    if "outcome" in result and result["outcome"] not in OUTCOMES:
        raise ValueError("typed_resolution.outcome unsupported")
    if result.get("outcome") == "REVIEW_REQUIRED":
        raise ValueError(
            "typed_resolution.outcome must resolve rather than preserve review"
        )
    if "selected_change_ids" in result:
        result["selected_change_ids"] = _normalized_selected_refs(
            result["selected_change_ids"]
        )
    if not result or not any(_has_semantic_value(value) for value in result.values()):
        raise ValueError("typed_resolution must not be semantically empty")
    return result


def record_human_decision(
    question: Mapping[str, Any],
    answer: str | Mapping[str, Any],
    *,
    author: str | None = None,
    comment: str | None = None,
    selected_refs: Iterable[str] | None = None,
    generated_at: str | None = None,
    previous: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create/update one answer bound to this exact version of the question."""
    if not isinstance(question, Mapping):
        raise ValueError("question object required")
    code = _answer_code(answer)
    allowed = {
        str(item.get("code"))
        for item in question.get("answer_options") or []
        if isinstance(item, Mapping)
    }
    if code not in allowed:
        raise ValueError(f"answer {code!r} is not offered by this question")
    answer_payload = answer if isinstance(answer, Mapping) else {}
    resolved_author = author if author is not None else answer_payload.get("author")
    resolved_comment = comment if comment is not None else answer_payload.get("comment")
    explicit_candidate = _normalize_explicit_candidate(question, answer_payload)
    typed_resolution = _normalize_typed_resolution(question, answer_payload)
    raw_selected = _normalized_selected_refs(
        selected_refs
        if selected_refs is not None
        else answer_payload.get("selected_refs")
    )
    if (
        question.get("question_type") == "CHANGE_CONTESTED"
        and code in {"YES", "OTHER"}
    ):
        allowed_change_ids = {
            str(value)
            for value in (question.get("context") or {}).get("change_ids") or []
            if isinstance(value, str) and value
        }
        typed_selected = set(
            _normalized_selected_refs(
                (typed_resolution or {}).get("selected_change_ids")
            )
        )
        legacy_selected = set(raw_selected)
        if typed_selected and legacy_selected and typed_selected != legacy_selected:
            raise ValueError(
                "typed_resolution.selected_change_ids conflicts with selected_refs"
            )
        selected_change_ids = typed_selected or legacy_selected
        if (
            not selected_change_ids
            or not selected_change_ids < allowed_change_ids
        ):
            raise ValueError(
                "selected change IDs must select offered changes as a "
                "non-empty proper subset"
            )
    elif (
        question.get("category") == "CHANGE"
        and raw_selected
    ):
        raise ValueError("selected_refs is unsupported for this question")
    if code.startswith("SELECT_RIGHT:") or code.startswith("SELECT_LEFT:"):
        raw_selected.append(code.split(":", 1)[1])
    if explicit_candidate and question.get("category") == "ENTITY":
        raw_selected.append(explicit_candidate["right_entity_ref"])
    normalized_selected = _normalized_selected_refs(raw_selected)
    if (
        explicit_candidate
        and question.get("category") == "ENTITY"
        and len(set(normalized_selected)) != 1
    ):
        raise ValueError("explicit entity candidate conflicts with selected_refs")
    question_id = str(question.get("question_id") or "").strip()
    question_signature = str(question.get("input_signature") or "").strip()
    if not question_id or not question_signature:
        raise ValueError("question_id and input_signature required")
    decision_id = stable_id(
        "hdecision_", question_id, question_signature, length=24
    )
    now = generated_at or utc_now()
    created_at = now
    if (
        isinstance(previous, Mapping)
        and previous.get("decision_id") == decision_id
        and isinstance(previous.get("created_at"), str)
    ):
        created_at = str(previous["created_at"])
    logical = {
        "decision_id": decision_id,
        "question_id": question_id,
        "question_input_signature": question_signature,
        "category": question.get("category"),
        "answer": code,
        "selected_refs": normalized_selected,
        "explicit_candidate": explicit_candidate,
        "typed_resolution": typed_resolution,
        "author": resolved_author,
        "comment": resolved_comment,
        "question_type": question.get("question_type"),
        "question_context": deepcopy(question.get("context") or {}),
        "dependencies": deepcopy(question.get("dependencies") or []),
    }
    return {
        **logical,
        "decision_input_signature": content_signature(logical),
        "created_at": created_at,
        "updated_at": now,
        "stale": False,
    }


def _answer_entries(answers: Any) -> list[tuple[str, Any]]:
    if isinstance(answers, Mapping):
        return sorted(
            ((str(question_id), value) for question_id, value in answers.items()),
            key=lambda item: item[0],
        )
    output = []
    for item in answers or []:
        if not isinstance(item, Mapping):
            raise ValueError("answer entry must be an object")
        question_id = item.get("question_id")
        if not isinstance(question_id, str) or not question_id.strip():
            raise ValueError("answer.question_id required")
        output.append((question_id.strip(), item))
    return sorted(output, key=lambda item: item[0])


def build_human_decisions(
    review_queue: Mapping[str, Any],
    answers: Mapping[str, Any] | Iterable[Mapping[str, Any]],
    *,
    previous: Mapping[str, Any] | None = None,
    author: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Persist answers separately from questions and source relations."""
    if not isinstance(review_queue, Mapping):
        raise ValueError("review queue artifact required")
    questions = {
        str(item.get("question_id")): item
        for item in review_queue.get("questions") or []
        if isinstance(item, Mapping)
    }
    previous_rows = {
        str(item.get("question_id")): item for item in _decision_rows(previous)
    }
    decisions_by_question = dict(previous_rows)
    now = generated_at or utc_now()
    for question_id, answer in _answer_entries(answers):
        question = questions.get(question_id)
        if question is None:
            raise ValueError(f"unknown or already-resolved question {question_id}")
        decisions_by_question[question_id] = record_human_decision(
            question,
            answer,
            author=author,
            generated_at=now,
            previous=previous_rows.get(question_id),
        )
    decisions = sorted(
        decisions_by_question.values(), key=lambda item: item["decision_id"]
    )
    logical_decisions = [
        {key: value for key, value in decision.items() if key not in _VOLATILE_KEYS}
        for decision in decisions
    ]
    revision = int(previous.get("revision", 0)) + 1 if isinstance(previous, Mapping) else 1
    input_signature = content_signature(
        {
            "builder": DECISIONS_BUILDER_VERSION,
            "review_queue_signature": review_queue.get("input_signature"),
            "decisions": logical_decisions,
        }
    )
    return {
        "kind": DECISIONS_KIND,
        "schema_version": DECISIONS_SCHEMA_VERSION,
        "builder_version": DECISIONS_BUILDER_VERSION,
        "version": 1,
        "revision": revision,
        "input_signature": input_signature,
        "source_review_queue_signature": review_queue.get("input_signature"),
        "generated_at": now,
        "decisions": decisions,
        "stale_semantics": {
            "key": "question_input_signature",
            "rule": "STALE_WHEN_DEPENDENT_QUESTION_INPUT_CHANGES",
        },
    }


def human_decisions_are_stale(
    decisions: Mapping[str, Any] | None, review_queue: Mapping[str, Any] | None
) -> bool:
    if not isinstance(decisions, Mapping) or not isinstance(review_queue, Mapping):
        return True
    signatures = review_queue.get("question_signatures")
    if not isinstance(signatures, Mapping):
        signatures = {
            str(item.get("question_id")): item.get("input_signature")
            for item in review_queue.get("questions") or []
            if isinstance(item, Mapping)
        }
    for decision in _decision_rows(decisions):
        question_id = str(decision.get("question_id") or "")
        if decision.get("question_input_signature") != signatures.get(question_id):
            return True
    return False


def _decision_summary(decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "decision_id": decision.get("decision_id"),
        "question_id": decision.get("question_id"),
        "answer": decision.get("answer"),
        "selected_refs": list(decision.get("selected_refs") or []),
        "explicit_candidate": deepcopy(decision.get("explicit_candidate")),
        "typed_resolution": deepcopy(decision.get("typed_resolution")),
        "author": decision.get("author"),
        "comment": decision.get("comment"),
        "question_input_signature": decision.get("question_input_signature"),
    }


def apply_answer_to_relation(
    relation: Mapping[str, Any],
    question: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    """Return an effective relation copy; the automatic artifact is untouched."""
    if decision_is_stale(decision, question):
        raise ValueError("human decision is stale for this question")
    result = deepcopy(dict(relation))
    answer = str(decision.get("answer") or "")
    category = question.get("category")
    if category == "ENTITY":
        automatic = result.get("relation", result.get("status"))
        right_ref = str(result.get("right_entity_ref") or "")
        selected_other = _resolved_entity_candidate_ref(decision)
        if answer == "YES":
            effective = "SAME_ENTITY"
        elif answer == "NO":
            effective = "DIFFERENT_ENTITY"
        elif answer == "OTHER" and selected_other is not None:
            effective = (
                "SAME_ENTITY"
                if right_ref == selected_other
                else "DIFFERENT_ENTITY"
            )
        elif answer == "OTHER":
            effective = str(automatic or "UNKNOWN")
        elif answer.startswith("SELECT_RIGHT:"):
            selected = answer.split(":", 1)[1]
            effective = "SAME_ENTITY" if right_ref == selected else "DIFFERENT_ENTITY"
        else:
            effective = str(automatic or "UNKNOWN")
        result["automatic_relation"] = automatic
        result["relation"] = effective
        result["status"] = effective
        resolved = answer != "UNSURE" and not (
            answer == "OTHER" and selected_other is None
        )
        result["confidence"] = "HUMAN" if resolved else result.get("confidence")
        result["review_required"] = not resolved
        if effective == "SAME_ENTITY":
            explicit = decision.get("explicit_candidate")
            explicit_project_ref = (
                explicit.get("project_entity_ref")
                if isinstance(explicit, Mapping)
                else None
            )
            result["project_entity_ref"] = (
                explicit_project_ref
                or result.get("project_entity_ref")
                or stable_id(
                    "project_entity_",
                    result.get("left_entity_ref"),
                    result.get("right_entity_ref"),
                    length=24,
                )
            )
            result["unique_candidate"] = True
    elif category == "SHEET":
        automatic_status = result.get("status")
        result["automatic_status"] = automatic_status
        context = question.get("context")
        context = context if isinstance(context, Mapping) else {}
        group_left = sorted({int(page) for page in context.get("left_pages") or []})
        group_right = sorted({int(page) for page in context.get("right_pages") or []})
        if answer == "YES":
            if group_left and group_right:
                result["left_pages"] = group_left
                result["right_pages"] = group_right
                result["relation_type"] = _cardinality_relation_type(
                    group_left, group_right
                )
            result["status"] = "HIGH"
        elif answer == "NO":
            result["status"] = "NO_MATCH"
        elif answer == "OTHER":
            explicit = decision.get("explicit_candidate")
            if _explicit_sheet_candidate_resolves(decision):
                assert isinstance(explicit, Mapping)
                result["left_pages"] = list(explicit.get("left_pages") or [])
                result["right_pages"] = list(explicit.get("right_pages") or [])
                result["relation_type"] = explicit.get("relation_type")
                result["status"] = "HIGH"
            else:
                result["status"] = "POSSIBLE"
                result["relation_type"] = "UNCERTAIN"
        elif answer.startswith("SELECT_RIGHT:"):
            selected = int(answer.split(":", 1)[1])
            if len(group_left) == 1:
                result["left_pages"] = group_left
            result["right_pages"] = [selected]
            result["relation_type"] = "MATCHED"
            result["status"] = "HIGH"
        elif answer.startswith("SELECT_LEFT:"):
            selected = int(answer.split(":", 1)[1])
            result["left_pages"] = [selected]
            if len(group_right) == 1:
                result["right_pages"] = group_right
            result["relation_type"] = "MATCHED"
            result["status"] = "HIGH"
        result["review_required"] = answer == "UNSURE" or (
            answer == "OTHER"
            and not _explicit_sheet_candidate_resolves(decision)
        )
    else:
        raise ValueError("relation answer requires SHEET or ENTITY question")
    result["human_decision"] = _decision_summary(decision)
    result["effective_relation_id"] = stable_id(
        "effective_relation_",
        result.get("relation_id"),
        decision.get("decision_id"),
        answer,
        length=24,
    )
    return result


def apply_human_decisions(
    review_queue: Mapping[str, Any],
    human_decisions: Mapping[str, Any],
    *,
    sheet_relations: Mapping[str, Any] | None = None,
    entity_relations: Mapping[str, Any] | None = None,
    synthesis: Mapping[str, Any] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Apply only dependent overrides; never rerun matching or synthesis."""
    questions = {
        str(item.get("question_id")): item
        for item in review_queue.get("questions") or []
        if isinstance(item, Mapping)
    }
    question_signatures = review_queue.get("question_signatures")
    if not isinstance(question_signatures, Mapping):
        question_signatures = {}
    valid: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    stale_ids: list[str] = []
    for decision in _decision_rows(human_decisions):
        question = questions.get(str(decision.get("question_id") or ""))
        # A queue rebuilt with saved decisions intentionally suppresses the
        # resolved question.  The decision retains its category/dependencies,
        # while ``question_signatures`` still lets us stale-check and apply it.
        if question is None:
            question_id = str(decision.get("question_id") or "")
            signature = question_signatures.get(question_id)
            if signature is not None:
                question = {
                    "question_id": question_id,
                    "input_signature": signature,
                    "category": decision.get("category"),
                    "question_type": decision.get("question_type"),
                    "context": deepcopy(decision.get("question_context") or {}),
                    "dependencies": deepcopy(decision.get("dependencies") or []),
                    "dependency_refs": [
                        str(item.get("ref"))
                        for item in decision.get("dependencies") or []
                        if isinstance(item, Mapping)
                    ],
                }
        if decision_is_stale(decision, question):
            stale_ids.append(str(decision.get("decision_id") or ""))
            continue
        valid.append((decision, question))

    effective_sheet = deepcopy(sheet_relations) if sheet_relations is not None else None
    effective_entity = deepcopy(entity_relations) if entity_relations is not None else None
    question_by_dependency: dict[str, tuple[Mapping[str, Any], Mapping[str, Any]]] = {}
    for decision, question in valid:
        for dependency in question.get("dependencies") or []:
            if isinstance(dependency, Mapping):
                question_by_dependency[str(dependency.get("ref") or "")] = (
                    decision,
                    question,
                )

    if isinstance(effective_sheet, Mapping):
        effective_relations = []
        for relation in effective_sheet.get("relations") or []:
            if not isinstance(relation, Mapping):
                continue
            relation_ref = _ref(relation, "relation_id", prefix="srel_")
            dependent = question_by_dependency.get(relation_ref)
            if dependent is None:
                effective_relations.append(relation)
                continue
            decision, question = dependent
            context = question.get("context")
            context = context if isinstance(context, Mapping) else {}
            materialization_ref = str(
                context.get("materialization_relation_id") or ""
            )
            grouped = bool(context.get("grouped_candidate"))
            answer = str(decision.get("answer") or "")
            resolves_group = (
                answer in {"YES", "NO"}
                or answer.startswith("SELECT_RIGHT:")
                or answer.startswith("SELECT_LEFT:")
                or (
                    answer == "OTHER"
                    and _explicit_sheet_candidate_resolves(decision)
                )
            )
            if grouped and relation_ref != materialization_ref:
                if not resolves_group:
                    effective_relations.append(relation)
                    continue
                superseded = deepcopy(dict(relation))
                superseded["automatic_status"] = relation.get("status")
                superseded["status"] = "CANDIDATE_SUPERSEDED"
                superseded["review_required"] = False
                superseded["superseded_by_relation_id"] = materialization_ref
                superseded["human_decision"] = _decision_summary(decision)
                superseded["effective_relation_id"] = stable_id(
                    "effective_relation_",
                    relation_ref,
                    decision.get("decision_id"),
                    "CANDIDATE_SUPERSEDED",
                    length=24,
                )
                effective_relations.append(superseded)
                continue
            effective_relations.append(
                apply_answer_to_relation(relation, question, decision)
            )
        effective_sheet["relations"] = effective_relations
    if isinstance(effective_entity, Mapping):
        effective_entity["relations"] = [
            apply_answer_to_relation(relation, *question_by_dependency[relation_ref][::-1])
            if relation_ref in question_by_dependency
            else relation
            for relation in effective_entity.get("relations") or []
            if isinstance(relation, Mapping)
            for relation_ref in [_ref(relation, "relation_id", prefix="erel_")]
        ]
        # ``OTHER`` can identify a legitimate candidate that was absent from
        # the automatic top-k set.  Materialize a separate effective relation
        # from that exact ref; never alter or invent evidence on the automatic
        # relation artifact.
        for decision, question in valid:
            if question.get("category") != "ENTITY" or decision.get("answer") != "OTHER":
                continue
            selected_ref = _resolved_entity_candidate_ref(decision)
            context = question.get("context")
            left_ref = (
                context.get("left_entity_ref")
                if isinstance(context, Mapping)
                else None
            )
            if (
                selected_ref is None
                or not isinstance(left_ref, str)
                or not left_ref.strip()
            ):
                continue
            left_ref = left_ref.strip()
            if any(
                item.get("left_entity_ref") == left_ref
                and item.get("right_entity_ref") == selected_ref
                for item in effective_entity["relations"]
            ):
                continue
            related = [
                item
                for item in effective_entity["relations"]
                if item.get("left_entity_ref") == left_ref
            ]
            left_project_refs = {
                str(item.get("left_project_entity_ref")).strip()
                for item in related
                if isinstance(item.get("left_project_entity_ref"), str)
                and str(item.get("left_project_entity_ref")).strip()
            }
            base_relation = {
                "relation_id": stable_id("erel_", left_ref, selected_ref),
                "left_entity_ref": left_ref,
                "right_entity_ref": selected_ref,
                "left_project_entity_ref": (
                    next(iter(left_project_refs))
                    if len(left_project_refs) == 1
                    else None
                ),
                "right_project_entity_ref": None,
                "project_entity_ref": None,
                "relation": "UNKNOWN",
                "status": "UNKNOWN",
                "confidence": "UNKNOWN",
                "score": None,
                "candidate_rank": None,
                "unique_candidate": False,
                "strong_signals": [],
                "conflicting_signals": [],
                "mismatched_signals": [],
                "evidence": [],
                "review_required": True,
                "provenance": {
                    "algorithm": APPLICATION_VERSION,
                    "candidate_source": "HUMAN_EXPLICIT_REFERENCE",
                    "name_is_primary": False,
                    "ai_final_decision": False,
                },
            }
            effective_entity["relations"].append(
                apply_answer_to_relation(base_relation, question, decision)
            )
        effective_entity["relations"].sort(
            key=lambda item: str(item.get("relation_id") or "")
        )
        counts = Counter(
            str(item.get("relation", item.get("status")) or "UNKNOWN")
            for item in effective_entity["relations"]
        )
        diagnostics = dict(effective_entity.get("diagnostics") or {})
        diagnostics["relation_counts"] = {
            relation: counts.get(relation, 0)
            for relation in (
                "DIFFERENT_ENTITY",
                "POSSIBLE_ENTITY",
                "SAME_ENTITY",
                "UNKNOWN",
            )
        }
        diagnostics["remaining_review_relations"] = sum(
            bool(item.get("review_required"))
            for item in effective_entity["relations"]
        )
        diagnostics["human_decision_overrides_applied"] = sum(
            "human_decision" in item for item in effective_entity["relations"]
        )
        effective_entity["diagnostics"] = diagnostics

    change_resolutions = []
    for decision, question in valid:
        if question.get("category") != "CHANGE":
            continue
        answer = str(decision.get("answer") or "")
        missing_fields = _change_resolution_requirements(question, decision)
        typed = _typed_resolution(decision)
        if answer == "NO":
            resolution = "REJECTED"
        elif answer == "YES" and not missing_fields:
            resolution = "CONFIRMED"
        elif (
            answer == "OTHER"
            and not missing_fields
            and (
                bool(typed)
                or question.get("question_type") == "CHANGE_CONTESTED"
            )
        ):
            resolution = "TYPED_RESOLUTION"
        else:
            resolution = "REVIEW_REQUIRED"
        change_resolutions.append(
            {
                "question_id": question.get("question_id"),
                "dependency_refs": list(question.get("dependency_refs") or []),
                "resolution": resolution,
                "resolution_complete": resolution == "REJECTED"
                or (
                    not missing_fields
                    and resolution in {"CONFIRMED", "TYPED_RESOLUTION"}
                ),
                "missing_typed_fields": missing_fields,
                "typed_resolution": typed or None,
                "decision": _decision_summary(decision),
            }
        )
    change_resolutions.sort(key=lambda item: str(item["question_id"]))
    applied_ids = sorted(
        str(decision.get("decision_id")) for decision, _question in valid
    )
    unresolved_ids = sorted(
        str(decision.get("decision_id"))
        for decision, question in valid
        if not _decision_resolves_question(decision, question)
    )
    source_signatures = {
        "review_queue": review_queue.get("input_signature"),
        "human_decisions": human_decisions.get("input_signature"),
        "sheet_relations": _artifact_signature(sheet_relations),
        "entity_relations": _artifact_signature(entity_relations),
        "synthesis": _artifact_signature(synthesis),
    }
    input_signature = content_signature(
        {
            "application": APPLICATION_VERSION,
            "source_signatures": source_signatures,
            "applied_decision_ids": applied_ids,
        }
    )
    return {
        "kind": APPLICATION_KIND,
        "schema_version": APPLICATION_SCHEMA_VERSION,
        "application_version": APPLICATION_VERSION,
        "version": 1,
        "input_signature": input_signature,
        "source_signatures": source_signatures,
        "generated_at": generated_at or utc_now(),
        "effective_sheet_relations": effective_sheet,
        "effective_entity_relations": effective_entity,
        "change_resolutions": change_resolutions,
        "applied_decision_ids": applied_ids,
        "stale_decision_ids": sorted(stale_ids),
        "unresolved_decision_ids": unresolved_ids,
        "diagnostics": {
            "pipeline_rerun": False,
            "automatic_artifacts_mutated": False,
            "applied_decisions": len(applied_ids),
            "unresolved_decisions": len(unresolved_ids),
            "uses_model": False,
        },
    }


build_human_review_queue = build_review_queue
record_human_decisions = build_human_decisions
apply_human_decision = apply_answer_to_relation


__all__ = [
    "APPLICATION_KIND",
    "APPLICATION_SCHEMA_VERSION",
    "APPLICATION_VERSION",
    "BASE_ANSWERS",
    "BUILDER_VERSION",
    "CATEGORIES",
    "CHANGE_TYPED_RESOLUTION_VERSION",
    "DECISIONS_BUILDER_VERSION",
    "DECISIONS_KIND",
    "DECISIONS_SCHEMA_VERSION",
    "KIND",
    "SCHEMA_VERSION",
    "apply_answer_to_relation",
    "apply_human_decision",
    "apply_human_decisions",
    "build_human_decisions",
    "build_human_review_queue",
    "build_review_queue",
    "decision_is_stale",
    "human_decisions_are_stale",
    "record_human_decision",
    "record_human_decisions",
    "review_queue_is_stale",
]
