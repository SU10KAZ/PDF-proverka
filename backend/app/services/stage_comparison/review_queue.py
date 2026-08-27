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


KIND = "stage_comparison_human_review_questions"
SCHEMA_VERSION = "human-review-queue.v1"
BUILDER_VERSION = "consolidated-human-review-queue-v1"
DECISIONS_KIND = "stage_comparison_human_decisions"
DECISIONS_SCHEMA_VERSION = "human-decisions.v1"
DECISIONS_BUILDER_VERSION = "human-decision-store-v1"
APPLICATION_KIND = "stage_comparison_human_decision_applications"
APPLICATION_SCHEMA_VERSION = "human-decision-applications.v1"
APPLICATION_VERSION = "human-decision-application-v1"
CATEGORIES = ("SHEET", "ENTITY", "CHANGE")
BASE_ANSWERS = ("YES", "NO", "OTHER", "UNSURE")

_VOLATILE_KEYS = frozenset(
    {"generated_at", "created_at", "updated_at", "timestamp", "stale"}
)


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


def _sheet_questions(sheet_relations: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(sheet_relations, Mapping):
        return []
    questions = []
    for relation in sheet_relations.get("relations") or []:
        if not isinstance(relation, Mapping):
            continue
        relation_type = str(relation.get("relation_type") or "UNCERTAIN")
        status = str(relation.get("status") or "UNKNOWN")
        if relation_type not in {"SPLIT", "MERGED", "UNCERTAIN"} and status not in {
            "POSSIBLE",
            "UNKNOWN",
        }:
            continue
        relation_id = _ref(relation, "relation_id", prefix="srel_")
        left_pages = sorted({int(page) for page in relation.get("left_pages") or []})
        right_pages = sorted({int(page) for page in relation.get("right_pages") or []})
        if relation_type == "SPLIT":
            question_type = "SHEET_SPLIT"
            prompt = (
                f"LEFT {', '.join(map(str, left_pages))} разделён на RIGHT "
                f"{', '.join(map(str, right_pages))}?"
            )
            options = [_option("YES", "Да, 1→N")]
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
                f"LEFT {', '.join(map(str, left_pages))} объединены в RIGHT "
                f"{', '.join(map(str, right_pages))}?"
            )
            options = [_option("YES", "Да, N→1")]
            options.extend(
                _option(
                    f"SELECT_LEFT:{page}",
                    f"Только LEFT {page}",
                    selected_left_pages=[page],
                )
                for page in left_pages
            )
            options.extend(_base_options()[1:])
        else:
            question_type = "SHEET_RELATION"
            prompt = (
                f"Листы LEFT {', '.join(map(str, left_pages))} и RIGHT "
                f"{', '.join(map(str, right_pages))} соответствуют?"
            )
            options = _base_options()
        questions.append(
            _question(
                identity={"relation_id": relation_id},
                category="SHEET",
                question_type=question_type,
                prompt=prompt,
                options=options,
                dependencies=[
                    {
                        "kind": "SHEET_RELATION",
                        "artifact_kind": sheet_relations.get("kind"),
                        "ref": relation_id,
                    }
                ],
                dependency_payload=relation,
                context={
                    "relation_id": relation_id,
                    "relation_type": relation_type,
                    "left_pages": left_pages,
                    "right_pages": right_pages,
                    "automatic_status": status,
                },
            )
        )
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


def _change_questions(synthesis: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(synthesis, Mapping):
        return []
    questions: list[dict[str, Any]] = []
    for item in synthesis.get("review_items") or []:
        if not isinstance(item, Mapping):
            continue
        review_ref = _ref(
            item, "review_evidence_id", "atom_id", prefix="review_evidence_"
        )
        dimension = str(item.get("dimension") or "UNKNOWN_DIMENSION")
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
                    "reason_codes": sorted(item.get("reason_codes") or []),
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
                },
            )
        )
    return questions


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


def build_review_queue(
    sheet_relations: Mapping[str, Any] | None = None,
    entity_relations: Mapping[str, Any] | None = None,
    synthesis: Mapping[str, Any] | None = None,
    *,
    human_decisions: Mapping[str, Any] | Iterable[Mapping[str, Any]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build one deduplicated queue and suppress unchanged resolved questions."""
    all_questions = _deduplicate(
        [
            *_sheet_questions(sheet_relations),
            *_entity_questions(entity_relations),
            *_change_questions(synthesis),
        ]
    )
    questions_by_id = {item["question_id"]: item for item in all_questions}
    decisions = _decision_rows(human_decisions)
    resolved_ids = {
        str(decision.get("question_id"))
        for decision in decisions
        if not decision_is_stale(
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
        "by_category": {
            category: category_counts.get(category, 0) for category in CATEGORIES
        },
    }
    # Convenience category counters are useful to API/UI clients and retain
    # the explicit A/B/C taxonomy in the persisted artifact.
    counts.update({category: category_counts.get(category, 0) for category in CATEGORIES})
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
            "stale_decision_ids": stale_decision_ids,
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
    raw_selected = (
        list(selected_refs)
        if selected_refs is not None
        else list(answer_payload.get("selected_refs") or [])
    )
    if code.startswith("SELECT_RIGHT:") or code.startswith("SELECT_LEFT:"):
        raw_selected.append(code.split(":", 1)[1])
    normalized_selected = sorted(
        {str(value).strip() for value in raw_selected if str(value).strip()}
    )
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
        "author": resolved_author,
        "comment": resolved_comment,
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
        if answer == "YES":
            effective = "SAME_ENTITY"
        elif answer in {"NO", "OTHER"}:
            effective = "DIFFERENT_ENTITY"
        elif answer.startswith("SELECT_RIGHT:"):
            selected = answer.split(":", 1)[1]
            effective = "SAME_ENTITY" if right_ref == selected else "DIFFERENT_ENTITY"
        else:
            effective = str(automatic or "UNKNOWN")
        result["automatic_relation"] = automatic
        result["relation"] = effective
        result["status"] = effective
        result["confidence"] = "HUMAN" if answer != "UNSURE" else result.get("confidence")
        result["review_required"] = answer == "UNSURE"
        if effective == "SAME_ENTITY":
            result["project_entity_ref"] = result.get("project_entity_ref") or stable_id(
                "project_entity_",
                result.get("left_entity_ref"),
                result.get("right_entity_ref"),
                length=24,
            )
            result["unique_candidate"] = True
    elif category == "SHEET":
        automatic_status = result.get("status")
        result["automatic_status"] = automatic_status
        if answer == "YES":
            result["status"] = "HIGH"
        elif answer == "NO":
            result["status"] = "NO_MATCH"
        elif answer == "OTHER":
            result["status"] = "POSSIBLE"
            result["relation_type"] = "UNCERTAIN"
        elif answer.startswith("SELECT_RIGHT:"):
            selected = int(answer.split(":", 1)[1])
            result["right_pages"] = [selected]
            result["relation_type"] = "MATCHED"
            result["status"] = "HIGH"
        elif answer.startswith("SELECT_LEFT:"):
            selected = int(answer.split(":", 1)[1])
            result["left_pages"] = [selected]
            result["relation_type"] = "MATCHED"
            result["status"] = "HIGH"
        result["review_required"] = answer == "UNSURE"
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
        effective_sheet["relations"] = [
            apply_answer_to_relation(relation, *question_by_dependency[relation_ref][::-1])
            if relation_ref in question_by_dependency
            else relation
            for relation in effective_sheet.get("relations") or []
            if isinstance(relation, Mapping)
            for relation_ref in [_ref(relation, "relation_id", prefix="srel_")]
        ]
    if isinstance(effective_entity, Mapping):
        effective_entity["relations"] = [
            apply_answer_to_relation(relation, *question_by_dependency[relation_ref][::-1])
            if relation_ref in question_by_dependency
            else relation
            for relation in effective_entity.get("relations") or []
            if isinstance(relation, Mapping)
            for relation_ref in [_ref(relation, "relation_id", prefix="erel_")]
        ]
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
        resolution = {
            "YES": "CONFIRMED",
            "NO": "REJECTED",
            "OTHER": "OTHER",
            "UNSURE": "REVIEW_REQUIRED",
        }.get(answer, "REVIEW_REQUIRED")
        change_resolutions.append(
            {
                "question_id": question.get("question_id"),
                "dependency_refs": list(question.get("dependency_refs") or []),
                "resolution": resolution,
                "decision": _decision_summary(decision),
            }
        )
    change_resolutions.sort(key=lambda item: str(item["question_id"]))
    applied_ids = sorted(
        str(decision.get("decision_id")) for decision, _question in valid
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
        "diagnostics": {
            "pipeline_rerun": False,
            "automatic_artifacts_mutated": False,
            "applied_decisions": len(applied_ids),
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
