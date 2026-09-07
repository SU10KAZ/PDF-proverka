"""Feature-gated Human Contour v1 over M1 Stable Domain Keys.

This module owns no generated comparison semantics.  It projects current
AtomicQuestions, resolves human authority through :mod:`decision_registry`,
and appends atomic answer events.  It never mutates matcher output, source
relations, AtomicReviewItems, AtomicChanges, or presentation groups.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from copy import deepcopy
import os
from pathlib import Path
from typing import Any

from .decision_registry import DecisionRegistry, RegistryConflict
from . import paths, review_queue


HUMAN_CONTOUR_FLAG = "HUMAN_CONTOUR_V1_ENABLED"
QUESTION_VISIBILITY_FLAG = "QUESTION_VISIBILITY_V1_ENABLED"
REGISTRY_PATH_ENV = "HUMAN_CONTOUR_DECISION_REGISTRY_PATH"
TRUE_VALUES = frozenset({"1", "true", "yes", "on"})

QUESTION_STATES = frozenset(
    {
        "ACTIONABLE",
        "STALE",
        "RESOLVED",
        "SUPERSEDED",
        "UNAVAILABLE",
        "LOCKED",
        "REQUIRES_REVALIDATION",
    }
)
ACTIONABLE_STATES = frozenset({"ACTIONABLE", "REQUIRES_REVALIDATION"})
ANSWERABLE_STATES = frozenset(
    {"ACTIONABLE", "RESOLVED", "LOCKED", "REQUIRES_REVALIDATION"}
)

TYPE_LABELS = {
    "SHEET_MATCHING": "Сопоставление листов",
    "ENTITY_MATCHING": "Сопоставление объектов",
    "CHANGE_CONFIRMATION": "Подтверждение изменения",
    "MISSING_DATA": "Недостающие данные",
    "CONFLICT": "Противоречие",
}
STATE_LABELS = {
    "ACTIVE": "Действующее решение",
    "ACTIONABLE": "Нужно решение инженера",
    "STALE": "Исходные данные изменились",
    "RESOLVED": "Решение сохранено",
    "SUPERSEDED": "Решение заменено",
    "UNAVAILABLE": "Вопрос недоступен",
    "LOCKED": "Решение зафиксировано",
    "REQUIRES_REVALIDATION": "Нужно подтвердить решение повторно",
}


class HumanContourUnavailable(ValueError):
    """The contour or its version-bound current materialization is absent."""


def _flag(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in TRUE_VALUES


def enabled() -> bool:
    return _flag(HUMAN_CONTOUR_FLAG)


def visibility_enabled() -> bool:
    return _flag(QUESTION_VISIBILITY_FLAG)


def registry_path() -> Path:
    configured = (os.environ.get(REGISTRY_PATH_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return paths.comparison_root_path() / "human_contour" / "decision_registry.sqlite"


def registry() -> DecisionRegistry:
    return DecisionRegistry(registry_path())


def _metadata(materialized: Mapping[str, Any] | None) -> Mapping[str, Any]:
    value = (
        materialized.get("stable_domain_keys")
        if isinstance(materialized, Mapping)
        else None
    )
    return value if isinstance(value, Mapping) else {}


def _question_type(question: Mapping[str, Any]) -> str:
    category = str(question.get("category") or "").upper()
    kind = str(question.get("question_type") or "").upper()
    context = question.get("context")
    context = context if isinstance(context, Mapping) else {}
    reasons = {
        str(value).upper()
        for value in context.get("reason_codes") or ()
    }
    if "CONTEST" in kind or "CONFLICT" in kind:
        return "CONFLICT"
    if (
        "MISSING" in kind
        or any("MISSING" in reason or "UNAVAILABLE" in reason for reason in reasons)
    ):
        return "MISSING_DATA"
    return {
        "SHEET": "SHEET_MATCHING",
        "ENTITY": "ENTITY_MATCHING",
        "CHANGE": "CHANGE_CONFIRMATION",
    }.get(category, "MISSING_DATA")


def _why(question: Mapping[str, Any], question_type: str) -> list[str]:
    context = question.get("context")
    context = context if isinstance(context, Mapping) else {}
    values = context.get("why_proposed") or context.get("reason_text") or []
    if isinstance(values, str):
        values = [values]
    readable = [str(value).strip() for value in values if str(value).strip()]
    if readable:
        return readable
    return {
        "SHEET_MATCHING": ["Автоматическое сопоставление листов неоднозначно."],
        "ENTITY_MATCHING": ["Автоматическое сопоставление объектов неоднозначно."],
        "CHANGE_CONFIRMATION": ["Автоматических данных недостаточно для вывода."],
        "MISSING_DATA": ["Для автоматического решения не хватает исходных данных."],
        "CONFLICT": ["Автоматические варианты противоречат друг другу."],
    }[question_type]


def _evidence(
    question: Mapping[str, Any], metadata: Mapping[str, Any]
) -> dict[str, Any]:
    context = question.get("context")
    context = context if isinstance(context, Mapping) else {}
    return {
        "document_versions": deepcopy(metadata.get("document_versions") or {}),
        "left_sheets": deepcopy(
            context.get("left_sheets") or context.get("left_pages") or []
        ),
        "right_sheets": deepcopy(
            context.get("right_sheets") or context.get("right_pages") or []
        ),
        "fragment_refs": deepcopy(context.get("fragment_refs") or []),
        "bboxes": deepcopy(context.get("bboxes") or []),
        "provenance_keys": list(question.get("provenance_keys") or []),
        "reason_codes": list(context.get("reason_codes") or []),
    }


def _history_summary(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "decision_id": row.get("decision_id"),
            "decision": row.get("decision"),
            "authority": row.get("authority"),
            "author": row.get("author"),
            "created_at": row.get("created_at"),
            "audit_event": row.get("audit_event"),
            "state": row.get("state"),
            "state_label": STATE_LABELS.get(
                str(row.get("state") or ""), "Состояние решения изменилось"
            ),
            "validation_state": row.get("validation_state", "VALID"),
            "supersedes_decision_id": row.get("supersedes_decision_id"),
        }
        for row in rows
    ]


def _answer_from_record(record: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(record, Mapping):
        return None
    payload = record.get("decision_payload")
    payload = dict(payload) if isinstance(payload, Mapping) else {}
    return {
        "answer": record.get("decision"),
        "author": record.get("author"),
        "comment": record.get("comment"),
        "selected_refs": list(payload.get("selected_refs") or []),
        "explicit_candidate": deepcopy(payload.get("explicit_candidate")),
        "typed_resolution": deepcopy(payload.get("typed_resolution")),
        "decision_id": record.get("decision_id"),
        "authority": record.get("authority"),
        "created_at": record.get("created_at"),
    }


def build_read_model(
    response: Mapping[str, Any],
    *,
    state: Mapping[str, Any],
    materialized: Mapping[str, Any] | None,
    decision_registry: DecisionRegistry | None = None,
) -> dict[str, Any]:
    """Decorate the existing question API with explicit visibility states."""
    output = deepcopy(dict(response))
    metadata = _metadata(materialized)
    stable_by_legacy = {
        str(row.get("question_id") or row.get("legacy_id") or ""): row
        for row in (materialized or {}).get("questions") or []
        if isinstance(row, Mapping)
    }
    questions = []
    registry_revision = 0
    histories_by_domain: dict[str, list[dict]] = {}
    use_registry = enabled()
    if use_registry:
        decision_registry = decision_registry or registry()
        registry_revision = decision_registry.revision()
        histories_by_domain = decision_registry.effective_histories(
            [
                str(row.get("domain_key") or "")
                for row in stable_by_legacy.values()
            ],
            "ATOMIC",
        )
    input_signature = str(metadata.get("input_content_signature") or "")
    algorithm_signature = str(metadata.get("algorithm_signature") or "")
    stale_generation = bool(state.get("stale"))

    for raw in output.get("questions") or []:
        if not isinstance(raw, Mapping):
            continue
        question = deepcopy(dict(raw))
        stable = stable_by_legacy.get(str(question.get("question_id") or ""), {})
        for field in (
            "domain_key",
            "domain_key_version",
            "domain_key_sha256",
            "legacy_id",
            "run_instance_key",
            "provenance_keys",
            "provenance_key",
            "occurrence_id",
        ):
            if field in stable:
                question[field] = deepcopy(stable[field])
        domain_key = str(question.get("domain_key") or "")
        history: list[dict] = histories_by_domain.get(domain_key, [])
        resolution: dict[str, Any] = {}
        if stale_generation:
            question_state = "STALE"
        elif use_registry and (
            not domain_key or not input_signature or not algorithm_signature
        ):
            question_state = "UNAVAILABLE"
        elif use_registry:
            assert decision_registry is not None
            resolution = decision_registry.resolve_history(
                history,
                input_signature,
                algorithm_signature,
            )
            resolved_state = str(resolution.get("state") or "UNAVAILABLE")
            if not history:
                question_state = "ACTIONABLE"
            elif resolved_state == "ACTIVE":
                question_state = "RESOLVED"
            else:
                question_state = resolved_state
        else:
            question_state = "ACTIONABLE"
        if question_state not in QUESTION_STATES:
            question_state = "UNAVAILABLE"
        current_record = resolution.get("record") or (history[-1] if history else None)
        question_type = _question_type(question)
        question.update(
            question_state=question_state,
            question_state_label=STATE_LABELS[question_state],
            actionable=question_state in ACTIONABLE_STATES,
            answerable=question_state in ANSWERABLE_STATES,
            visible=True,
            question_class=question_type,
            question_class_label=TYPE_LABELS[question_type],
            decision_scope="ATOMIC",
            decision_state=(
                current_record.get("stored_state", current_record.get("state"))
                if isinstance(current_record, Mapping)
                else None
            ),
            validation_state=resolution.get("validation_state", "VALID"),
            human_answer=(
                _answer_from_record(current_record)
                if question_state in {"RESOLVED", "LOCKED"}
                else None
            ),
            prior_human_answer=(
                _answer_from_record(current_record)
                if current_record is not None
                and question_state not in {"RESOLVED", "LOCKED"}
                else None
            ),
            decision_history=_history_summary(history),
            human_explanation={
                "what_to_decide": str(question.get("prompt") or ""),
                "why_asked": _why(question, question_type),
                "choices": deepcopy(
                    question.get("answer_options") or question.get("options") or []
                ),
                "effect": (
                    "Ответ сохранится только для этого атомарного вопроса; "
                    "связанные пункты не будут утверждены автоматически."
                ),
            },
            evidence=_evidence(question, metadata),
        )
        questions.append(question)

    counts = Counter(str(question["question_state"]) for question in questions)
    output.update(
        questions=questions,
        input_signature=(input_signature if use_registry else output.get("input_signature")),
        revision=(registry_revision if use_registry else output.get("revision", 0)),
        question_visibility_v1_enabled=visibility_enabled() or use_registry,
        human_contour_v1_enabled=use_registry,
        contour_state_counts={
            "atomic_questions_total": len(questions),
            "actionable": sum(bool(question["actionable"]) for question in questions),
            "visible": sum(bool(question["visible"]) for question in questions),
            "stale": counts["STALE"],
            "resolved": counts["RESOLVED"],
            "superseded": counts["SUPERSEDED"],
            "locked": counts["LOCKED"],
            "requires_revalidation": counts["REQUIRES_REVALIDATION"],
            "unavailable": counts["UNAVAILABLE"],
            "hidden_healthy": sum(
                not question["visible"]
                and question["question_state"] == "ACTIONABLE"
                for question in questions
            ),
        },
        resolution_semantics={
            "durable_target": "AtomicQuestion DomainKey",
            "propagation": "ATOMIC_ONLY",
            "atomic_review_item": "UNCHANGED",
            "sheet_relation": "UNCHANGED",
            "entity_relation": "UNCHANGED",
            "atomic_change": "UNCHANGED",
            "presentation_group": "DISPLAY_ONLY",
        },
        authority_order=["HUMAN", "AI", "DETERMINISTIC_SUGGESTION"],
    )
    return output


def append_answers(
    *,
    questions_artifact: Mapping[str, Any],
    state: Mapping[str, Any],
    answers: Iterable[Mapping[str, Any]],
    author: str,
    expected_input_signature: str,
    expected_revision: int,
    session_id: str,
    pair_id: str,
    decision_registry: DecisionRegistry | None = None,
) -> dict[str, Any]:
    """Validate current AtomicQuestions and persist an append-only batch."""
    if not enabled():
        raise HumanContourUnavailable("Human Contour v1 is disabled")
    if state.get("stale"):
        raise RegistryConflict("production result is stale")
    metadata = _metadata(questions_artifact)
    input_signature = str(metadata.get("input_content_signature") or "")
    algorithm_signature = str(metadata.get("algorithm_signature") or "")
    if not input_signature or not algorithm_signature:
        raise HumanContourUnavailable("stable question materialization is unavailable")
    if expected_input_signature != input_signature:
        raise RegistryConflict("question input signature changed")
    rows = [
        row
        for row in questions_artifact.get("questions") or []
        if isinstance(row, Mapping)
    ]
    by_domain = {str(row.get("domain_key") or ""): row for row in rows}
    decision_registry = decision_registry or registry()
    normalized_answers = [dict(answer) for answer in answers]
    if not normalized_answers:
        return {"receipts": [], "revision": decision_registry.revision()}
    records = []
    for answer in normalized_answers:
        domain_key = str(answer.get("domain_key") or "")
        question = by_domain.get(domain_key)
        if not domain_key or question is None:
            raise ValueError("answer must target a current AtomicQuestion DomainKey")
        legacy_id = str(question.get("question_id") or question.get("legacy_id") or "")
        supplied_legacy_id = str(answer.get("question_id") or "")
        if supplied_legacy_id and supplied_legacy_id != legacy_id:
            raise ValueError("legacy question_id does not match domain_key")
        validated = review_queue.record_human_decision(
            question,
            {
                key: deepcopy(value)
                for key, value in answer.items()
                if key
                in {
                    "answer",
                    "comment",
                    "selected_refs",
                    "explicit_candidate",
                    "typed_resolution",
                }
            },
            author=author,
        )
        previous = decision_registry.raw_history(domain_key, "ATOMIC")
        previous_record = previous[-1] if previous else None
        if previous_record is None:
            audit_event = "CREATED"
        elif (
            previous_record.get("based_on_input_content_signature") != input_signature
            or previous_record.get("based_on_algorithm_signature")
            != algorithm_signature
        ):
            audit_event = "REVALIDATED"
        elif answer.get("lock") and previous_record.get("state") != "LOCKED":
            audit_event = "LOCKED"
        else:
            audit_event = "CHANGED"
        records.append(
            {
                "domain_key": domain_key,
                "domain_key_version": question.get("domain_key_version"),
                "target_kind": "ATOMIC_QUESTION",
                "decision_scope": "ATOMIC",
                "decision": validated["answer"],
                "decision_payload": {
                    "selected_refs": validated.get("selected_refs") or [],
                    "explicit_candidate": validated.get("explicit_candidate"),
                    "typed_resolution": validated.get("typed_resolution"),
                },
                "authority": "HUMAN",
                "author": author,
                "comment": validated.get("comment"),
                "based_on_input_content_signature": input_signature,
                "based_on_algorithm_signature": algorithm_signature,
                "based_on_result_signature": metadata.get("result_signature"),
                "evidence_provenance_keys": list(
                    question.get("provenance_keys") or []
                ),
                "state": "LOCKED" if answer.get("lock") else "ACTIVE",
                "validation_state": "VALID",
                "supersedes_decision_id": (
                    previous[-1].get("decision_id") if previous else None
                ),
                "source_instance": {
                    "session_id": session_id,
                    "pair_id": pair_id,
                    "run_id": state.get("run_id"),
                    "legacy_target_id": legacy_id,
                },
                "provenance": {
                    "source": "HUMAN_CONTOUR_V1",
                    "question_class": _question_type(question),
                    "atomic_only": True,
                    "matcher_artifacts_mutated": False,
                },
                "audit_reason": "explicit engineer answer",
                "audit_event": audit_event,
            }
        )
    appended = decision_registry.append_many(
        records,
        expected_revision=expected_revision,
        current_input_signature=input_signature,
        authorized_human_supersession=True,
    )
    revision = decision_registry.revision()
    return {
        "receipts": [
            {
                "decision_id": row["decision_id"],
                "domain_key": row["domain_key"],
                "state": row["state"],
                "authority": row["authority"],
                "created_at": row["created_at"],
                "registry_revision": revision,
            }
            for row in appended
        ],
        "revision": revision,
        "input_signature": input_signature,
        "recompute_scheduled": False,
        "resolution_semantics": "ATOMIC_ONLY",
    }


def authorized_employee(user: Mapping[str, Any] | None) -> bool:
    if not isinstance(user, Mapping):
        return False
    return str(user.get("role") or "").strip().casefold() in {"admin", "expert"}


__all__ = [
    "ACTIONABLE_STATES",
    "ANSWERABLE_STATES",
    "HUMAN_CONTOUR_FLAG",
    "HumanContourUnavailable",
    "QUESTION_STATES",
    "QUESTION_VISIBILITY_FLAG",
    "REGISTRY_PATH_ENV",
    "append_answers",
    "authorized_employee",
    "build_read_model",
    "enabled",
    "registry",
    "registry_path",
    "visibility_enabled",
]
