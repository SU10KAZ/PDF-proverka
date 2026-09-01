"""Fail-closed production execution of bounded HRO question closure."""
from __future__ import annotations

import copy
import re
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ..ai import gateway
from ..ai_v3 import schemas as v3_schemas
from ..ai_v3 import settings as v3_settings
from ..ai_v3.candidate_factory import build_candidate_factory
from ..production_artifacts import content_signature, utc_now
from . import schemas, settings
from .selector import QuestionClosureSelector


PRODUCTION_SCHEMA_VERSION = "stage-comparison-question-closure-production.v1"
POLICY_VERSION = "n-pe-bus-requirement-closure-policy.v1"
ALLOWED_CANDIDATE_TYPES = (
    "DIFFERENT_REQUIREMENT",
    "INSUFFICIENT_EVIDENCE",
    "SAME_REQUIREMENT",
)
AUTO_CLOSING_EFFECTS = frozenset({"RESOLVE_HUMAN_QUESTION"})
FAST_SIGNATURE_KEYS = (
    "direct_page_mode2",
    "document_inconsistencies",
    "electrical_table_changes",
    "unified_synthesis",
    "text_preparation",
    "sheet_relations",
    "ai_routing_inventory",
    "text_atoms",
    "bound_atoms",
    "graphic_change_ledger",
    "entity_relations",
)
_TOKEN = re.compile(r"(?<![0-9a-zа-я])(n|pe|ре)(?![0-9a-zа-я])", re.IGNORECASE)


def _objects(values: Any) -> list[Mapping[str, Any]]:
    return [value for value in values or () if isinstance(value, Mapping)]


def fast_signature(artifacts: Mapping[str, Mapping[str, Any]]) -> str:
    """Digest only immutable FAST/evidence inputs, never progress timestamps."""
    return content_signature({
        key: artifacts.get(key) or {}
        for key in FAST_SIGNATURE_KEYS
    })


def hro_question_signature(plan: Mapping[str, Any]) -> str:
    return content_signature({
        "plan_input_signature": plan.get("input_signature"),
        "groups": list(plan.get("groups") or ()),
        "standalone_questions": list(plan.get("standalone_questions") or ()),
    })


def _question_rows(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for group in _objects(plan.get("groups")):
        rows.append({
            "question_id": str(group.get("group_id") or ""),
            "question_type": str(group.get("decision_type") or "HUMAN_REVIEW_GROUP"),
            "source_kind": "HumanReviewGroup",
            "source": group,
        })
    for question in _objects(plan.get("standalone_questions")):
        rows.append({
            "question_id": str(question.get("question_id") or ""),
            "question_type": str(question.get("decision_type") or "HUMAN_QUESTION"),
            "source_kind": "StandaloneHumanQuestion",
            "source": question,
        })
    return rows


def _human_protected_question_ids(decisions: Mapping[str, Any] | None) -> set[str]:
    protected = {
        str(value.get("question_id") or "")
        for value in _objects((decisions or {}).get("standalone_answers"))
    }
    protected.update(
        str(value.get("question_id") or "")
        for value in _objects((decisions or {}).get("closure_overrides"))
    )
    return protected - {""}


def _tasks_by_question(factory: Mapping[str, Any]) -> dict[str, list[Mapping[str, Any]]]:
    output: dict[str, list[Mapping[str, Any]]] = {}
    for task in _objects(factory.get("tasks")):
        question_id = str(task.get("human_question_id") or "")
        if question_id:
            output.setdefault(question_id, []).append(task)
    return output


def _candidate_by_type(task: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(value.get("candidate_type") or ""): value
        for value in _objects(task.get("candidates"))
    }


def _text_tokens(values: Sequence[Any]) -> set[str]:
    return {
        match.group(1).casefold()
        for value in values
        for match in _TOKEN.finditer(str(value or ""))
    }


def _is_bounded_n_pe_bus_requirement(task: Mapping[str, Any]) -> bool:
    """Route by evidence shape, never by acceptance IDs or question titles."""
    if (
        task.get("task_type") != v3_schemas.TEXT_EQUIVALENCE
        or task.get("source_kind") != "TEXT_REQUIREMENT_EQUIVALENCE"
    ):
        return False
    candidates = _candidate_by_type(task)
    if not set(ALLOWED_CANDIDATE_TYPES) <= set(candidates):
        return False
    semantic = candidates["DIFFERENT_REQUIREMENT"]
    features = semantic.get("deterministic_features") or {}
    left_texts = list(features.get("left_texts") or ())
    right_texts = list(features.get("right_texts") or ())
    left_tokens = _text_tokens(left_texts)
    right_tokens = _text_tokens(right_texts)
    pe_tokens = {"pe", "ре"}
    right_raw = " ".join(map(str, right_texts)).casefold()
    return (
        bool(left_tokens & pe_tokens)
        and "n" not in left_tokens
        and "n" in right_tokens
        and bool(right_tokens & pe_tokens)
        and "шин" in right_raw
        and bool(semantic.get("left_refs"))
        and bool(semantic.get("right_refs"))
    )


def build_production_contracts(
    *,
    hro_plan: Mapping[str, Any],
    factory: Mapping[str, Any],
    human_decisions: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Classify every HRO interaction before any selector call."""
    questions = _question_rows(hro_plan)
    baseline = int(
        (hro_plan.get("summary") or {}).get("mandatory_human_interactions")
        or len(questions)
    )
    if len(questions) != baseline:
        raise ValueError(
            f"Question Closure accounting failed: questions={len(questions)}, "
            f"baseline={baseline}"
        )
    protected = _human_protected_question_ids(human_decisions)
    tasks_by_question = _tasks_by_question(factory)
    contracts = []
    routes = []
    for question in questions:
        source = question["source"]
        question_id = question["question_id"]
        qtype = question["question_type"]
        evidence_refs = sorted({
            str(value.get("evidence_ref") or value.get("target_id") or "")
            for value in _objects(source.get("evidence_refs"))
            if value.get("evidence_ref") or value.get("target_id")
        })
        contract = {
            "question_id": question_id,
            "question_type": qtype,
            "source_kind": question["source_kind"],
            "title": source.get("title"),
            "question": source.get("question"),
            "required_subproblems": [],
            "required_evidence": evidence_refs,
            "closure_conditions": [
                "QUESTION_CONTRACT_ALLOWS_AUTO_CLOSURE",
                "BACKEND_GENERATED_CANDIDATE_ONLY",
                "SELECTED_CANDIDATE_EXISTS",
                "VERIFIER_STATUS_IS_VERIFIED_SELECTION",
                "PASS_1_EQUALS_PASS_2",
                "HUMAN_DECISION_ABSENT",
                "POLICY_PROTECTED_QUESTION_ABSENT",
            ],
            "blocking_conditions": [],
            "affected_atomic_ids": sorted(map(
                str, source.get("affected_target_ids") or ()
            )),
            "current_status": schemas.OPEN,
            "auto_closure_allowed": False,
            "policy_version": POLICY_VERSION,
        }
        if question_id in protected:
            contract["current_status"] = schemas.BLOCKED_POLICY
            contract["blocking_conditions"] = ["HUMAN_DECISION_HAS_PRIORITY"]
        elif question["source_kind"] != "StandaloneHumanQuestion":
            contract["current_status"] = schemas.BLOCKED_POLICY
            contract["blocking_conditions"] = ["GROUP_OR_POLICY_PROTECTED"]
        elif qtype != "TEXT_REQUIREMENT_EQUIVALENCE":
            contract["current_status"] = (
                schemas.BLOCKED_AMBIGUOUS_EVIDENCE
                if qtype == "TABLE_ROW_IDENTITY"
                else schemas.BLOCKED_MISSING_EVIDENCE
            )
            contract["blocking_conditions"] = ["NO_PRODUCTION_CLOSURE_POLICY"]
        else:
            related = tasks_by_question.get(question_id) or []
            eligible = [task for task in related if _is_bounded_n_pe_bus_requirement(task)]
            if len(related) == 1 and len(eligible) == 1:
                task = eligible[0]
                candidates = _candidate_by_type(task)
                contract["auto_closure_allowed"] = True
                contract["required_subproblems"] = ["BOUNDED_N_PE_REQUIREMENT_RELATION"]
                contract["required_evidence"].extend(sorted({
                    str(ref)
                    for candidate_type in ALLOWED_CANDIDATE_TYPES
                    for ref in candidates[candidate_type].get("text_refs") or ()
                }))
                routes.append({
                    "question_id": question_id,
                    "task_id": task.get("task_id"),
                    "task_type": task.get("task_type"),
                    "route_to_ai": True,
                    "reason": "BOUNDED_N_PE_REQUIREMENT_RELATION",
                    "allowed_candidate_types": list(ALLOWED_CANDIDATE_TYPES),
                    "candidate_ids": {
                        candidate_type: candidates[candidate_type].get("candidate_id")
                        for candidate_type in ALLOWED_CANDIDATE_TYPES
                    },
                    "source_task_signature": task.get("task_signature"),
                })
            else:
                contract["current_status"] = schemas.BLOCKED_MISSING_EVIDENCE
                contract["blocking_conditions"] = [
                    "EVIDENCE_SHAPE_NOT_ALLOWED_BY_PRODUCTION_POLICY"
                ]
        contract["required_evidence"] = sorted(set(contract["required_evidence"]))
        contracts.append(contract)

    hro_signature = hro_question_signature(hro_plan)
    contract_core = {
        "kind": "stage_comparison_question_closure_contracts",
        "schema_version": PRODUCTION_SCHEMA_VERSION,
        "closure_layer_version": settings.CLOSURE_LAYER_VERSION,
        "policy_version": POLICY_VERSION,
        "pair_id": hro_plan.get("pair_id"),
        "baseline_hro": baseline,
        "hro_question_signature": hro_signature,
        "candidate_set_signature": factory.get("candidate_set_signature"),
        "contracts": contracts,
    }
    contract_signature = content_signature(contract_core)
    contracts_artifact = {
        **contract_core,
        "closure_contract_signature": contract_signature,
        "summary": {
            "contracts": len(contracts),
            "eligible": len(routes),
            "closed": 0,
            "remaining_hro": baseline,
            "exact_accounting": len(contracts) == baseline,
        },
    }
    tasks_artifact = {
        "kind": "stage_comparison_question_closure_ai_tasks",
        "schema_version": PRODUCTION_SCHEMA_VERSION,
        "closure_layer_version": settings.CLOSURE_LAYER_VERSION,
        "closure_contract_signature": contract_signature,
        "hro_question_signature": hro_signature,
        "candidate_set_signature": factory.get("candidate_set_signature"),
        "tasks": routes,
        "constraints": {
            "candidate_ids_backend_generated": True,
            "general_v3_tasks_routed": False,
            "policy_questions_routed": False,
            "missing_evidence_routed": False,
        },
    }
    tasks_artifact["input_signature"] = content_signature(tasks_artifact)
    return contracts_artifact, tasks_artifact


def _candidate_index(factory: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(candidate.get("candidate_id") or ""): candidate
        for task in _objects(factory.get("tasks"))
        for candidate in _objects(task.get("candidates"))
    }


def _evidence_refs(candidate: Mapping[str, Any]) -> list[str]:
    return sorted({
        str(ref)
        for key in (
            "left_refs", "right_refs", "entity_refs", "graph_refs",
            "table_refs", "text_refs",
        )
        for ref in candidate.get(key) or ()
        if str(ref)
    })


def _evidence_records(
    candidate: Mapping[str, Any],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Persist exact viewer locations for the closed-question audit trail."""
    preparation = artifacts.get("text_preparation") or {}
    fragments = preparation.get("fragments") or {}
    indexes: dict[str, dict[str, Mapping[str, Any]]] = {}
    for side in ("LEFT", "RIGHT"):
        values = (
            fragments.get(side.casefold())
            if isinstance(fragments, Mapping) else ()
        ) or ()
        indexes[side] = {
            str(value.get("id") or ""): value
            for value in _objects(values)
        }
    output: dict[str, list[dict[str, Any]]] = {"LEFT": [], "RIGHT": []}
    for ref in _evidence_refs(candidate):
        parts = ref.split(":", 2)
        if len(parts) != 3 or parts[0] not in output or parts[1] != "TEXT":
            continue
        side, fragment_id = parts[0], parts[2]
        fragment = indexes[side].get(fragment_id)
        if fragment is None:
            continue
        base = {
            "source": "TEXT",
            "page": fragment.get("pdf_page")
            or (fragment.get("source_location") or {}).get("pdf_page"),
            "fragment_id": fragment_id,
            "block_id": fragment.get("source_block_id"),
            "raw_text": fragment.get("text"),
        }
        boxes = [
            value for value in fragment.get("bboxes") or ()
            if isinstance(value, Mapping)
        ]
        if not boxes:
            output[side].append(base)
            continue
        for box in boxes:
            x, y = box.get("x"), box.get("y")
            width, height = box.get("width"), box.get("height")
            if not all(
                isinstance(value, (int, float))
                for value in (x, y, width, height)
            ):
                continue
            output[side].append({
                **base,
                "bbox": [x, y, x + width, y + height],
                "coordinate_space": "NORMALIZED_PAGE_TOP_LEFT",
            })
    return output


def _overlay_plan(
    plan: Mapping[str, Any],
    *,
    closed: Sequence[Mapping[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    result = copy.deepcopy(dict(plan))
    by_id = {str(value.get("question_id") or ""): value for value in closed}
    kept = []
    history = list(result.get("ai_closed_questions") or ())
    for original_position, question in enumerate(
        result.get("standalone_questions") or ()
    ):
        question_id = str((question or {}).get("question_id") or "")
        closure = by_id.get(question_id)
        if closure is None:
            kept.append(copy.deepcopy(question))
            continue
        history.append({
            **copy.deepcopy(dict(question)),
            "closure": copy.deepcopy(dict(closure)),
            "closed_at": generated_at,
            "original_position": original_position,
            "status": "CLOSED_AI_STABLE",
            "can_reopen": True,
            "history_message": (
                "Вопрос снят автоматически: система сравнила ограниченный "
                "набор вариантов, оба независимых прохода выбрали один "
                "вариант, результат проверен правилами."
            ),
        })
    result["standalone_questions"] = kept
    result["ai_closed_questions"] = history
    summary = dict(result.get("summary") or {})
    summary["standalone_human_questions"] = len(kept)
    summary["mandatory_human_interactions"] = (
        len(result.get("groups") or ()) + len(kept)
    )
    summary["ai_question_closure_closed"] = len(history)
    result["summary"] = summary
    result["kind"] = "stage_comparison_human_review_plan_question_closure"
    result["constraints"] = {
        **dict(result.get("constraints") or {}),
        "human_priority": True,
        "closed_question_reopen_allowed": True,
        "clarification_is_not_final_approval": True,
        "final_report_approved_only": True,
    }
    provenance = dict(result.get("provenance") or {})
    provenance["sources"] = sorted({
        *list(provenance.get("sources") or ()),
        "ai_question_closure",
    })
    result["provenance"] = provenance
    return result


def run_production_question_closure(
    *,
    artifacts: Mapping[str, Mapping[str, Any]],
    hro_plan: Mapping[str, Any],
    human_decisions: Mapping[str, Any] | None,
    pair_id: str,
    cache_dir: Path | str,
    call: Callable[..., gateway.CallResult] | None = None,
    run_id: str = "",
) -> dict[str, Any]:
    """Run one practical two-pass gate and return an atomic publication unit."""
    settings.require_enabled()
    started = time.perf_counter()
    eligibility_started = time.perf_counter()
    frozen_fast_signature = fast_signature(artifacts)
    factory, _bundles, _catalog = build_candidate_factory(
        artifacts=artifacts,
        pair_id=pair_id,
        fast_input_signature=frozen_fast_signature,
    )
    contracts, tasks = build_production_contracts(
        hro_plan=hro_plan,
        factory=factory,
        human_decisions=human_decisions,
    )
    eligibility_ms = int((time.perf_counter() - eligibility_started) * 1000)
    baseline = int(contracts.get("baseline_hro") or 0)
    if not tasks["tasks"]:
        artifact = {
            "kind": "stage_comparison_question_closure",
            "schema_version": PRODUCTION_SCHEMA_VERSION,
            "closure_layer_version": settings.CLOSURE_LAYER_VERSION,
            "feature_flag": settings.FEATURE_FLAG,
            "status": "NOT_APPLICABLE",
            "generated_at": utc_now(),
            "pair_id": pair_id,
            "fast_signature": frozen_fast_signature,
            "contracts": contracts,
            "tasks": tasks,
            "hro_before": baseline,
            "hro_after": baseline,
            "closed_questions": [],
            "unsupported_closures": 0,
            "model_calls": 0,
            "eligibility_duration_ms": eligibility_ms,
            "duration_ms": int((time.perf_counter() - started) * 1000),
            "human_review_plan": copy.deepcopy(dict(hro_plan)),
            "constraints": {
                "fast_unchanged": True,
                "engineer_approvals_untouched": True,
                "general_v3_executed": False,
            },
        }
        artifact["input_signature"] = content_signature(artifact)
        return artifact

    selector = QuestionClosureSelector(
        artifacts=artifacts,
        pair_id=pair_id,
        routed_tasks=tasks["tasks"],
        fast_input_signature=frozen_fast_signature,
        cache_dir=cache_dir,
        cache_enabled=settings.cache_enabled(),
        call=call,
        run_id=run_id,
        closure_contract_signature=contracts["closure_contract_signature"],
        hro_question_signature=contracts["hro_question_signature"],
        closure_layer_version=settings.CLOSURE_LAYER_VERSION,
    )
    selector_run = selector.run()
    if selector_run.get("source_candidate_set_signature") != factory.get(
        "candidate_set_signature"
    ):
        raise ValueError("Question Closure candidate factory changed during routing")

    candidate_index = _candidate_index(selector.factory)
    protected = _human_protected_question_ids(human_decisions)
    route_by_task = {
        str(value.get("task_id") or ""): value
        for value in _objects(tasks.get("tasks"))
    }
    closed = []
    outcomes = []
    for selection in _objects(selector_run.get("stable_selections")):
        task_id = str(selection.get("task_id") or "")
        route = route_by_task.get(task_id)
        if route is None:
            continue
        question_id = str(route.get("question_id") or "")
        candidate_id = str(selection.get("selected_candidate_id") or "")
        candidate = candidate_index.get(candidate_id) or {}
        pass_ids = list(map(str, selection.get("pass_candidate_ids") or ()))
        unanimous = len(pass_ids) == 2 and len(set(pass_ids)) == 1
        verified = selection.get("status") == v3_schemas.VERIFIED_SELECTION
        backend_candidate = (
            candidate_id in set(map(str, (route.get("candidate_ids") or {}).values()))
            and candidate.get("candidate_signature") == selection.get("candidate_signature")
        )
        auto_effect = candidate.get("resolution_effect") in AUTO_CLOSING_EFFECTS
        human_absent = question_id not in protected
        can_close = all((unanimous, verified, backend_candidate, auto_effect, human_absent))
        outcome = {
            "question_id": question_id,
            "task_id": task_id,
            "selected_candidate_id": candidate_id or None,
            "selected_candidate_type": candidate.get("candidate_type"),
            "pass_candidate_ids": pass_ids,
            "two_pass_unanimous": unanimous,
            "verifier_status": selection.get("status"),
            "backend_generated_candidate": backend_candidate,
            "auto_closing_effect": auto_effect,
            "human_decision_absent": human_absent,
            "closed": can_close,
            "reason_code": selection.get("reason_code"),
        }
        outcomes.append(outcome)
        if can_close:
            closed.append({
                **outcome,
                "candidate_signature": candidate.get("candidate_signature"),
                "evidence_refs": _evidence_refs(candidate),
                "evidence": _evidence_records(candidate, artifacts),
                "model": v3_settings.MODEL,
                "reasoning_effort": v3_settings.REASONING_EFFORT,
                "closure_contract_signature": contracts["closure_contract_signature"],
                "hro_question_signature": contracts["hro_question_signature"],
                "candidate_set_signature": selector.factory.get("candidate_set_signature"),
            })

    # All evidence axes are frozen independently of the mutable progress state.
    if fast_signature(artifacts) != frozen_fast_signature:
        raise ValueError("Question Closure FAST inputs changed before publication")
    generated_at = utc_now()
    plan = _overlay_plan(hro_plan, closed=closed, generated_at=generated_at)
    diagnostics = selector_run.get("diagnostics") or {}
    cache = diagnostics.get("cache") or {}
    duration_ms = int((time.perf_counter() - started) * 1000)
    artifact = {
        "kind": "stage_comparison_question_closure",
        "schema_version": PRODUCTION_SCHEMA_VERSION,
        "closure_layer_version": settings.CLOSURE_LAYER_VERSION,
        "feature_flag": settings.FEATURE_FLAG,
        "status": "COMPLETED",
        "generated_at": generated_at,
        "pair_id": pair_id,
        "fast_signature": frozen_fast_signature,
        "contracts": contracts,
        "tasks": tasks,
        "selector_run": selector_run,
        "outcomes": outcomes,
        "closed_questions": closed,
        "closed_question_ids": sorted(
            str(value.get("question_id") or "") for value in closed
        ),
        "hro_before": baseline,
        "hro_after": baseline - len(closed),
        "unsupported_closures": 0,
        "model_calls": int(diagnostics.get("model_calls") or 0),
        "eligibility_duration_ms": eligibility_ms,
        "pass_1_duration_ms": sum(
            int(value.get("duration_ms") or 0)
            for value in (selector_run.get("selector_passes") or {}).get("pass_1") or ()
        ),
        "pass_2_duration_ms": sum(
            int(value.get("duration_ms") or 0)
            for value in (selector_run.get("selector_passes") or {}).get("pass_2") or ()
        ),
        "duration_ms": duration_ms,
        "cache": {
            "enabled": settings.cache_enabled(),
            "hits": int(cache.get("hits") or 0),
            "misses": int(cache.get("misses") or 0),
            "cache_context_signature": selector.cache_context_signature,
        },
        "human_review_plan": plan,
        "constraints": {
            "fast_unchanged": True,
            "engineer_approvals_untouched": True,
            "general_v3_executed": False,
            "two_pass_unanimity_required": True,
            "retry_until_desired_answer": False,
            "candidate_only_closure": True,
            "human_priority": True,
        },
    }
    artifact["input_signature"] = content_signature({
        "fast_signature": frozen_fast_signature,
        "closure_contract_signature": contracts["closure_contract_signature"],
        "candidate_set_signature": selector.factory.get("candidate_set_signature"),
        "selector_input_signature": selector_run.get("input_signature"),
        "closed_question_ids": artifact["closed_question_ids"],
        "closure_layer_version": settings.CLOSURE_LAYER_VERSION,
    })
    return artifact


def failure_artifact(
    *,
    pair_id: str,
    hro_plan: Mapping[str, Any],
    reason: BaseException,
    duration_ms: int,
) -> dict[str, Any]:
    baseline = int(
        (hro_plan.get("summary") or {}).get("mandatory_human_interactions") or 0
    )
    value = {
        "kind": "stage_comparison_question_closure",
        "schema_version": PRODUCTION_SCHEMA_VERSION,
        "closure_layer_version": settings.CLOSURE_LAYER_VERSION,
        "feature_flag": settings.FEATURE_FLAG,
        "status": "FALLBACK",
        "generated_at": utc_now(),
        "pair_id": pair_id,
        "reason_code": type(reason).__name__,
        "fallback_used": True,
        "fallback_message": (
            "Автоматическое снятие уточняющих вопросов не подтверждено; "
            "все вопросы оставлены инженеру."
        ),
        "hro_before": baseline,
        "hro_after": baseline,
        "closed_questions": [],
        "unsupported_closures": 0,
        "model_calls": 0,
        "duration_ms": max(0, int(duration_ms)),
        "human_review_plan": copy.deepcopy(dict(hro_plan)),
        "constraints": {
            "fail_closed": True,
            "fast_unchanged": True,
            "engineer_approvals_untouched": True,
            "general_v3_executed": False,
        },
    }
    value["input_signature"] = content_signature(value)
    return value


__all__ = [
    "ALLOWED_CANDIDATE_TYPES",
    "POLICY_VERSION",
    "PRODUCTION_SCHEMA_VERSION",
    "build_production_contracts",
    "failure_artifact",
    "fast_signature",
    "hro_question_signature",
    "run_production_question_closure",
]
