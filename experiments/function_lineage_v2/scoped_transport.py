"""Deterministic scoped selector transport for Function Lineage v2.4.1.

The runner projects the frozen v2.4 Function Scope Graph into model-ready,
bounded payloads.  It never reads the pre-scope selector shards and it never
calls a model, vision, shadow, materialization, deployment, or production.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import subprocess
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from backend.app.services.stage_comparison.ai import settings as ai_settings
from backend.app.services.stage_comparison.production_artifacts import (
    canonical_json,
    content_signature,
    stable_id,
)
from experiments.function_lineage_v2 import scope_graph, transport


REPO_ROOT = Path(__file__).resolve().parents[2]
SCOPE_SOURCE_COMMIT = "6d2e7a5e4710765f0b5b8450c73c31431e070d13"
PRODUCTION_BASE_COMMIT = "4d489bf9033ad40c40099fe5e1436493bc56c0ed"
PRODUCTION_RELEASE = "ui-real-4d489bf9"
ALGORITHM_VERSION = "function-lineage-scoped-selector-transport.v2.4.1"
SCHEMA_VERSION = "function-lineage-scoped-task.v2.4.1"
METRICS_SCHEMA_VERSION = "function-lineage-scoped-transport-metrics.v2.4.1"
MANIFEST_SCHEMA_VERSION = "function-lineage-scoped-transport-manifest.v2.4.1"
TARGET_CHARACTERS = 250_000
HARD_CHARACTERS = 350_000
TOKEN_ESTIMATOR = "ceil(unicode_characters/4)"
PAIR_ORDER = tuple(sorted(scope_graph.PROJECTS, key=scope_graph.PROJECTS.__getitem__))
IOS21_PAIR_ID = scope_graph.IOS21_PAIR_ID

SCOPE_ROOT = (
    REPO_ROOT
    / "comparison"
    / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_4_scope_graph"
)
DEFAULT_OUTPUT = (
    REPO_ROOT
    / "comparison"
    / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_4_1_scoped_transport"
)
PRE_SCOPE_SHARDS = (
    REPO_ROOT
    / "comparison"
    / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_1_transport"
    / "selector_shards.jsonl"
)
SCOPE_INPUT_SHA256 = {
    "candidate_scope_membership.json": (
        "a28cf9700151b98e7a5b1206dee08b3f9f17c59037510f57861736de9a1b0dba"
    ),
    "function_scope_graph.json": (
        "f86c2911d8ac8c3d65295d3f5c43bb9befb53c7ca99716e2979f9ae2574aec29"
    ),
    "group_derivability_audit.json": (
        "9bd14758895dca67f1397f56f8149f94362fc6c2a70c0cbe54f863c4eca10812"
    ),
    "ios21_scope_forensics.json": (
        "57e6456dba5d3166083b7f2980f9fe94fb5c9467a198c62658f821f038f52d1b"
    ),
    "report.md": "93c87a09eb4db011c5eed4e84172517a34a429eb50748c5dead78bd9cf383c83",
    "scope_metrics.json": (
        "23dce5a1f23b33c509b6e6b9015369011e3d7acfe56d27a16152f605d1f3e4a4"
    ),
    "selector_tasks_scoped.json": (
        "58432c19caa92b01e26e52d366a0344e1a63768b39f27ae83639d078ce2fa1af"
    ),
}
EXPECTED_RECALL = {
    "raw_candidate_recall": {
        "recall_at_1": 0.578947,
        "recall_at_3": 0.684211,
        "recall_at_5": 0.842105,
        "recall_at_10": 0.947368,
    },
    "scope_eligible_recall": {
        "recall_at_1": 0.789474,
        "recall_at_3": 0.842105,
        "recall_at_5": 0.894737,
        "recall_at_10": 0.947368,
    },
}
PROMPT_TEMPLATE_LINES = (
    "Independent scoped verification pass {PASS}.",
    "You are a bounded engineering FUNCTION LINEAGE selector.",
    "Each task is exactly one FunctionScope.",
    "Only listed EXACT_SCOPE candidates are selectable for that task.",
    "STRICT_SUBSET, STRICT_SUPERSET, OVERLAP, DISJOINT, and UNKNOWN candidates are not answers for this scope.",
    "Choose exactly one listed candidate_id, FUNCTION_REMOVED, or NEED_MORE_EVIDENCE for every task.",
    "DOCUMENT_LINK is documentary navigation and is never a FUNCTIONAL_ANALOGUE.",
    "Use exact object/zone, function, component role and topology evidence.",
    "A RIGHT physical page may be reused only through distinct right_fragment_ids.",
    "SHEET_SHARED_EVIDENCE is limited sheet context; FRAGMENT_OWNED_EVIDENCE never transfers between fragments merely because they share a page.",
    "Do not invent pages, functions, fragments, scopes, groups, relations, or evidence.",
    "A missing physical sheet never proves FUNCTION_REMOVED.",
    "Return only the JSON object required by the output schema.",
)
SCOPE_CORE_FIELDS = (
    "function_class",
    "role",
    "serviced_object",
    "corpus",
    "building",
    "zone",
    "floors",
    "consumers",
    "upstream",
    "downstream",
    "systems",
    "equipment_roles",
    "document_role",
    "neighbors",
)
ARTIFACT_NAMES = (
    "scoped_selector_manifest.json",
    "scoped_selector_shards.jsonl",
    "scoped_selector_transport_metrics.json",
    "ios21_critical_scoped_contexts.json",
    "report.md",
)
CRITICAL_SOURCE_TASKS = {
    "LEFT17": "ltask_0ea09fe595c5fbe81d8b",
    "LEFT18": "ltask_dca76a53cf8b39004e96",
    "LEFT19": "ltask_015d2dbabecfea8054ea",
}


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object: {path}")
    return value


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _jsonl_bytes(values: Iterable[Mapping[str, Any]]) -> bytes:
    return "".join(canonical_json(dict(value)) + "\n" for value in values).encode(
        "utf-8"
    )


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_file(path: Path) -> str:
    return _sha_bytes(path.read_bytes())


def _sha_json(value: Any) -> str:
    return _sha_bytes(canonical_json(value).encode("utf-8"))


def _git_ancestor(commit: str) -> bool:
    return subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
        cwd=REPO_ROOT,
        check=False,
    ).returncode == 0


def assert_frozen_scope_inputs() -> dict[str, str]:
    if not _git_ancestor(SCOPE_SOURCE_COMMIT):
        raise RuntimeError(f"scope source is not an ancestor: {SCOPE_SOURCE_COMMIT}")
    actual = {
        name: _sha_file(SCOPE_ROOT / name) for name in SCOPE_INPUT_SHA256
    }
    if actual != SCOPE_INPUT_SHA256:
        raise RuntimeError(
            "frozen Function Scope Graph drifted: "
            + json.dumps(actual, sort_keys=True)
        )
    return actual


def load_inputs() -> dict[str, Any]:
    scope_hashes = assert_frozen_scope_inputs()
    candidate_hashes = scope_graph.assert_frozen_inputs()
    datasets = transport.load_source_artifacts()
    return {
        "datasets": datasets,
        "scope_hashes": scope_hashes,
        "candidate_hashes": candidate_hashes,
        "graph": _read_json(SCOPE_ROOT / "function_scope_graph.json"),
        "memberships": _read_json(SCOPE_ROOT / "candidate_scope_membership.json"),
        "selector_tasks": _read_json(SCOPE_ROOT / "selector_tasks_scoped.json"),
        "scope_metrics": _read_json(SCOPE_ROOT / "scope_metrics.json"),
        "ios21_forensics": _read_json(SCOPE_ROOT / "ios21_scope_forensics.json"),
    }


def _scope_core(scope: Mapping[str, Any]) -> dict[str, Any]:
    core: dict[str, Any] = {
        "scope_id": scope["scope_id"],
        "scope_kind": scope["scope_kind"],
        "required_component_ids": list(scope.get("required_component_ids") or []),
        "optional_component_ids": list(scope.get("optional_component_ids") or []),
        "parent_scope_ids": list(scope.get("parent_scope_ids") or []),
        "child_scope_ids": list(scope.get("child_scope_ids") or []),
        "source_function_ids": list(scope.get("source_function_ids") or []),
        "source_fragment_ids": list(scope.get("source_fragment_ids") or []),
        "source_physical_pages": list(scope.get("source_physical_pages") or []),
    }
    for field in SCOPE_CORE_FIELDS:
        value = scope.get(field)
        if not transport._present(value):
            continue
        compacted, marker = transport.compact_value(value, field=field)
        core[field] = compacted
        if marker:
            core.setdefault("compacted_fields", {})[field] = marker
    return core


def _source_function_cores(
    source_tasks: Sequence[Mapping[str, Any]],
    passports: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for task in sorted(source_tasks, key=lambda value: str(value["task_id"])):
        row = transport._left_core(
            task, passports[str(task["left_function_id"])]
        )
        row["source_task_id"] = row.pop("task_id")
        rows.append(row)
    return rows


def _allowed_scope_evidence_ids(
    scope: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    passports: Mapping[str, Mapping[str, Any]],
    fragments: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    function_ids = set(str(value) for value in scope.get("source_function_ids") or [])
    fragment_ids = set(str(value) for value in scope.get("source_fragment_ids") or [])
    evidence_ids = set(str(value) for value in scope.get("evidence_refs") or [])
    for candidate in candidates:
        function_ids.update(str(value) for value in candidate.get("left_function_ids") or [])
        function_ids.update(str(value) for value in candidate.get("right_function_ids") or [])
        fragment_ids.update(str(value) for value in candidate.get("left_fragment_ids") or [])
        fragment_ids.update(str(value) for value in candidate.get("right_fragment_ids") or [])
        evidence_ids.update(str(value) for value in candidate.get("evidence_refs") or [])
    for function_id in function_ids:
        evidence_ids.update(
            str(value)
            for value in (passports.get(function_id) or {}).get("evidence_refs") or []
        )
    for fragment_id in fragment_ids:
        evidence_ids.update(
            str(value)
            for value in (fragments.get(fragment_id) or {}).get("evidence_refs") or []
        )
    return sorted(evidence_ids)


def _documentary_facts_for_scope(
    artifact: Mapping[str, Any],
    left_pages: Sequence[int],
    right_pages: set[int],
) -> list[dict[str, Any]]:
    rows = {
        canonical_json(row): row
        for left_page in left_pages
        for row in transport._documentary_facts(artifact, left_page, right_pages)
    }
    return [rows[key] for key in sorted(rows)]


def _validate_scope_evidence(
    task_id: str,
    scope: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    evidence: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    allowed_functions = {
        *(str(value) for value in scope.get("source_function_ids") or []),
        *(
            str(value)
            for candidate in candidates
            for field in ("left_function_ids", "right_function_ids")
            for value in candidate.get(field) or []
        ),
    }
    allowed_fragments = {
        *(str(value) for value in scope.get("source_fragment_ids") or []),
        *(
            str(value)
            for candidate in candidates
            for field in ("left_fragment_ids", "right_fragment_ids")
            for value in candidate.get(field) or []
        ),
    }
    allowed_pages = {
        *(('LEFT', int(value)) for value in scope.get("source_physical_pages") or []),
        *(
            (side, int(value))
            for candidate in candidates
            for side, field in (("LEFT", "left_pages"), ("RIGHT", "right_pages"))
            for value in candidate.get(field) or []
        ),
    }
    errors = []
    for evidence_id, fact in evidence.items():
        if fact["provenance_type"] == lineage.FRAGMENT_OWNED_EVIDENCE and (
            str(fact["owner_function_id"]) not in allowed_functions
            or str(fact["owner_fragment_id"]) not in allowed_fragments
        ):
            errors.append(f"{task_id}:EVIDENCE_OWNER_OUTSIDE_SCOPE:{evidence_id}")
        if (str(fact["side"]), int(fact["physical_page"])) not in allowed_pages:
            errors.append(f"{task_id}:EVIDENCE_PAGE_OUTSIDE_SCOPE:{evidence_id}")
    return errors


def project_scoped_tasks(
    artifact: Mapping[str, Any],
    scoped_tasks: Sequence[Mapping[str, Any]],
    scope_index: Mapping[str, Mapping[str, Any]],
    canonical_scope_by_candidate: Mapping[str, str | None],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Project only each task's eligible EXACT_SCOPE candidates."""
    passports = transport._passports(artifact)
    fragments = transport._fragments(artifact)
    candidate_map = transport._candidate_map(artifact)
    source_task_index = {
        str(value["task_id"]): value for value in artifact["candidate_tasks"]
    }
    contexts = []
    errors = []
    for task in sorted(scoped_tasks, key=lambda value: str(value["scoped_task_id"])):
        task_id = str(task["scoped_task_id"])
        scope_id = str(task["coverage_scope_id"])
        scope = scope_index[scope_id]
        candidate_ids = [str(value) for value in task.get("candidate_ids") or []]
        candidates = [candidate_map[value] for value in candidate_ids]
        source_tasks = [
            source_task_index[str(value)] for value in task.get("source_task_ids") or []
        ]
        if not candidate_ids:
            errors.append(f"{task_id}:NO_EXACT_SCOPE_CANDIDATES")
        for candidate_id in candidate_ids:
            if canonical_scope_by_candidate.get(candidate_id) != scope_id:
                errors.append(f"{task_id}:NON_EXACT_SCOPE_CANDIDATE:{candidate_id}")
        evidence_ids = _allowed_scope_evidence_ids(
            scope, candidates, passports, fragments
        )
        evidence, evidence_errors = transport._evidence_dictionary(
            artifact, evidence_ids
        )
        errors.extend(f"{task_id}:{value}" for value in evidence_errors)
        errors.extend(
            _validate_scope_evidence(task_id, scope, candidates, evidence)
        )
        candidate_rows = []
        for candidate_id, candidate in zip(candidate_ids, candidates):
            row = transport._candidate_projection(
                candidate,
                rank=int(task["candidate_ranks_for_display_only"][candidate_id]),
                passports=passports,
            )
            row["scope_relation"] = "EXACT_SCOPE"
            candidate_rows.append(row)
        right_pages = {
            int(value)
            for candidate in candidates
            for value in candidate.get("right_pages") or []
        }
        context: dict[str, Any] = {
            "task_id": task_id,
            "scope_id": scope_id,
            "function_scope_core": _scope_core(scope),
            "source_function_cores": _source_function_cores(source_tasks, passports),
            "functional_candidates": candidate_rows,
            "local_evidence": evidence,
            "allowed_decisions": [
                *candidate_ids,
                lineage.FUNCTION_REMOVED,
                lineage.NEED_MORE_EVIDENCE,
            ],
            "scope_policy": {
                "task_identity": "ONE_EXACT_FUNCTION_SCOPE",
                "selectable_scope_relation": "EXACT_SCOPE",
                "cross_scope_candidates_included": False,
                "unknown_is_selectable": False,
            },
        }
        documentary = _documentary_facts_for_scope(
            artifact,
            [int(value) for value in scope.get("source_physical_pages") or []],
            right_pages,
        )
        if documentary:
            context["documentary_support"] = documentary
        context["task_context_signature"] = content_signature(context)
        contexts.append(context)
    return contexts, sorted(set(errors))


def _payload(
    pair_id: str,
    input_signature: str,
    contexts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    task_ids = [str(value["task_id"]) for value in contexts]
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "transport_algorithm": ALGORITHM_VERSION,
        "candidate_algorithm": lineage.ALGORITHM_VERSION,
        "pair_id": pair_id,
        "candidate_input_signature": input_signature,
        "task_ids": task_ids,
        "scope_ids": [str(value["scope_id"]) for value in contexts],
        "task_contexts": list(contexts),
        "policy": {
            "one_exact_function_scope_per_task": True,
            "only_exact_scope_candidates_selectable": True,
            "cross_granularity_selectable_competition": False,
            "pre_scope_shards_are_not_input": True,
            "candidate_lists_are_never_truncated": True,
            "atomic_tasks_are_never_split": True,
            "document_link_is_not_functional_analogue": True,
            "physical_right_page_can_be_reused_by_distinct_fragments": True,
            "same_page_fragment_evidence_is_not_transferable": True,
            "function_removed_requires_exhaustive_evidence": True,
        },
    }
    payload["shard_id"] = stable_id("fssh_", pair_id, task_ids)
    payload["payload_signature"] = content_signature(payload)
    return payload


def build_prompt(payload: Mapping[str, Any], pass_name: str) -> str:
    if pass_name not in {"A", "B"}:
        raise ValueError(f"invalid pass: {pass_name}")
    return "\n".join(
        [
            *(line.replace("{PASS}", pass_name) for line in PROMPT_TEMPLATE_LINES),
            "payload=" + canonical_json(payload),
        ]
    )


def shard_scoped_task_contexts(
    pair_id: str,
    input_signature: str,
    contexts: Sequence[Mapping[str, Any]],
    *,
    target_characters: int = TARGET_CHARACTERS,
    hard_characters: int = HARD_CHARACTERS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Greedily pack complete scoped tasks; fail closed above the hard gate."""
    shards = []
    oversized = []
    current: list[Mapping[str, Any]] = []

    def chars(values: Sequence[Mapping[str, Any]]) -> int:
        return len(build_prompt(_payload(pair_id, input_signature, values), "A"))

    def emit(values: Sequence[Mapping[str, Any]]) -> None:
        payload = _payload(pair_id, input_signature, values)
        prompts = {name: build_prompt(payload, name) for name in ("A", "B")}
        prompt_characters = max(len(value) for value in prompts.values())
        if prompt_characters > hard_characters:
            raise RuntimeError(f"internal hard gate exceeded: {payload['shard_id']}")
        schema = transport.output_schema(payload)
        schema_problems = transport.provider_safe_schema_problems(schema)
        shards.append(
            {
                "pair_id": pair_id,
                "shard_id": payload["shard_id"],
                "task_ids": list(payload["task_ids"]),
                "scope_ids": list(payload["scope_ids"]),
                "model_payload": payload,
                "output_schema": schema,
                "provider_safe_schema_problems": schema_problems,
                "prompt_a_sha256": _sha_bytes(prompts["A"].encode("utf-8")),
                "prompt_b_sha256": _sha_bytes(prompts["B"].encode("utf-8")),
                "prompt_characters": prompt_characters,
                "estimated_tokens": math.ceil(prompt_characters / 4),
            }
        )

    for context in contexts:
        single_chars = chars([context])
        if single_chars > hard_characters:
            if current:
                emit(current)
                current = []
            oversized.append(
                {
                    "pair_id": pair_id,
                    "task_id": context["task_id"],
                    "scope_id": context.get("scope_id"),
                    "reason_code": "ATOMIC_SCOPE_CONTEXT_EXCEEDS_HARD_GATE",
                    "characters": single_chars,
                    "hard_gate": hard_characters,
                    "candidate_count": len(
                        context.get("functional_candidates") or []
                    ),
                    "candidate_list_truncated": False,
                    "task_split": False,
                }
            )
            continue
        proposed = [*current, context]
        if current and chars(proposed) > target_characters:
            emit(current)
            current = [context]
        else:
            current = proposed
    if current:
        emit(current)
    return shards, oversized


def verify_scoped_transport_response(
    payload: Mapping[str, Any], response: Mapping[str, Any] | None
) -> dict[str, Any]:
    """Use the provider-safe parser introduced at 0655372c unchanged."""
    return transport.verify_transport_response(payload, response)


def _percentile(values: Sequence[int], percentile: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile / 100 * len(ordered)) - 1)]


def _distribution(values: Sequence[int]) -> dict[str, int | float | None]:
    return {
        "min": min(values) if values else None,
        "median": statistics.median(values) if values else None,
        "p95": _percentile(values, 95),
        "max": max(values) if values else None,
    }


def _compaction_counts(contexts: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    scope_fields = sum(
        len(context["function_scope_core"].get("compacted_fields") or {})
        for context in contexts
    )
    source_fields = sum(
        len(core.get("compacted_fields") or {})
        for context in contexts
        for core in context["source_function_cores"]
    )
    evidence_facts = sum(
        "compaction" in fact
        for context in contexts
        for fact in context["local_evidence"].values()
    )
    return {
        "explicit_scope_field_compactions": scope_fields,
        "explicit_source_field_compactions": source_fields,
        "explicit_evidence_fact_compactions": evidence_facts,
        "silent_truncations": 0,
        "candidate_list_truncations": 0,
    }


def _parser_self_test(shards: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    valid_failures = []
    cross_scope_rejections = 0
    for shard in shards:
        payload = shard["model_payload"]
        valid = {
            "results": [
                {
                    "task_id": context["task_id"],
                    "decision": lineage.NEED_MORE_EVIDENCE,
                }
                for context in payload["task_contexts"]
            ]
        }
        if not verify_scoped_transport_response(payload, valid)["ok"]:
            valid_failures.append(shard["shard_id"])
        contexts = payload["task_contexts"]
        if len(contexts) < 2:
            continue
        foreign = contexts[0]["functional_candidates"][0]["candidate_id"]
        if foreign in contexts[1]["allowed_decisions"]:
            continue
        invalid = {"results": [dict(value) for value in valid["results"]]}
        invalid["results"][1]["decision"] = foreign
        checked = verify_scoped_transport_response(payload, invalid)
        errors = checked["task_results"][contexts[1]["task_id"]]["errors"]
        if errors == ["CANDIDATE_ID_NOT_ALLOWED_FOR_TASK"]:
            cross_scope_rejections += 1
    return {
        "valid_response_failures": valid_failures,
        "cross_scope_candidate_rejection_tests": cross_scope_rejections,
        "fail_closed": not valid_failures and cross_scope_rejections > 0,
    }


def _critical_contexts(
    contexts: Sequence[Mapping[str, Any]],
    ios21: Mapping[str, Any],
) -> dict[str, Any]:
    by_scope = {str(value["scope_id"]): value for value in contexts}
    by_source_task: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for context in contexts:
        for core in context["source_function_cores"]:
            by_source_task[str(core["source_task_id"])].append(context)

    def source_control(source_task_id: str, right_page: int) -> Mapping[str, Any]:
        matches = [
            context
            for context in by_source_task[source_task_id]
            if any(
                candidate["right_physical_pages"] == [right_page]
                for candidate in context["functional_candidates"]
            )
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"critical scoped control is ambiguous: {source_task_id}/R{right_page}"
            )
        return matches[0]

    selected: list[tuple[str, Mapping[str, Any]]] = [
        (
            "LEFT17",
            source_control(CRITICAL_SOURCE_TASKS["LEFT17"], 27),
        ),
        (
            "LEFT18",
            source_control(CRITICAL_SOURCE_TASKS["LEFT18"], 24),
        ),
        ("LEFT19", by_scope[str(ios21["LEFT19"]["scope_id"])]),
    ]
    left20 = ios21["LEFT20"]
    for role, label in (
        ("DOMESTIC_PRESSURE_BOOST", "LEFT20 DOMESTIC child"),
        ("FIRE_PRESSURE_BOOST", "LEFT20 FIRE child"),
        ("INCOMING_METERING", "LEFT20 METERING child"),
    ):
        selected.append((label, by_scope[str(left20["child_scope_ids"][role])]))
    selected.append(
        ("LEFT20 composite parent", by_scope[str(left20["parent_scope_id"])])
    )
    rows = [
        {
            "label": label,
            "task_id": context["task_id"],
            "scope_id": context["scope_id"],
            "scope_kind": context["function_scope_core"]["scope_kind"],
            "source_task_ids": [
                value["source_task_id"] for value in context["source_function_cores"]
            ],
            "candidate_ids": [
                value["candidate_id"] for value in context["functional_candidates"]
            ],
            "candidate_right_pages": {
                value["candidate_id"]: value["right_physical_pages"]
                for value in context["functional_candidates"]
            },
            "context_sha256": _sha_json(context),
            "context": context,
        }
        for label, context in selected
    ]
    return {
        "kind": "ios21_critical_scoped_contexts",
        "schema_version": SCHEMA_VERSION,
        "pair_id": IOS21_PAIR_ID,
        "task_count": len(rows),
        "tasks": rows,
        "contexts_sha256": _sha_json([row["context"] for row in rows]),
        "future_ai_smoke_status": "NOT_RUN",
        "model_calls": 0,
        "vision": False,
    }


def _critical_checks(
    critical: Mapping[str, Any], ios21: Mapping[str, Any]
) -> dict[str, Any]:
    tasks = {str(value["label"]): value for value in critical["tasks"]}
    left20 = ios21["LEFT20"]
    singleton_ids = {
        label: str(left20["singletons"][label]["candidate_id"])
        for label in ("R26", "R28", "R29")
    }
    parent_ids = set(tasks["LEFT20 composite parent"]["candidate_ids"])
    checks = {
        "LEFT17_R27_eligible": any(
            pages == [27]
            for pages in tasks["LEFT17"]["candidate_right_pages"].values()
        ),
        "LEFT18_R24_eligible": any(
            pages == [24]
            for pages in tasks["LEFT18"]["candidate_right_pages"].values()
        ),
        "LEFT19_R30_R25_together": {
            str(ios21["LEFT19"]["r30_candidate_id"]),
            str(ios21["LEFT19"]["r25_candidate_id"]),
        }.issubset(tasks["LEFT19"]["candidate_ids"]),
        "LEFT20_R26_domestic_child_eligible": singleton_ids["R26"]
        in tasks["LEFT20 DOMESTIC child"]["candidate_ids"],
        "LEFT20_R28_fire_child_eligible": singleton_ids["R28"]
        in tasks["LEFT20 FIRE child"]["candidate_ids"],
        "LEFT20_R29_metering_child_eligible": singleton_ids["R29"]
        in tasks["LEFT20 METERING child"]["candidate_ids"],
        "LEFT20_group_parent_eligible": str(
            left20["distributed_candidate"]["candidate_id"]
        )
        in parent_ids,
        "LEFT20_singletons_absent_from_parent": not parent_ids.intersection(
            singleton_ids.values()
        ),
    }
    return {**checks, "all_pass": all(checks.values())}


def _recall_metrics(inputs: Mapping[str, Any]) -> dict[str, Any]:
    recomputed = scope_graph.build_recall_metrics(
        inputs["datasets"],
        _read_json(scope_graph.FROZEN_METRICS),
        inputs["memberships"],
        inputs["selector_tasks"],
    )["overall"]
    observed = {
        name: {
            key: value
            for key, value in recomputed[name].items()
            if key.startswith("recall_at_")
        }
        for name in EXPECTED_RECALL
    }
    return {
        **observed,
        "expected_baselines": EXPECTED_RECALL,
        "raw_no_regression": observed["raw_candidate_recall"]
        == EXPECTED_RECALL["raw_candidate_recall"],
        "scope_eligible_no_regression": observed["scope_eligible_recall"]
        == EXPECTED_RECALL["scope_eligible_recall"],
    }


def _build_objects() -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    if ai_settings.function_lineage_shadow_enabled():
        raise RuntimeError("refusing build while Function Lineage shadow is enabled")
    if ai_settings.function_lineage_materialization_enabled():
        raise RuntimeError("refusing build while materialization is enabled")
    inputs = load_inputs()
    graph_scopes = {
        str(value["scope_id"]): value for value in inputs["graph"]["scopes"]
    }
    canonical_scope = {
        str(value["candidate_id"]): value.get("canonical_coverage_scope_id")
        for value in inputs["memberships"]["candidate_summaries"]
    }
    scoped_by_pair: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for task in inputs["selector_tasks"]["tasks"]:
        scoped_by_pair[str(task["pair_id"])].append(task)

    contexts_by_pair: dict[str, list[dict[str, Any]]] = {}
    all_contexts = []
    all_shards = []
    projection_errors = []
    oversized = []
    project_rows = []
    for pair_id in PAIR_ORDER:
        artifact = inputs["datasets"][pair_id]
        contexts, errors = project_scoped_tasks(
            artifact,
            scoped_by_pair[pair_id],
            graph_scopes,
            canonical_scope,
        )
        shards, pair_oversized = shard_scoped_task_contexts(
            pair_id, str(artifact["input_signature"]), contexts
        )
        contexts_by_pair[pair_id] = contexts
        all_contexts.extend(contexts)
        all_shards.extend(shards)
        projection_errors.extend(errors)
        oversized.extend(pair_oversized)
        raw_ids = {
            str(value["candidate_id"])
            for value in artifact["functional_candidates"]
        }
        projected_ids = [
            str(value["candidate_id"])
            for context in contexts
            for value in context["functional_candidates"]
        ]
        shard_chars = [int(value["prompt_characters"]) for value in shards]
        project_rows.append(
            {
                "pair_id": pair_id,
                "project": scope_graph.PROJECTS[pair_id],
                "scoped_task_count": len(contexts),
                "raw_candidate_count": len(raw_ids),
                "exact_scope_candidate_occurrences": len(projected_ids),
                "unique_exact_scope_candidates": len(set(projected_ids)),
                "candidate_partition_exact": (
                    Counter(projected_ids) == Counter({value: 1 for value in raw_ids})
                ),
                "shard_count": len(shards),
                "shard_characters": _distribution(shard_chars),
                "tasks_per_shard": _distribution(
                    [len(value["task_ids"]) for value in shards]
                ),
                "payloads_over_target": sum(
                    value > TARGET_CHARACTERS for value in shard_chars
                ),
                "payloads_over_hard_gate": sum(
                    value > HARD_CHARACTERS for value in shard_chars
                ),
                "oversized_task_count": len(pair_oversized),
                "source_search_failure_count": len(
                    artifact.get("diagnostics", {}).get("search_failures", [])
                ),
                "source_group_generation_failure_count": len(
                    artifact.get("diagnostics", {}).get(
                        "group_generation_failures", []
                    )
                ),
                "forensic_candidate_source": str(
                    transport._artifact_path(pair_id).relative_to(REPO_ROOT)
                ),
                "forensic_candidate_source_sha256": _sha_file(
                    transport._artifact_path(pair_id)
                ),
                "raw_candidates_sha256": _sha_json(
                    artifact["functional_candidates"]
                ),
            }
        )

    raw_ids = {
        str(value["candidate_id"])
        for dataset in inputs["datasets"].values()
        for value in dataset["functional_candidates"]
    }
    projected_ids = [
        str(value["candidate_id"])
        for context in all_contexts
        for value in context["functional_candidates"]
    ]
    shard_task_ids = [
        str(task_id) for shard in all_shards for task_id in shard["task_ids"]
    ]
    context_task_ids = [str(value["task_id"]) for value in all_contexts]
    schema_problems = sorted(
        {
            problem
            for shard in all_shards
            for problem in shard["provider_safe_schema_problems"]
        }
    )
    capacity_defects = [
        {"pair_id": pair_id, **defect}
        for pair_id in PAIR_ORDER
        for defect in transport._capacity_defects(inputs["datasets"][pair_id])
    ]
    search_failures = [
        {"pair_id": pair_id, "value": value}
        for pair_id in PAIR_ORDER
        for value in inputs["datasets"][pair_id]
        .get("diagnostics", {})
        .get("search_failures", [])
    ]
    group_generation_failures = [
        {"pair_id": pair_id, "left_physical_page": value}
        for pair_id in PAIR_ORDER
        for value in inputs["datasets"][pair_id]
        .get("diagnostics", {})
        .get("group_generation_failures", [])
    ]
    related_counts = Counter(
        relation
        for task in inputs["selector_tasks"]["tasks"]
        for relation, values in task["non_selectable_related_candidate_ids"].items()
        for _ in values
    )
    cross_scope_defects = [
        {
            "task_id": context["task_id"],
            "scope_id": context["scope_id"],
            "candidate_id": candidate["candidate_id"],
        }
        for context in all_contexts
        for candidate in context["functional_candidates"]
        if candidate.get("scope_relation") != "EXACT_SCOPE"
        or canonical_scope.get(str(candidate["candidate_id"])) != context["scope_id"]
    ]
    recall = _recall_metrics(inputs)
    parser_test = _parser_self_test(all_shards)
    task_atomicity = (
        Counter(shard_task_ids) == Counter({value: 1 for value in context_task_ids})
    )
    candidate_partition = (
        Counter(projected_ids) == Counter({value: 1 for value in raw_ids})
    )
    scope_metrics_safety = inputs["scope_metrics"]["safety"]
    critical = _critical_contexts(
        contexts_by_pair[IOS21_PAIR_ID], inputs["ios21_forensics"]
    )
    critical_checks = _critical_checks(critical, inputs["ios21_forensics"])

    safety = {
        "cross_granularity_selectable_competition": len(cross_scope_defects),
        "cross_scope_candidate_defects": cross_scope_defects,
        "non_selectable_related_candidates_forensic_only": dict(
            sorted(related_counts.items())
        ),
        "RIGHT_MAP_CONFLICT": int(scope_metrics_safety["RIGHT_MAP_CONFLICT"]),
        "capacity_defect_count": len(capacity_defects),
        "capacity_defects": capacity_defects,
        "search_failure_count": len(search_failures),
        "search_failures": search_failures,
        "frozen_group_generation_failure_count": len(group_generation_failures),
        "frozen_group_generation_failures": group_generation_failures,
        "projection_error_count": len(projection_errors),
        "projection_errors": sorted(projection_errors),
        "provider_schema_problem_count": len(schema_problems),
        "provider_schema_problems": schema_problems,
        "provider_schema_contains_oneOf": any(
            "oneOf" in canonical_json(shard["output_schema"])
            for shard in all_shards
        ),
        "provider_safe_parser": parser_test,
        "task_atomicity_defect_count": 0 if task_atomicity else 1,
        "oversized_task_count": len(oversized),
        "oversized_tasks": oversized,
        "payloads_over_hard_gate": sum(
            value["prompt_characters"] > HARD_CHARACTERS for value in all_shards
        ),
        "candidate_partition_defect_count": 0 if candidate_partition else 1,
        **_compaction_counts(all_contexts),
    }
    ready = (
        len(all_contexts) == 213
        and len(raw_ids) == 1461
        and len(projected_ids) == 1461
        and candidate_partition
        and safety["cross_granularity_selectable_competition"] == 0
        and safety["RIGHT_MAP_CONFLICT"] == 0
        and safety["capacity_defect_count"] == 0
        and safety["search_failure_count"] == 0
        and safety["projection_error_count"] == 0
        and safety["provider_schema_problem_count"] == 0
        and not safety["provider_schema_contains_oneOf"]
        and safety["provider_safe_parser"]["fail_closed"]
        and safety["task_atomicity_defect_count"] == 0
        and safety["oversized_task_count"] == 0
        and all(
            value["prompt_characters"] <= TARGET_CHARACTERS
            for value in all_shards
        )
        and safety["payloads_over_hard_gate"] == 0
        and safety["candidate_partition_defect_count"] == 0
        and recall["raw_no_regression"]
        and recall["scope_eligible_no_regression"]
        and critical_checks["all_pass"]
    )
    metrics = {
        "kind": "function_lineage_scoped_selector_transport_metrics",
        "schema_version": METRICS_SCHEMA_VERSION,
        "transport_algorithm": ALGORITHM_VERSION,
        "source_commit": SCOPE_SOURCE_COMMIT,
        "projects": project_rows,
        "scoped_task_count": len(all_contexts),
        "raw_candidate_count": len(raw_ids),
        "forensically_preserved_raw_candidate_count": len(raw_ids),
        "model_input_exact_scope_candidate_occurrences": len(projected_ids),
        "model_input_unique_exact_scope_candidates": len(set(projected_ids)),
        "candidate_partition_exact": candidate_partition,
        "shard_count": len(all_shards),
        "target_characters": TARGET_CHARACTERS,
        "hard_gate_characters": HARD_CHARACTERS,
        "token_estimator": TOKEN_ESTIMATOR,
        "payloads_over_target": sum(
            value["prompt_characters"] > TARGET_CHARACTERS for value in all_shards
        ),
        "recall": recall,
        "ios21_controls": critical_checks,
        "safety": safety,
        "pre_scope_shards_used_as_ai_input": False,
        "pre_scope_shards_path_forbidden": str(PRE_SCOPE_SHARDS.relative_to(REPO_ROOT)),
        "deterministic_replay_count": 2,
        "deterministic_replay_byte_identical": True,
        "model_calls": 0,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
        "verdict": (
            "A — scoped bounded transport готов к isolated scoped AI smoke"
            if ready
            else "B — scoped bounded transport не прошёл deterministic safety gates"
        ),
    }
    metadata = {
        "inputs": inputs,
        "contexts": all_contexts,
        "raw_candidate_ids": sorted(raw_ids),
    }
    return metrics, all_shards, {"critical": critical, "metadata": metadata}


def _report(metrics: Mapping[str, Any], critical: Mapping[str, Any]) -> str:
    recall = metrics["recall"]
    safety = metrics["safety"]
    lines = [
        "# Function Lineage v2.4.1 — scoped selector transport",
        "",
        "## Boundary",
        "",
        f"- Scope source: `{SCOPE_SOURCE_COMMIT}` (V2.4 verdict A).",
        f"- Production reference only: `{PRODUCTION_BASE_COMMIT}` / `{PRODUCTION_RELEASE}`.",
        "- Model calls `0`; deploy `NO`; shadow `NO`; materialization `NO`; vision `NO`.",
        "- Pre-scope selector shards are explicitly forbidden as future AI input and were not read.",
        "",
        "## Scoped transport",
        "",
        f"`{metrics['scoped_task_count']}` atomic selector tasks each carry one exact FunctionScope and only its eligible `EXACT_SCOPE` candidates.",
        f"All raw candidates remain in frozen forensic sources and the exact-scope task partition: `{metrics['forensically_preserved_raw_candidate_count']}/{metrics['raw_candidate_count']}`; model-input occurrences `{metrics['model_input_exact_scope_candidate_occurrences']}` and unique IDs `{metrics['model_input_unique_exact_scope_candidates']}`.",
        "`STRICT_SUBSET`, `STRICT_SUPERSET`, and `OVERLAP` IDs are retained in V2.4 forensic artifacts but are absent from foreign selectable candidate lists.",
        f"Target/hard gate: `{metrics['target_characters']}` / `{metrics['hard_gate_characters']}` characters; shards `{metrics['shard_count']}`; over target `{metrics['payloads_over_target']}`; over hard gate `{safety['payloads_over_hard_gate']}`; oversized atomic tasks `{safety['oversized_task_count']}`.",
        f"Silent truncations `{safety['silent_truncations']}`; candidate-list truncations `{safety['candidate_list_truncations']}`. Verbose facts use explicit compaction markers with full-value SHA-256.",
        "",
        "## Recall",
        "",
        "| Metric | R@1 | R@3 | R@5 | R@10 | No regression |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for key, label, gate in (
        ("raw_candidate_recall", "RAW", "raw_no_regression"),
        ("scope_eligible_recall", "SCOPE-ELIGIBLE", "scope_eligible_no_regression"),
    ):
        value = recall[key]
        lines.append(
            f"| {label} | {value['recall_at_1']} | {value['recall_at_3']} | "
            f"{value['recall_at_5']} | {value['recall_at_10']} | {recall[gate]} |"
        )
    lines.extend(
        [
            "",
            "## Safety",
            "",
            f"Cross-granularity selectable competition `{safety['cross_granularity_selectable_competition']}`; RIGHT_MAP_CONFLICT `{safety['RIGHT_MAP_CONFLICT']}`; capacity defects `{safety['capacity_defect_count']}`; search failures `{safety['search_failure_count']}`.",
            f"Frozen pre-existing group-generation failure diagnostics retained: `{safety['frozen_group_generation_failure_count']}` (not transport defects).",
            f"Projection errors `{safety['projection_error_count']}`; provider-schema problems `{safety['provider_schema_problem_count']}`; `oneOf` present `{safety['provider_schema_contains_oneOf']}`; parser fail-closed `{safety['provider_safe_parser']['fail_closed']}`.",
            "",
            "## IOS2.1 future isolated scoped smoke IDs",
            "",
            "| Control | task_id | scope_id | Kind |",
            "|---|---|---|---|",
        ]
    )
    for row in critical["tasks"]:
        lines.append(
            f"| {row['label']} | `{row['task_id']}` | `{row['scope_id']}` | `{row['scope_kind']}` |"
        )
    lines.extend(
        [
            "",
            "LEFT20 child tasks keep R26/R28/R29 eligible in DOMESTIC/FIRE/METERING respectively. The composite parent keeps `[26,28,29]` eligible while those singletons are not selectable there. LEFT19 keeps R30 and R25 together; LEFT17 R27 and LEFT18 R24 remain eligible.",
            "",
            "## Deterministic replay",
            "",
            "Two independent full builds are required to be byte-identical before any artifact is written; mismatch fails the command.",
            "",
            "## Verdict",
            "",
            f"**{metrics['verdict']}.**",
            "",
            "Even with verdict A: **NO MODEL CALLS. NO DEPLOY. NO SHADOW.**",
            "",
        ]
    )
    return "\n".join(lines)


def _build_payloads() -> dict[str, bytes]:
    metrics, shards, values = _build_objects()
    critical = values["critical"]
    shards_bytes = _jsonl_bytes(shards)
    metrics_bytes = _json_bytes(metrics)
    critical_bytes = _json_bytes(critical)
    report_bytes = _report(metrics, critical).encode("utf-8")
    inputs = values["metadata"]["inputs"]
    manifest = {
        "kind": "function_lineage_scoped_selector_manifest",
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "transport_algorithm": ALGORITHM_VERSION,
        "scope_source_commit": SCOPE_SOURCE_COMMIT,
        "production_baseline_at_research_start": PRODUCTION_BASE_COMMIT,
        "production_release_at_research_start": PRODUCTION_RELEASE,
        "input_artifacts": {
            **{
                str((SCOPE_ROOT / name).relative_to(REPO_ROOT)): digest
                for name, digest in sorted(inputs["scope_hashes"].items())
            },
            **{
                str(scope_graph._input_paths()[name].relative_to(REPO_ROOT)): digest
                for name, digest in sorted(inputs["candidate_hashes"].items())
            },
        },
        "pre_scope_selector_shards": {
            "path": str(PRE_SCOPE_SHARDS.relative_to(REPO_ROOT)),
            "read": False,
            "eligible_as_future_ai_input": False,
        },
        "prompt_template_sha256": _sha_json(PROMPT_TEMPLATE_LINES),
        "provider_safe_schema_source_commit": "0655372c",
        "provider_safe_schema_parser": (
            "experiments.function_lineage_v2.transport.output_schema/"
            "verify_transport_response"
        ),
        "oneOf": False,
        "target_characters": TARGET_CHARACTERS,
        "hard_gate_characters": HARD_CHARACTERS,
        "scoped_task_count": metrics["scoped_task_count"],
        "raw_candidate_count": metrics["raw_candidate_count"],
        "forensically_preserved_raw_candidate_count": metrics[
            "forensically_preserved_raw_candidate_count"
        ],
        "generated_artifacts_sha256": {
            "scoped_selector_shards.jsonl": _sha_bytes(shards_bytes),
            "scoped_selector_transport_metrics.json": _sha_bytes(metrics_bytes),
            "ios21_critical_scoped_contexts.json": _sha_bytes(critical_bytes),
            "report.md": _sha_bytes(report_bytes),
        },
        "deterministic_replay_count": 2,
        "deterministic_replay_byte_identical": True,
        "future_ai_smoke_status": "NOT_RUN",
        "model_configuration": "NOT_APPLICABLE_NO_MODEL_CALLS",
        "model_calls": 0,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
        "verdict": metrics["verdict"],
    }
    return {
        "scoped_selector_manifest.json": _json_bytes(manifest),
        "scoped_selector_shards.jsonl": shards_bytes,
        "scoped_selector_transport_metrics.json": metrics_bytes,
        "ios21_critical_scoped_contexts.json": critical_bytes,
        "report.md": report_bytes,
    }


def build_artifacts(output: Path, *, check: bool = False) -> dict[str, str]:
    first = _build_payloads()
    second = _build_payloads()
    if first != second:
        raise RuntimeError("two deterministic replays are not byte-identical")
    if set(first) != set(ARTIFACT_NAMES):
        raise RuntimeError("unexpected scoped artifact inventory")
    if check:
        mismatches = [
            name
            for name, payload in first.items()
            if not (output / name).is_file()
            or (output / name).read_bytes() != payload
        ]
        if mismatches:
            raise RuntimeError(f"generated artifacts differ: {mismatches}")
    else:
        output.mkdir(parents=True, exist_ok=True)
        for name, payload in first.items():
            (output / name).write_bytes(payload)
    return {name: _sha_bytes(payload) for name, payload in sorted(first.items())}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    hashes = build_artifacts(args.output.resolve(), check=args.check)
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "sha256": hashes,
                "model_calls": 0,
                "verdict": json.loads(
                    (args.output.resolve() / "scoped_selector_transport_metrics.json")
                    .read_text(encoding="utf-8")
                )["verdict"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
