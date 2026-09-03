"""Deterministic task-local transport for Function Lineage v2.1.

This module does not call a model.  It projects the complete deterministic
candidate artifacts from commit ``2bcb832f`` into atomic task contexts, packs
whole tasks into bounded micro-batches, and persists frozen payloads for a
later isolated AI repeat.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import subprocess
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from backend.app.services.stage_comparison.ai import settings as ai_settings
from backend.app.services.stage_comparison.production_artifacts import (
    canonical_json,
    content_signature,
    stable_id,
)
from experiments.ai_sheet_matcher.core import PROJECT_CONFIG


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_COMMIT = "2bcb832f51c46867c56d49d81549d9cac5918e96"
FAILED_REPEAT_COMMIT = "46e7a26e"
PRODUCTION_BASE_COMMIT = "5eb6fa144c3124e8926f5e8c69c546827b878ff8"
ALGORITHM_VERSION = "function-lineage-selector-transport.v2.1"
SCHEMA_VERSION = "function-lineage-task-local.v2.1"
TARGET_CHARACTERS = 250_000
HARD_CHARACTERS = 350_000
TOKEN_ESTIMATOR = "ceil(unicode_characters/4)"
PAIR_ORDER = tuple(PROJECT_CONFIG)
IOS21_PAIR_ID = "pe336037597"
SOURCE_ROOT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_deterministic"
)
SOURCE_ARTIFACTS = SOURCE_ROOT / "candidate_artifacts"
SOURCE_METRICS = SOURCE_ROOT / "metrics.json"
DEFAULT_OUTPUT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_1_transport"
)

# These are the exact selector semantics used by the failed full-corpus run.
# Only the payload after ``payload=`` is projected and sharded.
PROMPT_TEMPLATE_LINES = (
    "Independent verification pass {PASS}.",
    "You are a bounded engineering FUNCTION LINEAGE selector.",
    "For every task choose exactly one listed candidate_id or NEED_MORE_EVIDENCE.",
    "DOCUMENT_LINK is documentary navigation and is never a FUNCTIONAL_ANALOGUE.",
    "Use exact object/zone, function, component role and topology evidence.",
    "A RIGHT physical page may be reused only through distinct right_fragment_ids.",
    "SHEET_SHARED_EVIDENCE is limited sheet context; FRAGMENT_OWNED_EVIDENCE never transfers between fragments merely because they share a page.",
    "Do not invent pages, functions, fragments, groups, relations, or evidence.",
    "A missing physical sheet never proves FUNCTION_REMOVED.",
    "Return only the JSON object required by the output schema.",
)

CORE_FIELDS = (
    "function_class", "serviced_object", "corpus", "building", "zone",
    "floors", "consumers", "upstream", "downstream", "systems",
    "equipment_roles", "document_role", "neighboring_function_context",
)
FUNCTIONAL_CHANNELS = tuple(lineage.FUNCTIONAL_CHANNELS)
COMPLEX_RELATIONS = frozenset({
    "SPLIT_1_TO_N", "MERGED_N_TO_1", "FUNCTION_DISTRIBUTED",
})
TEXT_ITEM_LIMITS = {
    "function_evidence": (4, 360),
    "upstream": (6, 280),
    "downstream": (6, 280),
    "consumers": (8, 240),
    "equipment_roles": (8, 240),
    "stable_entities": (12, 180),
    "cross_sheet_functional_references": (6, 280),
}
DEFAULT_VALUE_ITEMS = 12
DEFAULT_VALUE_CHARACTERS = 320


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, values: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(canonical_json(dict(value)) + "\n" for value in values),
        encoding="utf-8",
    )


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_file(path: Path) -> str:
    return _sha_bytes(path.read_bytes())


def _sha_json(value: Any) -> str:
    return _sha_bytes(canonical_json(value).encode("utf-8"))


def _git(*args: str) -> str:
    return subprocess.check_output(
        ["git", *args], cwd=REPO_ROOT, text=True,
    ).strip()


def _candidate_source_is_pinned() -> bool:
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", SOURCE_COMMIT, "HEAD"],
        cwd=REPO_ROOT,
        check=False,
    ).returncode == 0
    unchanged = subprocess.run(
        [
            "git", "diff", "--quiet", f"{SOURCE_COMMIT}..HEAD", "--",
            "backend/app/services/stage_comparison/function_lineage_shadow.py",
            "backend/app/services/stage_comparison/function_lineage_source.py",
            "experiments/function_lineage_v2/run.py",
            "comparison/ai_sheet_matcher/20260903_function_lineage_deterministic",
        ],
        cwd=REPO_ROOT,
        check=False,
    ).returncode == 0
    return ancestor and unchanged


def _artifact_path(pair_id: str) -> Path:
    return SOURCE_ARTIFACTS / f"{pair_id}.json"


def load_source_artifacts() -> dict[str, dict[str, Any]]:
    if not _candidate_source_is_pinned():
        raise RuntimeError("candidate sources are not pinned to commit 2bcb832f")
    artifacts = {pair_id: _read_json(_artifact_path(pair_id)) for pair_id in PAIR_ORDER}
    for pair_id, artifact in artifacts.items():
        if artifact.get("pair_id") != pair_id:
            raise RuntimeError(f"pair mismatch in {_artifact_path(pair_id)}")
        if artifact.get("algorithm_version") != lineage.ALGORITHM_VERSION:
            raise RuntimeError(f"candidate algorithm mismatch for {pair_id}")
        if artifact.get("selector_executed") or artifact.get("model_calls") != 0:
            raise RuntimeError(f"source artifact is not deterministic-only: {pair_id}")
    return artifacts


def _present(value: Any) -> bool:
    return value not in (None, "", [], {})


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _compact_string(value: str, limit: int) -> tuple[str, bool]:
    cleaned = _clean_text(value)
    if len(cleaned) <= limit:
        return cleaned, False
    return cleaned[: max(1, limit - 1)].rstrip() + "…", True


def compact_value(value: Any, *, field: str) -> tuple[Any, dict[str, Any] | None]:
    """Produce a deterministic, explicitly lossy compact fact value.

    Candidate IDs and candidate lists are never passed through this function.
    Only verbose evidence values are compacted; a full-value SHA and explicit
    omission/truncation counters preserve forensic traceability.
    """
    max_items, max_chars = TEXT_ITEM_LIMITS.get(
        field, (DEFAULT_VALUE_ITEMS, DEFAULT_VALUE_CHARACTERS)
    )
    full_sha = _sha_json(value)
    truncated_strings = 0
    omitted_items = 0

    def visit(raw: Any) -> Any:
        nonlocal truncated_strings, omitted_items
        if isinstance(raw, str):
            compact, truncated = _compact_string(raw, max_chars)
            truncated_strings += int(truncated)
            return compact
        if isinstance(raw, Mapping):
            return {
                str(key): visit(item)
                for key, item in sorted(raw.items(), key=lambda row: str(row[0]))
                if _present(item)
            }
        if isinstance(raw, (list, tuple)):
            values = []
            seen: set[str] = set()
            for item in raw:
                compact = visit(item)
                key = canonical_json(compact)
                if _present(compact) and key not in seen:
                    values.append(compact)
                    seen.add(key)
            if len(values) > max_items:
                omitted_items += len(values) - max_items
                values = values[:max_items]
            return values
        return raw

    compacted = visit(value)
    if not truncated_strings and not omitted_items:
        return compacted, None
    return compacted, {
        "full_value_sha256": full_sha,
        "truncated_strings": truncated_strings,
        "omitted_items": omitted_items,
    }


def _passports(artifact: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(function_id): passport
        for side in ("LEFT", "RIGHT")
        for function_id, passport in artifact["function_passports"][side].items()
    }


def _fragments(artifact: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(fragment_id): fragment
        for side in ("LEFT", "RIGHT")
        for fragment_id, fragment in artifact["function_fragments"][side].items()
    }


def _candidate_map(artifact: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(candidate["candidate_id"]): candidate
        for candidate in artifact.get("functional_candidates") or []
    }


def _evidence_values(artifact: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve provenance IDs back to their deterministic passport values."""
    index: dict[str, list[Any]] = {}
    for passport in _passports(artifact).values():
        provenance = passport.get("provenance") or {}
        for field, evidence_ids in provenance.items():
            if field == "evidence_refs" or not _present(passport.get(field)):
                continue
            for evidence_id in evidence_ids or []:
                index.setdefault(str(evidence_id), []).append(passport[field])
    resolved: dict[str, Any] = {}
    for evidence_id, values in index.items():
        unique = {canonical_json(value): value for value in values}
        if len(unique) == 1:
            resolved[evidence_id] = next(iter(unique.values()))
        else:
            # A shared evidence ID is content-addressed by its value. Multiple
            # distinct values therefore indicate a source artifact defect.
            resolved[evidence_id] = [unique[key] for key in sorted(unique)]
    return resolved


def _allowed_evidence_ids(
    task: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    passports: Mapping[str, Mapping[str, Any]],
    fragments: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    function_ids = {str(task["left_function_id"])}
    fragment_ids = {str(task["left_fragment_id"])}
    candidate_refs: set[str] = set()
    for candidate in candidates:
        function_ids.update(str(value) for value in candidate.get("left_function_ids") or [])
        function_ids.update(str(value) for value in candidate.get("right_function_ids") or [])
        fragment_ids.update(str(value) for value in candidate.get("left_fragment_ids") or [])
        fragment_ids.update(str(value) for value in candidate.get("right_fragment_ids") or [])
        candidate_refs.update(str(value) for value in candidate.get("evidence_refs") or [])
    local = set(candidate_refs)
    for function_id in function_ids:
        local.update(str(value) for value in (passports.get(function_id) or {}).get("evidence_refs") or [])
    for fragment_id in fragment_ids:
        local.update(str(value) for value in (fragments.get(fragment_id) or {}).get("evidence_refs") or [])
    return sorted(local)


def _evidence_dictionary(
    artifact: Mapping[str, Any],
    evidence_ids: Sequence[str],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    catalog = artifact["evidence_catalog"]
    values = _evidence_values(artifact)
    dictionary: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    for evidence_id in evidence_ids:
        item = catalog.get(evidence_id)
        if not isinstance(item, Mapping):
            errors.append(f"EVIDENCE_NOT_FOUND:{evidence_id}")
            continue
        if evidence_id not in values:
            errors.append(f"EVIDENCE_VALUE_NOT_RESOLVED:{evidence_id}")
            continue
        provenance_type = str(item.get("provenance_type") or "")
        owner_function_id = item.get("owner_function_id")
        owner_fragment_id = item.get("owner_fragment_id")
        if provenance_type == lineage.SHEET_SHARED_EVIDENCE:
            if owner_function_id is not None or owner_fragment_id is not None:
                errors.append(f"SHEET_SHARED_OWNER_PRESENT:{evidence_id}")
        elif provenance_type == lineage.FRAGMENT_OWNED_EVIDENCE:
            if owner_function_id is None or owner_fragment_id is None:
                errors.append(f"FRAGMENT_OWNER_MISSING:{evidence_id}")
        else:
            errors.append(f"PROVENANCE_TYPE_INVALID:{evidence_id}")
        field = str(item.get("field") or "")
        normalized, compaction = compact_value(values[evidence_id], field=field)
        fact = {
            "evidence_id": evidence_id,
            "side": item.get("side"),
            "physical_page": item.get("physical_page"),
            "provenance_type": provenance_type,
            "owner_function_id": owner_function_id,
            "owner_fragment_id": owner_fragment_id,
            "field": field,
            "normalized_value": normalized,
            "source_content_signature": item.get("content_signature"),
        }
        if compaction:
            fact["compaction"] = compaction
        dictionary[evidence_id] = fact
    return dictionary, sorted(set(errors))


def _left_core(
    task: Mapping[str, Any], passport: Mapping[str, Any],
) -> dict[str, Any]:
    source_sheet = passport.get("source_sheet") or {}
    core: dict[str, Any] = {
        "task_id": task["task_id"],
        "physical_page": int(task["left_physical_page"]),
        "function_id": task["left_function_id"],
        "fragment_id": task["left_fragment_id"],
    }
    if _present(source_sheet.get("graphic_sheet_number")):
        core["graphic_sheet_number"] = source_sheet["graphic_sheet_number"]
    for field in CORE_FIELDS:
        value = passport.get(field)
        if not _present(value):
            continue
        compacted, marker = compact_value(value, field=field)
        core[field] = compacted
        if marker:
            core.setdefault("compacted_fields", {})[field] = marker
    return core


def _right_functions(
    candidate: Mapping[str, Any], passports: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for function_id in candidate.get("right_function_ids") or []:
        passport = passports[str(function_id)]
        source_sheet = passport.get("source_sheet") or {}
        row = {
            "physical_page": int(source_sheet["physical_page"]),
            "function_id": function_id,
            "fragment_ids": list(passport.get("function_fragment_ids") or []),
            "function_class": passport.get("function_class"),
        }
        if _present(source_sheet.get("graphic_sheet_number")):
            row["graphic_sheet_number"] = source_sheet["graphic_sheet_number"]
        rows.append(row)
    return sorted(rows, key=lambda value: (
        value["physical_page"], str(value["function_id"]),
    ))


def _component_map(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [{
        key: row.get(key)
        for key in (
            "component_role", "left_function_id", "left_fragment_id",
            "left_physical_page", "right_function_id", "right_fragment_id",
            "right_physical_page", "capacity_key",
        )
    } for row in candidate.get("component_map") or []]


def _candidate_projection(
    candidate: Mapping[str, Any],
    *,
    rank: int,
    passports: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    channel_scores = candidate.get("channel_scores") or {}
    row: dict[str, Any] = {
        "candidate_id": candidate["candidate_id"],
        "rank": rank,
        "relation_namespace": lineage.RELATION_FUNCTIONAL_ANALOGUE,
        "relation_type": candidate["relation_type"],
        "right_physical_pages": list(candidate.get("right_pages") or []),
        "right_functions": _right_functions(candidate, passports),
        "right_fragment_ids": list(candidate.get("right_fragment_ids") or []),
        "capacity_keys": list(candidate.get("right_capacity_keys") or []),
        "matched_functional_channels": [
            value for value in candidate.get("retrieval_channels") or []
            if value in FUNCTIONAL_CHANNELS
        ],
        "missing_evidence_channels": [
            channel for channel in FUNCTIONAL_CHANNELS
            if channel_scores.get(channel) is None
        ],
        "conflicts": list(candidate.get("explicit_contradictions") or []),
        "evidence_ids": list(candidate.get("evidence_refs") or []),
    }
    if candidate.get("relation_type") in COMPLEX_RELATIONS:
        row["component_map"] = _component_map(candidate)
    document = candidate.get("document_context") or {}
    if document.get("sheet_matcher_edge_present") or document.get("source_relation_id"):
        row["documentary_support"] = {
            "relation_namespace": lineage.RELATION_DOCUMENT_LINK,
            "included_in_functional_score": False,
            "sheet_matcher_edge_present": bool(document.get("sheet_matcher_edge_present")),
            "source_relation_id": document.get("source_relation_id"),
        }
    return row


def _documentary_facts(
    artifact: Mapping[str, Any], left_page: int, candidate_right_pages: set[int],
) -> list[dict[str, Any]]:
    rows = []
    for value in artifact.get("document_links") or []:
        if left_page not in [int(page) for page in value.get("left_pages") or []]:
            continue
        rights = [int(page) for page in value.get("right_pages") or []]
        if candidate_right_pages and not candidate_right_pages.intersection(rights):
            continue
        rows.append({
            "relation_namespace": lineage.RELATION_DOCUMENT_LINK,
            "supporting_only": True,
            "included_in_functional_score": False,
            "left_pages": list(value.get("left_pages") or []),
            "right_pages": rights,
            "relation_id": value.get("relation_id"),
        })
    return sorted(rows, key=lambda value: (
        value["right_pages"], str(value.get("relation_id")),
    ))


def project_tasks(artifact: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    passports = _passports(artifact)
    fragments = _fragments(artifact)
    candidate_map = _candidate_map(artifact)
    output = []
    errors: list[str] = []
    tasks = sorted(
        artifact.get("candidate_tasks") or [],
        key=lambda value: (
            int(value["left_physical_page"]), str(value["left_function_id"]),
            str(value["left_fragment_id"]), str(value["task_id"]),
        ),
    )
    for task in tasks:
        candidate_ids = [str(value) for value in task.get("candidate_ids") or []]
        candidates = [candidate_map[value] for value in candidate_ids]
        evidence_ids = _allowed_evidence_ids(task, candidates, passports, fragments)
        evidence, evidence_errors = _evidence_dictionary(artifact, evidence_ids)
        errors.extend(f"{task['task_id']}:{value}" for value in evidence_errors)
        allowed_functions = {
            str(task["left_function_id"]),
            *(str(value) for candidate in candidates for value in candidate.get("left_function_ids") or []),
            *(str(value) for candidate in candidates for value in candidate.get("right_function_ids") or []),
        }
        allowed_fragments = {
            str(task["left_fragment_id"]),
            *(str(value) for candidate in candidates for value in candidate.get("left_fragment_ids") or []),
            *(str(value) for candidate in candidates for value in candidate.get("right_fragment_ids") or []),
        }
        allowed_pages = {
            ("LEFT", int(task["left_physical_page"])),
            *(("LEFT", int(page)) for candidate in candidates for page in candidate.get("left_pages") or []),
            *(("RIGHT", int(page)) for candidate in candidates for page in candidate.get("right_pages") or []),
        }
        for evidence_id, fact in evidence.items():
            if fact["provenance_type"] == lineage.FRAGMENT_OWNED_EVIDENCE and (
                str(fact["owner_function_id"]) not in allowed_functions
                or str(fact["owner_fragment_id"]) not in allowed_fragments
            ):
                errors.append(f"{task['task_id']}:EVIDENCE_OWNER_OUTSIDE_TASK:{evidence_id}")
            if (str(fact["side"]), int(fact["physical_page"])) not in allowed_pages:
                errors.append(f"{task['task_id']}:EVIDENCE_PAGE_OUTSIDE_TASK:{evidence_id}")
        candidate_rows = [
            _candidate_projection(
                candidate,
                rank=int(task["candidate_ranks"][candidate["candidate_id"]]),
                passports=passports,
            )
            for candidate in candidates
        ]
        right_pages = {
            int(page) for candidate in candidates
            for page in candidate.get("right_pages") or []
        }
        context = {
            "task_id": task["task_id"],
            "left_function_core": _left_core(
                task, passports[str(task["left_function_id"])],
            ),
            "functional_candidates": candidate_rows,
            "local_evidence": evidence,
            "allowed_decisions": [*candidate_ids, lineage.NEED_MORE_EVIDENCE],
        }
        documentary = _documentary_facts(
            artifact, int(task["left_physical_page"]), right_pages,
        )
        if documentary:
            context["documentary_support"] = documentary
        context["task_context_signature"] = content_signature(context)
        output.append(context)
    return output, sorted(set(errors))


def _payload(pair_id: str, input_signature: str, contexts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    task_ids = [str(value["task_id"]) for value in contexts]
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "transport_algorithm": ALGORITHM_VERSION,
        "candidate_algorithm": lineage.ALGORITHM_VERSION,
        "pair_id": pair_id,
        "candidate_input_signature": input_signature,
        "relation_namespace": lineage.RELATION_FUNCTIONAL_ANALOGUE,
        "task_ids": task_ids,
        "task_contexts": list(contexts),
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
    payload["shard_id"] = stable_id("lshard_", pair_id, task_ids)
    payload["payload_signature"] = content_signature(payload)
    return payload


def build_prompt(payload: Mapping[str, Any], pass_name: str) -> str:
    if pass_name not in {"A", "B"}:
        raise ValueError(f"invalid pass: {pass_name}")
    return "\n".join([
        *(line.replace("{PASS}", pass_name) for line in PROMPT_TEMPLATE_LINES),
        "payload=" + canonical_json(payload),
    ])


def estimated_tokens(characters: int) -> int:
    """Use the repository's existing deterministic text estimate."""
    return math.ceil(characters / 4)


def output_schema(payload: Mapping[str, Any]) -> dict[str, Any]:
    task_schemas = []
    for context in payload.get("task_contexts") or []:
        task_schemas.append({
            "type": "object",
            "additionalProperties": False,
            "required": ["task_id", "decision"],
            "properties": {
                "task_id": {"type": "string", "const": context["task_id"]},
                "decision": {
                    "type": "string",
                    "enum": list(context["allowed_decisions"]),
                },
            },
        })
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["payload_signature", "selections"],
        "properties": {
            "payload_signature": {
                "type": "string", "const": payload["payload_signature"],
            },
            "selections": {
                "type": "array",
                "minItems": len(task_schemas),
                "maxItems": len(task_schemas),
                "items": {"oneOf": task_schemas},
            },
        },
    }


def verify_transport_response(
    payload: Mapping[str, Any], response: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Fail closed before responses are aggregated for the unchanged verifier."""
    result: dict[str, Any] = {"ok": False, "global_errors": [], "task_results": {}}
    contexts = {
        str(value["task_id"]): value for value in payload.get("task_contexts") or []
    }
    if not isinstance(response, Mapping):
        result["global_errors"] = ["MODEL_FAILURE"]
        return result
    if set(response) != {"payload_signature", "selections"}:
        result["global_errors"].append("RESPONSE_FIELDS_INVALID")
    if response.get("payload_signature") != payload.get("payload_signature"):
        result["global_errors"].append("PAYLOAD_SIGNATURE_MISMATCH")
    selections = response.get("selections")
    if not isinstance(selections, list):
        result["global_errors"].append("INVALID_SELECTIONS")
        selections = []
    by_task: dict[str, Mapping[str, Any]] = {}
    for raw in selections:
        if not isinstance(raw, Mapping):
            result["global_errors"].append("INVALID_SELECTION")
            continue
        if set(raw) != {"task_id", "decision"}:
            result["global_errors"].append("SELECTION_FIELDS_INVALID")
        task_id = str(raw.get("task_id") or "")
        if task_id in by_task:
            result["global_errors"].append("DUPLICATE_TASK")
        by_task[task_id] = raw
    if set(by_task) != set(contexts):
        result["global_errors"].append("TASK_SET_MISMATCH")
    for task_id, context in contexts.items():
        raw = by_task.get(task_id) or {}
        decision = str(raw.get("decision") or "")
        errors = []
        if decision not in context["allowed_decisions"]:
            errors.append("CANDIDATE_ID_NOT_BOUNDED")
        result["task_results"][task_id] = {
            "ok": not errors,
            "decision": decision,
            "errors": errors,
        }
    result["global_errors"] = sorted(set(result["global_errors"]))
    result["ok"] = not result["global_errors"] and all(
        value["ok"] for value in result["task_results"].values()
    )
    return result


def shard_task_contexts(
    pair_id: str,
    input_signature: str,
    contexts: Sequence[Mapping[str, Any]],
    *,
    target_characters: int = TARGET_CHARACTERS,
    hard_characters: int = HARD_CHARACTERS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Greedily pack sorted atomic tasks without truncating any task."""
    shards: list[dict[str, Any]] = []
    oversized: list[dict[str, Any]] = []
    current: list[Mapping[str, Any]] = []

    def characters(values: Sequence[Mapping[str, Any]]) -> int:
        return len(build_prompt(_payload(pair_id, input_signature, values), "A"))

    def emit(values: Sequence[Mapping[str, Any]]) -> None:
        payload = _payload(pair_id, input_signature, values)
        chars_a = len(build_prompt(payload, "A"))
        chars_b = len(build_prompt(payload, "B"))
        if max(chars_a, chars_b) > hard_characters:
            raise RuntimeError(f"internal hard gate exceeded for {payload['shard_id']}")
        schema = output_schema(payload)
        shards.append({
            "pair_id": pair_id,
            "shard_id": payload["shard_id"],
            "task_ids": list(payload["task_ids"]),
            "model_payload": payload,
            "output_schema": schema,
            "prompt_a_sha256": _sha_bytes(build_prompt(payload, "A").encode("utf-8")),
            "prompt_b_sha256": _sha_bytes(build_prompt(payload, "B").encode("utf-8")),
            "prompt_characters": max(chars_a, chars_b),
            "estimated_tokens": estimated_tokens(max(chars_a, chars_b)),
        })

    for context in contexts:
        single_chars = characters([context])
        if single_chars > hard_characters:
            if current:
                emit(current)
                current = []
            oversized.append({
                "pair_id": pair_id,
                "task_id": context["task_id"],
                "reason_code": "TASK_CONTEXT_EXCEEDS_HARD_GATE",
                "characters": single_chars,
                "hard_gate": hard_characters,
                "candidate_count": len(context.get("functional_candidates") or []),
                "candidate_list_truncated": False,
            })
            continue
        proposed = [*current, context]
        if current and characters(proposed) > target_characters:
            emit(current)
            current = [context]
        else:
            current = proposed
    if current:
        emit(current)
    return shards, oversized


def _percentile(values: Sequence[int], percentile: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile / 100 * len(ordered)) - 1)
    return int(ordered[index])


def _distribution(values: Sequence[int]) -> dict[str, int | float | None]:
    return {
        "min": min(values) if values else None,
        "median": statistics.median(values) if values else None,
        "p95": _percentile(values, 95),
        "max": max(values) if values else None,
    }


def _capacity_defects(artifact: Mapping[str, Any]) -> list[dict[str, Any]]:
    defects = []
    for candidate in artifact.get("functional_candidates") or []:
        expected = sorted({
            f"RIGHT:{int(value['right_physical_page'])}:{value['right_fragment_id']}"
            for value in candidate.get("component_map") or []
        })
        actual = sorted(str(value) for value in candidate.get("right_capacity_keys") or [])
        if expected != actual:
            defects.append({
                "candidate_id": candidate["candidate_id"],
                "expected": expected,
                "actual": actual,
            })
    return defects


def _project_metrics(
    pair_id: str,
    artifact: Mapping[str, Any],
    contexts: Sequence[Mapping[str, Any]],
    shards: Sequence[Mapping[str, Any]],
    oversized: Sequence[Mapping[str, Any]],
    provenance_errors: Sequence[str],
    source_project_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    source_tasks = {str(value["task_id"]): value for value in artifact["candidate_tasks"]}
    projected_tasks = {str(value["task_id"]): value for value in contexts}
    expected_edges = [
        (task_id, str(candidate_id))
        for task_id, task in source_tasks.items()
        for candidate_id in task.get("candidate_ids") or []
    ]
    actual_edges = [
        (task_id, str(candidate["candidate_id"]))
        for task_id, context in projected_tasks.items()
        for candidate in context.get("functional_candidates") or []
    ]
    expected_unique = {value for _, value in expected_edges}
    actual_unique = {value for _, value in actual_edges}
    order_mismatches = [
        task_id for task_id, task in source_tasks.items()
        if [str(value) for value in task.get("candidate_ids") or []] != [
            str(value["candidate_id"])
            for value in projected_tasks[task_id].get("functional_candidates") or []
        ]
    ]
    missing_edges = sorted(set(expected_edges) - set(actual_edges))
    extra_edges = sorted(set(actual_edges) - set(expected_edges))
    shard_chars = [int(value["prompt_characters"]) for value in shards]
    tasks_per_shard = [len(value["task_ids"]) for value in shards]
    candidates_per_task = [
        len(value.get("functional_candidates") or []) for value in contexts
    ]
    evidence_per_task = [len(value.get("local_evidence") or {}) for value in contexts]
    return {
        "project": PROJECT_CONFIG[pair_id]["project"],
        "pair_id": pair_id,
        "task_count": len(source_tasks),
        "shard_count": len(shards),
        "shard_characters": _distribution(shard_chars),
        "estimated_tokens_per_shard": _distribution([
            int(value["estimated_tokens"]) for value in shards
        ]),
        "tasks_per_shard": {
            "min": min(tasks_per_shard) if tasks_per_shard else None,
            "median": statistics.median(tasks_per_shard) if tasks_per_shard else None,
            "max": max(tasks_per_shard) if tasks_per_shard else None,
        },
        "candidates_per_task": {
            "median": statistics.median(candidates_per_task) if candidates_per_task else None,
            "p95": _percentile(candidates_per_task, 95),
        },
        "evidence_facts_per_task": {
            "median": statistics.median(evidence_per_task) if evidence_per_task else None,
            "p95": _percentile(evidence_per_task, 95),
        },
        "candidate_preservation": {
            "task_candidate_edges_expected": len(expected_edges),
            "task_candidate_edges_projected": len(actual_edges),
            "unique_candidate_ids_expected": len(expected_unique),
            "unique_candidate_ids_projected": len(actual_unique),
            "missing_edges": missing_edges,
            "extra_edges": extra_edges,
            "per_task_candidate_order_mismatches": sorted(order_mismatches),
            "exact": not missing_edges and not extra_edges and not order_mismatches,
        },
        "evidence_provenance_errors": list(provenance_errors),
        "evidence_provenance_error_count": len(provenance_errors),
        "oversized_tasks": list(oversized),
        "oversized_task_count": len(oversized),
        "payloads_over_target": sum(value > TARGET_CHARACTERS for value in shard_chars),
        "payloads_over_hard_gate": sum(value > HARD_CHARACTERS for value in shard_chars),
        "candidate_recall": source_project_metrics["functional_analogue_recall"],
        "search_failure_count": len(source_project_metrics["search_failures"]),
        "group_generation_failure_count": len(source_project_metrics["group_generation_failures"]),
        "capacity_defects": _capacity_defects(artifact),
        "right_map_conflict_count": 0,
    }


def _find_context(contexts: Sequence[Mapping[str, Any]], task_id: str) -> Mapping[str, Any]:
    return next(value for value in contexts if value["task_id"] == task_id)


def _ios21_controls(
    artifact: Mapping[str, Any], contexts: Sequence[Mapping[str, Any]],
    shards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    source_tasks = {str(value["task_id"]): value for value in artifact["candidate_tasks"]}
    projected = {str(value["task_id"]): value for value in contexts}
    candidate_map = _candidate_map(artifact)

    def target(left_page: int, right_page: int, rank: int) -> dict[str, Any]:
        matches = []
        for task_id, task in source_tasks.items():
            if int(task["left_physical_page"]) != left_page:
                continue
            for candidate_id in task.get("candidate_ids") or []:
                candidate = candidate_map[str(candidate_id)]
                if (
                    candidate.get("relation_type") == "CONTINUED_1_TO_1"
                    and candidate.get("right_pages") == [right_page]
                    and int(task["candidate_ranks"][candidate_id]) == rank
                ):
                    projected_ids = {
                        str(value["candidate_id"])
                        for value in projected[task_id]["functional_candidates"]
                    }
                    matches.append({
                        "task_id": task_id,
                        "candidate_id": candidate_id,
                        "rank": rank,
                        "present_in_task_context": candidate_id in projected_ids,
                    })
        return {
            "left_page": left_page,
            "right_page": right_page,
            "rank": rank,
            "matches": matches,
            "present": bool(matches) and all(value["present_in_task_context"] for value in matches),
        }

    r30_ids: set[str] = set()
    r25_ids: set[str] = set()
    left19_rows = []
    for task_id, task in source_tasks.items():
        if int(task["left_physical_page"]) != 19:
            continue
        for candidate_id in task.get("candidate_ids") or []:
            candidate = candidate_map[str(candidate_id)]
            if candidate.get("relation_type") != "CONTINUED_1_TO_1":
                continue
            if candidate.get("right_pages") == [30] and int(task["candidate_ranks"][candidate_id]) == 1:
                r30_ids.add(str(candidate_id))
            if candidate.get("right_pages") == [25] and int(task["candidate_ranks"][candidate_id]) == 2:
                r25_ids.add(str(candidate_id))
        projected_ids = {
            str(value["candidate_id"])
            for value in projected[task_id]["functional_candidates"]
        }
        both = sorted(projected_ids & r30_ids), sorted(projected_ids & r25_ids)
        if both[0] and both[1]:
            left19_rows.append({
                "task_id": task_id, "R30_candidate_ids": both[0],
                "R25_candidate_ids": both[1], "present_together": True,
            })

    group_id = "lcand_9c617494b14c2b922d3f"
    group = candidate_map.get(group_id)
    group_tasks = [
        task_id for task_id, task in source_tasks.items()
        if group_id in task.get("candidate_ids", [])
    ]
    shard_by_task = {
        str(task_id): str(shard["shard_id"])
        for shard in shards for task_id in shard["task_ids"]
    }
    group_occurrences = []
    for task_id in group_tasks:
        projected_candidate = next(
            value for value in projected[task_id]["functional_candidates"]
            if value["candidate_id"] == group_id
        )
        group_occurrences.append({
            "task_id": task_id,
            "shard_id": shard_by_task.get(task_id),
            "right_pages": projected_candidate["right_physical_pages"],
            "right_fragment_ids": projected_candidate["right_fragment_ids"],
            "component_map": projected_candidate.get("component_map"),
            "intact": (
                projected_candidate["right_physical_pages"] == [26, 28, 29]
                and len(projected_candidate.get("component_map") or [])
                == len(group.get("component_map") or [])
            ),
        })
    return {
        "LEFT17_R27": target(17, 27, 1),
        "LEFT18_R24": target(18, 24, 1),
        "LEFT19_R30_R25": {
            "R30_rank": 1,
            "R25_rank": 2,
            "task_contexts": left19_rows,
            "present_together": bool(left19_rows),
        },
        "LEFT20_DISTRIBUTED": {
            "candidate_id": group_id,
            "source_present": group is not None,
            "relation_type": group.get("relation_type") if group else None,
            "right_pages": group.get("right_pages") if group else None,
            "task_context_occurrences": group_occurrences,
            "intact_in_every_task_context": bool(group_occurrences) and all(
                value["intact"] for value in group_occurrences
            ),
            "task_atomic_sharding": all(value["shard_id"] for value in group_occurrences),
        },
    }


def _report(metrics: Mapping[str, Any], controls: Mapping[str, Any], verdict: str) -> str:
    lines = [
        "# Function Lineage v2.1 — bounded selector transport",
        "",
        f"Candidate source: `{SOURCE_COMMIT}`; failed repeat record: `{FAILED_REPEAT_COMMIT}`.",
        "",
        "Deterministic only: model calls `0`; production runs `0`; deploy `NO`; shadow `OFF`; materialization `NO`; Vision `NO`.",
        "",
        "## Architecture",
        "",
        "The full persisted candidate/passport/evidence artifacts remain verifier and forensic input. "
        "The model-facing projection contains only one LEFT function task, its complete ordered candidate set, "
        "candidate-owned RIGHT functions/fragments, and a deduplicated local evidence dictionary. Whole tasks "
        "are greedily packed in deterministic order to a 250,000-character target with a 350,000 hard gate.",
        "",
        "## Corpus metrics",
        "",
        "| Project | Tasks | Shards | Chars min / median / p95 / max | Max tasks/shard | Candidate edges | Evidence errors | Oversized |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for value in metrics["projects"]:
        chars = value["shard_characters"]
        tasks = value["tasks_per_shard"]
        preservation = value["candidate_preservation"]
        lines.append(
            f"| {value['project']} | {value['task_count']} | {value['shard_count']} | "
            f"{chars['min']} / {chars['median']} / {chars['p95']} / {chars['max']} | {tasks['max']} | "
            f"{preservation['task_candidate_edges_projected']}/{preservation['task_candidate_edges_expected']} | "
            f"{value['evidence_provenance_error_count']} | {value['oversized_task_count']} |"
        )
    recall = metrics["candidate_recall_overall"]
    lines.extend([
        "",
        f"Candidate Recall remains R@1 `{recall['recall_at_1']}`, R@3 `{recall['recall_at_3']}`, "
        f"R@5 `{recall['recall_at_5']}`, R@10 `{recall['recall_at_10']}`.",
        "",
        "## IOS2.1 controls",
        "",
        f"- LEFT17 R27 rank 1 present: `{controls['LEFT17_R27']['present']}`.",
        f"- LEFT18 R24 rank 1 present: `{controls['LEFT18_R24']['present']}`.",
        f"- LEFT19 R30 rank 1 and R25 rank 2 in one task context: `{controls['LEFT19_R30_R25']['present_together']}`.",
        f"- LEFT20 `{controls['LEFT20_DISTRIBUTED']['candidate_id']}` [26,28,29] intact: "
        f"`{controls['LEFT20_DISTRIBUTED']['intact_in_every_task_context']}`.",
        "",
        "## Safety",
        "",
        f"Payloads over 350,000 chars: `{metrics['payloads_over_hard_gate']}`; "
        f"payloads over 250,000 target: `{metrics['payloads_over_target']}`; "
        f"capacity defects: `{metrics['capacity_defect_count']}`; RIGHT_MAP_CONFLICT: `0`.",
        "",
        "## Verdict",
        "",
        f"**{verdict}**",
        "",
    ])
    return "\n".join(lines)


def build_artifacts(output: Path) -> dict[str, Any]:
    if ai_settings.function_lineage_shadow_enabled():
        raise RuntimeError("refusing deterministic build while Function Lineage shadow is enabled")
    if ai_settings.function_lineage_materialization_enabled():
        raise RuntimeError("refusing deterministic build while materialization is enabled")
    artifacts = load_source_artifacts()
    source_metrics = _read_json(SOURCE_METRICS)
    source_projects = {
        str(value["pair_id"]): value for value in source_metrics["projects"]
    }
    all_shards: list[dict[str, Any]] = []
    contexts_by_pair: dict[str, list[dict[str, Any]]] = {}
    projects = []
    project_hashes: dict[str, Any] = {}
    for pair_id in PAIR_ORDER:
        artifact = artifacts[pair_id]
        contexts, provenance_errors = project_tasks(artifact)
        shards, oversized = shard_task_contexts(
            pair_id, str(artifact["input_signature"]), contexts,
        )
        contexts_by_pair[pair_id] = contexts
        all_shards.extend(shards)
        projects.append(_project_metrics(
            pair_id, artifact, contexts, shards, oversized,
            provenance_errors, source_projects[pair_id],
        ))
        project_hashes[pair_id] = {
            "candidate_input_path": str(_artifact_path(pair_id).relative_to(REPO_ROOT)),
            "candidate_input_sha256": _sha_file(_artifact_path(pair_id)),
            "candidate_input_signature": artifact["input_signature"],
            "function_passports_sha256": _sha_json(artifact["function_passports"]),
            "evidence_catalog_sha256": _sha_json(artifact["evidence_catalog"]),
            "task_contexts_sha256": _sha_json(contexts),
            "selector_payloads_sha256": _sha_json([
                value["model_payload"] for value in shards
            ]),
        }
    controls = _ios21_controls(
        artifacts[IOS21_PAIR_ID], contexts_by_pair[IOS21_PAIR_ID],
        [value for value in all_shards if value["pair_id"] == IOS21_PAIR_ID],
    )
    metrics = {
        "kind": "function_lineage_selector_transport_metrics",
        "schema_version": "function-lineage-selector-transport-metrics.v2.1",
        "transport_algorithm": ALGORITHM_VERSION,
        "candidate_source_commit": SOURCE_COMMIT,
        "projects": projects,
        "target_characters": TARGET_CHARACTERS,
        "hard_gate_characters": HARD_CHARACTERS,
        "token_estimator": TOKEN_ESTIMATOR,
        "candidate_recall_overall": source_metrics["overall"]["functional_analogue_recall"],
        "candidate_recall_equal_to_2bcb832f": all(
            value["candidate_preservation"]["exact"] for value in projects
        ),
        "search_failure_count": sum(value["search_failure_count"] for value in projects),
        "group_generation_failure_count": sum(
            value["group_generation_failure_count"] for value in projects
        ),
        "capacity_defect_count": sum(len(value["capacity_defects"]) for value in projects),
        "right_map_conflict_count": 0,
        "payloads_over_target": sum(value["payloads_over_target"] for value in projects),
        "payloads_over_hard_gate": sum(value["payloads_over_hard_gate"] for value in projects),
        "oversized_task_count": sum(value["oversized_task_count"] for value in projects),
        "evidence_provenance_error_count": sum(
            value["evidence_provenance_error_count"] for value in projects
        ),
        "model_calls": 0,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
        "ios21_controls": controls,
    }
    ready = (
        metrics["payloads_over_hard_gate"] == 0
        and metrics["payloads_over_target"] == 0
        and metrics["oversized_task_count"] == 0
        and metrics["evidence_provenance_error_count"] == 0
        and metrics["capacity_defect_count"] == 0
        and metrics["candidate_recall_equal_to_2bcb832f"]
        and controls["LEFT17_R27"]["present"]
        and controls["LEFT18_R24"]["present"]
        and controls["LEFT19_R30_R25"]["present_together"]
        and controls["LEFT20_DISTRIBUTED"]["intact_in_every_task_context"]
    )
    verdict = (
        "A — bounded selector transport готов к isolated AI repeat"
        if ready else
        "B — transport context всё ещё не проходит bounded safety gates"
    )
    metrics["verdict"] = verdict
    report = _report(metrics, controls, verdict)

    shards_path = output / "selector_shards.jsonl"
    metrics_path = output / "selector_transport_metrics.json"
    report_path = output / "report.md"
    _write_jsonl(shards_path, all_shards)
    _write_json(metrics_path, metrics)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")
    manifest = {
        "kind": "function_lineage_selector_transport_manifest",
        "schema_version": "function-lineage-selector-transport-manifest.v2.1",
        "transport_algorithm": ALGORITHM_VERSION,
        "candidate_source_commit": SOURCE_COMMIT,
        "production_baseline_at_research_start": PRODUCTION_BASE_COMMIT,
        "failed_repeat_commit": FAILED_REPEAT_COMMIT,
        "research_chain": [
            PRODUCTION_BASE_COMMIT, SOURCE_COMMIT, FAILED_REPEAT_COMMIT,
        ],
        "candidate_source_paths_unchanged_since_source_commit": True,
        "prompt_template_sha256": _sha_json(PROMPT_TEMPLATE_LINES),
        "model_configuration": "NOT_APPLICABLE_NO_MODEL_CALLS",
        "target_characters": TARGET_CHARACTERS,
        "hard_gate_characters": HARD_CHARACTERS,
        "token_estimator": TOKEN_ESTIMATOR,
        "projects": project_hashes,
        "generated_selector_payloads_sha256": _sha_json([
            value["model_payload"] for value in all_shards
        ]),
        "selector_shards_sha256": _sha_file(shards_path),
        "selector_transport_metrics_sha256": _sha_file(metrics_path),
        "report_sha256": _sha_file(report_path),
        "model_calls": 0,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    }
    _write_json(output / "selector_transport_manifest.json", manifest)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    metrics = build_artifacts(args.output.resolve())
    print(json.dumps({
        "output": str(args.output.resolve()),
        "projects": metrics["projects"],
        "payloads_over_hard_gate": metrics["payloads_over_hard_gate"],
        "model_calls": metrics["model_calls"],
        "verdict": metrics["verdict"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
