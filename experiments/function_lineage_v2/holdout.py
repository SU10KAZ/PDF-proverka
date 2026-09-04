"""Function Lineage v2.6 — independent holdout sample and external gate.

Phase 5 of the v2.6 master task.  The module prepares, but never runs, an AI
evaluation:

* the v2.5 36-task sample has become a diagnostic set, so it is excluded;
* the seven v2.4.2 controls are excluded from the new sample and carried as
  sentinels with prompts byte-identical to the frozen v2.5 sentinel shards;
* selection is deterministic under a new salt, uses no page number, file name
  or manual choice, and covers every stratum available in each corpus;
* every model input is frozen and hashed before the gate.

Running the evaluation requires explicit user consent.  ``prepare`` stops at
the disclosure; there is no code path here that calls a model.
"""
from __future__ import annotations

import copy
import hashlib
import json
import statistics
import time
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from backend.app.services.stage_comparison.production_artifacts import (
    canonical_json,
    content_signature,
    stable_id,
)
from backend.app.services.stage_comparison.ai import gateway as ai_gateway
from backend.app.services.stage_comparison.ai import settings as ai_settings
from experiments.function_lineage_v2 import scoped_smoke
from experiments.function_lineage_v2 import scoped_transport
from experiments.function_lineage_v2 import smoke as base_smoke
from experiments.function_lineage_v2 import stratified
from experiments.function_lineage_v2 import transport


DEFAULT_OUTPUT = (
    stratified.COMPARISON_ROOT / "20260904_function_lineage_v2_6_holdout_evaluation"
)
V25_ROOT = stratified.DEFAULT_OUTPUT
SCHEMA_VERSION = "function-lineage-holdout-evaluation.v2.6"
PAYLOAD_SCHEMA_VERSION = "function-lineage-holdout-scoped-evaluation.v2.6"
SHARD_PREFIX = "fs26_"

#: A new salt, so the holdout ordering is independent of the v2.5 ordering.
SELECTION_SALT = "function-lineage-v2.6-holdout-sample-v1"

PASSES = stratified.PASSES
HOLDOUT_COLD_RUNS = (1, 2, 3)
SENTINEL_COLD_RUNS = (1,)
SAMPLE_SIZE_PER_CORPUS = 12
SAMPLE_SIZE = SAMPLE_SIZE_PER_CORPUS * len(stratified.CORPUS_ORDER)

STRATA = stratified.STRATA
CORPUS_ORDER = stratified.CORPUS_ORDER
PAIR_PROJECTS = stratified.PAIR_PROJECTS
PROJECT_PAIRS = stratified.PROJECT_PAIRS
SENTINELS = stratified.SENTINELS
SENTINEL_IDS = stratified.SENTINEL_IDS

MODEL_CONFIGURATION = copy.deepcopy(stratified.MODEL_CONFIGURATION)
MODEL_CONFIGURATION.update({
    "cold_runs": list(HOLDOUT_COLD_RUNS),
    "sentinel_cold_runs": list(SENTINEL_COLD_RUNS),
})

#: The v2.5 evaluation this holdout must stay independent of.
DIAGNOSTIC_SOURCES = {
    "stratified_population.json": "",
    "stratified_sample.json": "",
}


def _json_bytes(value: Any) -> bytes:
    return stratified._json_bytes(value)


def _jsonl_bytes(values: Sequence[Mapping[str, Any]]) -> bytes:
    return stratified._jsonl_bytes(values)


def _sha_json(value: Any) -> str:
    return base_smoke._sha_json(value)


def _selection_hash(task_id: str) -> str:
    return hashlib.sha256(
        f"{SELECTION_SALT}|{task_id}".encode("utf-8")
    ).hexdigest()


# ---------------------------------------------------------------------------
# population and exclusions
# ---------------------------------------------------------------------------


def diagnostic_task_ids() -> dict[str, Any]:
    """The v2.5 sample, now a diagnostic set, plus the sentinel controls."""
    sample = stratified._read_json(V25_ROOT / "stratified_sample.json")
    return {
        "v2_5_diagnostic_task_ids": sorted(str(value) for value in sample["selected_task_ids"]),
        "sentinel_task_ids": sorted(str(value) for value in SENTINEL_IDS),
        "v2_5_sample_sha256": base_smoke._sha_file(V25_ROOT / "stratified_sample.json"),
        "v2_5_population_sha256": base_smoke._sha_file(V25_ROOT / "stratified_population.json"),
    }


def build_population() -> dict[str, Any]:
    """The scoped population re-labelled for holdout eligibility."""
    sources = stratified._load_sources()
    population = stratified.build_population(sources)
    diagnostics = diagnostic_task_ids()
    excluded = set(diagnostics["v2_5_diagnostic_task_ids"]) | set(
        diagnostics["sentinel_task_ids"]
    )
    rows = []
    for row in population["tasks"]:
        task_id = str(row["task_id"])
        value = dict(row)
        value["holdout_selection_hash"] = _selection_hash(task_id)
        value["holdout_eligible"] = task_id not in excluded
        value["exclusion_reason"] = (
            "V2_5_DIAGNOSTIC_SET"
            if task_id in set(diagnostics["v2_5_diagnostic_task_ids"])
            else "V2_4_2_SENTINEL" if task_id in set(diagnostics["sentinel_task_ids"])
            else None
        )
        rows.append(value)
    rows.sort(key=lambda value: str(value["task_id"]))
    eligible = [value for value in rows if value["holdout_eligible"]]
    availability = {
        corpus: {
            stratum: sum(
                stratum in value["strata"] for value in eligible
                if value["corpus"] == corpus
            )
            for stratum in STRATA
        }
        for corpus in CORPUS_ORDER
    }
    return {
        "kind": "function_lineage_v2_6_holdout_population",
        "schema_version": "function-lineage-holdout-population.v2.6",
        "population_size": len(rows),
        "holdout_eligible_size": len(eligible),
        "holdout_eligible_by_corpus": dict(sorted(
            Counter(value["corpus"] for value in eligible).items()
        )),
        "excluded": diagnostics,
        "strata": STRATA,
        "label_thresholds": population["label_thresholds"],
        "reference_taxonomy": population["reference_taxonomy"],
        "selection_prohibitions": {
            "filename_used": False,
            "page_number_used_as_order_or_hash": False,
            "manual_task_choice": False,
            "v2_5_diagnostic_tasks_reused": False,
            "sentinels_in_new_sample": False,
        },
        "stratum_availability": availability,
        "selection_salt": SELECTION_SALT,
        "tasks": rows,
        "_sources": sources,
    }


# ---------------------------------------------------------------------------
# deterministic selection
# ---------------------------------------------------------------------------


def select_sample(population: Mapping[str, Any]) -> dict[str, Any]:
    """Greedy rare multi-label coverage with a salted SHA-256 tie-break."""
    rows = [value for value in population["tasks"] if value["holdout_eligible"]]
    row_by_id = {str(value["task_id"]): value for value in rows}
    selected_rows: list[Mapping[str, Any]] = []
    trace: list[dict[str, Any]] = []
    for corpus in CORPUS_ORDER:
        eligible = [value for value in rows if value["corpus"] == corpus]
        if len(eligible) < SAMPLE_SIZE_PER_CORPUS:
            raise RuntimeError(
                f"holdout corpus {corpus} has only {len(eligible)} eligible tasks"
            )
        availability = {
            stratum: sum(stratum in value["strata"] for value in eligible)
            for stratum in STRATA
        }
        selected_ids: set[str] = set()
        covered: set[str] = set()
        while len(selected_ids) < SAMPLE_SIZE_PER_CORPUS:
            options = [
                value for value in eligible
                if str(value["task_id"]) not in selected_ids
            ]

            def key(row: Mapping[str, Any]) -> tuple[Any, ...]:
                new = set(row["strata"]) - covered
                rarity_gain = sum(1 / availability[value] for value in new)
                return (
                    -len(new), -rarity_gain, -len(row["strata"]),
                    row["holdout_selection_hash"],
                )

            chosen = min(options, key=key)
            new_strata = sorted(set(chosen["strata"]) - covered)
            selected_ids.add(str(chosen["task_id"]))
            covered.update(chosen["strata"])
            trace.append({
                "corpus": corpus,
                "task_id": str(chosen["task_id"]),
                "phase": (
                    "GREEDY_RARE_MULTI_LABEL_COVERAGE" if new_strata else "HASHED_FILL"
                ),
                "new_strata": new_strata,
            })
        available = {stratum for value in eligible for stratum in value["strata"]}
        if not available.issubset(covered):
            raise RuntimeError(
                f"holdout sample misses available strata in {corpus}: "
                f"{sorted(available - covered)}"
            )
        selected_rows.extend(sorted(
            (row_by_id[task_id] for task_id in selected_ids),
            key=lambda value: value["holdout_selection_hash"],
        ))
    if len({str(value["task_id"]) for value in selected_rows}) != SAMPLE_SIZE:
        raise RuntimeError("holdout sample is not exactly 36 unique tasks")
    overlap = {str(value["task_id"]) for value in selected_rows} & (
        set(population["excluded"]["v2_5_diagnostic_task_ids"])
        | set(population["excluded"]["sentinel_task_ids"])
    )
    if overlap:
        raise RuntimeError(f"holdout sample reused excluded tasks: {sorted(overlap)}")
    coverage = {
        stratum: {
            "eligible_population": sum(
                stratum in value["strata"] for value in rows
            ),
            "selected_tasks": sum(
                stratum in value["strata"] for value in selected_rows
            ),
            "covered": any(stratum in value["strata"] for value in selected_rows),
        }
        for stratum in STRATA
    }
    return {
        "kind": "function_lineage_v2_6_holdout_sample",
        "schema_version": "function-lineage-holdout-sample.v2.6",
        "selection_algorithm": {
            "name": "greedy rare multi-label coverage with salted SHA-256 tie-break",
            "version": SELECTION_SALT,
            "salt": SELECTION_SALT,
            "corpus_quota": SAMPLE_SIZE_PER_CORPUS,
            "greedy_order": (
                "new-label count, inverse-frequency rarity gain, total labels, "
                "salted task hash"
            ),
            "page_or_filename_selection": False,
            "manual_selection": False,
            "independence": (
                "the v2.5 36-task diagnostic set and the seven v2.4.2 sentinels "
                "are excluded from the eligible population"
            ),
        },
        "sample_size": len(selected_rows),
        "sample_size_by_corpus": dict(sorted(
            Counter(value["corpus"] for value in selected_rows).items()
        )),
        "selected_task_ids": [str(value["task_id"]) for value in selected_rows],
        "selected_tasks": [{
            "task_id": str(value["task_id"]),
            "scope_id": value["scope_id"],
            "corpus": value["corpus"],
            "scope_kind": value["scope_kind"],
            "strata": value["strata"],
            "selection_reason": value["strata_reasons"],
            "selection_hash": value["holdout_selection_hash"],
            "frozen_context_sha256": value["frozen_context_sha256"],
            "candidate_count": value["candidate_count"],
            "reference_classes": value["reference_classes"],
        } for value in selected_rows],
        "stratum_coverage": coverage,
        "uncoverable_strata": sorted(
            stratum for stratum, value in coverage.items()
            if value["eligible_population"] == 0
        ),
        "selection_trace": trace,
        "sentinels_reported_separately": [
            {"label": label, **value} for label, value in SENTINELS.items()
        ],
    }


# ---------------------------------------------------------------------------
# frozen model inputs
# ---------------------------------------------------------------------------


def _payload(
    *, pair_id: str, eval_set: str, input_signature: str,
    contexts: Sequence[Mapping[str, Any]],
    schema_version: str, shard_prefix: str,
) -> dict[str, Any]:
    task_ids = [str(value["task_id"]) for value in contexts]
    payload = {
        "schema_version": schema_version,
        "transport_algorithm": schema_version,
        "candidate_algorithm": lineage.ALGORITHM_VERSION,
        "pair_id": pair_id,
        "evaluation_set": eval_set,
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
            "function_removed_selectable": False,
        },
    }
    payload["shard_id"] = stable_id(shard_prefix, pair_id, eval_set, task_ids)
    payload["payload_signature"] = content_signature(payload)
    return payload


def _prompt(payload: Mapping[str, Any], pass_name: str) -> str:
    if pass_name not in PASSES:
        raise ValueError(f"invalid pass: {pass_name}")
    return "\n".join([
        *(value.replace("{PASS}", pass_name) for value in scoped_smoke.PROMPT_LINES),
        "payload=" + canonical_json(payload),
    ])


def _build_shards(
    *, pair_id: str, eval_set: str, input_signature: str,
    contexts: Sequence[Mapping[str, Any]],
    schema_version: str, shard_prefix: str,
) -> list[dict[str, Any]]:
    shards: list[dict[str, Any]] = []
    current: list[Mapping[str, Any]] = []

    def make(values: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        return _payload(
            pair_id=pair_id, eval_set=eval_set, input_signature=input_signature,
            contexts=values, schema_version=schema_version, shard_prefix=shard_prefix,
        )

    def characters(values: Sequence[Mapping[str, Any]]) -> int:
        return len(_prompt(make(values), "A"))

    def emit(values: Sequence[Mapping[str, Any]]) -> None:
        payload = make(values)
        prompts = {name: _prompt(payload, name) for name in PASSES}
        prompt_characters = max(len(value) for value in prompts.values())
        if prompt_characters > scoped_transport.HARD_CHARACTERS:
            raise RuntimeError(f"shard exceeds hard character gate: {payload['shard_id']}")
        schema = transport.output_schema(payload)
        problems = transport.provider_safe_schema_problems(schema)
        if problems or "oneOf" in canonical_json(schema):
            raise RuntimeError(f"provider-unsafe schema: {problems}")
        shards.append({
            "evaluation_set": eval_set,
            "pair_id": pair_id,
            "corpus": PAIR_PROJECTS[pair_id],
            "shard_id": payload["shard_id"],
            "task_ids": list(payload["task_ids"]),
            "scope_ids": list(payload["scope_ids"]),
            "model_payload": payload,
            "output_schema": schema,
            "provider_safe_schema_problems": problems,
            "prompt_a_sha256": base_smoke._sha_bytes(prompts["A"].encode("utf-8")),
            "prompt_b_sha256": base_smoke._sha_bytes(prompts["B"].encode("utf-8")),
            "prompt_characters": prompt_characters,
        })

    for context in contexts:
        if characters([context]) > scoped_transport.HARD_CHARACTERS:
            raise RuntimeError(f"atomic task exceeds hard gate: {context['task_id']}")
        proposed = [*current, context]
        if current and characters(proposed) > scoped_transport.TARGET_CHARACTERS:
            emit(current)
            current = [context]
        else:
            current = proposed
    if current:
        emit(current)
    return shards


def build_frozen_objects() -> dict[str, Any]:
    population = build_population()
    sources = population.pop("_sources")
    sample = select_sample(population)
    row_by_id = {str(value["task_id"]): value for value in population["tasks"]}
    shards: list[dict[str, Any]] = []

    holdout_ids = set(sample["selected_task_ids"])
    for corpus in CORPUS_ORDER:
        pair_id = PROJECT_PAIRS[corpus]
        ordered = [
            str(value["task_id"]) for value in sorted(
                (
                    row_by_id[task_id] for task_id in holdout_ids
                    if row_by_id[task_id]["corpus"] == corpus
                ),
                key=lambda value: value["holdout_selection_hash"],
            )
        ]
        if not ordered:
            continue
        shards.extend(_build_shards(
            pair_id=pair_id,
            eval_set="HOLDOUT",
            input_signature=str(sources["raw"][pair_id]["input_signature"]),
            contexts=[
                stratified._smoke_context(sources["contexts"][task_id])
                for task_id in ordered
            ],
            schema_version=PAYLOAD_SCHEMA_VERSION,
            shard_prefix=SHARD_PREFIX,
        ))

    # Sentinel prompts are rebuilt with the frozen v2.5 payload identity so the
    # controls stay a true regression reference rather than a new measurement.
    for corpus in CORPUS_ORDER:
        pair_id = PROJECT_PAIRS[corpus]
        ordered = [
            str(value["task_id"]) for value in sorted(
                (
                    row_by_id[task_id] for task_id in SENTINEL_IDS
                    if row_by_id[task_id]["corpus"] == corpus
                ),
                key=lambda value: value["selection_hash"],
            )
        ]
        if not ordered:
            continue
        shards.extend(_build_shards(
            pair_id=pair_id,
            eval_set="SENTINEL",
            input_signature=str(sources["raw"][pair_id]["input_signature"]),
            contexts=[
                stratified._smoke_context(sources["contexts"][task_id])
                for task_id in ordered
            ],
            schema_version="function-lineage-stratified-scoped-evaluation.v2.5",
            shard_prefix="fs25_",
        ))
    shards.sort(key=lambda value: (
        value["evaluation_set"], value["corpus"], value["shard_id"]
    ))
    preflight = _preflight(population, sample, shards)
    if not preflight["ok"]:
        raise RuntimeError(f"holdout preflight failed: {preflight['failures']}")
    return {
        "population": population,
        "sample": sample,
        "shards": shards,
        "preflight": preflight,
    }


# ---------------------------------------------------------------------------
# preflight
# ---------------------------------------------------------------------------


def _frozen_sentinel_shards() -> dict[str, dict[str, Any]]:
    return {
        str(value["shard_id"]): value
        for value in stratified._read_jsonl(V25_ROOT / "model_inputs.jsonl")
        if str(value["evaluation_set"]) == "SENTINEL"
    }


def _preflight(
    population: Mapping[str, Any],
    sample: Mapping[str, Any],
    shards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    failures: list[str] = []
    holdout = [value for value in shards if value["evaluation_set"] == "HOLDOUT"]
    sentinel = [value for value in shards if value["evaluation_set"] == "SENTINEL"]

    covered_tasks = sorted({
        task_id for value in holdout for task_id in value["task_ids"]
    })
    if covered_tasks != sorted(sample["selected_task_ids"]):
        failures.append("HOLDOUT_SHARDS_DO_NOT_COVER_THE_SAMPLE_EXACTLY")
    if len(covered_tasks) != SAMPLE_SIZE:
        failures.append("HOLDOUT_SAMPLE_SIZE_MISMATCH")

    excluded = set(population["excluded"]["v2_5_diagnostic_task_ids"]) | set(
        population["excluded"]["sentinel_task_ids"]
    )
    if excluded & set(covered_tasks):
        failures.append("HOLDOUT_SHARD_CARRIES_AN_EXCLUDED_TASK")

    frozen = _frozen_sentinel_shards()
    sentinel_identical = []
    for value in sentinel:
        reference = frozen.get(str(value["shard_id"]))
        identical = bool(
            reference
            and reference["prompt_a_sha256"] == value["prompt_a_sha256"]
            and reference["prompt_b_sha256"] == value["prompt_b_sha256"]
        )
        sentinel_identical.append(identical)
        if not identical:
            failures.append(f"SENTINEL_PROMPT_DRIFTED:{value['shard_id']}")
    if len(sentinel) != len(frozen):
        failures.append("SENTINEL_SHARD_COUNT_CHANGED")

    if any(value["provider_safe_schema_problems"] for value in shards):
        failures.append("PROVIDER_UNSAFE_SCHEMA")
    if any(
        value["prompt_characters"] > scoped_transport.HARD_CHARACTERS
        for value in shards
    ):
        failures.append("PROMPT_EXCEEDS_HARD_GATE")

    truncated = [
        context["task_id"]
        for value in shards for context in value["model_payload"]["task_contexts"]
        if len(context["functional_candidates"]) != len(
            [item for item in context["allowed_decisions"]
             if str(item).startswith("lcand_")]
        )
    ]
    if truncated:
        failures.append("CANDIDATE_LIST_TRUNCATED")

    return {
        "ok": not failures,
        "failures": sorted(set(failures)),
        "holdout_shard_count": len(holdout),
        "sentinel_shard_count": len(sentinel),
        "sentinel_prompts_identical_to_v2_5": all(sentinel_identical) and bool(sentinel),
        "holdout_task_count": len(covered_tasks),
        "excluded_task_count": len(excluded),
        "uncoverable_strata": sample["uncoverable_strata"],
        "model_calls": 0,
    }


# ---------------------------------------------------------------------------
# external disclosure gate
# ---------------------------------------------------------------------------


def input_manifest(objects: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "kind": "function_lineage_holdout_input_manifest",
        "schema_version": SCHEMA_VERSION,
        "production_head": stratified.PRODUCTION_HEAD,
        "production_release": stratified.PRODUCTION_RELEASE,
        "frozen_research_sources": stratified.SOURCE_HASHES,
        "v2_5_diagnostic_artifacts": {
            "stratified_population.json": objects["population"]["excluded"][
                "v2_5_population_sha256"
            ],
            "stratified_sample.json": objects["population"]["excluded"][
                "v2_5_sample_sha256"
            ],
        },
        "holdout_population_sha256": hashlib.sha256(
            _json_bytes(objects["population"])
        ).hexdigest(),
        "holdout_sample_sha256": hashlib.sha256(
            _json_bytes(objects["sample"])
        ).hexdigest(),
        "model_inputs_sha256": hashlib.sha256(
            _jsonl_bytes(objects["shards"])
        ).hexdigest(),
        "shard_sha256": {
            str(value["shard_id"]): {
                "prompt_a_sha256": value["prompt_a_sha256"],
                "prompt_b_sha256": value["prompt_b_sha256"],
                "payload_signature": value["model_payload"]["payload_signature"],
            }
            for value in objects["shards"]
        },
        "dependency_sha256": stratified._dependency_hashes(),
        "selection_salt": SELECTION_SALT,
        "model_calls_made_so_far": 0,
    }


def disclosure(objects: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    shards = objects["shards"]
    holdout = [value for value in shards if value["evaluation_set"] == "HOLDOUT"]
    sentinel = [value for value in shards if value["evaluation_set"] == "SENTINEL"]
    planned = (
        len(holdout) * len(PASSES) * len(HOLDOUT_COLD_RUNS)
        + len(sentinel) * len(PASSES) * len(SENTINEL_COLD_RUNS)
    )
    return {
        "kind": "external_model_disclosure",
        "schema_version": "function-lineage-external-disclosure.v2.6",
        "consent_required": True,
        "consent_granted": False,
        "destination": "external subscription Codex CLI (no HTTP model API)",
        "provider": "OpenAI via Codex CLI subscription transport",
        "model": MODEL_CONFIGURATION["model"],
        "reasoning_effort": MODEL_CONFIGURATION["reasoning_effort"],
        "vision": False,
        "images": [],
        "session_isolation": {
            "ephemeral": True,
            "new_cli_process": True,
            "tools_disabled": True,
        },
        "planned_requests": planned,
        "planned_request_breakdown": {
            "holdout_shards": len(holdout),
            "holdout_cold_repeats": len(HOLDOUT_COLD_RUNS),
            "sentinel_shards": len(sentinel),
            "sentinel_cold_repeats": len(SENTINEL_COLD_RUNS),
            "passes_per_repeat": len(PASSES),
        },
        "transmitted_data_classes": [
            "FunctionScope core facts derived from project Markdown/OCR text "
            "(function class, role, serviced object, zone, building, floors, "
            "systems, consumers, equipment roles, upstream/downstream text)",
            "deterministic functional candidate metadata (candidate_id, "
            "relation_type, exact LEFT/RIGHT fragment and function identifiers, "
            "capacity keys, component_map, matched evidence channels, "
            "deterministic scores and ranks)",
            "task-local evidence records (evidence_id, field name, normalized "
            "textual value, owner fragment/function id, physical page, "
            "provenance type)",
            "RIGHT physical page numbers of the compared documents",
            "synthetic research identifiers (task_id, scope_id, shard_id, "
            "payload_signature)",
        ],
        "not_transmitted": [
            "page images, crops, or any raster/vector drawing content",
            "human engineer decisions, verdicts, or comparison results",
            "customer or personal data fields",
            "credentials, tokens, or infrastructure identifiers",
            "production database rows or live comparison state",
        ],
        "prompt_characters": {
            "median": statistics.median(
                value["prompt_characters"] for value in shards
            ),
            "p95": stratified._percentile(
                [value["prompt_characters"] for value in shards], 95
            ),
            "max": max(value["prompt_characters"] for value in shards),
        },
        "total_prompt_characters_per_pass": sum(
            value["prompt_characters"] for value in shards
        ),
        "holdout_task_count": SAMPLE_SIZE,
        "sentinel_task_count": len(SENTINEL_IDS),
        "task_ids": {
            "HOLDOUT": list(objects["sample"]["selected_task_ids"]),
            "SENTINEL": sorted(str(value) for value in SENTINEL_IDS),
        },
        "model_inputs_sha256": manifest["model_inputs_sha256"],
        "holdout_population_sha256": manifest["holdout_population_sha256"],
        "holdout_sample_sha256": manifest["holdout_sample_sha256"],
        "writes_production_state": False,
        "enables_shadow": False,
        "materializes_output": False,
        "tuning_after_inference_forbidden": True,
        "sample_change_after_inference_forbidden": True,
    }


def prepare(output: Path | None = None) -> dict[str, Any]:
    """Freeze the holdout inputs and stop at the external consent gate."""
    target = Path(output or DEFAULT_OUTPUT)
    first = build_frozen_objects()
    second = build_frozen_objects()
    for name in ("population", "sample"):
        if _json_bytes(first[name]) != _json_bytes(second[name]):
            raise RuntimeError(f"holdout {name} replay was not byte-identical")
    if _jsonl_bytes(first["shards"]) != _jsonl_bytes(second["shards"]):
        raise RuntimeError("holdout shard replay was not byte-identical")

    manifest = input_manifest(first)
    gate = disclosure(first, manifest)
    target.mkdir(parents=True, exist_ok=True)
    (target / "holdout_population.json").write_bytes(_json_bytes(first["population"]))
    (target / "holdout_sample.json").write_bytes(_json_bytes(first["sample"]))
    (target / "model_inputs.jsonl").write_bytes(_jsonl_bytes(first["shards"]))
    (target / "input_manifest.json").write_bytes(_json_bytes(manifest))
    (target / "external_model_disclosure.json").write_bytes(_json_bytes(gate))
    (target / "preflight.json").write_bytes(_json_bytes(first["preflight"]))
    (target / "report.md").write_text(
        render_report(first, manifest, gate), encoding="utf-8"
    )
    return {
        "output": target,
        "objects": first,
        "manifest": manifest,
        "disclosure": gate,
    }


def render_report(
    objects: Mapping[str, Any],
    manifest: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> str:
    population = objects["population"]
    sample = objects["sample"]
    preflight = objects["preflight"]
    lines = [
        "# Function Lineage v2.6 — independent holdout sample (prepared, not run)",
        "",
        "**AWAITING EXTERNAL AI CONSENT.** No model call has been made. "
        "`model_calls = 0`.",
        "",
        "The v2.5 36-task sample is now a diagnostic set and is excluded, together "
        "with the seven v2.4.2 controls. The controls are carried separately with "
        "prompts byte-identical to the frozen v2.5 sentinel shards.",
        "",
        "## Eligible population",
        "",
        f"Scoped population `{population['population_size']}`; holdout eligible "
        f"`{population['holdout_eligible_size']}`; excluded "
        f"`{preflight['excluded_task_count']}` "
        f"(`{len(population['excluded']['v2_5_diagnostic_task_ids'])}` diagnostic + "
        f"`{len(population['excluded']['sentinel_task_ids'])}` sentinel).",
        "",
        "| Corpus | Holdout eligible | Selected |",
        "|---|---:|---:|",
    ]
    for corpus in CORPUS_ORDER:
        lines.append(
            f"| {corpus} | {population['holdout_eligible_by_corpus'].get(corpus, 0)} | "
            f"{sample['sample_size_by_corpus'].get(corpus, 0)} |"
        )
    lines.extend([
        "",
        "## Stratum coverage",
        "",
        "| Stratum | Description | Eligible | Selected | Covered |",
        "|---|---|---:|---:|---|",
    ])
    for stratum, description in STRATA.items():
        row = sample["stratum_coverage"][stratum]
        lines.append(
            f"| {stratum} | {description} | {row['eligible_population']} | "
            f"{row['selected_tasks']} | {'YES' if row['covered'] else 'NO'} |"
        )
    lines.extend([
        "",
        f"Strata with no eligible holdout task: "
        f"`{sample['uncoverable_strata'] or 'none'}`. "
        "Stratum E stays empty because every frozen FUNCTION_DISTRIBUTED candidate "
        "belongs to the excluded LEFT20 PARENT control; it is reported through the "
        "sentinel only.",
        "",
        "## Preflight",
        "",
        f"OK: `{preflight['ok']}`; failures `{preflight['failures'] or 'none'}`.",
        f"Holdout shards `{preflight['holdout_shard_count']}`; sentinel shards "
        f"`{preflight['sentinel_shard_count']}`; sentinel prompts identical to v2.5: "
        f"`{preflight['sentinel_prompts_identical_to_v2_5']}`.",
        "",
        "## External model data gate",
        "",
        f"Provider: `{gate['provider']}`; model `{gate['model']}` / "
        f"effort `{gate['reasoning_effort']}`; vision `{gate['vision']}`.",
        f"Planned requests: `{gate['planned_requests']}` "
        f"({gate['planned_request_breakdown']}).",
        f"Prompt characters median/p95/max: "
        f"`{gate['prompt_characters']['median']}` / "
        f"`{gate['prompt_characters']['p95']}` / "
        f"`{gate['prompt_characters']['max']}`.",
        "",
        "### Transmitted data classes",
        "",
    ])
    for value in gate["transmitted_data_classes"]:
        lines.append(f"* {value}")
    lines.extend(["", "### Never transmitted", ""])
    for value in gate["not_transmitted"]:
        lines.append(f"* {value}")
    lines.extend([
        "",
        "### Hashes",
        "",
        f"* `model_inputs.jsonl` — `{manifest['model_inputs_sha256']}`",
        f"* `holdout_population.json` — `{manifest['holdout_population_sha256']}`",
        f"* `holdout_sample.json` — `{manifest['holdout_sample_sha256']}`",
        "",
        "Explicit user consent is required before any request is sent.",
        "",
    ])
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Phase 6 — inference under explicit user consent
# ---------------------------------------------------------------------------

#: The exact artifacts the user consented to.  Any drift stops the run.
CONSENTED_ARTIFACTS = (
    "model_inputs.jsonl",
    "holdout_population.json",
    "holdout_sample.json",
)


def consent_state(output: Path, expected: Mapping[str, str]) -> dict[str, Any]:
    """Compare the frozen artifacts with the hashes the user consented to."""
    observed = {
        name: base_smoke._sha_file(Path(output) / name)
        for name in CONSENTED_ARTIFACTS
    }
    drifted = sorted(
        name for name in CONSENTED_ARTIFACTS
        if observed[name] != str(expected.get(name, ""))
    )
    return {
        "consented_sha256": {name: str(expected.get(name, "")) for name in CONSENTED_ARTIFACTS},
        "observed_sha256": observed,
        "drifted_artifacts": drifted,
        "ok": not drifted,
    }


def _load_prepared(output: Path) -> dict[str, Any]:
    target = Path(output)
    population = stratified._read_json(target / "holdout_population.json")
    sample = stratified._read_json(target / "holdout_sample.json")
    shards = stratified._read_jsonl(target / "model_inputs.jsonl")
    disclosure_value = stratified._read_json(target / "external_model_disclosure.json")
    if disclosure_value["model"] != MODEL_CONFIGURATION["model"]:
        raise RuntimeError("disclosed model differs from the runner configuration")
    if disclosure_value["reasoning_effort"] != MODEL_CONFIGURATION["reasoning_effort"]:
        raise RuntimeError("disclosed reasoning effort differs from the runner configuration")
    if disclosure_value["vision"] or MODEL_CONFIGURATION["vision"]:
        raise RuntimeError("vision must stay disabled")
    datasets = {
        pair_id: scoped_smoke._synthetic_dataset(sources, contexts)
        for pair_id, sources, contexts in _dataset_inputs(shards)
    }
    return {
        "population": population,
        "sample": sample,
        "shards": shards,
        "disclosure": disclosure_value,
        "datasets": datasets,
    }


def _dataset_inputs(shards: Sequence[Mapping[str, Any]]):
    sources = stratified._load_sources()
    contexts_by_pair: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for shard in shards:
        for context in shard["model_payload"]["task_contexts"]:
            contexts_by_pair[str(shard["pair_id"])][str(context["task_id"])] = context
    for pair_id in sorted(contexts_by_pair):
        yield pair_id, sources["raw"][pair_id], list(contexts_by_pair[pair_id].values())


def _capacity_licences(
    datasets: Mapping[str, Any],
) -> dict[str, Mapping[tuple[str, str], Sequence[str]]]:
    return {
        pair_id: lineage.exact_child_union_licences(dataset.candidates)
        for pair_id, dataset in datasets.items()
    }


def _apply_capacity(
    records: Sequence[dict[str, Any]],
    datasets: Mapping[str, Any],
    licences: Mapping[str, Mapping[tuple[str, str], Sequence[str]]],
) -> list[str]:
    """v2.6 accounting: one uniform pass, no task-pair skipping heuristic."""
    all_errors: list[str] = []
    for pair_id in sorted({str(value["pair_id"]) for value in records}):
        pair_records = [value for value in records if str(value["pair_id"]) == pair_id]
        if any(
            not value["model_call"]["ok"] or not value["transport_verification"]["ok"]
            for value in pair_records
        ):
            for record in pair_records:
                record["capacity_verification"] = {
                    "applicable": False,
                    "ok": None,
                    "task_results": {},
                    "errors": [],
                    "reason": "INCOMPLETE_OR_INVALID_BATCH",
                }
            continue
        decisions = {
            str(result["task_id"]): str(result["decision"])
            for record in pair_records
            for result in record["response"]["results"]
        }
        errors = lineage.verify_capacity(
            [
                {"task_id": task_id, "candidate_id": candidate_id}
                for task_id, candidate_id in sorted(decisions.items())
            ],
            datasets[pair_id].candidates,
            licences=licences[pair_id],
        )
        affected = {
            candidate_id for candidate_id in decisions.values()
            if any(candidate_id in error for error in errors)
        }
        for record in pair_records:
            task_results = {
                task_id: {
                    "ok": decisions[task_id] not in affected,
                    "candidate_id": decisions[task_id],
                    "errors": [
                        error for error in errors if decisions[task_id] in error
                    ],
                }
                for task_id in record["task_ids"]
            }
            record["capacity_verification"] = {
                "applicable": True,
                "ok": all(value["ok"] for value in task_results.values()),
                "task_results": task_results,
                "errors": sorted(set(errors)),
                "scenario": "UNIFORM_LINEAGE_OWNERSHIP_ACCOUNTING",
                "cross_granularity_scenarios_mixed": False,
            }
        all_errors.extend(errors)
    return sorted(set(all_errors))


def _model_job(
    shard: Mapping[str, Any], *, cold_run: int, pass_name: str,
    experiment_id: str, datasets: Mapping[str, Any],
) -> dict[str, Any]:
    payload = shard["model_payload"]
    prompt = _prompt(payload, pass_name)
    prompt_hash = base_smoke._sha_bytes(prompt.encode("utf-8"))
    if prompt_hash != shard[f"prompt_{pass_name.lower()}_sha256"]:
        raise RuntimeError(f"frozen prompt drift: {shard['shard_id']}/{pass_name}")
    call = ai_gateway.call(
        ai_settings.CODEX_SESSION,
        prompt,
        model=str(MODEL_CONFIGURATION["model"]),
        reasoning_level=str(MODEL_CONFIGURATION["reasoning_effort"]),
        schema=copy.deepcopy(dict(shard["output_schema"])),
        images=(),
        retries=int(MODEL_CONFIGURATION["retries"]),
        timeout_s=int(MODEL_CONFIGURATION["timeout_seconds"]),
        run_id=experiment_id,
    )
    response = call.parsed if call.ok else None
    parsed = scoped_transport.verify_scoped_transport_response(payload, response)
    verifier: dict[str, Any] = {
        "applicable": False,
        "ok": None,
        "global_errors": ["NOT_REACHED"],
        "task_results": {},
    }
    if parsed["ok"]:
        verifier = scoped_smoke._task_local_verifier(
            datasets[str(shard["pair_id"])],
            str(payload["payload_signature"]),
            response["results"],
        )
    usage = dict(call.usage or {})
    return {
        "evaluation_set": shard["evaluation_set"],
        "cold_run": cold_run,
        "pass_name": pass_name,
        "pair_id": shard["pair_id"],
        "corpus": shard["corpus"],
        "shard_id": shard["shard_id"],
        "task_ids": list(shard["task_ids"]),
        "scope_ids": list(shard["scope_ids"]),
        "prompt_sha256": prompt_hash,
        "prompt_characters": shard["prompt_characters"],
        "payload_signature": payload["payload_signature"],
        "output_schema_sha256": _sha_json(shard["output_schema"]),
        "session_isolation": {
            "new_cli_process": True,
            "ephemeral": True,
            "tools_disabled": True,
            "vision_used": False,
        },
        "model_call": {
            "ok": bool(call.ok),
            "model": call.model,
            "reasoning_effort": call.reasoning_level,
            "duration_ms": int(call.duration_ms),
            "usage": usage,
            "tokens": base_smoke._usage_total(usage),
            "error": call.error,
            "error_kind": call.error_kind,
            "failure_kind": base_smoke._classify_request_failure(
                ok=bool(call.ok), error=call.error,
                raw_excerpt=call.raw_excerpt, error_kind=call.error_kind,
            ),
            "attempts": int(call.attempts),
            "exit_code": call.exit_code,
            "provider_session_id": call.session_id,
            "raw_excerpt": call.raw_excerpt,
        },
        "response": response,
        "transport_verification": parsed,
        "existing_verifier": verifier,
        "capacity_verification": None,
    }


def experiment(
    output: Path | None = None, *,
    consent_granted: bool,
    consented_sha256: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Run the consented 110 requests.  Refuses to repeat or to append."""
    target = Path(output or DEFAULT_OUTPUT)
    records_path = target / "model_runs.jsonl"
    if records_path.exists():
        raise RuntimeError(
            f"refusing to repeat or append model observations: {records_path}"
        )
    if not consent_granted:
        raise RuntimeError("external model consent was not granted")
    state = consent_state(target, consented_sha256)
    if not state["ok"]:
        raise RuntimeError(
            "consented artifacts changed; a new consent is required: "
            f"{state['drifted_artifacts']}"
        )
    prepared = _load_prepared(target)
    shards = prepared["shards"]
    datasets = prepared["datasets"]
    licences = _capacity_licences(datasets)
    planned = int(prepared["disclosure"]["planned_requests"])

    experiment_id = "flv2.6-holdout-" + uuid.uuid4().hex
    records: list[dict[str, Any]] = []
    started = time.monotonic()
    stopped = False
    stop_reason = None
    for evaluation_set, cold_runs in (
        ("HOLDOUT", HOLDOUT_COLD_RUNS), ("SENTINEL", SENTINEL_COLD_RUNS),
    ):
        selected = [
            value for value in shards
            if str(value["evaluation_set"]) == evaluation_set
        ]
        for cold_run in cold_runs:
            if stopped:
                break
            for pass_name in PASSES:
                batch: list[dict[str, Any]] = []
                with ThreadPoolExecutor(
                    max_workers=int(MODEL_CONFIGURATION["workers"])
                ) as pool:
                    futures = [
                        pool.submit(
                            _model_job, shard, cold_run=cold_run,
                            pass_name=pass_name, experiment_id=experiment_id,
                            datasets=datasets,
                        )
                        for shard in selected
                    ]
                    try:
                        for future in as_completed(futures):
                            batch.append(future.result())
                    except Exception:
                        ai_gateway.kill_live_processes(experiment_id)
                        raise
                batch.sort(key=lambda value: (value["corpus"], value["shard_id"]))
                capacity_errors = _apply_capacity(batch, datasets, licences)
                records.extend(batch)
                records.sort(key=lambda value: (
                    value["evaluation_set"], int(value["cold_run"]),
                    value["pass_name"], value["corpus"], value["shard_id"],
                ))
                stratified._write_jsonl(records_path, records)
                print(
                    f"{len(records)}/{planned} set={evaluation_set} "
                    f"cold={cold_run} pass={pass_name} "
                    f"model_ok={sum(value['model_call']['ok'] for value in batch)}"
                    f"/{len(batch)} capacity_errors={len(capacity_errors)}",
                    flush=True,
                )
                if any(
                    not value["model_call"]["ok"]
                    or not value["transport_verification"]["ok"]
                    for value in batch
                ):
                    stopped = True
                    stop_reason = "TECHNICAL_PROVIDER_OR_RESPONSE_CONTRACT_FAILURE"
                    break
                drift = consent_state(target, consented_sha256)
                if not drift["ok"]:
                    stopped = True
                    stop_reason = "CONSENTED_INPUT_CHANGED_AFTER_INFERENCE_BEGAN"
                    break
    telemetry = {
        "experiment_id": experiment_id,
        "planned_requests": planned,
        "request_records": len(records),
        **base_smoke._request_counters(records),
        "stopped_early": stopped,
        "stop_reason": stop_reason,
        "wall_time_ms": int((time.monotonic() - started) * 1000),
        "model_runtime_ms": sum(
            value["model_call"]["duration_ms"] for value in records
        ),
        "consent": state,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    }
    stratified._write_json(target / "run_telemetry.json", telemetry)
    return records



def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = prepare(args.output)
    print(json.dumps({
        "output": str(result["output"]),
        "planned_requests": result["disclosure"]["planned_requests"],
        "model_calls": 0,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
