"""Function Lineage v2.7 — independent tiered acceptance holdout.

Phase D of the v2.6/2.7 master task.  The module prepares, but never runs, an
AI evaluation.

Everything that could bias the result is fixed *before* any model call:

* the eligible population excludes the seven v2.4.2 controls, the v2.5
  diagnostic 36 and the v2.6 holdout 36 — all already seen;
* tier membership is a deterministic property of a task's own candidate
  inventory, so it cannot be revised after an output is seen;
* both AUTO sets are taken **whole**, so there is no sampling freedom in the
  sets that decide GO / NO-GO;
* the acceptance thresholds are written down here, before inference.

A relation class earns automatic publication only through its own set.
Proving ``CONTINUED_1_TO_1`` grants nothing to ``MERGED_N_TO_1`` and neither
grants anything to split, distributed, non-decomposable or mixed relations.
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
from backend.app.services.stage_comparison.ai import gateway as ai_gateway
from experiments.function_lineage_v2 import holdout
from experiments.function_lineage_v2 import scoped_transport
from experiments.function_lineage_v2 import smoke as base_smoke
from experiments.function_lineage_v2 import stratified


DEFAULT_OUTPUT = (
    stratified.COMPARISON_ROOT / "20260904_function_lineage_v2_7_tiered_acceptance"
)
SCHEMA_VERSION = "function-lineage-tiered-acceptance.v2.7"
PAYLOAD_SCHEMA_VERSION = "function-lineage-tiered-acceptance-scoped.v2.7"
SHARD_PREFIX = "fs27_"
SELECTION_SALT = "function-lineage-v2.7-tiered-acceptance-v1"

PASSES = stratified.PASSES
COLD_RUNS = (1, 2, 3)
SENTINEL_COLD_RUNS = (1,)
CORPUS_ORDER = stratified.CORPUS_ORDER
PAIR_PROJECTS = stratified.PAIR_PROJECTS
PROJECT_PAIRS = stratified.PROJECT_PAIRS
STRATA = stratified.STRATA
SENTINELS = stratified.SENTINELS
SENTINEL_IDS = stratified.SENTINEL_IDS

MODEL_CONFIGURATION = copy.deepcopy(holdout.MODEL_CONFIGURATION)

#: Tier definition.  A task belongs to an AUTO set only when *every* candidate
#: it may select carries a relation of that family, so whatever the model
#: chooses — including NEED_MORE_EVIDENCE — the outcome stays inside the
#: family the set is testing.  Tier membership therefore cannot move after an
#: output is seen.
TIERS = {
    "AUTO_ONE_TO_ONE": {
        "relations": frozenset({"CONTINUED_1_TO_1"}),
        "requires": frozenset({"CONTINUED_1_TO_1"}),
        "decides_go": True,
        "description": (
            "every selectable candidate is CONTINUED_1_TO_1; the set decides "
            "whether 1:1 becomes auto-eligible"
        ),
    },
    "AUTO_MERGED": {
        "relations": frozenset({"CONTINUED_1_TO_1", "MERGED_N_TO_1"}),
        "requires": frozenset({"MERGED_N_TO_1"}),
        "decides_go": True,
        "description": (
            "inventory is confined to 1:1 and N:1 and offers at least one N:1; "
            "the set decides whether N:1 becomes auto-eligible"
        ),
    },
    "HARD_DIAGNOSTIC": {
        "relations": None,
        "requires": None,
        "decides_go": False,
        "description": (
            "split, distributed, non-decomposable, mixed and everything else; "
            "analysed separately and never an input to the product decision"
        ),
    },
}

#: Sampling policy per tier.  ``None`` means the whole tier is taken, which
#: removes every degree of freedom from the sets that decide GO / NO-GO.
TIER_SAMPLE_SIZE = {
    "AUTO_ONE_TO_ONE": None,
    "AUTO_MERGED": None,
    "HARD_DIAGNOSTIC": 12,
}

#: Acceptance gates, fixed before inference.  They are the project's existing
#: verdict-A thresholds; they are never relaxed to fit an observation.
ACCEPTANCE_GATES = {
    "stable_3_of_3_min": stratified.VERDICT_THRESHOLDS["a_overall_stable_3_of_3_min"],
    "cross_cold_exact_consistency_min":
        stratified.VERDICT_THRESHOLDS["a_cross_cold_exact_consistency_min"],
    "unsupported_accepted_max": 0,
    "false_capacity_conflicts_max": 0,
    "right_map_conflict_max": 0,
    "technical_failures_max": 0,
    "sentinel_regression_max": 0,
    "batch_permutation_changes_max": 0,
    "accepted_match_requires": [
        "PASS_A_EQUALS_PASS_B",
        "PARSER_PASS",
        "VERIFIER_PASS",
        "CAPACITY_PASS",
    ],
    "majority_override": False,
    "threshold_source": "pre-existing v2.5 verdict-A thresholds",
}

#: Already-seen tasks that may never enter an acceptance set again.
CONSUMED_EVALUATIONS = (
    ("V2_5_DIAGNOSTIC", stratified.DEFAULT_OUTPUT / "stratified_sample.json"),
    ("V2_6_HOLDOUT", holdout.DEFAULT_OUTPUT / "holdout_sample.json"),
)


def _json_bytes(value: Any) -> bytes:
    return stratified._json_bytes(value)


def _jsonl_bytes(values: Sequence[Mapping[str, Any]]) -> bytes:
    return stratified._jsonl_bytes(values)


def _selection_hash(task_id: str) -> str:
    return hashlib.sha256(
        f"{SELECTION_SALT}|{task_id}".encode("utf-8")
    ).hexdigest()


def consumed_task_ids() -> dict[str, Any]:
    consumed: dict[str, list[str]] = {}
    hashes: dict[str, str] = {}
    for label, path in CONSUMED_EVALUATIONS:
        sample = stratified._read_json(path)
        consumed[label] = sorted(str(value) for value in sample["selected_task_ids"])
        hashes[label] = base_smoke._sha_file(path)
    consumed["V2_4_2_SENTINEL"] = sorted(str(value) for value in SENTINEL_IDS)
    return {"task_ids": consumed, "sample_sha256": hashes}


def assign_tier(row: Mapping[str, Any]) -> str:
    """Deterministic tier of one task, from its own candidate inventory."""
    relations = {str(value) for value in row.get("relation_types") or []}
    if not relations:
        return "HARD_DIAGNOSTIC"
    for name in ("AUTO_ONE_TO_ONE", "AUTO_MERGED"):
        definition = TIERS[name]
        if relations <= definition["relations"] and (
            relations & definition["requires"]
        ):
            return name
    return "HARD_DIAGNOSTIC"


def build_population() -> dict[str, Any]:
    sources = stratified._load_sources()
    population = stratified.build_population(sources)
    consumed = consumed_task_ids()
    excluded = {
        task_id for values in consumed["task_ids"].values() for task_id in values
    }
    rows = []
    for row in population["tasks"]:
        task_id = str(row["task_id"])
        value = dict(row)
        value["acceptance_selection_hash"] = _selection_hash(task_id)
        value["acceptance_eligible"] = task_id not in excluded
        value["exclusion_reason"] = next(
            (
                label for label, values in consumed["task_ids"].items()
                if task_id in set(values)
            ),
            None,
        )
        value["tier"] = assign_tier(row)
        rows.append(value)
    rows.sort(key=lambda value: str(value["task_id"]))
    eligible = [value for value in rows if value["acceptance_eligible"]]
    return {
        "kind": "function_lineage_v2_7_acceptance_population",
        "schema_version": "function-lineage-acceptance-population.v2.7",
        "population_size": len(rows),
        "acceptance_eligible_size": len(eligible),
        "acceptance_eligible_by_corpus": dict(sorted(
            Counter(value["corpus"] for value in eligible).items()
        )),
        "excluded": consumed,
        "tier_definition": {
            name: {
                "relations": sorted(value["relations"]) if value["relations"] else None,
                "requires": sorted(value["requires"]) if value["requires"] else None,
                "decides_go": value["decides_go"],
                "description": value["description"],
                "sample_size": TIER_SAMPLE_SIZE[name],
            }
            for name, value in TIERS.items()
        },
        "tier_assignment_rule": (
            "a task's tier follows only from the relation types of its own "
            "selectable candidates and is fixed before any model call"
        ),
        "tier_sizes": dict(sorted(
            Counter(value["tier"] for value in eligible).items()
        )),
        "tier_sizes_by_corpus": {
            tier: dict(sorted(Counter(
                value["corpus"] for value in eligible if value["tier"] == tier
            ).items()))
            for tier in TIERS
        },
        "acceptance_gates": ACCEPTANCE_GATES,
        "strata": STRATA,
        "selection_salt": SELECTION_SALT,
        "selection_prohibitions": {
            "filename_used": False,
            "page_number_used_as_order_or_hash": False,
            "manual_task_choice": False,
            "previously_evaluated_tasks_reused": False,
            "tier_revised_after_model_output": False,
            "auto_sets_sampled": False,
        },
        "tasks": rows,
        "_sources": sources,
    }


def select_sample(population: Mapping[str, Any]) -> dict[str, Any]:
    rows = [value for value in population["tasks"] if value["acceptance_eligible"]]
    selected: dict[str, list[Mapping[str, Any]]] = {}
    trace: list[dict[str, Any]] = []
    for tier in TIERS:
        pool = sorted(
            (value for value in rows if value["tier"] == tier),
            key=lambda value: value["acceptance_selection_hash"],
        )
        quota = TIER_SAMPLE_SIZE[tier]
        if quota is None:
            chosen = pool
            reason = "WHOLE_TIER_TAKEN_NO_SAMPLING_FREEDOM"
        else:
            chosen = _cover(pool, quota, trace, tier)
            reason = "GREEDY_RARE_MULTI_LABEL_COVERAGE"
        selected[tier] = chosen
        for value in chosen:
            trace.append({
                "tier": tier,
                "task_id": str(value["task_id"]),
                "phase": reason,
            })
    flat = [value for tier in TIERS for value in selected[tier]]
    if len({str(value["task_id"]) for value in flat}) != len(flat):
        raise RuntimeError("acceptance sample contains a duplicate task")
    excluded = {
        task_id for values in population["excluded"]["task_ids"].values()
        for task_id in values
    }
    if excluded & {str(value["task_id"]) for value in flat}:
        raise RuntimeError("acceptance sample reused an already-evaluated task")
    return {
        "kind": "function_lineage_v2_7_acceptance_sample",
        "schema_version": "function-lineage-acceptance-sample.v2.7",
        "selection_algorithm": {
            "auto_sets": "whole tier, no sampling",
            "hard_set": (
                "greedy rare multi-label coverage with salted SHA-256 tie-break"
            ),
            "salt": SELECTION_SALT,
            "page_or_filename_selection": False,
            "manual_selection": False,
        },
        "acceptance_gates": ACCEPTANCE_GATES,
        "tier_sizes": {tier: len(selected[tier]) for tier in TIERS},
        "go_deciding_tiers": [
            tier for tier, value in TIERS.items() if value["decides_go"]
        ],
        "sample_size": len(flat),
        "sample_size_by_corpus": dict(sorted(
            Counter(value["corpus"] for value in flat).items()
        )),
        "selected_task_ids": [str(value["task_id"]) for value in flat],
        "selected_task_ids_by_tier": {
            tier: [str(value["task_id"]) for value in selected[tier]]
            for tier in TIERS
        },
        "selected_tasks": [{
            "task_id": str(value["task_id"]),
            "tier": value["tier"],
            "scope_id": value["scope_id"],
            "corpus": value["corpus"],
            "scope_kind": value["scope_kind"],
            "strata": value["strata"],
            "relation_types": value["relation_types"],
            "candidate_count": value["candidate_count"],
            "selection_hash": value["acceptance_selection_hash"],
            "frozen_context_sha256": value["frozen_context_sha256"],
        } for value in flat],
        "stratum_coverage": {
            stratum: {
                "eligible_population": sum(
                    stratum in value["strata"] for value in rows
                ),
                "selected_tasks": sum(stratum in value["strata"] for value in flat),
            }
            for stratum in STRATA
        },
        "selection_trace": trace,
        "sentinels_reported_separately": [
            {"label": label, **value} for label, value in SENTINELS.items()
        ],
    }


def _cover(
    pool: Sequence[Mapping[str, Any]], quota: int,
    trace: list[dict[str, Any]], tier: str,
) -> list[Mapping[str, Any]]:
    if len(pool) <= quota:
        return list(pool)
    availability = {
        stratum: sum(stratum in value["strata"] for value in pool)
        for stratum in STRATA
    }
    chosen: list[Mapping[str, Any]] = []
    covered: set[str] = set()
    remaining = list(pool)
    while len(chosen) < quota:
        def key(row: Mapping[str, Any]) -> tuple[Any, ...]:
            new = set(row["strata"]) - covered
            gain = sum(1 / availability[value] for value in new if availability[value])
            return (
                -len(new), -gain, -len(row["strata"]),
                row["acceptance_selection_hash"],
            )

        pick = min(remaining, key=key)
        remaining.remove(pick)
        chosen.append(pick)
        covered.update(pick["strata"])
    return sorted(chosen, key=lambda value: value["acceptance_selection_hash"])


def build_frozen_objects() -> dict[str, Any]:
    population = build_population()
    sources = population.pop("_sources")
    sample = select_sample(population)
    row_by_id = {str(value["task_id"]): value for value in population["tasks"]}
    shards: list[dict[str, Any]] = []
    for tier in TIERS:
        for corpus in CORPUS_ORDER:
            pair_id = PROJECT_PAIRS[corpus]
            ordered = [
                task_id for task_id in sample["selected_task_ids_by_tier"][tier]
                if row_by_id[task_id]["corpus"] == corpus
            ]
            if not ordered:
                continue
            shards.extend(holdout._build_shards(
                pair_id=pair_id,
                eval_set=tier,
                input_signature=str(sources["raw"][pair_id]["input_signature"]),
                contexts=[
                    stratified._smoke_context(sources["contexts"][task_id])
                    for task_id in ordered
                ],
                schema_version=PAYLOAD_SCHEMA_VERSION,
                shard_prefix=SHARD_PREFIX,
            ))
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
        shards.extend(holdout._build_shards(
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
        raise RuntimeError(f"acceptance preflight failed: {preflight['failures']}")
    return {
        "population": population,
        "sample": sample,
        "shards": shards,
        "preflight": preflight,
    }


def _preflight(
    population: Mapping[str, Any], sample: Mapping[str, Any],
    shards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    failures: list[str] = []
    by_set: dict[str, list[str]] = {}
    for shard in shards:
        by_set.setdefault(str(shard["evaluation_set"]), []).extend(shard["task_ids"])
    for tier in TIERS:
        expected = sorted(sample["selected_task_ids_by_tier"][tier])
        if sorted(by_set.get(tier, [])) != expected:
            failures.append(f"TIER_SHARDS_DO_NOT_COVER_THE_SET:{tier}")
    frozen = holdout._frozen_sentinel_shards()
    sentinel = [value for value in shards if value["evaluation_set"] == "SENTINEL"]
    identical = all(
        (frozen.get(str(value["shard_id"])) or {}).get("prompt_a_sha256")
        == value["prompt_a_sha256"]
        and (frozen.get(str(value["shard_id"])) or {}).get("prompt_b_sha256")
        == value["prompt_b_sha256"]
        for value in sentinel
    ) and bool(sentinel)
    if not identical:
        failures.append("SENTINEL_PROMPT_DRIFTED")
    excluded = {
        task_id for values in population["excluded"]["task_ids"].values()
        for task_id in values
    }
    if excluded & {
        task_id for tier in TIERS for task_id in by_set.get(tier, [])
    }:
        failures.append("SHARD_CARRIES_AN_ALREADY_EVALUATED_TASK")
    if any(value["provider_safe_schema_problems"] for value in shards):
        failures.append("PROVIDER_UNSAFE_SCHEMA")
    if any(
        value["prompt_characters"] > scoped_transport.HARD_CHARACTERS
        for value in shards
    ):
        failures.append("PROMPT_EXCEEDS_HARD_GATE")
    for tier, definition in TIERS.items():
        if definition["relations"] is None:
            continue
        for task_id in sample["selected_task_ids_by_tier"][tier]:
            row = next(
                value for value in population["tasks"]
                if str(value["task_id"]) == task_id
            )
            if not set(row["relation_types"]) <= definition["relations"]:
                failures.append(f"TIER_MEMBER_ESCAPES_ITS_RELATION_FAMILY:{task_id}")
    return {
        "ok": not failures,
        "failures": sorted(set(failures)),
        "shard_count_by_set": {
            name: sum(1 for value in shards if value["evaluation_set"] == name)
            for name in (*TIERS, "SENTINEL")
        },
        "task_count_by_set": {name: len(value) for name, value in sorted(by_set.items())},
        "sentinel_prompts_identical_to_v2_5": identical,
        "model_calls": 0,
    }


def input_manifest(objects: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "kind": "function_lineage_acceptance_input_manifest",
        "schema_version": SCHEMA_VERSION,
        "production_head": stratified.PRODUCTION_HEAD,
        "production_release": stratified.PRODUCTION_RELEASE,
        "frozen_research_sources": stratified.SOURCE_HASHES,
        "consumed_evaluation_sha256": objects["population"]["excluded"]["sample_sha256"],
        "acceptance_population_sha256": hashlib.sha256(
            _json_bytes(objects["population"])
        ).hexdigest(),
        "acceptance_sample_sha256": hashlib.sha256(
            _json_bytes(objects["sample"])
        ).hexdigest(),
        "model_inputs_sha256": hashlib.sha256(
            _jsonl_bytes(objects["shards"])
        ).hexdigest(),
        "acceptance_gates": ACCEPTANCE_GATES,
        "selection_salt": SELECTION_SALT,
        "dependency_sha256": stratified._dependency_hashes(),
        "model_calls_made_so_far": 0,
    }


def disclosure(objects: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    shards = objects["shards"]
    tiered = [value for value in shards if value["evaluation_set"] in TIERS]
    sentinel = [value for value in shards if value["evaluation_set"] == "SENTINEL"]
    planned = (
        len(tiered) * len(PASSES) * len(COLD_RUNS)
        + len(sentinel) * len(PASSES) * len(SENTINEL_COLD_RUNS)
    )
    return {
        "kind": "external_model_disclosure",
        "schema_version": "function-lineage-external-disclosure.v2.7",
        "consent_required": True,
        "consent_granted": False,
        "destination": "external subscription Codex CLI (no HTTP model API)",
        "provider": "OpenAI via Codex CLI subscription transport",
        "model": MODEL_CONFIGURATION["model"],
        "reasoning_effort": MODEL_CONFIGURATION["reasoning_effort"],
        "vision": False,
        "images": [],
        "session_isolation": {
            "ephemeral": True, "new_cli_process": True, "tools_disabled": True,
        },
        "planned_requests": planned,
        "planned_request_breakdown": {
            "tiered_shards": len(tiered),
            "tiered_cold_repeats": len(COLD_RUNS),
            "sentinel_shards": len(sentinel),
            "sentinel_cold_repeats": len(SENTINEL_COLD_RUNS),
            "passes_per_repeat": len(PASSES),
            "shards_by_set": {
                name: sum(1 for value in shards if value["evaluation_set"] == name)
                for name in (*TIERS, "SENTINEL")
            },
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
        "task_count_by_tier": objects["sample"]["tier_sizes"],
        "go_deciding_tiers": objects["sample"]["go_deciding_tiers"],
        "sentinel_task_count": len(SENTINEL_IDS),
        "acceptance_gates": ACCEPTANCE_GATES,
        "model_inputs_sha256": manifest["model_inputs_sha256"],
        "acceptance_population_sha256": manifest["acceptance_population_sha256"],
        "acceptance_sample_sha256": manifest["acceptance_sample_sha256"],
        "writes_production_state": False,
        "enables_shadow": False,
        "materializes_output": False,
        "tuning_after_inference_forbidden": True,
        "tier_change_after_inference_forbidden": True,
    }


def prepare(output: Path | None = None) -> dict[str, Any]:
    target = Path(output or DEFAULT_OUTPUT)
    first = build_frozen_objects()
    second = build_frozen_objects()
    for name in ("population", "sample"):
        if _json_bytes(first[name]) != _json_bytes(second[name]):
            raise RuntimeError(f"acceptance {name} replay was not byte-identical")
    if _jsonl_bytes(first["shards"]) != _jsonl_bytes(second["shards"]):
        raise RuntimeError("acceptance shard replay was not byte-identical")
    manifest = input_manifest(first)
    gate = disclosure(first, manifest)
    target.mkdir(parents=True, exist_ok=True)
    (target / "acceptance_population.json").write_bytes(_json_bytes(first["population"]))
    (target / "acceptance_sample.json").write_bytes(_json_bytes(first["sample"]))
    (target / "model_inputs.jsonl").write_bytes(_jsonl_bytes(first["shards"]))
    (target / "input_manifest.json").write_bytes(_json_bytes(manifest))
    (target / "external_model_disclosure.json").write_bytes(_json_bytes(gate))
    (target / "preflight.json").write_bytes(_json_bytes(first["preflight"]))
    (target / "report.md").write_text(
        render_report(first, manifest, gate), encoding="utf-8"
    )
    return {"output": target, "objects": first, "manifest": manifest, "disclosure": gate}


def render_report(
    objects: Mapping[str, Any], manifest: Mapping[str, Any], gate: Mapping[str, Any],
) -> str:
    population = objects["population"]
    sample = objects["sample"]
    preflight = objects["preflight"]
    lines = [
        "# Function Lineage v2.7 — tiered acceptance holdout (prepared, not run)",
        "",
        "**AWAITING EXTERNAL AI CONSENT.** No model call has been made.",
        "",
        "Excluded as already seen: the seven v2.4.2 controls, the v2.5 diagnostic "
        "36 and the v2.6 holdout 36.",
        "",
        "## Tiers",
        "",
        "| Tier | Decides GO | Eligible | Selected | Sampling |",
        "|---|---|---:|---:|---|",
    ]
    for tier, definition in TIERS.items():
        lines.append(
            f"| `{tier}` | {'YES' if definition['decides_go'] else 'no'} | "
            f"{population['tier_sizes'].get(tier, 0)} | "
            f"{sample['tier_sizes'][tier]} | "
            f"{'whole tier' if TIER_SAMPLE_SIZE[tier] is None else 'sampled'} |"
        )
    lines.extend([
        "",
        "Tier follows only from the relation types a task's own candidates carry, "
        "so membership is fixed before inference and cannot be revised after an "
        "output is seen. Both GO-deciding tiers are taken whole: there is no "
        "sampling freedom in the sets that decide the product question.",
        "",
        "## Acceptance gates (fixed before inference)",
        "",
        "| Gate | Value |",
        "|---|---|",
    ])
    for name, value in ACCEPTANCE_GATES.items():
        lines.append(f"| `{name}` | `{value}` |")
    lines.extend([
        "",
        "## Preflight",
        "",
        f"OK `{preflight['ok']}`; failures `{preflight['failures'] or 'none'}`; "
        f"sentinel prompts identical to v2.5 "
        f"`{preflight['sentinel_prompts_identical_to_v2_5']}`.",
        "",
        f"Shards by set: `{preflight['shard_count_by_set']}`; "
        f"tasks by set: `{preflight['task_count_by_set']}`.",
        "",
        "## External model data gate",
        "",
        f"Provider `{gate['provider']}`; model `{gate['model']}` / effort "
        f"`{gate['reasoning_effort']}`; vision `{gate['vision']}`.",
        f"Planned requests **`{gate['planned_requests']}`** "
        f"({gate['planned_request_breakdown']}).",
        f"Prompt characters median/p95/max `{gate['prompt_characters']['median']}` / "
        f"`{gate['prompt_characters']['p95']}` / `{gate['prompt_characters']['max']}`.",
        "",
        "### Transmitted data classes",
        "",
        *(f"* {value}" for value in gate["transmitted_data_classes"]),
        "",
        "### Never transmitted",
        "",
        *(f"* {value}" for value in gate["not_transmitted"]),
        "",
        "### Hashes",
        "",
        f"* `model_inputs.jsonl` — `{manifest['model_inputs_sha256']}`",
        f"* `acceptance_population.json` — "
        f"`{manifest['acceptance_population_sha256']}`",
        f"* `acceptance_sample.json` — `{manifest['acceptance_sample_sha256']}`",
        "",
        "Explicit user consent is required before any request is sent.",
        "",
    ])
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Phase F — inference under explicit user consent
# ---------------------------------------------------------------------------

CONSENTED_ARTIFACTS = (
    "model_inputs.jsonl",
    "acceptance_population.json",
    "acceptance_sample.json",
)

#: How capacity is accounted, fixed BEFORE the first call.
#:
#: The primary view resolves capacity separately per tier, because the consent
#: requires the AUTO and HARD sets to be evaluated separately and the HARD set
#: is a sample of its tier, not the whole of it.  The secondary view resolves
#: capacity across every selected task of a pair, which is what production
#: would do, and is reported as a stricter cross-check.  Both are computed; the
#: pre-registered gate reads the primary one.
CAPACITY_VIEWS = {
    "PRIMARY_PER_TIER": {
        "grouping": ["pair_id", "evaluation_set", "cold_run"],
        "decides_gate": True,
        "reason": "the consent requires AUTO and HARD sets to be judged apart",
    },
    "SECONDARY_CROSS_TIER": {
        "grouping": ["pair_id", "cold_run"],
        "decides_gate": False,
        "reason": (
            "production resolves capacity over every stable claim of a run, "
            "including review-tier ones; reported as a conservative check"
        ),
    },
}


def consent_state(output: Path, expected: Mapping[str, str]) -> dict[str, Any]:
    observed = {
        name: base_smoke._sha_file(Path(output) / name)
        for name in CONSENTED_ARTIFACTS
    }
    drifted = sorted(
        name for name in CONSENTED_ARTIFACTS
        if observed[name] != str(expected.get(name, ""))
    )
    return {
        "consented_sha256": {
            name: str(expected.get(name, "")) for name in CONSENTED_ARTIFACTS
        },
        "observed_sha256": observed,
        "drifted_artifacts": drifted,
        "ok": not drifted,
    }


def _load_prepared(output: Path) -> dict[str, Any]:
    target = Path(output)
    shards = stratified._read_jsonl(target / "model_inputs.jsonl")
    gate = stratified._read_json(target / "external_model_disclosure.json")
    if gate["model"] != MODEL_CONFIGURATION["model"]:
        raise RuntimeError("disclosed model differs from the runner configuration")
    if gate["reasoning_effort"] != MODEL_CONFIGURATION["reasoning_effort"]:
        raise RuntimeError("disclosed effort differs from the runner configuration")
    if gate["vision"] or MODEL_CONFIGURATION["vision"]:
        raise RuntimeError("vision must stay disabled")
    if gate["acceptance_gates"] != ACCEPTANCE_GATES:
        raise RuntimeError("acceptance gates drifted from the frozen disclosure")
    sources = stratified._load_sources()
    contexts: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for shard in shards:
        for context in shard["model_payload"]["task_contexts"]:
            contexts[str(shard["pair_id"])][str(context["task_id"])] = context
    datasets = {
        pair_id: holdout.scoped_smoke._synthetic_dataset(
            sources["raw"][pair_id], list(values.values())
        )
        for pair_id, values in contexts.items()
    }
    return {
        "population": stratified._read_json(target / "acceptance_population.json"),
        "sample": stratified._read_json(target / "acceptance_sample.json"),
        "shards": shards,
        "disclosure": gate,
        "datasets": datasets,
    }


def experiment(
    output: Path | None = None, *,
    consent_granted: bool,
    consented_sha256: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Run the consented requests.  Refuses to repeat, append or drift.

    Capacity is deliberately absent from every record: it is a post-consensus
    global stage and is computed once, later, from the stable claims.
    """
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
    planned = int(prepared["disclosure"]["planned_requests"])

    experiment_id = "flv2.7-acceptance-" + uuid.uuid4().hex
    records: list[dict[str, Any]] = []
    started = time.monotonic()
    stopped = False
    stop_reason = None
    phases = [(tier, COLD_RUNS) for tier in TIERS]
    phases.append(("SENTINEL", SENTINEL_COLD_RUNS))
    for evaluation_set, cold_runs in phases:
        selected = [
            value for value in shards
            if str(value["evaluation_set"]) == evaluation_set
        ]
        if not selected:
            continue
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
                            holdout._model_job, shard, cold_run=cold_run,
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
                for record in batch:
                    # Capacity is a post-consensus global stage; a per-batch
                    # verdict would reintroduce the batch dependency removed in
                    # Phase A.
                    record["capacity_verification"] = {
                        "applicable": False,
                        "ok": None,
                        "task_results": {},
                        "errors": [],
                        "reason": "DEFERRED_TO_POST_CONSENSUS_GLOBAL_RESOLUTION",
                    }
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
                    f"/{len(batch)}",
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
        "experiment_valid": not stopped,
        "wall_time_ms": int((time.monotonic() - started) * 1000),
        "model_runtime_ms": sum(
            value["model_call"]["duration_ms"] for value in records
        ),
        "consent": state,
        "capacity_views": CAPACITY_VIEWS,
        "capacity_stage": "POST_CONSENSUS_GLOBAL",
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
