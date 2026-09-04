"""Function Lineage v2.7 — tiered acceptance metrics and GO / NO-GO gates.

Phase F reporting.  Written before the run's results were read; it only reads
observations and never changes a decision.

Outcome of one task in one cold run is derived in the v2.6 order:

1. Pass A / Pass B consensus, task-local, capacity absent;
2. one global capacity resolution over the stable claims of that run;
3. a claim is published only if every key it consumes is uncontested.

A task is reproducible when all three cold runs end in the *same* outcome.
``NEED_MORE_EVIDENCE`` is a legitimate identical outcome — it is a safe
unresolved result, never an automatic match, and it is counted separately so a
tier cannot look good by refusing to answer.
"""
from __future__ import annotations

import itertools
import json
import random
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import acceptance
from experiments.function_lineage_v2 import capacity_forensics as forensics
from experiments.function_lineage_v2 import holdout_metrics
from experiments.function_lineage_v2 import stratified


SCHEMA_VERSION = "function-lineage-tiered-acceptance-metrics.v2.7"
DEFAULT_OUTPUT = acceptance.DEFAULT_OUTPUT
COLD_RUNS = acceptance.COLD_RUNS
PASSES = acceptance.PASSES
SENTINEL_REFERENCES = holdout_metrics.SENTINEL_REFERENCES

#: Outcome kinds a task may reach in one cold run.
OUTCOME_PUBLISHED = "PUBLISHED_MATCH"
OUTCOME_NME = "NEED_MORE_EVIDENCE"
OUTCOME_CONTESTED = "CAPACITY_CONTESTED"
OUTCOME_DISAGREEMENT = "PASS_DISAGREEMENT"
OUTCOME_VERIFIER = "VERIFIER_REJECTION"
OUTCOME_PARSER = "RESPONSE_PARSER_REJECTION"
OUTCOME_FAILURE = "REQUEST_FAILURE"
OUTCOME_MISSING = "REQUEST_NOT_OBSERVED"

SAFE_UNRESOLVED_OUTCOMES = frozenset({
    OUTCOME_NME, OUTCOME_CONTESTED, OUTCOME_DISAGREEMENT, OUTCOME_VERIFIER,
})


def _json_bytes(value: Any) -> bytes:
    return stratified._json_bytes(value)


def load(output: Path | None = None) -> dict[str, Any]:
    target = Path(output or DEFAULT_OUTPUT)
    return {
        "output": target,
        "population": stratified._read_json(target / "acceptance_population.json"),
        "sample": stratified._read_json(target / "acceptance_sample.json"),
        "records": stratified._read_jsonl(target / "model_runs.jsonl"),
        "telemetry": stratified._read_json(target / "run_telemetry.json"),
        "disclosure": stratified._read_json(target / "external_model_disclosure.json"),
    }


def _pair_candidates() -> dict[str, dict[str, Any]]:
    return {
        pair_id: {
            str(value["candidate_id"]): value
            for value in stratified._read_json(
                stratified.CANDIDATE_ROOT / f"{pair_id}.json"
            )["functional_candidates"]
        }
        for pair_id in stratified.PAIR_PROJECTS
    }


def consensus(records: Sequence[Mapping[str, Any]]) -> dict[tuple, dict[str, Any]]:
    """Task-local Pass A / Pass B consensus.  Capacity is not consulted."""
    observations: dict[tuple, dict[str, dict[str, Any]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for record in records:
        key = (
            str(record["pair_id"]), str(record["evaluation_set"]),
            int(record["cold_run"]),
        )
        parser = (record.get("transport_verification") or {}).get("task_results") or {}
        verifier = (record.get("existing_verifier") or {}).get("task_results") or {}
        for task_id in record["task_ids"]:
            observations[key][str(task_id)][str(record["pass_name"])] = {
                "decision": (parser.get(task_id) or {}).get("decision"),
                "model_ok": bool((record.get("model_call") or {}).get("ok")),
                "parser_ok": bool((record.get("transport_verification") or {}).get("ok")),
                "verifier_ok": (verifier.get(task_id) or {}).get("ok"),
            }
    resolved: dict[tuple, dict[str, Any]] = {}
    for key, tasks in observations.items():
        rows: dict[str, Any] = {}
        for task_id, passes in tasks.items():
            values = list(passes.values())
            if set(passes) != set(PASSES):
                status, decision = OUTCOME_MISSING, None
            elif not all(value["model_ok"] for value in values):
                status, decision = OUTCOME_FAILURE, None
            elif not all(value["parser_ok"] for value in values):
                status, decision = OUTCOME_PARSER, None
            elif not all(value["verifier_ok"] for value in values):
                status, decision = OUTCOME_VERIFIER, None
            elif passes["A"]["decision"] != passes["B"]["decision"]:
                status, decision = OUTCOME_DISAGREEMENT, None
            elif passes["A"]["decision"] == lineage.NEED_MORE_EVIDENCE:
                status, decision = OUTCOME_NME, lineage.NEED_MORE_EVIDENCE
            else:
                status, decision = "STABLE_CLAIM", passes["A"]["decision"]
            rows[task_id] = {
                "status": status,
                "decision": decision,
                "pass_a": passes.get("A", {}).get("decision"),
                "pass_b": passes.get("B", {}).get("decision"),
            }
        resolved[key] = rows
    return resolved


def _resolve(
    view: str, consensus_rows: Mapping[tuple, Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
    licences: Mapping[str, Any],
) -> tuple[dict[str, dict[int, str]], list[dict[str, Any]]]:
    """Apply the global capacity stage under one of the pre-registered views."""
    grouped: dict[tuple, dict[str, str]] = defaultdict(dict)
    for (pair_id, evaluation_set, cold_run), rows in consensus_rows.items():
        group = (
            (pair_id, evaluation_set, cold_run) if view == "PRIMARY_PER_TIER"
            else (pair_id, cold_run)
        )
        for task_id, value in rows.items():
            if value["status"] == "STABLE_CLAIM":
                grouped[group][task_id] = str(value["decision"])

    outcomes: dict[str, dict[int, str]] = defaultdict(dict)
    resolutions: list[dict[str, Any]] = []
    for group, claims in sorted(grouped.items(), key=lambda item: str(item[0])):
        pair_id = group[0]
        cold_run = group[-1]
        resolution = lineage.resolve_lineage_capacity(
            claims, candidates[pair_id], licences=licences[pair_id]
        )
        resolutions.append({
            "view": view,
            "group": [str(value) for value in group],
            "pair_id": pair_id,
            "cold_run": cold_run,
            "stable_claims": resolution["stable_claim_count"],
            "published": resolution["published_count"],
            "withheld": resolution["withheld_count"],
            "contested_capacity_keys": resolution["contested_capacity_keys"],
            "errors": resolution["errors"],
        })
        for task_id, candidate_id in resolution["published"].items():
            outcomes[task_id][cold_run] = candidate_id
        for task_id in resolution["withheld"]:
            outcomes[task_id][cold_run] = OUTCOME_CONTESTED
    for (_pair_id, _evaluation_set, cold_run), rows in consensus_rows.items():
        for task_id, value in rows.items():
            if value["status"] != "STABLE_CLAIM":
                outcomes[task_id][cold_run] = value["status"]
    return outcomes, resolutions


def _permutation_invariance(
    consensus_rows: Mapping[tuple, Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
    licences: Mapping[str, Any],
    *,
    trials: int = 25,
) -> dict[str, Any]:
    """Re-resolve every group under permuted claim order; nothing may move."""
    generator = random.Random(20260904)
    changes = 0
    groups = 0
    for (pair_id, evaluation_set, cold_run), rows in sorted(
        consensus_rows.items(), key=lambda item: str(item[0])
    ):
        claims = {
            task_id: str(value["decision"])
            for task_id, value in rows.items()
            if value["status"] == "STABLE_CLAIM"
        }
        if not claims:
            continue
        groups += 1
        baseline = lineage.resolve_lineage_capacity(
            claims, candidates[pair_id], licences=licences[pair_id]
        )
        for _ in range(trials):
            keys = sorted(claims)
            generator.shuffle(keys)
            shuffled = lineage.resolve_lineage_capacity(
                {key: claims[key] for key in keys},
                candidates[pair_id], licences=licences[pair_id],
            )
            if (
                shuffled["published"] != baseline["published"]
                or shuffled["errors"] != baseline["errors"]
                or shuffled["contested_capacity_keys"]
                != baseline["contested_capacity_keys"]
            ):
                changes += 1
    return {
        "groups_checked": groups,
        "permutation_trials_per_group": trials,
        "changes": changes,
    }


def _tier_metrics(
    tasks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reproducible = [row for row in tasks if row["reproducible"]]
    matches = [row for row in reproducible if row["stable_outcome_kind"] == OUTCOME_PUBLISHED]
    nme = [row for row in reproducible if row["stable_outcome_kind"] == OUTCOME_NME]
    unresolved_safe = [
        row for row in tasks
        if not row["reproducible"]
        and set(row["outcome_kinds"]) <= SAFE_UNRESOLVED_OUTCOMES | {OUTCOME_PUBLISHED}
    ]
    return {
        "tasks": len(tasks),
        "reproducible_tasks": len(reproducible),
        "stable_3_of_3_rate": (
            round(len(reproducible) / len(tasks), 6) if tasks else None
        ),
        "cross_cold_exact_consistency": (
            round(len(reproducible) / len(tasks), 6) if tasks else None
        ),
        "stable_published_matches": len(matches),
        "stable_published_match_rate": (
            round(len(matches) / len(tasks), 6) if tasks else None
        ),
        "stable_need_more_evidence": len(nme),
        "unstable_tasks": len(tasks) - len(reproducible),
        "unstable_but_safely_unresolved": len(unresolved_safe),
        "outcome_kind_counts": dict(sorted(Counter(
            kind for row in tasks for kind in row["outcome_kinds"]
        ).items())),
    }


def build(output: Path | None = None) -> dict[str, Any]:
    loaded = load(output)
    candidates = _pair_candidates()
    licences = {
        pair_id: lineage.exact_child_union_licences(values)
        for pair_id, values in candidates.items()
    }
    consensus_rows = consensus(loaded["records"])
    views = {}
    resolutions = []
    for view in acceptance.CAPACITY_VIEWS:
        outcomes, rows = _resolve(view, consensus_rows, candidates, licences)
        views[view] = outcomes
        resolutions.extend(rows)

    population = {
        str(value["task_id"]): value for value in loaded["population"]["tasks"]
    }
    sample = loaded["sample"]
    tier_of = {
        task_id: tier
        for tier, values in sample["selected_task_ids_by_tier"].items()
        for task_id in values
    }
    sentinel_ids = set(acceptance.SENTINEL_IDS)

    tasks: list[dict[str, Any]] = []
    for task_id in [*sample["selected_task_ids"], *sorted(sentinel_ids)]:
        tier = tier_of.get(task_id, "SENTINEL")
        cold_runs = COLD_RUNS if tier != "SENTINEL" else (1,)
        row = population[task_id]
        entry: dict[str, Any] = {
            "task_id": task_id,
            "tier": tier,
            "corpus": row["corpus"],
            "scope_kind": row["scope_kind"],
            "strata": row["strata"],
            "relation_types": row["relation_types"],
            "candidate_count": row["candidate_count"],
            "sentinel_label": row.get("sentinel_label"),
        }
        for view, outcomes in views.items():
            values = [outcomes.get(task_id, {}).get(cold) for cold in cold_runs]
            kinds = [
                OUTCOME_PUBLISHED if str(value).startswith("lcand_") else str(value)
                for value in values
            ]
            reproducible = (
                all(value is not None for value in values) and len(set(values)) == 1
            )
            entry[view] = {
                "outcomes": values,
                "outcome_kinds": kinds,
                "reproducible": reproducible,
                "stable_outcome": values[0] if reproducible else None,
                "stable_outcome_kind": kinds[0] if reproducible else None,
            }
        entry.update(entry["PRIMARY_PER_TIER"])
        tasks.append(entry)

    tier_metrics = {
        tier: _tier_metrics([row for row in tasks if row["tier"] == tier])
        for tier in acceptance.TIERS
    }
    secondary = {
        tier: _tier_metrics([
            {**row, **row["SECONDARY_CROSS_TIER"]}
            for row in tasks if row["tier"] == tier
        ])
        for tier in acceptance.TIERS
    }
    safety = _safety(loaded, tasks, resolutions)
    sentinels = _sentinels(tasks)
    permutation = _permutation_invariance(consensus_rows, candidates, licences)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_lineage_v2_7_tiered_acceptance_metrics",
        "production_head": stratified.PRODUCTION_HEAD,
        "production_release": stratified.PRODUCTION_RELEASE,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
        "consent": loaded["telemetry"]["consent"],
        "experiment_valid": bool(loaded["telemetry"]["experiment_valid"]),
        "capacity_stage": "POST_CONSENSUS_GLOBAL",
        "capacity_views": acceptance.CAPACITY_VIEWS,
        "acceptance_gates": acceptance.ACCEPTANCE_GATES,
        "tier_metrics": tier_metrics,
        "tier_metrics_secondary_cross_tier": secondary,
        "capacity_resolutions": resolutions,
        "permutation_invariance": permutation,
        "sentinels": sentinels,
        "safety": safety,
        "cost": holdout_metrics._cost(loaded),
        "tasks": tasks,
    }


def _safety(
    loaded: Mapping[str, Any], tasks: Sequence[Mapping[str, Any]],
    resolutions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    population = {
        str(value["task_id"]): value for value in loaded["population"]["tasks"]
    }
    unsupported = []
    for row in tasks:
        outcome = row["stable_outcome"]
        if not str(outcome).startswith("lcand_"):
            continue
        inventory = {
            str(value["candidate_id"])
            for value in population[row["task_id"]]["candidates"]
        }
        if str(outcome) not in inventory:
            unsupported.append({"task_id": row["task_id"], "candidate_id": outcome})

    errors = sorted({
        str(error)
        for resolution in resolutions
        if resolution["view"] == "PRIMARY_PER_TIER"
        for error in resolution["errors"]
    })
    sources = forensics.load_sources()
    union_index = forensics._exact_union_index(sources["groups"])
    classified: dict[str, str] = {}
    for resolution in resolutions:
        if resolution["view"] != "PRIMARY_PER_TIER":
            continue
        for error in resolution["errors"]:
            parts = str(error).split(":")
            if len(parts) != 6 or error in classified:
                continue
            classified[error] = forensics.classify_conflict(
                pair_id=resolution["pair_id"],
                capacity_key=":".join(parts[1:4]),
                claims=(
                    {"candidate_id": parts[4], "task_id": None},
                    {"candidate_id": parts[5], "task_id": None},
                ),
                sources=sources,
                union_index=union_index,
            )["root_cause_class"]
    counts = Counter({name: 0 for name in forensics.ROOT_CAUSE_CLASSES})
    for value in classified.values():
        counts[value] += 1
    technical = [
        row["task_id"] for row in tasks
        if set(row["outcome_kinds"]) & {OUTCOME_FAILURE, OUTCOME_PARSER, OUTCOME_MISSING}
    ]
    verifier = [
        row["task_id"] for row in tasks
        if OUTCOME_VERIFIER in row["outcome_kinds"]
    ]
    return {
        "unsupported_accepted_matches": unsupported,
        "unsupported_accepted_match_count": len(unsupported),
        "capacity_errors": errors,
        "capacity_error_count": len(errors),
        "conflict_root_cause_counts": dict(sorted(counts.items())),
        "true_conflicts": counts["A_TRUE_FUNCTION_FRAGMENT_CONFLICT"],
        "false_conflicts": sum(
            counts[name] for name in forensics.FALSE_CONFLICT_CLASSES
        ),
        "RIGHT_MAP_CONFLICT": sum("RIGHT_MAP_CONFLICT" in value for value in errors),
        "technical_failure_tasks": sorted(technical),
        "technical_failure_task_count": len(technical),
        "verifier_rejection_tasks": sorted(verifier),
        "verifier_rejection_task_count": len(verifier),
        "capacity_identity": "RIGHT physical_page + exact function_fragment_id",
    }


def _sentinels(tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_label = {
        str(row["sentinel_label"]): row for row in tasks
        if row["tier"] == "SENTINEL" and row.get("sentinel_label")
    }
    rows = []
    for label, reference in sorted(SENTINEL_REFERENCES.items()):
        row = by_label.get(label)
        observed = (row or {}).get("stable_outcome")
        rows.append({
            "sentinel": label,
            "task_id": (row or {}).get("task_id"),
            "expected_v2_4_2": reference,
            "observed_v2_7": observed,
            "status": (
                "UNCHANGED" if observed == reference
                else "UNRESOLVED" if observed is None
                else "CHANGED"
            ),
        })
    changed = [row for row in rows if row["status"] != "UNCHANGED"]
    return {
        "sentinels": rows,
        "regression": bool(changed),
        "changed": changed,
        "reference_use": "REGRESSION_REFERENCE_ONLY_NEVER_A_MAPPING_RULE",
    }


def gates(metrics: Mapping[str, Any]) -> dict[str, Any]:
    """Pre-registered gates.  Each AUTO tier earns eligibility on its own data."""
    thresholds = metrics["acceptance_gates"]
    safety = metrics["safety"]
    shared = {
        "experiment_valid": metrics["experiment_valid"],
        "unsupported_accepted_zero": (
            safety["unsupported_accepted_match_count"]
            <= thresholds["unsupported_accepted_max"]
        ),
        "false_capacity_conflicts_zero": (
            safety["false_conflicts"] <= thresholds["false_capacity_conflicts_max"]
        ),
        "right_map_conflict_zero": (
            safety["RIGHT_MAP_CONFLICT"] <= thresholds["right_map_conflict_max"]
        ),
        "no_technical_failures": (
            safety["technical_failure_task_count"]
            <= thresholds["technical_failures_max"]
        ),
        "sentinels_do_not_regress": not metrics["sentinels"]["regression"],
        "batch_permutation_changes_zero": (
            metrics["permutation_invariance"]["changes"]
            <= thresholds["batch_permutation_changes_max"]
        ),
        "all_requests_successful": (
            metrics["cost"]["successful_inference_requests"]
            == metrics["cost"]["planned_requests"]
        ),
    }
    tiers: dict[str, Any] = {}
    for tier, definition in acceptance.TIERS.items():
        if not definition["decides_go"]:
            continue
        row = metrics["tier_metrics"][tier]
        rate = row["stable_3_of_3_rate"] or 0.0
        consistency = row["cross_cold_exact_consistency"] or 0.0
        values = {
            **shared,
            "stable_3_of_3_threshold": rate >= thresholds["stable_3_of_3_min"],
            "cross_cold_consistency_threshold": (
                consistency >= thresholds["cross_cold_exact_consistency_min"]
            ),
            "produces_at_least_one_auto_match": (
                row["stable_published_matches"] > 0
            ),
        }
        tiers[tier] = {
            "gates": dict(sorted(values.items())),
            "failed_gates": sorted(
                name for name, value in values.items() if not value
            ),
            "passed": all(values.values()),
            "relation_earned": (
                sorted(definition["requires"]) if all(values.values()) else []
            ),
            "metrics": row,
        }
    earned = sorted({
        relation for value in tiers.values() for relation in value["relation_earned"]
    })
    return {
        "shared_gates": dict(sorted(shared.items())),
        "tiers": tiers,
        "relations_earning_auto_eligibility": earned,
        "any_tier_passed": bool(earned),
        "verdict": "GO_LIMITED_SHADOW" if earned else "NOT_READY",
        "hard_set_never_decides": True,
        "deploy": False,
        "shadow_enabled": False,
    }


def render_report(metrics: Mapping[str, Any], verdict: Mapping[str, Any]) -> str:
    lines = [
        "# Function Lineage v2.7 — tiered acceptance evaluation",
        "",
        "Consented inference on frozen inputs. No deploy, no shadow, no "
        "materialization, no vision. Capacity is resolved once, globally, after "
        "two-pass consensus.",
        "",
        "## Consent",
        "",
        "| Artifact | Consented SHA-256 | Observed |",
        "|---|---|---|",
    ]
    for name, value in metrics["consent"]["consented_sha256"].items():
        observed = metrics["consent"]["observed_sha256"][name]
        lines.append(
            f"| `{name}` | `{value}` | {'MATCH' if observed == value else 'DRIFT'} |"
        )
    lines.extend([
        "",
        "## Tier results (primary view: capacity resolved per tier)",
        "",
        "| Tier | Decides GO | Tasks | Reproducible 3/3 | Rate | Auto matches | "
        "Stable NME | Unstable |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ])
    for tier, definition in acceptance.TIERS.items():
        row = metrics["tier_metrics"][tier]
        lines.append(
            f"| `{tier}` | {'YES' if definition['decides_go'] else 'no'} | "
            f"{row['tasks']} | {row['reproducible_tasks']} | "
            f"{row['stable_3_of_3_rate']} | {row['stable_published_matches']} | "
            f"{row['stable_need_more_evidence']} | {row['unstable_tasks']} |"
        )
    lines.extend([
        "",
        "## Secondary view (capacity across every tier, production-like)",
        "",
        "| Tier | Reproducible 3/3 | Rate | Auto matches |",
        "|---|---:|---:|---:|",
    ])
    for tier in acceptance.TIERS:
        row = metrics["tier_metrics_secondary_cross_tier"][tier]
        lines.append(
            f"| `{tier}` | {row['reproducible_tasks']} | "
            f"{row['stable_3_of_3_rate']} | {row['stable_published_matches']} |"
        )
    safety = metrics["safety"]
    permutation = metrics["permutation_invariance"]
    lines.extend([
        "",
        "## Safety",
        "",
        f"Unsupported accepted `{safety['unsupported_accepted_match_count']}`; "
        f"verifier rejection tasks `{safety['verifier_rejection_task_count']}`; "
        f"technical failures `{safety['technical_failure_task_count']}`; "
        f"RIGHT_MAP_CONFLICT `{safety['RIGHT_MAP_CONFLICT']}`.",
        f"Capacity conflicts `{safety['capacity_error_count']}` — true "
        f"`{safety['true_conflicts']}`, false `{safety['false_conflicts']}`.",
        f"Permutation invariance: `{permutation['groups_checked']}` groups × "
        f"`{permutation['permutation_trials_per_group']}` shuffles, changes "
        f"`{permutation['changes']}`.",
        "",
        "## Sentinels",
        "",
        f"Sentinel regression: **{'YES' if metrics['sentinels']['regression'] else 'NO'}**.",
        "",
        "| Sentinel | Expected v2.4.2 | Observed v2.7 | Status |",
        "|---|---|---|---|",
    ])
    for row in metrics["sentinels"]["sentinels"]:
        lines.append(
            f"| {row['sentinel']} | `{row['expected_v2_4_2']}` | "
            f"`{row['observed_v2_7']}` | {row['status']} |"
        )
    lines.extend([
        "",
        "## GO / NO-GO",
        "",
    ])
    for tier, row in verdict["tiers"].items():
        lines.extend([
            f"### `{tier}` — {'PASS' if row['passed'] else 'FAIL'}",
            "",
            "| Gate | Result |",
            "|---|---|",
            *(
                f"| `{name}` | {'PASS' if value else 'FAIL'} |"
                for name, value in row["gates"].items()
            ),
            "",
        ])
    lines.extend([
        f"Relations earning automatic publication: "
        f"`{verdict['relations_earning_auto_eligibility'] or 'none'}`.",
        "",
        f"**Verdict: {verdict['verdict']}.**",
        "",
        "The HARD set never decides the product question; it is reported for "
        "diagnosis only.",
        "",
        "**DO NOT DEPLOY. DO NOT ENABLE SHADOW.** Production authorization is a "
        "separate, explicit decision.",
        "",
    ])
    return "\n".join(lines)


def write(output: Path | None = None) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    metrics = build(target)
    verdict = gates(metrics)
    for name in ("metrics.json", "task_results.json", "gates.json", "evaluation_report.md"):
        if (target / name).exists():
            raise RuntimeError(f"refusing to replace immutable result artifact: {name}")
    tasks = metrics.pop("tasks")
    (target / "task_results.json").write_bytes(_json_bytes(tasks))
    (target / "metrics.json").write_bytes(_json_bytes(metrics))
    (target / "gates.json").write_bytes(_json_bytes(verdict))
    (target / "evaluation_report.md").write_text(
        render_report(metrics, verdict), encoding="utf-8"
    )
    return target


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    print(json.dumps({"output": str(write(args.output))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
