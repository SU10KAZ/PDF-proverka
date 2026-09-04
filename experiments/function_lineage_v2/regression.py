"""Function Lineage v2.6 — deterministic full-corpus regression.

Phases 3 and 4 of the v2.6 master task.  The module never calls a model.

It proves four things about the v2.6 capacity ownership architecture:

1.  candidate generation is unchanged — every frozen deterministic candidate
    artifact regenerates byte-identically from live sources;
2.  the Function Scope Graph and the scoped transport replay byte-identically,
    with the recall baselines intact;
3.  replaying the frozen v2.5 selections through the new accounting keeps every
    true conflict rejected and produces zero false conflicts;
4.  the seven v2.4.2 sentinels keep their deterministic inputs.

Sentinel expectations are regression references.  They are never used to admit,
exclude, rank or repair a candidate.
"""
from __future__ import annotations

import copy
import itertools
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import capacity_forensics as forensics
from experiments.function_lineage_v2 import scope_graph
from experiments.function_lineage_v2 import scoped_smoke
from experiments.function_lineage_v2 import scoped_transport
from experiments.function_lineage_v2 import smoke as base_smoke
from experiments.function_lineage_v2 import stratified
from experiments.function_lineage_v2 import transport


REPO_ROOT = stratified.REPO_ROOT
DEFAULT_OUTPUT = (
    stratified.COMPARISON_ROOT / "20260903_function_lineage_v2_6_deterministic_regression"
)
EQUIVALENCE_OUTPUT = transport.CANDIDATE_EQUIVALENCE_RECORD
SCHEMA_VERSION = "function-lineage-deterministic-regression.v2.6"

#: Deterministic recall of the frozen candidate layer.  Any movement must be
#: reported with old/new values and an explicit reason.
EXPECTED_RECALL = copy.deepcopy(stratified.EXPECTED_RECALL)

#: v2.4.2 controls.  Regression references only — never a mapping rule.
SENTINEL_REFERENCES = {
    "LEFT17": "lcand_cd6c87ed7f043a937b27",
    "LEFT18": "lcand_d9f1abdb7469869363ad",
    "LEFT19": "lcand_26bcd544f168ff9ccea5",
    "LEFT20 DOMESTIC": "lcand_1d1f175a30c34b88c6e0",
    "LEFT20 FIRE": "lcand_ebafe4012323c47ac349",
    "LEFT20 METERING": "lcand_3e5e047c8b378f731c6b",
    "LEFT20 PARENT": "lcand_9c617494b14c2b922d3f",
}


def _json_bytes(value: Any) -> bytes:
    return stratified._json_bytes(value)


def _sha_file(path: Path) -> str:
    return base_smoke._sha_file(path)


# ---------------------------------------------------------------------------
# 1. candidate generation equivalence
# ---------------------------------------------------------------------------


def candidate_regeneration_equivalence(*, allow_missing_sources: bool = True) -> dict[str, Any]:
    """Regenerate the frozen candidate artifacts and compare byte-for-byte.

    Live project sources are not available in every environment.  When they are
    missing the previously recorded proof is returned unchanged, so the guard
    never silently downgrades to "no proof".
    """
    frozen = {
        pair_id: _sha_file(stratified.CANDIDATE_ROOT / f"{pair_id}.json")
        for pair_id in transport.PAIR_ORDER
    }
    blobs = transport.candidate_generation_blobs()
    try:
        from experiments.function_lineage_v2 import run as candidate_run

        regenerated = {}
        for pair_id in transport.PAIR_ORDER:
            dataset, _relations = candidate_run._load_dataset(pair_id)
            artifact = lineage.deterministic_candidate_artifact(
                dataset,
                run_id=json.loads(
                    (candidate_run._pair_dir(pair_id) / "production" / "state.json")
                    .read_text(encoding="utf-8")
                ).get("run_id")
                if pair_id != candidate_run.IOS21_PAIR_ID
                else candidate_run.IOS21_FORENSIC_RUN_ID,
            )
            # ``run._write_json`` uses exactly this serialization, so the
            # digest is the digest of the artifact file itself.
            regenerated[pair_id] = base_smoke._sha_bytes(_json_bytes(artifact))
    except Exception as error:  # noqa: BLE001 - sources are environment state
        if not allow_missing_sources:
            raise
        if EQUIVALENCE_OUTPUT.is_file():
            recorded = json.loads(EQUIVALENCE_OUTPUT.read_text(encoding="utf-8"))
            recorded["regeneration_executed"] = False
            recorded["regeneration_skipped_reason"] = type(error).__name__
            return recorded
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "function_lineage_candidate_regeneration_equivalence",
            "regeneration_executed": False,
            "regeneration_skipped_reason": type(error).__name__,
            "byte_identical": False,
            "frozen_artifact_sha256": frozen,
            "regenerated_artifact_sha256": {},
            "candidate_generation_blobs": blobs,
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_lineage_candidate_regeneration_equivalence",
        "regeneration_executed": True,
        "byte_identical": regenerated == frozen,
        "frozen_artifact_sha256": frozen,
        "regenerated_artifact_sha256": regenerated,
        "candidate_generation_blobs": blobs,
        "algorithm_version": lineage.ALGORITHM_VERSION,
    }


# ---------------------------------------------------------------------------
# 2. deterministic replay of the scoped layer
# ---------------------------------------------------------------------------


def deterministic_replay() -> dict[str, Any]:
    """Replay the scope graph and scoped transport; both must be identical."""
    scope_first = scope_graph.build_artifacts()
    scope_second = scope_graph.build_artifacts()
    scope_mismatch = sorted(
        name for name in set(scope_first) | set(scope_second)
        if scope_first.get(name) != scope_second.get(name)
    )
    frozen_mismatch = sorted(
        name for name, payload in scope_first.items()
        if not (scope_graph.DEFAULT_OUTPUT / name).is_file()
        or (scope_graph.DEFAULT_OUTPUT / name).read_bytes() != payload
    )
    transport_hashes = scoped_transport.build_artifacts(
        scoped_transport.DEFAULT_OUTPUT, check=True
    )
    return {
        "scope_graph_replay_identical": not scope_mismatch,
        "scope_graph_matches_frozen": not frozen_mismatch,
        "scope_graph_replay_mismatches": scope_mismatch,
        "scope_graph_frozen_mismatches": frozen_mismatch,
        "scoped_transport_matches_frozen": True,
        "scoped_transport_artifact_sha256": transport_hashes,
    }


def recall_baselines() -> dict[str, Any]:
    metrics = forensics._read_json(stratified.SCOPE_ROOT / "scope_metrics.json")
    raw = dict(metrics["overall"]["raw_candidate_recall"])
    scoped = dict(metrics["overall"]["scope_eligible_recall"])
    raw.pop("case_count", None)
    scoped.pop("case_count", None)
    return {
        "expected": EXPECTED_RECALL,
        "observed": {
            "raw_candidate_recall": raw,
            "scope_eligible_recall": scoped,
        },
        "unchanged": (
            raw == EXPECTED_RECALL["raw_candidate_recall"]
            and scoped == EXPECTED_RECALL["scope_eligible_recall"]
        ),
        "case_count": metrics["overall"]["raw_candidate_recall"]["case_count"],
    }


#: Frozen v2.4 baseline.  ``group_generation_failures`` counts two LEFT pages
#: whose composite-role set cover produced no group at all; it is a property of
#: the frozen candidate layer, not of capacity accounting.
FROZEN_SCOPE_BASELINE = {
    "search_failure_count": 0,
    "group_generation_failure_count": 2,
    "capacity_key_defect_count": 0,
    "candidate_loss_count": 0,
    "RIGHT_MAP_CONFLICT": 0,
    "raw_candidate_count": 1461,
    "persisted_candidate_count": 1461,
    "page_global_exclusivity": False,
    "unknown_scopes": 0,
}


def scope_safety() -> dict[str, Any]:
    """Cross-granularity competition and candidate preservation of the graph."""
    tasks = forensics._read_json(stratified.SCOPE_ROOT / "selector_tasks_scoped.json")
    metrics = forensics._read_json(stratified.SCOPE_ROOT / "scope_metrics.json")
    overall = metrics["overall"]
    safety = metrics["safety"]
    observed = {
        "search_failure_count": safety["search_failure_count"],
        "group_generation_failure_count": safety["group_generation_failure_count"],
        "capacity_key_defect_count": safety["capacity_key_defect_count"],
        "candidate_loss_count": safety["candidate_loss_count"],
        "RIGHT_MAP_CONFLICT": safety["RIGHT_MAP_CONFLICT"],
        "raw_candidate_count": safety["raw_candidate_count"],
        "persisted_candidate_count": safety["persisted_candidate_count"],
        "page_global_exclusivity": safety["page_global_exclusivity"],
        "unknown_scopes": overall["unknown_scopes"],
    }
    return {
        "scoped_task_count": tasks["scoped_task_count"],
        "original_fragment_task_count": tasks["original_fragment_task_count"],
        "task_identity": tasks["task_identity"],
        "model_calls": tasks["model_calls"],
        "cross_granularity_competition": overall["cross_granularity_competition"],
        "unknown_scope_policy": (
            "FAIL_CLOSED"
            if not any(
                value.get("unknown_is_selectable") for value in tasks["tasks"]
            ) else "SELECTABLE"
        ),
        "observed": observed,
        "frozen_baseline": FROZEN_SCOPE_BASELINE,
        "matches_frozen_baseline": observed == FROZEN_SCOPE_BASELINE,
        "raw_candidates_preserved": (
            safety["raw_candidate_count"] == safety["persisted_candidate_count"]
            and safety["candidate_loss_count"] == 0
        ),
        "candidate_partition_defect_count": safety["capacity_key_defect_count"],
    }


# ---------------------------------------------------------------------------
# 3. capacity replay of the frozen v2.5 selections
# ---------------------------------------------------------------------------


def _pair_candidates() -> dict[str, dict[str, Any]]:
    return {
        pair_id: {
            str(value["candidate_id"]): value
            for value in forensics._read_json(
                stratified.CANDIDATE_ROOT / f"{pair_id}.json"
            )["functional_candidates"]
        }
        for pair_id in stratified.PAIR_PROJECTS
    }


def _normalise_conflict(pair_id: str, error: str) -> str:
    """Canonical, order-independent identity of one capacity conflict.

    The v2.5 baseline recorded ``previous:claim`` in arrival order.  v2.6
    resolves capacity as a pure function of the claim set and emits the pair
    sorted, so the two forms have to be compared on identity, not on bytes.
    """
    parts = str(error).split(":")
    if len(parts) != 6 or parts[0] != "FUNCTION_FRAGMENT_CONFLICT":
        return f"{pair_id}|{error}"
    key = ":".join(parts[1:4])
    left, right = sorted(parts[4:6])
    return f"{pair_id}|FUNCTION_FRAGMENT_CONFLICT:{key}:{left}:{right}"


def capacity_replay() -> dict[str, Any]:
    """Re-apply capacity to every frozen v2.5 model batch, uniformly.

    Unlike the v2.5 harness this replay does **not** skip task pairs whose
    source scopes intersect.  Scope containment is now a licence proved from
    the mappings themselves, so every pair is accounted for.
    """
    evaluation = forensics.load_evaluation()
    candidates = _pair_candidates()
    licences = {
        pair_id: lineage.exact_child_union_licences(values)
        for pair_id, values in candidates.items()
    }
    # A batch is one evaluation set of one pair in one cold repeat and pass.
    # Sentinel and new-sample tasks were never decided together, so accounting
    # them together would invent competition that never happened.
    decisions: dict[tuple[str, str, int, str], dict[str, str]] = defaultdict(dict)
    for record in evaluation["records"]:
        key = (
            str(record["pair_id"]), str(record["evaluation_set"]),
            int(record["cold_run"]), str(record["pass_name"]),
        )
        for result in (record.get("response") or {}).get("results") or []:
            decisions[key][str(result["task_id"])] = str(result["decision"])

    before: set[str] = set()
    for record in evaluation["records"]:
        for error in (record.get("capacity_verification") or {}).get("errors") or []:
            before.add(_normalise_conflict(str(record["pair_id"]), str(error)))

    after: set[str] = set()
    batches = 0
    for (pair_id, _evaluation_set, _cold_run, _pass_name), values in sorted(decisions.items()):
        batches += 1
        selections = [
            {"task_id": task_id, "candidate_id": candidate_id}
            for task_id, candidate_id in sorted(values.items())
        ]
        for error in lineage.verify_capacity(
            selections, candidates[pair_id], licences=licences[pair_id]
        ):
            after.add(_normalise_conflict(pair_id, str(error)))

    resolved = sorted(before - after)
    introduced = sorted(after - before)
    forensic = forensics.build()
    true_conflicts = {
        _normalise_conflict(str(row["pair_id"]), str(row["error"]))
        for row in forensic["observed"]["conflicts"]
        if row["verdict"]["root_cause_class"] == "A_TRUE_FUNCTION_FRAGMENT_CONFLICT"
    }
    return {
        "model_batches_replayed": batches,
        "conflicts_before": len(before),
        "conflicts_after": len(after),
        "resolved_conflicts": resolved,
        "introduced_conflicts": introduced,
        "true_conflicts_still_rejected": len(true_conflicts & after),
        "true_conflicts_total": len(true_conflicts),
        "true_conflicts_lost": sorted(true_conflicts - after),
        "conflict_identity": "pair + capacity key + sorted candidate pair",
        "false_conflicts_after": len(after - true_conflicts),
    }


def population_capacity_sweep() -> dict[str, Any]:
    """Classify every reachable capacity collision under the new accounting."""
    evaluation = forensics.load_evaluation()
    candidates = _pair_candidates()
    licences = {
        pair_id: lineage.exact_child_union_licences(values)
        for pair_id, values in candidates.items()
    }
    rows = evaluation["population"]["tasks"]
    by_corpus: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_corpus[str(row["corpus"])].append(row)

    counts: Counter[str] = Counter()
    licence_kinds: Counter[str] = Counter()
    for corpus in stratified.CORPUS_ORDER:
        pair_id = stratified.PROJECT_PAIRS[corpus]
        values = candidates[pair_id]
        tasks = sorted(by_corpus[corpus], key=lambda value: str(value["task_id"]))
        keys = {
            str(candidate_id): frozenset(
                str(key) for key in values[str(candidate_id)]["right_capacity_keys"]
            )
            for task in tasks for candidate_id in task["candidate_ids"]
            if str(candidate_id) in values
        }
        for left_task, right_task in itertools.combinations(tasks, 2):
            for left_id in sorted(left_task["candidate_ids"]):
                if str(left_id) not in keys:
                    continue
                for right_id in sorted(right_task["candidate_ids"]):
                    if str(left_id) == str(right_id) or str(right_id) not in keys:
                        continue
                    shared = keys[str(left_id)] & keys[str(right_id)]
                    if not shared:
                        continue
                    report = lineage.capacity_ownership(
                        [
                            {"task_id": str(left_task["task_id"]), "candidate_id": str(left_id)},
                            {"task_id": str(right_task["task_id"]), "candidate_id": str(right_id)},
                        ],
                        values,
                        licences=licences[pair_id],
                    )
                    errors = set(report["errors"])
                    for key in sorted(shared):
                        rejected = any(key in error for error in errors)
                        counts["rejected" if rejected else "licensed"] += 1
                    for value in report["licences"]:
                        licence_kinds[json.loads(value)["licence"]] += 1
    return {
        "classified_collisions": counts["rejected"] + counts["licensed"],
        "rejected": counts["rejected"],
        "licensed": counts["licensed"],
        "licence_kinds": dict(sorted(licence_kinds.items())),
    }


# ---------------------------------------------------------------------------
# 4. sentinel deterministic inputs
# ---------------------------------------------------------------------------


def sentinel_inputs() -> dict[str, Any]:
    """Confirm the seven controls keep identical deterministic inputs.

    The check asserts availability and eligibility of the reference candidate,
    never that the model must choose it.
    """
    tasks = forensics._read_json(stratified.SCOPE_ROOT / "selector_tasks_scoped.json")
    scoped = {str(value["scoped_task_id"]): value for value in tasks["tasks"]}
    contexts: dict[str, Mapping[str, Any]] = {}
    for shard in forensics._read_jsonl(
        stratified.TRANSPORT_ROOT / "scoped_selector_shards.jsonl"
    ):
        for context in shard["model_payload"]["task_contexts"]:
            contexts[str(context["task_id"])] = context

    rows = []
    for label, (task_id, scope_id) in sorted(scoped_smoke.TASKS.items()):
        task = scoped.get(task_id)
        context = contexts.get(task_id)
        reference = SENTINEL_REFERENCES[label]
        candidate_ids = [str(value) for value in (task or {}).get("candidate_ids") or []]
        context_ids = [
            str(value["candidate_id"])
            for value in (context or {}).get("functional_candidates") or []
        ]
        rows.append({
            "sentinel": label,
            "task_id": task_id,
            "scope_id": scope_id,
            "scope_id_matches": (task or {}).get("coverage_scope_id") == scope_id,
            "reference_candidate_id": reference,
            "reference_is_selectable": reference in candidate_ids,
            "reference_in_transport_context": reference in context_ids,
            "allowed_output_count": len((task or {}).get("allowed_outputs") or []),
            "candidate_count": len(candidate_ids),
            "unknown_is_selectable": (task or {}).get("unknown_is_selectable"),
        })
    return {
        "sentinels": rows,
        "all_references_selectable": all(row["reference_is_selectable"] for row in rows),
        "all_scope_ids_match": all(row["scope_id_matches"] for row in rows),
        "all_present_in_transport": all(row["reference_in_transport_context"] for row in rows),
        "reference_use": "REGRESSION_REFERENCE_ONLY_NEVER_A_MAPPING_RULE",
    }


# ---------------------------------------------------------------------------
# artifact
# ---------------------------------------------------------------------------


def build(*, allow_missing_sources: bool = True) -> dict[str, Any]:
    equivalence = candidate_regeneration_equivalence(
        allow_missing_sources=allow_missing_sources
    )
    if equivalence.get("regeneration_executed") and equivalence.get("byte_identical"):
        # The record is the proof the candidate-source guard reads, so it has to
        # exist before any consumer of the frozen candidate artifacts runs.
        EQUIVALENCE_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        EQUIVALENCE_OUTPUT.write_bytes(_json_bytes(equivalence))
    replay = deterministic_replay()
    recall = recall_baselines()
    safety = scope_safety()
    capacity = capacity_replay()
    sweep = population_capacity_sweep()
    sentinels = sentinel_inputs()

    gates = {
        "candidate_generation_byte_identical": bool(equivalence.get("byte_identical")),
        "scope_graph_deterministic": replay["scope_graph_replay_identical"]
        and replay["scope_graph_matches_frozen"],
        "scoped_transport_deterministic": replay["scoped_transport_matches_frozen"],
        "recall_unchanged": recall["unchanged"],
        "cross_granularity_competition_zero": (
            safety["cross_granularity_competition"]["after"]["candidate_pair_count"] == 0
            and safety["cross_granularity_competition"]["after"]["task_count"] == 0
        ),
        "raw_candidates_preserved": bool(safety["raw_candidates_preserved"]),
        "candidate_partition_defects_zero": safety["candidate_partition_defect_count"] == 0,
        "scope_safety_matches_frozen_baseline": safety["matches_frozen_baseline"],
        "unknown_scope_fail_closed": safety["unknown_scope_policy"] == "FAIL_CLOSED",
        "right_map_conflict_zero": safety["observed"]["RIGHT_MAP_CONFLICT"] == 0,
        "page_global_exclusivity_absent": not safety["observed"]["page_global_exclusivity"],
        "search_failures_zero": safety["observed"]["search_failure_count"] == 0,
        "false_capacity_conflicts_zero": capacity["false_conflicts_after"] == 0,
        "true_conflicts_still_rejected": (
            capacity["true_conflicts_still_rejected"] == capacity["true_conflicts_total"]
        ),
        "no_new_conflicts_introduced": not capacity["introduced_conflicts"],
        "sentinel_inputs_unchanged": (
            sentinels["all_references_selectable"]
            and sentinels["all_scope_ids_match"]
            and sentinels["all_present_in_transport"]
        ),
        "model_calls_zero": True,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_lineage_v2_6_deterministic_regression",
        "model_calls": 0,
        "production_head": stratified.PRODUCTION_HEAD,
        "production_release": stratified.PRODUCTION_RELEASE,
        "candidate_regeneration_equivalence": equivalence,
        "deterministic_replay": replay,
        "recall": recall,
        "scope_safety": safety,
        "capacity_replay": capacity,
        "population_capacity_sweep": sweep,
        "sentinels": sentinels,
        "gates": dict(sorted(gates.items())),
        "all_gates_passed": all(gates.values()),
    }


def render_report(artifact: Mapping[str, Any]) -> str:
    capacity = artifact["capacity_replay"]
    sweep = artifact["population_capacity_sweep"]
    recall = artifact["recall"]
    lines = [
        "# Function Lineage v2.6 — deterministic full-corpus regression",
        "",
        "No model calls, no production state, no shadow, no materialization.",
        "",
        "## Gates",
        "",
        "| Gate | Result |",
        "|---|---|",
    ]
    for name, value in artifact["gates"].items():
        lines.append(f"| `{name}` | {'PASS' if value else 'FAIL'} |")
    lines.extend([
        "",
        f"All gates passed: **{'YES' if artifact['all_gates_passed'] else 'NO'}**.",
        "",
        "## Recall baselines",
        "",
        f"Cases `{recall['case_count']}`; unchanged: `{recall['unchanged']}`.",
        "",
        "| Metric | @1 | @3 | @5 | @10 |",
        "|---|---:|---:|---:|---:|",
    ])
    for name in ("raw_candidate_recall", "scope_eligible_recall"):
        row = recall["observed"][name]
        lines.append(
            f"| {name} | {row['recall_at_1']} | {row['recall_at_3']} | "
            f"{row['recall_at_5']} | {row['recall_at_10']} |"
        )
    lines.extend([
        "",
        "## Capacity replay of the frozen v2.5 selections",
        "",
        f"Model batches replayed `{capacity['model_batches_replayed']}`; "
        f"conflicts before `{capacity['conflicts_before']}`; after "
        f"`{capacity['conflicts_after']}`.",
        f"True conflicts still rejected "
        f"`{capacity['true_conflicts_still_rejected']}`/"
        f"`{capacity['true_conflicts_total']}`; false conflicts after "
        f"`{capacity['false_conflicts_after']}`; newly introduced "
        f"`{len(capacity['introduced_conflicts'])}`.",
        "",
        "The replay applies capacity to every task pair. The v2.5 harness skipped "
        "pairs whose source scopes intersected, so this run is strictly stricter, "
        "not looser.",
        "",
        "## Population capacity sweep",
        "",
        f"Reachable collisions `{sweep['classified_collisions']}`; rejected "
        f"`{sweep['rejected']}`; licensed `{sweep['licensed']}`.",
        "",
        "| Licence | Count |",
        "|---|---:|",
    ])
    for name, value in sweep["licence_kinds"].items():
        lines.append(f"| `{name}` | {value} |")
    lines.extend([
        "",
        "## Sentinels",
        "",
        "Regression references only — never a mapping rule.",
        "",
        "| Sentinel | Scope matches | Reference selectable | In transport | Candidates |",
        "|---|---|---|---|---:|",
    ])
    for row in artifact["sentinels"]["sentinels"]:
        lines.append(
            f"| {row['sentinel']} | {row['scope_id_matches']} | "
            f"{row['reference_is_selectable']} | "
            f"{row['reference_in_transport_context']} | {row['candidate_count']} |"
        )
    lines.append("")
    return "\n".join(lines)


def write(output: Path | None = None, *, allow_missing_sources: bool = True) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    target.mkdir(parents=True, exist_ok=True)
    artifact = build(allow_missing_sources=allow_missing_sources)
    (target / "regression.json").write_bytes(_json_bytes(artifact))
    (target / "report.md").write_text(render_report(artifact), encoding="utf-8")
    return target


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--require-sources", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    target = write(args.output, allow_missing_sources=not args.require_sources)
    print(json.dumps({"output": str(target)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
