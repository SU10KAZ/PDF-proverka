"""Function Lineage v2.6 — diagnostic replay of the consented holdout.

Phase B.  The module re-reads the 110 model responses already recorded under
consent and re-derives the published outcome through the v2.6 post-consensus
global capacity stage.  **It never calls a model.**

This is a DIAGNOSTIC REPLAY.  It is explicitly not a new acceptance holdout:
the sample has already been seen, so its numbers may not be used as
independent evidence for a production decision.

Production semantics of a "run" are one cold repeat: Pass A and Pass B of
every task, consensus computed task-locally, then one global capacity
resolution over the stable claims of that run.  The three cold repeats stay a
research stability measure on top of that.
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import capacity_forensics as forensics
from experiments.function_lineage_v2 import holdout
from experiments.function_lineage_v2 import holdout_metrics
from experiments.function_lineage_v2 import stratified


SCHEMA_VERSION = "function-lineage-holdout-diagnostic-replay.v2.6"
DEFAULT_OUTPUT = holdout.DEFAULT_OUTPUT
EVALUATION_SET = "HOLDOUT"


def _json_bytes(value: Any) -> bytes:
    return stratified._json_bytes(value)


def _candidates() -> dict[str, dict[str, Any]]:
    return {
        pair_id: {
            str(value["candidate_id"]): value
            for value in stratified._read_json(
                stratified.CANDIDATE_ROOT / f"{pair_id}.json"
            )["functional_candidates"]
        }
        for pair_id in stratified.PAIR_PROJECTS
    }


def task_local_consensus(
    records: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, int], dict[str, dict[str, Any]]]:
    """Pass A / Pass B consensus per task, with capacity deliberately absent.

    Keyed by ``(pair_id, cold_run)`` because one cold repeat is one
    production-equivalent run.
    """
    observations: dict[
        tuple[str, int], dict[str, dict[str, dict[str, Any]]]
    ] = defaultdict(lambda: defaultdict(dict))
    for record in records:
        if str(record["evaluation_set"]) != EVALUATION_SET:
            continue
        run = (str(record["pair_id"]), int(record["cold_run"]))
        parser = (record.get("transport_verification") or {}).get("task_results") or {}
        verifier = (record.get("existing_verifier") or {}).get("task_results") or {}
        for task_id in record["task_ids"]:
            observations[run][str(task_id)][str(record["pass_name"])] = {
                "decision": (parser.get(task_id) or {}).get("decision"),
                "model_ok": bool((record.get("model_call") or {}).get("ok")),
                "parser_ok": bool((record.get("transport_verification") or {}).get("ok")),
                "verifier_ok": (verifier.get(task_id) or {}).get("ok"),
            }

    consensus: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}
    for run, tasks in observations.items():
        resolved: dict[str, dict[str, Any]] = {}
        for task_id, passes in tasks.items():
            values = list(passes.values())
            if set(passes) != set(holdout.PASSES):
                status, decision = "REQUEST_NOT_OBSERVED", None
            elif not all(value["model_ok"] for value in values):
                status, decision = "REQUEST_FAILURE", None
            elif not all(value["parser_ok"] for value in values):
                status, decision = "RESPONSE_PARSER_REJECTION", None
            elif not all(value["verifier_ok"] for value in values):
                status, decision = "VERIFIER_REJECTION", None
            elif passes["A"]["decision"] != passes["B"]["decision"]:
                status, decision = "PASS_DISAGREEMENT", None
            else:
                decision = passes["A"]["decision"]
                status = (
                    "STABLE_UNRESOLVED" if decision == lineage.NEED_MORE_EVIDENCE
                    else "STABLE_CLAIM"
                )
            resolved[task_id] = {
                "status": status,
                "decision": decision,
                "pass_a": passes.get("A", {}).get("decision"),
                "pass_b": passes.get("B", {}).get("decision"),
            }
        consensus[run] = resolved
    return consensus


def replay(output: Path | None = None) -> dict[str, Any]:
    """Re-derive published outcomes through the global capacity stage."""
    loaded = holdout_metrics.load(output)
    candidates = _candidates()
    licences = {
        pair_id: lineage.exact_child_union_licences(values)
        for pair_id, values in candidates.items()
    }
    consensus = task_local_consensus(loaded["records"])

    published: dict[str, dict[int, str]] = defaultdict(dict)
    withheld: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    statuses: dict[str, dict[int, str]] = defaultdict(dict)
    resolutions: list[dict[str, Any]] = []
    for (pair_id, cold_run), tasks in sorted(consensus.items()):
        claims = {
            task_id: str(value["decision"])
            for task_id, value in tasks.items()
            if value["status"] == "STABLE_CLAIM"
        }
        resolution = lineage.resolve_lineage_capacity(
            claims, candidates[pair_id], licences=licences[pair_id]
        )
        resolutions.append({
            "pair_id": pair_id,
            "corpus": stratified.PAIR_PROJECTS[pair_id],
            "cold_run": cold_run,
            "stable_claims": resolution["stable_claim_count"],
            "published": resolution["published_count"],
            "withheld": resolution["withheld_count"],
            "contested_capacity_keys": resolution["contested_capacity_keys"],
            "errors": resolution["errors"],
        })
        for task_id, value in tasks.items():
            statuses[task_id][cold_run] = value["status"]
        for task_id, candidate_id in resolution["published"].items():
            published[task_id][cold_run] = candidate_id
        for task_id, value in resolution["withheld"].items():
            withheld[task_id][cold_run] = value
            statuses[task_id][cold_run] = lineage.CAPACITY_CONTESTED
        for task_id, value in tasks.items():
            if value["status"] == "STABLE_UNRESOLVED":
                published[task_id][cold_run] = lineage.NEED_MORE_EVIDENCE

    old_tasks = {
        str(row["task_id"]): row
        for row in holdout_metrics.task_results(loaded)
        if row["evaluation_set"] == EVALUATION_SET
    }
    cold_runs = list(holdout.HOLDOUT_COLD_RUNS)
    rows: list[dict[str, Any]] = []
    for task_id in sorted(old_tasks):
        old = old_tasks[task_id]
        values = published.get(task_id, {})
        new_stable = (
            len(values) == len(cold_runs) and len(set(values.values())) == 1
        )
        new_decision = next(iter(set(values.values()))) if new_stable else None
        rows.append({
            "task_id": task_id,
            "corpus": old["corpus"],
            "strata": old["strata"],
            "candidate_count": old["candidate_count"],
            "old_stable": bool(old["cross_cold_exact_selection_consistent"]),
            "old_decision": old["stable_decision"],
            "old_repeat_statuses": [value["status"] for value in old["cold_repeats"]],
            "new_stable": new_stable,
            "new_decision": new_decision,
            "new_repeat_statuses": [
                statuses.get(task_id, {}).get(cold_run) for cold_run in cold_runs
            ],
            "changed": bool(old["cross_cold_exact_selection_consistent"]) != new_stable
            or old["stable_decision"] != new_decision,
        })

    recovered = [
        row for row in rows if not row["old_stable"] and row["new_stable"]
    ]
    lost = [row for row in rows if row["old_stable"] and not row["new_stable"]]
    still_unstable = [
        row for row in rows if not row["old_stable"] and not row["new_stable"]
    ]
    model_level = [
        row for row in still_unstable
        if not any(
            value == lineage.CAPACITY_CONTESTED
            for value in row["new_repeat_statuses"]
        )
    ]
    conflicts = _classify(resolutions)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_lineage_holdout_diagnostic_replay",
        "diagnostic_only": True,
        "usable_as_acceptance_evidence": False,
        "model_calls": 0,
        "reason": (
            "the sample has already been seen; a replay of it can diagnose an "
            "architecture change but can never accept one"
        ),
        "capacity_stage": "POST_CONSENSUS_GLOBAL",
        "run_semantics": "one cold repeat is one production-equivalent run",
        "capacity_resolutions": resolutions,
        "tasks": rows,
        "old_stable_3_of_3": sum(row["old_stable"] for row in rows),
        "new_stable_3_of_3": sum(row["new_stable"] for row in rows),
        "task_count": len(rows),
        "recovered_by_architecture_fix": [row["task_id"] for row in recovered],
        "recovered_count": len(recovered),
        "lost_by_architecture_fix": [row["task_id"] for row in lost],
        "lost_count": len(lost),
        "still_unstable_count": len(still_unstable),
        "model_level_unstable": [row["task_id"] for row in model_level],
        "model_level_unstable_count": len(model_level),
        "capacity_level_unstable_count": len(still_unstable) - len(model_level),
        "stability_ceiling_after_fix": (
            round(sum(row["new_stable"] for row in rows) / len(rows), 6)
            if rows else None
        ),
        **conflicts,
    }


def _classify(resolutions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    sources = forensics.load_sources()
    union_index = forensics._exact_union_index(sources["groups"])
    seen: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for resolution in resolutions:
        for error in resolution["errors"]:
            parts = str(error).split(":")
            if len(parts) != 6:
                continue
            key = ":".join(parts[1:4])
            signature = (resolution["pair_id"], key, parts[4], parts[5])
            if signature in seen:
                continue
            seen[signature] = forensics.classify_conflict(
                pair_id=resolution["pair_id"],
                capacity_key=key,
                claims=(
                    {"candidate_id": parts[4], "task_id": None},
                    {"candidate_id": parts[5], "task_id": None},
                ),
                sources=sources,
                union_index=union_index,
            )
    counts = Counter({name: 0 for name in forensics.ROOT_CAUSE_CLASSES})
    for verdict in seen.values():
        counts[str(verdict["root_cause_class"])] += 1
    return {
        "conflict_count": len(seen),
        "conflict_root_cause_counts": dict(sorted(counts.items())),
        "true_conflicts": counts["A_TRUE_FUNCTION_FRAGMENT_CONFLICT"],
        "false_conflicts": sum(
            counts[name] for name in forensics.FALSE_CONFLICT_CLASSES
        ),
    }


def render_report(artifact: Mapping[str, Any]) -> str:
    lines = [
        "# Function Lineage v2.6 — diagnostic replay of the consented holdout",
        "",
        "**DIAGNOSTIC ONLY.** No model call was made (`model_calls = 0`). The "
        "sample has already been seen, so these numbers diagnose the Phase A "
        "architecture change and are never acceptance evidence.",
        "",
        f"Capacity stage: `{artifact['capacity_stage']}`. "
        f"Run semantics: {artifact['run_semantics']}.",
        "",
        "## Effect of removing batch-dependent capacity",
        "",
        f"Stable 3/3 before `{artifact['old_stable_3_of_3']}`/"
        f"`{artifact['task_count']}`; after "
        f"`{artifact['new_stable_3_of_3']}`/`{artifact['task_count']}` "
        f"(ceiling `{artifact['stability_ceiling_after_fix']}`).",
        "",
        f"Recovered purely by the architecture fix: "
        f"`{artifact['recovered_count']}`; lost: `{artifact['lost_count']}`.",
        f"Still unstable `{artifact['still_unstable_count']}`, of which "
        f"model-level `{artifact['model_level_unstable_count']}` and still "
        f"capacity-contested `{artifact['capacity_level_unstable_count']}`.",
        "",
        "## Conflicts after the change",
        "",
        f"Distinct conflicts `{artifact['conflict_count']}`; true "
        f"`{artifact['true_conflicts']}`; false `{artifact['false_conflicts']}`.",
        "",
        "| Root cause | Conflicts |",
        "|---|---:|",
        *(
            f"| `{name}` | {value} |"
            for name, value in artifact["conflict_root_cause_counts"].items()
        ),
        "",
        "## Tasks whose outcome changed",
        "",
        "| Task | Corpus | Candidates | Before | After |",
        "|---|---|---:|---|---|",
    ]
    for row in artifact["tasks"]:
        if not row["changed"]:
            continue
        lines.append(
            f"| `{row['task_id']}` | {row['corpus']} | {row['candidate_count']} | "
            f"{'stable' if row['old_stable'] else 'unstable'} "
            f"({','.join(str(value) for value in row['old_repeat_statuses'])}) | "
            f"{'stable' if row['new_stable'] else 'unstable'} "
            f"({','.join(str(value) for value in row['new_repeat_statuses'])}) |"
        )
    lines.append("")
    return "\n".join(lines)


def write(output: Path | None = None) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    artifact = replay(target)
    (target / "diagnostic_replay.json").write_bytes(_json_bytes(artifact))
    (target / "diagnostic_replay_report.md").write_text(
        render_report(artifact), encoding="utf-8"
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
