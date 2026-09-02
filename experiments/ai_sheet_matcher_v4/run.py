"""Run the isolated Candidate Generator v4 AI Sheet Matcher repeat."""
from __future__ import annotations

import argparse
import json
import math
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from experiments.ai_sheet_matcher.core import (
    CONCRETE_DECISIONS,
    PROJECT_CONFIG,
    aggregate_decisions,
    canonical_json,
    decision_metrics,
    digest,
    production_sources_unchanged,
)
from experiments.ai_sheet_matcher.run import call_codex_bounded, render_images
from experiments.candidate_v4.core import ALGORITHM_VERSION as CANDIDATE_GENERATOR_VERSION

from .core import (
    ALGORITHM_VERSION,
    V4SelectorDataset,
    build_group_audit,
    build_selector_prompt,
    build_v4_selector_dataset,
    close_support_left_pages,
    output_schema,
    subset_selector_dataset,
    verify_v4_selector_response,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = REPO_ROOT / "comparison" / "ai_sheet_matcher" / "20260902_v4_ai_repeat"
OLD_EXPERIMENT = REPO_ROOT / "comparison" / "ai_sheet_matcher" / "20260902_experiment"
V4_BENCHMARK = REPO_ROOT / "comparison" / "ai_sheet_matcher" / "20260902_candidate_v4"
DEFAULT_MODEL = "gpt-5.6-sol"
DEFAULT_EFFORT = "medium"


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(canonical_json(dict(row)) + "\n" for row in rows), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _load_datasets() -> list[V4SelectorDataset]:
    return [build_v4_selector_dataset(REPO_ROOT, pair_id) for pair_id in PROJECT_CONFIG]


def _readme(*, complete: bool, signatures: Mapping[str, str]) -> str:
    status = "complete" if complete else "group shortlist audited; model calls not started"
    return f"""# AI Sheet Matcher repeat — Candidate Generator v4

Status: **{status}**

This is an isolated offline repeat of commit `41d43625`.  Selector output,
Pass A/Pass B, three-cold-run unanimity, document-map review, verifier, and
human-priority materialization gate are retained.  Only the candidate source is
replaced by `{CANDIDATE_GENERATOR_VERSION}` from commit `7948c97d`.

The experiment does not import into the backend or frontend, does not write to
pair/run directories, does not change `production-sheet-matcher.v3`, UI,
engineer mappings, or the production pipeline, and has no deploy action.

Run in two gated phases:

1. `python -m experiments.ai_sheet_matcher_v4.run candidate-audit`
2. `python -m experiments.ai_sheet_matcher_v4.run experiment --model {DEFAULT_MODEL} --effort {DEFAULT_EFFORT}`

Input signatures: `{json.dumps(dict(signatures), ensure_ascii=False, sort_keys=True)}`.

Artifacts:

- `experiment_report.md` — A/B findings, critical cases, safety verdict;
- `metrics.json` — project-level baseline/old/new metrics;
- `decisions.jsonl` — TEXT and final fallback decision traces;
- `model_runs.jsonl` — bounded outputs and call telemetry;
- `stability.json` — three-cold-run exact/map overlap and unstable relations;
- `manual_audit.json` — SUPPORTED/PARTIAL/UNSUPPORTED audit;
- `group_audit.json` — deterministic shortlist and post-shortlist recall;
- `cost_analysis.json` — TEXT/VISION call, token, runtime, and unit cost.
"""


def run_candidate_audit(output: Path) -> None:
    datasets = _load_datasets()
    audit = build_group_audit(datasets)
    if audit["summary"]["group_recall_after_shortlist"] != 1.0:
        raise RuntimeError("group shortlist recall gate failed; model calls are forbidden")
    output.mkdir(parents=True, exist_ok=True)
    _write_json(output / "group_audit.json", audit)
    (output / "README.md").write_text(
        _readme(complete=False, signatures=audit["input_signatures"]), encoding="utf-8",
    )
    print("group shortlist persisted before model calls")
    print("group recall after shortlist", audit["summary"]["group_recall_after_shortlist"])
    for project in audit["projects"]:
        print(
            project["project"],
            "generator_groups=" + str(project["generator_group_count"]),
            "max_shortlist=" + str(project["shortlist_count_max"]),
            "recall=" + str(project["group_recall_after_shortlist"]),
        )


def _job(
    dataset: V4SelectorDataset,
    *,
    mode: str,
    cold_run: int,
    pass_name: str,
    prompt: str,
    payload: Mapping[str, Any],
    images: Sequence[Path],
    image_manifest: Sequence[str],
    model: str,
    effort: str,
) -> dict[str, Any]:
    result = call_codex_bounded(
        prompt=prompt,
        schema=output_schema(dataset, str(payload["payload_signature"])),
        model=model,
        effort=effort,
        images=images,
    )
    verification = verify_v4_selector_response(
        dataset, str(payload["payload_signature"]), result.response,
    )
    return {
        "project": dataset.selector.project,
        "pair_id": dataset.selector.pair_id,
        "run_id": dataset.selector.run_id,
        "mode": mode,
        "cold_run": cold_run,
        "pass_name": pass_name,
        "left_pages": sorted(int(task["left_page"]) for task in dataset.selector.tasks),
        "payload_signature": payload["payload_signature"],
        "candidate_input_signature": dataset.selector.input_signature,
        "candidate_generator": CANDIDATE_GENERATOR_VERSION,
        "image_manifest": list(image_manifest),
        "model": model,
        "reasoning_effort": effort,
        "model_call": {
            "ok": result.ok,
            "duration_s": round(result.duration_s, 3),
            "usage": result.usage,
            "error": result.error,
            "attempts": result.attempts,
        },
        "response": result.response,
        "verification": verification,
    }


def _run_jobs(
    jobs: Sequence[tuple[V4SelectorDataset, str, int, str, str, Mapping[str, Any], Sequence[Path], Sequence[str]]],
    *,
    model: str,
    effort: str,
    workers: int,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(
                _job,
                dataset,
                mode=mode,
                cold_run=cold_run,
                pass_name=pass_name,
                prompt=prompt,
                payload=payload,
                images=images,
                image_manifest=manifest,
                model=model,
                effort=effort,
            ): (dataset.selector.pair_id, mode, cold_run, pass_name)
            for dataset, mode, cold_run, pass_name, prompt, payload, images, manifest in jobs
        }
        for index, future in enumerate(as_completed(futures), 1):
            key = futures[future]
            row = future.result()
            output.append(row)
            print(
                index, "/", len(jobs), key,
                "ok=" + str(row["model_call"]["ok"]),
                f"{row['model_call']['duration_s']:.1f}s",
            )
    return output


def _aggregate_text(
    datasets: Sequence[V4SelectorDataset], records: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    decisions: list[dict[str, Any]] = []
    stability: list[dict[str, Any]] = []
    for dataset in datasets:
        project_decisions, project_stability = aggregate_decisions(
            dataset.selector, mode="TEXT", run_records=records,
        )
        decisions.extend(project_decisions)
        stability.append(project_stability)
    return decisions, stability


def _fallback_reasons(
    dataset: V4SelectorDataset,
    decisions: Sequence[Mapping[str, Any]],
) -> dict[int, list[str]]:
    decision_by_left = {
        int(value["left_page"]): value for value in decisions
        if value["pair_id"] == dataset.selector.pair_id
    }
    close = close_support_left_pages(dataset)
    output: dict[int, list[str]] = {}
    for left_page, decision in decision_by_left.items():
        if decision.get("materialization_allowed"):
            continue
        reasons: list[str] = []
        pass_a = {int(row["cold_run"]): row for row in decision["pass_A"]}
        pass_b = {int(row["cold_run"]): row for row in decision["pass_B"]}
        if any(
            row.get("option_id") == "NEED_MORE_EVIDENCE"
            for row in [*pass_a.values(), *pass_b.values()]
        ):
            reasons.append("TEXT_NEED_MORE_EVIDENCE")
        if any(
            not pass_a[run].get("verified")
            or not pass_b[run].get("verified")
            or pass_a[run].get("option_id") != pass_b[run].get("option_id")
            for run in (1, 2, 3)
        ):
            reasons.append("TEXT_PASS_A_B_DISAGREEMENT_OR_FAILURE")
        cold = list(decision.get("cold_run_unanimous_options") or [])
        if len(cold) != 3 or any(value is None for value in cold) or len(set(cold)) > 1:
            reasons.append("TEXT_COLD_RUN_INSTABILITY")
        if left_page in close:
            reasons.append("CLOSE_DETERMINISTIC_SUPPORT_GAP_LE_0.05")
        if reasons:
            output[left_page] = reasons
    return output


def _combined_decisions(
    datasets: Sequence[V4SelectorDataset],
    text_decisions: Sequence[Mapping[str, Any]],
    vision_decisions: Sequence[Mapping[str, Any]],
    fallback_reasons: Mapping[str, Mapping[int, Sequence[str]]],
) -> list[dict[str, Any]]:
    vision_by_key = {
        (str(row["pair_id"]), int(row["left_page"])): row for row in vision_decisions
    }
    output = []
    for text_row in text_decisions:
        key = (str(text_row["pair_id"]), int(text_row["left_page"]))
        source = vision_by_key.get(key, text_row)
        row = dict(source)
        row["mode"] = "TEXT_VISION_FALLBACK"
        row["resolution_stage"] = "VISION_FALLBACK" if key in vision_by_key else "TEXT"
        row["vision_fallback_reasons"] = list(
            fallback_reasons.get(key[0], {}).get(key[1], [])
        )
        row["text_selected_option_id"] = text_row.get("selected_option_id")
        row["text_final_status"] = text_row.get("final_status")
        output.append(row)
    return output


def _stable_relation_counts(decisions: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    unique = {
        str(row["selected_option_id"]): str(row["selected_option"]["decision_type"])
        for row in decisions
        if row.get("selected_option_id") and isinstance(row.get("selected_option"), Mapping)
    }
    return {
        relation: sum(value == relation for value in unique.values())
        for relation in sorted(CONCRETE_DECISIONS)
    }


def _human_match(dataset: V4SelectorDataset, row: Mapping[str, Any]) -> bool | None:
    human = dataset.selector.human_by_left.get(int(row["left_page"]))
    option = row.get("selected_option")
    if human is None or not isinstance(option, Mapping):
        return None
    return (
        str(option.get("decision_type")) == str(human.get("decision_type"))
        and sorted(option.get("left_pages") or []) == sorted(human.get("left_pages") or [])
        and sorted(option.get("right_pages") or []) == sorted(human.get("right_pages") or [])
    )


def _manual_audit(
    datasets: Sequence[V4SelectorDataset], combined: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    dataset_by_pair = {value.selector.pair_id: value for value in datasets}
    records = []
    for row in combined:
        option = row.get("selected_option")
        if not isinstance(option, Mapping):
            continue
        dataset = dataset_by_pair[str(row["pair_id"])]
        human_match = _human_match(dataset, row)
        reference_match = any(
            sorted(int(page) for page in reference["left_pages"]) == sorted(option["left_pages"])
            and sorted(int(page) for page in reference["right_pages"]) == sorted(option["right_pages"])
            for reference in dataset.selector.reference_cases
        )
        if human_match is True:
            classification = "SUPPORTED"
            basis = "Exact saved engineer-accepted mapping plus bounded v4 provenance."
        elif human_match is False:
            classification = "UNSUPPORTED"
            basis = "Conflicts with a saved engineer-accepted mapping; blocked by human priority."
        elif reference_match:
            classification = "PARTIAL"
            basis = "Matches a non-authoritative research hypothesis; independent human confirmation is absent."
        else:
            classification = "UNSUPPORTED"
            basis = "No saved engineer support or exact audited reference group; not materialized."
        visual_review = {
            "fcand_f00e3f46720ddfe494a3": (
                "SUPPORTED: LEFT physical 27/28 are two same-title VРУ-3 calculation sheets "
                "(graphic 4/5); RIGHT physical 27 is the consolidated VРУ-3 sheet (graphic 5). "
                "Text functions align on ELECTRICAL_DISTRIBUTION/RISER_DISTRIBUTION and the raster "
                "shows the two legacy fragments covered by the wider consolidated diagram."
            ),
            "fcand_ff7c727fb1c2286ef562": (
                "SUPPORTED: LEFT physical 29/30 are the same-title VРУ-4 calculation sequence "
                "(graphic 6/7); RIGHT physical 28 is the consolidated VРУ-4 sheet (graphic 6). "
                "Text and raster jointly show input, power-distribution, and lighting content combined "
                "on the RIGHT sheet."
            ),
        }.get(str(row["selected_option_id"]))
        records.append({
            "project": row["project"],
            "pair_id": row["pair_id"],
            "left_page": row["left_page"],
            "selected_option_id": row["selected_option_id"],
            "relation_type": option["decision_type"],
            "left_pages": option["left_pages"],
            "right_pages": option["right_pages"],
            "final_status": row["final_status"],
            "materialization_allowed": row["materialization_allowed"],
            "classification": classification,
            "basis": basis,
            "manual_complex_relation_gate": option["decision_type"] != "MATCH_1_TO_1",
            "independent_text_and_raster_review": visual_review,
            "evidence_refs": list(option.get("evidence_refs") or []),
        })
    auto = [value for value in records if value["materialization_allowed"]]
    complex_auto = [value for value in auto if value["manual_complex_relation_gate"]]
    unique_auto_relations = {value["selected_option_id"] for value in auto}
    unique_complex_relations = {value["selected_option_id"] for value in complex_auto}
    return {
        "kind": "v4_ai_repeat_manual_audit",
        "schema_version": "manual-audit.v1",
        "classification_scale": ["SUPPORTED", "PARTIAL", "UNSUPPORTED"],
        "scope": "All stable concrete final decisions; every auto-resolved and every complex relation is explicit.",
        "records": records,
        "summary": {
            "stable_concrete_decisions": len(records),
            "auto_resolved_decisions": len(auto),
            "auto_resolved_unique_relations": len(unique_auto_relations),
            "auto_supported": sum(value["classification"] == "SUPPORTED" for value in auto),
            "auto_partial": sum(value["classification"] == "PARTIAL" for value in auto),
            "unsupported_auto_matches": sum(value["classification"] == "UNSUPPORTED" for value in auto),
            "complex_auto_relation_task_occurrences": len(complex_auto),
            "complex_auto_unique_relations": len(unique_complex_relations),
            "complex_auto_all_audited": all(
                value["independent_text_and_raster_review"] for value in complex_auto
            ),
        },
    }


def _call_metrics(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    calls = [row.get("model_call") or {} for row in records]
    tokens = [
        int(call.get("usage", {}).get("total_tokens"))
        for call in calls if isinstance(call.get("usage", {}).get("total_tokens"), int)
    ]
    return {
        "model_calls": len(calls),
        "model_attempts_including_retries": sum(int(call.get("attempts") or 0) for call in calls),
        "successful_calls": sum(bool(call.get("ok")) for call in calls),
        "failed_calls": sum(not bool(call.get("ok")) for call in calls),
        "verified_map_calls": sum(bool(row.get("verification", {}).get("ok")) for row in records),
        "rejected_map_calls": sum(not bool(row.get("verification", {}).get("ok")) for row in records),
        "runtime_sum_s": round(sum(float(call.get("duration_s") or 0.0) for call in calls), 3),
        "tokens_total": sum(tokens) if tokens else None,
        "token_telemetry_call_count": len(tokens),
    }


def _decision_status_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        **decision_metrics(rows),
        "stable_relations": _stable_relation_counts(rows),
        "correct_engineer_decisions": sum(row.get("materialization_allowed") for row in rows),
        "incorrect_engineer_conflicts": sum(
            row.get("verifier_status") == "BLOCKED_HUMAN_DECISION_CONFLICT" for row in rows
        ),
        "engineer_conflict_materializations": sum(
            row.get("materialization_allowed")
            and row.get("verifier_status") == "BLOCKED_HUMAN_DECISION_CONFLICT"
            for row in rows
        ),
    }


def _stability_from_decisions(
    rows: Sequence[Mapping[str, Any]], *, project: str, pair_id: str, mode: str,
) -> dict[str, Any]:
    selected = [row for row in rows if row["pair_id"] == pair_id]
    cold_maps = [
        {
            str(row["task_id"]): (row.get("cold_run_unanimous_options") or [None, None, None])[index]
            for row in selected
        }
        for index in range(3)
    ]
    pairs = []
    for left, right in ((0, 1), (0, 2), (1, 2)):
        exact = sum(cold_maps[left][task] == cold_maps[right][task] for task in cold_maps[left])
        concrete_left = {(task, value) for task, value in cold_maps[left].items() if value}
        concrete_right = {(task, value) for task, value in cold_maps[right].items() if value}
        union = concrete_left | concrete_right
        pairs.append({
            "cold_runs": [left + 1, right + 1],
            "exact_decision_overlap": round(exact / len(selected), 6) if selected else None,
            "same_task_count": exact,
            "map_overlap": round(len(concrete_left & concrete_right) / len(union), 6) if union else 1.0,
        })
    unstable = [
        {
            "left_page": row["left_page"],
            "relation_options": list(row.get("cold_run_unanimous_options") or []),
        }
        for row in selected if row.get("selected_option_id") is None
    ]
    return {
        "project": project,
        "pair_id": pair_id,
        "mode": mode,
        "cold_run_count": 3,
        "passes_per_cold_run": 2,
        "cold_map_signatures": [digest(value) for value in cold_maps],
        "pairwise_overlap": pairs,
        "stable_core": [
            {"left_page": row["left_page"], "option_id": row["selected_option_id"]}
            for row in selected if row.get("selected_option_id") is not None
        ],
        "stable_task_count": sum(row.get("selected_option_id") is not None for row in selected),
        "disagreement_count": len(unstable),
        "unstable_relations": unstable,
    }


def _cost_analysis(
    records: Sequence[Mapping[str, Any]], combined: Sequence[Mapping[str, Any]], *, wall_runtime_s: float,
) -> dict[str, Any]:
    text = [row for row in records if row["mode"] == "TEXT"]
    vision = [row for row in records if row["mode"] == "VISION_FALLBACK"]
    text_metrics = _call_metrics(text)
    vision_metrics = _call_metrics(vision)
    total_metrics = _call_metrics(records)
    stable_auto = sum(row.get("materialization_allowed") for row in combined)
    old = _read_json(OLD_EXPERIMENT / "metrics.json")
    old_tokens = old["overall_calls"]["tokens_total"]
    old_text_tokens = sum(project["AI_TEXT"]["tokens_total"] for project in old["projects"])
    old_vision_tokens = sum(project["AI_VISION_TEXT"]["tokens_total"] for project in old["projects"])
    old_best_auto = sum(project["AI_VISION_TEXT"]["stable_auto_decisions"] for project in old["projects"])
    vision_left_tasks = len({
        (str(row["pair_id"]), int(page))
        for row in vision for page in row.get("left_pages") or []
    })
    total_left_tasks = len({
        (str(row["pair_id"]), int(page))
        for row in text for page in row.get("left_pages") or []
    })
    return {
        "kind": "v4_ai_repeat_cost_analysis",
        "schema_version": "cost-analysis.v1",
        "TEXT": text_metrics,
        "VISION_FALLBACK": vision_metrics,
        "TOTAL": total_metrics,
        "wall_runtime_s": round(wall_runtime_s, 3),
        "stable_auto_decisions": stable_auto,
        "tokens_per_stable_auto_decision": (
            round(total_metrics["tokens_total"] / stable_auto, 3)
            if stable_auto and total_metrics["tokens_total"] is not None else None
        ),
        "previous_experiment": {
            "model_calls": old["overall_calls"]["model_calls"],
            "tokens_total": old_tokens,
            "wall_runtime_s": old["wall_runtime_s"],
            "stable_auto_decisions_TEXT": sum(p["AI_TEXT"]["stable_auto_decisions"] for p in old["projects"]),
            "stable_auto_decisions_VISION_TEXT": sum(p["AI_VISION_TEXT"]["stable_auto_decisions"] for p in old["projects"]),
            "TEXT_tokens": old_text_tokens,
            "VISION_TEXT_tokens": old_vision_tokens,
            "tokens_per_best_final_stable_auto_decision": round(old_tokens / old_best_auto, 3),
        },
        "vision_scope": {
            "fallback_left_tasks": vision_left_tasks,
            "all_benchmark_left_tasks": total_left_tasks,
            "left_tasks_avoided": total_left_tasks - vision_left_tasks,
            "fallback_share": round(vision_left_tasks / total_left_tasks, 6) if total_left_tasks else None,
            "task_pass_exposures": vision_left_tasks * 6,
            "full_vision_task_pass_exposures": total_left_tasks * 6,
        },
        "delta_vs_previous": {
            "model_calls": total_metrics["model_calls"] - old["overall_calls"]["model_calls"],
            "tokens_total": (
                total_metrics["tokens_total"] - old_tokens
                if total_metrics["tokens_total"] is not None and old_tokens is not None else None
            ),
            "vision_calls_avoided_vs_full_vision_arm": 18 - vision_metrics["model_calls"],
            "vision_tokens": (
                vision_metrics["tokens_total"] - old_vision_tokens
                if vision_metrics["tokens_total"] is not None else None
            ),
            "vision_tokens_reduction_fraction": (
                round(1 - vision_metrics["tokens_total"] / old_vision_tokens, 6)
                if vision_metrics["tokens_total"] is not None and old_vision_tokens else None
            ),
            "total_tokens_change_fraction": (
                round(total_metrics["tokens_total"] / old_tokens - 1, 6)
                if total_metrics["tokens_total"] is not None and old_tokens else None
            ),
            "tokens_per_stable_auto_change_fraction": (
                round(
                    (total_metrics["tokens_total"] / stable_auto) / (old_tokens / old_best_auto) - 1,
                    6,
                )
                if stable_auto and total_metrics["tokens_total"] is not None else None
            ),
        },
    }


def _metrics(
    datasets: Sequence[V4SelectorDataset],
    text: Sequence[Mapping[str, Any]],
    combined: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
    manual: Mapping[str, Any],
    group_audit: Mapping[str, Any],
    cost: Mapping[str, Any],
) -> dict[str, Any]:
    old = _read_json(OLD_EXPERIMENT / "metrics.json")
    old_by_pair = {value["pair_id"]: value for value in old["projects"]}
    v4 = _read_json(V4_BENCHMARK / "metrics.json")
    v4_by_pair = {value["pair_id"]: value for value in v4["projects"]}
    projects = []
    for dataset in datasets:
        pair_id = dataset.selector.pair_id
        text_rows = [row for row in text if row["pair_id"] == pair_id]
        final_rows = [row for row in combined if row["pair_id"] == pair_id]
        text_calls = [row for row in records if row["pair_id"] == pair_id and row["mode"] == "TEXT"]
        vision_calls = [row for row in records if row["pair_id"] == pair_id and row["mode"] == "VISION_FALLBACK"]
        old_row = old_by_pair[pair_id]
        v4_row = v4_by_pair[pair_id]
        projects.append({
            "project": dataset.selector.project,
            "pair_id": pair_id,
            "baseline_H_P_U": dataset.selector.baseline,
            "old_AI_TEXT": old_row["AI_TEXT"],
            "old_AI_VISION_TEXT": old_row["AI_VISION_TEXT"],
            "v4_candidate_recall": v4_row["v4"],
            "v4_AI_TEXT": {**_decision_status_metrics(text_rows), **_call_metrics(text_calls)},
            "v4_TEXT_VISION_FALLBACK": {**_decision_status_metrics(final_rows), **_call_metrics([*text_calls, *vision_calls])},
            "fallback_only_calls": _call_metrics(vision_calls),
        })
    old_text_auto = sum(value["AI_TEXT"]["stable_auto_decisions"] for value in old["projects"])
    old_best_auto = sum(value["AI_VISION_TEXT"]["stable_auto_decisions"] for value in old["projects"])
    final_auto = sum(value["v4_TEXT_VISION_FALLBACK"]["stable_auto_decisions"] for value in projects)
    stable_complex = sum(
        value["v4_TEXT_VISION_FALLBACK"]["materialized_relations"][relation]
        for value in projects
        for relation in ("SPLIT_1_TO_N", "MERGED_N_TO_1", "FUNCTION_DISTRIBUTED")
    )
    text_one_to_one = sum(
        value["v4_AI_TEXT"]["materialized_relations"]["MATCH_1_TO_1"] for value in projects
    )
    ordinary_human_tasks = sum(
        1 for dataset in datasets for page in dataset.comparison_left_pages
        if dataset.selector.human_by_left.get(page, {}).get("decision_type") == "MATCH_1_TO_1"
    )
    gates = {
        "unsupported_auto_matches_zero": manual["summary"]["unsupported_auto_matches"] == 0,
        "engineer_conflict_materializations_zero": all(
            value["v4_TEXT_VISION_FALLBACK"]["engineer_conflict_materializations"] == 0
            for value in projects
        ),
        "closed_engineer_mappings_all_supported": manual["summary"]["auto_partial"] == 0
        and manual["summary"]["unsupported_auto_matches"] == 0,
        "stable_auto_substantially_above_old_text": final_auto >= math.ceil(old_text_auto * 1.25),
        "stable_auto_substantially_above_old_best_arm": final_auto >= math.ceil(old_best_auto * 1.25),
        "at_least_one_stable_safe_complex_relation": stable_complex > 0,
        "text_solves_majority_of_ordinary_engineer_cases": (
            text_one_to_one > ordinary_human_tasks / 2 if ordinary_human_tasks else False
        ),
        "group_recall_after_shortlist_100_percent": group_audit["summary"]["group_recall_after_shortlist"] == 1.0,
        "vision_tokens_reduced_at_least_50_percent": (
            cost["delta_vs_previous"]["vision_tokens_reduction_fraction"] is not None
            and cost["delta_vs_previous"]["vision_tokens_reduction_fraction"] >= 0.5
        ),
        "total_tokens_not_above_previous": (
            cost["delta_vs_previous"]["total_tokens_change_fraction"] is not None
            and cost["delta_vs_previous"]["total_tokens_change_fraction"] <= 0
        ),
    }
    safety = (
        gates["unsupported_auto_matches_zero"]
        and gates["engineer_conflict_materializations_zero"]
        and gates["closed_engineer_mappings_all_supported"]
    )
    verdict = "A" if safety and all(gates.values()) else ("B" if safety and final_auto > old_text_auto else "C")
    return {
        "kind": "v4_ai_sheet_matcher_repeat_metrics",
        "schema_version": "ai-sheet-matcher-v4-repeat-metrics.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "candidate_generator": CANDIDATE_GENERATOR_VERSION,
        "selector_baseline_commit": "41d43625",
        "candidate_generator_commit": "7948c97d",
        "ios21_forensic_commit": "dbddb691",
        "cold_runs": 3,
        "passes_per_cold_run": 2,
        "projects": projects,
        "overall": {
            "old_TEXT_stable_auto": old_text_auto,
            "old_VISION_TEXT_stable_auto": sum(value["AI_VISION_TEXT"]["stable_auto_decisions"] for value in old["projects"]),
            "v4_TEXT_stable_auto": sum(value["v4_AI_TEXT"]["stable_auto_decisions"] for value in projects),
            "v4_TEXT_VISION_FALLBACK_stable_auto": final_auto,
            "stable_relations_TEXT": _stable_relation_counts(text),
            "stable_relations_TEXT_VISION_FALLBACK": _stable_relation_counts(combined),
            "unsupported_auto_matches": manual["summary"]["unsupported_auto_matches"],
            "model_calls": cost["TOTAL"]["model_calls"],
            "tokens": cost["TOTAL"]["tokens_total"],
            "wall_runtime_s": cost["wall_runtime_s"],
        },
        "acceptance_gates": gates,
        "safety_gate_passed": safety,
        "source_artifacts_unchanged": all(production_sources_unchanged(value.selector) for value in datasets),
        "verdict": verdict,
    }


def _choice(row: Mapping[str, Any] | None) -> str:
    if not row:
        return "—"
    option = row.get("selected_option")
    if isinstance(option, Mapping):
        return f"{option['decision_type']} {option['left_pages']}→{option['right_pages']} ({row['selected_option_id']})"
    return str(row.get("selected_option_id") or row.get("final_status") or "—")


def _pass_trace(row: Mapping[str, Any] | None) -> str:
    if not row:
        return "—"
    option_by_id = {
        value["candidate_id"]: value for value in row.get("candidate_evidence") or []
    }

    def label(value: Mapping[str, Any]) -> str:
        option_id = value.get("option_id")
        option = option_by_id.get(option_id)
        if option:
            result = f"{option['decision_type']}→{option['right_pages']}"
        else:
            result = str(option_id)
        return result + ("!map-rejected" if not value.get("verified") else "")

    pass_a = ", ".join(f"r{value['cold_run']}={label(value)}" for value in row["pass_A"])
    pass_b = ", ".join(f"r{value['cold_run']}={label(value)}" for value in row["pass_B"])
    return f"A[{pass_a}]; B[{pass_b}]"


def _report(
    metrics: Mapping[str, Any],
    decisions: Sequence[Mapping[str, Any]],
    fallback_reasons: Mapping[str, Mapping[int, Sequence[str]]],
    group_audit: Mapping[str, Any],
    manual: Mapping[str, Any],
    cost: Mapping[str, Any],
    stability: Mapping[str, Any],
) -> str:
    text = [row for row in decisions if row["mode"] == "TEXT"]
    final = [row for row in decisions if row["mode"] == "TEXT_VISION_FALLBACK"]
    text_by_key = {(row["pair_id"], row["left_page"]): row for row in text}
    final_by_key = {(row["pair_id"], row["left_page"]): row for row in final}
    lines = [
        "# AI Sheet Matcher repeat with Candidate Generator v4",
        "",
        "## Итог",
        "",
        f"Вердикт: **{metrics['verdict']}**. Safety gate: **{'PASS' if metrics['safety_gate_passed'] else 'FAIL'}**. "
        f"Unsupported auto matches: **{metrics['overall']['unsupported_auto_matches']}**.",
        "",
        "Повтор выполнен изолированно на том же 36-LEFT benchmark. Selector, JSON output shape, local→Document Map Review, "
        "Pass A/B, three-cold-run unanimity, human-priority gate и fail-closed поведение сохранены; заменён только источник "
        "bounded candidates на Candidate Generator v4. Production, UI, pipeline, engineer mappings и deploy не изменялись.",
        "",
        "## A/B по проектам",
        "",
        "| Проект | Baseline H/P/U | old TEXT auto/review/unresolved | v4 TEXT auto/review/unresolved | v4 TEXT+VISION fallback auto/review/unresolved |",
        "|---|---:|---:|---:|---:|",
    ]
    for project in metrics["projects"]:
        baseline = project["baseline_H_P_U"]
        old = project["old_AI_TEXT"]
        new = project["v4_AI_TEXT"]
        fallback = project["v4_TEXT_VISION_FALLBACK"]
        lines.append(
            f"| {project['project']} | {baseline['HIGH']}/{baseline['POSSIBLE']}/{baseline['UNKNOWN']} | "
            f"{old['stable_auto_decisions']}/{old['human_review']}/{old['unresolved']} | "
            f"{new['stable_auto_decisions']}/{new['human_review']}/{new['unresolved']} | "
            f"{fallback['stable_auto_decisions']}/{fallback['human_review']}/{fallback['unresolved']} |"
        )
    lines.extend([
        "",
        "Stable relation counts (unique bounded relation IDs):",
        "",
        f"- TEXT: `{metrics['overall']['stable_relations_TEXT']}`",
        f"- TEXT + VISION fallback: `{metrics['overall']['stable_relations_TEXT_VISION_FALLBACK']}`",
        "",
        "## ИОС 2.1 critical cases",
        "",
        "| LEFT | Engineer RIGHT | v4 rank | TEXT | Final / VISION fallback | Cold stability |",
        "|---:|---:|---:|---|---|---|",
    ])
    pair = "pe336037597"
    for left, right, rank in ((17, 7, 5), (18, 8, 2), (19, 9, 4)):
        text_row = text_by_key.get((pair, left))
        final_row = final_by_key.get((pair, left))
        used_vision = final_row and final_row.get("resolution_stage") == "VISION_FALLBACK"
        stable = "YES" if final_row and final_row.get("selected_option_id") is not None else "NO"
        agrees = bool(
            final_row and isinstance(final_row.get("selected_option"), Mapping)
            and final_row["selected_option"]["right_pages"] == [right]
        )
        lines.append(
            f"| {left} | {right} | {rank} | {_choice(text_row)} | "
            f"{'VISION: ' if used_vision else 'TEXT retained: '}{_choice(final_row)}; engineer={'YES' if agrees else 'NO'} | {stable} |"
        )
    lines.extend([
        "",
        "Pass-level trace:",
        "",
    ])
    for left in (17, 18, 19):
        text_row = text_by_key.get((pair, left))
        final_row = final_by_key.get((pair, left))
        lines.append(f"- LEFT {left} TEXT: `{_pass_trace(text_row)}`")
        if final_row and final_row.get("resolution_stage") == "VISION_FALLBACK":
            lines.append(f"- LEFT {left} VISION: `{_pass_trace(final_row)}`")
        else:
            lines.append(f"- LEFT {left} VISION: не запускался.")
    sheet = final_by_key.get((pair, 20))
    sheet_text = text_by_key.get((pair, 20))
    lines.extend([
        "",
        "### Графический лист 5 (physical LEFT 20)",
        "",
        f"Target `fcand_6294159aac7851a636dd` (`FUNCTION_DISTRIBUTED`, RIGHT `[26,28,29]`) был в shortlist "
        f"с rank 10. TEXT: `{_choice(sheet_text)}`. Final: `{_choice(sheet)}`. "
        f"Stable target selected: **{bool(sheet and sheet.get('selected_option_id') == 'fcand_6294159aac7851a636dd')}**.",
        f"TEXT pass trace: `{_pass_trace(sheet_text)}`. VISION pass trace: `{_pass_trace(sheet)}`. "
        "Группа появлялась в отдельных Pass B, но конфликтовала с другими uses RIGHT и была fail-closed; "
        "стабильно заменить NO_MATCH/одиночный лист не смогла.",
        "",
        "## Group shortlist",
        "",
        f"Передавалось не более {group_audit['shortlist_limit_per_left']} групп на LEFT из "
        f"{sum(value['generator_group_count'] for value in group_audit['projects'])} generated groups. "
        f"Group Recall after shortlist: **{group_audit['summary']['group_recall_after_shortlist'] * 100:.1f}%** "
        f"({group_audit['summary']['evaluation_hits_after_shortlist']}/{group_audit['summary']['evaluation_case_count']}). "
        "Reference map не участвовала в построении или ranking shortlist и использовалась только для evaluation.",
        "",
        "## Vision fallback",
        "",
        f"TEXT calls: **{cost['TEXT']['model_calls']}**; VISION fallback calls: **{cost['VISION_FALLBACK']['model_calls']}**; "
        f"full-vision calls avoided: **{cost['delta_vs_previous']['vision_calls_avoided_vs_full_vision_arm']}**.",
        "Vision получал только renders fallback LEFT и их bounded RIGHT pages. Уже materialization-safe TEXT cases не переоткрывались.",
        "",
    ])
    for project in metrics["projects"]:
        reasons = fallback_reasons.get(project["pair_id"], {})
        lines.append(
            f"- {project['project']}: fallback LEFT {sorted(int(page) for page in reasons)}; "
            f"reason trace сохранён в `decisions.jsonl`."
        )
    lines.extend([
        "",
        "## Manual audit and safety",
        "",
        f"Auto-resolved: {manual['summary']['auto_resolved_decisions']}; supported: {manual['summary']['auto_supported']}; "
        f"partial: {manual['summary']['auto_partial']}; unsupported: {manual['summary']['unsupported_auto_matches']}. "
        f"Complex auto relations manually gated: {manual['summary']['complex_auto_unique_relations']} unique "
        f"({manual['summary']['complex_auto_relation_task_occurrences']} LEFT-task occurrences).",
        "",
        "Document Map Review не применял безусловное 1→1 assignment: legal MERGED/SPLIT/DISTRIBUTED доступны только как "
        "atomic prebuilt v4 groups. Конкурирующие undeclared uses одного RIGHT блокируются; NEW/REMOVED sheet не материализуется "
        "как NEW/REMOVED function. Конфликт с engineer mapping всегда остаётся HUMAN_REVIEW.",
        "",
        "## Stability",
        "",
        "Три независимых cold runs, в каждом byte-identical Pass A и Pass B. Автоматическое решение требует совпадения всех "
        "шести verified map selections. Exact overlap, map overlap, stable core, disagreement count и unstable relations находятся "
        "в `stability.json`.",
        "",
        "| Проект/mode | Stable core | Disagreement | Exact overlap range | Map overlap range |",
        "|---|---:|---:|---:|---:|",
    ])
    for record in stability["records"]:
        exact = [value["exact_decision_overlap"] for value in record["pairwise_overlap"]]
        map_overlap = [value["map_overlap"] for value in record["pairwise_overlap"]]
        lines.append(
            f"| {record['project']} {record['mode']} | {record['stable_task_count']} | "
            f"{record['disagreement_count']} | {min(exact) * 100:.1f}–{max(exact) * 100:.1f}% | "
            f"{min(map_overlap) * 100:.1f}–{max(map_overlap) * 100:.1f}% |"
        )
    lines.extend([
        "",
        "## Cost",
        "",
        f"Calls TEXT/VISION/total: **{cost['TEXT']['model_calls']}/{cost['VISION_FALLBACK']['model_calls']}/{cost['TOTAL']['model_calls']}**. "
        f"Tokens TEXT/VISION/total: **{cost['TEXT']['tokens_total']}/{cost['VISION_FALLBACK']['tokens_total']}/{cost['TOTAL']['tokens_total']}**. "
        f"Wall time: **{cost['wall_runtime_s']:.1f}s**. Tokens per stable auto decision: "
        f"**{cost['tokens_per_stable_auto_decision']}**.",
        "",
        f"Previous experiment: 36 calls, 6,261,720 tokens, 721.0s. Call delta: "
        f"{cost['delta_vs_previous']['model_calls']:+d}; token delta: {cost['delta_vs_previous']['tokens_total']}.",
        f"Vision scope: {cost['vision_scope']['fallback_left_tasks']}/{cost['vision_scope']['all_benchmark_left_tasks']} LEFT; "
        f"vision-token reduction: {cost['delta_vs_previous']['vision_tokens_reduction_fraction'] * 100:.1f}%; "
        f"tokens/stable-auto change vs old best arm: {cost['delta_vs_previous']['tokens_per_stable_auto_change_fraction'] * 100:+.1f}%.",
        f"Model responses succeeded: {cost['TOTAL']['successful_calls']}/{cost['TOTAL']['model_calls']}; "
        f"whole-map verifier accepted {cost['TOTAL']['verified_map_calls']}/{cost['TOTAL']['model_calls']} and rejected "
        f"{cost['TOTAL']['rejected_map_calls']} maps fail-closed.",
        "",
        "## Acceptance gates",
        "",
    ])
    for key, passed in metrics["acceptance_gates"].items():
        lines.append(f"- {'PASS' if passed else 'FAIL'} — `{key}`")
    lines.extend([
        "",
        "## Production safety",
        "",
        f"Frozen source artifacts unchanged: **{metrics['source_artifacts_unchanged']}**. Запись выполнялась только в research "
        "package/tests и `comparison/ai_sheet_matcher/20260902_v4_ai_repeat/`. Deploy и push не выполнялись.",
        "",
    ])
    return "\n".join(lines)


def finalize_records(
    output: Path,
    *,
    datasets: Sequence[V4SelectorDataset],
    records: Sequence[dict[str, Any]],
    wall_runtime_s: float,
) -> dict[str, Any]:
    text_decisions, _text_stability = _aggregate_text(datasets, records)
    fallback: dict[str, dict[int, list[str]]] = {
        dataset.selector.pair_id: _fallback_reasons(dataset, text_decisions)
        for dataset in datasets
    }
    vision_decisions: list[dict[str, Any]] = []
    for dataset in datasets:
        pages = sorted(fallback[dataset.selector.pair_id])
        if not pages:
            continue
        subset = subset_selector_dataset(dataset, pages)
        project_decisions, _ = aggregate_decisions(
            subset.selector, mode="VISION_FALLBACK", run_records=records,
        )
        vision_decisions.extend(project_decisions)
    combined = _combined_decisions(datasets, text_decisions, vision_decisions, fallback)
    all_decisions = [*text_decisions, *combined]
    manual = _manual_audit(datasets, combined)
    group_audit = _read_json(output / "group_audit.json")
    cost = _cost_analysis(records, combined, wall_runtime_s=wall_runtime_s)
    metrics = _metrics(datasets, text_decisions, combined, records, manual, group_audit, cost)
    stability_records = []
    for dataset in datasets:
        stability_records.append(_stability_from_decisions(
            text_decisions, project=dataset.selector.project,
            pair_id=dataset.selector.pair_id, mode="TEXT",
        ))
        stability_records.append(_stability_from_decisions(
            combined, project=dataset.selector.project,
            pair_id=dataset.selector.pair_id, mode="TEXT_VISION_FALLBACK",
        ))
    stability = {
        "kind": "v4_ai_repeat_stability",
        "schema_version": "stability.v2",
        "records": stability_records,
    }
    _write_jsonl(output / "model_runs.jsonl", records)
    _write_jsonl(output / "decisions.jsonl", all_decisions)
    _write_json(output / "stability.json", stability)
    _write_json(output / "manual_audit.json", manual)
    _write_json(output / "cost_analysis.json", cost)
    _write_json(output / "metrics.json", metrics)
    (output / "experiment_report.md").write_text(
        _report(metrics, all_decisions, fallback, group_audit, manual, cost, stability), encoding="utf-8",
    )
    (output / "README.md").write_text(
        _readme(complete=True, signatures=group_audit["input_signatures"]), encoding="utf-8",
    )
    return metrics


def run_experiment(output: Path, *, model: str, effort: str, workers: int) -> None:
    audit_path = output / "group_audit.json"
    if not audit_path.is_file():
        raise RuntimeError("group_audit.json is required; run candidate-audit before model calls")
    datasets = _load_datasets()
    audit = _read_json(audit_path)
    expected = {value.selector.pair_id: value.selector.input_signature for value in datasets}
    if audit.get("input_signatures") != expected:
        raise RuntimeError("group audit is stale; rerun candidate-audit before model calls")
    if audit.get("summary", {}).get("group_recall_after_shortlist") != 1.0:
        raise RuntimeError("group shortlist recall is below 100%; model calls are forbidden")

    wall_started = time.monotonic()
    text_jobs = []
    for dataset in datasets:
        prompt, payload = build_selector_prompt(dataset, mode="TEXT")
        for cold_run in (1, 2, 3):
            for pass_name in ("A", "B"):
                text_jobs.append((dataset, "TEXT", cold_run, pass_name, prompt, payload, [], []))
    print("starting", len(text_jobs), "TEXT calls")
    records = _run_jobs(text_jobs, model=model, effort=effort, workers=workers)
    records.sort(key=lambda row: (row["pair_id"], row["mode"], row["cold_run"], row["pass_name"]))
    _write_jsonl(output / "model_runs.jsonl", records)

    text_decisions, _ = _aggregate_text(datasets, records)
    fallback = {
        dataset.selector.pair_id: _fallback_reasons(dataset, text_decisions)
        for dataset in datasets
    }
    with tempfile.TemporaryDirectory(prefix="ai_sheet_matcher_v4_fallback_") as render_root_raw:
        render_root = Path(render_root_raw)
        vision_jobs = []
        for dataset in datasets:
            pages = sorted(fallback[dataset.selector.pair_id])
            print(dataset.selector.project, "fallback LEFT", pages)
            if not pages:
                continue
            subset = subset_selector_dataset(dataset, pages)
            images = render_images(subset.selector, render_root / subset.selector.pair_id)
            manifest = [path.name for path in images]
            prompt, payload = build_selector_prompt(
                subset, mode="VISION_FALLBACK", image_manifest=manifest,
            )
            for cold_run in (1, 2, 3):
                for pass_name in ("A", "B"):
                    vision_jobs.append((
                        subset, "VISION_FALLBACK", cold_run, pass_name,
                        prompt, payload, images, manifest,
                    ))
        print("starting", len(vision_jobs), "VISION fallback calls")
        records.extend(_run_jobs(
            vision_jobs, model=model, effort=effort, workers=workers,
        ))
    records.sort(key=lambda row: (row["pair_id"], row["mode"], row["cold_run"], row["pass_name"]))
    wall_runtime = time.monotonic() - wall_started
    metrics = finalize_records(
        output, datasets=datasets, records=records, wall_runtime_s=wall_runtime,
    )
    print("experiment complete", output)
    print("safety", metrics["safety_gate_passed"], "verdict", metrics["verdict"])


def finalize_existing(output: Path) -> None:
    runs_path = output / "model_runs.jsonl"
    if not runs_path.is_file() or not (output / "group_audit.json").is_file():
        raise RuntimeError("model_runs.jsonl and group_audit.json are required")
    datasets = _load_datasets()
    by_pair = {value.selector.pair_id: value for value in datasets}
    records = _read_jsonl(runs_path)
    for record in records:
        dataset = by_pair[str(record["pair_id"])]
        left_pages = [int(page) for page in record.get("left_pages") or []]
        selected = dataset if record["mode"] == "TEXT" else subset_selector_dataset(dataset, left_pages)
        record["verification"] = verify_v4_selector_response(
            selected, str(record["payload_signature"]), record.get("response"),
        )
    previous_cost = (
        _read_json(output / "cost_analysis.json") if (output / "cost_analysis.json").is_file() else {}
    )
    wall = float(previous_cost.get("wall_runtime_s") or 0.0)
    metrics = finalize_records(output, datasets=datasets, records=records, wall_runtime_s=wall)
    print("derived artifacts finalized without model calls", metrics["verdict"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("candidate-audit", "experiment", "finalize"))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--effort", default=DEFAULT_EFFORT)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    output = args.output.resolve()
    if args.phase == "candidate-audit":
        run_candidate_audit(output)
    elif args.phase == "experiment":
        run_experiment(output, model=args.model, effort=args.effort, workers=args.workers)
    else:
        finalize_existing(output)


if __name__ == "__main__":
    main()
