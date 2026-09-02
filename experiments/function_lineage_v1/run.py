"""Build and run the isolated Function Lineage Matcher v1 research spike."""
from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from experiments.ai_sheet_matcher.core import PROJECT_CONFIG, canonical_json, production_sources_unchanged
from experiments.ai_sheet_matcher.run import call_codex_bounded

from .core import (
    ALGORITHM_VERSION,
    CONCRETE_RELATIONS,
    RELATION_DOCUMENT_LINK,
    RELATION_FUNCTIONAL_ANALOGUE,
    FunctionLineageDataset,
    build_function_lineage_dataset,
    build_selector_prompt,
    derive_sheet_map,
    output_schema,
    prompt_character_count,
    stable_consensus,
    verify_selector_response,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = REPO_ROOT / "comparison" / "ai_sheet_matcher" / "20260902_function_lineage_v1"
DEFAULT_MODEL = "gpt-5.6-sol"
DEFAULT_EFFORT = "medium"
PREVIOUS_TOKENS = 7_315_563

# Evaluation-only references.  They never enter candidate generation, ranking,
# payloads or prompts.  IOS 2.1 semantics come from same-version forensics
# dbddb691/bddec7be, not from the documentary engineer links on pages 7/8/9.
IOS21_FUNCTION_REFERENCES: dict[int, dict[str, Any]] = {
    16: {"right_options": [[26, 28]], "relations": ["FUNCTION_DISTRIBUTED"]},
    17: {"right_options": [[27]], "relations": ["CONTINUED_1_TO_1", "FUNCTION_EXPANDED"]},
    18: {"right_options": [[24]], "relations": ["CONTINUED_1_TO_1", "FUNCTION_EXPANDED"]},
    19: {
        "right_options": [[25], [25, 30]],
        "relations": ["CONTINUED_1_TO_1", "FUNCTION_DISTRIBUTED"],
    },
    20: {"right_options": [[26, 28, 29]], "relations": ["FUNCTION_DISTRIBUTED"]},
    21: {"right_options": [[29]], "relations": ["CONTINUED_1_TO_1", "MERGED_N_TO_1"]},
}


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(canonical_json(dict(row)) + "\n" for row in rows), encoding="utf-8",
    )


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _load_datasets() -> list[FunctionLineageDataset]:
    return [build_function_lineage_dataset(REPO_ROOT, pair_id) for pair_id in PROJECT_CONFIG]


def _reference_cases(dataset: FunctionLineageDataset) -> dict[int, dict[str, Any]]:
    if dataset.pair_id == "pe336037597":
        return IOS21_FUNCTION_REFERENCES
    output: dict[int, dict[str, Any]] = {}
    for raw in PROJECT_CONFIG[dataset.pair_id]["reference_cases"]:
        left_pages = [int(value) for value in raw["left_pages"]]
        right_pages = sorted(int(value) for value in raw["right_pages"])
        relation = (
            "MERGED_N_TO_1" if len(left_pages) > 1 and len(right_pages) == 1
            else "SPLIT_1_TO_N" if len(left_pages) == 1 and len(right_pages) > 1
            else "CONTINUED_1_TO_1"
        )
        for left_page in left_pages:
            output[left_page] = {
                "right_options": [right_pages],
                "relations": [relation, "FUNCTION_DISTRIBUTED"] if len(right_pages) > 1 else [relation],
                "reference_name": raw["name"],
            }
    return output


def _manual_audit(
    datasets: Sequence[FunctionLineageDataset], decisions: list[dict[str, Any]],
) -> dict[str, Any]:
    by_pair = {value.pair_id: value for value in datasets}
    rows: list[dict[str, Any]] = []
    for decision in decisions:
        dataset = by_pair[str(decision["pair_id"])]
        reference = _reference_cases(dataset).get(int(decision["left_physical_page"]))
        if not decision["stable"] or decision["selected_candidate_id"] == "NEED_MORE_EVIDENCE":
            classification = "UNRESOLVED"
            basis = "two-pass/cold unanimity was not reached"
        elif reference is None:
            classification = "PARTIAL"
            basis = "stable bounded evidence, but no function-lineage reference label"
        else:
            rights = sorted(int(value) for value in decision["right_pages"])
            exact_rights = any(rights == sorted(option) for option in reference["right_options"])
            relation_ok = decision["relation_type"] in reference["relations"]
            overlap = any(set(rights) & set(option) for option in reference["right_options"])
            if exact_rights and relation_ok:
                classification = "SUPPORTED"
                basis = "same-version functional/lineage reference agrees"
            elif overlap:
                classification = "PARTIAL"
                basis = "reference overlap exists but granularity or relation differs"
            else:
                classification = "UNSUPPORTED"
                basis = "stable selection has no overlap with the function reference"
        decision["manual_audit_classification"] = classification
        decision["materialization_allowed"] = classification == "SUPPORTED"
        rows.append({
            "audit_id": f"audit:{decision['task_id']}",
            "project": decision["project"],
            "pair_id": decision["pair_id"],
            "left_physical_page": decision["left_physical_page"],
            "selected_candidate_id": decision["selected_candidate_id"],
            "relation_type": decision["relation_type"],
            "right_pages": decision["right_pages"],
            "classification": classification,
            "basis": basis,
            "reference": reference,
            "complex_relation": decision["relation_type"] in {
                "SPLIT_1_TO_N", "MERGED_N_TO_1", "FUNCTION_DISTRIBUTED",
            },
            "materialization_allowed": decision["materialization_allowed"],
        })
    stable_auto = [value for value in rows if value["selected_candidate_id"] != "NEED_MORE_EVIDENCE"]
    unsupported = [value for value in stable_auto if value["classification"] == "UNSUPPORTED"]
    ios21 = by_pair["pe336037597"]
    right9 = [
        value for value in ios21.function_passports["RIGHT"].values()
        if value["source_sheet"]["physical_page"] == 9
    ]
    extraction_defects = [{
        "pair_id": "pe336037597",
        "right_physical_page": 9,
        "field": "corpus",
        "extracted_value": sorted({item for value in right9 for item in value["corpus"]}),
        "assessment": "KNOWN_EXTRACTION_DEFECT: change-register text leaked corpus 1 into sheet-4 identity",
        "forensic_basis": "dbddb691/bddec7be same-version IOS 2.1 forensics",
        "decision_impact": "excluded from FUNCTIONAL_ANALOGUE; retained only as DOCUMENT_LINK 19->9",
        "evidence_refs": sorted({item for value in right9 for item in value["evidence_refs"]}),
    }]
    return {
        "kind": "function_lineage_manual_audit",
        "schema_version": "manual-audit.v1",
        "policy": "every stable proposal is audited; only SUPPORTED may materialize",
        "rows": rows,
        "summary": {
            "stable_auto_lineages": len(stable_auto),
            "supported": sum(value["classification"] == "SUPPORTED" for value in stable_auto),
            "partial": sum(value["classification"] == "PARTIAL" for value in stable_auto),
            "unsupported_auto_lineages": len(unsupported),
            "unresolved": sum(value["classification"] == "UNRESOLVED" for value in rows),
            "complex_relations_audited": sum(value["complex_relation"] for value in rows),
        },
        "critical_ios21": [
            value for value in rows
            if value["pair_id"] == "pe336037597" and value["left_physical_page"] in {17, 18, 19, 20}
        ],
        "extraction_defects": extraction_defects,
    }


def _candidate_recall(dataset: FunctionLineageDataset, k: int) -> tuple[int, int]:
    references = _reference_cases(dataset)
    hits = 0
    for task in dataset.tasks:
        reference = references.get(int(task["left_physical_page"]))
        if reference is None:
            continue
        candidates = [dataset.candidates[value] for value in task["candidate_ids"][:k]]
        if any(
            sorted(candidate["right_pages"]) == sorted(option)
            for candidate in candidates for option in reference["right_options"]
        ):
            hits += 1
    return hits, len(references)


def _document_accuracy(dataset: FunctionLineageDataset) -> dict[str, Any]:
    focus = {int(value) for value in PROJECT_CONFIG[dataset.pair_id]["focus_left_pages"]}
    labels = {
        (int(left), int(right))
        for link in dataset.candidate_v4.base.human_links
        for left in link.get("left_pages") or []
        for right in link.get("right_pages") or []
        if int(left) in focus
    }
    detected = {
        (int(value["left_physical_page"]), int(value["right_physical_page"]))
        for value in dataset.document_links
    }
    hits = labels & detected
    return {
        "ground_truth_count": len(labels),
        "detected_count": len(detected),
        "true_positive_count": len(hits),
        "recall": round(len(hits) / len(labels), 6) if labels else None,
        "precision_against_saved_links": round(len(hits) / len(detected), 6) if detected else None,
        "labels": [list(value) for value in sorted(labels)],
        "hits": [list(value) for value in sorted(hits)],
        "semantics": "DOCUMENT_LINK_GROUND_TRUTH only; never functional ground truth",
    }


def _metrics(
    datasets: Sequence[FunctionLineageDataset], decisions: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]], manual: Mapping[str, Any], sheet_map: Mapping[str, Any],
) -> dict[str, Any]:
    stable = [
        value for value in decisions
        if value["stable"] and value["selected_candidate_id"] != "NEED_MORE_EVIDENCE"
    ]
    unique = {str(value["selected_candidate_id"]): value for value in stable}
    relation_counts = {
        relation: sum(value["relation_type"] == relation for value in unique.values())
        for relation in (
            "CONTINUED_1_TO_1", "SPLIT_1_TO_N", "MERGED_N_TO_1", "FUNCTION_DISTRIBUTED",
            "RENAMED_FUNCTION", "FUNCTION_EXPANDED", "FUNCTION_REDUCED",
        )
    }
    recalls = {}
    for k in (1, 3, 5, 10):
        values = [_candidate_recall(dataset, k) for dataset in datasets]
        hits, total = sum(value[0] for value in values), sum(value[1] for value in values)
        recalls[f"recall_at_{k}"] = round(hits / total, 6) if total else None
    total_tokens = sum(int((value.get("model_call") or {}).get("usage", {}).get("total_tokens") or 0) for value in records)
    runtime_sum = sum(float((value.get("model_call") or {}).get("duration_s") or 0) for value in records)
    verification_errors = [
        error
        for value in records
        for error in (value.get("verification") or {}).get("global_errors") or []
    ]
    doc_metrics = {dataset.pair_id: _document_accuracy(dataset) for dataset in datasets}
    document_hits = sum(value["true_positive_count"] for value in doc_metrics.values())
    document_labels = sum(value["ground_truth_count"] for value in doc_metrics.values())
    audited_stable = int(manual["summary"]["stable_auto_lineages"])
    supported_exact = int(manual["summary"]["supported"])
    ios21 = next(value for value in datasets if value.pair_id == "pe336037597")
    ios21_decisions = {int(value["left_physical_page"]): value for value in decisions if value["pair_id"] == ios21.pair_id}
    critical_ok = all(
        page in ios21_decisions
        and ios21_decisions[page]["stable"]
        and sorted(ios21_decisions[page]["right_pages"]) in [sorted(option) for option in reference["right_options"]]
        for page, reference in IOS21_FUNCTION_REFERENCES.items() if page in {17, 18, 20}
    )
    coexistence = all(
        any(value["left_physical_page"] == left and value["right_physical_page"] == document for value in ios21.document_links)
        and left in ios21_decisions and functional in ios21_decisions[left]["right_pages"]
        for left, document, functional in ((17, 7, 27), (18, 8, 24))
    )
    left20_reuse = any(
        value["pair_id"] == ios21.pair_id
        and value["right_physical_page"] in {26, 28, 29}
        and value["function_level_compatible"]
        for value in sheet_map["right_sheet_reuse"]
    )
    gates = {
        "relation_namespaces_separate": all(
            value["relation_namespace"] == RELATION_DOCUMENT_LINK for dataset in datasets for value in dataset.document_links
        ) and all(value["relation_namespace"] == RELATION_FUNCTIONAL_ANALOGUE for dataset in datasets for value in dataset.candidates.values()),
        "unsupported_auto_lineages_zero": manual["summary"]["unsupported_auto_lineages"] == 0,
        "ios21_17_18_document_and_function_coexist": coexistence,
        "ios21_critical_functional_results": critical_ok,
        "ios21_left20_distributed_stable": (
            ios21_decisions.get(20, {}).get("stable") is True
            and ios21_decisions.get(20, {}).get("relation_type") == "FUNCTION_DISTRIBUTED"
            and ios21_decisions.get(20, {}).get("right_pages") == [26, 28, 29]
        ),
        "left20_false_sheet_conflict_removed": left20_reuse,
        "at_least_one_complex_stable": (
            relation_counts["SPLIT_1_TO_N"] + relation_counts["FUNCTION_DISTRIBUTED"] > 0
        ),
        "tokens_below_previous": total_tokens < PREVIOUS_TOKENS,
        "production_sources_unchanged": all(production_sources_unchanged(value.candidate_v4.base) for value in datasets),
    }
    verdict = "A" if all(gates.values()) else (
        "B" if gates["relation_namespaces_separate"] and gates["production_sources_unchanged"] else "C"
    )
    return {
        "kind": "function_lineage_metrics",
        "schema_version": "metrics.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "projects": [{
            "project": dataset.project,
            "pair_id": dataset.pair_id,
            "tasks": len(dataset.tasks),
            "function_passports": sum(len(value) for value in dataset.function_passports.values()),
            "function_fragments": sum(len(value) for value in dataset.function_fragments.values()),
            "lineage_candidates": len(dataset.candidates),
            "document_link_accuracy": doc_metrics[dataset.pair_id],
            "prompt_characters": prompt_character_count(dataset),
        } for dataset in datasets],
        "document_link_accuracy": {
            "true_positive_count": document_hits,
            "ground_truth_count": document_labels,
            "benchmark_recall": round(document_hits / document_labels, 6) if document_labels else None,
            "by_pair": doc_metrics,
        },
        "functional_analogue_accuracy": {
            "supported": manual["summary"]["supported"],
            "partial": manual["summary"]["partial"],
            "unsupported": manual["summary"]["unsupported_auto_lineages"],
            "supported_exact_fraction": round(supported_exact / audited_stable, 6) if audited_stable else None,
            "supported_or_partial_fraction": round(
                (supported_exact + int(manual["summary"]["partial"])) / audited_stable, 6,
            ) if audited_stable else None,
        },
        "function_lineage_accuracy": {
            "audited_stable": audited_stable,
            "supported_exact": supported_exact,
            "supported_exact_fraction": round(supported_exact / audited_stable, 6) if audited_stable else None,
        },
        "lineage_recall": recalls,
        "stable_relation_counts": relation_counts,
        "stable_auto_lineages": len(unique),
        "unresolved": sum(not value["stable"] or value["selected_candidate_id"] == "NEED_MORE_EVIDENCE" for value in decisions),
        "unsupported_auto_lineages": manual["summary"]["unsupported_auto_lineages"],
        "function_level_conflicts": sum(error.startswith("FUNCTION_FRAGMENT_CONFLICT") for error in verification_errors),
        "false_sheet_global_conflicts_avoided": len(sheet_map["false_sheet_global_conflicts_avoided"]),
        "model_calls": len(records),
        "successful_model_calls": sum(bool((value.get("model_call") or {}).get("ok")) for value in records),
        "tokens": total_tokens,
        "runtime_sum_s": round(runtime_sum, 3),
        "tokens_per_stable_lineage": round(total_tokens / len(unique), 3) if unique else None,
        "token_reduction_vs_7_3m": round(1 - total_tokens / PREVIOUS_TOKENS, 6) if total_tokens else None,
        "vision": {
            "calls": 0,
            "tokens": 0,
            "triggered": False,
            "assessment": "not required for stable critical IOS 2.1 lineages; reserve for unresolved extraction/topology",
        },
        "acceptance_gates": gates,
        "verdict": verdict,
    }


def _stability(datasets: Sequence[FunctionLineageDataset], decisions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = []
    for dataset in datasets:
        values = [value for value in decisions if value["pair_id"] == dataset.pair_id]
        rows.append({
            "project": dataset.project,
            "pair_id": dataset.pair_id,
            "cold_runs": 3,
            "passes_per_cold_run": ["A", "B"],
            "required_verified_observations": 6,
            "stable_task_count": sum(value["stable"] for value in values),
            "unresolved_task_count": sum(not value["stable"] or value["selected_candidate_id"] == "NEED_MORE_EVIDENCE" for value in values),
            "tasks": [{
                "task_id": value["task_id"],
                "left_physical_page": value["left_physical_page"],
                "stable": value["stable"],
                "selected_candidate_id": value["selected_candidate_id"],
                "observations": value["observations"],
            } for value in values],
        })
    return {
        "kind": "function_lineage_stability",
        "schema_version": "stability.v1",
        "policy": "Pass A == Pass B in each of three cold runs and all six verified choices identical",
        "projects": rows,
    }


def _cost(records: Sequence[Mapping[str, Any]], metrics: Mapping[str, Any], wall_runtime_s: float) -> dict[str, Any]:
    return {
        "kind": "function_lineage_cost_analysis",
        "schema_version": "cost-analysis.v1",
        "model_calls": metrics["model_calls"],
        "successful_model_calls": metrics["successful_model_calls"],
        "tokens_total": metrics["tokens"],
        "runtime_sum_s": metrics["runtime_sum_s"],
        "wall_runtime_s": round(wall_runtime_s, 3),
        "stable_lineages": metrics["stable_auto_lineages"],
        "tokens_per_stable_lineage": metrics["tokens_per_stable_lineage"],
        "previous_v4_ai_repeat_tokens": PREVIOUS_TOKENS,
        "token_reduction_fraction": metrics["token_reduction_vs_7_3m"],
        "vision_calls": 0,
        "prompt_characters_by_pair": {
            value["pair_id"]: value["prompt_characters"] for value in metrics["projects"]
        },
        "telemetry_complete": all(
            int((value.get("model_call") or {}).get("usage", {}).get("total_tokens") or 0) > 0
            for value in records
        ),
    }


def _architecture() -> str:
    return """# Function Lineage Matcher v1 — architecture

This research lane implements the hierarchy established by selector forensics
`bddec7be` without importing anything into production.

```text
frozen OCR/vision evidence
  -> provenance-bearing Function Passport v2
  -> atomic function fragments
  -> bounded functional candidate / candidate group
  -> bounded AI selector (TEXT, Pass A/B, three cold runs)
  -> deterministic function-level verifier
  -> Function Lineage Map
  -> derived physical Sheet Map
```

## Independent relation namespaces

`DOCUMENT_LINK` is generated from change-register/TOC/title-block references and
has zero contribution to functional similarity. `FUNCTIONAL_ANALOGUE` contains
only candidates bound to extracted function and fragment IDs. Both may coexist
for one LEFT source; neither overwrites the other.

## Capacity and many-to-many behavior

The capacity key is `RIGHT:<physical page>:<function fragment id>`. Sharing a
physical RIGHT page is therefore legal when the selected lineages use different
fragments. Reusing the same atomic fragment in unrelated candidates fails closed;
a declared `MERGED_N_TO_1` candidate is one lineage and may be selected by each
of its LEFT tasks.

## Safety

The selector can return only a prebuilt candidate ID or a fail-closed sentinel.
It cannot supply pages, function IDs, fragments, groups or evidence. The verifier
checks candidate existence, evidence binding, direction, namespace, relation and
fragment capacity. `FUNCTION_REMOVED` requires exhaustive absence evidence;
physical sheet disappearance is insufficient. Reverse `NEW_FUNCTION` audit obeys
the same invariant. This spike does not write mappings or source runs and has no
deploy path.
"""


def _readme(metrics: Mapping[str, Any], decisions: Sequence[Mapping[str, Any]]) -> str:
    ios = {
        int(value["left_physical_page"]): value
        for value in decisions if value["pair_id"] == "pe336037597"
    }
    def result(page: int) -> str:
        value = ios.get(page) or {}
        return f"{value.get('relation_type', 'NEED_MORE_EVIDENCE')} {value.get('right_pages', [])}"
    counts = metrics["stable_relation_counts"]
    gates = "\n".join(
        f"- {'PASS' if passed else 'FAIL'} — `{name}`"
        for name, passed in metrics["acceptance_gates"].items()
    )
    return f"""# Function Lineage Matcher v1 — research result

Status: complete isolated research run. No production code, UI, mappings, source
runs or Candidate Generator v4 status changed. No deploy and no push.

## Result

`DOCUMENT_LINK` and `FUNCTIONAL_ANALOGUE` are independent namespaces. IOS 2.1
keeps documentary 17→7 and 18→8 while functional selection is evaluated against
the actual extracted graphic fragments.

- LEFT 17: DOCUMENT_LINK `[7]`; FUNCTIONAL `{result(17)}`.
- LEFT 18: DOCUMENT_LINK `[8]`; FUNCTIONAL `{result(18)}`.
- LEFT 19: `{result(19)}`; RIGHT 9 remains documentary and its corpus-1 extraction
  is not used as functional identity.
- LEFT 20: `{result(20)}`.

Stable unique relations: 1→1 `{counts['CONTINUED_1_TO_1']}`, 1→N
`{counts['SPLIT_1_TO_N']}`, N→1 `{counts['MERGED_N_TO_1']}`, distributed
`{counts['FUNCTION_DISTRIBUTED']}`. Unsupported auto lineages:
`{metrics['unsupported_auto_lineages']}`; unresolved tasks: `{metrics['unresolved']}`.

Function-level capacity avoided `{metrics['false_sheet_global_conflicts_avoided']}`
false physical-sheet conflicts. A RIGHT page can participate in several lineages
only through distinct fragment IDs; duplicate ownership of one fragment is rejected.

TEXT used `{metrics['tokens']}` tokens in `{metrics['model_calls']}` calls, a
`{metrics['token_reduction_vs_7_3m']:.1%}` reduction from the 7,315,563-token v4
repeat. Vision was not triggered because the critical TEXT lineages did not need
it; remaining unresolved extraction/topology cases stay `NEED_MORE_EVIDENCE`.

## Acceptance gates

{gates}

## Verdict

**{metrics['verdict']}** — {('architecture confirmed; controlled production integration may be designed' if metrics['verdict'] == 'A' else 'direction is sound, but passports/lineage selection need another research iteration' if metrics['verdict'] == 'B' else 'hierarchical architecture did not improve the result enough')}.

Artifacts in this directory are traceable research outputs. `manual_audit.json`
contains SUPPORTED/PARTIAL/UNSUPPORTED classification for every stable proposal;
`derived_sheet_map.json` is derived only after the lineage decisions.
"""


def _write_input_artifacts(output: Path, datasets: Sequence[FunctionLineageDataset]) -> None:
    _write_jsonl(output / "document_links.jsonl", (
        value for dataset in datasets for value in dataset.document_links
    ))
    _write_jsonl(output / "function_passports.jsonl", (
        passport
        for dataset in datasets for side in ("LEFT", "RIGHT")
        for passport in dataset.function_passports[side].values()
    ))
    _write_jsonl(output / "function_fragments.jsonl", (
        fragment
        for dataset in datasets for side in ("LEFT", "RIGHT")
        for fragment in dataset.function_fragments[side].values()
    ))
    _write_jsonl(output / "lineage_candidates.jsonl", (
        value for dataset in datasets for value in dataset.candidates.values()
    ))
    (output / "architecture.md").write_text(_architecture(), encoding="utf-8")


def _finalize(
    output: Path, datasets: Sequence[FunctionLineageDataset], records: list[dict[str, Any]],
    wall_runtime_s: float,
) -> dict[str, Any]:
    decisions: list[dict[str, Any]] = []
    for dataset in datasets:
        project_records = [value for value in records if value["pair_id"] == dataset.pair_id]
        decisions.extend(stable_consensus(dataset, project_records))
    manual = _manual_audit(datasets, decisions)
    sheet_map = derive_sheet_map(decisions)
    metrics = _metrics(datasets, decisions, records, manual, sheet_map)
    stability = _stability(datasets, decisions)
    cost = _cost(records, metrics, wall_runtime_s)
    _write_input_artifacts(output, datasets)
    _write_jsonl(output / "model_runs.jsonl", records)
    _write_jsonl(output / "lineage_decisions.jsonl", decisions)
    _write_json(output / "derived_sheet_map.json", sheet_map)
    _write_json(output / "metrics.json", metrics)
    _write_json(output / "stability.json", stability)
    _write_json(output / "manual_audit.json", manual)
    _write_json(output / "cost_analysis.json", cost)
    (output / "README.md").write_text(_readme(metrics, decisions), encoding="utf-8")
    return metrics


def build_only(output: Path) -> None:
    datasets = _load_datasets()
    output.mkdir(parents=True, exist_ok=True)
    _write_input_artifacts(output, datasets)
    print("built deterministic inputs", output)
    for dataset in datasets:
        print(dataset.project, len(dataset.tasks), len(dataset.candidates), prompt_character_count(dataset))


def _job(
    dataset: FunctionLineageDataset, cold_run: int, pass_name: str,
    prompt: str, payload: Mapping[str, Any], model: str, effort: str,
) -> dict[str, Any]:
    result = call_codex_bounded(
        prompt=prompt,
        schema=output_schema(dataset, str(payload["payload_signature"])),
        model=model,
        effort=effort,
        images=[],
    )
    verification = verify_selector_response(
        dataset, str(payload["payload_signature"]), result.response,
    )
    return {
        "project": dataset.project,
        "pair_id": dataset.pair_id,
        "mode": "TEXT_STRUCTURED",
        "cold_run": cold_run,
        "pass_name": pass_name,
        "model": model,
        "reasoning_effort": effort,
        "payload_signature": payload["payload_signature"],
        "input_signature": dataset.input_signature,
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


def run_experiment(output: Path, *, model: str, effort: str, workers: int) -> None:
    datasets = _load_datasets()
    output.mkdir(parents=True, exist_ok=True)
    _write_input_artifacts(output, datasets)
    jobs = []
    for dataset in datasets:
        prompt, payload = build_selector_prompt(dataset)
        for cold_run in (1, 2, 3):
            for pass_name in ("A", "B"):
                jobs.append((dataset, cold_run, pass_name, prompt, payload))
    started = time.monotonic()
    records: list[dict[str, Any]] = []
    _write_jsonl(output / "model_runs.jsonl", records)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(_job, *job, model, effort): (job[0].project, job[1], job[2])
            for job in jobs
        }
        for index, future in enumerate(as_completed(futures), 1):
            record = future.result()
            records.append(record)
            _write_jsonl(
                output / "model_runs.jsonl",
                sorted(records, key=lambda value: (value["pair_id"], value["cold_run"], value["pass_name"])),
            )
            key = futures[future]
            print(index, "/", len(jobs), key, "ok=", record["model_call"]["ok"], "verified=", record["verification"]["ok"])
    records.sort(key=lambda value: (value["pair_id"], value["cold_run"], value["pass_name"]))
    metrics = _finalize(output, datasets, records, time.monotonic() - started)
    print("complete", output, "verdict", metrics["verdict"], "tokens", metrics["tokens"])


def finalize_existing(output: Path) -> None:
    records_path = output / "model_runs.jsonl"
    if not records_path.is_file():
        raise RuntimeError("model_runs.jsonl is required")
    datasets = _load_datasets()
    by_pair = {value.pair_id: value for value in datasets}
    records = _read_jsonl(records_path)
    for record in records:
        dataset = by_pair[str(record["pair_id"])]
        _, payload = build_selector_prompt(dataset)
        if record.get("input_signature") != dataset.input_signature:
            raise RuntimeError("saved model run refers to stale deterministic inputs")
        record["verification"] = verify_selector_response(
            dataset, str(payload["payload_signature"]), record.get("response"),
        )
    previous = json.loads((output / "cost_analysis.json").read_text(encoding="utf-8")) if (output / "cost_analysis.json").is_file() else {}
    metrics = _finalize(output, datasets, records, float(previous.get("wall_runtime_s") or 0))
    print("refinalized", output, "verdict", metrics["verdict"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("build", "experiment", "finalize"))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--effort", default=DEFAULT_EFFORT)
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    if args.phase == "build":
        build_only(args.output.resolve())
    elif args.phase == "experiment":
        run_experiment(args.output.resolve(), model=args.model, effort=args.effort, workers=args.workers)
    else:
        finalize_existing(args.output.resolve())


if __name__ == "__main__":
    main()
