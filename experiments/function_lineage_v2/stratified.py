"""Run Function Lineage v2.5 stratified scoped corpus evaluation.

The experiment consumes the frozen v2.4 Function Scope Graph and v2.4.1
scoped transport.  It deterministically selects 36 non-sentinel tasks, keeps
the seven v2.4.2 controls separate, freezes every model payload before the
first call, and never writes production state.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from backend.app.services.stage_comparison.ai import gateway as ai_gateway
from backend.app.services.stage_comparison.ai import response_contract
from backend.app.services.stage_comparison.ai import settings as ai_settings
from backend.app.services.stage_comparison.production_artifacts import (
    canonical_json,
    content_signature,
    stable_id,
)
from experiments.ai_sheet_matcher.core import PROJECT_CONFIG
from experiments.function_lineage_v2 import scoped_smoke
from experiments.function_lineage_v2 import scoped_transport
from experiments.function_lineage_v2 import smoke as base_smoke
from experiments.function_lineage_v2 import transport


REPO_ROOT = Path(__file__).resolve().parents[2]
SCOPE_COMMIT = "6d2e7a5e4710765f0b5b8450c73c31431e070d13"
SCOPED_TRANSPORT_COMMIT = "edcaea0b997330b744f2c479783b9c3ced5e29ae"
CRITICAL_SMOKE_COMMIT = "f25d3bc7966a3c0b9c588c866049034eb774a5d1"
PRODUCTION_HEAD = "4d489bf9033ad40c40099fe5e1436493bc56c0ed"
PRODUCTION_RELEASE = "ui-real-4d489bf9"
PASSES = ("A", "B")
NEW_COLD_RUNS = (1, 2, 3)
SENTINEL_COLD_RUNS = (1,)
SAMPLE_SIZE_PER_CORPUS = 12
SAMPLE_SIZE = 36
PLAUSIBLE_SCORE_MARGIN = 0.01
CLOSE_SCORE_GAP = 0.005
LOW_EVIDENCE_MAX_CHANNELS = 3
LARGE_INVENTORY_MIN = 10
SELECTION_SALT = "function-lineage-v2.5-stratified-sample-v1"

PAIR_PROJECTS = {
    "p19cd7f695a": "IOS1.1",
    "pe336037597": "IOS2.1",
    "pb02de74a81": "IOS3.1",
}
PROJECT_PAIRS = {project: pair_id for pair_id, project in PAIR_PROJECTS.items()}
CORPUS_ORDER = ("IOS1.1", "IOS2.1", "IOS3.1")
SENTINELS = {
    label: {"task_id": task_id, "scope_id": scope_id}
    for label, (task_id, scope_id) in scoped_smoke.TASKS.items()
}
SENTINEL_IDS = frozenset(value["task_id"] for value in SENTINELS.values())

COMPARISON_ROOT = REPO_ROOT / "comparison" / "ai_sheet_matcher"
SCOPE_ROOT = COMPARISON_ROOT / "20260903_function_lineage_v2_4_scope_graph"
TRANSPORT_ROOT = COMPARISON_ROOT / "20260903_function_lineage_v2_4_1_scoped_transport"
SENTINEL_ROOT = COMPARISON_ROOT / "20260903_function_lineage_v2_4_2_ios21_critical_scoped_smoke"
CANDIDATE_ROOT = COMPARISON_ROOT / "20260903_function_lineage_deterministic" / "candidate_artifacts"
DEFAULT_OUTPUT = COMPARISON_ROOT / "20260903_function_lineage_v2_5_stratified_scoped_evaluation"

SOURCE_HASHES = {
    "comparison/ai_sheet_matcher/20260903_function_lineage_v2_4_scope_graph/selector_tasks_scoped.json": "58432c19caa92b01e26e52d366a0344e1a63768b39f27ae83639d078ce2fa1af",
    "comparison/ai_sheet_matcher/20260903_function_lineage_v2_4_scope_graph/group_derivability_audit.json": "9bd14758895dca67f1397f56f8149f94362fc6c2a70c0cbe54f863c4eca10812",
    "comparison/ai_sheet_matcher/20260903_function_lineage_v2_4_scope_graph/scope_metrics.json": "23dce5a1f23b33c509b6e6b9015369011e3d7acfe56d27a16152f605d1f3e4a4",
    "comparison/ai_sheet_matcher/20260903_function_lineage_v2_4_1_scoped_transport/scoped_selector_shards.jsonl": "e9f5757d3e3a0d60bf29f547fd65b28e90323c1be4923ec26d69c3b7f66a011f",
    "comparison/ai_sheet_matcher/20260903_function_lineage_v2_4_1_scoped_transport/scoped_selector_transport_metrics.json": "51d1ac2fe65e71328b2d96d7e44beea49a0e493bbbae89a92c54138b0ede0872",
    "comparison/ai_sheet_matcher/20260903_function_lineage_v2_4_2_ios21_critical_scoped_smoke/task_results.json": "4b003340c84be8df70f91b70dd4896c449917a1b812093c238c05fc4cd166560",
    "comparison/ai_sheet_matcher/20260903_function_lineage_v2_4_2_ios21_critical_scoped_smoke/metrics.json": "7f4318f5f04f581096a520494092183859e4859358f0bac85dd1ebef297e65dd",
    "comparison/ai_sheet_matcher/20260903_function_lineage_deterministic/candidate_artifacts/p19cd7f695a.json": "b709d4715cbfd234efcf9251dc263c9f4260f2f85a685aaad4f0c1e78ab407ca",
    "comparison/ai_sheet_matcher/20260903_function_lineage_deterministic/candidate_artifacts/pe336037597.json": "fff15f4e711c209d10b4940c2e19f1d6d8120a40d0243e1781d0ad758472039d",
    "comparison/ai_sheet_matcher/20260903_function_lineage_deterministic/candidate_artifacts/pb02de74a81.json": "ac27f287dd1e5b912df07afdf2046ec33d383c9e870be63ed05de6f3feba12b3",
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

STRATA = {
    "A": "simple same-scope CONTINUED_1_TO_1 with one score-plausible candidate",
    "B": "same-scope ambiguity with at least two score-plausible eligible candidates",
    "C": "eligible SPLIT_1_TO_N candidate",
    "D": "eligible MERGED_N_TO_1 candidate",
    "E": "eligible FUNCTION_DISTRIBUTED candidate",
    "F": "eligible EXACT_CHILD_UNION group candidate",
    "G": "eligible NON_DECOMPOSABLE_GROUP candidate",
    "H": "low evidence: top candidate has at most three matched functional channels",
    "I": "one RIGHT physical page appears with distinct exact fragment IDs in the task inventory",
    "J": "research-reference target candidate absent from old Sheet Matcher edges",
    "K": "large candidate inventory (at least ten)",
    "L": "adjacent deterministic source scores differ by at most 0.005",
}

MODEL_CONFIGURATION = copy.deepcopy(scoped_smoke.MODEL_CONFIGURATION)
MODEL_CONFIGURATION.update({
    "cold_runs": list(NEW_COLD_RUNS),
    "sentinel_cold_runs": list(SENTINEL_COLD_RUNS),
    "workers": 4,
})

VERDICT_THRESHOLDS = {
    "a_overall_stable_3_of_3_min": 0.90,
    "a_cross_cold_exact_consistency_min": 0.85,
    "a_group_stable_3_of_3_min": 0.80,
    "c_authoritative_min_cases": 5,
    "c_authoritative_alignment_min": 0.80,
}

DEPENDENCY_FILES = (
    Path(__file__),
    Path(scoped_smoke.__file__),
    Path(scoped_transport.__file__),
    Path(transport.__file__),
    Path(base_smoke.__file__),
    Path(lineage.__file__),
    REPO_ROOT / "experiments" / "ai_sheet_matcher" / "core.py",
)


def _read_json(path: Path) -> dict[str, Any]:
    return base_smoke._read_json(path)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return base_smoke._read_jsonl(path)


def _write_json(path: Path, value: Any) -> None:
    base_smoke._write_json(path, value)


def _write_jsonl(path: Path, values: Iterable[Mapping[str, Any]]) -> None:
    base_smoke._write_jsonl(path, values)


def _sha_file(path: Path) -> str:
    return base_smoke._sha_file(path)


def _sha_json(value: Any) -> str:
    return base_smoke._sha_json(value)


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _jsonl_bytes(values: Iterable[Mapping[str, Any]]) -> bytes:
    return b"".join((canonical_json(dict(value)) + "\n").encode("utf-8") for value in values)


def _display_path(path: Path) -> str:
    return base_smoke._display_path(path)


def _dependency_hashes() -> dict[str, str]:
    return {_display_path(path): _sha_file(path) for path in DEPENDENCY_FILES}


def _selection_hash(task_id: str) -> str:
    return hashlib.sha256(f"{SELECTION_SALT}|{task_id}".encode("utf-8")).hexdigest()


def _percentile(values: Sequence[int], percentile: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, (percentile * len(ordered) + 99) // 100 - 1)]


def _assert_sources() -> None:
    for commit in (SCOPE_COMMIT, SCOPED_TRANSPORT_COMMIT, CRITICAL_SMOKE_COMMIT):
        if not base_smoke._git_quiet("merge-base", "--is-ancestor", commit, "HEAD"):
            raise RuntimeError(f"research source is not an ancestor: {commit}")
    paths = [REPO_ROOT / name for name in SOURCE_HASHES]
    relative = [_display_path(path) for path in paths]
    if not base_smoke._git_quiet("diff", "--quiet", CRITICAL_SMOKE_COMMIT, "--", *relative):
        raise RuntimeError("tracked frozen research sources changed after v2.4.2")
    if not base_smoke._git_quiet("diff", "--quiet", "HEAD", "--", *relative):
        raise RuntimeError("working-tree frozen research source is modified")
    actual = {_display_path(path): _sha_file(path) for path in paths}
    if actual != SOURCE_HASHES:
        raise RuntimeError(f"frozen source SHA-256 drift: {actual}")


def _load_sources() -> dict[str, Any]:
    _assert_sources()
    tasks = _read_json(SCOPE_ROOT / "selector_tasks_scoped.json")
    groups = _read_json(SCOPE_ROOT / "group_derivability_audit.json")
    scope_metrics = _read_json(SCOPE_ROOT / "scope_metrics.json")
    transport_metrics = _read_json(TRANSPORT_ROOT / "scoped_selector_transport_metrics.json")
    previous_tasks = _read_json(SENTINEL_ROOT / "task_results.json")
    contexts: dict[str, dict[str, Any]] = {}
    for shard in _read_jsonl(TRANSPORT_ROOT / "scoped_selector_shards.jsonl"):
        for context in shard["model_payload"]["task_contexts"]:
            task_id = str(context["task_id"])
            if task_id in contexts:
                raise RuntimeError(f"duplicate frozen task context: {task_id}")
            contexts[task_id] = copy.deepcopy(context)
    raw = {
        pair_id: _read_json(CANDIDATE_ROOT / f"{pair_id}.json")
        for pair_id in PAIR_PROJECTS
    }
    task_ids = {str(value["scoped_task_id"]) for value in tasks["tasks"]}
    if tasks.get("scoped_task_count") != 213 or set(contexts) != task_ids:
        raise RuntimeError("frozen scoped population is not exactly 213 task contexts")
    if scope_metrics["overall"]["raw_candidate_recall"] != {
        "case_count": 19, **EXPECTED_RECALL["raw_candidate_recall"]
    }:
        raise RuntimeError("RAW recall baseline changed")
    if scope_metrics["overall"]["scope_eligible_recall"] != {
        "case_count": 19, **EXPECTED_RECALL["scope_eligible_recall"]
    }:
        raise RuntimeError("SCOPE-ELIGIBLE recall baseline changed")
    if transport_metrics["recall"]["expected_baselines"] != EXPECTED_RECALL:
        raise RuntimeError("scoped transport recall baseline changed")
    return {
        "tasks": tasks,
        "groups": groups,
        "scope_metrics": scope_metrics,
        "transport_metrics": transport_metrics,
        "previous_tasks": previous_tasks,
        "contexts": contexts,
        "raw": raw,
    }


def _reference_rows(
    task: Mapping[str, Any], candidates: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    pair_id = str(task["pair_id"])
    for index, reference in enumerate(PROJECT_CONFIG[pair_id]["reference_cases"], 1):
        candidate_ids = []
        absent_ids = []
        for candidate_id in task["candidate_ids"]:
            candidate = candidates[str(candidate_id)]
            if sorted(candidate.get("left_pages") or []) != sorted(reference["left_pages"]):
                continue
            mode = str(reference.get("expected_mode") or "ALL")
            right_pages = set(int(value) for value in candidate.get("right_pages") or [])
            expected_pages = set(int(value) for value in reference["right_pages"])
            matches = right_pages == expected_pages if mode == "ALL" else bool(right_pages & expected_pages)
            if not matches:
                continue
            candidate_ids.append(str(candidate_id))
            if not bool((candidate.get("document_context") or {}).get("sheet_matcher_edge_present")):
                absent_ids.append(str(candidate_id))
        if candidate_ids:
            rows.append({
                "reference_id": stable_id("flref_", pair_id, index, reference),
                "reference_class": "RESEARCH_REFERENCE",
                "name": str(reference["name"]),
                "expected_mode": str(reference.get("expected_mode") or "ALL"),
                "left_pages": sorted(int(value) for value in reference["left_pages"]),
                "right_pages": sorted(int(value) for value in reference["right_pages"]),
                "candidate_ids": sorted(candidate_ids),
                "old_sheet_matcher_edge_absent_candidate_ids": sorted(absent_ids),
                "authoritative_truth": False,
            })
    return rows


def _candidate_summary(
    context_candidate: Mapping[str, Any], raw_candidate: Mapping[str, Any],
    group: Mapping[str, Any] | None, local_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    evidence_ids = list(context_candidate.get("evidence_ids") or [])
    missing_evidence = sorted(set(evidence_ids) - set(local_evidence))
    if missing_evidence:
        raise RuntimeError(f"candidate evidence escaped task context: {missing_evidence}")
    return {
        "candidate_id": str(context_candidate["candidate_id"]),
        "rank": int(context_candidate["rank"]),
        "relation_type": str(context_candidate["relation_type"]),
        "source_score": float(raw_candidate.get("source_score") or 0.0),
        "functional_score": float(raw_candidate.get("functional_score") or 0.0),
        "right_physical_pages": list(context_candidate.get("right_physical_pages") or []),
        "right_fragment_ids": list(context_candidate.get("right_fragment_ids") or []),
        "right_function_ids": [
            str(value["function_id"])
            for value in context_candidate.get("right_functions") or []
        ],
        "capacity_keys": list(context_candidate.get("capacity_keys") or []),
        "component_mapping": copy.deepcopy(list(context_candidate.get("component_map") or [])),
        "matched_functional_channels": list(context_candidate.get("matched_functional_channels") or []),
        "missing_evidence_channels": list(context_candidate.get("missing_evidence_channels") or []),
        "evidence_ids": evidence_ids,
        "evidence_sha256": _sha_json({evidence_id: local_evidence[evidence_id] for evidence_id in evidence_ids}),
        "sheet_matcher_edge_present": bool((raw_candidate.get("document_context") or {}).get("sheet_matcher_edge_present")),
        "group_derivability": group.get("classification") if group else None,
        "child_candidate_ids": list(group.get("child_candidate_ids") or []) if group else [],
    }


def build_population(sources: Mapping[str, Any]) -> dict[str, Any]:
    group_by_candidate = {
        str(value["candidate_id"]): value for value in sources["groups"]["groups"]
    }
    raw_candidates = {
        pair_id: {
            str(value["candidate_id"]): value
            for value in artifact["functional_candidates"]
        }
        for pair_id, artifact in sources["raw"].items()
    }
    rows = []
    for task in sources["tasks"]["tasks"]:
        task_id = str(task["scoped_task_id"])
        context = sources["contexts"][task_id]
        pair_id = str(task["pair_id"])
        candidate_rows = []
        for context_candidate in context["functional_candidates"]:
            candidate_id = str(context_candidate["candidate_id"])
            candidate_rows.append(_candidate_summary(
                context_candidate,
                raw_candidates[pair_id][candidate_id],
                group_by_candidate.get(candidate_id),
                context["local_evidence"],
            ))
        scores = [float(value["source_score"]) for value in candidate_rows]
        plausible_ids = [
            value["candidate_id"] for value in candidate_rows
            if scores[0] - float(value["source_score"]) <= PLAUSIBLE_SCORE_MARGIN + 1e-12
        ]
        adjacent_gaps = [
            round(abs(scores[index] - scores[index + 1]), 8)
            for index in range(len(scores) - 1)
        ]
        relation_types = sorted({value["relation_type"] for value in candidate_rows})
        derivability = sorted({
            str(value["group_derivability"])
            for value in candidate_rows if value["group_derivability"]
        })
        fragments_by_page: dict[int, set[str]] = defaultdict(set)
        for candidate in candidate_rows:
            for mapping in candidate["component_mapping"]:
                fragments_by_page[int(mapping["right_physical_page"])].add(str(mapping["right_fragment_id"]))
        shared_fragment_pages = [
            {"right_physical_page": page, "right_fragment_ids": sorted(fragments)}
            for page, fragments in sorted(fragments_by_page.items()) if len(fragments) >= 2
        ]
        references = _reference_rows(task, raw_candidates[pair_id])
        strata_reasons: dict[str, str] = {}
        if len(plausible_ids) == 1 and candidate_rows[0]["relation_type"] == "CONTINUED_1_TO_1":
            strata_reasons["A"] = f"one candidate within {PLAUSIBLE_SCORE_MARGIN:.3f} of the best score; leader is CONTINUED_1_TO_1"
        if len(plausible_ids) >= 2:
            strata_reasons["B"] = f"{len(plausible_ids)} candidates within {PLAUSIBLE_SCORE_MARGIN:.3f} of the best score"
        relation_to_stratum = {
            "SPLIT_1_TO_N": "C",
            "MERGED_N_TO_1": "D",
            "FUNCTION_DISTRIBUTED": "E",
        }
        for relation, stratum in relation_to_stratum.items():
            count = sum(value["relation_type"] == relation for value in candidate_rows)
            if count:
                strata_reasons[stratum] = f"{count} eligible {relation} candidate(s)"
        if "EXACT_CHILD_UNION" in derivability:
            strata_reasons["F"] = "at least one eligible group is EXACT_CHILD_UNION"
        if "NON_DECOMPOSABLE_GROUP" in derivability:
            strata_reasons["G"] = "at least one eligible group is NON_DECOMPOSABLE_GROUP"
        top_channels = len(candidate_rows[0]["matched_functional_channels"])
        if top_channels <= LOW_EVIDENCE_MAX_CHANNELS:
            strata_reasons["H"] = f"top candidate has {top_channels} matched functional evidence channels"
        if shared_fragment_pages:
            strata_reasons["I"] = f"{len(shared_fragment_pages)} RIGHT page(s) carry distinct exact fragments in this inventory"
        absent_reference_ids = sorted({
            candidate_id for reference in references
            for candidate_id in reference["old_sheet_matcher_edge_absent_candidate_ids"]
        })
        if absent_reference_ids:
            strata_reasons["J"] = f"{len(absent_reference_ids)} research-reference target candidate(s) absent from old Sheet Matcher edges"
        if len(candidate_rows) >= LARGE_INVENTORY_MIN:
            strata_reasons["K"] = f"candidate inventory has {len(candidate_rows)} entries"
        min_gap = min(adjacent_gaps, default=None)
        if min_gap is not None and min_gap <= CLOSE_SCORE_GAP + 1e-12:
            strata_reasons["L"] = f"minimum adjacent source-score gap is {min_gap:.8f}"
        rows.append({
            "task_id": task_id,
            "scope_id": str(task["coverage_scope_id"]),
            "pair_id": pair_id,
            "corpus": PAIR_PROJECTS[pair_id],
            "scope_kind": str(task["scope_kind"]),
            "source_task_ids": list(task["source_task_ids"]),
            "required_component_ids": list(task["required_component_ids"]),
            "candidate_count": len(candidate_rows),
            "candidate_ids": [value["candidate_id"] for value in candidate_rows],
            "candidates": candidate_rows,
            "plausible_candidate_ids": plausible_ids,
            "relation_types": relation_types,
            "group_derivability_classes": derivability,
            "minimum_adjacent_source_score_gap": min_gap,
            "top_matched_functional_channel_count": top_channels,
            "same_right_page_distinct_fragments": shared_fragment_pages,
            "references": references,
            "reference_classes": sorted({value["reference_class"] for value in references}) or ["NO_REFERENCE"],
            "strata": sorted(strata_reasons),
            "strata_reasons": strata_reasons,
            "selection_hash": _selection_hash(task_id),
            "sentinel": task_id in SENTINEL_IDS,
            "sentinel_label": next((label for label, value in SENTINELS.items() if value["task_id"] == task_id), None),
            "frozen_context_sha256": _sha_json(context),
        })
    rows.sort(key=lambda value: value["task_id"])
    counts = {
        corpus: {
            stratum: sum(stratum in row["strata"] for row in rows if row["corpus"] == corpus and not row["sentinel"])
            for stratum in STRATA
        }
        for corpus in CORPUS_ORDER
    }
    return {
        "kind": "function_lineage_v2_5_stratified_population",
        "schema_version": "function-lineage-stratified-population.v2.5",
        "population_size": len(rows),
        "new_sample_eligible_size": sum(not value["sentinel"] for value in rows),
        "corpus_sizes": dict(sorted(Counter(value["corpus"] for value in rows).items())),
        "sentinel_count": len(SENTINEL_IDS),
        "strata": STRATA,
        "label_thresholds": {
            "plausible_score_margin": PLAUSIBLE_SCORE_MARGIN,
            "close_score_gap": CLOSE_SCORE_GAP,
            "low_evidence_max_channels": LOW_EVIDENCE_MAX_CHANNELS,
            "large_inventory_min": LARGE_INVENTORY_MIN,
        },
        "stratum_availability_excluding_sentinels": counts,
        "reference_taxonomy": {
            "AUTHORITATIVE_FUNCTIONAL_REFERENCE": "none exists in the frozen corpus",
            "RESEARCH_REFERENCE": "PROJECT_CONFIG functional hypotheses; never authoritative truth",
            "DOCUMENT_LINK": "frozen Sheet Matcher links; never functional truth",
            "NO_REFERENCE": "safety, stability, and evidence only",
        },
        "selection_prohibitions": {
            "filename_used": False,
            "page_number_used_as_order_or_hash": False,
            "manual_task_choice": False,
        },
        "tasks": rows,
    }


def _bundle_candidates(
    corpus_rows: Sequence[Mapping[str, Any]], candidate_to_task: Mapping[str, str],
    row_by_id: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    bundles = []
    for row in corpus_rows:
        for candidate in row["candidates"]:
            if candidate["group_derivability"] != "EXACT_CHILD_UNION":
                continue
            child_task_ids = [candidate_to_task[value] for value in candidate["child_candidate_ids"]]
            task_ids = sorted({row["task_id"], *child_task_ids})
            if any(row_by_id[value]["sentinel"] for value in task_ids):
                continue
            if any(row_by_id[value]["corpus"] != row["corpus"] for value in task_ids):
                raise RuntimeError("EXACT_CHILD_UNION child escaped its corpus")
            bundles.append({
                "parent_task_id": row["task_id"],
                "parent_candidate_id": candidate["candidate_id"],
                "parent_candidate_rank": candidate["rank"],
                "child_task_ids": sorted(child_task_ids),
                "child_candidate_ids": sorted(candidate["child_candidate_ids"]),
                "task_ids": task_ids,
                "selection_key": [
                    candidate["rank"] != 1,
                    len(task_ids),
                    row["candidate_count"],
                    _selection_hash(candidate["candidate_id"]),
                ],
            })
    return sorted(bundles, key=lambda value: value["selection_key"])


def select_sample(population: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(population["tasks"])
    row_by_id = {str(value["task_id"]): value for value in rows}
    candidate_to_task: dict[str, str] = {}
    for row in rows:
        for candidate_id in row["candidate_ids"]:
            if candidate_id in candidate_to_task:
                raise RuntimeError(f"candidate is selectable in multiple exact scopes: {candidate_id}")
            candidate_to_task[candidate_id] = row["task_id"]
    selected_rows = []
    bundles = []
    selection_trace = []
    for corpus in CORPUS_ORDER:
        eligible = [value for value in rows if value["corpus"] == corpus and not value["sentinel"]]
        corpus_bundles = _bundle_candidates(eligible, candidate_to_task, row_by_id)
        if not corpus_bundles:
            raise RuntimeError(f"no closed EXACT_CHILD_UNION bundle for {corpus}")
        bundle = corpus_bundles[0]
        selected_ids = set(bundle["task_ids"])
        covered = {
            stratum for task_id in selected_ids for stratum in row_by_id[task_id]["strata"]
        }
        bundles.append(bundle)
        for task_id in sorted(selected_ids, key=_selection_hash):
            selection_trace.append({
                "corpus": corpus,
                "task_id": task_id,
                "phase": "EXACT_CHILD_UNION_CLOSED_BUNDLE",
                "new_strata": sorted(set(row_by_id[task_id]["strata"]) - (covered - set(row_by_id[task_id]["strata"]))),
            })
        while len(selected_ids) < SAMPLE_SIZE_PER_CORPUS:
            candidates = [value for value in eligible if value["task_id"] not in selected_ids]
            availability = {
                stratum: sum(stratum in value["strata"] for value in eligible)
                for stratum in STRATA
            }

            def key(row: Mapping[str, Any]) -> tuple[Any, ...]:
                new = set(row["strata"]) - covered
                rarity_gain = sum(1 / availability[value] for value in new)
                return (-len(new), -rarity_gain, -len(row["strata"]), row["selection_hash"])

            chosen = min(candidates, key=key)
            new_strata = sorted(set(chosen["strata"]) - covered)
            selected_ids.add(chosen["task_id"])
            covered.update(chosen["strata"])
            selection_trace.append({
                "corpus": corpus,
                "task_id": chosen["task_id"],
                "phase": "GREEDY_RARE_MULTI_LABEL_COVERAGE" if new_strata else "HASHED_FILL",
                "new_strata": new_strata,
            })
        available = {stratum for value in eligible for stratum in value["strata"]}
        if not available.issubset(covered):
            raise RuntimeError(f"sample misses available strata in {corpus}: {sorted(available - covered)}")
        selected_rows.extend(sorted((row_by_id[value] for value in selected_ids), key=lambda value: value["selection_hash"]))
    if len(selected_rows) != SAMPLE_SIZE or len({value["task_id"] for value in selected_rows}) != SAMPLE_SIZE:
        raise RuntimeError("stratified sample is not exactly 36 unique tasks")
    coverage = {
        stratum: {
            "eligible_population": sum(stratum in value["strata"] and not value["sentinel"] for value in rows),
            "selected_tasks": sum(stratum in value["strata"] for value in selected_rows),
            "covered": any(stratum in value["strata"] for value in selected_rows),
        }
        for stratum in STRATA
    }
    return {
        "kind": "function_lineage_v2_5_stratified_sample",
        "schema_version": "function-lineage-stratified-sample.v2.5",
        "selection_algorithm": {
            "name": "closed-child-bundle then greedy rare multi-label coverage with salted SHA-256 tie-break",
            "version": "function-lineage-v2.5-stratified-sample-v1",
            "salt": SELECTION_SALT,
            "corpus_quota": SAMPLE_SIZE_PER_CORPUS,
            "page_or_filename_selection": False,
            "manual_selection": False,
            "exact_child_union_policy": "reserve the best deterministic closed parent+children bundle per corpus",
            "bundle_order": "rank-1 first, fewer tasks, smaller parent inventory, salted candidate hash",
            "greedy_order": "new-label count, inverse-frequency rarity gain, total labels, salted task hash",
        },
        "sample_size": SAMPLE_SIZE,
        "sample_size_by_corpus": dict(sorted(Counter(value["corpus"] for value in selected_rows).items())),
        "redistribution": None,
        "selected_task_ids": [value["task_id"] for value in selected_rows],
        "selected_tasks": [{
            "task_id": value["task_id"],
            "scope_id": value["scope_id"],
            "corpus": value["corpus"],
            "strata": value["strata"],
            "selection_reason": value["strata_reasons"],
            "selection_hash": value["selection_hash"],
            "frozen_context_sha256": value["frozen_context_sha256"],
        } for value in selected_rows],
        "exact_child_union_closed_bundles": bundles,
        "stratum_coverage": coverage,
        "selection_trace": selection_trace,
        "sentinels_excluded_from_headline": [
            {"label": label, **value} for label, value in SENTINELS.items()
        ],
    }


def _smoke_context(source: Mapping[str, Any]) -> dict[str, Any]:
    context = copy.deepcopy(dict(source))
    candidate_ids = [str(value["candidate_id"]) for value in context["functional_candidates"]]
    context["allowed_decisions"] = [*candidate_ids, lineage.NEED_MORE_EVIDENCE]
    context["scope_policy"]["function_removed_selectable"] = False
    context.pop("task_context_signature", None)
    context["task_context_signature"] = content_signature(context)
    return context


def _payload(
    *, pair_id: str, eval_set: str, input_signature: str,
    contexts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    task_ids = [str(value["task_id"]) for value in contexts]
    payload = {
        "schema_version": "function-lineage-stratified-scoped-evaluation.v2.5",
        "transport_algorithm": "function-lineage-stratified-scoped-evaluation.v2.5",
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
    payload["shard_id"] = stable_id("fs25_", pair_id, eval_set, task_ids)
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
) -> list[dict[str, Any]]:
    shards: list[dict[str, Any]] = []
    current: list[Mapping[str, Any]] = []

    def characters(values: Sequence[Mapping[str, Any]]) -> int:
        return len(_prompt(_payload(
            pair_id=pair_id, eval_set=eval_set,
            input_signature=input_signature, contexts=values,
        ), "A"))

    def emit(values: Sequence[Mapping[str, Any]]) -> None:
        payload = _payload(
            pair_id=pair_id, eval_set=eval_set,
            input_signature=input_signature, contexts=values,
        )
        prompts = {name: _prompt(payload, name) for name in PASSES}
        prompt_characters = max(len(value) for value in prompts.values())
        if prompt_characters > scoped_transport.HARD_CHARACTERS:
            raise RuntimeError(f"selected task exceeds hard character gate: {payload['shard_id']}")
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
            raise RuntimeError(f"atomic selected task exceeds hard gate: {context['task_id']}")
        proposed = [*current, context]
        if current and characters(proposed) > scoped_transport.TARGET_CHARACTERS:
            emit(current)
            current = [context]
        else:
            current = proposed
    if current:
        emit(current)
    return shards


def _datasets(
    sources: Mapping[str, Any], shards: Sequence[Mapping[str, Any]],
) -> dict[str, lineage.FunctionLineageDataset]:
    contexts_by_pair: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for shard in shards:
        for context in shard["model_payload"]["task_contexts"]:
            contexts_by_pair[str(shard["pair_id"])][str(context["task_id"])] = context
    return {
        pair_id: scoped_smoke._synthetic_dataset(
            sources["raw"][pair_id], list(contexts_by_pair[pair_id].values())
        )
        for pair_id in contexts_by_pair
    }


def _preflight(
    sources: Mapping[str, Any], shards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    datasets = _datasets(sources, shards)
    failures = []
    cross_scope_rejection_tests = 0
    unknown_id_rejection_tests = 0
    for shard in shards:
        payload = shard["model_payload"]
        valid = {"results": [
            {"task_id": context["task_id"], "decision": lineage.NEED_MORE_EVIDENCE}
            for context in payload["task_contexts"]
        ]}
        parsed = scoped_transport.verify_scoped_transport_response(payload, valid)
        contract_errors = response_contract.validate(valid, shard["output_schema"])
        verifier = scoped_smoke._task_local_verifier(
            datasets[str(shard["pair_id"])], str(payload["payload_signature"]), valid["results"]
        )
        if not parsed["ok"] or contract_errors or not verifier["ok"]:
            failures.append({
                "shard_id": shard["shard_id"],
                "parser": parsed,
                "schema_contract_errors": contract_errors,
                "verifier": verifier,
            })
        unknown = copy.deepcopy(valid)
        unknown["results"][0]["decision"] = "lcand_UNKNOWN_FAIL_CLOSED"
        unknown_id_rejection_tests += 1
        if scoped_transport.verify_scoped_transport_response(payload, unknown)["ok"]:
            failures.append({"shard_id": shard["shard_id"], "error": "UNKNOWN_CANDIDATE_ACCEPTED"})
        task_contexts = payload["task_contexts"]
        if len(task_contexts) >= 2:
            foreign = copy.deepcopy(valid)
            foreign["results"][0]["decision"] = task_contexts[1]["functional_candidates"][0]["candidate_id"]
            if foreign["results"][0]["decision"] not in task_contexts[0]["allowed_decisions"]:
                cross_scope_rejection_tests += 1
                if scoped_transport.verify_scoped_transport_response(payload, foreign)["ok"]:
                    failures.append({"shard_id": shard["shard_id"], "error": "CROSS_SCOPE_CANDIDATE_ACCEPTED"})
        for context in task_contexts:
            if any(value.get("scope_relation") != "EXACT_SCOPE" for value in context["functional_candidates"]):
                failures.append({"task_id": context["task_id"], "error": "CROSS_GRANULARITY_SELECTABLE_CANDIDATE"})
            if lineage.FUNCTION_REMOVED in context["allowed_decisions"]:
                failures.append({"task_id": context["task_id"], "error": "FUNCTION_REMOVED_SELECTABLE"})
    transport_safety = sources["transport_metrics"]["safety"]
    deterministic_safety = {
        "raw_candidate_count": sources["transport_metrics"]["raw_candidate_count"],
        "forensically_preserved_raw_candidate_count": sources["transport_metrics"]["forensically_preserved_raw_candidate_count"],
        "cross_granularity_selectable_competition": transport_safety["cross_granularity_selectable_competition"],
        "RIGHT_MAP_CONFLICT": transport_safety["RIGHT_MAP_CONFLICT"],
        "capacity_defect_count": transport_safety["capacity_defect_count"],
        "candidate_partition_defect_count": transport_safety["candidate_partition_defect_count"],
        "unknown_scope_policy": "FAIL_CLOSED",
        "raw_candidates_preserved": (
            sources["transport_metrics"]["raw_candidate_count"]
            == sources["transport_metrics"]["forensically_preserved_raw_candidate_count"]
        ),
    }
    if deterministic_safety != {
        "raw_candidate_count": 1461,
        "forensically_preserved_raw_candidate_count": 1461,
        "cross_granularity_selectable_competition": 0,
        "RIGHT_MAP_CONFLICT": 0,
        "capacity_defect_count": 0,
        "candidate_partition_defect_count": 0,
        "unknown_scope_policy": "FAIL_CLOSED",
        "raw_candidates_preserved": True,
    }:
        failures.append({"error": "DETERMINISTIC_SCOPE_OR_CAPACITY_BASELINE_DRIFT", "value": deterministic_safety})
    return {
        "ok": not failures,
        "failures": failures,
        "provider_schema_problem_count": sum(len(value["provider_safe_schema_problems"]) for value in shards),
        "provider_schema_contains_oneOf": any("oneOf" in canonical_json(value["output_schema"]) for value in shards),
        "unknown_candidate_rejection_tests": unknown_id_rejection_tests,
        "cross_scope_candidate_rejection_tests": cross_scope_rejection_tests,
        "deterministic_safety": deterministic_safety,
        "recall": EXPECTED_RECALL,
    }


def build_frozen_objects() -> dict[str, Any]:
    sources = _load_sources()
    population = build_population(sources)
    sample = select_sample(population)
    row_by_id = {value["task_id"]: value for value in population["tasks"]}
    new_ids = set(sample["selected_task_ids"])
    shards = []
    for eval_set, ids in (("NEW_SAMPLE", new_ids), ("SENTINEL", set(SENTINEL_IDS))):
        for corpus in CORPUS_ORDER:
            pair_id = PROJECT_PAIRS[corpus]
            ordered_ids = [
                value["task_id"] for value in sorted(
                    (row_by_id[task_id] for task_id in ids if row_by_id[task_id]["corpus"] == corpus),
                    key=lambda value: value["selection_hash"],
                )
            ]
            if not ordered_ids:
                continue
            contexts = [_smoke_context(sources["contexts"][task_id]) for task_id in ordered_ids]
            shards.extend(_build_shards(
                pair_id=pair_id,
                eval_set=eval_set,
                input_signature=str(sources["raw"][pair_id]["input_signature"]),
                contexts=contexts,
            ))
    shards.sort(key=lambda value: (value["evaluation_set"], value["corpus"], value["shard_id"]))
    preflight = _preflight(sources, shards)
    if not preflight["ok"]:
        raise RuntimeError(f"stratified experiment preflight failed: {preflight['failures']}")
    return {
        "sources": sources,
        "population": population,
        "sample": sample,
        "shards": shards,
        "preflight": preflight,
    }


def _disclosure(objects: Mapping[str, Any], model_inputs_sha256: str) -> dict[str, Any]:
    shards = objects["shards"]
    new_shards = [value for value in shards if value["evaluation_set"] == "NEW_SAMPLE"]
    sentinel_shards = [value for value in shards if value["evaluation_set"] == "SENTINEL"]
    return {
        "kind": "external_codex_disclosure",
        "schema_version": "function-lineage-external-disclosure.v2.5",
        "destination": "external subscription Codex CLI",
        "model": MODEL_CONFIGURATION["model"],
        "reasoning_effort": MODEL_CONFIGURATION["reasoning_effort"],
        "content": "frozen FunctionScope facts, candidate metadata, OCR/text-derived evidence, and task-local identifiers",
        "images": [],
        "vision": False,
        "new_task_count": SAMPLE_SIZE,
        "sentinel_task_count": len(SENTINEL_IDS),
        "task_ids": {
            "NEW_SAMPLE": list(objects["sample"]["selected_task_ids"]),
            "SENTINEL": [value["task_id"] for value in SENTINELS.values()],
        },
        "model_inputs_sha256": model_inputs_sha256,
        "population_sha256": hashlib.sha256(_json_bytes(objects["population"])).hexdigest(),
        "sample_sha256": hashlib.sha256(_json_bytes(objects["sample"])).hexdigest(),
        "planned_requests": len(new_shards) * len(PASSES) * len(NEW_COLD_RUNS) + len(sentinel_shards) * len(PASSES) * len(SENTINEL_COLD_RUNS),
        "model_shards": len(shards),
        "prompt_characters": {
            "median": statistics.median(value["prompt_characters"] for value in shards),
            "p95": _percentile([value["prompt_characters"] for value in shards], 95),
            "max": max(value["prompt_characters"] for value in shards),
        },
        "confirmation_required": True,
    }


def prepare(output: Path) -> dict[str, Any]:
    if output.exists():
        raise RuntimeError(f"refusing to replace immutable experiment directory: {output}")
    flags = base_smoke._assert_isolated_flags()
    runtime = ai_gateway.validate_runtime(require_vision=False, deep=False, mode=ai_settings.MODE_OFF)
    if not runtime.get("ok"):
        raise RuntimeError(f"isolated model runtime preflight failed: {runtime['problems']}")
    first = build_frozen_objects()
    second = build_frozen_objects()
    replay_names = ("population", "sample", "shards")
    replay_hashes = {}
    for name in replay_names:
        encoder = _jsonl_bytes if name == "shards" else _json_bytes
        first_bytes = encoder(first[name])
        second_bytes = encoder(second[name])
        if first_bytes != second_bytes:
            raise RuntimeError(f"independent deterministic replay differs: {name}")
        replay_hashes[name] = hashlib.sha256(first_bytes).hexdigest()
    output.mkdir(parents=True)
    population_path = output / "stratified_population.json"
    sample_path = output / "stratified_sample.json"
    inputs_path = output / "model_inputs.jsonl"
    _write_json(population_path, first["population"])
    _write_json(sample_path, first["sample"])
    _write_jsonl(inputs_path, first["shards"])
    disclosure = _disclosure(first, _sha_file(inputs_path))
    disclosure_path = output / "external_codex_disclosure.json"
    _write_json(disclosure_path, disclosure)
    disclosure_sha256 = _sha_file(disclosure_path)
    new_shards = [value for value in first["shards"] if value["evaluation_set"] == "NEW_SAMPLE"]
    sentinel_shards = [value for value in first["shards"] if value["evaluation_set"] == "SENTINEL"]
    manifest = {
        "kind": "function_lineage_v2_5_stratified_scoped_evaluation_input",
        "schema_version": "function-lineage-stratified-input.v2.5",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_chain": [SCOPE_COMMIT, SCOPED_TRANSPORT_COMMIT, CRITICAL_SMOKE_COMMIT],
        "checkout_head_before_calls": base_smoke._git("rev-parse", "HEAD"),
        "origin_main_observed_before_calls": base_smoke._git("rev-parse", "origin/main"),
        "production_head_at_start": PRODUCTION_HEAD,
        "production_release_at_start": PRODUCTION_RELEASE,
        "source_sha256": SOURCE_HASHES,
        "dependency_sha256": _dependency_hashes(),
        "population_path": _display_path(population_path),
        "population_sha256": _sha_file(population_path),
        "sample_path": _display_path(sample_path),
        "sample_sha256": _sha_file(sample_path),
        "model_inputs_path": _display_path(inputs_path),
        "model_inputs_sha256": _sha_file(inputs_path),
        "external_codex_disclosure_path": _display_path(disclosure_path),
        "external_codex_disclosure_sha256": disclosure_sha256,
        "deterministic_sample_replay_count": 2,
        "deterministic_sample_replay_byte_identical": True,
        "deterministic_replay_sha256": replay_hashes,
        "model_configuration": MODEL_CONFIGURATION,
        "model_configuration_sha256": _sha_json(MODEL_CONFIGURATION),
        "prompt_template_sha256": _sha_json(scoped_smoke.PROMPT_LINES),
        "verdict_thresholds": VERDICT_THRESHOLDS,
        "preflight": first["preflight"],
        "runtime_flags": flags,
        "runtime_preflight": runtime,
        "new_sample_shard_count": len(new_shards),
        "sentinel_shard_count": len(sentinel_shards),
        "planned_requests": disclosure["planned_requests"],
        "request_attempts_at_freeze": 0,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    }
    _write_json(output / "input_manifest.json", manifest)
    return manifest


def _validate_prepared(
    output: Path,
) -> tuple[
    dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]],
    dict[str, lineage.FunctionLineageDataset],
]:
    manifest = _read_json(output / "input_manifest.json")
    population = _read_json(output / "stratified_population.json")
    sample = _read_json(output / "stratified_sample.json")
    shards = _read_jsonl(output / "model_inputs.jsonl")
    if _dependency_hashes() != manifest["dependency_sha256"]:
        raise RuntimeError("harness, prompt, parser, verifier, or reference configuration changed after sample freeze")
    checks = {
        "population_sha256": output / "stratified_population.json",
        "sample_sha256": output / "stratified_sample.json",
        "model_inputs_sha256": output / "model_inputs.jsonl",
        "external_codex_disclosure_sha256": output / "external_codex_disclosure.json",
    }
    for key, path in checks.items():
        if _sha_file(path) != manifest[key]:
            raise RuntimeError(f"frozen experiment artifact changed: {path.name}")
    if base_smoke._flags() != manifest["runtime_flags"] or any(base_smoke._flags().values()):
        raise RuntimeError("Function Lineage runtime flags changed after sample freeze")
    rebuilt = build_frozen_objects()
    if _json_bytes(rebuilt["population"]) != _json_bytes(population):
        raise RuntimeError("frozen population does not replay from pinned inputs")
    if _json_bytes(rebuilt["sample"]) != _json_bytes(sample):
        raise RuntimeError("frozen sample does not replay from pinned inputs")
    if _jsonl_bytes(rebuilt["shards"]) != _jsonl_bytes(shards):
        raise RuntimeError("frozen model inputs do not replay from pinned inputs")
    return manifest, population, sample, shards, _datasets(rebuilt["sources"], shards)


def _model_job(
    shard: Mapping[str, Any], *, cold_run: int, pass_name: str,
    manifest: Mapping[str, Any], experiment_id: str,
    datasets: Mapping[str, lineage.FunctionLineageDataset],
) -> dict[str, Any]:
    payload = shard["model_payload"]
    prompt = _prompt(payload, pass_name)
    prompt_hash = base_smoke._sha_bytes(prompt.encode("utf-8"))
    if prompt_hash != shard[f"prompt_{pass_name.lower()}_sha256"]:
        raise RuntimeError(f"prepared prompt drift: {shard['shard_id']}/{pass_name}")
    config = manifest["model_configuration"]
    call = ai_gateway.call(
        ai_settings.CODEX_SESSION,
        prompt,
        model=str(config["model"]),
        reasoning_level=str(config["reasoning_effort"]),
        schema=copy.deepcopy(dict(shard["output_schema"])),
        images=(),
        retries=int(config["retries"]),
        timeout_s=int(config["timeout_seconds"]),
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
        "model_configuration_sha256": manifest["model_configuration_sha256"],
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


def _mark_capacity(
    records: Sequence[dict[str, Any]], *, errors: Sequence[str],
    decisions: Mapping[str, str], scenario: str,
) -> None:
    affected_candidates = {
        candidate_id for candidate_id in decisions.values()
        if any(candidate_id in error for error in errors)
    }
    for record in records:
        task_results = {
            task_id: {
                "ok": decisions[task_id] not in affected_candidates,
                "candidate_id": decisions[task_id],
                "errors": [error for error in errors if decisions[task_id] in error],
            }
            for task_id in record["task_ids"]
        }
        record["capacity_verification"] = {
            "applicable": True,
            "ok": all(value["ok"] for value in task_results.values()),
            "task_results": task_results,
            "errors": sorted(set(errors)),
            "scenario": scenario,
            "cross_granularity_scenarios_mixed": False,
        }


def _apply_sample_capacity(
    records: Sequence[dict[str, Any]], dataset: lineage.FunctionLineageDataset,
    population_rows: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    if any(not value["model_call"]["ok"] or not value["transport_verification"]["ok"] for value in records):
        for record in records:
            record["capacity_verification"] = {
                "applicable": False,
                "ok": None,
                "task_results": {},
                "errors": [],
                "reason": "INCOMPLETE_OR_INVALID_BATCH",
            }
        return []
    decisions = {
        str(result["task_id"]): str(result["decision"])
        for record in records for result in record["response"]["results"]
    }
    errors = []
    task_ids = sorted(decisions)
    for task_id in task_ids:
        errors.extend(lineage.verify_capacity(
            [{"task_id": task_id, "candidate_id": decisions[task_id]}],
            dataset.candidates,
        ))
    for index, left_id in enumerate(task_ids):
        left_components = set(population_rows[left_id]["required_component_ids"])
        for right_id in task_ids[index + 1:]:
            right_components = set(population_rows[right_id]["required_component_ids"])
            if left_components & right_components:
                continue
            errors.extend(lineage.verify_capacity([
                {"task_id": left_id, "candidate_id": decisions[left_id]},
                {"task_id": right_id, "candidate_id": decisions[right_id]},
            ], dataset.candidates))
    unique = sorted(set(str(value) for value in errors))
    _mark_capacity(
        records, errors=unique, decisions=decisions,
        scenario="PAIRWISE_DISJOINT_SOURCE_SCOPES_PLUS_TASK_LOCAL",
    )
    return unique


def _apply_capacity(
    records: Sequence[dict[str, Any]], *, evaluation_set: str,
    datasets: Mapping[str, lineage.FunctionLineageDataset],
    population: Mapping[str, Any],
) -> list[str]:
    rows = {str(value["task_id"]): value for value in population["tasks"]}
    all_errors = []
    for pair_id in sorted({str(value["pair_id"]) for value in records}):
        pair_records = [value for value in records if value["pair_id"] == pair_id]
        if evaluation_set == "SENTINEL":
            all_errors.extend(scoped_smoke._apply_capacity(pair_records, datasets[pair_id]))
        else:
            all_errors.extend(_apply_sample_capacity(pair_records, datasets[pair_id], rows))
    return sorted(set(all_errors))


def _freeze_drift(output: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    code_changed = _dependency_hashes() != manifest["dependency_sha256"]
    input_changed = False
    for key, name in (
        ("population_sha256", "stratified_population.json"),
        ("sample_sha256", "stratified_sample.json"),
        ("model_inputs_sha256", "model_inputs.jsonl"),
        ("external_codex_disclosure_sha256", "external_codex_disclosure.json"),
    ):
        input_changed |= _sha_file(output / name) != manifest[key]
    try:
        _assert_sources()
    except RuntimeError:
        input_changed = True
    return {"code_changed": code_changed, "input_changed": input_changed}


def experiment(
    output: Path, *, confirm_external_codex: bool,
    confirm_disclosure_sha256: str | None,
) -> list[dict[str, Any]]:
    records_path = output / "model_runs.jsonl"
    if records_path.exists():
        raise RuntimeError(f"refusing to repeat or append model observations: {records_path}")
    manifest, population, _sample, shards, datasets = _validate_prepared(output)
    expected_disclosure = str(manifest["external_codex_disclosure_sha256"])
    if not confirm_external_codex or confirm_disclosure_sha256 != expected_disclosure:
        raise RuntimeError(
            "external Codex confirmation missing or disclosure SHA-256 mismatch; "
            f"review {manifest['external_codex_disclosure_path']} and confirm {expected_disclosure}"
        )
    experiment_id = "flv2.5-stratified-scoped-" + uuid.uuid4().hex
    records: list[dict[str, Any]] = []
    started = time.monotonic()
    stopped = False
    stop_reason = None
    code_changed_after_first_call = False
    input_changed_after_first_call = False
    phases = (("NEW_SAMPLE", NEW_COLD_RUNS), ("SENTINEL", SENTINEL_COLD_RUNS))
    for evaluation_set, cold_runs in phases:
        selected_shards = [value for value in shards if value["evaluation_set"] == evaluation_set]
        for cold_run in cold_runs:
            if stopped:
                break
            for pass_name in PASSES:
                batch = []
                with ThreadPoolExecutor(max_workers=int(manifest["model_configuration"]["workers"])) as pool:
                    futures = [pool.submit(
                        _model_job, shard, cold_run=cold_run, pass_name=pass_name,
                        manifest=manifest, experiment_id=experiment_id, datasets=datasets,
                    ) for shard in selected_shards]
                    try:
                        for future in as_completed(futures):
                            batch.append(future.result())
                    except Exception:
                        ai_gateway.kill_live_processes(experiment_id)
                        raise
                batch.sort(key=lambda value: (value["corpus"], value["shard_id"]))
                capacity_errors = _apply_capacity(
                    batch, evaluation_set=evaluation_set,
                    datasets=datasets, population=population,
                )
                records.extend(batch)
                records.sort(key=lambda value: (
                    value["evaluation_set"], int(value["cold_run"]),
                    value["pass_name"], value["corpus"], value["shard_id"],
                ))
                _write_jsonl(records_path, records)
                print(
                    f"{len(records)}/{manifest['planned_requests']} set={evaluation_set} "
                    f"cold={cold_run} pass={pass_name} "
                    f"model_ok={sum(value['model_call']['ok'] for value in batch)}/{len(batch)} "
                    f"capacity_errors={len(capacity_errors)}",
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
                drift = _freeze_drift(output, manifest)
                code_changed_after_first_call |= drift["code_changed"]
                input_changed_after_first_call |= drift["input_changed"]
                if any(drift.values()):
                    stopped = True
                    stop_reason = "FROZEN_IMPLEMENTATION_OR_INPUT_CHANGED_AFTER_INFERENCE_BEGAN"
                    break
    counters = base_smoke._request_counters(records)
    telemetry = {
        "experiment_id": experiment_id,
        "planned_requests": manifest["planned_requests"],
        "request_records": len(records),
        **counters,
        "stopped_early": stopped,
        "stop_reason": stop_reason,
        "wall_time_ms": int((time.monotonic() - started) * 1000),
        "model_runtime_ms": sum(value["model_call"]["duration_ms"] for value in records),
        "input_changed_after_first_call": input_changed_after_first_call,
        "code_changed_after_first_call": code_changed_after_first_call,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    }
    _write_json(output / "run_telemetry.json", telemetry)
    return records


def _observations(
    task_ids: Iterable[str], records: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    output = {str(task_id): [] for task_id in task_ids}
    for record in records:
        parser_tasks = record["transport_verification"].get("task_results") or {}
        verifier_tasks = record["existing_verifier"].get("task_results") or {}
        capacity_tasks = (record.get("capacity_verification") or {}).get("task_results") or {}
        for task_id in record["task_ids"]:
            output[task_id].append({
                "evaluation_set": record["evaluation_set"],
                "cold_run": int(record["cold_run"]),
                "pass_name": str(record["pass_name"]),
                "decision": (parser_tasks.get(task_id) or {}).get("decision"),
                "model_ok": bool(record["model_call"]["ok"]),
                "request_failure_kind": record["model_call"].get("failure_kind"),
                "response_parser_ok": bool(record["transport_verification"]["ok"]) if record["model_call"]["ok"] else None,
                "verifier_ok": (verifier_tasks.get(task_id) or {}).get("ok"),
                "verifier_errors": (verifier_tasks.get(task_id) or {}).get("errors") or [],
                "capacity_ok": (capacity_tasks.get(task_id) or {}).get("ok"),
                "capacity_errors": (capacity_tasks.get(task_id) or {}).get("errors") or [],
                "shard_id": record["shard_id"],
            })
    for values in output.values():
        values.sort(key=lambda value: (value["cold_run"], value["pass_name"]))
    return output


def _repeat_result(
    by_pass: Mapping[str, Mapping[str, Any]], cold_run: int,
) -> dict[str, Any]:
    pair = list(by_pass.values())
    stable_decision = None
    if set(by_pass) != set(PASSES):
        status = "REQUEST_NOT_OBSERVED"
    elif not all(value["model_ok"] for value in pair):
        status = "REQUEST_FAILURE"
    elif not all(value["response_parser_ok"] for value in pair):
        status = "RESPONSE_PARSER_REJECTION"
    elif not all(value["verifier_ok"] for value in pair):
        status = "VERIFIER_REJECTION"
    elif not all(value["capacity_ok"] for value in pair):
        status = "CAPACITY_REJECTION"
    elif by_pass["A"]["decision"] != by_pass["B"]["decision"]:
        status = "PASS_DISAGREEMENT"
    else:
        stable_decision = by_pass["A"]["decision"]
        status = "STABLE_UNRESOLVED" if stable_decision == lineage.NEED_MORE_EVIDENCE else "STABLE_MATCH"
    return {
        "cold_run": cold_run,
        "pass_a": by_pass.get("A", {}).get("decision"),
        "pass_b": by_pass.get("B", {}).get("decision"),
        "status": status,
        "stable_decision": stable_decision,
    }


def _task_results(
    population: Mapping[str, Any], sample: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    row_by_id = {str(value["task_id"]): value for value in population["tasks"]}
    task_ids = [*sample["selected_task_ids"], *[value["task_id"] for value in SENTINELS.values()]]
    observations = _observations(task_ids, records)
    output = []
    for task_id in task_ids:
        metadata = row_by_id[task_id]
        evaluation_set = "SENTINEL" if metadata["sentinel"] else "NEW_SAMPLE"
        cold_runs = SENTINEL_COLD_RUNS if metadata["sentinel"] else NEW_COLD_RUNS
        values = observations[task_id]
        repeats = [
            _repeat_result(
                {value["pass_name"]: value for value in values if value["cold_run"] == cold_run},
                cold_run,
            )
            for cold_run in cold_runs
        ]
        stable_values = [
            value["stable_decision"] for value in repeats
            if value["status"] in {"STABLE_MATCH", "STABLE_UNRESOLVED"}
        ]
        expected_repeats = len(cold_runs)
        exact_consistency = (
            len(stable_values) == expected_repeats and len(set(stable_values)) == 1
        )
        exact_decision = stable_values[0] if exact_consistency else None
        candidate_by_id = {value["candidate_id"]: value for value in metadata["candidates"]}
        repeat_relations = [
            candidate_by_id[value]["relation_type"]
            for value in stable_values if value in candidate_by_id
        ]
        if exact_decision == lineage.NEED_MORE_EVIDENCE:
            result_relation = "NEED_MORE_EVIDENCE"
        elif len(repeat_relations) == expected_repeats and len(set(repeat_relations)) == 1:
            result_relation = repeat_relations[0]
        elif repeat_relations:
            result_relation = "MIXED_RELATION"
        else:
            result_relation = "UNRESOLVED_OR_INVALID"
        evidence_distribution = Counter()
        for decision in stable_values:
            if decision in candidate_by_id:
                evidence_distribution[candidate_by_id[decision]["evidence_sha256"]] += 1
            elif decision == lineage.NEED_MORE_EVIDENCE:
                evidence_distribution["NEED_MORE_EVIDENCE"] += 1
        output.append({
            "evaluation_set": evaluation_set,
            "label": metadata["sentinel_label"] if metadata["sentinel"] else None,
            "task_id": task_id,
            "scope_id": metadata["scope_id"],
            "corpus": metadata["corpus"],
            "scope_kind": metadata["scope_kind"],
            "strata": metadata["strata"],
            "candidate_count": metadata["candidate_count"],
            "candidate_inventory": metadata["candidates"],
            "references": metadata["references"],
            "observations": values,
            "cold_repeats": repeats,
            "selection_distribution": dict(sorted(Counter(
                str(value.get("decision") or "<NO_SELECTION>") for value in values
            ).items())),
            "stable_repeat_count": len(stable_values),
            "expected_repeat_count": expected_repeats,
            "pass_disagreement_count": sum(value["status"] == "PASS_DISAGREEMENT" for value in repeats),
            "stable_need_more_evidence_repeat_count": sum(value["status"] == "STABLE_UNRESOLVED" for value in repeats),
            "model_or_schema_failure_count": sum(value["status"] in {"REQUEST_FAILURE", "RESPONSE_PARSER_REJECTION"} for value in repeats),
            "verifier_failure_count": sum(value["status"] == "VERIFIER_REJECTION" for value in repeats),
            "capacity_failure_count": sum(value["status"] == "CAPACITY_REJECTION" for value in repeats),
            "cross_cold_exact_selection_consistent": exact_consistency,
            "stable_decision": exact_decision,
            "stable_preference": exact_decision,
            "stable_unresolved": exact_decision == lineage.NEED_MORE_EVIDENCE,
            "result_relation_type": result_relation,
            "selected_evidence_signature_distribution": dict(sorted(evidence_distribution.items())),
        })
    return output


def _aggregate(tasks: Sequence[Mapping[str, Any]], expected_repeats: int = 3) -> dict[str, Any]:
    stable_counts = Counter(int(value["stable_repeat_count"]) for value in tasks)
    exact_count = sum(bool(value["cross_cold_exact_selection_consistent"]) for value in tasks)
    return {
        "tasks": len(tasks),
        "candidate_bearing_tasks": sum(int(value["candidate_count"]) > 0 for value in tasks),
        **{
            f"stable_{count}_of_{expected_repeats}": stable_counts[count]
            for count in range(expected_repeats, -1, -1)
        },
        "stable_3_of_3_rate": round(stable_counts[3] / len(tasks), 6) if tasks and expected_repeats == 3 else None,
        "pass_disagreement_repeats": sum(int(value["pass_disagreement_count"]) for value in tasks),
        "pass_disagreement_tasks": sum(int(value["pass_disagreement_count"]) > 0 for value in tasks),
        "stable_need_more_evidence_tasks": sum(bool(value["stable_unresolved"]) for value in tasks),
        "stable_need_more_evidence_repeats": sum(int(value["stable_need_more_evidence_repeat_count"]) for value in tasks),
        "model_or_schema_failure_repeats": sum(int(value["model_or_schema_failure_count"]) for value in tasks),
        "model_or_schema_failure_tasks": sum(int(value["model_or_schema_failure_count"]) > 0 for value in tasks),
        "verifier_failure_repeats": sum(int(value["verifier_failure_count"]) for value in tasks),
        "verifier_failure_tasks": sum(int(value["verifier_failure_count"]) > 0 for value in tasks),
        "capacity_failure_repeats": sum(int(value["capacity_failure_count"]) for value in tasks),
        "capacity_failure_tasks": sum(int(value["capacity_failure_count"]) > 0 for value in tasks),
        "unsupported_accepted_matches": [],
        "unsupported_accepted_match_count": 0,
        "cross_cold_exact_selection_consistency": {
            "consistent_tasks": exact_count,
            "denominator_tasks": len(tasks),
            "rate": round(exact_count / len(tasks), 6) if tasks else None,
        },
    }


def _group_metrics(
    tasks: Sequence[Mapping[str, Any]], key: str,
    values: Sequence[str],
) -> dict[str, Any]:
    return {
        value: _aggregate([task for task in tasks if value in task[key]])
        for value in values
    }


def _exact_child_union(
    population: Mapping[str, Any], sample: Mapping[str, Any],
    tasks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    population_by_id = {value["task_id"]: value for value in population["tasks"]}
    result_by_id = {value["task_id"]: value for value in tasks}
    candidate_to_task = {
        candidate_id: row["task_id"]
        for row in population["tasks"] for candidate_id in row["candidate_ids"]
    }
    selected = set(sample["selected_task_ids"])
    rows = []
    for parent_id in sample["selected_task_ids"]:
        parent_metadata = population_by_id[parent_id]
        parent_result = result_by_id[parent_id]
        for candidate in parent_metadata["candidates"]:
            if candidate["group_derivability"] != "EXACT_CHILD_UNION":
                continue
            child_task_ids = [candidate_to_task[value] for value in candidate["child_candidate_ids"]]
            children = [result_by_id.get(value) for value in child_task_ids]
            child_complete = (
                all(value in selected for value in child_task_ids)
                and all(child is not None and child["stable_decision"] == expected for child, expected in zip(children, candidate["child_candidate_ids"]))
            )
            if parent_result["stable_decision"] == lineage.NEED_MORE_EVIDENCE:
                outcome = "parent NME"
            elif not child_complete:
                outcome = "child incomplete"
            elif parent_result["stable_decision"] == candidate["candidate_id"]:
                outcome = "child-stable + parent-stable agreement"
            else:
                outcome = "child-stable + parent disagreement"
            child_candidates = []
            for child_id, child_candidate_id in zip(child_task_ids, candidate["child_candidate_ids"]):
                child_candidates.append(next(
                    value for value in population_by_id[child_id]["candidates"]
                    if value["candidate_id"] == child_candidate_id
                ))
            comparisons = {
                field: sorted({item for child in child_candidates for item in child[field]}) == sorted(set(candidate[field]))
                for field in ("right_physical_pages", "right_function_ids", "right_fragment_ids", "capacity_keys")
            }
            rows.append({
                "corpus": parent_metadata["corpus"],
                "parent_task_id": parent_id,
                "parent_candidate_id": candidate["candidate_id"],
                "parent_stable_decision": parent_result["stable_decision"],
                "child_task_ids": child_task_ids,
                "child_candidate_ids": candidate["child_candidate_ids"],
                "child_stable_decisions": [value["stable_decision"] if value else None for value in children],
                "deterministic_union_field_comparisons": comparisons,
                "outcome": outcome,
                "model_bypassed": False,
            })
    return {
        "cases": rows,
        "outcome_counts": dict(sorted(Counter(value["outcome"] for value in rows).items())),
    }


def _reference_metrics(tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    research_rows = []
    for task in tasks:
        for reference in task["references"]:
            decision = task["stable_decision"]
            if decision == lineage.NEED_MORE_EVIDENCE:
                status = "STABLE_NEED_MORE_EVIDENCE"
            elif decision is None:
                status = "UNSTABLE_OR_INVALID"
            elif decision in reference["candidate_ids"]:
                status = "ALIGNED"
            else:
                status = "NOT_ALIGNED"
            research_rows.append({
                "task_id": task["task_id"],
                "corpus": task["corpus"],
                "reference_id": reference["reference_id"],
                "reference_class": reference["reference_class"],
                "stable_decision": decision,
                "reference_candidate_ids": reference["candidate_ids"],
                "status": status,
            })
    determined = [value for value in research_rows if value["status"] in {"ALIGNED", "NOT_ALIGNED"}]
    aligned = sum(value["status"] == "ALIGNED" for value in determined)
    document_link_candidate_count = sum(
        candidate["sheet_matcher_edge_present"]
        for task in tasks for candidate in task["candidate_inventory"]
    )
    no_reference = sum(not value["references"] for value in tasks)
    return {
        "AUTHORITATIVE_FUNCTIONAL_REFERENCE": {
            "available": False,
            "cases": 0,
            "determined": 0,
            "aligned": 0,
            "alignment_rate": None,
            "reason": "the frozen corpus contains no mapping proven to be an authoritative functional reference",
        },
        "RESEARCH_REFERENCE": {
            "task_reference_rows": len(research_rows),
            "determined": len(determined),
            "aligned": aligned,
            "alignment_rate": round(aligned / len(determined), 6) if determined else None,
            "rows": research_rows,
            "truth_status": "hypothesis only; alignment is not precision",
        },
        "DOCUMENT_LINK": {
            "candidate_occurrences": document_link_candidate_count,
            "used_as_functional_truth": False,
        },
        "NO_REFERENCE": {"tasks": no_reference},
    }


def _sentinel_metrics(
    sources: Mapping[str, Any], tasks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    previous = {
        value["task_id"]: value for value in sources["previous_tasks"]["tasks"]
    }
    rows = []
    for task in tasks:
        if task["evaluation_set"] != "SENTINEL":
            continue
        expected = previous[task["task_id"]]["stable_decision"]
        observed = task["stable_decision"]
        if observed is None:
            status = "NOT_COMPARABLE"
        elif observed == expected:
            status = "UNCHANGED"
        else:
            status = "REGRESSION"
        rows.append({
            "label": task["label"],
            "task_id": task["task_id"],
            "expected_v2_4_2": expected,
            "observed_v2_5": observed,
            "status": status,
        })
    if any(value["status"] == "REGRESSION" for value in rows):
        regression = "YES"
    elif any(value["status"] == "NOT_COMPARABLE" for value in rows):
        regression = "UNKNOWN"
    else:
        regression = "NO"
    return {"sentinel_regression": regression, "tasks": rows}


def _cost(records: Sequence[Mapping[str, Any]], telemetry: Mapping[str, Any]) -> dict[str, Any]:
    usages = [dict(value["model_call"].get("usage") or {}) for value in records]
    telemetry_defect = any(
        record["model_call"]["ok"] and base_smoke._usage_total(usage) == 0
        for record, usage in zip(records, usages)
    )
    prompt_characters = [int(value["prompt_characters"]) for value in records]
    return {
        **base_smoke._request_counters(records),
        "planned_requests": int(telemetry["planned_requests"]),
        "attempted_requests": len(records),
        "successful_inference_requests": sum(bool(value["model_call"]["ok"]) for value in records),
        "model_runtime_ms": int(telemetry["model_runtime_ms"]),
        "wall_time_ms": int(telemetry["wall_time_ms"]),
        "provider_usage_objects": [
            {
                "evaluation_set": record["evaluation_set"],
                "cold_run": record["cold_run"],
                "pass_name": record["pass_name"],
                "shard_id": record["shard_id"],
                "usage": usage,
            }
            for record, usage in zip(records, usages)
        ],
        "input_tokens": sum(int(value.get("total_input_tokens") or value.get("input_tokens") or 0) for value in usages),
        "output_tokens": sum(int(value.get("output_tokens") or 0) for value in usages),
        "total_tokens": sum(base_smoke._usage_total(value) for value in usages),
        "telemetry_defect": telemetry_defect,
        "telemetry_assessment": (
            "TELEMETRY_DEFECT: successful inference returned usage={} / zero tokens; zero is not interpreted as zero cost"
            if telemetry_defect else "token telemetry returned for every successful request"
        ),
        "model_shards": len({(value["evaluation_set"], value["shard_id"]) for value in records}),
        "average_tasks_per_request": round(
            sum(len(value["task_ids"]) for value in records) / len(records), 6
        ) if records else None,
        "prompt_characters": {
            "median": statistics.median(prompt_characters) if prompt_characters else None,
            "p95": _percentile(prompt_characters, 95),
            "max": max(prompt_characters) if prompt_characters else None,
        },
    }


def _verdict(
    overall: Mapping[str, Any], group_metrics: Mapping[str, Any],
    references: Mapping[str, Any], safety: Mapping[str, Any],
    sentinel: Mapping[str, Any], cost: Mapping[str, Any],
    telemetry: Mapping[str, Any],
) -> dict[str, Any]:
    authoritative = references["AUTHORITATIVE_FUNCTIONAL_REFERENCE"]
    authoritative_insufficient = (
        authoritative["determined"] >= VERDICT_THRESHOLDS["c_authoritative_min_cases"]
        and authoritative["alignment_rate"] < VERDICT_THRESHOLDS["c_authoritative_alignment_min"]
    )
    deterministic_defect = (
        safety["unsupported_accepted_match_count"] > 0
        or safety["RIGHT_MAP_CONFLICT"] > 0
        or safety["FUNCTION_FRAGMENT_CONFLICT"] > 0
        or safety["capacity_defect_count"] > 0
        or safety["verifier_rejects"] > 0
        or safety["scope_safety_before"] != safety["scope_safety_after"]
    )
    technical_invalid = (
        bool(telemetry["stopped_early"])
        or bool(telemetry["input_changed_after_first_call"])
        or bool(telemetry["code_changed_after_first_call"])
        or int(cost["attempted_requests"]) != int(cost["planned_requests"])
        or int(cost["model_runtime_failures"]) > 0
        or int(cost["schema_failures"]) > 0
        or int(cost["semantic_response_failures"]) > 0
    )
    group_rate = group_metrics["combined_group_strata"]["stable_3_of_3_rate"]
    strong = (
        overall["stable_3_of_3_rate"] >= VERDICT_THRESHOLDS["a_overall_stable_3_of_3_min"]
        and overall["cross_cold_exact_selection_consistency"]["rate"] >= VERDICT_THRESHOLDS["a_cross_cold_exact_consistency_min"]
        and (group_rate is None or group_rate >= VERDICT_THRESHOLDS["a_group_stable_3_of_3_min"])
        and sentinel["sentinel_regression"] == "NO"
    )
    if technical_invalid:
        code = "E"
        reason = "experiment technically invalid or incomplete"
    elif deterministic_defect:
        code = "D"
        reason = "scope/candidate/verifier/capacity architecture has a new safety defect"
    elif authoritative_insufficient:
        code = "C"
        reason = "authoritative functional-reference alignment is insufficient"
    elif strong:
        code = "A"
        reason = "stratified multi-corpus evaluation is strongly stable and safe"
    else:
        code = "B"
        reason = "safe, but instability remains in identifiable strata"
    return {
        "verdict": code,
        "reason": reason,
        "thresholds": VERDICT_THRESHOLDS,
        "ready_to_prepare_production_integration_candidate_for_shadow_only_validation": code == "A",
        "experiment_valid": code != "E",
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    }


def _report(
    *, sample: Mapping[str, Any], tasks: Sequence[Mapping[str, Any]],
    corpus_metrics: Mapping[str, Any], stratum_metrics: Mapping[str, Any],
    relation_metrics: Mapping[str, Any], ambiguity: Mapping[str, Any],
    exact_child_union: Mapping[str, Any], non_decomposable: Mapping[str, Any],
    references: Mapping[str, Any], sentinel: Mapping[str, Any],
    safety: Mapping[str, Any], cost: Mapping[str, Any], verdict: Mapping[str, Any],
) -> str:
    lines = [
        "# Function Lineage v2.5 — stratified scoped corpus AI evaluation",
        "",
        f"Frozen v2.4 scope graph `{SCOPE_COMMIT}` and v2.4.1 transport `{SCOPED_TRANSPORT_COMMIT}`; model `{MODEL_CONFIGURATION['model']}/{MODEL_CONFIGURATION['reasoning_effort']}`.",
        "No deploy, no shadow, no materialization, no vision. The seven IOS2.1 controls are reported only as sentinels and are excluded from headline metrics.",
        "",
        "## Sample and corpus stability",
        "",
        "| Corpus | Tasks | Stable 3/3 | Stable 2/3 | Stable 1/3 | Stable 0/3 | Exact cross-cold consistency | Stable NME | Disagreement repeats |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for corpus in CORPUS_ORDER:
        value = corpus_metrics[corpus]
        exact = value["cross_cold_exact_selection_consistency"]
        lines.append(
            f"| {corpus} | {value['tasks']} | {value['stable_3_of_3']} | {value['stable_2_of_3']} | "
            f"{value['stable_1_of_3']} | {value['stable_0_of_3']} | {exact['consistent_tasks']}/{exact['denominator_tasks']} ({exact['rate']}) | "
            f"{value['stable_need_more_evidence_tasks']} | {value['pass_disagreement_repeats']} |"
        )
    overall = corpus_metrics["OVERALL"]
    exact = overall["cross_cold_exact_selection_consistency"]
    lines.append(
        f"| **OVERALL** | **{overall['tasks']}** | **{overall['stable_3_of_3']}** | **{overall['stable_2_of_3']}** | "
        f"**{overall['stable_1_of_3']}** | **{overall['stable_0_of_3']}** | **{exact['consistent_tasks']}/{exact['denominator_tasks']} ({exact['rate']})** | "
        f"**{overall['stable_need_more_evidence_tasks']}** | **{overall['pass_disagreement_repeats']}** |"
    )
    lines.extend([
        "",
        "Exact distribution is 12/12/12; redistribution was not required.",
        "",
        "## Requested strata",
        "",
        "| Stratum | Eligible new population | Selected | Stable 3/3 | Exact consistency | Stable NME | Pass disagreements |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])
    for stratum in STRATA:
        coverage = sample["stratum_coverage"][stratum]
        metric = stratum_metrics[stratum]
        exact = metric["cross_cold_exact_selection_consistency"]
        lines.append(
            f"| {stratum} | {coverage['eligible_population']} | {coverage['selected_tasks']} | "
            f"{metric['stable_3_of_3']} | {exact['rate']} | {metric['stable_need_more_evidence_tasks']} | {metric['pass_disagreement_repeats']} |"
        )
    if sample["stratum_coverage"]["E"]["eligible_population"] == 0:
        lines.extend([
            "",
            "Stratum E has no NEW eligible task: all frozen FUNCTION_DISTRIBUTED candidates belong to the excluded LEFT20 PARENT sentinel. Its result appears only in the sentinel section.",
        ])
    lines.extend([
        "",
        "## Stable results by relation type",
        "",
        "| Relation | Tasks | Stable 3/3 | Exact consistency | Stable NME |",
        "|---|---:|---:|---:|---:|",
    ])
    for relation, value in relation_metrics.items():
        exact = value["cross_cold_exact_selection_consistency"]
        lines.append(
            f"| {relation} | {value['tasks']} | {value['stable_3_of_3']} | {exact['rate']} | {value['stable_need_more_evidence_tasks']} |"
        )
    lines.extend([
        "",
        "## Same-scope ambiguity",
        "",
        f"Ambiguous tasks: `{ambiguity['metrics']['tasks']}`; stable 3/3: `{ambiguity['metrics']['stable_3_of_3']}`; exact cross-cold consistency: `{ambiguity['metrics']['cross_cold_exact_selection_consistency']['rate']}`.",
        "Stability is only a model preference. It is not treated as proof that other eligible candidates are false.",
        "",
        "| Task | Corpus | Distribution | Stable preference | Evidence signatures |",
        "|---|---|---|---|---|",
    ])
    for task in ambiguity["tasks"]:
        lines.append(
            f"| `{task['task_id']}` | {task['corpus']} | `{json.dumps(task['selection_distribution'], sort_keys=True)}` | "
            f"`{task['stable_preference']}` | `{json.dumps(task['selected_evidence_signature_distribution'], sort_keys=True)}` |"
        )
    lines.extend([
        "",
        "## EXACT_CHILD_UNION",
        "",
        f"Outcome counts: `{json.dumps(exact_child_union['outcome_counts'], sort_keys=True)}`. The normal model selector ran first; model bypass was never used.",
        "",
        "| Corpus | Parent task | Candidate | Outcome | Deterministic union fields |",
        "|---|---|---|---|---|",
    ])
    for row in exact_child_union["cases"]:
        lines.append(
            f"| {row['corpus']} | `{row['parent_task_id']}` | `{row['parent_candidate_id']}` | {row['outcome']} | "
            f"`{json.dumps(row['deterministic_union_field_comparisons'], sort_keys=True)}` |"
        )
    lines.extend([
        "",
        "## NON_DECOMPOSABLE_GROUP",
        "",
        f"Selected tasks `{non_decomposable['tasks']}`; stable 3/3 `{non_decomposable['stable_3_of_3']}`; exact consistency `{non_decomposable['cross_cold_exact_selection_consistency']['rate']}`.",
        "",
        "## Reference classes",
        "",
        f"Authoritative functional references: `0`; alignment is `N/A` because the frozen corpus contains no genuinely authoritative functional mapping.",
        f"Research-reference determined rows `{references['RESEARCH_REFERENCE']['determined']}`; aligned `{references['RESEARCH_REFERENCE']['aligned']}`; rate `{references['RESEARCH_REFERENCE']['alignment_rate']}`. This is hypothesis alignment, not precision.",
        f"DOCUMENT_LINK candidate occurrences `{references['DOCUMENT_LINK']['candidate_occurrences']}`; used as functional truth: `NO`. NO_REFERENCE tasks `{references['NO_REFERENCE']['tasks']}`.",
        "",
        "## Sentinels (excluded from headline)",
        "",
        f"Sentinel regression: **{sentinel['sentinel_regression']}**.",
        "",
        "| Sentinel | Expected v2.4.2 | Observed v2.5 | Status |",
        "|---|---|---|---|",
    ])
    for row in sentinel["tasks"]:
        lines.append(
            f"| {row['label']} | `{row['expected_v2_4_2']}` | `{row['observed_v2_5']}` | {row['status']} |"
        )
    lines.extend([
        "",
        "## Safety and technical quality",
        "",
        f"Unsupported accepted matches `{safety['unsupported_accepted_match_count']}`; verifier rejects `{safety['verifier_rejects']}`; capacity defects `{safety['capacity_defect_count']}`; RIGHT_MAP_CONFLICT `{safety['RIGHT_MAP_CONFLICT']}`; FUNCTION_FRAGMENT_CONFLICT `{safety['FUNCTION_FRAGMENT_CONFLICT']}`.",
        f"Cross-granularity selectable competition before/after: `{safety['scope_safety_before']['cross_granularity_selectable_competition']}` / `{safety['scope_safety_after']['cross_granularity_selectable_competition']}`. Raw candidates preserved: `{safety['scope_safety_after']['raw_candidates_preserved']}`.",
        f"Model/schema failure tasks `{overall['model_or_schema_failure_tasks']}`; stable NME `{overall['stable_need_more_evidence_tasks']}`; PASS_DISAGREEMENT repeats `{overall['pass_disagreement_repeats']}`.",
        "",
        "## Cost and runtime",
        "",
        f"Planned / attempted / successful requests: `{cost['planned_requests']}` / `{cost['attempted_requests']}` / `{cost['successful_inference_requests']}`.",
        f"Model runtime `{cost['model_runtime_ms']} ms`; wall time `{cost['wall_time_ms']} ms`; shards `{cost['model_shards']}`; average tasks/request `{cost['average_tasks_per_request']}`.",
        f"Prompt characters median/p95/max: `{cost['prompt_characters']['median']}` / `{cost['prompt_characters']['p95']}` / `{cost['prompt_characters']['max']}`.",
        f"Input/output/total tokens: `{cost['input_tokens']}` / `{cost['output_tokens']}` / `{cost['total_tokens']}`. {cost['telemetry_assessment']}.",
        "",
        "## Verdict",
        "",
        f"**{verdict['verdict']} — {verdict['reason']}.**",
        "",
        f"Ready to prepare a production integration candidate for shadow-only validation: `{'YES' if verdict['ready_to_prepare_production_integration_candidate_for_shadow_only_validation'] else 'NO'}`.",
        "",
        "Even if verdict A: **DO NOT DEPLOY. DO NOT ENABLE SHADOW.**",
        "",
    ])
    return "\n".join(lines)


def finalize(output: Path) -> dict[str, Any]:
    for name in ("task_results.json", "stratum_metrics.json", "corpus_metrics.json", "metrics.json", "report.md"):
        if (output / name).exists():
            raise RuntimeError(f"refusing to replace immutable result artifact: {name}")
    manifest, population, sample, _shards, _datasets_value = _validate_prepared(output)
    records = _read_jsonl(output / "model_runs.jsonl")
    telemetry = _read_json(output / "run_telemetry.json")
    sources = _load_sources()
    tasks = _task_results(population, sample, records)
    new_tasks = [value for value in tasks if value["evaluation_set"] == "NEW_SAMPLE"]
    sentinel_tasks = [value for value in tasks if value["evaluation_set"] == "SENTINEL"]
    corpus_metrics = {
        corpus: _aggregate([value for value in new_tasks if value["corpus"] == corpus])
        for corpus in CORPUS_ORDER
    }
    corpus_metrics["OVERALL"] = _aggregate(new_tasks)
    stratum_metrics = _group_metrics(new_tasks, "strata", list(STRATA))
    relation_names = (
        "CONTINUED_1_TO_1", "SPLIT_1_TO_N", "MERGED_N_TO_1",
        "FUNCTION_DISTRIBUTED", "NEED_MORE_EVIDENCE", "MIXED_RELATION",
        "UNRESOLVED_OR_INVALID",
    )
    relation_metrics = {
        relation: _aggregate([value for value in new_tasks if value["result_relation_type"] == relation])
        for relation in relation_names
    }
    ambiguity_tasks = [value for value in new_tasks if "B" in value["strata"]]
    ambiguity = {
        "metrics": _aggregate(ambiguity_tasks),
        "tasks": [{
            "task_id": value["task_id"],
            "corpus": value["corpus"],
            "selection_distribution": value["selection_distribution"],
            "stable_preference": value["stable_preference"],
            "selected_evidence_signature_distribution": value["selected_evidence_signature_distribution"],
        } for value in ambiguity_tasks],
    }
    exact_child_union = _exact_child_union(population, sample, new_tasks)
    non_decomposable = _aggregate([value for value in new_tasks if "G" in value["strata"]])
    references = _reference_metrics(new_tasks)
    sentinel = _sentinel_metrics(sources, sentinel_tasks)
    capacity_errors = sorted({
        str(error) for record in records
        for error in (record.get("capacity_verification") or {}).get("errors") or []
    })
    verifier_rejects = sum(
        any(value.get("verifier_ok") is False for value in task["observations"])
        for task in tasks
    )
    unsupported = [
        {"task_id": task["task_id"], "candidate_id": task["stable_decision"]}
        for task in tasks
        if task["stable_decision"] not in {None, lineage.NEED_MORE_EVIDENCE}
        and task["stable_decision"] not in {value["candidate_id"] for value in task["candidate_inventory"]}
    ]
    before = manifest["preflight"]["deterministic_safety"]
    after = _preflight(sources, _read_jsonl(output / "model_inputs.jsonl"))["deterministic_safety"]
    safety = {
        "scope_safety_before": before,
        "scope_safety_after": after,
        "unsupported_accepted_matches": unsupported,
        "unsupported_accepted_match_count": len(unsupported),
        "verifier_rejects": verifier_rejects,
        "capacity_defects": capacity_errors,
        "capacity_defect_count": len(capacity_errors),
        "RIGHT_MAP_CONFLICT": sum("RIGHT_MAP_CONFLICT" in value for value in capacity_errors),
        "FUNCTION_FRAGMENT_CONFLICT": sum(value.startswith("FUNCTION_FRAGMENT_CONFLICT:") for value in capacity_errors),
        "capacity_identity": "RIGHT physical_page + exact function_fragment_id",
        "strict_subset_parent_selectable": False,
        "strict_superset_child_selectable": False,
        "unknown_scope_fail_closed": True,
    }
    cost = _cost(records, telemetry)
    group_metrics = {
        "combined_group_strata": _aggregate([
            value for value in new_tasks if set(value["strata"]) & {"C", "D", "E", "F", "G"}
        ]),
        "relation_types": relation_metrics,
    }
    verdict = _verdict(
        corpus_metrics["OVERALL"], group_metrics, references,
        safety, sentinel, cost, telemetry,
    )
    metrics = {
        "kind": "function_lineage_v2_5_stratified_scoped_evaluation_metrics",
        "sample_size": len(new_tasks),
        "sample_size_by_corpus": sample["sample_size_by_corpus"],
        "cold_repeat_count": len(NEW_COLD_RUNS),
        "passes_per_repeat": len(PASSES),
        "sentinel_task_count": len(sentinel_tasks),
        "sentinel_repeats": len(SENTINEL_COLD_RUNS),
        "stratum_coverage": sample["stratum_coverage"],
        "overall": corpus_metrics["OVERALL"],
        "relation_metrics": relation_metrics,
        "same_scope_ambiguity": ambiguity,
        "exact_child_union": exact_child_union,
        "non_decomposable_group": non_decomposable,
        "references": references,
        "sentinels": sentinel,
        "safety": safety,
        "cost": cost,
        "verdict": verdict,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    }
    _write_json(output / "task_results.json", {
        "kind": "function_lineage_v2_5_task_results",
        "new_sample": new_tasks,
        "sentinels": sentinel_tasks,
    })
    _write_json(output / "stratum_metrics.json", stratum_metrics)
    _write_json(output / "corpus_metrics.json", corpus_metrics)
    _write_json(output / "metrics.json", metrics)
    (output / "report.md").write_text(_report(
        sample=sample, tasks=new_tasks, corpus_metrics=corpus_metrics,
        stratum_metrics=stratum_metrics, relation_metrics=relation_metrics,
        ambiguity=ambiguity, exact_child_union=exact_child_union,
        non_decomposable=non_decomposable, references=references,
        sentinel=sentinel, safety=safety, cost=cost, verdict=verdict,
    ), encoding="utf-8")
    artifact_names = (
        "stratified_population.json", "stratified_sample.json", "input_manifest.json",
        "model_inputs.jsonl", "external_codex_disclosure.json", "model_runs.jsonl",
        "run_telemetry.json", "task_results.json", "stratum_metrics.json",
        "corpus_metrics.json", "metrics.json", "report.md",
    )
    _write_json(output / "artifact_hashes.json", {
        "files": {
            name: {"sha256": _sha_file(output / name), "bytes": (output / name).stat().st_size}
            for name in artifact_names
        },
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    })
    return metrics


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "experiment", "finalize"))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--confirm-external-codex", action="store_true")
    parser.add_argument("--confirm-disclosure-sha256")
    args = parser.parse_args()
    output = args.output.resolve()
    if args.phase == "prepare":
        manifest = prepare(output)
        print(json.dumps({
            "output": _display_path(output),
            "sample_size": SAMPLE_SIZE,
            "sample_size_by_corpus": _read_json(output / "stratified_sample.json")["sample_size_by_corpus"],
            "planned_requests": manifest["planned_requests"],
            "disclosure": manifest["external_codex_disclosure_path"],
            "disclosure_sha256": manifest["external_codex_disclosure_sha256"],
        }, ensure_ascii=False, indent=2), flush=True)
    elif args.phase == "experiment":
        experiment(
            output,
            confirm_external_codex=args.confirm_external_codex,
            confirm_disclosure_sha256=args.confirm_disclosure_sha256,
        )
    else:
        print(json.dumps(finalize(output), ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
