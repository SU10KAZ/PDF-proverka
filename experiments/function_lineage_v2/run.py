"""Evaluate production Function Lineage candidate coverage without a model.

This runner is intentionally pre-selector.  It reads already generated local
Markdown/index artifacts, rebuilds deterministic candidates, and writes only
research artifacts under ``comparison/ai_sheet_matcher``.
"""
from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.app.services.stage_comparison.function_lineage_shadow import (
    ALGORITHM_VERSION,
    COMPLEX_RELATIONS,
    PASSPORT_FIELDS,
    FunctionLineageDataset,
    build_dataset,
    deterministic_candidate_artifact,
)
from backend.app.services.stage_comparison.production_orchestrator import (
    _production_sheet_indexes,
)
from experiments.ai_sheet_matcher.core import PROJECT_CONFIG, SESSION_ID


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_deterministic"
)
FROZEN_V1 = REPO_ROOT / "comparison" / "ai_sheet_matcher" / "20260902_function_lineage_v1"
IOS21_FORENSIC_RUN_ID = "prun_8a28eb85d3ca435c5b04577e"
PRODUCTION_BASE_COMMIT = "5eb6fa144c3124e8926f5e8c69c546827b878ff8"
PRODUCTION_BASE_RELEASE = "ui-real-5eb6fa14"
IOS21_PAIR_ID = "pe336037597"

# Evaluation-only expected lineage.  These values never enter extraction,
# candidate generation, scoring, ranking, or IDs.
IOS21_GROUP_REFERENCE = {
    "left_pages": [20],
    "right_pages": [26, 28, 29],
    "relation_type": "FUNCTION_DISTRIBUTED",
    "name": "composite pump and incoming-metering function",
}


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _pair_dir(pair_id: str) -> Path:
    return REPO_ROOT / "comparison" / "sessions" / SESSION_ID / "pairs" / pair_id


def _load_dataset(pair_id: str) -> tuple[FunctionLineageDataset, dict[str, Any]]:
    pair_dir = _pair_dir(pair_id)
    pair = _read_json(pair_dir / "pair.json")
    relations = _read_json(pair_dir / "production" / "sheet_relations.json")
    dataset = build_dataset(
        pair_id=pair_id,
        sheet_indexes=_production_sheet_indexes(pair),
        sheet_relations=relations,
    )
    return dataset, relations


def _task_rank(dataset: FunctionLineageDataset, left_page: int, candidate_id: str) -> int | None:
    return min(
        (
            int(task["candidate_ranks"][candidate_id])
            for task in dataset.tasks
            if int(task["left_physical_page"]) == left_page
            and candidate_id in task.get("candidate_ranks", {})
        ),
        default=None,
    )


def _single_page_rank(
    dataset: FunctionLineageDataset, left_page: int, right_page: int,
) -> int | None:
    return min(
        (
            rank
            for candidate_id, candidate in dataset.candidates.items()
            if candidate.get("relation_type") == "CONTINUED_1_TO_1"
            and candidate.get("left_pages") == [left_page]
            and candidate.get("right_pages") == [right_page]
            for rank in [_task_rank(dataset, left_page, candidate_id)]
            if rank is not None
        ),
        default=None,
    )


def _exact_group(
    dataset: FunctionLineageDataset,
    left_pages: Sequence[int],
    right_pages: Sequence[int],
) -> tuple[dict[str, Any] | None, int | None]:
    expected_left = sorted(int(value) for value in left_pages)
    expected_right = sorted(int(value) for value in right_pages)
    rows = [
        value for value in dataset.candidates.values()
        if value.get("relation_type") in COMPLEX_RELATIONS
        and value.get("left_pages") == expected_left
        and value.get("right_pages") == expected_right
    ]
    if not rows:
        return None, None
    ranked = sorted(
        (
            (rank, value)
            for value in rows
            for left_page in expected_left
            for rank in [_task_rank(dataset, left_page, value["candidate_id"])]
            if rank is not None
        ),
        key=lambda item: (item[0], item[1]["candidate_id"]),
    )
    return (ranked[0][1], ranked[0][0]) if ranked else (rows[0], None)


def _reference_cases(pair_id: str) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for raw in PROJECT_CONFIG[pair_id]["reference_cases"]:
        for left_page in raw["left_pages"]:
            cases.append({
                "left_page": int(left_page),
                "left_pages": sorted(int(value) for value in raw["left_pages"]),
                "right_pages": sorted(int(value) for value in raw["right_pages"]),
                "expected_mode": str(raw.get("expected_mode") or "ALL"),
                "name": raw["name"],
                "source": "FUNCTIONAL_REFERENCE_HYPOTHESIS",
            })
    if pair_id == IOS21_PAIR_ID:
        cases.append({
            "left_page": 20,
            "left_pages": [20],
            "right_pages": [26, 28, 29],
            "expected_mode": "EXACT_GROUP",
            "name": IOS21_GROUP_REFERENCE["name"],
            "source": "FORENSIC_FUNCTIONAL_REGRESSION_CONTROL",
        })
    return cases


def _case_rank(dataset: FunctionLineageDataset, case: Mapping[str, Any]) -> int | None:
    if case["expected_mode"] == "EXACT_GROUP" or (
        case["expected_mode"] == "ALL"
        and (len(case["left_pages"]) > 1 or len(case["right_pages"]) > 1)
    ):
        return _exact_group(dataset, case["left_pages"], case["right_pages"])[1]
    ranks = [
        _single_page_rank(dataset, int(case["left_page"]), int(right_page))
        for right_page in case["right_pages"]
    ]
    present = [value for value in ranks if value is not None]
    if case["expected_mode"] == "ANY":
        return min(present) if present else None
    return max(present) if ranks and len(present) == len(ranks) else None


def _recall(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    return {
        "case_count": total,
        **{
            f"recall_at_{value}": (
                round(sum(row.get("rank") is not None and int(row["rank"]) <= value for row in rows) / total, 6)
                if total else None
            )
            for value in (1, 3, 5, 10)
        },
    }


def _document_link_recall(pair_id: str, dataset: FunctionLineageDataset) -> dict[str, Any]:
    labels_payload = _read_json(_pair_dir(pair_id) / "sheet_links.json")
    labels = {
        (int(left), int(right))
        for link in labels_payload.get("links") or []
        for left in link.get("left_pages") or []
        for right in link.get("right_pages") or []
    }
    detected = {
        (int(left), int(right))
        for link in dataset.document_link_map["links"]
        for left in link.get("left_pages") or []
        for right in link.get("right_pages") or []
    }
    hits = labels & detected
    return {
        "namespace": "DOCUMENT_LINK",
        "ground_truth_count": len(labels),
        "detected_count": len(detected),
        "true_positive_count": len(hits),
        "recall": round(len(hits) / len(labels), 6) if labels else None,
        "labels": [list(value) for value in sorted(labels)],
        "hits": [list(value) for value in sorted(hits)],
    }


def _project_metrics(
    pair_id: str, dataset: FunctionLineageDataset,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    cases = []
    for raw in _reference_cases(pair_id):
        case = dict(raw)
        case["rank"] = _case_rank(dataset, case)
        cases.append(case)
    single = [
        value for value in cases
        if len(value["left_pages"]) == 1
        and len(value["right_pages"]) == 1
        and value["expected_mode"] != "EXACT_GROUP"
    ]

    group_rows: list[dict[str, Any]] = []
    seen_groups: set[tuple[tuple[int, ...], tuple[int, ...]]] = set()
    for raw in PROJECT_CONFIG[pair_id]["reference_cases"]:
        left_pages = tuple(sorted(int(value) for value in raw["left_pages"]))
        right_pages = tuple(sorted(int(value) for value in raw["right_pages"]))
        if raw.get("expected_mode", "ALL") != "ALL" or (len(left_pages) == len(right_pages) == 1):
            continue
        seen_groups.add((left_pages, right_pages))
    if pair_id == IOS21_PAIR_ID:
        seen_groups.add(((20,), (26, 28, 29)))
    for left_pages, right_pages in sorted(seen_groups):
        candidate, rank = _exact_group(dataset, left_pages, right_pages)
        group_rows.append({
            "left_pages": list(left_pages),
            "right_pages": list(right_pages),
            "candidate_id": candidate.get("candidate_id") if candidate else None,
            "relation_type": candidate.get("relation_type") if candidate else None,
            "rank": rank,
            "present": candidate is not None,
        })

    counts = [len(task.get("candidate_ids") or []) for task in dataset.tasks]
    ordered = sorted(counts)
    p95 = (
        float(ordered[max(0, (95 * len(ordered) + 99) // 100 - 1)])
        if ordered else 0.0
    )
    search_failures = [
        {
            "task_id": task["task_id"],
            "left_page": task["left_physical_page"],
            "left_fragment_id": task["left_fragment_id"],
        }
        for task in dataset.tasks if not task.get("candidate_ids")
    ]
    false_internal_conflicts = [
        candidate["candidate_id"]
        for candidate in dataset.candidates.values()
        if candidate.get("relation_type") != "MERGED_N_TO_1"
        and len([
            row.get("capacity_key") for row in candidate.get("component_map") or []
        ]) != len({
            row.get("capacity_key") for row in candidate.get("component_map") or []
        })
    ]
    project = {
        "pair_id": pair_id,
        "project": PROJECT_CONFIG[pair_id]["project"],
        "algorithm_version": ALGORITHM_VERSION,
        "functional_analogue_recall": _recall(cases),
        "single_page_recall": _recall(single),
        "group_candidate_recall": {
            "case_count": len(group_rows),
            "exact_group_hits": sum(row["present"] for row in group_rows),
            "recall": (
                round(sum(row["present"] for row in group_rows) / len(group_rows), 6)
                if group_rows else None
            ),
        },
        "document_link_recall": _document_link_recall(pair_id, dataset),
        "candidate_count": {
            "scope": "per LEFT function fragment",
            "median": statistics.median(counts) if counts else 0.0,
            "p95": p95,
            "maximum": max(counts, default=0),
        },
        "function_passports": {
            side: len(dataset.function_passports[side]) for side in ("LEFT", "RIGHT")
        },
        "candidate_count_total": len(dataset.candidates),
        "search_failures": search_failures,
        "group_generation_failures": [row for row in group_rows if not row["present"]],
        "new_false_conflicts": false_internal_conflicts,
        "page_global_exclusivity": False,
        "cases": cases,
        "group_cases": group_rows,
    }
    return project, cases, group_rows


def _candidate_rows(dataset: FunctionLineageDataset, left_page: int) -> list[dict[str, Any]]:
    rows = []
    for candidate in dataset.candidates.values():
        if left_page not in candidate.get("left_pages", []):
            continue
        rank = _task_rank(dataset, left_page, candidate["candidate_id"])
        if rank is None:
            continue
        rows.append({
            "candidate_id": candidate["candidate_id"],
            "rank": rank,
            "relation_type": candidate["relation_type"],
            "right_pages": candidate["right_pages"],
            "retrieval_channels": candidate.get("retrieval_channels") or [],
            "supporting_channels": candidate.get("supporting_channels") or [],
            "functional_score": candidate.get("functional_score"),
            "ranking_score": candidate.get("source_score"),
            "source_kind": candidate.get("source_kind"),
        })
    return sorted(rows, key=lambda value: (value["rank"], value["right_pages"], value["candidate_id"]))


def _passports(
    values: Iterable[Mapping[str, Any]], *, left_page: int,
) -> list[dict[str, Any]]:
    rows = []
    for value in values:
        source_sheet = value.get("source_sheet") or {}
        if int(source_sheet.get("physical_page") or 0) != left_page:
            continue
        fields = {
            field: value.get(field)
            for field in PASSPORT_FIELDS
            if field not in {"evidence_refs"}
        }
        rows.append({
            "function_id": value.get("function_id"),
            "function_fragment_ids": value.get("function_fragment_ids"),
            "fields": fields,
            "provenance": value.get("provenance"),
            "evidence_refs": value.get("evidence_refs"),
        })
    return sorted(rows, key=lambda value: (str(value["fields"].get("function_class")), str(value["function_id"])))


def _ios21_comparison(dataset: FunctionLineageDataset) -> dict[str, Any]:
    frozen_passports = _read_jsonl(FROZEN_V1 / "function_passports.jsonl")
    frozen_candidates = _read_jsonl(FROZEN_V1 / "lineage_candidates.jsonl")
    current_map = _read_json(_pair_dir(IOS21_PAIR_ID) / "production" / "function_lineage_map.json")
    if current_map.get("run_id") != IOS21_FORENSIC_RUN_ID:
        raise RuntimeError("saved IOS2.1 forensic run no longer matches requested run_id")
    required_fields = (
        "function_class", "serviced_object", "building", "corpus", "section",
        "zone", "floors", "consumers", "upstream", "downstream", "systems",
        "equipment_roles", "document_role", "neighboring_function_context",
        "stable_entities", "cross_sheet_functional_references",
    )
    output: dict[str, Any] = {
        "kind": "function_lineage_stage_comparison",
        "pair_id": IOS21_PAIR_ID,
        "run_id": IOS21_FORENSIC_RUN_ID,
        "production_base_commit": PRODUCTION_BASE_COMMIT,
        "production_base_release": PRODUCTION_BASE_RELEASE,
        "pages": {},
    }
    current_values = list((current_map.get("function_passports") or {}).get("LEFT", {}).values())
    for page in (17, 18, 19, 20):
        frozen_values = [
            value for value in frozen_passports
            if value.get("pair_id") == IOS21_PAIR_ID and value.get("side") == "LEFT"
        ]
        new_values = list(dataset.function_passports["LEFT"].values())
        frozen_page = _passports(frozen_values, left_page=page)
        current_page = _passports(current_values, left_page=page)
        new_page = _passports(new_values, left_page=page)
        lost = []
        for field in required_fields:
            frozen_has = any(value["fields"].get(field) not in (None, [], "") for value in frozen_page)
            current_has = any(value["fields"].get(field) not in (None, [], "") for value in current_page)
            if frozen_has and not current_has:
                lost.append(field)
        frozen_page_candidates = [
            {
                "candidate_id": value["candidate_id"],
                "rank": value.get("source_rank"),
                "rank_basis": "frozen_v1_source_rank",
                "relation_type": value["relation_type"],
                "right_pages": value["right_pages"],
                "retrieval_channels": value.get("retrieval_channels") or [],
                "functional_score": value.get("functional_score"),
            }
            for value in frozen_candidates
            if value.get("pair_id") == IOS21_PAIR_ID
            and page in value.get("left_pages", [])
        ]
        current_candidates = [
            {
                "candidate_id": value.get("candidate_id"),
                "rank": None,
                "rank_basis": "not_recorded_in_saved_production_map",
                "relation_type": value.get("relation_type"),
                "right_pages": value.get("right_pages"),
                "retrieval_channels": value.get("retrieval_channels") or [],
                "source_score": value.get("source_score"),
            }
            for value in current_map.get("functional_candidates") or []
            if page in value.get("left_pages", [])
        ]
        present_fields = {
            "frozen_v1": sorted(
                field for field in required_fields
                if any(value["fields"].get(field) not in (None, [], "") for value in frozen_page)
            ),
            "current_production": sorted(
                field for field in required_fields
                if any(value["fields"].get(field) not in (None, [], "") for value in current_page)
            ),
            "new_deterministic": sorted(
                field for field in required_fields
                if any(value["fields"].get(field) not in (None, [], "") for value in new_page)
            ),
        }
        output["pages"][str(page)] = {
            "frozen_v1": {"passports": frozen_page, "candidates": frozen_page_candidates},
            "current_production": {"passports": current_page, "candidates": current_candidates},
            "new_deterministic": {"passports": new_page, "candidates": _candidate_rows(dataset, page)},
            "fields_present": present_fields,
            "fields_lost_during_production_integration": lost,
            "fields_restored_in_new_deterministic": sorted(
                set(lost) & set(present_fields["new_deterministic"])
            ),
        }
    return output


def _ios21_controls(
    dataset: FunctionLineageDataset, sheet_relations: Mapping[str, Any],
) -> dict[str, Any]:
    def single(left: int, right: int) -> dict[str, Any]:
        rows = [
            value for value in _candidate_rows(dataset, left)
            if value["right_pages"] == [right]
            and value["relation_type"] == "CONTINUED_1_TO_1"
        ]
        return {
            "left_page": left,
            "right_page": right,
            "present": bool(rows),
            "rank": min((value["rank"] for value in rows), default=None),
            "candidate_ids": [value["candidate_id"] for value in rows],
        }

    group, group_rank = _exact_group(dataset, [20], [26, 28, 29])
    edge_pages = {
        int(search["left_page"]): {
            int(value["right_page"])
            for value in search.get("deep_candidates") or []
            if value.get("status") != "NO_MATCH"
        }
        for search in sheet_relations.get("candidate_search") or []
    }
    controls = {
        "left17_right27": single(17, 27),
        "left18_right24": single(18, 24),
        "left19_right25": single(19, 25),
        "left19_right30": single(19, 30),
        "left20_distributed_26_28_29": {
            "present": group is not None,
            "candidate_id": group.get("candidate_id") if group else None,
            "rank": group_rank,
            "right_pages": group.get("right_pages") if group else None,
            "right_fragment_ids": group.get("right_fragment_ids") if group else None,
            "right_capacity_keys": group.get("right_capacity_keys") if group else None,
        },
        "sheet_matcher_edge_presence": {
            "left17_right27": 27 in edge_pages.get(17, set()),
            "left18_right24": 24 in edge_pages.get(18, set()),
            "left19_right25": 25 in edge_pages.get(19, set()),
            "left19_right30": 30 in edge_pages.get(19, set()),
            "left20_group_members": {
                str(page): page in edge_pages.get(20, set()) for page in (26, 28, 29)
            },
        },
    }
    controls["all_passed"] = all(
        controls[key]["present"]
        for key in (
            "left17_right27", "left18_right24", "left19_right25",
            "left19_right30", "left20_distributed_26_28_29",
        )
    )
    return controls


def _report(metrics: Mapping[str, Any], controls: Mapping[str, Any], comparison: Mapping[str, Any]) -> str:
    lines = [
        "# Function Lineage deterministic candidate coverage",
        "",
        f"Algorithm: `{metrics['algorithm_version']}`.",
        f"Production baseline: `{metrics['production_base_commit']}` "
        f"(`{metrics['production_base_release']}`).",
        "",
        "This run stopped before selector/model execution. Model calls: `0`; deploy: `false`; materialization: `false`.",
        "",
        "## Architecture",
        "",
        "Existing same-version Markdown is converted deterministically into compact, provenance-ready "
        "function facts. Function fragments search the complete RIGHT function corpus through independent "
        "functional channels; Sheet Matcher edges, titles, and physical proximity are supporting signals only. "
        "Bounded 1:1, 1:N, N:1, and FUNCTION_DISTRIBUTED candidates retain exact-fragment capacity keys.",
        "",
        "## Candidate recall",
        "",
        "| Project | R@1 | R@3 | R@5 | R@10 | single R@10 | group recall | DOCUMENT_LINK recall | median / p95 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for project in metrics["projects"]:
        recall = project["functional_analogue_recall"]
        single = project["single_page_recall"]
        group = project["group_candidate_recall"]
        document = project["document_link_recall"]
        count = project["candidate_count"]
        lines.append(
            f"| {project['project']} | {recall['recall_at_1']} | {recall['recall_at_3']} | "
            f"{recall['recall_at_5']} | {recall['recall_at_10']} | {single['recall_at_10']} | "
            f"{group['recall']} | {document['recall']} | {count['median']} / {count['p95']} |"
        )
    overall = metrics["overall"]
    lines.extend([
        "",
        f"Overall FUNCTIONAL_ANALOGUE recall: R@1 `{overall['functional_analogue_recall']['recall_at_1']}`, "
        f"R@3 `{overall['functional_analogue_recall']['recall_at_3']}`, "
        f"R@5 `{overall['functional_analogue_recall']['recall_at_5']}`, "
        f"R@10 `{overall['functional_analogue_recall']['recall_at_10']}`.",
        "",
        "### Single-page recall",
        "",
        "| Project | R@1 | R@3 | R@5 | R@10 |",
        "|---|---:|---:|---:|---:|",
    ])
    for project in metrics["projects"]:
        single = project["single_page_recall"]
        lines.append(
            f"| {project['project']} | {single['recall_at_1']} | "
            f"{single['recall_at_3']} | {single['recall_at_5']} | "
            f"{single['recall_at_10']} |"
        )
    lines.extend([
        "",
        "DOCUMENT_LINK and FUNCTIONAL_ANALOGUE are measured in separate namespaces; "
        "documentary links do not admit or exclude functional candidates.",
        "",
        "## IOS2.1 controls",
        "",
    ])
    for key in ("left17_right27", "left18_right24", "left19_right25", "left19_right30"):
        row = controls[key]
        lines.append(f"- {key}: present `{row['present']}`, rank `{row['rank']}`.")
    group = controls["left20_distributed_26_28_29"]
    lines.append(
        f"- LEFT20 → [26,28,29]: `{group['candidate_id']}`, rank `{group['rank']}`, present `{group['present']}`."
    )
    lines.extend([
        "",
        "R30 remains in the LEFT19 candidate set; no page-global exclusivity was applied.",
        "",
        "## Production integration drift",
        "",
    ])
    for page, value in comparison["pages"].items():
        lost = value["fields_lost_during_production_integration"]
        restored = value["fields_restored_in_new_deterministic"]
        lines.append(
            f"- LEFT {page}: lost `{', '.join(lost) if lost else 'none'}`; "
            f"restored `{', '.join(restored) if restored else 'none'}`."
        )
    lines.extend([
        "",
        "Full passports, candidates, channels, ranks, and provenance are in `stage_comparison_ios21.json`.",
        "",
        "## Safety verdict",
        "",
        f"Search failures: `{metrics['overall']['search_failure_count']}`. "
        f"Group-generation failures: `{metrics['overall']['group_generation_failure_count']}`. "
        f"New false conflicts: `{metrics['overall']['new_false_conflict_count']}`.",
        "",
        f"Verdict: **{metrics['verdict']}**",
        "",
    ])
    return "\n".join(lines)


def run(output: Path) -> dict[str, Any]:
    datasets: dict[str, FunctionLineageDataset] = {}
    relations: dict[str, dict[str, Any]] = {}
    projects = []
    all_cases: list[dict[str, Any]] = []
    all_groups: list[dict[str, Any]] = []
    for pair_id in PROJECT_CONFIG:
        dataset, sheet_relations = _load_dataset(pair_id)
        datasets[pair_id] = dataset
        relations[pair_id] = sheet_relations
        project, cases, groups = _project_metrics(pair_id, dataset)
        projects.append(project)
        all_cases.extend({**value, "pair_id": pair_id} for value in cases)
        all_groups.extend({**value, "pair_id": pair_id} for value in groups)
        _write_json(
            output / "candidate_artifacts" / f"{pair_id}.json",
            deterministic_candidate_artifact(
                dataset,
                run_id=(
                    IOS21_FORENSIC_RUN_ID
                    if pair_id == IOS21_PAIR_ID
                    else _read_json(_pair_dir(pair_id) / "production" / "state.json").get("run_id")
                ),
            ),
        )

    controls = _ios21_controls(datasets[IOS21_PAIR_ID], relations[IOS21_PAIR_ID])
    comparison = _ios21_comparison(datasets[IOS21_PAIR_ID])
    group_total = len(all_groups)
    overall = {
        "functional_analogue_recall": _recall(all_cases),
        "single_page_recall": _recall([
            value for value in all_cases
            if len(value["left_pages"]) == 1 and len(value["right_pages"]) == 1
            and value["expected_mode"] != "EXACT_GROUP"
        ]),
        "group_candidate_recall": {
            "case_count": group_total,
            "exact_group_hits": sum(value["present"] for value in all_groups),
            "recall": (
                round(sum(value["present"] for value in all_groups) / group_total, 6)
                if group_total else None
            ),
        },
        "search_failure_count": sum(len(value["search_failures"]) for value in projects),
        "group_generation_failure_count": sum(len(value["group_generation_failures"]) for value in projects),
        "new_false_conflict_count": sum(len(value["new_false_conflicts"]) for value in projects),
    }
    ready = (
        controls["all_passed"]
        and overall["functional_analogue_recall"]["recall_at_10"] is not None
        and overall["functional_analogue_recall"]["recall_at_10"] >= 0.9
        and overall["group_candidate_recall"]["recall"] == 1.0
        and overall["new_false_conflict_count"] == 0
    )
    metrics = {
        "kind": "function_lineage_deterministic_benchmark",
        "algorithm_version": ALGORITHM_VERSION,
        "production_base_commit": PRODUCTION_BASE_COMMIT,
        "production_base_release": PRODUCTION_BASE_RELEASE,
        "selector_executed": False,
        "model_calls": 0,
        "production_run_executed": False,
        "shadow_enabled": False,
        "deployed": False,
        "materialization_changed": False,
        "verifier_changed": False,
        "sheet_matcher_v3_changed": False,
        "projects": projects,
        "overall": overall,
        "ios21_controls_passed": controls["all_passed"],
        "verdict": (
            "A — deterministic candidate layer готов к isolated AI repeat"
            if ready else
            "B — Function Passport ещё недостаточен"
        ),
    }
    _write_json(output / "metrics.json", metrics)
    _write_json(output / "ios21_controls.json", controls)
    _write_json(output / "stage_comparison_ios21.json", comparison)
    (output / "report.md").write_text(
        _report(metrics, controls, comparison), encoding="utf-8"
    )
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    metrics = run(args.output.resolve())
    print(json.dumps({
        "output": str(args.output.resolve()),
        "model_calls": metrics["model_calls"],
        "overall": metrics["overall"],
        "verdict": metrics["verdict"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
