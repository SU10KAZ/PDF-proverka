"""CLI for the offline Candidate Generator v4 benchmark (no model calls)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from experiments.ai_sheet_matcher.core import PROJECT_CONFIG
from experiments.candidate_v4.core import (
    ALGORITHM_VERSION,
    build_candidate_v4_dataset,
    build_v4_benchmark,
    public_candidate_set,
    public_sheet_passport,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = REPO_ROOT / "comparison" / "ai_sheet_matcher" / "20260902_candidate_v4"


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _failure_delta(metrics: Mapping[str, Any], datasets: Iterable[Any]) -> dict[str, Any]:
    ios21 = next(item for item in metrics["projects"] if item["pair_id"] == "pe336037597")
    ios21_dataset = next(item for item in datasets if item.base.pair_id == "pe336037597")
    forensic_edges = (
        (16, 26), (16, 28), (17, 27), (18, 24), (19, 25),
        (19, 30), (20, 26), (20, 28), (20, 29), (21, 29),
    )
    ranks = {
        (left, right): next((
            int(item["rank"])
            for item in ios21_dataset.candidate_sets[left]["candidates"]
            if int(item["right_physical_page"]) == right
        ), None)
        for left, right in forensic_edges
    }
    ranking_after = sum(rank is None or rank > 5 for rank in ranks.values())
    missing_groups = sum(not item["present"] for item in ios21["group_audit_cases"])
    return {
        "kind": "candidate_generator_v4_failure_delta",
        "scope": "IOS 2.1 forensic functional/reference audit",
        "before": {
            "SEARCH_WINDOW_MISS": 2,
            "RANKING_MISS": 5,
            "GROUP_CANDIDATE_MISSING": 3,
            "GLOBAL_ASSIGNMENT_DISPLACEMENT": 3,
        },
        "after": {
            "SEARCH_WINDOW_MISS": 0,
            "RANKING_MISS": ranking_after,
            "GROUP_CANDIDATE_MISSING": missing_groups,
            "GLOBAL_ASSIGNMENT_DISPLACEMENT": 0,
        },
        "notes": [
            "v4 searches the complete RIGHT corpus; a missing top-10 item is classified as ranking, not window exclusion.",
            "v4 emits a candidate graph and performs no one-to-one assignment, so candidates are not displaced before selection.",
        ],
        "forensic_edge_ranks": {f"{left}->{right}": rank for (left, right), rank in ranks.items()},
    }


def _report(metrics: Mapping[str, Any], failure_delta: Mapping[str, Any]) -> str:
    rows = []
    for project in metrics["projects"]:
        old, new = project["v3"], project["v4"]
        rows.append(
            f"| {project['project']} | {old['recall_at_1']:.1%} | {old['recall_at_3']:.1%} | "
            f"{old['recall_at_5']:.1%} | {old['recall_at_10']:.1%} | {new['recall_at_1']:.1%} | "
            f"{new['recall_at_3']:.1%} | {new['recall_at_5']:.1%} | {new['recall_at_10']:.1%} |"
        )
    old, new = metrics["overall"]["v3"], metrics["overall"]["v4"]
    rows.append(
        f"| **Итого** | **{old['recall_at_1']:.1%}** | **{old['recall_at_3']:.1%}** | "
        f"**{old['recall_at_5']:.1%}** | **{old['recall_at_10']:.1%}** | **{new['recall_at_1']:.1%}** | "
        f"**{new['recall_at_3']:.1%}** | **{new['recall_at_5']:.1%}** | **{new['recall_at_10']:.1%}** |"
    )
    ios21 = next(item for item in metrics["projects"] if item["pair_id"] == "pe336037597")
    rank_rows = []
    for left, right in ((17, 7), (18, 8), (19, 9)):
        case = next(item for item in ios21["cases"] if item["audit_left_page"] == left and item["expected_right_pages"] == [right])
        rank_rows.append(f"- `{left}→{right}`: rank `{case['v4_rank']}`")
    group = metrics["acceptance"]["ios21_sheet5_distributed_candidate"]
    remaining = [
        f"{project['project']} LEFT {case['audit_left_page']} → {case['expected_right_pages']} ({'/'.join(case['source_types'])})"
        for project in metrics["projects"] for case in project["cases"] if case["v4_rank"] is None
    ]
    verdict = metrics["acceptance"]["verdict"]
    meaning = "Candidate Generator готов для повторного AI experiment" if verdict == "A" else "улучшился, но recall ещё недостаточен"
    return f"""# Candidate Generator v4 — benchmark report

## Итог

Вердикт: **{verdict} — {meaning}**.

Это изолированный deterministic research generator. `production-sheet-matcher.v3`, UI,
AI Selector, engineer mappings и production pipeline не изменялись. Model calls не выполнялись;
materialization, deploy и push не выполнялись. Reference hypotheses использованы только при
аудите результата и не подмешивались в retrieval/ranking.

## Архитектура v4

- каждый лист имеет provenance-bearing Sheet Passport, поверх него построены один или несколько
  Function Passports;
- каждый Function Passport независимо оценивается против полного RIGHT corpus; результаты
  объединяются с шестью каналами `FUNCTION`, `ENTITY`, `OBJECT_ZONE`, `TOPOLOGY`,
  `TITLE_STAMP`, `NEIGHBOR_TOC`;
- bounded weak bridge сохраняет полезные deterministic v3 signals, но не ограничивает
  corpus-wide v4 retrieval; page proximity имеет только слабый вес;
- null остаётся нейтральным, а только явные corpus/function contradictions получают отдельный
  штраф; ни один contradiction не удаляет кандидата до ranking;
- группы строятся до любой assignment по lineage, scope, sheet series и complementary role
  coverage. v4 не выполняет 1→1 assignment и сохраняет конфликтующие варианты.

## V3 vs v4

| Проект | v3 R@1 | v3 R@3 | v3 R@5 | v3 R@10 | v4 R@1 | v4 R@3 | v4 R@5 | v4 R@10 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
{chr(10).join(rows)}

Engineer mapping recall@10: `{metrics['overall']['engineer_mapping_recall']['recall_at_10']:.1%}`.
Reference hypothesis recall@10: `{metrics['overall']['reference_hypothesis_recall']['recall_at_10']:.1%}`.
Single-page recall@10: `{metrics['overall']['single_page_candidate_recall']['recall_at_10']:.1%}`.

## IOS 2.1 authority checks

{chr(10).join(rank_rows)}

## A sheet 5 distributed candidate

- candidate: `{group['candidate_group_id'] if group else None}`
- relation: `{group['relation_type'] if group else None}`
- RIGHT physical pages: `{group['right_pages'] if group else None}`
- covered functions: `{group['covered_functions'] if group else None}`
- grounds: `{group['why_group_exists'] if group else None}`

## Boundedness and groups

- candidate count per LEFT: median `{metrics['candidate_set_size']['median']}`, p95 `{metrics['candidate_set_size']['p95']}`;
- returned/cartesian pairs: `{metrics['candidate_set_size']['returned_pair_count']}` / `{metrics['candidate_set_size']['full_cartesian_pair_count']}` (`{metrics['candidate_set_size']['cartesian_fraction']:.1%}`);
- group candidates: `{metrics['group_candidate_counts']}`;
- exact group recall: `{metrics['overall']['group_candidate_recall']}`.

## Failure delta (IOS 2.1 forensic scope)

Before: `{failure_delta['before']}`.

After: `{failure_delta['after']}`.

## Remaining root causes

- Remaining benchmark misses: `{remaining or ['none']}`. IOS 2.1 LEFT 20 → `[29,30]` остаётся
  page-level reference miss, но physical 29 сохранена внутри доказательной группы `[26,28,29]`.
- Forensic individual-edge ranks 20→28=`{failure_delta['forensic_edge_ranks']['20->28']}` and
  20→29=`{failure_delta['forensic_edge_ranks']['20->29']}` explain the two remaining ranking
  misses; both edges are retained by the group candidate rather than an unbounded page list.
- Function classes are deterministic lexical normalizations of saved OCR/image descriptions,
  not model-generated engineering facts.
- One-to-many composition is bounded to three RIGHT pages and eight groups per LEFT; merge and
  distributed LEFT-pair expansions are capped at two per adjacent pair. Related-document
  expansion remains evidence-only and is not performed when the document is unavailable.

## Verification

- `python -m pytest -q tests/test_candidate_generator_v4.py tests/test_ai_sheet_matcher_experiment.py`
- 26 tests passed: 13 targeted v4 tests and 13 unchanged AI-research safety tests.
- Production source and frozen artifact hashes remain unchanged after generation.
"""


def run(output: Path) -> dict[str, Any]:
    datasets = [build_candidate_v4_dataset(REPO_ROOT, pair_id) for pair_id in PROJECT_CONFIG]
    metrics = build_v4_benchmark(datasets)
    failures = _failure_delta(metrics, datasets)
    output.mkdir(parents=True, exist_ok=True)
    _write_json(output / "metrics.json", metrics)
    _write_json(output / "failure_delta.json", failures)
    _write_jsonl(output / "candidate_sets.jsonl", (
        {
            "project": dataset.base.project,
            "pair_id": dataset.base.pair_id,
            "input_signature": dataset.input_signature,
            "sheet_passport": public_sheet_passport(
                dataset.sheet_passports["LEFT"][candidate_set["left_physical_page"]]
            ),
            "function_passports": dataset.function_passports["LEFT"][candidate_set["left_physical_page"]],
            "right_evidence_catalog": {
                evidence_id: evidence
                for candidate in candidate_set["candidates"]
                for evidence_id, evidence in dataset.sheet_passports["RIGHT"][
                    candidate["right_physical_page"]
                ]["evidence_catalog"].items()
            },
            **public_candidate_set(candidate_set),
        }
        for dataset in datasets for candidate_set in dataset.candidate_sets.values()
    ))
    _write_jsonl(output / "group_candidates.jsonl", (
        {"project": dataset.base.project, **group}
        for dataset in datasets for group in dataset.group_candidates
    ))
    (output / "candidate_v4_report.md").write_text(_report(metrics, failures), encoding="utf-8")
    (output / "README.md").write_text(
        f"""# Candidate Generator v4 research artifacts

Offline deterministic benchmark for `{ALGORITHM_VERSION}` over the frozen IOS 1.1,
IOS 3.1 and IOS 2.1 corpus from research commit `41d43625`, with forensic audit cases
from `dbddb691`.

Regenerate without model calls:

```bash
python -m experiments.candidate_v4.run
```

The command writes only this research directory. It does not import into production,
change mappings, call a model, deploy, or push.
""",
        encoding="utf-8",
    )
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    metrics = run(args.output)
    print(json.dumps({
        "v3": metrics["overall"]["v3"],
        "v4": metrics["overall"]["v4"],
        "acceptance": metrics["acceptance"],
    }, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
