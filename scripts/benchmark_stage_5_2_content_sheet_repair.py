#!/usr/bin/env python3
"""Benchmark Stage 5.2 on real fingerprints plus controlled link corruption.

Ground truth is selected independently from unique exact titles in real sheet
indexes.  Titles are then removed or made ambiguous before the content planner
runs.  No OCR, PDF parsing, graphics, vision, or model call is performed.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app.services.stage_comparison import project_change_summary, sheet_link_repair  # noqa: E402
from backend.app.services.stage_comparison import sheet_matching  # noqa: E402
from backend.app.services.stage_comparison.content_sheet_link_repair import (  # noqa: E402
    MIN_IMPROVEMENT,
    MIN_MARGIN,
    MIN_REVERSE_MARGIN,
    MIN_SCORE,
    assess_content_candidates,
)
from backend.app.services.stage_comparison.sheet_content_fingerprint import (  # noqa: E402
    build_sheet_content_fingerprint,
)


BENCHMARK_VERSION = 1
DEFAULT_PAIR_ROOT = REPO_ROOT / "comparison/sessions/121d764109184c13/pairs"
KNOWN_REAL_REVIEW = {
    "p26c08b83a6": (31, 29, "IOS_P31_RD29"),
    "p570d156f57": (14, 13, "AR_P14_RD13"),
}
CALIBRATION_PAIR_IDS = {
    "p16b108b9f5",  # AR1
    "p69de8daf0e",  # ODI
    "pba35af454b",  # KR2
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _split_for_pair(pair_id: str) -> str:
    if pair_id in KNOWN_REAL_REVIEW:
        return "holdout"
    return "calibration" if pair_id in CALIBRATION_PAIR_IDS else "holdout"


def _load_indexes(pair_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    pair = _read_json(pair_dir / "pair.json")
    indexes: dict[str, list[dict[str, Any]]] = {}
    for side in ("left", "right"):
        document = pair.get(side) or {}
        html_path = Path(str(document.get("html_path") or ""))
        md_path = Path(str(document.get("md_path") or ""))
        if not html_path.is_file() or not md_path.is_file():
            raise FileNotFoundError(f"structured inputs unavailable for {pair_dir.name}:{side}")
        index = sheet_matching.extract_sheet_index_from_results_html(
            html_path.read_text(encoding="utf-8")
        )
        semantics = sheet_matching.extract_page_semantics_from_markdown(
            md_path.read_text(encoding="utf-8")
        )
        for record in index:
            semantic_text = semantics.get(int(record["pdf_page"]))
            if semantic_text:
                record["_semantic_text"] = semantic_text
        indexes[side] = index
    suggestions = sheet_matching.match_sheet_indexes(indexes["left"], indexes["right"])
    return pair, suggestions


def _confirmed_pairs(suggestions: dict[str, Any]) -> list[tuple[int, int]]:
    left = suggestions.get("left_sheet_index") or []
    right = suggestions.get("right_sheet_index") or []
    left_titles = [sheet_link_repair.normalize_title(str(item.get("title") or "")) for item in left]
    right_titles = [sheet_link_repair.normalize_title(str(item.get("title") or "")) for item in right]
    left_counts, right_counts = Counter(left_titles), Counter(right_titles)
    right_by_title = {
        title: int(item["pdf_page"])
        for item, title in zip(right, right_titles)
        if title and right_counts[title] == 1 and item.get("content_fingerprint")
    }
    exact = [
        (int(item["pdf_page"]), right_by_title[title])
        for item, title in zip(left, left_titles)
        if (
            title and left_counts[title] == 1 and title in right_by_title
            and item.get("content_fingerprint")
        )
    ]
    left_by_page = {int(item["pdf_page"]): item for item in left}
    right_by_page = {int(item["pdf_page"]): item for item in right}
    permitted_reasons = {
        "same_sheet_number_and_title", "same_unique_title", "similar_title",
        "same_sheet_number_and_sequence", "sequence_repaired_sheet_number",
        "same_equipment_identifier", "same_equipment_identifier_group",
    }
    high_suggestions = [
        item for item in suggestions.get("suggestions") or []
        if (
            item.get("confidence") == "high"
            and len(item.get("primary_right_pages") or []) == 1
            and set(item.get("reason") or []) & permitted_reasons
        )
    ]
    claimed_right = Counter(
        int(item["primary_right_pages"][0]) for item in high_suggestions
    )
    independent = [
        (int(item["left_page"]), int(item["primary_right_pages"][0]))
        for item in high_suggestions
        if (
            claimed_right[int(item["primary_right_pages"][0])] == 1
            and (left_by_page.get(int(item["left_page"])) or {}).get("content_fingerprint")
            and (right_by_page.get(int(item["primary_right_pages"][0])) or {}).get("content_fingerprint")
        )
    ]
    return list(dict.fromkeys([*exact, *independent]))


def _hidden_titles(suggestions: dict[str, Any], replacement: str = "") -> dict[str, Any]:
    hidden = copy.deepcopy(suggestions)
    for side in ("left_sheet_index", "right_sheet_index"):
        for record in hidden.get(side) or []:
            record["title"] = replacement
    return hidden


def _link(link_id: str, left: int | list[int], right: int | list[int]) -> dict[str, Any]:
    return {
        "id": link_id,
        "left_pages": left if isinstance(left, list) else [left],
        "right_pages": right if isinstance(right, list) else [right],
        "source": "manual",
        "confidence": "manual",
        "reason": ["controlled_corruption"],
    }


def _case(
    *, case_id: str, split: str, kind: str, pair_id: str,
    suggestions: dict[str, Any], links: list[dict[str, Any]],
    expected_pairs: set[tuple[int, int]] | None,
    source_groups: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "split": split,
        "kind": kind,
        "pair_id": pair_id,
        "suggestions": suggestions,
        "links": links,
        "expected_pairs": expected_pairs,
        "source_groups": source_groups or [],
    }


def _controlled_cases(
    pair_id: str, suggestions: dict[str, Any], *, split: str | None = None,
    offset: int = 0,
) -> list[dict[str, Any]]:
    split = split or _split_for_pair(pair_id)
    confirmed = _confirmed_pairs(suggestions)[offset:offset + 12]
    if len(confirmed) < 2:
        return []
    hidden = _hidden_titles(suggestions)
    generic = _hidden_titles(suggestions, "Общие данные")
    cases: list[dict[str, Any]] = []
    for index, (left_page, right_page) in enumerate(confirmed[:6]):
        wrong_right = confirmed[(index + 1) % len(confirmed)][1]
        cases.append(_case(
            case_id=f"{pair_id}-{split}-correct-{offset + index + 1:02d}", split=split,
            kind="confirmed_correct_existing", pair_id=pair_id, suggestions=hidden,
            links=[_link("controlled", left_page, right_page)], expected_pairs=None,
        ))
        cases.append(_case(
            case_id=f"{pair_id}-{split}-wrong-{offset + index + 1:02d}", split=split,
            kind="controlled_wrong_link_missing_title", pair_id=pair_id,
            suggestions=hidden, links=[_link("controlled", left_page, wrong_right)],
            expected_pairs={(left_page, right_page)},
        ))
        cases.append(_case(
            case_id=f"{pair_id}-{split}-generic-{offset + index + 1:02d}", split=split,
            kind="controlled_generic_duplicated_title", pair_id=pair_id,
            suggestions=generic, links=[_link("controlled", left_page, wrong_right)],
            expected_pairs={(left_page, right_page)},
        ))

    for index in range(0, min(6, len(confirmed) - 1), 2):
        first, second = confirmed[index:index + 2]
        links = [
            _link("swap-a", first[0], second[1]),
            _link("swap-b", second[0], first[1]),
        ]
        cases.append(_case(
            case_id=f"{pair_id}-{split}-swap-{offset + index // 2 + 1:02d}", split=split,
            kind="controlled_swap", pair_id=pair_id, suggestions=hidden, links=links,
            expected_pairs={first, second},
        ))
    if len(confirmed) >= 3:
        first, second, third = confirmed[:3]
        cases.append(_case(
            case_id=f"{pair_id}-{split}-cycle-{offset + 1:02d}", split=split, kind="controlled_3_cycle",
            pair_id=pair_id, suggestions=hidden,
            links=[
                _link("cycle-a", first[0], second[1]),
                _link("cycle-b", second[0], third[1]),
                _link("cycle-c", third[0], first[1]),
            ],
            expected_pairs={first, second, third},
        ))

    first, second = confirmed[:2]
    ambiguous = copy.deepcopy(hidden)
    right_index = ambiguous.get("right_sheet_index") or []
    source = next(item for item in right_index if int(item["pdf_page"]) == first[1])
    duplicate = copy.deepcopy(source)
    duplicate["pdf_page"] = max(int(item["pdf_page"]) for item in right_index) + 1
    duplicate["sheet_number"] = f"AMB-{duplicate['pdf_page']}"
    right_index.append(duplicate)
    cases.extend([
        _case(
            case_id=f"{pair_id}-{split}-ambiguous-{offset + 1:02d}", split=split,
            kind="controlled_ambiguous_content", pair_id=pair_id,
            suggestions=ambiguous,
            links=[_link("ambiguous", first[0], second[1])], expected_pairs=None,
        ),
        _case(
            case_id=f"{pair_id}-{split}-many-{offset + 1:02d}", split=split,
            kind="many_to_many", pair_id=pair_id, suggestions=hidden,
            links=[_link("many", [first[0], second[0]], [first[1], second[1]])],
            expected_pairs=None,
        ),
        _case(
            case_id=f"{pair_id}-{split}-split-{offset + 1:02d}", split=split,
            kind="split_merge", pair_id=pair_id, suggestions=hidden,
            links=[_link("split", [first[0], second[0]], first[1])],
            expected_pairs=None,
        ),
    ])
    return cases


def _real_review_case(pair_id: str, pair_dir: Path, suggestions: dict[str, Any]) -> dict[str, Any] | None:
    if pair_id not in KNOWN_REAL_REVIEW:
        return None
    left_page, right_page, label = KNOWN_REAL_REVIEW[pair_id]
    links_payload = _read_json(pair_dir / "sheet_links.json")
    selected = [
        link for link in links_payload.get("links") or []
        if left_page in (link.get("left_pages") or [])
        and right_page in (link.get("right_pages") or [])
    ]
    final_path = pair_dir / "text_final_comparison.json"
    if not selected or not final_path.is_file():
        return None
    source_groups = project_change_summary.build_source_groups(_read_json(final_path))
    return _case(
        case_id=f"{pair_id}-{label}", split="falsification", kind="real_review",
        pair_id=pair_id, suggestions=suggestions, links=selected,
        expected_pairs=None, source_groups=source_groups,
    )


def _composite_cases(
    split: str, sources: list[tuple[str, dict[str, Any]]], *, offset: int = 0,
) -> list[dict[str, Any]]:
    """Build objective swap/cycle controls from rich real confirmed sheet pairs."""
    candidates: list[tuple[int, str, dict[str, Any], dict[str, Any]]] = []
    keys = ("unique_designations", "system_names", "equipment_codes", "structural_tokens")
    for pair_id, suggestions in sources:
        left_by_page = {
            int(item["pdf_page"]): item for item in suggestions.get("left_sheet_index") or []
        }
        right_by_page = {
            int(item["pdf_page"]): item for item in suggestions.get("right_sheet_index") or []
        }
        for left_page, right_page in _confirmed_pairs(suggestions)[offset:]:
            left = left_by_page[left_page]
            right = right_by_page[right_page]
            left_fp, right_fp = left["content_fingerprint"], right["content_fingerprint"]
            richness = sum(
                len(set(left_fp.get(key) or []) & set(right_fp.get(key) or []))
                for key in keys
            )
            if richness >= 4:
                candidates.append((richness, pair_id, left, right))
    selected: list[tuple[int, str, dict[str, Any], dict[str, Any]]] = []
    used_sources: Counter[str] = Counter()
    for candidate in sorted(candidates, key=lambda item: (-item[0], item[1], item[2]["pdf_page"])):
        if used_sources[candidate[1]] >= 1:
            continue
        selected.append(candidate)
        used_sources[candidate[1]] += 1
        if len(selected) >= 5:
            break
    if len(selected) < 3:
        return []
    left_records: list[dict[str, Any]] = []
    right_records: list[dict[str, Any]] = []
    for page, (_richness, _source, left, right) in enumerate(selected, 1):
        left_copy, right_copy = copy.deepcopy(left), copy.deepcopy(right)
        for record in (left_copy, right_copy):
            record["pdf_page"] = page
            record["sheet_number"] = str(page)
            record["title"] = ""
        left_records.append(left_copy)
        right_records.append(right_copy)
    suggestions = {"left_sheet_index": left_records, "right_sheet_index": right_records}
    cases = [_case(
        case_id=f"{split}-composite-swap", split=split,
        kind="controlled_composite_swap", pair_id=f"{split}-composite",
        suggestions=suggestions,
        links=[_link("swap-a", 1, 2), _link("swap-b", 2, 1)],
        expected_pairs={(1, 1), (2, 2)},
    )]
    cases.append(_case(
        case_id=f"{split}-composite-cycle", split=split,
        kind="controlled_composite_3_cycle", pair_id=f"{split}-composite",
        suggestions=suggestions,
        links=[_link("cycle-a", 1, 2), _link("cycle-b", 2, 3), _link("cycle-c", 3, 1)],
        expected_pairs={(1, 1), (2, 2), (3, 3)},
    ))
    return cases


def _deterministic_anchor_controls() -> list[dict[str, Any]]:
    """Strong discipline-neutral controls for atomic operation coverage."""
    left_text = (
        "Узел стойки. Труба FHV-101 80x4. Анкер BSR-M12. Разрез крепления.",
        "Схема насосной. Насос PUMP-202. Шкаф SHU-202. Датчик PS-202.",
        "План фасада. Панель FAS-303. Кронштейн KR-303. Ось ZONE-303.",
    )
    right_text = (
        "Монтажный узел стойки. Труба FHV-101 80x4. Анкер BSR-M12. Разрез крепления.",
        "Принципиальная схема насосной. Насос PUMP-202. Шкаф SHU-202. Датчик PS-202.",
        "Фасадный план. Панель FAS-303. Кронштейн KR-303. Ось ZONE-303.",
    )
    def records(values: tuple[str, ...]) -> list[dict[str, Any]]:
        return [
            {
                "pdf_page": page, "sheet_number": str(page), "title": "",
                "content_fingerprint": build_sheet_content_fingerprint(text),
            }
            for page, text in enumerate(values, 1)
        ]
    suggestions = {
        "left_sheet_index": records(left_text),
        "right_sheet_index": records(right_text),
    }
    return [
        _case(
            case_id="calibration-anchor-wrong", split="calibration",
            kind="controlled_wrong_link_missing_title", pair_id="anchor-control",
            suggestions=suggestions, links=[_link("wrong", 1, 2)],
            expected_pairs={(1, 1)},
        ),
        _case(
            case_id="calibration-anchor-swap", split="calibration",
            kind="controlled_swap", pair_id="anchor-control", suggestions=suggestions,
            links=[_link("swap-a", 1, 2), _link("swap-b", 2, 1)],
            expected_pairs={(1, 1), (2, 2)},
        ),
        _case(
            case_id="calibration-anchor-cycle", split="calibration",
            kind="controlled_3_cycle", pair_id="anchor-control", suggestions=suggestions,
            links=[
                _link("cycle-a", 1, 2), _link("cycle-b", 2, 3),
                _link("cycle-c", 3, 1),
            ],
            expected_pairs={(1, 1), (2, 2), (3, 3)},
        ),
    ]


def _evaluate(case: dict[str, Any]) -> dict[str, Any]:
    links_payload = {
        "version": 1,
        "pair_id": case["pair_id"],
        "links": case["links"],
        "unlinked_left_pages": [],
        "updated_at": "benchmark",
    }
    problem_ids = {str(item["id"]) for item in case["links"]}
    plan = sheet_link_repair.plan_repairs(
        links_payload,
        case["suggestions"],
        problem_ids,
        source_groups=case["source_groups"],
    )
    actual_pairs = (
        {
            (int(item["left_pages"][0]), int(item["right_pages"][0]))
            for item in plan["after_snapshot"]["links"]
        }
        if plan else set()
    )
    expected = case["expected_pairs"]
    expected_auto = expected is not None
    correct_auto = bool(plan) and expected_auto and actual_pairs == expected
    false_auto = bool(plan) and (not expected_auto or actual_pairs != expected)
    assessment = assess_content_candidates(
        links_payload, case["suggestions"], problem_ids, case["source_groups"],
    )
    compact_assessment = [
        {
            key: item.get(key)
            for key in (
                "group_id", "left_sheet", "right_sheet_before", "right_sheet_after",
                "confidence", "auto_repair", "decision_reasons", "current_score",
                "best_score", "second_score", "margin", "reverse_margin",
                "improvement_over_current", "unique_anchors", "mutual_best",
            )
        }
        for item in assessment
    ]
    return {
        "case_id": case["case_id"],
        "split": case["split"],
        "kind": case["kind"],
        "pair_id": case["pair_id"],
        "expected_auto_repair": expected_auto,
        "expected_pairs": sorted([list(pair) for pair in expected or set()]),
        "actual_pairs": sorted([list(pair) for pair in actual_pairs]),
        "auto_repaired": bool(plan),
        "correct_auto_repair": correct_auto,
        "false_auto_repair": false_auto,
        "assessment": compact_assessment,
    }


def _metrics(results: list[dict[str, Any]]) -> dict[str, Any]:
    expected = sum(bool(item["expected_auto_repair"]) for item in results)
    correct = sum(bool(item["correct_auto_repair"]) for item in results)
    false = sum(bool(item["false_auto_repair"]) for item in results)
    automatic = sum(bool(item["auto_repaired"]) for item in results)
    by_kind: dict[str, dict[str, int]] = {}
    for item in results:
        row = by_kind.setdefault(item["kind"], {"cases": 0, "correct": 0, "false": 0})
        row["cases"] += 1
        row["correct"] += int(bool(item["correct_auto_repair"]))
        row["false"] += int(bool(item["false_auto_repair"]))
    def recovery(kinds: set[str]) -> dict[str, Any]:
        selected = [item for item in results if item["kind"] in kinds]
        recovered = sum(bool(item["correct_auto_repair"]) for item in selected)
        return {
            "cases": len(selected), "recovered": recovered,
            "rate": round(recovered / len(selected), 6) if selected else 1.0,
        }
    return {
        "cases": len(results),
        "expected_auto_repairs": expected,
        "automatic_repairs": automatic,
        "correct_auto_repairs": correct,
        "false_auto_repairs": false,
        "auto_repair_precision": round(correct / automatic, 6) if automatic else 1.0,
        "auto_repair_recall": round(correct / expected, 6) if expected else 1.0,
        "ambiguous_left_untouched": all(
            not item["auto_repaired"] for item in results
            if item["kind"] == "controlled_ambiguous_content"
        ),
        "many_to_many_untouched": all(
            not item["auto_repaired"] for item in results
            if item["kind"] in {"many_to_many", "split_merge"}
        ),
        "swap_recovery": recovery({"controlled_swap", "controlled_composite_swap"}),
        "three_cycle_recovery": recovery({
            "controlled_3_cycle", "controlled_composite_3_cycle",
        }),
        "wrong_link_recovery": recovery({
            "controlled_wrong_link_missing_title", "controlled_generic_duplicated_title",
        }),
        "by_kind": by_kind,
    }


def run(pair_root: Path, selected_split: str) -> dict[str, Any]:
    cases: list[dict[str, Any]] = _deterministic_anchor_controls()
    skipped: list[dict[str, str]] = []
    sources: dict[str, list[tuple[str, dict[str, Any]]]] = {
        "calibration": [], "falsification": [], "holdout": [],
    }
    for pair_dir in sorted(path for path in pair_root.iterdir() if path.is_dir()):
        try:
            _pair, suggestions = _load_indexes(pair_dir)
        except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            skipped.append({"pair_id": pair_dir.name, "reason": str(error)})
            continue
        base_split = _split_for_pair(pair_dir.name)
        if base_split == "calibration":
            sources["calibration"].append((pair_dir.name, suggestions))
            cases.extend(_controlled_cases(
                pair_dir.name, suggestions, split="calibration", offset=0,
            ))
        else:
            sources["falsification"].append((pair_dir.name, suggestions))
            sources["holdout"].append((pair_dir.name, suggestions))
            cases.extend(_controlled_cases(
                pair_dir.name, suggestions, split="falsification", offset=0,
            ))
            cases.extend(_controlled_cases(
                pair_dir.name, suggestions, split="holdout", offset=6,
            ))
        real = _real_review_case(pair_dir.name, pair_dir, suggestions)
        if real:
            cases.append(real)
    for split, split_sources in sources.items():
        cases.extend(_composite_cases(
            split, split_sources, offset=6 if split == "holdout" else 0,
        ))
    selected = [case for case in cases if selected_split == "all" or case["split"] == selected_split]
    results = [_evaluate(case) for case in selected]
    split_metrics = {
        split: _metrics([item for item in results if item["split"] == split])
        for split in ("calibration", "falsification", "holdout")
        if any(item["split"] == split for item in results)
    }
    return {
        "version": BENCHMARK_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "pair_root": str(pair_root),
            "ground_truth": "unique_exact_real_titles_hidden_before_content_evaluation",
            "structured_inputs_only": True,
            "ocr_or_pdf_parser_run": False,
            "llm_used": False,
        },
        "thresholds": {
            "minimum_score": MIN_SCORE,
            "minimum_margin": MIN_MARGIN,
            "minimum_reverse_margin": MIN_REVERSE_MARGIN,
            "minimum_improvement": MIN_IMPROVEMENT,
        },
        "selected_split": selected_split,
        "historical_falsification": {
            "algorithm": "content_v1_before_grid_axis_guard",
            "cases": 115,
            "automatic_repairs": 11,
            "correct_auto_repairs": 8,
            "false_auto_repairs": 3,
            "precision": 0.727273,
            "finding": "drawing_grid_axes_were_incorrectly_treated_as_strong_designations",
        },
        "metrics": _metrics(results),
        "split_metrics": split_metrics,
        "skipped_pairs": skipped,
        "cases": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-root", type=Path, default=DEFAULT_PAIR_ROOT)
    parser.add_argument(
        "--split", choices=("calibration", "falsification", "holdout", "all"),
        default="all",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    artifact = run(args.pair_root.resolve(), args.split)
    encoded = json.dumps(artifact, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(json.dumps({
        "selected_split": artifact["selected_split"],
        "metrics": artifact["metrics"],
        "split_metrics": artifact["split_metrics"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
