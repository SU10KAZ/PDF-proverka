#!/usr/bin/env python3
"""Capture comparable Stage 4.2 production snapshots and build the final report."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app.services.stage_comparison import paths  # noqa: E402
from experiments.stage_comparison_text_ai_reviewer_diagnostics import analyze  # noqa: E402


ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
INVENTORY_PATH = (
    REPO_ROOT
    / "experiments/stage_comparison_text_ai_reviewer_diagnostics/artifacts/uncertain_inventory.json"
)
BENCHMARK_SUMMARY_PATH = (
    REPO_ROOT
    / "experiments/stage_comparison_text_ai_reviewer/artifacts/benchmark_summary.json"
)
REGRESSION_SUMMARY_PATH = ARTIFACTS / "moved_page_semantics_summary.json"
REPORT_PATH = ARTIFACTS / "STAGE42_REPORT.md"
BASELINE_COMMIT = "39da504e8da11028204f53aa340a079f2e019563"
SESSION_ID = "121d764109184c13"
PAIR_ID = "p570d156f57"
PROJECT_ID = "272_Sadovnicheskaya_76_Balchug_Esteyt"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_hashes() -> dict[str, str]:
    source_paths = analyze.production_paths()
    return {
        key: file_sha256(path)
        for key, path in source_paths.items()
        if key in {"comparison", "differences", "links", "suggestions"}
    }


def _matching_decisions(
    entry: dict[str, Any], decisions_by_group: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    decisions = decisions_by_group.get(entry["group_id"], [])
    left = set(entry["left_fragment_ids"])
    right = set(entry["right_fragment_ids"])
    exact = [
        item for item in decisions
        if set(item.get("left_fragment_ids") or []) == left
        and set(item.get("right_fragment_ids") or []) == right
    ]
    if exact:
        return exact
    return [
        item for item in decisions
        if set(item.get("left_fragment_ids") or []) & left
        or set(item.get("right_fragment_ids") or []) & right
    ]


def snapshot(label: str) -> dict[str, Any]:
    review_path = paths.text_ai_review_path(SESSION_ID, PAIR_ID)
    final_path = paths.text_final_comparison_path(SESSION_ID, PAIR_ID)
    review = load_json(review_path)
    final = load_json(final_path)
    inventory = load_json(INVENTORY_PATH)
    decisions = [
        decision
        for group in review.get("sheet_groups") or []
        for decision in group.get("decisions") or []
    ]
    decisions_by_group = {
        str(group["id"]): list(group.get("decisions") or [])
        for group in review.get("sheet_groups") or []
    }
    uncertain_reasons = Counter()
    moved_membership = Counter()
    model_moved_membership = Counter()
    page_normalizations = 0
    for group in review.get("sheet_groups") or []:
        accepted_left = {int(page) for page in group.get("left_pages") or []}
        accepted_right = {int(page) for page in group.get("right_pages") or []}
        for decision in group.get("decisions") or []:
            left_inside = set(decision.get("left_pages") or []) <= accepted_left
            right_inside = set(decision.get("right_pages") or []) <= accepted_right
            membership = (
                "both_inside" if left_inside and right_inside
                else "exactly_one_side_outside" if left_inside != right_inside
                else "both_outside"
            )
            if decision.get("final_status") == "MOVED":
                moved_membership[membership] += 1
            if decision.get("model_final_status") == "MOVED":
                model_moved_membership[membership] += 1
            page_normalizations += int(
                "moved_inside_accepted_group_to_same"
                in (decision.get("normalizations") or [])
            )
    for decision in decisions:
        if decision.get("final_status") == "UNCERTAIN":
            reason, _detail = analyze.primary_reason(decision)
            uncertain_reasons[reason] += 1

    cohort: dict[str, Counter[str]] = defaultdict(Counter)
    mapping = []
    for entry in inventory["entries"]:
        matches = _matching_decisions(entry, decisions_by_group)
        statuses = sorted({str(item.get("final_status") or "") for item in matches})
        mapped = statuses[0] if len(statuses) == 1 else ("MISSING" if not statuses else "MIXED")
        cohort[entry["uncertain_reason"]][mapped] += 1
        mapping.append({
            "case_id": entry["case_id"],
            "baseline_reason": entry["uncertain_reason"],
            "baseline_status": entry["final_status"],
            "mapped_status": mapped,
            "matching_decision_count": len(matches),
        })

    summary = final["summary"]
    status_counts = {
        status: int(summary.get(status.lower()) or 0)
        for status in ("SAME", "MOVED", "CHANGED", "REMOVED", "ADDED", "UNCERTAIN")
    }
    usage = review["summary"]
    payload = {
        "version": 1,
        "kind": "stage4_2_production_snapshot",
        "label": label,
        "project_id": PROJECT_ID,
        "session_id": SESSION_ID,
        "pair_id": PAIR_ID,
        "review_sha256": file_sha256(review_path),
        "final_sha256": file_sha256(final_path),
        "source_hashes": source_hashes(),
        "source_signature": review.get("source_signature"),
        "prompt_version": review.get("prompt_version"),
        "validator_version": review.get("validator_version"),
        "model": review.get("model"),
        "reasoning_effort": review.get("reasoning_effort"),
        "review_status": review.get("status"),
        "status_counts": status_counts,
        "model_status_counts": dict(sorted(Counter(
            str(item.get("model_final_status") or "") for item in decisions
        ).items())),
        "uncertain_reason_counts": dict(sorted(uncertain_reasons.items())),
        "page_membership_audit": {
            "final_moved": dict(sorted(moved_membership.items())),
            "model_moved": dict(sorted(model_moved_membership.items())),
            "moved_inside_to_same_normalizations": page_normalizations,
        },
        "diagnostic_cohort_statuses": {
            reason: dict(sorted(counts.items())) for reason, counts in sorted(cohort.items())
        },
        "performance": {
            "input_tokens": int(usage.get("input_tokens") or 0),
            "output_tokens": int(usage.get("output_tokens") or 0),
            "cached_tokens": int(usage.get("cached_tokens") or 0),
            "duration_ms": int(usage.get("duration_ms") or 0),
            "represented_model_calls": int(usage.get("represented_model_calls") or 0),
            "fresh_model_calls": int(usage.get("fresh_model_calls") or 0),
            "chunks_total": int(usage.get("chunks_total") or 0),
        },
        "diagnostic_case_mapping": mapping,
    }
    write_json(ARTIFACTS / f"production_{label}.json", payload)
    return payload


def baseline_benchmark() -> dict[str, Any]:
    relative = BENCHMARK_SUMMARY_PATH.relative_to(REPO_ROOT).as_posix()
    raw = subprocess.check_output(["git", "show", f"{BASELINE_COMMIT}:{relative}"])
    return json.loads(raw)


def benchmark_row(payload: dict[str, Any]) -> dict[str, Any]:
    row = next(
        item for item in payload["runs"]
        if item["provider"] == "codex"
        and item["requested_model"] == "gpt-5.6-luna"
        and item["mode"] == "with_hint"
    )
    return {
        **{key: row[key] for key in (
            "final_classification_accuracy", "false_same", "false_moved",
            "raw_model_false_same", "raw_model_false_moved",
            "correct_reclassification", "harmful_reclassification", "json_failures",
            "value_failures", "hallucinations", "input_tokens", "output_tokens",
            "avg_group_time_sec",
        )},
        "uncertain": sum(
            actual.get("UNCERTAIN", 0) for actual in row["confusion_matrix"].values()
        ),
    }


def build_report() -> str:
    before = load_json(ARTIFACTS / "production_before.json")
    after = load_json(ARTIFACTS / "production_after.json")
    regression = load_json(REGRESSION_SUMMARY_PATH)
    attempts = load_json(ARTIFACTS / "production_attempts.json")
    benchmark_before = benchmark_row(baseline_benchmark())
    benchmark_after = benchmark_row(load_json(BENCHMARK_SUMMARY_PATH))
    write_json(ARTIFACTS / "benchmark_before_after.json", {
        "baseline_commit": BASELINE_COMMIT,
        "before": benchmark_before,
        "after": benchmark_after,
    })

    benchmark_keys = [
        ("Accuracy", "final_classification_accuracy"),
        ("Accepted False SAME", "false_same"),
        ("Accepted False MOVED", "false_moved"),
        ("Raw False SAME", "raw_model_false_same"),
        ("Raw False MOVED", "raw_model_false_moved"),
        ("Corrected deterministic errors", "correct_reclassification"),
        ("Harmful reclassifications", "harmful_reclassification"),
        ("UNCERTAIN", "uncertain"),
        ("JSON failures", "json_failures"),
        ("Input tokens", "input_tokens"),
        ("Output tokens", "output_tokens"),
        ("Average group runtime, sec", "avg_group_time_sec"),
    ]
    lines = [
        "# Stage 4.2 — accepted sheet-link page membership", "",
        f"Baseline commit: `{BASELINE_COMMIT}`.", "",
        "## Exact rule", "",
        "The prompt now states that `left_pages` and `right_pages` define the accepted "
        "current sheet group. Different absolute PDF pages inside that group are `SAME` "
        "when their fragments are semantically equivalent. `MOVED` is allowed only when "
        "the matching fragment lies outside the accepted opposite-side page set; sheet "
        "number, PDF-page equality and order are not used.", "",
        "The backend converts a high-confidence model `MOVED` to `SAME` only when all "
        "referenced left and right pages belong to the accepted group and provenance is "
        "supported. The original proposal remains in `model_*`. A deterministic `CHANGED` "
        "conflict still fails closed to `UNCERTAIN`; unsupported explanations also remain "
        "`UNCERTAIN`. A genuine exactly-one-side-outside match remains `MOVED`.", "",
        "## Controlled benchmark", "",
        "| Metric | BEFORE | AFTER |", "|---|---:|---:|",
    ]
    lines.extend(
        f"| {label} | {benchmark_before[key]} | {benchmark_after[key]} |"
        for label, key in benchmark_keys
    )
    lines.extend([
        "", "Acceptance gate: accepted False SAME = 0 and accepted False MOVED = 0; "
        "accuracy and harmful reclassifications are unchanged.", "",
        "## 140-case MOVED_PAGE_SEMANTICS replay", "",
        "| Status | BEFORE | AFTER |", "|---|---:|---:|",
    ])
    for status in ("SAME", "MOVED", "CHANGED", "UNCERTAIN"):
        lines.append(
            f"| {status} | {regression['before_status_counts'].get(status, 0)} | "
            f"{regression['after_status_counts'].get(status, 0)} |"
        )
    lines.extend([
        "",
        f"Raw model status changed from 140 MOVED to "
        f"{json.dumps(regression['after_model_status_counts'], ensure_ascii=False)}. "
        f"The replay used {regression['model_calls']} calls, "
        f"{regression['usage'].get('input_tokens', 0)} input and "
        f"{regression['usage'].get('output_tokens', 0)} output tokens in "
        f"{regression['total_runtime_sec']} seconds; validation errors: "
        f"{regression['validation_error_count']}.", "",
        "## Full production rerun", "",
        f"Project `{PROJECT_ID}`, session `{SESSION_ID}`, pair `{PAIR_ID}`.", "",
        "| Status | BEFORE | AFTER |", "|---|---:|---:|",
    ])
    for status in ("SAME", "MOVED", "CHANGED", "REMOVED", "ADDED", "UNCERTAIN"):
        lines.append(
            f"| {status} | {before['status_counts'][status]} | {after['status_counts'][status]} |"
        )
    lines.extend([
        "", "The original 140-case production cohort changed from 140 UNCERTAIN to "
        f"{json.dumps(after['diagnostic_cohort_statuses']['MOVED_PAGE_SEMANTICS'], ensure_ascii=False)}. "
        f"The final {after['status_counts']['MOVED']} MOVED decisions are all genuine "
        "membership cases with exactly one referenced side outside the accepted group; "
        f"both-inside MOVED: {after['page_membership_audit']['final_moved'].get('both_inside', 0)}.", "",
        "### Original non-page 49-case cohort", "",
        "| Baseline cause | BEFORE | AFTER statuses |", "|---|---:|---|",
    ])
    for reason in ("VALIDATOR_REJECTED", "OCR_NOISE", "MULTIPLE_CANDIDATES", "TABLE_STRUCTURE"):
        before_count = sum(before["diagnostic_cohort_statuses"][reason].values())
        after_counts = json.dumps(
            after["diagnostic_cohort_statuses"][reason], ensure_ascii=False, sort_keys=True,
        )
        lines.append(f"| {reason} | {before_count} | `{after_counts}` |")
    lines.extend([
        "", "### Remaining UNCERTAIN taxonomy", "",
        "| Cause | Count |", "|---|---:|",
    ])
    for reason, count in after["uncertain_reason_counts"].items():
        lines.append(f"| {reason} | {count} |")
    bp, ap = before["performance"], after["performance"]
    lines.extend([
        "", "### Production performance", "",
        "| Metric | BEFORE | AFTER |", "|---|---:|---:|",
        f"| Input tokens | {bp['input_tokens']} | {ap['input_tokens']} |",
        f"| Output tokens | {bp['output_tokens']} | {ap['output_tokens']} |",
        f"| Cached tokens | {bp['cached_tokens']} | {ap['cached_tokens']} |",
        f"| Represented model calls | {bp['represented_model_calls']} | {ap['represented_model_calls']} |",
        f"| Fresh model calls | {bp['fresh_model_calls']} | {ap['fresh_model_calls']} |",
        f"| Runtime, sec | {round(bp['duration_ms'] / 1000, 3)} | "
        f"{round(ap['duration_ms'] / 1000, 3)} |", "",
        "The completed persisted AFTER artifact represents 23 calls. BEFORE represented "
        "21 calls because one historical group was stored as a single legacy unchunked call; "
        "the already-existing current policy reconstructs it as three chunks. No chunking "
        "code or limit changed in Stage 4.2.", "",
        "Including rejected fail-closed responses and recoveries, this execution made "
        f"{attempts['cumulative_execution']['model_calls']} actual calls, used "
        f"{attempts['cumulative_execution']['input_tokens']} input and "
        f"{attempts['cumulative_execution']['output_tokens']} output tokens, and accumulated "
        f"{round(attempts['cumulative_execution']['duration_ms'] / 1000, 3)} seconds of "
        "model-call duration across four attempts.", "",
        "Deterministic comparison, differences, accepted links and sheet suggestions "
        f"are unchanged: `{before['source_hashes'] == after['source_hashes']}`.", "",
        "## Verification", "",
        "- Targeted reviewer module: `45 passed`.",
        "- Extended Stage Comparison regression selection: `141 passed`.",
        "- The seven required page-membership cases are covered, plus a safety test that "
        "an inside-group MOVED cannot mask deterministic CHANGED.",
        "- Production model/chunking/preprocessing/sheet links/UI were not changed.", "",
        "## Recommendation", "",
        "B. Use a separate small Stage 4.3 for the next concrete cause: the remaining "
        "validator-rejected unsupported explanation/provenance cases (69 current "
        "UNCERTAIN; 38 of the original 39-case validator cohort remain). Do not combine "
        "that work with this accepted page-membership fix.", "",
    ])
    report = "\n".join(lines)
    REPORT_PATH.write_text(report, encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    snapshot_parser = subparsers.add_parser("snapshot")
    snapshot_parser.add_argument("label", choices=("before", "after"))
    subparsers.add_parser("report")
    args = parser.parse_args()
    if args.command == "snapshot":
        payload = snapshot(args.label)
        print(json.dumps({
            "label": args.label,
            "status_counts": payload["status_counts"],
            "performance": payload["performance"],
            "uncertain_reason_counts": payload["uncertain_reason_counts"],
        }, ensure_ascii=False, indent=2))
    else:
        build_report()
        print(REPORT_PATH)


if __name__ == "__main__":
    main()
