#!/usr/bin/env python3
"""Stage 4.2 regression over all 140 diagnosed page-semantics cases."""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app.services.stage_comparison import text_ai_reviewer as reviewer  # noqa: E402
from experiments.stage_comparison_text_ai_reviewer import benchmark  # noqa: E402
from experiments.stage_comparison_text_ai_reviewer_diagnostics import analyze  # noqa: E402


ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
INVENTORY_PATH = (
    REPO_ROOT
    / "experiments/stage_comparison_text_ai_reviewer_diagnostics/artifacts/uncertain_inventory.json"
)
DATASET_PATH = ARTIFACTS / "moved_page_semantics_dataset.json"
RUN_PATH = ARTIFACTS / "moved_page_semantics_run.json"
SUMMARY_PATH = ARTIFACTS / "moved_page_semantics_summary.json"
MODEL = "gpt-5.6-luna"
EFFORT = "medium"
BATCH_SIZE = 20


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_dataset() -> dict[str, Any]:
    inventory = load_json(INVENTORY_PATH)
    source_groups = {
        str(group["group_id"]): group for group in analyze.load_sources()["groups"]
    }
    entries = [
        item for item in inventory["entries"]
        if item["uncertain_reason"] == "MOVED_PAGE_SEMANTICS"
    ]
    if len(entries) != 140:
        raise RuntimeError(f"expected 140 MOVED_PAGE_SEMANTICS cases, got {len(entries)}")

    cases = []
    for entry in entries:
        if entry["model_status"] != "MOVED" or entry["final_status"] != "UNCERTAIN":
            raise RuntimeError(f"unexpected baseline state for {entry['case_id']}")
        parent = source_groups[entry["group_id"]]
        case = {
            "group_id": entry["case_id"],
            "parent_group_id": entry["group_id"],
            "decision_index": entry["decision_index"],
            "left_pages": list(parent.get("left_pages") or []),
            "right_pages": list(parent.get("right_pages") or []),
            "source_left": list(entry["source_left"]),
            "source_right": list(entry["source_right"]),
            "required_fragment_ids": {
                "left": list(entry["left_fragment_ids"]),
                "right": list(entry["right_fragment_ids"]),
            },
            "preliminary": list(entry["preliminary_evidence"]),
            "baseline": {
                "deterministic_status": entry["deterministic_status"],
                "model_final_status": entry["model_status"],
                "final_status": entry["final_status"],
                "confidence": entry["confidence"],
                "policy_reason": entry["validator_reason"],
            },
            "trace": entry["trace"],
        }
        cases.append(case)

    core = {
        "version": 1,
        "kind": "stage4_2_moved_page_semantics_regression_dataset",
        "source_inventory_sha256": inventory["inventory_sha256"],
        "source_review_sha256": inventory["source_review_sha256"],
        "prompt_version": reviewer.PROMPT_VERSION,
        "validator_version": reviewer.VALIDATOR_VERSION,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "batch_size": BATCH_SIZE,
        "cases": cases,
    }
    core["dataset_sha256"] = benchmark.sha256_json(core)
    write_json(DATASET_PATH, core)
    return core


def run(dataset: dict[str, Any], *, timeout: float, resume: bool) -> dict[str, Any]:
    batches: list[dict[str, Any]] = []
    if resume and RUN_PATH.is_file():
        existing = load_json(RUN_PATH)
        if existing.get("dataset_sha256") == dataset["dataset_sha256"]:
            batches = list(existing.get("batches") or [])
    completed = {
        group_id for batch in batches for group_id in batch.get("group_ids") or []
    }
    cases = dataset["cases"]
    for start in range(0, len(cases), BATCH_SIZE):
        batch = cases[start:start + BATCH_SIZE]
        batch_ids = [case["group_id"] for case in batch]
        if set(batch_ids) <= completed:
            continue
        print(f"[{MODEL}] cases {start + 1}-{start + len(batch)}/{len(cases)}", flush=True)
        call = benchmark.invoke(
            "codex", MODEL, reviewer.prompt_for_groups(batch, include_hint=True), timeout,
        )
        batches.append({"group_ids": batch_ids, **call})
        payload = {
            "version": 1,
            "kind": "stage4_2_moved_page_semantics_regression_run",
            "dataset_sha256": dataset["dataset_sha256"],
            "prompt_version": reviewer.PROMPT_VERSION,
            "validator_version": reviewer.VALIDATOR_VERSION,
            "provider": "codex",
            "requested_model": MODEL,
            "reasoning_effort": EFFORT,
            "native_json_schema_enforced": True,
            "batches": batches,
        }
        write_json(RUN_PATH, payload)
    return load_json(RUN_PATH)


def combined_status(decisions: list[dict[str, Any]]) -> str:
    if len(decisions) == 1:
        return str(decisions[0]["final_status"])
    statuses = {str(item["final_status"]) for item in decisions}
    if statuses == {"REMOVED", "ADDED"}:
        return "REMOVED_ADDED"
    return "MIXED"


def summarize(dataset: dict[str, Any], run_payload: dict[str, Any]) -> dict[str, Any]:
    cases = {case["group_id"]: case for case in dataset["cases"]}
    outcomes: dict[str, dict[str, Any]] = {}
    usage: Counter[str] = Counter()
    total_runtime = 0.0
    failed_calls = 0
    reported_models: set[str] = set()
    for call in run_payload["batches"]:
        total_runtime += float(call.get("elapsed_sec") or 0)
        usage.update({
            key: int(value) for key, value in (call.get("usage") or {}).items()
            if isinstance(value, int)
        })
        if call.get("reported_model"):
            reported_models.add(str(call["reported_model"]))
        if not call.get("ok"):
            failed_calls += 1
            for group_id in call["group_ids"]:
                outcomes[group_id] = {
                    "after_status": "UNCERTAIN",
                    "validation_error": call.get("error") or "provider_failure",
                    "decisions": [],
                }
            continue
        try:
            response = json.loads(call.get("answer") or "")
            raw_groups = response.get("groups") if isinstance(response, dict) else None
            if not isinstance(raw_groups, list):
                raise ValueError("root_schema")
            by_id = {
                str(item.get("group_id") or ""): item
                for item in raw_groups if isinstance(item, dict)
            }
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            failed_calls += 1
            for group_id in call["group_ids"]:
                outcomes[group_id] = {
                    "after_status": "UNCERTAIN",
                    "validation_error": f"response_parse:{exc}",
                    "decisions": [],
                }
            continue
        for group_id in call["group_ids"]:
            try:
                validated = reviewer.validate_group_response(by_id.get(group_id), cases[group_id])
            except reviewer.ReviewValidationError as exc:
                outcomes[group_id] = {
                    "after_status": "UNCERTAIN",
                    "validation_error": str(exc),
                    "decisions": [],
                }
            else:
                decisions = validated["decisions"]
                outcomes[group_id] = {
                    "after_status": combined_status(decisions),
                    "validation_error": None,
                    "decisions": decisions,
                }

    missing = set(cases) - set(outcomes)
    if missing:
        raise RuntimeError(f"missing regression outcomes: {sorted(missing)}")
    before = Counter(case["baseline"]["final_status"] for case in cases.values())
    before_model = Counter(case["baseline"]["model_final_status"] for case in cases.values())
    after = Counter(item["after_status"] for item in outcomes.values())
    after_model = Counter(
        decision.get("model_final_status")
        for item in outcomes.values()
        for decision in item["decisions"]
    )
    traces = []
    for group_id, case in cases.items():
        result = outcomes[group_id]
        traces.append({
            "case_id": group_id,
            "parent_group_id": case["parent_group_id"],
            "decision_index": case["decision_index"],
            "accepted_pages": {
                "left": case["left_pages"], "right": case["right_pages"],
            },
            "referenced_pages": {
                "left": sorted({item["page"] for item in case["source_left"]}),
                "right": sorted({item["page"] for item in case["source_right"]}),
            },
            "deterministic_status": case["baseline"]["deterministic_status"],
            "before_model_status": case["baseline"]["model_final_status"],
            "before_status": case["baseline"]["final_status"],
            **result,
        })
    summary = {
        "version": 1,
        "kind": "stage4_2_moved_page_semantics_regression_summary",
        "dataset_sha256": dataset["dataset_sha256"],
        "source_inventory_sha256": dataset["source_inventory_sha256"],
        "prompt_version": reviewer.PROMPT_VERSION,
        "validator_version": reviewer.VALIDATOR_VERSION,
        "provider": "codex",
        "requested_model": MODEL,
        "reported_models": sorted(reported_models),
        "reasoning_effort": EFFORT,
        "total_cases": len(cases),
        "before_status_counts": dict(sorted(before.items())),
        "before_model_status_counts": dict(sorted(before_model.items())),
        "after_status_counts": dict(sorted(after.items())),
        "after_model_status_counts": dict(sorted(
            (str(key), value) for key, value in after_model.items() if key
        )),
        "validation_error_count": sum(
            bool(item["validation_error"]) for item in outcomes.values()
        ),
        "failed_model_calls": failed_calls,
        "model_calls": len(run_payload["batches"]),
        "total_runtime_sec": round(total_runtime, 3),
        "usage": dict(sorted(usage.items())),
        "cases": traces,
    }
    write_json(SUMMARY_PATH, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("build")
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--timeout", type=float, default=300)
    run_parser.add_argument("--no-resume", action="store_true")
    subparsers.add_parser("summarize")
    args = parser.parse_args()

    if args.command == "build":
        dataset = build_dataset()
        print(f"cases={len(dataset['cases'])} sha256={dataset['dataset_sha256']}")
    elif args.command == "run":
        dataset = load_json(DATASET_PATH)
        result = run(dataset, timeout=args.timeout, resume=not args.no_resume)
        print(f"calls={len(result['batches'])}")
    else:
        dataset = load_json(DATASET_PATH)
        result = summarize(dataset, load_json(RUN_PATH))
        print(json.dumps({
            "total_cases": result["total_cases"],
            "before": result["before_status_counts"],
            "after": result["after_status_counts"],
            "calls": result["model_calls"],
            "runtime_sec": result["total_runtime_sec"],
            "usage": result["usage"],
            "validation_error_count": result["validation_error_count"],
        }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
