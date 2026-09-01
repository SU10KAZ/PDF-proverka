#!/usr/bin/env python3
"""Run the HRO question-closure experiment above frozen AI Analyst v3."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison.ai import gateway  # noqa: E402
from backend.app.services.stage_comparison.ai_v31 import (  # noqa: E402
    QuestionClosureSelector,
    analyze_question_closure,
    apply_closure_gate,
    build_pending_manual_audit,
    evaluate_closure_gate,
    materialize_closure_run,
)
from scripts.stage_comparison_ai_v2_experiment import (  # noqa: E402
    _fast_input_manifest,
)
from scripts.stage_comparison_ai_v3_experiment import _artifacts  # noqa: E402


def _load(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"artifact does not exist: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: root is not an object")
    return value


def _write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _v3_inputs(run_dirs: list[Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if len(run_dirs) != 3:
        raise ValueError("exactly three --v3-run-dir values are required")
    return (
        [_load(value / "run.json") for value in run_dirs],
        [_load(value / "manual_audit.json") for value in run_dirs],
    )


def _analyze(
    *, production_dir: Path, v3_run_dirs: list[Path], output_dir: Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    fast, artifacts = _artifacts(production_dir)
    runs, audits = _v3_inputs(v3_run_dirs)
    source_factories = [_load(value / "candidate_factory.json") for value in v3_run_dirs]
    signatures = {
        str(value.get("candidate_set_signature") or "") for value in source_factories
    }
    if len(signatures) != 1:
        raise ValueError("v3 candidate factory differs across source runs")
    contracts, analysis, tasks = analyze_question_closure(
        hro_plan=artifacts["human_review_plan"],
        factory=source_factories[0],
        v3_runs=runs,
        v3_audits=audits,
        direct_page=fast["direct_page_mode2"],
    )
    _write(output_dir / "question_closure_contracts.json", contracts)
    _write(output_dir / "closure_analysis.json", analysis)
    _write(output_dir / "closure_ai_tasks.json", tasks)
    return contracts, analysis, tasks


def _run_one(
    *,
    production_dir: Path,
    v3_run_dirs: list[Path],
    output_dir: Path,
    pair_id: str,
    run_number: int,
) -> dict[str, Any]:
    contracts, _analysis, tasks = _analyze(
        production_dir=production_dir,
        v3_run_dirs=v3_run_dirs,
        output_dir=output_dir,
    )
    runtime = gateway.validate_runtime(require_vision=False, deep=False)
    if not runtime.get("ok"):
        raise RuntimeError("AI runtime is not ready: " + "; ".join(runtime.get("problems") or ()))
    fast, artifacts = _artifacts(production_dir)
    manifest = _fast_input_manifest(production_dir, fast)
    selector = QuestionClosureSelector(
        artifacts=artifacts,
        pair_id=pair_id,
        routed_tasks=tasks["tasks"],
        fast_input_signature=manifest["input_signature"],
        prompt_capture_dir=output_dir / "prompt_payloads" / f"run_{run_number}",
        run_id=f"ai-v31-closure-run-{run_number}",
    )
    selector_run = selector.run()
    source_signature = next(iter({
        str(_load(value / "candidate_factory.json").get("candidate_set_signature") or "")
        for value in v3_run_dirs
    }))
    if selector_run.get("source_candidate_set_signature") != source_signature:
        raise ValueError("rebuilt v3 candidate factory does not match frozen source runs")
    result = materialize_closure_run(
        run_number=run_number,
        contracts_artifact=contracts,
        tasks_artifact=tasks,
        selector_run=selector_run,
    )
    _write(output_dir / f"closure_run_{run_number}.json", result)
    return result


def _run_artifacts(output_dir: Path) -> list[dict[str, Any]]:
    return [_load(output_dir / f"closure_run_{number}.json") for number in (1, 2, 3)]


def _prepare_audit(output_dir: Path) -> dict[str, Any]:
    contracts = _load(output_dir / "question_closure_contracts.json")
    audit = build_pending_manual_audit(
        contracts_artifact=contracts,
        run_artifacts=_run_artifacts(output_dir),
    )
    _write(output_dir / "manual_closure_audit.json", audit)
    return audit


def _gate(output_dir: Path) -> dict[str, Any]:
    contracts = _load(output_dir / "question_closure_contracts.json")
    analysis = _load(output_dir / "closure_analysis.json")
    audit = _load(output_dir / "manual_closure_audit.json")
    gate = evaluate_closure_gate(
        contracts_artifact=contracts,
        run_artifacts=_run_artifacts(output_dir),
        manual_audit=audit,
    )
    contracts = apply_closure_gate(contracts_artifact=contracts, gate=gate)
    stable = set(map(str, gate.get("stable_closed_question_ids") or ()))
    for question in analysis.get("questions") or ():
        question["closed_by_v31_gate"] = str(question.get("question_id") or "") in stable
    analysis["final_gate"] = {
        "verdict": gate["verdict"],
        "stable_closed_question_ids": gate["stable_closed_question_ids"],
        "stable_hro_after": gate["stable_hro_after"],
    }
    _write(output_dir / "question_closure_contracts.json", contracts)
    _write(output_dir / "closure_analysis.json", analysis)
    _write(output_dir / "closure_gate.json", gate)
    return gate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("production_dir", type=Path)
    parser.add_argument("--pair-id", required=True)
    parser.add_argument("--v3-run-dir", action="append", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--phase",
        choices=("analyze", "run1", "run2", "run3", "prepare-audit", "gate"),
        required=True,
    )
    args = parser.parse_args()
    production_dir = args.production_dir.resolve()
    output_dir = args.output_dir.resolve()
    v3_run_dirs = [value.resolve() for value in args.v3_run_dir]
    if output_dir == production_dir or production_dir in output_dir.parents:
        parser.error("output-dir must be outside production artifacts")
    try:
        if args.phase == "analyze":
            _contracts, analysis, tasks = _analyze(
                production_dir=production_dir,
                v3_run_dirs=v3_run_dirs,
                output_dir=output_dir,
            )
            result = {
                "closure_analysis": analysis["summary"],
                "routed_ai_tasks": len(tasks["tasks"]),
            }
        elif args.phase.startswith("run"):
            run_number = int(args.phase[-1])
            run = _run_one(
                production_dir=production_dir,
                v3_run_dirs=v3_run_dirs,
                output_dir=output_dir,
                pair_id=args.pair_id,
                run_number=run_number,
            )
            result = {
                "run_number": run_number,
                "closed_question_ids": run["provisional_closed_question_ids"],
                "hro_after": run["hro_after"],
                "model_calls": run["model_calls"],
                "runtime_ms": run["runtime_ms"],
            }
        elif args.phase == "prepare-audit":
            audit = _prepare_audit(output_dir)
            result = {"manual_audit_status": audit["status"], "items": len(audit["items"])}
        else:
            gate = _gate(output_dir)
            result = {
                "verdict": gate["verdict"],
                "stable_closed_question_ids": gate["stable_closed_question_ids"],
                "hro_runs": gate["hro_runs"],
                "model_calls": gate["total_model_calls"],
                "runtime_ms": gate["total_runtime_ms"],
            }
    except (RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
