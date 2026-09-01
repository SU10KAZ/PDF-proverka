#!/usr/bin/env python3
"""Run AI Analyst v3 without mutating production FAST/HRO artifacts."""
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
from backend.app.services.stage_comparison.ai_v3 import settings  # noqa: E402
from backend.app.services.stage_comparison.ai_v3.engine import (  # noqa: E402
    BoundedSelectorAnalyst,
    MODES,
)
from backend.app.services.stage_comparison.ai_v3.materialization import (  # noqa: E402
    materialize_stable_selections,
    pending_manual_audit,
)
from scripts.stage_comparison_ai_v2_experiment import (  # noqa: E402
    ARTIFACT_NAMES,
    _fast_baseline,
    _fast_input_manifest,
)


def _load(directory: Path, name: str) -> dict[str, Any]:
    path = directory / f"{name}.json"
    if not path.is_file():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: root is not an object")
    return value


def _write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def _artifacts(production_dir: Path) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    fast = {name: _load(production_dir, name) for name in ARTIFACT_NAMES}
    artifacts = {**fast, "human_review_plan": _load(production_dir, "human_review_plan")}
    return fast, artifacts


def _ensure_audit(path: Path, run: dict[str, Any]) -> dict[str, Any]:
    expected = {
        str(value.get("task_id") or "")
        for value in run.get("stable_selections") or ()
        if value.get("status") == "VERIFIED_SELECTION"
    }
    existing = _load(path.parent, path.stem)
    current = {
        str(value.get("task_id") or "")
        for value in existing.get("items") or () if isinstance(value, dict)
    }
    if existing.get("status") == "COMPLETE" and current == expected:
        return existing
    value = pending_manual_audit(run)
    _write(path, value)
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("production_dir", type=Path)
    parser.add_argument("--pair-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mode", choices=MODES, default="unanimity")
    parser.add_argument("--cache", choices=("enabled", "disabled"), default="enabled")
    parser.add_argument("--materialize-only", action="store_true")
    parser.add_argument("--skip-runtime-check", action="store_true")
    args = parser.parse_args()
    settings.require_enabled()
    production_dir = args.production_dir.resolve()
    output_dir = args.output_dir.resolve()
    if not production_dir.is_dir():
        parser.error(f"artifact directory does not exist: {production_dir}")
    if output_dir == production_dir or production_dir in output_dir.parents:
        parser.error("output-dir must be outside production artifacts")
    fast, artifacts = _artifacts(production_dir)
    required = [name for name in ("state", "direct_page_mode2", "unified_synthesis", "ai_routing_inventory", "preliminary_report") if not fast[name]]
    if required:
        parser.error("missing FAST artifacts: " + ", ".join(required))
    if not artifacts["human_review_plan"]:
        parser.error("missing frozen production human_review_plan.json")

    manifest = _fast_input_manifest(production_dir, fast)
    if args.materialize_only:
        factory = _load(output_dir, "candidate_factory")
        run = _load(output_dir, "run")
        audit = _load(output_dir, "manual_audit")
        if not factory or not run:
            parser.error("materialize-only requires candidate_factory.json and run.json")
    else:
        if not args.skip_runtime_check:
            runtime = gateway.validate_runtime(require_vision=False, deep=False)
            _write(output_dir / "runtime_check.json", runtime)
            if not runtime.get("ok"):
                print("AI runtime is not ready: " + "; ".join(runtime.get("problems") or ()), file=sys.stderr)
                return 2
        analyst = BoundedSelectorAnalyst(
            artifacts=artifacts,
            pair_id=args.pair_id,
            mode=args.mode,
            fast_input_signature=manifest["input_signature"],
            cache_dir=output_dir / "response_cache",
            cache_enabled=args.cache == "enabled",
            prompt_capture_dir=output_dir / "prompt_payloads",
        )
        run = analyst.run()
        factory = analyst.factory
        _write(output_dir / "fast_baseline.json", _fast_baseline(fast))
        _write(output_dir / "fast_input_manifest.json", manifest)
        _write(output_dir / "candidate_factory.json", factory)
        _write(output_dir / "candidate_bundles.json", analyst.bundles)
        _write(output_dir / "selector_batches.json", {
            "shared_context": analyst.shared_context,
            "shared_context_signature": analyst.shared_context_signature,
            "batches": run["selector_batches"],
        })
        for pass_identity, rows in run["selector_passes"].items():
            _write(output_dir / f"selector_{pass_identity}.json", {
                "pass_identity": pass_identity, "batches": rows,
            })
        _write(output_dir / "stable_selections.json", {
            "stability_mode": run["stability_mode"],
            "selections": run["stable_selections"],
        })
        _write(output_dir / "verifier.json", {
            "verifier_version": "stage-comparison-ai-v3-selection-verifier.v1",
            "checks": run["verifier"],
        })
        _write(output_dir / "run.json", run)
        audit = _ensure_audit(output_dir / "manual_audit.json", run)

    materialization = materialize_stable_selections(
        artifacts=artifacts,
        factory=factory,
        run=run,
        pair_id=args.pair_id,
        manual_audit=audit,
    )
    _write(output_dir / "materialization.json", materialization)
    _write(output_dir / "hro_after_v3.json", materialization["human_review_plan"])
    print(json.dumps({
        "output_dir": str(output_dir),
        "mode": run.get("stability_mode"),
        "candidate_set_signature": factory.get("candidate_set_signature"),
        "diagnostics": run.get("diagnostics"),
        "materialization": materialization.get("diagnostics"),
        "manual_audit_status": audit.get("status"),
    }, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
