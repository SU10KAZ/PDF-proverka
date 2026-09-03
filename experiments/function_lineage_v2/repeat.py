"""Run the isolated Function Lineage v2 AI repeat over frozen artifacts.

The harness has three deliberately separate phases:

``prepare``
    Validate the candidate commit and write an immutable-input manifest.
``experiment``
    Deserialize (never regenerate) the tracked deterministic artifacts and run
    three independent stateless Pass A/B observations for every project.
``finalize``
    Re-run the unchanged verifier over the saved responses and write the
    stability, capacity, safety, cost, and IOS2.1 forensic reports.

Nothing in this module writes below ``comparison/sessions`` or changes a
runtime feature flag.  It is a research-only consumer of the candidate files
created by :mod:`experiments.function_lineage_v2.run`.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import subprocess
import time
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from backend.app.services.stage_comparison.ai import gateway as ai_gateway
from backend.app.services.stage_comparison.ai import settings as ai_settings
from experiments.ai_sheet_matcher.core import PROJECT_CONFIG, canonical_json


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_COMMIT = "2bcb832f51c46867c56d49d81549d9cac5918e96"
PRODUCTION_BASE_COMMIT = "5eb6fa144c3124e8926f5e8c69c546827b878ff8"
PRODUCTION_BASE_RELEASE = "ui-real-5eb6fa14"
SOURCE_INPUT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_deterministic" / "candidate_artifacts"
)
DEFAULT_OUTPUT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_ai_repeat"
)
OLD_IOS21_MAP = (
    REPO_ROOT / "comparison" / "sessions" / "7cccec69bb0b4327"
    / "pairs" / "pe336037597" / "production" / "function_lineage_map.json"
)
DETERMINISTIC_IOS21_COMPARISON = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_deterministic" / "stage_comparison_ios21.json"
)
IOS21_PAIR_ID = "pe336037597"
OLD_IOS21_RUN_ID = "prun_8a28eb85d3ca435c5b04577e"
DEFAULT_MODEL = "gpt-5.6-sol"
DEFAULT_EFFORT = "low"
DEFAULT_WORKERS = 3
PREVIOUS_V4_TOKENS = 7_315_563
FUNCTION_LINEAGE_V1_TOKENS = 1_327_912
PASSES = ("A", "B")
COLD_RUNS = (1, 2, 3)
PAIR_ORDER = tuple(PROJECT_CONFIG)
COMPLEX_RELATIONS = (
    "SPLIT_1_TO_N", "MERGED_N_TO_1", "FUNCTION_DISTRIBUTED",
)
FORBIDDEN_PATHS = (
    REPO_ROOT / "docs" / "diverse_corpus_restore.md",
    REPO_ROOT / "scripts" / "restore_diverse_corpus.py",
)
PRODUCTION_SOURCES = (
    REPO_ROOT / "backend" / "app" / "services" / "stage_comparison"
    / "function_lineage_shadow.py",
    REPO_ROOT / "backend" / "app" / "services" / "stage_comparison"
    / "function_lineage_source.py",
    REPO_ROOT / "backend" / "app" / "services" / "stage_comparison"
    / "production_orchestrator.py",
)


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


def _write_jsonl(path: Path, values: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(canonical_json(dict(value)) + "\n" for value in values),
        encoding="utf-8",
    )


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_file(path: Path) -> str:
    return _sha_bytes(path.read_bytes())


def _sha_json(value: Any) -> str:
    return _sha_bytes(canonical_json(value).encode("utf-8"))


def _git(*args: str) -> str:
    return subprocess.check_output(
        ["git", *args], cwd=REPO_ROOT, text=True,
    ).strip()


def _tracked_clean(path: Path) -> bool:
    return subprocess.run(
        ["git", "diff", "--quiet", "HEAD", "--", str(path.relative_to(REPO_ROOT))],
        cwd=REPO_ROOT,
        check=False,
    ).returncode == 0


def _source_paths_unchanged_at_head(paths: Sequence[Path]) -> bool:
    return subprocess.run(
        [
            "git", "diff", "--quiet", f"{SOURCE_COMMIT}..HEAD", "--",
            *(str(path.relative_to(REPO_ROOT)) for path in paths),
        ],
        cwd=REPO_ROOT,
        check=False,
    ).returncode == 0


def _candidate_is_ancestor() -> bool:
    return subprocess.run(
        ["git", "merge-base", "--is-ancestor", SOURCE_COMMIT, "HEAD"],
        cwd=REPO_ROOT,
        check=False,
    ).returncode == 0


def _artifact_path(pair_id: str) -> Path:
    return SOURCE_INPUT / f"{pair_id}.json"


def _project(pair_id: str) -> str:
    return str(PROJECT_CONFIG[pair_id]["project"])


def _dataset(raw: Mapping[str, Any]) -> lineage.FunctionLineageDataset:
    return lineage.FunctionLineageDataset(
        pair_id=str(raw["pair_id"]),
        sheet_passports={
            side: {int(page): copy.deepcopy(value) for page, value in rows.items()}
            for side, rows in (raw.get("sheet_passports") or {}).items()
        },
        function_passports=copy.deepcopy(dict(raw["function_passports"])),
        function_fragments=copy.deepcopy(dict(raw["function_fragments"])),
        evidence_catalog=copy.deepcopy(dict(raw["evidence_catalog"])),
        document_link_map={
            "relation_namespace": lineage.RELATION_DOCUMENT_LINK,
            "links": copy.deepcopy(list(raw.get("document_links") or [])),
        },
        candidates={
            str(value["candidate_id"]): copy.deepcopy(value)
            for value in raw.get("functional_candidates") or []
        },
        tasks=copy.deepcopy(list(raw.get("candidate_tasks") or [])),
        input_signature=str(raw["input_signature"]),
    )


def _load_datasets() -> dict[str, lineage.FunctionLineageDataset]:
    return {
        pair_id: _dataset(_read_json(_artifact_path(pair_id)))
        for pair_id in PAIR_ORDER
    }


def _logical_hashes(raw: Mapping[str, Any]) -> dict[str, str]:
    candidates = list(raw.get("functional_candidates") or [])
    tasks = list(raw.get("candidate_tasks") or [])
    return {
        "function_passports_sha256": _sha_json(raw.get("function_passports")),
        "function_fragments_sha256": _sha_json(raw.get("function_fragments")),
        "candidate_ids_order_sha256": _sha_json([
            value.get("candidate_id") for value in candidates
        ]),
        "functional_candidates_sha256": _sha_json(candidates),
        "task_ids_order_sha256": _sha_json([
            value.get("task_id") for value in tasks
        ]),
        "candidate_tasks_sha256": _sha_json(tasks),
        "evidence_catalog_sha256": _sha_json(raw.get("evidence_catalog")),
        "document_links_sha256": _sha_json(raw.get("document_links")),
    }


def _prompt_template(prompt_a: str) -> str:
    marker = "Independent verification pass A."
    if not prompt_a.startswith(marker):
        raise RuntimeError("selector prompt template marker changed")
    return prompt_a.replace(marker, "Independent verification pass {PASS}.", 1)


def _flag_snapshot() -> dict[str, Any]:
    return {
        "shadow_enabled": ai_settings.function_lineage_shadow_enabled(),
        "shadow_pair_allowlist": sorted(
            ai_settings.function_lineage_shadow_pair_allowlist()
        ),
        "shadow_run_allowlist": sorted(
            ai_settings.function_lineage_shadow_run_allowlist()
        ),
        "materialization_enabled": (
            ai_settings.function_lineage_materialization_enabled()
        ),
    }


def _assert_flags_off() -> dict[str, Any]:
    flags = _flag_snapshot()
    if flags["shadow_enabled"] or flags["materialization_enabled"]:
        raise RuntimeError(f"production Function Lineage flags are not off: {flags}")
    return flags


def _source_safety() -> dict[str, Any]:
    head = _git("rev-parse", "HEAD")
    parent = _git("rev-parse", f"{SOURCE_COMMIT}^")
    pinned_paths = (
        *PRODUCTION_SOURCES,
        REPO_ROOT / "experiments" / "function_lineage_v2" / "run.py",
        *(_artifact_path(pair_id) for pair_id in PAIR_ORDER),
    )
    if not _candidate_is_ancestor():
        raise RuntimeError(f"candidate commit {SOURCE_COMMIT} is not an ancestor of {head}")
    if not _source_paths_unchanged_at_head(pinned_paths):
        raise RuntimeError(
            "lineage sources or frozen candidate artifacts changed after candidate commit"
        )
    if parent != PRODUCTION_BASE_COMMIT:
        raise RuntimeError(
            f"candidate commit parent must be production baseline, got {parent}"
        )
    dirty = [str(path.relative_to(REPO_ROOT)) for path in PRODUCTION_SOURCES if not _tracked_clean(path)]
    if dirty:
        raise RuntimeError(f"production sources differ from candidate commit: {dirty}")
    return {
        "checkout_head": head,
        "lineage_source_commit": SOURCE_COMMIT,
        "lineage_source_paths_unchanged_since_candidate": True,
        "production_base_commit": parent,
        "production_base_release": PRODUCTION_BASE_RELEASE,
        "origin_main": _git("rev-parse", "origin/main"),
        "production_sources_clean": True,
        "production_source_sha256": {
            str(path.relative_to(REPO_ROOT)): _sha_file(path)
            for path in PRODUCTION_SOURCES
        },
        "research_harness_sha256": _sha_file(Path(__file__)),
        "forbidden_existing_paths": {
            str(path.relative_to(REPO_ROOT)): {
                "exists": path.exists(),
                "sha256": _sha_file(path) if path.is_file() else None,
            }
            for path in FORBIDDEN_PATHS
        },
    }


def prepare(output: Path, *, model: str, effort: str, workers: int) -> dict[str, Any]:
    """Freeze and hash every logical selector input before any model call."""
    manifest_path = output / "input_manifest.json"
    if manifest_path.exists():
        raise RuntimeError(f"refusing to replace frozen manifest: {manifest_path}")
    safety = _source_safety()
    flags = _assert_flags_off()
    runtime = ai_gateway.validate_runtime(
        require_vision=False, deep=False, mode=ai_settings.MODE_OFF,
    )
    if not runtime.get("ok"):
        raise RuntimeError(f"isolated AI runtime preflight failed: {runtime['problems']}")

    artifacts: dict[str, Any] = {}
    prompt_template_hashes: set[str] = set()
    for pair_id in PAIR_ORDER:
        path = _artifact_path(pair_id)
        if not path.is_file() or not _tracked_clean(path):
            raise RuntimeError(f"candidate artifact is missing or modified: {path}")
        raw = _read_json(path)
        if raw.get("algorithm_version") != lineage.ALGORITHM_VERSION:
            raise RuntimeError(f"algorithm mismatch in {path}")
        if raw.get("selector_executed") or raw.get("model_calls") != 0:
            raise RuntimeError(f"artifact is not pre-selector: {path}")
        dataset = _dataset(raw)
        prompt_a, payload_a = lineage.build_selector_prompt(dataset, "A")
        prompt_b, payload_b = lineage.build_selector_prompt(dataset, "B")
        if payload_a != payload_b:
            raise RuntimeError(f"Pass A/B payload drift for {pair_id}")
        template = _prompt_template(prompt_a)
        if template.replace("{PASS}", "B", 1) != prompt_b:
            raise RuntimeError(f"Pass A/B prompt template drift for {pair_id}")
        prompt_template_hashes.add(_sha_json({
            "prefix": template[: template.index("payload=")],
        }))
        artifacts[pair_id] = {
            "project": _project(pair_id),
            "path": str(path.relative_to(REPO_ROOT)),
            "artifact_sha256": _sha_file(path),
            "input_signature": dataset.input_signature,
            "selector_payload_signature": payload_a["payload_signature"],
            "selector_payload_sha256": _sha_json(payload_a),
            "prompt_template_sha256": _sha_bytes(template.encode("utf-8")),
            "prompt_a_sha256": _sha_bytes(prompt_a.encode("utf-8")),
            "prompt_b_sha256": _sha_bytes(prompt_b.encode("utf-8")),
            "prompt_characters": len(prompt_a),
            "task_count": len(dataset.tasks),
            "candidate_bearing_task_count": sum(
                bool(value.get("candidate_ids")) for value in dataset.tasks
            ),
            "candidate_count": len(dataset.candidates),
            **_logical_hashes(raw),
        }
    if len(prompt_template_hashes) != 1:
        raise RuntimeError("projects do not use one selector prompt template")

    configuration = {
        "transport": ai_settings.CODEX_SESSION,
        "model": model,
        "reasoning_effort": effort,
        "passes": list(PASSES),
        "cold_runs": list(COLD_RUNS),
        "independent_cli_process_per_observation": True,
        "codex_ephemeral_session": True,
        "structured_output": True,
        "retries": 0,
        "timeout_seconds": ai_settings.call_timeout_seconds(),
        "workers": workers,
        "vision": False,
        "temperature": "provider_default_not_overridden",
    }
    manifest = {
        "kind": "function_lineage_v2_frozen_input_manifest",
        "schema_version": "function-lineage-v2-repeat-input.v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_safety": safety,
        "runtime_flags": flags,
        "runtime_preflight": runtime,
        "model_configuration": configuration,
        "model_configuration_sha256": _sha_json(configuration),
        "selector_prompt_template_prefix_sha256": next(iter(prompt_template_hashes)),
        "artifacts": artifacts,
        "model_calls_at_freeze": 0,
        "production_run_executed": False,
        "shadow_enabled": False,
        "deployed": False,
        "materialization_applied": False,
    }
    _write_json(manifest_path, manifest)
    return manifest


def _validate_frozen(output: Path) -> tuple[dict[str, Any], dict[str, lineage.FunctionLineageDataset]]:
    manifest_path = output / "input_manifest.json"
    manifest = _read_json(manifest_path)
    if not _candidate_is_ancestor():
        raise RuntimeError("candidate commit is no longer in the checkout lineage")
    pinned_paths = (
        *PRODUCTION_SOURCES,
        REPO_ROOT / "experiments" / "function_lineage_v2" / "run.py",
        *(_artifact_path(pair_id) for pair_id in PAIR_ORDER),
    )
    if not _source_paths_unchanged_at_head(pinned_paths):
        raise RuntimeError("candidate implementation changed after input freeze")
    if _sha_file(Path(__file__)) != manifest["source_safety"]["research_harness_sha256"]:
        raise RuntimeError("research harness changed after input freeze")
    if _flag_snapshot() != manifest["runtime_flags"]:
        raise RuntimeError("production Function Lineage flags changed after input freeze")
    datasets = _load_datasets()
    for pair_id, dataset in datasets.items():
        frozen = manifest["artifacts"][pair_id]
        path = _artifact_path(pair_id)
        raw = _read_json(path)
        if _sha_file(path) != frozen["artifact_sha256"]:
            raise RuntimeError(f"frozen artifact bytes changed: {pair_id}")
        if _logical_hashes(raw) != {
            key: frozen[key] for key in _logical_hashes(raw)
        }:
            raise RuntimeError(f"frozen logical inputs changed: {pair_id}")
        for pass_name in PASSES:
            prompt, payload = lineage.build_selector_prompt(dataset, pass_name)
            if _sha_bytes(prompt.encode("utf-8")) != frozen[f"prompt_{pass_name.lower()}_sha256"]:
                raise RuntimeError(f"frozen prompt changed: {pair_id}/{pass_name}")
            if _sha_json(payload) != frozen["selector_payload_sha256"]:
                raise RuntimeError(f"frozen payload changed: {pair_id}/{pass_name}")
    return manifest, datasets


def _usage_total(usage: Mapping[str, Any]) -> int:
    if isinstance(usage.get("total_tokens"), (int, float)):
        return int(usage["total_tokens"])
    if isinstance(usage.get("total_input_tokens"), (int, float)):
        return int(usage["total_input_tokens"]) + int(usage.get("output_tokens") or 0)
    return int(usage.get("input_tokens") or 0) + int(usage.get("output_tokens") or 0)


def _model_job(
    *,
    dataset: lineage.FunctionLineageDataset,
    project: str,
    cold_run: int,
    pass_name: str,
    manifest: Mapping[str, Any],
    experiment_id: str,
) -> dict[str, Any]:
    pair_id = dataset.pair_id
    frozen = manifest["artifacts"][pair_id]
    config = manifest["model_configuration"]
    prompt, payload = lineage.build_selector_prompt(dataset, pass_name)
    prompt_hash = _sha_bytes(prompt.encode("utf-8"))
    if prompt_hash != frozen[f"prompt_{pass_name.lower()}_sha256"]:
        raise RuntimeError(f"prompt drift before model call: {pair_id}/{cold_run}/{pass_name}")
    call_id = f"{experiment_id}:{pair_id}:cold{cold_run}:pass{pass_name}:{uuid.uuid4().hex}"
    call = ai_gateway.call(
        ai_settings.CODEX_SESSION,
        prompt,
        model=str(config["model"]),
        reasoning_level=str(config["reasoning_effort"]),
        schema=lineage.output_schema(dataset, str(payload["payload_signature"])),
        images=(),
        retries=int(config["retries"]),
        timeout_s=int(config["timeout_seconds"]),
        run_id=call_id,
    )
    verification = lineage.verify_selector_response(
        dataset, str(payload["payload_signature"]), call.parsed if call.ok else None,
    )
    usage = dict(call.usage or {})
    return {
        "project": project,
        "pair_id": pair_id,
        "cold_run": cold_run,
        "pass_name": pass_name,
        "mode": "TEXT_STRUCTURED",
        "input_signature": dataset.input_signature,
        "payload_signature": payload["payload_signature"],
        "prompt_sha256": prompt_hash,
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
            "tokens": _usage_total(usage),
            "error": call.error,
            "error_kind": call.error_kind,
            "attempts": int(call.attempts),
            "exit_code": call.exit_code,
            "provider_session_id": call.session_id,
            "raw_excerpt": call.raw_excerpt,
        },
        "response": call.parsed,
        "verification": verification,
    }


def experiment(output: Path) -> list[dict[str, Any]]:
    """Run exactly 18 stateless calls over the previously frozen input."""
    records_path = output / "model_runs.jsonl"
    if records_path.exists():
        raise RuntimeError(f"refusing to repeat or append model observations: {records_path}")
    manifest, datasets = _validate_frozen(output)
    config = manifest["model_configuration"]
    jobs = [
        (datasets[pair_id], _project(pair_id), cold_run, pass_name)
        for pair_id in PAIR_ORDER
        for cold_run in COLD_RUNS
        for pass_name in PASSES
    ]
    experiment_id = "flv2-isolated-" + uuid.uuid4().hex
    started = time.monotonic()
    records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=int(config["workers"])) as pool:
        futures = {
            pool.submit(
                _model_job,
                dataset=dataset,
                project=project,
                cold_run=cold_run,
                pass_name=pass_name,
                manifest=manifest,
                experiment_id=experiment_id,
            ): (dataset.pair_id, cold_run, pass_name)
            for dataset, project, cold_run, pass_name in jobs
        }
        try:
            for index, future in enumerate(as_completed(futures), 1):
                record = future.result()
                records.append(record)
                _write_jsonl(
                    records_path,
                    sorted(records, key=lambda value: (
                        PAIR_ORDER.index(str(value["pair_id"])),
                        int(value["cold_run"]), str(value["pass_name"]),
                    )),
                )
                print(
                    f"{index}/{len(jobs)} {record['project']} cold={record['cold_run']} "
                    f"pass={record['pass_name']} model_ok={record['model_call']['ok']} "
                    f"verifier_ok={record['verification']['ok']}",
                    flush=True,
                )
        except Exception as exc:
            ai_gateway.kill_live_processes(experiment_id)
            _write_json(output / "experiment_invalid.json", {
                "status": "NOT_VALID",
                "reason": "TECHNICAL_HARNESS_DEFECT",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "completed_model_calls": len(records),
                "no_code_tuning_performed": True,
            })
            raise
    records.sort(key=lambda value: (
        PAIR_ORDER.index(str(value["pair_id"])),
        int(value["cold_run"]), str(value["pass_name"]),
    ))
    _write_jsonl(records_path, records)
    _write_json(output / "run_telemetry.json", {
        "experiment_id": experiment_id,
        "model_calls": len(records),
        "wall_time_ms": int((time.monotonic() - started) * 1000),
        "started_from_commit": SOURCE_COMMIT,
        "production_run_executed": False,
        "shadow_enabled": False,
        "deployed": False,
        "materialization_applied": False,
    })
    return records


def _records_index(records: Sequence[Mapping[str, Any]]) -> dict[tuple[str, int, str], Mapping[str, Any]]:
    index: dict[tuple[str, int, str], Mapping[str, Any]] = {}
    for record in records:
        key = (
            str(record["pair_id"]), int(record["cold_run"]),
            str(record["pass_name"]),
        )
        if key in index:
            raise RuntimeError(f"duplicate model observation: {key}")
        index[key] = record
    expected = {
        (pair_id, cold_run, pass_name)
        for pair_id in PAIR_ORDER for cold_run in COLD_RUNS for pass_name in PASSES
    }
    if set(index) != expected:
        raise RuntimeError(f"model observation set mismatch: {sorted(expected - set(index))}")
    return index


def _reverify(
    datasets: Mapping[str, lineage.FunctionLineageDataset],
    records: list[dict[str, Any]],
) -> None:
    for record in records:
        dataset = datasets[str(record["pair_id"])]
        _, payload = lineage.build_selector_prompt(dataset, str(record["pass_name"]))
        current = lineage.verify_selector_response(
            dataset,
            str(payload["payload_signature"]),
            record.get("response") if (record.get("model_call") or {}).get("ok") else None,
        )
        if current != record.get("verification"):
            raise RuntimeError(
                f"verifier result drift: {record['pair_id']}/{record['cold_run']}/{record['pass_name']}"
            )


def _task_decisions(
    datasets: Mapping[str, lineage.FunctionLineageDataset],
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    indexed = _records_index(records)
    output: list[dict[str, Any]] = []
    for pair_id in PAIR_ORDER:
        dataset = datasets[pair_id]
        for task in dataset.tasks:
            task_id = str(task["task_id"])
            observations: list[dict[str, Any]] = []
            cold_results: list[dict[str, Any]] = []
            for cold_run in COLD_RUNS:
                run_observations = []
                for pass_name in PASSES:
                    record = indexed[(pair_id, cold_run, pass_name)]
                    verification = record.get("verification") or {}
                    task_result = (verification.get("task_results") or {}).get(task_id) or {}
                    row = {
                        "cold_run": cold_run,
                        "pass_name": pass_name,
                        "model_ok": bool((record.get("model_call") or {}).get("ok")),
                        "schema_ok": not any(
                            str(value) in {
                                "MODEL_FAILURE", "UNKNOWN_RESPONSE_FIELD",
                                "PAYLOAD_SIGNATURE_MISMATCH", "INVALID_SELECTIONS",
                                "INVALID_SELECTION", "DUPLICATE_TASK", "TASK_SET_MISMATCH",
                            }
                            for value in verification.get("global_errors") or []
                        ),
                        "verifier_ok": bool(task_result.get("ok")),
                        "candidate_id": task_result.get("candidate_id"),
                        "task_errors": list(task_result.get("errors") or []),
                        "global_errors": list(verification.get("global_errors") or []),
                    }
                    observations.append(row)
                    run_observations.append(row)
                left, right = run_observations
                choices = [left.get("candidate_id"), right.get("candidate_id")]
                if not all(value["model_ok"] and value["schema_ok"] for value in run_observations):
                    status = "MODEL_SCHEMA_FAILURE"
                    selected = None
                elif not all(value["verifier_ok"] for value in run_observations):
                    status = "VERIFIER_REJECTION"
                    selected = None
                elif choices[0] == choices[1] == lineage.NEED_MORE_EVIDENCE:
                    status = lineage.NEED_MORE_EVIDENCE
                    selected = lineage.NEED_MORE_EVIDENCE
                elif choices[0] == choices[1] and choices[0] in dataset.candidates:
                    status = "STABLE_PASS_PAIR"
                    selected = choices[0]
                else:
                    status = "PASS_DISAGREEMENT"
                    selected = None
                cold_results.append({
                    "cold_run": cold_run,
                    "status": status,
                    "selected_candidate_id": selected,
                    "pass_choices": choices,
                })
            concrete = [
                value["selected_candidate_id"] for value in cold_results
                if value["status"] == "STABLE_PASS_PAIR"
            ]
            if len(concrete) == len(COLD_RUNS) and len(set(concrete)) == 1:
                final_status = "STABLE"
                selected = str(concrete[0])
            elif all(value["status"] == lineage.NEED_MORE_EVIDENCE for value in cold_results):
                final_status = lineage.NEED_MORE_EVIDENCE
                selected = lineage.NEED_MORE_EVIDENCE
            elif any(value["status"] == "MODEL_SCHEMA_FAILURE" for value in cold_results):
                final_status = "MODEL_SCHEMA_FAILURE"
                selected = None
            elif any(value["status"] == "VERIFIER_REJECTION" for value in cold_results):
                final_status = "VERIFIER_REJECTION"
                selected = None
            else:
                final_status = "PASS_DISAGREEMENT"
                selected = None
            candidate = dataset.candidates.get(str(selected))
            observation_distribution = Counter(
                str(value.get("candidate_id") or "<NO_SELECTION>")
                for value in observations
            )
            cold_distribution = Counter(
                str(value.get("selected_candidate_id") or value["status"])
                for value in cold_results
            )
            output.append({
                "project": _project(pair_id),
                "pair_id": pair_id,
                "task_id": task_id,
                "left_physical_page": int(task["left_physical_page"]),
                "left_function_id": task["left_function_id"],
                "left_fragment_id": task["left_fragment_id"],
                "candidate_bearing": bool(task.get("candidate_ids")),
                "final_status": final_status,
                "selected_candidate_id": selected,
                "relation_type": (
                    candidate.get("relation_type") if candidate
                    else lineage.NEED_MORE_EVIDENCE
                ),
                "stable_across_passes": all(
                    value["status"] in {"STABLE_PASS_PAIR", lineage.NEED_MORE_EVIDENCE}
                    for value in cold_results
                ),
                "stable_concrete_across_passes": all(
                    value["status"] == "STABLE_PASS_PAIR" for value in cold_results
                ),
                "stable_across_runs": final_status == "STABLE",
                "observation_distribution": dict(sorted(observation_distribution.items())),
                "cold_run_distribution": dict(sorted(cold_distribution.items())),
                "cold_runs": cold_results,
                "observations": observations,
            })
    return output


def _stable_lineages(
    datasets: Mapping[str, lineage.FunctionLineageDataset],
    decisions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    selected_tasks: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for decision in decisions:
        if decision["final_status"] == "STABLE":
            selected_tasks[(
                str(decision["pair_id"]), str(decision["selected_candidate_id"]),
            )].append(decision)
    rows = []
    for (pair_id, candidate_id), task_rows in sorted(selected_tasks.items()):
        candidate = datasets[pair_id].candidates[candidate_id]
        all_observations = [
            value for task in task_rows for value in task.get("observations") or []
        ]
        rows.append({
            "project": _project(pair_id),
            "pair_id": pair_id,
            "candidate_id": candidate_id,
            "selected_by_task_ids": sorted(str(value["task_id"]) for value in task_rows),
            "left_pages": list(candidate["left_pages"]),
            "left_function_ids": list(candidate["left_function_ids"]),
            "left_fragment_ids": list(candidate["left_fragment_ids"]),
            "right_pages": list(candidate["right_pages"]),
            "right_function_ids": list(candidate["right_function_ids"]),
            "right_fragment_ids": list(candidate["right_fragment_ids"]),
            "relation_type": candidate["relation_type"],
            "evidence_refs": list(candidate["evidence_refs"]),
            "verifier_result": "PASS",
            "evidence_ownership": "PASS" if all(
                not any("EVIDENCE_" in str(error) for error in value.get("task_errors") or [])
                for value in all_observations
            ) else "FAIL",
            "capacity_result": "PASS" if all(
                not any("CONFLICT" in str(error) for error in value.get("global_errors") or [])
                for value in all_observations
            ) else "FAIL",
            "capacity_keys": list(candidate["right_capacity_keys"]),
            "bounded_candidate": True,
            "materialization_allowed": False,
        })
    return rows


def _capacity_audit(
    datasets: Mapping[str, lineage.FunctionLineageDataset],
    stable: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    key_defects = []
    for pair_id, dataset in datasets.items():
        for candidate in dataset.candidates.values():
            expected = sorted({
                f"RIGHT:{int(value['right_physical_page'])}:{value['right_fragment_id']}"
                for value in candidate.get("component_map") or []
            })
            actual = sorted(str(value) for value in candidate.get("right_capacity_keys") or [])
            if expected != actual:
                key_defects.append({
                    "pair_id": pair_id,
                    "candidate_id": candidate["candidate_id"],
                    "expected": expected,
                    "actual": actual,
                })
    selected_by_pair: dict[str, list[dict[str, str]]] = defaultdict(list)
    for value in stable:
        selected_by_pair[str(value["pair_id"])].append({
            "candidate_id": str(value["candidate_id"]),
        })
    final_conflicts = []
    for pair_id, selections in selected_by_pair.items():
        final_conflicts.extend({
            "pair_id": pair_id, "reason_code": error,
        } for error in lineage.verify_capacity(selections, datasets[pair_id].candidates))
    observed_conflicts = sorted({
        str(error)
        for record in records
        for error in (record.get("verification") or {}).get("global_errors") or []
        if "CONFLICT" in str(error)
    })
    reuse = []
    for pair_id in PAIR_ORDER:
        values = [value for value in stable if value["pair_id"] == pair_id]
        pages: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
        for value in values:
            for page in value["right_pages"]:
                pages[int(page)].append(value)
        for page, occupants in pages.items():
            if len(occupants) < 2:
                continue
            key_sets = [set(value["capacity_keys"]) for value in occupants]
            reuse.append({
                "pair_id": pair_id,
                "right_physical_page": page,
                "candidate_ids": [value["candidate_id"] for value in occupants],
                "exact_fragments_distinct": all(
                    not key_sets[left] & key_sets[right]
                    for left in range(len(key_sets))
                    for right in range(left + 1, len(key_sets))
                ),
            })
    return {
        "capacity_definition": "RIGHT physical_page + exact function_fragment_id",
        "candidate_capacity_key_defects": key_defects,
        "observed_model_pass_conflicts": observed_conflicts,
        "final_function_fragment_conflicts": final_conflicts,
        "function_fragment_conflict_count": len(final_conflicts),
        "right_map_conflict_count": len(final_conflicts),
        "right_page_reuse": sorted(reuse, key=lambda value: (
            value["pair_id"], value["right_physical_page"],
        )),
        "page_global_exclusivity_applied": False,
    }


def _project_metrics(
    datasets: Mapping[str, lineage.FunctionLineageDataset],
    decisions: Sequence[Mapping[str, Any]],
    stable: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for pair_id in PAIR_ORDER:
        dataset = datasets[pair_id]
        values = [value for value in decisions if value["pair_id"] == pair_id]
        stable_values = [value for value in values if value["final_status"] == "STABLE"]
        candidate_tasks = sum(value["candidate_bearing"] for value in values)
        relations = Counter(
            value["relation_type"] for value in stable if value["pair_id"] == pair_id
        )
        rows.append({
            "project": _project(pair_id),
            "pair_id": pair_id,
            "tasks_total": len(dataset.tasks),
            "candidate_bearing_tasks": candidate_tasks,
            "stable_tasks": len(stable_values),
            "stable_lineages": sum(value["pair_id"] == pair_id for value in stable),
            "stable_percent": round(100 * len(stable_values) / candidate_tasks, 3) if candidate_tasks else None,
            "unresolved": sum(value["final_status"] != "STABLE" for value in values),
            "PASS_DISAGREEMENT": sum(value["final_status"] == "PASS_DISAGREEMENT" for value in values),
            "NEED_MORE_EVIDENCE": sum(value["final_status"] == lineage.NEED_MORE_EVIDENCE for value in values),
            "verifier_rejections": sum(value["final_status"] == "VERIFIER_REJECTION" for value in values),
            "model_schema_failures": sum(value["final_status"] == "MODEL_SCHEMA_FAILURE" for value in values),
            "unsupported_accepted_matches": 0,
            "stable_across_passes_tasks": sum(value["stable_across_passes"] for value in values),
            "stable_concrete_across_passes_tasks": sum(value["stable_concrete_across_passes"] for value in values),
            "stable_across_runs_tasks": len(stable_values),
            "relation_type_counts": {
                "CONTINUED_1_TO_1": relations["CONTINUED_1_TO_1"],
                "SPLIT": relations["SPLIT_1_TO_N"],
                "MERGED": relations["MERGED_N_TO_1"],
                "FUNCTION_DISTRIBUTED": relations["FUNCTION_DISTRIBUTED"],
            },
        })
    return rows


def _group_audit(
    datasets: Mapping[str, lineage.FunctionLineageDataset],
    decisions: Sequence[Mapping[str, Any]],
    stable: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    input_candidates = [
        candidate
        for dataset in datasets.values()
        for candidate in dataset.candidates.values()
        if candidate.get("relation_type") in COMPLEX_RELATIONS
    ]
    selected_observations = [
        (decision, observation)
        for decision in decisions
        for observation in decision["observations"]
        if observation.get("candidate_id") in datasets[str(decision["pair_id"])].candidates
        and datasets[str(decision["pair_id"])].candidates[str(observation["candidate_id"])]["relation_type"] in COMPLEX_RELATIONS
    ]
    verified_observations = [
        value for value in selected_observations if value[1].get("verifier_ok")
    ]
    stable_groups = [
        value for value in stable if value["relation_type"] in COMPLEX_RELATIONS
    ]

    def count_by_type(values: Iterable[str]) -> dict[str, int]:
        count = Counter(values)
        return {key: count[key] for key in COMPLEX_RELATIONS}

    return {
        "group_candidates_in_ai_input": len(input_candidates),
        "group_candidates_in_ai_input_by_type": count_by_type(
            str(value["relation_type"]) for value in input_candidates
        ),
        "group_selection_observations": len(selected_observations),
        "group_selection_observations_by_type": count_by_type(
            datasets[str(decision["pair_id"])].candidates[str(observation["candidate_id"])]["relation_type"]
            for decision, observation in selected_observations
        ),
        "group_verifier_pass_observations": len(verified_observations),
        "group_verifier_pass_observations_by_type": count_by_type(
            datasets[str(decision["pair_id"])].candidates[str(observation["candidate_id"])]["relation_type"]
            for decision, observation in verified_observations
        ),
        "stable_group_lineages": len(stable_groups),
        "stable_group_lineages_by_type": count_by_type(
            str(value["relation_type"]) for value in stable_groups
        ),
        "one_to_many_checked": True,
        "many_to_one_checked": True,
        "function_distributed_checked": True,
    }


def _unstable_tasks(decisions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for value in decisions:
        distribution = dict(value["observation_distribution"])
        instability = (
            100 * (value["final_status"] != "STABLE")
            + 10 * (len(distribution) - 1)
            + sum(run["status"] == "PASS_DISAGREEMENT" for run in value["cold_runs"])
        )
        rows.append({
            "project": value["project"],
            "pair_id": value["pair_id"],
            "task_id": value["task_id"],
            "left_physical_page": value["left_physical_page"],
            "final_status": value["final_status"],
            "stable_across_passes": value["stable_across_passes"],
            "stable_across_runs": value["stable_across_runs"],
            "selection_distribution_6_observations": distribution,
            "cold_run_distribution_3_runs": value["cold_run_distribution"],
            "instability_score": instability,
        })
    return sorted(
        rows,
        key=lambda value: (-value["instability_score"], value["project"], value["task_id"]),
    )


def _candidate_rows(
    dataset: lineage.FunctionLineageDataset, left_page: int,
) -> list[dict[str, Any]]:
    rows = []
    for candidate in dataset.candidates.values():
        if left_page not in candidate.get("left_pages") or candidate.get("candidate_id") is None:
            continue
        ranks = [
            int(task["candidate_ranks"][candidate["candidate_id"]])
            for task in dataset.tasks
            if int(task["left_physical_page"]) == left_page
            and candidate["candidate_id"] in task.get("candidate_ranks", {})
        ]
        if not ranks:
            continue
        rows.append({
            "candidate_id": candidate["candidate_id"],
            "rank": min(ranks),
            "relation_type": candidate["relation_type"],
            "right_pages": candidate["right_pages"],
        })
    return sorted(rows, key=lambda value: (value["rank"], value["candidate_id"]))


def _page_control(
    *,
    dataset: lineage.FunctionLineageDataset,
    decisions: Sequence[Mapping[str, Any]],
    left_page: int,
    target_right: Sequence[int],
    relation_type: str | None = None,
) -> dict[str, Any]:
    candidates = [
        value for value in _candidate_rows(dataset, left_page)
        if list(value["right_pages"]) == list(target_right)
        and (relation_type is None or value["relation_type"] == relation_type)
    ]
    candidate_ids = {str(value["candidate_id"]) for value in candidates}
    tasks = [value for value in decisions if int(value["left_physical_page"]) == left_page]
    distribution = Counter()
    for task in tasks:
        for observation in task["observations"]:
            candidate_id = str(observation.get("candidate_id") or "<NO_SELECTION>")
            if candidate_id == lineage.NEED_MORE_EVIDENCE:
                distribution[lineage.NEED_MORE_EVIDENCE] += 1
            elif candidate_id in candidate_ids:
                distribution[f"TARGET_RIGHT_{'_'.join(str(v) for v in target_right)}"] += 1
            else:
                distribution["OTHER"] += 1
    return {
        "left_page": left_page,
        "target_right_pages": list(target_right),
        "target_candidates": candidates,
        "target_present": bool(candidates),
        "best_rank": min((int(value["rank"]) for value in candidates), default=None),
        "stable_target_task_ids": sorted(
            str(value["task_id"]) for value in tasks
            if value["final_status"] == "STABLE"
            and value.get("selected_candidate_id") in candidate_ids
        ),
        "task_results": [{
            "task_id": value["task_id"],
            "left_function_id": value["left_function_id"],
            "final_status": value["final_status"],
            "selected_candidate_id": value["selected_candidate_id"],
            "selection_distribution": value["observation_distribution"],
        } for value in tasks],
        "page_observation_distribution": dict(sorted(distribution.items())),
    }


def _ios21_controls(
    dataset: lineage.FunctionLineageDataset,
    decisions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ios = [value for value in decisions if value["pair_id"] == IOS21_PAIR_ID]
    left17 = _page_control(
        dataset=dataset, decisions=ios, left_page=17, target_right=[27],
        relation_type="CONTINUED_1_TO_1",
    )
    left18 = _page_control(
        dataset=dataset, decisions=ios, left_page=18, target_right=[24],
        relation_type="CONTINUED_1_TO_1",
    )
    left19_r30 = _page_control(
        dataset=dataset, decisions=ios, left_page=19, target_right=[30],
        relation_type="CONTINUED_1_TO_1",
    )
    left19_r25 = _page_control(
        dataset=dataset, decisions=ios, left_page=19, target_right=[25],
        relation_type="CONTINUED_1_TO_1",
    )
    r30 = {value["candidate_id"] for value in left19_r30["target_candidates"]}
    r25 = {value["candidate_id"] for value in left19_r25["target_candidates"]}
    canonical_tasks = [
        task for task in dataset.tasks
        if int(task["left_physical_page"]) == 19
        and any(task.get("candidate_ranks", {}).get(value) == 1 for value in r30)
        and any(task.get("candidate_ranks", {}).get(value) == 2 for value in r25)
    ]
    canonical_ids = {str(value["task_id"]) for value in canonical_tasks}
    ambiguity_distribution = Counter()
    for value in ios:
        if value["task_id"] not in canonical_ids:
            continue
        for observation in value["observations"]:
            candidate_id = observation.get("candidate_id")
            if candidate_id in r30:
                ambiguity_distribution["R30"] += 1
            elif candidate_id in r25:
                ambiguity_distribution["R25"] += 1
            elif candidate_id == lineage.NEED_MORE_EVIDENCE:
                ambiguity_distribution[lineage.NEED_MORE_EVIDENCE] += 1
            else:
                ambiguity_distribution["OTHER"] += 1

    group_id = "lcand_9c617494b14c2b922d3f"
    group = dataset.candidates.get(group_id)
    owner_tasks = [
        value for value in ios
        if group_id in next(
            task["candidate_ids"] for task in dataset.tasks
            if task["task_id"] == value["task_id"]
        )
    ] if group else []
    group_distribution = Counter(
        str(observation.get("candidate_id") or "<NO_SELECTION>")
        for value in owner_tasks for observation in value["observations"]
    )
    return {
        "LEFT17": left17,
        "LEFT18": left18,
        "LEFT19": {
            "R30": left19_r30,
            "R25": left19_r25,
            "canonical_ambiguity_task_ids": sorted(canonical_ids),
            "distribution_all_6_observations": dict(sorted(ambiguity_distribution.items())),
            "unresolved_is_allowed": True,
        },
        "LEFT20": {
            "candidate_id": group_id,
            "present": group is not None,
            "relation_type": group.get("relation_type") if group else None,
            "right_pages": group.get("right_pages") if group else None,
            "right_fragment_ids": group.get("right_fragment_ids") if group else None,
            "capacity_keys": group.get("right_capacity_keys") if group else None,
            "owner_task_ids": sorted(str(value["task_id"]) for value in owner_tasks),
            "stable_owner_task_ids": sorted(
                str(value["task_id"]) for value in owner_tasks
                if value["final_status"] == "STABLE"
                and value["selected_candidate_id"] == group_id
            ),
            "selected_distribution": dict(sorted(group_distribution.items())),
            "all_exact_fragment_capacities_valid": bool(group) and sorted(group["right_capacity_keys"]) == sorted({
                f"RIGHT:{int(value['right_physical_page'])}:{value['right_fragment_id']}"
                for value in group.get("component_map") or []
            }),
            "page_global_conflict": False,
        },
    }


def _old_task_pages(old: Mapping[str, Any]) -> dict[str, int]:
    return {
        str(value["task_id"]): int(value["left_physical_page"])
        for key in ("stable_lineages", "unresolved_lineages")
        for value in old.get(key) or []
        if value.get("task_id") is not None
    }


def _ios21_old_new_comparison(
    dataset: lineage.FunctionLineageDataset,
    decisions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    old = _read_json(OLD_IOS21_MAP)
    if old.get("run_id") != OLD_IOS21_RUN_ID:
        raise RuntimeError("saved IOS2.1 production run ID changed")
    deterministic = _read_json(DETERMINISTIC_IOS21_COMPARISON)
    task_pages = _old_task_pages(old)
    pages: dict[str, Any] = {}
    for page in (17, 18, 19, 20):
        old_choices = []
        for model_pass in old.get("model_passes") or []:
            for task_id, result in (model_pass.get("verification") or {}).get("task_results", {}).items():
                if task_pages.get(str(task_id)) == page:
                    old_choices.append({
                        "pass_name": model_pass.get("pass_name"),
                        "task_id": task_id,
                        "candidate_id": result.get("candidate_id"),
                        "verifier_ok": result.get("ok"),
                        "errors": result.get("errors") or [],
                    })
        new_values = [
            value for value in decisions
            if value["pair_id"] == IOS21_PAIR_ID
            and int(value["left_physical_page"]) == page
        ]
        stage = deterministic.get("pages", {}).get(str(page), {})
        pages[str(page)] = {
            "old_production_candidate_set": [
                {
                    "candidate_id": value.get("candidate_id"),
                    "relation_type": value.get("relation_type"),
                    "right_pages": value.get("right_pages"),
                    "rank": None,
                }
                for value in old.get("functional_candidates") or []
                if page in value.get("left_pages", [])
            ],
            "new_candidate_set": _candidate_rows(dataset, page),
            "old_model_choices": old_choices,
            "new_model_choices": [{
                "task_id": value["task_id"],
                "final_status": value["final_status"],
                "selection_distribution": value["observation_distribution"],
            } for value in new_values],
            "old_verifier_outcome": old.get("verifier_result"),
            "new_verifier_outcomes": [{
                "task_id": value["task_id"],
                "cold_runs": value["cold_runs"],
            } for value in new_values],
            "improvement_factors": {
                "SEARCH": "old bounded set compared with complete new function-corpus retrieval",
                "PASSPORT": {
                    "lost_in_production_integration": stage.get(
                        "fields_lost_during_production_integration", []
                    ),
                    "restored_in_new_deterministic": stage.get(
                        "fields_restored_in_new_deterministic", []
                    ),
                },
                "RANKING": "old rank was not persisted; new deterministic rank is recorded per task",
                "MODEL": "old two-pass choices compared with 6 new cold observations",
                "VERIFIER": "unchanged fail-closed verifier outcomes shown separately",
            },
        }
    return {
        "kind": "ios21_old_production_vs_isolated_repeat",
        "old_run_id": OLD_IOS21_RUN_ID,
        "old_algorithm_version": old.get("algorithm_version"),
        "old_shadow_status": old.get("shadow_status"),
        "old_model_calls": old.get("model_calls"),
        "old_tokens": old.get("tokens"),
        "new_source_commit": SOURCE_COMMIT,
        "pages": pages,
    }


def _cost(
    records: Sequence[Mapping[str, Any]], run_telemetry: Mapping[str, Any],
) -> dict[str, Any]:
    usages = [dict((value.get("model_call") or {}).get("usage") or {}) for value in records]
    input_tokens = sum(
        int(value.get("total_input_tokens") or value.get("input_tokens") or 0)
        for value in usages
    )
    cached_input_tokens = sum(int(value.get("cached_input_tokens") or 0) for value in usages)
    output_tokens = sum(int(value.get("output_tokens") or 0) for value in usages)
    total_tokens = sum(_usage_total(value) for value in usages)
    telemetry_defect = any(
        bool((record.get("model_call") or {}).get("ok")) and _usage_total(usage) == 0
        for record, usage in zip(records, usages)
    )

    def comparison(baseline: int) -> dict[str, Any]:
        return {
            "baseline_tokens": baseline,
            "absolute_change_tokens": total_tokens - baseline if not telemetry_defect else None,
            "percent_change": round(100 * (total_tokens - baseline) / baseline, 3) if not telemetry_defect else None,
        }

    return {
        "model_calls": len(records),
        "successful_model_calls": sum(
            bool((value.get("model_call") or {}).get("ok")) for value in records
        ),
        "wall_time_ms": int(run_telemetry.get("wall_time_ms") or 0),
        "model_runtime_ms": sum(
            int((value.get("model_call") or {}).get("duration_ms") or 0)
            for value in records
        ),
        "input_tokens": input_tokens,
        "cached_input_tokens": cached_input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "telemetry_complete": not telemetry_defect,
        "telemetry_defect": telemetry_defect,
        "telemetry_assessment": (
            "Successful calls returned empty/zero usage; token cost and percent comparisons are unavailable and are not used in the quality verdict."
            if telemetry_defect else "Provider usage was recorded for every successful call."
        ),
        "vs_old_v4_ai_repeat": comparison(PREVIOUS_V4_TOKENS),
        "vs_function_lineage_v1": comparison(FUNCTION_LINEAGE_V1_TOKENS),
    }


def _verdict_inputs(
    controls: Mapping[str, Any], capacity: Mapping[str, Any],
    projects: Sequence[Mapping[str, Any]], cost: Mapping[str, Any],
) -> dict[str, Any]:
    coverage = (
        controls["LEFT17"]["target_present"]
        and controls["LEFT18"]["target_present"]
        and controls["LEFT19"]["R30"]["target_present"]
        and controls["LEFT19"]["R25"]["target_present"]
        and controls["LEFT20"]["present"]
    )
    safety = (
        all(value["unsupported_accepted_matches"] == 0 for value in projects)
        and not capacity["candidate_capacity_key_defects"]
        and capacity["function_fragment_conflict_count"] == 0
        and capacity["right_map_conflict_count"] == 0
    )
    critical_stability = {
        "LEFT17_has_stable_R27": bool(controls["LEFT17"]["stable_target_task_ids"]),
        "LEFT18_has_stable_R24": bool(controls["LEFT18"]["stable_target_task_ids"]),
        "LEFT20_group_stable_for_all_owner_tasks": (
            bool(controls["LEFT20"]["owner_task_ids"])
            and controls["LEFT20"]["owner_task_ids"]
            == controls["LEFT20"]["stable_owner_task_ids"]
        ),
        "LEFT19_ambiguity_may_remain_unresolved": True,
    }
    selector_critical_stable = all(critical_stability.values())
    verifier_defect = bool(capacity["candidate_capacity_key_defects"])
    suggested = (
        "D" if verifier_defect
        else "C" if not coverage
        else "A" if safety and selector_critical_stable
        else "B" if safety
        else "E"
    )
    return {
        "candidate_coverage_controls_pass": coverage,
        "safety_controls_pass": safety,
        "critical_stability": critical_stability,
        "critical_stability_pass": selector_critical_stable,
        "telemetry_excluded_from_quality_verdict": bool(cost["telemetry_defect"]),
        "suggested_verdict": suggested,
    }


def _report(
    metrics: Mapping[str, Any], controls: Mapping[str, Any],
    cost: Mapping[str, Any], verdict: Mapping[str, Any],
) -> str:
    lines = [
        "# Function Lineage v2 — isolated AI repeat",
        "",
        f"Source candidate commit: `{SOURCE_COMMIT}`. Production baseline: "
        f"`{PRODUCTION_BASE_COMMIT}` (`{PRODUCTION_BASE_RELEASE}`).",
        "",
        "Research only: no production run, no shadow enablement, no deploy, no materialization, no Vision.",
        "",
        "## Project metrics",
        "",
        "| Project | Candidate tasks | Stable tasks | Stable lineages | Stable % | NME | Pass disagreement | Verifier reject | Model/schema fail |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for value in metrics["projects"]:
        lines.append(
            f"| {value['project']} | {value['candidate_bearing_tasks']} | "
            f"{value['stable_tasks']} | {value['stable_lineages']} | "
            f"{value['stable_percent']} | {value['NEED_MORE_EVIDENCE']} | "
            f"{value['PASS_DISAGREEMENT']} | {value['verifier_rejections']} | "
            f"{value['model_schema_failures']} |"
        )
    lines.extend([
        "",
        "Stable % is stable task decisions / candidate-bearing tasks; stable lineages are unique candidate IDs.",
        "",
        "## IOS2.1 controls",
        "",
        f"- LEFT17 → R27: present `{controls['LEFT17']['target_present']}`, best rank "
        f"`{controls['LEFT17']['best_rank']}`, stable target tasks "
        f"`{len(controls['LEFT17']['stable_target_task_ids'])}`.",
        f"- LEFT18 → R24: present `{controls['LEFT18']['target_present']}`, best rank "
        f"`{controls['LEFT18']['best_rank']}`, stable target tasks "
        f"`{len(controls['LEFT18']['stable_target_task_ids'])}`.",
        f"- LEFT19 ambiguity distribution (six observations of the canonical rank-1/rank-2 task): "
        f"`{json.dumps(controls['LEFT19']['distribution_all_6_observations'], ensure_ascii=False, sort_keys=True)}`.",
        f"- LEFT20 group `{controls['LEFT20']['candidate_id']}`: present "
        f"`{controls['LEFT20']['present']}`, stable owner tasks "
        f"`{len(controls['LEFT20']['stable_owner_task_ids'])}/{len(controls['LEFT20']['owner_task_ids'])}`, "
        f"exact capacities `{controls['LEFT20']['all_exact_fragment_capacities_valid']}`.",
        "",
        "## Safety and cost",
        "",
        f"Unsupported accepted matches: `{metrics['unsupported_accepted_matches']}`. "
        f"FUNCTION_FRAGMENT_CONFLICT: `{metrics['function_fragment_conflicts']}`. "
        f"RIGHT_MAP_CONFLICT: `{metrics['right_map_conflicts']}`.",
        "",
        f"Model calls: `{cost['model_calls']}`; successful: `{cost['successful_model_calls']}`; "
        f"wall time: `{cost['wall_time_ms']} ms`; model runtime sum: `{cost['model_runtime_ms']} ms`; "
        f"reported tokens: `{cost['total_tokens']}`.",
        "",
        f"Telemetry defect: `{cost['telemetry_defect']}`. {cost['telemetry_assessment']}",
        "",
        "## Verdict",
        "",
        f"**{verdict['suggested_verdict']}**",
        "",
        "This verdict does not authorize deployment or shadow enablement.",
        "",
    ])
    return "\n".join(lines)


def finalize(output: Path) -> dict[str, Any]:
    manifest, datasets = _validate_frozen(output)
    records = _read_jsonl(output / "model_runs.jsonl")
    _records_index(records)
    _reverify(datasets, records)
    decisions = _task_decisions(datasets, records)
    stable = _stable_lineages(datasets, decisions)
    capacity = _capacity_audit(datasets, stable, records)
    projects = _project_metrics(datasets, decisions, stable)
    group = _group_audit(datasets, decisions, stable)
    controls = _ios21_controls(datasets[IOS21_PAIR_ID], decisions)
    comparison = _ios21_old_new_comparison(datasets[IOS21_PAIR_ID], decisions)
    run_telemetry = _read_json(output / "run_telemetry.json")
    cost = _cost(records, run_telemetry)
    verdict = _verdict_inputs(controls, capacity, projects, cost)
    unstable = _unstable_tasks(decisions)
    all_relations = Counter(value["relation_type"] for value in stable)
    metrics = {
        "kind": "function_lineage_v2_repeat_metrics",
        "schema_version": "function-lineage-v2-repeat-metrics.v1",
        "source_commit": SOURCE_COMMIT,
        "production_base_commit": PRODUCTION_BASE_COMMIT,
        "projects": projects,
        "model_calls": cost["model_calls"],
        "stable_tasks": sum(value["stable_tasks"] for value in projects),
        "stable_lineages": len(stable),
        "unresolved": sum(value["unresolved"] for value in projects),
        "unsupported_accepted_matches": sum(
            value["unsupported_accepted_matches"] for value in projects
        ),
        "function_fragment_conflicts": capacity["function_fragment_conflict_count"],
        "right_map_conflicts": capacity["right_map_conflict_count"],
        "relation_type_counts": {
            "CONTINUED_1_TO_1": all_relations["CONTINUED_1_TO_1"],
            "SPLIT": all_relations["SPLIT_1_TO_N"],
            "MERGED": all_relations["MERGED_N_TO_1"],
            "FUNCTION_DISTRIBUTED": all_relations["FUNCTION_DISTRIBUTED"],
        },
        "vision_used": False,
        "production_run_executed": False,
        "shadow_enabled": False,
        "deployed": False,
        "materialization_applied": False,
        "verifier_changed_during_experiment": False,
        "candidate_generator_changed_during_experiment": False,
        "verdict": verdict["suggested_verdict"],
    }
    _write_jsonl(output / "task_decisions.jsonl", decisions)
    _write_jsonl(output / "stable_lineages.jsonl", stable)
    _write_json(output / "metrics.json", metrics)
    _write_json(output / "stability.json", {
        "policy": "Pass A/B per cold run; one concrete candidate unanimous across all three cold runs",
        "cold_runs": list(COLD_RUNS),
        "passes": list(PASSES),
        "unstable_tasks": unstable,
        "all_tasks": [{
            key: value[key] for key in (
                "project", "pair_id", "task_id", "left_physical_page",
                "final_status", "stable_across_passes", "stable_across_runs",
                "observation_distribution", "cold_run_distribution",
            )
        } for value in decisions],
    })
    _write_json(output / "capacity_audit.json", capacity)
    _write_json(output / "group_candidate_audit.json", group)
    _write_json(output / "ios21_controls.json", controls)
    _write_json(output / "ios21_old_new_comparison.json", comparison)
    _write_json(output / "cost_analysis.json", cost)
    _write_json(output / "verdict.json", verdict)
    (output / "report.md").write_text(
        _report(metrics, controls, cost, verdict), encoding="utf-8",
    )
    _write_json(output / "artifact_hashes.json", {
        str(path.relative_to(output)): _sha_file(path)
        for path in sorted(output.iterdir()) if path.is_file()
        and path.name != "artifact_hashes.json"
    })
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "experiment", "finalize"))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--effort", default=DEFAULT_EFFORT)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()
    output = args.output.resolve()
    if args.phase == "prepare":
        value = prepare(
            output, model=args.model, effort=args.effort, workers=args.workers,
        )
        print(json.dumps({
            "output": str(output),
            "model_calls_at_freeze": value["model_calls_at_freeze"],
            "model_configuration": value["model_configuration"],
            "artifacts": {
                pair_id: {
                    "artifact_sha256": row["artifact_sha256"],
                    "task_count": row["task_count"],
                    "candidate_count": row["candidate_count"],
                }
                for pair_id, row in value["artifacts"].items()
            },
        }, ensure_ascii=False, indent=2))
    elif args.phase == "experiment":
        values = experiment(output)
        print(json.dumps({
            "output": str(output), "model_calls": len(values),
            "successful": sum(value["model_call"]["ok"] for value in values),
        }, ensure_ascii=False, indent=2))
    else:
        value = finalize(output)
        print(json.dumps({
            "output": str(output), "metrics": value,
        }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
