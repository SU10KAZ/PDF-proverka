"""Run the IOS2.1 Function Lineage v2.2.1 provider-safe critical smoke.

The harness consumes the tracked v2.1 compact transport artifacts byte for
byte.  It never rebuilds candidate generation, passports, evidence, or task
projection.  Exactly four task contexts are selected from the frozen control
order, repacked without modification into bounded smoke shards, and observed
in three cold Pass A/B repeats.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import statistics
import subprocess
import time
import uuid
from collections import Counter
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
)
from experiments.function_lineage_v2 import transport


REPO_ROOT = Path(__file__).resolve().parents[2]
CANDIDATE_COMMIT = "2bcb832f51c46867c56d49d81549d9cac5918e96"
INVALID_REPEAT_COMMIT = "46e7a26e76cc9dcdaf7cc6841a7d7fd112f1583e"
TRANSPORT_COMMIT = "67b9f4e43067590d952d805f72c590c30fce1375"
FIRST_SMOKE_HARNESS_COMMIT = "ed90f576100b44bd204666c735a6d29f88f85241"
INVALID_SMOKE_COMMIT = "6e0965c44b3ac4b5810b14f10dca988a6dbf907e"
PRODUCTION_HEAD_AT_START = "4d489bf9033ad40c40099fe5e1436493bc56c0ed"
PRODUCTION_RELEASE_AT_START = "ui-real-4d489bf9"
PAIR_ID = "pe336037597"
PASSES = ("A", "B")
COLD_RUNS = (1, 2, 3)
FROZEN_ROOT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_1_transport"
)
CANDIDATE_INPUT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_deterministic"
    / "candidate_artifacts" / f"{PAIR_ID}.json"
)
INVALID_SMOKE_ROOT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_2_ios21_critical_smoke"
)
INVALID_SMOKE_MANIFEST = INVALID_SMOKE_ROOT / "input_manifest.json"
DEFAULT_OUTPUT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_2_1_ios21_critical_smoke"
)
FROZEN_FILES = (
    "selector_transport_manifest.json",
    "selector_shards.jsonl",
    "selector_transport_metrics.json",
    "report.md",
)
TASKS = {
    "LEFT17": "ltask_0ea09fe595c5fbe81d8b",
    "LEFT18": "ltask_dca76a53cf8b39004e96",
    "LEFT19": "ltask_015d2dbabecfea8054ea",
    "LEFT20": "ltask_4efcf3c03235385a614e",
}
MODEL_CONFIGURATION = {
    "codex_ephemeral_session": True,
    "cold_runs": list(COLD_RUNS),
    "independent_cli_process_per_observation": True,
    "model": "gpt-5.6-sol",
    "passes": list(PASSES),
    "reasoning_effort": "low",
    "retries": 0,
    "structured_output": True,
    "temperature": "provider_default_not_overridden",
    "timeout_seconds": 420,
    "transport": ai_settings.CODEX_SESSION,
    "vision": False,
    "workers": 3,
}
EXPECTED_MODEL_CONFIGURATION_SHA256 = (
    "5ca9a244cd831d5a9a88b4914a01e56e3054cfba01db9fc7086c12d62acc4411"
)


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object: {path}")
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
    return subprocess.check_output(["git", *args], cwd=REPO_ROOT, text=True).strip()


def _git_quiet(*args: str) -> bool:
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, check=False,
    ).returncode == 0


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _flags() -> dict[str, Any]:
    return {
        "shadow_enabled": ai_settings.function_lineage_shadow_enabled(),
        "materialization_enabled": ai_settings.function_lineage_materialization_enabled(),
    }


def _assert_isolated_flags() -> dict[str, Any]:
    flags = _flags()
    if any(flags.values()):
        raise RuntimeError(f"production Function Lineage flags are armed: {flags}")
    return flags


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


def _subset_dataset(
    dataset: lineage.FunctionLineageDataset, task_ids: Sequence[str],
) -> lineage.FunctionLineageDataset:
    wanted = set(task_ids)
    subset = copy.deepcopy(dataset)
    subset.tasks = [
        copy.deepcopy(task) for task in dataset.tasks
        if str(task["task_id"]) in wanted
    ]
    if {str(task["task_id"]) for task in subset.tasks} != wanted:
        raise RuntimeError(f"verifier task subset mismatch: {sorted(wanted)}")
    return subset


def _verify_frozen_files() -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    if not _git_quiet("merge-base", "--is-ancestor", TRANSPORT_COMMIT, "HEAD"):
        raise RuntimeError("bounded transport commit is not in the research lineage")
    pinned = [
        str((FROZEN_ROOT / name).relative_to(REPO_ROOT)) for name in FROZEN_FILES
    ] + [
        str(CANDIDATE_INPUT.relative_to(REPO_ROOT)),
    ]
    if not _git_quiet("diff", "--quiet", f"{TRANSPORT_COMMIT}..HEAD", "--", *pinned):
        raise RuntimeError("tracked frozen transport inputs changed after 67b9f4e4")
    if not _git_quiet("diff", "--quiet", "HEAD", "--", *pinned):
        raise RuntimeError("working-tree frozen transport inputs are modified")

    manifest = _read_json(FROZEN_ROOT / FROZEN_FILES[0])
    metrics = _read_json(FROZEN_ROOT / FROZEN_FILES[2])
    shards = _read_jsonl(FROZEN_ROOT / FROZEN_FILES[1])
    if manifest.get("candidate_source_commit") != CANDIDATE_COMMIT:
        raise RuntimeError("candidate source commit drift")
    if manifest.get("model_calls") != 0 or manifest.get("shadow_enabled"):
        raise RuntimeError("v2.1 source is not a deterministic frozen transport")
    hashes = {
        "selector_shards.jsonl": manifest["selector_shards_sha256"],
        "selector_transport_metrics.json": manifest[
            "selector_transport_metrics_sha256"
        ],
        "report.md": manifest["report_sha256"],
    }
    for name, expected in hashes.items():
        if _sha_file(FROZEN_ROOT / name) != expected:
            raise RuntimeError(f"frozen SHA-256 mismatch: {name}")
    project = manifest["projects"][PAIR_ID]
    raw = _read_json(CANDIDATE_INPUT)
    if _sha_file(CANDIDATE_INPUT) != project["candidate_input_sha256"]:
        raise RuntimeError("candidate input SHA-256 mismatch")
    if _sha_json(raw["function_passports"]) != project["function_passports_sha256"]:
        raise RuntimeError("Function Passports SHA-256 mismatch")
    if _sha_json(raw["evidence_catalog"]) != project["evidence_catalog_sha256"]:
        raise RuntimeError("evidence catalog SHA-256 mismatch")
    payloads = [value["model_payload"] for value in shards]
    if _sha_json(payloads) != manifest["generated_selector_payloads_sha256"]:
        raise RuntimeError("generated selector payload SHA-256 mismatch")
    for shard in shards:
        payload = shard["model_payload"]
        for pass_name in PASSES:
            prompt = transport.build_prompt(payload, pass_name)
            if _sha_bytes(prompt.encode("utf-8")) != shard[
                f"prompt_{pass_name.lower()}_sha256"
            ]:
                raise RuntimeError(f"frozen prompt drift: {shard['shard_id']}/{pass_name}")
    return manifest, metrics, shards


def _selected_contexts(
    metrics: Mapping[str, Any], shards: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    controls = metrics["ios21_controls"]
    frozen_control_tasks = {
        "LEFT17": controls["LEFT17_R27"]["matches"][0]["task_id"],
        "LEFT18": controls["LEFT18_R24"]["matches"][0]["task_id"],
        "LEFT19": controls["LEFT19_R30_R25"]["task_contexts"][0]["task_id"],
        "LEFT20": controls["LEFT20_DISTRIBUTED"]["task_context_occurrences"][0][
            "task_id"
        ],
    }
    if frozen_control_tasks != TASKS:
        raise RuntimeError(f"frozen critical control order drift: {frozen_control_tasks}")
    contexts: dict[str, dict[str, Any]] = {}
    occurrences: Counter[str] = Counter()
    for shard in shards:
        if shard.get("pair_id") != PAIR_ID:
            continue
        for context in shard["model_payload"]["task_contexts"]:
            task_id = str(context["task_id"])
            occurrences[task_id] += 1
            contexts[task_id] = copy.deepcopy(context)
    if any(occurrences[task_id] != 1 for task_id in TASKS.values()):
        raise RuntimeError("critical task does not occur exactly once in frozen shards")
    selected = [contexts[TASKS[label]] for label in TASKS]
    if [value["left_function_core"]["physical_page"] for value in selected] != [
        17, 18, 19, 20,
    ]:
        raise RuntimeError("critical LEFT page order drift")
    for context in selected:
        signature = context.get("task_context_signature")
        unsigned = {key: value for key, value in context.items() if key != "task_context_signature"}
        if signature != content_signature(unsigned):
            raise RuntimeError(f"frozen task context signature mismatch: {context['task_id']}")
    _assert_critical_inventory(selected)
    return selected


def _candidate_for(
    context: Mapping[str, Any], *, right_pages: Sequence[int], rank: int,
    relation_type: str | None = None,
) -> dict[str, Any]:
    rows = [
        value for value in context["functional_candidates"]
        if value["right_physical_pages"] == list(right_pages)
        and int(value["rank"]) == rank
        and (relation_type is None or value["relation_type"] == relation_type)
    ]
    if len(rows) != 1:
        raise RuntimeError(
            f"critical candidate inventory mismatch for {context['task_id']}: {rows}"
        )
    return copy.deepcopy(rows[0])


def _assert_critical_inventory(contexts: Sequence[Mapping[str, Any]]) -> None:
    by_page = {
        int(value["left_function_core"]["physical_page"]): value
        for value in contexts
    }
    _candidate_for(by_page[17], right_pages=[27], rank=1)
    _candidate_for(by_page[18], right_pages=[24], rank=1)
    _candidate_for(by_page[19], right_pages=[30], rank=1)
    _candidate_for(by_page[19], right_pages=[25], rank=2)
    group = next(
        (
            value for value in by_page[20]["functional_candidates"]
            if value["candidate_id"] == "lcand_9c617494b14c2b922d3f"
        ),
        None,
    )
    if not group or group["relation_type"] != "FUNCTION_DISTRIBUTED":
        raise RuntimeError("LEFT20 distributed candidate missing")
    if group["right_physical_pages"] != [26, 28, 29] or int(group["rank"]) != 1:
        raise RuntimeError("LEFT20 distributed candidate rank/pages drift")
    if len(group.get("component_map") or []) != 3:
        raise RuntimeError("LEFT20 distributed component map is not atomic")


def _candidate_inventory(context: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": value["candidate_id"],
            "rank": value["rank"],
            "relation_type": value["relation_type"],
            "right_physical_pages": value["right_physical_pages"],
            "right_function_ids": [
                row["function_id"] for row in value.get("right_functions") or []
            ],
            "right_fragment_ids": value["right_fragment_ids"],
            "component_mapping": value.get("component_map") or [],
            "capacity_keys": value["capacity_keys"],
            "evidence_refs": value["evidence_ids"],
        }
        for value in context["functional_candidates"]
    ]


def build_frozen_smoke_inputs() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source_manifest, metrics, source_shards = _verify_frozen_files()
    contexts = _selected_contexts(metrics, source_shards)
    source_project = source_manifest["projects"][PAIR_ID]
    smoke_shards, oversized = transport.shard_task_contexts(
        PAIR_ID, source_project["candidate_input_signature"], contexts,
    )
    if oversized or len(smoke_shards) != 3:
        raise RuntimeError(f"unexpected critical smoke packing: {oversized}")
    if [value["task_ids"] for value in smoke_shards] != [
        [TASKS["LEFT17"], TASKS["LEFT18"]],
        [TASKS["LEFT19"]],
        [TASKS["LEFT20"]],
    ]:
        raise RuntimeError("critical smoke shard layout drift")
    if any(value["prompt_characters"] > transport.TARGET_CHARACTERS for value in smoke_shards):
        raise RuntimeError("critical smoke shard exceeds target budget")
    metadata = {
        "source_manifest_sha256": _sha_file(FROZEN_ROOT / FROZEN_FILES[0]),
        "source_shards_sha256": _sha_file(FROZEN_ROOT / FROZEN_FILES[1]),
        "source_metrics_sha256": _sha_file(FROZEN_ROOT / FROZEN_FILES[2]),
        "candidate_input_sha256": _sha_file(CANDIDATE_INPUT),
        "function_passports_sha256": source_project["function_passports_sha256"],
        "evidence_catalog_sha256": source_project["evidence_catalog_sha256"],
        "selected_task_contexts_sha256": _sha_json(contexts),
        "selected_tasks": {
            label: {
                "task_id": context["task_id"],
                "physical_page": context["left_function_core"]["physical_page"],
                "function_id": context["left_function_core"]["function_id"],
                "fragment_id": context["left_function_core"]["fragment_id"],
                "task_context_signature": context["task_context_signature"],
                "task_context_sha256": _sha_json(context),
                "candidate_inventory": _candidate_inventory(context),
            }
            for label, context in zip(TASKS, contexts)
        },
        "smoke_shard_count_per_pass": len(smoke_shards),
        "smoke_shard_characters": [
            value["prompt_characters"] for value in smoke_shards
        ],
    }
    previous = _read_json(INVALID_SMOKE_MANIFEST)["frozen_inputs"]
    for label in TASKS:
        current_task = metadata["selected_tasks"][label]
        previous_task = previous["selected_tasks"][label]
        if current_task["task_context_sha256"] != previous_task["task_context_sha256"]:
            raise RuntimeError(f"{label} task context differs from immutable invalid smoke")
        if canonical_json(current_task["candidate_inventory"]) != canonical_json(
            previous_task["candidate_inventory"]
        ):
            raise RuntimeError(f"{label} candidate inventory differs from immutable invalid smoke")
    metadata["previous_invalid_smoke_manifest_sha256"] = _sha_file(
        INVALID_SMOKE_MANIFEST
    )
    metadata["candidate_inventories_sha256"] = _sha_json({
        label: metadata["selected_tasks"][label]["candidate_inventory"]
        for label in TASKS
    })
    return metadata, smoke_shards


def _preflight_verifier(
    dataset: lineage.FunctionLineageDataset, shards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    failures = []
    capacity_key_defects = []
    for shard in shards:
        payload = shard["model_payload"]
        for context in payload["task_contexts"]:
            task_id = str(context["task_id"])
            subset = _subset_dataset(dataset, [task_id])
            for decision in context["allowed_decisions"]:
                response = {
                    "payload_signature": payload["payload_signature"],
                    "selections": [{"task_id": task_id, "candidate_id": decision}],
                }
                verified = lineage.verify_selector_response(
                    subset, str(payload["payload_signature"]), response,
                )
                if not verified["ok"]:
                    failures.append({
                        "task_id": task_id,
                        "candidate_id": decision,
                        "verification": verified,
                    })
            for candidate in context["functional_candidates"]:
                mapping = candidate.get("component_map") or []
                if not mapping:
                    continue
                expected = sorted({str(value["capacity_key"]) for value in mapping})
                actual = sorted(str(value) for value in candidate["capacity_keys"])
                if expected != actual:
                    capacity_key_defects.append({
                        "candidate_id": candidate["candidate_id"],
                        "expected": expected,
                        "actual": actual,
                    })
    return {
        "all_allowed_decisions_verifier_pass": not failures,
        "allowed_decision_failures": failures,
        "capacity_key_defects": capacity_key_defects,
    }


def _preflight_schemas(shards: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    failures = []
    schema_hashes = {}
    for shard in shards:
        shard_id = str(shard["shard_id"])
        payload = shard["model_payload"]
        schema = shard["output_schema"]
        problems = transport.provider_safe_schema_problems(schema)
        response = {
            "results": [
                {
                    "task_id": context["task_id"],
                    "decision": lineage.NEED_MORE_EVIDENCE,
                }
                for context in payload["task_contexts"]
            ],
        }
        local_errors = response_contract.validate(response, schema)
        transport_verification = transport.verify_transport_response(payload, response)
        if problems or local_errors or not transport_verification["ok"]:
            validation_error = {
                "provider_subset_problems": problems,
                "local_contract_errors": local_errors,
                "transport_verification": transport_verification,
            }
            failures.append({"shard_id": shard_id, **validation_error})
        schema_hashes[shard_id] = _sha_json(schema)
    return {
        "ok": not failures,
        "provider_safe_keywords": sorted(transport.PROVIDER_SAFE_SCHEMA_KEYWORDS),
        "provider_proven_reference": (
            "experiments/function_lineage_v1/core.py::output_schema; "
            "18/18 successful gpt-5.6-sol structured calls in "
            "comparison/ai_sheet_matcher/20260902_function_lineage_v1/model_runs.jsonl"
        ),
        "local_validators": [
            "stage_comparison.ai.response_contract",
            "function_lineage_v2.transport.verify_transport_response",
        ],
        "schema_hashes": schema_hashes,
        "failures": failures,
    }


def prepare(output: Path) -> dict[str, Any]:
    manifest_path = output / "input_manifest.json"
    inputs_path = output / "smoke_inputs.jsonl"
    if manifest_path.exists() or inputs_path.exists():
        raise RuntimeError(f"refusing to replace frozen smoke inputs: {output}")
    flags = _assert_isolated_flags()
    runtime = ai_gateway.validate_runtime(
        require_vision=False, deep=False, mode=ai_settings.MODE_OFF,
    )
    if not runtime.get("ok"):
        raise RuntimeError(f"isolated model runtime preflight failed: {runtime['problems']}")
    previous = _read_json(INVALID_SMOKE_MANIFEST)
    if previous["model_configuration"] != MODEL_CONFIGURATION:
        raise RuntimeError("model configuration differs from invalid repeat")
    if _sha_json(MODEL_CONFIGURATION) != EXPECTED_MODEL_CONFIGURATION_SHA256:
        raise RuntimeError("model configuration SHA-256 drift")

    frozen, smoke_shards = build_frozen_smoke_inputs()
    dataset = _dataset(_read_json(CANDIDATE_INPUT))
    schema_preflight = _preflight_schemas(smoke_shards)
    if not schema_preflight["ok"]:
        raise RuntimeError(f"provider-safe schema preflight failed: {schema_preflight['failures']}")
    preflight = _preflight_verifier(dataset, smoke_shards)
    if not preflight["all_allowed_decisions_verifier_pass"]:
        raise RuntimeError("existing verifier rejects a frozen allowed decision")
    if preflight["capacity_key_defects"]:
        raise RuntimeError("frozen candidate has invalid fragment capacity keys")
    _write_jsonl(inputs_path, smoke_shards)
    manifest = {
        "kind": "function_lineage_v2_2_1_ios21_critical_smoke_input",
        "schema_version": "function-lineage-v2.2.1-critical-smoke.v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_chain": [
            CANDIDATE_COMMIT, INVALID_REPEAT_COMMIT, TRANSPORT_COMMIT,
            FIRST_SMOKE_HARNESS_COMMIT, INVALID_SMOKE_COMMIT,
        ],
        "checkout_head_before_calls": _git("rev-parse", "HEAD"),
        "production_head_at_start": PRODUCTION_HEAD_AT_START,
        "production_release_at_start": PRODUCTION_RELEASE_AT_START,
        "origin_main_observed_before_calls": _git("rev-parse", "origin/main"),
        "selection_policy": "first task occurrence recorded by each frozen v2.1 critical control",
        "frozen_inputs": frozen,
        "smoke_inputs_path": _display_path(inputs_path),
        "smoke_inputs_sha256": _sha_file(inputs_path),
        "model_configuration": MODEL_CONFIGURATION,
        "model_configuration_sha256": _sha_json(MODEL_CONFIGURATION),
        "research_harness_sha256": _sha_file(Path(__file__)),
        "runtime_flags": flags,
        "runtime_preflight": runtime,
        "schema_preflight": schema_preflight,
        "verifier_preflight": preflight,
        "request_attempts_at_freeze": 0,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    }
    _write_json(manifest_path, manifest)
    return manifest


def _validate_prepared(
    output: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], lineage.FunctionLineageDataset]:
    manifest = _read_json(output / "input_manifest.json")
    inputs_path = output / "smoke_inputs.jsonl"
    if _sha_file(inputs_path) != manifest["smoke_inputs_sha256"]:
        raise RuntimeError("prepared smoke inputs changed")
    if _sha_file(Path(__file__)) != manifest["research_harness_sha256"]:
        raise RuntimeError("smoke harness changed after input freeze")
    if _flags() != manifest["runtime_flags"] or any(_flags().values()):
        raise RuntimeError("Function Lineage runtime flags changed after input freeze")
    frozen, rebuilt = build_frozen_smoke_inputs()
    smoke_shards = _read_jsonl(inputs_path)
    if _sha_json(smoke_shards) != _sha_json(rebuilt):
        raise RuntimeError("prepared smoke inputs do not match frozen transport contexts")
    stable_keys = {
        "source_manifest_sha256", "source_shards_sha256", "source_metrics_sha256",
        "candidate_input_sha256", "function_passports_sha256",
        "evidence_catalog_sha256", "selected_task_contexts_sha256",
        "previous_invalid_smoke_manifest_sha256", "candidate_inventories_sha256",
    }
    if {key: frozen[key] for key in stable_keys} != {
        key: manifest["frozen_inputs"][key] for key in stable_keys
    }:
        raise RuntimeError("frozen source hashes changed after smoke preparation")
    return manifest, smoke_shards, _dataset(_read_json(CANDIDATE_INPUT))


def _usage_total(usage: Mapping[str, Any]) -> int:
    if isinstance(usage.get("total_tokens"), (int, float)):
        return int(usage["total_tokens"])
    if isinstance(usage.get("total_input_tokens"), (int, float)):
        return int(usage["total_input_tokens"]) + int(usage.get("output_tokens") or 0)
    return int(usage.get("input_tokens") or 0) + int(usage.get("output_tokens") or 0)


def _classify_request_failure(
    *, ok: bool, error: str = "", raw_excerpt: str = "", error_kind: str = "",
) -> str | None:
    if ok:
        return None
    diagnostic = f"{error}\n{raw_excerpt}".lower()
    if "invalid_json_schema" in diagnostic or "invalid schema for response_format" in diagnostic:
        return "SCHEMA_FAILURE"
    return f"MODEL_RUNTIME_FAILURE:{error_kind or 'UNKNOWN'}"


def _model_job(
    shard: Mapping[str, Any], *, cold_run: int, pass_name: str,
    manifest: Mapping[str, Any], experiment_id: str,
    dataset: lineage.FunctionLineageDataset,
) -> dict[str, Any]:
    payload = shard["model_payload"]
    prompt = transport.build_prompt(payload, pass_name)
    expected_prompt_hash = shard[f"prompt_{pass_name.lower()}_sha256"]
    prompt_hash = _sha_bytes(prompt.encode("utf-8"))
    if prompt_hash != expected_prompt_hash:
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
    transport_verification = transport.verify_transport_response(payload, response)
    translated = None
    existing_verification: dict[str, Any] = {
        "applicable": False, "ok": None,
        "global_errors": ["NOT_REACHED"],
        "task_results": {},
    }
    if transport_verification["ok"]:
        translated = {
            "payload_signature": payload["payload_signature"],
            "selections": [{
                "task_id": value["task_id"],
                "candidate_id": value["decision"],
            } for value in response["results"]],
        }
        subset = _subset_dataset(dataset, payload["task_ids"])
        existing_verification = lineage.verify_selector_response(
            subset, str(payload["payload_signature"]), translated,
        )
        existing_verification["applicable"] = True
    usage = dict(call.usage or {})
    failure_kind = _classify_request_failure(
        ok=bool(call.ok), error=call.error, raw_excerpt=call.raw_excerpt,
        error_kind=call.error_kind,
    )
    return {
        "cold_run": cold_run,
        "pass_name": pass_name,
        "pair_id": PAIR_ID,
        "shard_id": shard["shard_id"],
        "task_ids": list(shard["task_ids"]),
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
            "tokens": _usage_total(usage),
            "error": call.error,
            "error_kind": call.error_kind,
            "failure_kind": failure_kind,
            "attempts": int(call.attempts),
            "exit_code": call.exit_code,
            "provider_session_id": call.session_id,
            "raw_excerpt": call.raw_excerpt,
        },
        "response": response,
        "translated_response_for_existing_verifier": translated,
        "transport_verification": transport_verification,
        "existing_verifier": existing_verification,
        "cross_shard_capacity": None,
    }


def _apply_cross_shard_capacity(records: Sequence[dict[str, Any]]) -> list[str]:
    if any(
        not record["model_call"]["ok"]
        or not record["transport_verification"]["ok"]
        for record in records
    ):
        for record in records:
            record["cross_shard_capacity"] = {
                "applicable": False,
                "ok": None,
                "errors": [],
                "reason": "INCOMPLETE_OR_INVALID_BATCH",
            }
        return []
    selections = []
    for record in records:
        response = record.get("translated_response_for_existing_verifier") or {}
        selections.extend(response.get("selections") or [])
    dataset = _dataset(_read_json(CANDIDATE_INPUT))
    errors = lineage.verify_capacity(selections, dataset.candidates)
    for record in records:
        record["cross_shard_capacity"] = {
            "applicable": True,
            "ok": not errors,
            "errors": list(errors),
        }
    return errors


def _request_counters(records: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    failed = [record for record in records if not record["model_call"]["ok"]]
    schema_failures = sum(
        record["model_call"].get("failure_kind") == "SCHEMA_FAILURE"
        for record in failed
    )
    runtime_failures = sum(
        str(record["model_call"].get("failure_kind") or "").startswith(
            "MODEL_RUNTIME_FAILURE:"
        )
        for record in failed
    )
    semantic_failures = sum(
        bool(record["model_call"]["ok"])
        and not record["transport_verification"]["ok"]
        for record in records
    )
    affected = sum(
        len(record["task_ids"])
        for record in records
        if not record["model_call"]["ok"]
        or not record["transport_verification"]["ok"]
    )
    if len(failed) != schema_failures + runtime_failures:
        raise RuntimeError("request failure counters do not partition failed requests")
    return {
        "request_attempts": sum(
            int(record["model_call"].get("attempts") or 0) for record in records
        ),
        "request_start_failures": len(failed),
        "successful_inference_requests": sum(
            bool(record["model_call"]["ok"]) for record in records
        ),
        "affected_task_observations": affected,
        "schema_failures": schema_failures,
        "model_runtime_failures": runtime_failures,
        "semantic_response_failures": semantic_failures,
    }


def experiment(output: Path) -> list[dict[str, Any]]:
    records_path = output / "model_runs.jsonl"
    if records_path.exists():
        raise RuntimeError(f"refusing to repeat or append observations: {records_path}")
    manifest, shards, dataset = _validate_prepared(output)
    jobs_total = len(shards) * len(PASSES) * len(COLD_RUNS)
    records: list[dict[str, Any]] = []
    experiment_id = "flv2.2.1-ios21-smoke-" + uuid.uuid4().hex
    started = time.monotonic()
    stopped_early = False
    stop_reason = None
    for cold_run in COLD_RUNS:
        if stopped_early:
            break
        for pass_name in PASSES:
            batch: list[dict[str, Any]] = []
            with ThreadPoolExecutor(
                max_workers=int(manifest["model_configuration"]["workers"])
            ) as pool:
                futures = {
                    pool.submit(
                        _model_job,
                        shard,
                        cold_run=cold_run,
                        pass_name=pass_name,
                        manifest=manifest,
                        experiment_id=experiment_id,
                        dataset=dataset,
                    ): shard["shard_id"]
                    for shard in shards
                }
                try:
                    for future in as_completed(futures):
                        batch.append(future.result())
                except Exception:
                    ai_gateway.kill_live_processes(experiment_id)
                    raise
            batch.sort(key=lambda value: value["shard_id"])
            capacity_errors = _apply_cross_shard_capacity(batch)
            records.extend(batch)
            records.sort(key=lambda value: (
                int(value["cold_run"]), str(value["pass_name"]),
                str(value["shard_id"]),
            ))
            _write_jsonl(records_path, records)
            completed = len(records)
            successful = sum(value["model_call"]["ok"] for value in batch)
            print(
                f"{completed}/{jobs_total} cold={cold_run} pass={pass_name} "
                f"model_ok={successful}/{len(batch)} capacity_errors={len(capacity_errors)}",
                flush=True,
            )
            technical_failures = [
                value for value in batch
                if not value["model_call"]["ok"]
                or not value["transport_verification"]["ok"]
            ]
            if technical_failures:
                stopped_early = True
                stop_reason = "TECHNICAL_OR_MODEL_TRANSPORT_FAILURE"
                break
    counters = _request_counters(records)
    telemetry = {
        "experiment_id": experiment_id,
        "planned_requests": jobs_total,
        "request_records": len(records),
        **counters,
        "counter_definitions": {
            "request_attempts": "provider attempts reported by the gateway",
            "request_start_failures": "request records with no successful inference response",
            "successful_inference_requests": "request records with a parsed inference response",
            "affected_task_observations": "task observations affected by a request or semantic response failure",
            "schema_failures": "request-level invalid provider schema rejections",
            "model_runtime_failures": "request-level non-schema failures before a parsed response",
            "semantic_response_failures": "successful inference responses rejected by task-local parsing",
        },
        "stopped_early": stopped_early,
        "stop_reason": stop_reason,
        "wall_time_ms": int((time.monotonic() - started) * 1000),
        "model_runtime_ms": sum(value["model_call"]["duration_ms"] for value in records),
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
        "input_changed_after_first_call": False,
        "code_changed_after_first_call": False,
    }
    _write_json(output / "run_telemetry.json", telemetry)
    return records


def _observations(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    output = {task_id: [] for task_id in TASKS.values()}
    for record in records:
        transport_tasks = record["transport_verification"].get("task_results") or {}
        verifier_tasks = record["existing_verifier"].get("task_results") or {}
        capacity = record.get("cross_shard_capacity") or {"ok": None, "errors": []}
        for task_id in record["task_ids"]:
            transport_task = transport_tasks.get(task_id) or {}
            verifier_task = verifier_tasks.get(task_id) or {}
            inference_succeeded = bool(record["model_call"]["ok"])
            output[task_id].append({
                "cold_run": record["cold_run"],
                "pass_name": record["pass_name"],
                "decision": transport_task.get("decision"),
                "model_ok": inference_succeeded,
                "request_failure_kind": record["model_call"].get("failure_kind"),
                "response_contract_ok": (
                    bool(record["transport_verification"]["ok"])
                    if inference_succeeded else None
                ),
                "verifier_ok": verifier_task.get("ok"),
                "verifier_errors": verifier_task.get("errors") or [],
                "capacity_ok": capacity.get("ok"),
                "capacity_errors": capacity.get("errors") or [],
                "shard_id": record["shard_id"],
            })
    for values in output.values():
        values.sort(key=lambda value: (value["cold_run"], value["pass_name"]))
    return output


def _stage_result(values: Sequence[Mapping[str, Any]], key: str) -> str:
    reached = [value[key] for value in values if value.get(key) is not None]
    if not reached or len(reached) != len(values) or len(values) != 6:
        return "N/A"
    return "PASS" if all(reached) else "FAIL"


def _task_results(
    manifest: Mapping[str, Any], records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    observations = _observations(records)
    rows = []
    frozen_tasks = manifest["frozen_inputs"]["selected_tasks"]
    for label, task_id in TASKS.items():
        values = observations[task_id]
        repeat_rows = []
        for cold_run in COLD_RUNS:
            pair = [value for value in values if value["cold_run"] == cold_run]
            by_pass = {value["pass_name"]: value for value in pair}
            if set(by_pass) != set(PASSES):
                status = "REQUEST_NOT_OBSERVED"
                stable_decision = None
            elif not all(value["model_ok"] for value in pair):
                status = "REQUEST_START_FAILURE"
                stable_decision = None
            elif not all(value["response_contract_ok"] for value in pair):
                status = "RESPONSE_CONTRACT_REJECTION"
                stable_decision = None
            elif not all(value["verifier_ok"] for value in pair):
                status = "VERIFIER_REJECTION"
                stable_decision = None
            elif not all(value["capacity_ok"] for value in pair):
                status = "CAPACITY_REJECTION"
                stable_decision = None
            elif by_pass["A"]["decision"] != by_pass["B"]["decision"]:
                status = "PASS_DISAGREEMENT"
                stable_decision = None
            else:
                status = "STABLE_PASS_PAIR"
                stable_decision = by_pass["A"]["decision"]
            repeat_rows.append({
                "cold_run": cold_run,
                "pass_a": by_pass.get("A", {}).get("decision"),
                "pass_b": by_pass.get("B", {}).get("decision"),
                "status": status,
                "stable_decision": stable_decision,
            })
        stable = [
            value["stable_decision"] for value in repeat_rows
            if value["status"] == "STABLE_PASS_PAIR"
        ]
        distribution = Counter(
            str(value.get("decision") or "<NO_SELECTION>") for value in values
        )
        stable_across_cold_runs = len(stable) == 3 and len(set(stable)) == 1
        stable_decision = stable[0] if stable_across_cold_runs else None
        rows.append({
            "label": label,
            "task_id": task_id,
            "physical_page": frozen_tasks[label]["physical_page"],
            "function_id": frozen_tasks[label]["function_id"],
            "fragment_id": frozen_tasks[label]["fragment_id"],
            "candidate_inventory": frozen_tasks[label]["candidate_inventory"],
            "observations": values,
            "cold_repeats": repeat_rows,
            "selection_distribution": dict(sorted(distribution.items())),
            "stable_repeat_count": len(stable),
            "verifier_result": _stage_result(values, "verifier_ok"),
            "capacity_result": _stage_result(values, "capacity_ok"),
            "schema_failure_observations": sum(
                value["request_failure_kind"] == "SCHEMA_FAILURE" for value in values
            ),
            "model_runtime_failure_observations": sum(
                str(value["request_failure_kind"] or "").startswith(
                    "MODEL_RUNTIME_FAILURE:"
                )
                for value in values
            ),
            "semantic_response_failure_observations": sum(
                value["model_ok"] and value["response_contract_ok"] is False
                for value in values
            ),
            "stable_across_cold_runs": stable_across_cold_runs,
            "stable_decision": stable_decision,
            "stable_unresolved": (
                stable_decision == lineage.NEED_MORE_EVIDENCE
            ),
            "auto_match": (
                stable_decision
                if stable_decision not in {None, lineage.NEED_MORE_EVIDENCE}
                else None
            ),
        })
    return rows


def _cost(
    records: Sequence[Mapping[str, Any]], telemetry: Mapping[str, Any],
) -> dict[str, Any]:
    usages = [dict(value["model_call"].get("usage") or {}) for value in records]
    input_tokens = sum(
        int(value.get("total_input_tokens") or value.get("input_tokens") or 0)
        for value in usages
    )
    output_tokens = sum(int(value.get("output_tokens") or 0) for value in usages)
    total_tokens = sum(_usage_total(value) for value in usages)
    telemetry_defect = any(
        record["model_call"]["ok"] and _usage_total(usage) == 0
        for record, usage in zip(records, usages)
    )
    return {
        **_request_counters(records),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "model_runtime_ms": int(telemetry["model_runtime_ms"]),
        "wall_time_ms": int(telemetry["wall_time_ms"]),
        "telemetry_defect": telemetry_defect,
        "telemetry_assessment": (
            "Successful calls returned usage={} / zero tokens; token telemetry is defective."
            if telemetry_defect else "Token telemetry was returned for every successful call."
        ),
    }


def _capacity_audit(
    records: Sequence[Mapping[str, Any]], task_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    all_errors = sorted({
        str(error)
        for record in records
        for error in (record.get("cross_shard_capacity") or {}).get("errors") or []
    })
    group = next(
        candidate
        for task in task_results if task["label"] == "LEFT20"
        for candidate in task["candidate_inventory"]
        if candidate["candidate_id"] == "lcand_9c617494b14c2b922d3f"
    )
    expected_keys = sorted({
        str(value["capacity_key"]) for value in group["component_mapping"]
    })
    return {
        "capacity_errors": all_errors,
        "function_fragment_conflict_count": sum(
            value.startswith("FUNCTION_FRAGMENT_CONFLICT:") for value in all_errors
        ),
        "right_map_conflict_count": sum(
            "RIGHT_MAP_CONFLICT" in value for value in all_errors
        ),
        "multiple_functions_on_same_right_page_allowed_by_fragment_key": True,
        "left20_capacity_keys_exact": sorted(group["capacity_keys"]) == expected_keys,
        "left20_expected_capacity_keys": expected_keys,
    }


def _verdict(
    records: Sequence[Mapping[str, Any]], telemetry: Mapping[str, Any],
    tasks: Sequence[Mapping[str, Any]], capacity: Mapping[str, Any],
) -> dict[str, Any]:
    expected_calls = 3 * 2 * 3
    technical_failure = (
        telemetry["stopped_early"]
        or len(records) != expected_calls
        or any(not value["model_call"]["ok"] for value in records)
        or any(not value["transport_verification"]["ok"] for value in records)
    )
    verifier_capacity_defect = (
        bool(capacity["capacity_errors"])
        or not capacity["left20_capacity_keys_exact"]
        or any(value["verifier_result"] != "PASS" for value in tasks)
    )
    context_loss = any(
        not value["candidate_inventory"] or value["stable_unresolved"]
        for value in tasks
    )
    stable = all(value["stable_across_cold_runs"] for value in tasks)
    if technical_failure:
        code = "E"
        reason = "technical/model transport failure; experiment NOT VALID"
    elif verifier_capacity_defect:
        code = "D"
        reason = "existing verifier or fragment-capacity validation rejected an observation"
    elif context_loss:
        code = "C"
        reason = "bounded compact context left at least one critical task stably unresolved"
    elif stable:
        code = "A"
        reason = "critical IOS2.1 selector smoke is stable and safe"
    else:
        code = "B"
        reason = "candidate/transport are valid, but selector is not cold-repeat stable"
    return {
        "verdict": code,
        "reason": reason,
        "experiment_valid": code != "E",
        "broader_corpus_ai_evaluation_allowed": code == "A",
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
    }


def _report(
    tasks: Sequence[Mapping[str, Any]], cost: Mapping[str, Any],
    capacity: Mapping[str, Any], verdict: Mapping[str, Any],
) -> str:
    lines = [
        "# Function Lineage v2.2.1 — IOS2.1 provider-safe critical AI smoke",
        "",
        f"Frozen compact transport: `{TRANSPORT_COMMIT}`. Model: "
        f"`{MODEL_CONFIGURATION['model']}/{MODEL_CONFIGURATION['reasoning_effort']}`.",
        "",
        "Exactly four frozen task contexts; three cold repeats; Pass A/B; no majority vote.",
        "",
        "| Task | Candidate count | Cold 1 A/B | Cold 2 A/B | Cold 3 A/B | Distribution | Stable repeats | Verifier | Capacity | Schema failures¹ | Model failures¹ |",
        "|---|---:|---|---|---|---|---:|---|---|---:|---:|",
    ]
    for task in tasks:
        cold = task["cold_repeats"]
        cells = [f"{value['pass_a']} / {value['pass_b']}" for value in cold]
        lines.append(
            f"| {task['label']} | {len(task['candidate_inventory'])} | "
            f"{cells[0]} | {cells[1]} | {cells[2]} | "
            f"`{json.dumps(task['selection_distribution'], sort_keys=True)}` | "
            f"{task['stable_repeat_count']}/3 | {task['verifier_result']} | "
            f"{task['capacity_result']} | {task['schema_failure_observations']} | "
            f"{task['model_runtime_failure_observations']} |"
        )
    left20 = next(value for value in tasks if value["label"] == "LEFT20")
    group = next(
        value for value in left20["candidate_inventory"]
        if value["candidate_id"] == "lcand_9c617494b14c2b922d3f"
    )
    lines.extend([
        "",
        "¹ Per-task affected observations; request-level failure counters below are not derived by summing these columns.",
        "",
        "## LEFT20 distributed candidate",
        "",
        f"Candidate `{group['candidate_id']}`; RIGHT pages `{group['right_physical_pages']}`; "
        f"functions `{group['right_function_ids']}`; fragments `{group['right_fragment_ids']}`; "
        f"capacity keys `{group['capacity_keys']}`; evidence refs `{len(group['evidence_refs'])}`.",
        "",
        f"Atomic component mapping: `{json.dumps(group['component_mapping'], ensure_ascii=False, sort_keys=True)}`.",
        "",
        "## Runtime and safety",
        "",
        f"Request attempts `{cost['request_attempts']}`; successful inference requests "
        f"`{cost['successful_inference_requests']}`; request-start failures "
        f"`{cost['request_start_failures']}`; affected task observations "
        f"`{cost['affected_task_observations']}`.",
        "",
        f"Schema failures `{cost['schema_failures']}`; model runtime failures "
        f"`{cost['model_runtime_failures']}`; semantic response failures "
        f"`{cost['semantic_response_failures']}`.",
        "",
        f"Input/output/total tokens "
        f"`{cost['input_tokens']}/{cost['output_tokens']}/{cost['total_tokens']}`; "
        f"model runtime `{cost['model_runtime_ms']} ms`; wall time `{cost['wall_time_ms']} ms`.",
        "",
        f"Telemetry defect: `{cost['telemetry_defect']}` — {cost['telemetry_assessment']}",
        "",
        f"Capacity errors `{len(capacity['capacity_errors'])}`; RIGHT_MAP_CONFLICT "
        f"`{capacity['right_map_conflict_count']}`.",
        "",
        "Production runs `0`; deploy `NO`; shadow `OFF`; materialization `NO`; Vision `NO`.",
        "",
        "## Verdict",
        "",
        f"**{verdict['verdict']} — {verdict['reason']}.**",
        "",
    ])
    return "\n".join(lines)


def finalize(output: Path) -> dict[str, Any]:
    manifest, _, _ = _validate_prepared(output)
    records = _read_jsonl(output / "model_runs.jsonl")
    telemetry = _read_json(output / "run_telemetry.json")
    tasks = _task_results(manifest, records)
    capacity = _capacity_audit(records, tasks)
    cost = _cost(records, telemetry)
    verdict = _verdict(records, telemetry, tasks, capacity)
    metrics = {
        "kind": "function_lineage_v2_2_1_ios21_critical_smoke_metrics",
        "task_count": 4,
        "cold_repeat_count": 3,
        "passes_per_repeat": 2,
        "shards_per_pass": 3,
        "observation_count_per_task": {
            value["label"]: len(value["observations"]) for value in tasks
        },
        "stable_repeat_count": {
            value["label"]: value["stable_repeat_count"] for value in tasks
        },
        "request_counters": {
            key: cost[key] for key in (
                "request_attempts", "request_start_failures",
                "successful_inference_requests", "affected_task_observations",
                "schema_failures", "model_runtime_failures",
                "semantic_response_failures",
            )
        },
        "cost": cost,
        "capacity": capacity,
        "verdict": verdict,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    }
    _write_json(output / "task_results.json", {"tasks": tasks})
    _write_json(output / "capacity_audit.json", capacity)
    _write_json(output / "metrics.json", metrics)
    _write_json(output / "verdict.json", verdict)
    (output / "report.md").write_text(
        _report(tasks, cost, capacity, verdict), encoding="utf-8",
    )
    artifact_names = (
        "input_manifest.json", "smoke_inputs.jsonl", "model_runs.jsonl",
        "run_telemetry.json", "task_results.json", "capacity_audit.json",
        "metrics.json", "verdict.json", "report.md",
    )
    _write_json(output / "artifact_hashes.json", {
        "files": {
            name: {"sha256": _sha_file(output / name), "bytes": (output / name).stat().st_size}
            for name in artifact_names
        },
        "request_attempts": cost["request_attempts"],
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
    })
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "experiment", "finalize", "all"))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    output = args.output.resolve()
    if args.phase in {"prepare", "all"}:
        prepare(output)
        print(f"frozen smoke inputs: {output}", flush=True)
    if args.phase in {"experiment", "all"}:
        experiment(output)
    if args.phase in {"finalize", "all"}:
        metrics = finalize(output)
        print(json.dumps(metrics, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
