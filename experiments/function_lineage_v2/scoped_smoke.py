"""Run the frozen Function Lineage v2.4.2 IOS2.1 scoped critical smoke.

The harness consumes only the seven critical contexts frozen by v2.4.1.  It
removes FUNCTION_REMOVED from the experiment output contract, freezes the
derived smoke payloads before inference, and performs three cold Pass A/B
repeats without changing model, prompt, ordering, parser, verifier, or inputs.
"""
from __future__ import annotations

import copy
import json
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
    stable_id,
)
from experiments.function_lineage_v2 import scoped_transport
from experiments.function_lineage_v2 import smoke as base_smoke
from experiments.function_lineage_v2 import transport


REPO_ROOT = Path(__file__).resolve().parents[2]
PAIR_ID = "pe336037597"
SCOPE_COMMIT = "6d2e7a5e4710765f0b5b8450c73c31431e070d13"
SCOPED_TRANSPORT_COMMIT = "edcaea0b997330b744f2c479783b9c3ced5e29ae"
PROVIDER_SAFE_COMMIT = "0655372c"
PRODUCTION_HEAD = "4d489bf9033ad40c40099fe5e1436493bc56c0ed"
PRODUCTION_RELEASE = "ui-real-4d489bf9"
PASSES = ("A", "B")
COLD_RUNS = (1, 2, 3)
SCOPED_ROOT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_4_1_scoped_transport"
)
SCOPE_ROOT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_4_scope_graph"
)
CANDIDATE_INPUT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_deterministic" / "candidate_artifacts"
    / f"{PAIR_ID}.json"
)
DEFAULT_OUTPUT = (
    REPO_ROOT / "comparison" / "ai_sheet_matcher"
    / "20260903_function_lineage_v2_4_2_ios21_critical_scoped_smoke"
)
SOURCE_FILES = {
    "scoped_selector_manifest.json": "430d016216b9c45bd1a11f509d4484e4444ea98fbe8a099ab0e1a3097d2c4ec5",
    "scoped_selector_shards.jsonl": "e9f5757d3e3a0d60bf29f547fd65b28e90323c1be4923ec26d69c3b7f66a011f",
    "scoped_selector_transport_metrics.json": "51d1ac2fe65e71328b2d96d7e44beea49a0e493bbbae89a92c54138b0ede0872",
    "ios21_critical_scoped_contexts.json": "a5409f6d7084075b16748000fce4f6e9fca2c9adc26df05372e290515a78eca2",
}
SCOPE_FILES = {
    "function_scope_graph.json": "f86c2911d8ac8c3d65295d3f5c43bb9befb53c7ca99716e2979f9ae2574aec29",
    "candidate_scope_membership.json": "a28cf9700151b98e7a5b1206dee08b3f9f17c59037510f57861736de9a1b0dba",
    "selector_tasks_scoped.json": "58432c19caa92b01e26e52d366a0344e1a63768b39f27ae83639d078ce2fa1af",
}
EXPECTED_CANDIDATE_SOURCE_SHA256 = (
    "fff15f4e711c209d10b4940c2e19f1d6d8120a40d0243e1781d0ad758472039d"
)
EXPECTED_FUNCTION_PASSPORTS_SHA256 = (
    "cfdd012ee90dc51ad80da49417766c82b85e8c8d05a34d02ad2d23abbb1ec15b"
)
EXPECTED_EVIDENCE_CATALOG_SHA256 = (
    "778f7da960fecb84c02e5b0a9ca3abf3cc9aa1201a33764ef3efa0cee9a915c0"
)
EXPECTED_CONTEXTS_SHA256 = (
    "94dbd6ee7f5c737672ffb5db380c06fb07111c341c973d98f865bceb39bbfb3f"
)
TASKS = {
    "LEFT17": ("fstask_e626d29f5317c598bf32", "fscope_2b9be69ab7ab0329c05c"),
    "LEFT18": ("fstask_3778375037ec99747b0c", "fscope_a0958d87cf3434c11438"),
    "LEFT19": ("fstask_135321825e7b00340f49", "fscope_d218cf99622ef6ffff16"),
    "LEFT20 DOMESTIC": ("fstask_c289ca22f53fcdbe6f99", "fscope_90a63adbb11d34d61f4b"),
    "LEFT20 FIRE": ("fstask_263e2f49af1b34aafb1c", "fscope_472f43e47a98f8cb7b35"),
    "LEFT20 METERING": ("fstask_3412531f08348a502fc6", "fscope_2bb6cd1e14a1c59c591e"),
    "LEFT20 PARENT": ("fstask_329baf4983e5d00118f2", "fscope_d1faafb9db1c9aca8074"),
}
SOURCE_LABELS = {
    "LEFT17": "LEFT17",
    "LEFT18": "LEFT18",
    "LEFT19": "LEFT19",
    "LEFT20 DOMESTIC child": "LEFT20 DOMESTIC",
    "LEFT20 FIRE child": "LEFT20 FIRE",
    "LEFT20 METERING child": "LEFT20 METERING",
    "LEFT20 composite parent": "LEFT20 PARENT",
}
MODEL_CONFIGURATION = copy.deepcopy(base_smoke.MODEL_CONFIGURATION)
MODEL_CONFIGURATION["workers"] = 4
PROMPT_LINES = (
    "Independent scoped verification pass {PASS}.",
    "You are a bounded engineering FUNCTION LINEAGE selector.",
    "Each task is exactly one FunctionScope.",
    "Only listed EXACT_SCOPE candidates are selectable for that task.",
    "STRICT_SUBSET, STRICT_SUPERSET, OVERLAP, DISJOINT, and UNKNOWN candidates are not answers for this scope.",
    "Choose exactly one listed candidate_id or NEED_MORE_EVIDENCE for every task.",
    "DOCUMENT_LINK is documentary navigation and is never a FUNCTIONAL_ANALOGUE.",
    "Use exact object/zone, function, component role and topology evidence.",
    "A RIGHT physical page may be reused only through distinct right_fragment_ids.",
    "SHEET_SHARED_EVIDENCE is limited sheet context; FRAGMENT_OWNED_EVIDENCE never transfers between fragments merely because they share a page.",
    "Do not invent pages, functions, fragments, scopes, groups, relations, or evidence.",
    "Return only the JSON object required by the output schema.",
)
DEPENDENCY_FILES = (
    Path(__file__),
    scoped_transport.__file__ and Path(scoped_transport.__file__),
    transport.__file__ and Path(transport.__file__),
    base_smoke.__file__ and Path(base_smoke.__file__),
    Path(lineage.__file__),
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


def _display_path(path: Path) -> str:
    return base_smoke._display_path(path)


def _dependency_hashes() -> dict[str, str]:
    return {_display_path(path): _sha_file(path) for path in DEPENDENCY_FILES if path}


def _assert_source_files() -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    for commit in (SCOPE_COMMIT, SCOPED_TRANSPORT_COMMIT, PROVIDER_SAFE_COMMIT):
        if not base_smoke._git_quiet("merge-base", "--is-ancestor", commit, "HEAD"):
            raise RuntimeError(f"research source is not an ancestor: {commit}")
    pinned = [SCOPED_ROOT / name for name in SOURCE_FILES]
    pinned.extend(SCOPE_ROOT / name for name in SCOPE_FILES)
    pinned.append(CANDIDATE_INPUT)
    relative = [_display_path(path) for path in pinned]
    if not base_smoke._git_quiet(
        "diff", "--quiet", f"{SCOPED_TRANSPORT_COMMIT}..HEAD", "--", *relative
    ):
        raise RuntimeError("tracked V2.4.1 frozen inputs changed")
    if not base_smoke._git_quiet("diff", "--quiet", "HEAD", "--", *relative):
        raise RuntimeError("working-tree V2.4.1 frozen inputs changed")
    actual_sources = {name: _sha_file(SCOPED_ROOT / name) for name in SOURCE_FILES}
    if actual_sources != SOURCE_FILES:
        raise RuntimeError(f"V2.4.1 scoped source SHA-256 drift: {actual_sources}")
    actual_scope = {name: _sha_file(SCOPE_ROOT / name) for name in SCOPE_FILES}
    if actual_scope != SCOPE_FILES:
        raise RuntimeError(f"V2.4 Function Scope Graph SHA-256 drift: {actual_scope}")
    if _sha_file(CANDIDATE_INPUT) != EXPECTED_CANDIDATE_SOURCE_SHA256:
        raise RuntimeError("candidate source SHA-256 drift")
    raw = _read_json(CANDIDATE_INPUT)
    if _sha_json(raw["function_passports"]) != EXPECTED_FUNCTION_PASSPORTS_SHA256:
        raise RuntimeError("Function Passports SHA-256 drift")
    if _sha_json(raw["evidence_catalog"]) != EXPECTED_EVIDENCE_CATALOG_SHA256:
        raise RuntimeError("evidence catalog SHA-256 drift")
    critical = _read_json(SCOPED_ROOT / "ios21_critical_scoped_contexts.json")
    contexts = [dict(value["context"]) for value in critical["tasks"]]
    if critical.get("contexts_sha256") != EXPECTED_CONTEXTS_SHA256:
        raise RuntimeError("recorded critical contexts SHA-256 drift")
    if _sha_json(contexts) != EXPECTED_CONTEXTS_SHA256:
        raise RuntimeError("seven selected critical contexts SHA-256 drift")
    manifest = _read_json(SCOPED_ROOT / "scoped_selector_manifest.json")
    if manifest.get("model_calls") != 0 or manifest.get("future_ai_smoke_status") != "NOT_RUN":
        raise RuntimeError("V2.4.1 source is not a no-model frozen transport")
    return raw, list(critical["tasks"]), manifest


def _candidate_inventory(context: Mapping[str, Any]) -> list[dict[str, Any]]:
    local_evidence = dict(context["local_evidence"])
    rows = []
    for candidate in context["functional_candidates"]:
        evidence_ids = [str(value) for value in candidate.get("evidence_ids") or []]
        missing = sorted(set(evidence_ids) - set(local_evidence))
        if missing:
            raise RuntimeError(f"candidate evidence is not task-local: {missing}")
        rows.append({
            "candidate_id": candidate["candidate_id"],
            "rank": candidate["rank"],
            "scope_relation": candidate["scope_relation"],
            "relation_type": candidate["relation_type"],
            "right_physical_pages": candidate["right_physical_pages"],
            "right_function_ids": [
                value["function_id"] for value in candidate.get("right_functions") or []
            ],
            "right_fragment_ids": candidate["right_fragment_ids"],
            "capacity_keys": candidate["capacity_keys"],
            "component_mapping": candidate.get("component_map") or [],
            "evidence_refs": evidence_ids,
            "evidence_sha256": _sha_json({key: local_evidence[key] for key in evidence_ids}),
        })
    return rows


def _smoke_context(source: Mapping[str, Any]) -> dict[str, Any]:
    context = copy.deepcopy(dict(source))
    candidate_ids = [
        str(value["candidate_id"]) for value in context["functional_candidates"]
    ]
    context["allowed_decisions"] = [*candidate_ids, lineage.NEED_MORE_EVIDENCE]
    context["scope_policy"]["function_removed_selectable"] = False
    context.pop("task_context_signature", None)
    context["task_context_signature"] = content_signature(context)
    return context


def _payload(input_signature: str, contexts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    task_ids = [str(value["task_id"]) for value in contexts]
    payload = {
        "schema_version": "function-lineage-scoped-smoke.v2.4.2",
        "transport_algorithm": "function-lineage-scoped-critical-smoke.v2.4.2",
        "candidate_algorithm": lineage.ALGORITHM_VERSION,
        "pair_id": PAIR_ID,
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
    payload["shard_id"] = stable_id("fssm_", PAIR_ID, task_ids)
    payload["payload_signature"] = content_signature(payload)
    return payload


def _prompt(payload: Mapping[str, Any], pass_name: str) -> str:
    if pass_name not in PASSES:
        raise ValueError(f"invalid pass: {pass_name}")
    return "\n".join([
        *(value.replace("{PASS}", pass_name) for value in PROMPT_LINES),
        "payload=" + canonical_json(payload),
    ])


def _build_shards(input_signature: str, contexts: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    shards: list[dict[str, Any]] = []
    current: list[Mapping[str, Any]] = []

    def characters(values: Sequence[Mapping[str, Any]]) -> int:
        return len(_prompt(_payload(input_signature, values), "A"))

    def emit(values: Sequence[Mapping[str, Any]]) -> None:
        payload = _payload(input_signature, values)
        prompts = {name: _prompt(payload, name) for name in PASSES}
        count = max(len(value) for value in prompts.values())
        if count > scoped_transport.HARD_CHARACTERS:
            raise RuntimeError(f"critical task exceeds hard gate: {payload['shard_id']}")
        schema = transport.output_schema(payload)
        problems = transport.provider_safe_schema_problems(schema)
        if problems or "oneOf" in canonical_json(schema):
            raise RuntimeError(f"provider-unsafe schema: {problems}")
        shards.append({
            "pair_id": PAIR_ID,
            "shard_id": payload["shard_id"],
            "task_ids": list(payload["task_ids"]),
            "scope_ids": list(payload["scope_ids"]),
            "model_payload": payload,
            "output_schema": schema,
            "provider_safe_schema_problems": problems,
            "prompt_a_sha256": base_smoke._sha_bytes(prompts["A"].encode()),
            "prompt_b_sha256": base_smoke._sha_bytes(prompts["B"].encode()),
            "prompt_characters": count,
        })

    for context in contexts:
        if characters([context]) > scoped_transport.HARD_CHARACTERS:
            raise RuntimeError(f"atomic critical task exceeds hard gate: {context['task_id']}")
        proposed = [*current, context]
        if current and characters(proposed) > scoped_transport.TARGET_CHARACTERS:
            emit(current)
            current = [context]
        else:
            current = proposed
    if current:
        emit(current)
    return shards


def _synthetic_dataset(
    raw: Mapping[str, Any], contexts: Sequence[Mapping[str, Any]]
) -> lineage.FunctionLineageDataset:
    dataset = base_smoke._dataset(raw)
    tasks = []
    for context in contexts:
        core = context["function_scope_core"]
        candidate_ids = [
            str(value["candidate_id"]) for value in context["functional_candidates"]
        ]
        tasks.append({
            "task_id": context["task_id"],
            "left_physical_page": int(core["source_physical_pages"][0]),
            "left_function_id": str(core["source_function_ids"][0]),
            "left_fragment_id": str(core["source_fragment_ids"][0]),
            "candidate_ids": candidate_ids,
            "candidate_ranks": {
                str(value["candidate_id"]): int(value["rank"])
                for value in context["functional_candidates"]
            },
            "allowed_outputs": [*candidate_ids, lineage.NEED_MORE_EVIDENCE],
        })
    dataset.tasks = tasks
    return dataset


def _task_local_verifier(
    dataset: lineage.FunctionLineageDataset,
    payload_signature: str,
    results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    task_results: dict[str, Any] = {}
    global_errors: list[str] = []
    for selection in results:
        task_id = str(selection["task_id"])
        subset = base_smoke._subset_dataset(dataset, [task_id])
        translated = {
            "payload_signature": payload_signature,
            "selections": [{
                "task_id": task_id,
                "candidate_id": selection["decision"],
            }],
        }
        verified = lineage.verify_selector_response(subset, payload_signature, translated)
        task_results[task_id] = verified["task_results"][task_id]
        global_errors.extend(str(value) for value in verified["global_errors"])
    return {
        "applicable": True,
        "ok": all(value["ok"] for value in task_results.values()),
        "global_errors": sorted(set(global_errors)),
        "task_results": task_results,
        "verifier_function": "function_lineage_shadow.verify_selector_response",
        "verification_granularity": "ONE_EXACT_FUNCTION_SCOPE",
    }


def _preflight(
    dataset: lineage.FunctionLineageDataset, shards: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    failures = []
    parser_fail_closed = True
    for shard in shards:
        payload = shard["model_payload"]
        valid = {"results": [
            {"task_id": context["task_id"], "decision": lineage.NEED_MORE_EVIDENCE}
            for context in payload["task_contexts"]
        ]}
        parsed = scoped_transport.verify_scoped_transport_response(payload, valid)
        contract_errors = response_contract.validate(valid, shard["output_schema"])
        if not parsed["ok"] or contract_errors:
            failures.append({"shard_id": shard["shard_id"], "parser": parsed, "contract": contract_errors})
        invalid = copy.deepcopy(valid)
        invalid["extra"] = True
        parser_fail_closed &= not scoped_transport.verify_scoped_transport_response(payload, invalid)["ok"]
        verifier = _task_local_verifier(dataset, payload["payload_signature"], valid["results"])
        if not verifier["ok"]:
            failures.append({"shard_id": shard["shard_id"], "verifier": verifier})
        for context in payload["task_contexts"]:
            if any(value.get("scope_relation") != "EXACT_SCOPE" for value in context["functional_candidates"]):
                failures.append({"task_id": context["task_id"], "error": "CROSS_GRANULARITY_CANDIDATE"})
            if lineage.FUNCTION_REMOVED in context["allowed_decisions"]:
                failures.append({"task_id": context["task_id"], "error": "FUNCTION_REMOVED_SELECTABLE"})
    return {
        "ok": not failures and parser_fail_closed,
        "failures": failures,
        "parser_fail_closed": parser_fail_closed,
        "cross_granularity_selectable_competition": 0 if not failures else None,
    }


def build_frozen_smoke_inputs() -> tuple[dict[str, Any], list[dict[str, Any]], lineage.FunctionLineageDataset]:
    raw, source_rows, source_manifest = _assert_source_files()
    by_label = {SOURCE_LABELS[str(value["label"])]: value for value in source_rows}
    if set(by_label) != set(TASKS):
        raise RuntimeError(f"critical label set drift: {sorted(by_label)}")
    contexts = []
    selected_tasks = {}
    for label, (task_id, scope_id) in TASKS.items():
        row = by_label[label]
        if (row["task_id"], row["scope_id"]) != (task_id, scope_id):
            raise RuntimeError(f"critical task/scope drift: {label}")
        source_context = dict(row["context"])
        context = _smoke_context(source_context)
        inventory = _candidate_inventory(context)
        if any(value["scope_relation"] != "EXACT_SCOPE" for value in inventory):
            raise RuntimeError(f"non-EXACT_SCOPE candidate in {label}")
        contexts.append(context)
        selected_tasks[label] = {
            "task_id": task_id,
            "scope_id": scope_id,
            "scope_kind": context["function_scope_core"]["scope_kind"],
            "source_context_sha256": row["context_sha256"],
            "smoke_context_sha256": _sha_json(context),
            "local_evidence_sha256": _sha_json(context["local_evidence"]),
            "candidate_inventory": inventory,
        }
    forensic = _read_json(SCOPE_ROOT / "ios21_scope_forensics.json")
    references = {
        "LEFT17_R27": "lcand_cd6c87ed7f043a937b27",
        "LEFT18_R24": "lcand_d9f1abdb7469869363ad",
        "LEFT19_R30": forensic["LEFT19"]["r30_candidate_id"],
        "LEFT19_R25": forensic["LEFT19"]["r25_candidate_id"],
        "LEFT20_R26": forensic["LEFT20"]["singletons"]["R26"]["candidate_id"],
        "LEFT20_R28": forensic["LEFT20"]["singletons"]["R28"]["candidate_id"],
        "LEFT20_R29": forensic["LEFT20"]["singletons"]["R29"]["candidate_id"],
        "LEFT20_DISTRIBUTED": forensic["LEFT20"]["distributed_candidate"]["candidate_id"],
    }
    expected_membership = {
        "LEFT17": references["LEFT17_R27"],
        "LEFT18": references["LEFT18_R24"],
        "LEFT20 DOMESTIC": references["LEFT20_R26"],
        "LEFT20 FIRE": references["LEFT20_R28"],
        "LEFT20 METERING": references["LEFT20_R29"],
        "LEFT20 PARENT": references["LEFT20_DISTRIBUTED"],
    }
    ids_by_label = {
        label: {value["candidate_id"] for value in row["candidate_inventory"]}
        for label, row in selected_tasks.items()
    }
    for label, candidate_id in expected_membership.items():
        if candidate_id not in ids_by_label[label]:
            raise RuntimeError(f"research control candidate missing: {label}/{candidate_id}")
    if not {references["LEFT19_R30"], references["LEFT19_R25"]}.issubset(ids_by_label["LEFT19"]):
        raise RuntimeError("LEFT19 R30/R25 ambiguity control drift")
    child_ids = {references[key] for key in ("LEFT20_R26", "LEFT20_R28", "LEFT20_R29")}
    if child_ids.intersection(ids_by_label["LEFT20 PARENT"]):
        raise RuntimeError("LEFT20 singleton leaked into parent selectable candidates")
    shards = _build_shards(str(raw["input_signature"]), contexts)
    frozen = {
        "candidate_source": {_display_path(CANDIDATE_INPUT): EXPECTED_CANDIDATE_SOURCE_SHA256},
        "function_passports_sha256": EXPECTED_FUNCTION_PASSPORTS_SHA256,
        "scope_graph": {_display_path(SCOPE_ROOT / name): digest for name, digest in SCOPE_FILES.items()},
        "candidate_scope_membership_sha256": SCOPE_FILES["candidate_scope_membership.json"],
        "evidence_catalog_sha256": EXPECTED_EVIDENCE_CATALOG_SHA256,
        "scoped_selector_tasks_sha256": SCOPE_FILES["selector_tasks_scoped.json"],
        "scoped_shards_sha256": SOURCE_FILES["scoped_selector_shards.jsonl"],
        "seven_selected_critical_contexts_file_sha256": SOURCE_FILES["ios21_critical_scoped_contexts.json"],
        "seven_selected_critical_contexts_sha256": EXPECTED_CONTEXTS_SHA256,
        "scoped_transport_manifest_sha256": SOURCE_FILES["scoped_selector_manifest.json"],
        "source_prompt_template_sha256": source_manifest["prompt_template_sha256"],
        "smoke_prompt_template_sha256": _sha_json(PROMPT_LINES),
        "selected_tasks": selected_tasks,
        "candidate_inventories_sha256": _sha_json({label: row["candidate_inventory"] for label, row in selected_tasks.items()}),
        "references_for_post_inference_comparison_only": references,
        "smoke_contexts_sha256": _sha_json(contexts),
        "smoke_shard_count_per_pass": len(shards),
        "smoke_shard_characters": [value["prompt_characters"] for value in shards],
    }
    return frozen, shards, _synthetic_dataset(raw, contexts)


def prepare(output: Path) -> dict[str, Any]:
    if output.exists():
        raise RuntimeError(f"refusing to replace immutable experiment record: {output}")
    flags = base_smoke._assert_isolated_flags()
    runtime = ai_gateway.validate_runtime(require_vision=False, deep=False, mode=ai_settings.MODE_OFF)
    if not runtime.get("ok"):
        raise RuntimeError(f"isolated model runtime preflight failed: {runtime['problems']}")
    frozen, shards, dataset = build_frozen_smoke_inputs()
    preflight = _preflight(dataset, shards)
    if not preflight["ok"]:
        raise RuntimeError(f"smoke preflight failed: {preflight['failures']}")
    output.mkdir(parents=True)
    inputs_path = output / "smoke_inputs.jsonl"
    _write_jsonl(inputs_path, shards)
    manifest = {
        "kind": "function_lineage_v2_4_2_ios21_critical_scoped_smoke_input",
        "schema_version": "function-lineage-scoped-critical-smoke.v2.4.2",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_chain": [SCOPE_COMMIT, SCOPED_TRANSPORT_COMMIT, PROVIDER_SAFE_COMMIT],
        "checkout_head_before_calls": base_smoke._git("rev-parse", "HEAD"),
        "production_head_at_start": PRODUCTION_HEAD,
        "production_release_at_start": PRODUCTION_RELEASE,
        "origin_main_observed_before_calls": base_smoke._git("rev-parse", "origin/main"),
        "frozen_inputs": frozen,
        "smoke_inputs_path": _display_path(inputs_path),
        "smoke_inputs_sha256": _sha_file(inputs_path),
        "model_configuration": MODEL_CONFIGURATION,
        "model_configuration_sha256": _sha_json(MODEL_CONFIGURATION),
        "dependency_sha256": _dependency_hashes(),
        "runtime_flags": flags,
        "runtime_preflight": runtime,
        "preflight": preflight,
        "request_attempts_at_freeze": 0,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    }
    _write_json(output / "input_manifest.json", manifest)
    return manifest


def _validate_prepared(output: Path) -> tuple[dict[str, Any], list[dict[str, Any]], lineage.FunctionLineageDataset]:
    manifest = _read_json(output / "input_manifest.json")
    inputs_path = output / "smoke_inputs.jsonl"
    if _sha_file(inputs_path) != manifest["smoke_inputs_sha256"]:
        raise RuntimeError("prepared smoke inputs changed")
    if _dependency_hashes() != manifest["dependency_sha256"]:
        raise RuntimeError("harness/parser/verifier changed after input freeze")
    if base_smoke._flags() != manifest["runtime_flags"] or any(base_smoke._flags().values()):
        raise RuntimeError("Function Lineage runtime flags changed after freeze")
    raw, _, _ = _assert_source_files()
    shards = _read_jsonl(inputs_path)
    contexts = [context for shard in shards for context in shard["model_payload"]["task_contexts"]]
    if _sha_json(contexts) != manifest["frozen_inputs"]["smoke_contexts_sha256"]:
        raise RuntimeError("prepared scoped contexts changed")
    if [task for shard in shards for task in shard["task_ids"]] != [value[0] for value in TASKS.values()]:
        raise RuntimeError("prepared critical task order changed")
    return manifest, shards, _synthetic_dataset(raw, contexts)


def _model_job(
    shard: Mapping[str, Any], *, cold_run: int, pass_name: str,
    manifest: Mapping[str, Any], experiment_id: str,
    dataset: lineage.FunctionLineageDataset,
) -> dict[str, Any]:
    payload = shard["model_payload"]
    prompt = _prompt(payload, pass_name)
    prompt_hash = base_smoke._sha_bytes(prompt.encode())
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
    verifier = {"applicable": False, "ok": None, "global_errors": ["NOT_REACHED"], "task_results": {}}
    if parsed["ok"]:
        verifier = _task_local_verifier(dataset, str(payload["payload_signature"]), response["results"])
    usage = dict(call.usage or {})
    return {
        "cold_run": cold_run,
        "pass_name": pass_name,
        "pair_id": PAIR_ID,
        "shard_id": shard["shard_id"],
        "task_ids": list(shard["task_ids"]),
        "scope_ids": list(shard["scope_ids"]),
        "prompt_sha256": prompt_hash,
        "prompt_characters": shard["prompt_characters"],
        "payload_signature": payload["payload_signature"],
        "output_schema_sha256": _sha_json(shard["output_schema"]),
        "model_configuration_sha256": manifest["model_configuration_sha256"],
        "session_isolation": {"new_cli_process": True, "ephemeral": True, "tools_disabled": True, "vision_used": False},
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
                ok=bool(call.ok), error=call.error, raw_excerpt=call.raw_excerpt,
                error_kind=call.error_kind,
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


def _apply_capacity(records: Sequence[dict[str, Any]], dataset: lineage.FunctionLineageDataset) -> list[str]:
    if any(not value["model_call"]["ok"] or not value["transport_verification"]["ok"] for value in records):
        for record in records:
            record["capacity_verification"] = {"applicable": False, "ok": None, "errors": [], "reason": "INCOMPLETE_OR_INVALID_BATCH"}
        return []
    decisions = {
        str(result["task_id"]): str(result["decision"])
        for record in records for result in record["response"]["results"]
    }
    parent_id = TASKS["LEFT20 PARENT"][0]
    child_ids = {TASKS[label][0] for label in ("LEFT20 DOMESTIC", "LEFT20 FIRE", "LEFT20 METERING")}
    scenarios = {
        "ATOMIC_CHILD_SCOPES": [
            {"task_id": task_id, "candidate_id": decision}
            for task_id, decision in decisions.items() if task_id != parent_id
        ],
        "COMPOSITE_PARENT_SCOPE": [
            {"task_id": task_id, "candidate_id": decision}
            for task_id, decision in decisions.items() if task_id not in child_ids
        ],
    }
    scenario_errors = {name: lineage.verify_capacity(values, dataset.candidates) for name, values in scenarios.items()}
    all_errors = sorted({error for values in scenario_errors.values() for error in values})
    affected_candidates = {
        candidate_id for candidate_id in decisions.values()
        if any(candidate_id in error for error in all_errors)
    }
    for record in records:
        task_results = {
            task_id: {
                "ok": decisions[task_id] not in affected_candidates,
                "candidate_id": decisions[task_id],
                "errors": [error for error in all_errors if decisions[task_id] in error],
            }
            for task_id in record["task_ids"]
        }
        record["capacity_verification"] = {
            "applicable": True,
            "ok": all(value["ok"] for value in task_results.values()),
            "task_results": task_results,
            "scenario_errors": scenario_errors,
            "errors": all_errors,
            "cross_granularity_scenarios_mixed": False,
        }
    return all_errors


def experiment(output: Path) -> list[dict[str, Any]]:
    records_path = output / "model_runs.jsonl"
    if records_path.exists():
        raise RuntimeError(f"refusing to repeat or append observations: {records_path}")
    manifest, shards, dataset = _validate_prepared(output)
    total = len(shards) * len(PASSES) * len(COLD_RUNS)
    experiment_id = "flv2.4.2-ios21-scoped-smoke-" + uuid.uuid4().hex
    records: list[dict[str, Any]] = []
    started = time.monotonic()
    stopped = False
    stop_reason = None
    for cold_run in COLD_RUNS:
        if stopped:
            break
        for pass_name in PASSES:
            batch: list[dict[str, Any]] = []
            with ThreadPoolExecutor(max_workers=int(manifest["model_configuration"]["workers"])) as pool:
                futures = [pool.submit(
                    _model_job, shard, cold_run=cold_run, pass_name=pass_name,
                    manifest=manifest, experiment_id=experiment_id, dataset=dataset,
                ) for shard in shards]
                try:
                    for future in as_completed(futures):
                        batch.append(future.result())
                except Exception:
                    ai_gateway.kill_live_processes(experiment_id)
                    raise
            batch.sort(key=lambda value: str(value["shard_id"]))
            capacity_errors = _apply_capacity(batch, dataset)
            records.extend(batch)
            records.sort(key=lambda value: (int(value["cold_run"]), str(value["pass_name"]), str(value["shard_id"])))
            _write_jsonl(records_path, records)
            print(
                f"{len(records)}/{total} cold={cold_run} pass={pass_name} "
                f"model_ok={sum(value['model_call']['ok'] for value in batch)}/{len(batch)} "
                f"capacity_errors={len(capacity_errors)}",
                flush=True,
            )
            if any(not value["model_call"]["ok"] or not value["transport_verification"]["ok"] for value in batch):
                stopped = True
                stop_reason = "TECHNICAL_PROVIDER_OR_RESPONSE_CONTRACT_FAILURE"
                break
    counters = base_smoke._request_counters(records)
    telemetry = {
        "experiment_id": experiment_id,
        "planned_requests": total,
        "request_records": len(records),
        **counters,
        "stopped_early": stopped,
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


def _observations(records: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    output = {task_id: [] for task_id, _ in TASKS.values()}
    for record in records:
        parser_tasks = record["transport_verification"].get("task_results") or {}
        verifier_tasks = record["existing_verifier"].get("task_results") or {}
        capacity_tasks = (record.get("capacity_verification") or {}).get("task_results") or {}
        for task_id in record["task_ids"]:
            output[task_id].append({
                "cold_run": record["cold_run"],
                "pass_name": record["pass_name"],
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


def _stage(values: Sequence[Mapping[str, Any]], key: str) -> str:
    reached = [value.get(key) for value in values]
    return "PASS" if len(reached) == 6 and all(value is True for value in reached) else "FAIL"


def _task_results(manifest: Mapping[str, Any], records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    observations = _observations(records)
    rows = []
    for label, (task_id, scope_id) in TASKS.items():
        values = observations[task_id]
        repeats = []
        for cold_run in COLD_RUNS:
            by_pass = {value["pass_name"]: value for value in values if value["cold_run"] == cold_run}
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
            repeats.append({
                "cold_run": cold_run,
                "pass_a": by_pass.get("A", {}).get("decision"),
                "pass_b": by_pass.get("B", {}).get("decision"),
                "status": status,
                "stable_decision": stable_decision,
            })
        stable = [value["stable_decision"] for value in repeats if value["status"] in {"STABLE_MATCH", "STABLE_UNRESOLVED"}]
        stable_across = len(stable) == 3 and len(set(stable)) == 1
        stable_decision = stable[0] if stable_across else None
        frozen = manifest["frozen_inputs"]["selected_tasks"][label]
        rows.append({
            "label": label,
            "task_id": task_id,
            "scope_id": scope_id,
            "scope_kind": frozen["scope_kind"],
            "eligible_candidate_count": len(frozen["candidate_inventory"]),
            "candidate_inventory": frozen["candidate_inventory"],
            "observations": values,
            "cold_repeats": repeats,
            "selection_distribution": dict(sorted(Counter(str(value.get("decision") or "<NO_SELECTION>") for value in values).items())),
            "stable_repeat_count": len(stable),
            "pass_disagreement_count": sum(value["status"] == "PASS_DISAGREEMENT" for value in repeats),
            "need_more_evidence_count": sum(value.get("decision") == lineage.NEED_MORE_EVIDENCE for value in values),
            "response_parser_result": _stage(values, "response_parser_ok"),
            "verifier_result": _stage(values, "verifier_ok"),
            "capacity_result": _stage(values, "capacity_ok"),
            "stable_across_cold_runs": stable_across,
            "stable_decision": stable_decision,
            "stable_unresolved": stable_decision == lineage.NEED_MORE_EVIDENCE,
            "auto_match": stable_decision if stable_decision not in {None, lineage.NEED_MORE_EVIDENCE} else None,
        })
    return rows


def _child_union(tasks: Sequence[Mapping[str, Any]], references: Mapping[str, str]) -> dict[str, Any]:
    by_label = {str(value["label"]): value for value in tasks}
    children = [by_label[label] for label in ("LEFT20 DOMESTIC", "LEFT20 FIRE", "LEFT20 METERING")]
    parent = by_label["LEFT20 PARENT"]
    distributed_id = str(references["LEFT20_DISTRIBUTED"])
    parent_candidate = next(value for value in parent["candidate_inventory"] if value["candidate_id"] == distributed_id)
    selected = []
    reason = ""
    for child in children:
        decision = child["stable_decision"]
        candidate = next((value for value in child["candidate_inventory"] if value["candidate_id"] == decision), None)
        if not child["stable_across_cold_runs"] or candidate is None:
            reason = "at least one child scope lacks one stable concrete decision"
            return {"derivability": "EXACT_CHILD_UNION", "result": "NO", "reason": reason, "child_candidate_ids": [], "parent_candidate_id": distributed_id}
        selected.append(candidate)
    fields = ("right_physical_pages", "right_function_ids", "right_fragment_ids", "capacity_keys")
    comparisons = {
        field: sorted({item for candidate in selected for item in candidate[field]}) == sorted(set(parent_candidate[field]))
        for field in fields
    }
    result = "YES" if all(comparisons.values()) else "NO"
    return {
        "derivability": "EXACT_CHILD_UNION",
        "result": result,
        "reason": "stable child union equals frozen distributed parent candidate" if result == "YES" else "stable child union differs from frozen distributed parent candidate",
        "child_candidate_ids": [value["candidate_id"] for value in selected],
        "parent_candidate_id": distributed_id,
        "field_comparisons": comparisons,
        "model_bypassed": False,
    }


def _cost(records: Sequence[Mapping[str, Any]], telemetry: Mapping[str, Any]) -> dict[str, Any]:
    usages = [dict(value["model_call"].get("usage") or {}) for value in records]
    telemetry_defect = any(
        record["model_call"]["ok"] and base_smoke._usage_total(usage) == 0
        for record, usage in zip(records, usages)
    )
    return {
        **base_smoke._request_counters(records),
        "input_tokens": sum(int(value.get("total_input_tokens") or value.get("input_tokens") or 0) for value in usages),
        "output_tokens": sum(int(value.get("output_tokens") or 0) for value in usages),
        "total_tokens": sum(base_smoke._usage_total(value) for value in usages),
        "model_runtime_ms": int(telemetry["model_runtime_ms"]),
        "wall_time_ms": int(telemetry["wall_time_ms"]),
        "telemetry_defect": telemetry_defect,
        "telemetry_assessment": "Successful inference returned usage={} / zero tokens; token telemetry is defective." if telemetry_defect else "Token telemetry was returned for every successful inference request.",
    }


def _safety(records: Sequence[Mapping[str, Any]], tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    errors = sorted({
        str(error) for record in records
        for error in (record.get("capacity_verification") or {}).get("errors") or []
    })
    unsupported = [
        {"task_id": task["task_id"], "candidate_id": task["auto_match"]}
        for task in tasks if task["auto_match"] is not None
        and (task["verifier_result"] != "PASS" or task["capacity_result"] != "PASS")
    ]
    return {
        "cross_granularity_selectable_competition": 0,
        "unsupported_accepted_matches": unsupported,
        "unsupported_accepted_match_count": len(unsupported),
        "RIGHT_MAP_CONFLICT": sum("RIGHT_MAP_CONFLICT" in value for value in errors),
        "FUNCTION_FRAGMENT_CONFLICT": sum(value.startswith("FUNCTION_FRAGMENT_CONFLICT:") for value in errors),
        "capacity_defects": errors,
        "capacity_scenarios": ["ATOMIC_CHILD_SCOPES", "COMPOSITE_PARENT_SCOPE"],
        "parent_and_children_simultaneously_materialized": False,
    }


def _verdict(
    records: Sequence[Mapping[str, Any]], telemetry: Mapping[str, Any],
    tasks: Sequence[Mapping[str, Any]], safety: Mapping[str, Any],
) -> dict[str, Any]:
    expected = 4 * 2 * 3
    technical = (
        telemetry["stopped_early"] or len(records) != expected
        or any(not value["model_call"]["ok"] or not value["transport_verification"]["ok"] for value in records)
    )
    defect = (
        bool(safety["capacity_defects"]) or safety["unsupported_accepted_match_count"] != 0
        or any(value["verifier_result"] != "PASS" or value["capacity_result"] != "PASS" for value in tasks)
    )
    context_loss = any(value["stable_unresolved"] for value in tasks)
    stable = all(value["stable_across_cold_runs"] for value in tasks)
    if technical:
        code, reason = "E", "technical/provider failure; experiment NOT VALID"
    elif defect:
        code, reason = "D", "verifier, capacity, or scope-membership safety defect"
    elif context_loss:
        code, reason = "C", "scope compaction/scoping left a critical task stably unresolved"
    elif stable:
        code, reason = "A", "inference completed; seven scoped critical tasks are stable and safe"
    else:
        code, reason = "B", "inference completed, but the same-scope selector remains materially unstable"
    return {
        "verdict": code,
        "reason": reason,
        "experiment_valid": code != "E",
        "broader_stratified_corpus_ai_evaluation_allowed": code == "A",
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
    }


def _report(
    tasks: Sequence[Mapping[str, Any]], child_union: Mapping[str, Any],
    cost: Mapping[str, Any], safety: Mapping[str, Any], verdict: Mapping[str, Any],
) -> str:
    lines = [
        "# Function Lineage v2.4.2 — IOS2.1 isolated scoped AI smoke",
        "",
        f"Frozen scoped transport `{SCOPED_TRANSPORT_COMMIT}`; model `{MODEL_CONFIGURATION['model']}/{MODEL_CONFIGURATION['reasoning_effort']}`.",
        "Three independent cold repeats, Pass A/B, no majority override. `FUNCTION_REMOVED` was excluded by the frozen smoke contract.",
        "",
        "| Task | Eligible | Cold 1 A/B | Cold 2 A/B | Cold 3 A/B | Distribution | Stable | Disagree | NME | Parser | Verifier | Capacity |",
        "|---|---:|---|---|---|---|---:|---:|---:|---|---|---|",
    ]
    for task in tasks:
        pairs = [f"{value['pass_a']} / {value['pass_b']}" for value in task["cold_repeats"]]
        lines.append(
            f"| {task['label']} | {task['eligible_candidate_count']} | {pairs[0]} | {pairs[1]} | {pairs[2]} | "
            f"`{json.dumps(task['selection_distribution'], sort_keys=True)}` | {task['stable_repeat_count']}/3 | "
            f"{task['pass_disagreement_count']} | {task['need_more_evidence_count']} | {task['response_parser_result']} | "
            f"{task['verifier_result']} | {task['capacity_result']} |"
        )
    lines.extend(["", "## Eligible candidate inventories", ""])
    for task in tasks:
        lines.extend([
            f"### {task['label']}", "",
            "| Rank | Candidate | Scope relation | Relation | RIGHT pages | RIGHT functions | RIGHT fragments | Evidence |",
            "|---:|---|---|---|---|---|---|---|",
        ])
        for candidate in task["candidate_inventory"]:
            lines.append(
                f"| {candidate['rank']} | `{candidate['candidate_id']}` | {candidate['scope_relation']} | "
                f"{candidate['relation_type']} | `{candidate['right_physical_pages']}` | `{candidate['right_function_ids']}` | "
                f"`{candidate['right_fragment_ids']}` | {len(candidate['evidence_refs'])} refs; `{candidate['evidence_sha256']}` |"
            )
        lines.append("")
    left19 = next(value for value in tasks if value["label"] == "LEFT19")
    lines.extend([
        "## Controls and safety", "",
        f"LEFT19 independent distribution: `{json.dumps(left19['selection_distribution'], sort_keys=True)}`. A repeated R30 preference, if present, is only a stable model preference and does not prove R25 deterministically wrong.",
        "",
        f"LEFT20 child union == parent distributed candidate: **{child_union['result']}** — {child_union['reason']}. Model bypass: `NO`.",
        "",
        f"Cross-granularity selectable competition `{safety['cross_granularity_selectable_competition']}`; unsupported accepted `{safety['unsupported_accepted_match_count']}`; RIGHT_MAP_CONFLICT `{safety['RIGHT_MAP_CONFLICT']}`; FUNCTION_FRAGMENT_CONFLICT `{safety['FUNCTION_FRAGMENT_CONFLICT']}`.",
        "",
        "Capacity was checked in separate atomic-child and composite-parent scenarios; nested parent and child results were never treated as simultaneous materialization.",
        "",
        "## Cost", "",
        f"Request attempts `{cost['request_attempts']}`; successful inference requests `{cost['successful_inference_requests']}`; input/output/total tokens `{cost['input_tokens']}/{cost['output_tokens']}/{cost['total_tokens']}`.",
        "",
        f"Model runtime `{cost['model_runtime_ms']} ms`; wall time `{cost['wall_time_ms']} ms`; telemetry defect `{cost['telemetry_defect']}` — {cost['telemetry_assessment']}",
        "",
        "Production runs `0`; deploy `NO`; shadow `OFF`; materialization `NO`; Vision `NO`.",
        "",
        "## Verdict", "",
        f"**{verdict['verdict']} — {verdict['reason']}.**", "",
        "Even with verdict A: **NO DEPLOY. NO SHADOW. NO MATERIALIZATION.**", "",
    ])
    return "\n".join(lines)


def finalize(output: Path) -> dict[str, Any]:
    manifest = _read_json(output / "input_manifest.json")
    if _dependency_hashes() != manifest["dependency_sha256"]:
        raise RuntimeError("harness/parser/verifier changed after inference")
    if _sha_file(output / "smoke_inputs.jsonl") != manifest["smoke_inputs_sha256"]:
        raise RuntimeError("smoke inputs changed after inference")
    records = _read_jsonl(output / "model_runs.jsonl")
    telemetry = _read_json(output / "run_telemetry.json")
    tasks = _task_results(manifest, records)
    references = manifest["frozen_inputs"]["references_for_post_inference_comparison_only"]
    child_union = _child_union(tasks, references)
    cost = _cost(records, telemetry)
    safety = _safety(records, tasks)
    verdict = _verdict(records, telemetry, tasks, safety)
    metrics = {
        "kind": "function_lineage_v2_4_2_ios21_critical_scoped_smoke_metrics",
        "task_count": 7,
        "cold_repeat_count": 3,
        "passes_per_repeat": 2,
        "shards_per_pass": 4,
        "observation_count_per_task": {value["label"]: len(value["observations"]) for value in tasks},
        "stable_repeat_count": {value["label"]: value["stable_repeat_count"] for value in tasks},
        "left20_exact_child_union": child_union,
        "cost": cost,
        "safety": safety,
        "verdict": verdict,
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
        "vision": False,
    }
    _write_json(output / "task_results.json", {"tasks": tasks})
    _write_json(output / "metrics.json", metrics)
    _write_json(output / "verdict.json", verdict)
    (output / "report.md").write_text(_report(tasks, child_union, cost, safety, verdict), encoding="utf-8")
    names = (
        "input_manifest.json", "smoke_inputs.jsonl", "model_runs.jsonl",
        "run_telemetry.json", "task_results.json", "metrics.json",
        "verdict.json", "report.md",
    )
    _write_json(output / "artifact_hashes.json", {
        "files": {name: {"sha256": _sha_file(output / name), "bytes": (output / name).stat().st_size} for name in names},
        "request_attempts": cost["request_attempts"],
        "production_runs": 0,
        "deploy": False,
        "shadow_enabled": False,
        "materialization": False,
    })
    return metrics


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "experiment", "finalize", "all"))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    output = args.output.resolve()
    if args.phase in {"prepare", "all"}:
        prepare(output)
        print(f"frozen scoped smoke inputs: {output}", flush=True)
    if args.phase in {"experiment", "all"}:
        experiment(output)
    if args.phase in {"finalize", "all"}:
        print(json.dumps(finalize(output), ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
