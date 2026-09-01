"""Bounded selector execution, cache isolation and fail-closed unanimity."""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ..ai import cache as cache_module
from ..ai import gateway
from ..ai import settings as gateway_settings
from ..production_artifacts import content_signature
from . import prompts, schemas, settings, verifier
from .candidate_factory import build_candidate_factory

SINGLE = "single"
UNANIMITY = "unanimity"
MODES = (SINGLE, UNANIMITY)
BATCH_TASK_BYTES_LIMIT = 480_000
MAX_PROMPT_BYTES = 800_000


class BoundedSelectorAnalyst:
    def __init__(
        self,
        *,
        artifacts: Mapping[str, Mapping[str, Any]],
        pair_id: str,
        mode: str = UNANIMITY,
        fast_input_signature: str | None = None,
        cache_dir: Path | str | None = None,
        cache_enabled: bool = True,
        prompt_capture_dir: Path | str | None = None,
        call: Callable[..., gateway.CallResult] | None = None,
        run_id: str = "",
        require_feature: bool = True,
        cache_context: Mapping[str, Any] | None = None,
        model_retries: int = 1,
    ) -> None:
        if require_feature:
            settings.require_enabled()
        if mode not in MODES:
            raise ValueError(f"unknown selector mode: {mode}")
        if model_retries < 0:
            raise ValueError("model_retries must be non-negative")
        self.artifacts = {key: dict(value) for key, value in artifacts.items()}
        self.pair_id = pair_id
        self.mode = mode
        self.artifact_snapshot_signature = content_signature(self.artifacts)
        self.fast_input_signature = fast_input_signature or content_signature(self.artifacts)
        self.factory, self.bundles, self.catalog = build_candidate_factory(
            artifacts=self.artifacts,
            pair_id=pair_id,
            fast_input_signature=self.fast_input_signature,
        )
        self._call = call or gateway.call
        self.run_id = run_id or uuid.uuid4().hex
        self.cache_context = dict(cache_context or {})
        self.cache_context_signature = content_signature(self.cache_context)
        self.model_retries = model_retries
        self.cache = cache_module.ResponseCache(cache_dir if cache_enabled else None)
        self.cache_enabled = cache_enabled
        self.prompt_capture_dir = Path(prompt_capture_dir) if prompt_capture_dir else None
        self.model_calls = 0
        self.duration_ms = 0
        self.call_metrics: list[dict[str, Any]] = []
        self.prompt_manifest: list[dict[str, Any]] = []
        self.shared_context = self._shared_context()
        self.shared_context_signature = content_signature(self.shared_context)

    def _shared_context(self) -> dict[str, Any]:
        direct = self.artifacts.get("direct_page_mode2") or {}
        result = direct.get("comparison_result") or {}
        sides = {}
        for side, graph_key in (("LEFT", "left_graph"), ("RIGHT", "right_graph")):
            graph = direct.get(graph_key) or {}
            source = (direct.get("sources") or {}).get(side) or {}
            sections = [
                {
                    "id": value.get("id"),
                    "label": value.get("label"),
                    "identity": value.get("canonical_identity"),
                }
                for value in graph.get("nodes") or ()
                if isinstance(value, Mapping) and value.get("type") == "BUS_SECTION"
            ]
            sides[side] = {
                "document": (source.get("document") or {}).get("document_code"),
                "page": int(source.get("page_index_0based") or 0) + 1,
                "discipline": graph.get("discipline"),
                "sections": sections,
                "node_count": len(graph.get("nodes") or ()),
            }
        fast = [
            {
                "change_id": value.get("change_id"),
                "facet": value.get("facet_ref"),
                "outcome": value.get("outcome"),
            }
            for value in (self.artifacts.get("unified_synthesis") or {}).get("changes") or ()
            if isinstance(value, Mapping)
        ]
        inconsistencies = [
            {
                "id": value.get("inconsistency_id"),
                "kind": value.get("kind"),
                "subject": value.get("subject"),
                "summary": value.get("summary"),
            }
            for value in (self.artifacts.get("document_inconsistencies") or {}).get("items") or ()
            if isinstance(value, Mapping)
        ]
        return {
            "pair_id": self.pair_id,
            "sides": sides,
            "functional_areas": result.get("functional_groups") or {},
            "proven_fast_findings": fast,
            "document_inconsistencies": inconsistencies,
            "recognition": (result.get("comparison_quality") or {}),
        }

    @staticmethod
    def _needs_model(task: Mapping[str, Any]) -> bool:
        if task.get("deterministic_winner_candidate_id"):
            return False
        selectable = [
            value for value in task.get("candidates") or ()
            if value.get("candidate_id") in set(task.get("selectable_candidate_ids") or ())
        ]
        semantic = [
            value for value in selectable
            if value.get("eligibility") == schemas.AUTO
            and value.get("resolution_effect") != "HUMAN_REQUIRED"
        ]
        return bool(semantic) and len(selectable) >= 2

    def _model_task(self, task: Mapping[str, Any]) -> dict[str, Any]:
        selectable = set(task.get("selectable_candidate_ids") or ())
        options = []
        for candidate in task.get("candidates") or ():
            if candidate.get("candidate_id") not in selectable:
                continue
            refs = sorted({
                str(ref)
                for key in (
                    "left_refs", "right_refs", "entity_refs", "graph_refs",
                    "table_refs", "text_refs",
                )
                for ref in candidate.get(key) or () if str(ref)
            })
            options.append({
                "candidate_id": candidate.get("candidate_id"),
                "candidate_type": candidate.get("candidate_type"),
                "summary": candidate.get("summary"),
                "evidence_bundle": {
                    key: candidate.get(key)
                    for key in (
                        "left_refs", "right_refs", "values", "units",
                        "entity_refs", "graph_refs", "table_refs", "text_refs",
                        "deterministic_features", "proof_requirements",
                        "candidate_signature",
                    )
                },
                "prebound_evidence": [self.catalog[ref] for ref in refs if ref in self.catalog],
            })
        return {
            "task_id": task.get("task_id"),
            "task_type": task.get("task_type"),
            "question": task.get("question"),
            "subject": task.get("subject"),
            "options": options,
        }

    def batches(self) -> list[dict[str, Any]]:
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for task in self.factory.get("tasks") or ():
            if self._needs_model(task):
                grouped.setdefault(str(task.get("selector_group") or "other"), []).append(task)
        output = []
        for group in sorted(grouped):
            tasks = sorted(grouped[group], key=lambda value: str(value.get("task_id") or ""))
            chunks: list[list[tuple[Mapping[str, Any], dict[str, Any]]]] = []
            current: list[tuple[Mapping[str, Any], dict[str, Any]]] = []
            current_bytes = 0
            for task in tasks:
                view = self._model_task(task)
                size = len(json.dumps(
                    view, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8"))
                if current and current_bytes + size > BATCH_TASK_BYTES_LIMIT:
                    chunks.append(current)
                    current, current_bytes = [], 0
                current.append((task, view))
                current_bytes += size
            if current:
                chunks.append(current)
            for ordinal, chunk in enumerate(chunks, 1):
                chunk_tasks = [value[0] for value in chunk]
                views = [value[1] for value in chunk]
                batch_group = f"{group}:{ordinal}/{len(chunks)}"
                core = {
                    "batch_id": stable_batch_id(
                        batch_group,
                        [str(task.get("task_id")) for task in chunk_tasks],
                    ),
                    "selector_group": group,
                    "batch_partition": batch_group,
                    "task_ids": [str(task.get("task_id")) for task in chunk_tasks],
                    "tasks": views,
                }
                core["task_batch_signature"] = content_signature(core)
                output.append(core)
        return output

    def _capture(
        self, *, batch: Mapping[str, Any], pass_identity: str,
        prompt: str, schema: Mapping[str, Any], cache_key: str,
    ) -> dict[str, Any]:
        payload = {
            "system_prompt": prompts.SYSTEM_PROMPT,
            "prompt": prompt,
            "response_schema": dict(schema),
        }
        record = {
            "batch_id": batch["batch_id"],
            "selector_group": batch["selector_group"],
            "pass_identity": pass_identity,
            "task_ids": list(batch["task_ids"]),
            "task_batch_signature": batch["task_batch_signature"],
            "frozen_fast_signature": self.fast_input_signature,
            "candidate_factory_version": schemas.FACTORY_VERSION,
            "candidate_set_signature": self.factory["candidate_set_signature"],
            "shared_context_signature": self.shared_context_signature,
            "prompt_version": schemas.PROMPT_VERSION,
            "response_schema_version": schemas.SELECTOR_SCHEMA_VERSION,
            "model": settings.MODEL,
            "reasoning": settings.REASONING_EFFORT,
            "prompt_signature": content_signature(payload),
            "schema_signature": content_signature(schema),
            "cache_context": self.cache_context,
            "cache_context_signature": self.cache_context_signature,
            "cache_key": cache_key,
            "prompt_bytes": len(prompt.encode("utf-8")) + len(prompts.SYSTEM_PROMPT.encode("utf-8")),
        }
        if self.prompt_capture_dir is not None:
            self.prompt_capture_dir.mkdir(parents=True, exist_ok=True)
            path = self.prompt_capture_dir / f"{batch['batch_id']}_{pass_identity}.json"
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
            record["payload_file"] = path.name
        self.prompt_manifest.append(record)
        return record

    def _call_batch(
        self, batch: Mapping[str, Any], pass_identity: str,
    ) -> dict[str, Any]:
        candidate_ids = [
            str(option["candidate_id"])
            for task in batch["tasks"] for option in task["options"]
        ]
        schema = schemas.selector_schema(candidate_ids)
        prompt = prompts.selector_prompt(
            shared_context=self.shared_context, tasks=batch["tasks"]
        )
        prompt_digest = cache_module.digest_prompt(prompt, prompts.SYSTEM_PROMPT)
        schema_digest = cache_module.digest_schema(schema)
        evidence_digest = content_signature({
            "frozen_fast_signature": self.fast_input_signature,
            "factory_version": schemas.FACTORY_VERSION,
            "candidate_set_signature": self.factory["candidate_set_signature"],
            "shared_context_signature": self.shared_context_signature,
            "task_batch_signature": batch["task_batch_signature"],
            "prompt_version": schemas.PROMPT_VERSION,
            "response_schema": schemas.SELECTOR_SCHEMA_VERSION,
            "model": settings.MODEL,
            "reasoning": settings.REASONING_EFFORT,
            "selector_pass_identity": pass_identity,
            "cache_context": self.cache_context,
        })
        role = f"ai_v3_selector:{batch['selector_group']}:{pass_identity}"
        key = cache_module.cache_key(
            evidence_digest=evidence_digest,
            model=settings.MODEL,
            reasoning_level=settings.REASONING_EFFORT,
            prompt_version=schemas.PROMPT_VERSION,
            schema_version=schemas.SELECTOR_SCHEMA_VERSION,
            role=role,
            prompt_digest=prompt_digest,
            schema_digest=schema_digest,
        )
        audit = self._capture(
            batch=batch, pass_identity=pass_identity, prompt=prompt,
            schema=schema, cache_key=key,
        )
        if audit["prompt_bytes"] > MAX_PROMPT_BYTES:
            return {
                **audit,
                "ok": False,
                "cache_hit": False,
                "payload": {},
                "duration_ms": 0,
                "error_kind": "PROMPT_TOO_LARGE",
                "error": (
                    f"selector prompt is {audit['prompt_bytes']} bytes; "
                    f"limit is {MAX_PROMPT_BYTES}"
                ),
            }
        cached = self.cache.load(key)
        if cached is not None:
            return {**audit, "ok": True, "cache_hit": True, "payload": cached, "duration_ms": 0}
        started = time.perf_counter()
        self.model_calls += 1
        result = self._call(
            gateway_settings.CODEX_SESSION,
            prompt,
            model=settings.MODEL,
            schema=schema,
            reasoning_level=settings.REASONING_EFFORT,
            timeout_s=settings.timeout_seconds(),
            retries=self.model_retries,
            run_id=self.run_id,
            system_prompt=prompts.SYSTEM_PROMPT,
        )
        duration = int((time.perf_counter() - started) * 1000)
        if result.ok and isinstance(result.parsed, Mapping):
            self.cache.store(key, result.parsed, audit)
            return {**audit, "ok": True, "cache_hit": False, "payload": dict(result.parsed), "duration_ms": duration}
        return {
            **audit,
            "ok": False,
            "cache_hit": False,
            "payload": {},
            "duration_ms": duration,
            "error_kind": result.error_kind,
            "error": result.error,
        }

    def run(self) -> dict[str, Any]:
        started = time.perf_counter()
        task_index = {str(task["task_id"]): task for task in self.factory["tasks"]}
        batch_rows = self.batches()
        pass_outputs: dict[str, list[dict[str, Any]]] = {"pass_1": []}
        if self.mode == UNANIMITY:
            pass_outputs["pass_2"] = []
        selections_by_pass: dict[str, dict[str, dict[str, Any]]] = {
            key: {} for key in pass_outputs
        }
        response_errors: dict[str, dict[str, list[str]]] = {key: {} for key in pass_outputs}
        for pass_identity in pass_outputs:
            for batch in batch_rows:
                row = self._call_batch(batch, pass_identity)
                pass_outputs[pass_identity].append(row)
                self.call_metrics.append({
                    key: row.get(key) for key in (
                        "batch_id", "selector_group", "pass_identity", "ok",
                        "cache_hit", "duration_ms", "prompt_bytes", "error_kind",
                    )
                })
                tasks = [task_index[task_id] for task_id in batch["task_ids"]]
                if not row["ok"]:
                    response_errors[pass_identity][batch["batch_id"]] = [
                        str(row.get("error_kind") or "MODEL_FAILED")
                    ]
                    continue
                parsed, errors = verifier.verify_batch_response(tasks, row["payload"])
                response_errors[pass_identity][batch["batch_id"]] = errors
                if not errors:
                    selections_by_pass[pass_identity].update(parsed)

        stable: list[dict[str, Any]] = []
        checks: list[dict[str, Any]] = []
        current_signature = content_signature(self.artifacts)
        model_task_ids = {
            task_id for batch in batch_rows for task_id in batch["task_ids"]
        }
        for task_id, task in sorted(task_index.items()):
            deterministic = str(task.get("deterministic_winner_candidate_id") or "")
            if deterministic:
                selection = {
                    "task_id": task_id,
                    "selected_candidate_id": deterministic,
                    "confidence_bucket": schemas.HIGH,
                    "optional_short_reason": "deterministic single winner",
                }
                source = "DETERMINISTIC"
                unanimous = True
                pass_values = []
            elif task_id not in model_task_ids:
                stable.append({
                    "task_id": task_id,
                    "task_type": task.get("task_type"),
                    "status": schemas.HUMAN_REQUIRED,
                    "source": "PREFILTER",
                    "selected_candidate_id": None,
                    "unanimous": None,
                    "reason_code": "NO_AUTO_RESOLVABLE_CANDIDATE",
                })
                continue
            else:
                first = selections_by_pass["pass_1"].get(task_id)
                second = selections_by_pass.get("pass_2", {}).get(task_id)
                pass_values = [
                    value.get("selected_candidate_id") if value else None
                    for value in ([first, second] if self.mode == UNANIMITY else [first])
                ]
                unanimous = bool(first) and (
                    self.mode == SINGLE
                    or bool(second) and first.get("selected_candidate_id") == second.get("selected_candidate_id")
                )
                if not unanimous:
                    stable.append({
                        "task_id": task_id,
                        "task_type": task.get("task_type"),
                        "status": schemas.HUMAN_REQUIRED,
                        "source": "AI_SELECTOR",
                        "selected_candidate_id": None,
                        "pass_candidate_ids": pass_values,
                        "unanimous": False,
                        "reason_code": "SELECTOR_DISAGREEMENT_OR_FAILURE",
                    })
                    continue
                selection = dict(first or {})
                source = "AI_SELECTOR"

            check = verifier.verify_selection(
                task=task,
                selection=selection,
                catalog=self.catalog,
                artifacts=self.artifacts,
                frozen_fast_signature=self.artifact_snapshot_signature,
                current_fast_signature=current_signature,
            )
            checks.append(check)
            stable.append({
                "task_id": task_id,
                "task_type": task.get("task_type"),
                "status": check["status"],
                "source": source,
                "selected_candidate_id": selection.get("selected_candidate_id"),
                "candidate_signature": check.get("candidate_signature"),
                "confidence_bucket": selection.get("confidence_bucket"),
                "optional_short_reason": selection.get("optional_short_reason"),
                "pass_candidate_ids": pass_values,
                "unanimous": unanimous,
                "reason_code": (check.get("errors") or [None])[0],
            })
        stable.sort(key=lambda value: value["task_id"])
        self.duration_ms = int((time.perf_counter() - started) * 1000)
        run = {
            "kind": "stage_comparison_ai_v3_run",
            "schema_version": schemas.RUN_SCHEMA_VERSION,
            "pair_id": self.pair_id,
            "experimental": True,
            "feature_flag": settings.FEATURE_FLAG,
            "model": settings.MODEL,
            "reasoning_effort": settings.REASONING_EFFORT,
            "stability_mode": self.mode,
            "fast_input_signature": self.fast_input_signature,
            "candidate_set_signature": self.factory["candidate_set_signature"],
            "shared_context_signature": self.shared_context_signature,
            "selector_batches": batch_rows,
            "selector_passes": pass_outputs,
            "response_errors": response_errors,
            "stable_selections": stable,
            "verifier": checks,
            "prompt_manifest": self.prompt_manifest,
            "diagnostics": {
                "factory_tasks": len(self.factory["tasks"]),
                "model_tasks": len(model_task_ids),
                "batches_per_pass": len(batch_rows),
                "model_calls": self.model_calls,
                "duration_ms": self.duration_ms,
                "verified_selections": sum(value["status"] == schemas.VERIFIED_SELECTION for value in stable),
                "human_required": sum(value["status"] == schemas.HUMAN_REQUIRED for value in stable),
                "rejected_selections": sum(value["status"] in {schemas.REJECTED_SELECTION, schemas.INVALID_RESPONSE} for value in stable),
                "selector_disagreements": sum(value.get("unanimous") is False for value in stable),
                "deterministic_winners": sum(value.get("source") == "DETERMINISTIC" for value in stable),
                "unsupported_published": 0,
                "cache": self.cache.statistics(),
                "call_metrics": self.call_metrics,
            },
            "constraints": {
                "model_output_contains_evidence": False,
                "model_output_contains_values": False,
                "human_priority": True,
                "mode_mapping_materialized": False,
                "fast_unchanged": True,
            },
        }
        run["input_signature"] = content_signature({
            "schema": schemas.RUN_SCHEMA_VERSION,
            "fast": self.fast_input_signature,
            "candidates": self.factory["candidate_set_signature"],
            "context": self.shared_context_signature,
            "prompts": self.prompt_manifest,
            "mode": self.mode,
        })
        return run


def stable_batch_id(group: str, task_ids: Sequence[str]) -> str:
    return "aiv3batch_" + content_signature([group, list(task_ids)])[:16]


__all__ = [
    "BATCH_TASK_BYTES_LIMIT",
    "BoundedSelectorAnalyst",
    "MAX_PROMPT_BYTES",
    "MODES",
    "SINGLE",
    "UNANIMITY",
]
