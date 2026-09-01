"""Experimental whole-document analyst execution and materialization."""
from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from backend.app.pipeline.stages.block_grounding.electrical_table_diff import (
    compare_match,
)

from ..ai import cache as cache_module
from ..ai import gateway
from ..ai import identity as table_identity
from ..ai import response_contract
from ..ai import settings as legacy_settings
from ..production_artifacts import content_signature, utc_now
from . import context, expansion, inventory as inventory_module
from . import prompts, schemas, settings, verifier

KIND = "stage_comparison_ai_analyst_v2"
SCHEMA_VERSION = "stage-comparison-ai-analyst-v2-run.v1"

MODEL_FAILED = "MODEL_FAILED"
MODEL_TIMEOUT = "MODEL_TIMEOUT"
VERIFIER_REJECTED = "VERIFIER_REJECTED"
INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
BUDGET_EXHAUSTED = "SESSION_BUDGET_EXHAUSTED"


def _human(
    task_id: str, task_type: str, reason: str, detail: str = "",
    *, resolution: Mapping[str, Any] | None = None,
    check: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "task_type": task_type,
        "status": "HUMAN_REQUIRED",
        "reason_code": reason,
        "reason_detail": detail[:1000],
        "resolution": dict(resolution) if resolution else None,
        "verifier": dict(check) if check else None,
        "published": False,
        "saves_human_decision": False,
    }


def _resolved(
    task_id: str, task_type: str, resolution: Mapping[str, Any],
    check: Mapping[str, Any], *, source_kind: str = "",
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "task_type": task_type,
        "source_kind": source_kind,
        "status": "AI_RESOLVED_VERIFIED",
        "reason_code": None,
        "reason_detail": "",
        "resolution": dict(resolution),
        "verifier": dict(check),
        "published": True,
        "saves_human_decision": False,
    }


class WholeDocumentAnalyst:
    """One immutable evidence set, at most four isolated Codex sessions."""

    def __init__(
        self,
        *,
        artifacts: Mapping[str, Mapping[str, Any]],
        pair_id: str,
        effort: str,
        cache_dir: Path | str | None = None,
        call: Callable[..., gateway.CallResult] | None = None,
        cancel: gateway.CancelToken | None = None,
        run_id: str = "",
    ) -> None:
        settings.require_enabled()
        self.artifacts = {key: dict(value) for key, value in artifacts.items()}
        self.pair_id = str(pair_id)
        self.effort = settings.effort(effort)
        self.cancel = cancel or gateway.CancelToken()
        self.run_id = run_id or uuid.uuid4().hex
        self._call = call or gateway.call
        self.cache = cache_module.ResponseCache(cache_dir)
        self.sessions = 0
        self.model_calls = 0
        self.model_failures = 0
        self.model_timeouts = 0
        self.expansion_budget = expansion.ExpansionBudget(settings.max_expansions())

        self.inventory = inventory_module.build_inventory(
            legacy_inventory=self.artifacts.get("ai_routing_inventory") or {},
            direct_page=self.artifacts.get("direct_page_mode2") or {},
            pair_id=self.pair_id,
        )
        self.bundle = context.build_context_bundle(
            artifacts=self.artifacts, inventory=self.inventory,
            pair_id=self.pair_id,
        )

    def _cached_call(
        self, *, prompt: str, schema: Mapping[str, Any], role: str,
        evidence_digest: str,
    ) -> tuple[dict[str, Any] | None, gateway.CallResult | None, bool]:
        key = cache_module.cache_key(
            evidence_digest=evidence_digest,
            model=settings.MODEL,
            reasoning_level=self.effort,
            prompt_version=schemas.PROMPT_VERSION,
            schema_version=schemas.SCHEMA_VERSION,
            role=role,
            prompt_digest=cache_module.digest_prompt(prompt, prompts.SYSTEM_PROMPT),
            schema_digest=cache_module.digest_schema(schema),
        )
        cached = self.cache.load(key)
        if cached is not None:
            return cached, None, True
        if self.sessions >= settings.max_sessions():
            return None, gateway.CallResult(
                legacy_settings.CODEX_SESSION, settings.MODEL, self.effort, False,
                error="исчерпан предел аналитических сессий",
                error_kind=BUDGET_EXHAUSTED,
            ), False
        self.sessions += 1
        self.model_calls += 1
        result = self._call(
            legacy_settings.CODEX_SESSION,
            prompt,
            model=settings.MODEL,
            schema=dict(schema),
            reasoning_level=self.effort,
            timeout_s=settings.timeout_seconds(),
            retries=1,
            cancel=self.cancel,
            run_id=self.run_id,
            system_prompt=prompts.SYSTEM_PROMPT,
        )
        if not result.ok:
            if result.error_kind == "TIMEOUT":
                self.model_timeouts += 1
            else:
                self.model_failures += 1
            return None, result, False
        payload = result.parsed or {}
        self.cache.store(key, payload, {
            "role": role,
            "model": settings.MODEL,
            "reasoning_level": self.effort,
            "context_signature": self.bundle.signature,
        })
        return payload, result, False

    def _table_packages(self) -> list[table_identity.IdentityPackage]:
        direct = self.artifacts.get("direct_page_mode2") or {}
        load_tables = ((direct.get("diagnostics") or {}).get(
            "electrical_load_tables"
        ) or {})
        questions = table_identity.build_questions(
            inventory=self.artifacts.get("ai_routing_inventory") or {},
            load_tables=load_tables,
        )
        # Legacy item IDs are not globally unique: a consistency review and
        # a table-row ambiguity may point at the same row and therefore share
        # one item_id.  The v2 inventory mints a unique task_id for the later
        # occurrence.  Translate legacy identity questions to that canonical
        # v2 route before batching, and drop anything the v2 eligibility
        # inventory did not explicitly route.
        routes = {
            (
                str(task.get("source_item_id") or task.get("task_id") or ""),
                str(task.get("source_kind") or ""),
            ): str(task.get("task_id") or "")
            for task in inventory_module.eligible_items(self.inventory)
            if task.get("task_type") == schemas.TABLE_ROW_IDENTITY
        }
        routed_questions: list[table_identity.IdentityQuestion] = []
        for question in questions:
            task_id = routes.get((question.source_item_id, question.kind))
            if not task_id:
                continue
            question.source_item_id = task_id
            routed_questions.append(question)
        questions = routed_questions
        table_identity.attach_base_context(
            questions,
            load_tables=load_tables,
            contradictions=load_tables,
        )
        if not questions:
            return []
        # Two whole-sheet sessions leave one normal session and one controlled
        # expansion within the product ceiling of four.
        by_section: dict[str, list[table_identity.IdentityQuestion]] = {}
        for question in questions:
            by_section.setdefault(question.section or "—", []).append(question)
        # Keep a coherent largest section together; the remaining (usually
        # smaller section plus whole-sheet totals) share the other session.
        # No panel designation is special-cased here.
        primary_key = max(
            sorted(by_section), key=lambda key: len(by_section[key]), default=""
        )
        primary = list(by_section.get(primary_key) or ())
        other = [
            question for key, values in sorted(by_section.items())
            if key != primary_key for question in values
        ]
        return [
            table_identity.IdentityPackage(primary_key or "ОСНОВНАЯ ОБЛАСТЬ", primary),
            table_identity.IdentityPackage("ОСТАЛЬНЫЕ ОБЛАСТИ", other),
        ] if primary and other else [
            table_identity.IdentityPackage("ВЕСЬ ЛИСТ", questions)
        ]

    def _run_table_identity(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        results: list[dict[str, Any]] = []
        matches: list[dict[str, Any]] = []
        direct = self.artifacts.get("direct_page_mode2") or {}
        load_tables = ((direct.get("diagnostics") or {}).get(
            "electrical_load_tables"
        ) or {})
        rows_by_id = {
            str(row.get("row_id") or ""): row
            for side in ("LEFT", "RIGHT")
            for row in (load_tables.get(side) or {}).get("rows") or ()
            if isinstance(row, Mapping)
        }
        for package in self._table_packages():
            prompt = prompts.table_identity_prompt(
                sheet_context=context.model_sheet_view(self.bundle.sheet_context),
                package_view=package.model_view(),
            )
            digest = content_signature({
                "context": self.bundle.signature,
                "package": package.model_view(),
            })
            payload, call, _cache_hit = self._cached_call(
                prompt=prompt,
                schema=table_identity.IDENTITY_SCHEMA,
                role="table_identity",
                evidence_digest=digest,
            )
            if payload is None:
                reason = (
                    MODEL_TIMEOUT if call and call.error_kind == "TIMEOUT"
                    else BUDGET_EXHAUSTED if call and call.error_kind == BUDGET_EXHAUSTED
                    else MODEL_FAILED
                )
                for question in package.questions:
                    results.append(_human(
                        question.source_item_id, schemas.TABLE_ROW_IDENTITY,
                        reason, call.error if call else "модель не ответила",
                    ))
                continue
            contract_errors = response_contract.validate(
                payload, table_identity.IDENTITY_SCHEMA
            )
            by_id = {
                str(value.get("question_id") or ""): value
                for value in payload.get("resolutions") or ()
                if isinstance(value, Mapping)
            }
            for question in package.questions:
                resolution = by_id.get(question.question_id)
                if contract_errors or resolution is None:
                    results.append(_human(
                        question.source_item_id, schemas.TABLE_ROW_IDENTITY,
                        MODEL_FAILED,
                        "; ".join(contract_errors) or "нет ответа на вопрос",
                    ))
                    continue
                check = table_identity.verify_identity(question, resolution)
                if not check.ok:
                    results.append(_human(
                        question.source_item_id, schemas.TABLE_ROW_IDENTITY,
                        VERIFIER_REJECTED, "; ".join(check.errors),
                        resolution=resolution, check=check.as_dict(),
                    ))
                    continue
                verdict = str(resolution.get("verdict") or "")
                if verdict == table_identity.VERDICT_SAME:
                    match = table_identity.match_from(question, resolution, rows_by_id)
                    if match is None:
                        results.append(_human(
                            question.source_item_id, schemas.TABLE_ROW_IDENTITY,
                            VERIFIER_REJECTED, "выбранные строки не материализуются",
                            resolution=resolution, check=check.as_dict(),
                        ))
                        continue
                    matches.append(match)
                    results.append(_resolved(
                        question.source_item_id, schemas.TABLE_ROW_IDENTITY,
                        resolution, check.as_dict(),
                        source_kind=question.kind,
                    ))
                elif verdict == table_identity.VERDICT_DIFFERENT:
                    results.append(_resolved(
                        question.source_item_id, schemas.TABLE_ROW_IDENTITY,
                        resolution, check.as_dict(), source_kind=question.kind,
                    ))
                else:
                    results.append(_human(
                        question.source_item_id, schemas.TABLE_ROW_IDENTITY,
                        INSUFFICIENT_EVIDENCE,
                        str(resolution.get("human_question") or ""),
                        resolution=resolution, check=check.as_dict(),
                    ))

        derived = table_identity.deterministic_changes(matches, compare_match)
        materialized_source_ids = {
            str(value.get("source_item_id") or "")
            for bucket in ("changes", "unchanged")
            for value in derived.get(bucket) or ()
        }
        for value in results:
            if value["task_id"] in materialized_source_ids:
                value["saves_human_decision"] = True
        return results, derived

    def _general_tasks(self) -> list[dict[str, Any]]:
        return [
            task for task in inventory_module.eligible_items(self.inventory)
            if task["task_type"] != schemas.TABLE_ROW_IDENTITY
        ]

    def _task_view(self, task: Mapping[str, Any]) -> dict[str, Any]:
        task_id = str(task.get("task_id") or "")
        focus = dict(self.bundle.focused_by_task.get(task_id) or {})
        refs = [
            *list(focus.get("candidate_refs") or ()),
            *list(focus.get("context_refs") or ()),
        ]
        return {
            "task_id": task_id,
            "task_type": task.get("task_type"),
            "source_kind": task.get("source_kind"),
            "problem": task.get("summary"),
            "subject": task.get("subject"),
            "candidate_refs": focus.get("candidate_refs") or [],
            "context_refs": focus.get("context_refs") or [],
            "evidence": context.dereference(refs, self.bundle.evidence_catalog),
        }

    def _run_general_call(
        self, tasks: Sequence[Mapping[str, Any]], *, role: str = "analyst",
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        views = [self._task_view(task) for task in tasks]
        prompt = prompts.analyst_prompt(
            sheet_context=context.model_sheet_view(self.bundle.sheet_context),
            tasks=views,
        )
        payload, call, _cache_hit = self._cached_call(
            prompt=prompt, schema=schemas.ANALYST_SCHEMA, role=role,
            evidence_digest=content_signature({
                "context": self.bundle.signature, "tasks": views,
            }),
        )
        if payload is None:
            reason = (
                MODEL_TIMEOUT if call and call.error_kind == "TIMEOUT"
                else BUDGET_EXHAUSTED if call and call.error_kind == BUDGET_EXHAUSTED
                else MODEL_FAILED
            )
            return [
                _human(
                    str(task["task_id"]), str(task["task_type"]), reason,
                    call.error if call else "модель не ответила",
                ) for task in tasks
            ], []
        checks, batch_errors = verifier.verify_batch(tasks, payload, self.bundle)
        by_id = {
            str(value.get("task_id") or ""): value
            for value in payload.get("resolutions") or ()
            if isinstance(value, Mapping)
        }
        results: list[dict[str, Any]] = []
        expansion_requests: list[dict[str, Any]] = []
        for task in tasks:
            task_id = str(task["task_id"])
            resolution = by_id.get(task_id)
            check = checks.get(task_id)
            if batch_errors or resolution is None or check is None:
                results.append(_human(
                    task_id, str(task["task_type"]), MODEL_FAILED,
                    "; ".join(batch_errors) or "нет ответа на задачу",
                ))
                continue
            if not check.ok:
                results.append(_human(
                    task_id, str(task["task_type"]), VERIFIER_REJECTED,
                    "; ".join(check.errors), resolution=resolution,
                    check=check.as_dict(),
                ))
                continue
            if resolution.get("status") == "NEED_MORE_EVIDENCE":
                expansion_requests.append({
                    "task": task,
                    "requested": list(resolution.get("requested_evidence") or ()),
                    "first_resolution": resolution,
                })
                continue
            if resolution.get("status") != "RESOLVED":
                results.append(_human(
                    task_id, str(task["task_type"]), INSUFFICIENT_EVIDENCE,
                    str(resolution.get("human_question") or ""),
                    resolution=resolution, check=check.as_dict(),
                ))
                continue
            record = _resolved(
                task_id, str(task["task_type"]), resolution, check.as_dict(),
                source_kind=str(task.get("source_kind") or ""),
            )
            if task.get("source_kind") != "GRAPH_ENTITY_AMBIGUITY":
                record["saves_human_decision"] = True
            results.append(record)
        return results, expansion_requests

    def _run_general(self) -> list[dict[str, Any]]:
        tasks = self._general_tasks()
        if not tasks:
            return []
        results, requests = self._run_general_call(tasks)
        retry_tasks: list[dict[str, Any]] = []
        for value in requests:
            task = value["task"]
            if not self.expansion_budget.take():
                results.append(_human(
                    str(task["task_id"]), str(task["task_type"]),
                    BUDGET_EXHAUSTED, "исчерпан бюджет контролируемых доборов",
                    resolution=value["first_resolution"],
                ))
                continue
            added = expansion.expand_focus(
                self.bundle, str(task["task_id"]), value["requested"]
            )
            if not added:
                results.append(_human(
                    str(task["task_id"]), str(task["task_type"]),
                    INSUFFICIENT_EVIDENCE,
                    "запрошенного доказательства нет в замороженном контексте",
                    resolution=value["first_resolution"],
                ))
                continue
            retry_tasks.append(task)
        if retry_tasks:
            retry_results, nested = self._run_general_call(
                retry_tasks, role="controlled_expansion"
            )
            results.extend(retry_results)
            for value in nested:
                task = value["task"]
                results.append(_human(
                    str(task["task_id"]), str(task["task_type"]),
                    INSUFFICIENT_EVIDENCE,
                    "после одного контролируемого добора данных недостаточно",
                    resolution=value["first_resolution"],
                ))
        return results

    def _fill_ineligible(self, existing: set[str]) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []
        for task in self.inventory.get("items") or ():
            if not isinstance(task, Mapping) or not task.get("unresolved", True):
                continue
            task_id = str(task.get("task_id") or "")
            if task_id in existing:
                continue
            values.append(_human(
                task_id, str(task.get("task_type") or ""),
                str(task.get("decision") or INSUFFICIENT_EVIDENCE),
                "; ".join(str(value) for value in task.get("missing_evidence") or ()),
            ))
        return values

    def _preliminary_report(
        self, resolutions: Sequence[Mapping[str, Any]],
        derived: Mapping[str, Any],
    ) -> dict[str, Any]:
        baseline = self.artifacts.get("preliminary_report") or {}
        base_counts = ((baseline.get("summary") or {}).get("counts") or {})
        saved = [value for value in resolutions if value.get("saves_human_decision")]
        ai_verified = [
            value for value in resolutions
            if value.get("status") == "AI_RESOLVED_VERIFIED"
        ]
        narrative = []
        for value in saved:
            task_id = str(value.get("task_id") or "")
            # The unresolved task text is the question and may be precisely
            # what the verified resolution corrected.  Publishing it would
            # resurrect a rejected fact (for example a graph-extraction
            # count) despite a sound resolution.  Narrative is therefore
            # copied only from the already verified resolution itself.
            summary = str(
                ((value.get("resolution") or {}).get("engineering_summary"))
                or ""
            )
            if summary:
                narrative.append({"text": summary, "finding_ids": [task_id]})
        return {
            "kind": "stage_comparison_preliminary_report_ai_v2",
            "schema_version": "stage-comparison-preliminary-report-ai-v2.v1",
            "pair_id": self.pair_id,
            "sections": {
                "Найдено автоматически": int(base_counts.get("automatic") or 0),
                "Уточнено ИИ и проверено правилами": len(ai_verified),
                "Требуется проверка инженера": max(
                    0, int(base_counts.get("review") or 0) - len(saved)
                ),
                "Внутренние противоречия документа": int(
                    base_counts.get("inconsistency") or 0
                ),
                "Недостаточно доказательств": int(base_counts.get("unproven") or 0),
            },
            "verified_resolution_ids": [value["task_id"] for value in ai_verified],
            "engineering_narrative": narrative,
            "constraints": {
                "summary_from_verified_only": True,
                "summary_creates_no_facts": True,
                "engineer_approved": False,
                "final_report_unchanged": True,
            },
            "derived_table_changes": list(derived.get("changes") or ()),
        }

    def run(self) -> dict[str, Any]:
        started = time.perf_counter()
        table_results, derived = self._run_table_identity()
        general_results = self._run_general()
        resolutions = [*table_results, *general_results]
        existing = {str(value.get("task_id") or "") for value in resolutions}
        resolutions.extend(self._fill_ineligible(existing))
        resolutions.sort(key=lambda value: str(value.get("task_id") or ""))
        expected_ids = {
            str(task.get("task_id") or "")
            for task in self.inventory.get("items") or ()
            if isinstance(task, Mapping) and task.get("unresolved", True)
        }
        result_ids = [str(value.get("task_id") or "") for value in resolutions]
        if len(result_ids) != len(expected_ids) or set(result_ids) != expected_ids:
            raise AssertionError(
                "AI Analyst v2 result accounting is not one-to-one with "
                "the unresolved inventory"
            )
        diagnostics = {
            "engineering_unresolved": self.inventory["counts"][
                "total_engineering_unresolved"
            ],
            "ai_eligible": self.inventory["counts"][inventory_module.ELIGIBLE],
            "ai_ineligible_no_evidence": self.inventory["counts"][
                inventory_module.NO_EVIDENCE
            ],
            "ai_ineligible_policy": self.inventory["counts"][inventory_module.POLICY],
            "ai_ineligible_human_authority": self.inventory["counts"][
                inventory_module.HUMAN_AUTHORITY
            ],
            "routed": self.inventory["counts"]["routed"],
            "not_routed": self.inventory["counts"]["not_routed"],
            "ai_resolved_verified": sum(
                value["status"] == "AI_RESOLVED_VERIFIED" for value in resolutions
            ),
            "human_required": sum(
                value["status"] == "HUMAN_REQUIRED" for value in resolutions
            ),
            "verifier_rejected": sum(
                value.get("reason_code") == VERIFIER_REJECTED for value in resolutions
            ),
            "need_more_evidence": self.expansion_budget.used,
            "model_calls": self.model_calls,
            "sessions": self.sessions,
            "model_failures": self.model_failures,
            "model_timeouts": self.model_timeouts,
            "duration_ms": int((time.perf_counter() - started) * 1000),
            "human_decisions_saved": sum(
                bool(value.get("saves_human_decision")) for value in resolutions
            ),
            "unsupported_published": 0,
            "cache": self.cache.statistics(),
        }
        saved = diagnostics["human_decisions_saved"]
        diagnostics["seconds_per_saved_decision"] = (
            round(diagnostics["duration_ms"] / 1000 / saved, 3) if saved else None
        )
        artifact = {
            "kind": KIND,
            "schema_version": SCHEMA_VERSION,
            "version": 1,
            "generated_at": utc_now(),
            "pair_id": self.pair_id,
            "feature_flag": settings.FEATURE_FLAG,
            "experimental": True,
            "model": settings.MODEL,
            "reasoning_effort": self.effort,
            "context_signature": self.bundle.signature,
            "inventory_signature": self.inventory["input_signature"],
            "settings": {
                "max_sessions": settings.max_sessions(),
                "max_expansions": settings.max_expansions(),
                "timeout_seconds": settings.timeout_seconds(),
            },
            "resolutions": resolutions,
            "derived_table": derived,
            "diagnostics": diagnostics,
            "constraints": {
                "fast_unchanged": True,
                "human_decisions_unchanged": True,
                "ai_cannot_approve": True,
                "unsupported_results_published": False,
            },
        }
        artifact["input_signature"] = content_signature({
            "schema": SCHEMA_VERSION,
            "context": self.bundle.signature,
            "inventory": self.inventory["input_signature"],
            "effort": self.effort,
            "model": settings.MODEL,
        })
        artifact["preliminary_report"] = self._preliminary_report(
            resolutions, derived
        )
        return artifact


__all__ = [
    "BUDGET_EXHAUSTED",
    "INSUFFICIENT_EVIDENCE",
    "KIND",
    "MODEL_FAILED",
    "MODEL_TIMEOUT",
    "SCHEMA_VERSION",
    "VERIFIER_REJECTED",
    "WholeDocumentAnalyst",
]
