"""Minimal v3 selector projection for remaining HRO closure blockers only."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ..ai import gateway
from ..ai_v3.engine import BoundedSelectorAnalyst, UNANIMITY
from ..production_artifacts import content_signature
from . import settings


class QuestionClosureSelector(BoundedSelectorAnalyst):
    """Reuse v3 candidates/verifier while sending only closure-relevant tasks.

    The v3 candidate factory is rebuilt deterministically, then projected to the
    explicitly routed task IDs and candidate types. No v3 task, candidate, or
    verifier implementation is changed.
    """

    def __init__(
        self,
        *,
        artifacts: Mapping[str, Mapping[str, Any]],
        pair_id: str,
        routed_tasks: Sequence[Mapping[str, Any]],
        fast_input_signature: str,
        cache_dir: Path | str | None = None,
        prompt_capture_dir: Path | str | None = None,
        call: Callable[..., gateway.CallResult] | None = None,
        run_id: str = "",
        cache_enabled: bool = False,
        closure_contract_signature: str = "",
        hro_question_signature: str = "",
        closure_layer_version: str = settings.CLOSURE_LAYER_VERSION,
    ) -> None:
        settings.require_enabled()
        super().__init__(
            artifacts=artifacts,
            pair_id=pair_id,
            mode=UNANIMITY,
            fast_input_signature=fast_input_signature,
            cache_dir=cache_dir,
            cache_enabled=cache_enabled,
            prompt_capture_dir=prompt_capture_dir,
            call=call,
            run_id=run_id,
            require_feature=False,
            cache_context={
                "closure_contract_signature": closure_contract_signature,
                "hro_question_signature": hro_question_signature,
                "closure_layer_version": closure_layer_version,
            },
            # A failed selector pass is a fail-closed outcome, not a reason to
            # retry until the model eventually returns the desired answer.
            model_retries=0,
        )
        source_signature = str(self.factory.get("candidate_set_signature") or "")
        source_tasks = {
            str(value.get("task_id") or ""): value
            for value in self.factory.get("tasks") or ()
            if isinstance(value, Mapping)
        }
        projected = []
        for route in sorted(routed_tasks, key=lambda value: str(value.get("task_id") or "")):
            task_id = str(route.get("task_id") or "")
            if not route.get("route_to_ai"):
                continue
            if task_id not in source_tasks:
                raise ValueError(f"closure task is absent from v3 factory: {task_id}")
            task = deepcopy(source_tasks[task_id])
            allowed_types = {
                str(value) for value in route.get("allowed_candidate_types") or ()
            }
            if allowed_types:
                task["candidates"] = [
                    value for value in task.get("candidates") or ()
                    if str(value.get("candidate_type") or "") in allowed_types
                ]
            candidate_ids = {
                str(value.get("candidate_id") or "")
                for value in task.get("candidates") or ()
            }
            task["selectable_candidate_ids"] = [
                value for value in task.get("selectable_candidate_ids") or ()
                if str(value) in candidate_ids
            ]
            task["deterministic_winner_candidate_id"] = None
            if not self._needs_model(task):
                raise ValueError(f"closure task has no bounded semantic choice: {task_id}")
            task["source_task_signature"] = task.get("task_signature")
            task["task_signature"] = content_signature({
                key: value for key, value in task.items() if key != "task_signature"
            })
            projected.append(task)
        if not projected:
            raise ValueError("no HRO closure task is eligible for AI routing")
        self.factory = {
            **self.factory,
            "kind": "stage_comparison_question_closure_candidate_projection",
            "schema_version": "stage-comparison-question-closure-candidates.v1",
            "source_candidate_set_signature": source_signature,
            "tasks": projected,
        }
        self.factory["candidate_set_signature"] = content_signature({
            "source_candidate_set_signature": source_signature,
            "tasks": projected,
        })

    def run(self) -> dict[str, Any]:
        value = super().run()
        value["kind"] = "stage_comparison_question_closure_selector_run"
        value["schema_version"] = "stage-comparison-question-closure-selector-run.v1"
        value["experimental"] = False
        value["feature_flag"] = settings.FEATURE_FLAG
        value["closure_layer_version"] = settings.CLOSURE_LAYER_VERSION
        value["source_candidate_set_signature"] = self.factory.get(
            "source_candidate_set_signature"
        )
        value["constraints"]["stable_v3_core_reselected"] = False
        value["constraints"]["closure_tasks_only"] = True
        return value


__all__ = ["QuestionClosureSelector"]
