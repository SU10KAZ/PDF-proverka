"""Stage: перенос вердиктов эксперта из предыдущей версии (decision carryover)."""
from backend.app.pipeline.stages.decision_carryover.runner import (
    run_decision_carryover_stage,
)

__all__ = ["run_decision_carryover_stage"]
