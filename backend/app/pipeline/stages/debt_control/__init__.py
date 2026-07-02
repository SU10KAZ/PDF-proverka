"""Stage: «Контроль долгов» — согласованные замечания прошлой версии не теряются."""
from backend.app.pipeline.stages.debt_control.runner import run_debt_control_stage

__all__ = ["run_debt_control_stage"]
