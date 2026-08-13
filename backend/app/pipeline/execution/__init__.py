"""Абстракция исполнения аудита: локально или на удалённом audit-worker.

Пакет намеренно тонкий. Он НЕ переизобретает оркестрацию этапов: локальное
исполнение остаётся тем же вызовом `PipelineManager._dispatch_action`, что и
раньше, а удалённое — durable-заданием в подсистеме распределённых воркеров.
Общее у них только одно: типизированный контракт, по которому менеджер
одинаково спрашивает «жив ли?», «отмени», «подхвати после рестарта».
"""
from backend.app.pipeline.execution.contracts import (  # noqa: F401
    AuditExecutionOptions,
    ExecutionBackend,
    ExecutionError,
    ExecutionHandle,
    ExecutionMode,
    ExecutionRequest,
    ExecutionResult,
    ExecutionSnapshot,
    Liveness,
    LivenessVerdict,
)

__all__ = [
    "AuditExecutionOptions",
    "ExecutionBackend",
    "ExecutionError",
    "ExecutionHandle",
    "ExecutionMode",
    "ExecutionRequest",
    "ExecutionResult",
    "ExecutionSnapshot",
    "Liveness",
    "LivenessVerdict",
]
