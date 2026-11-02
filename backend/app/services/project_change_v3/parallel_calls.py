"""Bounded per-stage provider calls; validation and all artifact writes stay on the caller thread.

Each work item is a coroutine yielding a provider request and receiving a CallOutcome.
Only complete() runs in workers. A work item keeps its own provider across retries;
no last_response/last_transport state is shared between concurrent calls.
"""
from __future__ import annotations

from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from copy import copy, deepcopy
from dataclasses import dataclass, field
import os
import time
from typing import Any, Callable, Generator, Iterable

MINER_CONCURRENCY_ENV = "PROJECT_COMPARISON_V3_MINER_CONCURRENCY"
MAPPER_CONCURRENCY_ENV = "PROJECT_COMPARISON_V3_MAPPER_CONCURRENCY"
UNMATCHED_CONCURRENCY_ENV = "PROJECT_COMPARISON_V3_UNMATCHED_CONCURRENCY"
VERIFICATION_CONCURRENCY_ENV = "PROJECT_COMPARISON_V3_SOURCE_VERIFICATION_CONCURRENCY"
DEFAULT_CONCURRENCY = 5
_RUN_CONCURRENCY: ContextVar[int | None] = ContextVar("projectchange_concurrency", default=None)


@contextmanager
def use_concurrency(value: int | None):
    if value is not None and (type(value) is not int or value not in (5, 10)):
        raise ValueError("parallelism must be 5 or 10")
    token = _RUN_CONCURRENCY.set(value)
    try:
        yield
    finally:
        _RUN_CONCURRENCY.reset(token)


def concurrency(env_name: str) -> int:
    selected = _RUN_CONCURRENCY.get()
    if selected is not None:
        return selected
    value = int(os.environ.get(env_name, str(DEFAULT_CONCURRENCY)))
    if not 1 <= value <= 10:
        raise ValueError(f"{env_name} must be between 1 and 10")
    return value


CAPACITY_RETRY_DELAYS = (60.0, 120.0, 240.0)
_CAPACITY_POLICY: ContextVar[Any] = ContextVar("projectchange_capacity_policy", default=None)


class CapacityPause(RuntimeError):
    """All remaining stage items have drained; manual intervention is required."""


@dataclass
class CapacityPolicy:
    limit: int = 10
    events: list = field(default_factory=list)
    on_retry: Callable | None = None


@contextmanager
def use_capacity_policy():
    policy = CapacityPolicy()
    token = _CAPACITY_POLICY.set(policy)
    try:
        yield policy
    finally:
        _CAPACITY_POLICY.reset(token)


def capacity_policy():
    return _CAPACITY_POLICY.get()


def is_capacity_error(error):
    from .provider import ProviderError
    return (isinstance(error, ProviderError) and error.code == "TRANSIENT"
            and any(word in error.message.lower() for word in
                    ("at capacity", "overloaded", "overload", "429", "too many requests", "rate limit")))


@dataclass
class CallOutcome:
    response: Any = None
    error: Exception | None = None
    receipt: dict[str, Any] = field(default_factory=dict)
    last_response: Any = None


@dataclass
class CallControl:
    stopped: bool = False


#: Ключ запроса: пауза перед вызовом (повтор после отказа провайдера). Провайдер его не видит.
DELAY_KEY = "retry_delay_s"


def _wait(delay_s: float, cancelled: Callable[[], bool]) -> bool:
    """Пауза в рабочем потоке: координатор и соседние вызовы не ждут. False — прогон отменён."""
    deadline = time.monotonic() + delay_s
    while (left := deadline - time.monotonic()) > 0:
        if cancelled():
            return False
        time.sleep(min(1.0, left))
    return not cancelled()


def _complete(provider: Any, request: dict[str, Any],
              cancelled: Callable[[], bool] = lambda: False) -> CallOutcome:
    # A failed call must never inherit a preceding call's receipt/answer.
    for name in ("last_transport", "last_response"):
        if hasattr(provider, name):
            setattr(provider, name, None)
    outcome = CallOutcome()
    request = dict(request)
    delay_s = float(request.pop(DELAY_KEY, 0) or 0)
    if delay_s > 0 and not _wait(delay_s, cancelled):
        from .provider import ProviderError
        outcome.error = ProviderError("CANCELLED", "run cancelled before the delayed retry")
        return outcome
    try:
        outcome.response = provider.complete(**request)
    except Exception as exc:
        outcome.error = exc
    outcome.receipt = deepcopy(getattr(provider, "last_transport", None) or {})
    outcome.last_response = getattr(provider, "last_response", None)
    return outcome


def run_items(
    items: Iterable[tuple[int, Any]],
    *,
    start_item: Callable[..., Generator],
    provider: Any,
    workers: int,
    control: CallControl,
    cancelled: Callable[[], bool],
    on_completed: Callable[[], None],
    thread_name: str,
) -> None:
    """Keep at most workers work items in flight, including their identical retries.

    Capacity overloads reduce admission to five and retry the same request after
    60/120/240 seconds. Exhaustion drains the entire stage before CapacityPause.
    Other errors/cancel stop admission and drain already submitted calls.
    No worker mutates run progress, checkpoints, coverage or shared receipts.
    """
    pending: dict = {}
    ready = deque()
    waiting = iter(items)
    exhausted = False
    first_error: Exception | None = None
    capacity_failures = []
    policy = _CAPACITY_POLICY.get()

    def limit():
        return min(workers, policy.limit) if policy else workers

    def failed(exc: Exception) -> None:
        nonlocal first_error
        control.stopped = True
        if first_error is None:
            first_error = exc

    def advance(generator, instance, outcome=None, *, first=False, capacity_exhausted=False):
        try:
            request = next(generator) if first else generator.send(outcome)
            if not control.stopped and not cancelled() and not capacity_exhausted:
                ready.append((generator, instance, request, 0))
            else:
                generator.close()
        except StopIteration:
            if not control.stopped and not cancelled():
                try:
                    on_completed()
                except Exception as exc:
                    failed(exc)
        except Exception as exc:
            # The item has persisted its final failed answer. A capacity failure
            # must not cancel admission of the remaining items in this stage.
            if not capacity_exhausted or getattr(exc, "reason", getattr(exc, "code", None)) != "CAPACITY_RETRIES_EXHAUSTED":
                failed(exc)
            generator.close()

    try:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=thread_name) as pool:
            while pending or ready or (not exhausted and not control.stopped):
                if cancelled():
                    control.stopped = True
                while len(pending) < limit() and not control.stopped:
                    if any(future.done() for future in pending):
                        break
                    if ready:
                        generator, instance, request, retries = ready.popleft()
                        future = pool.submit(copy_context().run, _complete, instance, request, cancelled)
                        pending[future] = (generator, instance, request, retries, limit())
                    elif not exhausted:
                        try:
                            index, item = next(waiting)
                        except StopIteration:
                            exhausted = True
                            break
                        instance = copy(provider) if workers > 1 else provider
                        advance(start_item(index, item), instance, first=True)
                    else:
                        break
                    if cancelled():
                        control.stopped = True
                if not pending:
                    break
                done, _ = wait(pending, timeout=0.1, return_when=FIRST_COMPLETED)
                if cancelled():
                    control.stopped = True
                for future in done:
                    generator, instance, request, retries, submitted_limit = pending.pop(future)
                    try:
                        outcome = future.result()
                    except Exception as exc:
                        failed(exc)
                        generator.close()
                        continue
                    if policy and is_capacity_error(outcome.error) and not control.stopped and not cancelled():
                        lowered = submitted_limit > 5
                        policy.limit = min(policy.limit, 5)
                        retry = lowered or retries < len(CAPACITY_RETRY_DELAYS)
                        delay = 0.0 if lowered else CAPACITY_RETRY_DELAYS[retries] if retry else None
                        event = {"stage": request.get("stage"), "call_id": request.get("call_id"),
                                 "submitted_parallelism": submitted_limit, "effective_parallelism": limit(),
                                 "retry": retries + (not lowered) if retry else None,
                                 "delay_s": delay, "exhausted": not retry,
                                 "error": str(outcome.error)}
                        policy.events.append(event)
                        try:
                            if policy.on_retry:
                                policy.on_retry(event, outcome, retry)
                        except Exception as exc:
                            failed(exc)
                            generator.close()
                            continue
                        if retry:
                            ready.append((generator, instance, {**request, DELAY_KEY: delay},
                                          retries if lowered else retries + 1))
                            continue
                        capacity_failures.append(request.get("call_id"))
                        # Consumers must not apply another layer of transient retries.
                        from .provider import ProviderError
                        outcome.error = ProviderError("CAPACITY_RETRIES_EXHAUSTED", str(outcome.error))
                        advance(generator, instance, outcome, capacity_exhausted=True)
                    else:
                        advance(generator, instance, outcome)
            if first_error is not None:
                raise first_error
            if capacity_failures and not cancelled():
                raise CapacityPause("Модель перегружена: исчерпаны 3 повтора с паузами 1, 2 и 4 минуты. "
                                    "Остальные блоки этапа обработаны, прогресс сохранён. "
                                    "Нужна ручная проверка. Запросы: " + ", ".join(map(str, capacity_failures)))
    finally:
        for generator, *_ in ready:
            generator.close()
