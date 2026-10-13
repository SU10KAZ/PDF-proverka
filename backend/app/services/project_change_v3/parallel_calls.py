"""Bounded Mapper/Miner calls; validation and all artifact writes stay on the caller thread.

Each work item is a coroutine yielding a provider request and receiving a CallOutcome.
Only complete() runs in workers. A work item keeps its own provider across retries;
no last_response/last_transport state is shared between concurrent calls.
"""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextvars import copy_context
from copy import copy, deepcopy
from dataclasses import dataclass, field
import os
from typing import Any, Callable, Generator, Iterable

MINER_CONCURRENCY_ENV = "PROJECT_COMPARISON_V3_MINER_CONCURRENCY"
MAPPER_CONCURRENCY_ENV = "PROJECT_COMPARISON_V3_MAPPER_CONCURRENCY"
DEFAULT_CONCURRENCY = 10


def concurrency(env_name: str) -> int:
    value = int(os.environ.get(env_name, str(DEFAULT_CONCURRENCY)))
    if not 1 <= value <= 10:
        raise ValueError(f"{env_name} must be between 1 and 10")
    return value


@dataclass
class CallOutcome:
    response: Any = None
    error: Exception | None = None
    receipt: dict[str, Any] = field(default_factory=dict)
    last_response: Any = None


@dataclass
class CallControl:
    stopped: bool = False


def _complete(provider: Any, request: dict[str, Any]) -> CallOutcome:
    # A failed call must never inherit a preceding call's receipt/answer.
    for name in ("last_transport", "last_response"):
        if hasattr(provider, name):
            setattr(provider, name, None)
    outcome = CallOutcome()
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

    On error/cancel: stop admission and retries, drain already submitted calls
    through their region validators/persistence, then raise the first error.
    No worker mutates run progress, checkpoints, coverage or shared receipts.
    """
    pending: dict = {}
    waiting = iter(items)
    exhausted = False
    first_error: Exception | None = None

    def failed(exc: Exception) -> None:
        nonlocal first_error
        control.stopped = True
        if first_error is None:
            first_error = exc

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=thread_name) as pool:
        def submit(generator, instance, request):
            future = pool.submit(copy_context().run, _complete, instance, request)
            pending[future] = (generator, instance)

        def advance(generator, instance, outcome=None, *, first=False):
            try:
                request = next(generator) if first else generator.send(outcome)
                if not control.stopped and not cancelled():
                    submit(generator, instance, request)
                else:
                    generator.close()
            except StopIteration:
                if not control.stopped and not cancelled():
                    try:
                        on_completed()
                    except Exception as exc:
                        failed(exc)
            except Exception as exc:
                failed(exc)
                generator.close()

        while pending or (not exhausted and not control.stopped):
            if cancelled():
                control.stopped = True
            while len(pending) < workers and not exhausted and not control.stopped:
                # Observe a finished call (including failure) before spending
                # time preparing and submitting another source bundle.
                if any(future.done() for future in pending):
                    break
                try:
                    index, item = next(waiting)
                except StopIteration:
                    exhausted = True
                    break
                # Real providers hold immutable settings and per-call state only.
                # Keep the original instance in explicit sequential mode.
                instance = copy(provider) if workers > 1 else provider
                advance(start_item(index, item), instance, first=True)
                if cancelled():
                    control.stopped = True
            if not pending:
                break
            done, _ = wait(pending, timeout=0.1, return_when=FIRST_COMPLETED)
            if cancelled():
                control.stopped = True
            # Process the whole ready group before admitting another region.
            for future in done:
                generator, instance = pending.pop(future)
                try:
                    outcome = future.result()
                except Exception as exc:
                    failed(exc)
                    generator.close()
                    continue
                advance(generator, instance, outcome)
        if first_error is not None:
            raise first_error
