"""Trial-only subscription retries. This module never imports or calls OpenRouter."""
from __future__ import annotations

import asyncio
from functools import wraps
import json
import os
from pathlib import Path
import re
import time

RETRY_DELAY_SECONDS = 10
_FATAL = re.compile(r'unauthorized|not authenticated|invalid api key|permission denied|unsupported model|model_not_found|insufficient_quota|quota exceeded|usage limit reached', re.I)
_TRANSIENT = re.compile(r'(?:claude|codex)_json_not_found|\b(429|500|502|503|504|529)\b|too many requests|rate[_ ]limit|overload|at capacity|temporarily unavailable|service unavailable|bad gateway|internal server error|connection (?:reset|refused|closed)|stream (?:error|disconnected)|(?:codex|claude)[_ ]timeout|\[timeout\]', re.I)
_USAGE_FIELDS = ('input_tokens', 'output_tokens', 'cached_tokens', 'cache_write_tokens', 'reasoning_tokens', 'cost_usd', 'duration_ms')


class TrialStopped(RuntimeError):
    pass


def transient_reason(result) -> str:
    if not getattr(result, 'is_error', False):
        return ''
    text = f"{getattr(result, 'error_message', '') or ''}\n{getattr(result, 'text', '') or ''}"
    if _FATAL.search(text):
        return ''
    match = _TRANSIENT.search(text)
    return match.group(0).lower() if match else ''


async def retry_subscription_call(call, *args, stop_requested, on_attempt, sleep=None, max_attempts=None, **kwargs):
    """Repeat the identical failed request after ten seconds, with an optional attempt cap.

    A terminal authentication/configuration failure is returned to the existing
    pipeline, rather than turning a broken local setup into an endless loop.
    Receipts account for every attempt, including failures. Nothing from the
    engineering input is copied into a retry receipt.
    """
    if max_attempts is not None and (type(max_attempts) is not int or max_attempts < 1):
        raise ValueError('max_attempts must be a positive integer or None')
    sleep = sleep or asyncio.sleep
    attempt = 0
    totals = {field: 0 for field in _USAGE_FIELDS}
    while True:
        if stop_requested():
            raise TrialStopped('User stop request is active')
        attempt += 1
        result = await call(*args, **kwargs)
        reason = transient_reason(result)
        usage = {field: getattr(result, field, 0) or 0 for field in _USAGE_FIELDS}
        for field, value in usage.items():
            totals[field] += value
        exhausted = bool(reason and max_attempts is not None and attempt >= max_attempts)
        on_attempt({
            'timestamp': time.time(), 'stage': kwargs.get('stage'),
            'model': kwargs.get('model') or getattr(result, 'model', None),
            'attempt': attempt, 'is_error': bool(getattr(result, 'is_error', False)),
            'retry_reason': reason or None, 'next_delay_seconds': RETRY_DELAY_SECONDS if reason and not exhausted else None,
            'retry_budget_exhausted': exhausted,
            'usage': usage,
        })
        if not reason or exhausted:
            # Existing stage counters receive total usage of this request,
            # including unsuccessful attempts, rather than only the last answer.
            if attempt > 1:
                for field, value in totals.items():
                    if hasattr(result, field):
                        setattr(result, field, value)
            return result
        print(f"[trial retry] {kwargs.get('stage') or 'json'} {kwargs.get('model') or getattr(result, 'model', '')}: {reason}; wait 10 seconds, retry the same request (attempt {attempt + 1})", flush=True)
        await sleep(RETRY_DELAY_SECONDS)


def install_subscription_retries(workspace: Path):
    from backend.app.services.llm import codex_runner, claude_json_runner
    def record(item):
        path = workspace / 'subscription_attempts.jsonl'
        # One append syscall per event, shared by concurrent block workers.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, (json.dumps(item, ensure_ascii=False) + '\n').encode())
        finally:
            os.close(fd)
    for module, name in ((codex_runner, 'run_codex_json_messages'), (claude_json_runner, 'run_claude_json_messages')):
        original = getattr(module, name)
        if getattr(original, '_classic_trial_retry', False):
            continue
        def make_wrapper(call):
            @wraps(call)
            async def wrapped(*args, **kwargs):
                return await retry_subscription_call(
                    call, *args, stop_requested=lambda: (workspace / 'STOP_REQUESTED.json').exists(),
                    on_attempt=record, **kwargs)
            wrapped._classic_trial_retry = True
            return wrapped
        setattr(module, name, make_wrapper(original))
