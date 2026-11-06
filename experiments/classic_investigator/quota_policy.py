"""Fail-closed local subscription ceiling; never queries a provider."""
import json
from pathlib import Path
import time

class SubscriptionCeiling(RuntimeError):pass


def assert_allowed(work, model, *, cache_file=None, now=None):
    if 'claude' not in str(model).lower() and 'sonnet' not in str(model).lower():return
    policy_file=Path(work)/'provider_policy.json'
    if not policy_file.exists():return
    policy=json.loads(policy_file.read_text())
    if policy.get('claude_mode')=='disabled':raise SubscriptionCeiling('Claude disabled by user quota policy')
    now=time.time() if now is None else now
    try:
        cache=json.loads(Path(cache_file or Path.home()/'.claude.json').read_text())['cachedUsageUtilization']
        age=now-float(cache['fetchedAtMs'])/1000
        pct=float(cache['utilization']['seven_day']['utilization'])
        if age<0 or age>policy.get('max_cache_age_seconds',60):raise ValueError('stale usage')
    except (OSError,KeyError,ValueError,TypeError) as e:
        raise SubscriptionCeiling('Claude usage unknown or stale; use Codex') from e
    if pct>=float(policy['claude_stop_percent']):raise SubscriptionCeiling(f'Claude ceiling reached: {pct}%')
