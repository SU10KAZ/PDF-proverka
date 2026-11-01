"""Simulate historical pre-Miner failures without retaining the removed production cap."""
from contextlib import contextmanager
from unittest.mock import patch
from backend.app.services.project_change_v3 import engine, parallel_calls


@contextmanager
def legacy_miner_limit(limit):
    save = engine._save_artifact
    run = parallel_calls.run_items
    plan = {}

    def save_legacy(session_id, pair_id, key, payload):
        if key == 'project_change_v3_miner_plan':
            payload['max_new_calls'] = limit
            plan.update(payload)
        return save(session_id, pair_id, key, payload)

    def run_legacy(*args, **kwargs):
        if kwargs.get('thread_name') == 'v3-miner' and plan.get('new_calls', 0) > limit:
            raise engine._V3Failure('miner_plan_budget_exceeded', 'Historical Miner plan limit', 0)
        return run(*args, **kwargs)

    with patch.object(engine, '_save_artifact', save_legacy), patch.object(parallel_calls, 'run_items', run_legacy):
        yield
