"""A complete finite Miner plan is processed without a total-call budget; fake inference only."""
from backend.app.services.project_change_v3 import engine, miner_plan
from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider
from backend.tests.project_change_v3.test_failure_states import env  # noqa: F401
from backend.tests.project_change_v3.test_miner_parallel import handlers
from backend.tests.project_change_v3.test_resume_and_dedupe_repair import _run


def test_all_155_regions_run_despite_legacy_environment_and_request_limit(env, monkeypatch):
    monkeypatch.setenv(miner_plan.MAX_NEW_CALLS_ENV, '64')
    monkeypatch.setenv(engine.UNMATCHED_REVIEW_ENV, '0')
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    fake = FakeProvider(handlers=handlers(155))
    set_test_provider(fake)
    state = _run(env['session_id'], max_new_miner_calls=64)
    assert state['status'] in {'COMPLETED', 'REVIEW'}, state
    assert sum(call['stage'] == 'MINING' for call in fake.calls) == 155
    assert state['provenance']['miner_plan']['new_calls'] == 155
    assert state['provenance']['miner_plan']['max_new_calls'] is None
    assert state['provenance']['miner_concurrency'] == 1  # fixture's explicit concurrency is preserved
