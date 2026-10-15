"""Recover completed mapping on another model; synthetic sources, no inference."""
import json
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import contracts, engine, miner_plan, resume, run_storage
from backend.app.services.project_change_v3.provider import ProviderError, set_test_provider
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env  # noqa: F401
from backend.tests.project_change_v3.test_miner_parallel import handlers
from backend.tests.project_change_v3.test_partial_miner_resume import _snapshot
from backend.tests.project_change_v3.test_resume_and_dedupe_repair import _run
from backend.tests.project_change_v3.test_resume_model_handoff import Receipted


@pytest.fixture
def planned(env, monkeypatch):
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MODEL_CHOICES', 'astra,opus55')
    monkeypatch.setenv(miner_plan.MAX_NEW_CALLS_ENV, '1')
    monkeypatch.setenv(engine.UNMATCHED_REVIEW_ENV, '0')
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    fake = Receipted(handlers=handlers(3))
    set_test_provider(fake)
    state = _run(env['session_id'], model_profile='opus55')
    assert state['reason_code'] == 'miner_plan_budget_exceeded', state
    assert [c['stage'] for c in fake.calls] == ['MAPPING']
    # The engine intentionally excludes FakeProvider calls from paid counters.
    # Model the persisted production counter alongside our synthetic receipts.
    state['model_calls'] = len(state['provenance']['transport_calls'])
    (run_storage.run_dir(env['session_id'], gf.PAIR_ID, state['run_id']) / 'state.json').write_text(json.dumps(state))
    assert resume.load_donor(env['session_id'], gf.PAIR_ID, state['run_id']).before_mining
    return state


def _continue(env, planned, *, limit=3, **kwargs):
    return _run(env['session_id'], resume_from_run_id=planned['run_id'], model_profile='astra',
                resume_model_policy='remaining_stages', max_new_miner_calls=limit, **kwargs)


def test_saved_mapping_handoff_uses_only_astra_for_new_work_and_keeps_donor(env, planned, monkeypatch):
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, planned['run_id'])
    before = _snapshot(directory)
    fake = Receipted(handlers=handlers(3)); set_test_provider(fake)
    checked = []
    fits = engine._miner_fits
    def target_fits(*args):
        checked.append(contracts.active_profile().key)
        return fits(*args)
    monkeypatch.setattr(engine, '_miner_fits', target_fits)
    state = _continue(env, planned)
    assert state['status'] in {'REVIEW', 'COMPLETED'}, state
    assert [c['stage'] for c in fake.calls] == ['MINING'] * 3 + ['DEDUPE']
    assert state['pipeline_stages']['MAPPING']['status'] == 'REUSED'
    prov = state['provenance']
    assert prov['model_handoff']['reused_stages'] == ['MAPPING']
    assert set(prov['model_handoff']['remaining_stages']) == resume.POST_MAPPING_STAGES
    assert prov['resumed_from']['before_mining'] and prov['resumed_from']['saved_regions'] == 0
    for call in prov['transport_calls']:
        assert call['model'] == ('claude-opus-5-5' if call['stage'] == 'MAPPING' else 'gpt-6-astra')
    assert _snapshot(directory) == before
    assert prov['miner_plan']['max_new_calls'] == 3 and miner_plan.max_new_calls() == 1
    assert checked and set(checked) == {'astra'}


def test_unconfirmed_model_switch_is_refused(env, planned):
    fake = Receipted(handlers=handlers(3)); set_test_provider(fake)
    state = _run(env['session_id'], resume_from_run_id=planned['run_id'], model_profile='astra', max_new_miner_calls=3)
    assert state['reason_code'] == 'resume_refused' and not fake.calls


def test_target_plan_budget_stops_before_calls_and_can_resume_again(env, planned):
    fake = Receipted(handlers=handlers(3)); set_test_provider(fake)
    stopped = _continue(env, planned, limit=1)
    assert stopped['reason_code'] == 'miner_plan_budget_exceeded' and not fake.calls
    assert resume.resume_options(env['session_id'], gf.PAIR_ID, stopped['run_id'])['model_profile'] == 'astra'
    done = _run(env['session_id'], resume_from_run_id=stopped['run_id'], max_new_miner_calls=3)
    assert done['status'] in {'REVIEW', 'COMPLETED'}, done
    assert done['provenance']['model_handoff']['source_model']['model'] == 'claude-opus-5-5'
    assert 'MAPPING' not in [c['stage'] for c in fake.calls]


@pytest.mark.parametrize('fail_stage', ['MINING', 'DEDUPE'])
def test_another_failure_preserves_mixed_receipts_for_same_target_resume(env, planned, fail_stage):
    base = handlers(3)
    original = base[fail_stage]
    def failure(**kwargs):
        if fail_stage == 'DEDUPE' or kwargs['data']['frozen_region']['region_id'] == 'P002':
            raise ProviderError('provider_timeout', 'synthetic interruption')
        return original(**kwargs)
    set_test_provider(Receipted(handlers={**base, fail_stage: failure}))
    stopped = _continue(env, planned)
    assert stopped['reason_code'] == 'provider_timeout', stopped
    donor = resume.load_donor(env['session_id'], gf.PAIR_ID, stopped['run_id'])
    assert not donor.before_mining
    fake = Receipted(handlers=base); set_test_provider(fake)
    done = _run(env['session_id'], resume_from_run_id=stopped['run_id'], max_new_miner_calls=3)
    assert done['status'] in {'REVIEW', 'COMPLETED'}, done
    assert [c['stage'] for c in fake.calls] == (['MINING'] * 2 if fail_stage == 'MINING' else []) + ['DEDUPE']
    mapping = [c for c in done['provenance']['transport_calls'] if c['stage'] == 'MAPPING']
    assert len(mapping) == 1 and mapping[0]['model'] == 'claude-opus-5-5'


@pytest.mark.parametrize('damage', ['attempt', 'missing_receipt', 'reason', 'source', 'checkpoint', 'prompt'])
def test_handoff_refuses_unproven_empty_mining_or_changed_evidence_before_calls(env, planned, damage):
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, planned['run_id'])
    path = directory / 'state.json'
    state = json.loads(path.read_text())
    if damage == 'attempt':
        receipt = {**state['provenance']['transport_calls'][0], 'stage': 'MINING', 'call_id': 'failed-miner'}
        state['provenance']['transport_calls'].append(receipt)
        state['model_calls'] += 1
    elif damage == 'missing_receipt':
        state['model_calls'] += 1
    elif damage == 'reason':
        state['reason_code'] = 'provider_timeout'
    elif damage == 'prompt':
        state['provenance']['mapper_prompt_sha256'] = 'changed'
    elif damage == 'source':
        source = directory / 'project_change_v3_source_manifest.json'
        manifest = json.loads(source.read_text()); manifest['old_pdf_sha256'] = 'changed'
        source.write_text(json.dumps(manifest))
    else:
        Path(state['provenance']['miner_checkpoint']['path']).write_text('{}')
    path.write_text(json.dumps(state))
    fake = Receipted(handlers=handlers(3)); set_test_provider(fake)
    stopped = _continue(env, planned)
    assert stopped['reason_code'] == 'resume_refused' and not fake.calls


def test_receipt_guard_accepts_remaining_work_but_never_new_mapping(env, planned):
    donor = resume.load_donor(env['session_id'], gf.PAIR_ID, planned['run_id'])
    prov = {'provider': 'codex_cli_subscription', 'model': 'gpt-6-astra', 'reasoning': 'xhigh',
            'model_handoff': {'reused_stages': ['MAPPING']}}
    call = {k: prov[k] for k in ('provider', 'model', 'reasoning')}
    for stage in resume.POST_MAPPING_STAGES:
        assert resume.mismatched_calls([{**call, 'stage': stage, 'call_id': 'new'}], prov, donor) == []
    assert resume.mismatched_calls([{**call, 'stage': 'MAPPING', 'call_id': 'new'}], prov, donor) == ['new']


def test_repeated_resume_rejects_relabelled_historical_mapping(env, planned):
    set_test_provider(Receipted(handlers=handlers(3)))
    stopped = _continue(env, planned, limit=1)
    path = run_storage.run_dir(env['session_id'], gf.PAIR_ID, stopped['run_id']) / 'state.json'
    saved = json.loads(path.read_text())
    saved['provenance']['transport_calls'][0]['model'] = 'gpt-6-astra'
    path.write_text(json.dumps(saved))
    fake = Receipted(handlers=handlers(3)); set_test_provider(fake)
    state = _continue(env, stopped)
    assert state['reason_code'] == 'resume_refused' and not fake.calls


@pytest.mark.parametrize('limit', [0, 257, True, 1.5, '3'])
def test_api_rejects_invalid_per_run_limit_before_work(limit):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as api
    app = FastAPI(); app.include_router(api.router)
    response = TestClient(app).post('/api/stage-comparison/sessions/s/pairs/p/production/run',
                                   json={'input_mode': 'DOCUMENT', 'max_new_miner_calls': limit})
    assert response.status_code == 422


def test_public_state_and_api_expose_and_forward_the_explicit_recovery(env, planned, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as api
    from backend.app.services.stage_comparison.production_orchestrator import get_production_state
    state = get_production_state(env['session_id'], gf.PAIR_ID)
    assert state['resume_available']
    assert state['resume_options'] == {
        'model_profile': 'opus55', 'can_switch_model': True, 'mode': 'before_mining',
        'saved_regions': 0, 'planned_new_calls': 3, 'max_new_miner_calls': 1,
    }
    captured = {}
    def start(*args, **kwargs):
        captured.update(kwargs)
        return {'status': 'RUNNING'}
    monkeypatch.setattr(api.production, 'run_production_comparison', start)
    app = FastAPI(); app.include_router(api.router)
    response = TestClient(app).post(
        f"/api/stage-comparison/sessions/{env['session_id']}/pairs/{gf.PAIR_ID}/production/run",
        json={'input_mode': 'DOCUMENT', 'resume_from_run_id': planned['run_id'],
              'model_profile': 'astra', 'resume_model_policy': 'remaining_stages', 'max_new_miner_calls': 128})
    assert response.status_code == 200
    assert captured['max_new_miner_calls'] == 128 and captured['resume_model_policy'] == 'remaining_stages'
