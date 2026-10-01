"""Explicit tail-model handoff preserves donor evidence and model attribution."""
import hashlib
import json

import pytest

from backend.app.services.project_change_v3 import contracts, resume, run_storage
from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env, _stored  # noqa: F401
from backend.tests.project_change_v3.test_resume_and_dedupe_repair import _run


class Receipted(FakeProvider):
    def complete(self, **kwargs):
        profile = contracts.active_profile()
        self.last_transport = {'provider': profile.provider, 'model': profile.model,
                               'reasoning': contracts.REASONING}
        return super().complete(**kwargs)


@pytest.fixture
def donor(env, monkeypatch):
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MODEL_CHOICES', 'astra,opus55')
    fake = Receipted(handlers={**gf.fake_handlers(), 'DEDUPE': lambda **_: {
        'pair': 'wrong', 'decisions': [], 'notes': []}})
    set_test_provider(fake)
    state = _run(env['session_id'], model_profile='opus55')
    assert state['reason_code'] == 'dedupe_failed', state
    return state


def _resume(env, donor, **kwargs):
    return _run(env['session_id'], resume_from_run_id=donor['run_id'], model_profile='astra', **kwargs)


def test_handoff_reuses_claude_answers_and_attributes_only_tail_calls_to_astra(env, donor):
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, donor['run_id'])
    before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.rglob('*') if p.is_file()}
    fake = Receipted(handlers=gf.fake_handlers())
    set_test_provider(fake)
    state = _resume(env, donor, resume_model_policy='remaining_stages')
    assert state['status'] in {'REVIEW', 'COMPLETED'}, state
    assert [c['stage'] for c in fake.calls] == ['DEDUPE']
    prov = _stored(env['session_id'], 'project_change_v3_result')['provenance']
    assert prov['model'] == 'gpt-6-astra'
    assert prov['resumed_from']['model'] == 'claude-opus-5-5'
    handoff = prov['model_handoff']
    assert handoff['source_model']['model'] == 'claude-opus-5-5'
    assert handoff['remaining_model']['model'] == 'gpt-6-astra'
    for call in prov['transport_calls']:
        assert call['model'] == ('claude-opus-5-5' if call.get('resumed_from_run_id') else 'gpt-6-astra')
    assert before == {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.rglob('*') if p.is_file()}


def test_switching_model_without_explicit_handoff_is_refused(env, donor):
    fake = Receipted(handlers=gf.fake_handlers())
    set_test_provider(fake)
    state = _resume(env, donor)
    assert state['reason_code'] == 'resume_refused'
    assert not fake.calls


@pytest.mark.parametrize('field', ['mapper_prompt_sha256', 'miner_prompt_sha256', 'unmatched_prompt_sha256',
                                   'miner_output_format', 'reasoning'])
def test_handoff_still_rejects_changed_replay_contract(env, donor, field):
    p = run_storage.run_dir(env['session_id'], gf.PAIR_ID, donor['run_id']) / 'state.json'
    saved = json.loads(p.read_text())
    if field == 'miner_output_format':
        saved['provenance']['miner_output'] = {'format': 'changed'}
    else:
        saved['provenance'][field] = 'changed'
    p.write_text(json.dumps(saved))
    fake = Receipted(handlers=gf.fake_handlers())
    set_test_provider(fake)
    state = _resume(env, donor, resume_model_policy='remaining_stages')
    assert state['reason_code'] == 'resume_refused', state
    assert not fake.calls


def test_handoff_still_rejects_changed_pdf(env, donor):
    p = run_storage.run_dir(env['session_id'], gf.PAIR_ID, donor['run_id']) / 'project_change_v3_source_manifest.json'
    saved = json.loads(p.read_text()); saved['new_pdf_sha256'] = 'changed'
    p.write_text(json.dumps(saved))
    fake = Receipted(handlers=gf.fake_handlers()); set_test_provider(fake)
    state = _resume(env, donor, resume_model_policy='remaining_stages')
    assert state['reason_code'] == 'resume_refused' and not fake.calls


def test_unexpected_donor_model_is_refused_before_new_calls(env, donor):
    p = run_storage.run_dir(env['session_id'], gf.PAIR_ID, donor['run_id']) / 'state.json'
    saved = json.loads(p.read_text()); saved['provenance']['transport_calls'][0]['model'] = 'wrong'
    p.write_text(json.dumps(saved))
    fake = Receipted(handlers=gf.fake_handlers()); set_test_provider(fake)
    state = _resume(env, donor, resume_model_policy='remaining_stages')
    assert state['reason_code'] == 'resume_refused' and not fake.calls


def test_handoff_does_not_allow_new_calls_to_use_the_donor_model(env, donor):
    class Wrong(Receipted):
        def complete(self, **kwargs):
            answer = super().complete(**kwargs)
            self.last_transport['model'] = 'claude-opus-5-5'
            return answer
    set_test_provider(Wrong(handlers=gf.fake_handlers()))
    state = _resume(env, donor, resume_model_policy='remaining_stages')
    assert state['reason_code'] == 'v3_model_mixing', state
    assert _stored(env['session_id'], 'project_change_v3_result') is None


def test_handoff_model_guard_covers_verification_and_rejects_new_mining():
    prov = {'provider': 'codex_cli_subscription', 'model': 'gpt-6-astra', 'reasoning': 'xhigh', 'model_handoff': { 'policy': 'remaining_stages' }}
    call = {**prov, 'stage': 'SOURCE_VERIFICATION', 'call_id': 'verify'}
    assert resume.mismatched_calls([call], prov) == []
    assert resume.mismatched_calls([{**call, 'model': 'wrong'}], prov) == ['verify']
    assert resume.mismatched_calls([{**call, 'stage': 'MINING'}], prov) == ['verify']
    assert resume.mismatched_calls([{**call, 'resumed_from_run_id': 'forged'}], prov) == ['verify']


def test_handoff_api_requires_donor_and_target_and_forwards_policy(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as api
    app = FastAPI(); app.include_router(api.router); client = TestClient(app)
    url = '/api/stage-comparison/sessions/s/pairs/p/production/run'
    body = {'input_mode': 'DOCUMENT', 'resume_model_policy': 'remaining_stages'}
    assert client.post(url, json=body).status_code == 400
    assert client.post(url, json={**body, 'resume_from_run_id': 'a' * 32}).status_code == 400
    assert client.post(url, json={**body, 'model_profile': 'astra'}).status_code == 400
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MODEL_CHOICES', 'astra,opus55')
    monkeypatch.setattr(resume, 'load_donor', lambda *args: None)
    captured = {}
    def run(*args, **kwargs):
        captured.update(kwargs); return {'status': 'REVIEW'}
    monkeypatch.setattr(api.production, 'run_production_comparison', run)
    assert client.post(url, json={**body, 'resume_from_run_id': 'a' * 32, 'model_profile': 'astra'}).status_code == 200
    assert captured['resume_model_policy'] == 'remaining_stages'


@pytest.mark.parametrize('wrong_verifier', [False, True])
def test_handoff_verification_uses_astra_and_rejects_other_models(env, monkeypatch, wrong_verifier):
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MODEL_CHOICES', 'astra,opus55')
    handlers = gf.fake_handlers()
    def mining(**kwargs):
        answer = handlers['MINING'](**kwargs)
        for card in answer['projectchanges']:
            card['changed_parameters'][0]['new_value'] = '99999'
        return answer
    set_test_provider(Receipted(handlers={**handlers, 'MINING': mining, 'DEDUPE': lambda **_: {
        'pair': 'wrong', 'decisions': [], 'notes': []}}))
    donor = _run(env['session_id'], model_profile='opus55')
    assert donor['reason_code'] == 'dedupe_failed'
    def verification(**kwargs):
        return {'pair': gf.PAIR_ID, 'results': [
            {'work_item_id': item['work_item_id'], 'verdict': 'CORRECTED',
             'old_value': '1000', 'new_value': '1200', 'unit': 'м3/ч', 'explanation': 'Source value',
             'evidence_refs': [{k: source[k] for k in ['side', 'physical_page', 'block_id']}
                               for source in item['sources']]}
            for item in kwargs['data']['items']]}
    class Verifier(Receipted):
        def complete(self, **kwargs):
            answer = super().complete(**kwargs)
            if wrong_verifier and kwargs['stage'] == 'SOURCE_VERIFICATION':
                self.last_transport['model'] = 'wrong'
            return answer
    fake = Verifier(handlers={**handlers, 'SOURCE_VERIFICATION': verification})
    set_test_provider(fake)
    state = _resume(env, donor, resume_model_policy='remaining_stages')
    assert [c['stage'] for c in fake.calls] == ['DEDUPE', 'SOURCE_VERIFICATION']
    if wrong_verifier:
        assert state['reason_code'] == 'v3_model_mixing', state
        assert _stored(env['session_id'], 'project_change_v3_result') is None
    else:
        assert state['status'] in {'REVIEW', 'COMPLETED'}, state
        result = _stored(env['session_id'], 'project_change_v3_result')
        assert result['source_verification']['accepted'] == 1
        new_calls = [c for c in result['provenance']['transport_calls'] if not c.get('resumed_from_run_id')]
        assert [c['model'] for c in new_calls] == ['gpt-6-astra', 'gpt-6-astra']


def test_handoff_still_rejects_a_changed_graphic_crop(env, donor):
    from pathlib import Path
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, donor['run_id'])
    structure = json.loads((directory / 'project_change_v3/DOCUMENT_STRUCTURE.json').read_text())
    crop = next(b['graphic_crop_ref'] for p in structure for b in p['blocks'] if b.get('graphic_crop_ref'))
    Path(crop).write_bytes(b'changed crop')
    fake = Receipted(handlers=gf.fake_handlers()); set_test_provider(fake)
    state = _resume(env, donor, resume_model_policy='remaining_stages')
    assert state['reason_code'] == 'resume_refused' and not fake.calls


def test_catalog_and_provenance_name_both_models():
    from backend.app.services.project_change_catalog.catalog import provenance_model_display
    from backend.app.services.project_change_v3.provenance import provenance_lines
    prov = {'model': 'gpt-6-astra', 'model_handoff': {
        'source_model': {'model': 'claude-opus-5-5'}, 'remaining_model': {'model': 'gpt-6-astra'}}}
    assert provenance_model_display(prov) == 'Claude Opus 5.5 → GPT-6 Astra (досборка)'
    assert 'claude-opus-5-5' in provenance_lines(prov)[0] and 'gpt-6-astra' in provenance_lines(prov)[0]
