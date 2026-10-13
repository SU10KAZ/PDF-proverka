"""Partial Miner recovery: synthetic documents and fake providers, no model requests."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import contracts, engine, resume, run_storage
from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError, set_test_provider
from backend.app.services.stage_comparison.production_orchestrator import get_production_state
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env, _stored  # noqa: F401
from backend.tests.project_change_v3.test_miner_parallel import handlers
from backend.tests.project_change_v3.test_resume_and_dedupe_repair import _run


def _snapshot(directory):
    return {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in directory.rglob('*') if p.is_file()}


def _checkpoint(state):
    return json.loads(Path(state['provenance']['miner_checkpoint']['path']).read_text())


def _handlers():
    base = handlers(10)
    mapping = base['MAPPING']

    def wide_last(**kwargs):
        answer = mapping(**kwargs)
        answer['regions'][-1].update(old_pages=[1, 2], new_pages=[1, 2])
        return answer

    base['MAPPING'] = wide_last
    return base


@pytest.fixture
def partial(env, monkeypatch):
    monkeypatch.setattr(contracts, 'DEFAULT_PROFILE_KEY', 'astra')
    monkeypatch.setenv(engine.UNMATCHED_REVIEW_ENV, '0')
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    base = _handlers()
    mining = base['MINING']

    def fail_last(**kwargs):
        if kwargs['data']['frozen_region']['region_id'] == 'P010':
            raise ProviderError('CONTEXT_COMPACTED', 'synthetic context compaction')
        return mining(**kwargs)

    set_test_provider(FakeProvider(handlers={**base, 'MINING': fail_last}))
    state = _run(env['session_id'])
    assert state['reason_code'] == 'provider_context_compacted', state
    assert state['pipeline_current'] == 'MINING'
    assert len(_checkpoint(state)['regions']) == 9
    return state


def _remaining(monkeypatch, *, fail_at=None, fail_dedupe=False):
    # Only the unfinished wide region may reach planning. Saved regions remain
    # usable even if the new envelope policy would reject their original input.
    def fits(_pair, region, _work, _contract):
        assert region['region_id'].startswith('P010'), 'replanning an accepted region'
        return len(region['old_pages']) == len(region['new_pages']) == 1

    monkeypatch.setattr(engine, '_miner_fits', fits)

    def mining(**kwargs):
        rid = kwargs['data']['frozen_region']['region_id']
        if rid == fail_at:
            raise ProviderError('provider_timeout', 'synthetic failure')
        return {'pair': gf.PAIR_ID, 'region_id': rid, 'projectchanges': [],
                'unresolved_hints': [], 'coverage_notes': []}

    base = {**_handlers(), 'MINING': mining}
    if fail_dedupe:
        base['DEDUPE'] = lambda **_: {'pair': 'wrong', 'decisions': [], 'notes': []}
    fake = FakeProvider(handlers=base)
    set_test_provider(fake)
    return fake


def test_resume_keeps_nine_answers_and_splits_only_the_missing_region(env, partial, monkeypatch):
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, partial['run_id'])
    before = _snapshot(directory)
    assert get_production_state(env['session_id'], gf.PAIR_ID)['resume_available'] is True
    fake = _remaining(monkeypatch)
    state = _run(env['session_id'], resume_from_run_id=partial['run_id'])
    assert state['status'] in {'REVIEW', 'COMPLETED'}, state.get('message')
    assert [c['stage'] for c in fake.calls] == ['MINING'] * 4 + ['DEDUPE']
    assert [c['call_id'].removeprefix(gf.PAIR_ID + '_') for c in fake.calls[:4]] == [
        'P010.1', 'P010.2', 'P010.3', 'P010.4']
    cp = _checkpoint(state)
    assert len(cp['regions']) == cp['regions_total'] == 13
    assert [r['ordinal'] for r in cp['regions']] == list(range(1, 14))
    original_work = str(directory / 'project_change_v3')
    new_work = str(Path(state['provenance']['miner_checkpoint']['path']).parent.parent)
    expected_answers = json.loads(json.dumps([r['result'] for r in _checkpoint(partial)['regions']])
                                  .replace(original_work, new_work))
    assert [r['result'] for r in cp['regions'][:9]] == expected_answers
    assert all(r['source_run_id'] == partial['run_id'] for r in cp['regions'][:9])
    assert cp['run_id'] == state['run_id']
    assert state['pipeline_stages']['MINING']['processed'] == 13
    assert state['pipeline_stages']['MAPPING']['status'] == 'REUSED'
    assert state['provenance']['resumed_from']['saved_regions'] == 9
    assert state['provenance']['resumed_from']['partial_mining'] is True
    result = _stored(env['session_id'], 'project_change_v3_result')
    assert len(result['projectchanges']) == 9
    assert _snapshot(directory) == before


def test_second_failure_keeps_both_reused_and_new_answers_for_another_resume(env, partial, monkeypatch):
    fake = _remaining(monkeypatch, fail_at='P010.2')
    second = _run(env['session_id'], resume_from_run_id=partial['run_id'])
    assert second['reason_code'] == 'provider_timeout', second
    assert len(_checkpoint(second)['regions']) == 10
    assert _stored(env['session_id'], 'project_change_v3_result') is None
    assert resume.load_donor(env['session_id'], gf.PAIR_ID, second['run_id']).partial_mining
    fake = _remaining(monkeypatch)
    third = _run(env['session_id'], resume_from_run_id=second['run_id'])
    assert third['status'] in {'REVIEW', 'COMPLETED'}, third
    assert [c['call_id'].removeprefix(gf.PAIR_ID + '_') for c in fake.calls if c['stage'] == 'MINING'] == [
        'P010.2', 'P010.3', 'P010.4']
    assert len(_checkpoint(third)['regions']) == 13


def test_tail_failure_after_partial_resume_can_resume_with_no_more_mining(env, partial, monkeypatch):
    _remaining(monkeypatch, fail_dedupe=True)
    second = _run(env['session_id'], resume_from_run_id=partial['run_id'])
    assert second['reason_code'] == 'dedupe_failed', second
    assert not resume.load_donor(env['session_id'], gf.PAIR_ID, second['run_id']).partial_mining
    fake = _remaining(monkeypatch)
    third = _run(env['session_id'], resume_from_run_id=second['run_id'])
    assert third['status'] in {'REVIEW', 'COMPLETED'}, third
    assert [c['stage'] for c in fake.calls] == ['DEDUPE']


def test_partial_resume_runs_enabled_unmatched_planning_after_mining(env, partial, monkeypatch):
    monkeypatch.setenv(engine.UNMATCHED_REVIEW_ENV, '1')
    real_plan = engine.build_supplemental_batches
    planned = []

    def plan(**kwargs):
        planned.append(deepcopy(kwargs['semantic_map']))
        return real_plan(**kwargs)

    monkeypatch.setattr(engine, 'build_supplemental_batches', plan)
    _remaining(monkeypatch)
    state = _run(env['session_id'], resume_from_run_id=partial['run_id'])
    assert state['status'] in {'REVIEW', 'COMPLETED'}, state
    assert len(planned) == 1 and len(planned[0]['regions']) == 13
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, state['run_id'])
    supplemental = json.loads((directory / 'project_change_v3_supplemental_results.json').read_text())
    assert supplemental['enabled'] is True
    assert supplemental['limits']['existing_subjects_policy'] == 'frozen_after_mining'


def _change_checkpoint(env, state, mutation, *, update_digest=True):
    p = Path(state['provenance']['miner_checkpoint']['path'])
    cp = json.loads(p.read_text())
    mutation(cp)
    p.write_text(json.dumps(cp))
    if update_digest:
        sp = run_storage.run_dir(env['session_id'], gf.PAIR_ID, state['run_id']) / 'state.json'
        saved = json.loads(sp.read_text())
        saved['provenance']['miner_checkpoint']['sha256'] = hashlib.sha256(p.read_bytes()).hexdigest()
        sp.write_text(json.dumps(saved))


@pytest.mark.parametrize('damage', ['digest', 'duplicate', 'unknown', 'rejected', 'ordinal', 'map', 'schema', 'answer'])
def test_damaged_partial_checkpoint_is_refused_before_any_new_call(env, partial, monkeypatch, damage):
    def mutate(cp):
        if damage == 'duplicate':
            cp['regions'].append(deepcopy(cp['regions'][0]))
        elif damage == 'unknown':
            cp['regions'][0]['region_id'] = 'UNKNOWN'
        elif damage == 'rejected':
            cp['regions'][0]['validation'] = 'REJECTED'
        elif damage == 'ordinal':
            cp['regions'][0]['ordinal'] = 8
        elif damage == 'map':
            cp['semantic_map_sha256'] = 'wrong'
        elif damage == 'schema':
            cp['miner_schema_sha256'] = 'wrong'
        else:
            cp['regions'][0]['result']['projectchanges'][0]['old_pages'] = [999]

    _change_checkpoint(env, partial, mutate, update_digest=damage != 'digest')
    fake = _remaining(monkeypatch)
    state = _run(env['session_id'], resume_from_run_id=partial['run_id'])
    assert state['reason_code'] == 'resume_refused', state
    assert fake.calls == []


@pytest.mark.parametrize('damage', ['pdf', 'prompt'])
def test_partial_resume_refuses_changed_sources_or_prompt(env, partial, monkeypatch, damage):
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, partial['run_id'])
    p = directory / ('project_change_v3_source_manifest.json' if damage == 'pdf' else 'state.json')
    data = json.loads(p.read_text())
    if damage == 'pdf':
        data['old_pdf_sha256'] = 'changed'
    else:
        data['provenance']['miner_prompt_sha256'] = 'changed'
    p.write_text(json.dumps(data))
    fake = _remaining(monkeypatch)
    state = _run(env['session_id'], resume_from_run_id=partial['run_id'])
    assert state['reason_code'] == 'resume_refused' and not fake.calls


def test_partial_resume_refuses_model_handoff(env, partial, monkeypatch):
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MODEL_CHOICES', 'astra,opus55')
    fake = _remaining(monkeypatch)
    state = _run(env['session_id'], resume_from_run_id=partial['run_id'],
                 model_profile='opus55', resume_model_policy='remaining_stages')
    assert state['reason_code'] == 'resume_refused' and not fake.calls
    assert 'той же моделью' in state['message']


def test_missing_region_that_cannot_fit_stops_before_new_calls(env, partial, monkeypatch):
    fake = _remaining(monkeypatch)
    monkeypatch.setattr(engine, '_miner_fits', lambda *_: False)
    state = _run(env['session_id'], resume_from_run_id=partial['run_id'])
    assert state['reason_code'] == 'miner_envelope_exceeded' and not fake.calls
    assert state['pipeline_current'] == 'MINING'
    assert resume.resume_available(env['session_id'], gf.PAIR_ID, state['run_id'])
    assert resume.load_donor(env['session_id'], gf.PAIR_ID, state['run_id']).run_id == partial['run_id']
    fake = _remaining(monkeypatch)
    completed = _run(env['session_id'], resume_from_run_id=state['run_id'])
    assert completed['status'] in {'REVIEW', 'COMPLETED'}, completed
    assert completed['provenance']['resumed_from']['run_id'] == partial['run_id']
    assert [c['stage'] for c in fake.calls] == ['MINING'] * 4 + ['DEDUPE']


def test_resume_rejects_a_cycle_in_planning_failure_ancestry(env, partial, monkeypatch):
    _remaining(monkeypatch)
    monkeypatch.setattr(engine, '_miner_fits', lambda *_: False)
    state = _run(env['session_id'], resume_from_run_id=partial['run_id'])
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, state['run_id'])
    saved = json.loads((directory / 'state.json').read_text())
    saved['provenance']['resumed_from']['run_id'] = state['run_id']
    (directory / 'state.json').write_text(json.dumps(saved))
    assert resume.resume_available(env['session_id'], gf.PAIR_ID, state['run_id']) is False
    with pytest.raises(resume.ResumeRefused, match='цепочка'):
        resume.load_donor(env['session_id'], gf.PAIR_ID, state['run_id'])


def test_reused_checkpoint_write_failure_stops_before_new_calls(env, partial, monkeypatch):
    fake = _remaining(monkeypatch)

    def fail_write(*_):
        raise OSError('synthetic disk failure')

    monkeypatch.setattr(engine, 'write_miner_checkpoint', fail_write)
    state = _run(env['session_id'], resume_from_run_id=partial['run_id'])
    assert state['reason_code'] == 'miner_checkpoint_persistence_failed' and not fake.calls
