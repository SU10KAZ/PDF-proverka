"""Experimental mode contract tests. Synthetic documents; zero external calls."""
from copy import deepcopy
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import engine, run_storage, subject_contracts as sc
from backend.app.services.project_change_v3 import subject_search as ss
from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError, set_test_provider
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env  # noqa: F401


class SubjectFake(FakeProvider):
    def complete(self, **request):
        self.calls.append(deepcopy(request))
        return self.handlers[request['stage']](**request)


def catalog(pages):
    evidence = [{"side": p['side'], "physical_page": p['physical_page'], 'block_id': b['block_id'],
                 'block_type': b['modality'], 'relevance': 'Синтетический предмет'} for p in pages for b in p['blocks']]
    return {'observations': [{'local_id': 'one', 'name': 'Установка', 'aliases': ['П1'],
        'function': 'Приток', 'locations': ['Помещение'], 'source_role': 'description',
        'physical_quantity': 2, 'working_quantity': None, 'reserve_quantity': None,
        'parameters': [], 'evidence': evidence, 'uncertainties': []}], 'other_blocks': []}


def handlers(*, fail_phase=None):
    def mapping(**request):
        data, prompt = request['data'], request['prompt']
        if prompt == sc.CATALOG_PROMPT:
            if fail_phase == 'catalog_new' and data['pages'][0]['side'] == 'NEW':
                raise ProviderError('TEST_STOP', 'synthetic stopped run')
            return catalog(data['pages'])
        if prompt == sc.ROUTE_PROMPT:
            return {'links': [{'old_ids': [r['id'] for r in data['old']],
                               'new_ids': [r['id'] for r in data['new']], 'rationale': 'Та же функция'}],
                    'unresolved_ids': []}
        if prompt == sc.SEARCH_PROMPT:
            return {'queries': [], 'unresolved': []}
        pages = data['pages']
        return {'pair': gf.PAIR_ID, 'regions': [{
            'region_id': 'R', 'old_pages': [p['physical_page'] for p in pages if p['side'] == 'OLD'],
            'new_pages': [p['physical_page'] for p in pages if p['side'] == 'NEW'],
            'engineering_domain': 'Вентиляция', 'scope': 'Установка', 'locations': [],
            'reason_for_correspondence': 'Назначение', 'important_text_blocks': [],
            'important_table_blocks': [], 'important_graphic_blocks': [], 'confidence': 0.8}],
            'unmatched_old': [], 'unmatched_new': [], 'coverage_notes': []}

    def mining(**request):
        answer = gf.fake_handlers()['MINING'](**request)
        answer['region_id'] = request['data']['frozen_region']['region_id']
        return answer
    return {**gf.fake_handlers(), 'MAPPING': mapping, 'MINING': mining}


def run(env, **kwargs):
    return engine.run_v3_pipeline(session_id=env['session_id'], pair_id=gf.PAIR_ID,
                                  comparison_mode='subjects', **kwargs)


def artifact(env, state, name):
    return run_storage.read(run_storage.run_dir(env['session_id'], gf.PAIR_ID, state['run_id']) / (name + '.json'))


def test_subject_mode_reads_sources_and_freezes_separate_result(env, monkeypatch):
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    fake = SubjectFake(handlers=handlers())
    set_test_provider(fake)
    state = run(env)
    assert state['reason_code'] == 'v3_completed', state
    assert state['status'] == 'REVIEW'
    assert state['provenance']['comparison_mode'] == 'subjects'
    ledger = artifact(env, state, ss.ARTIFACT)
    assert ledger['summary']['catalog_pages'] == 4
    assert ledger['summary']['observations'] == 2
    assert ledger['summary']['page_pairs_reviewed'] == 4
    assert ledger['semantic_completeness_proven'] is False
    assert all(row['working_quantity'] is None for row in ledger['catalog'])
    assert artifact(env, state, 'project_change_v3_result')['projectchanges'][0]['old_state'] == '1000 м3/ч'
    assert sc.AUTHORITY in next(c['prompt'] for c in fake.calls if c['stage'] == 'MINING')
    mapping = [c for c in fake.calls if c['prompt'] == engine.MAPPER_PROMPT]
    assert len(mapping) == 1 and len(mapping[0]['data']['pages']) == 4
    assert mapping[0]['images']  # originals, including graphics, reach detailed Mapper
    run_storage.validate(env['session_id'], gf.PAIR_ID, state['run_id'])


def test_stopped_catalog_resumes_without_repeating_accepted_call(env, monkeypatch):
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    set_test_provider(SubjectFake(handlers=handlers(fail_phase='catalog_new')))
    stopped = run(env)
    assert stopped['reason_code'] == 'TEST_STOP', stopped
    from backend.app.services.project_change_v3 import resume
    assert resume.resume_available(env['session_id'], gf.PAIR_ID, stopped['run_id'])
    assert resume.resume_options(env['session_id'], gf.PAIR_ID, stopped['run_id'])['mode'] == 'subject_search'
    fake = SubjectFake(handlers=handlers())
    set_test_provider(fake)
    completed = run(env, resume_from_run_id=stopped['run_id'])
    assert completed['reason_code'] == 'v3_completed', completed
    calls = [c for c in fake.calls if c['prompt'] == sc.CATALOG_PROMPT]
    assert len(calls) == 1 and calls[0]['data']['pages'][0]['side'] == 'NEW'
    assert completed['provenance']['subject_search']['reused_requests'] == 1
    assert artifact(env, stopped, ss.CHECKPOINT)['entries'][0]['accepted']


@pytest.mark.parametrize('mutation', ['text', 'checkpoint', 'model', 'mode'])
def test_recovery_refuses_incompatible_inputs_before_inference(env, monkeypatch, mutation):
    set_test_provider(SubjectFake(handlers=handlers(fail_phase='catalog_new')))
    stopped = run(env)
    assert stopped['reason_code'] == 'TEST_STOP'
    kwargs = {'resume_from_run_id': stopped['run_id']}
    if mutation == 'text':
        path = Path(env['left']['pdf_path']).with_suffix('.md')
        path.write_text(path.read_text() + '\nChanged authoritative MD\n')
    elif mutation == 'checkpoint':
        path = run_storage.run_dir(env['session_id'], gf.PAIR_ID, stopped['run_id']) / (ss.CHECKPOINT + '.json')
        value = run_storage.read(path)
        value['entries'][0]['answer']['observations'][0]['name'] = 'tampered'
        run_storage.atomic(path, value)
    elif mutation == 'model':
        monkeypatch.setenv('PROJECT_COMPARISON_V3_MODEL_CHOICES', 'astra,opus55')
        kwargs['model_profile'] = 'opus55'
    fake = SubjectFake(handlers=handlers())
    set_test_provider(fake)
    if mutation == 'mode':
        with pytest.raises(ValueError, match='comparison mode'):
            engine.run_v3_pipeline(session_id=env['session_id'], pair_id=gf.PAIR_ID,
                                   comparison_mode='standard', **kwargs)
    else:
        state = run(env, **kwargs)
        assert state['status'] == 'FAILED', state
    assert not fake.calls


def test_catalog_requires_all_blocks_and_valid_evidence():
    pages = [{'side': 'OLD', 'physical_page': 1, 'blocks': [
        {'block_id': 'a', 'modality': 'TEXT'}, {'block_id': 'b', 'modality': 'GRAPHIC'}]}]
    answer = catalog(pages)
    ss.validate_catalog(answer, pages)
    missing = deepcopy(answer)
    missing['observations'][0]['evidence'].pop()
    with pytest.raises(ValueError, match='unaccounted'):
        ss.validate_catalog(missing, pages)
    forged = deepcopy(answer)
    forged['observations'][0]['evidence'][0]['physical_page'] = 900
    with pytest.raises(ValueError, match='unavailable'):
        ss.validate_catalog(forged, pages)


def test_search_normalizes_aliases_only_and_keeps_all_unseen_hits():
    original = 'B17-3 осн/рез, базовый L=240'
    pages = [{'side': 'NEW', 'physical_page': p, 'blocks': [{'structured_md': original}]} for p in range(1, 5)]
    index = ss.SourceIndex(pages)
    found = index.search(['В17-3'], limit=2)
    assert found == {'hits': [['NEW', 1], ['NEW', 2]], 'total_hits': 4, 'truncated': True}
    assert pages[0]['blocks'][0]['structured_md'] == original


def test_route_cannot_drop_subjects_or_invent_ids():
    old, new = [{'id': 'o'}], [{'id': 'n'}, {'id': 'n2'}]
    with pytest.raises(ValueError, match='omitted'):
        ss.validate_route({'links': [{'old_ids': ['o'], 'new_ids': ['n'], 'rationale': ''}],
                           'unresolved_ids': []}, old, new)
    ss.validate_route({'links': [{'old_ids': ['o'], 'new_ids': ['n', 'n2'], 'rationale': ''}],
                       'unresolved_ids': []}, old, new)


def test_api_mode_is_independent_of_depth_and_model():
    from backend.app.api.routers.stage_comparison import ProductionRunRequest, ProductionBatchRequest
    from pydantic import ValidationError
    assert ProductionRunRequest(input_mode='DOCUMENT').comparison_mode is None
    value = ProductionRunRequest(input_mode='DOCUMENT', comparison_mode='subjects', ai_mode='STANDARD')
    assert value.ai_mode == 'STANDARD' and value.model_profile is None
    with pytest.raises(ValidationError):
        ProductionRunRequest(input_mode='DOCUMENT', comparison_mode='large')
    assert ProductionBatchRequest(pairs=[{'left_pdf': 'x', 'right_pdf': 'y'}]).comparison_mode == 'standard'


def test_http_resume_inherits_experimental_mode(env, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    app = FastAPI()
    app.include_router(stage_comparison.router)
    client = TestClient(app)
    url = f"/api/stage-comparison/sessions/{env['session_id']}/pairs/{gf.PAIR_ID}/production/run"
    set_test_provider(SubjectFake(handlers=handlers(fail_phase='catalog_new')))
    response = client.post(url, json={'input_mode': 'DOCUMENT', 'comparison_mode': 'subjects'})
    assert response.status_code == 200
    stopped = response.json()
    fake = SubjectFake(handlers=handlers())
    set_test_provider(fake)
    response = client.post(url, json={'input_mode': 'DOCUMENT', 'resume_from_run_id': stopped['run_id']})
    assert response.status_code == 200, response.text
    assert response.json()['reason_code'] == 'v3_completed', response.text
    assert response.json()['provenance']['comparison_mode'] == 'subjects'
    assert not any(c['prompt'] == sc.CATALOG_PROMPT and c['data']['pages'][0]['side'] == 'OLD' for c in fake.calls)


def test_parallel_catalog_finishes_deterministically_and_drains_failure(env, monkeypatch):
    import threading
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MAPPER_CONCURRENCY', '2')
    base = handlers()
    mapping = base['MAPPING']
    gate = threading.Barrier(2)
    def concurrent(**request):
        if request['prompt'] == sc.CATALOG_PROMPT:
            gate.wait(timeout=10)
            if request['data']['pages'][0]['side'] == 'OLD':
                raise ProviderError('TEST_STOP', 'synthetic error')
        return mapping(**request)
    base['MAPPING'] = concurrent
    fake = SubjectFake(handlers=base)
    set_test_provider(fake)
    stopped = run(env)
    assert stopped['reason_code'] == 'TEST_STOP', stopped
    checkpoint = artifact(env, stopped, ss.CHECKPOINT)
    assert len(checkpoint['entries']) == 1
    assert checkpoint['entries'][0]['answer']['observations'][0]['evidence'][0]['side'] == 'NEW'
    assert len(checkpoint['attempts']) == 2
    assert all(call['prompt'] == sc.CATALOG_PROMPT for call in fake.calls)
    set_test_provider(SubjectFake(handlers=handlers()))
    completed = run(env, resume_from_run_id=stopped['run_id'])
    assert completed['reason_code'] == 'v3_completed', completed
    ledger = artifact(env, completed, ss.ARTIFACT)
    assert [r['side'] for r in ledger['catalog']] == ['OLD', 'NEW']


def test_verifier_receives_other_source_pages_and_can_report_conflict(env, monkeypatch):
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '1')
    base = handlers()
    def verify(**request):
        [item] = request['data']['items']
        # Page 2 was not cited in the initial ProjectChange but belongs to its source group.
        assert any(r['physical_page'] == 2 and r['side'] == 'NEW' for r in item['sources'])
        assert request['images']
        return {'pair': gf.PAIR_ID, 'results': [{'work_item_id': item['work_item_id'],
            'verdict': 'CONFLICT', 'old_value': '1000', 'new_value': '', 'unit': 'м3/ч',
            'explanation': 'Synthetic contradictory source',
            'evidence_refs': [{'side': 'NEW', 'physical_page': 2, 'block_id': 'n2_text'}]}]}
    base['SOURCE_VERIFICATION'] = verify
    set_test_provider(SubjectFake(handlers=base))
    state = run(env)
    assert state['reason_code'] == 'v3_completed', state
    quality = artifact(env, state, 'project_change_v3_quality')
    assert quality['summary']['source_verification_conflicts'] == 1
    assert artifact(env, state, ss.ARTIFACT)['verification_queries']


def test_search_budgets_record_gap_instead_of_claiming_complete(env, monkeypatch):
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    base = handlers()
    mapping = base['MAPPING']
    def request_more(**request):
        if request['prompt'] == sc.SEARCH_PROMPT:
            return {'queries': [{'terms': ['missing-system'], 'side': 'BOTH', 'purpose': 'Locate source'}] * 13,
                    'unresolved': ['Missing counterpart']}
        return mapping(**request)
    base['MAPPING'] = request_more
    set_test_provider(SubjectFake(handlers=base))
    state = run(env)
    assert state['reason_code'] == 'v3_completed', state
    ledger = artifact(env, state, ss.ARTIFACT)
    reasons = {r['reason'] for r in ledger['gaps']}
    assert {'query_budget', 'search_no_hits', 'unresolved'} <= reasons
    assert ledger['summary']['open_search_gaps'] > 0
    assert state['status'] == 'REVIEW'


def test_oversized_verification_is_pending_without_spending_a_call(env, monkeypatch):
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '1')
    build = engine.build_verification_batches
    def oversized(**kwargs):
        batches = build(**kwargs)
        for batch in batches:
            batch['data']['full_source_test_padding'] = 'X' * 2_500_000
        return batches
    monkeypatch.setattr(engine, 'build_verification_batches', oversized)
    fake = SubjectFake(handlers=handlers())
    set_test_provider(fake)
    state = run(env)
    assert state['reason_code'] == 'v3_completed', state
    assert not any(call['stage'] == 'SOURCE_VERIFICATION' for call in fake.calls)
    result = artifact(env, state, 'project_change_v3_verification_results')
    assert result['batches'][0]['error_code'] == 'verification_envelope_exceeded'
    assert state['quality_source_verification_pending'] == 1


def test_miner_planning_stop_preserves_subject_search(env, monkeypatch):
    from backend.app.services.project_change_v3 import resume
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    fake = SubjectFake(handlers=handlers())
    set_test_provider(fake)
    with monkeypatch.context() as patch:
        patch.setattr(engine, '_miner_measure', lambda *args, **kwargs: (_ for _ in ()).throw(
            engine.mapper_portions_mod.PortioningImpossible('planning stopped')))
        stopped = run(env)
    assert stopped['status'] == 'FAILED'
    assert all(call['stage'] == 'MAPPING' for call in fake.calls)
    assert resume.resume_available(env['session_id'], gf.PAIR_ID, stopped['run_id'])
    fresh = SubjectFake(handlers=handlers())
    set_test_provider(fresh)
    completed = run(env, resume_from_run_id=stopped['run_id'])
    assert completed['reason_code'] == 'v3_completed', completed
    assert all(call['stage'] != 'MAPPING' for call in fresh.calls)
