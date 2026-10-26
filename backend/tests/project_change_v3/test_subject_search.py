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


def test_subject_progress_reports_planned_packets_pages_and_completed_groups(env, monkeypatch):
    from backend.app.services.stage_comparison import production_store
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    save, chunks = production_store.save_artifact, ss.SubjectSearch.chunks
    states = []

    def record(session_id, pair_id, name, value):
        if name == 'state':
            states.append(deepcopy(value))
        return save(session_id, pair_id, name, value)

    def one_page(self, pages, prompt, *args, **kwargs):
        return [[p] for p in pages] if prompt == sc.CATALOG_PROMPT else chunks(self, pages, prompt, *args, **kwargs)

    monkeypatch.setattr(production_store, 'save_artifact', record)
    monkeypatch.setattr(ss.SubjectSearch, 'chunks', one_page)
    set_test_provider(SubjectFake(handlers=handlers()))
    state = run(env)
    assert state['reason_code'] == 'v3_completed', state
    catalog_rows = [s['pipeline_stages']['MAPPING'] for s in states
                    if (s.get('pipeline_current') == 'MAPPING' and s['message'].startswith('Каталог:'))]
    assert {(r['processed'], r['total'], r['unit']) for r in catalog_rows} == {
        (i, 4, 'batch') for i in range(5)}
    assert any('Страницы: 4 из 4.' in r['message'] for r in catalog_rows)
    assert all(r['processed'] <= r['total'] for r in catalog_rows)
    route_rows = [s['pipeline_stages']['MAPPING'] for s in states
                  if s.get('pipeline_current') == 'MAPPING' and s['message'].startswith('Поиск связей:')]
    assert {(r['processed'], r['total']) for r in route_rows} == {(0, 1), (1, 1)}
    group_rows = [s['pipeline_stages']['MAPPING'] for s in states
                  if s.get('pipeline_current') == 'MAPPING' and s['message'].startswith('Группы предметов:')]
    assert {(r['processed'], r['total'], r['unit']) for r in group_rows} == {(0, 1, 'group'), (1, 1, 'group')}
    ledger = artifact(env, state, ss.ARTIFACT)
    assert ledger['progress']['processed'] == ledger['progress']['total'] == 1
    assert ledger['progress']['unit'] == 'group'


def test_stopped_catalog_resumes_without_repeating_accepted_call(env, monkeypatch):
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    set_test_provider(SubjectFake(handlers=handlers(fail_phase='catalog_new')))
    stopped = run(env)
    assert stopped['reason_code'] == 'TEST_STOP', stopped
    assert stopped['pipeline_stages']['MAPPING']['processed'] == 1
    assert stopped['pipeline_stages']['MAPPING']['total'] == 2
    from backend.app.services.project_change_v3 import resume
    assert resume.resume_available(env['session_id'], gf.PAIR_ID, stopped['run_id'])
    assert resume.resume_options(env['session_id'], gf.PAIR_ID, stopped['run_id'])['mode'] == 'subject_search'
    base = handlers()
    mapping = base['MAPPING']

    def check_saved_count(**request):
        if request['prompt'] == sc.CATALOG_PROMPT:
            assert request['data']['pages'][0]['side'] == 'NEW'
            saved = run_storage.read(run_storage.active_dir(env['session_id'], gf.PAIR_ID) / 'state.json')
            assert saved['pipeline_stages']['MAPPING']['processed'] == 1
            assert saved['pipeline_stages']['MAPPING']['total'] == 2
        return mapping(**request)

    base['MAPPING'] = check_saved_count
    fake = SubjectFake(handlers=base)
    set_test_provider(fake)
    completed = run(env, resume_from_run_id=stopped['run_id'])
    assert completed['reason_code'] == 'v3_completed', completed
    calls = [c for c in fake.calls if c['prompt'] == sc.CATALOG_PROMPT]
    assert len(calls) == 1 and calls[0]['data']['pages'][0]['side'] == 'NEW'
    assert completed['provenance']['subject_search']['reused_requests'] == 1
    assert artifact(env, stopped, ss.CHECKPOINT)['entries'][0]['accepted']


def test_subject_handoff_reuses_astra_catalog_and_only_calls_sol_for_missing_work(env, monkeypatch):
    from backend.app.services.project_change_v3 import contracts, resume
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MODEL_CHOICES', 'astra,sol61')
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')

    class Receipted(SubjectFake):
        def complete(self, **request):
            profile = contracts.active_profile()
            self.last_transport = {'provider': profile.provider, 'model': profile.model,
                                   'reasoning': contracts.REASONING}
            return super().complete(**request)

    set_test_provider(Receipted(handlers=handlers(fail_phase='catalog_new')))
    stopped = run(env, model_profile='astra')
    assert stopped['reason_code'] == 'TEST_STOP'
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, stopped['run_id'])
    import hashlib
    before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.iterdir() if p.is_file()}
    assert resume.resume_options(env['session_id'], gf.PAIR_ID, stopped['run_id'])['can_switch_model']

    # A second stop on Sol exercises normal same-model recovery after a handoff.
    fake = Receipted(handlers=handlers(fail_phase='catalog_new'))
    set_test_provider(fake)
    halfway = run(env, model_profile='sol61', resume_from_run_id=stopped['run_id'],
                  resume_model_policy='remaining_stages')
    assert halfway['reason_code'] == 'TEST_STOP', halfway
    cached = artifact(env, halfway, ss.CHECKPOINT)['entries']
    assert len(cached) == 1 and cached[0]['transport_receipt']['model'] == 'gpt-6-astra'
    assert cached[0]['reused_from_key'] == artifact(env, stopped, ss.CHECKPOINT)['entries'][0]['key']
    assert all(c['data']['pages'][0]['side'] == 'NEW' for c in fake.calls)

    fake = Receipted(handlers=handlers())
    set_test_provider(fake)
    completed = run(env, model_profile='sol61', resume_from_run_id=halfway['run_id'])
    assert completed['reason_code'] == 'v3_completed', completed
    catalogs = [c for c in fake.calls if c['prompt'] == sc.CATALOG_PROMPT]
    assert len(catalogs) == 1 and catalogs[0]['data']['pages'][0]['side'] == 'NEW'
    prov = completed['provenance']
    assert prov['model'] == 'gpt-6.1-sol'
    assert prov['model_handoff']['source_model']['model'] == 'gpt-6-astra'
    assert prov['subject_search']['reused_requests'] == 1
    assert all(c['model'] == 'gpt-6.1-sol' for c in prov['transport_calls'])
    entries = artifact(env, completed, ss.CHECKPOINT)['entries']
    assert {e['transport_receipt']['model'] for e in entries} == {'gpt-6-astra', 'gpt-6.1-sol'}
    assert before == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.iterdir() if p.is_file()}


@pytest.mark.parametrize('mutation', ['text', 'reasoning', 'receipt_model'])
def test_subject_handoff_rejects_changed_sources_or_donor_identity(env, monkeypatch, mutation):
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MODEL_CHOICES', 'astra,sol61')
    set_test_provider(SubjectFake(handlers=handlers(fail_phase='catalog_new')))
    stopped = run(env, model_profile='astra')
    if mutation == 'text':
        path = Path(env['left']['pdf_path']).with_suffix('.md')
        path.write_text(path.read_text() + '\nChanged authoritative MD\n')
    else:
        path = run_storage.run_dir(env['session_id'], gf.PAIR_ID, stopped['run_id']) / 'state.json'
        saved = run_storage.read(path)
        saved['provenance']['reasoning' if mutation == 'reasoning' else 'model'] = 'forged'
        run_storage.atomic(path, saved)
    fake = SubjectFake(handlers=handlers())
    set_test_provider(fake)
    state = run(env, model_profile='sol61', resume_from_run_id=stopped['run_id'],
                resume_model_policy='remaining_stages')
    assert state['reason_code'] == 'resume_refused', state
    assert not fake.calls


def test_handoff_saves_later_cached_packet_before_a_new_request_can_fail(env, monkeypatch):
    import threading
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MODEL_CHOICES', 'astra,sol61')
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MAPPER_CONCURRENCY', '2')
    base = handlers()
    original = base['MAPPING']
    new_finished = threading.Event()

    def stop_old(**request):
        if request['prompt'] == sc.CATALOG_PROMPT:
            if request['data']['pages'][0]['side'] == 'OLD':
                assert new_finished.wait(5)
                raise ProviderError('TEST_STOP', 'stop OLD after NEW finishes')
            answer = original(**request)
            new_finished.set()
            return answer
        return original(**request)

    base['MAPPING'] = stop_old
    set_test_provider(SubjectFake(handlers=base))
    stopped = run(env, model_profile='astra')
    assert stopped['reason_code'] == 'TEST_STOP'
    assert len(artifact(env, stopped, ss.CHECKPOINT)['entries']) == 1

    def fail_old(**request):
        assert request['data']['pages'][0]['side'] == 'OLD'
        saved = run_storage.read(run_storage.active_dir(env['session_id'], gf.PAIR_ID) /
                                (ss.CHECKPOINT + '.json'))
        assert len(saved['entries']) == 1  # NEW was replayed before OLD was admitted.
        raise ProviderError('TEST_STOP', 'stop new Sol request')

    fake = SubjectFake(handlers={**handlers(), 'MAPPING': fail_old})
    set_test_provider(fake)
    stopped_again = run(env, model_profile='sol61', resume_from_run_id=stopped['run_id'],
                        resume_model_policy='remaining_stages')
    assert stopped_again['reason_code'] == 'TEST_STOP', stopped_again
    assert len(fake.calls) == 1
    assert len(artifact(env, stopped_again, ss.CHECKPOINT)['entries']) == 1


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


def parameter(evidence):
    return {'name': 'Расход', 'value': '1000', 'unit': 'м3/ч', 'operating_mode': 'Основной',
            'basis': 'Проект', 'scope': 'Установка', 'evidence': deepcopy(evidence)}


@pytest.mark.parametrize('owner', ['observation', 'parameter'])
def test_catalog_schema_rejects_empty_evidence(owner):
    import jsonschema
    answer = catalog([{'side': 'OLD', 'physical_page': 1, 'blocks': [{'block_id': 'a', 'modality': 'TEXT'}]}])
    row = answer['observations'][0]
    row['parameters'] = [parameter(row['evidence'])]
    (row if owner == 'observation' else row['parameters'][0])['evidence'] = []
    with pytest.raises(jsonschema.ValidationError, match='non-empty'):
        ss.validate_schema(answer, sc.CATALOG_SCHEMA)


@pytest.mark.parametrize('failure', ['missing', 'outside', 'provider_schema'])
def test_catalog_corrects_evidence_once_and_preserves_rejected_answer(env, monkeypatch, failure):
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    base = handlers()
    mapping = base['MAPPING']
    catalog_calls = []

    def with_invalid_evidence(**request):
        if not request['prompt'].startswith(sc.CATALOG_PROMPT):
            return mapping(**request)
        catalog_calls.append(deepcopy(request))
        answer = catalog(request['data']['pages'])
        row = answer['observations'][0]
        row['parameters'] = [parameter(row['evidence'][:1])]
        if request['data']['pages'][0]['side'] == 'OLD':
            if sc.CATALOG_REPAIR_PROMPT in request['prompt']:
                checkpoint = run_storage.read(run_storage.active_dir(env['session_id'], gf.PAIR_ID) /
                                              (ss.CHECKPOINT + '.json'))
                assert checkpoint['attempts'][-1]['accepted'] is False
                assert checkpoint['attempts'][-1]['answer'] != answer
            elif failure == 'outside':
                row['evidence'].pop(0)
            else:
                row['parameters'][0]['evidence'] = []
                if failure == 'provider_schema':
                    fake.last_response = answer
                    raise ProviderError('schema_invalid', 'synthetic schema rejection')
        return answer

    base['MAPPING'] = with_invalid_evidence
    fake = SubjectFake(handlers=base)
    set_test_provider(fake)
    state = run(env)
    assert state['reason_code'] == 'v3_completed', state
    first, repair, new = catalog_calls
    assert first['data']['pages'][0]['side'] == repair['data']['pages'][0]['side'] == 'OLD'
    assert new['data']['pages'][0]['side'] == 'NEW'
    assert repair['data']['pages'] == first['data']['pages']
    assert repair['images'] == first['images']
    assert repair['data']['catalog_validation']
    assert repair['call_id'] == first['call_id'] + '_RETRY2'
    checkpoint = artifact(env, state, ss.CHECKPOINT)
    initial, corrected = checkpoint['attempts'][:2]
    assert initial['accepted'] is False and corrected['accepted'] is True
    assert initial['answer_sha256'] == ss.digest(initial['answer'])
    assert initial['request_key'] != corrected['request_key']
    assert corrected['key'] == initial['key']
    summary = artifact(env, state, ss.ARTIFACT)['summary']
    assert summary['catalog_retries'] == 1 and summary['reused_requests'] == 0
    assert summary['new_requests'] == summary['accepted_requests'] + 1


def test_catalog_stops_after_two_invalid_answers_without_inventing_evidence(env):
    base = handlers()
    mapping = base['MAPPING']

    def always_invalid(**request):
        if not request['prompt'].startswith(sc.CATALOG_PROMPT):
            return mapping(**request)
        answer = catalog(request['data']['pages'])
        answer['observations'][0]['parameters'] = [parameter([])]
        return answer

    base['MAPPING'] = always_invalid
    fake = SubjectFake(handlers=base)
    set_test_provider(fake)
    state = run(env)
    assert state['reason_code'] == 'subject_search_failed'
    assert 'после 2 попыток' in state['message']
    assert len(fake.calls) == 2
    assert state['pipeline_stages']['MAPPING']['processed'] == 0
    assert state['pipeline_stages']['MAPPING']['total'] == 2
    checkpoint = artifact(env, state, ss.CHECKPOINT)
    assert not checkpoint['entries']
    assert len(checkpoint['attempts']) == 2
    assert all(not row['accepted'] and not row['answer']['observations'][0]['parameters'][0]['evidence']
               for row in checkpoint['attempts'])
    assert artifact(env, state, 'project_change_v3_result') is None


def test_catalog_never_automatically_retries_an_external_provider(env):
    class ExternalProvider:
        calls = 0

        def complete(self, **request):
            self.calls += 1
            answer = catalog(request['data']['pages'])
            answer['observations'][0]['parameters'] = [parameter([])]
            return answer

    provider = ExternalProvider()
    set_test_provider(provider)
    state = run(env)
    assert state['reason_code'] == 'subject_search_failed'
    assert provider.calls == 1


@pytest.mark.parametrize('stop', ['cancel', 'envelope'])
def test_catalog_correction_rechecks_cancel_and_full_request_size(env, monkeypatch, stop):
    from types import SimpleNamespace
    token = SimpleNamespace(cancelled=False)
    base = handlers()
    mapping = base['MAPPING']
    packet = ss.SubjectSearch.packet

    def measured(self, prompt, *args, **kwargs):
        result = packet(self, prompt, *args, **kwargs)
        return {**result, 'fits': False} if sc.CATALOG_REPAIR_PROMPT in prompt else result

    if stop == 'envelope':
        monkeypatch.setattr(ss.SubjectSearch, 'packet', measured)

    def invalid(**request):
        if request['prompt'] != sc.CATALOG_PROMPT:
            return mapping(**request)
        if stop == 'cancel':
            token.cancelled = True
        answer = catalog(request['data']['pages'])
        answer['observations'][0]['parameters'] = [parameter([])]
        return answer

    base['MAPPING'] = invalid
    fake = SubjectFake(handlers=base)
    set_test_provider(fake)
    state = run(env, cancel_token=token)
    assert state['reason_code'] == ('v3_cancelled' if stop == 'cancel' else 'subject_search_failed')
    assert len(fake.calls) == 1
    checkpoint = artifact(env, state, ss.CHECKPOINT)
    assert len(checkpoint['attempts']) == 1 and not checkpoint['entries']


@pytest.mark.parametrize('mutation', ['none', 'text', 'model', 'contract'])
def test_legacy_catalog_recovery_revalidates_and_keeps_accepted_answers(env, monkeypatch, mutation):
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    # Pin the exact old contract so an unrelated change cannot widen migration.
    assert ss.digest({'version': 'projectchange_subjects/1',
                      'catalog': [sc.LEGACY_CATALOG_PROMPT, sc.LEGACY_CATALOG_SCHEMA],
                      'route': [sc.ROUTE_PROMPT, sc.ROUTE_SCHEMA],
                      'search': [sc.SEARCH_PROMPT, sc.SEARCH_SCHEMA]}) == sc.LEGACY_CONTRACT_SHA256
    with monkeypatch.context() as old:
        old.setattr(sc, 'CATALOG_PROMPT', sc.LEGACY_CATALOG_PROMPT)
        old.setattr(sc, 'CATALOG_SCHEMA', sc.LEGACY_CATALOG_SCHEMA)
        old.setattr(sc, 'CONTRACT_SHA256', '0' * 64 if mutation == 'contract' else sc.LEGACY_CONTRACT_SHA256)
        old.setattr(sc, 'VERSION', 'projectchange_subjects/1')
        set_test_provider(SubjectFake(handlers=handlers(fail_phase='catalog_new')))
        stopped = run(env)
    assert stopped['reason_code'] == 'TEST_STOP'
    old_checkpoint = artifact(env, stopped, ss.CHECKPOINT)
    kwargs = {'resume_from_run_id': stopped['run_id']}
    if mutation == 'text':
        path = Path(env['left']['pdf_path']).with_suffix('.md')
        path.write_text(path.read_text() + '\nChanged authoritative MD\n')
    elif mutation == 'model':
        monkeypatch.setenv('PROJECT_COMPARISON_V3_MODEL_CHOICES', 'astra,opus55')
        kwargs['model_profile'] = 'opus55'
    fake = SubjectFake(handlers=handlers())
    set_test_provider(fake)
    completed = run(env, **kwargs)
    if mutation != 'none':
        assert completed['status'] == 'FAILED', completed
        assert not fake.calls
        return
    assert completed['reason_code'] == 'v3_completed', completed
    assert not any(c['prompt'] == sc.CATALOG_PROMPT and c['data']['pages'][0]['side'] == 'OLD'
                   for c in fake.calls)
    checkpoint = artifact(env, completed, ss.CHECKPOINT)
    assert checkpoint['identity'] != old_checkpoint['identity']
    assert checkpoint['migrated_from_identity'] == old_checkpoint['identity']
    assert checkpoint['entries'][0]['reused_from_key'] == old_checkpoint['entries'][0]['key']
    assert checkpoint['entries'][0]['answer'] == old_checkpoint['entries'][0]['answer']
    assert artifact(env, completed, ss.ARTIFACT)['summary']['reused_requests'] == 1


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
