from copy import deepcopy
from types import SimpleNamespace

from backend.app.services.section_optimization_history_service import build_historical_ideas, read_previous_bundles
from backend.app.services.section_optimization_service import (
    _accepted_decision_matches_row, _accepted_optimizations, _v2_optimization_history,
    _legacy_optimization_history, build_section_optimization,
)

ITEM = {'id': 'OPT-1', 'current': 'Датчик движения ДД-010',
        'proposed': 'Проверить переход на потолочные датчики 360 градусов',
        'spec_items': ['Датчик движения ДД-010'], 'savings_pct': 20}


def bundle(vid, decision='accepted', item=None, **decision_extra):
    return {'version_id': vid, 'optimization': {'items': [deepcopy(item or ITEM)]},
            'expert_review': {'decisions': [{'item_id': (item or ITEM)['id'],
                'item_type': 'optimization', 'decision': decision, **decision_extra}]}}


def current(history=None):
    return {'version_id': 'v003', 'optimization': {'items': []}, 'expert_review': {'decisions': []},
            'optimization_history': {'bundles': history if history is not None else [bundle('v001')], 'warnings': []}}


def row(quantity='50', **extra):
    return {'row_id': 'R1', 'project_id': 'P1', 'version_id': 'v003',
            'name': 'Датчик движения ДД-010', 'quantity': quantity, 'unit': 'шт.', 'page': 23, **extra}


def ideas(data, rows=None):
    return build_historical_ideas('P1', 'Корпус', data, rows if rows is not None else [row()],
                                 _accepted_decision_matches_row, object_id='O1')


def test_history_keeps_current_quantity_and_version_bound_provenance_without_savings():
    result = ideas(current())[0]
    assert result['status'] == 'requires_review'
    assert result['current_rows'][0]['quantity'] == '50'
    assert result['origins'][0]['source_ref'] == 'P1:v001:OPT-1'
    assert result['origins'][0]['item']['savings_pct'] == 20
    assert result['confirmed_savings'] is None
    assert result['implementation_status'] == 'unknown'
    assert result['input_fingerprint'] != ideas(current(), [row('26')])[0]['input_fingerprint']


def test_exact_duplicates_keep_all_origins_but_different_actions_and_reused_ids_do_not_merge():
    other = {**ITEM, 'proposed': 'Изменить схему управления'}
    results = ideas(current([bundle('v001'), bundle('v002'), bundle('v002', item=other)]))
    assert len(results) == 2
    repeated = next(i for i in results if i['proposed'] == ITEM['proposed'])
    assert len(repeated['origins']) == 2
    assert len({o['source_ref'] for o in repeated['origins']}) == 2
    data = current()
    data.update(bundle('v003', 'rejected', item=other))
    assert ideas(data)[0]['status'] == 'requires_review'


def test_current_rejection_prevents_resurrection_and_keeps_reason():
    data = current()
    data.update(bundle('v003', 'rejected', rejection_reason='Не подходит для помещения'))
    result = ideas(data)[0]
    assert result['status'] == 'current_rejected'
    assert result['current_decisions'][0]['decision']['rejection_reason'] == 'Не подходит для помещения'


def test_intermediate_rejection_is_preserved_when_current_analysis_has_no_matching_idea():
    data = current([bundle('v001'), bundle('v002', 'rejected', rejection_reason='Препятствия обзору')])
    result = ideas(data)[0]
    assert result['status'] == 'previously_rejected'
    assert result['past_rejections'][0]['version_id'] == 'v002'


def test_carried_acceptance_requires_review_and_is_not_active_replication_source():
    data = current()
    data.update(bundle('v003', carried_over=True, carried_from_version='v001', carried_from_item_id='OPT-1'))
    assert ideas(data)[0]['status'] == 'carried_requires_review'
    assert _accepted_optimizations({'project_id': 'P1'}, data)[0] == []
    data['expert_review']['decisions'][0]['carried_over'] = False
    assert ideas(data)[0]['status'] == 'current_accepted'
    assert len(_accepted_optimizations({'project_id': 'P1'}, data)[0]) == 1
    assert ideas(data)[0]['implementation_status'] == 'unknown'


def test_missing_or_nonmatching_current_rows_never_prove_implementation():
    assert ideas(current(), [row(project_id='P2')])[0]['status'] == 'subject_not_matched'
    assert ideas(current(), [row(version_id='v001')])[0]['current_rows'] == []
    result = ideas(current(), [row(name='Выключатель одноклавишный IP44')])[0]
    assert result['status'] == 'subject_not_matched'
    assert result['implementation_status'] == 'unknown'
    assert ideas(current(), [])[0]['status'] == 'insufficient_data'


def test_history_does_not_collect_rejected_or_unreviewed_old_items():
    assert ideas(current([bundle('v001', 'rejected'), bundle('v002', 'pending')])) == []


def test_previous_reader_excludes_current_future_and_duplicate_versions_and_reports_missing_data():
    called = []
    def read(vid):
        called.append(vid)
        return {'version_id': vid, 'optimization': None, 'expert_review': {}}
    versions = [{'version_id': vid} for vid in ['v004', 'v002', 'v001', 'v001', 'v003']]
    result = read_previous_bundles(versions, 'v003', read)
    assert called == ['v001', 'v002']
    assert len(result['warnings']) == 2
    assert read_previous_bundles(versions, 'missing', read)['bundles'] == []
    assert called == ['v001', 'v002']


def test_reader_rejects_wrong_version_and_retains_other_sources():
    versions = [{'version_id': v} for v in ['v001', 'v002', 'v003']]
    result = read_previous_bundles(versions, 'v003', lambda vid: bundle('v003' if vid == 'v001' else vid))
    assert [b['version_id'] for b in result['bundles']] == ['v002']
    assert len(result['warnings']) == 1


def test_v2_reader_stays_in_exact_document_and_versions(tmp_path):
    calls = []
    class Adapter:
        def list_versions(self, doc):
            assert doc == tmp_path
            return [{'version_id': v} for v in ['v001', 'v002', 'v003']]
        def read_optimization(self, doc, vid):
            assert doc == tmp_path
            calls.append(('optimization', vid))
            return bundle(vid)['optimization']
        def read_review(self, doc, vid, name):
            assert doc == tmp_path and name == 'expert_review.json'
            calls.append(('review', vid))
            return bundle(vid)['expert_review']
    result = _v2_optimization_history(Adapter(), tmp_path, 'v002')
    assert calls == [('optimization', 'v001'), ('review', 'v001')]
    assert result['bundles'][0]['version_id'] == 'v001'


def test_legacy_reader_uses_each_explicit_version(monkeypatch, tmp_path):
    import json
    from backend.app.services.section_optimization_service import version_service
    output = tmp_path / 'v1' / '_output'
    output.mkdir(parents=True)
    old = bundle('v1')
    (output / 'optimization.json').write_text(json.dumps(old['optimization']))
    (output / 'expert_review.json').write_text(json.dumps(old['expert_review']))
    monkeypatch.setattr(version_service, 'list_versions_for_history', lambda *_: [{'version_id': 'v1'}, {'version_id': 'v2'}])
    def resolve(pid, vid):
        assert (pid, vid) == ('P1', 'v1')
        return {'version_id': vid, 'version_dir': tmp_path / vid, 'output_dir': output}
    monkeypatch.setattr(version_service, 'resolve_project_version_context', resolve)
    result = _legacy_optimization_history('P1', {'project_dir': tmp_path, 'version_id': 'v2'})
    assert result['bundles'][0]['expert_review'] == old['expert_review']


def test_section_pipeline_exposes_history_without_adding_it_to_accepted_or_replication():
    data = current()
    data['md_text'] = '''## СТРАНИЦА 23
| Поз. | Наименование | Тип, марка | Ед. изм. | Количество |
|---|---|---|---|---|
| 1 | Датчик движения ДД-010 | ДД-010 | шт. | 50 |
'''
    projects = [SimpleNamespace(project_id='P1', name='Корпус', section='EOM')]
    payload = build_section_optimization('EOM', object_id='O1', projects=projects, loader=lambda _: data)
    assert payload['meta']['historical_optimizations'] == 1
    assert payload['meta']['accepted_optimizations'] == 0
    assert payload['meta']['replication_candidates'] == 0
    assert payload['historical_optimizations'][0]['current_rows'][0]['quantity'] == '50'
    assert payload['capabilities']['optimization_history'] is True


def test_carryover_provenance_matches_version_alias_without_relying_on_item_number():
    data = current()
    data.update(bundle('v003', item={**ITEM, 'id': 'OPT-99', 'proposed': 'Уточнённая формулировка'},
                       carried_over=True, carried_from_version='v1', carried_from_item_id='OPT-1'))
    assert ideas(data)[0]['status'] == 'carried_requires_review'
    assert ideas(data)[0]['current_decisions'][0]['item_id'] == 'OPT-99'


def test_orphaned_accepted_decision_reports_incomplete_history():
    old = bundle('v001')
    old['optimization']['items'] = []
    result = read_previous_bundles([{'version_id': 'v001'}, {'version_id': 'v002'}], 'v002', lambda _: old)
    assert 'отсутствует текст предложения' in result['warnings'][0]
