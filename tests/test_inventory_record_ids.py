from copy import deepcopy
from backend.app.pipeline.stages.text_analysis.inventory_enrichment import merge_inventory_analysis
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry


def test_colliding_records_survive_but_ambiguous_receipts_are_not_inherited():
    objects = [{'id': 'same', 'identity': 'C2321', 'scope': scope,
                'kind': 'branch', 'source_refs': ['graphic']} for scope in ('QF3.2.1', 'QF3.21')]
    payload = {'object_registry': objects, 'object_operation_results': [
        {'object_id': 'same', 'operation': 'quantity_balance', 'status': 'checked',
         'reason': 'claimed', 'source_refs': ['graphic']}],
        'object_inventory_coverage': [{'block_id': 'graphic', 'object_ids': ['same'],
                                      'status': 'inventoried', 'reason': 'claimed'}]}
    original = deepcopy(payload)
    base = {'text_findings': [{'id': 'T-001', 'finding': 'Useful classic finding'}]}
    packet = {'packet_id': 'p', 'block_ids': ['graphic'], 'payload': payload}
    merged = merge_inventory_analysis(base, [packet], {'graphic'})
    assert payload == original
    assert merged['text_findings'] == base['text_findings']
    assert len(merged['object_registry']) == 2
    assert len({o['id'] for o in merged['object_registry']}) == 2
    assert {o['scope'] for o in merged['object_registry']} == {'QF3.2.1', 'QF3.21'}
    assert all(not o['record_identity']['physical_identity_resolved'] for o in merged['object_registry'])
    coverage = reconcile_object_registry(merged, {'graphic'})
    from backend.app.pipeline.stages.text_analysis.object_registry import OPERATIONS
    assert coverage['unresolved_operations'] == 2 * len(OPERATIONS)
    assert len(coverage['unmatched_operations']) == 1
    assert coverage['unresolved_source_blocks'] == 1
    report, = merged['inventory_enrichment']['packets']
    assert report['raw_payload'] == original
    assert len(report['record_identity_repairs']) == 2
    assert merge_inventory_analysis(base, [packet], {'graphic'}) == merged


def test_equal_duplicate_rows_are_records_not_proven_distinct_instances():
    from backend.app.pipeline.stages.text_analysis.inventory_record_ids import separate_colliding_record_ids
    obj = {'id': 'same', 'identity': 'mark', 'scope': 'view', 'kind': 'device', 'source_refs': ['b']}
    data, repairs = separate_colliding_record_ids({'object_registry': [obj, obj]})
    assert len(repairs) == 2
    assert data['object_registry'][0]['id'] != data['object_registry'][1]['id']
    assert all(not r['physical_identity_resolved'] for r in repairs)
    single, repairs = separate_colliding_record_ids({'object_registry': [obj]})
    assert single == {'object_registry': [obj]} and repairs == []
