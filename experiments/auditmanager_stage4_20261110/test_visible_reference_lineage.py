import pytest
from experiments.auditmanager_stage4_20261110.audit_visible_references import audit_lineage


def row(display, source, description=''):
    return {'id': display, 'source_stage': 'stage', 'source_finding_ids': [source], 'description': description}


def test_existing_but_wrong_target_is_detected():
    before = [row('F-001', 'a', 'См. F-003.'), row('F-003', 'b')]
    after = [row('F-001', 'a', 'См. F-003.'), row('F-002', 'b'), row('F-003', 'c')]
    result = audit_lineage(before, after)['suspected_stale_references']
    assert len(result) == 1
    assert result[0]['current_reference_exists']
    assert not result[0]['current_reference_has_same_lineage']
    assert result[0]['expected_target_by_lineage'] == 'F-002'


def test_valid_and_corrected_references_are_not_flagged():
    before = [row('F-001', 'a', 'См. F-003.'), row('F-003', 'b')]
    assert not audit_lineage(before, before)['suspected_stale_references']
    after = [row('F-001', 'a', 'См. F-002.'), row('F-002', 'b')]
    assert not audit_lineage(before, after)['suspected_stale_references']


def test_ambiguous_lineage_is_not_guessed():
    before = [row('F-001', 'a', 'См. F-003.'), row('F-003', 'b')]
    after = [row('F-001', 'a', 'См. F-003.'), row('F-002', 'b'), row('F-004', 'b')]
    result = audit_lineage(before, after)
    assert not result['suspected_stale_references']
    assert result['unresolved_references'][0]['reason'] == 'target_lineage_missing_or_ambiguous'


def test_self_reference_counts_exact_tokens():
    before = [row('F-003', 'a', 'F-003; F-003; F-0030')]
    after = [row('F-002', 'a', 'F-003; F-003; F-0030')]
    item = audit_lineage(before, after)['suspected_stale_references'][0]
    assert item['self_reference_by_lineage'] and item['occurrences'] == 2


def test_duplicate_display_ids_rejected():
    with pytest.raises(ValueError, match='Duplicate IDs'):
        audit_lineage([row('F-001', 'a'), row('F-001', 'b')], [])
