from copy import deepcopy
from pathlib import Path
import sys
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from experiments.auditmanager_stage4_20261110.audit_visible_references import audit


def test_detects_historical_target_even_when_current_id_exists_without_mutation():
    data = [
        {'id': 'F-001', 'description': 'Комментарий: сохранено F-002.',
         'recovery_addenda': [{'reported_target_id': 'F-002', 'reason': 'сохранено F-002.'}]},
        {'id': 'F-002', 'description': 'Другая тема'},
    ]
    before = deepcopy(data)
    result = audit(data)
    assert result['affected_historical_findings'] == 1
    assert result['historical_target_mentions'][0]['current_reference_exists'] is True
    assert result['missing_visible_references'] == []
    assert data == before


def test_historical_metadata_alone_is_not_a_visible_reference():
    result = audit([{'id': 'F-001', 'description': 'Текст без ссылки',
                     'recovery_addenda': [{'reported_target_id': 'F-002', 'reason': 'F-002'}]}])
    assert result['historical_target_mentions'] == []


def test_id_boundaries_and_all_visible_fields():
    result = audit([{'id': 'F-001', 'solution': 'См. F-091; XF-092; F-093abc; F-094-1.'}])
    assert result['missing_visible_references'] == [
        {'finding_id': 'F-001', 'field': 'solution', 'reference': 'F-091'}]


def test_current_self_reference_is_not_historical():
    result = audit([{'id': 'F-001', 'description': 'F-001',
                     'recovery_addenda': [{'reported_target_id': 'F-001', 'reason': 'F-001'}]}])
    assert result['historical_target_mentions'] == []


def test_duplicate_final_ids_are_not_silently_resolved():
    with pytest.raises(ValueError, match='Duplicate'):
        audit([{'id': 'F-001'}, {'id': 'F-001'}])
