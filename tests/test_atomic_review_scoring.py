import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from experiments.auditmanager_stage4_20261110.score_atomic_review import prepare, score


def setup_case(tmp_path):
    evaluator = tmp_path / 'evaluator.json'
    findings = tmp_path / 'findings.json'
    evaluator.write_text(json.dumps({
        'targets': [{'target_id': i, 'route': route, 'contexts': [{'candidate_ids': ['B-EOM-F-001']}]} for i, route in [('t','technical'),('q','question'),('n','negative'),('held','negative')]],
        'truth_denominators': {'technical': ['t'], 'question': ['q'], 'negative': ['n']},
        'holds': [{'target_ids': ['held'], 'held_dimensions': ['truth_scoring']},
                  {'target_ids': ['q'], 'held_dimensions': ['report_section_accuracy']}],
    }))
    findings.write_text(json.dumps({'findings': [{'id': 'F-001'}, {'id': 'F-002'}]}))
    return evaluator, findings, prepare(evaluator, findings, 'EOM')


def reviewed(tmp_path):
    evaluator, findings, review = setup_case(tmp_path)
    review.update(reviewer='engineer-test', entire_output_reviewed=True)
    for row in review['targets']:
        row.update(availability='available', coverage='absent', reason='Reviewed sealed output')
    for row in review['targets'][:2]:
        row.update(coverage='full', finding_ids=['F-001'], evidence=['Source block and mandatory context checked'], section_assessment='correct')
    return evaluator, findings, review


def test_pending_is_not_zero_recall_or_successful_filter(tmp_path):
    result = score(*setup_case(tmp_path))
    for route in result['routes'].values():
        assert route['full_match_rate'] is None
        assert not route['review_complete']
    assert result['filter_effectiveness'] is None
    assert not result['production_acceptance']


def test_one_compound_finding_can_cover_two_distinct_atoms(tmp_path):
    result = score(*reviewed(tmp_path))
    assert result['routes']['technical']['full_match_rate'] == 1
    assert result['routes']['question']['full_match_rate'] == 1
    assert result['routes']['negative']['full_match_rate'] == 0
    assert result['routes']['negative']['counts']['held'] == 1
    assert result['section_accuracy_counts'] == {'correct': 1, 'held': 1}
    assert result['filter_effectiveness'] is None


def test_partial_not_full_and_unavailable_not_miss(tmp_path):
    evaluator, findings, review = reviewed(tmp_path)
    review['targets'][0]['coverage'] = 'partial'
    review['targets'][0]['section_assessment'] = 'unchecked'
    review['targets'][1].update(availability='unavailable', coverage='unchecked', finding_ids=[], section_assessment='unchecked')
    result = score(evaluator, findings, review)
    assert result['routes']['technical']['full_match_rate'] == 0
    assert result['routes']['technical']['counts']['partial'] == 1
    assert result['routes']['question']['full_match_rate'] is None
    assert result['routes']['question']['counts']['denominator'] == 1


@pytest.mark.parametrize('fault', ['changed_output','missing_target','duplicate_target','bad_link','no_evidence','no_reviewer','absence_without_review','unavailable_miss'])
def test_invalid_review_fails_closed(tmp_path, fault):
    evaluator, findings, review = reviewed(tmp_path)
    if fault == 'changed_output':
        findings.write_text(findings.read_text()+' ')
    elif fault == 'missing_target':
        review['targets'].pop()
    elif fault == 'duplicate_target':
        review['targets'].append(review['targets'][0])
    elif fault == 'bad_link':
        review['targets'][0]['finding_ids'] = ['unknown']
    elif fault == 'no_evidence':
        review['targets'][0]['evidence'] = []
    elif fault == 'no_reviewer':
        review['reviewer'] = None
    elif fault == 'absence_without_review':
        review['entire_output_reviewed'] = False
    else:
        review['targets'][2]['availability'] = 'unavailable'
    with pytest.raises(ValueError):
        score(evaluator, findings, review)
