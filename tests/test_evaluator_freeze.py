from copy import deepcopy
import pytest
import sys
from pathlib import Path

# pytest prepends tests/, whose experiments package shadows the source tree.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.auditmanager_stage4_20261110.freeze_evaluator import assemble, freeze


def data():
    original = {'technical': ['t'], 'question': ['q'], 'negative': ['n']}
    view = {
        'original_release': {}, 'errata': {},
        'original_denominators': original,
        'working_truth_denominators': {'technical': ['t'], 'question': ['q'], 'negative': []},
        'holds': [
            {'target_ids': ['n'], 'held_dimensions': ['truth_scoring', 'filter_acceptance']},
            {'target_ids': ['q'], 'held_dimensions': ['report_section_accuracy']},
        ],
    }
    release = {'denominators': deepcopy(original), 'evaluation_contract': {}, 'unscored_themes': []}
    targets = {'targets': [{'target_id': i, 'route': r} for r, ids in original.items() for i in ids],
               'aliases': {'alias': 't'}, 'theme_index': {}, 'matching_policy': 'semantic'}
    return view, release, targets, {'entries': []}


def test_dimension_hold_retains_question_truth_and_original_inventory():
    args = data()
    before = deepcopy(args)
    snapshot = assemble(*args)
    assert args == before
    assert snapshot['counts'] == {'technical': 1, 'question': 1, 'negative': 0}
    assert len(snapshot['targets']) == 3
    assert snapshot['original_denominators']['negative'] == ['n']
    assert snapshot['observed_quality_metrics'] is None
    assert snapshot['production_acceptance'] is False


@pytest.mark.parametrize('fault', ['duplicate', 'route', 'missing', 'alias', 'unexplained_exclusion'])
def test_invalid_inventory_fails_closed(fault):
    view, release, targets, errata = data()
    if fault == 'duplicate':
        targets['targets'].append(deepcopy(targets['targets'][0]))
    elif fault == 'route':
        targets['targets'][0]['route'] = 'negative'
    elif fault == 'missing':
        targets['targets'].pop()
    elif fault == 'alias':
        targets['aliases']['alias'] = 'missing'
    else:
        view['working_truth_denominators']['question'] = []
    with pytest.raises(ValueError):
        assemble(view, release, targets, errata)


def test_real_release_and_no_overwrite(tmp_path):
    path = tmp_path / 'snapshot.json'
    result = freeze(path)
    assert result['counts'] == {'technical': 84, 'question': 198, 'negative': 76}
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        freeze(path)
    assert path.read_bytes() == before
