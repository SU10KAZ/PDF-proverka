"""Simulated fresh decisions on a known source; not model-quality evidence."""
from copy import deepcopy
import pytest
from experiments.auditmanager_stage4_20261110.test_reference_retention_admission_bridge import case
from experiments.auditmanager_stage4_20261110.reference_retention_admission_bridge import prepare_reference_adjudication
from experiments.auditmanager_stage4_20261110.reference_retention_review_gate import validate_reference_review


@pytest.fixture
def ready(case):
    request, entry, workflow, md = case
    preparation = prepare_reference_adjudication(*case)
    decision = deepcopy(entry['decision'])
    decision['approved_fields'] = deepcopy(workflow['candidate_fields'])
    decision['context_reference_obligations'] = deepcopy(preparation['required_obligations'])
    decision['final_wording_verified'] = True
    return preparation, decision, entry['response'], workflow['original_fields'], md


def test_complete_selection_still_requires_normal_admission(ready):
    before = deepcopy(ready)
    result = validate_reference_review(*ready)
    assert ready == before
    assert result['status'] == 'ready_for_normal_admission_validation'
    assert not result['automatic_release'] and not result['semantic_acceptance']


def test_changed_source_version_is_held(ready):
    with pytest.raises(ValueError, match='stale_reference_review'):
        validate_reference_review(*ready[:-1], ready[-1] + '\nchanged source')


@pytest.mark.parametrize('change', ['omit', 'empty', 'duplicate', 'rebind', 'revision', 'unverified', 'wording', 'stale'])
def test_incomplete_or_unreviewed_rewrite_is_held(ready, change):
    decision = ready[1]
    if change == 'omit':
        del decision['context_reference_obligations']
    elif change == 'empty':
        decision['context_reference_obligations'] = []
    elif change == 'duplicate':
        decision['context_reference_obligations'] *= 2
    elif change == 'rebind':
        decision['context_reference_obligations'][0]['claim_id'] = 'C7'
    elif change == 'revision':
        decision['reference_selection_requires_revision'] = True
    elif change == 'unverified':
        decision['final_wording_verified'] = False
    elif change == 'wording':
        decision['approved_fields'] = deepcopy(ready[3])
    else:
        decision['response_sha256'] = 'stale'
    with pytest.raises(ValueError):
        validate_reference_review(*ready)
