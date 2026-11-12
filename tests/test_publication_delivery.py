from copy import deepcopy
import pytest
from backend.app.pipeline.stages.findings_merge.publication_delivery import check_publication_delivery
from backend.app.pipeline.stages.findings_merge.publication_reconciliation import InvalidPublication, fingerprint


def example():
    core = {'publication_id': 'F:core', 'statement': 'Local conflict.'}
    pending = {'publication_id': 'F:reserve', 'statement': 'Reserve role unknown.',
               'required_context': ['Operating mode']}
    projection = {'technical_findings': [core], 'questions': [], 'pending': [pending],
                  'withdrawn': [], 'history_only': [], 'archive': {'id': 'F', 'description': 'Original'}}
    finding = {'id': 'F', 'description': 'Local conflict. Consequences unknown.'}
    decisions = [
        {'publication_id': 'F:core', 'part_sha256': fingerprint(core), 'state': 'emitted',
         'reviewer': 'control', 'reason': 'Retained local conflict',
         'targets': [{'finding_id': 'F', 'finding_sha256': fingerprint(finding),
                      'field_quotes': {'description': 'Local conflict.'}}]},
        {'publication_id': 'F:reserve', 'part_sha256': fingerprint(pending), 'state': 'deferred',
         'reviewer': 'control', 'reason': 'Need operating mode', 'targets': []}]
    return projection, [finding], decisions


def test_emitted_parent_does_not_close_pending_child():
    args = example()
    before = deepcopy(args)
    result = check_publication_delivery(*args)
    assert args == before
    assert result['pending_parts'] == args[0]['pending']
    assert result['accounting_complete'] and not result['semantic_verified']


@pytest.mark.parametrize('mutation,error', [
    (lambda p, f, d: d.pop(), 'missing_delivery_decisions'),
    (lambda p, f, d: d[1].update(state='history'), 'pending_part_cannot_disappear'),
    (lambda p, f, d: f[0].update(description='Changed'), 'final_finding_changed'),
    (lambda p, f, d: d[0]['targets'][0]['field_quotes'].update(description='Absent'), 'delivery_wording_not_present'),
    (lambda p, f, d: d.append(deepcopy(d[0])), 'unknown_or_duplicate_delivery_decision'),
])
def test_loss_stale_report_and_invented_delivery_are_rejected(mutation, error):
    args = example()
    mutation(*args)
    with pytest.raises(InvalidPublication, match=error):
        check_publication_delivery(*args)
