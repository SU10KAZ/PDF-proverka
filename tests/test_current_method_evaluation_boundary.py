import json
from pathlib import Path
import sys
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from experiments.auditmanager_stage4_20261110.prepare_current_method_evaluation import build
from experiments.auditmanager_stage4_20261110.score_atomic_review import sha


def trial_fixture(tmp_path, terminal=False):
    root = tmp_path/'audit'; root.mkdir()
    evaluator = tmp_path/'evaluator.json'; evaluator.write_text('{}')
    work = root/'ak_r1_classic_current'
    status = work/'classic_control/status/AK.json'; status.parent.mkdir(parents=True)
    status.write_text(json.dumps({'status': 'completed_unadjudicated'}))
    row = {'work': str(work), 'discipline': 'AK', 'variant': 'classic_current', 'repeat': 1}
    (root/'experimental_collection_trial.json').write_text(json.dumps({
        'evaluator': {'path': str(evaluator), 'sha256': sha(evaluator)}, 'runs': [row]}))
    record = row | ({'exit_code': 0, 'status_sha256': 'tampered'} if terminal else {})
    (root/'experimental_collection_status.json').write_text(json.dumps({'runs': [record]}))
    return root


def test_completion_label_without_terminal_exit_does_not_open_outputs(tmp_path):
    root = trial_fixture(tmp_path)
    # Protocol, manifest and findings deliberately absent: they must not be read.
    result = build(root, tmp_path/'evaluation')
    assert result['entries'] == [] and len(result['pending']) == 1
    assert not (tmp_path/'evaluation').exists()


def test_modified_completion_receipt_is_rejected_before_findings(tmp_path):
    root = trial_fixture(tmp_path, terminal=True)
    with pytest.raises(AssertionError):
        build(root, tmp_path/'evaluation')
    assert not (tmp_path/'evaluation').exists()


def test_evaluator_cannot_be_written_into_audit_workspace(tmp_path):
    root = trial_fixture(tmp_path)
    with pytest.raises(ValueError, match='separate'):
        build(root, root/'evaluation')


def failed_trial_fixture(tmp_path):
    root = trial_fixture(tmp_path)
    collection_path = root/'experimental_collection_status.json'
    collection = json.loads(collection_path.read_text())
    record = collection['runs'][0]
    seal = Path(record['work'])/'classic_control/status/AK.json'
    seal.write_text(json.dumps({'status': 'failed'}))
    record.update(status='terminal', exit_code=1, status_sha256=sha(seal))
    collection_path.write_text(json.dumps(collection))
    return root, seal


def test_sealed_failure_has_no_quality_score_and_does_not_open_findings(tmp_path):
    root, seal = failed_trial_fixture(tmp_path)
    # No protocol, manifest or findings exist for this unsuccessful execution.
    result = build(root, tmp_path/'evaluation')
    assert result['entries'] == []
    assert result['pending'][0]['reason'] == 'terminal_failed'
    failure, = result['execution_failures']
    assert failure['quality_score'] is None
    assert failure['count_as_empty_output'] is False
    assert failure['seal_sha256'] == sha(seal)
    assert not (tmp_path/'evaluation').exists()


def test_modified_failure_receipt_is_rejected(tmp_path):
    root, seal = failed_trial_fixture(tmp_path)
    seal.write_text(json.dumps({'status': 'failed', 'modified': True}))
    with pytest.raises(AssertionError):
        build(root, tmp_path/'evaluation')
    assert not (tmp_path/'evaluation').exists()
