import pytest
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location('experimental_collection',
    Path(__file__).resolve().parents[1] / 'experiments/auditmanager_stage4_20261110/run_experimental_collection.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
prerequisite = module.prerequisite


def completed():
    return {'status': 'completed_collection_unadjudicated', 'runs': [
        {'exit_code': 0, 'execution_status': 'completed_unadjudicated'} for _ in range(18)]}


def test_running_or_unobservable_process_never_authorizes_overlap():
    for live in (True, None):
        assert prerequisite({'status': 'running'}, live) == 'wait'
        assert prerequisite(completed(), live) == 'wait'
    with pytest.raises(ValueError):
        prerequisite({'status': 'running'}, False)
    assert prerequisite(completed(), False) == 'ready'


@pytest.mark.parametrize('state', [
    {'status': 'failed_requires_inspection'}, {'status': 'stopped'}, {},
    {'status': 'completed_collection_unadjudicated', 'runs': []},
])
def test_failure_or_incomplete_completion_cannot_start_experiments(state):
    with pytest.raises(ValueError):
        prerequisite(state, False)


@pytest.mark.parametrize('field,value', [('exit_code', 1), ('execution_status', 'completed_with_incomplete_usage'),
                                        ('execution_status', 'completed_with_unresolved_methods')])
def test_all_eighteen_successful_receipts_are_required(field, value):
    state = completed(); state['runs'][8][field] = value
    with pytest.raises(ValueError):
        prerequisite(state, False)
