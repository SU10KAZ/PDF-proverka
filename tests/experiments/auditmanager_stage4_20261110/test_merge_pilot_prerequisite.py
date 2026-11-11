import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import pytest
from experiments.auditmanager_stage4_20261110.run_merge_primary_text_pilot import prerequisite


def completed():
    return {'status': 'stopped', 'runs': [dict(status='terminal', exit_code=0,
        execution_status='completed_unadjudicated') for _ in range(2)]}


def test_waits_for_live_child_even_if_collector_says_stopped():
    assert prerequisite(completed(), [{'live': False}, {'live': True}]) == 'wait'


def test_observation_failure_does_not_trigger_dispatch():
    assert prerequisite(completed(), [{'live': None}]) == 'wait'


def test_requires_terminal_receipts_after_process_exit():
    state = completed()
    state['runs'][1]['status'] = 'running'
    with pytest.raises(ValueError):
        prerequisite(state, [{'live': False}])


def test_failed_collection_is_not_accepted():
    state = completed()
    state['status'] = 'failed_requires_inspection'
    with pytest.raises(ValueError):
        prerequisite(state, [{'live': False}])


def test_successful_stop_allows_next_measurement():
    assert prerequisite(completed(), [{'live': False}]) == 'ready'
