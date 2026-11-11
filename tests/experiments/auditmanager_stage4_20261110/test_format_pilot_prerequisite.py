import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]))
import pytest
from experiments.auditmanager_stage4_20261110.run_recovery_format_pilot import prerequisite


def test_live_or_unknown_predecessor_never_allows_dispatch():
    assert prerequisite({}, True) == 'wait'
    assert prerequisite({}, None) == 'wait'


def test_missing_receipts_after_exit_rejected():
    with pytest.raises(ValueError):
        prerequisite({'status':'completed_unadjudicated','attempts':[]}, False)


def test_both_successful_arm_receipts_required():
    state={'status':'completed_unadjudicated','attempts':[
        {'arm':arm,'is_error':False,'json_object':True} for arm in ['control','primary_text']]}
    assert prerequisite(state, False) == 'ready'
    state['attempts'][1]['is_error']=True
    with pytest.raises(ValueError):
        prerequisite(state, False)
