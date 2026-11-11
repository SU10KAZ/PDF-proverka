"""Method isolation is checked in fresh processes so imports cannot cross arms."""
import json
import subprocess
import sys
from pathlib import Path
import pytest

ROOT=Path(__file__).resolve().parents[1]

@pytest.mark.parametrize('variant,callbacks',[
    ('classic_current',set()),
    ('inventory',{'inventory_invoke'}),
    ('source_review',{'source_review_invoke','source_final_check_invoke'}),
    ('candidate',{'inventory_invoke','source_review_invoke','source_final_check_invoke'}),
])
def test_real_preflight_enables_only_selected_callbacks(tmp_path,variant,callbacks):
    result=subprocess.run([sys.executable,'-m','experiments.auditmanager_stage4_20261110.paired_runner',
        'preflight','--variant',variant,'--source-root',str(ROOT),'--work',str(tmp_path)],
        cwd=ROOT,capture_output=True,text=True)
    assert result.returncode==0,result.stdout+result.stderr
    report=json.loads((tmp_path/'transport_preflight.json').read_text())
    assert {k for k in report['context_option_keys'] if k.endswith('_invoke')}==callbacks
    assert report['environment']['AUDIT_OBJECT_CONTEXT_FOLLOWUPS']==('1' if 'inventory_invoke' in callbacks else '0')
    assert 'grouping_invoke' not in report['context_option_keys']
    assert report['model_calls']==0
    assert report['cross_arm_imports']=={}


def test_review_cannot_be_enabled_without_release_check():
    code="""
import sys
sys.path.insert(0,'experiments/classic_investigator')
import classic_control
classic_control.METHOD_CONFIG={'source_review_max_findings':5,'source_review_auto_graphic_regions':True}
try:
    classic_control.method_context_options('test')
except ValueError as exc:
    assert 'Incomplete method configuration' in str(exc)
else:
    raise AssertionError('Incomplete safety dependency accepted')
assert 'backend' not in sys.modules
"""
    result=subprocess.run([sys.executable,'-c',code],cwd=ROOT,capture_output=True,text=True)
    assert result.returncode==0,result.stdout+result.stderr
