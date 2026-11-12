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
    assert report['environment']['AUDIT_MERGE_PRIMARY_TEXT']==('1' if 'source_review_invoke' in callbacks else '0')
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


def test_inline_boundary_cannot_inherit_tools_or_silently_drop_requested_tools():
    code="""
import sys
sys.path.insert(0,'experiments/classic_investigator')
import classic_control as c
for value in (None, ''):
    original={'model':'codex/gpt-6-astra','allowed_tools':value}
    checked=c.inline_evidence_options(original)
    assert checked['allowed_tools']=='' and original['allowed_tools']==value
for value in ('Read', 'WebSearch', 'mcp__norms__search', ['Read']):
    try:c.inline_evidence_options({'allowed_tools':value})
    except RuntimeError:pass
    else:raise AssertionError('Tool request admitted')
"""
    result=subprocess.run([sys.executable,'-c',code],cwd=ROOT,capture_output=True,text=True)
    assert result.returncode==0,result.stdout+result.stderr


def test_inventory_sources_checked_without_model_calls_before_audit(tmp_path):
    code = r'''
import json, sys
from pathlib import Path
sys.path.insert(0,'experiments/classic_investigator')
import classic_control as c
root=Path(sys.argv[1]); version=root/'v'; out=version/'03_analysis/runs/test'
graphics=out/'blocks_stage02_100';graphics.mkdir(parents=True)
work=version/'02_work';work.mkdir()
md=work/'document.md';md.write_text('### BLOCK #1 [TEXT]: t1\nFirst source\n')
(graphics/'index.json').write_text('{"blocks":[]}')
assert c.preflight_inventory_inputs(version,out,{})['status']=='not_enabled'
assert 'backend' not in sys.modules
config={'inventory_max_packets':1,'inventory_max_source_chars':100,'inventory_max_images':1}
report=c.preflight_inventory_inputs(version,out,config)
assert report['status']=='passed' and report['model_calls']==0
assert not report['semantic_inventory_complete']
md.write_text('### BLOCK #1 [TEXT]: t1\n'+'a'*80+'\n### BLOCK #2 [TEXT]: t2\n'+'b'*80)
try:c.preflight_inventory_inputs(version,out,config)
except ValueError as e:assert 'packet_budget_exceeded' in str(e)
else:raise AssertionError('Unaffordable inventory admitted')
md.write_text('### BLOCK #1 [IMAGE]: g1\nDescription without actual image')
try:c.preflight_inventory_inputs(version,out,config)
except ValueError as e:assert 'incomplete_source_coverage' in str(e)
else:raise AssertionError('Missing graphic admitted')
assert not list(root.rglob('response*.json'))
'''
    result=subprocess.run([sys.executable,'-c',code,str(tmp_path)],cwd=ROOT,capture_output=True,text=True)
    assert result.returncode==0,result.stdout+result.stderr
