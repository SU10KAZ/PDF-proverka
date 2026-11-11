import json
from copy import deepcopy
import pytest
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry
from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes, validate_object_operation_payload


def packet(tmp_path):
    md=tmp_path/'document.md'
    md.write_text('## Page 1\n### BLOCK #1 [TEXT]: b\nCircuit A: flow 2 + 3 = 5 m3/h at design mode.\n')
    obj={'id':'A','identity':'Circuit A','scope':'design mode','kind':'pipe','source_refs':['b']}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[obj]}))
    return build_object_operation_passes(tmp_path,md)[0],obj


def receipt():
    return {'object_id':'A','operation':'calculation_consistency','status':'checked',
        'reason':'Same boundary and mode compared','source_refs':['b'],
        'calculation_checks':[{'expression':'2 + 3','result':'5','unit':'m3/h',
            'basis':'Same circuit and simultaneous design mode','source_refs':['b']}]}


def test_counting_receipt_cannot_close_calculation_operation(tmp_path):
    request,obj=packet(tmp_path)
    counted={**receipt(),'operation':'quantity_balance'}
    r=validate_object_operation_payload(request,{'object_operation_results':[counted]})
    states={o['operation']:o['status'] for o in r['operation_coverage']['operations']}
    assert states['quantity_balance']=='reported_checked'
    assert states['calculation_consistency']=='not_checked'
    assert 'physical balances' in request.messages[0]['content']


@pytest.mark.parametrize('bad',['missing','unknown_source','no_basis','empty_result'])
def test_unreviewable_calculation_cannot_claim_completion(tmp_path,bad):
    request,obj=packet(tmp_path);r=receipt()
    if bad=='missing':r.pop('calculation_checks')
    if bad=='unknown_source':r['calculation_checks'][0]['source_refs']=['unseen']
    if bad=='no_basis':r['calculation_checks'][0]['basis']=''
    if bad=='empty_result':r['calculation_checks'][0]['result']=''
    before=deepcopy(r)
    validated=validate_object_operation_payload(request,{'object_operation_results':[r]})
    assert validated['object_operation_results'][0]['status']=='not_checked'
    assert validated['unverified_operation_receipts']==[before] and r==before
    coverage=reconcile_object_registry({'object_registry':[obj],'object_operation_results':[r]}, {'b'})
    operation=next(o for o in coverage['operations'] if o['operation']=='calculation_consistency')
    assert operation['status']=='not_checked'


def test_trace_is_reviewable_but_not_semantic_certification(tmp_path):
    request,obj=packet(tmp_path)
    validated=validate_object_operation_payload(request,{'object_operation_results':[receipt()]})
    operation=next(o for o in validated['operation_coverage']['operations'] if o['operation']=='calculation_consistency')
    assert operation['status']=='reported_checked' and operation['semantic_verified'] is False
    assert validated['operation_coverage']['coverage_complete'] is False
