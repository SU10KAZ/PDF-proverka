from copy import deepcopy
from dataclasses import replace
import json
import pytest
from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes
from backend.app.pipeline.stages.prepare.object_operation_disposition import prepare_operation_disposition


def setup(tmp_path):
    md=tmp_path/'document.md';md.write_text('### BLOCK #1 [TEXT]: a\nKnown source\n### BLOCK #2 [TEXT]: b\nOther detail\n')
    obj={'id':'o','identity':'A','scope':'local','kind':'unit','source_refs':['a']}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[obj]}))
    request,=build_object_operation_passes(tmp_path,md,enable_followup=True)
    payload={'object_operation_results':[{'object_id':'o','operation':'dimension_consistency','status':'needs_data',
              'reason':'Unclear surface','context_queries':['Missing-section']}],'findings':[]}
    return request,payload,md


def test_real_no_additional_sources_keeps_evidence_and_does_not_generate_finding(tmp_path):
    request,payload,md=setup(tmp_path);before=deepcopy((request,payload))
    terminal,receipt=prepare_operation_disposition(request,payload)
    assert (request,payload)==before
    assert receipt['status']=='no_additional_sources' and receipt['absence_proven'] is False
    old=json.loads(request.messages[-1]['content']);new=json.loads(terminal.messages[-1]['content'])
    for k in old:assert new[k]==old[k]
    assert terminal.image_paths==request.image_paths and terminal.continuation_source==request.continuation_source
    assert new['terminal_disposition']['previous_response']==payload
    assert 'internal_unresolved' in terminal.messages[0]['content']
    with pytest.raises(ValueError,match='disposition_already_requested'):prepare_operation_disposition(terminal,payload)


@pytest.mark.parametrize('case',['found_source','budget_limit','changed_md','unbound','wrong_object'])
def test_nonexhausted_or_unbound_search_cannot_trigger_terminal_review(tmp_path,case):
    request,payload,md=setup(tmp_path)
    if case=='found_source':payload['object_operation_results'][0]['context_queries']=['Other detail']
    elif case=='budget_limit':
        packet=json.loads(request.messages[-1]['content']);packet['context_revision']=3
        request=replace(request,messages=[request.messages[0],{'role':'user','content':json.dumps(packet)}])
    elif case=='changed_md':md.write_text(md.read_text()+'changed')
    elif case=='unbound':request=replace(request,continuation_source=None)
    else:payload['object_operation_results'][0]['object_id']='other'
    with pytest.raises(ValueError):prepare_operation_disposition(request,payload)
