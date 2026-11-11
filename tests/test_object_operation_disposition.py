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


def test_executor_opt_in_prepares_once_and_preserves_internal_uncertainty(tmp_path):
    from backend.app.pipeline.stages.prepare.object_operation_disposition import advance_operation_pass
    request,payload,md=setup(tmp_path)
    normal,next_request=advance_operation_pass(request,payload)
    assert next_request is None and normal['context_retrieval']['status']=='no_additional_sources'
    first,terminal=advance_operation_pass(request,payload,enable_disposition=True)
    assert terminal is not None and first['context_retrieval']['status']=='terminal_disposition_prepared'
    unresolved={**payload,'terminal_dispositions':[{'operation':'dimension_consistency','disposition':'internal_unresolved','reason':'Need another uninspected view'}]}
    final,more=advance_operation_pass(terminal,unresolved,enable_disposition=True)
    assert more is None and final['findings']==[]
    assert final['terminal_dispositions']==unresolved['terminal_dispositions']
    assert final['context_retrieval']['absence_proven'] is False


def test_budget_exhaustion_does_not_trigger_terminal_dispatch(tmp_path):
    from backend.app.pipeline.stages.prepare.object_operation_disposition import advance_operation_pass
    request,payload,md=setup(tmp_path)
    packet=json.loads(request.messages[-1]['content']);packet['context_revision']=3
    request=replace(request,messages=[request.messages[0],{'role':'user','content':json.dumps(packet)}])
    result,followup=advance_operation_pass(request,payload,enable_disposition=True)
    assert followup is None and result['context_retrieval']['status']=='revision_limit'


def test_terminal_search_receipt_cannot_be_forged_by_budget_status(tmp_path):
    from backend.app.pipeline.stages.prepare.object_operation_disposition import advance_operation_pass
    request,payload,md=setup(tmp_path);terminal,_=prepare_operation_disposition(request,payload)
    packet=json.loads(terminal.messages[-1]['content'])
    packet['terminal_disposition']['search_receipt']['status']='revision_limit'
    terminal=replace(terminal,messages=[terminal.messages[0],{'role':'user','content':json.dumps(packet)}])
    with pytest.raises(ValueError,match='invalid_terminal_search_receipt'):
        advance_operation_pass(terminal,payload,enable_disposition=True)


def test_main_executor_preserves_first_core_and_adds_only_review_candidate(monkeypatch,tmp_path):
    import asyncio
    from backend.app.services.llm import claude_runner
    from backend.app.models.usage import LLMResult
    from backend.app.pipeline.stages.prepare import codex_targeted_findings as targeted
    request,payload,md=setup(tmp_path)
    payload['object_operation_results'].append({'object_id':'o','operation':'document_links','status':'checked','reason':'Conflicting source labels','source_refs':['a']})
    payload['findings']=[{'object_id':'o','operation':'document_links','problem':'Preserve earlier source conflict','evidence':[{'block_id':'a'}]}]
    baseline={'findings':[{'id':'F-001','problem':'Existing classic finding'}],'meta':{'total_findings':1}}
    before=deepcopy(baseline);calls=[]
    async def fake(**kwargs):
        calls.append(kwargs)
        if len(calls)==1:response=deepcopy(payload)
        else:
            assert len(calls)==2
            packet=json.loads(kwargs['messages'][-1]['content'])
            assert packet['terminal_disposition']['search_receipt']['absence_proven'] is False
            response={'object_operation_results':[deepcopy(payload['object_operation_results'][0])],
                'terminal_dispositions':[{'operation':'dimension_consistency','disposition':'engineer_question','reason':'Actionable surface identity ambiguity'}],
                'findings':[{'object_id':'o','operation':'dimension_consistency','problem':'Clarify source surface identity',
                    'report_section':'questions','question_basis':'actionable_source_ambiguity','profile_reason':'Affects the dimension comparison',
                    'engineer_action_required':True,'evidence':[{'block_id':'a'}]}]}
        return 0,'ok',LLMResult(json_data=response,model='codex/gpt-6-astra')
    monkeypatch.setenv('AUDIT_OBJECT_TERMINAL_DISPOSITION','1')
    monkeypatch.setattr(targeted,'build_targeted_findings_passes',lambda *a:[request])
    monkeypatch.setattr(claude_runner,'_codex_targeted_findings_enabled',lambda:True)
    monkeypatch.setattr(claude_runner,'_run_codex_json_stage',fake)
    monkeypatch.setattr(claude_runner,'_save_audit_trail',lambda *a,**k:None)
    code,_,result=asyncio.run(claude_runner._run_codex_targeted_findings_merge(
        project_info={},project_id='test',model='codex/gpt-6-astra',base_result=LLMResult(json_data=baseline),
        base_text='base',on_output=None,output_dir=tmp_path,version_dir=tmp_path,version_id='v001'))
    assert code==0 and len(calls)==2 and baseline==before
    findings=result.json_data['findings']
    assert [f['problem'] for f in findings]==['Existing classic finding','Preserve earlier source conflict','Clarify source surface identity']
    question=findings[-1]
    assert question['report_section']=='questions' and question['operation_semantic_verified'] is False
    assert question['operation_candidate_lineage'][0]['status']=='requires_semantic_review'
    reports=result.json_data['object_operation_reports']
    assert reports[-1]['context_retrieval']['status']=='terminal_disposition_completed'
    assert reports[-1]['unverified_terminal_dispositions'][0]['disposition']=='engineer_question'
