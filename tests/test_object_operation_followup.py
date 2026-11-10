import asyncio
from copy import deepcopy
import json

from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes
from backend.app.pipeline.stages.prepare.object_operation_followup import build_object_operation_followup


def setup_case(tmp_path, enabled=True):
    md=tmp_path/'document.md'
    md.write_text('### BLOCK #1 [TEXT]: a\nPrimary drawing\n### BLOCK #2 [TEXT]: b\nCounterpart dimension 20\n')
    obj={'id':'obj','identity':'A','scope':'single','kind':'unit','source_refs':['a']}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[obj]}))
    req=build_object_operation_passes(tmp_path,md,enable_followup=enabled)[0]
    payload={'object_operation_results':[
        {'object_id':'obj','operation':'dimension_consistency','status':'needs_data','reason':'Need counterpart','context_queries':['Counterpart']},
        {'object_id':'obj','operation':'document_links','status':'checked','reason':'Source conflict','source_refs':['a']}],
        'findings':[{'object_id':'obj','operation':'document_links','problem':'Keep first finding','evidence':[{'block_id':'a'}]}]}
    return req,payload,md


def test_followup_is_explicitly_enabled_and_same_version_bound(tmp_path):
    request,payload,md=setup_case(tmp_path,False)
    assert build_object_operation_followup(request,payload)==(None,{'status':'disabled'})
    request,payload,md=setup_case(tmp_path)
    followup,receipt=build_object_operation_followup(request,payload)
    assert receipt['status']=='prepared'
    packet=json.loads(followup.messages[-1]['content'])
    assert packet['allowed_source_refs']==['a','b']
    assert packet['context_revision']==1
    assert 'md_path' not in followup.messages[-1]['content']
    md.write_text(md.read_text()+'changed')
    assert build_object_operation_followup(request,payload)[1]['reason']=='md_version_changed'


def test_main_codex_executor_runs_followup_and_preserves_first_findings(monkeypatch,tmp_path):
    from backend.app.services.llm import claude_runner
    from backend.app.models.usage import LLMResult
    from backend.app.pipeline.stages.prepare import codex_targeted_findings as targeted
    request,payload,md=setup_case(tmp_path)
    baseline={'findings':[{'id':'F-001','problem':'Classic finding'}],'meta':{'total_findings':1}}
    original=deepcopy(baseline);calls=[]
    async def fake(**kwargs):
        calls.append(kwargs)
        if len(calls)==1:response=deepcopy(payload)
        else:
            packet=json.loads(kwargs['messages'][-1]['content'])
            assert packet['allowed_source_refs']==['a','b']
            response={'object_operation_results':[{'object_id':'obj','operation':'dimension_consistency',
                'status':'checked','reason':'Both sources read','source_refs':['a','b']}],'findings':[]}
        return 0,'ok',LLMResult(json_data=response,model='codex/gpt-6-astra')
    monkeypatch.setattr(targeted,'build_targeted_findings_passes',lambda *a:[request])
    monkeypatch.setattr(claude_runner,'_codex_targeted_findings_enabled',lambda:True)
    monkeypatch.setattr(claude_runner,'_run_codex_json_stage',fake)
    monkeypatch.setattr(claude_runner,'_save_audit_trail',lambda *a,**k:None)
    exit_code,_,result=asyncio.run(claude_runner._run_codex_targeted_findings_merge(
        project_info={},project_id='test',model='codex/gpt-6-astra',base_result=LLMResult(json_data=baseline),
        base_text='base',on_output=None,output_dir=tmp_path,version_dir=tmp_path,version_id='v001'))
    assert exit_code==0 and len(calls)==2
    assert baseline==original
    assert [f['problem'] for f in result.json_data['findings']]==['Classic finding','Keep first finding']
    reports=result.json_data['object_operation_reports']
    assert reports[0]['context_retrieval']['status']=='prepared'
    assert reports[1]['context_retrieval']['status']=='no_context_request'
    assert result.json_data['findings'][1]['operation_candidate_lineage']


def test_dispatch_binding_checks_text_and_image_bytes(tmp_path):
    from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
    from dataclasses import replace
    import hashlib
    request,payload,md=setup_case(tmp_path)
    assert verify_operation_source_binding(request) is None
    image=tmp_path/'image.png';image.write_bytes(b'frozen bytes')
    packet=json.loads(request.messages[-1]['content']);packet['attached_image_ids_in_order']=['graphic']
    binding=deepcopy(request.continuation_source)
    binding['images']={'graphic':{'path':str(image),'sha256':hashlib.sha256(image.read_bytes()).hexdigest()}}
    request=replace(request,messages=[request.messages[0],{'role':'user','content':json.dumps(packet)}],
        image_paths=(image,),continuation_source=binding)
    assert verify_operation_source_binding(request) is None
    image.write_bytes(b'changed bytes')
    assert verify_operation_source_binding(request)=='image_bytes_changed'
    md.write_text(md.read_text()+'changed')
    assert verify_operation_source_binding(request)=='md_version_changed'
