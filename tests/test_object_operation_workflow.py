import hashlib
import json

import pytest

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.prepare.object_operation_workflow import run_object_operation_workflow


def case():
    md='### BLOCK #1 [TEXT]: a\nA 10\n### BLOCK #2 [TEXT]: b\nA 20\n'
    sources={bid:{'block_id':bid,'kind':'TEXT','content':text} for bid,text in [('a','A 10\n'),('b','A 20\n')]}
    packet={'object':{'id':'obj','identity':'A','kind':'unit','scope':'single','source_refs':['a']},
        'operations':[{'operation':'dimension_consistency'},{'operation':'document_links'}],
        'source_context':[sources['a']],'attached_image_ids_in_order':[],
        'allowed_source_refs':['a'],'omitted':[]}
    request={'messages':[{'role':'system','content':'Check'}, {'role':'user','content':json.dumps(packet)}],'image_paths':[]}
    result={'is_error':False,'json_data':{'object_operation_results':[
        {'object_id':'obj','operation':'dimension_consistency','status':'checked','reason':'Bounded conflict','source_refs':['a']},
        {'object_id':'obj','operation':'document_links','status':'needs_data','reason':'Need counterpart','context_queries':['A 20']}],
        'findings':[{'object_id':'obj','operation':'dimension_consistency','problem':'Original claim', 'evidence':[{'block_id':'a'}]}]}}
    kwargs={'executor_id':'test-only','expected_request_sha256':evidence_digest(request),
        'expected_md_sha256':hashlib.sha256(md.encode()).hexdigest()}
    return request,sources,md,result,kwargs


@pytest.mark.asyncio
async def test_imported_result_prepares_followup_without_dispatch_and_preserves_found_claim(tmp_path):
    request,sources,md,result,kwargs=case()
    first=await run_object_operation_workflow(request,sources,{},md,tmp_path,initial_result=result,**kwargs)
    assert first['status']=='awaiting_dispatch'
    assert not list(tmp_path.glob('*/attempt.json'))
    followup=json.loads((tmp_path/'01/request.json').read_text())
    packet=json.loads(followup['messages'][-1]['content'])
    assert [o['operation'] for o in packet['operations']]==['document_links']
    assert packet['allowed_source_refs']==['a','b']
    calls=[]
    async def invoke(req,phase):
        calls.append(phase)
        return {'is_error':False,'json_data':{'object_operation_results':[{
            'object_id':'obj','operation':'document_links','status':'checked','reason':'Counterpart read','source_refs':['a','b']}],'findings':[]}}
    report=await run_object_operation_workflow(request,sources,{},md,tmp_path,initial_result=result,invoke=invoke,**kwargs)
    assert calls==['object_operations_revision_1']
    assert report['steps'][0]['payload']['findings'][0]['problem']=='Original claim'
    assert not report['semantic_verified']
    await run_object_operation_workflow(request,sources,{},md,tmp_path,initial_result=result,invoke=invoke,**kwargs)
    assert len(calls)==1


@pytest.mark.asyncio
async def test_ambiguous_failed_dispatch_is_not_retried(tmp_path):
    request,sources,md,result,kwargs=case()
    calls=[]
    async def invoke(req,phase):
        calls.append(phase)
        raise TimeoutError('unknown remote outcome')
    with pytest.raises(TimeoutError):
        await run_object_operation_workflow(request,sources,{},md,tmp_path,invoke=invoke,**kwargs)
    with pytest.raises(ValueError,match='do_not_repeat'):
        await run_object_operation_workflow(request,sources,{},md,tmp_path,invoke=invoke,**kwargs)
    assert len(calls)==1


@pytest.mark.asyncio
async def test_changed_inputs_or_results_cannot_reuse_cached_answer(tmp_path):
    request,sources,md,result,kwargs=case()
    await run_object_operation_workflow(request,sources,{},md,tmp_path,initial_result=result,**kwargs)
    changed={**kwargs,'executor_id':'another-model'}
    with pytest.raises(ValueError,match='workflow_inputs_changed'):
        await run_object_operation_workflow(request,sources,{},md,tmp_path,initial_result=result,**changed)
    saved=tmp_path/'00/result.json'
    altered=json.loads(saved.read_text());altered['json_data']['findings']=[]
    saved.write_text(json.dumps(altered))
    with pytest.raises(ValueError,match='saved_result_changed'):
        await run_object_operation_workflow(request,sources,{},md,tmp_path,initial_result=result,**kwargs)


@pytest.mark.asyncio
async def test_revision_limit_retains_unresolved_work_and_cancellation_prevents_dispatch(tmp_path):
    request,sources,md,result,kwargs=case()
    report=await run_object_operation_workflow(request,sources,{},md,tmp_path/'limit',initial_result=result,max_revisions=0,**kwargs)
    assert report['status']=='revision_limit'
    assert report['steps'][0]['payload']['operation_coverage']['unresolved_operations']>0
    async def forbidden(*args):
        pytest.fail('Cancelled workflow dispatched')
    report=await run_object_operation_workflow(request,sources,{},md,tmp_path/'cancel',invoke=forbidden,is_cancelled=lambda:True,**kwargs)
    assert report['status']=='cancelled'


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['timeout', 'error_result', 'invalid_json'])
async def test_continuation_failure_keeps_first_claim_in_report_and_output(tmp_path, failure):
    from copy import deepcopy
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
    from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews
    request,sources,md,result,kwargs=case()
    baseline={'findings':[{'id':'F-001','problem':'Classic finding','description':'Keep unchanged'}]}
    original=deepcopy(baseline)
    payloads=[]
    def accept_step(step):
        payloads.append((f"object_operations_test_revision_{step['revision']}",step['payload']))
        combined=combine_findings_with_targeted(baseline,payloads)
        attach_pending_source_reviews(combined['findings'],set(sources))
        (tmp_path/'combined.json').write_text(json.dumps(combined))
        # Consumer mutation must not alter saved workflow evidence.
        step['payload']['findings']=[]
    calls=[]
    async def invoke(req,phase):
        calls.append(phase)
        if failure=='timeout':raise TimeoutError('ambiguous remote outcome')
        if failure=='error_result':return {'is_error':True,'json_data':{}}
        return {'is_error':False,'json_data':None}
    expected=TimeoutError if failure=='timeout' else ValueError
    with pytest.raises(expected):
        await run_object_operation_workflow(request,sources,{},md,tmp_path/'work',
            initial_result=result,invoke=invoke,on_step=accept_step,**kwargs)
    combined=json.loads((tmp_path/'combined.json').read_text())
    assert combined['findings'][0]==original['findings'][0]
    assert combined['findings'][1]['problem']=='Original claim'
    assert combined['findings'][1]['source_review_requests']
    report=json.loads((tmp_path/'work/report.json').read_text())
    assert report['failed_revision']==1
    assert report['steps'][0]['payload']['findings'][0]['problem']=='Original claim'
    assert len(report['steps'])==1
    # Resume revalidates the old result but never repeats an ambiguous/failed call.
    with pytest.raises(ValueError):
        await run_object_operation_workflow(request,sources,{},md,tmp_path/'work',
            initial_result=result,invoke=invoke,**kwargs)
    assert calls==['object_operations_revision_1']
    assert baseline==original
