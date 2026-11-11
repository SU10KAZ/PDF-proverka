from copy import deepcopy
import json
import pytest
from tests.test_source_review_final_check import fixture
from backend.app.pipeline.stages.findings_verify.source_review_completion_workflow import run_with_citation_completion
from backend.app.pipeline.stages.findings_verify.source_review_release_workflow import run_final_release_phase


def executor(entry,template,calls,*,failure=None,noncitation=False):
    async def invoke(request,phase):
        calls.append(phase);data=json.loads(request['messages'][1]['content'])
        if phase=='citation_completion':
            if failure=='interrupted':raise TimeoutError('Unknown remote outcome')
            return {'json_data':{'repair_binding':data['repair_binding'],'status':'candidate','reason':'Complete source support',
                'additions':[{'claim_id':entry['response']['claims'][0]['claim_id'],'reason':'Operand evidence',
                              'evidence':{'type':'text','block_id':'t','quote':'wrong source' if failure=='quote' else 'rows sum 12'}}]}}
        response=deepcopy(template);response['binding_sha256']=data['binding_sha256']
        if calls.count('final_release_check')==1:
            response['decision']='hold';response['checks'][-1]['status']='fail'
            if noncitation:response['checks'][0]['status']='fail'
        return {'json_data':response}
    return invoke


@pytest.mark.asyncio
async def test_single_completion_then_independent_check_and_cached_resume(tmp_path):
    doc,entry,md,template=fixture();before=deepcopy(doc);calls=[]
    invoke=executor(entry,template,calls)
    after,receipt=await run_with_citation_completion(entry,doc,md,tmp_path,invoke=invoke,executor_id='test')
    assert calls==['final_release_check','citation_completion','final_release_check']
    assert receipt['status']=='applied' and doc==before
    assert receipt['requested_entry_sha256'] and after['source_review_history'][-1]['final_release_check']==receipt
    again,second=await run_with_citation_completion(entry,doc,md,tmp_path,invoke=invoke,executor_id='test')
    assert again==after and second==receipt and len(calls)==3
    payload=json.loads((tmp_path/'after_completion/request.json').read_text())
    assert 'citation_check' not in json.loads(payload['messages'][1]['content'])


@pytest.mark.asyncio
async def test_fact_failure_never_triggers_citation_repair(tmp_path):
    doc,entry,md,template=fixture();calls=[]
    after,receipt=await run_with_citation_completion(entry,doc,md,tmp_path,invoke=executor(entry,template,calls,noncitation=True),executor_id='test')
    assert after==doc and receipt['status']=='held' and calls==['final_release_check']


@pytest.mark.asyncio
@pytest.mark.parametrize('failure',['interrupted','quote'])
async def test_failed_completion_preserves_input_and_never_redispatches(tmp_path,failure):
    doc,entry,md,template=fixture();before=deepcopy(doc);calls=[];invoke=executor(entry,template,calls,failure=failure)
    for _ in range(2):
        with pytest.raises((ValueError,TimeoutError,FileExistsError)):
            await run_with_citation_completion(entry,doc,md,tmp_path,invoke=invoke,executor_id='test')
    assert doc==before and calls==['final_release_check','citation_completion']


@pytest.mark.asyncio
async def test_phase_reuses_completed_edit_by_original_candidate_identity(tmp_path):
    doc,entry,md,template=fixture();calls=[]
    (tmp_path/'03_findings.json').write_text(json.dumps(doc))
    (tmp_path/'03_source_review_candidates.json').write_text(json.dumps({'reports':[{'status':'candidate_ready','entry':entry}]}))
    source=tmp_path/'document.md';source.write_text(md)
    invoke=executor(entry,template,calls)
    first=await run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1,complete_citations=True)
    sealed=(tmp_path/'03_findings.json').read_bytes()
    second=await run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1,complete_citations=True)
    assert first['applied']==1 and second['already_applied']==1 and len(calls)==3
    assert (tmp_path/'03_findings.json').read_bytes()==sealed
