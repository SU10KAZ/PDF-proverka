from copy import deepcopy
import json
import pytest
from tests.test_source_review_final_check import fixture
from backend.app.pipeline.stages.findings_verify.source_review_release_workflow import run_final_release_check,run_final_release_phase

@pytest.mark.asyncio
async def test_release_uses_separate_call_and_reuses_bound_result(tmp_path):
    doc,entry,md,response=fixture();calls=[];before=deepcopy(doc)
    async def invoke(request,phase):calls.append(phase);return {'json_data':response,'is_error':False}
    after,receipt=await run_final_release_check(entry,doc,md,tmp_path,invoke=invoke,executor_id='test-checker')
    assert receipt['status']=='applied' and doc==before
    assert after['findings'][1]==before['findings'][1]
    assert after['source_review_history'][-1]['final_release_check']['review']==response
    again,_=await run_final_release_check(entry,doc,md,tmp_path,invoke=invoke,executor_id='test-checker')
    assert again==after and calls==['final_release_check']

@pytest.mark.asyncio
async def test_hold_does_not_change_report(tmp_path):
    doc,entry,md,response=fixture();response['decision']='hold';response['checks'][0]['status']='needs_data'
    async def invoke(*args):return {'json_data':response,'is_error':False}
    after,receipt=await run_final_release_check(entry,doc,md,tmp_path,invoke=invoke,executor_id='test')
    assert receipt['status']=='held' and after==doc

@pytest.mark.asyncio
async def test_interrupted_attempt_is_never_dispatched_again(tmp_path):
    doc,entry,md,response=fixture();calls=[]
    async def invoke(*args):calls.append(1);raise RuntimeError('connection interrupted')
    with pytest.raises(RuntimeError):await run_final_release_check(entry,doc,md,tmp_path,invoke=invoke,executor_id='test')
    with pytest.raises(FileExistsError):await run_final_release_check(entry,doc,md,tmp_path,invoke=invoke,executor_id='test')
    assert len(calls)==1

@pytest.mark.asyncio
async def test_phase_applies_once_and_detects_changed_source(tmp_path):
    doc,entry,md,response=fixture();calls=[]
    path=tmp_path/'03_findings.json';path.write_text(json.dumps(doc))
    source=tmp_path/'document.md';source.write_text(md)
    (tmp_path/'03_source_review_candidates.json').write_text(json.dumps({'reports':[{'status':'candidate_ready','entry':entry}]}))
    async def invoke(*args):calls.append(1);return {'json_data':response,'is_error':False}
    first=await run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1)
    second=await run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1)
    assert first['applied']==1 and second['already_applied']==1 and len(calls)==1

@pytest.mark.asyncio
async def test_source_edit_while_model_runs_preserves_report(tmp_path):
    doc,entry,md,response=fixture()
    path=tmp_path/'03_findings.json';path.write_text(json.dumps(doc));before=path.read_bytes()
    source=tmp_path/'document.md';source.write_text(md)
    (tmp_path/'03_source_review_candidates.json').write_text(json.dumps({'reports':[{'status':'candidate_ready','entry':entry}]}))
    async def invoke(*args):source.write_text(md+'changed');return {'json_data':response,'is_error':False}
    result=await run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1)
    assert result['not_completed']==1 and path.read_bytes()==before

@pytest.mark.asyncio
async def test_cached_final_result_cannot_be_modified(tmp_path):
    doc,entry,md,response=fixture()
    async def invoke(*args):return {'json_data':response,'is_error':False}
    await run_final_release_check(entry,doc,md,tmp_path,invoke=invoke,executor_id='test')
    path=tmp_path/'result.json';data=json.loads(path.read_text());data['json_data']['decision']='hold';path.write_text(json.dumps(data))
    with pytest.raises(ValueError,match='changed_or_unbound'):
        await run_final_release_check(entry,doc,md,tmp_path,invoke=invoke,executor_id='test')

@pytest.mark.asyncio
async def test_previously_applied_check_does_not_cover_new_source_version(tmp_path):
    doc,entry,md,response=fixture();calls=[]
    (tmp_path/'03_findings.json').write_text(json.dumps(doc));source=tmp_path/'document.md';source.write_text(md)
    (tmp_path/'03_source_review_candidates.json').write_text(json.dumps({'reports':[{'status':'candidate_ready','entry':entry}]}))
    async def invoke(*args):calls.append(1);return {'json_data':response,'is_error':False}
    await run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1)
    source.write_text(md+'changed')
    result=await run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1)
    assert result['not_completed']==1 and result['already_applied']==0 and len(calls)==1

@pytest.mark.asyncio
async def test_concurrent_phase_does_not_dispatch_second_request(tmp_path):
    import asyncio
    doc,entry,md,response=fixture();calls=[];started=asyncio.Event();finish=asyncio.Event()
    (tmp_path/'03_findings.json').write_text(json.dumps(doc));source=tmp_path/'document.md';source.write_text(md)
    (tmp_path/'03_source_review_candidates.json').write_text(json.dumps({'reports':[{'status':'candidate_ready','entry':entry}]}))
    async def invoke(*args):
        calls.append(1);started.set();await finish.wait();return {'json_data':response,'is_error':False}
    first=asyncio.create_task(run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1))
    await started.wait()
    try:
        with pytest.raises(BlockingIOError):
            await run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1)
    finally:finish.set()
    assert (await first)['applied']==1 and len(calls)==1

@pytest.mark.asyncio
async def test_verifier_runtime_invokes_explicit_final_check_and_applies(tmp_path):
    from types import SimpleNamespace
    from unittest.mock import patch, AsyncMock
    from backend.app.pipeline.stages.findings_verify.runner import run_findings_verify
    doc,entry,md,response=fixture();calls=[]
    (tmp_path/'03_findings.json').write_text(json.dumps(doc));source=tmp_path/'document.md';source.write_text(md)
    (tmp_path/'03_source_review_candidates.json').write_text(json.dumps({'reports':[{'status':'candidate_ready','entry':entry}]}))
    async def invoke(request,phase):calls.append(phase);return {'json_data':response,'is_error':False}
    ctx=SimpleNamespace(project_id='test',output_dir=tmp_path,project_info={},is_cancelled=None,
        log=AsyncMock(),update_pipeline_log=lambda *args,**kwargs:None,
        source_final_check_invoke=invoke,source_final_check_executor_id='test',source_final_check_max_findings=1)
    with patch('backend.app.pipeline.stages.findings_verify.runner._run_deterministic_phase',return_value={'findings_total':2}), \
         patch('backend.app.pipeline.stages.findings_verify.runner._run_absence_phase',return_value={'candidates':0}), \
         patch('backend.app.pipeline.stages.prepare.graph_builder.resolve_document_markdown',return_value=source):
        result=await run_findings_verify(ctx)
    assert result.final_release_applied==1 and result.final_release_not_completed==0
    assert calls==['final_release_check']
    after=json.loads((tmp_path/'03_findings.json').read_text())
    assert after['source_review_history'][-1]['final_release_check']['review']==response
    assert after['findings'][1]==doc['findings'][1]

@pytest.mark.asyncio
async def test_changed_review_policy_does_not_silently_recertify_old_release(tmp_path,monkeypatch):
    from backend.app.pipeline.stages.findings_verify import source_review_final_check as check
    doc,entry,md,response=fixture();calls=[]
    path=tmp_path/'03_findings.json';path.write_text(json.dumps(doc))
    source=tmp_path/'document.md';source.write_text(md)
    (tmp_path/'03_source_review_candidates.json').write_text(json.dumps({'reports':[{'status':'candidate_ready','entry':entry}]}))
    async def invoke(*args):calls.append(1);return {'json_data':response,'is_error':False}
    first=await run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1)
    assert first['applied']==1
    sealed=path.read_bytes()
    monkeypatch.setattr(check,'OUTPUT_PROFILE',check.OUTPUT_PROFILE+' New classification rule.')
    second=await run_final_release_phase(tmp_path,source,invoke=invoke,executor_id='test',max_findings=1)
    assert second['already_applied']==0 and second['not_completed']==1
    assert second['records'][0]['reason']=='previous_release_state_or_review_policy_changed'
    assert path.read_bytes()==sealed and len(calls)==1
