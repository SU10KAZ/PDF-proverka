import json
from copy import deepcopy
import pytest
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow


def setup_case():
    md='### BLOCK #1 [TEXT]: t\nObject X\n### BLOCK #2 [TEXT]: extra\nX specification value 12\n'
    f={'id':'F1','problem':'Check X','description':'X value differs','solution':'Align values',
       'source_evidence_review_status':'pending','source_review_requests':[{'block_ids':['t']}]}
    return md,f,build_source_review_queue([f],md)


def provider(calls):
    async def invoke(request,phase):
        calls.append(phase);payload=json.loads(request['messages'][1]['content'])
        if phase=='review':
            supplied={s['block_id'] for s in payload['authoritative_text']}
            claim={'claim_id':'C1','claim':'X value differs','status':'needs_data','reason':'Need specification',
                   'missing_data':['Specification'],'context_queries':['X specification']}
            if 'extra' in supplied:
                claim={'claim_id':'C1','claim':'X value differs','status':'supported','reason':'Compared source',
                       'evidence':[{'block_id':'extra','quote':'X specification value 12'}]}
            result={k:payload[k] for k in ('finding_id','finding_sha256','source_md_sha256','context_sha256')}
            result['claims']=[claim]
        else:
            result={'response_sha256':payload['required_response_sha256'],'origin':'explicit_source_review',
                    'reviewer':'test','reason':'Explicit test decision','action':'retain','claim_reviews':[
                        {'claim_id':'C1','role':'defect_basis','verdict':'supported','same_scope_verified':True,
                         'evidence_sufficiency_verified':True,'reason':'Same proposition'}]}
        return {'json_data':result,'is_error':False,'duration_ms':1}
    return invoke


@pytest.mark.asyncio
async def test_full_chain_and_resume_without_duplicate_calls(tmp_path):
    md,f,q=setup_case();before=deepcopy(q);calls=[]
    run=lambda:run_source_review_workflow(q,f,md,tmp_path,tmp_path/'workflow',invoke=provider(calls),executor_id='test')
    result=await run()
    assert calls==['review','review','adjudication']
    assert result['status']=='candidate_ready' and not result['semantic_acceptance']
    assert q==before and f==q['packets'][0]['finding_snapshot']
    again=await run()
    assert calls==['review','review','adjudication']
    assert all(c['reused'] for c in again['calls'])
    with pytest.raises(ValueError,match='inputs_changed'):
        await run_source_review_workflow(q,f,md+'changed',tmp_path,tmp_path/'workflow',invoke=provider(calls),executor_id='test')


@pytest.mark.asyncio
async def test_interrupted_dispatch_is_not_repeated(tmp_path):
    md,f,q=setup_case();calls=[]
    async def failed(request,phase):
        calls.append(phase);raise TimeoutError('unknown remote outcome')
    run=lambda:run_source_review_workflow(q,f,md,tmp_path,tmp_path/'workflow',invoke=failed,executor_id='test')
    with pytest.raises(TimeoutError):await run()
    with pytest.raises(FileExistsError):await run()
    assert calls==['review']
    assert not (tmp_path/'workflow/candidate.json').exists()


@pytest.mark.asyncio
async def test_invalid_quote_stops_before_adjudication(tmp_path):
    md,f,q=setup_case();calls=[]
    async def invalid(request,phase):
        result=await provider(calls)(request,phase)
        result['json_data']['claims'][0]={'claim_id':'C1','claim':'False conclusion','status':'supported',
            'reason':'Invented quote','evidence':[{'block_id':'t','quote':'Not in source'}]}
        return result
    with pytest.raises(ValueError,match='invalid_source_review'):
        await run_source_review_workflow(q,f,md,tmp_path,tmp_path/'workflow',invoke=invalid,executor_id='test')
    assert calls==['review']


@pytest.mark.asyncio
async def test_cancelled_workflow_sends_nothing(tmp_path):
    md,f,q=setup_case();calls=[]
    with pytest.raises(ValueError,match='cancelled'):
        await run_source_review_workflow(q,f,md,tmp_path,tmp_path/'workflow',invoke=provider(calls),executor_id='test',is_cancelled=lambda:True)
    assert calls==[]


@pytest.mark.asyncio
async def test_file_phase_preserves_findings_and_separates_candidate_from_admission(tmp_path):
    from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow_phase
    md,f,q=setup_case();path=tmp_path/'03_findings.json'
    path.write_text(json.dumps({'findings':[f]}));before=path.read_bytes();calls=[]
    result=await run_source_review_workflow_phase(tmp_path,md,invoke=provider(calls),executor_id='test',max_findings=1)
    assert result['candidate_ready']==1 and result['not_completed']==0
    assert path.read_bytes()==before
    assert not (tmp_path/'03_source_review_admissions.json').exists()
    assert (tmp_path/'03_source_review_candidates.json').exists()
    again=await run_source_review_workflow_phase(tmp_path,md,invoke=provider(calls),executor_id='test',max_findings=1)
    assert again['candidate_ready']==1 and len(calls)==3


def test_direct_adapter_rejects_unapproved_provider_routes():
    from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor
    for model in ('openrouter/claude','anthropic/sonnet','',None):
        with pytest.raises(ValueError,match='unsupported_direct_review_model'):
            direct_cli_executor(model,project_id='test')


@pytest.mark.asyncio
async def test_runtime_hook_runs_only_explicit_callback_and_keeps_original(tmp_path):
    from types import SimpleNamespace
    from unittest.mock import patch, AsyncMock
    from backend.app.pipeline.stages.findings_verify.runner import run_findings_verify
    md,f,q=setup_case();(tmp_path/'03_findings.json').write_text(json.dumps({'findings':[f]}))
    path=tmp_path/'document.md';path.write_text(md);calls=[]
    ctx=SimpleNamespace(project_id='test',output_dir=tmp_path,project_info={},is_cancelled=None,
        log=AsyncMock(),update_pipeline_log=lambda *a,**kw:None,
        source_review_invoke=provider(calls),source_review_executor_id='test',source_review_max_findings=1)
    before=(tmp_path/'03_findings.json').read_bytes()
    with patch('backend.app.pipeline.stages.findings_verify.runner._run_deterministic_phase',return_value={'findings_total':1}), \
         patch('backend.app.pipeline.stages.findings_verify.runner._run_absence_phase',return_value={'candidates':0}), \
         patch('backend.app.pipeline.stages.prepare.graph_builder.resolve_document_markdown',return_value=path):
        result=await run_findings_verify(ctx)
    assert result.source_review_candidates_ready==1 and result.source_review_applied==0
    assert (tmp_path/'03_findings.json').read_bytes()==before
    assert calls==['review','review','adjudication']


@pytest.mark.asyncio
async def test_image_budget_is_bound_across_followup_and_resume(tmp_path, monkeypatch):
    import backend.app.pipeline.stages.findings_verify.source_review_workflow as workflow
    md, finding, queue = setup_case()
    calls, budgets, followup_budgets = [], [], []
    original_request = workflow.prepare_source_review_request
    original_followup = workflow.prepare_review_followup

    def request(*args, **kwargs):
        budgets.append(kwargs['max_images'])
        return original_request(*args, **kwargs)

    def followup(*args, **kwargs):
        followup_budgets.append(kwargs['max_images'])
        return original_followup(*args, **kwargs)

    monkeypatch.setattr(workflow, 'prepare_source_review_request', request)
    monkeypatch.setattr(workflow, 'prepare_review_followup', followup)
    await run_source_review_workflow(queue, finding, md, tmp_path, tmp_path/'workflow',
        invoke=provider(calls), executor_id='test', max_images=16)
    assert budgets == [16, 16] and followup_budgets == [16, 16]
    with pytest.raises(ValueError, match='workflow_inputs_changed'):
        await run_source_review_workflow(queue, finding, md, tmp_path, tmp_path/'workflow',
            invoke=provider(calls), executor_id='test', max_images=8)
    assert calls == ['review', 'review', 'adjudication']
    with pytest.raises(ValueError, match='invalid_image_budget'):
        await run_source_review_workflow(queue, finding, md, tmp_path, tmp_path/'bad',
            invoke=provider(calls), executor_id='test', max_images=17)


@pytest.mark.asyncio
async def test_quote_spacing_resume_preserves_raw_response_and_reuses_review(tmp_path):
    md='### BLOCK #1 [TEXT]: t\n| A |  | 28 |\n'
    _,f,_=setup_case();q=build_source_review_queue([f],md);calls=[]
    async def invoke(request,phase):
        if phase!='review':return await provider(calls)(request,phase)
        calls.append(phase);data=json.loads(request['messages'][-1]['content'])
        response={k:data[k] for k in ('finding_id','finding_sha256','source_md_sha256','context_sha256')}
        response['claims']=[{'claim_id':'C1','claim':'X value differs','status':'supported','reason':'Source row',
                            'evidence':[{'type':'text','block_id':'t','quote':'| A | | 28 |'}]}]
        return {'json_data':response,'is_error':False}
    root=tmp_path/'workflow'
    with pytest.raises(ValueError,match='invalid_source_review'):
        await run_source_review_workflow(q,f,md,tmp_path,root,invoke=invoke,executor_id='test')
    raw_before=(root/'00_review/result.json').read_bytes()
    result=await run_source_review_workflow(q,f,md,tmp_path,root,invoke=invoke,executor_id='test',repair_quote_spacing=True)
    assert calls==['review','adjudication']
    assert result['status']=='candidate_ready' and result['calls'][0]['reused']
    assert (root/'00_review/result.json').read_bytes()==raw_before
    assert json.loads((root/'00_validation.json').read_text())['errors']==['claim_0:quote_not_in_source']
    assert not json.loads((root/'00_repaired_validation.json').read_text())['errors']
    assert result['entry']['response']['claims'][0]['evidence'][0]['quote']=='| A |  | 28 |'
    assert len(json.loads((root/'00_quote_spacing.json').read_text())['repairs'])==1


@pytest.mark.asyncio
async def test_phase_opt_in_for_automatic_graphic_regions_is_forwarded_and_isolated(tmp_path):
    from unittest.mock import AsyncMock, patch
    from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow_phase
    md,f,q=setup_case();(tmp_path/'03_findings.json').write_text(json.dumps({'findings':[f]}))
    stub=AsyncMock(return_value={'status':'candidate_ready'})
    with patch('backend.app.pipeline.stages.findings_verify.source_review_workflow.run_source_review_workflow',stub):
        for enabled in (False,True):
            await run_source_review_workflow_phase(tmp_path,md,invoke=provider([]),executor_id='test',max_findings=1,auto_graphic_regions=enabled)
    first,second=stub.call_args_list
    assert first.kwargs['auto_graphic_regions'] is False and second.kwargs['auto_graphic_regions'] is True
    assert first.args[4]!=second.args[4]
    assert json.loads((tmp_path/'03_findings.json').read_text())=={'findings':[f]}
