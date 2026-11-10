from copy import deepcopy
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_followup import prepare_review_followup


def data():
    md = '### BLOCK #1 [TEXT]: t\nK22 plan\n### BLOCK #2 [TEXT]: extra\nK22 specification\n'
    findings = [{'id':'F1','source_evidence_review_status':'pending','source_review_requests':[{'block_ids':['t']}]}]
    queue = build_source_review_queue(findings,md)
    response = {'finding_id':'F1','finding_sha256':queue['packets'][0]['finding_sha256'],
        'source_md_sha256':queue['source_md_sha256'], 'claims':[{'claim_id':'C1','claim':'Check K22 volume',
        'status':'needs_data','reason':'Specification required','missing_data':['Specification'],
        'context_queries':['K22']}]}
    return md,findings,queue,response


def test_model_queries_prepare_bound_followup_without_dispatch(tmp_path):
    md,findings,queue,response=data(); before=deepcopy(queue)
    result=prepare_review_followup(response,queue,findings,md,tmp_path)
    assert result['status']=='prepared_awaiting_dispatch_authorization'
    assert result['search_receipt']['added_block_ids']==['extra']
    assert result['request']['sent'] is False and not result['automatic_dispatch']
    assert queue==before
    expanded=result['expanded_queue']
    response['context_sha256']=expanded['packets'][0]['context_sha256']
    assert prepare_review_followup(response,expanded,findings,md,tmp_path)['status']=='no_search_backlog'


def test_stale_response_cannot_trigger_retrieval(tmp_path):
    md,findings,queue,response=data(); response['finding_sha256']='wrong'
    result=prepare_review_followup(response,queue,findings,md,tmp_path)
    assert result['status']=='invalid_response' and 'request' not in result


def test_empty_search_does_not_prove_absence(tmp_path):
    md,findings,queue,response=data(); response['claims'][0]['context_queries']=['missing object']
    result=prepare_review_followup(response,queue,findings,md,tmp_path)
    assert result['status']=='no_additional_sources' and not result['absence_proven']
    assert 'request' not in result


def test_supported_claim_queries_do_not_trigger_search(tmp_path):
    md,findings,queue,response=data(); c=response['claims'][0]
    c['status']='supported'; c['evidence']=[{'block_id':'t','quote':'K22 plan'}]
    assert prepare_review_followup(response,queue,findings,md,tmp_path)['status']=='no_model_queries'


def test_file_phase_prepares_sources_without_mutating_findings(tmp_path):
    import json
    from backend.app.pipeline.stages.findings_verify.source_review_followup import run_review_response_phase
    md,findings,queue,response=data()
    path=tmp_path/'03_findings.json';path.write_text(json.dumps({'findings':findings}));before=path.read_bytes()
    (tmp_path/'03_source_review_responses.json').write_text(json.dumps([{'response':response,'queue':queue}]))
    stats=run_review_response_phase(tmp_path,md)
    assert stats['prepared']==1 and stats['invalid']==0 and path.read_bytes()==before
    report=json.loads((tmp_path/'03_source_review_followups.json').read_text())[0]
    assert report['request']['sent'] is False


def test_missing_inbox_removes_stale_followup(tmp_path):
    from backend.app.pipeline.stages.findings_verify.source_review_followup import run_review_response_phase
    target=tmp_path/'03_source_review_followups.json';target.write_text('stale')
    assert run_review_response_phase(tmp_path,'')['skipped'] and not target.exists()


def test_recorded_backlog_continues_without_new_model_queries(tmp_path):
    md,findings,_,response=data()
    md += ''.join(f'### BLOCK #{i+3} [TEXT]: extra{i}\nK22 source {i}\n' for i in range(15))
    queue=build_source_review_queue(findings,md)
    response['source_md_sha256']=queue['source_md_sha256']
    first=prepare_review_followup(response,queue,findings,md,tmp_path)
    queue=first['expanded_queue']
    response['context_sha256']=queue['packets'][0]['context_sha256']
    response['claims'][0].pop('context_queries')
    second=prepare_review_followup(response,queue,findings,md,tmp_path)
    assert second['status']=='prepared_awaiting_dispatch_authorization'
    assert second['expanded_queue']['packets'][0]['context_revision']==2
    assert second['request']['sent'] is False
    assert second['search_receipt']['added_block_ids']
    response['context_sha256']=queue['packets'][0]['context_sha256']
    assert prepare_review_followup(response,second['expanded_queue'],findings,md,tmp_path)['status']=='invalid_response'
