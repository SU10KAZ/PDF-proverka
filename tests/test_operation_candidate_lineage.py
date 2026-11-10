from copy import deepcopy
import json
from types import SimpleNamespace

from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.findings_merge.candidate_coverage import build_candidate_coverage
from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage
from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews, build_source_review_queue


def case():
    packet={'object':{'id':'obj','identity':'A','scope':'single','kind':'unit','source_refs':['t']},
        'allowed_source_refs':['t'],'operations':[{'operation':'document_links'},{'operation':'requirement_applicability'}]}
    request=SimpleNamespace(messages=[{'role':'user','content':json.dumps(packet)}])
    response={'object_operation_results':[
        {'object_id':'obj','operation':'document_links','status':'checked','reason':'Source comparison','source_refs':['t']},
        {'object_id':'obj','operation':'requirement_applicability','status':'needs_data','reason':'Missing calculation'}],
        'findings':[{'object_id':'obj','operation':'document_links','problem':'Wrong sheet reference','description':'Source claim',
            'solution':'Reconcile reference','report_section':'questions','evidence':[{'block_id':'t'}]},
            {'object_id':'obj','operation':'requirement_applicability','problem':'Provide calculation',
             'report_section':'questions','evidence':[{'block_id':'t'}]}]}
    return request,response


def test_operation_candidates_remain_accounted_and_enter_semantic_review_after_duplicate_merge():
    request,response=case();original=deepcopy(response)
    validated=validate_object_operation_payload(request,response)
    source_id=validated['findings'][0]['source_finding_ids'][0]
    assert source_id.startswith('O-')
    assert validate_object_operation_payload(request,response)['findings'][0]['source_finding_ids']==[source_id]
    baseline={'findings':[{**deepcopy(response['findings'][0]),'id':'F-001','source_finding_ids':['G-001']}]}
    unchanged=deepcopy(baseline)
    combined=combine_findings_with_targeted(baseline,[('object_operations_test',validated)])
    assert baseline==unchanged and response==original
    assert len(combined['findings'])==1
    finding=combined['findings'][0]
    assert finding['description']=='Source claim'
    assert finding['source_finding_ids']==['G-001',source_id]
    # Normal T-ID backfill must not erase executor-owned operation lineage.
    text_candidate_lineage(combined['findings'],{'text_findings':[]},{'t'})
    assert attach_pending_source_reviews(combined['findings'],{'t'})==1
    queue=build_source_review_queue(combined['findings'],'### BLOCK #1 [TEXT]: t\nSource text\n')
    assert queue['pending_count']==1
    assert 'Source claim' in queue['packets'][0]['finding_snapshot']['source_review_requests'][0]['source_claim']
    coverage=build_candidate_coverage({}, {}, combined)
    assert coverage['candidate_count']==2
    assert coverage['counts']=={'linked_requires_semantic_review':1,'operation_pending_requires_review':1}
    assert coverage['candidates'][1]['candidate']['raw_candidate']==response['findings'][1]
    assert not coverage['preservation_verified']


def test_different_response_or_request_cannot_reuse_operation_candidate_identity():
    request,response=case()
    initial=validate_object_operation_payload(request,response)['operation_candidate_ledger'][0]['candidate']['id']
    response['findings'][0]['description']='Changed source claim'
    changed=validate_object_operation_payload(request,response)['operation_candidate_ledger'][0]['candidate']['id']
    assert changed!=initial
    request.messages.append({'role':'user','content':'Additional context'})
    # Candidate ID generation is independent of model-supplied final IDs.
    from backend.app.pipeline.stages.findings_merge.operation_lineage import operation_candidate
    candidate,_=operation_candidate(request.messages,response['findings'][0],0,{'t'})
    assert candidate['id']!=changed


def test_later_group_merge_keeps_operation_source_from_nonleader(tmp_path):
    from backend.app.pipeline.stages.findings_merge.runner import merge_similar_findings
    request,response=case()
    response['findings'][0]['problem']='Wrong sheet reference 4'
    validated=validate_object_operation_payload(request,response)
    candidate=validated['findings'][0]
    candidate.update(id='F-002',severity='low',category='documentation')
    baseline={'id':'F-001','problem':'Wrong sheet reference 3','description':'Classic',
        'severity':'low','category':'documentation','report_section':'questions'}
    path=tmp_path/'03_findings.json'
    path.write_text(json.dumps({'findings':[baseline,candidate]}))
    result=merge_similar_findings('test',output_dir=tmp_path)
    assert result['merged_groups']==1
    final=json.loads(path.read_text())['findings']
    assert len(final)==1
    assert final[0]['operation_candidate_lineage']==candidate['operation_candidate_lineage']
    assert attach_pending_source_reviews(final,{'t'})==1
    assert 'Source claim' in final[0]['source_review_requests'][0]['source_claim']


def test_merge_does_not_mutate_frozen_base_metadata_or_nested_evidence():
    request,response=case()
    payload=validate_object_operation_payload(request,response)
    baseline={'findings':[{'id':'F-001','problem':'Classic','evidence':[{'block_id':'t','quote':'original'}]}],
        'meta':{'total_findings':1,'nested':{'status':'original'}}}
    before=deepcopy(baseline);raw_before=deepcopy(payload)
    first=combine_findings_with_targeted(baseline,[('object_operations_first',payload)])
    assert baseline==before
    first['meta']['nested']['status']='changed'
    first['findings'][0]['evidence'][0]['quote']='changed'
    first['object_operation_reports'][0]['candidate_ledger'].clear()
    second=combine_findings_with_targeted(baseline,[('object_operations_second',payload)])
    assert baseline==before and payload==raw_before
    assert second['findings'][0]['evidence'][0]['quote']=='original'
    assert second['meta']['nested']['status']=='original'
    assert second['object_operation_reports'][0]['candidate_ledger']


def test_equal_titles_do_not_erase_different_cabinet_locations_or_claims():
    request,response=case()
    validated=validate_object_operation_payload(request,response)
    incoming=validated['findings'][0]
    for field,value in [('description','Other supported claim'),('sheet','Other sheet'),
                        ('evidence',[{'block_id':'other_cabinet'}]),('solution','Different correction')]:
        baseline={'findings':[{**deepcopy(incoming),'id':'F-001',field:value}]}
        before=deepcopy(baseline)
        combined=combine_findings_with_targeted(baseline,[('object_operations_test',validated)])
        assert baseline==before
        assert len(combined['findings'])==2
        assert combined['findings'][0]==before['findings'][0]
        assert combined['findings'][1].get(field)==incoming.get(field)
        assert combined['findings'][1]['id']=='F-002'
