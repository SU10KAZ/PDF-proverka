from copy import deepcopy
import json
import pytest
from tests.test_source_review_admission import case
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check,validate_final_check,CHECKS
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest


def fixture():
    doc,entries,md=case('retain','supported');entry=entries[0]
    prepared=prepare_final_check(entry,doc,md)
    response={'finding_id':'F1','binding_sha256':evidence_digest(prepared['binding']),'decision':'release',
        'checks':[{'check_id':key,'status':'pass','reason':'Explicit independent check',
                   'evidence':[{'type':'text','block_id':'t','quote':'rows sum 12'}]} for key in CHECKS]}
    return doc,entry,md,response


def test_request_has_sources_and_visible_peers_without_prior_verdicts():
    doc,entry,md,response=fixture()
    entry['decision']['reason']='SECRET PREVIOUS REVIEW REASON'
    doc['engineer_answers']='SECRET ENGINEER ANSWERS'
    before=deepcopy(doc)
    request=prepare_final_check(entry,doc,md)['request'];wire=json.dumps(request)
    assert 'SECRET PREVIOUS' not in wire and 'SECRET ENGINEER' not in wire
    payload=json.loads(request['messages'][1]['content'])
    assert payload['current_report_peers'][0]['id']=='F2'
    assert payload['sources']['authoritative_text'] and doc==before


def test_bound_release_is_only_a_candidate_and_never_mutates():
    doc,entry,md,response=fixture();before=deepcopy(doc)
    result=validate_final_check(entry,doc,md,response)
    assert result['status']=='release_candidate' and result['semantic_certified'] is False and doc==before

@pytest.mark.parametrize('bad',['stale_peer','missing_check','unknown_source','wrong_quote','unresolved_release','empty_fact_evidence','duplicate_check'])
def test_unsafe_final_checks_rejected(bad):
    doc,entry,md,response=fixture()
    if bad=='stale_peer':doc['findings'][1]['problem']='Changed peer'
    if bad=='missing_check':response['checks'].pop()
    if bad=='unknown_source':response['checks'][0]['evidence'][0]['block_id']='invented'
    if bad=='wrong_quote':response['checks'][0]['evidence'][0]['quote']='not in source'
    if bad=='unresolved_release':response['checks'][0]['status']='needs_data'
    if bad=='empty_fact_evidence':response['checks'][0]['evidence']=[]
    if bad=='duplicate_check':response['checks'][-1]=deepcopy(response['checks'][0])
    with pytest.raises(ValueError):validate_final_check(entry,doc,md,response)


def test_hold_keeps_unresolved_reason_without_suppressing_original():
    doc,entry,md,response=fixture();before=deepcopy(doc)
    response['decision']='hold';response['checks'][0]['status']='needs_data'
    result=validate_final_check(entry,doc,md,response)
    assert result['status']=='held' and doc==before
