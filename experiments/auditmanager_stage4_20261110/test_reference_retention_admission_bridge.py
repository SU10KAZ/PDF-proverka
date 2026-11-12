"""Known-control bindings only; simulated selection/repair is not model evidence."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from experiments.auditmanager_stage4_20261110.reference_retention_admission_bridge import prepare_reference_adjudication
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

BASE=Path(__file__).parent

@pytest.fixture
def case():
    root=BASE/'ar_rope_current_release_evidence/adjudication'
    entry=json.loads((root/'candidate.json').read_text())['entry']
    request=json.loads((root/'request.json').read_text())
    md=json.loads((BASE/'ar_rope_bundle_source_evidence/bundle.json').read_text())['md']
    quote=next(c for c in entry['response']['claims'] if c['claim_id']=='C8')['evidence'][0]
    fields=deepcopy(entry['decision']['approved_fields'])
    workflow={'status':'candidate_requires_semantic_review','original_fields':deepcopy(fields),
        'candidate_fields':fields,'selection':{'reference_dispositions':[{
            'reference':'КМ8','role':'calculation_context','block_id':quote['block_id'],
            'quote':quote['quote'],'disposition':'required','reason':'Existing calculation-context address.'}]}}
    workflow['candidate_fields']['solution']+=' Проверить применимый расчёт по ссылке КМ8.'
    return request,entry,workflow,md


def test_real_reference_maps_to_c8_but_demands_fresh_review(case):
    before=deepcopy(case);result=prepare_reference_adjudication(*case)
    assert case==before
    assert result['status']=='prepared_for_fresh_semantic_adjudication'
    assert result['required_obligations'][0]['claim_id']=='C8'
    assert result['required_obligations'][0]['evidence_index']==0
    assert not result['semantic_acceptance'] and not result['automatic_release']
    assert 'decision' not in result
    payload=json.loads(result['request']['messages'][1]['content'])
    assert payload['untrusted_reference_repair']['proposed_fields']==case[2]['candidate_fields']


def test_shorter_source_quote_is_not_guessed_as_existing_claim(case):
    request,entry,workflow,md=case
    workflow['selection']['reference_dispositions'][0]['quote']='см. раздел КМ 8'
    result=prepare_reference_adjudication(request,entry,workflow,md)
    assert result['status']=='source_review_required'
    assert result['unresolved'][0]['reason']=='missing_claim_evidence'
    assert 'request' not in result


def test_duplicate_citation_requires_disambiguation(case):
    request,entry,workflow,md=case
    claim=deepcopy(next(c for c in entry['response']['claims'] if c['claim_id']=='C8'))
    claim['claim_id']='C8-other'
    entry['response']['claims'].append(claim)
    payload=json.loads(request['messages'][1]['content'])
    payload['untrusted_review']=entry['response'];payload['required_response_sha256']=evidence_digest(entry['response'])
    request['messages'][1]['content']=json.dumps(payload)
    result=prepare_reference_adjudication(request,entry,workflow,md)
    assert result['status']=='source_review_required'
    assert result['unresolved'][0]['reason']=='ambiguous_claim_evidence'


def test_stale_rewrite_cannot_reuse_previous_adjudication(case):
    case[2]['original_fields']['description']+=' changed'
    with pytest.raises(ValueError,match='stale_reference_candidate'):
        prepare_reference_adjudication(*case)
