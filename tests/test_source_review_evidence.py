from copy import deepcopy
import json
from pathlib import Path
import pytest
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest, record_evidence_reviews

HERE = Path(__file__).resolve().parents[1]/'experiments/auditmanager_stage4_20261110'


def fixture():
    validation = json.loads((HERE/'ITP_astra_graphic_pilot_validation.json').read_text())
    assessment = json.loads((HERE/'ITP_astra_graphic_pilot_assessment.json').read_text())
    response = validation['raw_response']
    by_id = {c['claim_id']: c for c in response['claims']}
    statuses = {'supported_in_region':'supported', 'supported_broad_context_only':'context_only',
                'wrong_region':'wrong_region', 'incomplete_region':'incomplete_region'}
    decisions = []
    for region in assessment['visual_regions']:
        evidence = by_id[region['claim_id']]['evidence'][region['evidence_index']]
        decisions.append({'claim_id': region['claim_id'], 'evidence_index': region['evidence_index'],
            'response_sha256': evidence_digest(response), 'evidence_sha256': evidence_digest(evidence),
            'status': statuses[region['visual_review']], 'reviewer':'assistant_manual_visual_review_20261110',
            'reason': region['reason']})
    return validation, decisions


def test_actual_itp_bad_regions_do_not_remove_supported_claims():
    validation, decisions = fixture()
    before = deepcopy(validation)
    result = record_evidence_reviews(validation, decisions)
    assert validation == before
    assert result['raw_response'] == before['raw_response']
    assert len(result['claims']) == 3
    assert all(c['has_reviewed_support'] for c in result['claims'])
    bad = [e for c in result['claims'] for e in c['evidence'] if e['review_status'] in ('wrong_region','incomplete_region')]
    assert len(bad) == 2 and all(e['eligible_as_support'] is False for e in bad)
    assert all(not c['claim_semantically_accepted'] and not c['automatic_suppression'] and not c['emit_engineer_question'] for c in result['claims'])


def test_only_bad_evidence_requires_more_sources_without_refuting_claim():
    validation, _ = fixture()
    response = validation['raw_response']
    response['claims'] = [deepcopy(response['claims'][1])]
    response['claims'][0]['evidence'] = [response['claims'][0]['evidence'][4]]
    c = response['claims'][0]
    d = {'claim_id':c['claim_id'], 'evidence_index':0, 'response_sha256':evidence_digest(response),
         'evidence_sha256':evidence_digest(c['evidence'][0]), 'status':'wrong_region', 'reviewer':'test', 'reason':'No tank in box'}
    result = record_evidence_reviews(validation,[d])['claims'][0]
    assert result['next_action'] == 'request_source_evidence'
    assert result['original_claim']['status'] == 'supported'
    assert not result['claim_semantically_accepted'] and not result['automatic_suppression']


@pytest.mark.parametrize('change', ['response','evidence','duplicate','invalid_receipt'])
def test_reject_unbound_or_unvalidated_decisions(change):
    validation, decisions = fixture()
    if change == 'response': decisions[0]['response_sha256'] = 'old'
    elif change == 'evidence': decisions[0]['evidence_sha256'] = 'old'
    elif change == 'duplicate': decisions.append(deepcopy(decisions[0]))
    else: validation['status'] = 'unusable'
    with pytest.raises(ValueError): record_evidence_reviews(validation, decisions)


def test_no_reviews_cannot_accept_or_drop_claims():
    validation, _ = fixture()
    result = record_evidence_reviews(validation, [])
    assert all(c['next_action']=='review_remaining_evidence' for c in result['claims'])
    assert all(not c['has_reviewed_support'] for c in result['claims'])
