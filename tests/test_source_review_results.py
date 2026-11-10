from copy import deepcopy

import pytest

from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_results import validate_source_review


def inputs():
    md = '### BLOCK #1 [TEXT]: t\n12 pieces\n### BLOCK #2 [GRAPHIC]: g\n12 symbols\n'
    findings = [{'id': 'F-001', 'problem': 'Claim', 'source_evidence_review_status': 'pending',
                 'source_review_requests': [{'block_ids': ['t']}]}]
    queue = build_source_review_queue(findings, md)
    response = {'finding_id': 'F-001', 'finding_sha256': queue['packets'][0]['finding_sha256'],
                'source_md_sha256': queue['source_md_sha256'],
                'claims': [{'claim_id': 'c1', 'claim': '12 pieces specified', 'status': 'supported',
                            'reason': 'Explicit quantity', 'evidence': [{'block_id': 't', 'quote': '12 pieces'}]}]}
    return response, queue, findings, md


def test_matching_receipt_is_not_semantic_acceptance_and_inputs_are_preserved():
    args = inputs()
    before = deepcopy(args)
    result = validate_source_review(*args)
    assert result['status'] == 'recorded_pending_semantic_acceptance'
    assert not result['semantic_verified'] and not result['claim_coverage_verified']
    assert not result['automatic_application']
    assert args == before
    args[0]['claims'][0]['reason'] = 'Changed later'
    assert result['claims'][0]['reason'] == 'Explicit quantity'


@pytest.mark.parametrize('target', ['finding', 'md', 'packet'])
def test_changed_input_cannot_reuse_old_receipt(target):
    response, queue, findings, md = inputs()
    if target == 'finding':
        findings[0]['problem'] = 'Another claim'
    elif target == 'md':
        md = md.replace('12 pieces', '13 pieces')
    else:
        queue['packets'][0]['sources'][0]['authoritative_text'] = 'Forged source'
    result = validate_source_review(response, queue, findings, md)
    assert result['status'] == 'unusable' and result['errors']


@pytest.mark.parametrize('change', ['quote', 'graphic', 'duplicate', 'no_evidence'])
def test_false_or_ambiguous_evidence_is_rejected(change):
    response, queue, findings, md = inputs()
    claim = response['claims'][0]
    if change == 'quote':
        claim['evidence'][0]['quote'] = '13 pieces'
    elif change == 'graphic':
        claim['evidence'][0] = {'block_id': 'g', 'quote': '12 symbols'}
    elif change == 'duplicate':
        response['claims'].append(deepcopy(claim))
    else:
        claim['evidence'] = []
    assert validate_source_review(response, queue, findings, md)['status'] == 'unusable'


def test_needs_data_can_be_recorded_without_fabricated_evidence():
    response, queue, findings, md = inputs()
    response['claims'] = [{'claim_id': 'c1', 'claim': 'Actual quantity', 'status': 'needs_data',
                           'reason': 'Specification does not identify installed items',
                           'missing_data': ['Installation layout'], 'evidence': []}]
    assert validate_source_review(response, queue, findings, md)['status'] == 'recorded_pending_semantic_acceptance'
    response['claims'][0]['missing_data'] = []
    assert validate_source_review(response, queue, findings, md)['status'] == 'unusable'
