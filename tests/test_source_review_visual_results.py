from copy import deepcopy

from PIL import Image
import pytest

from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_results import validate_source_review


def inputs(tmp_path):
    md = '### BLOCK #1 [IMAGE]: g\nGenerated summary\n### BLOCK #2 [TEXT]: t\nAuthoritative text\n'
    findings = [{'id': 'F1', 'source_evidence_review_status': 'pending', 'source_review_requests': [{'block_ids': ['g']}]}]
    Image.new('RGB', (10, 10)).save(tmp_path/'block_g.png')
    queue = build_source_review_queue(findings, md)
    attach_graphic_receipts(queue, md, tmp_path)
    packet = queue['packets'][0]
    # Synthetic dispatch receipt only; no provider is called in these tests.
    packet['graphic_attachments'][0]['attached_to_model'] = True
    response = {'finding_id': 'F1', 'finding_sha256': packet['finding_sha256'], 'source_md_sha256': queue['source_md_sha256'],
                'claims': [{'claim_id': 'c', 'claim': 'Visual observation', 'reason': 'Indicated region', 'status': 'supported',
                            'evidence': [{'type': 'image', 'block_id': 'g',
                                          'image_sha256': packet['graphic_attachments'][0]['sha256'],
                                          'bbox_norm_xyxy': [0.1, 0.2, 0.4, 0.6], 'observation': 'Synthetic observation'}]}]}
    return response, queue, findings, md


def test_visual_receipt_checks_traceability_not_truth(tmp_path):
    args = inputs(tmp_path)
    before = deepcopy(args)
    result = validate_source_review(*args)
    assert result['status'] == 'recorded_pending_semantic_acceptance'
    assert not result['semantic_verified'] and not result['automatic_application']
    assert args == before


@pytest.mark.parametrize('failure', ['not_sent', 'changed_image', 'wrong_hash', 'text_as_image', 'bad_box', 'nan_box', 'no_observation'])
def test_unbound_or_untraceable_graphic_observation_is_rejected(tmp_path, failure):
    response, queue, findings, md = inputs(tmp_path)
    evidence = response['claims'][0]['evidence'][0]
    if failure == 'not_sent':
        queue['packets'][0]['graphic_attachments'][0]['attached_to_model'] = False
    elif failure == 'changed_image':
        Image.new('RGB', (11, 11)).save(tmp_path/'block_g.png')
    elif failure == 'wrong_hash':
        evidence['image_sha256'] = 'wrong'
    elif failure == 'text_as_image':
        evidence['block_id'] = 't'
        queue['packets'][0]['graphic_attachments'][0]['block_id'] = 't'
    elif failure == 'bad_box':
        evidence['bbox_norm_xyxy'] = [0.5, 0.2, 0.1, 0.7]
    elif failure == 'nan_box':
        evidence['bbox_norm_xyxy'][0] = float('nan')
    else:
        evidence['observation'] = ''
    assert validate_source_review(response, queue, findings, md)['status'] == 'unusable'
