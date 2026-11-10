import json
from copy import deepcopy
import pytest
from backend.app.pipeline.stages.findings_verify.source_review_metadata import graphic_stamp_metadata
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_results import validate_source_review

MD = '### BLOCK #1 [GRAPHIC]: g\n> **Stamp:** Code: EOM | Sheet: 2.2\nGenerated drawing claim\n### BLOCK #2 [TEXT]: t\nLiteral table\n'


def inputs():
    f = {'id': 'F1', 'source_evidence_review_status': 'pending', 'source_review_requests': [{'block_ids': ['g']}]}
    q = build_source_review_queue([f], MD)
    request = prepare_source_review_request(q, f, MD)
    payload = json.loads(request['messages'][1]['content'])
    response = {k: payload[k] for k in ('finding_id', 'finding_sha256', 'source_md_sha256', 'context_sha256')}
    response['claims'] = [{'claim_id': 'c1', 'claim': 'Sheet identity', 'reason': 'Prepared stamp', 'status': 'supported',
                           'evidence': [{'type': 'metadata', 'block_id': 'g', 'quote': 'Code: EOM | Sheet: 2.2'}]}]
    return f, q, request, response


def test_stamp_is_supplied_without_generated_summary_or_image_proof():
    f, q, request, response = inputs()
    assert 'Generated drawing claim' not in str(request)
    payload = json.loads(request['messages'][1]['content'])
    assert payload['graphic_metadata'] == [{'block_id': 'g', 'stamp': '> **Stamp:** Code: EOM | Sheet: 2.2'}]
    assert payload['authoritative_text'] == [] and request['image_paths'] == []
    checked = validate_source_review(response, q, [f], MD)
    assert checked['errors'] == [] and not checked['semantic_verified']
    response['claims'][0]['evidence'][0]['type'] = 'text'
    assert 'claim_0:source_not_in_packet' in validate_source_review(response, q, [f], MD)['errors']


@pytest.mark.parametrize('change', ['unsent', 'tampered', 'summary_quote', 'other_block'])
def test_metadata_requires_exact_supplied_source(change):
    f, q, request, response = inputs()
    item = response['claims'][0]['evidence'][0]
    if change == 'unsent': q['packets'][0].pop('graphic_metadata')
    elif change == 'tampered':
        q['packets'][0]['graphic_metadata'][0]['stamp'] = 'Sheet: WRONG'
        with pytest.raises(ValueError, match='packet_graphic_metadata_changed'):
            prepare_source_review_request(q, f, MD)
    elif change == 'summary_quote': item['quote'] = 'Generated drawing claim'
    else: item['block_id'] = 't'
    assert validate_source_review(response, q, [f], MD)['errors']


def test_ambiguous_blocks_stamps_and_page_boundaries():
    assert graphic_stamp_metadata(MD + '### BLOCK #3 [IMAGE]: g\n> **Stamp:** Code: OTHER\n', ['g']) == []
    assert graphic_stamp_metadata(MD.replace('Generated drawing claim', '> **Stamp:** Code: OTHER'), ['g']) == []
    assert graphic_stamp_metadata('### BLOCK #1 [GRAPHIC]: g\n## Page 2\n> **Stamp:** Code: OTHER', ['g']) == []
    assert graphic_stamp_metadata(MD, ['t']) == []
