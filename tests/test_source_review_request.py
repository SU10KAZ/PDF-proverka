from copy import deepcopy
import json

from PIL import Image
import pytest

from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request


def fixture(tmp_path):
    md = '### BLOCK #1 [TEXT]: t\nLiteral source\n### BLOCK #2 [IMAGE]: g\nGenerated summary\n'
    f = {'id': 'F1', 'problem': 'Final claim', 'source_evidence_review_status': 'pending',
         'source_review_requests': [{'source_finding_id': 'T1', 'source_claim': 'Candidate claim', 'block_ids': ['t', 'g']}],
         'engineer_decision': 'SECRET_ORACLE', 'detector_comparison': {'answer': 'SECRET_ORACLE'}}
    Image.new('RGB', (2, 2)).save(tmp_path/'block_g.png')
    q = build_source_review_queue([f], md)
    attach_graphic_receipts(q, md, tmp_path)
    return q, f, md


def test_input_is_source_only_and_does_not_mark_dispatch(tmp_path):
    args = fixture(tmp_path)
    before = deepcopy(args)
    request = prepare_source_review_request(*args)
    assert args == before
    text = json.dumps(request['messages'])
    assert 'SECRET_ORACLE' not in text and 'Generated summary' not in text
    payload = json.loads(request['messages'][1]['content'])
    assert payload['authoritative_text'][0]['authoritative_text'] == '\nLiteral source\n'
    assert payload['images_in_attachment_order'][0]['block_id'] == 'g'
    assert len(request['image_paths']) == 1 and not request['sent']


@pytest.mark.parametrize('change', ['md', 'finding', 'image'])
def test_changed_inputs_fail_before_dispatch(tmp_path, change):
    queue, finding, md = fixture(tmp_path)
    if change == 'md':
        md += 'changed'
    elif change == 'finding':
        finding['problem'] = 'Changed'
    else:
        Image.new('RGB', (3, 3)).save(tmp_path/'block_g.png')
    with pytest.raises(ValueError, match='version_changed'):
        prepare_source_review_request(queue, finding, md)


def test_image_budget_keeps_missing_evidence_visible(tmp_path):
    request = prepare_source_review_request(*fixture(tmp_path), max_images=0)
    assert request['image_paths'] == []
    payload = json.loads(request['messages'][1]['content'])
    assert payload['unavailable_images'] == [{'block_id': 'g', 'reason': 'image_limit'}]
