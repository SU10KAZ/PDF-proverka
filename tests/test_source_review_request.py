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


def test_detail_evidence_is_bound_to_the_exact_supplied_image(tmp_path):
    import hashlib
    from backend.app.pipeline.stages.findings_verify.source_review_results import validate_source_review
    queue, finding, md = fixture(tmp_path)
    detail = tmp_path/'detail.png';Image.new('RGB',(4,3),'red').save(detail)
    manifest={'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),
              'images':[{'block_id':'g','file':'detail.png','sha256':hashlib.sha256(detail.read_bytes()).hexdigest(),
                         'page_index':0,'clip_rotated_page_points':[10,20,30,40]}]}
    (tmp_path/'graphic_detail_manifest.json').write_text(json.dumps(manifest))
    attach_graphic_receipts(queue,md,tmp_path)
    request=prepare_source_review_request(queue,finding,md)
    payload=json.loads(request['messages'][1]['content'])
    assert [im['block_id'] for im in request['image_bindings']]==['g','g']
    assert payload['images_in_attachment_order'][1]['view_kind']=='detail_of_original_graphic'
    assert 'SECRET_ORACLE' not in json.dumps(request['messages'])
    supplied={(im['block_id'],im['sha256']) for im in request['image_bindings']}
    for receipt in queue['packets'][0]['graphic_attachments']:
        receipt['attached_to_model']=(receipt['block_id'],receipt['sha256']) in supplied
    response={k:payload[k] for k in ('finding_id','finding_sha256','source_md_sha256','context_sha256')}
    response['claims']=[{'claim_id':'c1','claim':'Visible local detail','status':'supported','reason':'Read detail',
        'evidence':[{'type':'image','block_id':'g','image_sha256':manifest['images'][0]['sha256'],
                     'bbox_norm_xyxy':[0,0,1,1],'observation':'Visible feature in the detail crop'}]}]
    assert validate_source_review(response,queue,[finding],md)['errors']==[]
    limited=prepare_source_review_request(queue,finding,md,max_images=1)
    assert len(limited['image_paths'])==1
    supplied={(im['block_id'],im['sha256']) for im in limited['image_bindings']}
    for receipt in queue['packets'][0]['graphic_attachments']:
        receipt['attached_to_model']=(receipt['block_id'],receipt['sha256']) in supplied
    assert 'claim_0:graphic_not_supplied' in validate_source_review(response,queue,[finding],md)['errors']
    Image.new('RGB',(4,3),'blue').save(detail)
    with pytest.raises(ValueError,match='graphic_version_changed'):
        prepare_source_review_request(queue,finding,md)
