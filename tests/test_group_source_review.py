from copy import deepcopy
import json
import pytest
from tests.test_reviewed_grouping import case
from backend.app.pipeline.stages.findings_merge.group_source_review import prepare_group_review,validate_group_review,CHECKS
from backend.app.pipeline.stages.findings_merge.reviewed_grouping import apply_reviewed_group


def setup(tmp_path):
    doc,proposal,_,md=case();prepared=prepare_group_review(doc,proposal,md,tmp_path)
    result={'binding':prepared['binding'],'action':'group','reason':'Shared discrepancy independently checked',
        'checks':[{'check_id':c,'status':'pass','reason':'Same source and member scopes checked',
                   'evidence':[{'type':'text','block_id':'b','quote':'Shared specification'}]} for c in CHECKS]}
    return doc,proposal,md,result


def test_source_checked_group_preserves_children_and_other_finding(tmp_path):
    doc,proposal,md,result=setup(tmp_path);before=deepcopy(doc)
    decision=validate_group_review(doc,proposal,md,tmp_path,result)
    after,receipt=apply_reviewed_group(doc,proposal,decision,md)
    assert receipt['status']=='applied' and after['findings'][1]==doc['findings'][2] and doc==before
    assert after['findings'][0]['grouping_original_findings']==doc['findings'][:2]


def test_request_excludes_history_and_proposer_reason(tmp_path):
    doc,proposal,md,result=setup(tmp_path)
    doc['engineer_answers']='SECRET ENGINEER'
    doc['findings'][0]['source_review_history']='SECRET PRIOR REVIEW'
    proposal['reason']='SECRET PROPOSER'
    request=prepare_group_review(doc,proposal,md,tmp_path)['request']
    assert 'SECRET' not in json.dumps(request)


@pytest.mark.parametrize('bad',['stale_member','stale_peer','wrong_quote','unattached_image','missing_check','fail_group','no_fact_evidence'])
def test_unbound_or_unresolved_group_rejected(tmp_path,bad):
    doc,proposal,md,result=setup(tmp_path)
    if bad=='stale_member':doc['findings'][0]['problem']='changed'
    if bad=='stale_peer':doc['findings'][2]['problem']='changed'
    if bad=='wrong_quote':result['checks'][0]['evidence'][0]['quote']='invented'
    if bad=='unattached_image':result['checks'][0]['evidence']=[{'type':'image','block_id':'b','image_sha256':'fake'}]
    if bad=='missing_check':result['checks'].pop()
    if bad=='fail_group':result['checks'][0]['status']='fail'
    if bad=='no_fact_evidence':result['checks'][0]['evidence']=[]
    with pytest.raises(ValueError):validate_group_review(doc,proposal,md,tmp_path,result)


def test_keep_separate_never_authorizes_group(tmp_path):
    doc,proposal,md,result=setup(tmp_path);result['action']='keep_separate';result['checks'][0]['status']='fail'
    decision=validate_group_review(doc,proposal,md,tmp_path,result)
    after,receipt=apply_reviewed_group(doc,proposal,decision,md)
    assert after==doc and receipt['status']=='not_applied'


def test_only_exact_attached_graphic_receipt_is_eligible(tmp_path):
    from PIL import Image
    doc,proposal,md,result=setup(tmp_path)
    md+='\n### BLOCK #2 [GRAPHIC]: g\nDrawing navigation only\n'
    for f in doc['findings'][:2]:f['evidence'].append({'type':'image','block_id':'g'})
    Image.new('RGB',(12,12),'white').save(tmp_path/'block_g.png')
    prepared=prepare_group_review(doc,proposal,md,tmp_path)
    result['binding']=prepared['binding'];image=prepared['request']['image_bindings'][0]
    result['checks'][0]['evidence']=[{'type':'image','block_id':'g','image_sha256':image['sha256'],
        'bbox_norm_xyxy':[0,0,1,1],'observation':'Test image supplied; source binding only'}]
    assert validate_group_review(doc,proposal,md,tmp_path,result)['action']=='group'
    result['checks'][0]['evidence'][0]['image_sha256']='different-image'
    with pytest.raises(ValueError,match='group_image_not_attached'):validate_group_review(doc,proposal,md,tmp_path,result)


def test_retrieved_context_is_authoritative_and_invalidates_old_verdict(tmp_path):
    doc,proposal,md,result=setup(tmp_path)
    md+='\n### BLOCK #3 [TEXT]: index\nIndex links two drawing sheets\n'
    initial=prepare_group_review(doc,proposal,md,tmp_path)
    result['binding']=initial['binding']
    expanded=prepare_group_review(doc,proposal,md,tmp_path,extra_text_block_ids=['index'])
    wire=json.loads(expanded['request']['messages'][1]['content'])
    assert 'index' in {b['block_id'] for b in wire['sources']['authoritative_text']}
    with pytest.raises(ValueError,match='stale_group_source_review'):
        validate_group_review(doc,proposal,md,tmp_path,result,extra_text_block_ids=['index'])
    with pytest.raises(ValueError,match='requires_authoritative_text'):
        prepare_group_review(doc,proposal,md,tmp_path,extra_text_block_ids=['unknown'])
