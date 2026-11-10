import json
import pytest
from backend.app.pipeline.stages.prepare.object_identity_passes import prepare_object_identity_request


def case():
    objects=[{'id':'a','identity':'A','source_refs':['t1']},{'id':'b','identity':'A','source_refs':['t2']}]
    pairs=[{'left_object_id':'a','right_object_id':'b','triggers':[]}]
    request={'messages':[{'role':'user','content':json.dumps({'source_blocks':[{'block_id':'t1','kind':'TEXT','content':'A 10'},{'block_id':'t2','kind':'TEXT','content':'A 20'}],'images_in_attachment_order':[]})}],'image_paths':[]}
    request['messages'].insert(0,{'role':'system','content':'original extraction'})
    return objects,pairs,[request]


def test_pair_request_contains_both_sources_and_does_not_merge():
    objects,pairs,requests=case();r=prepare_object_identity_request(objects,pairs,requests)
    payload=json.loads(r['messages'][1]['content'])
    assert len(payload['candidate_objects_untrusted'])==2
    assert r['source_block_ids']==['t1','t2']
    assert r['pair_ids']==['P001']
    assert not payload['whole_project_context_supplied']


def test_missing_counterpart_and_budget_are_explicit():
    objects,pairs,requests=case()
    with pytest.raises(ValueError,match='budget_exceeded'):
        prepare_object_identity_request(objects,pairs,requests,max_chars=1)
    objects[1]['source_refs']=['missing']
    with pytest.raises(ValueError,match='missing_pair_sources'):
        prepare_object_identity_request(objects,pairs,requests)


def response_case():
    objects,pairs,requests=case();req=prepare_object_identity_request(objects,pairs,requests)
    evidence=[{'type':'text','block_id':'t1','quote':'A 10'},{'type':'text','block_id':'t2','quote':'A 20'}]
    response={'pair_reviews':[{'pair_id':'P001','relation':'unresolved','reason':'Same name but instance unknown','evidence':evidence,'checks':[]}],'findings':[]}
    return req,response


def test_receipt_check_does_not_certify_object_identity():
    from backend.app.pipeline.stages.prepare.object_identity_passes import validate_object_identity_response
    req,response=response_case();v=validate_object_identity_response(req,response)
    assert not v['errors'] and not v['semantic_verified'] and v['automatic_merges']==0
    response['pair_reviews'][0]['relation']='same_instance'
    response['pair_reviews'][0]['evidence'].pop()
    assert any('right_object' in e for e in validate_object_identity_response(req,response)['errors'])


def test_forged_quote_and_missing_pair_cannot_pass():
    from backend.app.pipeline.stages.prepare.object_identity_passes import validate_object_identity_response
    req,response=response_case();response['pair_reviews'][0]['evidence'][0]['quote']='A 99'
    assert any('quote_not_in_source' in e for e in validate_object_identity_response(req,response)['errors'])
    response['pair_reviews']=[]
    assert 'incomplete_or_duplicate_pair_coverage' in validate_object_identity_response(req,response)['errors']


def test_cross_packet_review_retains_original_exposure_boundary():
    objects,pairs,requests=case()
    objects[0]['id']='object_inventory_000:a'
    objects[1]['id']='object_inventory_001:b'
    pairs[0].update(left_object_id=objects[0]['id'],right_object_id=objects[1]['id'])
    source=json.loads(requests[0]['messages'][1]['content'])
    split=[]
    for i,block in enumerate(source['source_blocks']):
        payload={'packet_index':i,'source_blocks':[block],'images_in_attachment_order':[]}
        split.append({'messages':[{'role':'system','content':'inventory'},
            {'role':'user','content':json.dumps(payload)}],'image_paths':[]})
    result=prepare_object_identity_request(objects,pairs,split)
    payload=json.loads(result['messages'][1]['content'])
    boundaries=payload['candidate_extraction_boundaries']
    assert boundaries[0]['boundary']['supplied_block_ids']==['t1']
    assert boundaries[1]['boundary']['supplied_block_ids']==['t2']
    assert all(b['boundary_known'] for b in boundaries)
    assert len(payload['source_blocks'])==2


def test_unknown_extraction_boundary_is_not_guessed_from_cited_sources():
    objects,pairs,requests=case()
    payload=json.loads(prepare_object_identity_request(objects,pairs,requests)['messages'][1]['content'])
    assert all(b['boundary'] is None and not b['boundary_known'] for b in payload['candidate_extraction_boundaries'])
