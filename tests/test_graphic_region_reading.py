import hashlib
import json
from PIL import Image
import pytest
from backend.app.pipeline.stages.findings_verify.graphic_region_reading import prepare_review_region_readings


def setup(tmp_path):
    p=tmp_path/'source.png';Image.new('RGB',(300,900),'white').save(p)
    digest=hashlib.sha256(p.read_bytes()).hexdigest()
    request={'image_paths':[str(p)],'image_bindings':[{'block_id':'graphic','sha256':digest,'path':str(p)}]}
    response={'claims':[{'claim_id':'C1','claim':'EXPECTED_SECRET_NUMBER','evidence':[{'type':'image','block_id':'graphic','image_sha256':digest,'bbox_norm_xyxy':[0.4,0.1,0.6,0.8]}]}]}
    return request,response


def test_selection_rotation_and_replay_without_answer_leakage(tmp_path):
    request,response=setup(tmp_path)
    result=prepare_review_region_readings(request,response,tmp_path/'regions')
    region,=result['regions']
    assert region['source']['rotation_ccw']==90
    assert 'EXPECTED_SECRET_NUMBER' not in json.dumps(region['request'])
    assert prepare_review_region_readings(request,response,tmp_path/'regions')==result
    with Image.open(region['request']['image_paths'][0]) as image:assert image.width>image.height


def test_unbound_and_changed_source_rejected(tmp_path):
    request,response=setup(tmp_path)
    response['claims'][0]['evidence'][0]['image_sha256']='foreign'
    with pytest.raises(ValueError,match='not_attached'):prepare_review_region_readings(request,response,tmp_path/'regions')


def test_tiny_region_does_not_hide_later_usable_reference(tmp_path):
    request,response=setup(tmp_path)
    from copy import deepcopy
    tiny=deepcopy(response['claims'][0]);tiny['evidence'][0]['bbox_norm_xyxy']=[0,0,0.01,0.01]
    response['claims'].insert(0,tiny)
    result=prepare_review_region_readings(request,response,tmp_path/'regions')
    assert len(result['regions'])==1 and result['skipped'][0]['reason']=='region_under_16_pixels'


def test_reading_region_is_mapped_to_original_attached_pixels(tmp_path):
    from backend.app.pipeline.stages.findings_verify.graphic_region_reading import reading_on_parent
    from backend.app.pipeline.stages.findings_verify.graphic_literal_reading import bind_literal_reading
    request,response=setup(tmp_path)
    region,=prepare_review_region_readings(request,response,tmp_path/'regions')['regions']
    raw={'annotations':[{'literal':'untrusted reading','bbox_norm_xyxy':[0,0,1,1]}],'reading_uncertainties':['edge cropped']}
    bound=bind_literal_reading(region['request'],raw)
    mapped=reading_on_parent(region,bound)
    x0,y0,x1,y1=region['source']['pixel_box']
    assert mapped['reading']['annotations'][0]['bbox_norm_xyxy']==pytest.approx([x0/300,y0/900,x1/300,y1/900])
    assert mapped['image_sha256']==request['image_bindings'][0]['sha256']
    assert mapped['reading']['reading_uncertainties']==['edge cropped']
    assert mapped['derived_view']['original_reading']==raw


def test_new_location_requires_new_reading_but_contained_location_does_not(tmp_path):
    from copy import deepcopy
    from backend.app.pipeline.stages.findings_verify.graphic_region_reading import region_was_read, reading_on_parent
    from backend.app.pipeline.stages.findings_verify.graphic_literal_reading import bind_literal_reading
    request,response=setup(tmp_path)
    region,=prepare_review_region_readings(request,response,tmp_path/'regions')['regions']
    reading=reading_on_parent(region,bind_literal_reading(region['request'],{'annotations':[],'reading_uncertainties':['unreadable']}))
    assert region_was_read(region,[reading])
    moved=deepcopy(region);moved['source']['proposed_bbox_norm_xyxy']=[0,0,0.2,0.2]
    assert not region_was_read(moved,[reading])
    moved['source']['proposed_bbox_norm_xyxy']=[0.45,0.2,0.55,0.6]
    assert region_was_read(moved,[reading])
    moved['source']['parent_image_sha256']='other version'
    assert not region_was_read(moved,[reading])
