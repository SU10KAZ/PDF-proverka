from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import fitz
import pytest

from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass
from backend.app.pipeline.stages.prepare.object_operation_followup import (
    freeze_operation_sources, build_object_operation_followup, verify_operation_source_binding)
from backend.app.pipeline.stages.prepare.object_graphic_followup import freeze_graphic_source


def setup(tmp_path):
    pdf = tmp_path/'source.pdf'
    doc = fitz.open(); page = doc.new_page(width=300, height=200)
    page.draw_line((20,20),(280,180)); doc.save(pdf)
    image = tmp_path/'g.png'; page.get_pixmap().save(image); doc.close()
    blocks = tmp_path/'blocks.json'
    blocks.write_text(json.dumps({'coordinate_space':'normalized_page_top_left','blocks':[
        {'block_id':'g','block_type':'image','coords_norm':[0.1,0.1,0.9,0.9],'page_index':0}]}))
    md = tmp_path/'document.md'
    graphic = '### BLOCK #1 [GRAPHIC]: g\nSource diagram\n'
    text = '### BLOCK #2 [TEXT]: t\nAuthoritative text\n'
    md.write_text(graphic + text)
    binding = freeze_operation_sources(md,tmp_path,{'g':'g.png'},None)
    binding['graphic_source'] = freeze_graphic_source(pdf,blocks,tmp_path/'details')
    packet = {'object':{'id':'obj'},'operations':[{'operation':'document_links'}],
              'attached_image_ids_in_order':['g'],'allowed_source_refs':['g','t'],'omitted':[],
              'source_context':[{'block_id':'g','kind':'GRAPHIC','content':graphic},
                                {'block_id':'t','kind':'TEXT','content':text}]}
    request = CodexTargetedPass('object_operations_test','result.json',
                               [{'role':'system','content':'Inspect sources'},
                                {'role':'user','content':json.dumps(packet)}],
                               (image,),continuation_source=binding)
    payload = {'object_operation_results':[{'object_id':'obj','operation':'document_links',
               'status':'needs_data','reason':'Unreadable branches', 'source_refs':['g'],
               'graphic_detail_requests':[{'block_id':'g','reason':'Read branch labels'}]}],
               'findings':[{'problem':'Preserve earlier candidate'}]}
    return request,payload,pdf,blocks,md


def test_full_graphic_details_preserve_sources_findings_and_binding(tmp_path):
    request,payload,*_ = setup(tmp_path)
    original = deepcopy(request.messages)
    followup,receipt = build_object_operation_followup(request,payload)
    assert receipt['status']=='prepared' and receipt['route']=='graphic_detail'
    assert len(followup.image_paths)==10 and followup.image_paths[0]==str(request.image_paths[0])
    old = json.loads(request.messages[-1]['content']); new = json.loads(followup.messages[-1]['content'])
    assert new['source_context']==old['source_context']
    assert new['operations']==old['operations']
    assert new['prior_operation_response_untrusted']==payload
    assert new['context_revision']==1 and new['attached_image_ids_in_order']==['g']*10
    assert new['graphic_detail_views'][0]['semantic_verified'] is False
    assert request.messages==original
    assert verify_operation_source_binding(followup) is None
    repeat,limit = build_object_operation_followup(followup,payload)
    assert repeat is None and limit['graphic_detail']['status']=='graphic_details_already_supplied'
    Path(followup.image_paths[-1]).write_bytes(b'changed')
    assert verify_operation_source_binding(followup)=='image_bytes_changed'


@pytest.mark.parametrize('changed',['pdf','blocks','md'])
def test_changed_sources_refuse_details(tmp_path,changed):
    request,payload,pdf,blocks,md = setup(tmp_path)
    path = {'pdf':pdf,'blocks':blocks,'md':md}[changed]
    path.write_bytes(path.read_bytes()+b'changed')
    followup,receipt = build_object_operation_followup(request,payload)
    assert followup is None and receipt['status']=='context_preparation_failed'
    assert not (tmp_path/'details').exists()


def test_authoritative_text_cannot_be_requested_as_graphic(tmp_path):
    request,payload,*_ = setup(tmp_path)
    payload['object_operation_results'][0]['graphic_detail_requests'][0]['block_id']='t'
    followup,receipt = build_object_operation_followup(request,payload)
    assert followup is None and 'supplied GRAPHIC' in receipt['reason']
    assert not (tmp_path/'details').exists()


def test_full_image_budget_preserves_previous_views(tmp_path):
    request,payload,*_ = setup(tmp_path)
    packet = json.loads(request.messages[-1]['content'])
    packet['attached_image_ids_in_order']=['g']*16
    request = replace(request,image_paths=request.image_paths*16,
                      messages=[request.messages[0],{'role':'user','content':json.dumps(packet)}])
    followup,receipt = build_object_operation_followup(request,payload)
    assert followup is None and receipt['graphic_detail']['status']=='graphic_image_budget'
    assert not (tmp_path/'details').exists()


def test_no_explicit_request_does_not_render(tmp_path):
    request,payload,*_ = setup(tmp_path)
    del payload['object_operation_results'][0]['graphic_detail_requests']
    followup,receipt = build_object_operation_followup(request,payload)
    assert followup is None
    assert not (tmp_path/'details').exists()


def test_builder_opt_in_freezes_source_and_adds_model_contract(tmp_path):
    from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes
    _,_,pdf,_,md = setup(tmp_path)
    pdf.rename(md.with_suffix('.pdf'))
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[
        {'id':'obj','identity':'branch','scope':'sheet 1','kind':'pipe','source_refs':['g']}]}))
    with pytest.raises(ValueError,match='requires followups'):
        build_object_operation_passes(tmp_path,md,enable_graphic_details=True)
    request, = build_object_operation_passes(tmp_path,md,enable_followup=True,enable_graphic_details=True)
    assert request.continuation_source['graphic_source']['pdf_sha256']==hashlib.sha256(md.with_suffix('.pdf').read_bytes()).hexdigest()
    assert 'graphic_detail_requests' in request.messages[0]['content']
    legacy, = build_object_operation_passes(tmp_path,md,enable_followup=True)
    assert 'graphic_source' not in legacy.continuation_source
    assert 'graphic_detail_requests' not in legacy.messages[0]['content']


@pytest.mark.parametrize('size,expected', [((1200,200),(3,1)),((200,1200),(1,3))])
def test_long_drawings_use_full_extent_three_tile_strip(tmp_path,size,expected):
    from backend.app.pipeline.stages.prepare.object_graphic_followup import choose_detail_grid
    request,_,pdf,blocks,_=setup(tmp_path)
    doc=fitz.open();doc.new_page(width=size[0],height=size[1]);doc.save(pdf);doc.close()
    source=freeze_graphic_source(pdf,blocks,tmp_path/'details')
    assert choose_detail_grid(source,'g',16)==expected
    assert choose_detail_grid(source,'g',3)==expected
    assert choose_detail_grid(source,'g',2)==(0,0)


def test_graphic_and_counterpart_requests_share_one_revision(tmp_path):
    request,payload,_,_,md=setup(tmp_path)
    md.write_text(md.read_text()+'### BLOCK #3 [TEXT]: b\nCounterpart calculation\n')
    binding=deepcopy(request.continuation_source)
    binding['md_sha256']=hashlib.sha256(md.read_bytes()).hexdigest()
    request=replace(request,continuation_source=binding)
    payload['object_operation_results'][0]['context_queries']=['Counterpart calculation']
    followup,receipt=build_object_operation_followup(request,payload)
    assert receipt['status']=='prepared'
    assert receipt['simultaneous_context_retrieval']['status']=='prepared'
    packet=json.loads(followup.messages[-1]['content'])
    assert packet['context_revision']==1
    assert set(packet['allowed_source_refs'])=={'g','t','b'}
    assert len(followup.image_paths)==10
    assert packet['prior_operation_response_untrusted']==payload
    assert verify_operation_source_binding(followup) is None
