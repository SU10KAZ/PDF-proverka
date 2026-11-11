import json
from PIL import Image
import pytest
from backend.app.pipeline.stages.prepare.object_inventory_passes import build_object_inventory_passes


def test_all_source_blocks_once_and_graphics_bounded(tmp_path):
    md=tmp_path/'document.md';images=tmp_path/'graphics';images.mkdir()
    rows=[];index=[]
    for i in range(7):
        kind='TEXT' if i%2==0 else 'IMAGE'
        rows.append(f'### BLOCK #{i} [{kind}]: b{i}\nSource {i}\n')
        if kind=='IMAGE':
            Image.new('RGB',(10,10),'white').save(images/f'{i}.png');index.append({'block_id':f'b{i}','file':f'{i}.png'})
    md.write_text(''.join(rows));(images/'index.json').write_text(json.dumps({'blocks':index}))
    requests,receipt=build_object_inventory_passes(md,images,max_images=1)
    ids=[bid for p in receipt['packets'] for bid in p['block_ids']]
    assert ids==[f'b{i}' for i in range(7)] and len(set(ids))==7
    assert all(len(r.image_paths)<=1 for r in requests)
    assert receipt['input_coverage_complete'] and not receipt['semantic_inventory_complete']
    assert len(requests)==3
    (images/'1.png').unlink()
    _,receipt=build_object_inventory_passes(md,images,max_images=1)
    assert not receipt['input_coverage_complete']
    assert receipt['unavailable_graphics']==[{'block_id':'b1','reason':'graphic_unavailable'}]


def test_duplicate_ids_fail_instead_of_overwriting_sources(tmp_path):
    md=tmp_path/'document.md';md.write_text('### BLOCK #1 [TEXT]: same\na\n### BLOCK #2 [TEXT]: same\nb')
    with pytest.raises(ValueError,match='duplicate_source_block_ids'):
        build_object_inventory_passes(md,tmp_path)


def test_large_drawing_is_isolated_without_reencoding(tmp_path):
    md=tmp_path/'document.md';images=tmp_path/'graphics';images.mkdir()
    rows=[];index=[];original={}
    for i,size in enumerate([50,150,50]):
        bid=f'g{i}';name=f'{i}.png';original[name]=bytes([i])*size
        (images/name).write_bytes(original[name]);index.append({'block_id':bid,'file':name})
        rows.append(f'### BLOCK #{i} [IMAGE]: {bid}\nDrawing {i}\n')
    md.write_text(''.join(rows));(images/'index.json').write_text(json.dumps({'blocks':index}))
    requests,receipt=build_object_inventory_passes(md,images,max_image_bytes=100,max_single_image_bytes=200)
    assert [p['block_ids'] for p in receipt['packets']]==[['g0'],['g1'],['g2']]
    assert [p['isolated_large_image'] for p in receipt['packets']]==[False,True,False]
    assert [p['image_bytes'] for p in receipt['packets']]==[50,150,50]
    assert receipt['input_coverage_complete']
    assert all(p.read_bytes()==original[p.name] for r in requests for p in r.image_paths)
    with pytest.raises(ValueError,match='single_source_exceeds_budget:g1'):
        build_object_inventory_passes(md,images,max_image_bytes=100,max_single_image_bytes=149)


def test_oversize_text_still_fails_without_truncation(tmp_path):
    md=tmp_path/'document.md';images=tmp_path/'graphics';images.mkdir()
    md.write_text('### BLOCK #1 [TEXT]: b1\n'+'a'*101)
    (images/'index.json').write_text('{"blocks": []}')
    with pytest.raises(ValueError,match='single_source_exceeds_budget:b1'):
        build_object_inventory_passes(md,images,max_chars=100)
