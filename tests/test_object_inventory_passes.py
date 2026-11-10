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
