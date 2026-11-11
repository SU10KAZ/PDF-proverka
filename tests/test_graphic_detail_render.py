import hashlib
import json
import fitz
import pytest
from backend.app.pipeline.stages.prepare.graphic_detail_render import render


def sources(tmp_path,kind='image'):
    pdf=tmp_path/'source.pdf';doc=fitz.open();page=doc.new_page(width=300,height=200)
    page.draw_line((20,20),(280,180));doc.save(pdf);doc.close()
    blocks=tmp_path/'blocks.json';blocks.write_text(json.dumps({'coordinate_space':'normalized_page_top_left',
        'blocks':[{'block_id':'g','block_type':kind,'coords_norm':[0.1,0.1,0.9,0.9],'page_index':0}]}))
    return pdf,blocks,hashlib.sha256(pdf.read_bytes()).hexdigest()


def test_graphic_tiles_are_bound_to_original_source_and_regions(tmp_path):
    pdf,blocks,digest=sources(tmp_path);r=render(pdf,blocks,'g',digest,tmp_path/'tiles',columns=3,rows=3,scale=2)
    assert len(r['tiles'])==9 and r['pdf_sha256']==digest
    assert not r['outside_original_crop'] and not r['model_call_performed'] and not r['semantic_verified']
    for tile in r['tiles']:
        from pathlib import Path
        assert hashlib.sha256(Path(tile['path']).read_bytes()).hexdigest()==tile['sha256']
        x0,y0,x1,y1=tile['clip_rotated_page_points']
        assert 30<=x0<x1<=270 and 20<=y0<y1<=180


@pytest.mark.parametrize('reason',['text','version','geometry'])
def test_renderer_refuses_text_or_unbound_sources(tmp_path,reason):
    pdf,blocks,digest=sources(tmp_path,kind='text' if reason=='text' else 'image')
    if reason=='version':digest='wrong'
    if reason=='geometry':
        data=json.loads(blocks.read_text());data['blocks'][0]['coords_norm']=[0.9,0.1,0.1,0.8];blocks.write_text(json.dumps(data))
    with pytest.raises(ValueError):render(pdf,blocks,'g',digest,tmp_path/'tiles')
    assert not (tmp_path/'tiles').exists()
