"""Same finding/source controls with sealed GRAPHIC details available to reviewers."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import re
import shutil
from . import run_eom_cable_source_workflow as workflow
from .run_eom_cable_operations import BASE, immutable
ROOT=Path('/tmp/auditmanager_eom_cable_detail_source_workflow_20261111')

def prepare_graphics():
    ROOT.mkdir(exist_ok=True);dest=ROOT/'graphics';dest.mkdir(exist_ok=True)
    original=BASE/'03_analysis/runs/control/blocks_stage02_100'
    for p in original.glob('block_*.png'):
        target=dest/p.name
        if target.exists():assert hashlib.sha256(target.read_bytes()).digest()==hashlib.sha256(p.read_bytes()).digest()
        else:shutil.copyfile(p,target)
    document=json.loads((workflow.OUTPUT/'03_findings.json').read_text());rows=[]
    sources=[]
    for fid,block in [('F-050',0),('F-051',1)]:
        finding=next(f for f in document['findings'] if f['id']==fid)
        root=Path(f'/tmp/auditmanager_eom_cable_graphic_control_20261110/block_{block:03d}')
        manifest=json.loads((root/'manifest.json').read_text());tiles=manifest['source_tiles']
        assert hashlib.sha256((BASE/'02_work/document.pdf').read_bytes()).hexdigest()==tiles['pdf_sha256']
        ids=sorted({int(n) for e in finding['evidence'] for n in re.findall(r'detail_tile_(\d+)',json.dumps(e,ensure_ascii=False))})
        assert ids
        for i in ids:
            tile=tiles['tiles'][i];raw=Path(tile['path']).read_bytes();assert hashlib.sha256(raw).hexdigest()==tile['sha256']
            name=f'detail_{block}_{i}.png';target=dest/name
            if target.exists():assert target.read_bytes()==raw
            else:target.write_bytes(raw)
            rows.append({'block_id':tile['block_id'],'file':name,'sha256':tile['sha256'],'page_index':tile['page_index'],
                'clip_rotated_page_points':tile['clip_rotated_page_points'],'source_pdf_sha256':tiles['pdf_sha256']})
        sources.append({'finding_id':fid,'selection':'Tile references in current candidate evidence, not a historical expected label','tile_indices':ids,'source_manifest_sha256':hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest()})
    immutable(dest/'graphic_detail_manifest.json',{'source_md_sha256':hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest(),'images':rows})
    immutable(ROOT/'detail_preparation.json',{'sources':sources,'original_graphics_preserved':True,'no_TEXT_raster':True})
    workflow.ROOT=ROOT;workflow.GRAPHIC_DIR=dest

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');args=p.parse_args()
    prepare_graphics();asyncio.run(workflow.main(args.execute))
