"""Prepare source-bound readable tiles for unresolved GRAPHIC inventory blocks."""
import hashlib
import json
from pathlib import Path

from .render_graphic_detail import render

BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001/02_work')
COVERAGE=Path('/tmp/auditmanager_whole_project_inventory_20261110/KM/execution/aggregate_coverage.json')
DEST=Path('/tmp/auditmanager_km_unresolved_graphics_20261110')
PDF_SHA='e8d9ad046e7c3207717ac1d26c94b3f0c5fb8559cfa40d5300bba29518bff11e'


def main():
    blocks=json.loads((BASE/'blocks.json').read_text())
    index={b['block_id']:b for b in blocks['blocks']}
    coverage=json.loads(COVERAGE.read_text())
    assert hashlib.sha256((BASE/'document.pdf').read_bytes()).hexdigest()==PDF_SHA
    rows=[];non_graphic=[]
    for item in coverage['source_inventory']:
        if item['status'] not in ('needs_data','not_checked'):continue
        bid=item['block_id'];block=index[bid]
        if block['block_type']!='image':
            non_graphic.append(item);continue
        folder=DEST/bid
        if (folder/'manifest.json').exists():
            manifest=json.loads((folder/'manifest.json').read_text())
            assert manifest['pdf_sha256']==PDF_SHA
            for tile in manifest['tiles']:
                assert hashlib.sha256(Path(tile['path']).read_bytes()).hexdigest()==tile['sha256']
        else:
            manifest=render(BASE/'document.pdf',BASE/'blocks.json',bid,PDF_SHA,folder,columns=4,scale=3)
        rows.append({'block_id':bid,'previous_coverage':item,'manifest':manifest,
            'recovery_status':'tiles_prepared_not_rechecked','same_crop_limits_preserved':True})
    result={'project':'KM','coverage_path':str(COVERAGE),'coverage_sha256':hashlib.sha256(COVERAGE.read_bytes()).hexdigest(),
        'graphic_blocks':len(rows),'tiles':sum(len(r['manifest']['tiles']) for r in rows),
        'image_bytes':sum(Path(t['path']).stat().st_size for r in rows for t in r['manifest']['tiles']),
        'records':rows,'non_graphic_unresolved':non_graphic,'new_model_calls':0,'semantic_coverage_complete':False,
        'limits':['Resolution improves legibility; it does not recover content outside the original crop.',
            'Inventory linkage conflicts require reconciliation, not just rendering.',
            'No authoritative TEXT was re-extracted or corrected.','Prepared tiles are not proof that model reading or engineering checks are complete.']}
    Path(__file__).with_name('KM_unresolved_graphics_preparation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:result[k] for k in ('graphic_blocks','tiles','image_bytes','new_model_calls')}))

if __name__=='__main__':main()
