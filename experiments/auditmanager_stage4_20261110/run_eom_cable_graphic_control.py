"""Read an entire EOM circuit diagram as coordinate-bound graphic tiles."""
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from .render_graphic_detail import render
from .run_km_graphic_recovery import INSTRUCTIONS
from .run_eom_instance_inventory_control import BASE
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT=Path('/tmp/auditmanager_eom_cable_graphic_control_20261110')
BIDS=['blk_8fac348a820c41aeb2d941339987da14','blk_513e35c4f8a7432986e9a498d4573996']
BID=BIDS[0]
COLUMNS=4

def save(path,data):path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')

def prepare():
    ROOT.mkdir(parents=True,exist_ok=True);source=BASE/'02_work';pdf=source/'document.pdf'
    digest=hashlib.sha256(pdf.read_bytes()).hexdigest();folder=ROOT/'tiles'
    if (folder/'manifest.json').exists():
        tiles=json.loads((folder/'manifest.json').read_text())
        assert tiles['pdf_sha256']==digest
        assert tiles['blocks_sha256']==hashlib.sha256((source/'blocks.json').read_bytes()).hexdigest()
    else:tiles=render(pdf,source/'blocks.json',BID,digest,folder,columns=COLUMNS,rows=2,scale=3)
    for t in tiles['tiles']:assert hashlib.sha256(Path(t['path']).read_bytes()).hexdigest()==t['sha256']
    instruction=INSTRUCTIONS.replace('left-to-right tiles', 'row-major grid tiles')+'\nThis is a recovery pass for unreadable outgoing circuit labels, not another complete\ninventory of apparatus or background objects. Register one object per distinguishable\noutgoing circuit. Record its literal line identifier, full cable designation including\nexecution suffix, conductor count and cross-section, length with units, consumer,\nprotective-device label, and reserve status when visible. Keep these as attributes\nof that circuit; do not multiply every symbol into another circuit. Trace the actual\nvertical/connected line to link labels; never align text by guessed sequence.\nPreserve unknown characters and unresolved connections. Do not infer absent cable\nsupply, add lengths without scope, or substitute a similar cable execution.\nOverlapping grid tiles show the same source; join them by coordinates. Do not count\nthe overlap twice. Do not read raster tables or notes to replace authoritative MD TEXT.\nCoverage is bounded to outgoing-circuit labels; other operations remain unperformed.\nNo expected circuit IDs, lengths, findings or prior model answers are supplied.\n'
    meta=[{k:t[k] for k in ['block_id','page_index','clip_rotated_page_points','width','height']} for t in tiles['tiles']]
    request={'messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps({'source_blocks':[BID],'images_in_order':meta,'grid_columns':COLUMNS,'grid_rows':2},ensure_ascii=False)}],'image_paths':[t['path'] for t in tiles['tiles']]}
    manifest={'request_sha256':evidence_digest(request),'source_tiles':tiles,'expected_answers_supplied':False,'known_source_control':True,'model':'codex/gpt-6-astra','reasoning_effort':'high','automatic_retries':0}
    for name,data in [('request',request),('manifest',manifest)]:
        path=ROOT/(name+'.json')
        if path.exists():assert json.loads(path.read_text())==data
        else:save(path,data)
    return request,manifest

async def main(execute):
    request,manifest=prepare();print('PREPARED',len(request['image_paths']),'tiles',flush=True)
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    output=ROOT/'result.json'
    if output.exists():result=json.loads(output.read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started','request_sha256':manifest['request_sha256']},f)
        result=asdict(await run_codex_json_messages(request['messages'],model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='graphic_inventory_recovery',project_id='EOM-cable-graphic',image_paths=request['image_paths'],allowed_tools=''))
        save(output,result);save(ROOT/'attempt.json',{'status':'finished','is_error':result['is_error']})
    assert not result['is_error'];prepare()
    print('COMPLETE',flush=True)

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');p.add_argument('--block',type=int,choices=(0,1),required=True);a=p.parse_args()
    ROOT=ROOT/f'block_{a.block:03d}';BID=BIDS[a.block];COLUMNS=(4,8)[a.block]
    asyncio.run(main(a.execute))
