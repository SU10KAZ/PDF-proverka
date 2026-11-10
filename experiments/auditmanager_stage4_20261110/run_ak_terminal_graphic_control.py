"""Read an entire AK connection drawing as coordinate-bound graphic tiles."""
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from .render_graphic_detail import render
from .run_km_graphic_recovery import INSTRUCTIONS
from .run_ak_cabinet_inventory_control import BASE
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT=Path('/tmp/auditmanager_ak_terminal_graphic_control_20261110')
BID='blk_cd9d763d198246859c3df42c18b5c3d8'

def save(path,data):path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')

def prepare():
    ROOT.mkdir(exist_ok=True);source=BASE/'02_work';pdf=source/'document.pdf'
    digest=hashlib.sha256(pdf.read_bytes()).hexdigest();folder=ROOT/'tiles'
    if (folder/'manifest.json').exists():
        tiles=json.loads((folder/'manifest.json').read_text())
        assert tiles['pdf_sha256']==digest
        assert tiles['blocks_sha256']==hashlib.sha256((source/'blocks.json').read_bytes()).hexdigest()
    else:tiles=render(pdf,source/'blocks.json',BID,digest,folder,columns=6,rows=1,scale=3)
    for t in tiles['tiles']:assert hashlib.sha256(Path(t['path']).read_bytes()).hexdigest()==t['sha256']
    instruction=INSTRUCTIONS+'''
For connection diagrams extract each equipment connection separately: equipment mark,
its own contact labels, upstream cabinet identity, cabinet-side terminal strip label
(if explicitly visible), terminal numbers, cable mark, and stated common/bridge role.
Read the literal labels; never complete numbering by sequence or by another circuit.
Distinguish device contacts from cabinet contacts and intermediate terminal blocks.
When a label is unreadable, record uncertainty rather than a guessed value.
Preserve overlaps by coordinates; they are not extra circuits. This is extraction,
not confirmation of an expected defect. No previous model answers are supplied.
'''
    meta=[{k:t[k] for k in ['block_id','page_index','clip_rotated_page_points','width','height']} for t in tiles['tiles']]
    request={'messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps({'source_blocks':[BID],'images_in_order':meta,'grid_columns':6,'grid_rows':1},ensure_ascii=False)}],'image_paths':[t['path'] for t in tiles['tiles']]}
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
        result=asdict(await run_codex_json_messages(request['messages'],model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='graphic_inventory_recovery',project_id='AK-terminal-graphic',image_paths=request['image_paths'],allowed_tools=''))
        save(output,result);save(ROOT/'attempt.json',{'status':'finished','is_error':result['is_error']})
    assert not result['is_error'];prepare()
    print('COMPLETE',flush=True)

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
