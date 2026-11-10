"""Repeat the actual KM object operation with existing source-bound detail tiles."""
import argparse
import asyncio
from dataclasses import asdict,replace
import hashlib
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .run_km_geometry_operations import BASE,immutable
from backend.app.pipeline.stages.prepare.object_operation_followup import freeze_operation_sources,verify_operation_source_binding
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_km_bent_handrail_detail_20261111')
PREVIOUS=Path('/tmp/auditmanager_km_geometry_operations_20261110/p4_19')
MANIFEST=Path(__file__).with_name('KM_packet004_graphic_recovery.json')


def prepare():
    ROOT.mkdir(exist_ok=True)
    _,request,_=load_frozen_operation(PREVIOUS,BASE/'02_work/document.md')
    tiles=json.loads(MANIFEST.read_text())
    assert hashlib.sha256((BASE/'02_work/document.pdf').read_bytes()).hexdigest()==tiles['pdf_sha256']
    assert hashlib.sha256((BASE/'02_work/blocks.json').read_bytes()).hexdigest()==tiles['blocks_sha256']
    packet=json.loads(request.messages[-1]['content']);images=list(request.image_paths)
    assert all(t['block_id'] in packet['allowed_source_refs'] for t in tiles['tiles'])
    for t in tiles['tiles']:
        p=Path(t['path']);assert hashlib.sha256(p.read_bytes()).hexdigest()==t['sha256']
        images.append(p);packet['attached_image_ids_in_order'].append(t['block_id'])
    packet['graphic_detail_views']=[{k:v for k,v in t.items() if k!='path'} for t in tiles['tiles']]
    packet['graphic_detail_boundary']='Coordinate-bound overlapping views of the same graphic, not additional objects. Inspect previously unreadable labels; do not substitute a horizontal projection or incomplete chain for a complete developed length. No expected dimensions or verdicts supplied.'
    crops=BASE/'03_analysis/runs/control/blocks_stage02_100';index=json.loads((crops/'index.json').read_text())
    source=freeze_operation_sources(BASE/'02_work/document.md',crops,{b['block_id']:b.get('file') for b in index['blocks']},None,detail_tiles=tiles['tiles'])
    request=replace(request,messages=[request.messages[0],{'role':'user','content':json.dumps(packet,ensure_ascii=False)}],image_paths=tuple(images),continuation_source=source)
    assert verify_operation_source_binding(request) is None
    wire={'messages':request.messages,'image_paths':[str(p) for p in images]}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',{'request_sha256':evidence_digest(wire),'source_binding':source,'tile_manifest_sha256':hashlib.sha256(MANIFEST.read_bytes()).hexdigest(),'model':'codex/gpt-6-astra','automatic_retries':0,'expected_answers_supplied':False,'scope':'Known-object detail recovery, not whole-project discovery.'})
    print('PREPARED',sum(len(m['content']) for m in request.messages),'chars',len(images),'images',sum(p.stat().st_size for p in images),'bytes',flush=True)
    return request


async def main(execute):
    request=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    if (ROOT/'result.json').exists():r=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        r=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='object_operations',project_id='KM-bent-handrail-detail',image_paths=request.image_paths,allowed_tools=''))
        immutable(ROOT/'result.json',r);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':r['is_error']}))
    assert not r['is_error'];assert verify_operation_source_binding(request) is None
    immutable(ROOT/'validated.json',validate_object_operation_payload(request,r['json_data']))
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
