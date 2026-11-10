"""Focused graphic operation over model-requested sources, with explicit context omissions."""
import argparse
import asyncio
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .render_graphic_detail import render
from .run_itp_pipe_operations import BASE, immutable
from backend.app.pipeline.stages.prepare.object_operation_followup import freeze_operation_sources, verify_operation_source_binding
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
SOURCE=Path('/tmp/auditmanager_itp_inventory_navigation_priority_followup_20261111')
ROOT=Path('/tmp/auditmanager_itp_pipe_graphic_window_20261111')
BLOCKS=['blk_6d5640f1a71b440cb7ca3f2c177af08a','blk_94b58ecf5d404f7d90c272d267eb2e48','blk_049ee88c136045da9910995a4c44a1e8']

def prepare():
    ROOT.mkdir(exist_ok=True)
    _,original,parent=load_frozen_operation(SOURCE,BASE/'02_work/document.md')
    previous=json.loads((SOURCE/'result.json').read_text());assert not previous['is_error']
    queries={q for r in previous['json_data']['object_operation_results'] for q in r.get('context_queries',[])}
    assert set(BLOCKS)<=queries
    packet=json.loads(original.messages[-1]['content'])
    pdf=BASE/'02_work/document.pdf';pdf_hash=hashlib.sha256(pdf.read_bytes()).hexdigest()
    blocks=BASE/'02_work/blocks.json';tiles=[]
    for i,bid in enumerate(BLOCKS):
        folder=ROOT/f'tiles_{i}'
        if (folder/'manifest.json').exists():
            manifest=json.loads((folder/'manifest.json').read_text())
            assert manifest['pdf_sha256']==pdf_hash and manifest['blocks_sha256']==hashlib.sha256(blocks.read_bytes()).hexdigest()
        else:manifest=render(pdf,blocks,bid,pdf_hash,folder,columns=3,rows=1 if i==0 else 2,scale=3)
        tiles.extend(manifest['tiles'])
    assert len(tiles)==15
    for tile in tiles:assert hashlib.sha256(Path(tile['path']).read_bytes()).hexdigest()==tile['sha256']
    md=(BASE/'02_work/document.md').read_text()
    import re
    hs=list(re.finditer(r'^### BLOCK[^\n]*?\[([^]]+)\]:\s*(\S+)[^\n]*$',md,re.M))
    all_blocks={h[2]:{'block_id':h[2],'kind':h[1].upper(),'content':md[h.start():hs[i+1].start() if i+1<len(hs) else len(md)]} for i,h in enumerate(hs)}
    context=[b for b in packet['source_context'] if b['kind'] in ('TEXT','TABLE')]
    context.extend(all_blocks[bid] for bid in BLOCKS)
    omitted=[b['block_id'] for b in packet['source_context'] if b['kind'] not in ('TEXT','TABLE') and b['block_id'] not in BLOCKS]
    packet.update(source_context=context,attached_image_ids_in_order=[t['block_id'] for t in tiles],
        allowed_source_refs=sorted({b['block_id'] for b in context}),prior_operation_response_untrusted=previous['json_data'],
        omitted=[{'block_id':bid,'reason':'not_resent_in_focused_graphic_window'} for bid in omitted],scope_complete=False,
        focused_graphic_window={'parent_request_sha256':parent['request_sha256'],'selection':'Exact source IDs requested by prior model; no evaluator answer supplied.',
            'boundary':'New focused operation window, not a lossless continuation. Prior graphic observations are untrusted; only current images may support new graphic claims.'},
        graphic_detail_tiles=[{k:t[k] for k in ['block_id','page_index','clip_rotated_page_points','grid_row','grid_column','width','height']} for t in tiles])
    crops=BASE/'03_analysis/runs/control/blocks_stage02_100';index=json.loads((crops/'index.json').read_text())
    binding=freeze_operation_sources(BASE/'02_work/document.md',crops,{b['block_id']:b.get('file') for b in index['blocks']},None,detail_tiles=tiles)
    messages=[dict(original.messages[0]),{'role':'user','content':json.dumps(packet,ensure_ascii=False)}]
    messages[0]['content']+='\nThis focused window preserves all earlier authoritative TEXT but resends only requested graphics at higher resolution. Prior graphic observations are untrusted. Resolve only what current sources prove; list remaining unperformed checks. Tiles overlap and are not extra instances. Never infer same pipe from diameter alone.\n'
    request=replace(original,messages=messages,image_paths=tuple(Path(t['path']) for t in tiles),continuation_source=binding)
    assert verify_operation_source_binding(request) is None
    wire={'messages':messages,'image_paths':[str(p) for p in request.image_paths]}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',{'request_sha256':evidence_digest(wire),'source_binding':binding,'parent_request_sha256':parent['request_sha256'],'model':'codex/gpt-6-astra','automatic_retries':0,'history_or_engineer_answers_supplied':False,'known_source_control':True})
    print('PREPARED',sum(len(m['content']) for m in messages),'chars',len(tiles),'images',flush=True)
    return request

async def main(execute):
    request=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    if (ROOT/'result.json').exists():result=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='object_operations',project_id='ITP-pipe-graphic-window',image_paths=request.image_paths,allowed_tools=''))
        immutable(ROOT/'result.json',result);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'];assert verify_operation_source_binding(request) is None
    immutable(ROOT/'validated.json',validate_object_operation_payload(request,result['json_data']))
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');a=p.parse_args();asyncio.run(main(a.execute))
