"""Continue the real AR operation with source graphics chosen by a navigation model."""
import argparse
import asyncio
from collections import Counter
from dataclasses import asdict,replace
import hashlib
import json
import os
from pathlib import Path
import re
from .run_ar_roof_operations import BASE,immutable
from .frozen_operation_request import load_frozen_operation
from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_ar_selected_source_operations_20261111')
NAV=Path('/tmp/auditmanager_ar_source_navigation_20261111')
PREVIOUS=Path('/tmp/auditmanager_ar_roof_operations_20261111/P03/context_1')


def prepare():
    ROOT.mkdir(exist_ok=True)
    _,request,binding=load_frozen_operation(PREVIOUS,BASE/'02_work/document.md')
    nav=json.loads((NAV/'result.json').read_text());assert not nav['is_error']
    chosen=nav['json_data']['selected_sources'];ids=[r['block_id'] for r in chosen]
    assert ids and len(ids)<=8 and len(ids)==len(set(ids))
    packet=json.loads(request.messages[-1]['content']);md=(BASE/'02_work/document.md').read_text()
    assert hashlib.sha256(md.encode()).hexdigest()==json.loads((NAV/'binding.json').read_text())['source_md_sha256']
    hs=list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$',md,re.M|re.I));counts=Counter(h[2] for h in hs)
    pages=list(re.finditer(r'^##[ \t]+(?:Page|СТРАНИЦА)[ \t]+(\d+)\b',md,re.M|re.I));blocks={};pageof={}
    for i,h in enumerate(hs):
        if counts[h[2]]!=1:continue
        content=md[h.start():hs[i+1].start() if i+1<len(hs) else len(md)]
        content=re.split(r'^##[ \t]+(?:Page|СТРАНИЦА)\b',content,maxsplit=1,flags=re.M|re.I)[0]
        blocks[h[2]]={'block_id':h[2],'kind':h[1].upper(),'content':content}
        preceding=[p for p in pages if p.start()<h.start()];pageof[h[2]]=int(preceding[-1][1]) if preceding else None
    images=list(request.image_paths)
    for bid in ids:
        assert blocks[bid]['kind'] in ('IMAGE','GRAPHIC') and bid not in packet['attached_image_ids_in_order']
        image=request.continuation_source['images'][bid];path=Path(image['path'])
        assert hashlib.sha256(path.read_bytes()).hexdigest()==image['sha256']
        images.append(path);packet['attached_image_ids_in_order'].append(bid)
    wanted_pages={pageof[bid] for bid in ids if pageof[bid] is not None}
    add=ids+[bid for bid,b in blocks.items() if b['kind'] in ('TEXT','TABLE') and pageof[bid] in wanted_pages]
    added=[]
    for bid in dict.fromkeys(add):
        if bid not in packet['allowed_source_refs']:
            packet['source_context'].append(blocks[bid]);packet['allowed_source_refs'].append(bid);added.append(bid)
    previous=json.loads((PREVIOUS/'result.json').read_text());assert not previous['is_error']
    packet['prior_operation_response_untrusted']=previous['json_data']
    packet['source_navigation']={'selected_sources':chosen,'new_source_ids':added,'boundary':'Selection is a navigation hypothesis. Inspect actual graphics and verify scope, level, execution and references before applying any detail. Source summaries are not proof.'}
    packet['scope_complete']=False
    messages=[request.messages[0],{'role':'user','content':json.dumps(packet,ensure_ascii=False)}]
    assert sum(len(m['content']) for m in messages)<=240000 and len(images)<=16
    request=replace(request,stage=request.stage+'_source_navigation',messages=messages,image_paths=tuple(images))
    assert verify_operation_source_binding(request) is None
    wire={'messages':messages,'image_paths':[str(p) for p in images]}
    bound={'request_sha256':evidence_digest(wire),'source_binding':request.continuation_source,'navigation_result_sha256':evidence_digest(nav),'added_source_ids':added,'selected_graphic_ids':ids,'model':'codex/gpt-6-astra','history_or_engineer_answers_supplied':False,'automatic_retries':0}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',bound)
    print('PREPARED',sum(len(m['content']) for m in messages),'chars',len(images),'images',sum(p.stat().st_size for p in images),'bytes',flush=True)
    return request


async def main(execute):
    request=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    if (ROOT/'result.json').exists():r=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        r=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='object_operations',project_id='AR-selected-source-operation',image_paths=request.image_paths,allowed_tools=''))
        immutable(ROOT/'result.json',r);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':r['is_error']}))
    assert not r['is_error'];assert verify_operation_source_binding(request) is None
    immutable(ROOT/'validated.json',validate_object_operation_payload(request,r['json_data']))
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
