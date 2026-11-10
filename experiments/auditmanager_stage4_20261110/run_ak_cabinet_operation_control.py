"""One model-extracted cabinet through the actual identity-operation builder."""
import argparse
import asyncio
from dataclasses import asdict,replace
import hashlib
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes,validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from .run_ak_cabinet_inventory_control import ROOT as INVENTORY,BASE


def prepare(packet_index,object_id,revision):
    root=Path('/tmp/auditmanager_ak_cabinet_operations_20261110')/revision/f'packet_{packet_index:03d}'
    root.mkdir(parents=True,exist_ok=True)
    source=INVENTORY/'AK/execution'/f'object_inventory_{packet_index:03d}'/'result.json'
    result=json.loads(source.read_text());assert not result['is_error']
    objects=[o for o in result['json_data']['object_registry'] if o['id']==object_id]
    assert len(objects)==1
    data={'object_registry':objects,'text_findings':[]}
    path=root/'02_text_analysis.json'
    if path.exists():assert json.loads(path.read_text())==data
    else:path.write_text(json.dumps(data,ensure_ascii=False,indent=2))
    crops=root/'blocks_stage02_100'
    if not crops.exists():crops.symlink_to(BASE/'03_analysis/runs/control/blocks_stage02_100',target_is_directory=True)
    request,=build_object_operation_passes(root,BASE/'02_work/document.md',max_requests=1)
    packet=json.loads(request.messages[-1]['content'])
    packet['operations']=[o for o in packet['operations'] if o['operation']=='identity_across_views']
    assert len(packet['operations'])==1
    request=replace(request,messages=[request.messages[0],{'role':'user','content':json.dumps(packet,ensure_ascii=False)}])
    wire={'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
    binding={'request_sha256':evidence_digest(wire),'inventory_result_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
        'md_sha256':hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest(),
        'images':[{'path':str(p),'sha256':hashlib.sha256(Path(p).read_bytes()).hexdigest()} for p in request.image_paths],
        'model':'codex/gpt-6-astra','reasoning_effort':'high','max_calls':1,'automatic_retries':0,
        'scope':'Known-source model-extracted cabinet; identity operation only. No historical answers or expected finding supplied.'}
    for name,data in [('request',wire),('binding',binding)]:
        path=root/(name+'.json')
        if path.exists():assert json.loads(path.read_text())==data
        else:path.write_text(json.dumps(data,ensure_ascii=False,indent=2))
    return root,request,binding

async def main(args):
    root,request,binding=prepare(args.packet,args.object_id,args.revision)
    print('PREPARED',root,flush=True)
    if not args.execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    output=root/'result.json'
    if output.exists():result=json.loads(output.read_text())
    else:
        with (root/'attempt.json').open('x') as f:json.dump({'status':'started',**binding},f)
        result=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',
            timeout=900,stage='object_operations',project_id='AK-cabinet-identity',image_paths=request.image_paths,allowed_tools=''))
        output.write_text(json.dumps(result,ensure_ascii=False,indent=2))
        (root/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error']
    assert all(hashlib.sha256(Path(i['path']).read_bytes()).hexdigest()==i['sha256'] for i in binding['images'])
    assert hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest()==binding['md_sha256']
    validated=validate_object_operation_payload(request,result['json_data'])
    (root/'validated.json').write_text(json.dumps(validated,ensure_ascii=False,indent=2))
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--packet',type=int,required=True);p.add_argument('--object-id',required=True);p.add_argument('--execute',action='store_true');p.add_argument('--revision',choices=['local_claim_v2','page_context_v3'],default='page_context_v3')
    asyncio.run(main(p.parse_args()))
