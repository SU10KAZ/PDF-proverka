"""Check actual pump inventory object with the already bound circuit context.

Known-source control. No earlier operation answers or historical findings sent.
"""
import argparse
import asyncio
from dataclasses import replace,asdict
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .run_itp_pipe_operations import BASE,immutable
from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes,validate_object_operation_payload
from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_itp_gvs_pump_context_20261111')
SOURCE=Path('/tmp/auditmanager_itp_gvs_graphic_window_20261111')
OBJECT_ID='k15_view'
INVENTORY=Path('/tmp/auditmanager_itp_gvs_source_inventory_20261111/ITP/execution/object_inventory_000/result.json')


def prepare():
    ROOT.mkdir(exist_ok=True)
    _,context,parent=load_frozen_operation(SOURCE,BASE/'02_work/document.md')
    inventory=json.loads(INVENTORY.read_text());assert not inventory['is_error']
    objects=inventory['json_data']['object_registry'];target,=[o for o in objects if o['id']==OBJECT_ID]
    immutable(ROOT/'02_text_analysis.json',{'object_registry':[target]+[o for o in objects if o['id']!=target['id']], 'text_findings':[]})
    crops=ROOT/'blocks_stage02_100'
    if not crops.exists():crops.symlink_to(BASE/'03_analysis/runs/control/blocks_stage02_100',target_is_directory=True)
    base,=build_object_operation_passes(ROOT,BASE/'02_work/document.md',max_requests=1)
    template=json.loads(base.messages[-1]['content']);packet=json.loads(context.messages[-1]['content'])
    for key in ('object','operations','inventory_source_navigation'):packet[key]=template[key]
    packet['operations']=[op for op in packet['operations'] if op['operation'] in ('identity_across_views','document_links','calculation_consistency')]
    assert len(packet['operations'])==3
    for key in ('prior_operation_response_untrusted','context_searches','context_revision'):packet.pop(key,None)
    packet['context_origin']='Previously retrieved original source window reused for another actually inventoried object. No previous operation answer supplied. Scope incomplete; current images only.'
    messages=[base.messages[0],{'role':'user','content':json.dumps(packet,ensure_ascii=False)}]
    request=replace(context,messages=messages)
    assert verify_operation_source_binding(request) is None
    wire={'messages':messages,'image_paths':[str(p) for p in request.image_paths]}
    immutable(ROOT/'request.json',wire)
    immutable(ROOT/'binding.json',{'request_sha256':evidence_digest(wire),'source_binding':request.continuation_source,
        'context_parent_sha256':parent['request_sha256'],'actual_inventory_object_id':OBJECT_ID,
        'model':'codex/gpt-6-astra','previous_operation_answers_supplied':False,
        'history_or_engineer_answers_supplied':False,'automatic_retries':0,'known_source_control':True})
    print('PREPARED',sum(len(m['content']) for m in messages),len(request.image_paths),'images',flush=True)
    return request


async def main(execute):
    request=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    if (ROOT/'result.json').exists():result=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='object_operations',project_id='ITP-GVS-pump-context',image_paths=request.image_paths,allowed_tools=''))
        immutable(ROOT/'result.json',result);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'];assert verify_operation_source_binding(request) is None
    immutable(ROOT/'validated.json',validate_object_operation_payload(request,result['json_data']))
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true')
    p.add_argument('--object-id',default=OBJECT_ID);p.add_argument('--output-root',default=str(ROOT))
    args=p.parse_args();OBJECT_ID=args.object_id;ROOT=Path(args.output_root)
    asyncio.run(main(args.execute))
