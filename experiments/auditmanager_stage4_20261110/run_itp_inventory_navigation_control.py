"""Same ITP operation sources; add current inventory navigation only."""
import argparse
import asyncio
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .run_itp_pipe_operations import BASE, immutable
from backend.app.pipeline.stages.prepare.inventory_source_navigation import build_inventory_source_navigation, INVENTORY_NAVIGATION_INSTRUCTIONS
import re
from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT = Path('/tmp/auditmanager_itp_inventory_navigation_control_20261111')
SOURCE = Path('/tmp/auditmanager_itp_pipe_operations_20261111/inv_036')

def prepare():
    ROOT.mkdir(exist_ok=True)
    _, original, binding = load_frozen_operation(SOURCE, BASE/'02_work/document.md')
    messages = [dict(m) for m in original.messages]
    messages[0]['content'] += INVENTORY_NAVIGATION_INSTRUCTIONS
    inventory_path=Path('/tmp/auditmanager_itp_pipe_source_inventory_20261111/ITP/execution/object_inventory_000/result.json')
    inventory=json.loads(inventory_path.read_text());assert not inventory['is_error']
    md=(BASE/'02_work/document.md').read_text()
    blocks={bid:{'kind':kind} for kind,bid in re.findall(r'^### BLOCK[^\n]*?\[([^]]+)\]:\s*(\S+)',md,re.M)}
    packet=json.loads(messages[-1]['content'])
    packet['inventory_source_navigation']=build_inventory_source_navigation(inventory['json_data']['object_registry'],blocks)
    messages[-1]={'role':'user','content':json.dumps(packet,ensure_ascii=False)}
    request = replace(original, messages=messages)
    compared=json.loads(request.messages[-1]['content']);compared.pop('inventory_source_navigation')
    assert compared==json.loads(original.messages[-1]['content'])
    assert request.image_paths == original.image_paths
    wire = {'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
    manifest = {'request_sha256':evidence_digest(wire),'original_request_sha256':binding['request_sha256'],
        'source_binding':request.continuation_source,'only_change':'Generic current-inventory source catalogue and navigation instruction',
        'model':'codex/gpt-6-astra','reasoning_effort':'high','automatic_retries':0,
        'history_or_engineer_answers_supplied':False,'previous_operation_response_supplied':False,
        'known_control_not_blind':True}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',manifest)
    return request

async def main(execute):
    request=prepare();print('PREPARED',sum(len(m['content']) for m in request.messages),len(request.image_paths),flush=True)
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    output=ROOT/'result.json'
    if output.exists():result=json.loads(output.read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='object_operations',project_id='ITP-inventory-navigation-control',image_paths=request.image_paths,allowed_tools=''))
        immutable(output,result);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'];assert verify_operation_source_binding(request) is None
    immutable(ROOT/'validated.json',validate_object_operation_payload(request,result['json_data']))
    print('COMPLETE',flush=True)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
