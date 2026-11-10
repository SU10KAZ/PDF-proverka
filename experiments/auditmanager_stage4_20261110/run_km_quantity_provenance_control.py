"""Same source/context KM control; only generic quantity provenance instructions change."""
import argparse
import asyncio
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .run_km_quantity_operations import BASE, immutable
from backend.app.pipeline.stages.prepare.quantity_provenance import QUANTITY_PROVENANCE_INSTRUCTIONS
from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT = Path('/tmp/auditmanager_km_quantity_provenance_control_20261111')
SOURCE = Path('/tmp/auditmanager_km_quantity_operations_20261110/obj_09/context_3')

def prepare():
    ROOT.mkdir(exist_ok=True)
    _, original, binding = load_frozen_operation(SOURCE, BASE/'02_work/document.md')
    messages = [dict(m) for m in original.messages]
    messages[0]['content'] += QUANTITY_PROVENANCE_INSTRUCTIONS
    request = replace(original, messages=messages)
    assert request.messages[1:] == original.messages[1:]
    assert request.image_paths == original.image_paths
    wire = {'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
    manifest = {'request_sha256':evidence_digest(wire),'original_request_sha256':binding['request_sha256'],
        'source_binding':request.continuation_source,'only_change':'Generic quantity provenance instruction',
        'model':'codex/gpt-6-astra','reasoning_effort':'high','automatic_retries':0,
        'history_or_engineer_answers_supplied':False,'failed_context3_response_supplied':False,
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
        result=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='object_operations',project_id='KM-quantity-provenance-control',image_paths=request.image_paths,allowed_tools=''))
        immutable(output,result);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'];assert verify_operation_source_binding(request) is None
    immutable(ROOT/'validated.json',validate_object_operation_payload(request,result['json_data']))
    print('COMPLETE',flush=True)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
