"""Continue the sealed EOM operation with its own queries and bound detail tiles."""
import argparse
import asyncio
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .run_eom_instance_operations import immutable, BASE
from backend.app.pipeline.stages.prepare.object_operation_followup import freeze_operation_sources, build_object_operation_followup, verify_operation_source_binding
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
SOURCE=Path('/tmp/auditmanager_eom_instance_operations_20261110/hvac_32')
ROOT=Path('/tmp/auditmanager_eom_instance_followup_20261111')

def prepare():
    ROOT.mkdir(exist_ok=True)
    _,original,old_binding=load_frozen_operation(SOURCE,BASE/'02_work/document.md')
    crops=BASE/'03_analysis/runs/control/blocks_stage02_100'
    index=json.loads((crops/'index.json').read_text())
    files={b['block_id']:b.get('file') for b in index['blocks']}
    detail=json.loads(Path('/tmp/auditmanager_eom_instance_graphic_control_20261110/manifest.json').read_text())['source_tiles']['tiles']
    binding=freeze_operation_sources(BASE/'02_work/document.md',crops,files,None,detail_tiles=detail)
    original=replace(original,continuation_source=binding)
    assert verify_operation_source_binding(original) is None
    raw=json.loads((SOURCE/'result.json').read_text());assert not raw['is_error']
    request,receipt=build_object_operation_followup(original,raw['json_data'])
    immutable(ROOT/'retrieval.json',receipt)
    if request is None: return None
    assert verify_operation_source_binding(request) is None
    wire={'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
    immutable(ROOT/'request.json',wire)
    immutable(ROOT/'binding.json',{'request_sha256':evidence_digest(wire),'source_binding':binding,
        'original_request_sha256':old_binding['request_sha256'],'model':'codex/gpt-6-astra',
        'reasoning_effort':'high','automatic_retries':0,'history_or_engineer_answers_supplied':False,
        'known_source_control':True,'query_origin':'Previous model response, untrusted; no evaluator target added'})
    return request

async def main(execute):
    request=prepare()
    print('PREPARED',sum(len(m['content']) for m in request.messages) if request else 0,'chars',len(request.image_paths) if request else 0,'images',flush=True)
    if not request or not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    output=ROOT/'result.json'
    if output.exists():result=json.loads(output.read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='object_operations',project_id='EOM-instance-followup',image_paths=request.image_paths,allowed_tools=''))
        immutable(output,result);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error']
    assert verify_operation_source_binding(request) is None
    immutable(ROOT/'validated.json',validate_object_operation_payload(request,result['json_data']))
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');args=p.parse_args();asyncio.run(main(args.execute))
