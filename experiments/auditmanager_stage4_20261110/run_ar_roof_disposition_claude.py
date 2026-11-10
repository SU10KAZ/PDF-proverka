"""Same-input Claude comparison of terminal review of a real unresolved AR operation; no evaluator answers."""
import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
from .run_ar_roof_operations import BASE,immutable
from .frozen_operation_request import load_frozen_operation
from backend.app.pipeline.stages.prepare.object_operation_disposition import prepare_operation_disposition
from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_ar_roof_disposition_claude_20261111')
PREVIOUS=Path('/tmp/auditmanager_ar_roof_operations_20261111/P03/context_1')


def prepare():
    ROOT.mkdir(exist_ok=True)
    _,original,_=load_frozen_operation(PREVIOUS,BASE/'02_work/document.md')
    result=json.loads((PREVIOUS/'result.json').read_text());assert not result['is_error']
    request,search=prepare_operation_disposition(original,result['json_data'])
    wire={'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
    binding={'request_sha256':evidence_digest(wire),'previous_result_sha256':evidence_digest(result),
             'source_binding':request.continuation_source,'search_receipt':search,'model':'claude/claude-sonnet-5-5',
             'automatic_retries':0,'history_or_engineer_answers_supplied':False,
             'scope':'Known-object control of generic terminal disposition, not whole-project absence or automatic engineer escalation.'}
    assert wire == json.loads(Path('/tmp/auditmanager_ar_roof_disposition_20261111/request.json').read_text())
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',binding)
    print('PREPARED',sum(len(m['content']) for m in request.messages),'chars',len(request.image_paths),'images',flush=True)
    return request


async def main(execute):
    request=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.claude_json_runner import run_claude_json_messages
    output=ROOT/'result.json'
    if output.exists():result=json.loads(output.read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=asdict(await run_claude_json_messages(request.messages,model='claude/claude-sonnet-5-5',reasoning_effort='high',timeout=900,stage='object_operations',project_id='AR-terminal-disposition',image_paths=request.image_paths))
        immutable(output,result);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'];assert verify_operation_source_binding(request) is None
    immutable(ROOT/'validated.json',validate_object_operation_payload(request,result['json_data'],context_search_exhausted=True))
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
