"""Continue the frozen pipe operation through the actual runtime helper, without resetting its budget."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .run_itp_pipe_operations import BASE,immutable
from backend.app.pipeline.stages.prepare.object_operation_disposition import advance_operation_pass
from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor

PARENT=Path('/tmp/auditmanager_itp_pipe_graphic_window_20261111')
ROOT=Path('/tmp/auditmanager_itp_pipe_runtime_followup_20261111')


def prepare():
    ROOT.mkdir(exist_ok=True)
    _,request,binding=load_frozen_operation(PARENT,BASE/'02_work/document.md')
    previous=json.loads((PARENT/'result.json').read_text());assert not previous['is_error']
    validated,followup=advance_operation_pass(request,previous['json_data'],enable_disposition=True)
    if followup is None:raise ValueError('No new runtime request prepared')
    assert verify_operation_source_binding(followup) is None
    packet=json.loads(followup.messages[-1]['content'])
    wire={'messages':followup.messages,'image_paths':[str(p) for p in followup.image_paths]}
    manifest={'parent_request_sha256':binding['request_sha256'],'parent_result_sha256':evidence_digest(previous),
        'request_sha256':evidence_digest(wire),'source_binding':followup.continuation_source,
        'source_md_sha256':hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest(),
        'context_revision':packet.get('context_revision'),'budget_reset':False,
        'message_chars':sum(len(m['content']) for m in wire['messages']),'image_count':len(wire['image_paths']),
        'image_bytes':sum(Path(p).stat().st_size for p in wire['image_paths']),
        'model':'codex/gpt-6-astra','effort':'high','engineer_answers_supplied':False,'historical_control_answers_supplied':False,
        'prior_model_response_supplied_as_untrusted':True,'known_development_control':True,'automatic_retries':0,'maximum_new_model_calls':1}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',manifest);immutable(ROOT/'parent_validation.json',validated)
    frozen=asdict(followup);frozen['image_paths']=wire['image_paths'];immutable(ROOT/'targeted_pass.json',frozen)
    print('PREPARED',json.dumps({k:v for k,v in manifest.items() if k!='source_binding'}),flush=True)
    return followup,wire,manifest

async def main(execute):
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    request,wire,manifest=prepare()
    if not execute:return
    output=ROOT/'result.json'
    if output.exists():
        result=json.loads(output.read_text())
        assert json.loads((ROOT/'result_binding.json').read_text())=={'request':manifest['request_sha256'],'result':evidence_digest(result)}
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started','request_sha256':manifest['request_sha256']},f)
        result=await direct_cli_executor('codex/gpt-6-astra',project_id='ITP-pipe-runtime-followup')(wire,'object_operation_followup')
        immutable(output,result);immutable(ROOT/'result_binding.json',{'request':manifest['request_sha256'],'result':evidence_digest(result)})
    assert verify_operation_source_binding(request) is None
    if result.get('is_error') or not isinstance(result.get('json_data'),dict):raise ValueError('Provider result unusable')
    validated,next_request=advance_operation_pass(request,result['json_data'],enable_disposition=True)
    immutable(ROOT/'validated.json',validated)
    if next_request is not None:
        frozen=asdict(next_request);frozen['image_paths']=[str(p) for p in next_request.image_paths]
        immutable(ROOT/'next_request_prepared_not_dispatched.json',frozen)
    summary={'status':'response_validated','retrieval':validated.get('context_retrieval'),
        'operation_statuses':[{k:r.get(k) for k in ('operation','status','reason')} for r in validated['object_operation_results']],
        'new_findings':len(validated.get('findings',[])),'next_request_prepared':next_request is not None,
        'prior_candidates_preserved':'parent_validation.json','full_acceptance':False,'budget_reset':False}
    immutable(ROOT/'completion.json',summary);print('COMPLETE',json.dumps(summary,ensure_ascii=False),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
