"""Known-candidate verification after independent operation results were sealed."""
import argparse
import asyncio
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
from .run_ar_roof_operations import immutable
from .frozen_operation_request import load_frozen_operation
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
ROOT=Path('/tmp/auditmanager_km_handrail_claim_check_20261111')
PREVIOUS=Path('/tmp/auditmanager_km_bent_handrail_followup_20261111')
MD=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001/02_work/document.md')


def prepare():
    ROOT.mkdir(exist_ok=True)
    _,request,_=load_frozen_operation(PREVIOUS,MD)
    # Read only the original candidate, never engineer decisions or later labels.
    register=json.loads(Path('experiments/auditmanager_stage1_20261109/register.json').read_text())
    row=next(c for c in register['controls'] if c['control_id']=='KM:5')['historical_record']
    candidate={k:row[k] for k in ('original_title','original_claim','location')}
    packet=json.loads(request.messages[-1]['content'])
    packet['untrusted_candidate_to_check']=candidate
    instruction='''\nKNOWN-CANDIDATE VERIFICATION, not independent discovery: check every factual and geometric assertion in untrusted_candidate_to_check against supplied sources and images. The candidate is not an answer key. Read the actual referenced dimensions, determine their endpoints and physical quantity, and check whether the asserted arithmetic compares the same complete object. Separate correct arithmetic, observed marks, unresolved geometry and proven contradictions. Do not assume either a defect or correctness. Return candidate_claim_checks with claim, verdict (supported/refuted/needs_data), exact source evidence and reason, in addition to the usual operation results and any justified findings. Preserve uncertainty and source scope. No historical decisions or expected verdicts are supplied.\n'''
    messages=[{**request.messages[0],'content':request.messages[0]['content']+instruction},{'role':'user','content':json.dumps(packet,ensure_ascii=False)}]
    request=replace(request,messages=messages)
    assert verify_operation_source_binding(request) is None
    wire={'messages':messages,'image_paths':[str(p) for p in request.image_paths]}
    binding={'request_sha256':evidence_digest(wire),'source_binding':request.continuation_source,
        'known_candidate_supplied':True,'independent_discovery':False,'engineer_answers_or_later_labels_supplied':False,
        'independent_result_already_sealed':str(PREVIOUS/'result.json'),'model':'codex/gpt-6-astra','automatic_retries':0}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',binding)
    print('PREPARED',sum(len(m['content']) for m in messages),'chars',len(request.image_paths),'images',flush=True)
    return request


async def main(execute):
    request=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    if (ROOT/'result.json').exists():r=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        r=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='object_operations',project_id='KM-known-candidate-check',image_paths=request.image_paths,allowed_tools=''))
        immutable(ROOT/'result.json',r);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':r['is_error']}))
    assert not r['is_error'];assert verify_operation_source_binding(request) is None
    immutable(ROOT/'validated.json',validate_object_operation_payload(request,r['json_data']))
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
