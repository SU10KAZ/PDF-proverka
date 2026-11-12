"""Re-adjudicate unchanged AR source review using current shared output policy."""
import argparse
import asyncio
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_workflow import prepare_adjudication_request, direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from .run_primary_text_runtime_probe import sha, write

SOURCE=Path('/tmp/auditmanager_ar_rope_bundle_review_20261111')
ROOT=Path('/tmp/auditmanager_ar_rope_current_profile_20261112')


def prepare():
    ROOT.mkdir(exist_ok=True)
    candidate=json.loads((SOURCE/'workflow/candidate.json').read_text())
    entry=candidate['entry']
    original=json.loads((SOURCE/'workflow/00_review/request.json').read_text())['request']
    request=prepare_adjudication_request(original,entry['response'])
    bundle=json.loads((SOURCE/'bundle.json').read_text())
    import hashlib
    assert hashlib.sha256(bundle['md'].encode()).hexdigest()==entry['queue']['source_md_sha256']
    assert request['image_paths']==[b['path'] for b in request['image_bindings']]
    for b in request['image_bindings']:assert sha(Path(b['path']))==b['sha256']
    finding=json.loads((SOURCE/'finding.json').read_text())
    binding={'source_candidate_sha256':sha(SOURCE/'workflow/candidate.json'),
        'source_bundle_sha256':sha(SOURCE/'bundle.json'),'request_sha256':evidence_digest(request),
        'model':'codex/gpt-6-astra','reasoning_effort':'high','engineer_answers_supplied':False,
        'previous_adjudication_or_local_profile_supplied':False,'known_case_not_blind':True,
        'message_chars':sum(len(m['content'])for m in request['messages']),
        'image_count':len(request['image_paths']),'automatic_retries':0}
    for name,data in [('request',request),('binding',binding)]:
        p=ROOT/(name+'.json')
        if p.exists():assert json.loads(p.read_text())==data
        else:write(p,data)
    return entry,request,bundle,finding,binding


async def main(execute):
    entry,request,bundle,finding,binding=prepare()
    print(json.dumps(binding,ensure_ascii=False),flush=True)
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    with (ROOT/'attempt.json').open('x')as f:json.dump({'status':'started','pid':os.getpid()},f)
    result=await direct_cli_executor('codex/gpt-6-astra',project_id='AR-rope-current-profile')(request,'adjudication')
    write(ROOT/'result.json',result)
    write(ROOT/'attempt.json',{'status':'finished','is_error':result['is_error']})
    assert not result['is_error']
    assert sha(SOURCE/'workflow/candidate.json')==binding['source_candidate_sha256']
    assert sha(SOURCE/'bundle.json')==binding['source_bundle_sha256']
    for b in request['image_bindings']:assert sha(Path(b['path']))==b['sha256']
    updated={**entry,'decision':result['json_data']}
    output,receipts=apply_review_admissions({'findings':[finding]},[updated],bundle['md'])
    write(ROOT/'candidate.json',{'entry':updated,'receipts':receipts,'semantic_acceptance':False})
    write(ROOT/'output.json',output)
    print('COMPLETE',json.dumps(receipts),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');a=p.parse_args()
    asyncio.run(main(a.execute))
