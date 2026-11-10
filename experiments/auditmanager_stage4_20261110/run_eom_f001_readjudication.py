"""Re-adjudicate a preserved source review after fixing the category contract."""
import argparse
import asyncio
import json
import os
from pathlib import Path
from .run_eom_instance_operations import BASE, immutable
from backend.app.pipeline.stages.findings_verify.source_review_workflow import prepare_adjudication_request, direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
SOURCE = Path('/tmp/auditmanager_eom_classic_semantics_control_20261111')
ROOT = Path('/tmp/auditmanager_eom_f001_readjudication_20261111')

async def main(execute):
    ROOT.mkdir(exist_ok=True)
    old = json.loads((SOURCE/'F-001/candidate.json').read_text())
    entry = old['entry']
    original = json.loads((SOURCE/'F-001/00_review/request.json').read_text())['request']
    request = prepare_adjudication_request(original, entry['response'])
    md = (BASE/'02_work/document.md').read_text()
    assert entry['queue']['source_md_sha256'] == __import__('hashlib').sha256(md.encode()).hexdigest()
    immutable(ROOT/'request.json', request)
    immutable(ROOT/'binding.json', {'source_candidate_sha256':evidence_digest(old),
        'request_sha256':evidence_digest(request), 'model':'codex/gpt-6-astra',
        'history_or_engineer_answers_supplied':False,'previous_adjudication_supplied':False,
        'automatic_retries':0,'reason':'Previous decision used free prose as report severity.'})
    print('PREPARED', sum(len(m['content']) for m in request['messages']), flush=True)
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    if (ROOT/'result.json').exists():result=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=await direct_cli_executor('codex/gpt-6-astra',project_id='EOM-F001-readjudication')(request,'adjudication')
        immutable(ROOT/'result.json',result)
        (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error']
    assert (BASE/'02_work/document.md').read_text()==md
    new_entry={**entry,'decision':result['json_data']}
    document={'findings':json.loads((SOURCE/'findings.json').read_text())}
    out,receipts=apply_review_admissions(document,[new_entry],md)
    immutable(ROOT/'candidate.json',{'entry':new_entry,'admission_receipts':receipts,
        'semantic_acceptance':False,'original_findings_mutated':False})
    immutable(ROOT/'local_output.json',out)
    print('COMPLETE',json.dumps(receipts),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true')
    asyncio.run(main(p.parse_args().execute))
