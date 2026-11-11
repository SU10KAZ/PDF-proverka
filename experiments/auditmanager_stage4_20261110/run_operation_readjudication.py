"""Repeat only adjudication of a saved source review, without previous decision."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_workflow import prepare_adjudication_request, direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest


async def main(args):
    source=Path(args.source_root);root=Path(args.output_root);root.mkdir(parents=True,exist_ok=True)
    old=json.loads((source/'workflow/candidate.json').read_text());entry=old['entry']
    reviews=sorted((source/'workflow').glob('[0-9][0-9]_review/request.json'))
    original=json.loads(reviews[-1].read_text())['request']
    request=prepare_adjudication_request(original,entry['response'])
    md=Path(args.md_path).read_text()
    assert hashlib.sha256(md.encode()).hexdigest()==entry['queue']['source_md_sha256']
    def verify():
        assert Path(args.md_path).read_text()==md
        for image in request['image_bindings']:
            assert hashlib.sha256(Path(image['path']).read_bytes()).hexdigest()==image['sha256']
    verify()
    manifest={'source_candidate_sha256':evidence_digest(old),'request_sha256':evidence_digest(request),
        'model':'codex/gpt-6-astra','previous_adjudication_supplied':False,'history_or_engineer_answers_supplied':False,
        'automatic_retries':0,'message_chars':sum(len(m['content']) for m in request['messages']),
        'images':len(request['image_paths']),'reason':'Clarify generic distinction between supported comparison inputs and a supported defect proposition.'}
    immutable(root/'request.json',request);immutable(root/'binding.json',manifest)
    print('PREPARED',json.dumps(manifest),flush=True)
    if not args.execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    if (root/'result.json').exists():result=json.loads((root/'result.json').read_text())
    else:
        with (root/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=await direct_cli_executor('codex/gpt-6-astra',project_id=args.finding_id+'-readjudication')(request,'adjudication')
        immutable(root/'result.json',result);(root/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    verify();assert not result['is_error']
    new_entry={**entry,'decision':result['json_data']}
    finding=json.loads((source/'finding.json').read_text());assert finding['id']==args.finding_id
    document,receipts=apply_review_admissions({'findings':[finding]},[new_entry],md)
    immutable(root/'candidate.json',{'entry':new_entry,'admission_receipts':receipts,'semantic_acceptance':False,'original_findings_mutated':False})
    immutable(root/'local_output.json',document)
    print('COMPLETE',json.dumps(receipts),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for name in ('source-root','output-root','md-path','finding-id'):p.add_argument('--'+name,required=True)
    p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args()))
