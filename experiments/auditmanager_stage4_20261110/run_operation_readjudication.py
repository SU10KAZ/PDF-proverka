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
    peer_binding = None
    if args.current_findings:
        peer_path = Path(args.current_findings)
        peer_document = json.loads(peer_path.read_text())
        fields = ('id', 'problem', 'description', 'solution', 'severity', 'report_section', 'evidence', 'sheet', 'page')
        peers = [{k: f[k] for k in fields if k in f} for f in peer_document['findings'] if f.get('id') != args.finding_id]
        assert len({f['id'] for f in peers}) == len(peers)
        payload = json.loads(request['messages'][1]['content'])
        payload['untrusted_current_report_peers'] = peers
        request['messages'][1]['content'] = json.dumps(payload, ensure_ascii=False)
        request['messages'][0]['content'] += "\nCurrent report peers are supplied only to assess presentation overlap, not as evidence of truth. Do not change other findings. Do not promote a peer discrepancy into the principal basis of this finding merely to retain its technical category. Preserve every independently supported unique clause of the original finding. If a proposition also appears in a peer, distinguish contextual cross-reference from a new separate defect; do not erase a unique aggregate calculation or unresolved original clause. Explain partial overlap and retained unique scope in reason. Choose the output profile on that retained scope. Missing evidence is not a false claim. Do not use peers as engineer answers.\n"
        peer_binding = {'path':str(peer_path),'sha256':hashlib.sha256(peer_path.read_bytes()).hexdigest(),'count':len(peers)}
    md=Path(args.md_path).read_text()
    assert hashlib.sha256(md.encode()).hexdigest()==entry['queue']['source_md_sha256']
    def verify():
        assert Path(args.md_path).read_text()==md
        if peer_binding:
            assert hashlib.sha256(Path(peer_binding['path']).read_bytes()).hexdigest()==peer_binding['sha256']
        for image in request['image_bindings']:
            assert hashlib.sha256(Path(image['path']).read_bytes()).hexdigest()==image['sha256']
    verify()
    manifest={'source_candidate_sha256':evidence_digest(old),'request_sha256':evidence_digest(request),
        'model':'codex/gpt-6-astra','previous_adjudication_supplied':False,'history_or_engineer_answers_supplied':False,
        'current_report_peers':peer_binding,
        'automatic_retries':0,'message_chars':sum(len(m['content']) for m in request['messages']),
        'images':len(request['image_paths']),'reason':'Review same-source claims with explicit final evidence selection and optional current-report overlap context.'}
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
    p.add_argument('--current-findings');p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args()))
