"""Finish one added operation candidate against a frozen full report snapshot."""
import argparse
import asyncio
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check
from backend.app.pipeline.stages.findings_verify.source_review_completion_workflow import run_with_citation_completion
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor

async def main(args):
    root=args.output_root;root.mkdir(parents=True,exist_ok=True)
    paths=[args.candidate,args.baseline,args.md]
    hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    candidate=json.loads(args.candidate.read_text());assert candidate['status']=='candidate_ready'
    entry=candidate['entry'];fid=entry['response']['finding_id']
    original,=[p['finding_snapshot'] for p in entry['queue']['packets'] if p['finding_id']==fid]
    baseline=json.loads(args.baseline.read_text());assert isinstance(baseline,dict)
    assert all(f['id']!=fid for f in baseline['findings']), 'Addition must have a new ID'
    document=deepcopy(baseline);document['findings'].append(original)
    md=args.md.read_text();prepared=prepare_final_check(entry,document,md);request=prepared['request']
    manifest={'input_sha256':hashes,'binding':prepared['binding'],'model':'codex/gpt-6-astra','effort':'high',
              'message_chars':sum(len(m['content']) for m in request['messages']),
              'image_count':len(request['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
              'baseline_count':len(baseline['findings']),'maximum_model_calls':3,'automatic_retries':0,
              'known_candidate_control':True,'engineer_answers_supplied':False,'expected_verdict_supplied':False,
              'boundary':'Snapshot plus one candidate, not a full independent discovery audit. Original report never modified.'}
    immutable(root/'preparation.json',manifest);immutable(root/'input_document.json',document)
    immutable(root/'initial_request.json',request)
    print('PREPARED',json.dumps(manifest),flush=True)
    if not args.execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    after,receipt=await run_with_citation_completion(entry,document,md,root/'workflow',
        invoke=direct_cli_executor('codex/gpt-6-astra',project_id=fid+'-final-release'),executor_id='codex/gpt-6-astra:high:final-check-v1')
    assert hashes=={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    assert [f for f in after['findings'] if f['id']!=fid]==baseline['findings']
    immutable(root/'output_preview.json',after);immutable(root/'receipt.json',receipt)
    immutable(root/'completion.json',{'status':receipt['status'],'original_rows_unchanged':True,
        'baseline_count':len(baseline['findings']),'output_count':len(after['findings']),
        'full_acceptance':False,'production_changed':False})
    print('COMPLETE',receipt['status'],flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser()
    for name in ('candidate','baseline','md','output-root'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args()))
