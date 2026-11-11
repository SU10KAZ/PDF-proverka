"""Final release check for the native F014 source review; no expected verdict supplied."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check
from backend.app.pipeline.stages.findings_verify.source_review_completion_workflow import run_with_citation_completion
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor

ROOT=Path('/tmp/auditmanager_km_f014_native_final_release_20261111')
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')
CANDIDATES={'native':Path('/tmp/auditmanager_km_f014_native_cross_plan_20261111/workflow/candidate.json')}


def immutable(path,data):
    if path.exists():assert json.loads(path.read_text())==data,str(path)
    else:path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')

async def main(execute, output_root=ROOT, candidate_path=None):
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    source=BASE/'inputs/KM/versions/v001/02_work/document.md'
    md=source.read_text();baseline=json.loads((BASE/'snapshots/KM/findings_verify.json').read_text())
    candidates=CANDIDATES if candidate_path is None else {'native':candidate_path}
    for name,path in candidates.items():
        entry=json.loads(path.read_text())['entry'];fid=entry['response']['finding_id']
        original=next(p['finding_snapshot'] for p in entry['queue']['packets'] if p['finding_id']==fid)
        document={**baseline,'findings':[original if f['id']==fid else f for f in baseline['findings']]}
        prepared=prepare_final_check(entry,document,md);root=output_root/name;root.mkdir(parents=True,exist_ok=True)
        request=prepared['request']
        manifest={'candidate_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
            'binding':prepared['binding'],'message_chars':sum(len(m['content']) for m in request['messages']),
            'image_count':len(request['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
            'model':'codex/gpt-6-astra','effort':'high','engineer_answers_supplied':False,'expected_verdict_supplied':False,
            'prior_reviewer_verdict_supplied':False,'automatic_retries':0,'maximum_model_calls':3,'known_development_control':True}
        immutable(root/'preparation.json',manifest)
        immutable(root/'input_document.json',document)
        immutable(root/'initial_request.json',request)
        print('PREPARED',name,json.dumps(manifest),flush=True)
        if not execute:continue
        after,receipt=await run_with_citation_completion(entry,document,md,root/'workflow',
            invoke=direct_cli_executor('codex/gpt-6-astra',project_id='KM-F014-native-final-check'),executor_id='codex/gpt-6-astra:high:final-check-v1')
        assert source.read_text()==md
        immutable(root/'output_preview.json',after);immutable(root/'receipt.json',receipt)
        print('COMPLETE',name,receipt['status'],flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true')
    parser.add_argument('--output-root',type=Path,default=ROOT)
    parser.add_argument('--candidate',type=Path)
    args=parser.parse_args()
    asyncio.run(main(args.execute,args.output_root,args.candidate))
