"""One citation-only proposal and one independent full check on a known control."""
import argparse
import asyncio
from copy import deepcopy
import json
import os
from pathlib import Path
from .citation_completion import prepare,apply_candidate
from .run_km_final_check_pair import BASE,CANDIDATES
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_release_workflow import run_final_release_check

ROOT=Path('/tmp/auditmanager_km_citation_completion_20261111')

def write_once(path,value):
    if path.exists():
        if json.loads(path.read_text())!=value:raise ValueError('Changed artifact: '+str(path))
    else:
        with path.open('x') as f:json.dump(value,f,ensure_ascii=False,indent=2)

async def main(execute):
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    entry=json.loads(CANDIDATES['version_b'].read_text())['entry']
    md=(BASE/'inputs/KM/versions/v001/02_work/document.md').read_text()
    document=json.loads((BASE/'snapshots/KM/findings_verify.json').read_text())
    original=next(p['finding_snapshot'] for p in entry['queue']['packets'] if p['finding_id']=='F-005')
    document['findings']=[original if f['id']=='F-005' else f for f in document['findings']]
    check=json.loads(Path('/tmp/auditmanager_km_final_check_pair_20261111/version_b/receipt.json').read_text())['review']
    request,binding=prepare(entry,document,md,check);ROOT.mkdir(exist_ok=True)
    manifest={'binding':binding,'request_sha256':evidence_digest(request),'model':'codex/gpt-6-astra','effort':'high',
        'message_chars':sum(len(m['content']) for m in request['messages']),
        'image_count':len(request['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
        'known_development_control':True,'engineer_answers_supplied':False,'expected_verdict_supplied':False,
        'prior_citation_failure_supplied_to_repair_only':True,'automatic_retries':0,'maximum_model_calls':2}
    write_once(ROOT/'preparation.json',manifest);write_once(ROOT/'request.json',request)
    print('PREPARED',json.dumps(manifest),flush=True)
    if not execute:return
    invoke=direct_cli_executor('codex/gpt-6-astra',project_id='KM-citation-completion')
    result_path=ROOT/'result.json'
    if result_path.exists():
        result=json.loads(result_path.read_text())
        assert json.loads((ROOT/'result_binding.json').read_text())=={'request':manifest['request_sha256'],'result':evidence_digest(result)}
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'dispatched','request':manifest['request_sha256']},f)
        result=await invoke(deepcopy(request),'citation_completion')
        write_once(result_path,result);write_once(ROOT/'result_binding.json',{'request':manifest['request_sha256'],'result':evidence_digest(result)})
    if result.get('is_error') or not isinstance(result.get('json_data'),dict):raise ValueError('Invalid provider response')
    current_request,_=prepare(entry,document,md,check)
    if evidence_digest(current_request)!=manifest['request_sha256']:raise ValueError('Sources changed during dispatch')
    revised=apply_candidate(entry,document,md,check,result['json_data'])
    write_once(ROOT/'candidate.json',{'entry':revised,'semantic_acceptance':False,'repair_binding':binding})
    print('CITATIONS_PREPARED',len(result['json_data']['additions']),flush=True)
    after,receipt=await run_final_release_check(revised,document,md,ROOT/'final_check',invoke=invoke,
        executor_id='codex/gpt-6-astra:high:final-check-v1')
    write_once(ROOT/'output_preview.json',after);write_once(ROOT/'receipt.json',receipt)
    print('COMPLETE',receipt['status'],flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
