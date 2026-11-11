"""Review every original F005 assertion using project context previously located.

Known-candidate control, not blind discovery. No prior decisions/answers supplied.
"""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow, direct_cli_executor
from .run_itp_pipe_operations import immutable

CONTROL=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')
BASE=CONTROL/'inputs/KM/versions/v001'
ROOT=Path('/tmp/auditmanager_km_f005_compound_review_20261111')


def prepare():
    ROOT.mkdir(exist_ok=True)
    baseline=CONTROL/'snapshots/KM/findings_verify.json'
    original=next(f for f in json.loads(baseline.read_text())['findings'] if f['id']=='F-005')
    # Only source IDs from previous retrieval; never its claims or adjudication.
    prior=json.loads((Path(__file__).parent/'km_lk6_quantity_full_context_evidence/workflow/candidate.json').read_text())
    packet=prior['entry']['queue']['packets'][0]
    bids=[s['block_id'] for s in packet['sources']]+packet['required_graphic_block_ids']
    finding={k:original[k] for k in ('id','problem','description','solution','severity','norm','norm_quote','risk','sheet','page','evidence') if k in original}
    finding.update(source_evidence_review_status='pending',source_review_requests=[{'source_finding_id':'classic-KM-F005','source_claim':'\n'.join(str(original.get(k,'')) for k in ('problem','description','solution')),'block_ids':bids}])
    md_path=BASE/'02_work/document.md';md=md_path.read_text();queue=build_source_review_queue([finding],md)
    crops=BASE/'03_analysis/runs/control/blocks_stage02_100';attach_graphic_receipts(queue,md,crops)
    request=prepare_source_review_request(queue,finding,md,max_images=16)
    payload=json.loads(request['messages'][1]['content'])
    assert not payload['unavailable_images'] and not payload['budgeted_text_block_ids']
    binding={'baseline_sha256':hashlib.sha256(baseline.read_bytes()).hexdigest(),'md_sha256':hashlib.sha256(md_path.read_bytes()).hexdigest(),
        'source_context_ids':bids,'prior_responses_or_engineer_answers_supplied':False,'known_candidate_control':True,
        'message_chars':sum(len(m['content']) for m in request['messages']),'images':len(request['image_paths']),
        'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),'model':'codex/gpt-6-astra','automatic_retries':0}
    for name,value in [('finding',finding),('queue',queue),('request',request),('binding',binding)]:immutable(ROOT/(name+'.json'),value)
    print('PREPARED',json.dumps(binding,ensure_ascii=False),flush=True)
    return finding,queue,md,crops,binding


async def main(execute):
    finding,queue,md,crops,binding=prepare()
    if not execute:return
    direct=direct_cli_executor('codex/gpt-6-astra',project_id='KM-F005-compound')
    async def invoke(request,phase):
        assert hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest()==binding['md_sha256']
        return await direct(request,phase)
    result=await run_source_review_workflow(queue,finding,md,crops,ROOT/'workflow',invoke=invoke,
        executor_id='direct-codex-gpt-6-astra-high',max_images=16,repair_quote_spacing=True)
    print('COMPLETE',result['status'],flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
