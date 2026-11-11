"""Review F014 from its original references and exercise native context retrieval.

Known-candidate control, not blind discovery. No prior decisions/answers supplied.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow, direct_cli_executor
from .run_itp_pipe_operations import immutable

CONTROL=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')
BASE=CONTROL/'inputs/KM/versions/v001'
ROOT=Path('/tmp/auditmanager_km_f014_sheet_priority_20261111')


def prepare():
    ROOT.mkdir(exist_ok=True)
    baseline=CONTROL/'snapshots/KM/findings_verify.json'
    original=next(f for f in json.loads(baseline.read_text())['findings'] if f['id']=='F-014')
    # Preserve the exact original finding and only its existing source addresses.
    # Model-facing request preparation uses an explicit allow-list of claim fields.
    from copy import deepcopy
    from backend.app.pipeline.stages.findings_review.deterministic_critic import _referenced_block_ids
    image_refs,text_refs,_=_referenced_block_ids(original)
    bids=list(dict.fromkeys(image_refs+text_refs))
    finding=deepcopy(original)
    finding.update(source_evidence_review_status='pending',source_review_requests=[{'source_finding_id':'classic-KM-F014','source_claim':'\n'.join(str(original.get(k,'')) for k in ('problem','description','solution')),'block_ids':bids}])
    md_path=BASE/'02_work/document.md';md=md_path.read_text();queue=build_source_review_queue([finding],md)
    crops=BASE/'03_analysis/runs/control/blocks_stage02_100';attach_graphic_receipts(queue,md,crops)
    request=prepare_source_review_request(queue,finding,md,max_images=8)
    payload=json.loads(request['messages'][1]['content'])
    assert not payload['unavailable_images'] and not payload['budgeted_text_block_ids']
    binding={'baseline_sha256':hashlib.sha256(baseline.read_bytes()).hexdigest(),'md_sha256':hashlib.sha256(md_path.read_bytes()).hexdigest(),
        'source_context_ids':bids,'prior_responses_or_engineer_answers_supplied':False,'known_candidate_control':True,'source_context_selection':'Only addresses already present in original finding; additional context must be requested and retrieved by native workflow.', 'maximum_new_calls':5,'initial_review':'exact saved response replayed; subsequent responses fresh',
        'message_chars':sum(len(m['content']) for m in request['messages']),'images':len(request['image_paths']),
        'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),'model':'codex/gpt-6-astra','automatic_retries':0}
    for name,value in [('finding',finding),('queue',queue),('request',request),('binding',binding)]:immutable(ROOT/(name+'.json'),value)
    print('PREPARED',json.dumps(binding,ensure_ascii=False),flush=True)
    return finding,queue,md,crops,binding


async def main(execute):
    finding,queue,md,crops,binding=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
    parent=Path('/tmp/auditmanager_km_f014_original_refs_20261111/workflow/00_review')
    first_wire=json.loads((parent/'request.json').read_text())['request']
    first_result=json.loads((parent/'result.json').read_text())
    immutable(ROOT/'paired_control.json',{'initial_request_sha256':evidence_digest(first_wire),'initial_result_sha256':evidence_digest(first_result),'initial_response_replayed':True,'fresh_followups':True,'blind_discovery':False})
    direct=direct_cli_executor('codex/gpt-6-astra',project_id='KM-F014-sheet-priority')
    async def invoke(request,phase):
        assert hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest()==binding['md_sha256']
        if phase=='review' and evidence_digest(request)==evidence_digest(first_wire):
            return first_result
        return await direct(request,phase)
    result=await run_source_review_workflow(queue,finding,md,crops,ROOT/'workflow',invoke=invoke,
        executor_id='recorded-initial-fresh-astra-followup-sheet-priority-v1',max_images=8,repair_quote_spacing=True)
    print('COMPLETE',result['status'],flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
