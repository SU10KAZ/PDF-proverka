"""Fresh F050 source review with explicit prepared-MD stamps missing from legacy queue."""
import argparse
import asyncio
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from .run_eom_cable_readjudication import BASE,REVIEW,BEFORE
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_metadata import graphic_stamp_metadata
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow,direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_eom_f050_stamp_workflow_20261111')

async def main(execute):
    ROOT.mkdir(exist_ok=True)
    md=(BASE/'02_work/document.md').read_text();document=json.loads(BEFORE.read_text())
    old=json.loads((REVIEW/'F-050/candidate.json').read_text())['entry'];queue=deepcopy(old['queue'])
    finding,=[f for f in document['findings'] if f['id']=='F-050']
    changes=[]
    assert queue['source_md_sha256']==hashlib.sha256(md.encode()).hexdigest()
    for packet in queue['packets']:
        expected=graphic_stamp_metadata(md,packet.get('required_graphic_block_ids',[]))
        previous=packet.get('graphic_metadata',[])
        if previous and previous!=expected:raise ValueError('Existing metadata is inconsistent')
        if expected!=previous:changes.append({'finding_id':packet['finding_id'],'previous':previous,'current':expected})
        packet['graphic_metadata']=expected
    assert changes
    request=prepare_source_review_request(queue,finding,md,max_images=16)
    manifest={'original_queue_sha256':evidence_digest(old['queue']),'new_queue_sha256':evidence_digest(queue),
        'request_sha256':evidence_digest(request),'metadata_changes':changes,
        'message_chars':sum(len(m['content']) for m in request['messages']),
        'image_count':len(request['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
        'model':'codex/gpt-6-astra','effort':'high','automatic_retries':0,'maximum_model_calls':6,
        'previous_review_or_decision_supplied':False,'history_or_engineer_answers_supplied':False,
        'boundary':'Fresh known-candidate review, same frozen MD and source images; metadata newly supplied, not retroactively credited to earlier review.'}
    immutable(ROOT/'request.json',request);immutable(ROOT/'queue.json',queue);immutable(ROOT/'manifest.json',manifest)
    print('PREPARED',json.dumps({k:v for k,v in manifest.items() if k!='metadata_changes'}),flush=True)
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    result=await run_source_review_workflow(queue,finding,md,REVIEW/'graphics',ROOT/'workflow',
        invoke=direct_cli_executor('codex/gpt-6-astra',project_id='EOM-F050-stamp-review'),executor_id='direct-codex-gpt-6-astra-high',
        max_images=16,repair_quote_spacing=True,repair_image_refs=True)
    assert (BASE/'02_work/document.md').read_text()==md
    immutable(ROOT/'completion.json',{'status':result['status'],'full_acceptance':False,'metadata_change_explicit':True})
    print('COMPLETE',result['status'],flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
