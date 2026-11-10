"""Direct Astra source-review workflow for the actual recovered AR K-3 question."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import shutil
from .run_ar_roof_operations import BASE, immutable
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow, direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_ar_roof_question_source_workflow_20261111')
GRAPHIC_DIR=None
OUTPUT=Path('/tmp/auditmanager_ar_roof_question_output_20261111')

def prepare():
    ROOT.mkdir(exist_ok=True)
    document=json.loads((OUTPUT/'03_findings.json').read_text())
    md=(BASE/'02_work/document.md').read_text()
    findings=[f for f in document['findings'] if f['id'] in ('F-024',)];assert len(findings)==1
    queue=build_source_review_queue(findings,md)
    crops=GRAPHIC_DIR or BASE/'03_analysis/runs/control/blocks_stage02_100'
    attach_graphic_receipts(queue,md,crops)
    requests=[prepare_source_review_request(queue,f,md) for f in findings]
    manifest={'findings_sha256':evidence_digest(findings),'queue_sha256':evidence_digest(queue),'model':'codex/gpt-6-astra','automatic_retries':0,'history_or_engineer_answers_supplied':False,'scope':'Bounded source verification plus adjudication, using the real workflow; candidate decisions require independent admission review.','initial_requests':[{'finding_id':f['id'],'request_sha256':evidence_digest(r),'message_chars':sum(len(m['content']) for m in r['messages']),'image_count':len(r['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in r['image_paths'])} for f,r in zip(findings,requests,strict=True)]}
    immutable(ROOT/'manifest.json',manifest);immutable(ROOT/'queue.json',queue);immutable(ROOT/'findings.json',findings)
    for f,r in zip(findings,requests,strict=True):immutable(ROOT/(f['id']+'_initial_request.json'),r)
    print(json.dumps(manifest['initial_requests'],ensure_ascii=False),flush=True)
    return queue,findings,md,crops

async def main(execute):
    queue,findings,md,crops=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    invoke=direct_cli_executor('codex/gpt-6-astra',project_id='AR-recovered-K3-question-source-review')
    for f in findings:
        try:
            report=await run_source_review_workflow(queue,f,md,crops,ROOT/f['id'],invoke=invoke,executor_id='direct-codex-gpt-6-astra-high')
            print('DONE',f['id'],report['status'],flush=True)
        except Exception as exc:
            immutable(ROOT/(f['id']+'_failure.json'),{'type':type(exc).__name__,'reason':str(exc)})
            print('FAILED',f['id'],type(exc).__name__,str(exc),flush=True)
    print('COMPLETE',flush=True)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
