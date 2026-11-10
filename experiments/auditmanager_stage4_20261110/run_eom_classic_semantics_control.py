"""Known-candidate control of three classic findings; no evaluation answers sent."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from .run_eom_instance_operations import immutable, BASE
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow, direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_eom_classic_semantics_control_20261111')
SOURCE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/snapshots/EOM/findings_verify.json')
REFS={
 'F-001':['blk_d90c772851514a0a9c65cdd019e56957','blk_67c8cc82d7e24f738797d9b75ce14e97'],
 'F-030':['blk_7d22e811017e4e969b2d732e9e30c50d','blk_7ca68a936c85428dbd4b60b2893398a2'],
 'F-032':['blk_fb5f780dae7a475ca6c249f62b64771e','blk_f2b273baee994702906a6e5d94a43063','blk_29be4ff018f040daaa2000c0a3ebde08']}

def prepare():
    ROOT.mkdir(exist_ok=True)
    assert hashlib.sha256(SOURCE.read_bytes()).hexdigest()=='d15870091424921798f5b4b5beb1bc227dad0d9d3e12a376e61d09f2f7edef8a'
    md=(BASE/'02_work/document.md').read_text()
    findings=[f for f in json.loads(SOURCE.read_text())['findings'] if f['id'] in REFS]
    assert len(findings)==3
    for f in findings:
        f['source_evidence_review_status']='pending'
        f['source_review_requests']=[{'source_finding_id':f['source_finding_ids'][0],
            'source_claim':'\n'.join(f'{key}: {f[key]}' for key in ['problem','description','solution']),
            'block_ids':REFS[f['id']]}]
    queue=build_source_review_queue(findings,md)
    crops=BASE/'03_analysis/runs/control/blocks_stage02_100'
    attach_graphic_receipts(queue,md,crops)
    requests=[prepare_source_review_request(queue,f,md,max_images=16) for f in findings]
    manifest={'model':'codex/gpt-6-astra','automatic_retries':0,'known_candidate_control':True,
        'source_selection':'Known case source blocks; original classic findings, no engineer feedback or corrected benchmark wording.',
        'original_findings_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),
        'history_or_engineer_answers_supplied':False,'semantic_acceptance':False,'requests':[]}
    for f,r in zip(findings,requests,strict=True):
        payload=json.loads(r['messages'][-1]['content']);assert not payload['budgeted_text_block_ids'] and not payload['unavailable_images']
        assert not r['image_paths']
        immutable(ROOT/(f['id']+'_request.json'),r)
        manifest['requests'].append({'finding_id':f['id'],'request_sha256':evidence_digest(r),'message_chars':sum(len(m['content']) for m in r['messages'])})
    immutable(ROOT/'findings.json',findings);immutable(ROOT/'queue.json',queue);immutable(ROOT/'manifest.json',manifest)
    print('PREPARED',json.dumps(manifest['requests']),flush=True)
    return queue,findings,md,crops

async def main(execute, finding_ids):
    queue,findings,md,crops=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    executor=direct_cli_executor('codex/gpt-6-astra',project_id='EOM-classic-semantic-control')
    digest=hashlib.sha256(md.encode()).hexdigest()
    async def invoke(request,phase):
        assert hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest()==digest
        result=await executor(request,phase)
        assert hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest()==digest
        return result
    for finding_id in finding_ids:
        finding=next(f for f in findings if f['id']==finding_id)
        result=await run_source_review_workflow(queue,finding,md,crops,ROOT/finding['id'],invoke=invoke,executor_id='direct-codex-gpt-6-astra-high',max_images=16)
        print('DONE',finding['id'],result['status'],flush=True)
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true')
    p.add_argument('--finding-ids',nargs='+',choices=list(REFS),default=list(REFS))
    p.add_argument('--output-root',default=str(ROOT));args=p.parse_args();ROOT=Path(args.output_root)
    asyncio.run(main(args.execute,args.finding_ids))
