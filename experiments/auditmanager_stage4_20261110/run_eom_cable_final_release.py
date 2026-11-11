"""Finish existing cable candidates independently of the disabled grouping trial."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check
from backend.app.pipeline.stages.findings_verify.source_review_release_workflow import run_final_release_phase
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor

ROOT=Path('/tmp/auditmanager_eom_cable_final_release_20261111')
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002')
REVIEW=Path('/tmp/auditmanager_eom_cable_current_adjudication_20261111')
BEFORE=Path('/tmp/auditmanager_eom_cable_output_20261111/03_findings.json')

async def main(args):
    root=args.output_root
    root.mkdir(exist_ok=True)
    ids=args.finding_ids
    if len(set(ids))!=len(ids):raise ValueError("Duplicate finding selection")
    document=json.loads(BEFORE.read_text());md=(BASE/'02_work/document.md').read_text()
    candidates=[json.loads((REVIEW/fid/'candidate.json').read_text()) for fid in ids]
    manifests=[]
    for candidate in candidates:
        assert candidate['status']=='candidate_ready'
        entry=candidate['entry'];prepared=prepare_final_check(entry,document,md);wire=prepared['request']
        manifests.append({'finding_id':entry['response']['finding_id'],'binding':prepared['binding'],
            'message_chars':sum(len(m['content']) for m in wire['messages']),
            'image_count':len(wire['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in wire['image_paths'])})
    immutable(root/'preparation.json',{'source_document_sha256':hashlib.sha256(BEFORE.read_bytes()).hexdigest(),
        'model':'codex/gpt-6-astra','effort':'high','initial_checks':manifests,'maximum_model_calls':3*len(ids),'selected_finding_ids':ids,
        'automatic_retries':0,'history_and_engineer_answers_supplied':False,'grouping_enabled':False,
        'boundary':'Known two-candidate control. Later checks see any already-admitted peer edit as untrusted report context.'})
    immutable(root/'input_document.json',document)
    immutable(root/'03_source_review_candidates.json',{'status':'finished','reports':candidates,'not_attempted':0})
    if not (root/'03_findings.json').exists():immutable(root/'03_findings.json',document)
    print('PREPARED',json.dumps(manifests),flush=True)
    if not args.execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    summary=await run_final_release_phase(root,BASE/'02_work/document.md',
        invoke=direct_cli_executor('codex/gpt-6-astra',project_id='EOM-cable-final-control'),
        executor_id='codex/gpt-6-astra:high:final-check-v1',max_findings=len(ids),complete_citations=True)
    after=json.loads((root/'03_findings.json').read_text())
    assert [f for f in after['findings'] if f['id'] not in ids]==[f for f in document['findings'] if f['id'] not in ids]
    assert (BASE/'02_work/document.md').read_text()==md
    immutable(root/'completion.json',{'status':summary['status'],'records':[{'finding_id':r['finding_id'],'status':r['status']} for r in summary['records']],
        'applied':summary['applied'],'held':summary['held'],'not_completed':summary['not_completed'],'not_attempted':summary['not_attempted'],
        'untargeted_rows_unchanged':len(document['findings'])-len(ids),'full_acceptance':False,'grouping_enabled':False})
    print('COMPLETE',json.dumps({k:summary[k] for k in ['applied','held','not_completed','not_attempted']}),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true')
    p.add_argument('--finding-ids',nargs='+',choices=['F-050','F-051'],default=['F-050','F-051'])
    p.add_argument('--output-root',type=Path,default=ROOT)
    asyncio.run(main(p.parse_args()))
