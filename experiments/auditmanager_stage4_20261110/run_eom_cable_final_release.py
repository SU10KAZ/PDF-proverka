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

async def main(execute):
    ROOT.mkdir(exist_ok=True)
    document=json.loads(BEFORE.read_text());md=(BASE/'02_work/document.md').read_text()
    candidates=[json.loads((REVIEW/fid/'candidate.json').read_text()) for fid in ('F-050','F-051')]
    manifests=[]
    for candidate in candidates:
        assert candidate['status']=='candidate_ready'
        entry=candidate['entry'];prepared=prepare_final_check(entry,document,md);wire=prepared['request']
        manifests.append({'finding_id':entry['response']['finding_id'],'binding':prepared['binding'],
            'message_chars':sum(len(m['content']) for m in wire['messages']),
            'image_count':len(wire['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in wire['image_paths'])})
    immutable(ROOT/'preparation.json',{'source_document_sha256':hashlib.sha256(BEFORE.read_bytes()).hexdigest(),
        'model':'codex/gpt-6-astra','effort':'high','initial_checks':manifests,'maximum_model_calls':6,
        'automatic_retries':0,'history_and_engineer_answers_supplied':False,'grouping_enabled':False,
        'boundary':'Known two-candidate control. Later checks see any already-admitted peer edit as untrusted report context.'})
    immutable(ROOT/'input_document.json',document)
    immutable(ROOT/'03_source_review_candidates.json',{'status':'finished','reports':candidates,'not_attempted':0})
    if not (ROOT/'03_findings.json').exists():immutable(ROOT/'03_findings.json',document)
    print('PREPARED',json.dumps(manifests),flush=True)
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    summary=await run_final_release_phase(ROOT,BASE/'02_work/document.md',
        invoke=direct_cli_executor('codex/gpt-6-astra',project_id='EOM-cable-final-control'),
        executor_id='codex/gpt-6-astra:high:final-check-v1',max_findings=2,complete_citations=True)
    after=json.loads((ROOT/'03_findings.json').read_text())
    assert [f for f in after['findings'] if f['id'] not in ('F-050','F-051')]==[f for f in document['findings'] if f['id'] not in ('F-050','F-051')]
    assert (BASE/'02_work/document.md').read_text()==md
    immutable(ROOT/'completion.json',{'status':summary['status'],'records':[{'finding_id':r['finding_id'],'status':r['status']} for r in summary['records']],
        'applied':summary['applied'],'held':summary['held'],'not_completed':summary['not_completed'],'not_attempted':summary['not_attempted'],
        'original_49_rows_unchanged':True,'full_acceptance':False,'grouping_enabled':False})
    print('COMPLETE',json.dumps({k:summary[k] for k in ['applied','held','not_completed','not_attempted']}),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
