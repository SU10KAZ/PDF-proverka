"""One saved-finding control through the generic workflow; not new discovery."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue,attach_pending_source_reviews
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow,direct_cli_executor
from backend.app.pipeline.stages.prepare.graph_builder import build_block_page_index
from experiments.auditmanager_stage4_20261110.replay_integrated_admission import BASE

ROOT=Path('/tmp/auditmanager_vk_source_workflow_control_20261110')

def prepare():
    if (ROOT/'manifest.json').exists():raise RuntimeError('Already prepared; inspect before replacing')
    p=next(p for p in json.loads((BASE/'input_manifest.json').read_text()) if p['discipline']=='VK')
    source=Path(p['output_dir']);md_path=Path(p['version_dir'])/'02_work/document.md';md=md_path.read_text()
    assert hashlib.sha256(md.encode()).hexdigest()==p['source_md_sha256']
    findings=json.loads((source/'03_findings.json').read_text())['findings']
    graph=json.loads((source/'document_graph.json').read_text());blocks=json.loads((source/'01_blocks_analysis.json').read_text())
    known=set(build_block_page_index(graph))|{b['block_id'] for b in blocks['block_analyses'] if b.get('block_id')}
    text_candidate_lineage(findings,json.loads((source/'02_text_analysis.json').read_text()),known)
    attach_pending_source_reviews(findings,known)
    finding=next(f for f in findings if f['id']=='F-013')
    queue=build_source_review_queue([finding],md);attach_graphic_receipts(queue,md,source/'blocks_stage02_100')
    request=prepare_source_review_request(queue,finding,md)
    config={'model':'claude/claude-sonnet-5-5','md_path':str(md_path),'graphic_dir':str(source/'blocks_stage02_100'),
        'finding':finding,'queue':queue,'executor_id':'claude/claude-sonnet-5-5:high:workflow-v1'}
    ROOT.mkdir(exist_ok=True)
    (ROOT/'inputs.json').write_text(json.dumps(config,ensure_ascii=False,indent=2))
    manifest={'inputs_sha256':evidence_digest(config),'discipline':'VK','finding_id':'F-013',
        'initial_request_sha256':request['request_sha256'],'initial_message_chars':sum(len(m['content']) for m in request['messages']),
        'initial_images':len(request['image_paths']),'initial_image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
        'model':config['model'],'max_model_calls':6,'max_context_revisions':4,'max_total_retrieved_text_chars':240000,
        'max_images_per_request':8,'context_boundary':'Only this frozen VK prepared MD and graphic index. No linked external documents.',
        'history_or_engineer_answers_sent':False,'known_saved_finding_not_blind':True,'automatic_retries':0,
        'production_changed':False,'cost':'Subscription quota; monetary maximum unknown'}
    (ROOT/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2));print(json.dumps(manifest,ensure_ascii=False,indent=2))

async def execute(digest):
    c=json.loads((ROOT/'inputs.json').read_text())
    if evidence_digest(c)!=digest:raise ValueError('Prepared inputs changed')
    md=Path(c['md_path']).read_text()
    if hashlib.sha256(md.encode()).hexdigest()!=c['queue']['source_md_sha256']:raise ValueError('Source changed')
    invoke=direct_cli_executor(c['model'],project_id='VK-known-F013-workflow')
    async def logged(request,phase):
        print('MODEL_START',phase,'chars',sum(len(m['content']) for m in request['messages']),'images',len(request['image_paths']),flush=True)
        result=await invoke(request,phase)
        print('MODEL_DONE',phase,'error',result['is_error'],'ms',result['duration_ms'],flush=True)
        return result
    result=await run_source_review_workflow(c['queue'],c['finding'],md,c['graphic_dir'],ROOT/'workflow',
        invoke=logged,executor_id=c['executor_id'])
    print(json.dumps({k:v for k,v in result.items() if k!='entry'},ensure_ascii=False,indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute');a=p.parse_args()
    if a.execute:asyncio.run(execute(a.execute))
    else:prepare()
