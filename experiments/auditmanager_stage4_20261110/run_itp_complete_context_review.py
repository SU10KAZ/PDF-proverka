"""Bounded known-case review with recovered context; direct Claude, no retries."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from experiments.auditmanager_stage4_20261110.run_itp_graphic_review_pilot import MD
from experiments.auditmanager_stage4_20261110.replay_integrated_admission import BASE

ROOT=Path('/tmp/auditmanager_itp_complete_context_review_20261110')

def prepare():
    ROOT.mkdir(exist_ok=True)
    if (ROOT/'attempt.json').exists():raise RuntimeError('Existing attempt; do not restart')
    queue=json.loads(Path('/tmp/auditmanager_itp_context_backlog_20261110/expanded_queue.json').read_text())
    finding=json.loads(Path('/tmp/auditmanager_itp_initial_context_astra_20261110/finding.json').read_text())
    source=next(p for p in json.loads((BASE/'input_manifest.json').read_text()) if p['discipline']=='ITP')
    attach_graphic_receipts(queue,MD.read_text(),Path(source['output_dir'])/'blocks_stage02_100')
    request=prepare_source_review_request(queue,finding,MD.read_text())
    for name,obj in [('queue',queue),('finding',finding),('request',request)]:
        (ROOT/f'{name}.json').write_text(json.dumps(obj,ensure_ascii=False,indent=2))
    manifest={'model':'claude/claude-sonnet-5-5','request_sha256':request['request_sha256'],
        'message_chars':sum(len(m['content']) for m in request['messages']),
        'images':len(request['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
        'text_blocks':len(queue['packets'][0]['sources']),'source_search_revision':queue['packets'][0]['context_revision'],
        'engineer_answers_or_evaluation_outcomes_included':False,'new_calls':1,'automatic_retries':0,
        'cost':'Claude subscription quota; monetary maximum unknown','known_case_not_blind':True}
    (ROOT/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    print(json.dumps(manifest,ensure_ascii=False,indent=2))

async def execute(digest):
    request=json.loads((ROOT/'request.json').read_text())
    queue=json.loads((ROOT/'queue.json').read_text());finding=json.loads((ROOT/'finding.json').read_text())
    if request['request_sha256']!=digest or prepare_source_review_request(queue,finding,MD.read_text())!=request:
        raise ValueError('Source packet changed')
    with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started','request_sha256':digest},f)
    from backend.app.services.llm.claude_json_runner import run_claude_json_messages
    try:
        result=await run_claude_json_messages(request['messages'],model='claude/claude-sonnet-5-5',image_paths=request['image_paths'],timeout=900,reasoning_effort='high',stage='source_review',project_id='ITP-known-complete-context')
        (ROOT/'result.json').write_text(json.dumps(asdict(result),ensure_ascii=False,indent=2))
        (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','model_error':result.is_error}))
        print(json.dumps({'model_error':result.is_error,'duration_ms':result.duration_ms}))
    except BaseException as exc:
        (ROOT/'attempt.json').write_text(json.dumps({'status':'failed','error_type':type(exc).__name__}))
        raise

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute');a=p.parse_args()
    if a.execute:asyncio.run(execute(a.execute))
    else:prepare()
