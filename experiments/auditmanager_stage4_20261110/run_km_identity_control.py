"""Check all lexical pair candidates from two sealed source inventory packets."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.prepare.object_identity_passes import prepare_object_identity_request
from backend.app.pipeline.stages.text_analysis.object_identity_candidates import build_object_identity_candidates
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT=Path('/tmp/auditmanager_km_identity_control_20261110')
SOURCE=Path('/tmp/auditmanager_whole_project_inventory_20261110/KM')

def prepare():
    if (ROOT/'attempt.json').exists():raise RuntimeError('Existing attempt')
    objects=[];requests=[];provenance=[]
    for name in ('object_inventory_000','object_inventory_001'):
        path=SOURCE/'execution'/name/'result.json';result=json.loads(path.read_text())
        if result['is_error']:raise ValueError('Invalid source inventory')
        coverage=json.loads((path.parent/'coverage.json').read_text())
        objects.extend({**o,'id':name+':'+o['id']} for o in coverage['objects'])
        requests.append(json.loads((SOURCE/(name+'.json')).read_text()))
        provenance.append({'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    candidates=build_object_identity_candidates(objects)
    request=prepare_object_identity_request(objects,candidates['pairs'],requests)
    ROOT.mkdir(exist_ok=True)
    (ROOT/'request.json').write_text(json.dumps(request,ensure_ascii=False,indent=2))
    manifest={'request_sha256':evidence_digest(request),'model':'claude/claude-sonnet-5-5','pair_count':len(request['pair_ids']),
        'message_chars':sum(len(m['content']) for m in request['messages']),'images':len(request['image_paths']),
        'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
        'inventory_results':provenance,'history_or_engineer_answers_sent':False,'new_calls':1,'automatic_retries':0,
        'cost':'Subscription quota; monetary maximum unknown','scope':'All lexical candidates from first two KM inventory packets; not full-project acceptance'}
    (ROOT/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2));print(json.dumps(manifest,ensure_ascii=False,indent=2))

async def execute(digest):
    request=json.loads((ROOT/'request.json').read_text())
    if evidence_digest(request)!=digest:raise ValueError('Request changed')
    for path,binding in zip(request['image_paths'],request['image_bindings'],strict=True):
        if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=binding['sha256']:raise ValueError('Image changed')
    with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started','request_sha256':digest},f)
    from backend.app.services.llm.claude_json_runner import run_claude_json_messages
    try:
        r=await run_claude_json_messages(request['messages'],model='claude/claude-sonnet-5-5',image_paths=request['image_paths'],
            reasoning_effort='high',timeout=900,stage='object_identity_control',project_id='KM-source-only-pair-control')
        (ROOT/'result.json').write_text(json.dumps(asdict(r),ensure_ascii=False,indent=2))
        (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','model_error':r.is_error}))
        print(json.dumps({'model_error':r.is_error,'duration_ms':r.duration_ms}))
    except BaseException as exc:
        (ROOT/'attempt.json').write_text(json.dumps({'status':'failed','error_type':type(exc).__name__}));raise

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute');a=p.parse_args()
    if a.execute:asyncio.run(execute(a.execute))
    else:prepare()
