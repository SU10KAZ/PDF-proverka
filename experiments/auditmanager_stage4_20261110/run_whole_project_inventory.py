"""Source-only whole-project inventory control, explicit direct Astra execution."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry

ROOT=Path('/tmp/auditmanager_whole_project_inventory_20261110')

def preflight(disc):
    root=ROOT/disc;manifest=json.loads((root/'manifest.json').read_text())
    requests=[]
    for row,digest in zip(manifest['packets'],manifest['request_digests'],strict=True):
        request=json.loads((root/(row['stage']+'.json')).read_text())
        if evidence_digest(request)!=digest:raise ValueError('Prepared request changed')
        if len(request['image_paths'])!=len(row['images']):raise ValueError('Image count changed')
        for path,binding in zip(request['image_paths'],row['images'],strict=True):
            if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=binding['sha256']:raise ValueError('Source image changed')
        requests.append((row,request))
    return manifest,requests

async def execute(disc,digest):
    manifest,requests=preflight(disc)
    if evidence_digest(manifest)!=digest:raise ValueError('Manifest changed')
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    if substitute_codex_model('gpt-6-astra')!='gpt-6-astra':raise ValueError('Model substitution not allowed')
    root=ROOT/disc/'execution';root.mkdir(exist_ok=True);reports=[];aggregate={'object_registry':[],'object_inventory_coverage':[],'object_operation_results':[],'text_findings':[]}
    for i,(row,request) in enumerate(requests):
        directory=root/row['stage'];directory.mkdir(exist_ok=True)
        binding={'request_sha256':evidence_digest(request),'model':'codex/gpt-6-astra','reasoning_effort':'high'}
        bound=directory/'binding.json';output=directory/'result.json'
        if bound.exists():
            if json.loads(bound.read_text())!=binding:raise ValueError('Stored request changed')
        else:bound.write_text(json.dumps(binding))
        if output.exists():result=json.loads(output.read_text())
        else:
            with (directory/'attempt.json').open('x') as f:json.dump({'status':'started',**binding},f)
            print('START',disc,i+1,'/',len(requests),row['stage'],'images',len(request['image_paths']),flush=True)
            try:
                result=asdict(await run_codex_json_messages(request['messages'],model='codex/gpt-6-astra',
                    reasoning_effort='high',timeout=900,stage='object_inventory',project_id=disc+'-whole-source-control',
                    image_paths=request['image_paths'],allowed_tools=''))
                tmp=output.with_suffix('.tmp');tmp.write_text(json.dumps(result,ensure_ascii=False,indent=2));tmp.replace(output)
                (directory/'attempt.json').write_text(json.dumps({'status':'finished','model_error':result['is_error']}))
            except BaseException as exc:
                (directory/'attempt.json').write_text(json.dumps({'status':'failed','error_type':type(exc).__name__}));raise
        if result['is_error'] or not isinstance(result.get('json_data'),dict):raise ValueError('Provider result unusable; inspect before retry')
        data=result['json_data'];coverage=reconcile_object_registry(data,set(row['block_ids']))
        (directory/'coverage.json').write_text(json.dumps(coverage,ensure_ascii=False,indent=2))
        # Keep packet-local identities distinct. Cross-packet correspondence must
        # be established from sources later, never guessed by matching labels.
        prefix=row['stage']+':'
        for obj in coverage['objects']:aggregate['object_registry'].append({**obj,'id':prefix+obj['id']})
        for rec in data.get('object_inventory_coverage',[]):
            if isinstance(rec,dict):aggregate['object_inventory_coverage'].append({**rec,'object_ids':[prefix+oid for oid in rec.get('object_ids',[]) if isinstance(oid,str)]})
        for rec in data.get('object_operation_results',[]):
            if isinstance(rec,dict) and isinstance(rec.get('object_id'),str):aggregate['object_operation_results'].append({**rec,'object_id':prefix+rec['object_id']})
        for f in data.get('text_findings',[]):
            if isinstance(f,dict):aggregate['text_findings'].append({**f,'inventory_packet':row['stage'],'semantic_verified':False})
        reports.append({'stage':row['stage'],'objects':len(coverage['objects']),'invalid_objects':len(coverage['invalid_objects']),
            'unresolved_source_blocks':coverage['unresolved_source_blocks'],'duration_ms':result['duration_ms'],
            'input_tokens':result.get('input_tokens'),'output_tokens':result.get('output_tokens'),'cost_source':result.get('cost_source')})
        (root/'progress.json').write_text(json.dumps({'completed':len(reports),'total':len(requests),'packets':reports,'semantic_verified':False},ensure_ascii=False,indent=2))
        print('DONE',disc,i+1,'objects',len(coverage['objects']),'unresolved_blocks',coverage['unresolved_source_blocks'],flush=True)
    (root/'02_text_analysis.json').write_text(json.dumps(aggregate,ensure_ascii=False,indent=2))
    all_blocks={bid for row in manifest['packets'] for bid in row['block_ids']}
    coverage=reconcile_object_registry(aggregate,all_blocks)
    (root/'aggregate_coverage.json').write_text(json.dumps(coverage,ensure_ascii=False,indent=2))
    print('COMPLETE',disc,'objects',len(coverage['objects']),'unresolved_blocks',coverage['unresolved_source_blocks'],flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--discipline',default='KM',choices=['AK','ITP','AR','KM','VK','EOM']);p.add_argument('--execute');a=p.parse_args()
    if a.execute:asyncio.run(execute(a.discipline,a.execute))
    else:
        manifest,requests=preflight(a.discipline)
        print(json.dumps({'discipline':a.discipline,'manifest_sha256':evidence_digest(manifest),'calls':len(requests),
            'message_chars':sum(sum(len(m['content']) for m in r['messages']) for _,r in requests),
            'image_attachments':sum(len(r['image_paths']) for _,r in requests),'model':'codex/gpt-6-astra','automatic_retries':0},indent=2))
