"""Source-only known-fragment registry/operation pilot; no historical answer input."""
import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import re
from backend.app.pipeline.stages.block_context.contract import resolve_blocks_dir
from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes, validate_object_operation_payload
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry
from backend.app.services.llm.codex_runner import run_codex_json_messages
from backend.app.services.llm.codex_model_substitution import substitute_codex_model

ROOT=Path('/tmp/auditmanager_object_registry_astra_20261110')
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/AK')

async def call(name, messages, images):
    destination=ROOT/(name+'.json')
    request={'messages':messages,'image_paths':[str(p.resolve()) for p in images],'model':'gpt-6-astra'}
    request_path=ROOT/(name+'_request.json')
    if destination.exists():
        assert json.loads(request_path.read_text())==request, 'Cached request differs'
        return json.loads(destination.read_text())
    request_path.write_text(json.dumps(request,ensure_ascii=False,indent=2))
    for attempt in range(1,4):
        assert substitute_codex_model('gpt-6-astra')=='gpt-6-astra'
        print(f'START {name} Astra images={len(images)} attempt={attempt}',flush=True)
        result=await run_codex_json_messages(messages,model='codex/gpt-6-astra',timeout=900,
            stage='object_registry_pilot',project_id='AK-page57',image_paths=images)
        receipt=asdict(result)
        (ROOT/f'{name}_attempt{attempt}.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
        if not result.is_error and isinstance(result.json_data,dict):
            destination.write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
            print(f'DONE {name} duration_ms={result.duration_ms}',flush=True)
            return receipt
        if attempt<3:await asyncio.sleep(10)
    raise RuntimeError('Attempts exhausted: '+name)

async def main():
    ROOT.mkdir(exist_ok=True)
    version=next(BASE.glob('versions/*'));source=version/'03_analysis/runs/control'
    original_md=version/'02_work/document.md';md=original_md.read_text()
    headers=list(re.finditer(r'^##[ \t]+Page[ \t]+(\d+)\b[^\n]*$',md,re.M))
    i=next(i for i,h in enumerate(headers) if int(h.group(1))==57)
    fragment=md[headers[i].start():headers[i+1].start()]
    md_path=ROOT/'document.md';md_path.write_text(fragment)
    block_matches=list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)',fragment,re.M))
    block_ids={m.group(2) for m in block_matches}
    crops=resolve_blocks_dir(source); index=json.loads((crops/'index.json').read_text())
    files={b['block_id']:b.get('file') for b in index['blocks']}
    images=[];image_ids=[]
    for m in block_matches:
        if m.group(1).upper() in ('IMAGE','GRAPHIC'):
            name=files.get(m.group(2))
            path=(crops/name).resolve() if isinstance(name,str) else None
            if path and path.is_relative_to(crops.resolve()) and path.is_file():
                images.append(path);image_ids.append(m.group(2))
    link=ROOT/crops.name
    if not link.exists():link.symlink_to(crops)
    prompt=Path('prompts/pipeline/en/text_analysis_task.md').read_text()
    instructions=prompt.split('## Object registry and operation coverage',1)[1].split('## Source-addressed object balances',1)[0]
    messages=[{'role':'system','content':'Extract source-addressed objects and execute applicable checks. Respond JSON only, Russian values. No historical answers are supplied. MD TEXT is authoritative; images are for GRAPHIC blocks. Never treat graphic summaries as proof. Return object_registry, object_inventory_coverage, object_operation_results, text_findings.\n'+instructions},
              {'role':'user','content':json.dumps({'scope':'Known regression fragment, AK PDF page 57 only; other pages not supplied.','authoritative_md':fragment,'image_block_ids_in_order':image_ids},ensure_ascii=False)}]
    receipt=await call('registry',messages,images)
    data=receipt['json_data']
    coverage=reconcile_object_registry(data,block_ids)
    (ROOT/'registry_coverage.json').write_text(json.dumps(coverage,ensure_ascii=False,indent=2))
    (ROOT/'02_text_analysis.json').write_text(json.dumps(data,ensure_ascii=False,indent=2))
    plan=build_object_operation_passes(ROOT,md_path,max_requests=8)
    print(f'REGISTRY objects={len(coverage["objects"])} invalid={len(coverage["invalid_objects"])} pending_operations={coverage["unresolved_operations"]} targeted_passes={len(plan)}',flush=True)
    results=[]
    for request in plan:
        result=await call(request.stage,request.messages,request.image_paths)
        validated=validate_object_operation_payload(request,result['json_data'])
        results.append({'stage':request.stage,'result':result,'validated':validated})
    (ROOT/'pilot_result.json').write_text(json.dumps({'kind':'known_fragment_source_only_pilot','registry':receipt,
        'coverage':coverage,'operation_passes':results,'source':{'path':str(original_md),'sha256':hashlib.sha256(original_md.read_bytes()).hexdigest()},
        'boundary':'Only page57; not a blind discovery test or whole-project acceptance.'},ensure_ascii=False,indent=2))
    print('PILOT COMPLETE',flush=True)

if __name__=='__main__':asyncio.run(main())
