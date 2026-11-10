"""Replay saved source query with node navigation; optionally invoke one Astra call."""
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass, combine_findings_with_targeted
from backend.app.pipeline.stages.prepare.recovery_context import build_context_followup
from backend.app.pipeline.stages.prepare.candidate_recovery import validate_recovery_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

OLD=Path('/tmp/auditmanager_km_deferred_control_20261110')
ROOT=Path('/tmp/auditmanager_km_node_navigation_control_20261110')
OUT=Path(__file__).parent

def save(path,data):
    path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')

def prepare():
    ROOT.mkdir(exist_ok=True)
    packet=json.loads((OLD/'pass_0/request.json').read_text())
    binding=json.loads((OLD/'pass_0/binding.json').read_text())
    assert evidence_digest(packet)==binding['request_sha256']
    assert all(hashlib.sha256(Path(i['path']).read_bytes()).hexdigest()==i['sha256'] for i in binding['images'])
    initial=CodexTargetedPass('candidate_recovery_km_navigation','navigation.json',packet['messages'],tuple(map(Path,packet['image_paths'])))
    result=json.loads((OLD/'pass_0/result.json').read_text())
    request,receipt=build_context_followup(initial,validate_recovery_payload(initial,result['json_data']))
    assert request is not None
    node='blk_cfd1c741a6fa4cd99c3eccd1d42afc7d'
    assert node in [r['block_id'] for r in receipt['images']['attached']]
    hit=next(h for h in receipt['search']['hits'] if h['block_id']==node)
    assert hit['source_provenance']['stage']=='П'
    assert not hit['reference_applicability_verified']
    new={'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
    newbinding={'request_sha256':evidence_digest(new),'images':[{'path':str(p),'sha256':hashlib.sha256(Path(p).read_bytes()).hexdigest()} for p in request.image_paths]}
    for name,data in [('request',new),('binding',newbinding),('retrieval',receipt)]:
        p=ROOT/(name+'.json')
        if p.exists():assert json.loads(p.read_text())==data
        else:save(p,data)
    save(OUT/'KM_node_navigation_replay.json',{'target_node_attached':True,'target_provenance':hit['source_provenance'],'applicability_verified':False,'initial_response_reused':True,'request_sha256':newbinding['request_sha256'],'chars':sum(len(m['content']) for m in request.messages),'images':len(request.image_paths)})
    return request,newbinding

async def main(invoke):
    request,binding=prepare()
    print('PREPARED',len(request.image_paths),'images',flush=True)
    if not invoke:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    path=ROOT/'result.json'
    if path.exists():result=json.loads(path.read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started',**binding},f)
        result=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='candidate_recovery',project_id='KM-node-navigation',image_paths=request.image_paths,allowed_tools=''))
        save(path,result)
        save(ROOT/'attempt.json',{'status':'finished','is_error':result['is_error']})
    assert not result['is_error']
    assert all(hashlib.sha256(Path(i['path']).read_bytes()).hexdigest()==i['sha256'] for i in binding['images'])
    source=json.loads(request.messages[-1]['content'])['md_source']
    assert hashlib.sha256(Path(source['path']).read_bytes()).hexdigest()==source['sha256']
    validated=validate_recovery_payload(request,result['json_data'])
    basepath=Path(source['path']).parents[1]/'03_analysis/runs/control/03_findings.json'
    base=json.loads(basepath.read_text())
    combined=combine_findings_with_targeted(base,[(request.stage,validated)])
    assert combined['findings'][:len(base['findings'])]==base['findings']
    save(ROOT/'combined.json',combined)
    save(OUT/'KM_node_navigation_result.json',result)
    save(OUT/'KM_node_navigation_summary.json',{'baseline_preserved':len(base['findings']),'new_rows':len(combined['findings'])-len(base['findings']),'validated':validated,'semantic_verified':False,'production_accepted':False})
    print('COMPLETE',flush=True)

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--invoke',action='store_true')
    asyncio.run(main(parser.parse_args().invoke))
