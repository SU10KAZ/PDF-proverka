"""Actual targeted executor: one exact sealed response, at most two fresh followups."""
import asyncio
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT=Path('/tmp/auditmanager_integrated_ogm5_control_20261110')
SEALED=Path('/tmp/auditmanager_km_ogm5_ranked_continuation_20261110')
INVENTORY=Path('/tmp/auditmanager_whole_project_inventory_20261110/KM')
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001')
EXPECTED='5cd3bc373c26fd8208541c236610f5e8449bd4da75219b5dd4f9f5480ae05719'


def prepare():
    ROOT.mkdir(exist_ok=True)
    request=json.loads((SEALED/'request.json').read_text())
    result=json.loads((SEALED/'result.json').read_text())
    assert evidence_digest(request)==EXPECTED and not result['is_error']
    inventory=json.loads((INVENTORY/'manifest.json').read_text());images={}
    for row,digest in zip(inventory['packets'],inventory['request_digests'],strict=True):
        prepared=json.loads((INVENTORY/(row['stage']+'.json')).read_text())
        assert evidence_digest(prepared)==digest
        for item,path in zip(row['images'],prepared['image_paths'],strict=True):
            images[item['block_id']]={**item,'path':path}
    # Prepared inventory image bindings contain a canonical path for each block.
    for image in images.values():
        assert hashlib.sha256(Path(image['path']).read_bytes()).hexdigest()==image['sha256']
    graph=json.loads(Path('/tmp/auditmanager_six_candidate_inputs_20261110/KM/03_analysis/runs/candidate/document_graph.json').read_text())
    binding={'md_path':str(BASE/'02_work/document.md'),'md_sha256':inventory['source_md_sha256'],'images':images,'graph':graph}
    assert hashlib.sha256(Path(binding['md_path']).read_bytes()).hexdigest()==binding['md_sha256']
    manifest={'sealed_request':EXPECTED,'sealed_result':evidence_digest(result),'source_binding':evidence_digest(binding),
        'max_new_calls':2,'model':'codex/gpt-6-astra','reasoning_effort':'high','automatic_retries':0,
        'history_or_engineer_answers_supplied':False,'scope':'Known OGM5 continuation mechanism; classic baseline stays executor-side.'}
    path=ROOT/'manifest.json'
    if path.exists():assert json.loads(path.read_text())==manifest
    else:path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    return request,result,binding,manifest


async def execute():
    os.environ['CODEX_TRANSIENT_RETRIES']='0';os.environ['AUDIT_CODEX_JSON_ATTEMPTS']='1'
    os.environ['AUDIT_CODEX_TARGETED_FINDINGS']='1'
    request,sealed,binding,manifest=prepare()
    from backend.app.models.usage import LLMResult
    from backend.app.services.llm import claude_runner,codex_runner
    from backend.app.pipeline.stages.prepare import codex_targeted_findings as targeted
    assert claude_runner._CODEX_JSON_ATTEMPTS==1
    assert codex_runner.substitute_codex_model('gpt-6-astra')=='gpt-6-astra'
    initial=targeted.CodexTargetedPass('object_operations_integrated_ogm5','03_object_operations_integrated_ogm5.json',
        request['messages'],tuple(request['image_paths']),continuation_source=binding)
    base=json.loads((BASE/'03_analysis/runs/control/03_findings.json').read_text())
    old=deepcopy(base);transport=codex_runner.run_codex_json_messages;calls=[]
    async def guarded_transport(messages,**kwargs):
        current={'messages':messages,'image_paths':[str(p) for p in kwargs.get('image_paths',[])]}
        digest=evidence_digest(current)
        if digest==EXPECTED:
            calls.append({'origin':'sealed_exact_request','request_sha256':digest})
            return replace(LLMResult(**sealed),duration_ms=0,input_tokens=0,output_tokens=0,cost_usd=0)
        if sum(c['origin']!='sealed_exact_request' for c in calls)>=2:
            calls.append({'origin':'local_budget_stop','request_sha256':digest})
            return LLMResult(is_error=True,error_message='control_call_budget_exhausted',model='codex/gpt-6-astra')
        number=sum(c['origin']!='sealed_exact_request' for c in calls)
        folder=ROOT/f'followup_{number:02d}';folder.mkdir(exist_ok=True)
        path=folder/'request.json'
        if path.exists():assert evidence_digest(json.loads(path.read_text()))==digest
        else:path.write_text(json.dumps(current,ensure_ascii=False,indent=2)+'\n')
        receipt={'request_sha256':digest,'message_chars':sum(len(m['content']) for m in messages),
            'images':len(current['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in current['image_paths'])}
        output=folder/'result.json';seal=folder/'result_binding.json'
        if output.exists():
            data=json.loads(output.read_text());assert json.loads(seal.read_text())=={'request':digest,'result':evidence_digest(data)}
            calls.append({**receipt,'origin':'reused_followup'})
            return replace(LLMResult(**data),duration_ms=0,input_tokens=0,output_tokens=0,cost_usd=0)
        attempt=folder/'attempt.json'
        with attempt.open('x') as h:json.dump({'status':'started',**receipt},h)
        print('START',number,json.dumps(receipt),flush=True)
        try:
            kwargs.update(model='codex/gpt-6-astra',reasoning_effort='high',allowed_tools='')
            result=await transport(messages,**kwargs);data=asdict(result)
            tmp=output.with_suffix('.tmp');tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2));tmp.replace(output)
            seal.write_text(json.dumps({'request':digest,'result':evidence_digest(data)}))
            attempt.write_text(json.dumps({'status':'finished','model_error':result.is_error}))
        except BaseException as exc:
            attempt.write_text(json.dumps({'status':'interrupted_or_failed','error_type':type(exc).__name__}));raise
        calls.append({**receipt,'origin':'fresh_followup','duration_ms':result.duration_ms,'model_error':result.is_error})
        (ROOT/'calls.json').write_text(json.dumps(calls,ensure_ascii=False,indent=2)+'\n')
        print('DONE',number,result.is_error,result.duration_ms,flush=True)
        return result
    with patch.object(targeted,'build_targeted_findings_passes',return_value=[initial]),patch.object(codex_runner,'run_codex_json_messages',guarded_transport):
        code,_,result=await claude_runner._run_codex_targeted_findings_merge(project_info={},
            project_id='KM-integrated-context-control',model='codex/gpt-6-astra',base_result=LLMResult(json_data=base),
            base_text='sealed classic base; not supplied to provider',on_output=None,
            output_dir=ROOT,version_dir=BASE,version_id='v001')
    assert base==old
    assert result.json_data['findings'][:len(base['findings'])]==base['findings']
    summary={'exit_code':code,'calls':calls,'baseline_findings_preserved':len(base['findings']),
        'final_transport_rows':len(result.json_data['findings']),'operation_reports':result.json_data.get('object_operation_reports',[]),
        'incremental_useful_findings_verified':False,'production_accepted':False,
        'limits':['Integrated executor tested on one known case with sealed first answer; not full discovery or six-project quality.',
            'Final transport rows may include semantic duplicates; count is not unique defect count.']}
    (ROOT/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    print('COMPLETE',json.dumps({k:summary[k] for k in ('exit_code','baseline_findings_preserved','final_transport_rows')}),flush=True)

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');parser.add_argument('--seed',choices=['ranked','small'],default='ranked');a=parser.parse_args()
    if a.seed=='small':
        ROOT=Path('/tmp/auditmanager_integrated_ogm5_small_control_20261110')
        SEALED=Path('/tmp/auditmanager_km_ogm5_operation_control_20261110')
        EXPECTED='26e8ad0768bcad521e22fc9cfaddacd105fdb88f55e63f0c93c6ddc80d72328f'
    if a.execute:asyncio.run(execute())
    else:
        *_,manifest=prepare();print(json.dumps(manifest,ensure_ascii=False))
