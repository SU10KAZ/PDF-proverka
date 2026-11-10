"""Known deferred AR candidate through actual recovery and one context hop.

No historical engineer decisions or expected answer are supplied. The archived
current-run candidate is the task under review, not independent discovery.
"""
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.prepare.candidate_recovery import build_recovery_passes, validate_recovery_payload
from backend.app.pipeline.stages.prepare.recovery_context import build_context_followup
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.findings_merge.candidate_coverage import write_candidate_coverage

BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/AR/versions/v001')
ROOT=Path('/tmp/auditmanager_ar_deferred_control_20261110')
BID='blk_b6f2b57505d842e2bdc39369c3b5d93a'


def prepare():
    ROOT.mkdir(exist_ok=True)
    source=BASE/'03_analysis/runs/control'
    original=json.loads((source/'01_blocks_analysis.json').read_text())
    selected=next(b for b in original['block_analyses'] if b['block_id']==BID)
    assert len(selected['deferred_findings'])==1
    block={'block_id':BID,'findings':[],'deferred_findings':selected['deferred_findings']}
    for name,data in [('01_blocks_analysis.json',{'block_analyses':[block]}),
                      ('02_text_analysis.json',{'text_findings':[]}),('03_findings.json',{'findings':[]})]:
        (ROOT/name).write_text(json.dumps(data,ensure_ascii=False))
    (ROOT/'document_graph.json').write_bytes((source/'document_graph.json').read_bytes())
    if not (ROOT/'blocks_stage02_100').exists():
        (ROOT/'blocks_stage02_100').symlink_to(source/'blocks_stage02_100',target_is_directory=True)
    requests=build_recovery_passes(ROOT,BASE/'02_work/document.md')
    request,=requests
    manifest={'source_md_sha256':hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest(),
        'source_candidates_sha256':hashlib.sha256((source/'01_blocks_analysis.json').read_bytes()).hexdigest(),
        'selected_block':BID,'max_calls':2,'model':'codex/gpt-6-astra','reasoning_effort':'high',
        'automatic_retries':0,'engineer_answers_supplied':False,'known_recovery_not_blind_discovery':True}
    path=ROOT/'manifest.json'
    if path.exists():assert json.loads(path.read_text())==manifest
    else:path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    return request,manifest


async def main():
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    request,manifest=prepare()
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    assert substitute_codex_model('gpt-6-astra')=='gpt-6-astra'
    payloads=[];calls=[]
    for index in range(2):
        folder=ROOT/f'pass_{index}';folder.mkdir(exist_ok=True)
        packet={'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
        binding={'request_sha256':evidence_digest(packet),'images':[
            {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in request.image_paths]}
        for name,data in [('request.json',packet),('binding.json',binding)]:
            path=folder/name
            if path.exists():assert json.loads(path.read_text())==data
            else:path.write_text(json.dumps(data,ensure_ascii=False,indent=2))
        assert hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest()==manifest['source_md_sha256']
        output=folder/'result.json'
        if output.exists():result=json.loads(output.read_text())
        else:
            with (folder/'attempt.json').open('x') as f:json.dump({'status':'started',**binding},f)
            print('START',index,sum(len(m['content']) for m in request.messages),'chars',len(request.image_paths),'images',flush=True)
            result=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',
                reasoning_effort='high',timeout=900,stage='candidate_recovery',project_id='AR-deferred-control',
                image_paths=request.image_paths,allowed_tools=''))
            output.write_text(json.dumps(result,ensure_ascii=False,indent=2))
            (folder/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
        assert not result['is_error'] and isinstance(result['json_data'],dict)
        assert all(hashlib.sha256(Path(i['path']).read_bytes()).hexdigest()==i['sha256'] for i in binding['images'])
        assert hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest()==manifest['source_md_sha256']
        validated=validate_recovery_payload(request,result['json_data'])
        payloads.append((request.stage,validated))
        calls.append({'pass':index,'duration_ms':result['duration_ms'],'input_tokens':result['input_tokens'],
                      'output_tokens':result['output_tokens'],'request_sha256':binding['request_sha256']})
        followup,retrieval=build_context_followup(request,validated)
        (folder/'retrieval.json').write_text(json.dumps(retrieval,ensure_ascii=False,indent=2))
        base=json.loads((BASE/'03_analysis/runs/control/03_findings.json').read_text())
        combined=combine_findings_with_targeted(base,payloads)
        assert combined['findings'][:len(base['findings'])]==base['findings']
        (ROOT/'03_findings.json').write_text(json.dumps(combined,ensure_ascii=False,indent=2))
        coverage=write_candidate_coverage(ROOT)
        summary={'calls':calls,'baseline_findings_preserved':len(base['findings']),
                 'new_transport_rows':len(combined['findings'])-len(base['findings']),
                 'decision_coverage':coverage['decision_coverage'],'retrieval_status':retrieval['status'],
                 'semantic_verified':False,'production_accepted':False}
        (ROOT/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
        print('DONE',index,retrieval['status'],flush=True)
        if followup is None:break
        request=followup
    print('COMPLETE',flush=True)


if __name__=='__main__':asyncio.run(main())
