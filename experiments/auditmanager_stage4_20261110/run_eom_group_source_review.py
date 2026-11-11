"""Review eligible real grouping proposals; no expected group or engineer labels."""
import argparse
import asyncio
import json
import os
from pathlib import Path
from .run_eom_semantic_grouping_control import SOURCE,ROOT as PROPOSAL_ROOT
from .run_eom_instance_inventory_control import BASE
from backend.app.pipeline.stages.findings_merge.group_source_review import prepare_group_review,validate_group_review
from backend.app.pipeline.stages.findings_merge.reviewed_grouping import apply_reviewed_group
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor

ROOT=Path('/tmp/auditmanager_eom_group_source_review_20261111')


def immutable(path,value):
    if path.exists():
        if json.loads(path.read_text())!=value:raise ValueError('Changed artifact: '+str(path))
    else:
        with path.open('x') as f:json.dump(value,f,ensure_ascii=False,indent=2)


async def main(execute):
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    document=json.loads(SOURCE.read_text());md=(BASE/'02_work/document.md').read_text()
    proposals=json.loads((PROPOSAL_ROOT/'result.json').read_text())['json_data']['groups']
    graphics=BASE/'03_analysis/runs/control/blocks_stage02_100'
    ROOT.mkdir(exist_ok=True);reports=[]
    for index,proposal in enumerate(proposals):
        root=ROOT/f'group_{index:02d}';root.mkdir(exist_ok=True)
        try:prepared=prepare_group_review(document,proposal,md,graphics)
        except ValueError as exc:
            report={'member_ids':proposal['member_ids'],'status':'not_eligible','reason':str(exc),'model_calls':0}
            immutable(root/'preparation.json',report);reports.append(report);print('SKIPPED',json.dumps(report),flush=True);continue
        request=prepared['request']
        manifest={'binding':prepared['binding'],'request_sha256':evidence_digest(request),'model':'codex/gpt-6-astra','effort':'high',
            'message_chars':sum(len(m['content']) for m in request['messages']),'image_count':len(request['image_paths']),
            'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
            'engineer_answers_supplied':False,'expected_decision_supplied':False,'known_development_control':True,'automatic_retries':0}
        immutable(root/'preparation.json',manifest);immutable(root/'request.json',request)
        print('PREPARED',index,json.dumps(manifest),flush=True)
        if not execute:continue
        result_path=root/'result.json'
        if result_path.exists():
            result=json.loads(result_path.read_text())
            assert json.loads((root/'result_binding.json').read_text())=={'request':manifest['request_sha256'],'result':evidence_digest(result)}
        else:
            with (root/'attempt.json').open('x') as f:json.dump({'status':'started',**manifest},f)
            result=await direct_cli_executor('codex/gpt-6-astra',project_id='EOM-group-source-review')(request,'grouping_review')
            immutable(result_path,result);immutable(root/'result_binding.json',{'request':manifest['request_sha256'],'result':evidence_digest(result)})
        if result.get('is_error') or not isinstance(result.get('json_data'),dict):raise ValueError('Unusable provider result')
        if evidence_digest(prepare_group_review(document,proposal,md,graphics)['request'])!=manifest['request_sha256']:raise ValueError('Source images changed during dispatch')
        decision=validate_group_review(document,proposal,md,graphics,result['json_data'])
        after,receipt=apply_reviewed_group(document,proposal,decision,md)
        immutable(root/'decision.json',decision);immutable(root/'output_preview.json',after);immutable(root/'receipt.json',receipt)
        reports.append({'member_ids':proposal['member_ids'],'status':receipt['status'],'model_action':decision['action']})
        print('COMPLETE',index,receipt['status'],decision['action'],flush=True)
    if execute:immutable(ROOT/'summary.json',{'reports':reports,'full_project_acceptance':False,'original_report_mutated':False})

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
