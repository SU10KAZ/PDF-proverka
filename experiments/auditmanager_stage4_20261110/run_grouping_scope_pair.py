"""Same grouping-scope rule on two known controls; expected actions withheld."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from .run_eom_group_source_review import immutable
from backend.app.pipeline.stages.findings_merge.group_source_review import prepare_group_review,validate_group_review
from backend.app.pipeline.stages.findings_merge.reviewed_grouping import apply_reviewed_group
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor

ROOT=Path('/tmp/auditmanager_grouping_scope_pair_20261111')
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs')
CASES=[('AK',Path('/tmp/auditmanager_ak_group_workflow_control_20261111/input.json'),
 Path('/tmp/auditmanager_ak_group_workflow_control_20261111/workflow/group_proposal/result.json'),'v001',[]),
 ('EOM',Path('/tmp/auditmanager_eom_reviewed_cable_output_20261111/03_findings.json'),
 Path('/tmp/auditmanager_eom_semantic_grouping_control_20261111/result.json'),'v002',['blk_84e0cee1c8a145c482898b1c10b3a24b'])]

async def main(execute):
    os.environ['CODEX_TRANSIENT_RETRIES']='0';ROOT.mkdir(exist_ok=True)
    for disc,doc_path,proposal_path,version,extra in CASES:
        document=json.loads(doc_path.read_text());proposals=json.loads(proposal_path.read_text())['json_data']['groups']
        base=BASE/disc/'versions'/version;md=(base/'02_work/document.md').read_text();graphics=base/'03_analysis/runs/control/blocks_stage02_100'
        for index,proposal in enumerate(proposals):
            root=ROOT/f'{disc}_{index:02d}';root.mkdir(exist_ok=True)
            try:prepared=prepare_group_review(document,proposal,md,graphics,extra_text_block_ids=extra)
            except ValueError as exc:
                immutable(root/'preparation.json',{'status':'not_eligible','reason':str(exc)});continue
            request=prepared['request'];manifest={'request_sha256':evidence_digest(request),'binding':prepared['binding'],
                'model':'codex/gpt-6-astra','effort':'high','message_chars':sum(len(m['content']) for m in request['messages']),
                'image_count':len(request['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
                'known_development_control':True,'expected_actions_supplied':False,'prior_verdicts_supplied':False,'engineer_answers_supplied':False,'automatic_retries':0}
            immutable(root/'request.json',request);immutable(root/'preparation.json',manifest)
            print('PREPARED',disc,index,json.dumps(manifest),flush=True)
            if not execute:continue
            output=root/'result.json'
            if output.exists():
                result=json.loads(output.read_text());assert json.loads((root/'result_binding.json').read_text())=={'request':manifest['request_sha256'],'result':evidence_digest(result)}
            else:
                with (root/'attempt.json').open('x') as f:json.dump({'status':'started',**manifest},f)
                result=await direct_cli_executor('codex/gpt-6-astra',project_id='grouping-scope-pair')(request,'group_scope_review')
                immutable(output,result);immutable(root/'result_binding.json',{'request':manifest['request_sha256'],'result':evidence_digest(result)})
            if result.get('is_error') or not isinstance(result.get('json_data'),dict):raise ValueError('Provider response unusable')
            assert evidence_digest(prepare_group_review(document,proposal,md,graphics,extra_text_block_ids=extra)['request'])==manifest['request_sha256']
            decision=validate_group_review(document,proposal,md,graphics,result['json_data'],extra_text_block_ids=extra)
            after,receipt=apply_reviewed_group(document,proposal,decision,md)
            immutable(root/'decision.json',decision);immutable(root/'receipt.json',receipt);immutable(root/'output_preview.json',after)
            print('COMPLETE',disc,index,receipt['status'],decision['action'],flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
