"""Known five-cabinet control: unmodified model propositions, no expected group supplied."""
import argparse
import asyncio
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.findings_merge.group_proposal import prepare_group_proposal
from backend.app.pipeline.stages.findings_merge.group_workflow import run_grouping_workflow
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor

HERE=Path(__file__).parent
ROOT=Path('/tmp/auditmanager_ak_group_workflow_control_20261111')
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/AK/versions/v001')
SELECT=[(0,2,0),(1,4,0),(2,2,0),(3,3,2),(4,3,0)]


def immutable(path,value):
    if path.exists():
        if json.loads(path.read_text())!=value:raise ValueError('Changed artifact: '+str(path))
    else:
        with path.open('x') as f:json.dump(value,f,ensure_ascii=False,indent=2)


def prepare():
    ROOT.mkdir(exist_ok=True);members=[];sources=[]
    for n,(packet,version,index) in enumerate(SELECT,1):
        path=HERE/f'AK_cabinet_{packet:03d}_operation_v{version}_validated.json'
        item=deepcopy(json.loads(path.read_text())['findings'][index]);item['id']=f'C-{n:03d}'
        members.append(item);sources.append({'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'finding_index':index,'assigned_id':item['id']})
    document={'findings':members};request=prepare_group_proposal(document)
    md=(BASE/'02_work/document.md').read_text()
    manifest={'scope':'Known five-cabinet local control, not whole-project blind grouping or independent positive labels.',
        'source_candidates':sources,'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),
        'original_propositions_and_categories_preserved':True,'message_chars':sum(len(m['content']) for m in request['messages']),
        'model':'codex/gpt-6-astra','effort':'high','maximum_model_calls':3,'maximum_images_per_group_review':12,
        'expected_groups_supplied':False,'engineer_answers_supplied':False,'automatic_retries':0}
    immutable(ROOT/'input.json',document);immutable(ROOT/'preparation.json',manifest);immutable(ROOT/'proposal_request.json',request)
    return document,md,manifest

async def main(execute):
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    document,md,manifest=prepare();print('PREPARED',json.dumps(manifest,ensure_ascii=False),flush=True)
    if not execute:return
    output,receipt=await run_grouping_workflow(document,md,BASE/'03_analysis/runs/control/blocks_stage02_100',ROOT/'workflow',
        invoke=direct_cli_executor('codex/gpt-6-astra',project_id='AK-group-workflow-control'),executor_id='codex/gpt-6-astra:high:grouping-v1',max_groups=2)
    immutable(ROOT/'output_preview.json',output);immutable(ROOT/'receipt.json',receipt)
    print('COMPLETE',json.dumps({k:v for k,v in receipt.items() if k!='records'},ensure_ascii=False),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
