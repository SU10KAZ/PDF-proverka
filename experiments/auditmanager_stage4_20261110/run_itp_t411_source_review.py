"""Review the actual instance-identity question with its full bounded source context."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .run_itp_pipe_operations import immutable, BASE
from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews, build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow, direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
SOURCE=Path('/tmp/auditmanager_itp_gvs_graphic_window_20261111')
ROOT=Path('/tmp/auditmanager_itp_t411_source_review_20261111')
DETAIL_ROOT=SOURCE
FINDING_ID='ITP-T411-flow-conflict'

def prepare():
    ROOT.mkdir(exist_ok=True)
    _,operation,binding=load_frozen_operation(SOURCE,BASE/'02_work/document.md')
    md=(BASE/'02_work/document.md').read_text()
    validated=json.loads((SOURCE/'validated.json').read_text())
    finding,=validated['findings'];finding['id']=FINDING_ID
    assert attach_pending_source_reviews([finding],set(json.loads(operation.messages[-1]['content'])['allowed_source_refs']))==1
    queue=build_source_review_queue([finding],md)
    crops=ROOT/'graphics';crops.mkdir(exist_ok=True)
    for bid in queue['packets'][0]['required_graphic_block_ids']:
        image=binding['source_binding']['images'][bid];raw=Path(image['path']).read_bytes()
        assert hashlib.sha256(raw).hexdigest()==image['sha256']
        path=crops/f'block_{bid}.png'
        if path.exists():assert path.read_bytes()==raw
        else:path.write_bytes(raw)
    details=[]
    # Both direct graphic proofs retain every detail tile. K15 remains available
    # as its original graphic; its three detail tiles are not resent here.
    for folder in ('tiles_0','tiles_1'):
        source_tiles=json.loads((DETAIL_ROOT/folder/'manifest.json').read_text())
        for tile in source_tiles['tiles']:
            raw=Path(tile['path']).read_bytes();assert hashlib.sha256(raw).hexdigest()==tile['sha256']
            name=f'detail_{len(details)}.png';path=crops/name
            if path.exists():assert path.read_bytes()==raw
            else:path.write_bytes(raw)
            details.append({**{k:tile[k] for k in ['block_id','sha256','page_index','clip_rotated_page_points']},'file':name,'source_pdf_sha256':source_tiles['pdf_sha256']})
    immutable(crops/'graphic_detail_manifest.json',{'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),'images':details})
    attach_graphic_receipts(queue,md,crops)
    request=prepare_source_review_request(queue,finding,md,max_images=16)
    data=json.loads(request['messages'][-1]['content'])
    assert not data['budgeted_text_block_ids']
    direct_refs={e['block_id'] for e in finding['evidence']} & set(queue['packets'][0]['required_graphic_block_ids'])
    assert direct_refs <= {im['block_id'] for im in request['image_bindings']}
    assert not direct_refs.intersection({im['block_id'] for im in data['unavailable_images']})
    assert len([r for r in request['image_bindings'] if r.get('view_kind')=='detail_of_original_graphic'])==12
    immutable(ROOT/'finding.json',finding);immutable(ROOT/'queue.json',queue);immutable(ROOT/'initial_request.json',request)
    manifest={'source_operation_sha256':binding['request_sha256'],'request_sha256':evidence_digest(request),'source_candidate_sha256':hashlib.sha256((SOURCE/'validated.json').read_bytes()).hexdigest(),'model':'codex/gpt-6-astra','automatic_retries':0,'history_or_engineer_answers_supplied':False,'message_chars':sum(len(m['content']) for m in request['messages']),'images':len(request['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),'semantic_acceptance':False,'unavailable_images':data['unavailable_images'],'graphic_window_limit':'Twelve detail tiles for both direct proofs; pump detail tiles omitted, original pump graphic retained when required.'}
    immutable(ROOT/'manifest.json',manifest);print('PREPARED',json.dumps(manifest),flush=True)
    return queue,finding,md,crops,operation

async def main(execute):
    queue,finding,md,crops,operation=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    executor=direct_cli_executor('codex/gpt-6-astra',project_id=FINDING_ID+'-source-review')
    async def invoke(request,phase):
        assert verify_operation_source_binding(operation) is None
        result=await executor(request,phase)
        assert verify_operation_source_binding(operation) is None
        return result
    result=await run_source_review_workflow(queue,finding,md,crops,ROOT/'workflow',invoke=invoke,executor_id='direct-codex-gpt-6-astra-high',max_images=16)
    print('COMPLETE',result['status'],flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true')
    p.add_argument('--source-root',default=str(SOURCE));p.add_argument('--output-root',default=str(ROOT))
    p.add_argument('--detail-root',default=str(DETAIL_ROOT));p.add_argument('--finding-id',default=FINDING_ID)
    args=p.parse_args();SOURCE=Path(args.source_root);ROOT=Path(args.output_root);DETAIL_ROOT=Path(args.detail_root);FINDING_ID=args.finding_id
    asyncio.run(main(args.execute))
