"""Review a frozen operation candidate with its original project sources; no answers."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews, build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow, direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest


def copy_bound(source,target,digest):
    raw=Path(source).read_bytes();assert hashlib.sha256(raw).hexdigest()==digest
    if target.exists():assert target.read_bytes()==raw
    else:target.write_bytes(raw)


def prepare(args):
    root=Path(args.output_root);root.mkdir(parents=True,exist_ok=True)
    source=Path(args.source_root);md_path=Path(args.md_path);crops_source=Path(args.crops_root)
    _,operation,binding=load_frozen_operation(source,md_path)
    validated=json.loads((source/'validated.json').read_text())
    finding,=[f for f in validated['findings'] if args.source_finding_id in f.get('source_finding_ids',[])]
    finding['id']=args.finding_id
    payload=json.loads(operation.messages[-1]['content'])
    assert attach_pending_source_reviews([finding],set(payload['allowed_source_refs']))==1
    md=md_path.read_text();queue=build_source_review_queue([finding],md)
    crops=root/'graphics';crops.mkdir(exist_ok=True)
    index=json.loads((crops_source/'index.json').read_text())
    required=set(queue['packets'][0]['required_graphic_block_ids'])
    original_bindings=[]
    for bid in sorted(required):
        row,=[r for r in index['blocks'] if r['block_id']==bid]
        path=(crops_source/row['file']).resolve()
        assert path.is_relative_to(crops_source.resolve())
        digest=row.get('sha256') or hashlib.sha256(path.read_bytes()).hexdigest()
        copy_bound(path,crops/f'block_{bid}.png',digest)
        original_bindings.append({'block_id':bid,'path':str(path),'sha256':digest})
    retrieval_bindings=[]
    if args.populate_context_graphics:
        for row in index['blocks']:
            bid=row.get('block_id');filename=row.get('file')
            if not isinstance(bid,str) or not isinstance(filename,str):continue
            path=(crops_source/filename).resolve()
            assert path.is_relative_to(crops_source.resolve())
            if not path.is_file():continue
            digest=row.get('sha256') or hashlib.sha256(path.read_bytes()).hexdigest()
            copy_bound(path,crops/f'block_{bid}.png',digest)
            retrieval_bindings.append({'block_id':bid,'sha256':digest})
    details=[]
    for i,(bid,path) in enumerate(zip(payload['attached_image_ids_in_order'],operation.image_paths,strict=True)):
        if bid not in required:continue
        digest=hashlib.sha256(Path(path).read_bytes()).hexdigest();name=f'detail_{i}.png'
        copy_bound(path,crops/name,digest)
        details.append({'block_id':bid,'sha256':digest,'file':name})
    if args.extra_detail_manifest:
        import fitz
        extra=json.loads(Path(args.extra_detail_manifest).read_text())
        assert extra['source_md_sha256']==hashlib.sha256(md.encode()).hexdigest()
        blocks=json.loads((md_path.parent/'blocks.json').read_text())
        assert blocks['coordinate_space']=='normalized_page_top_left'
        for i,row in enumerate(extra['images']):
            provenance=row['provenance'];bid=provenance['source_block']
            assert bid in required
            pdf=md_path.with_suffix('.pdf')
            assert Path(provenance['source_pdf']).resolve()==pdf.resolve()
            assert hashlib.sha256(pdf.read_bytes()).hexdigest()==provenance['source_pdf_sha256']
            block,=[b for b in blocks['blocks'] if b['block_id']==bid]
            assert block['block_type']=='image' and block['page_index']==provenance['page_index']
            with fitz.open(pdf) as doc:
                page=doc[block['page_index']];n=block['coords_norm']
                bounds=fitz.Rect(n[0]*page.rect.width,n[1]*page.rect.height,n[2]*page.rect.width,n[3]*page.rect.height)
                clip=fitz.Rect(provenance['clip']);assert bounds.contains(clip) and not clip.is_empty
                assert 0 < provenance['scale'] <= 4 and provenance['rotation_for_reading'] in (0,90,180,270)
                rendered=page.get_pixmap(matrix=fitz.Matrix(provenance['scale'],provenance['scale']).prerotate(provenance['rotation_for_reading']),clip=clip,alpha=False).tobytes('png')
            assert hashlib.sha256(rendered).hexdigest()==row['sha256']
            name=f'extra_{i}.png';copy_bound(row['path'],crops/name,row['sha256'])
            details.append({'block_id':bid,'file':name,'sha256':row['sha256'],
                'page_index':provenance['page_index'],'clip_rotated_page_points':provenance['clip'],
                'source_pdf_sha256':provenance['source_pdf_sha256']})
    immutable(crops/'graphic_detail_manifest.json',{'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),'images':details})
    attach_graphic_receipts(queue,md,crops)
    request=prepare_source_review_request(queue,finding,md,max_images=16)
    data=json.loads(request['messages'][-1]['content'])
    assert not data['unavailable_images'] and not data['budgeted_text_block_ids']
    for name,value in [('finding',finding),('queue',queue),('initial_request',request)]:
        immutable(root/(name+'.json'),value)
    manifest={'source_root':str(source),'source_candidate_sha256':hashlib.sha256((source/'validated.json').read_bytes()).hexdigest(),
        'source_finding_id':args.source_finding_id,'request_sha256':evidence_digest(request),
        'original_graphic_bindings':original_bindings,'model':'codex/gpt-6-astra','automatic_retries':0,
        'history_or_engineer_answers_supplied':False,'evaluator_corrections_supplied':False,
        'message_chars':sum(len(m['content']) for m in request['messages']),
        'images':len(request['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
        'known_candidate_control':True,'semantic_acceptance':False,
        **({'available_context_graphics':retrieval_bindings,'context_graphics_boundary':'Available locally for model-requested retrieval; only attachments in each recorded request are sent.'} if args.populate_context_graphics else {}),
        **({'additional_graphic_manifest_sha256':hashlib.sha256(Path(args.extra_detail_manifest).read_bytes()).hexdigest(),
            'graphic_selection':'Executor-selected orientation/detail control; not an automatic model-requested crop.'} if args.extra_detail_manifest else {})}
    immutable(root/'manifest.json',manifest)
    print('PREPARED',json.dumps(manifest,ensure_ascii=False),flush=True)
    return queue,finding,md,crops


async def main(args):
    queue,finding,md,crops=prepare(args)
    if not args.execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    executor=direct_cli_executor('codex/gpt-6-astra',project_id=args.finding_id+'-source-review')
    async def invoke(request,phase):
        # Revalidate the frozen operation and all original attachments, not a
        # mutable in-memory reference to the model output.
        load_frozen_operation(Path(args.source_root),Path(args.md_path))
        assert Path(args.md_path).read_text()==md
        result=await executor(request,phase)
        load_frozen_operation(Path(args.source_root),Path(args.md_path))
        assert Path(args.md_path).read_text()==md
        return result
    result=await run_source_review_workflow(queue,finding,md,crops,Path(args.output_root)/'workflow',
        invoke=invoke,executor_id='direct-codex-gpt-6-astra-high',max_images=16,repair_quote_spacing=True,literal_image_hashes=args.literal_image_hash)
    print('COMPLETE',result['status'],flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for name in ('source-root','output-root','md-path','crops-root','source-finding-id','finding-id'):
        p.add_argument('--'+name,required=True)
    p.add_argument('--extra-detail-manifest')
    p.add_argument('--populate-context-graphics',action='store_true')
    p.add_argument('--literal-image-hash',action='append',default=[])
    p.add_argument('--execute',action='store_true')
    asyncio.run(main(p.parse_args()))
