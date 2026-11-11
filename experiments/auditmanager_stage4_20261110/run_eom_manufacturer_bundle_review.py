"""Review the actual QF3.20 question against version-separated project/catalogue images."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from .run_eom_instance_operations import BASE, immutable
from .run_eom_manufacturer_applicability import ROOT as PREVIOUS, SOURCE, PDF, MANIFEST
from backend.app.pipeline.stages.findings_verify.source_review_bundle import build_source_review_bundle, bind_source_review_bundle, verify_source_review_bundle
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow, direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT=Path('/tmp/auditmanager_eom_manufacturer_bundle_review_20261111')


def copy_bound(source,target,digest):
    raw=Path(source).read_bytes();assert hashlib.sha256(raw).hexdigest()==digest
    if target.exists():assert target.read_bytes()==raw
    else:target.write_bytes(raw)


def prepare():
    ROOT.mkdir(exist_ok=True)
    wire=json.loads((PREVIOUS/'request.json').read_text());binding=json.loads((PREVIOUS/'binding.json').read_text())
    assert evidence_digest(wire)==binding['request_sha256']
    project_md=BASE/'02_work/document.md'
    assert hashlib.sha256(project_md.read_bytes()).hexdigest()==binding['project_md_sha256']
    manufacturer=json.loads(MANIFEST.read_text())
    assert hashlib.sha256(PDF.read_bytes()).hexdigest()==binding['manufacturer_pdf_sha256']==manufacturer['sha256']
    result=json.loads((PREVIOUS/'result.json').read_text());assert not result['is_error']
    question,=result['json_data']['candidate_findings']
    payload=json.loads(wire['messages'][-1]['content'])
    # Provenance-only GRAPHIC index, not OCR, a catalogue summary, or project TEXT.
    external_md=ROOT/'manufacturer_graphic_index.md'
    content=''.join(f"### BLOCK #{i+1} [GRAPHIC]: {r['source_id']}\n\n> Source URL: {r['source_url']}\n> PDF SHA256: {r['pdf_sha256']}\n> PDF page: {r['pdf_page_number']}\n\n" for i,r in enumerate(payload['external_manufacturer_sources']))
    if external_md.exists():assert external_md.read_text()==content
    else:external_md.write_text(content)
    bundle=build_source_review_bundle([
        {'document_code':'project_EOM','version':BASE.name,'md_path':str(project_md),
         'block_ids':[b['block_id'] for b in payload['source_context']]},
        {'document_code':'manufacturer_manual_990886','version':manufacturer['sha256'],
         'md_path':str(external_md)}])
    mapped={o['original_block_id']:bid for bid,o in bundle['source_origins'].items()}
    crops=ROOT/'graphics';crops.mkdir(exist_ok=True)
    project_id=payload['attached_image_ids_in_order'][0]
    originals=BASE/'03_analysis/runs/control/blocks_stage02_100'
    original,=[b for b in json.loads((originals/'index.json').read_text())['blocks'] if b['block_id']==project_id]
    copy_bound(originals/original['file'],crops/f'block_{mapped[project_id]}.png',original['sha256'])
    details=[]
    for i,(bid,image) in enumerate(zip(payload['attached_image_ids_in_order'],binding['image_bindings'])):
        name=f'detail_{i}.png';copy_bound(image['path'],crops/name,image['sha256'])
        details.append({'block_id':mapped[bid],'file':name,'sha256':image['sha256']})
    for row in payload['external_manufacturer_sources']:
        image=binding['image_bindings'][row['attachment_index_one_based']-1]
        assert image['sha256']==row['image_sha256']
        copy_bound(image['path'],crops/f"block_{mapped[row['source_id']]}.png",image['sha256'])
    immutable(crops/'graphic_detail_manifest.json',{'source_md_sha256':bundle['source_md_sha256'],'images':details})
    finding={'id':'EOM-QF320-manufacturer-question','problem':question['problem'],
        'description':question['description'],'solution':question['solution'],
        'severity':'question','report_section':'questions','question_basis':'actionable_source_ambiguity',
        'profile_reason':question['type'],'engineer_action_required':True,
        'source_evidence_review_status':'pending','source_review_requests':[{
            'source_finding_id':'EOM-QF320-applicability-candidate',
            'source_claim':json.dumps(question,ensure_ascii=False),'block_ids':list(bundle['source_origins'])}]}
    queue=bind_source_review_bundle(build_source_review_queue([finding],bundle['md']),bundle)
    attach_graphic_receipts(queue,bundle['md'],crops)
    request=prepare_source_review_request(queue,finding,bundle['md'],max_images=16)
    data=json.loads(request['messages'][-1]['content'])
    assert not data['unavailable_images'] and not data['budgeted_text_block_ids']
    assert len(request['image_paths'])==12
    for name,value in [('bundle',bundle),('finding',finding),('queue',queue),('initial_request',request)]:
        immutable(ROOT/(name+'.json'),value)
    manifest={'request_sha256':evidence_digest(request),'candidate_result_sha256':evidence_digest(result),
        'model':'codex/gpt-6-astra','automatic_retries':0,'history_or_engineer_answers_supplied':False,
        'message_chars':sum(len(m['content']) for m in request['messages']),'images':len(request['image_paths']),
        'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
        'scope':'Known-source candidate review; original prose report_section normalized to question for review, not yet admitted.',
        'manufacturer_pdf_sha256':manufacturer['sha256'],'semantic_acceptance':False}
    immutable(ROOT/'manifest.json',manifest);print('PREPARED',json.dumps(manifest),flush=True)
    return queue,finding,bundle['md'],crops


async def main(execute):
    queue,finding,md,crops=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    executor=direct_cli_executor('codex/gpt-6-astra',project_id='EOM-manufacturer-bundle-review')
    async def invoke(request,phase):
        verify_source_review_bundle(queue)
        assert hashlib.sha256(PDF.read_bytes()).hexdigest()==json.loads(MANIFEST.read_text())['sha256']
        result=await executor(request,phase)
        verify_source_review_bundle(queue)
        return result
    result=await run_source_review_workflow(queue,finding,md,crops,ROOT/'workflow',invoke=invoke,
        executor_id='direct-codex-gpt-6-astra-high',max_images=16)
    print('COMPLETE',result['status'],flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
