"""Actual source/adjudication workflow for a model-produced cross-document question."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
from .run_ar_roof_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_bundle import build_source_review_bundle, bind_source_review_bundle, verify_source_review_bundle
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow, direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT=Path('/tmp/auditmanager_ar_rope_bundle_review_20261111')
PREVIOUS=Path('/tmp/auditmanager_ar_linked_graphic_check_20261111')


def prepare():
    ROOT.mkdir(exist_ok=True)
    wire=json.loads((PREVIOUS/'request.json').read_text());binding=json.loads((PREVIOUS/'binding.json').read_text())
    assert evidence_digest(wire)==binding['request_sha256']
    result=json.loads((PREVIOUS/'result.json').read_text());assert not result['is_error']
    question=result['json_data']['candidate_question'];assert question
    payload=json.loads(wire['messages'][1]['content'])
    ar_refs=[b['block_id'] for b in payload['AR_authoritative_text']]
    ar_refs += [i['block_id'] for i in binding['images'] if i['document_code']=='ПД-00580716-АР6.2']
    sources=[{'document_code':'ПД-00580716-АР6.2','version':'v001',
        'md_path':binding['source_binding']['md_path'],'block_ids':list(dict.fromkeys(ar_refs))}]
    for source in binding['linked_source_bindings']:
        assert hashlib.sha256(Path(source['path']).read_bytes()).hexdigest()==source['sha256']
        content=next(s for s in payload['linked_sources'] if s['version']==source['version'])
        refs=[b['block_id'] for b in content['authoritative_text']]
        refs += [i['block_id'] for i in binding['images'] if i['document_code']=='ПД-00580716-КМ5.2' and i['version']==source['version']]
        sources.append({'document_code':'ПД-00580716-КМ5.2','version':source['version'],'md_path':source['path'], 'block_ids':list(dict.fromkeys(refs))})
    bundle=build_source_review_bundle(sources)
    assert bundle['source_catalogue'][0]['source_md_sha256']==binding['source_binding']['md_sha256']
    by_namespace={s['namespace']:s for s in bundle['source_catalogue']}
    mapped={(by_namespace[o['namespace']]['document_code'],by_namespace[o['namespace']]['version'],o['original_block_id']):bid
            for bid,o in bundle['source_origins'].items()}
    crops=ROOT/'graphics';crops.mkdir(exist_ok=True)
    for image in binding['images']:
        raw=Path(image['path']).read_bytes();assert hashlib.sha256(raw).hexdigest()==image['sha256']
        bid=mapped[(image['document_code'],image['version'],image['block_id'])]
        target=crops/f'block_{bid}.png'
        if target.exists():assert target.read_bytes()==raw
        else:target.write_bytes(raw)
    # Exact model wording, merely placed in the finding fields. No evaluator answer.
    finding={'id':'AR-O27-crossdoc','problem':question['question'],
        'description':'Известные факты:\n'+'\n'.join(question['known_facts'])+'\nНеустановленные данные:\n'+'\n'.join(question['unknowns']),
        'solution':question['engineer_action'],'severity':'question','report_section':'questions',
        'question_basis':'actionable_source_ambiguity','profile_reason':question['question'],'engineer_action_required':True,
        'source_evidence_review_status':'pending','source_review_requests':[{'source_finding_id':'AR-O27-source-question',
        'source_claim':json.dumps(question,ensure_ascii=False),'block_ids':list(bundle['source_origins'])}]}
    queue=bind_source_review_bundle(build_source_review_queue([finding],bundle['md']),bundle)
    attach_graphic_receipts(queue,bundle['md'],crops)
    request=prepare_source_review_request(queue,finding,bundle['md'],max_images=16)
    data=json.loads(request['messages'][1]['content'])
    assert not data['unavailable_images'] and not data['budgeted_text_block_ids']
    assert len(request['image_paths'])==11 and len(data['source_catalogue'])==3
    immutable(ROOT/'bundle.json',bundle);immutable(ROOT/'finding.json',finding);immutable(ROOT/'queue.json',queue)
    immutable(ROOT/'initial_request.json',request)
    manifest={'request_sha256':evidence_digest(request),'candidate_result_sha256':evidence_digest(result),
        'model':'codex/gpt-6-astra','automatic_retries':0,'history_or_engineer_answers_supplied':False,
        'message_chars':sum(len(m['content']) for m in request['messages']),'image_count':len(request['image_paths']),
        'source_documents':bundle['source_catalogue'],'semantic_acceptance':False}
    immutable(ROOT/'manifest.json',manifest)
    print('PREPARED',manifest['message_chars'],'chars',manifest['image_count'],'images',flush=True)
    return queue,finding,bundle['md'],crops


async def main(execute):
    queue,finding,md,crops=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    executor=direct_cli_executor('codex/gpt-6-astra',project_id='AR-rope-bundle-review')
    async def invoke(request,phase):
        verify_source_review_bundle(queue)
        result=await executor(request,phase)
        verify_source_review_bundle(queue)
        return result
    result=await run_source_review_workflow(queue,finding,md,crops,ROOT/'workflow',invoke=invoke,
        executor_id='direct-codex-gpt-6-astra-high',max_images=16)
    print('COMPLETE',result['status'],flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
