"""Check model-requested linked graphics without merging source versions."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from .run_ar_roof_operations import immutable
from .frozen_operation_request import load_frozen_operation
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT=Path('/tmp/auditmanager_ar_linked_graphic_check_20261111')
NAV=Path('/tmp/auditmanager_ar_linked_document_navigation_20261111')
AR=Path('/tmp/auditmanager_ar_rope_navigation_followup_20261111')
MD=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/AR/versions/v001/02_work/document.md')


def prepare():
    ROOT.mkdir(exist_ok=True)
    _,original,_=load_frozen_operation(AR,MD)
    ar=json.loads(original.messages[-1]['content'])
    navigation=json.loads((NAV/'result.json').read_text());assert not navigation['is_error']
    navwire=json.loads((NAV/'request.json').read_text());navbinding=json.loads((NAV/'binding.json').read_text())
    assert evidence_digest(navwire)==navbinding['request_sha256']
    for source in navbinding['source_bindings']:
        assert hashlib.sha256(Path(source['path']).read_bytes()).hexdigest()==source['sha256']
    linked=json.loads(navwire['messages'][1]['content'])
    graphics=json.loads(Path('/tmp/auditmanager_ar_linked_graphics_20261111/manifest.json').read_text())
    for source in graphics['sources']:
        for f in source['files']:assert hashlib.sha256(Path(f['path']).read_bytes()).hexdigest()==f['sha256']
    # Original O27 views and its explicitly referenced placement plan.
    ids=list(dict.fromkeys(ar['object']['source_refs']+['blk_8df48eff096342b68d80dbde8a26883e']))
    images=[]
    for bid in ids:
        image=original.continuation_source['images'][bid]
        images.append({'document_code':'ПД-00580716-АР6.2','version':'v001','block_id':bid,**image})
    images+=graphics['images']
    for im in images:assert hashlib.sha256(Path(im['path']).read_bytes()).hexdigest()==im['sha256']
    payload={'object':ar['object'],'original_AR_version':'v001',
        'AR_source_md_sha256':original.continuation_source['md_sha256'],
        'AR_authoritative_text':[b for b in ar['source_context'] if b.get('kind') in ('TEXT','TABLE')],
        'linked_sources':linked['linked_sources'], 'local_document_index':linked['local_document_index'],
        'navigation_untrusted':navigation['json_data'],
        'images_in_order':[{k:v for k,v in im.items() if k!='path'} for im in images]}
    instruction='''Check the requested cross-document graphical applicability for the supplied object.
No tools, web, engineer decisions or historical findings. Read all images and authoritative TEXT. Do not correct TEXT from raster or treat generated summaries/previous navigation as technical evidence.
Keep AR v001, KM5.2 v001 and KM5.2 v002 separate. Establish each document's depicted execution, locations and marks from sources. A similar mark number, shared project cipher or document title does not establish same physical object. Do not silently prefer a newer version or combine their dimensions.
For each KM version, assess whether its graphics and TEXT actually establish the relevant AR object and the requested connection/load-path context. Distinguish not_same_execution, applicability_unproven, and applicability_supported with concrete graphical and textual evidence. A missing detail in these packets does not prove a project-wide omission or strength failure.
Return JSON: inspected_images:[{document_code,version,block_id,image_sha256,observation}], version_comparisons:[{version,status,reason,evidence,remaining_unknowns}], requested_documents:[{document_code,reason}], next_action, limitations. If a bounded actionable question is justified, provide it separately as candidate_question with evidence, known_facts, unknowns and engineer_action; do not invent a question to fill the field. No calculation or physical defect may be asserted without appropriate premises.'''
    wire={'messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}],
          'image_paths':[im['path'] for im in images]}
    binding={'request_sha256':evidence_digest(wire),'images':images,'model':'codex/gpt-6-astra','automatic_retries':0,
        'history_or_engineer_answers_supplied':False,'known_source_selection':True,'source_binding':original.continuation_source,
        'linked_source_bindings':navbinding['source_bindings'],'message_chars':sum(len(m['content']) for m in wire['messages'])}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',binding)
    print('PREPARED',binding['message_chars'],'chars',len(images),'images',flush=True)
    return wire,binding


async def main(execute):
    wire,binding=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    if (ROOT/'result.json').exists():r=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        r=asdict(await run_codex_json_messages(wire['messages'],model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='source_review',project_id='AR-linked-graphics',image_paths=[Path(p) for p in wire['image_paths']],allowed_tools=''))
        immutable(ROOT/'result.json',r);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':r['is_error']}))
    assert not r['is_error'];prepare()
    allowed={(i['document_code'],i['version'],i['block_id'],i['sha256']) for i in binding['images']}
    seen={(i['document_code'],i['version'],i['block_id'],i['image_sha256']) for i in r['json_data']['inspected_images']}
    immutable(ROOT/'validation.json',{'unrecognized_images':[list(i) for i in seen-allowed],
        'unreported_images':[list(i) for i in allowed-seen],'semantic_acceptance':False})
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
