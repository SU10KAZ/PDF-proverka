"""Check actual project circuit against an explicitly bound manufacturer source."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .run_eom_instance_operations import immutable, BASE
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_eom_manufacturer_applicability_20261111')
SOURCE=Path('/tmp/auditmanager_eom_shunt_graphic_operations_20261111/rp3_qf320')
MANIFEST=Path(__file__).with_name('EOM_manufacturer_source_manifest.json')
PDF=Path('/tmp/auditmanager_eom_manufacturer_source_20261111/dekraft_2025_va300.pdf')

def prepare():
    ROOT.mkdir(exist_ok=True)
    _,original,binding=load_frozen_operation(SOURCE,BASE/'02_work/document.md')
    packet=json.loads(original.messages[-1]['content'])
    source=json.loads(MANIFEST.read_text())
    assert hashlib.sha256(PDF.read_bytes()).hexdigest()==source['sha256']
    images=list(original.image_paths)
    external=[]
    for row in source['images']:
        p=Path(row['path']);assert hashlib.sha256(p.read_bytes()).hexdigest()==row['sha256']
        external.append({'source_id':f'manufacturer_pdf_page_{row["page_index"]+1}',
            'document_id':'manufacturer_manual_990886','source_url':source['url'],'pdf_sha256':source['sha256'],
            'pdf_page_number':row['page_index']+1,'attachment_index_one_based':len(images)+1,'image_sha256':row['sha256']})
        images.append(p)
    # Exclude evaluator observations from the source manifest and do not supply benchmark verdicts.
    packet['external_manufacturer_sources']=external
    packet['boundary']='Project sources retain their block IDs; manufacturer pages are a separate document, not project drawings or proof of the selected article.'
    prompt='''Check the actual object in this packet against the supplied project sources and manufacturer pages. This is a bounded applicability control, not an independent full-project audit. No engineer decisions or expected verdicts are supplied. Project TEXT is authoritative; read the graphic images for symbols and exact connections. Manufacturer pages are separate sources: establish document scope, product family, exact execution and accessory conditions before applying them. A series name alone need not determine pole count; cable conductor count is not proof of device poles. Do not transfer a restriction to another manufacturer's device or another family without evidence. A project note permitting alternative manufacturers is not by itself a selected article. Distinguish observed circuit identity, actual drawn contacts, catalogue conditions, missing selection and proven incompatibility. Do not invent manufacturer data or use external tools. Return JSON in Russian with apparatus_identity, applicability_checks (claim, verdict supported/refuted/needs_data, project_evidence, manufacturer_evidence, applicability_reason), remaining_context, and candidate_findings (empty if none justified). Each finding must distinguish proven facts from unknowns, include problem, description, solution, report_section and source evidence. Use original project block IDs and manufacturer source_id values; no unseen source citations. Questions need an evidenced ambiguity and a concrete engineer action, not merely absent packet context. Do not assert physical failure or universal incompatibility from a conditional manufacturer restriction.'''
    wire={'messages':[{'role':'system','content':prompt},{'role':'user','content':json.dumps(packet,ensure_ascii=False)}],'image_paths':[str(p) for p in images]}
    manifest={'request_sha256':evidence_digest(wire),'parent_request_sha256':binding['request_sha256'],
        'project_md_sha256':hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest(),
        'manufacturer_pdf_sha256':source['sha256'],'external_sources':external,'model':'codex/gpt-6-astra',
        'image_bindings':[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in images],
        'history_or_engineer_answers_supplied':False,'manufacturer_evaluator_notes_supplied':False,'automatic_retries':0}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',manifest)
    print('PREPARED',sum(len(m['content']) for m in wire['messages']),len(images),'images',flush=True)
    return wire

async def main(execute):
    wire=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    if (ROOT/'result.json').exists():result=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=asdict(await run_codex_json_messages(wire['messages'],model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='object_operations',project_id='EOM-manufacturer-applicability',image_paths=wire['image_paths'],allowed_tools=''))
        immutable(ROOT/'result.json',result);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'];prepare()
    assert isinstance(result['json_data'].get('applicability_checks'),list)
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');a=p.parse_args();asyncio.run(main(a.execute))
