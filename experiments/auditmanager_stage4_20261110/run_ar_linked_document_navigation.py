"""Navigate available linked sources with explicit versions, without historical answers."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from .run_ar_roof_operations import immutable

ROOT = Path('/tmp/auditmanager_ar_linked_document_navigation_20261111')
PREVIOUS = Path('/tmp/auditmanager_ar_rope_navigation_followup_20261111')
DIRECTORY = Path('/home/coder/projects/PDF-proverka/projects_v2/objects/314_Sobytie_6_1_Donstroy/disciplines/KM/documents')
ARCHIVE = Path('/home/coder/projects/PDF-proverka/projects_v2/_system/destructive_backups/20260914T143051948760Z_00580716-_5.2_V1_v001__delete_project/01_input/ПД-00580716-КМ5.2_V1_results.md')


def prepare():
    ROOT.mkdir(exist_ok=True)
    previous = json.loads((PREVIOUS/'result.json').read_text())
    assert not previous['is_error']
    packet = json.loads(json.loads((PREVIOUS/'request.json').read_text())['messages'][-1]['content'])
    manifest = json.loads((DIRECTORY/'ПД-00580716-КМ5.2/versions/v001/01_input/input_manifest.json').read_text())
    expected = next(r['sha256'] for r in manifest['input_quad'] if r['role'] == 'document_md')
    assert hashlib.sha256(ARCHIVE.read_bytes()).hexdigest() == expected
    paths = [('v001', ARCHIVE), ('v002', DIRECTORY/'ПД-00580716-КМ5.2/versions/v002/02_work/document.md')]
    sources, bindings = [], []
    for version, path in paths:
        raw = path.read_bytes(); md = raw.decode()
        sources.append({'document_code':'ПД-00580716-КМ5.2', 'version':version,
            'source_md_sha256':hashlib.sha256(raw).hexdigest(),
            'authoritative_text':[{'block_id':bid, 'text':text} for bid,text in authoritative_text_blocks(md).items()]})
        bindings.append({'version':version, 'path':str(path), 'sha256':hashlib.sha256(raw).hexdigest()})
    index = sorted(p.name for p in DIRECTORY.iterdir() if p.is_dir())
    instruction = '''Review the supplied linked-document TEXT to plan the next source checks for the unresolved object.
No tools, web, historical findings or engineer answers. Prepared TEXT is authoritative.
Two local versions of one linked document are supplied SEPARATELY. Do not silently merge them or assume either was coordinated with the original AR version. A matching cipher or object directory is navigation evidence, not proven technical applicability.
Read the general scope, component descriptions and document references. Explain what the text can and cannot resolve for this object. Do not infer physical strength, capacity, failure or installation impossibility from names or missing calculations. Do not treat a model's previous observations as established facts.
The directory list establishes availability only in this specific local directory now, never absence in the project or at historical audit time.
Return JSON with version_assessments:[{version, relevant_source_blocks:[{block_id, quote, relevance, applicability_limit}], unresolved_graphic_checks}], requested_documents:[{document_code, reason, source_version, block_id, quote}], next_action, remaining_limits. Quote only exact supplied TEXT. This is navigation, not an audit finding or engineer verdict.'''
    payload = {'object':packet['object'], 'previous_operation_untrusted':previous['json_data'],
        'local_document_index':index, 'linked_sources':sources,
        'source_selection_boundary':'Known linked-document follow-up after model-selected references; not blind whole-project discovery.'}
    wire = {'messages':[{'role':'system','content':instruction}, {'role':'user','content':json.dumps(payload,ensure_ascii=False)}], 'image_paths':[]}
    binding = {'request_sha256':evidence_digest(wire), 'source_bindings':bindings,
        'previous_result_sha256':evidence_digest(previous), 'local_document_index':index,
        'model':'codex/gpt-6-astra', 'history_or_engineer_answers_supplied':False,
        'automatic_retries':0, 'message_chars':sum(len(m['content']) for m in wire['messages'])}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',binding)
    print('PREPARED',binding['message_chars'],'chars',flush=True)
    return wire


async def main(execute):
    wire=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    if (ROOT/'result.json').exists():result=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=asdict(await run_codex_json_messages(wire['messages'],model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='source_navigation',project_id='AR-linked-documents',allowed_tools=''))
        immutable(ROOT/'result.json',result)
        (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'];prepare()
    payload=json.loads(wire['messages'][1]['content'])
    sources={s['version']:{b['block_id']:b['text'] for b in s['authoritative_text']} for s in payload['linked_sources']}
    for row in result['json_data']['version_assessments']:
        for item in row['relevant_source_blocks']:
            assert item['quote'] and item['quote'] in sources[row['version']][item['block_id']]
    for item in result['json_data']['requested_documents']:
        assert item['quote'] and item['quote'] in sources[item['source_version']][item['block_id']]
    immutable(ROOT/'validation.json',{'literal_quotes_verified':True,'technical_applicability_verified':False,'semantic_acceptance':False})
    print('COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
