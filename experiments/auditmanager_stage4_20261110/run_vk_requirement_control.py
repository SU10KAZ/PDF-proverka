"""Source-only applicability control on known VK fragments; no expected answers."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks, reconcile_requirement_checks

ROOT = Path('/tmp/auditmanager_vk_requirement_control_20261110')
SOURCE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/VK/versions/v002/02_work/document.md')
IDS = ['blk_c64786c6b8ed4532a0ff7c797c3f335b', 'blk_1d17a25462884606ae486c345090fe20',
       'blk_e6ea1f0742914463bbf7ce02d97c5afa', 'blk_9b1d774c2f6c4127b2f8618e3792a647',
       'blk_2832f2bda58d416ab8c388c81c339b02', 'blk_8a30696159964429a9b51a5b81e5cb7e',
       'blk_0bcb4172b9fc465ca64fe8ab4aafdc27', 'blk_5a8674f9dc2343498ae3b666f20e67c9']


def prepare():
    blocks = authoritative_text_blocks(SOURCE.read_text())
    selected = {bid: blocks[bid] for bid in IDS}
    contract = Path('prompts/pipeline/en/text_analysis_task.md').read_text().split('## Requirement applicability records', 1)[1]
    instructions = '''Review supplied project requirements and specified components.
Determine permitted comparisons, their exact scope and remaining uncertainty.
MD TEXT is authoritative. No images, external documents, history or engineer
answers are supplied. Use no tools, web or files. Extract the objects yourself.
Do not transfer requirements or quantities between systems without source evidence.
Separate project document conflicts from physical defects and actual installation.
Keep arithmetic operands, units and assumptions explicit. Unknown revision or
context remains unknown; do not invent a value to satisfy a schema.
Return JSON with object_registry (id, identity, scope, kind, source_refs),
requirement_registry, requirement_applicability_checks, and findings with
problem, description, solution, evidence and report_section
(technical_findings/questions). Missing context in this packet is an internal
retrieval task, not proof of whole-project absence. Add literal context_queries
when needed. This is a known-source control, not blind whole-project discovery.
''' + contract
    request = {'messages': [{'role': 'system', 'content': instructions},
                            {'role': 'user', 'content': json.dumps({'authoritative_text_blocks': selected}, ensure_ascii=False)}],
               'image_paths': []}
    manifest = {'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
                'source_md_sha256': hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
                'request_sha256': evidence_digest(request), 'block_ids': IDS,
                'message_chars': sum(len(m['content']) for m in request['messages']),
                'automatic_retries': 0, 'history_or_engineer_answers_sent': False,
                'selection_known_case': True, 'max_calls': 1}
    ROOT.mkdir(exist_ok=True)
    for name, value in [('request.json', request), ('manifest.json', manifest)]:
        path = ROOT / name
        if path.exists():
            assert json.loads(path.read_text()) == value
        else:
            path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(manifest, ensure_ascii=False), flush=True)
    return request, manifest, selected


async def execute():
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    request, manifest, blocks = prepare()
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    assert substitute_codex_model('gpt-6-astra') == 'gpt-6-astra'
    with (ROOT / 'attempt.json').open('x') as handle:
        json.dump({'status': 'started', 'request_sha256': manifest['request_sha256']}, handle)
    result = await run_codex_json_messages(request['messages'], model='codex/gpt-6-astra',
        reasoning_effort='high', timeout=900, allowed_tools='',
        stage='requirement_scope_control', project_id='VK-source-requirement-control')
    (ROOT / 'result.json').write_text(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    assert hashlib.sha256(SOURCE.read_bytes()).hexdigest() == manifest['source_md_sha256']
    (ROOT / 'attempt.json').write_text(json.dumps({'status': 'finished', 'is_error': result.is_error}))
    if not result.is_error and isinstance(result.json_data, dict):
        receipt = reconcile_requirement_checks(result.json_data, blocks)
        (ROOT / 'validation.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2))
    print('DONE', result.is_error, result.duration_ms, flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    if args.execute:
        asyncio.run(execute())
    else:
        prepare()
