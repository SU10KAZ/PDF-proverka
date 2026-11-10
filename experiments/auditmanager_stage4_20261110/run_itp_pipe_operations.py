"""Known-source ITP pipe identity and dimensional controls using actual operation requests, without answers."""
import argparse
import asyncio
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes, validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
INVENTORY = Path('/tmp/auditmanager_itp_pipe_source_inventory_20261111')
BASE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/ITP/versions/v003')

ROOT = Path('/tmp/auditmanager_itp_pipe_operations_20261111')

def immutable(path, data):
    if path.exists():
        assert json.loads(path.read_text()) == data, str(path)
    else:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2)+'\n')

def prepare(object_id, packet_index=0):
    assert object_id.replace('_', '').isalnum()
    root = ROOT/object_id
    root.mkdir(parents=True, exist_ok=True)
    source = INVENTORY/'ITP/execution'/f'object_inventory_{packet_index:03d}'/'result.json'
    manifest = json.loads((INVENTORY/'ITP/manifest.json').read_text())
    assert manifest['full_source_md_sha256'] == hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest()
    raw = json.loads(source.read_text())
    assert not raw['is_error']
    objects = [o for o in raw['json_data']['object_registry'] if o['id'] == object_id]
    assert len(objects) == 1
    immutable(root/'02_text_analysis.json', {'object_registry': objects, 'text_findings': []})
    crops = root/'blocks_stage02_100'
    if not crops.exists():
        crops.symlink_to(BASE/'03_analysis/runs/control/blocks_stage02_100', target_is_directory=True)
    request, = build_object_operation_passes(root, BASE/'02_work/document.md', max_requests=1, enable_followup=True)
    packet = json.loads(request.messages[-1]['content'])
    packet['operations'] = [o for o in packet['operations'] if o['operation'] in ('identity_across_views', 'document_links', 'dimension_consistency')]
    assert len(packet['operations']) == 3
    request = replace(request, messages=[request.messages[0], {'role': 'user', 'content': json.dumps(packet, ensure_ascii=False)}])
    wire = {'messages': request.messages, 'image_paths': [str(p) for p in request.image_paths]}
    binding = {'source_binding': request.continuation_source, 'request_sha256': evidence_digest(wire), 'inventory_result_sha256': hashlib.sha256(source.read_bytes()).hexdigest(), 'inventory_result_path': str(source),
        'md_sha256': hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest(),
        'images': [{'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in request.image_paths],
        'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high', 'automatic_retries': 0,
        'known_source_control': True, 'expected_answers_supplied': False}
    immutable(root/'request.json', wire)
    immutable(root/'binding.json', binding)
    return root, request, binding

async def main(args):
    global INVENTORY, ROOT
    INVENTORY = Path(args.inventory_root)
    ROOT = Path(args.output_root)
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    for object_id in args.objects:
        root, request, binding = prepare(object_id, args.packet)
        print('PREPARED', object_id, flush=True)
        if not args.execute:
            continue
        from backend.app.services.llm.codex_runner import run_codex_json_messages
        output = root/'result.json'
        if output.exists():
            result = json.loads(output.read_text())
        else:
            with (root/'attempt.json').open('x') as f:
                json.dump({'status': 'started', **binding}, f)
            result = asdict(await run_codex_json_messages(request.messages, model='codex/gpt-6-astra', reasoning_effort='high',
                timeout=900, stage='object_operations', project_id='ITP-pipe-operation-control', image_paths=request.image_paths, allowed_tools=''))
            immutable(output, result)
            (root/'attempt.json').write_text(json.dumps({'status': 'finished', 'is_error': result['is_error']}))
        assert not result['is_error']
        prepare(object_id, args.packet)  # Recheck original inputs and request binding.
        validated = validate_object_operation_payload(request, result['json_data'])
        immutable(root/'validated.json', validated)
        print('COMPLETE', object_id, 'findings', len(validated['findings']), flush=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--objects', nargs='+', required=True)
    parser.add_argument('--inventory-root', default=str(INVENTORY))
    parser.add_argument('--output-root', default=str(ROOT))
    parser.add_argument('--packet', type=int, default=0)
    parser.add_argument('--execute', action='store_true')
    asyncio.run(main(parser.parse_args()))
