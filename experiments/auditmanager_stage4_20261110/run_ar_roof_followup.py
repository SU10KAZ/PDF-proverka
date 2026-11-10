"""One bounded source-context hop from the model's own AR roof queries."""
import argparse
import asyncio
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
from .run_ar_roof_operations import prepare as initial_prepare, immutable, BASE
from .frozen_operation_request import load_frozen_operation
from backend.app.pipeline.stages.prepare.object_operation_followup import freeze_operation_sources, build_object_operation_followup, verify_operation_source_binding
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

def prepare(object_id, revision=1):
    assert revision in (1, 2, 3)
    from . import run_ar_roof_operations as initial_control
    initial_root = initial_control.ROOT/object_id
    if (initial_root/'request.json').exists():
        initial_root, request, _ = load_frozen_operation(initial_root, BASE/'02_work/document.md')
    else:
        initial_root, request, _ = initial_prepare(object_id)
    source_binding = request.continuation_source
    assert source_binding is not None
    previous_root = initial_root
    if revision > 1:
        previous_root = initial_root/f'context_{revision-1}'
        if (previous_root/'request.json').exists():
            _, request, _ = load_frozen_operation(previous_root, BASE/'02_work/document.md')
        else:
            previous_root, request = prepare(object_id, revision-1)
        assert request is not None
    previous = json.loads((previous_root/'result.json').read_text())
    assert not previous['is_error']
    request, receipt = build_object_operation_followup(request, previous['json_data'])
    root = initial_root/f'context_{revision}'
    root.mkdir(exist_ok=True)
    immutable(root/'retrieval.json', receipt)
    if request is None:
        return root, None
    assert verify_operation_source_binding(request) is None
    wire = {'messages': request.messages, 'image_paths': [str(p) for p in request.image_paths]}
    immutable(root/'request.json', wire)
    immutable(root/'binding.json', {'request_sha256': evidence_digest(wire), 'source_binding': source_binding,
        'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high', 'automatic_retries': 0,
        'expected_answers_supplied': False, 'previous_answer': 'Untrusted source-operation response, not engineer feedback'})
    return root, request

async def main(args):
    root, request = prepare(args.object_id, args.revision)
    print('PREPARED', root, 'images', len(request.image_paths) if request else 0, flush=True)
    if not request or not args.execute:
        return
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    output = root/'result.json'
    if output.exists():
        result = json.loads(output.read_text())
    else:
        with (root/'attempt.json').open('x') as f:
            json.dump({'status': 'started'}, f)
        result = asdict(await run_codex_json_messages(request.messages, model='codex/gpt-6-astra', reasoning_effort='high',
            timeout=900, stage='object_operations', project_id='AR-roof-context', image_paths=request.image_paths, allowed_tools=''))
        immutable(output, result)
        (root/'attempt.json').write_text(json.dumps({'status': 'finished', 'is_error': result['is_error']}))
    assert not result['is_error']
    assert verify_operation_source_binding(request) is None
    immutable(root/'validated.json', validate_object_operation_payload(request, result['json_data']))
    print('COMPLETE', args.object_id, flush=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--object-id', required=True)
    parser.add_argument('--revision', type=int, choices=(1, 2, 3), default=1)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    asyncio.run(main(args))
