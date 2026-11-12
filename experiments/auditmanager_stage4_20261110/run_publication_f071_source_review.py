"""Source review of the original F071; known candidate, not blind discovery."""
import argparse
import asyncio
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow, direct_cli_executor
from .run_primary_text_runtime_probe import sha, write

ROOT = Path('/tmp/auditmanager_publication_f071_source_review_20261112')
REPORT = Path('/tmp/auditmanager_runtime_recovered_findings_20261112.json')
MD = Path('/tmp/auditmanager_corrected_source_inputs_20261111/AK/02_work/document.md')
GRAPHICS = Path('/tmp/auditmanager_primary_text_runtime_recovery_bound_20261112/work/blocks_stage02_100')


def immutable(path, value):
    if path.exists():
        assert json.loads(path.read_text()) == value, path
    else:
        write(path, value)


def prepare(root=ROOT, finding_id='F-071', detail_dir=None):
    original = next(f for f in json.loads(REPORT.read_text())['findings'] if f['id'] == finding_id)
    finding = deepcopy(original)
    finding['source_evidence_review_status'] = 'pending'
    finding['source_review_requests'] = [{
        'source_finding_id': 'publication-' + finding_id.replace('-', '') + '-original',
        'source_claim': '\n'.join(str(original.get(k) or '') for k in ('problem', 'description', 'solution', 'risk')),
        'block_ids': original['source_block_ids']}]
    md = MD.read_text()
    queue = build_source_review_queue([finding], md)
    if detail_dir is not None:
        queue['_operation_graphic_detail_dir'] = str(detail_dir.resolve())
    attach_graphic_receipts(queue, md, GRAPHICS)
    max_images = 16 if detail_dir is not None else 8
    request = prepare_source_review_request(queue, finding, md, max_images=max_images)
    payload = json.loads(request['messages'][1]['content'])
    assert not payload['unavailable_images'] and not payload['budgeted_text_block_ids']
    assert request['image_paths']
    manifest = {'report_path': str(REPORT), 'report_sha256': sha(REPORT),
                'md_path': str(MD), 'md_sha256': sha(MD),
                'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
                'known_candidate_control': True, 'engineer_answers_supplied': False,
                'publication_verdict_or_coordinator_assessment_supplied': False,
                'message_chars': sum(len(m['content']) for m in request['messages']),
                'images': [{'path': p, 'sha256': sha(Path(p))} for p in request['image_paths']],
                'request_sha256': hashlib.sha256(json.dumps(request, ensure_ascii=False, sort_keys=True).encode()).hexdigest(),
                'production_acceptance': False}
    root.mkdir(exist_ok=True)
    for name, value in [('finding', finding), ('queue', queue), ('request', request), ('manifest', manifest)]:
        immutable(root / (name + '.json'), value)
    print('PREPARED', manifest['message_chars'], 'characters,', len(request['image_paths']), 'images', flush=True)
    return finding, queue, md, manifest


async def main(execute, root=ROOT, finding_id='F-071', detail_dir=None):
    finding, queue, md, manifest = prepare(root, finding_id, detail_dir)
    if not execute:
        return
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    direct = direct_cli_executor('codex/gpt-6-astra', project_id='AK-' + finding_id.replace('-', '') + '-publication-source-review')
    async def invoke(request, phase):
        assert sha(REPORT) == manifest['report_sha256'] and sha(MD) == manifest['md_sha256']
        return await direct(request, phase)
    write(root / 'process.json', {'status': 'running', 'pid': os.getpid(), 'model': manifest['model']})
    try:
        result = await run_source_review_workflow(queue, finding, md, GRAPHICS, root / 'workflow',
            invoke=invoke, executor_id='direct-codex-gpt-6-astra-high', max_images=16 if detail_dir else 8)
    except Exception as exc:
        write(root / 'process.json', {'status': 'failed', 'error': str(exc), 'pid': os.getpid()})
        raise
    write(root / 'completion.json', result)
    write(root / 'process.json', {'status': 'finished', 'result': result['status'], 'pid': os.getpid()})
    print('COMPLETE', result['status'], flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--finding-id', default='F-071')
    parser.add_argument('--detail-dir', type=Path)
    args = parser.parse_args()
    asyncio.run(main(args.execute, args.root, args.finding_id, args.detail_dir))
