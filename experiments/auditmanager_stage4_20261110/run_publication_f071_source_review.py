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


def prepare():
    original = next(f for f in json.loads(REPORT.read_text())['findings'] if f['id'] == 'F-071')
    finding = deepcopy(original)
    finding['source_evidence_review_status'] = 'pending'
    finding['source_review_requests'] = [{
        'source_finding_id': 'publication-F071-original',
        'source_claim': '\n'.join(str(original.get(k) or '') for k in ('problem', 'description', 'solution', 'risk')),
        'block_ids': ['blk_127bea47b93f4214ac6430c54b21517a']}]
    md = MD.read_text()
    queue = attach_graphic_receipts(build_source_review_queue([finding], md), md, GRAPHICS)
    request = prepare_source_review_request(queue, finding, md, max_images=8)
    payload = json.loads(request['messages'][1]['content'])
    assert not payload['unavailable_images'] and not payload['budgeted_text_block_ids']
    assert len(request['image_paths']) == 1
    manifest = {'report_path': str(REPORT), 'report_sha256': sha(REPORT),
                'md_path': str(MD), 'md_sha256': sha(MD),
                'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
                'known_candidate_control': True, 'engineer_answers_supplied': False,
                'publication_verdict_or_coordinator_assessment_supplied': False,
                'message_chars': sum(len(m['content']) for m in request['messages']),
                'images': [{'path': p, 'sha256': sha(Path(p))} for p in request['image_paths']],
                'request_sha256': hashlib.sha256(json.dumps(request, ensure_ascii=False, sort_keys=True).encode()).hexdigest(),
                'production_acceptance': False}
    ROOT.mkdir(exist_ok=True)
    for name, value in [('finding', finding), ('queue', queue), ('request', request), ('manifest', manifest)]:
        immutable(ROOT / (name + '.json'), value)
    print('PREPARED', manifest['message_chars'], 'characters, one image', flush=True)
    return finding, queue, md, manifest


async def main(execute):
    finding, queue, md, manifest = prepare()
    if not execute:
        return
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    direct = direct_cli_executor('codex/gpt-6-astra', project_id='AK-F071-publication-source-review')
    async def invoke(request, phase):
        assert sha(REPORT) == manifest['report_sha256'] and sha(MD) == manifest['md_sha256']
        return await direct(request, phase)
    write(ROOT / 'process.json', {'status': 'running', 'pid': os.getpid(), 'model': manifest['model']})
    try:
        result = await run_source_review_workflow(queue, finding, md, GRAPHICS, ROOT / 'workflow',
            invoke=invoke, executor_id='direct-codex-gpt-6-astra-high', max_images=8)
    except Exception as exc:
        write(ROOT / 'process.json', {'status': 'failed', 'error': str(exc), 'pid': os.getpid()})
        raise
    write(ROOT / 'completion.json', result)
    write(ROOT / 'process.json', {'status': 'finished', 'result': result['status'], 'pid': os.getpid()})
    print('COMPLETE', result['status'], flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    asyncio.run(main(parser.parse_args().execute))
