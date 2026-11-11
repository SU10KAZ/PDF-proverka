"""Source-only known-object control of the actual automatic continuation path.

The target comes from the current inventory. No historical answers, old model
findings or preselected higher-resolution blocks are supplied. This is a DEV
control, not a blinded full-project discovery evaluation.
"""
import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
from . import run_itp_gvs_operations as initial
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.prepare.object_operation_followup import (
    build_object_operation_followup, verify_operation_source_binding)
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT = Path('/tmp/auditmanager_itp_automatic_graphic_control_20261111')


async def main(execute):
    initial.ROOT = ROOT / 'initial'
    _, request, _ = initial.prepare('pipe66_t411', enable_graphic_details=True)
    print('PREPARED', sum(len(m['content']) for m in request.messages), 'chars',
          len(request.image_paths), 'images', flush=True)
    if not execute:
        return
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    steps = []
    for step in range(4):
        folder = ROOT / f'step_{step:02d}'; folder.mkdir(exist_ok=True)
        wire = {'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
        immutable(folder/'request.json', wire)
        immutable(folder/'binding.json', {'request_sha256':evidence_digest(wire),
            'source_binding':request.continuation_source, 'model':'codex/gpt-6-astra',
            'history_or_expected_answers_supplied':False, 'known_object_control':True})
        assert verify_operation_source_binding(request) is None
        if (folder/'result.json').exists():
            result = json.loads((folder/'result.json').read_text())
        else:
            with (folder/'attempt.json').open('x') as f:
                json.dump({'status':'started'}, f)
            result = asdict(await run_codex_json_messages(request.messages,
                model='codex/gpt-6-astra', reasoning_effort='high', timeout=900,
                stage='object_operations', project_id='ITP-automatic-graphic-control',
                image_paths=request.image_paths, allowed_tools=''))
            immutable(folder/'result.json', result)
            (folder/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
        assert verify_operation_source_binding(request) is None
        if result['is_error']:
            immutable(ROOT/'completion.json', {'status':'model_error','step':step,'automatic_retries':0})
            return
        immutable(folder/'validated.json', validate_object_operation_payload(request,result['json_data']))
        from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass
        if (folder/'continuation.json').exists():
            receipt = json.loads((folder/'continuation.json').read_text())
            if receipt['status'] == 'prepared':
                frozen = json.loads((folder/'followup.json').read_text())
                frozen['image_paths'] = tuple(frozen['image_paths'])
                followup = CodexTargetedPass(**frozen)
            else:
                followup = None
        else:
            followup, receipt = build_object_operation_followup(request,result['json_data'])
            if followup is not None:
                frozen = asdict(followup)
                frozen['image_paths'] = [str(p) for p in followup.image_paths]
                immutable(folder/'followup.json', frozen)
            immutable(folder/'continuation.json', receipt)
        steps.append({'step':step,'stage':request.stage,'continuation':receipt,
                      'request_sha256':evidence_digest(wire)})
        print('STEP', step, receipt['status'], receipt.get('route','text'), flush=True)
        if followup is None:
            break
        request = followup
    immutable(ROOT/'completion.json', {'status':'completed','steps':steps,
        'full_acceptance':False,'known_source_control':True,
        'findings_require_source_review':True})
    print('COMPLETE', flush=True)


if __name__ == '__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--execute',action='store_true')
    asyncio.run(main(parser.parse_args().execute))
