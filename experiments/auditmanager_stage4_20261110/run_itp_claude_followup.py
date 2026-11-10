"""Direct Claude follow-up; execute only after specific payload authorization."""
import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
from experiments.auditmanager_stage4_20261110 import run_itp_graphic_review_pilot as checks

ROOT=Path('/tmp/auditmanager_itp_claude_followup_20261110')
PREPARED=Path('/tmp/auditmanager_itp_initial_context_astra_20261110/followup_request.json')


def preflight():
    checks.PREPARED=PREPARED; checks.EXPECTED_IMAGES=8
    request,manifest=checks.preflight()
    manifest.update(model='claude/claude-sonnet-5-5',cost='Claude subscription quota; monetary maximum unknown',
        scope='Known-case Astra-generated query retrieval followed by Claude review; mixed-model control, not blind discovery.')
    return request,manifest


async def execute(approved_digest):
    request,manifest=preflight()
    if request['request_sha256']!=approved_digest:
        raise ValueError('Approved payload mismatch')
    model=manifest['model'];ROOT.mkdir(exist_ok=True)
    with (ROOT/'attempt.json').open('x') as f:
        json.dump({'status':'started','request_sha256':approved_digest,'model':model},f)
    (ROOT/'request.json').write_text(json.dumps(request,ensure_ascii=False,indent=2))
    (ROOT/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    from backend.app.services.llm.claude_json_runner import run_claude_json_messages
    try:
        result=await run_claude_json_messages(request['messages'],model=model,image_paths=request['image_paths'],
            timeout=900,reasoning_effort='high',stage='source_review_followup',project_id='ITP-known-model-retrieved-context')
        (ROOT/'result.json').write_text(json.dumps(asdict(result),ensure_ascii=False,indent=2))
        (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','request_sha256':approved_digest,'model':model,'model_error':result.is_error}))
        print('Finished; model_error=',result.is_error)
    except BaseException as exc:
        (ROOT/'attempt.json').write_text(json.dumps({'status':'failed','request_sha256':approved_digest,'error_type':type(exc).__name__}))
        raise


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',metavar='APPROVED_REQUEST_SHA256');args=parser.parse_args()
    if args.execute:asyncio.run(execute(args.execute))
    else:
        _,manifest=preflight()
        Path(__file__).with_name('ITP_claude_followup_preflight.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps(manifest,ensure_ascii=False,indent=2))
