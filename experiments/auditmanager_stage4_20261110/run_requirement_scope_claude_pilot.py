"""One explicitly payload-approved direct Claude scope pilot; no retries."""
import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from experiments.auditmanager_stage4_20261110.run_requirement_scope_pilot import SOURCE

ROOT=Path('/tmp/auditmanager_requirement_scope_claude_20261110')
PREPARED=Path('/tmp/auditmanager_requirement_scope_astra_20261110/request.json')

async def main():
    request=json.loads(PREPARED.read_text())
    assert hashlib.sha256(SOURCE.read_bytes()).hexdigest()==request['source_sha256']
    payload=json.loads(request['messages'][1]['content'])
    assert len(payload['authoritative_md'])==27170
    assert payload['selected_pdf_pages']==[1,29,30,31,150,151,152]
    request['model']='claude/claude-sonnet-5-5'
    digest=hashlib.sha256(json.dumps(request,ensure_ascii=False,sort_keys=True).encode()).hexdigest()
    ROOT.mkdir(exist_ok=True)
    with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started','request_sha256':digest},f)
    (ROOT/'request.json').write_text(json.dumps(request,ensure_ascii=False,indent=2))
    from backend.app.services.llm.claude_json_runner import run_claude_json_messages
    try:
        result=await run_claude_json_messages(request['messages'],model=request['model'],reasoning_effort='high',timeout=900,
            stage='requirement_scope_pilot',project_id='ITP-known-requirement-scope')
        (ROOT/'result.json').write_text(json.dumps(asdict(result),ensure_ascii=False,indent=2))
        (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','request_sha256':digest,'model_error':result.is_error}))
        print('Finished; error=',result.is_error,'duration_ms=',result.duration_ms)
    except BaseException as exc:
        (ROOT/'attempt.json').write_text(json.dumps({'status':'failed','request_sha256':digest,'error_type':type(exc).__name__}))
        raise

if __name__=='__main__':asyncio.run(main())
