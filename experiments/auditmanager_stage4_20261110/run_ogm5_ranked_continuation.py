"""Single frozen OGM5 ranking control through direct Astra; no retries."""
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT=Path('/tmp/auditmanager_km_ogm5_ranked_continuation_20261110')
EXPECTED='5cd3bc373c26fd8208541c236610f5e8449bd4da75219b5dd4f9f5480ae05719'

async def execute():
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    request=json.loads((ROOT/'request.json').read_text())
    manifest=json.loads((ROOT/'manifest.json').read_text())
    assert evidence_digest(request)==manifest['request_sha256']==EXPECTED
    assert sum(len(m['content']) for m in request['messages'])==116578
    assert len(request['image_paths'])==16
    assert sum(Path(p).stat().st_size for p in request['image_paths'])==4548875
    for path,binding in zip(request['image_paths'],manifest['images'],strict=True):
        assert str(Path(path).resolve())==str(Path(binding['path']).resolve())
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==binding['sha256']
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    assert substitute_codex_model('gpt-6-astra')=='gpt-6-astra'
    attempt=ROOT/'attempt.json'
    with attempt.open('x') as h:
        json.dump({'status':'started','request_sha256':EXPECTED,'model':'codex/gpt-6-astra','automatic_retries':0},h)
    try:
        result=asdict(await run_codex_json_messages(request['messages'],model='codex/gpt-6-astra',
            image_paths=request['image_paths'],reasoning_effort='high',timeout=900,
            stage='object_operations_ogm5_ranked_context',project_id='KM-OGM5-ranking-control',allowed_tools=''))
        temporary=ROOT/'result.tmp';temporary.write_text(json.dumps(result,ensure_ascii=False,indent=2))
        temporary.replace(ROOT/'result.json')
        attempt.write_text(json.dumps({'status':'finished','request_sha256':EXPECTED,'model_error':result['is_error'],'automatic_retries':0}))
        print(json.dumps({k:result.get(k) for k in ('is_error','duration_ms','input_tokens','output_tokens','error_message')},ensure_ascii=False))
    except BaseException as exc:
        attempt.write_text(json.dumps({'status':'interrupted_or_failed','request_sha256':EXPECTED,'error_type':type(exc).__name__}))
        raise

if __name__=='__main__':asyncio.run(execute())
