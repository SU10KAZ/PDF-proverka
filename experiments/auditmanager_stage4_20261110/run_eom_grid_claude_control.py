"""One independent Claude reading of the exact prepared EOM grid task.

Same source task as Astra, not a complete quality/model benchmark. Claude's
transport may resize images; record encoded sizes rather than claiming byte equality.
"""
import asyncio
import base64
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry

SOURCE=Path('/tmp/auditmanager_eom_graphic_recovery_20261110_grid')
ROOT=Path('/tmp/auditmanager_eom_graphic_grid_claude_control_20261110')


def prepare():
    from backend.app.services.llm.claude_json_runner import _image_blocks
    request=json.loads((SOURCE/'request.json').read_text())
    parent=json.loads((SOURCE/'manifest.json').read_text())
    assert evidence_digest(request)==parent['request_sha256']
    for t in parent['source_tiles']['tiles']:
        assert hashlib.sha256(Path(t['path']).read_bytes()).hexdigest()==t['sha256']
    blocks,resized,error=_image_blocks(request['image_paths']);assert not error,error
    encoded=[{'sha256':hashlib.sha256(base64.b64decode(b['source']['data'])).hexdigest(),
              'bytes':len(base64.b64decode(b['source']['data']))} for b in blocks if b['type']=='image']
    manifest={'source_request_sha256':parent['request_sha256'],'model':'claude/claude-sonnet-5-5',
              'reasoning_effort':'high','source_images':parent['source_tiles']['tiles'],
              'encoded_images':encoded,'resized_images':resized,'message_chars':parent['message_chars'],
              'model_answers_supplied':False,'engineer_answers_supplied':False,'automatic_retries':0,
              'purpose':'Read branch identities after earlier Astra extraction lacked marks; same task as pending grid Astra.',
              'cost':'Claude subscription quota; monetary maximum unknown.'}
    ROOT.mkdir(exist_ok=True)
    for name,data in [('request.json',request),('manifest.json',manifest)]:
        p=ROOT/name
        if p.exists():assert json.loads(p.read_text())==data
        else:p.write_text(json.dumps(data,ensure_ascii=False,indent=2))
    return request,manifest


async def main():
    request,manifest=prepare()
    from backend.app.services.llm.claude_json_runner import run_claude_json_messages
    output=ROOT/'result.json'
    if output.exists():result=json.loads(output.read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:
            json.dump({'status':'started','request_sha256':manifest['source_request_sha256']},f)
        print('START',manifest['message_chars'],'chars',len(request['image_paths']),'images','resized',manifest['resized_images'],flush=True)
        result=asdict(await run_claude_json_messages(request['messages'],model='claude/claude-sonnet-5-5',
            reasoning_effort='high',timeout=900,stage='graphic_inventory_recovery',project_id='EOM-grid-model-control',
            image_paths=request['image_paths'],allowed_tools=''))
        output.write_text(json.dumps(result,ensure_ascii=False,indent=2))
        (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'] and isinstance(result['json_data'],dict)
    prepare()
    coverage=reconcile_object_registry(result['json_data'],{t['block_id'] for t in manifest['source_images']})
    (ROOT/'coverage.json').write_text(json.dumps(coverage,ensure_ascii=False,indent=2))
    print('COMPLETE',len(coverage['objects']),'objects',coverage['unresolved_source_blocks'],'unresolved',flush=True)


if __name__=='__main__':asyncio.run(main())
