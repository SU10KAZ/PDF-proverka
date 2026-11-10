"""One payload-approved Astra graphic review; default is local preflight only."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path

PREPARED = Path('/tmp/auditmanager_itp_tank_source_review_prepared.json')
ROOT = Path('/tmp/auditmanager_itp_graphic_review_astra_20261110')
MD = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/ITP/versions/v003/02_work/document.md')
MODEL = 'codex/gpt-6-astra'


def preflight():
    from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks
    request = json.loads(PREPARED.read_text())
    digest = hashlib.sha256(json.dumps({'messages': request['messages'], 'images': request['image_bindings']}, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    if digest != request['request_sha256']:
        raise ValueError('Prepared request fingerprint changed')
    payload = json.loads(request['messages'][1]['content'])
    md = MD.read_text()
    if hashlib.sha256(md.encode()).hexdigest() != payload['source_md_sha256']:
        raise ValueError('Source version changed')
    blocks = authoritative_text_blocks(md)
    for source in payload['authoritative_text']:
        if blocks.get(source['block_id']) != source['authoritative_text']:
            raise ValueError('Source TEXT changed')
    if request['image_paths'] != [im['path'] for im in request['image_bindings']]:
        raise ValueError('Image order changed')
    for im in request['image_bindings']:
        if hashlib.sha256(Path(im['path']).read_bytes()).hexdigest() != im['sha256']:
            raise ValueError('Image version changed')
    if payload['finding_id'] != 'F-042' or len(request['image_paths']) != 6:
        raise ValueError('Unexpected pilot scope')
    manifest = {'request_sha256': digest, 'model': MODEL, 'finding_id': 'F-042',
                'message_chars': sum(len(m['content']) for m in request['messages']),
                'text_blocks': len(payload['authoritative_text']), 'images': len(request['image_paths']),
                'image_bytes': sum(Path(p).stat().st_size for p in request['image_paths']),
                'max_cli_invocations': 1, 'automatic_retries': 0, 'timeout_seconds': 900,
                'cost': 'Codex subscription quota; monetary maximum unknown',
                'scope': 'Known-case supplied-context verification; not independent context retrieval or blind audit',
                'approval_required': 'Specific approval of this text and six project images; preflight sends nothing'}
    return request, manifest


async def execute(approved_digest):
    request, manifest = preflight()
    if manifest['request_sha256'] != approved_digest:
        raise ValueError('Approval digest mismatch')
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    if substitute_codex_model('gpt-6-astra') != 'gpt-6-astra':
        raise ValueError('Model substitution is not authorized')
    ROOT.mkdir(exist_ok=True)
    attempt = ROOT/'attempt.json'
    with attempt.open('x') as f:
        json.dump({'status': 'started', 'request_sha256': approved_digest}, f)
    (ROOT/'request.json').write_text(json.dumps(request, ensure_ascii=False, indent=2))
    (ROOT/'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    previous = os.environ.get('CODEX_TRANSIENT_RETRIES')
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    try:
        result = await run_codex_json_messages(request['messages'], model=MODEL,
            image_paths=request['image_paths'], timeout=900,
            stage='source_review_pilot', project_id='ITP-known-graphic-verifier')
        (ROOT/'result.json').write_text(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        attempt.write_text(json.dumps({'status': 'finished', 'request_sha256': approved_digest,
            'model_error': result.is_error, 'semantic_verified': False, 'automatic_application': False}))
        print('Finished; model_error=', result.is_error, '; semantic review required')
    except BaseException as exc:
        attempt.write_text(json.dumps({'status': 'failed', 'request_sha256': approved_digest,
                                      'error_type': type(exc).__name__}))
        raise
    finally:
        if previous is None:
            os.environ.pop('CODEX_TRANSIENT_RETRIES', None)
        else:
            os.environ['CODEX_TRANSIENT_RETRIES'] = previous


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', metavar='APPROVED_REQUEST_SHA256')
    args = parser.parse_args()
    if args.execute:
        asyncio.run(execute(args.execute))
    else:
        _, manifest = preflight()
        Path(__file__).with_name('ITP_graphic_review_pilot_preflight.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+'\n')
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
