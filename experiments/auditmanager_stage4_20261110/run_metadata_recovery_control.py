"""Bounded DEV control; historic candidate and explicitly synthetic mixed variant."""
import argparse
import asyncio
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path

from backend.app.pipeline.stages.prepare.candidate_recovery import build_recovery_passes, validate_recovery_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT = Path('/tmp/auditmanager_metadata_recovery_control_20261112')
BASE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002')
HISTORY = Path('experiments/auditmanager_stage2_20261110/metadata_gate_replay.json')


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def prepare(kind):
    folder = ROOT / kind
    folder.mkdir(parents=True, exist_ok=True)
    candidate = deepcopy(json.loads(HISTORY.read_text())['cases'][0]['input'])
    # Remove the earlier peer comparison: it is a reviewer verdict, not a source.
    candidate.pop('_comparison', None)
    if kind == 'mixed_synthetic':
        candidate['recommendation'] = 'Исправить тип фрагмента в разметке.'
    source = BASE / '03_analysis/runs/control'
    for name in ['document_graph.json']:
        (folder / name).write_bytes((source / name).read_bytes())
    crops = folder / 'blocks_stage02_100'
    if not crops.exists():
        crops.symlink_to(source / 'blocks_stage02_100', target_is_directory=True)
    for name, value in {
        '02_text_analysis.json': {'text_findings': []},
        '01_blocks_analysis.json': {'block_analyses': [{
            'block_id': 'blk_bd26d730d0a84c57900a87e3f5a47cce',
            'findings': [], 'deferred_findings': [candidate]}]},
        '03_findings.json': {'findings': []},
    }.items():
        save(folder / name, value)
    md = BASE / '02_work/document.md'
    request, = build_recovery_passes(folder, md)
    packet = {'messages': request.messages, 'image_paths': [str(p) for p in request.image_paths]}
    binding = {
        'kind': kind, 'synthetic': kind == 'mixed_synthetic',
        'request_sha256': evidence_digest(packet),
        'md_sha256': hashlib.sha256(md.read_bytes()).hexdigest(),
        'images': [{'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in request.image_paths],
        'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
        'message_chars': sum(len(m['content']) for m in request.messages),
        'engineer_answers_supplied': False, 'expected_verdict_supplied': False,
        'known_candidate_supplied': True, 'independent_discovery': False,
        'removed_prior_peer_verdict': True, 'automatic_retries': 0,
    }
    for name, value in [('request.json', packet), ('binding.json', binding)]:
        path = folder / name
        if path.exists():
            assert json.loads(path.read_text()) == value, 'Frozen request changed'
        else:
            save(path, value)
    return request, binding


async def execute(kind):
    request, binding = prepare(kind)
    folder = ROOT / kind
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    assert substitute_codex_model('gpt-6-astra') == 'gpt-6-astra'
    with (folder / 'attempt.json').open('x') as f:
        json.dump({'status': 'started', 'pid': os.getpid(), **binding}, f)
    print('START', kind, binding['message_chars'], len(request.image_paths), flush=True)
    result = asdict(await run_codex_json_messages(
        request.messages, model='codex/gpt-6-astra', reasoning_effort='high',
        timeout=900, stage='candidate_recovery', project_id='EOM-metadata-' + kind,
        image_paths=request.image_paths, allowed_tools=''))
    save(folder / 'result.json', result)
    save(folder / 'attempt.json', {'status': 'finished', 'is_error': result['is_error']})
    assert hashlib.sha256((BASE / '02_work/document.md').read_bytes()).hexdigest() == binding['md_sha256']
    assert all(hashlib.sha256(Path(i['path']).read_bytes()).hexdigest() == i['sha256'] for i in binding['images'])
    if not result['is_error'] and isinstance(result.get('json_data'), dict):
        save(folder / 'validated.json', validate_recovery_payload(request, result['json_data']))
    print('FINISHED', kind, 'error=', result['is_error'], flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--kind', choices=['historical', 'mixed_synthetic'], required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    if args.execute:
        asyncio.run(execute(args.kind))
    else:
        print(json.dumps(prepare(args.kind)[1], ensure_ascii=False, indent=2))
