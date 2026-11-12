"""Current recovery contract against the four original H01 no-duplicate controls."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.prepare.candidate_recovery import build_recovery_passes, validate_recovery_payload
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT = Path('/tmp/auditmanager_h01_duplicate_controls_20261112')
INVENTORY = Path('experiments/auditmanager_stage2_20261110/merge_coverage_inventory.json')
TARGETS = {'AK': {'T-004', 'T-008', 'T-017'}, 'ITP': {'T-016'}}


def save(p, x):
    p.write_text(json.dumps(x, ensure_ascii=False, indent=2) + '\n')


def prepare():
    requests = []
    for project in json.loads(INVENTORY.read_text())['projects']:
        disc = project['discipline']
        if disc not in TARGETS:
            continue
        folder = ROOT / disc
        folder.mkdir(parents=True, exist_ok=True)
        for ref in project['sources']:
            p = Path(ref['path'])
            assert hashlib.sha256(p.read_bytes()).hexdigest() == ref['sha256']
            (folder / p.name).write_bytes(p.read_bytes())
        source = Path(project['sources'][0]['path']).parent
        (folder / 'document_graph.json').write_bytes((source / 'document_graph.json').read_bytes())
        crops = folder / 'blocks_stage02_100'
        if not crops.exists():
            crops.symlink_to(source / 'blocks_stage02_100', target_is_directory=True)
        md = source.parents[2] / '02_work/document.md'
        selected = []
        for request in build_recovery_passes(folder, md):
            packet = json.loads(request.messages[-1]['content'])
            sid = packet['candidate']['source_id']
            if packet['candidate']['origin'] != 'text' or sid not in TARGETS[disc]:
                continue
            selected.append(sid)
            sub = folder / sid
            sub.mkdir(exist_ok=True)
            payload = {'messages': request.messages, 'image_paths': [str(p) for p in request.image_paths]}
            binding = {'request_sha256': evidence_digest(payload), 'model': 'codex/gpt-6-astra',
                'reasoning_effort': 'high', 'message_chars': sum(len(m['content']) for m in request.messages),
                'md_sha256': hashlib.sha256(md.read_bytes()).hexdigest(), 'md_path': str(md),
                'images': [{'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in request.image_paths],
                'known_case': True, 'engineer_answers_supplied': False, 'expected_target_ids_supplied': False,
                'existing_findings_supplied': True, 'automatic_retries': 0}
            for name, value in [('request.json', payload), ('binding.json', binding)]:
                p = sub / name
                if p.exists(): assert json.loads(p.read_text()) == value
                else: save(p, value)
            requests.append((sub, request, binding))
        assert set(selected) == TARGETS[disc]
    assert len(requests) == 4
    return requests


async def execute():
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    assert substitute_codex_model('gpt-6-astra') == 'gpt-6-astra'
    for folder, request, binding in prepare():
        if (folder / 'result.json').exists():
            continue
        with (folder / 'attempt.json').open('x') as f:
            json.dump({'status': 'started', 'pid': os.getpid(), **binding}, f)
        print('START', str(folder), binding['message_chars'], len(request.image_paths), flush=True)
        result = asdict(await run_codex_json_messages(request.messages, model='codex/gpt-6-astra',
            reasoning_effort='high', timeout=900, stage='candidate_recovery',
            project_id='H01-' + folder.parent.name + '-' + folder.name,
            image_paths=request.image_paths, allowed_tools=''))
        save(folder / 'result.json', result)
        save(folder / 'attempt.json', {'status': 'finished', 'is_error': result['is_error']})
        assert not result['is_error'] and isinstance(result.get('json_data'), dict), 'Inspect failed request before retry'
        assert hashlib.sha256(Path(binding['md_path']).read_bytes()).hexdigest() == binding['md_sha256']
        assert all(hashlib.sha256(Path(i['path']).read_bytes()).hexdigest() == i['sha256'] for i in binding['images'])
        save(folder / 'validated.json', validate_recovery_payload(request, result['json_data']))
        print('FINISHED', folder.name, flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('--execute', action='store_true'); a = p.parse_args()
    if a.execute: asyncio.run(execute())
    else:
        for folder, _, binding in prepare(): print(folder, json.dumps(binding, ensure_ascii=False))
