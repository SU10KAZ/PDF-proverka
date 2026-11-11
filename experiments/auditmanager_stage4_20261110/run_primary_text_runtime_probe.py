"""Prepare/run a sealed AK runtime merge probe; direct Codex only, no history."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
OLD = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111/ak_r1_classic_current/classic_control')
SOURCES = Path('/tmp/auditmanager_corrected_source_inputs_20261111/AK')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    tmp.replace(path)


def prepare(destination):
    from backend.app.services.common.audit_scope import bind_audit_scope
    from backend.app.pipeline.stages.prepare.prompt_builder import build_findings_merge_messages
    sealed = OLD / 'requests/AK/1794421719150038973/request.json'
    original = json.loads(sealed.read_text())
    text, blocks = original['messages'][1]['content'].split('## 02_text_analysis.json:\n\n')[1].split('\n\n## 01_blocks_analysis.json:\n\n', 1)
    destination.mkdir(parents=True, exist_ok=False)
    output = destination / 'input_analyses'
    output.mkdir()
    for name, content in [('02_text_analysis.json', text), ('01_blocks_analysis.json', blocks)]:
        write(output / name, json.loads(content))
    info = json.loads((SOURCES / '01_input/project_info.json').read_text())
    assert info['section'] == 'SS'
    old_md = OLD / 'inputs/AK/versions/v001/02_work/document.md'
    assert sha(old_md) == sha(SOURCES / '02_work/document.md')
    prior = os.environ.get('AUDIT_MERGE_PRIMARY_TEXT')
    try:
        os.environ['AUDIT_MERGE_PRIMARY_TEXT'] = '1'
        with bind_audit_scope(output_dir=output, version_dir=SOURCES, project_id='AK-runtime-probe', version_id='v001'):
            messages = build_findings_merge_messages(info, 'AK-runtime-probe')
    finally:
        if prior is None:
            os.environ.pop('AUDIT_MERGE_PRIMARY_TEXT', None)
        else:
            os.environ['AUDIT_MERGE_PRIMARY_TEXT'] = prior
    request = destination / 'request.json'
    write(request, {'messages': messages, 'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
                    'allowed_tools': '', 'image_paths': []})
    receipt = {'status': 'prepared_not_dispatched', 'git_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
               'request_sha256': sha(request), 'source_request_sha256': sha(sealed),
               'md_sha256': sha(old_md), 'section': info['section'],
               'message_characters': sum(len(m['content']) for m in messages),
               'scope': 'Known AK merge-only runtime probe, not a full audit or paired quality comparison.',
               'changes_since_prior_pilot': ['Current runtime builder and subclaim instructions', 'Corrected SS profile for AK', 'Primary source manifest'],
               'history_and_engineer_answers_attached': False, 'production_acceptance': False}
    write(destination / 'preparation.json', receipt)
    print(json.dumps(receipt, ensure_ascii=False))


async def run(destination):
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    preparation = json.loads((destination / 'preparation.json').read_text())
    request = destination / 'request.json'
    assert sha(request) == preparation['request_sha256']
    assert substitute_codex_model('gpt-6-astra') == 'gpt-6-astra'
    packet = json.loads(request.read_text())
    with (destination / 'started.json').open('x') as stream:
        json.dump({'pid': os.getpid(), 'started': time.time(), 'request_sha256': sha(request)}, stream)
    state = {'pid': os.getpid(), 'status': 'running', 'model': packet['model'], 'attempts': [], 'started': time.time()}
    write(destination / 'status.json', state)
    try:
        for attempt in range(1, 4):
            if (destination / 'STOP_REQUESTED').exists():
                state['status'] = 'stopped_before_dispatch'
                break
            assert sha(request) == preparation['request_sha256']
            result = await run_codex_json_messages(packet['messages'], model=packet['model'],
                reasoning_effort=packet['reasoning_effort'], allowed_tools='', image_paths=[],
                stage='primary_text_runtime_probe', project_id='AK-runtime-probe', timeout=3600)
            path = destination / f'attempt_{attempt}.json'
            write(path, asdict(result))
            state['attempts'].append({'attempt': attempt, 'sha256': sha(path), 'is_error': result.is_error,
                                      'json_object': isinstance(result.json_data, dict)})
            write(destination / 'status.json', state)
            if not result.is_error and isinstance(result.json_data, dict):
                state['status'] = 'completed_unadjudicated'
                break
            if attempt < 3:
                await asyncio.sleep(10)
        else:
            state['status'] = 'failed_requires_inspection'
    except Exception as exc:
        state.update(status='failed_requires_inspection', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        state['finished'] = time.time()
        write(destination / 'status.json', state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'run'])
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    if args.action == 'prepare':
        prepare(args.destination.resolve())
    else:
        asyncio.run(run(args.destination.resolve()))
