"""Run the prepared format-repair case after the paired merge pilot exits."""
import asyncio
import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import sys
import time
from .materialize_trial_inputs import sha
from .observe_baseline_collection import process

HERE = Path(__file__).resolve().parent
ROOT = Path('/tmp/auditmanager_recovery_format_pilot_20261111')
PREDECESSOR = Path('/tmp/auditmanager_merge_primary_text_pilot_20261111')


def prerequisite(state, live):
    if live is not False:
        return 'wait'
    if state.get('status') != 'completed_unadjudicated':
        raise ValueError('Preceding pilot absent without successful terminal receipt')
    for arm in ('control', 'primary_text'):
        if not any(a.get('arm') == arm and a.get('is_error') is False and a.get('json_object') is True
                   for a in state.get('attempts', [])):
            raise ValueError('Missing preceding arm response: ' + arm)
    return 'ready'


def write(state):
    p = ROOT/'pilot_status.json'
    temp = p.with_suffix('.tmp')
    temp.write_text(json.dumps(state, ensure_ascii=False, indent=2)+'\n')
    temp.replace(p)


async def main():
    prep_path = HERE/'recovery_format_pilot_preparation.json'
    prep = json.loads(prep_path.read_text())
    request_path = Path(prep['request_path'])
    if sha(request_path) != prep['request_sha256']:
        raise ValueError('Prepared request changed')
    request = json.loads(request_path.read_text())
    assert request['model'] == 'codex/gpt-6-astra' and not request['image_paths']
    for name, digest in request['source_hashes'].items():
        if sha(Path(name)) != digest:
            raise ValueError('Original request or response changed')
    snapshot_path = HERE/'corrected_candidate_catalog_snapshot_provenance.json'
    snapshot = json.loads(snapshot_path.read_text())
    source = Path(snapshot['source_root'])
    for name, digest in snapshot['source_files'].items():
        if sha(source/name) != digest:
            raise ValueError('Frozen implementation changed: '+name)
    state = {'pid': os.getpid(), 'started': time.time(), 'status': 'waiting_for_merge_pilot',
             'model': request['model'], 'attempts': [], 'production_acceptance': False}
    ROOT.mkdir(parents=True, exist_ok=True)
    with (ROOT/'pilot_started.json').open('x') as f:
        json.dump({**state, 'request_sha256': sha(request_path), 'script_sha256': sha(Path(__file__)),
                   'snapshot_path': str(snapshot_path), 'snapshot_sha256': sha(snapshot_path)}, f)
    write(state)
    try:
        while True:
            if (ROOT/'STOP_REQUESTED').exists():
                state['status'] = 'stopped_before_dispatch'
                return
            previous = json.loads((PREDECESSOR/'pilot_status.json').read_text())
            observed = process(previous.get('pid'), 'run_merge_primary_text_pilot')
            state.update(observed_at=time.time(), predecessor_process=observed)
            write(state)
            if prerequisite(previous, observed['live']) == 'ready':
                break
            await asyncio.sleep(10)
        for attempt in previous['attempts']:
            if sha(Path(attempt['path'])) != attempt['sha256']:
                raise ValueError('Preceding pilot response changed')
        if any(n == 'backend' or n.startswith('backend.') for n in sys.modules):
            raise ValueError('Backend imported before frozen source selection')
        sys.path.insert(0, str(source))
        from backend.app.services.llm.codex_runner import run_codex_json_messages
        from backend.app.services.llm.codex_model_substitution import substitute_codex_model
        from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass
        from backend.app.pipeline.stages.prepare.candidate_recovery import validate_recovery_payload
        from backend.app.pipeline.stages.prepare.recovery_format import build_recovery_format_followup
        packet = CodexTargetedPass(stage=request['stage'], output_filename='format_response.json',
                                  messages=request['messages'])
        for attempt in (1, 2, 3):
            if (ROOT/'STOP_REQUESTED').exists():
                state['status'] = 'stopped_before_dispatch'
                return
            if sha(request_path) != prep['request_sha256'] or substitute_codex_model('gpt-6-astra') != 'gpt-6-astra':
                raise ValueError('Request/model changed')
            state.update(status='running', active_attempt=attempt)
            write(state)
            result = await run_codex_json_messages(packet.messages, model=request['model'],
                reasoning_effort='high', timeout=900, stage=packet.stage, project_id='AK-T002-format-pilot',
                allowed_tools='', image_paths=[])
            path = ROOT/f'response_attempt_{attempt}.json'
            with path.open('x') as f:
                json.dump(asdict(result), f, ensure_ascii=False, indent=2)
            state['attempts'].append({'path': str(path), 'sha256': sha(path), 'is_error': result.is_error})
            write(state)
            if not result.is_error and isinstance(result.json_data, dict):
                validated = validate_recovery_payload(packet, result.json_data)
                followup, limit = build_recovery_format_followup(packet, validated)
                assert followup is None and limit['status'] == 'format_repair_limit'
                with (ROOT/'validated_response.json').open('x') as f:
                    json.dump({'payload': validated, 'next_repair': limit,
                               'semantic_acceptance': False}, f, ensure_ascii=False, indent=2)
                state.update(status='completed_unadjudicated',
                    validated_response_sha256=sha(ROOT/'validated_response.json'))
                return
            if attempt < 3:
                await asyncio.sleep(10)
        raise RuntimeError('Format pilot attempts exhausted')
    except Exception as exc:
        state.update(status='failed_requires_inspection', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        state['finished'] = time.time()
        write(state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT)
    parser.add_argument('--predecessor', type=Path, default=PREDECESSOR)
    args = parser.parse_args()
    ROOT = args.output.resolve()
    PREDECESSOR = args.predecessor.resolve()
    asyncio.run(main())
