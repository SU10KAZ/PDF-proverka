"""Serial direct-Astra pilot after successful completion of the old collection."""
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
from .verify_experimental_seals import verify, verify_run

HERE = Path(__file__).resolve().parent
COLLECTION = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
OUTPUT = Path('/tmp/auditmanager_merge_primary_text_pilot_20261111')


def prerequisite(state, observations):
    if any(p.get('live') is None for p in observations):
        return 'wait'
    if any(p.get('live') is True for p in observations):
        return 'wait'
    if state.get('status') != 'stopped':
        raise ValueError('Old collection absent without planned terminal stop')
    runs = state.get('runs', [])
    if len(runs) != 2 or any(r.get('status') != 'terminal' or r.get('exit_code') != 0 or
            r.get('execution_status') != 'completed_unadjudicated' for r in runs):
        raise ValueError('Old collection completion is not verified')
    return 'ready'


def write_status(state):
    path = OUTPUT / 'pilot_status.json'
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + '\n')
    temp.replace(path)


def reviewed_failure_seals(state, receipt_path):
    receipt = json.loads(receipt_path.read_text())
    if state.get('status') != 'failed_requires_inspection':
        raise ValueError('Expected reviewed terminal collection failure')
    runs = state.get('runs', [])
    if len(runs) != 2 or runs[-1].get('execution_status') != 'failed' or runs[-1].get('exit_code') != 1:
        raise ValueError('Unexpected terminal failed run')
    status_path = Path(runs[-1]['work'])/'classic_control/status/AK.json'
    if str(status_path) != receipt['status_path'] or sha(status_path) != receipt['status_sha256']:
        raise ValueError('Reviewed failure differs')
    if sha(status_path) != runs[-1]['status_sha256']:
        raise ValueError('Collection failure binding differs')
    if sha(Path(receipt['process_log_path'])) != receipt['process_log_sha256']:
        raise ValueError('Reviewed failure log changed')
    successful = verify_run(runs[0], runs[0])
    return {'status': 'reviewed_terminal_failure_allows_independent_pilot_only',
            'review_receipt': str(receipt_path), 'review_sha256': sha(receipt_path),
            'verified_successful_runs': [successful], 'failed_run': runs[-1],
            'failed_audit_accepted': False}


async def main(reviewed_failure=None):
    receipt_path = HERE / 'merge_primary_text_pilot_preparation.json'
    snapshot_path = HERE / 'corrected_candidate_catalog_snapshot_provenance.json'
    receipt = json.loads(receipt_path.read_text())
    snapshot = json.loads(snapshot_path.read_text())
    receipt_hash, snapshot_hash = sha(receipt_path), sha(snapshot_path)
    source = Path(snapshot['source_root'])
    for name, digest in snapshot['source_files'].items():
        if sha(source / name) != digest:
            raise ValueError('Frozen source changed: ' + name)
    for item in receipt['requests'].values():
        if sha(Path(item['path'])) != item['sha256']:
            raise ValueError('Prepared request changed')
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with (OUTPUT / 'pilot_started.json').open('x') as stream:
        json.dump({'pid': os.getpid(), 'started': time.time(), 'script_sha256': sha(Path(__file__)),
                   'preparation_sha256': receipt_hash, 'snapshot_sha256': snapshot_hash}, stream)
    state = {'pid': os.getpid(), 'status': 'waiting_for_old_collection', 'attempts': [],
             'started': time.time(), 'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
             'production_acceptance': False}
    write_status(state)
    try:
        while True:
            if (OUTPUT / 'STOP_REQUESTED').exists():
                state['status'] = 'stopped_before_dispatch'
                return
            collection = json.loads((COLLECTION / 'experimental_collection_status.json').read_text())
            observations = [process(collection.get('pid'), 'run_experimental_collection')]
            observations += [process(r.get('pid'), 'paired_runner') for r in collection.get('runs', [])]
            state.update(observed_at=time.time(), collection_processes=observations)
            write_status(state)
            if reviewed_failure is not None and all(p.get('live') is False for p in observations):
                seals = reviewed_failure_seals(collection, reviewed_failure)
                break
            if prerequisite(collection, observations) == 'ready':
                seals = verify(COLLECTION)
                if len(seals['verified_runs']) != 2 or seals['pending_records']:
                    raise ValueError('Old runs not sealed')
                break
            await asyncio.sleep(10)
        with (OUTPUT / 'preceding_collection_seals.json').open('x') as stream:
            json.dump(seals, stream, ensure_ascii=False, indent=2)
        # All backend imports come from the frozen code, not changing main.
        if any(n == 'backend' or n.startswith('backend.') for n in sys.modules):
            raise RuntimeError('Backend imported before source selection')
        sys.path.insert(0, str(source))
        from backend.app.services.llm.codex_runner import run_codex_json_messages
        from backend.app.services.llm.codex_model_substitution import substitute_codex_model
        state['status'] = 'running'
        for arm in ('control', 'primary_text'):
            item = receipt['requests'][arm]
            path = Path(item['path'])
            if sha(path) != item['sha256'] or sha(receipt_path) != receipt_hash or sha(snapshot_path) != snapshot_hash:
                raise ValueError('Pilot binding changed')
            packet = json.loads(path.read_text())
            for attempt in (1, 2, 3):
                if (OUTPUT / 'STOP_REQUESTED').exists():
                    state['status'] = 'stopped_before_dispatch'
                    return
                if substitute_codex_model('gpt-6-astra') != 'gpt-6-astra':
                    raise ValueError('Model substitution forbidden')
                state.update(active_arm=arm, active_attempt=attempt)
                write_status(state)
                result = await run_codex_json_messages(packet['messages'], model='codex/gpt-6-astra',
                    timeout=3600, stage='merge_primary_text_pilot', project_id='AK-'+arm,
                    reasoning_effort='high', allowed_tools='', image_paths=[])
                out = OUTPUT / f'{arm}_attempt_{attempt}.json'
                with out.open('x') as stream:
                    json.dump(asdict(result), stream, ensure_ascii=False, indent=2)
                state['attempts'].append({'arm': arm, 'attempt': attempt, 'path': str(out),
                    'sha256': sha(out), 'is_error': result.is_error,
                    'json_object': isinstance(result.json_data, dict)})
                write_status(state)
                if not result.is_error and isinstance(result.json_data, dict):
                    break
                if attempt < 3:
                    await asyncio.sleep(10)
            else:
                raise RuntimeError('Pilot attempts exhausted: ' + arm)
        state['status'] = 'completed_unadjudicated'
    except Exception as exc:
        state.update(status='failed_requires_inspection', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        state['finished'] = time.time()
        write_status(state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUTPUT)
    parser.add_argument('--reviewed-failure', type=Path)
    args = parser.parse_args()
    OUTPUT = args.output.resolve()
    asyncio.run(main(args.reviewed_failure))
