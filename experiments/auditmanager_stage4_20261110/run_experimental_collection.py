"""Wait for the frozen historical series, then dispatch independent arms serially."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
REPO = Path(__file__).resolve().parents[2]


def prerequisite(state, live):
    status = state.get('status')
    if status not in ('running', 'completed_collection_unadjudicated'):
        raise ValueError('Historical collection unsuccessful or unknown: ' + str(status))
    if status == 'running':
        if live is False:
            raise ValueError('Historical process absent without terminal success')
        return 'wait'
    rows = state.get('runs', [])
    if len(rows) != 18 or any(r.get('exit_code') != 0 or
        r.get('execution_status') != 'completed_unadjudicated' for r in rows):
        raise ValueError('Historical completion receipts incomplete')
    return 'ready' if live is False else 'wait'


def write(path, value):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    temp.replace(path)


def main(root=ROOT, baseline_root=None):
    root = Path(root).resolve()
    baseline_root = Path(baseline_root).resolve() if baseline_root else root
    from .materialize_trial_inputs import sha
    from .observe_baseline_collection import process
    from .paired_protocol import shared_manifest, verify_shared_protocol, verify_trial_binding
    from .verify_completed_baseline_seals import verify as verify_baseline_seals
    trial_path = root / 'experimental_collection_trial.json'
    trial = json.loads(trial_path.read_text())
    trial_hash = sha(trial_path)
    bound_keys = ('evaluator', 'acceptance_protocol', 'source_snapshot', 'experimental_decisions',
                  'preflight_checks', 'source_input_checks', 'historical_baseline_trial', 'freeze_script')
    if 'dispatcher' in trial:
        bound_keys += ('dispatcher',)
    if 'profile_disagreements' in trial:
        bound_keys += ('profile_disagreements',)
    if trial.get('historical_baseline_root', str(baseline_root)) != str(baseline_root):
        raise ValueError('Historical baseline root differs from frozen trial')

    def validate():
        assert sha(trial_path) == trial_hash
        for key in bound_keys:
            item = trial[key]
            assert sha(Path(item['path'])) == item['sha256'], key
        assert shared_manifest(REPO) == trial['shared_executor_files']
        assert subprocess.check_output(['codex', '--version'], text=True).strip() == trial['codex_cli_version']

    validate()
    with (root / 'experimental_collection_started.json').open('x') as f:
        json.dump({'pid': os.getpid(), 'started': time.time(), 'trial_sha256': trial_hash,
                   'runner_sha256': sha(Path(__file__))}, f)
    path = root / 'experimental_collection_status.json'
    state = {'pid': os.getpid(), 'started': time.time(), 'status': 'waiting_for_baseline',
             'trial_sha256': trial_hash, 'runs': []}
    write(path, state)
    source = json.loads(Path(trial['source_snapshot']['path']).read_text())['source_root']
    try:
        while True:
            if (root / 'STOP_REQUESTED.json').exists() or (root / 'EXPERIMENTAL_STOP_REQUESTED.json').exists():
                state['status'] = 'stopped_before_dispatch'; return
            baseline = json.loads((baseline_root / 'baseline_collection_status.json').read_text())
            live = process(baseline.get('pid'), 'run_baseline_collection')
            decision = prerequisite(baseline, live['live'])
            state.update(observed_at=time.time(), baseline_process=live,
                         baseline_recorded_runs=len(baseline.get('runs', [])))
            write(path, state)
            if decision == 'ready':
                break
            time.sleep(10)
        seals = verify_baseline_seals(baseline_root)
        assert len(seals['verified_runs']) == 18
        write(root / 'experimental_baseline_completion_seals.json', seals)
        validate()
        state['status'] = 'running'; write(path, state)
        for run in trial['runs']:
            if (root / 'STOP_REQUESTED.json').exists() or (root / 'EXPERIMENTAL_STOP_REQUESTED.json').exists():
                state['status'] = 'stopped'; break
            validate()
            work = Path(run['work']); arm = run['variant']; disc = run['discipline']
            verify_shared_protocol(REPO, source, work, arm)
            verify_trial_binding(work, arm, disc)
            status_path = work / 'classic_control/status' / (disc + '.json')
            assert not status_path.exists(), 'Existing run requires explicit recovery'
            command = [sys.executable, '-m', 'experiments.auditmanager_stage4_20261110.paired_runner',
                       'run', '--source-root', source, '--work', str(work), '--variant', arm, '--discipline', disc]
            record = {**run, 'status': 'running', 'started': time.time()}
            state['runs'].append(record); write(path, state)
            with (work / 'process.log').open('x') as log:
                child = subprocess.Popen(command, cwd=REPO, stdout=log, stderr=subprocess.STDOUT)
                record['pid'] = child.pid; write(path, state)
                code = child.wait()
            record.update(exit_code=code, finished=time.time(), status='terminal')
            if status_path.exists():
                record['execution_status'] = json.loads(status_path.read_text()).get('status')
                record['status_sha256'] = sha(status_path)
            write(path, state)
            if code != 0 or record.get('execution_status') != 'completed_unadjudicated':
                state['status'] = 'failed_requires_inspection'; break
        else:
            state['status'] = 'completed_collection_unadjudicated'
    except Exception as exc:
        state.update(status='failed_requires_inspection', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        state['finished'] = time.time(); write(path, state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--baseline-root', type=Path)
    args = parser.parse_args()
    main(args.root, args.baseline_root)
