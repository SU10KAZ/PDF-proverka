"""Explicit inspected continuation: retain failures, run only untouched frozen slots."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from .run_experimental_collection import write
from .observe_baseline_collection import process
from .materialize_trial_inputs import sha
from .paired_protocol import shared_manifest, verify_shared_protocol, verify_trial_binding

REPO = Path(__file__).resolve().parents[2]


def pending_runs(trial, state):
    if state.get('status') != 'failed_requires_inspection':
        raise ValueError('Only inspected terminal failure can continue')
    if process(state.get('pid'), 'run_experimental_collection')['live']:
        raise ValueError('Prior dispatcher still live')
    runs = state.get('runs', [])
    if len(runs) >= len(trial['runs']):
        raise ValueError('No untouched slots')
    for expected, actual in zip(trial['runs'], runs):
        for key in ('work', 'discipline', 'variant', 'repeat'):
            if actual.get(key) != expected.get(key):
                raise ValueError('Recorded sequence differs from frozen trial')
        if actual.get('status') != 'terminal' or type(actual.get('exit_code')) is not int:
            raise ValueError('Prior run lacks terminal receipt')
        if process(actual.get('pid'), 'paired_runner')['live']:
            raise ValueError('Prior audit still live')
    return trial['runs'][len(runs):]


def main(root):
    root = Path(root).resolve()
    tp = root/'experimental_collection_trial.json'
    sp = root/'experimental_collection_status.json'
    trial = json.loads(tp.read_text()); state = json.loads(sp.read_text())
    todo = pending_runs(trial, state)
    trial_hash = sha(tp)
    assert state['trial_sha256'] == trial_hash
    source = json.loads(Path(trial['source_snapshot']['path']).read_text())['source_root']
    bound = ('evaluator', 'acceptance_protocol', 'source_snapshot', 'experimental_decisions',
             'preflight_checks', 'source_input_checks', 'historical_baseline_trial', 'freeze_script',
             'dispatcher', 'profile_disagreements')

    def validate():
        assert sha(tp) == trial_hash
        for key in bound:
            if key in trial:
                item = trial[key]; assert sha(Path(item['path'])) == item['sha256'], key
        assert shared_manifest(REPO) == trial['shared_executor_files']
        assert subprocess.check_output(['codex', '--version'], text=True).strip() == trial['codex_cli_version']

    validate()
    for record in state['runs']:
        p = Path(record['work'])/'classic_control/status'/(record['discipline']+'.json')
        assert sha(p) == record['status_sha256']
        assert json.loads(p.read_text())['status'] == record['execution_status']
        verify_shared_protocol(REPO, source, Path(record['work']), record['variant'])
        verify_trial_binding(Path(record['work']), record['variant'], record['discipline'])
    for run in todo:
        assert not (Path(run['work'])/'classic_control/status'/(run['discipline']+'.json')).exists()
        assert not (Path(run['work'])/'process.log').exists()
    # Exclusive marker prevents two continuations from dispatching same slots.
    marker = root/'inspected_continuation_started.json'
    with marker.open('x') as f:
        json.dump({'pid': os.getpid(), 'started': time.time(), 'trial_sha256': trial_hash,
                   'prior_state_sha256': sha(sp), 'script_sha256': sha(Path(__file__)),
                   'policy': 'Retain failed runs; no retry or replacement; untouched slots only'}, f)
    with (root/'experimental_collection_pre_continuation.json').open('x') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    state['continuation'] = json.loads(marker.read_text())
    state.update(pid=os.getpid(), status='running')
    state.pop('finished', None); write(sp, state)
    try:
        for run in todo:
            if any((root/n).exists() for n in ('STOP_REQUESTED.json', 'EXPERIMENTAL_STOP_REQUESTED.json')):
                state['status'] = 'stopped'; break
            validate()
            work = Path(run['work']); arm = run['variant']; disc = run['discipline']
            verify_shared_protocol(REPO, source, work, arm); verify_trial_binding(work, arm, disc)
            status_path = work/'classic_control/status'/(disc+'.json')
            assert not status_path.exists()
            command = [sys.executable, '-m', 'experiments.auditmanager_stage4_20261110.paired_runner',
                       'run', '--source-root', source, '--work', str(work), '--variant', arm, '--discipline', disc]
            record = {**run, 'status': 'running', 'started': time.time()}
            state['runs'].append(record); write(sp, state)
            with (work/'process.log').open('x') as log:
                child = subprocess.Popen(command, cwd=REPO, stdout=log, stderr=subprocess.STDOUT)
                record['pid'] = child.pid; write(sp, state); code = child.wait()
            record.update(exit_code=code, finished=time.time(), status='terminal')
            if status_path.exists():
                record['execution_status'] = json.loads(status_path.read_text()).get('status')
                record['status_sha256'] = sha(status_path)
            write(sp, state)
            if code != 0 or record.get('execution_status') != 'completed_unadjudicated':
                state['status'] = 'failed_requires_inspection'; break
        else:
            state['status'] = 'completed_collection_with_failures'
    except Exception as exc:
        state.update(status='failed_requires_inspection', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        state['finished'] = time.time(); write(sp, state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    main(parser.parse_args().root)
