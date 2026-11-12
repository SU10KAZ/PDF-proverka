"""Continue untouched frozen slots after repeated, independently recorded interruptions."""
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
    if state.get('status') != 'running':
        raise ValueError('Expected stale running dispatcher state')
    if process(state.get('pid'), 'run_experimental_collection')['live']:
        raise ValueError('Prior dispatcher still live')
    runs = state.get('runs', [])
    if not runs or len(runs) >= len(trial['runs']):
        raise ValueError('Missing interrupted slot or no untouched slots')
    for index, (expected, actual) in enumerate(zip(trial['runs'], runs)):
        for key in ('work', 'discipline', 'variant', 'repeat'):
            if actual.get(key) != expected.get(key):
                raise ValueError('Recorded sequence differs from frozen trial')
        if process(actual.get('pid'), 'paired_runner')['live']:
            raise ValueError('Prior audit still live')
        if index == len(runs) - 1:
            if actual.get('status') != 'running' or 'exit_code' in actual:
                raise ValueError('Last slot must lack terminal receipt')
        elif actual.get('status') == 'interrupted':
            if actual.get('exit_code') is not None or actual.get('execution_status') != 'interrupted_without_terminal_receipt' or not actual.get('interruption_receipt'):
                raise ValueError('Earlier interruption lacks explicit receipt')
        elif actual.get('status') != 'terminal' or type(actual.get('exit_code')) is not int:
            raise ValueError('Earlier slot lacks terminal receipt')
    return trial['runs'][len(runs):]


def verify_incident(incident, root, state):
    if incident.get('classification') != 'interrupted_without_terminal_receipt':
        raise ValueError('Wrong incident classification')
    record = state['runs'][-1]
    expected = {
        'collection_state': root/'experimental_collection_status.json',
        'run_state': Path(record['work'])/'classic_control/status'/(record['discipline']+'.json'),
        'trial': root/'experimental_collection_trial.json',
    }
    for key, path in expected.items():
        item = incident[key]
        if Path(item['path']).resolve() != path.resolve() or sha(path) != item['sha256']:
            raise ValueError('Incident binding changed: ' + key)
    if incident['collection_process']['pid'] != state['pid'] or incident['run_process']['pid'] != record['pid']:
        raise ValueError('Incident process identity differs')
    request = incident['last_request']
    path = Path(request['path']).resolve()
    request_root = (Path(record['work'])/'classic_control/requests'/record['discipline']).resolve()
    if not path.is_relative_to(request_root) or sha(path) != request['sha256']:
        raise ValueError('Last request binding changed')


def verify_prior_interruption(record, status_path):
    receipt = record['interruption_receipt']
    path = Path(receipt['path'])
    if sha(path) != receipt['sha256']:
        raise ValueError('Prior interruption receipt changed')
    incident = json.loads(path.read_text())
    if incident['classification'] != 'interrupted_without_terminal_receipt':
        raise ValueError('Wrong prior interruption classification')
    if Path(incident['run_state']['path']).resolve() != status_path.resolve() or sha(status_path) != incident['run_state']['sha256']:
        raise ValueError('Prior interrupted run binding changed')
    request = incident['last_request']
    request_path = Path(request['path']).resolve()
    request_root = status_path.parent.parent/'requests'/record['discipline']
    if not request_path.is_relative_to(request_root.resolve()) or sha(request_path) != request['sha256']:
        raise ValueError('Prior interrupted request changed')


def main(root, incident_path, execute=False):
    root = Path(root).resolve()
    tp = root/'experimental_collection_trial.json'
    sp = root/'experimental_collection_status.json'
    trial = json.loads(tp.read_text()); state = json.loads(sp.read_text())
    todo = pending_runs(trial, state)
    incident_path = Path(incident_path).resolve()
    incident = json.loads(incident_path.read_text())
    verify_incident(incident, root, state)
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
        if record is state['runs'][-1]:
            assert sha(p) == incident['run_state']['sha256']
        else:
            assert sha(p) == record['status_sha256']
            if record['status'] == 'interrupted':
                verify_prior_interruption(record, p)
            else:
                assert json.loads(p.read_text())['status'] == record['execution_status']
        verify_shared_protocol(REPO, source, Path(record['work']), record['variant'])
        verify_trial_binding(Path(record['work']), record['variant'], record['discipline'])
    for run in todo:
        assert not (Path(run['work'])/'classic_control/status'/(run['discipline']+'.json')).exists()
        assert not (Path(run['work'])/'process.log').exists()
    if not execute:
        print(json.dumps({'status': 'preflight_passed', 'untouched_slots': len(todo),
                          'trial_sha256': trial_hash, 'model_calls': 0}))
        return
    # Exclusive marker prevents two continuations from dispatching same slots.
    state_hash = sha(sp)
    marker = root/('inspected_interruption_continuation_' + state_hash + '.json')
    with marker.open('x') as f:
        json.dump({'pid': os.getpid(), 'started': time.time(), 'trial_sha256': trial_hash,
                   'prior_state_sha256': sha(sp), 'script_sha256': sha(Path(__file__)),
                   'incident': {'path': str(incident_path), 'sha256': sha(incident_path)},
                   'policy': 'Retain failed and interrupted runs; no retry or replacement; untouched slots only'}, f)
    with (root/('experimental_collection_pre_interruption_' + state_hash + '.json')).open('x') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    state.setdefault('interruption_continuation_history', []).append(json.loads(marker.read_text()))
    interrupted = state['runs'][-1]
    interrupted.update(status='interrupted', exit_code=None,
                       execution_status='interrupted_without_terminal_receipt',
                       status_sha256=incident['run_state']['sha256'],
                       interruption_receipt={'path': str(incident_path), 'sha256': sha(incident_path)})
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
    parser.add_argument('--incident', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    main(args.root, args.incident, args.execute)
