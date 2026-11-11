"""Wait for the existing probe, seal its response and prepare recovery; no inference."""
import argparse
import json
from pathlib import Path
import time

from .collect_primary_text_runtime_probe import collect
from .prepare_runtime_probe_recovery import prepare
from .observe_baseline_collection import process
from .run_primary_text_runtime_probe import write


def finish(root, review, recovery):
    marker = root / 'local_followup_started.json'
    with marker.open('x') as stream:
        json.dump({'started': time.time(), 'model_calls': 0}, stream)
    state = {'status': 'waiting_for_existing_probe', 'model_calls': 0}
    path = root / 'local_followup_status.json'
    write(path, state)
    try:
        while True:
            probe = json.loads((root / 'status.json').read_text())
            observation = process(probe.get('pid'), 'run_primary_text_runtime_probe')
            state.update(probe_status=probe['status'], process_observation=observation)
            write(path, state)
            if observation['live'] is False:
                if probe['status'] != 'completed_unadjudicated':
                    raise RuntimeError('Probe ended without a successful sealed result')
                break
            time.sleep(10)
        state['structural_review'] = collect(root, review)
        state['recovery_preparation'] = prepare(root, recovery)
        state.update(status='local_review_and_recovery_prepared', review=str(review), recovery=str(recovery))
    except Exception as exc:
        state.update(status='failed_requires_inspection', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        state['finished'] = time.time()
        write(path, state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('review', type=Path)
    parser.add_argument('recovery', type=Path)
    args = parser.parse_args()
    finish(args.root, args.review, args.recovery)
