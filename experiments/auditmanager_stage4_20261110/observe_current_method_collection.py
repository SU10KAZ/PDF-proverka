"""Read execution metadata only; never infer quality from completed requests."""
import argparse
from collections import Counter
import json
import hashlib
from pathlib import Path
import time
from .observe_baseline_collection import process, read

ROOT = Path('/tmp/auditmanager_current_method_trial_context_20261112')



def recovery_progress(base):
    """Read plan identities and file presence only, never candidate propositions."""
    manifest = read(base/'input_manifest.json')
    if not isinstance(manifest, list) or len(manifest) != 1:
        return {'status': 'input_manifest_unavailable'}
    output = Path(manifest[0]['output_dir'])
    plan_path = output/'03_candidate_recovery_plan.json'
    if not plan_path.exists():
        return {'status': 'not_planned'}
    plan = read(plan_path)
    requests = plan.get('requests', [])
    if plan.get('count') != len(requests):
        return {'status': 'inconsistent_plan'}
    expected = []
    for packet in requests:
        candidate = packet.get('candidate', {})
        if not all(isinstance(candidate.get(k), str) for k in ('origin', 'pointer')):
            return {'status': 'unrecognized_plan_identity'}
        key = hashlib.sha256((candidate['origin'] + candidate['pointer']).encode()).hexdigest()[:16]
        expected.append(output/f'03_candidate_recovery_{key}.json')
    if len(set(expected)) != len(expected):
        return {'status': 'ambiguous_plan_identity'}
    present = sum(p.is_file() for p in expected)
    return {'status': 'plan_observed', 'planned_requests': len(expected),
            'output_files_present': present, 'output_files_pending': len(expected)-present,
            'semantic_results_read': False,
            'limits': 'File presence for the current saved plan, not acceptance or full-audit completion.'}


def observe(root=ROOT):
    trial = read(root/'experimental_collection_trial.json')
    state = read(root/'experimental_collection_status.json')
    rows = []
    for run in state.get('runs', []):
        base = Path(run['work'])/'classic_control'
        status = read(base/'status'/(run['discipline']+'.json'))
        folders = list((base/'requests'/run['discipline']).glob('*'))
        attempts = []
        for folder in folders:
            receipt = read(folder/'attempts.json')
            if isinstance(receipt, list):
                attempts.extend(a for a in receipt if isinstance(a, dict))
        usage = {}
        for key in ('input_tokens', 'output_tokens', 'cached_tokens', 'duration_ms'):
            values = [a['usage'].get(key) for a in attempts if isinstance(a.get('usage'), dict)]
            valid = [v for v in values if type(v) in (int, float) and v >= 0]
            usage[key] = {'recorded_sum': sum(valid), 'missing_attempt_values': len(attempts)-len(valid)}
        complete = run.get('exit_code') == 0 and status.get('status') == 'completed_unadjudicated'
        rows.append({k:run[k] for k in ('discipline', 'variant', 'repeat')} | {
            'execution_complete': complete, 'execution_status': status.get('status', run.get('status')),
            'process': process(run.get('pid'), 'paired_runner'),
            'block_progress': status.get('block_progress'), 'stages': status.get('stages', {}),
            'candidate_recovery_progress': recovery_progress(base),
            'request_records_created': len(folders), 'attempt_receipts': len(attempts),
            'failed_attempt_receipts': sum(a.get('is_error') is True for a in attempts),
            'reported_models': sorted({str(a.get('reported_model') or a.get('model') or 'unknown') for a in attempts}),
            'usage_lower_bounds': usage,
            'elapsed_seconds': round((run.get('finished') or time.time())-run['started'], 1),
        })
    counts = Counter((r['discipline'], r['variant']) for r in rows if r['execution_complete'])
    return {'observed_at_epoch': time.time(), 'root': str(root),
        'collection_status': state.get('status', 'not_started'),
        'collection_process': process(state.get('pid'), 'run_experimental_collection'),
        'planned_runs': len(trial.get('runs', [])), 'started_runs': len(rows),
        'completed_runs': sum(r['execution_complete'] for r in rows),
        'model': trial.get('model'), 'reasoning_effort': trial.get('reasoning_effort'),
        'completed_by_project_arm': [{'discipline': d, 'variant': a, 'count': n} for (d,a),n in sorted(counts.items())],
        'runs': rows, 'semantic_results_read': False, 'production_acceptance': False,
        'limits': 'Completion means execution only. Missing receipts and monetary subscription cost remain unknown. Live process identity is checked separately from stored status.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    args = parser.parse_args()
    print(json.dumps(observe(args.root), ensure_ascii=False, indent=2))
