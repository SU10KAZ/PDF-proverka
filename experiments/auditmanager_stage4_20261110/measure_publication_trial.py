"""Measure saved trial responses; subscription quota is not inferred from USD=0."""
import argparse
from collections import Counter
import json
from pathlib import Path
import statistics
import time

from .run_primary_text_runtime_probe import sha, write


FIELDS = ('input_tokens', 'output_tokens', 'cached_tokens', 'reasoning_tokens', 'duration_ms')


def measure(roots):
    seen, records, jobs = set(), [], []
    for root in roots:
        path = root / 'status.json'
        state = json.loads(path.read_text())
        jobs.append({'run': str(root), 'status': state['status'], 'status_sha256': sha(path),
                     'planned_packets': state['planned_ready'], 'completed_packets': state['completed_packets']})
        for attempt in state['attempts']:
            response_path = Path(attempt['response_path'])
            if response_path in seen:
                raise ValueError('Response counted twice')
            seen.add(response_path)
            if sha(response_path) != attempt['response_sha256']:
                raise ValueError('Response changed')
            response = json.loads(response_path.read_text())
            records.append({'response_path': str(response_path), 'response_sha256': attempt['response_sha256'],
                            'stage': attempt['stage'], 'attempt': attempt['attempt'],
                            'success': attempt['success'], 'model': response.get('model'),
                            **{k: response.get(k) for k in FIELDS},
                            'reported_cost_usd': response.get('cost_usd'),
                            'cost_source': response.get('cost_source')})
    usage = {}
    for field in FIELDS:
        values = [r[field] for r in records if isinstance(r[field], (int, float)) and r[field] >= 0]
        usage[field] = {'reported_total': sum(values), 'records_with_value': len(values),
                        'complete_for_saved_attempts': len(values) == len(records)}
    durations = [r['duration_ms'] for r in records if isinstance(r['duration_ms'], (int, float))]
    return {'status': 'resource_snapshot_not_comparative_acceptance', 'observed_at_unix': time.time(),
            'jobs': jobs, 'saved_outer_attempts': len(records),
            'successful_saved_attempts': sum(r['success'] for r in records),
            'model_counts': dict(Counter(r['model'] for r in records)), 'usage': usage,
            'median_saved_attempt_seconds': statistics.median(durations) / 1000 if durations else None,
            'elapsed_model_seconds_sum': sum(durations) / 1000,
            'monetary_cost_estimate': None, 'subscription_quota_fraction': None,
            'records': records, 'production_acceptance': False,
            'limits': ['Cached tokens are reported separately, not added again to input tokens.',
                       'Reported USD=0 is not proof of zero cost or zero quota use.',
                       'Unfinished requests and unreported internal retries are not measured by this snapshot.',
                       'Per-finding research reconciliation is not yet the production topology; full-audit cost/time gates remain open.',
                       'No improvement in quality or engineer workload follows from successful transport.']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('roots', nargs='+', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    result = measure([r.resolve() for r in args.roots])
    write(args.output.resolve(), result)
    print(json.dumps({k: result[k] for k in ('saved_outer_attempts', 'model_counts', 'usage', 'median_saved_attempt_seconds')}, ensure_ascii=False))
