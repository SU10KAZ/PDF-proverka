"""Record failed/interrupted frozen slots and sunk usage; never dispatch requests."""
import json
from collections import Counter
from pathlib import Path
from .observe_baseline_collection import process
from .prepare_current_method_evaluation import execution_classification
from .run_experimental_collection_repeated_interruption_continue import verify_prior_interruption
from .score_atomic_review import sha

ROOT = Path('/tmp/auditmanager_current_method_trial_context_20261112')


def build(root=ROOT):
    trial_path = root / 'experimental_collection_trial.json'
    state_path = root / 'experimental_collection_status.json'
    trial = json.loads(trial_path.read_text())
    state = json.loads(state_path.read_text())
    if sha(trial_path) != state['trial_sha256']:
        raise ValueError('Frozen trial changed')
    planned = {r['work']: r for r in trial['runs']}
    rows = []
    for record in state['runs']:
        if record.get('execution_status') not in ('failed', 'interrupted_without_terminal_receipt'):
            continue
        run = planned[record['work']]
        if any(run[k] != record[k] for k in ('discipline', 'variant', 'repeat', 'input_manifest_sha256')):
            raise ValueError('Run identity changed')
        live = process(record.get('pid'), 'paired_runner')
        base = Path(record['work']) / 'classic_control'
        status_path = base / 'status' / (record['discipline'] + '.json')
        status = json.loads(status_path.read_text())
        if sha(status_path) != record['status_sha256']:
            raise ValueError('Failed/interrupted state changed')
        classification = execution_classification(record, status, live['live'])
        if classification == 'interrupted_requires_receipt_verification':
            verify_prior_interruption(record, status_path)
        elif classification != 'terminal_failed':
            raise ValueError('No verified failed or interrupted attempt')
        request_root = base / 'requests' / record['discipline']
        receipts, totals, models = [], Counter(), set()
        attempts = errors = 0
        missing = []
        for folder in sorted(request_root.iterdir()):
            if not folder.is_dir():
                continue
            p = folder / 'attempts.json'
            if not p.exists():
                missing.append(folder.name)
                continue
            receipts.append({'path': str(p), 'sha256': sha(p)})
            for receipt in json.loads(p.read_text()):
                attempts += 1
                errors += receipt.get('is_error') is True
                models.add(receipt.get('reported_model') or receipt.get('model'))
                for key, value in receipt.get('usage', {}).items():
                    if type(value) in (int, float) and value >= 0:
                        totals[key] += value
        rows.append({
            **{k: run[k] for k in ('discipline', 'variant', 'repeat', 'work')},
            'classification': classification, 'process_checked': live,
            'status_source': {'path': str(status_path), 'sha256': sha(status_path)},
            'interruption_receipt': record.get('interruption_receipt'),
            'original_error': status.get('error'),
            'recorded_attempts': attempts, 'recorded_failed_attempts': errors,
            'reported_models': sorted(models), 'recorded_usage_lower_bounds': dict(totals),
            'request_directories_without_attempt_receipt': missing,
            'attempt_receipt_sources': receipts,
            'monetary_cost_usd': None, 'quality_score': None,
            'count_as_empty_successful_output': False,
            'recovery_status': 'deferred_until_current_frozen_collection_terminal',
            'recovery_attempt_work': str(root.parent / (root.name + '_recovery') / (Path(run['work']).name + '_attempt2')),
        })
    return {
        'version': 1, 'trial': {'path': str(trial_path), 'sha256': sha(trial_path)},
        'original_slots_requiring_recovery': len(rows), 'rows': rows,
        'model_calls': 0, 'dispatch_performed': False,
        'required_before_recovery': [
            'Recheck collection and all audit process handles; do not launch recovery while this frozen series runs.',
            'Retain original states, receipts, failures and costs; recovery is a linked additional attempt, not an overwrite.',
            'Materialize a fresh workspace from the same frozen code, input sources, prompts, model and protocol.',
            'Keep evaluator answers, reviewed findings and history out of independent audit inputs.',
            'If a setting or method must change, label the attempt a revised configuration rather than a matched frozen repeat.',
            'Inspect each failure before retry; record timeout and interruption separately from semantic misses.',
            'After completion, verify seals and evaluate output; add original and recovery costs to reliability accounting.',
        ],
        'limits': [
            'Missing attempt receipts leave unknown usage; recorded counters are lower bounds, not total quota consumption.',
            'Subscription zero cost is not a measured monetary cost.',
            'This register prepares recovery; it neither launches it nor claims an automatic recovery service exists.',
        ],
        'production_acceptance': False,
    }


if __name__ == '__main__':
    result = build()
    path = Path(__file__).with_name('current_method_recovery_register.json')
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'recovery_slots': len(result['rows']), 'model_calls': 0}))
