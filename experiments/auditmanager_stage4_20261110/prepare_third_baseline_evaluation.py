"""Prepare evaluator-only ledgers for sealed third repeats; never expose answers to runners."""
import json
from pathlib import Path
from .score_atomic_review import prepare, sha

HERE = Path(__file__).resolve().parent
TRIAL = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
DEST = Path('/tmp/auditmanager_comparison_evaluation_20261111/baseline_r3')


def build():
    evaluator = HERE / 'evaluator_release_errata_v2.json'
    DEST.mkdir(parents=True, exist_ok=True)
    entries, pending = [], []
    for disc in ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'):
        base = TRIAL / f'{disc.lower()}_r3_classic/classic_control'
        seal_path = base / f'status/{disc}.json'
        if not seal_path.exists():
            pending.append({'discipline': disc, 'reason': 'no_completion_seal'})
            continue
        seal = json.loads(seal_path.read_text())
        if seal['status'] != 'completed_unadjudicated':
            pending.append({'discipline': disc, 'reason': 'not_completed', 'status': seal['status']})
            continue
        manifest = json.loads((base / 'input_manifest.json').read_text())[0]
        final = Path(manifest['output_dir']) / '03_findings.json'
        assert sha(final) == seal['final_sha256']
        report = json.loads(final.read_text())
        ledger_path = DEST / f'{disc}_coverage_ledger.json'
        if ledger_path.exists():
            ledger = json.loads(ledger_path.read_text())
            assert ledger['findings_sha256'] == sha(final)
            assert ledger['evaluator_sha256'] == sha(evaluator)
        else:
            ledger = prepare(evaluator, final, disc)
            ledger_path.write_text(json.dumps(ledger, ensure_ascii=False, indent=2) + '\n')
        entries.append({'discipline': disc, 'repeat': 3,
                        'seal_path': str(seal_path), 'seal_sha256': sha(seal_path),
                        'final_path': str(final), 'final_sha256': sha(final),
                        'findings': len(report['findings']),
                        'ledger_path': str(ledger_path), 'ledger_sha256': sha(ledger_path),
                        'targets': len(ledger['targets']),
                        'unchecked_targets': sum(t['coverage'] == 'unchecked' for t in ledger['targets'])})
    return {'status': 'third_repeat_evaluation_prepared_not_adjudicated',
            'evaluator_sha256': sha(evaluator), 'entries': entries, 'pending': pending,
            'model_calls': 0, 'production_acceptance': False,
            'limits': ['Only sealed outputs opened. Existing ledgers preserved.',
                       'No history or evaluator data written into model run directories.',
                       'Prepared targets are unchecked, not scored misses.']}


if __name__ == '__main__':
    result = build()
    (HERE / 'baseline_r3_evaluation_preparation.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'prepared': [(e['discipline'], e['findings'], e['targets']) for e in result['entries']], 'pending': result['pending']}, ensure_ascii=False))
