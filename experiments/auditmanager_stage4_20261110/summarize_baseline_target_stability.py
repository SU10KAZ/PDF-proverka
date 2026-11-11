"""Evaluator-only stability of frozen target coverage; no trial acceptance."""
import json
from collections import Counter
from pathlib import Path
from .score_atomic_review import sha
from .verify_output_review_history import encode

HERE = Path(__file__).resolve().parent


def build():
    summary_path = HERE / 'baseline_three_repeat_coverage_summary.json'
    summary = json.loads(summary_path.read_text())
    evaluator_path = HERE / 'evaluator_release_errata_v2.json'
    assert sha(evaluator_path) == summary['evaluator_sha256']
    evaluator = json.loads(evaluator_path.read_text())
    targets = {t['target_id']: t for t in evaluator['targets']}
    held = {tid for hold in evaluator['holds'] if 'truth_scoring' in hold['held_dimensions'] for tid in hold['target_ids']}
    observed = {}
    for repeat in summary['repeats']:
        for project in repeat['projects']:
            source = project['source']
            assert sha(source['ledger_path']) == source['ledger_sha256']
            assert sha(source['final_path']) == source['final_sha256']
            ledger = json.loads(Path(source['ledger_path']).read_text())
            for row in ledger['targets']:
                key = row['target_id']
                record = observed.setdefault(key, {'target_id': key, 'discipline': project['discipline'], 'route': targets[key]['route'], 'held': key in held, 'claim': targets[key].get('source_atom', {}).get('claim'), 'contexts': targets[key]['contexts'], 'observations': []})
                assert record['discipline'] == project['discipline']
                record['observations'].append({'repeat': repeat['repeat'], 'coverage': row['coverage'], 'finding_ids': row['finding_ids'], 'reason': row['reason'], 'ledger_path': source['ledger_path'], 'ledger_sha256': source['ledger_sha256']})
    assert set(observed) == set(targets)
    totals = {}; per_discipline = {}
    for record in observed.values():
        obs = sorted(record['observations'], key=lambda x: x['repeat'])
        assert [x['repeat'] for x in obs] == [1, 2, 3]
        record['observations'] = obs
        states = [x['coverage'] for x in obs]
        if record['held']:
            category = 'held'
        elif len(set(states)) == 1:
            category = 'always_' + states[0]
        else:
            category = 'variable'
        record['stability'] = category
        totals.setdefault(record['route'], Counter())[category] += 1
        per_discipline.setdefault(record['discipline'], {}).setdefault(record['route'], Counter())[category] += 1
    return {'status': 'baseline_target_stability_not_comparative_retention', 'source_summary': str(summary_path), 'source_summary_sha256': sha(summary_path), 'evaluator_sha256': sha(evaluator_path), 'counts_by_route': totals, 'counts_by_discipline': per_discipline, 'targets': list(observed.values()), 'production_acceptance': False, 'limits': ['Three repeats of the same six known projects are not independent project validation.', 'Full coverage does not certify unsupported additions, output section or absence of duplicates.', 'Always-absent technical targets are regression controls, not proof of a particular root cause.', 'Always-absent negative targets do not establish successful filtering.', 'Candidate-versus-baseline useful retention remains unmeasured.', 'Evaluator-only artifact; do not expose answers to independent detection before outputs are sealed.']}


if __name__ == '__main__':
    result = build()
    path = HERE / 'baseline_target_stability.json'
    with path.open('xb') as stream:
        stream.write(encode(result))
    print(json.dumps(result['counts_by_route'], ensure_ascii=False))
    for row in result['targets']:
        if row['route'] == 'technical' and row['stability'] == 'always_absent':
            print(row['discipline'], row['target_id'], row['claim'])
