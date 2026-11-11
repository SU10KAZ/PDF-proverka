"""Compare reviewed baseline repeats; never infer candidate improvement."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
from experiments.auditmanager_stage4_20261110.score_atomic_review import score

HERE = Path(__file__).resolve().parent
TRIAL = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')


def bind(path):
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--include-eom', action='store_true')
    args = parser.parse_args()
    disciplines = ('AK', 'ITP', 'AR', 'KM', 'VK') + (('EOM',) if args.include_eom else ())
    ep = HERE / 'evaluator_release_errata_v2.json'
    ev = json.loads(ep.read_text())
    targets = {t['target_id']: t for t in ev['targets']}
    held = {tid for hold in ev['holds'] if 'truth_scoring' in hold['held_dimensions'] for tid in hold['target_ids']}
    result = {'status': 'partial_baseline_repeat_stability_not_candidate_acceptance',
              'evaluator': bind(ep), 'section_input_normalizations': [], 'disciplines': [], 'pending_discipline': None if args.include_eom else 'EOM',
              'limits': ['Only sealed and reviewed repeat pairs are included; no automatic quality acceptance.',
                         'Targets are claims, not independent defects. Questions and technical claims stay separate.',
                         'Negative-target absence earns no filter credit. Partial false claims stay separate from exact repeats.',
                         'Both runs are the same baseline configuration, not baseline versus candidate.',
                         'Coverage judgments do not complete assessment of novel claims, duplicates, section placement or engineer time.']}
    totals = {r: Counter() for r in ('technical', 'question', 'negative')}
    for disc in disciplines:
        reviews, scores, sources = [], [], []
        for repeat in (1, 2):
            suffix = '_completed_coverage_ledger.json' if repeat == 1 or disc in ('AR', 'KM', 'VK', 'EOM') else '_coverage_ledger.json'
            lp = HERE / f'{disc}_r{repeat}{suffix}'
            review = json.loads(lp.read_text())
            base = TRIAL / f'{disc.lower()}_r{repeat}_classic/classic_control'
            fp = base / f'inputs/{disc}/versions/v001/03_analysis/runs/comparison/03_findings.json'
            seal = json.loads((base / f'status/{disc}.json').read_text())
            assert seal['status'] == 'completed_unadjudicated' and seal['final_sha256'] == bind(fp)['sha256']
            for row in review['targets']:
                if row['section_assessment'] == 'held_disputed_output_section_only':
                    assert any(row['target_id'] in hold['target_ids'] and 'report_section_accuracy' in hold['held_dimensions'] for hold in ev['holds'])
                    result['section_input_normalizations'].append({'discipline': disc, 'repeat': repeat, 'target_id': row['target_id'], 'original': row['section_assessment'], 'scorer_input': 'unchecked', 'reason': 'Historical held-section marker; frozen evaluator still excludes section accuracy. Original ledger unchanged.'})
                    row['section_assessment'] = 'unchecked'
            scored = score(ep, fp, review)
            assert all(v['review_complete'] for v in scored['routes'].values())
            reviews.append({r['target_id']: r for r in review['targets']})
            scores.append(scored['routes'])
            sources.append({'repeat': repeat, 'review': bind(lp), 'output': bind(fp)})
        transitions = {r: Counter() for r in totals}
        changes, persistent_misses = [], []
        assert reviews[0].keys() == reviews[1].keys()
        for tid, before in reviews[0].items():
            if tid in held:
                continue
            after = reviews[1][tid]
            route = targets[tid]['route']
            transition = before['coverage'] + ' -> ' + after['coverage']
            transitions[route][transition] += 1
            totals[route][transition] += 1
            row = {'target_id': tid, 'route': route, 'source_atom': targets[tid].get('source_atom'), 'contexts': targets[tid]['contexts'],
                   'before': {k: before[k] for k in ('coverage', 'finding_ids', 'reason')},
                   'after': {k: after[k] for k in ('coverage', 'finding_ids', 'reason')}}
            if before['coverage'] != after['coverage']:
                changes.append(row)
            if route == 'technical' and transition == 'absent -> absent':
                persistent_misses.append(row)
        result['disciplines'].append({'discipline': disc, 'sources': sources,
                                      'scores': scores, 'transitions': {k: dict(v) for k, v in transitions.items()},
                                      'changed_targets': changes, 'persistent_technical_misses': persistent_misses})
    result['aggregate_transitions'] = {k: dict(v) for k, v in totals.items()}
    result['reviewed_target_count_per_repeat_including_holds'] = sum(len(json.loads(Path(d['sources'][0]['review']['path']).read_text())['targets']) for d in result['disciplines'])
    out = HERE / ('baseline_r1_r2_six_discipline_stability.json' if args.include_eom else 'baseline_r1_r2_five_discipline_stability.json')
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result['aggregate_transitions'], ensure_ascii=False))
    print('reviewed targets:', result['reviewed_target_count_per_repeat_including_holds'])


if __name__ == '__main__':
    main()
