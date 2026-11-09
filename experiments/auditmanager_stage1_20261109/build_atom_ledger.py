"""Bind scoring decisions to exact atoms; never infer labels from verdict text."""
import hashlib
import json
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def atom_id(filename, atom):
    canonical = json.dumps(atom, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return filename.removesuffix('.json') + ':' + hashlib.sha256(canonical.encode()).hexdigest()[:20]


def main():
    review_path = HERE / 'atom_scoring_reviews.json'
    reviews = json.loads(review_path.read_text())['reviews']
    records = json.loads((HERE / 'consolidated/records.json').read_text())
    disciplines = {r['candidate_id']: r['discipline'] for r in records}
    items = []
    seen = set()
    for path in sorted((HERE / 'decisions').glob('*.json')):
        decision = json.loads(path.read_text())
        for index, atom in enumerate(decision['atomic_dispositions']):
            key = atom_id(path.name, atom)
            assert key not in seen, key
            seen.add(key)
            review = reviews.get(key)
            if review:
                assert review['source_atom'] == atom
                assert review['truth_label'] in {'confirmed', 'refuted', 'insufficient_data', 'not_checked', 'out_of_profile', 'conditional'}
                assert review['metric_role'] in {'technical_positive', 'question_positive', 'negative_control', 'supporting_fact', 'method_control', 'unscored'}
                if review['truth_label'] in {'insufficient_data', 'not_checked', 'out_of_profile', 'conditional'}:
                    assert review['metric_role'] == 'unscored'
            items.append({
                'atom_id': key, 'candidate_ids': decision['candidate_ids'],
                'disciplines': sorted({disciplines[c] for c in decision['candidate_ids']}),
                'source_atom': atom,
                'source': {'path': str(path.relative_to(ROOT)),
                           'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                           'json_pointer': '/atomic_dispositions/' + str(index)},
                'output_section_context': decision['output_section'],
                'followup_boundaries': decision.get('followup_dispositions', []),
                'review': review,
                'scoring_status': 'explicitly_reviewed_pending_freeze' if review else 'normalization_pending_not_new_engineering_review',
                'eligible_for_run_scoring': False,
            })
    assert set(reviews) <= seen, 'Orphan scoring review; source atom changed'
    result = {
        'purpose': 'Evaluator-only atomic ledger; source adjudications are retained verbatim.',
        'protocol': 'evaluation_protocol.json',
        'summary': {'atoms': len(items), 'explicit_scoring_reviews': len(reviews),
                    'remaining_scoring_normalizations': len(items)-len(reviews),
                    'reviewed_metric_roles': dict(Counter(r['metric_role'] for r in reviews.values())),
                    'benchmark_frozen': False},
        'limits': ['Unnormalized atoms already have engineering adjudications; this is not a request to redo source review.',
                   'Supporting facts and method controls are not independent project defects.',
                   'No run scoring before a versioned freeze and declared eligible denominator.'],
        'items': items,
    }
    (HERE / 'consolidated/atom_ledger.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result['summary'], ensure_ascii=False))


if __name__ == '__main__':
    main()
