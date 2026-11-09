"""Assemble reviewed targets with explicit aliases, never infer truth from prose."""
import hashlib
import json
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
ROUTES = {'technical_positive': 'technical', 'question_positive': 'question',
          'negative_control': 'negative'}
# Same narrow claim, checked explicitly against both source decisions.
ALIASES = {
    'AK137_F019_cable_schedule_scope:6dd62a3aff02e2e71c38':
        'AK_F020_heater_cable_conflict:e7d303bee6d1b202f3e3',
    'ITP_F034_mixed_norm_references:ccc9fdab3b3c3cec9446':
        'ITP_F053_standard_title:2b8fdfa112de84cdbbba',
    'VK32_drain_count_identity:2ad54a1c465fa24a0c7a':
        'VK_F013_drain_assembly_scope:f37578f6de4df40d140e',
}


def read(path):
    return json.loads(path.read_text())


def context(theme):
    path = HERE / 'decisions' / (theme + '.json')
    decision = read(path)
    return {
        'theme': theme, 'candidate_ids': decision['candidate_ids'],
        'source': {'path': str(path.relative_to(ROOT)),
                   'sha256': hashlib.sha256(path.read_bytes()).hexdigest()},
        'wording': decision.get('wording', decision.get('required_behavior')),
        'primary_adjudication': decision.get('primary_adjudication'),
        'limits': decision.get('limits'),
        'followup_boundaries': decision.get('followup_dispositions', []),
    }


def main():
    ledger = read(HERE / 'consolidated/atom_ledger.json')
    assert ledger['summary']['remaining_scoring_normalizations'] == 0
    targets = {}
    for atom in ledger['items']:
        review = atom['review']
        role = review['metric_role']
        if role not in ROUTES:
            continue
        assert review['truth_label'] == ('refuted' if role == 'negative_control' else 'confirmed')
        key = atom['atom_id']
        targets[key] = {
            'target_id': key, 'route': ROUTES[role], 'origin': 'reviewed_atom',
            'source_atom': atom['source_atom'], 'review_scope': review['scope'],
            'contexts': [context(key.split(':')[0])], 'source_ids': [key],
            'matching_rule': 'Match the narrow atom WITH its reviewed scope and object relation in context. A supporting number alone is not a found defect. Never require unknown consequences to be true.',
            'eligible_for_run_scoring': False,
        }
    for alias, canonical in ALIASES.items():
        assert alias in targets and canonical in targets
        old, kept = targets.pop(alias), targets[canonical]
        assert old['route'] == kept['route']
        kept['source_ids'].extend(old['source_ids'])
        kept['contexts'].extend(old['contexts'])
    composites = read(HERE / 'composite_target_reviews.json')
    for item in composites['items']:
        ctx = context(item['theme'])
        assert ctx['source']['sha256'] == item['source']['sha256']
        for target in item['targets']:
            key = target['target_id']
            assert key not in targets
            targets[key] = {
                **target, 'origin': 'explicit_composite', 'contexts': [ctx],
                'source_ids': [key], 'context_atom_ids': item['source_atom_ids'],
                'matching_rule': 'Match the explicit required meaning. Context atoms include refuted and unknown claims and are NOT all positive requirements.',
            }
    # Every original positive/negative target is still addressable after aliasing.
    source_ids = [sid for target in targets.values() for sid in target['source_ids']]
    assert len(source_ids) == len(set(source_ids))
    index = {}
    for target in targets.values():
        for ctx in target['contexts']:
            index.setdefault(ctx['theme'], []).append(target['target_id'])
    result = {
        'status': 'assembled_pending_denominator_freeze',
        'benchmark_frozen': False, 'production_promotion_allowed': False,
        'summary': {'canonical_targets': len(targets),
                    'routes': dict(Counter(t['route'] for t in targets.values())),
                    'explicit_aliases': len(ALIASES), 'covered_themes': len(index)},
        'aliases': ALIASES,
        'overlap_rules': 'target_overlap_reviews.json',
        'matching_policy': 'A theme can contain several targets. Full theme coverage requires all applicable canonical targets, including mandatory locations specified in source wording. Extra unsupported assertions are scored separately.',
        'limits': ['Counts are target inventory, not measured recall or an accepted per-input denominator.',
                   'Negative targets describe claims which must not be emitted, not useful positive findings.',
                   'Input availability, profile, exposure and canonical overlap must be sealed before run scoring.'],
        'theme_index': index, 'targets': list(targets.values()),
    }
    (HERE / 'consolidated/evaluation_targets.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result['summary'], ensure_ascii=False))


if __name__ == '__main__':
    main()
