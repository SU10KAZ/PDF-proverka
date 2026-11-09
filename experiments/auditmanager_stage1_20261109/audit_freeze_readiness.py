"""Audit structural coverage and expose pair gaps; never infer semantic acceptance."""
import hashlib
import json
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    inventory_path = HERE / 'consolidated/freeze_review_inventory.json'
    inventory = read(inventory_path)
    records_path = HERE / 'consolidated/records.json'
    records = read(records_path)
    assert sha(records_path) == inventory['source_records_sha256']
    pairs = []
    pair_ids = {}
    for path in sorted((HERE / 'followup').glob('*control_pair.json')):
        pair = read(path)
        members = []
        for side in ('positive', 'negative'):
            entry = pair.get(side, {})
            ids = entry.get('candidate_ids', [])
            if entry.get('candidate_id'):
                ids = ids + [entry['candidate_id']]
            members.extend(ids)
        ref = {'path': str(path.relative_to(ROOT)), 'sha256': sha(path),
               'control_id': pair.get('control_id'),
               'implementation_test_status': pair.get('implementation_test_status', 'not_established'),
               'evidence_limits': pair.get('evidence_limits', [])}
        pairs.append(ref)
        for cid in members:
            pair_ids.setdefault(cid, []).append(ref['control_id'])
    seen = []
    items = []
    requirements = 0
    for item in inventory['items']:
        path = ROOT / item['decision_file']
        assert sha(path) == item['decision_sha256'], path
        seen.extend(item['candidate_ids'])
        dispositions = item['followup_dispositions']
        assert [d['requirement'] for d in dispositions] == item['followup_requirements']
        assert all(d['unscored_claims'] and d['retained_evidence_boundary'] for d in dispositions)
        requirements += len(dispositions)
        standalone = sorted({pid for cid in item['candidate_ids'] for pid in pair_ids.get(cid, [])})
        description = bool(item['control_pair'] or item['other_pair_evidence'])
        status = ('standalone_design_runtime_unproven' if standalone else
                  'description_requires_pair_validation' if description else
                  'no_explicit_pair_link')
        items.append({'candidate_ids': item['candidate_ids'],
                      'decision_file': item['decision_file'],
                      'decision_sha256': item['decision_sha256'],
                      'pair_status': status, 'standalone_controls': standalone,
                      'automatic_filter_authorized': False,
                      'evaluation_boundary': 'Use only explicitly supported narrow claims. Unknown, conditional, unchecked and out-of-profile claims are not technical positives or technical refutations. Preserve engineer feedback separately.',
                      'next_action': ('Evaluate the named controls against sealed runtime outputs; retain their evidence gaps.' if standalone else
                                      'Validate both sides and historical provenance before filter acceptance; otherwise retain explicit evidence gap.' if description else
                                      'No filter acceptance on this theme alone. Pair evidence is not linked here; do not infer that it does not exist elsewhere.')})
    assert len(seen) == len(set(seen)) == len(records) == 291
    assert set(seen) == {r['candidate_id'] for r in records}
    disciplines = Counter(r['discipline'] for r in records)
    assert set(disciplines) == {'AK', 'AR', 'EOM', 'ITP', 'KM', 'VK'}
    result = {
        'purpose': 'Freeze readiness audit, not a frozen scoring benchmark or model-quality result.',
        'input_inventory': {'path': str(inventory_path.relative_to(ROOT)), 'sha256': sha(inventory_path)},
        'summary': {'records': len(seen), 'themes': len(items), 'disciplines': dict(sorted(disciplines.items())),
                    'followup_requirements_with_dispositions': requirements,
                    'pair_status_counts': dict(Counter(i['pair_status'] for i in items)),
                    'structural_coverage_passed': True, 'stage_complete': False},
        'remaining_freeze_requirements': [
            'Adopt an explicit versioned scoring protocol preserving atomic verdicts and unknowns; do not classify by verdict prefix or output section alone.',
            'Record which proposed filters each validated pair can support; missing pairs remain evidence gaps, not implicit approvals.',
            'Seal evaluator artifacts and manifest; exclude answers and history from independent discovery inputs.',
        ],
        'standalone_controls': pairs,
        'items': items,
        'limits': ['This audit checks coverage, links and disposition presence, not engineering truth.',
                   'The 291 records and 247 themes are not independent defects or a recall denominator.',
                   'A completed primary queue and followup disposition list do not prove any runtime improvement.'],
    }
    (HERE / 'consolidated/freeze_readiness_audit.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result['summary'], ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
