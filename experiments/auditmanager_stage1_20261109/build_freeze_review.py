"""Inventory evidence boundaries before freeze; never adjudicate by field presence."""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def read(path):
    return json.loads(path.read_text())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    records = read(HERE / 'consolidated/records.json')
    by_id = {r['candidate_id']: r for r in records}
    items = []
    seen = set()
    for path in sorted((HERE / 'decisions').glob('*.json')):
        d = read(path)
        for cid in d['candidate_ids']:
            assert cid in by_id and cid not in seen, cid
            seen.add(cid)
        linked = None
        if d.get('primary_adjudication'):
            ref = d['primary_adjudication']
            p = Path(ref['path'])
            if not p.is_absolute():
                p = ROOT / p
            assert digest(p) == ref['sha256'], str(p)
            linked = read(p)
        items.append({
            'candidate_ids': d['candidate_ids'],
            'decision_file': str(path.relative_to(ROOT)),
            'decision_sha256': digest(path),
            'original_claims': [{
                'candidate_id': cid,
                'discipline': by_id[cid]['discipline'],
                'claim': by_id[cid]['original_candidate']['claim'],
            } for cid in d['candidate_ids']],
            'narrow_output': d.get('wording', d.get('required_behavior')),
            'output_section': d['output_section'],
            'atomic_dispositions': d.get('atomic_dispositions'),
            'linked_claim_disposition': (linked or {}).get('claim_disposition'),
            'primary_adjudication': d.get('primary_adjudication'),
            'limits': d.get('limits'),
            'unsupported_extensions': d.get('unsupported_extensions'),
            'linked_scope': (linked or {}).get('scope'),
            'linked_do_not_publish': (linked or {}).get('do_not_publish'),
            'followup_requirements': d.get('followup_requirements', []),
            'control_pair': d.get('control_pair'),
            'other_pair_evidence': {k: d[k] for k in (
                'control_pair_candidate', 'control_pair_limits', 'pair_reference',
                'control_case', 'conditional_counterexample', 'dedup_validation_case'
            ) if k in d},
            'benchmark_scope_review': d.get('benchmark_scope_review'),
            'freeze_review_status': d.get('benchmark_scope_review', {}).get('status', 'pending_semantic_review'),
            'scoring_authorization': False,
        })
    assert seen == set(by_id)
    result = {
        'purpose': 'Review all original claims against narrow decisions before freezing the benchmark.',
        'warnings': [
            'This is an inventory, not a new adjudication or scoring set.',
            'Missing fields do not prove missing review; inspect linked evidence.',
            'Presence of a control_pair does not establish a validated positive/negative pair.',
            'Unresolved claims must remain visible and unscored; never count omission as refutation.',
            'Primary queue completion does not close stage 1.',
        ],
        'source_records_sha256': digest(HERE / 'consolidated/records.json'),
        'summary': {
            'candidate_records': len(seen),
            'decision_themes': len(items),
            'themes_with_explicit_followups': sum(bool(x['followup_requirements']) for x in items),
            'themes_with_direct_atomic_dispositions': sum(x['atomic_dispositions'] is not None for x in items),
            'themes_with_linked_claim_disposition': sum(x['linked_claim_disposition'] is not None for x in items),
            'themes_with_direct_control_pair': sum(x['control_pair'] is not None for x in items),
            'themes_with_scope_review': sum(x['benchmark_scope_review'] is not None for x in items),
            'stage_complete': False,
        },
        'items': items,
    }
    out = HERE / 'consolidated/freeze_review_inventory.json'
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result['summary'], ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
