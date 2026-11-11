"""Connect the 145-rule design to frozen trials without promoting thematic coverage."""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).parent
DESIGN = Path('experiments/auditmanager_stage3_20261110')


def ref(p):
    return {'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}


def main():
    matrix_path = DESIGN / 'transfer_matrix_145.json'
    hypotheses_path = DESIGN / 'selected_hypotheses.json'
    limits_path = DESIGN / 'nontransfer_boundaries.json'
    decisions_path = HERE / 'experimental_arm_decisions_v2.json'
    snapshot_path = HERE / 'candidate_retrieval_v2_snapshot_provenance.json'
    matrix = json.loads(matrix_path.read_text())['rules']
    selected = json.loads(hypotheses_path.read_text())['hypotheses']
    limits = json.loads(limits_path.read_text())['boundaries']
    decisions = {h['id']: h for h in json.loads(decisions_path.read_text())['hypotheses']}
    snapshot = json.loads(snapshot_path.read_text())
    rules = {r['id']: r for r in matrix}
    assert len(matrix) == len(rules) == 145
    assert all(set(h['rule_ids']) <= set(rules) for h in selected)
    named = {rid: [h['id'] for h in selected if rid in h['rule_ids']] for rid in rules}
    rows = []
    for r in matrix:
        links = r['hypotheses']
        assert set(links) <= set(decisions) | {'evaluation'}
        sources = []
        for source in r['implementation_sources']:
            candidate_hash = snapshot['source_files'].get(source['path'])
            sources.append({'path': source['path'], 'symbol': source.get('symbol'),
                'design_sha256': source['sha256'], 'frozen_candidate_sha256': candidate_hash,
                'state': 'not_in_runtime_capsule' if candidate_hash is None else
                         ('same_bytes_as_design' if candidate_hash == source['sha256'] else 'changed_requires_semantic_reassessment')})
        rows.append({'id': r['id'], 'discipline': r['discipline'], 'meaning': r['meaning'],
            'design_recommendation': r['recommendation'],
            'design_implementation_status_not_current_acceptance': r['implementation_status'],
            'design_hypothesis_links': links, 'explicitly_named_in_selected_hypotheses': named[r['id']],
            'hypothesis_outcomes': [{'id': h, 'local_outcome': decisions[h]['local_outcome'],
                                     'limit': decisions[h]['limit']} for h in links if h in decisions],
            'historical_implementation_source_identity': sources,
            'nontransfer_boundaries': [b for b in limits if b['rule_ids'] == 'ALL145' or r['id'] in b['rule_ids'].split(',')],
            'whole_rule_runtime_acceptance': False,
            'interpretation': 'Explicitly selected control scope still does not prove the whole rule.' if named[r['id']] else
                              'Design-level association only; do not count this rule as exercised merely because its hypothesis has a pilot.'})
    report = {'status': '145_rule_trial_traceability_not_acceptance',
        'inputs': [ref(p) for p in (matrix_path, hypotheses_path, limits_path, decisions_path, snapshot_path)],
        'candidate_commit': snapshot['provenance']['commit'], 'rules': rows,
        'counts': {'rules': len(rows), 'explicitly_named_rules': sum(bool(r['explicitly_named_in_selected_hypotheses']) for r in rows),
                   'evaluation_only_rules': sum(r['design_hypothesis_links'] == ['evaluation'] for r in rows),
                   'rules_with_changed_source_bytes': sum(any(s['state'] == 'changed_requires_semantic_reassessment' for s in r['historical_implementation_source_identity']) for r in rows),
                   'whole_rules_accepted': 0},
        'boundaries': ['Preserves all 145 design rows; seven hypotheses do not certify all linked rules.',
                       'Changed bytes are a review trigger, not proof that behavior was added or removed.',
                       'Static support, prompt instructions, control evidence and full-audit acceptance remain separate.',
                       'Historical nontransfer wording about model permissions is subordinate to current user authorization; frozen comparison remains Astra-only by design.'],
        'stage4_complete': False, 'production_acceptance': False}
    (HERE / 'rule_trial_traceability_145.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(report['counts']))


if __name__ == '__main__':
    main()
