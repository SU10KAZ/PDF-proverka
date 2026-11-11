"""Separate experimental measurement eligibility from semantic method acceptance."""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).parent
LIMITS = {
    'H01': ('bounded_with_routing_dispute', 'F032 routing remains disputed; preserve its evidence and separate questions. No severity recovery from source-ID existence.'),
    'H02': ('bounded_controls_recorded', 'Known-case context retrieval passed bounded checks; independent discovery and full-project costs remain unmeasured.'),
    'H03': ('partial_identity_and_missing_negative_pair', 'EOM physical instance identity unresolved. No generic terminal-number suppression is accepted; operation final disposition is a distinct mechanism.'),
    'H04': ('partial_scope_and_profile_disputes', 'F050 document scope and F005 profile remain unresolved. Automatic grouping stays disabled. Quantity arithmetic does not prove procurement shortage.'),
    'H05': ('partial_and_invalid_negative_control', 'F014 supports a cross-view conflict rather than the proposed negative control. H72 pipe allocation unresolved. Do not accept a suppression rule from either case.'),
    'H06': ('bounded_controls_recorded', 'Applicability controls passed only with supplied sources; inaccessible external documents remain availability limits, not proof of compliance.'),
    'H07': ('historical_negative_pair_missing', 'Synthetic metadata controls are not historical pairs. Broad automatic metadata suppression is not accepted.'),
}


def ref(path):
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def main():
    checklist_path = HERE / 'original_control_checklist.json'
    checklist = json.loads(checklist_path.read_text())
    hypotheses = []
    for hid, (status, limit) in LIMITS.items():
        rows = [r for r in checklist['rows'] if r['hypothesis'] == hid]
        assert rows
        for row in rows:
            for item in row['evidence']:
                assert hashlib.sha256(Path(item['path']).read_bytes()).hexdigest() == item['sha256']
        hypotheses.append({'id': hid, 'local_outcome': status, 'limit': limit,
            'original_controls': [{'id': r['id'], 'state': r['evidence_state']} for r in rows],
            'full_method_acceptance': False})
    assert sum(len(h['original_controls']) for h in hypotheses) == 37
    result = {
        'status': 'experimental_measurement_decisions_not_production_admission',
        'bindings': [ref(checklist_path), ref(HERE / 'candidate_retrieval_v2_snapshot_provenance.json'),
                     ref(HERE / 'retrieval_v2_preflight_checks.json'), ref(HERE / 'original_trial_protocol.json')],
        'hypotheses': hypotheses,
        'arms': {
            'classic_current': {'decision': 'eligible_for_reference_measurement', 'purpose': 'Measure common candidate foundation with both optional families off; compare with historical classic.'},
            'inventory': {'decision': 'eligible_for_experimental_measurement_only', 'purpose': 'Measure inventory, dependent operations and context retrieval as one bundle against classic_current. No claim of isolated H01-H07 effects.'},
            'source_review': {'decision': 'eligible_for_experimental_measurement_only', 'purpose': 'Measure source review with mandatory independent release check against classic_current. Record incorrect suppressions as failures, including protected useful findings.'},
            'candidate': {'decision': 'held', 'purpose': 'Combined arm requires accepted constituent methods; successful preflight is insufficient.'},
        },
        'dispatch_prerequisites': ['Separate frozen trial with identical six source packages, three repeats per arm, numeric budgets, evaluator and shared executor hashes.',
            'Preregister serial dispatch and load conditions; do not overlap candidate full audits with the live historical series.',
            'Executor receives project sources only, never this decision file, control IDs, expected answers or engineering history.'],
        'assessment_requirements': ['Report unresolved controls separately; do not count them as passed, false or recovered by analogy.',
            'All discovered suppressions and useful-findings losses require source-based evaluation; no generic filter accepted without a protected pair.',
            'Frozen evaluator v2 remains unchanged; F014 does not become a scored negative control.',
            'Compare foundation separately; common code changes cannot be attributed to an optional method.',
            'Experimental runs may reject a method. Production and combination require original acceptance thresholds, new-project evidence and independent engineer review.'],
        'stage4_complete': False, 'full_method_acceptance': False,
        'runnable_trial_created': False, 'new_model_calls': 0,
    }
    (HERE / 'experimental_arm_decisions_v2.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'controls': 37, 'hypotheses': 7, 'combined_arm': 'held', 'model_calls': 0}))


if __name__ == '__main__':
    main()
