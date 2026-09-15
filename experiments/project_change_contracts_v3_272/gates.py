"""Fixed diagnostic gates, excluded from isolated inference context."""
from .common import read
from experiments.project_change_contracts_272.witnesses import raster_locator_errors


def package_gates(rows):
    r13 = next(r for r in rows if r['case_id'] == 'R13')
    packet = read(r13['semantic_packet']['path'])
    requirements = packet['evidence_coverage']['requirements']
    checks = {}
    for side in ('OLD', 'NEW'):
        primary = [r for r in requirements if r['requirement']['side'] == side and r['delivery']['purpose'] == 'PRIMARY_STATE']
        schemes = [r for r in requirements if r['requirement']['side'] == side and r['delivery']['purpose'] == 'GRAPHIC_COUNTERPART']
        checks[side + '_primary_delivered'] = bool(primary) and all(r['delivery']['delivered'] for r in primary)
        checks[side + '_mandatory_scheme_delivered'] = bool(schemes) and all(
            r['delivery']['mandatory'] and r['delivery']['required_type'] == 'GRAPHIC_SCHEME' and
            r['delivery']['selected_evidence_type'] == 'GRAPHIC_SCHEME' and r['delivery']['type_match'] == 'YES' and
            r['delivery']['delivered'] for r in schemes)
    witnesses = []
    for row in rows:
        pkt = read(row['semantic_packet']['path'])
        for side in ('old', 'new'):
            for e in pkt['evidence'][side]:
                if e.get('raster'):
                    errors = raster_locator_errors(dict(evidence_id=e['evidence_id'],
                        visual_locator='П1', bbox_norm=[0, 0, 1, 1], binding_reason='Short graphic locator binding'), e)
                    witnesses.append(dict(case_token=row['case_token'], evidence_id=e['evidence_id'], errors=errors))
    checks['F4_short_graphic_locators'] = bool(witnesses) and not any(w['errors'] for w in witnesses)
    checks['fixed_budget'] = all(r['raster_allocation']['used'] <= 8 for r in rows)
    checks['type_mismatch_never_complete'] = all(
        r['completeness'] != 'COMPLETE' for row in rows for r in read(row['coverage']['path'])['requirements']
        if r['delivery']['type_match'] == 'NO')
    return dict(status='PASS' if all(checks.values()) else 'FAIL', checks=checks,
        R13_requirements=requirements, F4=witnesses, model_calls=0)


def replay_gates(rows, out):
    expected = dict(C01='ACCEPT', C07='NOT_CHANGE', C15='ACCEPT', C18='ACCEPT', C22='NOT_CHANGE',
                    R01='ACCEPT', R19='ACCEPT', Q04='REVIEW', R21='ACCEPT')
    by_case = {r['case_id']: r for r in rows}
    checks = {case: by_case[case]['effective_verdict'] == verdict for case, verdict in expected.items()}
    checks.update({case: by_case[case]['effective_verdict'] != 'ACCEPT' for case in ('C05', 'S_FP01')})
    checks['R13_old_response_preserved'] = by_case['R13']['effective_verdict'] == 'REVIEW'
    normalized = {c: read(out / 'offline_replay' / (r['key'] + '.json')) for c, r in by_case.items()}
    for case in ('R19', 'R21'):
        checks[case + '_phase_not_applicable'] = all(normalized[case][s + '_state']['stage_phase'] == 'NOT_APPLICABLE' for s in ('old', 'new'))
        checks[case + '_material'] = normalized[case]['f2']['materiality']['status'] == 'MATERIAL'
    checks['C07_change_before_materiality'] = normalized['C07']['exists_change'] == 'NO' and normalized['C07']['f2']['materiality']['status'] == 'NOT_APPLICABLE'
    checks['C18_bounded_negative'] = normalized['C18']['negative_state']['status'] == 'PROVEN_ABSENT_IN_BOUNDED_SCOPE'
    checks['R21_placement_conflict_scoped'] = not normalized['R21']['source_conflict']['blocking']
    return dict(status='PASS' if all(checks.values()) else 'FAIL', checks=checks, cases=rows,
                model_calls=0, known_dev=True, inference_inputs_include_replay=False)
