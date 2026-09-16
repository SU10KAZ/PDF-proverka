"""Opt-in replay adapter. V1 normalization/binding and V4 admission are reused."""
from copy import deepcopy
from dataclasses import fields

from experiments.project_change_f2_binding_v4_272.normalization import normalize, compare, phase_is_applicable
from experiments.project_change_f2_binding_v4_272.contract import EngineeringState, EvidenceBinding
from experiments.project_change_f2_binding_v4_272.admission import admit
from experiments.project_change_contracts_272.states import StateTransition
from experiments.project_change_contracts_v2_272.sufficiency import evaluate as sufficient
from experiments.project_change_post_inference_repair_272.replay import profile
from .applicability import evaluate
from experiments.project_change_post_inference_repair_v2_272.numeric import NumericConflictGuard


def state(data):
    allowed = {f.name for f in fields(EngineeringState)}
    values = {k: v for k, v in data.items() if k in allowed}
    values['evidence_bindings'] = tuple(EvidenceBinding(**b) for b in data['evidence_bindings'])
    return EngineeringState(**values)


def repair(raw, packet, context, facts=()):
    n = normalize(raw, packet, profile(raw), subject_contract=context)
    if not all(s + '_state' in n for s in ('old', 'new')):
        return n
    states = {s: state(n[s + '_state']) for s in ('old', 'new')}
    app, states = evaluate(raw, packet, states, n.get('claim_applicability') or {}, n['source_conflict'])
    numeric = NumericConflictGuard().evaluate(raw, facts, app['primary_evidence']['complete'])
    issues = list(n['issues'])
    optional = []
    # No binding is created or accepted here. Only a failed auxiliary reference
    # can cease to veto an independently sufficient direct primary claim.
    if app['primary_evidence']['complete'] and not app['issues'] and not n['source_conflict']['blocking'] and not numeric['blocking']:
        optional = [b for b in n['rejected_evidence_bindings'] if b['role'] == 'COUNTER_EVIDENCE'
                    and b['reason'] == 'BINDING_SUBJECT_WITNESS_REQUIRED'
                    and raw.get('old_absence', {}).get('mode') in {None, 'NOT_APPLICABLE'}]
        remaining = [b for b in n['rejected_evidence_bindings'] if b not in optional]
        if optional and not any(b['reason'] == 'BINDING_SUBJECT_WITNESS_REQUIRED' for b in remaining):
            issues = [i for i in issues if i != 'BINDING_SUBJECT_WITNESS_REQUIRED']
    issues += app['issues']
    if numeric['blocking']: issues.append('PRINTED_DERIVED_CONFLICT')
    issues = list(dict.fromkeys(issues))
    support = all(raw[s + '_state'].get('support') == 'PROVEN' for s in ('old', 'new')) and not issues
    transition = StateTransition(states['old'], states['new'], raw.get('identity_basis', ''), raw.get('mapping_basis', ''),
        source_conflict='BLOCKING_OR_UNKNOWN' if n['source_conflict']['blocking'] or numeric['blocking'] else '',
        state_support='SOURCE_SUPPORTED' if support else 'UNKNOWN', design_use=raw.get('design_use', ''))
    comparison = compare(transition, phase_applicable=phase_is_applicable(transition, raw.get('applicable_conditions', [])), claim_applicability=app)
    mapped = deepcopy(raw)
    for side, s in states.items(): mapped[side + '_state']['state_role'] = s.state_role.value
    p = profile(mapped)
    suff = sufficient(p, transition, evidence_forms={s: raw[s + '_state'].get('evidence_form', 'UNKNOWN') for s in ('old', 'new')},
                      negative=n['negative_state'], comparison=comparison)
    if raw.get('verdict') == 'NOT_CHANGE':
        negative_reasons = list(issues)
        if not app['primary_evidence']['complete']: negative_reasons.append('NEGATIVE_PRIMARY_PROOF_REQUIRED')
        if comparison['status'] != 'COMPARABLE': negative_reasons.append('NEGATIVE_COMPARABLE_STATES_REQUIRED')
        suff = dict(schema='NEGATIVE_SUFFICIENCY/3', profile='LOCAL_NEGATIVE_ASSERTION',
                    reasons=list(dict.fromkeys(negative_reasons)), NEGATIVE_SCOPE=app['NEGATIVE_SCOPE'])
    suff['graphics_complete_required'] = app['graphic']['requirement'] == 'GRAPHIC_REQUIRED'
    suff['reasons'] += app['issues'] + (['PRINTED_DERIVED_CONFLICT'] if numeric['blocking'] else [])
    suff['sufficient'] = not suff['reasons']
    valid = {b.evidence_id for s in states.values() for b in s.evidence_bindings if b.witness_validated}
    admission = admit(raw, transition, comparison, suff, valid, issues)
    # V4 no-change equivalence may short circuit sufficiency. Required missing
    # graphic/basis/witness proof must also veto an unsupported NOT_CHANGE.
    final = 'REVIEW' if (app['issues'] or numeric['blocking'] or
        (raw.get('verdict') == 'NOT_CHANGE' and not suff['sufficient'])) else admission['effective_verdict']
    n.update(schema='PAIR_A_POST_INFERENCE_REPAIR/3', status='REVIEW' if issues else 'NORMALIZED', issues=issues,
        claim_applicability=app, numeric_conflict=numeric, optional_rejected_supporting_references=optional,
        supporting_reference_decision='Optional references stay rejected and are never used as evidence.',
        old_state=states['old'].to_dict(), new_state=states['new'].to_dict(), sufficiency=suff,
        f2=dict(comparability=comparison, exists_change=admission['exists_change'], materiality=admission['materiality']),
        exists_change=admission['exists_change']['status'], effective_verdict=final,
        admission_order=['NORMALIZATION_V1', 'BINDING_V1', 'APPLICABILITY_V3', 'NUMERIC_CONFLICT_GUARD', 'F2', 'V4', 'FINAL_ADMISSION'])
    n['transformations'].append(dict(path='applicability_v3', before=None, after=app,
        reason='Claim-specific applicability after unchanged V1 binding.', provenance=dict(raw_response_hash=n['raw_response_hash'])))
    return n
