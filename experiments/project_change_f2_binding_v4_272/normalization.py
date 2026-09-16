"""Deterministic raw-output adapter. Every transformation retains source and reason.

Semantic mappings must be supplied explicitly with delivered references; prose is
never interpreted as a boolean or used to guess cross-version identity.
"""
from dataclasses import fields, replace
import re

from experiments.project_change_contracts_272.evidence import fingerprint
from experiments.project_change_contracts_272.states import StateTransition, Cardinality
from .contract import EngineeringState, EvidenceRole
from .binding import bind
from experiments.project_change_contracts_272.comparability import applicable_dimensions, DIMENSIONS, known
from .admission import admit
from experiments.project_change_contracts_v2_272.negative import bounded_contract, certify
from experiments.project_change_contracts_v2_272.sufficiency import evaluate
from experiments.project_change_contracts_272.witnesses import raster_locator_errors

VERSION = 'F2_MODEL_NORMALIZATION/4'


def clean(value):
    return ' '.join(value.split()) if isinstance(value, str) else value


def phase(value):
    if not value:
        return None
    # Only version metadata is erased. Engineering phase is never inferred from it.
    value = re.sub(r'\b(?:OLD|NEW)\b|\bstage_[12]\b|\blogical\s+v002\b', '', value, flags=re.I)
    value = re.sub(r'^[\s;,]+|[\s;,]+$', '', value)
    value = clean(value)
    if not value or value.upper() in {'UNKNOWN', 'UNRESOLVED'}:
        return None
    if value.upper() in {'NOT_APPLICABLE', 'PROJECT_DESIGN', 'WORKING_DESIGN', 'INSTALLED', 'FIRST_STAGE', 'FULL_OBJECT'}:
        return value.upper()
    # Residual wording about comparison direction is not engineering phase.
    if not re.search(r'проект|рабоч|очеред|полный объект|полному объекту|стадия|design|installed|phase', value, flags=re.I):
        return None
    return value


def conflict(raw, subject, role, claim_id):
    if not isinstance(raw, dict):
        return dict(status='NONE' if not raw else 'UNKNOWN', relevance='UNKNOWN',
            affected_claim_ids=[], affected_subject='', affected_state='', explanation=raw or '',
            blocking=bool(raw), reason='Legacy free text is not a conflict boolean; nonempty prose needs structured review')
    status = raw.get('status', 'UNKNOWN')
    relevance = raw.get('relevance', raw.get('relevance_to_claim', 'UNKNOWN'))
    targets = raw.get('affected_claim_ids', raw.get('affected_claims', []))
    if status not in {'NONE', 'PRESENT', 'UNKNOWN'} or relevance not in {'RELEVANT', 'IRRELEVANT', 'UNKNOWN'} or not isinstance(targets, list):
        status, relevance, targets = 'UNKNOWN', 'UNKNOWN', []
    affected_subject, affected_state = raw.get('affected_subject', ''), raw.get('affected_state', '')
    explicitly_related = claim_id in targets or ('*' in targets) or (not targets and
        clean(affected_subject) == clean(subject) and affected_state == role)
    if status == 'NONE':
        blocking = False
    elif targets and not explicitly_related:
        blocking = False
    elif status == 'UNKNOWN':
        blocking = relevance != 'IRRELEVANT' or explicitly_related
    elif relevance == 'IRRELEVANT':
        # Contradictory relevance cannot suppress an explicitly targeted conflict.
        blocking = explicitly_related or not raw.get('explanation')
    else:
        blocking = relevance == 'UNKNOWN' or explicitly_related or not targets
    return dict(status=status, relevance=relevance, affected_claim_ids=targets,
        affected_subject=affected_subject, affected_state=affected_state,
        explanation=raw.get('explanation', ''), blocking=blocking,
        reason='Claim-specific conflict relevance; explanation presence does not imply conflict')


def phase_is_applicable(transition, declared=()):
    # Administrative design-stage labels do not condition a criterion or a
    # selected-equipment claim. Explicit construction/calculation phases do.
    if any(c.get('dimension') == 'stage_phase' and c.get('applicability') == 'APPLICABLE' for c in declared):
        return True
    for state in (transition.old, transition.new):
        if re.search(r'FIRST_STAGE|FULL_OBJECT|INSTALLED|очеред|строитель|расч[её]тн.*фаз', state.stage_phase or '', re.I):
            return True
    return False


def compare(transition, *, phase_applicable=None, claim_applicability=None):
    dimensions = applicable_dimensions(transition)
    if all(s.state_role in {'INPUT_CRITERION', 'REQUIREMENT'} for s in (transition.old, transition.new)):
        # Criteria are compared at their declared subject/scope, not as sums.
        dimensions -= {'component_or_total', 'consumer_composition', 'calculation_basis'}
    if not (phase_is_applicable(transition) if phase_applicable is None else phase_applicable):
        dimensions.discard('stage_phase')
    if claim_applicability:
        dimensions -= set(claim_applicability['not_applicable'])
    conditions, different, unknown = [], [], []
    for name in sorted(DIMENSIONS):
        a, b = getattr(transition.old, name), getattr(transition.new, name)
        relevant = name in dimensions
        if not relevant:
            status = 'NOT_APPLICABLE'
        elif not known(a) or not known(b) or a == 'NOT_APPLICABLE' or b == 'NOT_APPLICABLE':
            status = 'UNKNOWN'; unknown.append(name)
        elif (set(a) == set(b) if name == 'consumer_composition' else a == b):
            status = 'SAME'
        else:
            status = 'DIFFERENT'; different.append(name)
        conditions.append(dict(dimension=name, old=a, new=b, comparison=status,
            applicability='APPLICABLE' if relevant else 'NOT_APPLICABLE'))
    if not transition.identity_basis:
        unknown.append('identity_basis')
    if transition.source_conflict:
        unknown.append('relevant_or_unresolved_source_conflict')
    mapping = transition.cardinality_errors()
    return dict(status='INCOMPARABLE' if different or mapping else 'REVIEW' if unknown else 'COMPARABLE',
        different=different, unknown=unknown, conditions=conditions, mapping_errors=mapping,
        cardinality=transition.old.comparison_cardinality.value)


def normalize(raw, packet, profile, *, subject_contract=None):
    audit, issues, states, warnings = [], [], {}, []
    def record(path, before, after, reason, evidence_ids=()):
        audit.append(dict(path=path, before=before, after=after, reason=reason,
            provenance=dict(raw_response_hash=fingerprint(raw), evidence_ids=list(evidence_ids))))
    all_evidence = [e for side in ('old', 'new') for e in packet['evidence'][side]]
    by_id = {e['evidence_id']: e for e in all_evidence}
    valid_witnesses = set()
    for witness in raw.get('witnesses', []):
        e = by_id.get(witness.get('evidence_id'))
        if not e or witness.get('side') != e['side']:
            issues.append('WITNESS_SOURCE_OR_SIDE_MISMATCH')
            continue
        if witness.get('kind') == 'RASTER_LOCATOR':
            errors = raster_locator_errors(witness, e)
        else:
            quote = clean(witness.get('literal_quote', ''))
            errors = [] if quote and quote in clean(e.get('quote', '')) else ['UNGROUNDED_LITERAL_WITNESS']
        issues.extend(errors)
        if not errors:
            valid_witnesses.add(e['evidence_id'])
    bindings = bind(raw, packet, valid_witnesses, subject_contract=subject_contract)
    issues.extend(bindings['issues'])
    negative = certify(bounded_contract(packet), raw.get('old_absence', {}), all_evidence)
    allowed_fields = {f.name for f in fields(EngineeringState)}
    for side in ('old', 'new'):
        source = raw[side + '_state']
        value = {k: v for k, v in source.items() if k in allowed_fields}
        delivered = {e['evidence_id']: e for e in packet['evidence'][side]}
        value_ids = source.get('evidence_ids', [])
        links = bindings['states'][side]
        value['evidence_bindings'] = links
        ids = value['evidence_ids'] = list(dict.fromkeys(b.evidence_id for b in links))
        record(side + '.evidence_ids', value_ids, ids,
            'Union of explicit source-grounded roles; value and identity references stay separate', ids)
        if not value_ids or not set(value_ids) <= delivered.keys():
            issues.append(side + '_UNDELIVERED_STATE_EVIDENCE')
        if not set(value_ids) & valid_witnesses:
            issues.append(side + '_STATE_WITNESS_REQUIRED')
        value['stage_phase'] = phase(source.get('stage_phase'))
        record(side + '.stage_phase', source.get('stage_phase'), value['stage_phase'],
               'Remove side/version tokens; retain explicitly supplied engineering phase', ids)
        identity = source.get('subject_identity')
        if isinstance(identity, dict) and all(known(identity.get(k)) for k in ('functional_owner', 'system', 'subsystem', 'scope')):
            canonical = {k: clean(identity[k]) for k in ('functional_owner', 'system', 'subsystem', 'scope')}
            identity_ids = identity.get('evidence_ids', [])
            bound_identity = {b.evidence_id for b in links if b.role == EvidenceRole.SUBJECT_IDENTITY}
            if identity_ids and set(identity_ids) <= bound_identity:
                value['engineering_subject'] = 'subject_' + fingerprint(canonical)[:24]
                if subject_contract is not None:
                    from experiments.project_change_post_inference_repair_272.identity import canonical_key, physical_identity
                    canonical = physical_identity(source)
                    value['engineering_subject'] = canonical_key(canonical, subject_contract)
                value['scope'] = canonical['scope']
                record(side + '.engineering_subject', source['engineering_subject'], canonical,
                    'Explicit functional identity grounded independently of state-value references', identity_ids)
            else:
                issues.append(side + '_SUBJECT_MAPPING_NOT_GROUNDED')
        else:
            issues.append(side + '_AMBIGUOUS_SUBJECT_MAPPING')
        value['branch_identity'] = source.get('functional_branch_identity') or None
        record(side + '.branch_identity', source.get('branch_identity'),
            dict(functional_branch_identity=value['branch_identity'], source_label=source.get('source_label', source.get('branch_identity'))),
            'Source labels are retained separately, never functional identity', ids)
        for name, item in list(value.items()):
            if isinstance(item, str):
                value[name] = clean(item)
        for name in ('members', 'consumer_composition'):
            if value.get(name) is not None:
                before = value[name]
                value[name] = list(dict.fromkeys(clean(x) for x in before if isinstance(x, str) and clean(x)))
                if list(before) != value[name]:
                    warnings.append(side + '_' + name.upper() + '_NORMALIZED')
                    record(side + '.' + name, before, value[name], 'Whitespace normalization and exact duplicate removal', ids)
        value['provenance'] = dict(raw_response_hash=fingerprint(raw), side=side,
            source_label=source.get('source_label', source.get('branch_identity')),
            witnesses=[dict(evidence_id=i, document_version=delivered[i]['document_version'],
                page=delivered[i]['page'], source_receipt=delivered[i].get('source_receipt')) for i in ids if i in delivered])
        try:
            states[side] = EngineeringState(**value)
        except (TypeError, ValueError) as exc:
            return dict(schema=VERSION, status='REVIEW', issues=issues + [str(exc)], transformations=audit,
                        negative_state=negative, f2=None, effective_verdict='REVIEW')
    applicability = None
    if subject_contract is not None:
        from experiments.project_change_post_inference_repair_272.applicability import evaluate as applicability_evaluate
        applicability = applicability_evaluate(raw, states)
        issues.extend(applicability['issues'])
        record('claim_applicability', None, applicability,
            'Claim-specific conditions require witnessed physical identity; changed role is a state property')
    old, new = states['old'], states['new']
    counts = len(old.members), len(new.members)
    if all(counts):
        card = '1→1' if counts == (1, 1) else '1→N' if counts[0] == 1 else 'N→1' if counts[1] == 1 else 'N→M'
        record('comparison_cardinality', [old.comparison_cardinality, new.comparison_cardinality], card,
            'Derived from explicit normalized member counts', old.evidence_ids + new.evidence_ids)
        if any(s.comparison_cardinality != card for s in (old, new)):
            warnings.append('MODEL_CARDINALITY_OVERRIDDEN_BY_NORMALIZED_MEMBERS')
    elif profile in {'NOVEL_SYSTEM', 'ADDED_FUNCTION'} and negative['absence_proven']:
        # The comparison is one parent function before/after, not fictitious OLD equipment.
        card = '1→1'
        record('comparison_cardinality', list(counts), card, 'Certified absent/present state of one bounded parent function')
    else:
        card = '1→1'
        issues.append('CARDINALITY_UNKNOWN_WITHOUT_MEMBERS')
    old, new = replace(old, comparison_cardinality=Cardinality(card)), replace(new, comparison_cardinality=Cardinality(card))
    if card == '1→1' and old.engineering_subject == new.engineering_subject and old.scope == new.scope and old.functional_role == new.functional_role and known(old.functional_role) and raw.get('mapping_basis'):
        if not old.branch_identity and not new.branch_identity:
            branch = 'function_' + fingerprint([old.engineering_subject, old.scope, old.functional_role])[:24]
            old, new = replace(old, branch_identity=branch), replace(new, branch_identity=branch)
            record('functional_branch_identity', None, branch,
                'Single functional branch with equal source-bound subject, scope, role and explicit mapping', old.evidence_ids + new.evidence_ids)
    conflict_state = conflict(raw.get('source_conflict'), raw['old_state']['engineering_subject'], old.state_role.value, raw.get('case_token', ''))
    record('source_conflict', raw.get('source_conflict'), conflict_state, conflict_state['reason'])
    support = all(raw[s + '_state'].get('support') == 'PROVEN' for s in ('old', 'new'))
    if profile in {'NOVEL_SYSTEM', 'ADDED_FUNCTION'}:
        support = negative['absence_proven'] and raw['new_state'].get('support') == 'PROVEN'
    transition = StateTransition(old, new, raw.get('identity_basis', ''), raw.get('mapping_basis', ''),
        source_conflict='BLOCKING_OR_UNKNOWN' if conflict_state['blocking'] else '',
        state_support='SOURCE_SUPPORTED' if support and not issues else 'UNKNOWN', design_use=raw.get('design_use', ''))
    phase_applicable = phase_is_applicable(transition, raw.get('applicable_conditions', []))
    if not phase_applicable:
        record('stage_phase.applicability', [old.stage_phase, new.stage_phase], 'NOT_APPLICABLE',
            'Claim has no construction/calculation phase dependency; administrative stage is source metadata')
        old, new = replace(old, stage_phase='NOT_APPLICABLE'), replace(new, stage_phase='NOT_APPLICABLE')
        transition = replace(transition, old=old, new=new)
    comparison = compare(transition, phase_applicable=phase_applicable, claim_applicability=applicability)
    if profile in {'NOVEL_SYSTEM', 'ADDED_FUNCTION'} and negative['absence_proven'] and raw.get('mapping_basis'):
        # Parent functional state may go absent -> several present members. Keep
        # the observed members intact and expose physical presence cardinality.
        comparison['presence_cardinality'] = f'{counts[0]}→{counts[1]}'
        comparison['comparison_level'] = 'BOUNDED_PARENT_FUNCTION'
        comparison['mapping_errors'] = []
        comparison['status'] = 'INCOMPARABLE' if comparison['different'] else 'REVIEW' if comparison['unknown'] else 'COMPARABLE'
    sufficiency = evaluate(profile, transition,
        evidence_forms={s: raw[s + '_state'].get('evidence_form', 'UNKNOWN') for s in ('old', 'new')},
        negative=negative, comparison=comparison)
    if profile == 'TOPOLOGY':
        for side in ('old', 'new'):
            if not any(e['evidence_id'] in valid_witnesses and e.get('raster') and e.get('route') == 'GRAPHIC' for e in packet['evidence'][side]):
                sufficiency['reasons'].append(side + '_CLAIM_GRAPHIC_WITNESS_REQUIRED')
        sufficiency['sufficient'] = not sufficiency['reasons']
    admission = admit(raw, transition, comparison, sufficiency, valid_witnesses, issues)
    return dict(schema=VERSION, status='REVIEW' if issues else 'NORMALIZED', issues=issues, warnings=warnings,
        claim_applicability=applicability,
        transformations=audit, old_state=old.to_dict(), new_state=new.to_dict(),
        source_conflict=conflict_state, negative_state=negative, sufficiency=sufficiency,
        claim_evidence_bindings=bindings['claims'], rejected_evidence_bindings=bindings['rejected'],
        f2=dict(comparability=comparison, exists_change=admission['exists_change'], materiality=admission['materiality']),
        exists_change=admission['exists_change']['status'], effective_verdict=admission['effective_verdict'],
        admission_order=['NORMALIZATION', 'CLAIM_TYPE', 'APPLICABLE_CONDITIONS', 'COMPARABILITY',
                         'COUNTER_EVIDENCE', 'EXISTS_CHANGE', 'MATERIALITY', 'FINAL_ADMISSION'],
        raw_verdict=raw.get('verdict'), raw_response_hash=fingerprint(raw),
        evidence_support_is_model_observation=True)
