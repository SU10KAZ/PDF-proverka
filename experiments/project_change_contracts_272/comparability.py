"""Claim-specific comparability and materiality; no assembly/admission rewrite."""
from dataclasses import fields
from decimal import Decimal
import re

from .evidence import comparison_readiness, fingerprint
from .states import EngineeringState, StateRole, Cardinality


IDENTITY = {'engineering_subject', 'scope', 'state_role', 'stage_phase', 'functional_role'}
QUANTITY = {'physical_quantity', 'unit', 'local_or_global', 'component_or_total'}
AGGREGATES = {StateRole.CALCULATED_RESULT, StateRole.CAPACITY, StateRole.COUNT}
INTRINSIC = {StateRole.INPUT_CRITERION, StateRole.REQUIREMENT, StateRole.TOPOLOGY,
             StateRole.INSTALLED_CONFIGURATION, StateRole.SELECTED_EQUIPMENT,
             StateRole.ROUTING, StateRole.OPERATING_MODE}
DIMENSIONS = [f.name for f in fields(EngineeringState) if f.name not in
              {'value', 'provenance', 'evidence_ids', 'members', 'comparison_cardinality'}]


def known(value):
    if isinstance(value, (list, tuple)):
        return bool(value) and all(known(item) for item in value)
    if isinstance(value, str):
        return value.strip().upper() not in {'', 'UNKNOWN', 'UNRESOLVED', 'NOT_PROVEN'}
    return value is not None


def applicable_dimensions(transition):
    """Union both profiles so a changed role cannot hide a required condition."""
    applicable = set(IDENTITY)
    for state in (transition.old, transition.new):
        role = state.state_role
        if role in AGGREGATES or role in {StateRole.INPUT_CRITERION, StateRole.REQUIREMENT}:
            applicable |= QUANTITY
        if role in AGGREGATES or state.component_or_total == 'TOTAL':
            applicable |= {'consumer_composition', 'operating_mode'}
        if role == StateRole.CALCULATED_RESULT:
            applicable.add('calculation_basis')
        if role == StateRole.SELECTED_EQUIPMENT:
            applicable |= {'local_or_global', 'component_or_total'}
        # Branch IDs may change as part of a documented parent transformation.
        # They must match for direct numeric/equipment comparisons of one branch.
        if state.comparison_cardinality == Cardinality.ONE_TO_ONE and role in AGGREGATES | {StateRole.SELECTED_EQUIPMENT}:
            applicable.add('branch_identity')
    return applicable


def compare(transition):
    old, new = transition.old, transition.new
    applicable = applicable_dimensions(transition)
    conditions, different, unknown = [], [], []
    for dimension in DIMENSIONS:
        a, b = getattr(old, dimension), getattr(new, dimension)
        relevant = dimension in applicable
        comparison = 'NOT_APPLICABLE'
        if relevant:
            if not known(a) or not known(b):
                comparison = 'UNKNOWN'; unknown.append(dimension)
            else:
                # Consumer composition is a membership set, not display order.
                equal = set(a) == set(b) if dimension == 'consumer_composition' else a == b
                comparison = 'SAME' if equal else 'DIFFERENT'
                if not equal:
                    different.append(dimension)
        conditions.append(dict(dimension=dimension, applicability='APPLICABLE' if relevant else 'NOT_APPLICABLE',
            old=a, new=b, comparison=comparison,
            reason=('Identity or role-specific comparison condition' if relevant else
                    'Not required to compare this state role at the declared scope')))
    mapping_errors = transition.cardinality_errors()
    if not transition.identity_basis.strip():
        unknown.append('identity_basis')
    if old.state_role == StateRole.OTHER or new.state_role == StateRole.OTHER:
        unknown.append('state_role_applicability_profile')
    if transition.source_conflict:
        unknown.append('same_version_source_conflict')
    status = 'INCOMPARABLE' if different or mapping_errors else 'REVIEW' if unknown else 'COMPARABLE'
    return dict(status=status, conditions=conditions, different=different, unknown=unknown,
                mapping_errors=mapping_errors, cardinality=old.comparison_cardinality.value,
                comparison_level='PARENT_SUBJECT' if old.comparison_cardinality != '1→1' else 'SUBJECT',
                identity_basis=transition.identity_basis, mapping_basis=transition.mapping_basis)


def materiality(transition, comparison=None):
    comparison = comparison or compare(transition)
    if comparison['status'] != 'COMPARABLE':
        return dict(status='REVIEW', rule='Comparable typed states required')
    if transition.source_conflict:
        return dict(status='REVIEW', rule='Resolve same-version conflict without voting')
    if transition.state_support != 'SOURCE_SUPPORTED' or any(
            not s.evidence_ids or not s.provenance for s in (transition.old, transition.new)):
        return dict(status='REVIEW', rule='Source support for both typed states required')
    if same_value(transition.old.value, transition.new.value):
        return dict(status='NOT_MATERIAL', rule='No changed state')
    role = transition.old.state_role
    if role in INTRINSIC:
        return dict(status='MATERIAL', rule='Changed source-supported ' + role.value,
                    downstream_equipment_required=False)
    if role in AGGREGATES and transition.design_use.strip():
        return dict(status='MATERIAL', rule='Comparable result with explicit design use',
                    design_use=transition.design_use, downstream_equipment_required=False)
    return dict(status='REVIEW', rule='Numeric difference alone does not establish design significance')


def same_value(old, new):
    """Do not turn formatting of a scalar into an engineering change."""
    a, b = (' '.join(s.split()) for s in (old, new))
    scalar = r'[+-]?\d+(?:[.,]\d+)?'
    if re.fullmatch(scalar, a) and re.fullmatch(scalar, b):
        return Decimal(a.replace(',', '.')) == Decimal(b.replace(',', '.'))
    return a == b


def evaluate_typed_claim(packet, transition):
    """Diagnostic interface for future packages; never merges or admits events.

    Comparability can be evaluated against source truth separately from package
    completeness. A future request's readiness additionally requires delivered
    OLD/NEW evidence and side-correct, subject-bound state witness references.
    """
    comparison = compare(transition)
    significance = materiality(transition, comparison)
    coverage = comparison_readiness(packet['evidence_coverage'], transition.old.engineering_subject)
    errors = list(coverage['reasons'])
    if packet['evidence_coverage'].get('delivery_hash') != fingerprint(packet['evidence']):
        errors.append('EVIDENCE_COVERAGE_DRIFT')
    for side, state in [('OLD', transition.old), ('NEW', transition.new)]:
        delivered = {e['evidence_id'] for e in packet['evidence']
                     if e['side'] == side and e['subject'] == state.engineering_subject}
        if not state.evidence_ids or not set(state.evidence_ids) <= delivered:
            errors.append(side + '_STATE_WITNESS_NOT_DELIVERED')
    if comparison['status'] != 'COMPARABLE':
        errors.append('TYPED_STATES_NOT_COMPARABLE')
    if significance['status'] != 'MATERIAL':
        errors.append('MATERIALITY_NOT_ESTABLISHED')
    return dict(comparability=comparison, materiality=significance,
                ready_for_semantic_review=not errors, reasons=errors,
                engineering_event_proven=False, admission_or_grouping_changed=False)
