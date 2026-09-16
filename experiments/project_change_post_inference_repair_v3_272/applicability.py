"""Claim dependencies only; no case IDs, truth, binding changes or new evidence."""
from dataclasses import replace
import re

from experiments.project_change_contracts_272.states import StateRole
from experiments.project_change_post_inference_repair_272.identity import physical_identity
from experiments.project_change_post_inference_repair_272.applicability import classify
from experiments.project_change_post_inference_repair_v2_272.applicability import (
    AGGREGATE, GEOMETRY, ROWS, component_rows, present, primary_complete,
    graphic_delivery, role_mapping,
)

RULES = {
    'primary': 'Unchanged V1 bindings must prove identity, both values, scope and provenance; F2 still checks comparability.',
    'witness': 'PRIMARY_SUFFICIENT / PRIMARY_NEEDS_WITNESS / WITNESS_REQUIRED_BY_CONFLICT. Every required witness has an explicit dependency reason. Source route alone is never a reason.',
    'direct_graphic': 'Claim-bound OLD and NEW raster locators may be sufficient primary proof; no second copy required.',
    'arithmetic': 'Explicit component rows with a known basis can supply their own supporting proof; mixed unknown basis stays REVIEW.',
    'optional_reference': 'A rejected auxiliary counter reference stays rejected; it cannot veto independently sufficient proof with satisfied claim dependencies and no source/numeric conflict.',
    'negative': 'Only the stated local negative assertion is assessed. No implication that its page, room or system is wholly unchanged.',
    'numeric': 'Unchanged V2 exact decimal NumericConflictGuard; no tolerance or correction.',
}
NEGATIVE_CONTRACT = {
    'schema': 'NEGATIVE_VERDICT_CONTRACT/3',
    'scope': 'Preserve OLD/NEW scopes, members and raw negative assertion; whole_page_unchanged=False.',
    'primary': 'Require unchanged V1 binding of OLD/NEW values and identity, explicit mapping and comparable states.',
    'graphic': 'Required for position, shape, wall/opening geometry, route, passage or a local visual element; not for direct table/text values.',
    'calculation_basis': 'Required only when the assertion depends on a derivation or aggregate, never solely because a support field is unknown.',
    'admission': 'Run the existing V4 existence admission after the separate negative sufficiency contract; missing required proof stays REVIEW.',
    'role': 'Ambiguous OTHER remains REVIEW; this repair does not add semantic role mappings.',
}
AMBIGUOUS = re.compile(r'неоднознач|неявн|ambiguous|implicit|unclear endpoint|неясн.*(?:границ|конец)|same label.*multiple', re.I)
EXCLUDED = re.compile(r'[^.!?;]*(?:не используется|не используются|не сравнивается|не сравниваются|исключен[а-яё]*|not used|excluded)[^.!?;]*[.!?;]?', re.I)


def negative_identity(raw, states):
    """Inspect identities already proved by V1; never create or relax a binding."""
    a, b = (raw[s + '_state'] for s in ('old', 'new'))
    ia, ib = physical_identity(a), physical_identity(b)
    return bool(ia and ia == ib and raw.get('identity_basis') and raw.get('mapping_basis')
                and a.get('members') and a['members'] == b.get('members')
                and states['old'].engineering_subject == states['new'].engineering_subject
                and states['old'].scope == states['new'].scope
                and all(set(raw[s+'_state']['subject_identity']['evidence_ids'])
                        <= set(states[s].subject_identity_evidence_ids) for s in states))


def evaluate(raw, packet, states, v1, source_conflict):
    mapping = role_mapping(raw)
    states = {s: replace(states[s], state_role=StateRole(mapping['normalized_state_role'][i]))
              for i, s in enumerate(('old', 'new'))}
    kind = mapping['normalized_role'][0] if mapping['normalized_role'][0].endswith('_CHANGE') else classify(raw)
    source = [raw[s + '_state'] for s in ('old', 'new')]
    negative = raw.get('verdict') == 'NOT_CHANGE'
    identity = v1.get('identity_proven', False) or (negative and negative_identity(raw, states))
    primary = primary_complete(raw, states) and identity
    values = ' '.join(s['value'] for s in source)
    # Excluded supporting calculations are not assertions. Positive value/summary
    # dependencies remain intact, including mixed direct + aggregate claims.
    dependencies = values + ' ' + raw.get('project_change_summary', '') + ' ' + ' '.join(EXCLUDED.sub('', (s.get('component_or_total') or '') + ';' +
                    (s.get('calculation_basis') or '')) for s in source)
    aggregate = bool(AGGREGATE.search(dependencies))
    spatial = any(s.get('evidence_form') == 'GRAPHIC_STATE' or s['state_role'] == 'ROUTING' for s in source)
    if not spatial and kind not in {'AREA_CHANGE', 'ROOM_FUNCTION_CHANGE', 'COUNT_CHANGE', 'ELEVATION_CHANGE', 'DIMENSION_CHANGE'}:
        # A declaration of layer thickness is not a wall-position assertion.
        spatial = bool(GEOMETRY.search(values + ' ' + raw.get('project_change_summary', '')))
    delivery = graphic_delivery(raw, packet, states) if spatial else {}
    graphics_complete = not spatial or all(d['complete'] for d in delivery.values())
    same_metric = all(source[0].get(k) == source[1].get(k) for k in ('physical_quantity', 'unit'))
    direct_form = all(s.get('evidence_form') in {'TABLE_RESULT', 'DECLARATION', 'TEXT_LITERAL', 'GRAPHIC_STATE'} for s in source)
    ambiguous = bool(AMBIGUOUS.search(values + ' ' + raw.get('mapping_basis', '')))
    direct = bool(primary and same_metric and direct_form and not aggregate and not ambiguous and graphics_complete)
    basis_known = all(present(s.get('calculation_basis')) for s in source)
    basis = 'NOT_APPLICABLE' if not aggregate else 'APPLICABLE' if basis_known else 'UNKNOWN_BUT_REQUIRED'
    reasons = []
    if source_conflict['blocking']: reasons.append('CONFLICTING_SOURCES')
    if ambiguous: reasons.append('AMBIGUOUS_SUBJECT_LOCATION_OR_ENDPOINT')
    if not primary: reasons.append('PRIMARY_IDENTITY_SCOPE_OR_PROVENANCE_INCOMPLETE')
    if aggregate: reasons.append('DERIVED_OR_AGGREGATE_VALUE' if basis_known else 'AGGREGATE_BASIS_UNKNOWN')
    if spatial and not graphics_complete: reasons.append('VISUAL_ENDPOINT_NOT_DELIVERED')
    if not direct_form: reasons.append('PRIMARY_REPRESENTATION_NOT_DIRECT')
    # Only bound primary component locators can satisfy arithmetic support.
    arithmetic = bool(primary and aggregate and basis_known
                      and all(len(component_rows(s['value'])) >= 2 for s in source))
    required = bool(reasons)
    satisfied = bool(required and arithmetic and reasons == ['DERIVED_OR_AGGREGATE_VALUE'])
    category = 'WITNESS_REQUIRED_BY_CONFLICT' if source_conflict['blocking'] else 'PRIMARY_NEEDS_WITNESS' if required else 'PRIMARY_SUFFICIENT'
    witness = 'NOT_REQUIRED_PRIMARY_SUFFICIENT' if not required else 'APPLICABLE' if satisfied else 'UNKNOWN_BUT_REQUIRED'
    not_applicable = list(v1.get('not_applicable', []))
    if basis == 'NOT_APPLICABLE': not_applicable.append('calculation_basis')
    row_keys = [{m[0] for m in ROWS.findall(s['value'])} for s in source]
    keyed = bool(row_keys[0]) and row_keys[0] == row_keys[1]
    scalar = all(re.fullmatch(r'[+-]?\d+(?:[.,]\d+)?', s['value'].strip()) for s in source)
    if direct and (keyed or scalar): not_applicable.append('consumer_composition')
    issues = []
    if mapping['ambiguous']: issues.append('AMBIGUOUS_OTHER_ROLE')
    if basis == 'UNKNOWN_BUT_REQUIRED': issues.append('CALCULATION_BASIS_REQUIRED_UNKNOWN')
    if witness == 'UNKNOWN_BUT_REQUIRED': issues.append('SUPPORTING_WITNESS_REQUIRED')
    if not graphics_complete: issues.append('GRAPHIC_REQUIRED_NOT_DELIVERED')
    scope = dict(assertion=raw.get('project_change_summary'), old_scope=source[0]['scope'],
                 new_scope=source[1]['scope'], members={s:raw[s+'_state'].get('members', []) for s in states},
                 whole_page_unchanged=False, extends_to_other_properties=False) if negative else None
    return dict(claim_type=kind, contract='NEGATIVE' if negative else 'POSITIVE', NEGATIVE_SCOPE=scope,
        not_applicable=sorted(set(not_applicable)), identity_proven=identity,
        primary_evidence=dict(role='PRIMARY_EVIDENCE', complete=bool(primary), direct=direct),
        supporting_witness=dict(role='SUPPORTING_WITNESS', applicability=witness, category=category,
            required=required, satisfied=satisfied, witness_required_reason=reasons,
            reason='; '.join(reasons) if required else 'Bound primary evidence independently proves the scoped claim.',
            support_evidence_ids={s:list(states[s].state_value_evidence_ids) for s in states} if satisfied else {}),
        graphic=dict(requirement='GRAPHIC_REQUIRED' if spatial else 'GRAPHIC_NOT_REQUIRED',
            applicability='APPLICABLE' if spatial and graphics_complete else 'UNKNOWN_BUT_REQUIRED' if spatial else 'NOT_APPLICABLE', sides=delivery),
        calculation_basis=dict(applicability=basis, derived_or_mixed=aggregate, direct_source_native=direct,
            reason='Claim contains a derivation/aggregate.' if aggregate else 'No asserted calculation dependency.'),
        other_role=mapping, issues=issues), states
