"""Claim-local applicability from response fields and already bound primary proof.

No case IDs, truth labels, new evidence or identity rules are used here.
A witness locator attached to PRIMARY_EVIDENCE is not an independent
SUPPORTING_WITNESS. Binding V1 continues to require the former.
"""
from dataclasses import replace
import re
from experiments.project_change_post_inference_repair_272.identity import meaningful
from experiments.project_change_post_inference_repair_272.applicability import classify
from experiments.project_change_contracts_272.states import StateRole
from experiments.project_change_contracts_272.witnesses import raster_locator_errors

RULES = {
    'primary': 'V1 subject/value/scope/provenance binding stays mandatory and unchanged.',
    'supporting_witness': 'Not required for fully bound direct source-native states. Required for derived, ambiguous, implicit or conflicted claims; bound component witnesses may support explicit arithmetic.',
    'graphic': 'Spatial geometry, layout, routing, opening, hatch, element position and geometric dimensions require both claim-bound raster sides. TABLE/TEXT values do not require an additional drawing.',
    'calculation_basis': 'Required for derived sums, ratios, mixed categories and component inclusion rules; not for direct same-metric source-native states.',
    'consumer_composition': 'Not applicable only for direct scalar or individually keyed rows with identical keys. Aggregate membership remains mandatory.',
    'OTHER': 'Map only explicit quantity/unit or room-function response fields to an existing typed role. Ambiguity stays REVIEW. Original role retained.',
    'numeric': 'Decimal exact comparison; no global epsilon; printed totals and derived sums remain separate facts.',
}

ROWS = re.compile(r'(?<![\w.])(\d+(?:\.\d+)+)\s*[—–:]\s*([+-]?\d+(?:[.,]\d+)?)')
AGGREGATE = re.compile(r'итог|сумм|агрегат|коэффициент|отношени|total|sum|ratio|aggregate|derived|вычислен', re.I)
GEOMETRY = re.compile(r'планиров|перегород|границ|проход|про[её]м|люк|маршрут|располож|размещ|конфигурац|геометри|layout|wall|opening|hatch|routing|position|geometry', re.I)
UNKNOWN = re.compile(r'не\s+(?:указан|извест|установлен|определен|раскрыт)|unknown|unresolved|not.proven', re.I)


def component_rows(value):
    numbered = ROWS.findall(value)
    if numbered:
        return numbered
    named = []
    for part in re.split(r'[;\n]', value):
        m = re.match(r'\s*([^—–:]+?)\s*[—–:]\s*([+-]?\d+(?:[.,]\d+)?)(?=[.;\s]|$)', part)
        if m and not AGGREGATE.search(m[1]):
            named.append((m[1].strip().casefold(), m[2]))
    return named


def present(value):
    return meaningful(value) and not UNKNOWN.search(value)


def role_mapping(raw):
    """Normalize semantic claim type separately from the existing StateRole enum."""
    states = [raw[s + '_state'] for s in ('old', 'new')]
    original = [s['state_role'] for s in states]
    result = dict(original_role=original, normalized_role=original, normalized_state_role=original,
                  normalization_reason='Existing supported roles retained.', ambiguous=False)
    if 'OTHER' not in original:
        return result
    kind = classify(raw)
    quantities = [s.get('physical_quantity') or '' for s in states]
    if all(re.search(r'количеств|count', q, re.I) for q in quantities):
        kind = 'COUNT_CHANGE'
    roles = {'AREA_CHANGE': 'CALCULATED_RESULT', 'DIMENSION_CHANGE': 'CALCULATED_RESULT',
             'ROOM_FUNCTION_CHANGE': 'TOPOLOGY', 'ELEVATION_CHANGE': 'CALCULATED_RESULT',
             'COUNT_CHANGE': 'COUNT'}
    # OTHER presence/configuration prose with no explicit typed metric is ambiguous.
    valid = kind in roles and all(s.get('support') == 'PROVEN' for s in states)
    if kind == 'ROOM_FUNCTION_CHANGE':
        valid = valid and all(re.search(r'назначени|function', s['engineering_subject'], re.I) for s in states)
    else:
        valid = valid and all(present(s.get('unit')) for s in states)
    if not valid:
        return result | dict(ambiguous=True, normalization_reason='Response fields do not uniquely define a supported claim type.')
    return result | dict(normalized_role=[kind, kind], normalized_state_role=[roles[kind]] * 2,
                         normalization_reason='Explicit response quantity/unit or room-function fields uniquely identify ' + kind)


def primary_complete(raw, states):
    return all(
        bool(raw[s + '_state'].get('evidence_ids'))
        and set(raw[s + '_state']['evidence_ids']) <= set(states[s].state_value_evidence_ids)
        and bool(states[s].subject_identity_evidence_ids)
        and all(b.witness_validated for b in states[s].evidence_bindings
                if b.role in {'STATE_VALUE', 'SUBJECT_IDENTITY', 'SCOPE_BINDING'})
        and raw[s + '_state'].get('support') == 'PROVEN'
        for s in ('old', 'new'))


def graphic_delivery(raw, packet, states):
    result = {}
    for side in ('old', 'new'):
        bound = set(states[side].state_value_evidence_ids)
        valid = []
        for e in packet['evidence'][side]:
            if e['evidence_id'] not in bound:
                continue
            for w in raw.get('witnesses', []):
                if w.get('side') == side and w.get('kind') == 'RASTER_LOCATOR' and w.get('evidence_id') == e['evidence_id'] and not raster_locator_errors(w, e):
                    valid.append(e['evidence_id'])
        # Every primary graphic reference must actually be delivered; an unrelated
        # image on that side cannot compensate for an absent cited counterpart.
        required = set(raw[side + '_state'].get('evidence_ids', []))
        result[side] = dict(required_ids=sorted(required), delivered_ids=sorted(set(valid)),
                            complete=bool(required) and required <= set(valid))
    return result


def evaluate(raw, packet, states, v1, source_conflict):
    mapping = role_mapping(raw)
    states = {s: replace(states[s], state_role=StateRole(mapping['normalized_state_role'][i]))
              for i, s in enumerate(('old', 'new'))}
    kind = mapping['normalized_role'][0] if mapping['normalized_role'][0].endswith('_CHANGE') else classify(raw)
    source = [raw[s + '_state'] for s in ('old', 'new')]
    primary = primary_complete(raw, states) and v1.get('identity_proven', False)
    values = ' '.join(s['value'] for s in source)
    dependency_text = ' '.join(s['value'] + ' ' + (s.get('component_or_total') or '') + ' ' + (s.get('calculation_basis') or '') for s in source)
    aggregate = bool(AGGREGATE.search(dependency_text))
    # A scope may locate a room on a plan without making the *claim* geometric.
    spatial = any(s.get('evidence_form') == 'GRAPHIC_STATE' for s in source) or (
        kind not in {'AREA_CHANGE', 'ROOM_FUNCTION_CHANGE', 'COUNT_CHANGE', 'ELEVATION_CHANGE'}
        and bool(GEOMETRY.search(values + ' ' + raw.get('project_change_summary', ''))))
    if any(s['state_role'] == 'ROUTING' for s in source):
        spatial = True
    if kind == 'DIMENSION_CHANGE' and any(s['state_role'] == 'TOPOLOGY' for s in source):
        spatial = True
    direct_form = all(s.get('evidence_form') in {'TABLE_RESULT', 'DECLARATION', 'TEXT_LITERAL'} for s in source)
    same_metric = all(source[0].get(k) == source[1].get(k) for k in ('physical_quantity', 'unit'))
    row_keys = [{m[0] for m in ROWS.findall(s['value'])} for s in source]
    keyed = bool(row_keys[0]) and row_keys[0] == row_keys[1]
    scalar = all(re.fullmatch(r'[+-]?\d+(?:[.,]\d+)?', s['value'].strip()) for s in source)
    direct = primary and same_metric and not aggregate and (direct_form or kind == 'DIMENSION_CHANGE') and (
        keyed or scalar or kind in {'ROOM_FUNCTION_CHANGE', 'ELEVATION_CHANGE'})
    basis_required = aggregate or not direct
    # Non-numeric topology/function claims do not need an arithmetic basis.
    if kind in {'ROOM_FUNCTION_CHANGE', 'LAYOUT_CHANGE'} and not aggregate:
        basis_required = False
    basis_known = all(present(s.get('calculation_basis')) for s in source)
    basis = 'NOT_APPLICABLE' if not basis_required else 'APPLICABLE' if basis_known else 'UNKNOWN_BUT_REQUIRED'
    ambiguous = bool(re.search(r'неоднознач|неявн|ambiguous|implicit', values, re.I))
    support_required = aggregate or ambiguous or spatial or source_conflict['blocking'] or not primary
    # Existing primary row locators + explicit basis ground deterministic arithmetic;
    # a second document is never imposed solely because it would be reassuring.
    support_satisfied = primary and ((aggregate and basis_known and all(len(component_rows(s['value'])) >= 2 for s in source)) or (
        spatial and all(sum(w.get('side') == side for w in raw.get('witnesses', [])) >= 2 for side in ('old', 'new'))))
    if source_conflict['blocking'] or ambiguous:
        support_satisfied = False
    witness = 'NOT_APPLICABLE' if not support_required else 'APPLICABLE' if support_satisfied else 'UNKNOWN_BUT_REQUIRED'
    delivery = graphic_delivery(raw, packet, states) if spatial else {}
    not_applicable = list(v1.get('not_applicable', []))
    if basis == 'NOT_APPLICABLE':
        not_applicable.append('calculation_basis')
    if direct and (keyed or scalar):
        not_applicable.append('consumer_composition')
    issues = []
    if mapping['ambiguous']: issues.append('AMBIGUOUS_OTHER_ROLE')
    if basis == 'UNKNOWN_BUT_REQUIRED': issues.append('CALCULATION_BASIS_REQUIRED_UNKNOWN')
    if witness == 'UNKNOWN_BUT_REQUIRED': issues.append('SUPPORTING_WITNESS_REQUIRED')
    if spatial and not all(d['complete'] for d in delivery.values()): issues.append('GRAPHIC_REQUIRED_NOT_DELIVERED')
    return dict(claim_type=kind, not_applicable=sorted(set(not_applicable)), identity_proven=v1.get('identity_proven', False),
                primary_evidence=dict(role='PRIMARY_EVIDENCE', complete=primary),
                supporting_witness=dict(role='SUPPORTING_WITNESS', applicability=witness, required=support_required,
                    satisfied=support_satisfied, reason='Direct fully bound primary states suffice.' if not support_required else 'Derived/ambiguous/spatial/conflicted claim requires supporting proof.'),
                graphic=dict(requirement='GRAPHIC_REQUIRED' if spatial else 'GRAPHIC_NOT_REQUIRED',
                    applicability='APPLICABLE' if spatial and all(d['complete'] for d in delivery.values()) else 'UNKNOWN_BUT_REQUIRED' if spatial else 'NOT_APPLICABLE', sides=delivery),
                calculation_basis=dict(applicability=basis, derived_or_mixed=aggregate, direct_source_native=direct,
                    reason='Arithmetic/inclusion basis affects this claim.' if basis_required else 'No calculation dependency; matching metric and bound scope.'),
                other_role=mapping, issues=issues), states
