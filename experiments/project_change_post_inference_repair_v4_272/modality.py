"""Modality requirements are projections of existing bindings, never new bindings.

A nonvisual state citation may support identity/scope only when V1 already
bound that role. Otherwise an unclassified geometric state citation remains
dispositive and fails closed. Graphic intent survives missing/invalid delivery.
"""
from dataclasses import asdict, dataclass

from experiments.project_change_contracts_272.witnesses import raster_locator_errors


ROLES = ('PRIMARY_STATE_EVIDENCE', 'SUBJECT_IDENTITY', 'SCOPE_BINDING',
         'SUPPORTING_WITNESS', 'COUNTER_EVIDENCE', 'CALCULATION_SUPPORT')
RULES = {
    'primary': 'Geometric state requires nonempty OLD and NEW claim-bound primary graphics. Every designated primary must pass unchanged raster_locator_errors.',
    'support': 'Nonvisual state citation with an existing validated identity/scope binding supports that role; it is not geometric dispositive evidence.',
    'unknown': 'Nonvisual geometric state citation without an explicit bound supporting role remains primary and requires GRAPHIC; no silent demotion.',
    'intent': 'GRAPHIC source route or explicit RASTER_LOCATOR preserves primary graphic intent even when raster delivery is missing or invalid.',
    'identity_scope': 'TEXT/TABLE/GRAPHIC permitted for already proved identity and scope. Modality approval does not establish identity or geometry.',
    'witness': 'Identity, scope and numeric identity allow text/table. Geometry, visual endpoint, position, shape and route require graphic.',
    'negative': 'Same role rules for local NOT_CHANGE; numeric/table primary does not require raster. Negative scope unchanged.',
    'binding': 'No binding, value, subject, calculation rule or numeric conflict decision is changed.',
}
VISUAL_PURPOSES = {'GEOMETRY', 'POSITION', 'SHAPE', 'ROUTE', 'VISUAL_ENDPOINT'}


@dataclass(frozen=True)
class EvidenceRoleRequirement:
    evidence_id: str
    claim_id: str
    evidence_role: str
    source_type: str
    required_modality: str
    actual_modality: str
    requirement_status: str
    requirement_reason: str
    side: str
    citation_kind: str
    witness_required_for: str
    witness_required_modality: str
    original_binding_role: str
    provenance: dict


def required_modality(role, purpose, spatial=False):
    if (role == 'PRIMARY_STATE_EVIDENCE' and spatial) or purpose in VISUAL_PURPOSES:
        return 'GRAPHIC'
    return 'TEXT_ALLOWED'


def requirements(raw, packet, states, spatial, arithmetic=False):
    result = []
    for side, state in states.items():
        by_id = {e['evidence_id']: e for e in packet['evidence'][side]}
        roles = {}
        for b in state.evidence_bindings:
            if b.witness_validated:
                roles.setdefault(b.evidence_id, set()).add(b.role.value)
        for b in state.evidence_bindings:
            e = by_id[b.evidence_id]
            ws = [w for w in raw.get('witnesses', [])
                  if w.get('side') == side and w.get('evidence_id') == b.evidence_id]
            graphic_intent = e.get('route') == 'GRAPHIC' or any(w.get('kind') == 'RASTER_LOCATOR' for w in ws)
            raster_valid = b.witness_validated and any(w.get('kind') == 'RASTER_LOCATOR'
                and not raster_locator_errors(w, e) for w in ws)
            actual = 'GRAPHIC' if raster_valid else e.get('route', 'UNKNOWN')
            if actual == 'UNKNOWN' and b.witness_validated and any(w.get('kind') == 'TEXT_LITERAL' for w in ws):
                actual = 'TEXT'
            if actual == 'GRAPHIC' and not raster_valid:
                actual = 'GRAPHIC_UNVALIDATED'
            role = b.role.value
            purpose = {'SUBJECT_IDENTITY': 'IDENTITY', 'SCOPE_BINDING': 'SCOPE',
                'COUNTER_EVIDENCE': 'COUNTER_ASSERTION', 'CONDITION_SUPPORT': 'CONDITION'}.get(role, 'STATE_VALUE')
            citation = 'claim_supporting_citation'
            reason = 'Existing V1 binding proves only this evidence role; claim modality does not propagate.'
            if role == 'STATE_VALUE':
                supporting = roles.get(b.evidence_id, set()) & {'SUBJECT_IDENTITY', 'SCOPE_BINDING'}
                if spatial and not graphic_intent and supporting:
                    role = 'SUPPORTING_WITNESS'
                    purpose = 'IDENTITY' if 'SUBJECT_IDENTITY' in supporting else 'SCOPE'
                    reason = 'Nonvisual citation has an explicit validated V1 ' + purpose + ' binding; separate primary graphics must prove geometry.'
                else:
                    role = 'PRIMARY_STATE_EVIDENCE'
                    purpose = 'GEOMETRY' if spatial else 'EXPLICIT_STATE'
                    citation = 'claim_dispositive_evidence'
                    reason = 'Explicit same-subject state binding; geometric primary cannot be replaced by a supporting citation.'
            elif role == 'CONDITION_SUPPORT':
                role = 'SUPPORTING_WITNESS'
            # A raster counter locator proves a visual counter assertion, not
            # necessarily the state's primary geometric assertion.
            if role == 'COUNTER_EVIDENCE' and graphic_intent:
                purpose = 'GEOMETRY'
            needed = required_modality(role, purpose, spatial)
            passed = bool(b.witness_validated and (raster_valid if needed == 'GRAPHIC'
                else actual in {'TEXT', 'TABLE', 'GRAPHIC'}))
            row = EvidenceRoleRequirement(b.evidence_id, b.claim_id, role, e.get('route', 'UNKNOWN'),
                needed, actual, 'SATISFIED' if passed else 'MISSING_OR_INVALID', reason, side,
                citation, purpose, needed, b.role.value, b.provenance)
            result.append(asdict(row))
            if arithmetic and role == 'PRIMARY_STATE_EVIDENCE':
                result.append(asdict(row) | dict(evidence_role='CALCULATION_SUPPORT',
                    citation_kind='claim_supporting_citation', required_modality='TEXT_ALLOWED',
                    witness_required_for='EXPLICIT_COMPONENT_ARITHMETIC', witness_required_modality='TEXT_ALLOWED',
                    requirement_reason='Unchanged V3 explicit component arithmetic supplies its own support.'))
    return result


def graphic_delivery(raw, packet, states):
    rows = requirements(raw, packet, states, spatial=True)
    result = {}
    for side in ('old', 'new'):
        primary = [r for r in rows if r['side'] == side and r['evidence_role'] == 'PRIMARY_STATE_EVIDENCE']
        required = {r['evidence_id'] for r in primary}
        delivered = {r['evidence_id'] for r in primary if r['requirement_status'] == 'SATISFIED'}
        result[side] = dict(required_ids=sorted(required), delivered_ids=sorted(delivered),
            complete=bool(required) and required <= delivered,
            supporting_ids=sorted({r['evidence_id'] for r in rows if r['side'] == side
                and r['citation_kind'] == 'claim_supporting_citation'} - required))
    return result
