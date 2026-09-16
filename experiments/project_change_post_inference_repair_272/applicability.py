"""Conservative architectural condition applicability, independent of verdicts."""
import re
from .identity import clean, physical_identity, rooms

RULES = {
    'AREA_CHANGE': dict(not_applicable=['operating_mode'], required=['room_or_zone_identity']),
    'DIMENSION_CHANGE': dict(not_applicable=['operating_mode'], required=['same_geometric_element']),
    'ROOM_FUNCTION_CHANGE': dict(not_applicable=['functional_role'], required=['room_identity', 'physical_location']),
    'ELEVATION_CHANGE': dict(not_applicable=[], required=['same_reference_point', 'reference_level_system']),
    'LAYOUT_CHANGE': dict(not_applicable=[], required=['room_or_zone_correspondence']),
}


def classify(raw):
    a, b = raw['old_state'], raw['new_state']
    identities = [physical_identity(s) for s in (a, b)]
    architectural = all(i and re.search(r'архитект|architect', i['system']) for i in identities)
    if not architectural:
        return 'UNCLASSIFIED'
    if (a.get('functional_role') != b.get('functional_role') and
            all(re.search(r'помещени|room', s.get('engineering_subject', ''), re.I) for s in (a, b))):
        return 'ROOM_FUNCTION_CHANGE'
    quantities = [clean(s.get('physical_quantity') or '') for s in (a, b)]
    if all(re.search(r'площад|area', q) for q in quantities):
        return 'AREA_CHANGE'
    if all(re.search(r'отметк|elevation|уровень', q) for q in quantities):
        return 'ELEVATION_CHANGE'
    if all(re.search(r'размер|толщин|длин|ширин|dimension|length', q) for q in quantities):
        return 'DIMENSION_CHANGE'
    if all(s.get('state_role') == 'TOPOLOGY' for s in (a, b)):
        return 'LAYOUT_CHANGE'
    return 'UNCLASSIFIED'


def evaluate(raw, states):
    claim_type = classify(raw)
    result = dict(claim_type=claim_type, not_applicable=[], required=RULES.get(claim_type, {}).get('required', []),
                  identity_proven=False, issues=[])
    if claim_type == 'UNCLASSIFIED':
        return result
    a, b = raw['old_state'], raw['new_state']
    ia, ib = physical_identity(a), physical_identity(b)
    identity = (ia is not None and ia == ib and bool(raw.get('mapping_basis'))
        and all(s.subject_identity_evidence_ids for s in states.values())
        and bool(a.get('members')) and a.get('members') == b.get('members'))
    if claim_type == 'ROOM_FUNCTION_CHANGE':
        room = rooms(ia['scope']) if ia else set()
        # A room number alone is insufficient: physical scope must locate it,
        # and the mapping must explicitly describe position/geometry.
        identity = identity and bool(room) and bool(re.search(r'этаж|floor|location|оси', ia['scope']))
        identity = identity and bool(re.search(r'положени|располож|геометр|position|location|geometry', raw.get('mapping_basis', ''), re.I))
    if claim_type == 'ELEVATION_CHANGE':
        identity = identity and bool(ia.get('reference_system')) and bool(ia.get('geometric_element'))
    result['identity_proven'] = bool(identity)
    if not identity:
        result['issues'].append('CLAIM_PHYSICAL_IDENTITY_REQUIRED')
        return result
    result['not_applicable'] = RULES[claim_type]['not_applicable']
    return result
