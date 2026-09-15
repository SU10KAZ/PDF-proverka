"""F5-only evidence boundary contract. No F1/F2/F4/V4 semantics are changed.

An observation describes source-grounded parts and delivery, not an engineering
verdict. Unknown extent/relevance is fail-closed. Complete means all mandatory
parts of this subject's bounded evidence, never all pages of a PDF.
"""
from dataclasses import asdict, dataclass

from .common import fingerprint

STATUSES = {'BOUNDED_COMPLETE', 'PARTIAL', 'UNKNOWN_BOUNDARY', 'CONTINUATION_MISSING',
            'FOOTNOTE_MISSING', 'WRONG_BOUNDARY', 'UNAVAILABLE_BY_POLICY'}
GRAPHICS = {'GRAPHIC_REGION', 'GRAPHIC_SCHEME'}
TABLES = {'TABLE_COMPLETE', 'EQUIPMENT_SCHEDULE'}


@dataclass(frozen=True)
class EvidenceBoundaryCertificate:
    requirement_id: str
    evidence_type: str
    mandatory: bool
    status: str
    start: dict | None
    end: dict | None
    continuation: dict
    notes: list
    evidence_ids: list
    delivered: bool
    usable: bool
    correct_type: bool
    reasons: list
    proof: dict
    provenance: dict

    def to_dict(self):
        body = dict(schema='EvidenceBoundaryCertificate/1', **asdict(self))
        body['certificate_id'] = 'bc_' + fingerprint(body)[:24]
        return body


def part_complete(part):
    return bool(part.get('source_locator') and part.get('source_hash') and
                part.get('delivered') is True and part.get('evidence_ids') and
                part.get('native_verified') is True)


def decide(kind, observation):
    """Recompute status from proof; never consume a caller's COMPLETE flag."""
    o, failures = observation, []
    if o.get('unlocated'):
        return 'UNKNOWN_BOUNDARY', ['SOURCE_SUBJECT_UNLOCATED']
    if o.get('policy_blocked'):
        return 'UNAVAILABLE_BY_POLICY', ['SOURCE_POLICY_BLOCKED']
    if o.get('wrong_subject') or o.get('wrong_boundary'):
        return 'WRONG_BOUNDARY', ['SUBJECT_OR_BOUNDARY_MISMATCH']
    if not o.get('correct_type'):
        return 'WRONG_BOUNDARY', ['WRONG_EVIDENCE_TYPE']
    if not o.get('delivered'):
        failures.append('REQUIRED_PAYLOAD_NOT_DELIVERED')
    if not o.get('usable'):
        failures.append('REQUIRED_PAYLOAD_NOT_USABLE')
    if not o.get('subject_found'):
        failures.append('SUBJECT_NOT_GROUNDED_INSIDE_BOUNDARY')
    if not o.get('start') or not o.get('end'):
        failures.append('SEMANTIC_EXTENT_UNPROVEN')
    if o.get('truncated'):
        failures.append('TRUNCATED_CONTENT')
    continuation = o.get('continuation', {})
    if continuation.get('status') == 'MISSING':
        failures.append('CONTINUATION_NOT_DELIVERED')
    elif continuation.get('status') not in {'CHECKED_NONE', 'COMPLETE', 'NOT_APPLICABLE'} or not continuation.get('proof'):
        failures.append('CONTINUATION_NOT_CHECKED')
    elif continuation['status'] == 'COMPLETE' and (
            not continuation.get('parts') or not all(part_complete(p) for p in continuation['parts'])):
        failures.append('CONTINUATION_NOT_DELIVERED')
    notes = o.get('notes', [])
    if not o.get('notes_examined'):
        failures.append('NOTE_RELEVANCE_UNPROVEN')
    for note in notes:
        if not note.get('reason') or note.get('relevance') not in {'RELEVANT', 'NOT_APPLICABLE'}:
            failures.append('NOTE_RELEVANCE_UNPROVEN')
        elif note['relevance'] == 'RELEVANT' and not part_complete(note):
            failures.append('MANDATORY_NOTE_NOT_DELIVERED')
    parts = o.get('parts', [])
    if not parts or not all(part_complete(p) for p in parts):
        failures.append('REQUIRED_PARTS_NOT_DELIVERED_OR_VERIFIED')
    roles = {p.get('role') for p in parts}
    if kind in TABLES:
        for role in ('identity', 'columns', 'row'):
            if role not in roles:
                failures.append('TABLE_' + role.upper() + '_MISSING')
        expected, actual = set(o.get('expected_row_ids', [])), {p.get('row_id') for p in parts if p.get('role') == 'row'}
        if not expected or actual != expected:
            failures.append('RELEVANT_ROW_GROUP_INCOMPLETE')
        if not o.get('row_group_end_proven'):
            failures.append('ROW_GROUP_END_UNPROVEN')
        if o.get('broken_rows'):
            failures.append('BROKEN_TABLE_ROW')
    elif kind in GRAPHICS:
        g = o.get('topology', {})
        nodes, edges = set(g.get('nodes', [])), g.get('edges', [])
        reached = {g.get('subject_node')} & nodes
        while True:
            expanded = reached | {v for edge in edges if len(edge) == 2 and set(edge) & reached for v in edge}
            if expanded == reached:
                break
            reached = expanded
        if not edges or not g.get('required_nodes') or not set(g['required_nodes']) <= reached or not reached <= nodes:
            failures.append('FUNCTIONAL_TOPOLOGY_UNPROVEN')
        if not g.get('labels_bound') or not g.get('source_geometry_hash'):
            failures.append('LABEL_NODE_BINDING_UNPROVEN')
        if g.get('cropped_connections'):
            failures.append('CONNECTION_CROPPED')
        if not g.get('boundary_checked'):
            failures.append('FUNCTIONAL_FRAGMENT_END_UNPROVEN')
        if 'raster' not in roles:
            failures.append('GRAPHIC_RASTER_MISSING')
    else:
        required_roles = {'note'} if kind == 'NOTE' else {'heading', 'paragraph'}
        if not required_roles <= roles:
            failures.append('HEADING_ONLY_OR_SECTION_BODY_MISSING')
        if not o.get('claim_inside'):
            failures.append('SUBJECT_CONTENT_OUTSIDE_SECTION')
    failures = sorted(set(failures))
    if not failures:
        return 'BOUNDED_COMPLETE', []
    if 'CONTINUATION_NOT_DELIVERED' in failures:
        status = 'CONTINUATION_MISSING'
    elif 'MANDATORY_NOTE_NOT_DELIVERED' in failures:
        status = 'FOOTNOTE_MISSING'
    elif any(x in failures for x in ('SEMANTIC_EXTENT_UNPROVEN', 'CONTINUATION_NOT_CHECKED',
            'NOTE_RELEVANCE_UNPROVEN', 'ROW_GROUP_END_UNPROVEN', 'FUNCTIONAL_TOPOLOGY_UNPROVEN',
            'FUNCTIONAL_FRAGMENT_END_UNPROVEN', 'SUBJECT_NOT_GROUNDED_INSIDE_BOUNDARY')):
        status = 'UNKNOWN_BOUNDARY'
    else:
        status = 'PARTIAL'
    return status, failures


def certify_requirement(requirement, mandatory, observation, provenance):
    status, reasons = decide(requirement['required_type'], observation)
    return EvidenceBoundaryCertificate(requirement_id=requirement['requirement_id'],
        evidence_type=requirement['required_type'], mandatory=mandatory, status=status,
        start=observation.get('start'), end=observation.get('end'),
        continuation=observation.get('continuation', {}), notes=observation.get('notes', []),
        evidence_ids=sorted(observation.get('evidence_ids', [])),
        delivered=bool(observation.get('delivered')), usable=bool(observation.get('usable')),
        correct_type=bool(observation.get('correct_type')), reasons=reasons,
        proof=observation, provenance=provenance).to_dict()


def package_completeness(certificates):
    mandatory = [c for c in certificates if c['mandatory']]
    failed = [c for c in mandatory if c['status'] != 'BOUNDED_COMPLETE' or
              not all(c[k] for k in ('delivered', 'usable', 'correct_type'))]
    status = ('COMPLETE' if mandatory and not failed else
              'MISSING' if mandatory and all(not c['delivered'] for c in mandatory) else 'PARTIAL')
    return dict(completeness=status, mandatory_requirements=[c['requirement_id'] for c in mandatory],
        mandatory_certificates=[c['certificate_id'] for c in mandatory],
        blockers=[dict(requirement_id=c['requirement_id'], certificate_id=c['certificate_id'],
                       status=c['status'], reasons=c['reasons']) for c in failed],
        missing_supporting=[dict(requirement_id=c['requirement_id'], certificate_id=c['certificate_id'],
            status=c['status'], reason='F1_ALLOCATION_OPTIONAL; no independently relevant dependency may be omitted')
            for c in certificates if not c['mandatory'] and c['status'] != 'BOUNDED_COMPLETE'],
        meaning='Delivery completeness for mandatory subject evidence; no engineering verdict')
