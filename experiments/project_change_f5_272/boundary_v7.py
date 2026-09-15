"""Claim-scoped boundary decisions, independent of retrieval and allocation.

Inputs are positive, provenance-bound observations. Missing verification is not
a claim of missing source content. Page presence is never a boundary proof.
"""
from dataclasses import asdict, dataclass

from .common import fingerprint

TEXT = {'TEXT_SECTION', 'NOTE'}
TABLE = {'TABLE_COMPLETE', 'EQUIPMENT_SCHEDULE'}
GRAPHIC = {'GRAPHIC_REGION', 'GRAPHIC_SCHEME'}


@dataclass(frozen=True)
class RelevantRowGroup:
    table_identity: dict
    columns: list
    expected_rows: list
    delivered_rows: list
    component_rows: list
    end: dict | None
    cell_mapping_verified: bool


@dataclass(frozen=True)
class LocalFunctionalFragment:
    mode: str
    subject: str
    nodes: list
    edges: list
    required_nodes: list
    labels: list
    geometry_verified: bool
    endpoints_visible: bool
    boundary_checked: bool
    off_page: bool


@dataclass(frozen=True)
class ClaimBoundaryCertificate:
    claim_id: str
    subject_id: list
    evidence_requirement_id: str
    evidence_type: str
    mandatory: bool
    supporting: bool
    boundary_start: dict | None
    boundary_end: dict | None
    continuation_status: dict
    note_status: dict
    connection_status: dict
    delivered_parts: list
    missing_parts: list
    completeness: str
    explanation: list
    provenance: dict
    proof: dict

    def to_dict(self):
        body = dict(schema='ClaimBoundaryCertificate/7', **asdict(self))
        return body | dict(certificate_id='cbc_' + fingerprint(body)[:24])


def part_status(part):
    if part.get('required') is False and part.get('not_required_reason'):
        return 'PART_NOT_REQUIRED_FOR_CLAIM'
    if not part.get('delivered') or not part.get('evidence_ids'):
        return 'PART_NOT_DELIVERED'
    if not (part.get('verified') and part.get('source_locator') and part.get('source_hash')):
        return 'PART_DELIVERED_NOT_VERIFIED'
    return 'PART_VERIFIED'


def continuation(kind, facts):
    """Applicability precedes delivery; unrelated next groups are not gaps."""
    keys = (('broken_sentence', 'broken_paragraph', 'explicit_continuation',
             'condition_continues', 'heading_continues', 'starts_mid_sentence') if kind in TEXT else
            ('split_row', 'same_subject_continues', 'same_row_group_continues',
             'relevant_footnote_external') if kind in TABLE else
            ('connection_off_page', 'relevant_legend_external', 'required_node_external'))
    signals = [key for key in keys if facts.get(key)]
    if kind in TABLE and facts.get('same_table_identity') and facts.get('repeated_header'):
        # Explicitly closed independent group wins over a repeated table header.
        independent = any(facts.get(k) for k in ('different_floor', 'different_building',
            'different_section', 'different_equipment', 'new_table', 'unrelated_row_group'))
        if not facts.get('row_group_closed') and not independent:
            signals.append('same_table_repeated_header')
    if signals:
        return dict(applicable='YES', decision='REQUIRED', reasons=signals,
                    verified=bool(facts.get('continuation_verified')),
                    parts=facts.get('continuation_parts', []))
    if facts.get('uncertain_dependency') and facts.get('uncertain_dependency_reason'):
        return dict(applicable='UNKNOWN_WITH_REASON', decision='UNKNOWN_WITH_REASON',
                    reasons=[facts['uncertain_dependency_reason']], verified=False, parts=[])
    return dict(applicable='NO', decision='NOT_REQUIRED',
                reasons=['No claim-specific continuation signal; extent is checked separately'],
                verified=True, parts=[])


def notes(items):
    decisions = []
    for item in items:
        signals = [k for k in ('marker', 'reference', 'proximity', 'legend_reference',
                              'continuation_note') if item.get(k)]
        if item.get('other_subject') and item.get('reason'):
            status = 'IRRELEVANT'
        elif item.get('same_subject') or item.get('changes_claim_meaning') or item.get('marker_bound'):
            status = 'RELEVANT'
        elif signals:
            status = 'UNKNOWN_WITH_REASON'
        else:
            status = 'NO_RELEVANT_NOTE'
        decisions.append(dict(status=status, signals=signals,
            reason=item.get('reason') if status in {'RELEVANT', 'IRRELEVANT'} else
                   'Potential claim note: ' + ', '.join(signals) if signals else
                   'No scoped marker, reference, proximity or explicit applicability',
            part=item.get('part')))
    status = ('UNKNOWN_WITH_REASON' if any(x['status'] == 'UNKNOWN_WITH_REASON' for x in decisions)
              else 'RELEVANT' if any(x['status'] == 'RELEVANT' for x in decisions)
              else 'IRRELEVANT' if any(x['status'] == 'IRRELEVANT' for x in decisions)
              else 'NO_RELEVANT_NOTE')
    return dict(status=status, decisions=decisions)


def connection(fragment):
    if fragment.get('mode') == 'ANNOTATED_POSITION':
        return dict(status='PART_NOT_REQUIRED_FOR_CLAIM',
                    reason='Frozen subject scope is local position/label and explicitly excludes topology')
    if fragment.get('off_page'):
        return dict(status='CONNECTION_CONTINUES_OFF_PAGE', reason='Required connection leaves delivered extent')
    if not fragment.get('edges'):
        return dict(status='CONNECTION_NOT_VISIBLE', reason='No verified edge between required nodes')
    nodes = set(fragment.get('nodes', []))
    reached = {fragment.get('subject')} & nodes
    while True:
        expanded = reached | {n for e in fragment['edges'] if len(e) == 2 and set(e) & reached for n in e}
        if expanded == reached:
            break
        reached = expanded
    visible = (bool(fragment.get('required_nodes')) and set(fragment['required_nodes']) <= reached
               and reached <= nodes and fragment.get('geometry_verified')
               and fragment.get('endpoints_visible') and fragment.get('boundary_checked')
               and bool(fragment.get('labels')))
    return dict(status='CONNECTION_VISIBLE' if visible else 'CONNECTION_AMBIGUOUS',
                reason='Verified local endpoints and continuous edges' if visible else
                       'Required endpoints, label binding, or continuous geometry not established')


def certify(claim, observation, provenance):
    o = observation
    kind = claim['evidence_type']
    cont = continuation(kind, o.get('continuation_facts', {}))
    note = notes(o.get('notes', []))
    conn = connection(o.get('fragment', {})) if kind in GRAPHIC else dict(status='NOT_APPLICABLE')
    parts = [p | dict(status=part_status(p)) for p in o.get('parts', [])]
    failures = []
    if not o.get('subject_found'):
        failures.append('SUBJECT_NOT_GROUNDED')
    if not o.get('correct_type'):
        failures.append('WRONG_EVIDENCE_TYPE')
    if not o.get('usable'):
        failures.append('PAYLOAD_NOT_USABLE')
    if not o.get('delivered'):
        failures.append('PART_NOT_DELIVERED')
    if o.get('wrong_subject'):
        failures.append('SUBJECT_OR_DOCUMENT_MISMATCH')
    if not o.get('start') or not o.get('end'):
        failures.append('PART_DELIVERED_NOT_VERIFIED' if o.get('delivered') else 'PART_NOT_DELIVERED')
    if o.get('semantic_extent_reason'):
        failures.append('SEMANTIC_EXTENT_UNPROVEN: ' + o['semantic_extent_reason'])
    if not parts or not any(p['status'] == 'PART_VERIFIED' for p in parts):
        failures.append('CLAIM_CONTENT_NOT_VERIFIED')
    failures.extend(p['status'] for p in parts if p['status'] in {'PART_NOT_DELIVERED', 'PART_DELIVERED_NOT_VERIFIED'})
    if cont['applicable'] == 'YES':
        if not cont['verified'] or not cont['parts'] or any(part_status(p) != 'PART_VERIFIED' for p in cont['parts']):
            failures.append('CONTINUATION_REQUIRED_NOT_VERIFIED')
    elif cont['applicable'] == 'UNKNOWN_WITH_REASON':
        failures.append('CONTINUATION_UNKNOWN_WITH_REASON')
    for n in note['decisions']:
        if n['status'] == 'UNKNOWN_WITH_REASON':
            failures.append('NOTE_UNKNOWN_WITH_REASON')
        if n['status'] == 'RELEVANT' and (not n['part'] or part_status(n['part']) != 'PART_VERIFIED'):
            failures.append('RELEVANT_NOTE_NOT_VERIFIED')
    if kind in TABLE:
        group = o.get('row_group', {})
        if not (group.get('table_identity') and group.get('columns') and group.get('end')
                and group.get('cell_mapping_verified') and group.get('expected_rows')
                and set(group['expected_rows']) <= set(group.get('delivered_rows', []))
                and set(group.get('component_rows', [])) <= set(group.get('delivered_rows', []))):
            failures.append('RELEVANT_ROW_GROUP_NOT_VERIFIED')
    elif kind in GRAPHIC:
        if conn['status'] == 'PART_NOT_REQUIRED_FOR_CLAIM':
            if not (claim.get('scope_kind') == 'LOCAL_POSITION_LABEL'
                    and o.get('annotation_binding_verified') and o.get('fragment', {}).get('labels')):
                failures.append('ANNOTATED_POSITION_NOT_VERIFIED')
        elif conn['status'] != 'CONNECTION_VISIBLE':
            failures.append(conn['status'])
        if not any(p.get('role') == 'raster' and p['status'] == 'PART_VERIFIED' for p in parts):
            failures.append('GRAPHIC_RASTER_NOT_VERIFIED')
    elif not o.get('claim_content_complete'):
        failures.append('CLAIM_TEXT_NOT_VERIFIED')
    mandatory_provenance = ('package_hash', 'packet_hash', 'requirement_hash', 'document_version',
                            'side', 'source_pdf_sha256', 'evidence_hashes')
    if any(not provenance.get(k) for k in mandatory_provenance):
        failures.append('PROVENANCE_INCOMPLETE')
    failures = sorted(set(failures))
    status = ('UNAVAILABLE_BY_POLICY' if o.get('policy_blocked') else
              'MISSING' if not o.get('delivered') else 'PARTIAL' if failures else 'COMPLETE')
    return ClaimBoundaryCertificate(claim_id=claim['claim_id'], subject_id=claim['subject_id'],
        evidence_requirement_id=claim['requirement_id'], evidence_type=kind,
        mandatory=claim['mandatory'], supporting=not claim['mandatory'],
        boundary_start=o.get('start'), boundary_end=o.get('end'), continuation_status=cont,
        note_status=note, connection_status=conn,
        delivered_parts=[p for p in parts if p.get('delivered')],
        missing_parts=[p for p in parts if p['status'] in {'PART_NOT_DELIVERED', 'PART_DELIVERED_NOT_VERIFIED'}],
        completeness=status, explanation=failures or [o['sufficiency_reason']],
        provenance=provenance, proof=dict(claim=claim, observation=o)).to_dict()


def package_completeness(certificates, *, baseline_status=None):
    mandatory = [c for c in certificates if c['mandatory']]
    blockers = [c for c in mandatory if c['completeness'] != 'COMPLETE']
    missing_support = [c['evidence_requirement_id'] for c in certificates
                       if c['supporting'] and c['completeness'] != 'COMPLETE']
    status = ('MISSING' if baseline_status == 'MISSING' else
              'COMPLETE' if mandatory and not blockers else
              'MISSING' if mandatory and all(c['completeness'] == 'MISSING' for c in mandatory) else 'PARTIAL')
    return dict(completeness=status, mandatory_requirements=[c['evidence_requirement_id'] for c in mandatory],
        supporting_missing='YES' if missing_support else 'NO', missing_supporting=missing_support,
        blockers=[dict(requirement_id=c['evidence_requirement_id'], completeness=c['completeness'],
                       reasons=c['explanation']) for c in blockers])
