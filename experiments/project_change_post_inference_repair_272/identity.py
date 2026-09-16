"""Source-backed identity contract, separate from candidate IDs and display prose.

Discovery IDs are stable *within a version*. They are not proof of OLD/NEW
equivalence. Cross-version identity additionally needs equal physical scope and
explicit, witnessed model identity references. No source/audit labels are read.
"""
import re
from experiments.project_change_contracts_272.evidence import fingerprint


def clean(value):
    return ' '.join(value.split()).casefold().replace('ё', 'е') if isinstance(value, str) else value


def meaningful(value):
    return isinstance(value, str) and bool(value.strip()) and not re.search(
        r'unknown|unresolved|not.proven|не установлен|не определен|не подтвержден', clean(value))


def tokens(value):
    return set(re.findall(r'[\w]+(?:\.[\w]+)*', clean(value or '')))


def rooms(value):
    return set(re.findall(r'(?<![\w.])\d{1,3}\.\d{1,3}(?:\.\d{1,3})?(?!\w|\.\d)', value or ''))


def physical_identity(state):
    identity = state.get('subject_identity') or {}
    fields = ('functional_owner', 'system', 'subsystem', 'scope')
    if not all(meaningful(identity.get(k)) for k in fields):
        return None
    # Explicit physical dimensions must never disappear behind a common label.
    extra = ('discipline', 'location', 'floor', 'room', 'zone', 'reference_system', 'geometric_element')
    return {k: clean(identity[k]) for k in fields + extra if identity.get(k) is not None}


def contract(candidate, packet):
    return dict(schema='BINDING_CONTRACT/1', technical_candidate_id=candidate['candidate_id'],
        source_label=candidate['subject'], canonical_subjects=candidate.get('canonical_subjects', {}),
        stable_subject_id=candidate.get('stable_subject_id'), scope=candidate.get('scope', []),
        pair_key=packet.get('pair_key'), source_versions=packet['source_versions'])


def resolve(raw, packet, item, side, role, valid_witnesses, context):
    """Require an explicit role reference, matching F1 provenance and witness scope.

    Called only for references already explicitly enumerated by the V4 binder.
    Delivered F1 roles are retrieval purposes, not semantic claim roles: the
    latter stay at their exact raw paths and must have a validated witness.
    """
    source = raw[side + '_state']
    eid = item['evidence_id']
    identity = physical_identity(source)
    if not identity:
        return None, 'BINDING_CANONICAL_IDENTITY_UNRESOLVED'
    rows = []
    candidate = context['technical_candidate_id']
    for row in packet.get('evidence_coverage', {}).get('requirements', []):
        q = row['requirement']; p = q.get('provenance', {})
        if (eid in row.get('evidence_ids', []) and q.get('side') == side.upper()
                and q.get('page') == item['page'] and q.get('document_version') == item['document_version']
                and p.get('candidate_id') == candidate and clean(q.get('subject')) == clean(item.get('subject'))
                and row.get('delivery', {}).get('payload_delivered')
                and set(p.get('discovery_subject_ids', [])) & set(context['canonical_subjects'].get(side, []))
                and any(r.get('requirement_id') == q['requirement_id'] and r.get('payload_delivered')
                        for r in item.get('region_bindings', []))):
            rows.append(row)
    if not rows:
        return None, 'BINDING_CANONICAL_SOURCE_MISMATCH'
    identity_refs = source.get('subject_identity', {}).get('evidence_ids', [])
    if not identity_refs or eid not in valid_witnesses:
        return None, 'BINDING_SUBJECT_WITNESS_REQUIRED'
    witnesses = [w for w in raw.get('witnesses', []) if w.get('evidence_id') == eid and w.get('side') == side]
    locator = ' '.join(str(w.get(k, '')) for w in witnesses
                       for k in ('visual_locator', 'binding_reason', 'literal_quote'))
    scope = identity['scope']
    # A source-bound room/zone/owner anchor is required even on a shared page.
    anchors = tokens(scope) | tokens(identity['functional_owner'])
    anchors -= {'помещения', 'помещений', 'жилой', 'комплекс', 'комплекса', 'объект', 'этажа', 'этаж',
                'корпус', 'корпуса', 'корпусов', 'первый', 'второй', 'третий', 'область', 'зона'}
    anchors = {t for t in anchors if len(t) >= 3 or t.isdigit()}
    if not anchors & tokens(locator):
        return None, 'BINDING_SCOPE_WITNESS_REQUIRED'
    explicit_rooms = set(re.findall(r'(?:помещение|room)\s+(\d+(?:\.\d+)+)', scope, re.I))
    if explicit_rooms and not explicit_rooms & rooms(locator):
        return None, 'BINDING_ROOM_WITNESS_MISMATCH'
    # Discovery IDs provide the primary source identity; model display wording
    # does not participate. Source pages remain side-specific provenance.
    source_ids = sorted({s for r in rows for s in r['requirement']['provenance']['discovery_subject_ids']
                         if s in context['canonical_subjects'].get(side, [])})
    canonical = dict(identity=identity, stable_subject_id=context.get('stable_subject_id'),
        discovery_subject_ids=source_ids, source_pages=sorted({r['requirement']['page'] for r in rows}),
        discipline=identity.get('discipline'), functional_role=source.get('functional_role'),
        model_wording=source['engineering_subject'], source_label=item['subject'],
        technical_candidate_id=candidate, evidence_role=role.value, requirements=rows)
    return canonical, None


def canonical_key(identity, context):
    # Bind a cross-version pair of discovery IDs only at the explicitly mapped
    # physical scope. Different rooms/systems remain different subsubjects.
    primary = context.get('stable_subject_id') or context['canonical_subjects']
    return 'subject_' + fingerprint(dict(primary=primary, pair=context.get('pair_key'), physical=identity))[:24]
