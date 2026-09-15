"""Resolve explicit model references; never infer links from package membership."""
from copy import deepcopy
from dataclasses import asdict

from experiments.project_change_contracts_272.evidence import fingerprint
from .contract import EvidenceBinding, EvidenceRole as Role


def clean(value):
    return ' '.join(value.split()) if isinstance(value, str) else value


def bind(raw, packet, valid_witnesses):
    states = {s: [] for s in ('old', 'new')}
    claims, rejected, issues = [], [], []
    evidence = {}
    for side in states:
        for item in packet['evidence'][side]:
            eid = item['evidence_id']
            if eid in evidence:
                raise ValueError('Ambiguous delivered evidence ID: ' + eid)
            evidence[eid] = (side, item)
    claim_id = raw.get('case_token') or 'current_claim'
    raw_hash, packet_hash = fingerprint(raw), fingerprint(packet)

    def reject(eid, role, path, reason):
        rejected.append(dict(evidence_id=eid, role=role.value, raw_path=path, reason=reason))
        issues.append(reason)

    def add(eid, role, path, side=None, target=None, claim_only=False, subject=None):
        located = evidence.get(eid) if isinstance(eid, str) else None
        if not located:
            reject(eid, role, path, 'UNDELIVERED_BINDING_EVIDENCE')
            return
        delivered_side, item = located
        side = side or delivered_side
        source_subject = raw[side + '_state']['engineering_subject']
        if delivered_side != side or item.get('side') != side:
            reject(eid, role, path, 'BINDING_SIDE_MISMATCH')
            return
        if (not item.get('document_version') or
                item['document_version'] != packet.get('source_versions', {}).get(side)):
            reject(eid, role, path, 'BINDING_VERSION_MISMATCH')
            return
        if not clean(item.get('subject')) or clean(item['subject']) != clean(source_subject):
            reject(eid, role, path, 'BINDING_SUBJECT_MISMATCH')
            return
        receipt = item.get('source_receipt')
        if (not isinstance(receipt, dict) or not receipt.get('sha256') or not receipt.get('path')
                or type(item.get('page')) is not int or item['page'] < 1):
            reject(eid, role, path, 'BINDING_PROVENANCE_REQUIRED')
            return
        requirements = [deepcopy(row) for row in packet.get('evidence_coverage', {}).get('requirements', [])
            if eid in row.get('evidence_ids', [])
            and row['requirement'].get('side') == side.upper()
            and row['requirement'].get('document_version') == item['document_version']
            and row['requirement'].get('page') == item['page']
            and clean(row['requirement'].get('subject')) == clean(source_subject)]
        provenance = dict(raw_response_hash=raw_hash, packet_hash=packet_hash,
            source_package_hash=packet.get('source_package_hash'),
            document_version=item['document_version'], page=item['page'],
            source_receipt=deepcopy(receipt), bbox=deepcopy(item.get('bbox')),
            raster=deepcopy(item.get('raster')), requirements=requirements,
            relation='EXPLICIT_MODEL_REFERENCE_TO_SAME_SUBJECT_DELIVERED_SOURCE')
        link = EvidenceBinding(eid, role, side, subject or source_subject, target or claim_id,
                               path, provenance, eid in valid_witnesses)
        (claims if claim_only else states[side]).append(link)

    def many(ids, role, path, **kwargs):
        if not isinstance(ids, (list, tuple)):
            reject(None, role, path, 'INVALID_BINDING_ID_LIST')
            return
        for index, eid in enumerate(ids):
            add(eid, role, f'{path}[{index}]', **kwargs)

    for side in states:
        source = raw[side + '_state']
        base = side + '_state.'
        many(source.get('evidence_ids', []), Role.STATE_VALUE, base + 'evidence_ids', side=side)
        identity = source.get('subject_identity')
        if isinstance(identity, dict):
            many(identity.get('evidence_ids', []), Role.SUBJECT_IDENTITY,
                 base + 'subject_identity.evidence_ids', side=side)
            if clean(identity.get('scope')):
                many(identity.get('evidence_ids', []), Role.SCOPE_BINDING,
                     base + 'subject_identity.evidence_ids', side=side)
        many(source.get('scope_evidence_ids', []), Role.SCOPE_BINDING,
             base + 'scope_evidence_ids', side=side)

    for index, counter in enumerate(raw.get('old_counter_evidence', [])):
        add(counter.get('evidence_id'), Role.COUNTER_EVIDENCE,
            f'old_counter_evidence[{index}].evidence_id', side='old')
    absence = raw.get('old_absence', {})
    for name in ('inspected_evidence_ids', 'positive_evidence_ids'):
        many(absence.get(name, []), Role.COUNTER_EVIDENCE, 'old_absence.' + name, side='old')
    if absence.get('negative_evidence_id'):
        add(absence['negative_evidence_id'], Role.COUNTER_EVIDENCE,
            'old_absence.negative_evidence_id', side='old')
    for index, condition in enumerate(raw.get('applicable_conditions', [])):
        many(condition.get('evidence_ids', []), Role.CONDITION_SUPPORT,
             f'applicable_conditions[{index}].evidence_ids')
    semantic = raw.get('exists_change')
    if isinstance(semantic, dict):
        many(semantic.get('evidence_ids', []), Role.CHANGE_OBSERVATION,
             'exists_change.evidence_ids', claim_only=True)
    conflict = raw.get('source_conflict')
    if isinstance(conflict, dict):
        # Keep explicit subsidiary targets separate from the core state.
        targets = conflict.get('affected_claim_ids', conflict.get('affected_claims', []))
        if not isinstance(targets, list) or any(not isinstance(t, str) or not t.strip() for t in targets):
            reject(None, Role.CONFLICT_EVIDENCE, 'source_conflict', 'INVALID_CONFLICT_BINDING_TARGETS')
            targets = []
        for target in targets or [claim_id]:
            many(conflict.get('evidence_ids', []), Role.CONFLICT_EVIDENCE,
                 'source_conflict.evidence_ids', target=target, claim_only=True,
                 subject=conflict.get('affected_subject'))
    return dict(states=states, claims=[asdict(b) for b in claims], rejected=rejected,
                issues=list(dict.fromkeys(issues)))
