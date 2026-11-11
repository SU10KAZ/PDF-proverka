"""Carry prior-pass targets with stage-qualified identities, never bare F numbers."""
from collections import Counter
from copy import deepcopy
from .recovery_links import target_signature


def replace_targets(decisions, mapping):
    for decision in decisions if isinstance(decisions, list) else []:
        if not isinstance(decision, dict):
            continue
        claims = decision.get('subclaims', [])
        records = [decision, *(c for c in claims if isinstance(c, dict))] if isinstance(claims, list) else [decision]
        for record in records:
            ids = record.get('target_finding_ids')
            if isinstance(ids, list) and all(isinstance(x, str) for x in ids):
                changed = [mapping.get(x, x) for x in ids]
                if changed != ids:
                    record.setdefault('reported_target_finding_ids', deepcopy(ids))
                    record['target_finding_ids'] = changed


def carry_parent_targets(packet, stage, payload):
    existing = packet.setdefault('existing_findings', [])
    occupied = {f.get('id') for f in existing if isinstance(f, dict)}
    findings = payload.get('findings', [])
    findings = findings if isinstance(findings, list) else []
    counts = Counter(f.get('id') for f in findings if isinstance(f, dict) and isinstance(f.get('id'), str))
    mapping, bindings = {}, []
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        fid = finding.get('id')
        if not isinstance(fid, str) or counts[fid] != 1 or fid in occupied:
            continue
        alias = f'parent:{stage}:{fid}'
        if alias in occupied:
            continue
        snapshot = {**deepcopy(finding), 'id': alias}
        existing.append(snapshot)
        mapping[fid] = alias
        bindings.append({'alias': alias, 'parent_stage': stage, 'parent_local_id': fid,
                         'source_signature': target_signature(finding),
                         'alias_signature': target_signature(snapshot)})
    packet['parent_target_bindings'] = bindings
    replace_targets(packet.get('previous_decisions', []), mapping)
    for candidate in packet.get('candidate_group', []):
        for obligation in candidate.get('pending_subclaims', []):
            claim = obligation.get('claim') if isinstance(obligation, dict) else None
            if isinstance(claim, dict):
                replace_targets([claim], mapping)


def bind_parent_targets(payload, registry, findings):
    """Resolve only executor-owned aliases against the exact earlier payload."""
    if not payload.get('recovery_parent_target_bindings'):
        return {'resolved': {}, 'pending': [], 'semantic_verified': False}
    mapping, signatures, pending = {}, {}, []
    for binding in payload.get('recovery_parent_target_bindings', []):
        alias = binding['alias']
        matches = registry.get((binding['parent_stage'], binding['parent_local_id']), [])
        if len(matches) != 1 or matches[0]['source_signature'] != binding['source_signature']:
            pending.append({'alias': alias, 'reason': 'parent_target_not_proven'})
            continue
        targets = [f for f in findings if f.get('id') == matches[0]['final_id']]
        if len(targets) != 1:
            pending.append({'alias': alias, 'reason': 'parent_final_target_missing_or_ambiguous'})
            continue
        mapping[alias] = targets[0]['id']
        signatures[alias] = (binding['alias_signature'], target_signature(targets[0]))
    replace_targets(payload.get('candidate_decisions', []), mapping)
    payload['recovery_existing_target_ids'] = [mapping.get(x, x) for x in payload.get('recovery_existing_target_ids', [])]
    for field in ('recovery_link_updates', 'recovery_addenda_updates'):
        for update in payload.get(field, []):
            alias = update.get('target_id')
            if alias not in mapping:
                continue
            before, after = signatures[alias]
            if update.get('target_signature') != before:
                pending.append({'alias': alias, 'reason': 'parent_update_snapshot_mismatch'})
                continue
            update.update(reported_target_id=alias, reported_target_signature=before,
                          target_id=mapping[alias], target_signature=after)
    return {'resolved': mapping, 'pending': pending, 'semantic_verified': False}
