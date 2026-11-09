"""Repair explicit duplicate provenance; never promote it to verified evidence."""
from collections import defaultdict
import hashlib
import json


def target_signature(finding):
    identity = {k: finding.get(k) for k in ('id', 'problem', 'description', 'page', 'sheet')}
    return hashlib.sha256(json.dumps(identity, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def prepare_duplicate_links(packet, decisions):
    candidates = {(c['origin'], c['pointer']): c for c in packet.get('candidate_group', [packet['candidate']])}
    targets = defaultdict(list)
    for finding in packet['existing_findings']:
        if isinstance(finding, dict) and isinstance(finding.get('id'), str):
            targets[finding['id']].append(finding)
    rows = decisions if isinstance(decisions, list) else []
    index = defaultdict(list)
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get('origin'), str) and isinstance(row.get('pointer'), str):
            index[(row['origin'], row['pointer'])].append(row)
    updates, pending = [], []
    for key, receipts in index.items():
        if not any(r.get('decision') == 'duplicate' for r in receipts):
            continue
        row = receipts[0]
        reason = None
        ids, refs = row.get('target_finding_ids'), row.get('source_refs')
        if len(receipts) != 1 or key not in candidates:
            reason = 'ambiguous_or_unknown_candidate'
        elif not isinstance(ids, list) or not ids or any(not isinstance(fid, str) or len(targets[fid]) != 1 for fid in ids):
            reason = 'missing_or_ambiguous_duplicate_targets'
        elif not isinstance(refs, list) or not refs or any(not isinstance(ref, str) or ref not in packet['allowed_source_refs'] for ref in refs):
            reason = 'missing_or_unknown_duplicate_sources'
        elif not isinstance(row.get('reason'), str) or not row['reason'].strip():
            reason = 'missing_duplicate_reason'
        if reason:
            pending.append({'decision': row, 'reason': reason})
            continue
        for fid in dict.fromkeys(ids):
            updates.append({'source_id': candidates[key]['source_id'], 'target_id': fid,
                            'target_signature': target_signature(targets[fid][0]),
                            'receipt': row, 'semantic_verified': False})
    return updates, pending


def apply_duplicate_links(findings, updates):
    index = defaultdict(list)
    for finding in findings:
        index[finding.get('id')].append(finding)
    applied, pending = [], []
    for update in updates:
        targets = index.get(update['target_id'], [])
        if len(targets) != 1 or target_signature(targets[0]) != update['target_signature']:
            pending.append({**update, 'reason': 'target_changed_after_review'})
            continue
        target = targets[0]
        refs = target.get('source_finding_ids', [])
        if not isinstance(refs, list):
            pending.append({**update, 'reason': 'invalid_existing_source_ids'})
            continue
        target['source_finding_ids'] = list(dict.fromkeys([*refs, update['source_id']]))
        receipts = list(target.get('recovery_link_receipts', []))
        if update not in receipts:
            receipts.append(update)
        target['recovery_link_receipts'] = receipts
        applied.append(update)
    return {'applied': applied, 'pending': pending, 'semantic_verified': False}
