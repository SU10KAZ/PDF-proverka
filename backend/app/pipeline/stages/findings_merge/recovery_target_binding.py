"""Translate request-local finding IDs after merge assigns global finding IDs."""
from copy import deepcopy
from collections import Counter


def bind_report_targets(report, existing_ids, local_targets):
    existing_counts = Counter(existing_ids)
    existing = set(existing_counts)
    issues, repairs = [], []
    report['original_candidate_decisions'] = deepcopy(report.get('candidate_decisions', []))
    for decision in report.get('candidate_decisions', []):
        if not isinstance(decision, dict):
            continue
        claims = decision.get('subclaims', [])
        records = [decision, *(c for c in claims if isinstance(c, dict))] if isinstance(claims, list) else [decision]
        for record in records:
            if 'target_finding_ids' not in record:
                continue
            raw = record['target_finding_ids']
            resolved, reason = [], None
            if not isinstance(raw, list) or not all(isinstance(fid, str) for fid in raw):
                reason = 'invalid_target_ids'
            else:
                for fid in raw:
                    mapped = local_targets.get(fid, [])
                    if existing_counts[fid] > 1:
                        reason = 'ambiguous_existing_target'
                    elif fid in existing and fid in local_targets:
                        reason = 'existing_and_local_id_collision'
                    elif fid in existing:
                        resolved.append(fid)
                    elif len(mapped) == 1:
                        resolved.append(mapped[0])
                    else:
                        reason = 'unresolved_or_ambiguous_local_target'
                    if reason:
                        break
            address = {'origin': decision.get('origin'), 'pointer': decision.get('pointer'),
                       'claim_id': record.get('id') if record is not decision else None}
            if reason:
                issues.append({**address, 'reported_targets': deepcopy(raw), 'reason': reason})
                record['reported_target_finding_ids'] = deepcopy(raw)
                record['target_finding_ids'] = []
            elif raw != resolved:
                repairs.append({**address, 'reported_targets': deepcopy(raw), 'bound_targets': resolved})
                record['reported_target_finding_ids'] = deepcopy(raw)
                record['target_finding_ids'] = resolved
    report['finding_target_binding'] = {'repairs': repairs, 'unresolved': issues,
        'local_to_final': deepcopy(local_targets), 'semantic_verified': False}
    return report['finding_target_binding']
