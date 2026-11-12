"""Offline diagnostics for sealed outputs; never rewrite historical identifiers."""
import re


VISIBLE_FIELDS = ('problem', 'description', 'solution', 'reason', 'risk')
REFERENCE = re.compile(r'(?<![\w-])F-\d+(?![\w-])')


def audit(findings):
    ids = [f['id'] for f in findings]
    if len(ids) != len(set(ids)):
        raise ValueError('Duplicate final IDs prevent unambiguous reference lookup')
    current = set(ids)
    missing, historical = [], []
    for index, finding in enumerate(findings):
        for field in VISIBLE_FIELDS:
            value = finding.get(field)
            if not isinstance(value, str):
                continue
            for ref in sorted(set(REFERENCE.findall(value)) - current):
                missing.append({'finding_id': finding['id'], 'field': field,
                                'reference': ref})
        for offset, addendum in enumerate(finding.get('recovery_addenda') or []):
            old = addendum.get('reported_target_id')
            reason = addendum.get('reason')
            if (not old or old == finding['id'] or not isinstance(reason, str)
                    or old not in REFERENCE.findall(reason)):
                continue
            fields = [field for field in VISIBLE_FIELDS
                      if isinstance(finding.get(field), str)
                      and reason in finding[field]]
            if fields:
                historical.append({
                    'finding_id': finding['id'],
                    'addendum_pointer': f'/findings/{index}/recovery_addenda/{offset}',
                    'historical_target_id': old, 'visible_fields': fields,
                    'current_reference_exists': old in current,
                })
    return {
        'status': 'diagnostic_not_semantic_adjudication',
        'findings': len(findings),
        'missing_visible_references': missing,
        'historical_target_mentions': historical,
        'affected_historical_findings': len({r['finding_id'] for r in historical}),
        'limits': [
            'Missing IDs may deliberately refer to another report; review context.',
            'Historical target mentions require lineage review before correction.',
            'No global ID substitution, quality score or project defect count.',
            'Caller must verify a terminal sealed output before supplying findings.',
        ],
    }


def audit_lineage(before, after):
    """Flag unchanged prose whose old display ID no longer names its source.

    Exact source-stage/source-ID identity is deliberately conservative. Missing
    or many-to-one lineage stays unresolved; this is not an automatic rewriter.
    """
    def identity(row):
        stage, ids = row.get('source_stage'), row.get('source_finding_ids')
        if not isinstance(stage, str) or not stage or not isinstance(ids, list) or not ids:
            return None
        if any(not isinstance(item, str) or not item for item in ids):
            return None
        return stage, tuple(sorted(set(ids)))

    def index(rows):
        by_id, by_identity = {}, {}
        for row in rows:
            if row['id'] in by_id:
                raise ValueError('Duplicate IDs prevent lineage lookup')
            by_id[row['id']] = row
            key = identity(row)
            if key is not None:
                by_identity.setdefault(key, []).append(row)
        return by_id, by_identity

    old_ids, old_keys = index(before)
    new_ids, new_keys = index(after)
    suspects, unresolved = [], []
    for row in after:
        key = identity(row)
        old_owners = old_keys.get(key, []) if key is not None else []
        new_owners = new_keys.get(key, []) if key is not None else []
        if len(old_owners) != 1 or len(new_owners) != 1:
            if any(REFERENCE.search(row.get(field, '')) for field in VISIBLE_FIELDS
                   if isinstance(row.get(field), str)):
                unresolved.append({'finding_id': row['id'], 'reason': 'owner_lineage_missing_or_ambiguous'})
            continue
        old_owner = old_owners[0]
        for field in VISIBLE_FIELDS:
            value = row.get(field)
            if not isinstance(value, str) or value != old_owner.get(field):
                continue
            for ref in sorted(set(REFERENCE.findall(value))):
                old_target = old_ids.get(ref)
                target_key = identity(old_target) if old_target else None
                predecessors = old_keys.get(target_key, []) if target_key is not None else []
                successors = new_keys.get(target_key, []) if target_key is not None else []
                if len(predecessors) != 1 or len(successors) != 1:
                    unresolved.append({'finding_id': row['id'], 'field': field,
                                       'reference': ref, 'reason': 'target_lineage_missing_or_ambiguous'})
                    continue
                target = successors[0]
                if target['id'] == ref:
                    continue
                suspects.append({
                    'finding_id': row['id'], 'field': field,
                    'reference': ref, 'occurrences': sum(m.group() == ref for m in REFERENCE.finditer(value)),
                    'expected_target_by_lineage': target['id'],
                    'current_reference_exists': ref in new_ids,
                    'current_reference_has_same_lineage': ref in new_ids and identity(new_ids[ref]) == target_key,
                    'self_reference_by_lineage': target['id'] == row['id'],
                    'basis': 'unchanged_visible_field_and_unique_exact_source_lineage',
                })
    return {
        'status': 'diagnostic_requires_context_review',
        'suspected_stale_references': suspects,
        'unresolved_references': unresolved,
        'limits': [
            'Exact lineage does not establish prose intent; references may deliberately describe historical IDs.',
            'Changed prose is not assessed by this conservative check.',
            'Ambiguous, absent or merged lineage is not guessed.',
            'No runtime filtering, replacement, defect count or acceptance follows.',
        ],
    }
