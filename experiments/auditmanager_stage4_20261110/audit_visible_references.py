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
