"""Check publication-part delivery without treating model routing as acceptance.

This opt-in accounting check does not edit reports or certify final wording.
Explicitly deferred parts remain obligations even when their parent is emitted.
"""
from copy import deepcopy
from .publication_reconciliation import InvalidPublication, fingerprint


def check_publication_delivery(projection, findings, decisions):
    """Bind each part to actual final rows or an explicit retained disposition."""
    routes = ('technical_findings', 'questions', 'pending', 'withdrawn', 'history_only')
    parts = {}
    for route in routes:
        for part in projection[route]:
            key = part['publication_id']
            if key in parts:
                raise InvalidPublication('duplicate_delivery_part')
            parts[key] = (route, part)
    rows = {}
    for finding in findings:
        key = finding.get('id')
        if not isinstance(key, str) or not key or key in rows:
            raise InvalidPublication('ambiguous_final_finding_id')
        rows[key] = finding
    seen, receipts, pending = set(), [], []
    for decision in decisions:
        key = decision.get('publication_id')
        if key not in parts or key in seen:
            raise InvalidPublication('unknown_or_duplicate_delivery_decision')
        seen.add(key)
        route, part = parts[key]
        if decision.get('part_sha256') != fingerprint(part):
            raise InvalidPublication('stale_delivery_part')
        if not all(isinstance(decision.get(k), str) and decision[k].strip()
                   for k in ('reviewer', 'reason')):
            raise InvalidPublication('missing_delivery_review')
        state = decision.get('state')
        if state not in ('emitted', 'deferred', 'withheld', 'history'):
            raise InvalidPublication('invalid_delivery_state')
        targets = decision.get('targets')
        if not isinstance(targets, list) or (state == 'emitted') != bool(targets):
            raise InvalidPublication('invalid_delivery_targets')
        if state == 'emitted':
            if route in ('pending', 'withdrawn', 'history_only'):
                raise InvalidPublication('new_source_review_required_before_emission')
            used = set()
            for target in targets:
                fid = target.get('finding_id')
                if fid not in rows or fid in used:
                    raise InvalidPublication('missing_or_duplicate_delivery_target')
                used.add(fid)
                if target.get('finding_sha256') != fingerprint(rows[fid]):
                    raise InvalidPublication('final_finding_changed')
                actual_route = ('questions' if rows[fid].get('report_section') == 'questions'
                                else 'technical_findings')
                if actual_route != route:
                    raise InvalidPublication('delivery_section_changed_requires_review')
                quotes = target.get('field_quotes')
                if not isinstance(quotes, dict) or not quotes:
                    raise InvalidPublication('missing_delivery_wording')
                for field, quote in quotes.items():
                    if field not in ('problem', 'description', 'solution', 'risk') or not isinstance(quote, str) or not quote.strip():
                        raise InvalidPublication('invalid_delivery_wording')
                    if not isinstance(rows[fid].get(field), str) or quote not in rows[fid][field]:
                        raise InvalidPublication('delivery_wording_not_present')
        if route == 'pending' and state != 'deferred':
            raise InvalidPublication('pending_part_cannot_disappear')
        if state == 'history' and route != 'history_only':
            raise InvalidPublication('technical_part_cannot_become_history')
        receipt = {**deepcopy(decision), 'original_route': route,
                   'part': deepcopy(part), 'semantic_verified': False}
        receipts.append(receipt)
        if state == 'deferred':
            pending.append(deepcopy(part))
    if seen != set(parts):
        raise InvalidPublication('missing_delivery_decisions')
    return {'projection_sha256': fingerprint(projection), 'receipts': receipts,
            'pending_parts': pending, 'archive': deepcopy(projection['archive']),
            'accounting_complete': True, 'semantic_verified': False,
            'production_acceptance': False}


def check_document_publication_delivery(document):
    """Validate opt-in records against the exact rows about to be exported."""
    records = document.get('publication_delivery_checks', [])
    if not isinstance(records, list):
        raise InvalidPublication('invalid_delivery_records')
    seen, receipts = set(), []
    for record in records:
        if not isinstance(record, dict) or set(record) != {'projection', 'projection_sha256', 'decisions'}:
            raise InvalidPublication('invalid_delivery_record')
        projection = record['projection']
        if fingerprint(projection) != record['projection_sha256']:
            raise InvalidPublication('delivery_projection_changed')
        parent = projection['archive']['id']
        if parent in seen:
            raise InvalidPublication('duplicate_parent_delivery_record')
        seen.add(parent)
        receipts.append(check_publication_delivery(projection, document['findings'], record['decisions']))
    return receipts
