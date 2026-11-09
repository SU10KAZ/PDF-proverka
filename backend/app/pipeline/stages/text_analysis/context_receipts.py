"""Account for deferred requests without treating model receipts as proof."""
from collections import defaultdict


def reconcile_context_requests(compact: dict, result: dict, known_blocks: set[str]) -> dict:
    answers = defaultdict(list)
    for item in result.get('context_request_results', []) or []:
        if isinstance(item, dict) and isinstance(item.get('request_id'), str):
            answers[item['request_id']].append(item)
    finding_ids = {f.get('id') for f in result.get('text_findings', []) if isinstance(f, dict) and isinstance(f.get('id'), str)}
    rows = []
    requests = compact.get('context_requests', []) + compact.get('context_request_backlog', [])
    expected = {r['request_id'] for r in requests}
    backlog = {r['request_id'] for r in compact.get('context_request_backlog', [])}
    for request in requests:
        rid = request['request_id']
        matches = answers.get(rid, [])
        row = {'request_id': rid, 'source_pointer': request.get('source_pointer'),
               'status': 'not_checked', 'semantic_verified': False}
        if rid in backlog:
            row['reason'] = 'compact_budget'
        elif len(matches) != 1:
            row['reason'] = 'missing_receipt' if not matches else 'ambiguous_receipt'
        else:
            answer = matches[0]
            status = answer.get('status')
            refs = answer.get('source_refs', [])
            links = answer.get('finding_ids', [])
            valid_lists = (isinstance(refs, list) and isinstance(links, list)
                           and all(isinstance(x, str) for x in refs + links))
            if status not in {'resolved', 'needs_data', 'not_checked'} or not valid_lists:
                row['reason'] = 'invalid_receipt'
            elif status == 'resolved' and (not refs or not answer.get('reason')
                                          or any(x not in known_blocks for x in refs)
                                          or any(x not in finding_ids for x in links)):
                row['reason'] = 'unsupported_resolution_receipt'
            else:
                row.update(status='reported_resolved' if status == 'resolved' else status,
                           reason=answer.get('reason'), source_refs=refs, finding_ids=links)
        rows.append(row)
    return {'requests': rows, 'unknown_receipt_ids': sorted(set(answers) - expected),
            'total': len(rows), 'reported_resolved': sum(r['status'] == 'reported_resolved' for r in rows),
            'unresolved': sum(r['status'] != 'reported_resolved' for r in rows),
            'boundary': 'Valid receipt syntax and existing IDs do not prove source relevance or actual graphic inspection.'}
