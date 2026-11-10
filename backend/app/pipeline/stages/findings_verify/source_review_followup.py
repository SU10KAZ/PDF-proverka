"""Prepare one local follow-up from a source-bound model response; never dispatch."""
from .source_review_results import validate_source_review
from .source_review_context import expand_source_review_context
from .source_review_graphics import attach_graphic_receipts
from .source_review_request import prepare_source_review_request


def prepare_review_followup(response, queue, findings, md, graphic_dir):
    validation = validate_source_review(response, queue, findings, md)
    result = {'validation': validation, 'status': 'invalid_response',
              'automatic_dispatch': False, 'absence_proven': False}
    if validation['errors']:
        return result
    queries, origins = [], []
    for claim in response['claims']:
        if claim['status'] != 'needs_data':
            continue
        values = claim.get('context_queries', [])
        if not isinstance(values, list) or any(not isinstance(q, str) or not q.strip() for q in values):
            result['status'] = 'invalid_context_queries'
            return result
        for q in values:
            q = q.strip()
            if q not in queries:
                queries.append(q)
            origins.append({'claim_id':claim['claim_id'], 'query':q})
    result['query_origins'] = origins
    if not queries:
        result['status'] = 'no_model_queries'
        return result
    packet = next(p for p in queue['packets'] if p['finding_id'] == response['finding_id'])
    if packet.get('context_revision', 0):
        result['status'] = 'followup_limit_reached'
        return result
    expanded = expand_source_review_context(queue, response['finding_id'], md, queries)
    attach_graphic_receipts(expanded, md, graphic_dir)
    packet = next(p for p in expanded['packets'] if p['finding_id'] == response['finding_id'])
    result['search_receipt'] = packet['context_search_receipt']
    result['expanded_queue'] = expanded
    if not packet['context_search_receipt']['added_block_ids']:
        result['status'] = 'no_additional_sources'
        return result
    finding = next(f for f in findings if f['id'] == response['finding_id'])
    result['request'] = prepare_source_review_request(expanded, finding, md)
    result['status'] = 'prepared_awaiting_dispatch_authorization'
    return result
