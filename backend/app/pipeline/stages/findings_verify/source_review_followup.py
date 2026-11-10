"""Prepare bounded local follow-ups from source-bound responses; never dispatch."""
from .source_review_results import validate_source_review
from .source_review_context import expand_source_review_context, MAX_CONTEXT_REVISIONS
from .source_review_graphics import attach_graphic_receipts
from .source_review_request import prepare_source_review_request


def prepare_review_followup(response, queue, findings, md, graphic_dir, *, max_images=8):
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
    packet = next(p for p in queue['packets'] if p['finding_id'] == response['finding_id'])
    revision = packet.get('context_revision', 0)
    previous = packet.get('context_search_receipt', {})
    unfinished = bool(previous.get('backlog') or previous.get('deferred_queries'))
    if revision >= MAX_CONTEXT_REVISIONS:
        result['status'] = 'followup_limit_reached'
        return result
    if revision and not unfinished:
        searched = {q.casefold() for receipt in [*packet.get('context_search_history', []), previous]
                    for q in receipt.get('query_hit_counts', {})}
        queries = [q for q in queries if q.casefold() not in searched]
    if not queries and not (revision and unfinished):
        result['status'] = 'no_search_backlog' if revision else 'no_model_queries'
        return result
    # A supported core does not imply the bounded search was exhausted.
    # Continue only recorded searches and preserve all original source bindings.
    try:
        expanded = expand_source_review_context(queue, response['finding_id'], md, queries, continue_backlog=bool(revision and unfinished), continue_queries=bool(revision and not unfinished))
    except ValueError as exc:
        if str(exc) != 'context_total_budget':
            raise
        result['status'] = 'context_total_budget'
        return result
    # Exhaust deferred query batches locally when a page contains no new
    # sources. No model call is needed to advance an empty search page.
    while True:
        expanded_packet = next(p for p in expanded['packets'] if p['finding_id'] == response['finding_id'])
        receipt = expanded_packet['context_search_receipt']
        if (receipt['added_block_ids'] or not receipt.get('deferred_queries')
                or expanded_packet['context_revision'] >= MAX_CONTEXT_REVISIONS):
            break
        try:
            expanded = expand_source_review_context(expanded, response['finding_id'], md, [], continue_backlog=True)
        except ValueError as exc:
            if str(exc) != 'context_total_budget':
                raise
            break
    attach_graphic_receipts(expanded, md, graphic_dir)
    packet = next(p for p in expanded['packets'] if p['finding_id'] == response['finding_id'])
    result['search_receipt'] = packet['context_search_receipt']
    result['expanded_queue'] = expanded
    if not packet['context_search_receipt']['added_block_ids']:
        result['status'] = 'no_additional_sources'
        return result
    finding = next(f for f in findings if f['id'] == response['finding_id'])
    result['request'] = prepare_source_review_request(expanded, finding, md, max_images=max_images)
    result['status'] = 'prepared_awaiting_dispatch_authorization'
    return result


def run_review_response_phase(output_dir, md):
    """Consume optional saved responses into local follow-up requests only.

    Receipt validation binds responses to current findings and source. No response
    can authorize its own suppression or create an engineer-facing question.
    """
    import json
    from pathlib import Path
    from collections import Counter
    directory=Path(output_dir)
    inbox=directory/'03_source_review_responses.json'
    target=directory/'03_source_review_followups.json'
    # Prevent a prior run's prepared packet surviving a failed/absent rebuild.
    target.unlink(missing_ok=True)
    if not inbox.exists():return {'prepared':0,'invalid':0,'skipped':True}
    entries=json.loads(inbox.read_text())
    document=json.loads((directory/'03_findings.json').read_text())
    if not isinstance(entries,list) or not isinstance(document.get('findings'),list):
        raise ValueError('invalid_response_inbox_or_findings')
    counts=Counter(e.get('response',{}).get('finding_id') for e in entries
        if isinstance(e,dict) and isinstance(e.get('response'),dict)
        and isinstance(e['response'].get('finding_id'),str))
    reports=[]
    for entry in entries:
        fid=None
        try:
            fid=entry['response']['finding_id']
            if not isinstance(fid,str) or counts[fid]!=1:raise ValueError('duplicate_or_invalid_response')
            report=prepare_review_followup(entry['response'],entry['queue'],document['findings'],md,directory/'blocks_stage02_100')
            reports.append({'finding_id':fid,**report})
        except (KeyError,TypeError,ValueError,OSError) as exc:
            reports.append({'finding_id':fid,'status':'invalid_response','reason':str(exc),'automatic_dispatch':False})
    target.write_text(json.dumps(reports,ensure_ascii=False,indent=2))
    return {'prepared':sum(r['status']=='prepared_awaiting_dispatch_authorization' for r in reports),
            'invalid':sum(r['status'] in ('invalid_response','invalid_context_queries') for r in reports),'skipped':False}
