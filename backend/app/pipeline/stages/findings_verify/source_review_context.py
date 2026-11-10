"""Bounded local context expansion and continuation of unfinished source search."""
from collections import Counter
from copy import deepcopy
import hashlib
import json
import re

from backend.app.pipeline.stages.prepare.recovery_context import search_md_context
from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


MAX_CONTEXT_REVISIONS = 4
MAX_TOTAL_TEXT_CHARS = 240000


def expand_source_review_context(queue, finding_id, md, queries, *, max_chars=60000, max_blocks=12, continue_backlog=False):
    if queue.get('source_md_sha256') != hashlib.sha256(md.encode()).hexdigest():
        raise ValueError('source_version_changed')
    result = deepcopy(queue)
    packets = [p for p in result.get('packets', []) if p.get('finding_id') == finding_id]
    if len(packets) != 1 or packets[0].get('status') != 'pending':
        raise ValueError('missing_or_ambiguous_pending_packet')
    packet = packets[0]
    revision = packet.get('context_revision', 0)
    if revision and not continue_backlog:
        raise ValueError('context_followup_limit')
    if revision >= MAX_CONTEXT_REVISIONS:
        raise ValueError('context_followup_limit')
    previous = packet.get('context_search_receipt', {})
    if continue_backlog:
        if not revision or not (previous.get('backlog') or previous.get('deferred_queries')):
            raise ValueError('no_search_backlog')
        # Continue the model's recorded search, not new evaluator-provided hints.
        recorded = list(previous.get('query_hit_counts', {}))
        deferred = list(previous.get('deferred_queries', []))
        queries = recorded + deferred if previous.get('backlog') else deferred
    remaining_chars = MAX_TOTAL_TEXT_CHARS - sum(len(s['authoritative_text']) for s in packet['sources'])
    if remaining_chars <= 0:
        raise ValueError('context_total_budget')
    max_chars = min(max_chars, remaining_chars)
    existing = {s['block_id'] for s in packet['sources']} | set(packet.get('required_graphic_block_ids', []))
    search = search_md_context(md, queries, max_chars=max_chars, max_blocks=max_blocks, exclude_block_ids=existing,
                               prior_backlog_ids=[h['block_id'] for h in previous.get('backlog', [])] if continue_backlog else ())
    blocks = authoritative_text_blocks(md)
    headers = re.findall(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', md, re.M | re.I)
    counts = Counter(bid for _, bid in headers)
    graphics = {bid for kind, bid in headers if kind.upper() in ('IMAGE', 'GRAPHIC') and counts[bid] == 1}
    added, rejected = [], []
    for hit in search['hits']:
        bid = hit['block_id']
        if bid in existing:
            continue
        if bid in blocks:
            packet['sources'].append({'block_id': bid, 'authoritative_text': blocks[bid]})
        elif bid in graphics:
            packet.setdefault('required_graphic_block_ids', []).append(bid)
            packet['unresolved_block_ids'].append(bid)
        else:
            rejected.append(bid)
            continue
        packet.setdefault('reference_origins', {}).setdefault(bid, []).append('context_retrieval')
        existing.add(bid)
        added.append(bid)
    # Keep the search trace, but never place generated IMAGE summaries among TEXT evidence.
    packet['context_search_receipt'] = {**search, 'hits': [{k: v for k, v in h.items() if k != 'content'} for h in search['hits']],
                                       'added_block_ids': added, 'rejected_ambiguous_block_ids': rejected}
    if previous:
        packet.setdefault('context_search_history', []).append(previous)
    packet['context_revision'] = revision + 1
    packet['context_sha256'] = hashlib.sha256(json.dumps({
        'sources': packet['sources'], 'required_graphics': packet.get('required_graphic_block_ids', []),
        'search_receipt': packet['context_search_receipt']}, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    if packet.get('required_graphic_block_ids') or rejected or search['backlog'] or search['deferred_queries']:
        packet['source_context_status'] = 'needs_data'
    # Existing image receipts must be rebuilt for the expanded set before dispatch.
    for key in ('graphic_attachments', 'unavailable_graphics', 'graphic_context_status'):
        packet.pop(key, None)
    return result
