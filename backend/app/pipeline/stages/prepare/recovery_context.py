"""Bounded literal search over authoritative prepared MD, never PDF re-extraction."""
import hashlib
import json
import re
from pathlib import Path


def search_md_context(md, queries, *, max_chars=60000, max_blocks=12, exclude_block_ids=()):
    valid, rejected = [], []
    for query in queries if isinstance(queries, list) else []:
        if isinstance(query, str) and 2 <= len(query.strip()) <= 160:
            if query.strip() not in valid:
                valid.append(query.strip())
        else:
            rejected.append(query)
    deferred_queries = valid[6:]
    valid = valid[:6]
    headers = list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', md, re.M | re.I))
    pages = list(re.finditer(r'^##[ \t]+(?:Page|СТРАНИЦА)[ \t]+(\d+)\b[^\n]*$', md, re.M | re.I))
    hits, backlog, used = [], [], 0
    matches_all, already_supplied = [], []
    query_hits = {q: 0 for q in valid}
    for i, h in enumerate(headers):
        end = headers[i+1].start() if i+1 < len(headers) else len(md)
        following_pages = [p.start() for p in pages if h.start() < p.start() < end]
        if following_pages:
            end = min(following_pages)
        content = md[h.start():end]
        matches = [q for q in valid if q.casefold() in content.casefold()]
        if not matches:
            continue
        for q in matches:
            query_hits[q] += 1
        preceding = [p for p in pages if p.start() < h.start()]
        row = {'block_id': h.group(2), 'kind': h.group(1).upper(),
               'page': int(preceding[-1].group(1)) if preceding else None,
               'queries': matches, 'start_line': md.count('\n', 0, h.start())+1}
        if row['block_id'] in exclude_block_ids:
            already_supplied.append(row)
            continue
        matches_all.append({**row, 'content': content})
    # Round-robin across queries prevents common phrases from starving exact
    # marks that occur later in the document. Source order remains stable within
    # each query; no text is truncated or synthesized.
    queues = [[i for i, hit in enumerate(matches_all) if q in hit['queries']] for q in valid]
    order, seen = [], set()
    for depth in range(max((len(q) for q in queues), default=0)):
        for queue in queues:
            if depth < len(queue) and queue[depth] not in seen:
                seen.add(queue[depth])
                order.append(queue[depth])
    for i in order:
        row = matches_all[i]
        if used + len(row['content']) > max_chars or len(hits) >= max_blocks:
            backlog.append({k: v for k, v in row.items() if k != 'content'} | {'reason': 'context_budget'})
            continue
        hits.append(row)
        used += len(row['content'])
    return {'hits': hits, 'backlog': backlog, 'query_hit_counts': query_hits,
            'rejected_queries': rejected, 'deferred_queries': deferred_queries,
            'already_supplied': already_supplied, 'absence_proven': False, 'boundary': 'Literal MD search only; zero matches do not establish absence.'}


def build_context_followup(request, payload):
    """At most one follow-up, only on new source content from a hash-bound MD."""
    from .codex_targeted_findings import CodexTargetedPass
    if request.stage.endswith('_context'):
        return None, {'status': 'followup_limit'}
    packet = json.loads(request.messages[-1]['content'])
    decisions = payload.get('candidate_decisions', [])
    queries = []
    group = packet.get('candidate_group', [packet.get('candidate', {})])
    known = {(c.get('origin'), c.get('pointer')): c for c in group}
    unresolved = []
    for decision in decisions if isinstance(decisions, list) else []:
        if isinstance(decision, dict) and decision.get('decision') == 'needs_data':
            key = (decision.get('origin'), decision.get('pointer'))
            if not all(isinstance(k, str) for k in key) or key not in known:
                continue
            if known[key] not in unresolved:
                unresolved.append(known[key])
            values = decision.get('context_queries', [])
            if isinstance(values, list):
                queries.extend(values)
    if not queries:
        return None, {'status': 'no_explicit_context_queries'}
    source = packet.get('md_source', {})
    path = Path(source.get('path', ''))
    if not path.is_file():
        return None, {'status': 'md_unavailable'}
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != source.get('sha256'):
        return None, {'status': 'md_version_changed'}
    existing_ids = set(re.findall(r'^###[ \t]+BLOCK[^\n]*?\[[^]]+\]:[ \t]*(\S+)',
                                  packet['authoritative_md'], re.M | re.I))
    report = search_md_context(raw.decode('utf-8'), queries, exclude_block_ids=existing_ids)
    new = [h for h in report['hits'] if h['content'] not in packet['authoritative_md']]
    if not new:
        return None, {'status': 'no_new_context', 'search': report}
    packet['candidate_group'] = unresolved
    packet['candidate'] = unresolved[0]
    packet['retrieved_context'] = new
    packet['context_search_receipt'] = report
    packet['previous_decisions'] = decisions
    # GRAPHIC summaries remain navigation hints, not evidence without an image.
    packet['allowed_source_refs'] = sorted(set(packet['allowed_source_refs']) | {
        h['block_id'] for h in new if h['kind'] in {'TEXT', 'TABLE'}})
    followup = CodexTargetedPass(stage=request.stage+'_context',
        output_filename=request.output_filename.replace('.json', '_context.json'),
        messages=[request.messages[0], {'role': 'user', 'content': json.dumps(packet, ensure_ascii=False)}],
        image_paths=request.image_paths)
    return followup, {'status': 'scheduled', 'search': report}
