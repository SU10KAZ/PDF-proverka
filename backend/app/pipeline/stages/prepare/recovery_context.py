"""Bounded literal search over authoritative prepared MD, never PDF re-extraction."""
from copy import deepcopy
import hashlib
import json
import re
from pathlib import Path


def expand_operation_queries(queries):
    """Extract navigation keys explicitly present in a model's context request.

    No guessed synonym, corrected OCR, or inferred object identity is introduced.
    The original queries and the derivation of each search key stay auditable.
    """
    expanded, derivations = [], []
    for query in queries if isinstance(queries, list) else []:
        if not isinstance(query, str) or not 2 <= len(query.strip()) <= 160:
            expanded.append(query)
            continue
        keys = []
        for match in re.finditer(r'(?<!\w)[A-ZА-ЯЁ]{2,8}[ -]?\d+(?:[./-]\d+)*(?!\w)', query):
            keys.append((match.start(), match.group(), 'explicit_mark'))
        for match in re.finditer(r'\bлист(?:е|а|ы|ах|ов)?\s+(\d+)(?:\s*[-–—]\s*(\d+))?', query, re.I):
            key = 'Лист '+match[1] if match[2] is None else 'Листы '+match[1]+'–'+match[2]
            keys.append((match.start(), key, 'explicit_sheet_reference'))
        for _, key, kind in sorted(keys):
            derivations.append({'original_query': query, 'search_key': key, 'kind': kind})
            if key not in expanded:
                expanded.append(key)
        if query not in expanded:
            expanded.append(query)
    return {'queries': expanded, 'derivations': derivations,
            'identity_verified': False, 'absence_proven': False}



def _dimension_navigation_text(text):
    # Navigation only: typographic multiplication signs between digits. Never
    # normalize object marks, cable execution suffixes, or authoritative content.
    return re.sub(r"(?<=\d)\s*[xх×]\s*(?=\d)", "×", text.casefold())


def search_md_context(md, queries, *, max_chars=60000, max_blocks=12, exclude_block_ids=(), prior_backlog_ids=(), source_graph=None):
    sheet_pages = {}
    if source_graph is not None:
        if source_graph.get('source_md_sha256') != hashlib.sha256(md.encode()).hexdigest():
            raise ValueError('context_graph_version_changed')
        from .graph_builder import build_page_sheet_map
        for page, sheet in build_page_sheet_map(source_graph).items():
            if sheet.isdigit():sheet_pages.setdefault(int(sheet), []).append(page)
    def requested_sheet_pages(query):
        match = re.fullmatch(r'листы?\s+(\d+)(?:\s*[-–—]\s*(\d+))?\.?', query, re.I)
        if not match:return set()
        first=int(match.group(1));last=int(match.group(2) or first)
        if last<first or last-first>100:return set()
        return {page for sheet in range(first,last+1) for page in sheet_pages.get(sheet, [])}
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
        preceding = [p for p in pages if p.start() < h.start()]
        page_num = int(preceding[-1].group(1)) if preceding else None
        # Ranking only: retain substring matches, including metadata, in the
        # complete candidate set. Exact node/mark tokens should precede address
        # fragments (e.g. 'по А' inside 'по адресу'). Source text is unchanged.
        body = re.sub(r'^>[ \t]*\*\*(?:Created|Crop|Stamp):\*\*[^\n]*$', '', content,
                      flags=re.M | re.I)
        stamp_match = re.search(r'^>[ \t]*\*\*Stamp:\*\*([^\n]*)', content, re.M)
        stamp = stamp_match.group(1) if stamp_match else ''
        provenance = {}
        for field in ('Code', 'Stage', 'Sheet', 'Revisions'):
            value = re.search(r'(?:^|\|)\s*' + field + r':\s*([^|]+)', stamp)
            if value:
                provenance[field.lower()] = value.group(1).strip()
        match_kinds = {}
        for q in valid:
            # A compact drawing reference is a navigation hypothesis, not proof
            # that a same-numbered sheet from another stage is applicable.
            scoped_sheet = re.fullmatch(r'(\S+)\s+лист(?:е|а)?\s+(\d+(?:\.\d+)?)', q, re.I)
            node_ref = re.fullmatch(r'(?:по\s+)?([А-ЯЁA-Z])\s*\((\d+)\)', q, re.I)
            if (scoped_sheet and provenance.get('code', '').casefold() == scoped_sheet[1].casefold()
                    and provenance.get('sheet') == scoped_sheet[2]):
                # Exact prepared-MD metadata only: no OCR repair, code shortening,
                # stage equivalence or claim that a same-numbered sheet applies.
                match_kinds[q] = 'document_sheet_navigation'
            elif (node_ref and provenance.get('sheet') == node_ref[2]
                    and re.search(r'(?<!\w)' + re.escape(node_ref[1]) + r'(?!\w)', body, re.I)):
                match_kinds[q] = 'node_sheet_navigation'
            elif page_num in requested_sheet_pages(q):
                match_kinds[q] = 'sheet_reference'
            elif q.casefold() in content.casefold():
                pattern = r'(?<!\w)' + re.escape(q.casefold()) + r'(?!\w)'
                if re.search(pattern, body.casefold()):
                    match_kinds[q] = 'body_token'
                elif re.search(pattern, content.casefold()):
                    match_kinds[q] = 'metadata_token'
                else:
                    match_kinds[q] = 'substring'
            elif _dimension_navigation_text(q) in _dimension_navigation_text(content):
                match_kinds[q] = 'dimension_format_navigation'
        matches = list(match_kinds)
        if not matches:
            continue
        for q in matches:
            query_hits[q] += 1
        preceding = [p for p in pages if p.start() < h.start()]
        row = {'block_id': h.group(2), 'kind': h.group(1).upper(),
               'page': int(preceding[-1].group(1)) if preceding else None,
               'queries': matches, 'query_match_kinds': match_kinds, 'start_line': md.count('\n', 0, h.start())+1,
               'source_provenance': provenance, 'reference_applicability_verified': False}
        if row['block_id'] in exclude_block_ids:
            already_supplied.append(row)
            continue
        matches_all.append({**row, 'content': content})
    # Round-robin across queries prevents common phrases from starving exact
    # marks that occur later in the document. Source order remains stable within
    # each query; no text is truncated or synthesized.
    priorities = {'document_sheet_navigation': -2, 'node_sheet_navigation': -1, 'sheet_reference': 0, 'body_token': 0, 'dimension_format_navigation': 0, 'metadata_token': 1, 'substring': 2}
    queues = [sorted((i for i, hit in enumerate(matches_all) if q in hit['queries']),
                     key=lambda i: priorities[matches_all[i]['query_match_kinds'][q]]) for q in valid]
    order, seen = [], set()
    for depth in range(max((len(q) for q in queues), default=0)):
        for queue in queues:
            if depth < len(queue) and queue[depth] not in seen:
                seen.add(queue[depth])
                order.append(queue[depth])
    # Resume previously found hits in their recorded order before new matches.
    # IDs only reorder literal matches; they cannot introduce unmatching sources.
    prior_rank = {bid: rank for rank, bid in enumerate(prior_backlog_ids)}
    # Resume within each relevance tier, never let an address-only backlog
    # displace a newly found exact node. No matching source is discarded.
    order.sort(key=lambda i: (
        min(priorities[k] for k in matches_all[i]['query_match_kinds'].values()),
        prior_rank.get(matches_all[i]['block_id'], len(prior_rank))))
    for i in order:
        row = matches_all[i]
        if used + len(row['content']) > max_chars or len(hits) >= max_blocks:
            backlog.append({k: v for k, v in row.items() if k != 'content'} | {'reason': 'context_budget'})
            continue
        hits.append(row)
        used += len(row['content'])
    return {'hits': hits, 'backlog': backlog, 'query_hit_counts': query_hits,
            'rejected_queries': rejected, 'deferred_queries': deferred_queries,
            'already_supplied': already_supplied, 'absence_proven': False, 'sheet_lookup_enabled':source_graph is not None, 'boundary': 'Literal MD search, numeric dimension typography navigation and optional same-version sheet lookup; navigation does not prove identity or equivalent execution. Zero matches do not establish absence.'}


def build_context_followup(request, payload):
    """At most one follow-up, only on new source content from a hash-bound MD."""
    from .codex_targeted_findings import CodexTargetedPass
    if request.stage.endswith('_context'):
        return None, {'status': 'followup_limit'}
    packet = json.loads(request.messages[-1]['content'])
    if packet.get('recovery_format_repair'):
        return None, {'status': 'format_repair_no_context_expansion'}
    decisions = payload.get('candidate_decisions', [])
    queries = []
    group = packet.get('candidate_group', [packet.get('candidate', {})])
    known = {(c.get('origin'), c.get('pointer')): c for c in group}
    unresolved = []
    for decision in decisions if isinstance(decisions, list) else []:
        if not isinstance(decision, dict):
            continue
        key = (decision.get('origin'), decision.get('pointer'))
        if not all(isinstance(k, str) for k in key) or key not in known:
            continue
        claims = decision.get('subclaims', [])
        records = [decision, *(c for c in claims if isinstance(c, dict))] if isinstance(claims, list) else [decision]
        missing = [r for r in records if r.get('decision') == 'needs_data']
        if not missing:
            continue
        if known[key] not in unresolved:
            unresolved.append(known[key])
        for record in missing:
            values = record.get('context_queries', [])
            if isinstance(values, list):
                queries.extend(q for q in values if isinstance(q, str) and q.strip())
    if not queries:
        return None, {'status': 'no_explicit_context_queries'}
    source = packet.get('md_source', {})
    path = Path(source.get('path', ''))
    if not path.is_file():
        return None, {'status': 'md_unavailable'}
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != source.get('sha256'):
        return None, {'status': 'md_version_changed'}
    existing_ids = set(re.findall(r'^###[ \t]+BLOCK[^\n]*?\[(?:TEXT|TABLE)\]:[ \t]*(\S+)',
                                  packet['authoritative_md'], re.M | re.I))
    existing_ids.update(packet.get('attached_image_block_ids_in_order', []))
    report = search_md_context(raw.decode('utf-8'), queries, exclude_block_ids=existing_ids)
    new = report['hits']
    if not new:
        return None, {'status': 'no_new_context', 'search': report}
    # The previous answer may split a compound obligation into new stable IDs.
    # Carry those missing parts explicitly; a previous_decisions narrative alone
    # is not an executable publication obligation. Preserve omitted older parts.
    refreshed = []
    for candidate in unresolved:
        candidate = deepcopy(candidate)
        obligations = candidate.get('pending_subclaims', [])
        obligations = deepcopy(obligations) if isinstance(obligations, list) else []
        for decision in decisions:
            if not isinstance(decision, dict) or (decision.get('origin'), decision.get('pointer')) != (candidate.get('origin'), candidate.get('pointer')):
                continue
            claims = decision.get('subclaims', [])
            for claim in claims if isinstance(claims, list) else []:
                if not isinstance(claim, dict) or claim.get('decision') != 'needs_data' or not isinstance(claim.get('id'), str):
                    continue
                obligations = [o for o in obligations if not (isinstance(o, dict) and isinstance(o.get('claim'), dict) and o['claim'].get('id') == claim['id'])]
                obligations.append({'reason': 'needs_data', 'claim': deepcopy(claim)})
        candidate['pending_subclaims'] = obligations
        refreshed.append(candidate)
    unresolved = refreshed
    packet['candidate_group'] = unresolved
    packet['candidate'] = unresolved[0]
    packet['retrieved_context'] = new
    packet['context_search_receipt'] = report
    packet['reference_navigation_boundary'] = (
        'Retrieved references are navigation candidates, not established applicability. '
        'Compare source Code, Stage, Sheet and Revisions with the original candidate. '
        'A matching node/sheet number does not establish that a different stage or '
        'revision applies. If the relationship is unproved, retain needs_data; '
        'do not use it to confirm or refute the claim.')
    packet['previous_decisions'] = deepcopy(decisions)
    from ..findings_merge.recovery_parent_targets import carry_parent_targets
    carry_parent_targets(packet, request.stage, payload)
    # Attach only local, explicitly indexed crops from the same frozen index.
    # A summary or an unavailable crop never becomes graphic evidence.
    images = list(request.image_paths)
    image_ids = list(packet.get('attached_image_block_ids_in_order', []))
    crop_source = packet.get('crop_source', {})
    crop_dir = Path(crop_source.get('directory', '')).resolve()
    index_path = crop_dir / 'index.json'
    files = {}
    crop_status = 'index_unavailable_or_changed'
    if index_path.is_file():
        index_raw = index_path.read_bytes()
        if hashlib.sha256(index_raw).hexdigest() == crop_source.get('index_sha256'):
            files = {b.get('block_id'): b.get('file') for b in json.loads(index_raw).get('blocks', []) if isinstance(b, dict)}
            crop_status = 'index_verified'
    unavailable, attached = [], []
    for hit in new:
        if hit['kind'] not in {'IMAGE', 'GRAPHIC'}:
            continue
        bid = hit['block_id']
        name = files.get(bid)
        path = (crop_dir / name).resolve() if isinstance(name, str) else None
        if path is None or not path.is_relative_to(crop_dir) or not path.is_file() or len(images) >= 8:
            unavailable.append(bid)
            continue
        images.append(path)
        image_ids.append(bid)
        attached.append({'block_id': bid, 'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    packet['attached_image_block_ids_in_order'] = image_ids
    packet['context_image_receipt'] = {'status': crop_status, 'attached': attached, 'unavailable': unavailable}
    packet['allowed_source_refs'] = sorted(set(packet['allowed_source_refs']) | set(image_ids) | {
        h['block_id'] for h in new if h['kind'] in {'TEXT', 'TABLE'}})
    followup = CodexTargetedPass(stage=request.stage+'_context',
        output_filename=request.output_filename.replace('.json', '_context.json'),
        messages=[request.messages[0], {'role': 'user', 'content': json.dumps(packet, ensure_ascii=False)}],
        image_paths=tuple(images))
    return followup, {'status': 'scheduled', 'search': report, 'images': packet['context_image_receipt']}
