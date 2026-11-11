"""Resolve explicitly selected final citations; never infer support from proximity."""
from collections import Counter
from copy import deepcopy
import re

from backend.app.pipeline.stages.prepare.graph_builder import build_md_line_page_index, page_for_md_line


def final_reference_updates(response, decision, md):
    """Optional upgraded contract. Old receipts remain readable without invented refs."""
    refs = decision.get('final_evidence_refs')
    if refs is None:
        return {}
    if decision.get('action') not in ('retain', 'revise', 'question'):
        raise ValueError('final_evidence_requires_visible_finding')
    if not isinstance(refs, list) or not refs:
        raise ValueError('empty_final_evidence_refs')
    claims = {c['claim_id']: c for c in response['claims']}
    reviews = {c['claim_id']: c for c in decision['claim_reviews']}
    headings = list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', md, re.M | re.I))
    counts = Counter(h.group(2) for h in headings)
    page_index = build_md_line_page_index(md)
    pages = {h.group(2): page_for_md_line(page_index, md.count('\n', 0, h.start()) + 1)
             for h in headings if counts[h.group(2)] == 1}
    sheets = {}
    for line in md.splitlines():
        match = re.match(r'^## Page (\d+)\s+[—–-]\s+Sheet\s+(.+?)(?:\s+[—–-]\s+.*)?$', line)
        if match:
            sheets[int(match[1])] = match[2].strip()
    selected = []; seen = set()
    for ref in refs:
        if not isinstance(ref, dict) or set(ref) != {'claim_id', 'evidence_index'}:
            raise ValueError('invalid_final_evidence_ref')
        cid, index = ref['claim_id'], ref['evidence_index']
        if not isinstance(cid, str) or type(index) is not int or cid not in claims:
            raise ValueError('invalid_final_evidence_ref')
        evidence = claims[cid].get('evidence', [])
        if not 0 <= index < len(evidence) or (cid, index) in seen:
            raise ValueError('invalid_final_evidence_ref')
        seen.add((cid, index))
        if reviews[cid]['verdict'] != 'supported':
            raise ValueError('final_evidence_from_unresolved_or_refuted_claim')
        item = deepcopy(evidence[index]); bid = item.get('block_id')
        if bid not in pages:
            raise ValueError('final_evidence_source_ambiguous')
        # Never trust a page number supplied by the model.
        item.pop('page', None)
        if pages[bid] is not None:
            item['page'] = pages[bid]
        if item not in selected:
            selected.append(item)
    updates = {'evidence': selected, 'source_review_final_evidence_refs': deepcopy(refs)}
    known_pages = sorted({e['page'] for e in selected if 'page' in e})
    # Replace location only when every selected citation has a known location.
    if len([e for e in selected if 'page' in e]) == len(selected):
        updates['page'] = known_pages
        if all(page in sheets for page in known_pages):
            updates['sheet'] = 'Листы ' + ', '.join(dict.fromkeys(sheets[p] for p in known_pages))
    return updates
