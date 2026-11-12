"""Offline navigation proposal only. Never edits findings or invokes a model."""
from collections import defaultdict


def propose_navigation(finding, graph):
    """Use explicit evidence blocks; reject unknown/ambiguous graph bindings.

    The caller must bind graph and finding to the same frozen document version.
    Context-only related/selected blocks do not determine finding location.
    """
    refs = {str(e['block_id']) for e in finding.get('evidence', [])
            if isinstance(e, dict) and e.get('block_id')}
    if not refs:
        return {'status': 'unresolved', 'reason': 'no_explicit_evidence_blocks', 'patch': {}}
    locations = defaultdict(set)
    for page in graph.get('pages', []):
        number = page.get('page')
        if type(number) is not int or number < 1:
            continue
        for key in ('text_blocks', 'image_blocks', 'blocks'):
            for block in page.get(key) or []:
                if isinstance(block, dict):
                    bid = block.get('id') or block.get('block_id')
                    if bid:
                        locations[str(bid)].add(number)
    missing = sorted(refs - locations.keys())
    ambiguous = sorted(b for b in refs if len(locations[b]) > 1)
    if missing or ambiguous:
        return {'status': 'unresolved', 'reason': 'incomplete_or_ambiguous_binding',
                'missing_blocks': missing, 'ambiguous_blocks': ambiguous, 'patch': {}}
    pages = sorted({p for b in refs for p in locations[b]})
    page_value = pages[0] if len(pages) == 1 else pages
    # Sheet labels need the production metadata parser; this proposal only
    # repairs PDF page bindings, including nested evidence.page values.
    patch = {}
    if finding.get('page') != page_value:
        patch['page'] = page_value
    evidence_pages = []
    for i, evidence in enumerate(finding.get('evidence', [])):
        if not isinstance(evidence, dict) or not evidence.get('block_id'):
            continue
        page = next(iter(locations[str(evidence['block_id'])]))
        if evidence.get('page') != page:
            evidence_pages.append({'index': i, 'page': page})
    if evidence_pages:
        patch['evidence_pages'] = evidence_pages
    return {'status': 'proposed' if patch else 'consistent', 'pages': pages,
            'patch': patch, 'scope': 'PDF pages only; no semantic acceptance'}
