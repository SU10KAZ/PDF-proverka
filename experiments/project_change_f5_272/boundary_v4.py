"""Only subject, continuation, note and document corrections to V3 observations.

The boundary decision function and its native table/topology/part predicates
are unchanged. Typed UNKNOWN still blocks certification.
"""
from .boundary_sources import observe, compact, delivered_part


def observation(req, row, doc, evidence, candidate, analysis, guard):
    o = observe(req, row, doc, evidence, candidate['functional_key'])
    if guard and guard['usable'] == 'NO':
        return o | dict(usable=False, wrong_subject=True,
            document_identity=guard, unusable_reason='WRONG_DOCUMENT_OR_CIPHER')
    if candidate['subject_confidence'] == 'SUBJECT_UNRESOLVED':
        o.update(subject_found=False, claim_inside=False,
                 identity_status='SUBJECT_UNRESOLVED', identity_probe_only=True)
    if not analysis:
        return o
    parts = []
    if analysis['requirement'] == 'REQUIRED':
        for page in analysis['required_pages']:
            if page in doc.text:
                text = compact(doc.text[page]['native_text'])
                parts.append(delivered_part(doc, page, 0, len(text), 'continuation', evidence))
        complete = bool(parts) and len(parts) == len(analysis['required_pages']) and all(p['delivered'] for p in parts)
        continuation = dict(status='COMPLETE' if complete else 'MISSING',
            proof=analysis, parts=parts, checked_pages=analysis['required_pages'])
    elif analysis['requirement'] == 'NOT_REQUIRED':
        continuation = dict(status='CHECKED_NONE', proof=analysis, parts=[])
    else:
        continuation = dict(status='UNKNOWN', proof=analysis, parts=[])
    notes = []
    for n in analysis['notes']:
        item = dict(relevance='NOT_APPLICABLE' if n['status'] in {'NO_NOTE', 'IRRELEVANT'} else n['status'],
            reason=n['reason'], applicability=n, delivered=False)
        if n['status'] == 'RELEVANT' and req['page'] in doc.text:
            native = compact(doc.text[req['page']]['native_text'])
            selected = compact(n['text'])
            pos = native.find(selected)
            if selected and pos >= 0:
                item.update(delivered_part(doc, req['page'], pos, pos + len(selected), 'note', evidence))
        notes.append(item)
    o.update(continuation=continuation, notes=notes, notes_examined=True,
        early_layer_correction=dict(subject_confidence=candidate['subject_confidence'],
            continuation_analysis=analysis, boundary_predicates='UNCHANGED_V3'))
    if req['required_type'] in {'GRAPHIC_REGION', 'GRAPHIC_SCHEME'} and o.get('topology'):
        # Delivery uses a full-page raster; the inventory region is only a
        # locator. V3 treated crossings of that locator as raster truncation.
        # Functional endpoint/edge/label proof remains absent, hence incomplete.
        o['topology'] = o['topology'] | dict(cropped_connections=[])
    return o
