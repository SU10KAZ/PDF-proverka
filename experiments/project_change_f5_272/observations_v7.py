"""Read frozen V6 payload/proofs only; never open a PDF or retrieve a page."""
import re

from .boundary_v7 import RelevantRowGroup, LocalFunctionalFragment, certify
from dataclasses import asdict
from .common import fingerprint


def compact(text):
    return re.sub(r'\s+', '', text)


def native_text(evidence):
    quote = evidence.get('quote', '')
    if not quote.startswith('PDF NATIVE:\n'):
        return None
    # The frozen serializer inserts one separator newline before the OCR label.
    return quote.removeprefix('PDF NATIVE:\n').split('\nOCR (fallible):', 1)[0]


def part(role, req, evidence, text=None, **extra):
    locator = dict(document_version=req['document_version'], side=req['side'], page=req['page'],
                   evidence_id=evidence['evidence_id'], source_pdf_sha256=evidence['source_receipt']['sha256'])
    if text is not None:
        locator['quote_compact_offset'] = compact(evidence['quote']).find(compact(text))
    return dict(role=role, required=True, delivered=True, verified=True,
        evidence_ids=[evidence['evidence_id']], source_locator=locator,
        source_hash=fingerprint(text) if text is not None else evidence['raster']['sha256'], **extra)


def legacy_part(p):
    return dict(p, required=True, verified=p.get('native_verified', False))


def scoped_notes(old, scoped_text, req, evidence):
    result = []
    # Signals must occur inside this claim's text, not somewhere on its page.
    for match in re.finditer(r'\*|\[\d+\]|\bсм\.|примечани\w*|сноск\w*', scoped_text, re.I):
        result.append(dict(reference=match.group(), reason='Explicit reference inside claim text'))
    for n in old.get('notes', []):
        app = n.get('applicability', {})
        if n.get('same_subject') or n.get('other_subject'):
            result.append(n)
        elif n.get('relevance') == 'RELEVANT':
            p = legacy_part(n) if n.get('source_locator') else None
            result.append(dict(same_subject=True, reason=n.get('reason'), part=p))
        elif app.get('status') == 'IRRELEVANT':
            result.append(dict(other_subject=True, reason=app.get('reason')))
        elif app.get('text') and compact(app['text']) in compact(scoped_text):
            result.append(dict(proximity='Note text occurs inside claim fragment', reason=app.get('reason')))
    return result


def support_rows(package, req):
    subjects = [package['canonical_subjects'][sid]
                for sid in req['provenance'].get('discovery_subject_ids', [])]
    rows = [(s, r) for s in subjects for r in s['evidence_support']
            if r.get('accepted') and r['page'] == req['page']]
    return subjects, rows


def annotation_observation(package, req, old, evidence, rows, claim):
    """Local *position and label* is a distinct already-frozen subject scope.

    A bare mark is insufficient. Require every accepted native label with its
    position, the entire hash-matching native page, and a verified full raster.
    This certifies annotation placement, never equipment/node connectivity.
    """
    if (claim['scope_kind'] != 'LOCAL_POSITION_LABEL' or not evidence
            or req['required_type'] not in {'TEXT_SECTION', 'GRAPHIC_REGION'}):
        return None
    graphics = [r for _, r in rows if r['source_type'] == 'GRAPHIC']
    labels = [label for r in graphics for label in r.get('native_label_locators', [])]
    if not labels:
        return None
    e = evidence[0]
    native = native_text(e)
    graphic_proofs = [c['proof'] for c in package['boundary_certificates']
                      if c['evidence_type'] in {'GRAPHIC_REGION', 'GRAPHIC_SCHEME'}
                      and e['evidence_id'] in c['evidence_ids']]
    full_page = any(p.get('continuation', {}).get('proof', {}).get('source_facts', {}).get(
                    'delivered_raster_extent') == 'FULL_PAGE' for p in graphic_proofs)
    valid = bool(native is not None and graphics and e.get('raster') and full_page
                 and all(fingerprint(native) == r['native_sha256'] for r in graphics))
    valid &= all(compact(l['text']) in compact(native or '') and
                 len(re.findall(r'[А-Яа-яЁёA-Za-z]{2,}', l['text'])) >= 2 and
                 len(l.get('bbox_norm', [])) == 4 and
                 0 < l['bbox_norm'][0] < l['bbox_norm'][2] < 1 and
                 0 < l['bbox_norm'][1] < l['bbox_norm'][3] < 1 for l in labels)
    if not valid:
        return None
    parts = [part('annotation', req, e, l['text'], bbox_norm=l['bbox_norm']) for l in labels]
    parts.append(part('raster', req, e))
    text = '\n'.join(l['text'] for l in labels)
    fragment = LocalFunctionalFragment('ANNOTATED_POSITION', claim['claim_id'], [], [], [], labels,
                                       True, True, True, False)
    return dict(old, subject_found=True, start=parts[0]['source_locator'] | dict(bbox_norm=labels[0]['bbox_norm']),
        end=parts[-2]['source_locator'] | dict(bbox_norm=labels[-1]['bbox_norm']), parts=parts,
        claim_content_complete=True, annotation_binding_verified=True, fragment=asdict(fragment),
        continuation_facts={}, notes=scoped_notes(old, text, req, e),
        semantic_extent_reason=None,
        sufficiency_reason='Every native annotation and its coordinates are inside the delivered full-page raster; '
                           'the entire native page matches the frozen support hash. The existing subject scope '
                           'is LOCAL_POSITION_LABEL, explicitly excluding system topology.')


def text_observation(req, old, evidence, rows):
    supports = [r for _, r in rows if r['region_id'] == req['scope_binding'] and r['source_type'] == 'TEXT']
    if not evidence or not supports:
        return None
    e = evidence[0]
    native = native_text(e)
    if native is None or not all(fingerprint(native) == r['native_sha256'] for r in supports):
        return None
    snippets = list(dict.fromkeys(t for r in supports for t in r['snippets']))
    if not snippets or not all(compact(t) in compact(native) for t in snippets):
        return None
    # Keep *all* accepted snippets. Never cherry-pick a finished sentence from
    # an unfinished list, condition, or paragraph for the same requirement.
    finished = all(re.search(r'[.!?][»\")]*$', t.rstrip()) and
                   re.match(r'[А-ЯЁA-Z]', t.lstrip()) and
                   len(re.findall(r'[А-Яа-яЁёA-Za-z]{2,}', t)) >= 6 for t in snippets)
    if not finished:
        return None
    parts = [part('paragraph', req, e, t) for t in snippets]
    text = '\n'.join(snippets)
    return dict(old, subject_found=True, start=parts[0]['source_locator'],
        end=parts[-1]['source_locator'] | dict(quote_compact_length=len(compact(snippets[-1]))),
        parts=parts, claim_content_complete=True, semantic_extent_reason=None,
        continuation_facts=dict(paragraph_complete=True),
        notes=scoped_notes(old, text, req, e),
        sufficiency_reason='All accepted native subject paragraphs on the requirement page are delivered in full, '
                           'sentence-closed, with the entire native page hash verified; no scoped continuation signal.')


def baseline_observation(req, cert):
    old = cert['proof']
    source = old.get('continuation', {}).get('proof', {})
    facts = source.get('source_facts', {}) if isinstance(source, dict) else {}
    facts = dict(facts)
    # Keep proven dependencies, distinguish them from legacy page/section guesses.
    rename = dict(broken_row='split_row', needed_relation_clipped='connection_off_page',
                  off_page_connector='connection_off_page', previous_page_required='starts_mid_sentence')
    for a, b in rename.items():
        if facts.get(a):
            facts[b] = True
    if old.get('continuation', {}).get('status') == 'COMPLETE':
        facts.update(continuation_verified=True, continuation_parts=[legacy_part(p)
                     for p in old['continuation'].get('parts', [])])
    notes = []
    for n in old.get('notes', []):
        app = n.get('applicability', {})
        if n.get('relevance') == 'RELEVANT':
            notes.append(dict(same_subject=True, reason=n.get('reason'), part=legacy_part(n)))
        elif app.get('status') == 'IRRELEVANT':
            notes.append(dict(other_subject=True, reason=app['reason']))
        elif n.get('reference_texts'):
            notes.append(dict(reference=n['reference_texts'], reason=n.get('reason')))
    parts = [legacy_part(p) for p in old.get('parts', [])]
    # Preserve unverified content as a part; an empty list must not disappear.
    if not parts:
        parts = [dict(role='claim_content', required=True, delivered=cert['delivered'], verified=False,
                      evidence_ids=cert['evidence_ids'], source_locator=dict(page=req['page']),
                      source_hash=None)]
    g = old.get('topology', {})
    fragment = asdict(LocalFunctionalFragment('CONNECTION', g.get('subject_node', ''),
        g.get('nodes', []), g.get('edges', []), g.get('required_nodes', []),
        old.get('native_label_locators', []) if g.get('labels_bound') else [],
        bool(g.get('labels_bound') and g.get('source_geometry_hash')), bool(g.get('boundary_checked')),
        bool(g.get('boundary_checked')), bool(g.get('cropped_connections') or facts.get('connection_off_page'))))
    group = asdict(RelevantRowGroup(
        next((p for p in parts if p.get('role') == 'identity'), {}),
        [p for p in parts if p.get('role') == 'columns'], old.get('expected_row_ids', []),
        [p.get('row_id') for p in parts if p.get('role') == 'row' and p.get('verified') and p.get('delivered')],
        [], old.get('end') if old.get('row_group_end_proven') else None,
        bool(old.get('native_cell_mapping_verified'))))
    return dict(delivered=cert['delivered'], usable=cert['usable'], correct_type=cert['correct_type'],
        subject_found=old.get('subject_found', False), wrong_subject=old.get('wrong_subject', False),
        policy_blocked=old.get('policy_blocked', False), start=old.get('start'), end=old.get('end'),
        parts=parts, continuation_facts=facts, notes=notes, fragment=fragment, row_group=group,
        claim_content_complete=bool(old.get('claim_inside') and old.get('start') and old.get('end')
                                    and not old.get('truncated')),
        sufficiency_reason='Frozen native part verification establishes every mandatory claim part',
        semantic_extent_reason=None,
        verification_limitation=('Delivered layout lacks a verified claim-to-row/cell or annotation boundary; '
                                 'unverified content remains a part, not an invented continuation'))


def evaluate(package):
    result = []
    certs = {c['requirement_id']: c for c in package['boundary_certificates']}
    for req in package['f1_requirement_package']['requirements']:
        old = certs[req['requirement_id']]
        subjects, rows = support_rows(package, req)
        scope_kinds = {s['subject_scope']['kind'] for s in subjects}
        claim = dict(requirement_id=req['requirement_id'], subject_id=[s['subject_id'] for s in subjects],
            evidence_type=req['required_type'], mandatory=old['mandatory'],
            expected_semantic_content=req['expected_semantic_content'], scope=req['provenance'].get('scope', []),
            scope_kind=next(iter(scope_kinds)) if len(scope_kinds) == 1 else 'MIXED_SCOPES',
            evidence_role=req['evidence_role'])
        claim['claim_id'] = 'claim_' + fingerprint(claim)[:24]
        evidence = [e for e in package['evidence_packet']['evidence'][req['side'].lower()]
                    if e['document_version'] == req['document_version'] and e['page'] == req['page']
                    and any(b['requirement_id'] == req['requirement_id'] and b['payload_delivered']
                            for b in e['region_bindings'])]
        o = baseline_observation(req, old)
        if req['required_type'] in {'GRAPHIC_REGION', 'GRAPHIC_SCHEME'} and evidence and evidence[0].get('raster'):
            o['parts'].append(part('raster', req, evidence[0]))
        # Identity-only and wrong-document gates are authoritative and unchanged.
        if not old['proof'].get('identity_probe_only') and not o['wrong_subject'] and not o['policy_blocked']:
            refined = annotation_observation(package, req, o, evidence, rows, claim)
            if refined is None and req['required_type'] == 'TEXT_SECTION':
                refined = text_observation(req, o, evidence, rows)
            if refined is not None:
                o = refined
        provenance = dict(package_hash=package['package_hash'], packet_hash=fingerprint(package['evidence_packet']),
            requirement_hash=fingerprint(req), document_version=req['document_version'], side=req['side'],
            source_pdf_sha256=evidence[0]['source_receipt']['sha256'] if evidence else None,
            evidence_hashes={e['evidence_id']: fingerprint(e) for e in evidence},
            canonical_subject_hashes={s['subject_id']: fingerprint(s) for s in subjects},
            legacy_certificate_id=old['certificate_id'], evaluator='F5_CLAIM_BOUNDARY/7')
        result.append(certify(claim, o, provenance))
    return result
