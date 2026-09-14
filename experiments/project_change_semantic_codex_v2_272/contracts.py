"""Fail-closed structural gates. Semantic certificates require source review.

No document IDs, known values, regression labels or reference verdicts belong here.
Passing these checks is necessary, never sufficient for source-audited correctness.
"""
import math
import re

VERSIONS = {name: 1 for name in ['ConditionSignature', 'CounterEvidenceEscalation',
    'MaterialityGate', 'RasterLocatorContract', 'CrossDocumentOwnership']}
DIMENSIONS = ['engineering_subject', 'consumer_composition', 'calculation_basis',
    'operating_mode', 'stage_phase', 'building_block', 'apartment_unit', 'floor',
    'zone', 'time_regime', 'footnote_basis', 'quantity_semantics', 'total_partial',
    'maximum_design', 'installed_calculated', 'local_building_wide']


def norm(s):
    return re.sub(r'\s+', ' ', s).strip()


def references(ids, evidence, side=None):
    return bool(ids) and len(ids) == len(set(ids)) and all(
        i in evidence and (side is None or evidence[i]['side'] == side) for i in ids)


def witness_errors(w, evidence):
    e = evidence.get(w.get('evidence_id'))
    if not e:
        return ['UNKNOWN_EVIDENCE']
    if w.get('kind') == 'TEXT_LITERAL':
        q = norm(w.get('literal_quote', ''))
        return [] if (len(q) >= 12 and q in norm(e.get('text', ''))
                      and not w.get('visual_locator')) else ['QUOTE_NOT_IN_SOURCE']
    if w.get('kind') != 'RASTER_LOCATOR':
        return ['UNKNOWN_WITNESS_KIND']
    box = w.get('bbox_norm', [])
    valid_box = (len(box) == 4 and all(isinstance(x, (int, float)) and
        not isinstance(x, bool) and math.isfinite(x) and 0 <= x <= 1 for x in box)
        and box[0] < box[2] and box[1] < box[3])
    if (not e.get('raster') or not w.get('visual_locator', '').strip() or
            w.get('literal_quote') or not valid_box or not w.get('binding_reason', '').strip()):
        return ['INVALID_RASTER_LOCATOR']
    return []


def condition_errors(signature, evidence):
    rows = {r.get('dimension'): r for r in signature}
    if len(rows) != len(signature) or set(rows) != set(DIMENSIONS):
        return ['INCOMPLETE_CONDITION_SIGNATURE']
    errors = []
    for dimension, row in rows.items():
        status = row.get('comparison')
        if not row.get('reason', '').strip():
            errors.append('UNSUPPORTED_CONDITION:' + dimension)
        if status == 'NOT_APPLICABLE':
            # Applicability is a semantic judgement; a reason must survive audit.
            continue
        if status != 'SAME' or not row.get('old') or not row.get('new'):
            errors.append('REVIEW_INCOMPARABLE_CONDITIONS:' + dimension)
        if not references(row.get('old_evidence_ids', []), evidence, 'old') or not references(
                row.get('new_evidence_ids', []), evidence, 'new'):
            errors.append('UNSUPPORTED_CONDITION:' + dimension)
    if rows['engineering_subject'].get('comparison') != 'SAME':
        errors.append('UNPROVEN_SUBJECT')
    return sorted(set(errors))


def escalate(scope, query, pages, max_pages=8):
    """Search an explicit same-subject allowlist; never discover scope by a value.

    `pages` is admitted metadata/text only. Cross-document relation certificates
    are separately supplied; they do not enlarge this same-pair search silently.
    """
    if max_pages < 1:
        raise ValueError('Positive bounded budget required')
    words = {w[:7] for w in re.findall(r'[a-zа-яё]{4,}', query.casefold())}
    selected, trace = [], []
    allowed = set(scope['allowed_page_ids'])
    for level in range(4):
        candidates = []
        for p in pages:
            if p['evidence_id'] not in allowed or p['side'] != 'old':
                continue
            if p['pair_index'] != scope['pair_index'] or p['document_version'] != scope['old_version']:
                raise ValueError('Escalation crosses document/physical version boundary')
            target = p.get('level', 0)
            if target != level or p['evidence_id'] in selected:
                continue
            if level == 3 and not p.get('raster'):
                continue
            tokens = {w[:7] for w in re.findall(r'[a-zа-яё]{4,}', p.get('text', '').casefold())}
            candidates.append((len(words & tokens), p['evidence_id']))
        ranked = sorted(candidates, key=lambda r: (-r[0], r[1]))
        chosen = [i for _, i in ranked[:max(0, max_pages-len(selected))]]
        selected.extend(chosen)
        trace.append(dict(level=level, searched=[i for _, i in ranked], selected=chosen,
            exhausted_budget=len(chosen) < len(ranked), absence_proven=False))
    return dict(version=1, selected=selected, trace=trace, absence_proven=False,
                status='SEARCHED_BOUNDED_SCOPE' if selected else 'REVIEW_NO_COUNTER_SCOPE')


def counter_errors(counter, evidence, novelty):
    errors = []
    if counter.get('absence_proven') is not False:
        errors.append('UNSUPPORTED_ABSENCE')
    if counter.get('outcome') == 'OLD_CONTAINS_NEW':
        errors.append('OLD_ALREADY_CONTAINS_NEW')
    if counter.get('outcome') in {'UNKNOWN', 'SOURCE_CONFLICT'}:
        errors.append('REVIEW_COUNTER_EVIDENCE')
    steps = counter.get('steps', [])
    if not steps or len({s['level'] for s in steps}) != len(steps):
        errors.append('MISSING_COUNTER_PASS')
    for step in steps:
        ids = step.get('evidence_ids', [])
        if step['level'] not in range(4) or not step.get('reason') or not references(ids, evidence, 'old'):
            errors.append('INVALID_COUNTER_PROVENANCE')
        if step['level'] == 3 and any(not evidence.get(i, {}).get('raster') for i in ids):
            errors.append('INVALID_COUNTER_RASTER')
    # An addition or relocation needs a positive previous configuration, not no-hit.
    if novelty != 'NONE' and counter.get('outcome') != 'POSITIVE_DIFFERENT_OLD_STATE':
        errors.append('REVIEW_UNSUPPORTED_NOVELTY')
    return sorted(set(errors))


def materiality_errors(materiality, evidence):
    if materiality.get('status') != 'PROVEN' or not materiality.get('changed_design_result'):
        return ['REVIEW_MATERIALITY']
    if not references(materiality.get('old_evidence_ids', []), evidence, 'old') or not references(
            materiality.get('new_evidence_ids', []), evidence, 'new'):
        return ['UNSUPPORTED_MATERIALITY']
    return []


def evaluate(packet, output):
    evidence = {e['evidence_id']: e for e in packet['sources']}
    errors = []
    if output['case_id'] != packet['case_id']:
        errors.append('CASE_ID_MISMATCH')
    errors += condition_errors(output['condition_signature'], evidence)
    errors += counter_errors(output['counter_evidence'], evidence, output['novelty'])
    errors += materiality_errors(output['materiality'], evidence)
    expected = {c['claim_id'] for c in packet['candidate_claims']}
    actual = [c['claim_id'] for c in output['claim_audits']]
    if len(actual) != len(set(actual)) or (expected and set(actual) != expected) or not actual:
        errors.append('ORIGINAL_BUNDLE_INCOMPLETE')
    originals = {c['claim_id']: c for c in packet['candidate_claims']}
    for claim in output['claim_audits']:
        original = originals.get(claim['claim_id'])
        if original and any(norm(claim[k]) != norm(original[k]) for k in ['property','old_value','new_value']):
            errors.append('ORIGINAL_CLAIM_MUTATED:' + claim['claim_id'])
        if claim['state_support'] != 'PROVEN':
            errors.append('REVIEW_STATE_SUPPORT:' + claim['claim_id'])
        if claim['conditions'] != 'COMPARABLE':
            errors.append('REVIEW_INCOMPARABLE_CONDITIONS:' + claim['claim_id'])
        if claim.get('role', 'PRIMARY_CHANGE') == 'PRIMARY_CHANGE' and claim['materiality'] != 'PROVEN':
            errors.append('REVIEW_MATERIALITY:' + claim['claim_id'])
        for side in ['old', 'new']:
            witnesses = claim[side + '_witnesses']
            if not witnesses:
                errors.append('MISSING_STATE_WITNESS:' + claim['claim_id'])
            for w in witnesses:
                errors += witness_errors(w, evidence)
                if evidence.get(w['evidence_id'], {}).get('side') != side:
                    errors.append('WITNESS_DIRECTION_ERROR')
    if not any(c.get('role', 'PRIMARY_CHANGE') == 'PRIMARY_CHANGE' and c['materiality'] == 'PROVEN'
               for c in output['claim_audits']):
        errors.append('NO_MATERIAL_PRIMARY_CHANGE')
    for conflict in output['cross_source_conflicts']:
        if conflict['resolution'] not in {'DIFFERENT_CONDITIONS_PROVEN', 'DIFFERENT_SCOPE_PROVEN'}:
            errors.append('CROSS_SOURCE_CONFLICT')
        if not references(conflict['evidence_ids'], evidence) or not conflict['reason']:
            errors.append('UNSUPPORTED_CONFLICT_RESOLUTION')
    if not output['local_owner_proven']:
        errors.append('REVIEW_OWNERSHIP')
    if output['model_verdict'] != 'ACCEPT':
        errors.append('MODEL_' + output['model_verdict'])
    return dict(case_id=packet['case_id'], status='REVIEW' if errors else 'ACCEPT',
        reasons=sorted(set(errors)), versions=VERSIONS, source_packet_hash=packet['package_hash'],
        semantic_certifier='CODEX_SAME_MODEL_NOT_INDEPENDENT', source_audit_required=True)


OWNER_FIELDS = ['engineering_subject', 'system', 'scope', 'old_state', 'new_state',
                'event_type', 'functional_role', 'evidence_relationship']


def resolve_ownership(events, links, evidence):
    """Only a complete pairwise proof may fuse a component (no transitive guess)."""
    by_id = {e['event_id']: e for e in events}
    if len(by_id) != len(events):
        raise ValueError('Duplicate event IDs')
    approved, reviews = set(), []
    for link in links:
        ids = link.get('event_ids', [])
        ok = (len(ids) == 2 and len(set(ids)) == 2 and all(i in by_id for i in ids)
            and link.get('relation') == 'SAME_ENGINEERING_EVENT'
            and all(by_id[i].get('object_id') == 272 and by_id[i].get('status') == 'ACCEPT' for i in ids)
            and set(link.get('proof', {})) == set(OWNER_FIELDS))
        if ok:
            ok = all(p.get('proven') is True and p.get('reason') and
                     references(p.get('evidence_ids', []), evidence) for p in link['proof'].values())
            cited = {i for p in link['proof'].values() for i in p.get('evidence_ids', [])}
            # Evidence must cover BOTH event owners and BOTH states, not just a shared word.
            ok = ok and all(any(i in cited and evidence[i]['side'] == side
                for i in by_id[event_id].get('evidence_ids', []) if i in evidence)
                for event_id in ids for side in ['old', 'new'])
        if ok:
            approved.add(tuple(sorted(ids)))
        else:
            reviews.append(dict(event_ids=ids, status='REVIEW', reason='UNPROVEN_SAME_ENGINEERING_EVENT'))
    groups = [{i} for i in sorted(by_id)]
    for a, b in sorted(approved):
        ga, gb = next(g for g in groups if a in g), next(g for g in groups if b in g)
        if ga is gb:
            continue
        if all(tuple(sorted((x, y))) in approved for x in ga for y in gb):
            ga.update(gb); groups.remove(gb)
        else:
            reviews.append(dict(event_ids=sorted(ga | gb), status='REVIEW', reason='INCOMPLETE_PAIRWISE_OWNERSHIP'))
    return dict(version=1, groups=[dict(member_ids=sorted(g), members=[by_id[i] for i in sorted(g)]) for g in groups], review=reviews)
