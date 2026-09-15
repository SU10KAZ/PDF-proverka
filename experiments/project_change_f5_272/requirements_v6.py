"""Subject confidence and dependency closure precede the fixed evidence budget."""
from dataclasses import asdict
from types import SimpleNamespace

from experiments.project_change_contracts_v3_272.evidence import EvidenceRequirement
from experiments.project_change_contracts_v3_272.allocation import plan as legacy_plan
from .common import fingerprint
from .continuation_v4 import analyze
from .requirement_builder import TYPES


from .priority_v6 import allocation_plan


def build(candidate, canonical, prepared, docs, guards):
    reqs, regions, analyses = [], {}, {}
    cid = candidate['candidate_id']
    unresolved = candidate['subject_confidence'] == 'SUBJECT_UNRESOLVED'
    guard_map = {(g['side'], g['page']): g for g in guards}

    def add(side, region, role, kind, *, parents=(), target_page=None):
        rid = region['region_id'] if region else 'UNLOCATED'
        page = region['page'] if region else target_page or 1
        qid = 'q_' + fingerprint(['F5V4', cid, side, rid, page, role, kind, parents])[:24]
        doc = prepared['pair'][side]
        provenance = dict(producer='F5_SUBJECT_SCOPED_REQUIREMENTS/6', candidate_id=cid,
            region_id=rid, discovery_subject_ids=candidate[side], subject_confidence=candidate['subject_confidence'],
            canonical_scope_hash=fingerprint([canonical[s]['subject_scope'] for s in candidate[side]]),
            scope=candidate['scope'], source_candidate_pages=[page] if region or target_page else [],
            source_candidate_regions=[region['bbox_norm']] if region else [],
            unlocated_page_sentinel=region is None and target_page is None,
            dependency_of=list(parents), purpose='SUBJECT_IDENTITY_ONLY' if unresolved else role,
            physical_dependency_page=target_page, functional_key=candidate['functional_key'],
            evidence_roles=(['SUBJECT_IDENTITY', 'SCOPE_BINDING'] + (['STATE_SUPPORT'] if role == 'STATE' else [])
                + (['COUNTER_EVIDENCE'] if role == 'COUNTER' else []))
                if region and not parents and (kind == 'TEXT_SECTION' or unresolved) else
                (['COUNTER_EVIDENCE'] if role == 'COUNTER' else ['STATE_SUPPORT', 'SCOPE_BINDING']),
            continuation_status='UNKNOWN', continuation_proof=None)
        # A source request specification, not yet an EvidenceRequirement. Its
        # dependency closure is computed before constructing the F1 contracts.
        r = SimpleNamespace(requirement_id=qid, subject=cid + ': ' + candidate['subject'],
            side=side.upper(), evidence_role=role, document=doc['document_code'],
            document_version=doc['document_version'], page=page,
            expected_semantic_content=candidate['subject'] + '; scope=' + ','.join(candidate['scope']) +
                ('; establish subject identity only' if unresolved else ''),
            evidence_type='COUNTER_EVIDENCE' if role == 'COUNTER' else kind, required_type=kind,
            scope_binding=rid if region else '', provenance=provenance)
        reqs.append(r)
        regions[qid] = region
        return r

    for side in ('old', 'new'):
        selected = {r['region_id'] for sid in candidate[side]
                    for r in canonical[sid]['evidence_support'] if r['accepted']}
        candidates = [docs[side].regions[r] for r in sorted(selected)]
        if unresolved:
            # Identity-only is a view of every accepted source region. The
            # missing endpoint remains an unlocated probe, never page-1 evidence.
            for region in sorted(candidates, key=lambda r: (r['page'], r['source_type'], r['region_id'])):
                add(side, region, 'COUNTER' if side == 'old' else 'IDENTITY', TYPES[region['source_type']])
            if not candidates:
                add(side, None, 'COUNTER' if side == 'old' else 'IDENTITY', 'TEXT_SECTION')
            continue
        for r in sorted(candidates, key=lambda r: (r['page'], r['source_type'], r['region_id'])):
            add(side, r, 'STATE', TYPES[r['source_type']])
        context = sorted(candidates, key=lambda r: (r['source_type'] != 'TEXT', r['page'], r['region_id']))
        if context:
            add(side, context[0], 'COUNTER' if side == 'old' else 'CONFIRMING', TYPES[context[0]['source_type']])
        else:
            add(side, None, 'COUNTER' if side == 'old' else 'IDENTITY', 'TEXT_SECTION')

    # Analyse every existing source region before any priority/selection. Then
    # close dependencies recursively; missing/excluded pages remain requests.
    queue = [(r, frozenset({(r.side, r.page, r.required_type)})) for r in reqs]
    cursor = 0
    while cursor < len(queue):
        req, visited = queue[cursor]
        cursor += 1
        region = regions[req.requirement_id]
        if region is None:
            continue
        side = req.side.lower()
        roles = {canonical[s]['functional_role'] for s in candidate[side]}
        role = next(iter(roles)) if len(roles) == 1 else 'SCOPED_CONTEXT_GROUP'
        analysis = analyze(region, docs[side], candidate['functional_key'], role)
        if unresolved:
            analysis = analysis | dict(identity_only=True, dependency_expansion='DEFERRED_UNTIL_IDENTITY')
        analyses[req.requirement_id] = analysis
        if unresolved or req.required_type == 'NOTE':
            continue
        for page in analysis['required_pages']:
            token = (req.side, page, req.required_type)
            if token in visited:
                continue
            choices = [r for r in docs[side].regions.values()
                       if r['page'] == page and r['source_type'] == region['source_type']]
            target = sorted(choices, key=lambda r: r['region_id'])[0] if choices else None
            if guard_map.get((side, page), {}).get('usable') == 'NO':
                target = None
            dep = add(side, target, 'CONTINUATION', req.required_type,
                      parents=(req.requirement_id,), target_page=page)
            dep.provenance.update(continuation_status=analysis['requirement'],
                continuation_proof=analysis['proof_hash'])
            queue.append((dep, visited | {token}))
        if any(n['mandatory'] for n in analysis['notes']):
            # Notes are same-page here. External note references remain unknown
            # until a physical locator is established; no page-number guessing.
            note = add(side, region, 'APPLICABLE_NOTE', 'NOTE', parents=(req.requirement_id,))
            note.provenance.update(continuation_status='REQUIRED',
                continuation_proof=fingerprint([n for n in analysis['notes'] if n['mandatory']]))
    reqs = [EvidenceRequirement(**vars(spec)) for spec in reqs]
    priorities = allocation_plan(reqs)
    rows = [asdict(r) | dict(**{k: v for k, v in priorities[r.requirement_id].items() if k != 'requirement_id'},
        supporting=not priorities[r.requirement_id]['mandatory']) for r in reqs]
    return reqs, regions, analyses, rows
