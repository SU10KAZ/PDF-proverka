"""F5 delivery adapter: unchanged fixed-budget mechanics, dependency-first plan.

F1 coverage evaluation is deferred until boundary observation. The F1 contract
implementation is imported at that stage, never modified or monkey-patched.
Delivery mechanics copied from contracts_v3 allocation.package; source hash is
recorded by the V4 algorithm freeze. F5 owns dependency priority, not F1 semantics.
"""
from dataclasses import asdict
from experiments.project_change_contracts_v3_272.evidence import fingerprint
from experiments.project_change_contracts_272.packages import select_sections, same_subject_counter
from .requirements_v4 import allocation_plan as plan
VERSION = 'F5_DEPENDENCY_DELIVERY/4'

def package(requirements, page_loader, *, profile='OTHER', text_budget=28000, raster_budget=8):
    if text_budget < 0 or raster_budget < 0:
        raise ValueError('Nonnegative budgets required')
    if len({r.requirement_id for r in requirements}) != len(requirements):
        raise ValueError('Duplicate requirements')
    from experiments.project_change_contracts_v3_272.sources import refine_requirements
    pages = {r.requirement_id: page_loader(r) for r in requirements}
    requirements = refine_requirements(requirements, pages)
    allocation = plan(requirements, profile)

    def key(r):
        page = pages[r.requirement_id]
        raster = page.get('raster') if page else None
        return (r.side, r.document, r.document_version, r.page, raster['sha256']) if raster else None

    selected, order = set(), []
    # Round-robin sides WITHIN each requirement priority, not global top-N.
    for priority in sorted({x['priority'] for x in allocation.values()}):
        sides = {s: sorted((r for r in requirements if r.side == s and
                    allocation[r.requirement_id]['priority'] == priority),
                    key=lambda r: (r.page, r.requirement_id)) for s in ('OLD', 'NEW')}
        for index in range(max(map(len, sides.values()))):
            for side in ('OLD', 'NEW'):
                if index < len(sides[side]):
                    r = sides[side][index]
                    order.append(r)
                    if key(r) and r.required_type in (pages[r.requirement_id] or {}).get('evidence_types', []) and len(selected) < raster_budget:
                        selected.add(key(r))
    evidence, gaps, receipts = [], [], []
    for r in order:
        page = pages[r.requirement_id]
        row = dict(allocation[r.requirement_id], requested=True, available=bool(key(r)),
                   selected=bool(key(r) in selected), delivered=False, omitted=True,
                   omission_reason='NOT_ADMITTED_OR_UNAVAILABLE')
        types = page.get('evidence_types', []) if page else []
        match = r.required_type in types
        selected_type = r.required_type.value if match else next((t for t in
            ('GRAPHIC_SCHEME', 'EQUIPMENT_SCHEDULE', 'TABLE_COMPLETE', 'GRAPHIC_REGION', 'NOTE', 'TEXT_SECTION', 'COUNTER_EVIDENCE')
            if t in types), None)
        row.update(required_type=r.required_type.value, selected_evidence_type=selected_type,
                   type_match='YES' if match else 'NO')
        if page:
            raw_text = 'PDF NATIVE:\n' + page.get('native', '') + '\nOCR (fallible):\n' + page.get('ocr', '')
            text = select_sections(raw_text, [r.expected_semantic_content], text_budget)
            text_budget -= len(text['text'])
            raster = page.get('raster') if key(r) in selected else None
            grounded = bool(r.scope_binding and page.get('scope_binding') == r.scope_binding)
            complete = bool(raster and page.get('full_page') and grounded and match)
            row.update(delivered=complete, omitted=not complete,
                omission_reason='' if complete else 'TYPE_MISMATCH' if not match else 'BUDGET_LIMIT' if key(r) and not raster else 'UNVERIFIED_BOUNDARY')
            e = dict(evidence_id='e_' + fingerprint(asdict(r))[:24], subject=r.subject,
                side=r.side, document=r.document, document_version=r.document_version, page=r.page,
                text=text['text'], raster=raster, bbox=page.get('bbox'),
                requirement_id=r.requirement_id, required_type=r.required_type.value,
                selected_evidence_type=selected_type, evidence_types=types,
                type_provenance=page.get('type_provenance', {}),
                content_kind=page.get('content_kind', 'PAGE'), scope_binding=r.scope_binding if grounded else '',
                delivered_parts=sorted(r.parts) if complete else [], boundary_complete=complete,
                truncated=bool(text['omitted_sections']) and not complete,
                text_complete=text['boundary_complete'], omitted_sections=text['omitted_sections'],
                provenance=page['provenance'])
            evidence.append(e)
        if row['omitted']:
            gaps.append(dict(requirement_id=r.requirement_id, reason=row['omission_reason']))
        receipts.append(row)
    coverage = dict(requirements=[dict(requirement=asdict(r), completeness='MISSING', evidence_ids=[], missing_reason='F1_ASSESSMENT_DEFERRED_UNTIL_BOUNDARY') for r in requirements], complete=False, subjects=[])
    by_id = {r['requirement_id']: r for r in receipts}
    for row in coverage['requirements']:
        receipt = by_id[row['requirement']['requirement_id']]
        row['delivery'] = receipt
        if receipt['omission_reason'] == 'BUDGET_LIMIT':
            row['completeness'] = row['requirement']['completeness'] = 'PARTIAL_BUDGET_LIMIT'
            row['missing_reason'] = 'Available raster omitted at fixed package budget'
    coverage['schema'] = 'EVIDENCE_COVERAGE/3'
    coverage['status'] = ('COMPLETE' if coverage['complete'] else 'PARTIAL_BUDGET_LIMIT'
        if any(r['omission_reason'] == 'BUDGET_LIMIT' for r in receipts) else 'PARTIAL')
    body = dict(schema=VERSION, inference_executed=False,
        requirements=[asdict(r) for r in requirements], evidence=evidence,
        evidence_coverage=coverage, coverage_complete=coverage['complete'], gaps=gaps,
        raster_allocation=dict(schema='RASTER_ALLOCATION/3', budget=raster_budget,
            used=len(selected), requirements=receipts,
            delivered_by_side={s: len({key(r) for r in requirements if r.side == s and key(r) in selected}) for s in ('OLD', 'NEW')}),
        counter_evidence=same_subject_counter(requirements, evidence),
        scope_discovery='PREEXISTING_BOUNDED_SCOPE_MAP_NOT_AUTONOMOUS',
        limits=dict(remaining_text_characters=text_budget, raster_budget=raster_budget),
        sufficiency_profile=profile,
        model_instruction='Package completeness measures delivery of ALL requested context. '
            'Evaluate claim sufficiency separately using its EvidenceSufficiencyProfile. '
            'Missing supporting graphics do not negate an unambiguous declaration. '
            'Do not infer source content from a requirement or delivery receipt.')
    body['package_hash'] = fingerprint(body)
    return body
