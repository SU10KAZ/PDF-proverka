"""F5 delivery adapter: unchanged fixed-budget mechanics, dependency-first plan.

F1 coverage evaluation is deferred until boundary observation. The F1 contract
implementation is imported at that stage, never modified or monkey-patched.
Delivery mechanics copied from contracts_v3 allocation.package; source hash is
recorded by the V4 algorithm freeze. F5 owns dependency priority, not F1 semantics.
"""
from dataclasses import asdict
from experiments.project_change_contracts_v3_272.evidence import fingerprint
from experiments.project_change_contracts_272.packages import select_sections, same_subject_counter
from .priority_v6 import allocation_plan as plan, ordered, text_allocation, audit_displacements
VERSION = 'F5_PROTECTED_DELIVERY/6'

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

    selected, order = set(), ordered(requirements, allocation)
    for r in order:
        if key(r) and r.required_type in (pages[r.requirement_id] or {}).get('evidence_types', []) and (key(r) in selected or len(selected) < raster_budget):
            selected.add(key(r))
    text_payloads, text_budget = text_allocation(order, pages, allocation, text_budget)
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
            text = text_payloads[r.requirement_id]
            raster = page.get('raster') if key(r) in selected else None
            grounded = bool(r.scope_binding and page.get('scope_binding') == r.scope_binding)
            complete = bool(raster and page.get('full_page') and grounded and match)
            row.update(delivered=complete, omitted=not complete,
                omission_reason='' if complete else 'TYPE_MISMATCH' if not match else 'BUDGET_LIMIT' if key(r) and not raster else 'UNVERIFIED_BOUNDARY')
            e = dict(evidence_id='e_' + fingerprint(asdict(r))[:24], subject=r.subject,
                evidence_roles=row['roles'], protected=row['protected'], priority=row['priority'],
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
    units = {e['requirement_id']: e for e in evidence}
    logs = []
    for r in order:
        e = units.get(r.requirement_id, {})
        a = allocation[r.requirement_id]
        # Required route payload, independent of semantic boundary completeness.
        table_rows = sum(line.strip().startswith('|') and any(c.isalnum() for c in line)
                         for line in e.get('text', '').splitlines())
        route = bool(e.get('raster') or (table_rows >= 3 if r.required_type in
            {'TABLE_COMPLETE', 'EQUIPMENT_SCHEDULE'} else e.get('text') if r.required_type not in
            {'GRAPHIC_REGION', 'GRAPHIC_SCHEME'} else False))
        raster_omitted = bool(key(r) and key(r) not in selected)
        blockers = [u for u in order if key(u) in selected and key(u) != key(r)] if raster_omitted and not route and r.required_type in (pages[r.requirement_id] or {}).get('evidence_types', []) else []
        # Only a lower tier occupying capacity is displacement. A full budget
        # of equal/higher priority is reported as capacity exhaustion instead.
        displaced = sorted({'e_' + fingerprint(asdict(u))[:24] for u in blockers
                            if allocation[u.requirement_id]['priority'] > a['priority']})
        reason = '' if route else 'NOT_ADMITTED_OR_UNAVAILABLE' if not pages[r.requirement_id] else 'NOT_DELIVERED_BUDGET_LIMIT'
        logs.append(dict(evidence_id='e_' + fingerprint(asdict(r))[:24], requirement_id=r.requirement_id,
            subject_id=r.provenance.get('candidate_id', r.subject), side=r.side, page=r.page,
            type=r.required_type.value, role=a['roles'], legacy_role=r.evidence_role,
            priority='P' + str(a['priority']), selected='YES' if route else 'NO',
            protected='YES' if a['protected'] else 'NO', omitted_reason=reason,
            displaced_by=displaced, raster_selected=bool(e.get('raster')), text_selected=bool(e.get('text')),
            raster_omitted_reason='NOT_DELIVERED_BUDGET_LIMIT' if raster_omitted else '',
            continuation_status=r.provenance.get('continuation_status') if a['dependency'] else None,
            dependency=a['dependency'], mandatory=a['mandatory'],
            identity_payload_retained=bool(e.get('text') or e.get('raster')) if 'SUBJECT_IDENTITY' in a['roles'] else None))
    body['delivery_decision_log'] = logs
    body['identity_evidence_ids'] = sorted(row['evidence_id'] for row in logs if 'SUBJECT_IDENTITY' in row['role'])
    body['priority_audit'] = audit_displacements(logs)
    body['protected_capacity_omissions'] = [row['evidence_id'] for row in logs
        if row['protected'] == 'YES' and row['selected'] == 'NO' and row['omitted_reason'] == 'NOT_DELIVERED_BUDGET_LIMIT']
    # Budget exhaustion does not silently waive requirements or boundaries.
    if body['priority_audit']['status'] != 'PASS':
        raise ValueError('PROTECTED_EVIDENCE_DISPLACED')
    body['package_hash'] = fingerprint(body)
    return body
