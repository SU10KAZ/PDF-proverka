"""Automatic candidate scopes -> existing F1 EvidenceRequirements and priorities."""
from dataclasses import asdict

from experiments.project_change_contracts_v3_272.evidence import EvidenceRequirement
from experiments.project_change_contracts_v3_272.allocation import plan
from .common import fingerprint

TYPES = {'TEXT': 'TEXT_SECTION', 'TABLE': 'TABLE_COMPLETE', 'GRAPHIC': 'GRAPHIC_REGION'}


def build_requirements(candidate, subjects, inventories, pair):
    requirements, sources = [], {}
    subject_name = candidate['candidate_id'] + ': ' + candidate['subject']
    for side in ('old', 'new'):
        inv, doc = inventories[side], pair[side]
        refs = {rid for sid in candidate[side] for rid in subjects[sid]['supporting_evidence_refs']}
        regions = [r for r in inv['regions'] if r['region_id'] in refs]
        # Preserve all observed source forms and pages, including repetitions.
        # F1 is responsible for the budget; F5 never hides omissions to look complete.
        specs = [(r, 'STATE', TYPES[r['source_type']]) for r in regions]
        context = sorted(regions, key=lambda r: (r['source_type'] != 'TEXT', r['page'], r['region_id']))
        if context:
            # Whole same-subject OLD context is a search surface, not negative truth.
            specs.append((context[0], 'COUNTER' if side == 'old' else 'CONFIRMING',
                          TYPES[context[0]['source_type']]))
            for r in regions:
                if r['notes']:
                    specs.append((r, 'SUPPORTING_NOTES', 'NOTE'))
        else:
            specs = [(None, 'STATE', 'TEXT_SECTION'),
                     (None, 'COUNTER' if side == 'old' else 'CONFIRMING', 'TEXT_SECTION')]
        for region, role, required_type in specs:
            rid = region['region_id'] if region else 'UNLOCATED'
            req_id = 'q_' + fingerprint([candidate['candidate_id'], side, rid, role, required_type])[:24]
            provenance = dict(producer='F5_AUTOMATIC_REQUIREMENT_BUILDER', candidate_id=candidate['candidate_id'],
                discovery_subject_ids=candidate[side], region_id=rid, scope=candidate['scope'],
                source_candidate_pages=[region['page']] if region else [],
                source_candidate_regions=[region['bbox_norm']] if region else [],
                unlocated_page_sentinel=region is None,
                purpose='COUNTER_SEARCH_CONTEXT_NOT_ABSENCE_PROOF' if role == 'COUNTER' else role)
            req = EvidenceRequirement(requirement_id=req_id, subject=subject_name, side=side.upper(),
                evidence_role=role, document=doc['document_code'], document_version=doc['document_version'],
                page=region['page'] if region else 1,
                expected_semantic_content=candidate['subject'] + '; scope=' + ','.join(candidate['scope']),
                evidence_type='COUNTER_EVIDENCE' if role == 'COUNTER' else required_type,
                required_type=required_type, scope_binding=rid if region else '', provenance=provenance)
            requirements.append(req)
            sources[req_id] = region
    allocation = plan(requirements, 'OTHER')
    rows = [asdict(r) | dict(priority=allocation[r.requirement_id]['priority'],
             mandatory=allocation[r.requirement_id]['mandatory'],
             supporting=not allocation[r.requirement_id]['mandatory'],
             scope=candidate['scope'], source_candidate_pages=r.provenance['source_candidate_pages'],
             source_candidate_regions=r.provenance['source_candidate_regions']) for r in requirements]
    return requirements, sources, rows
