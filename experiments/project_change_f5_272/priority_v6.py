"""Protect accepted identity and primary routes before optional delivery.

Mandatory retains its boundary meaning. Protection is a transport priority,
not a claim of subject, boundary, or correspondence completeness.
"""
import re
from experiments.project_change_contracts_v3_272.allocation import plan as legacy_plan
from experiments.project_change_contracts_272.packages import select_sections
from .subject_discovery import FUNCTIONS


def allocation_plan(requirements, profile='OTHER'):
    ordinary = [r for r in requirements if not r.provenance.get('dependency_of')]
    rows = legacy_plan(ordinary, profile)
    for r in ordinary:
        row = rows[r.requirement_id]
        roles = set(r.provenance.get('evidence_roles', []))
        if r.evidence_role in {'PRIMARY', 'MANDATORY'}:
            row.update(mandatory=True, purpose=r.evidence_role)
        primary = row['purpose'] in {'PRIMARY_STATE', 'PRIMARY'}
        identity = 'SUBJECT_IDENTITY' in roles
        priority = 0 if primary else 1 if row['mandatory'] or identity else 2 if row['priority'] < 5 else 3
        if r.provenance.get('redundant') and not (primary or row['mandatory'] or identity):
            priority = 4
        row.update(priority=priority, protected=primary or row['mandatory'] or identity,
                   roles=sorted(roles), dependency=False, priority_reason=row['purpose'])
    pending = {r.requirement_id: r for r in requirements if r.provenance.get('dependency_of')}
    while pending:
        progressed = False
        for qid, r in list(pending.items()):
            parents = r.provenance['dependency_of']
            if not all(p in rows for p in parents):
                continue
            proven = r.provenance.get('continuation_status') == 'REQUIRED' and bool(r.provenance.get('continuation_proof'))
            mandatory = proven and any(rows[p]['mandatory'] for p in parents)
            rows[qid] = dict(requirement_id=qid, side=r.side, priority=1 if mandatory else 3,
                purpose='SUBJECT_CONTINUATION_OR_APPLICABLE_NOTE', mandatory=mandatory,
                protected=mandatory, roles=r.provenance.get('evidence_roles', []), dependency=True,
                priority_reason='PROVEN_REQUIRED_DEPENDENCY' if proven else 'CONTINUATION_UNCHECKED')
            del pending[qid]
            progressed = True
        if not progressed:
            raise ValueError('Unbound or cyclic continuation requirement')
    return rows


def ordered(requirements, allocation):
    """Reserve OLD/NEW P0 and direct P1 types before dependency P1 slots."""
    def reservation(a):
        return (a['priority'], a['dependency'], not a['mandatory'])
    result = []
    for tier in sorted({reservation(a) for a in allocation.values()}):
        sides = {s: sorted((r for r in requirements if r.side == s and
            reservation(allocation[r.requirement_id]) == tier),
            key=lambda r: (r.page, r.requirement_id)) for s in ('OLD', 'NEW')}
        for i in range(max(map(len, sides.values()))):
            result.extend(sides[s][i] for s in ('OLD', 'NEW') if i < len(sides[s]))

    return result


def protected_text(page, requirement):
    """A whole native line witnessing the function; no invented or sliced text.

    This is identity payload, explicitly not a complete section. Full sections
    are expanded only after every protected identity unit has been reserved.
    """
    native = page.get('native', '')
    pattern = FUNCTIONS.get(requirement.provenance.get('functional_key'), (None,))[0]
    lines = [line for line in native.splitlines() if line.strip() and
             (re.search(pattern, line, re.I) if pattern else requirement.expected_semantic_content.casefold() in line.casefold())]
    return next(iter(lines), native)


def text_allocation(order, pages, allocation, budget):
    # A page is delivered once. Share its union of identity lines across region
    # aliases so the unchanged downstream deduplicator cannot discard a role.
    groups = {}
    for r in order:
        if pages[r.requirement_id]:
            groups.setdefault((r.side, r.document, r.document_version, r.page), []).append(r)
    payloads, spent = {}, 0
    for token, group in groups.items():
        protected = [r for r in group if allocation[r.requirement_id]['protected']]
        value = '\n'.join(dict.fromkeys(line for r in protected
            for line in protected_text(pages[r.requirement_id], r).splitlines()))
        if value and len(value) <= budget - spent:
            payloads[token] = dict(text=value, boundary_complete=False, omitted_sections=['FULL_SECTION_NOT_DELIVERED'])
            spent += len(value)
        else:
            payloads[token] = dict(text='', boundary_complete=False, omitted_sections=['TEXT_BUDGET_LIMIT'])
    # Never evict a reserved unit while enlarging another page's text.
    for token, group in groups.items():
        native = '\n'.join(dict.fromkeys(pages[r.requirement_id].get('native', '') for r in group))
        ocr = '\n'.join(dict.fromkeys(pages[r.requirement_id].get('ocr', '') for r in group))
        raw = 'PDF NATIVE:\n' + native + '\nOCR (fallible):\n' + ocr
        old = payloads[token]
        full = select_sections(raw, [r.expected_semantic_content for r in group], budget - spent + len(old['text']))
        # Existing protected source lines must actually remain in the view.
        if len(full['text']) > len(old['text']) and all(line in full['text'] for line in old['text'].splitlines()):
            payloads[token] = full
            spent += len(full['text']) - len(old['text'])
    return {r.requirement_id: payloads[token] for token, group in groups.items() for r in group}, budget - spent


def audit_displacements(log):
    """Capacity exhaustion is explicit; lower-priority eviction always fails."""
    by_id = {row['evidence_id']: row for row in log}
    issues = []
    for row in log:
        if row['protected'] == 'YES' and row['displaced_by']:
            issues.append(dict(evidence_id=row['evidence_id'], reason='PROTECTED_EVIDENCE_DISPLACED',
                               displaced_by=row['displaced_by']))
        for eid in row['displaced_by']:
            if eid not in by_id:
                issues.append(dict(evidence_id=row['evidence_id'], reason='UNKNOWN_DISPLACER'))
    return dict(status='FAIL' if issues else 'PASS', displaced_count=len(issues), issues=issues)
