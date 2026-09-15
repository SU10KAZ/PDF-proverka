"""Existence precedes significance. Semantic observations retain their provenance."""
from experiments.project_change_contracts_272.comparability import same_value, materiality


def change_existence(raw, transition, comparison, sufficiency, valid_witnesses, issues):
    ids = set(transition.old.evidence_ids) | set(transition.new.evidence_ids)
    supported = transition.state_support == 'SOURCE_SUPPORTED' and not issues
    same_subject = (transition.old.engineering_subject == transition.new.engineering_subject and
                    transition.old.scope == transition.new.scope and bool(transition.identity_basis))
    counters = raw.get('old_counter_evidence', [])
    counter_ids = {c.get('evidence_id') for c in counters}
    counter_receipt = dict(evidence_ids=sorted(i for i in counter_ids if i),
        witnesses_validated=bool(counter_ids) and counter_ids <= valid_witnesses,
        observations=counters, explanation=raw.get('mismatch_or_counter_reason', ''))
    reason, source, value = '', 'NORMALIZED_STATE_COMPARISON', 'UNKNOWN'
    semantic = raw.get('exists_change')
    if isinstance(semantic, dict):
        refs = set(semantic.get('evidence_ids', []))
        source = 'STRUCTURED_MODEL_OBSERVATION'
        grounded = bool(refs) and refs <= valid_witnesses and bool(refs & set(transition.old.evidence_ids)) and bool(refs & set(transition.new.evidence_ids))
        observed = semantic.get('status', 'UNKNOWN') if grounded else 'UNKNOWN'
        reason = semantic.get('explanation', '')
    else:
        # V2 has no existence field. Preserve its explicit source-backed
        # no-change judgment, including differently worded equivalent aggregates.
        # No arithmetic, verdict or expected value is supplied by a case ID.
        observed = 'NO' if raw.get('verdict') == 'NOT_CHANGE' and raw.get('materiality', {}).get('status') == 'NOT_MATERIAL' else 'YES' if raw.get('verdict') == 'ACCEPT' else 'UNKNOWN'
        source = 'LEGACY_SEMANTIC_VERDICT_ADAPTER'
        reason = raw.get('mismatch_or_counter_reason') or raw.get('reasoning_ru', '')
    if not same_subject or not supported or transition.source_conflict:
        reason = 'Same supported subject without a relevant source conflict required'
    elif same_value(transition.old.value, transition.new.value):
        value, reason = 'NO', 'Equal supported state values at the same subject and scope'
    elif observed == 'NO' and reason and (comparison['status'] == 'COMPARABLE' or counter_receipt['witnesses_validated']):
        value = 'NO'
    elif comparison['status'] != 'COMPARABLE':
        reason = 'Comparable conditions required before establishing a change'
    elif observed == 'YES' and sufficiency['sufficient']:
        value = 'YES'
    return dict(status=value, source=source, explanation=reason, counter_evidence=counter_receipt,
        same_subject=same_subject, comparable_states=comparison['status'], evidence_ids=sorted(ids))


def admit(raw, transition, comparison, sufficiency, valid_witnesses, issues):
    exists = change_existence(raw, transition, comparison, sufficiency, valid_witnesses, issues)
    if exists['status'] == 'YES':
        significance = materiality(transition, comparison)
    else:
        significance = dict(status='NOT_APPLICABLE' if exists['status'] == 'NO' else 'UNRESOLVED',
            rule='Materiality evaluated only after EXISTS_CHANGE=YES', evaluated=False)
    verdict = raw.get('verdict', 'REVIEW')
    if exists['status'] == 'NO':
        verdict = 'NOT_CHANGE'
    elif verdict == 'NOT_CHANGE' or (verdict == 'ACCEPT' and
            (issues or exists['status'] != 'YES' or not sufficiency['sufficient'] or significance['status'] != 'MATERIAL')):
        verdict = 'REVIEW'
    return dict(exists_change=exists, materiality=significance, effective_verdict=verdict)
