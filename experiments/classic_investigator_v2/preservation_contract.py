"""Validate preservation-review coverage and literal evidence, not semantic truth."""
from collections import Counter
import copy


def text_fields(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from text_fields(item)
    elif isinstance(value, list):
        for item in value:
            yield from text_fields(item)


def assess(sources, outputs, response):
    source_ids = [r['candidate']['id'] for r in sources]
    output_ids = [r['id'] for r in outputs]
    if len(source_ids) != len(set(source_ids)) or len(output_ids) != len(set(output_ids)):
        raise ValueError('Ambiguous input IDs')
    source_map = {r['candidate']['id']: r['candidate'] for r in sources}
    output_map = {r['id']: r for r in outputs}
    reviews = response.get('reviews')
    if not isinstance(reviews, list):
        raise ValueError('Review array missing')
    counts = Counter(r['source_id'] for r in reviews if isinstance(r, dict) and isinstance(r.get('source_id'), str))
    assessed = []
    for review in reviews:
        errors = []
        if not isinstance(review, dict):
            assessed.append({'review': review, 'contract_errors': ['not_an_object']})
            continue
        sid = review.get('source_id')
        if not isinstance(sid, str):
            sid = None
        if sid not in source_map:
            errors.append('unknown_source_id')
        if counts[sid] != 1:
            errors.append('repeated_source_review')
        status = review.get('status')
        if status not in {'preserved', 'partial', 'needs_disposition', 'ambiguous'}:
            errors.append('unknown_status')
        ids = review.get('output_ids')
        if not isinstance(ids, list) or any(not isinstance(i, str) or i not in output_map for i in ids):
            errors.append('invalid_output_ids'); ids = []
        issues = review.get('issues')
        if not isinstance(issues, list):
            errors.append('missing_issues_array'); issues = []
        if status == 'preserved' and (issues or not ids):
            errors.append('preserved_without_clean_output_link')
        if status == 'partial' and not issues:
            errors.append('partial_without_explanation')
        source_texts = list(text_fields(source_map.get(sid, {})))
        output_texts = [text for i in ids for text in text_fields(output_map[i])]
        for index, issue in enumerate(issues):
            if not isinstance(issue, dict):
                errors.append(f'issue{index}:not_an_object'); continue
            if issue.get('kind') not in {'lost_caveat', 'missing_atomic_claim', 'unjustified_strengthening', 'unresolved_disposition'}:
                errors.append(f'issue{index}:unknown_kind')
            quote = issue.get('source_quote')
            if not isinstance(quote, str) or not quote or not any(quote in text for text in source_texts):
                errors.append(f'issue{index}:source_quote_not_literal')
            quote = issue.get('output_quote')
            if not isinstance(quote, str) or (quote and not any(quote in text for text in output_texts)):
                errors.append(f'issue{index}:output_quote_not_literal')
            if not issue.get('reason'):
                errors.append(f'issue{index}:reason_missing')
        assessed.append({'review': copy.deepcopy(review), 'contract_errors': errors})
    missing = [sid for sid in source_ids if sid not in counts]
    unlinked = response.get('unlinked_output_ids')
    unlinked_valid = isinstance(unlinked, list) and all(isinstance(i, str) and i in output_map for i in unlinked)
    return {'rows': assessed, 'expected_sources': len(source_ids), 'missing_source_ids': missing,
            'duplicate_source_ids': [sid for sid, count in counts.items() if count > 1],
            'unlinked_output_ids_valid': unlinked_valid,
            'contract_complete': not missing and unlinked_valid and all(not r['contract_errors'] for r in assessed),
            'semantic_correctness_verified': False,
            'limits': 'Only complete ID accounting and literal source/output quotes. A quoted passage may be irrelevant, '
                      'a preserved judgment wrong, or a proposed correction harmful. No automatic edits, deletion or technical acceptance.'}


def assess_source_adjudication(payload, response):
    """Check proposed edits against frozen inputs; never execute them."""
    issues = {r['issue_id']: r for r in payload['review_issues']}
    outputs = {r['id']: r for r in payload['final_findings']}
    packet = payload['source_packet']
    evidence_sources = {
        'TEXT': {r['block_id']: r['text'] for r in packet['authoritative_text']},
        'source_candidate': {r['candidate']['id']: r['candidate'] for r in payload['source_candidates']},
        'final_finding': outputs,
        'location_metadata': {r['block_id']: r for r in packet['location_metadata']}}
    decisions, patches = response.get('decisions'), response.get('patches')
    if not isinstance(decisions, list) or not isinstance(patches, list):
        raise ValueError('Decisions and patches arrays required')
    errors = []; seen = Counter(); accepted = {}
    for index, decision in enumerate(decisions):
        if not isinstance(decision, dict) or not isinstance(decision.get('issue_id'), str):
            errors.append(f'decision{index}:invalid_id'); continue
        iid = decision['issue_id']; seen[iid] += 1
        if iid not in issues:
            errors.append(f'{iid}:unknown_issue')
        status = decision.get('decision'); accepted[iid] = status
        if status not in {'change_supported', 'keep_supported', 'needs_data', 'no_material_loss'}:
            errors.append(f'{iid}:unknown_decision')
        if not decision.get('reason'):
            errors.append(f'{iid}:missing_reason')
        evidence = decision.get('evidence')
        if not isinstance(evidence, list):
            errors.append(f'{iid}:missing_evidence_array'); continue
        if status in {'change_supported', 'keep_supported'} and not evidence:
            errors.append(f'{iid}:unsupported_decision')
        for e in evidence:
            if not isinstance(e, dict) or not isinstance(e.get('id'), str) or not isinstance(e.get('kind'), str):
                errors.append(f'{iid}:invalid_evidence'); continue
            source = evidence_sources.get(e['kind'], {}).get(e['id'])
            quote = e.get('quote')
            if source is None or not isinstance(quote, str) or not quote or not any(quote in t for t in text_fields(source)):
                errors.append(f'{iid}:nonliteral_or_unknown_evidence')
    errors.extend(f'{iid}:missing_decision' for iid in issues if not seen[iid])
    errors.extend(f'{iid}:duplicate_decision' for iid, count in seen.items() if count > 1)
    fields = set(); patched_issues = set()
    for index, patch in enumerate(patches):
        if not isinstance(patch, dict):
            errors.append(f'patch{index}:invalid_object'); continue
        fid, field = patch.get('finding_id'), patch.get('field')
        if not isinstance(fid, str) or fid not in outputs or not isinstance(field, str) or field not in {'problem', 'description', 'solution', 'risk'}:
            errors.append(f'patch{index}:invalid_target'); continue
        key = (fid, field)
        if key in fields: errors.append(f'patch{index}:duplicate_target')
        fields.add(key)
        if patch.get('old_value') != outputs[fid].get(field):
            errors.append(f'patch{index}:old_value_mismatch')
        if not isinstance(patch.get('new_value'), str) or not patch['new_value'].strip() or patch['new_value'] == patch.get('old_value'):
            errors.append(f'patch{index}:invalid_replacement')
        links = patch.get('issue_ids')
        if not isinstance(links, list) or not links or any(not isinstance(i, str) or i not in issues for i in links):
            errors.append(f'patch{index}:invalid_issue_links'); continue
        for iid in links:
            if accepted.get(iid) != 'change_supported' or fid not in issues[iid]['output_ids']:
                errors.append(f'patch{index}:{iid}:unapproved_or_unlinked_target')
            patched_issues.add(iid)
    errors.extend(f'{iid}:change_without_patch' for iid, status in accepted.items()
                  if status == 'change_supported' and iid not in patched_issues)
    return {'contract_complete': not errors, 'errors': errors, 'decisions': len(decisions),
            'patches': len(patches), 'semantic_correctness_verified': False,
            'limits': 'Coverage, exact citations and edit preconditions only. No patches applied; '
                      'evidence relevance and replacement semantics still require adjudication.'}
