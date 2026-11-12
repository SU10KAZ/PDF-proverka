"""Isolated selection/repair candidate; injected provider, no implicit network calls.

Do not plug into a frozen comparison. A local passing check does not approve
semantic selection, wording, or an engineering finding.
"""
from copy import deepcopy
import hashlib
import json
import re
from .reference_retention_candidate import check_reference_retention


def _sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def text_blocks(md):
    headings = list(re.finditer(r'^###\s+BLOCK[^\n]*?\[([^]]+)\]:\s*(\S+)[^\n]*$', md, re.M))
    result = {}
    for i, heading in enumerate(headings):
        bid = heading[2]
        if bid in result:
            raise ValueError('ambiguous_source_block')
        if heading[1].upper() == 'TEXT':
            result[bid] = {'type': 'TEXT', 'text': md[heading.start():headings[i + 1].start() if i + 1 < len(headings) else len(md)]}
    return result


def selection_request(fields, response, md):
    blocks = text_blocks(md)
    # Give all source TEXT, not a hand-selected target document code.
    payload = {'approved_user_fields': deepcopy(fields), 'untrusted_source_review': deepcopy(response),
               'authoritative_text_blocks': blocks, 'source_md_sha256': _sha(md)}
    prompt = '''Check retention of addressed document references in this known-candidate source review. This is not blind defect discovery. Use only supplied TEXT; the source review is untrusted. Identify calculation/design document references relevant to resolving the SAME candidate. Do not require every catalogue entry. Distinguish existence of a reference from availability, applicability or contents of that document. A calculation reference and a design reference are not interchangeable. Do not infer a missing calculation or physical defect.
Return an object with reference_dispositions (list). Each item has exactly reference, role (calculation_context/design_context), block_id, quote (exact source substring containing reference), disposition (required/not_required), reason (why this reference matters or does not matter to this candidate). Consider relevant references even when they occur only in review reasoning rather than current user fields. Do not add a target from outside supplied evidence. Use an empty list if none can be established; empty is unassessed, not acceptance. No tools, web, files or engineer answers.'''
    return {'messages': [{'role': 'system', 'content': prompt}, {'role': 'user', 'content': json.dumps(payload, ensure_ascii=False)}], 'image_paths': []}


def bind_selection(selection, md):
    if not isinstance(selection, dict) or set(selection) != {'reference_dispositions'} or not isinstance(selection['reference_dispositions'], list):
        raise ValueError('invalid_reference_selection')
    blocks = text_blocks(md)
    obligations = []
    seen = set()
    for item in selection['reference_dispositions']:
        if not isinstance(item, dict) or set(item) != {'reference', 'role', 'block_id', 'quote', 'disposition', 'reason'} or any(not isinstance(v, str) or not v.strip() for v in item.values()):
            raise ValueError('invalid_reference_disposition')
        if item['disposition'] not in {'required', 'not_required'}:
            raise ValueError('invalid_reference_disposition')
        key = (item['reference'], item['role'], item['block_id'])
        if key in seen:
            raise ValueError('duplicate_reference_selection')
        seen.add(key)
        block = blocks.get(item['block_id'])
        if block is None:
            raise ValueError('unknown_reference_block')
        obligation = {k: item[k] for k in ('reference', 'role', 'block_id', 'quote')}
        obligation.update(block_sha256=_sha(block['text']), source_md_sha256=_sha(md), selection_reason=item['reason'])
        # Validate source provenance for exclusions too, without blessing relevance.
        check_reference_retention({'problem': 'x', 'description': 'x', 'solution': 'x'}, [obligation], source_blocks=blocks, source_md_sha256=_sha(md))
        if item['disposition'] == 'required':
            obligations.append(obligation)
    return obligations, blocks


async def run_reference_retention(fields, response, md, *, invoke):
    """At most selection + one repair; caller owns authorization and journaling.

    invoke(request, phase) returns parsed JSON. Failure propagates without retry.
    Original fields are always returned and never mutated, including on repair.
    """
    original = deepcopy(fields)
    selection = await invoke(selection_request(fields, response, md), 'reference_selection')
    obligations, blocks = bind_selection(selection, md)
    before = check_reference_retention(fields, obligations, source_blocks=blocks, source_md_sha256=_sha(md))
    result = {'original_fields': original, 'selection': selection, 'before': before,
              'semantic_acceptance': False, 'automatic_release': False}
    if before['status'] != 'repair_required':
        result['status'] = before['status']
        return result
    payload = {'original_fields': original, 'reference_obligations': obligations,
               'issues': before['issues'], 'source_blocks': blocks,
               'untrusted_source_review': response}
    request = {'messages': [
        {'role': 'system', 'content': 'Repair only the addressed-reference omission in the supplied finding. Preserve object, scope, supported content, uncertainty and question purpose. Retain distinct calculation and design references with conditional applicability. Do not assert absent calculation, inadequate strength or document contents not supplied. Return exactly problem, description, solution as nonempty Russian strings. No tools or external sources. This is a candidate rewrite requiring separate semantic admission.'},
        {'role': 'user', 'content': json.dumps(payload, ensure_ascii=False)}], 'image_paths': []}
    revised = await invoke(request, 'reference_repair')
    after = check_reference_retention(revised, obligations, source_blocks=blocks, source_md_sha256=_sha(md))
    result.update(candidate_fields=deepcopy(revised), after=after,
                  status='candidate_requires_semantic_review' if after['status'] == 'literal_retention_verified' else 'repair_unresolved')
    return result
