"""Offline candidate: check explicitly selected source references before release.

Not wired into the frozen runtime. Selection/relevance remains a semantic task;
this guard proves only source binding and literal retention, never engineering truth.
"""
import hashlib
import re


def _digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def _contains(text, reference):
    # Only whitespace variation is allowed; do not confuse KM8 with KM80/KM8.1.
    chars = [re.escape(c) for c in reference if not c.isspace()]
    if not chars:
        return False
    return bool(re.search(r'(?<![\w.])' + r'\s*'.join(chars) + r'(?!\w|\s*\.\s*\d)', text))


def check_reference_retention(fields, obligations, *, source_blocks, source_md_sha256):
    """Return repair issues without editing/suppressing the original finding.

    obligations must be selected separately by semantic review, bound to an
    authoritative TEXT block and quote. Empty obligations are explicitly unassessed.
    A matching reference alone does not establish a satisfactory final question.
    """
    if set(fields) != {'problem', 'description', 'solution'} or any(
        not isinstance(v, str) or not v.strip() for v in fields.values()
    ):
        raise ValueError('invalid_user_fields')
    if not isinstance(obligations, list):
        raise ValueError('invalid_obligations')
    issues = []
    seen = set()
    for item in obligations:
        if not isinstance(item, dict) or set(item) != {
            'reference', 'role', 'block_id', 'quote', 'block_sha256',
            'source_md_sha256', 'selection_reason',
        } or any(not isinstance(v, str) or not v.strip() for v in item.values()):
            raise ValueError('invalid_reference_obligation')
        if item['role'] not in {'calculation_context', 'design_context'}:
            raise ValueError('invalid_reference_role')
        if item['source_md_sha256'] != source_md_sha256:
            raise ValueError('stale_reference_source')
        block = source_blocks.get(item['block_id'])
        if not isinstance(block, dict) or block.get('type') != 'TEXT':
            raise ValueError('authoritative_text_block_required')
        text = block.get('text')
        if not isinstance(text, str) or _digest(text) != item['block_sha256']:
            raise ValueError('stale_reference_block')
        if item['quote'] not in text or not _contains(item['quote'], item['reference']):
            raise ValueError('reference_not_in_bound_quote')
        key = (item['reference'], item['role'], item['block_id'])
        if key in seen:
            raise ValueError('duplicate_reference_obligation')
        seen.add(key)
        if not any(_contains(value, item['reference']) for value in fields.values()):
            issues.append({'reference': item['reference'], 'role': item['role'],
                           'block_id': item['block_id'], 'reason': 'missing_from_user_fields'})
    return {
        'status': 'repair_required' if issues else ('literal_retention_verified' if obligations else 'not_assessed'),
        'issues': issues,
        'semantic_acceptance': False,
        'suppress_finding': False,
        'automatic_rewrite': False,
    }
