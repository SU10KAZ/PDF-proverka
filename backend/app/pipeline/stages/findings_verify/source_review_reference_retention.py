"""Keep explicitly selected context addresses in final user-facing wording.

This optional contract checks source binding and literal retention only. A source
reference may be known while the referenced calculation remains unavailable.
Neither its presence nor model selection certifies applicability or contents.
"""
import re
from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def _contains(text, reference):
    chars = [re.escape(c) for c in reference if not c.isspace()]
    return bool(chars and re.search(r'(?<![\w.])' + r'\s*'.join(chars)
                                    + r'(?!\w|\s*\.\s*\d)', text))


def check_context_reference_retention(response, decision, original, md):
    """Raise before admission on a missing selected address; never rewrite/drop.

    Older decisions without context_reference_obligations remain readable. The
    caller must separately verify the completeness and relevance of selection.
    These references are context addresses, not conclusive final evidence refs.
    """
    obligations = decision.get('context_reference_obligations')
    if obligations is None:
        return
    if not isinstance(obligations, list):
        raise ValueError('invalid_context_reference_obligations')
    if not obligations:
        return
    if decision.get('action') not in ('retain','revise','question'):
        raise ValueError('context_references_require_visible_finding')
    fields = original if decision['action'] == 'retain' else decision['approved_fields']
    values = [fields.get(k, '') for k in ('problem','description','solution')]
    if any(not isinstance(v, str) for v in values):
        raise ValueError('invalid_context_reference_wording')
    claims = {c['claim_id']:c for c in response['claims']}
    blocks = authoritative_text_blocks(md)
    seen = set()
    for item in obligations:
        if not isinstance(item, dict) or set(item) != {'claim_id','evidence_index','reference','role','reason'}:
            raise ValueError('invalid_context_reference_obligation')
        if (any(not isinstance(item[k], str) or not item[k].strip() for k in ('claim_id','reference','role','reason'))
                or type(item['evidence_index']) is not int
                or item['role'] not in ('calculation_context','design_context')):
            raise ValueError('invalid_context_reference_obligation')
        cid,index = item['claim_id'],item['evidence_index']
        evidence = claims.get(cid, {}).get('evidence', [])
        if not 0 <= index < len(evidence):
            raise ValueError('unbound_context_reference')
        e = evidence[index]
        quote = e.get('quote')
        if (e.get('type') != 'text' or e.get('block_id') not in blocks
                or not isinstance(quote,str) or not quote.strip()
                or quote not in blocks[e['block_id']] or not _contains(quote,item['reference'])):
            raise ValueError('context_reference_not_in_authoritative_quote')
        key = (cid,index,item['reference'],item['role'])
        if key in seen:
            raise ValueError('duplicate_context_reference_obligation')
        seen.add(key)
        if not any(_contains(value,item['reference']) for value in values):
            raise ValueError('context_reference_missing_from_final_wording')
