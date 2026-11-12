"""Evidence-bound publication projection; never mutate detector history.

This is an opt-in building block, not a semantic verifier or a replacement for
candidate coverage. Consumers must retain both the returned archive and pending
items. Structural validation does not certify a model's interpretation.
"""
from copy import deepcopy
import hashlib
import json


class InvalidPublication(ValueError):
    pass


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def build_packet(finding, text_sources, graphic_refs=(), *, contract_version=2):
    """Only current run content and supplied primary sources enter the packet.

    Text source values are authoritative MD blocks; graphic refs must name
    images actually attached by the caller, not just known in a source index.
    """
    if contract_version not in (1, 2, 3):
        raise InvalidPublication('unknown_contract_version')
    if not isinstance(finding.get('id'), str) or not finding['id']:
        raise InvalidPublication('missing_finding_id')
    units = []
    for field in ('problem', 'description'):
        value = finding.get('recovery_original_description', finding.get('description')) if field == 'description' else finding.get(field)
        if not isinstance(value, str):
            raise InvalidPublication('invalid_original_text')
        if value:
            units.append({'id': field, 'text': value})
    if contract_version >= 2:
        for field in ('solution', 'risk', 'norm', 'norm_quote', 'candidate_norm_references'):
            value = finding.get(field)
            if value is None or value == '' or value == [] or value == {}:
                continue
            if not isinstance(value, (str, list, dict)):
                raise InvalidPublication('invalid_claim_field')
            text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)
            units.append({'id': field, 'text': text})
    addenda = finding.get('recovery_addenda', [])
    if not isinstance(addenda, list):
        raise InvalidPublication('invalid_addenda')
    for index, addendum in enumerate(addenda):
        for field in ('claim', 'reason'):
            value = addendum.get(field) if isinstance(addendum, dict) else None
            if not isinstance(value, str) or not value:
                raise InvalidPublication('invalid_addendum_text')
            units.append({'id': f'addendum/{index}/{field}', 'text': value})
    if addenda:
        from .recovery_addenda import render_description
        if finding.get('description') != render_description(finding.get('recovery_original_description', ''), addenda):
            raise InvalidPublication('current_description_not_bound_to_history')
    if not units:
        raise InvalidPublication('empty_input')
    if not isinstance(text_sources, dict) or not all(isinstance(k, str) and k and isinstance(v, str) and v for k, v in text_sources.items()):
        raise InvalidPublication('invalid_text_sources')
    graphic_refs = list(graphic_refs)
    if not all(isinstance(k, str) and k for k in graphic_refs) or set(graphic_refs) & set(text_sources):
        raise InvalidPublication('invalid_graphic_refs')
    packet = {'finding_id': finding['id'], 'finding_sha256': fingerprint(finding),
            'input_units': units, 'text_sources': deepcopy(text_sources),
            'attached_graphic_refs': list(dict.fromkeys(graphic_refs))}
    if contract_version >= 2:
        packet['contract_version'] = contract_version
    if contract_version == 3:
        packet['finding_purpose'] = {key: deepcopy(finding.get(key)) for key in
                                     ('report_section', 'question_basis', 'profile_reason',
                                      'engineer_action_required')}
        packet['purpose_is_prior_classification_not_evidence'] = True
    return packet


INSTRUCTIONS = '''Reconcile this finding against the supplied primary sources.
The earlier finding and addenda are claims, not evidence. Return JSON with
packet_sha256 (provided by the caller) and parts. Split independently checkable
claims. Each part has id, statement (current Russian wording), outcome
(supported/question/refuted/needs_data/out_of_profile/superseded/provenance_only), reason, input_spans,
evidence, and required_context. Every input character must be accounted for in
exactly one input span: {unit_id,start,end,quote}, Python character offsets,
end exclusive, quote exactly equal to the corresponding input text. Split an
input unit if different clauses need different outcomes. Cover old caveats,
headings and service references too; explain their removal without treating them
as independent technical defects. Do not discard a true part of a mixed claim.
Use exactly two top-level fields: packet_sha256 and parts. All decisions, including
questions, obsolete limitations and service references, belong inside parts.
Use superseded for an old context limitation now resolved by primary evidence;
use provenance_only for a service reference or processing note, never for a
technical claim. These are history-only outcomes, not questions for an engineer
and not counted as false technical findings. A superseded outcome requires evidence.
Do not repeat the same conflict in separate supported parts: combine complementary
statements about the same issue and retain all their input spans and evidence.
Parts are audit dispositions, not counts of independent project defects.
Evidence entries: {block_id,type:text,quote:<literal authoritative text>} or
{block_id,type:graphic,observation:<concrete observation in an attached image>}.
Supported or refuted outcomes require evidence. Insufficient context is needs_data,
not refuted; list missing context in required_context. Questions and out_of_profile
items go to a separate questions section. Supported means a source-backed technical
conflict, not an assumption about physical installation, regulatory applicability,
object identity or command-versus-feedback semantics. Minor editorial issues are
questions. Do not silently correct MD TEXT using an image. Inspect GRAPHIC images.
Remove stale caveats from current wording only when their specific limitation is
resolved by supplied evidence; preserve unresolved limitations as needs_data.
Do not expose parent: aliases or old local finding IDs in current statements.
Input units can also include solution, risk and normative references. Reconcile
these too: do not retain an unsafe correction, an unproved consequence or an
unverified normative applicability merely because the description was corrected.
Keep conditional risks conditional, and preserve useful recommendations to
reconcile documents without inventing the correct design. Do not count a risk or
recommendation as another independent defect.
No engineer decisions or reference answers are supplied. Your output is a proposed
publication, not independent acceptance. Do not invent sources or norm quotations.
'''


PURPOSE_AWARE_INSTRUCTIONS = INSTRUCTIONS + '''
The optional finding_purpose records the original report routing and reason, not
source evidence or an engineer verdict. Evaluate factual support and usefulness
separately. A literal source observation alone does not justify promoting an
existing clarification into a technical defect; identify a substantive defect
basis before proposing that change. Conversely, retain a confirmed technical
conflict even when the correct replacement design is unknown.
Keep a recommendation to fix the same conflict with that conflict, not as an
additional question. An actionable source ambiguity can remain a question with
its unresolved conclusion explicitly stated; do not hide it solely because a
normative or design conclusion is unverified. Missing context available in the
project remains an internal retrieval obligation, not automatically a question
for the engineer. Do not convert all needs_data parts into questions.
Prior routing must not be preserved merely because it exists. When linked sources
for the same object and scope explicitly disagree in quantity, type or connection,
the disagreement itself is a technical document conflict. Uncertainty about which
source is correct, possible reserve equipment or actual procurement does not by
itself demote that conflict to a question. Preserve the conflict, qualify the
unknown correction and consequences separately. Conversely, different names or
references without established common applicability may remain a clarification.
'''

def reconcile_publication(finding, packet, response):
    """Validate a complete disposition and return an isolated publication view.

    Does not update candidate receipts: disappearance from the current view must
    never automatically close an unresolved detection/recovery obligation.
    """
    if packet.get('finding_sha256') != fingerprint(finding) or packet.get('finding_id') != finding.get('id'):
        raise InvalidPublication('finding_changed_after_packet')
    expected = build_packet(finding, packet.get('text_sources'), packet.get('attached_graphic_refs', []),
                            contract_version=packet.get('contract_version', 1))
    if packet != expected:
        raise InvalidPublication('packet_input_changed')
    if not isinstance(response, dict) or response.get('packet_sha256') != fingerprint(packet):
        raise InvalidPublication('response_packet_mismatch')
    if set(response) != {'packet_sha256', 'parts'}:
        raise InvalidPublication('unexpected_response_fields')
    parts = response.get('parts')
    if not isinstance(parts, list) or not parts:
        raise InvalidPublication('missing_parts')
    units = {u['id']: u['text'] for u in packet['input_units']}
    intervals = {key: [] for key in units}
    seen = set()
    result = {'technical_findings': [], 'questions': [], 'withdrawn': [], 'pending': [],
              'history_only': [], 'part_counts_not_finding_counts': True,
              'archive': deepcopy(finding), 'packet_sha256': fingerprint(packet),
              'semantic_verified': False, 'candidate_coverage_updated': False,
              'review_scope': 'claim_fields' if packet.get('contract_version', 1) >= 2 else 'description_only_legacy',
              'unreviewed_claim_fields': [k for k in ('solution', 'risk', 'norm', 'norm_quote', 'candidate_norm_references')
                                         if finding.get(k) and k not in units]}
    for part in parts:
        if not isinstance(part, dict) or not all(isinstance(part.get(k), str) and part[k].strip() for k in ('id', 'statement', 'reason')):
            raise InvalidPublication('missing_part_fields')
        if part['id'] in seen:
            raise InvalidPublication('duplicate_part_id')
        seen.add(part['id'])
        route = {'supported': 'technical_findings', 'question': 'questions',
                 'out_of_profile': 'questions', 'refuted': 'withdrawn', 'needs_data': 'pending',
                 'superseded': 'history_only', 'provenance_only': 'history_only'}.get(part.get('outcome'))
        if route is None:
            raise InvalidPublication('invalid_outcome')
        spans = part.get('input_spans')
        if not isinstance(spans, list) or not spans:
            raise InvalidPublication('missing_input_spans')
        for span in spans:
            if not isinstance(span, dict) or not isinstance(span.get('unit_id'), str) or span['unit_id'] not in units:
                raise InvalidPublication('unknown_input_unit')
            start, end = span.get('start'), span.get('end')
            text = units[span['unit_id']]
            if type(start) is not int or type(end) is not int or not 0 <= start < end <= len(text) or span.get('quote') != text[start:end]:
                raise InvalidPublication('invalid_input_span')
            intervals[span['unit_id']].append((start, end))
        evidence = part.get('evidence')
        if not isinstance(evidence, list) or (part['outcome'] in {'supported', 'refuted', 'superseded'} and not evidence):
            raise InvalidPublication('missing_evidence')
        for ev in evidence:
            if not isinstance(ev, dict) or not isinstance(ev.get('block_id'), str):
                raise InvalidPublication('invalid_evidence')
            ref = ev['block_id']
            if ev.get('type') == 'text':
                quote = ev.get('quote')
                if ref not in packet['text_sources'] or not isinstance(quote, str) or not quote.strip() or quote not in packet['text_sources'][ref]:
                    raise InvalidPublication('text_quote_not_in_supplied_source')
            elif ev.get('type') == 'graphic':
                observation = ev.get('observation')
                if ref not in packet['attached_graphic_refs'] or not isinstance(observation, str) or not observation.strip():
                    raise InvalidPublication('graphic_not_supplied_or_observed')
            else:
                raise InvalidPublication('invalid_evidence_type')
        required = part.get('required_context')
        if not isinstance(required, list) or not all(isinstance(x, str) and x.strip() for x in required) or (part['outcome'] == 'needs_data' and not required):
            raise InvalidPublication('missing_required_context')
        result[route].append({**deepcopy(part), 'parent_finding_id': finding['id'],
                              'publication_id': f"{finding['id']}:{part['id']}",
                              'semantic_verified': False})
    for key, spans in intervals.items():
        cursor = 0
        for start, end in sorted(spans):
            if start != cursor:
                raise InvalidPublication('omitted_or_overlapping_input_text')
            cursor = end
        if cursor != len(units[key]):
            raise InvalidPublication('omitted_or_overlapping_input_text')
    return result


def rebind_unique_input_quotes(packet, response):
    """Repair only wrong offsets of literal, uniquely located input quotes.

    Preserve the raw response separately. This never rewrites a claim, outcome,
    evidence or quote. The caller must still run full reconciliation validation:
    unique occurrences alone do not prove complete, nonoverlapping coverage.
    """
    bound = deepcopy(response)
    units = {u['id']: u['text'] for u in packet['input_units']}
    repairs = []
    if not isinstance(bound, dict) or not isinstance(bound.get('parts'), list):
        raise InvalidPublication('missing_parts')
    for part in bound['parts']:
        if not isinstance(part, dict) or not isinstance(part.get('input_spans'), list):
            raise InvalidPublication('missing_input_spans')
        for index, span in enumerate(part['input_spans']):
            if not isinstance(span, dict) or not isinstance(span.get('unit_id'), str) or span['unit_id'] not in units:
                raise InvalidPublication('unknown_input_unit')
            text, quote = units[span['unit_id']], span.get('quote')
            if not isinstance(quote, str) or not quote:
                raise InvalidPublication('missing_literal_input_quote')
            start, end = span.get('start'), span.get('end')
            if type(start) is int and type(end) is int and 0 <= start < end <= len(text) and text[start:end] == quote:
                continue
            location = text.find(quote)
            if location < 0 or text.find(quote, location + 1) >= 0:
                raise InvalidPublication('input_quote_not_uniquely_located')
            repairs.append({'part_id': part.get('id'), 'span_index': index, 'unit_id': span['unit_id'],
                            'reported': {'start': start, 'end': end},
                            'bound': {'start': location, 'end': location + len(quote)}})
            span.update(start=location, end=location + len(quote))
    return bound, repairs
