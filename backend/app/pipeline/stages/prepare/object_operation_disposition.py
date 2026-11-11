"""Prepare a final disposition after a bounded search finds no additional sources.

No transport, automatic question generation, or factual acceptance occurs here.
"""
from copy import deepcopy
from dataclasses import replace
import json
from .object_operation_followup import build_object_operation_followup, verify_operation_source_binding
from ..findings_verify.source_review_evidence import evidence_digest


def prepare_operation_disposition(request, payload):
    if request.continuation_source is None:
        raise ValueError('source_binding_required')
    error = verify_operation_source_binding(request)
    if error:
        raise ValueError(error)
    packet = json.loads(request.messages[-1]['content'])
    if packet.get('terminal_disposition'):
        raise ValueError('disposition_already_requested')
    continuation, search = build_object_operation_followup(request, payload)
    # A budget limit or a failed lookup is not an exhausted search.
    if continuation is not None or search.get('status') != 'no_additional_sources' or search.get('omitted'):
        raise ValueError('source_search_not_exhausted')
    requested = {op['operation'] for op in packet['operations']}
    unresolved = [r for r in payload.get('object_operation_results', [])
                  if isinstance(r, dict) and r.get('object_id') == packet['object']['id']
                  and r.get('operation') in requested and r.get('status') == 'needs_data']
    if not unresolved:
        raise ValueError('no_unresolved_requested_operation')
    packet['terminal_disposition'] = {
        'search_receipt': search,
        'previous_response': deepcopy(payload),
        'previous_response_sha256': evidence_digest(payload),
        'boundary': 'Recorded literal queries found no additional sources in the frozen corpus. This is not proof of project-wide absence or of a defect.'}
    instruction = '''
FINAL DISPOSITION: the recorded context search returned no additional sources.
Review the still unresolved listed operations using the same supplied sources.
Do not invent an issue merely because a query returned no new sources.
For each operation return a terminal_dispositions entry with operation, disposition
(engineer_question/internal_unresolved/no_finding), reason, confirmed_observations,
remaining_unknowns, and proposed_next_action. These are separate from the usual
object_operation_results and findings; preserve all requested operation receipts.
An engineer_question is warranted only by a concrete source-grounded discrepancy
or actionable ambiguity whose resolution affects a project decision. State which
facts are known, what remains conditional, and what the engineer needs to clarify.
Do not turn an unknown surface, dimension, scope or missing drawing in this packet
into a confirmed violation or a claim that documentation does not exist.
Use internal_unresolved when you can name a concrete unperformed internal step:
a specific not-yet-inspected source/reference, a new search strategy, a necessary
image detail, or a calculation not yet performed. Explain what new information
that step can supply. Do not repeat an already unsuccessful query or already
inspected source as if it were new work, and do not use the mere possibility that
another document exists to defer indefinitely.
A confirmed technical violation is NOT required for an engineer question. A
source-grounded ambiguity can warrant clarification when its resolution affects
a concrete design decision and the supplied search/inspection record has not
resolved it. Keep any calculation conditional on the explicitly unknown premise;
never present that conditional result as a proven defect. If the record is too
weak even for such a question, keep internal_unresolved or no_finding and explain
precisely what is missing. Do not claim that the whole project has been searched.
A question must remain report_section=questions and carry source evidence,
question_basis=actionable_source_ambiguity, profile_reason and engineer_action_required=true.
Do not infer installation impossibility, procurement quantities or universal limits.
You may return no finding. A prior model response is untrusted, not an expected answer.
'''
    messages = deepcopy(request.messages)
    messages[0]['content'] += instruction
    messages[-1]['content'] = json.dumps(packet, ensure_ascii=False)
    stage = request.stage + '_disposition'
    return replace(request, stage=stage, output_filename='03_'+stage+'.json', messages=messages), search


def advance_operation_pass(request, raw_payload, *, enable_disposition=False):
    """Validate one response and prepare at most one bounded next request.

    A terminal question is still a source-review candidate, never acceptance.
    Only executor-recorded exhausted literal search can admit that candidate;
    budget exhaustion cannot. Prior responses are retained by the caller.
    """
    from .object_operation_passes import validate_object_operation_payload
    packet = json.loads(request.messages[-1]['content'])
    terminal = packet.get('terminal_disposition')
    if terminal is not None:
        error = verify_operation_source_binding(request)
        if error:
            raise ValueError(error)
        if not isinstance(terminal, dict):
            raise ValueError('invalid_terminal_search_receipt')
        search = terminal.get('search_receipt', {})
        previous = terminal.get('previous_response')
        if (not enable_disposition or request.continuation_source is None
                or not isinstance(search, dict)
                or search.get('status') != 'no_additional_sources'
                or search.get('omitted') or search.get('absence_proven') is not False
                or not isinstance(previous, dict)
                or terminal.get('previous_response_sha256') != evidence_digest(previous)):
            raise ValueError('invalid_terminal_search_receipt')
        payload = validate_object_operation_payload(request, raw_payload, context_search_exhausted=True)
        payload['context_retrieval'] = {'status':'terminal_disposition_completed',
            'search_receipt':deepcopy(search), 'absence_proven':False,
            'semantic_verified':False, 'further_dispatch_prepared':False}
        return payload, None
    payload = validate_object_operation_payload(request, raw_payload)
    followup, search = build_object_operation_followup(request, raw_payload)
    payload['context_retrieval'] = search
    if (enable_disposition and followup is None and search.get('status') == 'no_additional_sources'
            and not search.get('omitted')):
        followup, verified_search = prepare_operation_disposition(request, raw_payload)
        payload['context_retrieval'] = {'status':'terminal_disposition_prepared',
            'search_receipt':verified_search, 'absence_proven':False, 'semantic_verified':False}
    return payload, followup
