"""Source-bound operation context for independent review, never a truth certificate."""
from copy import deepcopy
from .source_review_evidence import evidence_digest


def build_operation_context_handoff(request, payload, packet):
    binding = getattr(request, 'continuation_source', None)
    if not isinstance(binding, dict) or not binding.get('md_sha256'):
        return None
    from ..prepare.object_operation_followup import verify_operation_source_binding
    if verify_operation_source_binding(request) is not None:
        return None
    allowed = set(packet['allowed_source_refs'])
    refs = list(dict.fromkeys(s['block_id'] for s in packet['source_context']
                             if isinstance(s, dict) and s.get('block_id') in allowed))
    terminal = packet.get('terminal_disposition', {})
    previous = terminal.get('previous_response', {}) if isinstance(terminal, dict) else {}
    previous_operations = previous.get('object_operation_results', []) if isinstance(previous, dict) else []
    current_operations = payload.get('object_operation_results', [])
    operations = (previous_operations if isinstance(previous_operations, list) else []) + (current_operations if isinstance(current_operations, list) else [])
    inspections = []
    for op in operations:
        if not isinstance(op, dict) or op.get('object_id') != packet['object']['id']:
            continue
        for loc in op.get('inspected_locations', []) if isinstance(op.get('inspected_locations', []), list) else []:
            if isinstance(loc, dict) and loc.get('block_id') in allowed:
                inspections.append({k:deepcopy(loc[k]) for k in ('block_id','sheet','description') if k in loc})
    # Literal query lists only; model verdicts and external evaluation fields are
    # deliberately not transferred. Supplied sources are not proven inspected.
    queries = []
    for op in operations:
        if isinstance(op, dict) and op.get('object_id') == packet['object']['id']:
            for q in op.get('context_queries', []) if isinstance(op.get('context_queries', []), list) else []:
                if isinstance(q, str) and q not in queries: queries.append(q)
    terminal = packet.get('terminal_disposition', {})
    search = terminal.get('search_receipt', {}) if isinstance(terminal, dict) else {}
    return {'source_md_sha256':binding['md_sha256'], 'request_sha256':evidence_digest(request.messages),
            'supplied_source_ids':refs, 'attached_graphic_ids':list(dict.fromkeys(packet['attached_image_ids_in_order'])),
            'reported_inspections_untrusted':inspections, 'reported_context_queries':queries,
            'local_search_stop':{k:deepcopy(search[k]) for k in ('status','omitted','absence_proven') if k in search},
            'boundary':'Supplied sources and local search stop are provenance, not completeness. Inspection descriptions are prior model observations, not established facts. Recheck applicability and evidence independently.',
            'semantic_verified':False}


def sanitize_operation_context(context):
    """Allow provenance only; extra evaluation/verdict fields must not reach review."""
    result = {key: context[key] for key in ('source_md_sha256', 'request_sha256')
              if isinstance(context.get(key), str)}
    for key in ('supplied_source_ids', 'attached_graphic_ids', 'reported_context_queries'):
        values = context.get(key, [])
        result[key] = [v for v in values if isinstance(v, str)] if isinstance(values, list) else []
    allowed = set(result['supplied_source_ids'])
    observations = context.get('reported_inspections_untrusted', [])
    result['reported_inspections_untrusted'] = [
        {k: item[k] for k in ('block_id', 'sheet', 'description') if isinstance(item.get(k), str)}
        for item in observations if isinstance(item, dict) and item.get('block_id') in allowed
    ] if isinstance(observations, list) else []
    stop = context.get('local_search_stop', {})
    result['local_search_stop'] = {}
    if isinstance(stop, dict):
        if isinstance(stop.get('status'), str):
            result['local_search_stop']['status'] = stop['status']
        omitted = stop.get('omitted')
        if isinstance(omitted, list):
            result['local_search_stop']['omitted_count'] = len(omitted)
        elif type(omitted) is int:
            result['local_search_stop']['omitted_count'] = omitted
    result['local_search_stop']['absence_proven'] = False
    result['semantic_verified'] = False
    return result
