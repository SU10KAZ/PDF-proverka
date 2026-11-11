"""Give serial recovery passes current duplicate context, never historical answers."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json


def refresh_recovery_snapshot(request, base, completed_payloads):
    if not request.stage.startswith('candidate_recovery_'):
        return request
    from .codex_targeted_findings import combine_findings_with_targeted

    current = combine_findings_with_targeted(base, completed_payloads)['findings']
    messages = deepcopy(request.messages)
    packet = json.loads(messages[-1]['content'])
    packet['existing_findings'] = current
    packet['existing_findings_snapshot'] = {
        'completed_passes': len(completed_payloads), 'findings': len(current),
        'sha256': hashlib.sha256(json.dumps(
            current, ensure_ascii=False, sort_keys=True).encode()).hexdigest(),
        'purpose': 'duplicate_navigation_not_independent_source_evidence',
    }
    messages[-1]['content'] = json.dumps(packet, ensure_ascii=False)
    messages[0]['content'] += (
        '\nexisting_findings is the current output snapshot from earlier passes. '
        'Use its exact IDs only to explain duplicate links; its claims are not '
        'independent factual evidence. Inspect supplied original sources. '
        'A shared source_finding_id does not prove that all split claims are duplicates. '
        'Preserve each independently supported missing claim.'
    )
    return replace(request, messages=messages)
