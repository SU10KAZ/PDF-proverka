"""One evidence-format correction per recovery request, without inferred evidence."""
from copy import deepcopy
import hashlib
import json


def build_recovery_format_followup(request, payload):
    from .codex_targeted_findings import CodexTargetedPass

    packet = json.loads(request.messages[-1]['content'])
    if packet.get('recovery_format_repair'):
        return None, {'status': 'format_repair_limit'}
    pending = [row for row in payload.get('recovery_pending', [])
               if isinstance(row, dict)
               and row.get('reason') == 'missing_inspected_source_reference'
               and isinstance(row.get('finding'), dict)]
    if not pending:
        return None, {'status': 'no_repairable_evidence_format'}
    # Preserve the same source packet and images. No source reference is copied
    # from candidate decisions into a finding: the model must inspect and assign it.
    packet['recovery_format_repair'] = {
        'attempt': 1, 'parent_stage': request.stage,
        'rejected_findings': deepcopy(pending),
        'previous_candidate_decisions': deepcopy(payload.get('candidate_decisions', [])),
        'already_admitted_findings': deepcopy(payload.get('findings', [])),
    }
    instruction = '''Correct only the rejected_findings in recovery_format_repair.
Their missing structured evidence is not a technical refutation or confirmation.
Inspect the same supplied sources again. For each supported claim, return a finding
with evidence as a nonempty ARRAY of objects: block_id from allowed_source_refs,
type text/image, and an exact TEXT quote or a concrete attached-image observation.
Assign evidence separately to each split finding. Candidate-level source_refs and
related_block_ids do not establish support for every child claim. Never manufacture
evidence, copy all references indiscriminately, or use graphic summaries as images.
If unsupported, do not publish it; explain needs_data in candidate_decisions.
Do not re-emit already_admitted_findings, introduce new claims or request new source
searches in this format correction. Preserve source_finding_ids and report_section.
Use only severity values КРИТИЧЕСКОЕ, ЭКОНОМИЧЕСКОЕ, ЭКСПЛУАТАЦИОННОЕ,
РЕКОМЕНДАТЕЛЬНОЕ, ПРОВЕРИТЬ ПО СМЕЖНЫМ. Return the ordinary recovery JSON.
'''
    followup = CodexTargetedPass(
        stage=request.stage + '_format',
        output_filename=request.output_filename.replace('.json', '_format.json'),
        messages=[{'role': 'system', 'content': request.messages[0]['content'] + '\n' + instruction},
                  {'role': 'user', 'content': json.dumps(packet, ensure_ascii=False)}],
        image_paths=request.image_paths, continuation_source=request.continuation_source)
    return followup, {'status': 'scheduled', 'rejected_findings': len(pending),
                      'parent_stage': request.stage,
                      'payload_sha256': hashlib.sha256(json.dumps(
                          payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest(),
                      'evidence_inferred': False}
