"""Check a fresh reference decision before normal admission validation.

This is a completeness gate, not a semantic reviewer or release authorization.
It does not change the frozen trial or the prepared selection pilot.
"""
import hashlib
import json
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_reference_retention import check_context_reference_retention


def validate_reference_review(preparation, decision, response, original, md):
    if preparation.get('status') != 'prepared_for_fresh_semantic_adjudication':
        raise ValueError('reference_review_not_prepared')
    payload = json.loads(preparation['request']['messages'][1]['content'])
    if (preparation['original_response_sha256'] != evidence_digest(response)
            or payload['untrusted_review'] != response
            or decision.get('response_sha256') != evidence_digest(response)
            or response.get('source_md_sha256') != hashlib.sha256(md.encode()).hexdigest()):
        raise ValueError('stale_reference_review')
    required = preparation['required_obligations']
    if not required or required != payload['untrusted_reference_repair']['selected_context_reference_obligations']:
        raise ValueError('inconsistent_reference_preparation')
    if decision.get('reference_selection_requires_revision', False) is not False:
        raise ValueError('reference_selection_requires_revision')
    # A reviewer may explain a changed selection, but it must be resolved in a
    # new preparation. Dropping an obligation never silently approves release.
    actual = decision.get('context_reference_obligations')
    keys = ('claim_id', 'evidence_index', 'reference', 'role')
    if not isinstance(actual, list) or any(not isinstance(x, dict) for x in actual):
        raise ValueError('reference_selection_incomplete')
    expected_keys = sorted(tuple(x[k] for k in keys) for x in required)
    try:
        actual_keys = sorted(tuple(x[k] for k in keys) for x in actual)
    except (KeyError, TypeError):
        raise ValueError('reference_selection_incomplete') from None
    if actual_keys != expected_keys:
        raise ValueError('reference_selection_incomplete')
    if decision.get('final_wording_verified') is not True:
        raise ValueError('reference_wording_not_reviewed')
    check_context_reference_retention(response, decision, original, md)
    return {'status': 'ready_for_normal_admission_validation',
            'automatic_release': False, 'semantic_acceptance': False}
