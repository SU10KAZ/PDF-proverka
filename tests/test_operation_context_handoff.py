import json
from copy import deepcopy
from dataclasses import replace

from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes, validate_object_operation_payload
from backend.app.pipeline.stages.findings_verify.operation_context_handoff import build_operation_context_handoff
from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews, build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request


def setup(tmp_path):
    md = tmp_path/'document.md'
    md.write_text('### BLOCK #1 [TEXT]: a\nObject A\n### BLOCK #2 [TEXT]: b\nSupporting detail\n')
    obj = {'id': 'o', 'identity': 'A', 'scope': 'local', 'kind': 'unit', 'source_refs': ['a', 'b']}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry': [obj]}))
    request, = build_object_operation_passes(tmp_path, md, enable_followup=True)
    payload = {'object_operation_results': [{'object_id': 'o', 'operation': 'document_links',
        'status': 'checked', 'reason': 'Read sources', 'source_refs': ['a', 'b'],
        'inspected_locations': [{'block_id': 'b', 'description': 'Prior observation', 'engineer_decision': 'SECRET'}]}],
        'findings': [{'object_id': 'o', 'operation': 'document_links', 'problem': 'Clarify reference',
        'description': 'Ambiguous reference', 'solution': 'Clarify', 'report_section': 'questions',
        'question_basis': 'minor_confirmed', 'profile_reason': 'Editorial ambiguity',
        'evidence': [{'block_id': 'a', 'quote': 'Object A'}]}], 'engineer_decision': 'SECRET'}
    return md, request, payload


def test_operation_to_review_retains_uncited_source_and_question_purpose(tmp_path):
    md, request, payload = setup(tmp_path)
    before = deepcopy(payload)
    result = validate_object_operation_payload(request, payload)
    finding, = result['findings']
    finding['id'] = 'F1'
    assert attach_pending_source_reviews([finding], {'a', 'b'}) == 1
    # Neither the finding's evidence nor its final wording cites b.
    assert finding['evidence'] == [{'block_id': 'a', 'quote': 'Object A'}]
    queue = build_source_review_queue([finding], md.read_text())
    wire = prepare_source_review_request(queue, finding, md.read_text())
    packet = json.loads(wire['messages'][1]['content'])
    assert [s['block_id'] for s in packet['authoritative_text']] == ['a', 'b']
    assert packet['finding_purpose']['report_section'] == 'questions'
    assert packet['upstream_operation_contexts'][0]['semantic_verified'] is False
    assert 'Prior observation' in json.dumps(packet)
    assert 'SECRET' not in json.dumps(packet)
    assert payload == before


def test_changed_source_or_unbound_request_does_not_create_handoff(tmp_path):
    md, request, payload = setup(tmp_path)
    packet = json.loads(request.messages[-1]['content'])
    assert build_operation_context_handoff(replace(request, continuation_source=None), payload, packet) is None
    md.write_text(md.read_text() + 'changed')
    assert build_operation_context_handoff(request, payload, packet) is None


def test_stale_handoff_cannot_add_sources_and_extras_are_not_forwarded(tmp_path):
    md, request, payload = setup(tmp_path)
    finding, = validate_object_operation_payload(request, payload)['findings']
    finding['id'] = 'F1'
    receipt = finding['operation_candidate_lineage'][0]
    receipt['upstream_operation_context']['engineer_decision'] = 'SECRET'
    attach_pending_source_reviews([finding], {'a', 'b'})
    queue = build_source_review_queue([finding], md.read_text())
    wire = prepare_source_review_request(queue, finding, md.read_text())
    assert 'SECRET' not in json.dumps(wire['messages'])
    stale = build_source_review_queue([finding], md.read_text() + 'changed')['packets'][0]
    assert stale['rejected_upstream_contexts'] == ['source_version_mismatch']
    assert [s['block_id'] for s in stale['sources']] == ['a']
