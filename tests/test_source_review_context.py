from copy import deepcopy

import pytest

from backend.app.pipeline.stages.findings_verify.source_review_context import expand_source_review_context
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_results import validate_source_review


def fixture():
    md = '### BLOCK #1 [TEXT]: t\nInitial claim\n### BLOCK #2 [TEXT]: extra\nK22 specification\n### BLOCK #3 [IMAGE]: g\nK22 summary\n'
    findings = [{'id': 'F1', 'source_evidence_review_status': 'pending', 'source_review_requests': [{'block_ids': ['t']}]}]
    return md, findings, build_source_review_queue(findings, md)


def test_expansion_preserves_sources_and_never_promotes_image_summary():
    md, findings, queue = fixture()
    original = deepcopy(queue)
    expanded = expand_source_review_context(queue, 'F1', md, ['K22'])
    packet = expanded['packets'][0]
    assert queue == original
    assert packet['finding_snapshot'] == findings[0]
    assert [s['block_id'] for s in packet['sources']] == ['t', 'extra']
    assert packet['sources'][1]['authoritative_text'] == '\nK22 specification\n'
    assert packet['required_graphic_block_ids'] == ['g']
    assert all('content' not in h for h in packet['context_search_receipt']['hits'])
    assert packet['context_search_receipt']['absence_proven'] is False
    with pytest.raises(ValueError, match='followup_limit'):
        expand_source_review_context(expanded, 'F1', md, ['K22'])


def test_changed_md_and_context_cannot_reuse_response():
    md, findings, queue = fixture()
    with pytest.raises(ValueError, match='source_version_changed'):
        expand_source_review_context(queue, 'F1', md+'changed', ['K22'])
    expanded = expand_source_review_context(queue, 'F1', md, ['K22'])
    packet = expanded['packets'][0]
    response = {'finding_id': 'F1', 'finding_sha256': packet['finding_sha256'], 'source_md_sha256': queue['source_md_sha256'],
                'claims': [{'claim_id': 'c', 'claim': 'Need diagram', 'status': 'needs_data', 'reason': 'No image inspected', 'missing_data': ['Diagram']}]}
    assert 'stale_review_context' in validate_source_review(response, expanded, findings, md)['errors']
    response['context_sha256'] = packet['context_sha256']
    assert validate_source_review(response, expanded, findings, md)['status'] == 'recorded_pending_semantic_acceptance'


def test_budget_and_zero_matches_never_prove_absence():
    md, _, queue = fixture()
    packet = expand_source_review_context(queue, 'F1', md, ['K22'], max_chars=1)['packets'][0]
    assert packet['context_search_receipt']['backlog']
    assert packet['source_context_status'] == 'needs_data'
    packet = expand_source_review_context(queue, 'F1', md, ['not present'])['packets'][0]
    assert packet['context_search_receipt']['added_block_ids'] == []
    assert not packet['context_search_receipt']['absence_proven']


def test_backlog_continuation_preserves_sources_and_stops_at_bound():
    md = ''.join(f'### BLOCK #{i} [TEXT]: t{i}\nneedle source {i}\n' for i in range(60))
    findings = [{'id':'F1','source_evidence_review_status':'pending','source_review_requests':[{'block_ids':['t0']}]}]
    queue = build_source_review_queue(findings, md)
    expanded = expand_source_review_context(queue, 'F1', md, ['needle'])
    hashes = []
    for revision in range(2, 5):
        previous = deepcopy(expanded)
        expanded = expand_source_review_context(expanded, 'F1', md, [], continue_backlog=True)
        p = expanded['packets'][0]
        assert p['context_revision'] == revision
        assert p['sources'][:len(previous['packets'][0]['sources'])] == previous['packets'][0]['sources']
        assert len(p['context_search_history']) == revision-1
        hashes.append(p['context_sha256'])
    assert len(set(hashes)) == 3
    assert len(expanded['packets'][0]['sources']) == 49
    assert expanded['packets'][0]['context_search_receipt']['backlog']
    with pytest.raises(ValueError, match='followup_limit'):
        expand_source_review_context(expanded, 'F1', md, [], continue_backlog=True)


def test_backlog_cannot_override_total_budget():
    md, _, queue = fixture()
    expanded = expand_source_review_context(queue, 'F1', md, ['K22'], max_blocks=1)
    from unittest.mock import patch
    with patch('backend.app.pipeline.stages.findings_verify.source_review_context.MAX_TOTAL_TEXT_CHARS', 1):
        with pytest.raises(ValueError, match='context_total_budget'):
            expand_source_review_context(expanded, 'F1', md, [], continue_backlog=True)
