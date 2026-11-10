from copy import deepcopy

from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue


MD = '## Page 1\n### BLOCK #1 [TEXT]: t\nExact source 12\n### BLOCK #2 [GRAPHIC]: g\nModel summary\n'


def finding(refs=None):
    return {'id': 'F-001', 'problem': 'Final claim',
            'source_evidence_review_status': 'pending',
            'source_review_requests': [{'original_claim': 'Original claim',
                                        'block_ids': refs if refs is not None else ['t']}]}


def test_packet_keeps_independent_original_and_final_claim_snapshots():
    findings = [finding(), {'id': 'F-002'}]
    before = deepcopy(findings)
    queue = build_source_review_queue(findings, MD)
    assert findings == before
    assert queue['pending_count'] == 1
    packet = queue['packets'][0]
    assert packet['finding_snapshot'] == before[0]
    assert packet['sources'] == [{'block_id': 't', 'authoritative_text': '\nExact source 12\n'}]
    assert packet['source_context_status'] == 'available_bounded_text'
    assert not packet['semantic_verified'] and not queue['automatic_dispatch']
    assert build_source_review_queue(findings, MD) == queue
    findings[0]['source_review_requests'][0]['original_claim'] = 'Changed'
    assert packet['finding_snapshot'] == before[0]
    assert build_source_review_queue(findings, MD)['packets'][0]['finding_sha256'] != packet['finding_sha256']


def test_graphics_missing_and_oversized_blocks_are_explicit_not_truncated():
    packet = build_source_review_queue([finding(['g', 'missing', 't', 't'])], MD, max_chars=1)['packets'][0]
    assert packet['sources'] == []
    assert packet['unresolved_block_ids'] == ['g', 'missing']
    assert packet['budgeted_block_ids'] == ['t']
    assert packet['source_context_status'] == 'needs_data'
    assert build_source_review_queue([finding()], '')['packets'][0]['unresolved_block_ids'] == ['t']
    assert build_source_review_queue([finding([])], MD)['packets'][0]['source_context_status'] == 'needs_data'


def test_ambiguous_finding_or_block_identity_cannot_be_ready():
    queue = build_source_review_queue([finding(), finding()], MD)
    assert all(p['status'] == 'not_ready' for p in queue['packets'])
    duplicate = MD + '### BLOCK #3 [TEXT]: t\nConflicting source\n'
    packet = build_source_review_queue([finding()], duplicate)['packets'][0]
    assert packet['unresolved_block_ids'] == ['t']
    assert packet['sources'] == []
