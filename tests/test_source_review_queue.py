from copy import deepcopy
import json

from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews


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
    assert packet['required_graphic_block_ids'] == ['g']
    assert packet['missing_or_ambiguous_block_ids'] == ['missing']
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


def test_image_headers_require_raster_and_duplicate_headers_remain_ambiguous():
    md = '### BLOCK #1 [IMAGE]: i\nA generated description\n'
    packet = build_source_review_queue([finding(['i'])], md)['packets'][0]
    assert packet['sources'] == []
    assert packet['required_graphic_block_ids'] == ['i']
    packet = build_source_review_queue([finding(['i'])], md + '### BLOCK #2 [TEXT]: i\nConflicting identity\n')['packets'][0]
    assert packet['required_graphic_block_ids'] == []
    assert packet['missing_or_ambiguous_block_ids'] == ['i']


def test_direct_evidence_does_not_bypass_lineage_review_and_stale_links_are_ignored():
    f = {'id': 'F-1', 'problem': 'Final claim', 'severity': 'critical', 'related_block_ids': ['t'],
         'source_finding_ids': ['T-1'], 'source_candidate_lineage': [
             {'source_finding_id': 'T-1', 'status': 'requires_semantic_review',
              'source_claim': 'Original claim', 'existing_block_ids': ['t', 'missing']},
             {'source_finding_id': 'T-2', 'status': 'requires_semantic_review',
              'source_claim': 'Stale claim', 'existing_block_ids': ['t']} ]}
    before = deepcopy(f)
    assert attach_pending_source_reviews([f], {'t'}) == 1
    assert attach_pending_source_reviews([f], {'t'}) == 0
    assert all(f[k] == v for k, v in before.items())
    assert f['source_review_requests'] == [{'source_finding_id': 'T-1', 'source_claim': 'Original claim', 'block_ids': ['t']}]
    assert build_source_review_queue([f], MD)['pending_count'] == 1


def test_structural_phantom_cleanup_still_runs_before_queue_attachment(tmp_path):
    from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
    f = {'id': 'F-1', 'problem': 'Claim', 'severity': 'КРИТИЧЕСКОЕ', 'related_block_ids': ['phantom'],
         'source_finding_ids': ['T-1'], 'source_candidate_lineage': [
             {'source_finding_id': 'T-1', 'status': 'requires_semantic_review',
              'source_claim': 'Source claim', 'existing_block_ids': ['t']}]}
    (tmp_path/'03_findings.json').write_text(json.dumps({'findings': [f]}))
    (tmp_path/'01_blocks_analysis.json').write_text(json.dumps({'block_analyses': [{'block_id': 't', 'page': 1}]}))
    (tmp_path/'02_work').mkdir()
    (tmp_path/'02_work/document.md').write_text(MD)
    stats = _run_deterministic_phase(tmp_path, 'synthetic-queue-test')
    assert not stats['source_review_queue_error']
    assert stats['phantom_cleaned'] == 1
    after = json.loads((tmp_path/'03_findings.json').read_text())['findings'][0]
    assert after['related_block_ids'] == []
    assert after['severity'] == f['severity'] and after['problem'] == f['problem']
    assert after['source_evidence_review_status'] == 'pending'
    assert stats['source_reviews_pending'] == 1
