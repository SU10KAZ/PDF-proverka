from backend.app.pipeline.stages.text_analysis.context_receipts import reconcile_context_requests


def test_missing_and_invented_receipts_cannot_close_requests():
    compact = {'context_requests': [{'request_id': 'D1'}, {'request_id': 'D2'}]}
    result = {'context_request_results': [
        {'request_id': 'D2', 'status': 'resolved', 'source_refs': ['ghost'], 'reason': 'done'},
        {'request_id': 'invented', 'status': 'resolved'}]}
    report = reconcile_context_requests(compact, result, {'real'})
    assert report['unresolved'] == 2
    assert report['unknown_receipt_ids'] == ['invented']


def test_valid_receipt_is_not_semantic_verification_and_backlog_stays_open():
    compact = {'context_requests': [{'request_id': 'D1'}], 'context_request_backlog': [{'request_id': 'D2'}]}
    answer = {'status': 'resolved', 'source_refs': ['real'], 'finding_ids': ['T-1'], 'reason': 'addressed'}
    result = {'text_findings': [{'id': 'T-1'}], 'context_request_results': [dict(answer, request_id='D1'), dict(answer, request_id='D2')]}
    report = reconcile_context_requests(compact, result, {'real'})
    assert report['reported_resolved'] == 1 and report['unresolved'] == 1
    assert all(r['semantic_verified'] is False for r in report['requests'])
    result['context_request_results'].append(dict(answer, request_id='D1'))
    assert reconcile_context_requests(compact, result, {'real'})['unresolved'] == 2
