from backend.app.pipeline.stages.findings_merge.candidate_coverage import build_candidate_coverage
from backend.app.pipeline.stages.findings_merge.candidate_decisions import reconcile_candidate_decisions


def test_retention_requires_actual_link_and_never_certifies_semantics():
    coverage = build_candidate_coverage({'text_findings': [{'id': 'T-1'}]}, {}, {'findings': []})
    receipt = dict(origin='text', pointer='/text_findings/0', decision='retained',
                   reason='saved', source_refs=['b'])
    report = reconcile_candidate_decisions(coverage, [receipt], {'b'})
    assert report['not_checked'] == 1
    coverage['candidates'][0]['linked_findings'] = [{'id': 'F-1'}]
    report = reconcile_candidate_decisions(coverage, [receipt], {'b'})
    assert report['not_checked'] == 0
    assert report['decisions'][0]['semantic_verified'] is False


def test_refutation_requires_addressed_evidence_and_cannot_conflict_with_output():
    coverage = build_candidate_coverage({'text_findings': [{'id': 'T-1'}]}, {}, {'findings': []})
    receipt = dict(origin='text', pointer='/text_findings/0', decision='refuted', reason='contrary value', source_refs=[])
    assert reconcile_candidate_decisions(coverage, [receipt], {'b'})['not_checked'] == 1
    receipt['source_refs'] = ['missing']
    assert reconcile_candidate_decisions(coverage, [receipt], {'b'})['not_checked'] == 1
    receipt['source_refs'] = ['b']
    assert reconcile_candidate_decisions(coverage, [receipt], {'b'})['not_checked'] == 0
    coverage['candidates'][0]['linked_findings'] = [{'id': 'F-1'}]
    assert reconcile_candidate_decisions(coverage, [receipt], {'b'})['not_checked'] == 1


def test_missing_duplicate_and_unknown_receipts_remain_visible():
    coverage = build_candidate_coverage({'text_findings': [{'id': 'T-1'}]}, {}, {'findings': []})
    receipt = dict(origin='text', pointer='/text_findings/0', decision='needs_data', reason='upper plan', source_refs=[])
    assert reconcile_candidate_decisions(coverage, [], set())['not_checked'] == 1
    assert reconcile_candidate_decisions(coverage, [receipt, receipt], set())['not_checked'] == 1
    receipt['pointer'] = '/text_findings/999'
    report = reconcile_candidate_decisions(coverage, [receipt, None], set())
    assert report['not_checked'] == 1
    assert report['unmatched_decisions'] == [receipt]
    assert report['malformed_decisions'] == [None]
