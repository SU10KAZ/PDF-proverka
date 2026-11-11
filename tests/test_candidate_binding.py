from copy import deepcopy
from backend.app.pipeline.stages.findings_merge.candidate_binding import bind_candidate_decisions
from backend.app.pipeline.stages.findings_merge.candidate_coverage import build_candidate_coverage
from backend.app.pipeline.stages.findings_merge.candidate_decisions import effective_candidate_decisions, reconcile_candidate_decisions
from backend.app.pipeline.stages.findings_merge.subclaim_coverage import pending_subclaims


def fixture():
    return build_candidate_coverage({}, {'block_analyses': [
        {'block_id': 'g1', 'findings': [{'id': 'G-102'}]},
        {'block_id': 'g2', 'findings': [{'id': 'G-104'}]}]}, {'findings': []})


def receipt(cid, pointer, action='needs_data'):
    return {'candidate_id': cid, 'origin': 'graphic', 'pointer': pointer,
            'decision': action, 'reason': 'source reviewed', 'source_refs': ['g2'],
            'subclaims': [{'id': '1', 'claim': 'check this object', 'decision': action,
                          'reason': 'source reviewed', 'source_refs': ['g2']}]}


def test_wrong_pointer_to_existing_other_candidate_is_bound_by_unique_explicit_id():
    coverage = fixture()
    row = receipt('G-104', '/block_analyses/0/findings/0', 'refuted')
    before = deepcopy(row)
    result = bind_candidate_decisions(coverage, [row])
    assert row == before
    assert result['decisions'][0]['pointer'] == '/block_analyses/1/findings/0'
    assert result['repairs'][0]['original'] == row
    checked = reconcile_candidate_decisions(coverage, [row], {'g2'})
    assert checked['decisions'][0]['reason'] == 'missing_decision'
    assert checked['decisions'][1]['status'] == 'reported_requires_semantic_review'
    assert not checked['semantic_verified']


def test_later_misaddressed_receipt_cannot_erase_other_candidate_obligation():
    coverage = fixture()
    first = receipt('G-102', '/block_analyses/0/findings/0')
    later = receipt('G-104', '/block_analyses/0/findings/0', 'refuted')
    final = {'findings': [], 'candidate_decisions': [first],
             'candidate_recovery_reports': [{'candidate_decisions': [later]}]}
    effective = effective_candidate_decisions(final, coverage)
    assert {r['candidate_id'] for r in effective} == {'G-102', 'G-104'}
    pending = pending_subclaims(final, {'g2'}, coverage)
    assert set(pending) == {('graphic', '/block_analyses/0/findings/0')}


def test_unknown_or_ambiguous_id_never_closes_valid_pointer():
    coverage = fixture()
    wrong = receipt('invented', '/block_analyses/0/findings/0', 'refuted')
    valid = receipt('G-102', '/block_analyses/0/findings/0', 'refuted')
    binding = bind_candidate_decisions(coverage, [wrong])
    assert binding['decisions'] == []
    assert binding['unresolved']
    pending = pending_subclaims({'candidate_decisions': [wrong, valid]}, {'g2'}, coverage)
    assert ('graphic', '/block_analyses/0/findings/0') in pending
    coverage['candidates'][1]['source_id'] = 'G-102'
    assert not bind_candidate_decisions(coverage, [valid])['decisions']


def test_legacy_pointer_is_not_guessed_and_duplicate_bindings_stay_visible():
    coverage = fixture()
    legacy = {'origin': 'graphic', 'pointer': '/block_analyses/99/deferred_findings/0'}
    bound = bind_candidate_decisions(coverage, [legacy])
    assert bound['decisions'] == [legacy]
    assert not bound['repairs']
    duplicate = receipt('G-104', '/block_analyses/0/findings/0')
    checked = reconcile_candidate_decisions(coverage, [duplicate, deepcopy(duplicate)], {'g2'})
    assert checked['decisions'][1]['reason'] == 'duplicate_decisions'
