import copy
from backend.app.pipeline.stages.findings_merge.recovery_links import prepare_duplicate_links, apply_duplicate_links


def packet():
    return {'candidate': {'origin': 'text', 'pointer': '/text_findings/0', 'source_id': 'T-1'},
            'existing_findings': [{'id': 'F-1', 'problem': 'one atom', 'severity': 'important'},
                                  {'id': 'F-2', 'problem': 'another atom'}], 'allowed_source_refs': ['b']}


def receipt():
    return {'origin': 'text', 'pointer': '/text_findings/0', 'decision': 'duplicate',
            'target_finding_ids': ['F-1', 'F-2'], 'source_refs': ['b'], 'reason': 'two atoms already preserved'}


def test_compound_duplicate_repairs_all_links_and_preserves_claims():
    p = packet()
    updates, pending = prepare_duplicate_links(p, [receipt()])
    assert not pending and len(updates) == 2
    final = copy.deepcopy(p['existing_findings'])
    report = apply_duplicate_links(final, updates)
    assert len(report['applied']) == 2
    assert all(f['source_finding_ids'] == ['T-1'] for f in final)
    assert final[0]['problem'] == 'one atom' and final[0]['severity'] == 'important'
    before = copy.deepcopy(final)
    apply_duplicate_links(final, updates)
    assert before == final
    assert not report['semantic_verified']


def test_unknown_duplicate_ids_and_unavailable_evidence_stay_pending():
    p, r = packet(), receipt()
    r['target_finding_ids'] = ['F-999']
    assert not prepare_duplicate_links(p, [r])[0]
    r = receipt(); r['source_refs'] = ['missing']
    assert not prepare_duplicate_links(p, [r])[0]
    assert not prepare_duplicate_links(p, [receipt(), receipt()])[0]


def test_changed_or_ambiguous_target_is_not_relinked():
    p = packet(); updates, _ = prepare_duplicate_links(p, [receipt()])
    findings = p['existing_findings']
    findings[0]['problem'] = 'different claim under same ID'
    findings.append(dict(findings[1]))
    report = apply_duplicate_links(findings, updates)
    assert len(report['pending']) == 2 and not report['applied']
    assert all('source_finding_ids' not in f for f in findings)


def test_targeted_union_repairs_provenance_without_mutating_base():
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
    p = packet()
    updates, _ = prepare_duplicate_links(p, [receipt()])
    base = {'findings': p['existing_findings']}
    snapshot = copy.deepcopy(base)
    result = combine_findings_with_targeted(base, [('candidate_recovery_test', {
        'findings': [], 'candidate_decisions': [receipt()], 'recovery_link_updates': updates})])
    assert base == snapshot
    assert len(result['findings']) == 2
    assert all(f['source_finding_ids'] == ['T-1'] for f in result['findings'])
    assert result['meta']['codex_targeted_added'] == 0
