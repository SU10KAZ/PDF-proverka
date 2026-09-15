"""Synthetic diagnostic invariants; no corpus or expected engineering answers."""
from copy import deepcopy
from pathlib import Path
import pytest

from .diagnose_v5 import (DiagnosticAccess, V4, allocation_selection,
                         classify_loss, page_losses, select_events)
from .common import fingerprint


def package(confidence='STRONG', pages=(1, 2), supported=True):
    return dict(candidate_subject=dict(confidence=confidence),
                evidence_packet=dict(evidence=dict(old=[dict(page=p) for p in pages], new=[])),
                canonical_subjects=dict(s=dict(confidence='SUBJECT_POSSIBLE' if supported else 'SUBJECT_UNRESOLVED')))


def test_loss_ignores_duplicate_evidence_ids_and_order():
    assert page_losses(package(pages=(2, 1, 2)), package(pages=(1,))) == dict(old=[2], new=[])


def test_strong_selection_is_not_limited_to_diagnostic_roster():
    rows = select_events({'x': package()}, {'x': package('POSSIBLE')}, set())
    assert rows == [('STRONG', 'x')]


def test_delivery_selection_requires_preexisting_diagnostic_roster():
    before, after = {'x': package()}, {'x': package(pages=(1,))}
    assert select_events(before, after, set()) == []
    assert select_events(before, after, {'x'}) == [('DELIVERY', 'x')]


def test_zero_supported_endpoint_not_automatically_a_delivery_regression():
    assert select_events({'x': package('POSSIBLE')},
                         {'x': package('POSSIBLE', pages=(), supported=False)}, {'x'}) == []


def test_changed_roster_fails_closed():
    with pytest.raises(ValueError, match='roster'):
        select_events({'a': package()}, {'b': package()}, {'a'})


def test_selection_never_consumes_verdict_fields():
    a, b = package(), package(pages=(1,))
    result = select_events({'x': a}, {'x': b}, {'x'})
    a['expected_answer'] = 'NOT'; b['expected_answer'] = 'REAL'
    assert select_events({'x': a}, {'x': b}, {'x'}) == result


def location(reason='BUDGET_LIMIT', accepted=True):
    return dict(side='old', page=7, support=[dict(accepted=accepted, reason='NATIVE_SUBJECT_CONTEXT', source_type='TEXT')],
                v4_requirements=[dict(delivery=dict(omission_reason=reason))])


def test_budget_loss_is_not_mislabeled_graphic_subject_loss():
    assert classify_loss(location(), [])['subtype'] == 'FIXED_BUDGET_REQUIREMENT_ORDER'


def test_dependency_displacement_needs_counterfactual_page_match():
    loc = location()
    assert classify_loss(loc, [('OLD', 'A', 'v1', 7)])['category'] == 'CONTINUATION_SIDE_EFFECT'
    assert classify_loss(loc, [('NEW', 'A', 'v1', 7)])['category'] == 'OTHER'


def test_wrong_document_loss_is_not_a_restore_target():
    loc = location(); loc['support'][0].update(accepted=False, reason='WRONG_DOCUMENT_OR_CIPHER')
    loc['v4_requirements'] = []
    assert classify_loss(loc, [])['repair_target'] is False


def test_accepted_subject_lost_before_allocation_is_identity_failure():
    loc = location(); loc['v4_requirements'] = []
    assert classify_loss(loc, [])['category'] == 'CORRESPONDENCE_RULE_SIDE_EFFECT'


def test_missing_native_graphic_needs_source_review():
    loc = location(); loc['v4_requirements'] = []
    loc['support'][0].update(accepted=False, source_type='GRAPHIC', reason='NO_NATIVE_SUBJECT_SUPPORT')
    assert classify_loss(loc, [])['requires_raster_confirmation'] is True


def test_unexplained_loss_is_not_promoted_to_graphic():
    loc = location(); loc.update(support=[], v4_requirements=[])
    assert classify_loss(loc, [])['subtype'] == 'UNEXPLAINED_REQUIRES_REVIEW'


def test_allocation_retains_duplicate_requirement_turns():
    reqs = [dict(requirement_id=q, side=s, page=p, document='A', document_version=s)
            for q, s, p in [('a', 'OLD', 1), ('b', 'OLD', 1), ('c', 'OLD', 2),
                            ('d', 'NEW', 1), ('e', 'NEW', 2)]]
    receipts = {r['requirement_id']: dict(priority=3, available=True, type_match='YES') for r in reqs}
    chosen = allocation_selection(reqs, receipts, receipts, 3)
    assert chosen == {('OLD', 'A', 'OLD', 1), ('NEW', 'A', 'NEW', 1), ('NEW', 'A', 'NEW', 2)}
    assert allocation_selection(reqs, receipts, receipts, 0) == set()
    assert fingerprint(sorted(chosen)) == fingerprint(sorted(allocation_selection(deepcopy(reqs), receipts, receipts, 3)))


@pytest.mark.parametrize('event,args', [('socket.connect', ()), ('socket.getaddrinfo', ()),
                                      ('subprocess.Popen', ()), ('os.system', ())])
def test_no_network_or_indirect_inference(tmp_path, event, args):
    with pytest.raises(PermissionError, match='offline'):
        DiagnosticAccess(tmp_path, []).check(event, args)


def test_truth_and_holdout_reads_fail_closed(tmp_path):
    guard = DiagnosticAccess(tmp_path, [V4 / 'PACKAGE_INDEX.json'])
    guard.check('open', (str(V4 / 'PACKAGE_INDEX.json'), 'r', 0))
    for path in (V4 / 'DIAGNOSTIC_12_RESULTS.json', V4.parent / 'final_holdout.json'):
        with pytest.raises(PermissionError):
            guard.check('open', (str(path), 'r', 0))


def test_previous_artifact_writes_fail_even_if_read_admitted(tmp_path):
    path = V4 / 'PACKAGE_INDEX.json'
    guard = DiagnosticAccess(tmp_path, [path])
    with pytest.raises(PermissionError):
        guard.check('open', (str(path), 'w', 0))


def test_output_write_is_not_reported_as_a_source_read(tmp_path):
    guard = DiagnosticAccess(tmp_path, [])
    guard.check('open', (str(tmp_path / 'result.json'), 'w', 0))
    assert not guard.reads
