"""Coalescing must preserve exact search coverage and accepted answers."""
from collections import Counter
from copy import deepcopy
from itertools import product

import pytest

from backend.app.services.project_change_v3 import miner_plan as plan, mapper_portions as mp, native_text_portions as nt
from backend.tests.project_change_v3.test_mapper_portions import _region


def _pages(old, new, text='abcdefgh'):
    return {(s, p): {'side': s, 'physical_page': p, 'native_page_text': text}
            for s, nums in [('OLD', old), ('NEW', new)] for p in nums}


def _coalesce(parts, pages, *, reused=(), fits=lambda _: True):
    return plan.coalesce(parts, reused_ids=set(reused), splits=[{'region_id': 'R', 'parts': parts}],
                         previous_merges=[], pages=pages, fits=fits)


def _coverage(parts):
    covered = Counter()
    for p in parts:
        covered.update(product(p['old_pages'], p['new_pages']))
    return covered


def test_hundreds_of_page_pairs_coalesce_but_accepted_holes_are_never_revisited():
    pages = _pages(range(1, 20), range(1, 20))
    parts = mp.split_region_for_envelope(_region('R', list(range(1, 20)), list(range(1, 20))),
                                         lambda r: len(r['old_pages']) == len(r['new_pages']) == 1)
    before = deepcopy(parts)
    accepted = {p['region_id'] for p in parts[:13]}
    merged, receipts = _coalesce(parts, pages, reused=accepted)
    assert len(parts) == 361 and len(merged) < 40
    assert _coverage(merged) == _coverage(parts)
    assert [p for p in merged if p['region_id'] in accepted] == parts[:13]
    assert parts == before and receipts
    repeated, _ = plan.coalesce(merged, reused_ids=accepted, splits=[{'region_id': 'R', 'parts': parts}],
                                previous_merges=receipts, pages=pages, fits=lambda _: True)
    assert _coverage(repeated) == _coverage(parts)


def test_windows_coalesce_only_when_their_rectangle_has_no_gap_or_overlap():
    pages = _pages([1], [2])
    texts = {k: v['native_page_text'] for k, v in pages.items()}
    parts = nt.split_region(_region('R', [1], [2]), texts,
                            lambda p: all(w['end'] - w['start'] <= 4 for w in p[nt.KEY]))
    merged, _ = _coalesce(parts, pages)
    assert len(merged) == 1 and nt.KEY not in merged[0]
    remaining, _ = _coalesce(parts, pages, reused=[parts[0]['region_id']])
    rectangles = [[(w['start'], w['end']) for w in p[nt.KEY]] for p in remaining]
    nt.validate_coverage([(0, 8), (0, 8)], rectangles)
    assert remaining[0] == parts[0] and len(remaining) < len(parts)
    assert plan._candidate([parts[0], parts[0]], 'R', pages) is None
    assert plan._candidate([parts[0], parts[-1]], 'R', pages) is None


def test_incompatible_evidence_or_over_budget_candidates_are_not_merged():
    parts = mp.split_region_for_envelope(_region('R', [1, 2], [1, 2]),
                                         lambda r: len(r['old_pages']) == len(r['new_pages']) == 1)
    assert _coalesce(parts, _pages([1, 2], [1, 2]), fits=lambda _: False) == (parts, [])
    parts[1]['synthetic_subject'] = 'different'
    assert plan._candidate(parts[:2], 'R', _pages([1, 2], [1, 2])) is None
    with pytest.raises(ValueError, match='cyclic'):
        plan._root('R', {'R': 'X', 'X': 'R'})


def test_bad_window_digest_is_refused_before_coalescing():
    pages = _pages([1], [2])
    parts = nt.split_region(_region('R', [1], [2]), {k: v['native_page_text'] for k, v in pages.items()},
                            lambda p: all(w['end'] - w['start'] <= 4 for w in p[nt.KEY]))
    parts[0][nt.KEY][0]['sha256'] = 'wrong'
    with pytest.raises(nt.NativeTextPortionError):
        _coalesce(parts, pages)


@pytest.mark.parametrize('value', ['0', '-1', '257', 'bad'])
def test_invalid_limit_is_not_silently_disabled(monkeypatch, value):
    monkeypatch.setenv(plan.MAX_NEW_CALLS_ENV, value)
    with pytest.raises(ValueError):
        plan.max_new_calls()
