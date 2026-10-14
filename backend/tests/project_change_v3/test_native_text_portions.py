"""Exhaustive source-text windows, synthetic data only; no inference."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import contracts, engine, mapper_portions, native_text_portions as nt, source_prep, transport
from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError, build_codex_payload, set_test_provider
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env, _stored  # noqa: F401
from backend.tests.project_change_v3.test_mapper_portions import _region
from backend.tests.project_change_v3.test_partial_miner_resume import _handlers
from backend.tests.project_change_v3.test_resume_and_dedupe_repair import _run


@pytest.mark.parametrize('old,new', [('абвгдежз', '12345678'), ('', '0123456789'), ('ab\r\ncd\n🙂ef', 'xyz')])
def test_windows_cover_each_text_pair_without_losing_characters(old, new):
    region = _region('R', [1], [7])
    raw = {('OLD', 1): old, ('NEW', 7): new}
    before = deepcopy(region)

    def fits(part):
        return sum(w['end'] - w['start'] for w in part[nt.KEY]) <= 5

    parts = nt.split_region(region, raw, fits)
    covered = set()
    for part in parts:
        positions = []
        for w in part[nt.KEY]:
            key = (w['side'], w['physical_page'])
            page = {'side': key[0], 'physical_page': key[1], 'native_page_text': raw[key],
                    'blocks': [{'block_id': 'kept', 'structured_md': 'whole table'}]}
            result = nt.page_text(page, part)
            assert result['native_page_text'] == raw[key][w['start']:w['end']]
            assert result['blocks'] == page['blocks']
            assert page['native_page_text'] == raw[key]
            positions.append(list(range(w['start'], w['end'])) or [-1])
        for a in positions[0]:
            for b in positions[1]:
                assert (a, b) not in covered
                covered.add((a, b))
    assert covered == {(a, b) for a in (list(range(len(old))) or [-1])
                       for b in (list(range(len(new))) or [-1])}
    assert region == before


def test_inherited_window_can_be_subdivided_without_revisiting_other_text():
    texts = {('OLD', 1): 'abcdefgh', ('NEW', 2): '12345678'}
    parts = nt.split_region(_region('R', [1], [2]), texts,
                            lambda r: all(w['end'] - w['start'] <= 4 for w in r[nt.KEY]))
    original = parts[-1]
    smaller = nt.split_region(original, texts,
                              lambda r: all(w['end'] - w['start'] <= 2 for w in r[nt.KEY]))
    bounds = [(w['start'], w['end']) for w in original[nt.KEY]]
    nt.validate_coverage(bounds, [[(w['start'], w['end']) for w in p[nt.KEY]] for p in smaller])
    assert len(smaller) == 4


@pytest.mark.parametrize('windows', [[], [[(0, 2), (0, 3)]], [[(0, 3), (0, 3)]] * 2,
                                      [[(0, 0), (0, 3)], [(1, 3), (0, 3)]]])
def test_coverage_checker_refuses_gaps_and_duplicates(windows):
    with pytest.raises(nt.NativeTextPortionError):
        nt.validate_coverage([(0, 3), (0, 3)], windows)


def test_window_rejects_changed_source_text():
    region = nt.split_region(_region('R', [1], [2]), {('OLD', 1): 'abc', ('NEW', 2): 'def'}, lambda _: True)[0]
    with pytest.raises(nt.NativeTextPortionError, match='исходной странице'):
        nt.page_text({'side': 'OLD', 'physical_page': 1, 'native_page_text': 'xyz'}, region)


def test_oversized_fixed_evidence_and_unbounded_partition_count_are_refused(monkeypatch):
    region = _region('R', [1], [2])
    raw = {('OLD', 1): 'a' * 100, ('NEW', 2): 'b' * 100}
    with pytest.raises(nt.NativeTextPortionError, match='блоки и изображения'):
        nt.split_region(region, raw, lambda _: False)
    monkeypatch.setattr(nt, 'MAX_PARTS', 2)
    with pytest.raises(nt.NativeTextPortionError, match='более 2'):
        nt.split_region(region, raw, lambda r: all(w['end'] - w['start'] <= 20 for w in r[nt.KEY]))


def test_page_planner_keeps_fitting_groups_and_only_windows_the_oversized_pair():
    raw = {(side, p): 'x' * (100 if side == 'NEW' and p == 3 else 10)
           for side, pages in [('OLD', [1]), ('NEW', [1, 2, 3])] for p in pages}

    def fits(region):
        if nt.KEY in region:
            return sum(w['end'] - w['start'] for w in region[nt.KEY]) <= 40
        return sum(len(raw[(side, p)]) for side in ('OLD', 'NEW')
                   for p in region[side.lower() + '_pages']) <= 40

    parts = mapper_portions.split_region_for_envelope(_region('R', [1], [1, 2, 3]), fits, native_texts=raw)
    assert parts[0]['new_pages'] == [1, 2] and nt.KEY not in parts[0]
    assert all(p['new_pages'] == [3] and nt.KEY in p for p in parts[1:])


def test_partial_resume_ignores_legacy_raw_text_and_keeps_md_and_images(env, monkeypatch):
    monkeypatch.setattr(contracts, 'DEFAULT_PROFILE_KEY', 'astra')
    monkeypatch.setenv(engine.UNMATCHED_REVIEW_ENV, '0')
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    raw_text = '\n'.join(f'Строка {i:05d}: расход 123; марка М-456; полные данные страницы.' for i in range(9000))
    real_load = source_prep.load_page_record

    def load(work, side, page):
        value = real_load(work, side, page)
        if side == 'NEW' and page == 2:
            value['native_page_text'] = raw_text
        return value

    monkeypatch.setattr(source_prep, 'load_page_record', load)
    monkeypatch.setattr(engine, 'load_page_record', load)
    base = _handlers()
    normal_mining = base['MINING']

    def fail_last(**kwargs):
        if kwargs['data']['frozen_region']['region_id'] == 'P010':
            raise ProviderError('CONTEXT_COMPACTED', 'synthetic old-runtime failure')
        return normal_mining(**kwargs)

    set_test_provider(FakeProvider(handlers={**base, 'MINING': fail_last}))
    # Simulate a donor from before Miner budget admission existed.
    with monkeypatch.context() as old_runtime:
        old_runtime.setattr(engine, '_miner_fits', lambda *_: True)
        donor = _run(env['session_id'])
    assert donor['reason_code'] == 'provider_context_compacted', donor
    snapshots = []

    class Checked(FakeProvider):
        def complete(self, **kwargs):
            if kwargs['stage'] == 'MINING':
                data = kwargs['data']
                payload, images, _ = build_codex_payload(kwargs['prompt'], data, kwargs['images'])
                assert transport.plan_codex_miner(payload, images, kwargs['schema'])
                region = data['frozen_region']
                snapshots.append(deepcopy(region))
                for page in data['pages']:
                    assert 'native_page_text' not in page
                    original = load(engine_work[0], page['side'], page['physical_page'])
                    assert page['blocks'] == original['blocks']
                    assert page['full_page_ref'] == original['full_page_ref']
                return {'pair': gf.PAIR_ID, 'region_id': region['region_id'], 'projectchanges': [],
                        'unresolved_hints': [], 'coverage_notes': []}
            return super().complete(**kwargs)

    engine_work = []
    real_bundle = engine.optimized_region_bundle

    def bundle(**kwargs):
        engine_work[:] = [kwargs['work_dir']]
        return real_bundle(**kwargs)

    monkeypatch.setattr(engine, 'optimized_region_bundle', bundle)
    fake = Checked(handlers=base)
    set_test_provider(fake)
    state = _run(env['session_id'], resume_from_run_id=donor['run_id'])
    assert state['status'] in {'COMPLETED', 'REVIEW'}, state.get('message')
    assert all(c['stage'] != 'MAPPING' for c in fake.calls)
    assert [p['region_id'] for p in snapshots] == ['P010']
    assert all(nt.KEY not in p for p in snapshots)
    assert len(_stored(env['session_id'], 'project_change_v3_result')['projectchanges']) == 9
    checkpoint = json.loads(Path(state['provenance']['miner_checkpoint']['path']).read_text())
    assert checkpoint['regions_total'] == len(checkpoint['regions']) == 9 + len(snapshots)


def test_legacy_pdf_padding_cannot_inflate_md_only_plan(env, monkeypatch):
    from backend.app.services.project_change_v3 import source_compaction
    from backend.tests.project_change_v3.test_miner_context_budget import prepare_wide_region

    handlers = prepare_wide_region(monkeypatch)
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MAX_NEW_MINER_CALLS', '1')
    real_load = source_prep.load_page_record

    def padded(work, side, number):
        page = real_load(work, side, number)
        page['native_page_text'] = ''.join(
            f'{side} {number}: строка {i}' + ' ' * (1000 + i) + f'Значение {i},90\r\n'
            for i in range(300))
        return page

    monkeypatch.setattr(source_prep, 'load_page_record', padded)
    monkeypatch.setattr(engine, 'load_page_record', padded)
    fake = FakeProvider(handlers=handlers)
    set_test_provider(fake)
    # Reproduce the old plan with lossless layout-run encoding unavailable.
    with monkeypatch.context() as old_runtime:
        old_runtime.setattr(source_compaction, 'compact', lambda data: (data, {}))
        stopped = _run(env['session_id'])
    assert stopped['status'] in {'COMPLETED', 'REVIEW'}, stopped
    assert [c['stage'] for c in fake.calls] == ['MAPPING', 'MINING']
    assert stopped['provenance']['miner_plan']['new_calls'] == 1
