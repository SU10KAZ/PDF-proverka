"""Capacity failures reduce admission, retry exact requests and pause after stage drain."""
from collections import Counter
from copy import deepcopy
import threading

import pytest

from backend.app.services.project_change_v3 import parallel_calls as pc
from backend.app.services.project_change_v3.provider import ProviderError


def overload():
    return ProviderError('TRANSIENT', 'Selected model is at capacity. Please try a different model.')


def run(provider, count, workers, saved):
    def item(index, value):
        outcome = yield {'stage': 'MINING', 'call_id': str(index), 'data': {'index': value}}
        saved[index] = outcome.response if outcome.error is None else str(outcome.error)
        if outcome.error:
            raise outcome.error
    pc.run_items(enumerate(range(count)), start_item=item, provider=provider, workers=workers,
                 control=pc.CallControl(), cancelled=lambda: False,
                 on_completed=lambda: None, thread_name='test-capacity')


@pytest.mark.parametrize('workers,expected_attempts', [(5, 4), (10, 5)])
def test_exhausted_item_does_not_prevent_remaining_stage_items(monkeypatch, workers, expected_attempts):
    calls, waits, saved = [], [], {}
    monkeypatch.setattr(pc, '_wait', lambda delay, cancelled: waits.append(delay) or True)

    class Provider:
        def complete(self, **request):
            calls.append(deepcopy(request))
            if request['call_id'] == '0':
                raise overload()
            return {'done': request['call_id']}

    with pc.use_capacity_policy() as policy, pytest.raises(pc.CapacityPause):
        run(Provider(), 27, workers, saved)
    assert len(saved) == 27
    assert all(saved[i] == {'done': str(i)} for i in range(1, 27))
    attempts = [c for c in calls if c['call_id'] == '0']
    assert len(attempts) == expected_attempts
    assert all(c == attempts[0] for c in attempts), 'retry must preserve the entire request'
    assert waits == [60, 120, 240]
    assert policy.events[-1]['exhausted'] is True
    assert policy.limit == 5


def test_reduced_admission_is_kept_in_later_stages(monkeypatch):
    barrier = threading.Barrier(10)
    lock = threading.Lock()
    calls = Counter()
    active = 0
    monkeypatch.setattr(pc, '_wait', lambda *_: True)

    class Provider:
        def complete(self, **request):
            nonlocal active
            index = int(request['call_id'])
            with lock:
                calls[index] += 1
                attempt = calls[index]
                active += 1
                if index >= 10 and pc.capacity_policy().limit == 5:
                    assert active <= 5
            try:
                if index < 10 and attempt == 1:
                    barrier.wait(timeout=5)
                    if index == 0:
                        raise overload()
                return index
            finally:
                with lock:
                    active -= 1

    with pc.use_capacity_policy() as policy:
        saved = {}
        run(Provider(), 30, 10, saved)
        assert policy.limit == 5
        assert calls[0] == 2
        assert len(saved) == 30
        # A later stage inherits the lower limit rather than returning to ten.
        policy.events.clear()
        class Later:
            def complete(self, **request):
                raise overload()
        with pytest.raises(pc.CapacityPause):
            run(Later(), 1, 10, {})
        assert policy.events[0]['submitted_parallelism'] == 5
    with pc.use_capacity_policy() as fresh:
        assert fresh.limit == 10


def test_non_capacity_errors_still_stop_admission():
    class Provider:
        def complete(self, **request):
            raise ProviderError('PERMANENT', 'quota exhausted')
    saved = {}
    with pc.use_capacity_policy(), pytest.raises(ProviderError):
        run(Provider(), 20, 1, saved)
    assert len(saved) == 1


def test_persistence_failure_after_exhaustion_is_fatal(monkeypatch):
    monkeypatch.setattr(pc, '_wait', lambda *_: True)
    class Provider:
        def complete(self, **request):
            raise overload()
    def item(*_):
        yield {'stage': 'MINING', 'call_id': '0'}
        raise OSError('checkpoint unavailable')
    with pc.use_capacity_policy(), pytest.raises(OSError, match='checkpoint unavailable'):
        pc.run_items([(0, None)], start_item=item, provider=Provider(), workers=5,
                     control=pc.CallControl(), cancelled=lambda: False,
                     on_completed=lambda: None, thread_name='test-capacity')


from backend.tests.project_change_v3.test_failure_states import env  # noqa: E402,F401


def test_pipeline_pauses_after_all_miner_items_and_resumes_only_missing(env, monkeypatch):
    import json
    from pathlib import Path
    from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider
    from backend.app.services.stage_comparison.production_orchestrator import run_production_comparison
    from backend.tests.project_change_v3 import generic_fixture as gf
    from backend.tests.project_change_v3.test_miner_parallel import handlers

    monkeypatch.setattr(pc, '_wait', lambda *_: True)
    base = handlers(18)
    good = base['MINING']
    def mining(**request):
        if request['data']['frozen_region']['region_id'] == 'P001':
            raise overload()
        return good(**request)
    fake = FakeProvider(handlers={**base, 'MINING': mining})
    set_test_provider(fake)
    state = run_production_comparison(env['session_id'], gf.PAIR_ID, input_mode='DOCUMENT', parallelism=10)
    assert state['status'] == 'PAUSED', state['message']
    assert state['reason_code'] == 'v3_capacity_paused'
    assert state['pipeline_stages']['MINING']['status'] == 'PAUSED'
    assert state['pipeline_stages']['UNMATCHED_REVIEW']['status'] == 'WAITING'
    assert all(c['stage'] in {'MAPPING', 'MINING'} for c in fake.calls)
    checkpoint = json.loads(Path(state['provenance']['miner_checkpoint']['path']).read_text())
    assert len(checkpoint['regions']) == 17
    from backend.app.services.project_change_v3 import run_storage
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, state['run_id'])
    assert json.loads((directory / 'run_manifest.json').read_text())['state'] == 'PAUSED'
    assert (directory / 'project_change_v3_capacity_control.json').is_file()
    from backend.app.services.project_change_v3.resume import resume_available
    assert resume_available(env['session_id'], gf.PAIR_ID, state['run_id'])

    fake = FakeProvider(handlers=base)
    set_test_provider(fake)
    resumed = run_production_comparison(env['session_id'], gf.PAIR_ID, input_mode='DOCUMENT',
                                        resume_from_run_id=state['run_id'], parallelism=5)
    assert resumed['status'] in {'REVIEW', 'COMPLETED'}, resumed['message']
    assert not any(c['stage'] == 'MAPPING' for c in fake.calls)
    assert [c['call_id'] for c in fake.calls if c['stage'] == 'MINING'] == [f'{gf.PAIR_ID}_P001']


@pytest.mark.parametrize('stage', ['UNMATCHED_REVIEW', 'SOURCE_VERIFICATION'])
def test_review_pause_saves_every_batch_and_does_not_publish_result(env, monkeypatch, stage):
    import json
    from backend.app.services.project_change_v3 import run_storage
    from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider
    from backend.app.services.stage_comparison.production_orchestrator import run_production_comparison
    from backend.tests.project_change_v3 import generic_fixture as gf
    from backend.tests.project_change_v3.test_review_parallel import prepare

    monkeypatch.setattr(pc, '_wait', lambda *_: True)
    base = prepare(env, monkeypatch, stage, 96 if stage == 'SOURCE_VERIFICATION' else 12)
    good = base[stage]
    calls = []
    def review(**request):
        calls.append(request['call_id'])
        if ('U-R001' if stage == 'UNMATCHED_REVIEW' else 'VERIFY-B001') in request['call_id']:
            raise overload()
        return good(**request)
    set_test_provider(FakeProvider(handlers={**base, stage: review}))
    state = run_production_comparison(env['session_id'], gf.PAIR_ID, input_mode='DOCUMENT', parallelism=5)
    assert state['status'] == 'PAUSED', state['message']
    assert state['pipeline_current'] == stage
    directory = run_storage.run_dir(env['session_id'], gf.PAIR_ID, state['run_id'])
    name = 'supplemental' if stage == 'UNMATCHED_REVIEW' else 'verification'
    batches = json.loads((directory / f'project_change_v3_{name}_results.json').read_text())['batches']
    assert len(batches) == 12
    assert sum(row['status'] == 'ACCEPTED' for row in batches) == 11
    assert not (directory / 'project_change_v3_result.json').exists()



def test_cancel_during_retry_wait_makes_no_further_provider_call(env, monkeypatch):
    from backend.app.services.project_change_v3 import engine
    from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider
    from backend.app.services.stage_comparison.ai.gateway import CancelToken
    from backend.tests.project_change_v3 import generic_fixture as gf
    token = CancelToken()
    calls = []
    def mining(**request):
        calls.append(request['call_id'])
        raise overload()
    def pause(*_):
        token.cancel()
        return False
    monkeypatch.setattr(pc, '_wait', pause)
    set_test_provider(FakeProvider(handlers={**gf.fake_handlers(), 'MINING': mining}))
    state = engine.run_v3_pipeline(session_id=env['session_id'], pair_id=gf.PAIR_ID,
                                   cancel_token=token, parallelism=5)
    assert state['reason_code'] == 'v3_cancelled', state['message']
    assert len(calls) <= 2


def test_empty_miner_checkpoint_after_capacity_pause_can_resume(env, monkeypatch):
    from backend.app.services.project_change_v3 import engine, resume
    from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider
    from backend.tests.project_change_v3 import generic_fixture as gf
    monkeypatch.setattr(pc, '_wait', lambda *_: True)
    def mining(**_):
        raise overload()
    set_test_provider(FakeProvider(handlers={**gf.fake_handlers(), 'MINING': mining}))
    state = engine.run_v3_pipeline(session_id=env['session_id'], pair_id=gf.PAIR_ID, parallelism=5)
    assert state['status'] == 'PAUSED', state['message']
    assert resume.resume_available(env['session_id'], gf.PAIR_ID, state['run_id'])
    assert resume.load_donor(env['session_id'], gf.PAIR_ID, state['run_id']).checkpoint['regions'] == []
