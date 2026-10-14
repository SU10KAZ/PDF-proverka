from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from backend.app.services.project_change_v3 import parallel_calls as p


def test_run_limits_are_isolated_and_restored(monkeypatch):
    monkeypatch.setenv(p.MAPPER_CONCURRENCY_ENV, '3')
    gate = threading.Barrier(2)
    def run(value):
        with p.use_concurrency(value):
            gate.wait(timeout=5)
            return [p.concurrency(name) for name in (p.MAPPER_CONCURRENCY_ENV, p.MINER_CONCURRENCY_ENV,
                     p.UNMATCHED_CONCURRENCY_ENV, p.VERIFICATION_CONCURRENCY_ENV)]
    with ThreadPoolExecutor(2) as pool:
        a, b = pool.submit(run, 5), pool.submit(run, 10)
        assert a.result() == [5] * 4
        assert b.result() == [10] * 4
    assert p.concurrency(p.MAPPER_CONCURRENCY_ENV) == 3
    with pytest.raises(RuntimeError), p.use_concurrency(10):
        raise RuntimeError()
    assert p.concurrency(p.MAPPER_CONCURRENCY_ENV) == 3


@pytest.mark.parametrize('value', [0, 2, 6, 11, True, '5'])
def test_invalid_run_limit_rejected(value):
    with pytest.raises(ValueError):
        with p.use_concurrency(value):
            pass


def test_api_defaults_and_allowed_values():
    from backend.app.api.routers.stage_comparison import ProductionRunRequest, ProductionBatchRequest
    from pydantic import ValidationError
    for cls, payload in [(ProductionRunRequest, {'input_mode': 'DOCUMENT'}),
                         (ProductionBatchRequest, {'pairs': [{'left_pdf': 'a', 'right_pdf': 'b'}]})]:
        assert cls(**payload).parallelism == 5
        assert cls(**payload, parallelism=10).parallelism == 10
        with pytest.raises(ValidationError):
            cls(**payload, parallelism=7)


def test_default_is_five(monkeypatch):
    monkeypatch.delenv(p.MAPPER_CONCURRENCY_ENV, raising=False)
    assert p.concurrency(p.MAPPER_CONCURRENCY_ENV) == 5
