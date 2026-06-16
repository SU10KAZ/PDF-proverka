"""Тесты bounded rate-limit retry для text_analysis (Часть A).

Баг: раньше runner делал ОДИН wait+retry; при сохраняющемся rate-limit (или
когда reset time не распарсился) проект жёстко падал «Текстовый анализ: код 1».
Теперь — до N попыток с fallback backoff и понятным статусом rate_limit_exhausted.

Run: python -m pytest tests/test_text_analysis_rate_limit_retry.py -v
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.pipeline.stages.text_analysis.runner as runner  # noqa: E402
import backend.app.services.llm.claude_runner as claude_runner  # noqa: E402
from backend.app.pipeline.stages.text_analysis import rate_limit_retry as rlr  # noqa: E402


# ─── Часть A.1: pure backoff / config ────────────────────────────────────────

def test_compute_fallback_backoff_exponential_and_capped():
    cfg = rlr.RateLimitRetryConfig(
        fallback_backoff_sec=100, backoff_multiplier=2.0, max_backoff_sec=350
    )
    assert rlr.compute_fallback_backoff(1, cfg) == 100
    assert rlr.compute_fallback_backoff(2, cfg) == 200
    assert rlr.compute_fallback_backoff(3, cfg) == 350   # 400 → capped to 350
    assert rlr.compute_fallback_backoff(10, cfg) == 350  # bounded
    assert rlr.compute_fallback_backoff(0, cfg) == 100   # clamp attempt→1


def test_load_rate_limit_config_env(monkeypatch):
    monkeypatch.setenv("TEXT_ANALYSIS_RATE_LIMIT_MAX_RETRIES", "5")
    monkeypatch.setenv("TEXT_ANALYSIS_RATE_LIMIT_FALLBACK_BACKOFF_SEC", "120")
    monkeypatch.setenv("TEXT_ANALYSIS_PAUSE_ON_RATE_LIMIT", "true")
    cfg = rlr.load_rate_limit_config()
    assert cfg.max_retries == 5
    assert cfg.fallback_backoff_sec == 120
    assert cfg.pause_on_exhausted is True


def test_load_rate_limit_config_defaults(monkeypatch):
    for k in ("TEXT_ANALYSIS_RATE_LIMIT_MAX_RETRIES",
              "TEXT_ANALYSIS_RATE_LIMIT_FALLBACK_BACKOFF_SEC",
              "TEXT_ANALYSIS_PAUSE_ON_RATE_LIMIT"):
        monkeypatch.delenv(k, raising=False)
    cfg = rlr.load_rate_limit_config()
    assert cfg.max_retries == 3
    assert cfg.pause_on_exhausted is False


# ─── Часть A.2: runner loop (fake ctx) ───────────────────────────────────────

class FakeCtx:
    def __init__(self, output_dir: Path, *, wait_returns, cancelled=False):
        self.project_id = "pX"
        self.output_dir = output_dir
        self.project_info = {"project_id": "pX"}
        self.logs: list[tuple[str, str]] = []
        self.pipeline_updates: list[tuple] = []
        self._wait_returns = list(wait_returns)  # очередь True/False для wait_for_rate_limit
        self._cancelled = cancelled

    async def log(self, msg, level="info"):
        self.logs.append((str(msg), level))

    async def check_before_launch(self):
        return True

    def record_cli_usage(self, cli_result, label):
        pass

    def update_pipeline_log(self, stage, status, **kwargs):
        self.pipeline_updates.append((stage, status, kwargs))

    async def wait_for_rate_limit(self, reason, output):
        return self._wait_returns.pop(0) if self._wait_returns else True

    def is_cancelled(self):
        return self._cancelled


def _patch_runner(monkeypatch, run_results, *, parsed_reset=None, sleep_calls=None):
    """run_results — список (exit_code, output); возвращаются по очереди.
    is_rate_limited управляется маркером 'RL' в output."""
    seq = list(run_results)

    async def fake_run(project_info, pid, on_output=None):
        ec, out = seq.pop(0)
        return ec, out, object()

    monkeypatch.setattr(claude_runner, "run_text_analysis", fake_run)
    monkeypatch.setattr(claude_runner, "run_triage", fake_run)
    monkeypatch.setattr(claude_runner, "parse_rate_limit_reset", lambda t: parsed_reset)
    monkeypatch.setattr(runner, "is_rate_limited", lambda ec, out, err: "RL" in (out or ""))
    monkeypatch.setattr(runner, "is_cancelled", lambda ec: ec == -2)

    async def fake_sleep(sec):
        if sleep_calls is not None:
            sleep_calls.append(sec)

    monkeypatch.setattr(runner, "_SLEEP", fake_sleep)


def _mk_output(tmp_path) -> Path:
    out = tmp_path / "_output"
    out.mkdir()
    return out


def test_parsed_reset_wait_then_success(monkeypatch, tmp_path):
    out = _mk_output(tmp_path)
    (out / "01_text_analysis.json").write_text("{}")
    # attempt0: rate limit; wait True; retry → success
    _patch_runner(monkeypatch, [(1, "RL hit your limit"), (0, "ok")],
                  parsed_reset=42)
    ctx = FakeCtx(out, wait_returns=[True])
    res = asyncio.run(runner.run_text_analysis(ctx))
    assert res.success is True
    assert res.cancelled is False


def test_unparsed_reset_fallback_then_success(monkeypatch, tmp_path):
    out = _mk_output(tmp_path)
    (out / "01_text_analysis.json").write_text("{}")
    sleeps = []
    # attempt0: RL; wait False (reset не распознан); fallback sleep; retry → success
    _patch_runner(monkeypatch, [(1, "RL overloaded"), (0, "ok")],
                  parsed_reset=None, sleep_calls=sleeps)
    ctx = FakeCtx(out, wait_returns=[False])  # wait не дождался
    res = asyncio.run(runner.run_text_analysis(ctx))
    assert res.success is True
    assert sleeps and sleeps[0] >= 1   # был fallback backoff


def test_persistent_rate_limit_exhausted(monkeypatch, tmp_path):
    out = _mk_output(tmp_path)
    monkeypatch.setenv("TEXT_ANALYSIS_RATE_LIMIT_MAX_RETRIES", "3")
    monkeypatch.setenv("TEXT_ANALYSIS_PAUSE_ON_RATE_LIMIT", "true")
    sleeps = []
    # всегда RL → после 3 retry → exhausted
    _patch_runner(monkeypatch, [(1, "RL")] * 6, parsed_reset=10, sleep_calls=sleeps)
    ctx = FakeCtx(out, wait_returns=[True, True, True, True])
    res = asyncio.run(runner.run_text_analysis(ctx))
    assert res.success is False
    assert res.cancelled is False
    assert res.data and res.data.get("reason") == rlr.REASON_RATE_LIMIT_EXHAUSTED
    assert res.data.get("pause_on_rate_limit") is True
    assert "rate_limit_exhausted" in (res.error or "")


def test_non_rate_limit_code1_hard_fail(monkeypatch, tmp_path):
    out = _mk_output(tmp_path)
    # exit 1 БЕЗ маркера RL → обычный hard fail «код 1»
    _patch_runner(monkeypatch, [(1, "some other error")], parsed_reset=None)
    ctx = FakeCtx(out, wait_returns=[])
    res = asyncio.run(runner.run_text_analysis(ctx))
    assert res.success is False
    assert "код 1" in (res.error or "")
    assert not (res.data and res.data.get("reason"))  # не rate_limit_exhausted


def test_cancel_during_wait(monkeypatch, tmp_path):
    out = _mk_output(tmp_path)
    _patch_runner(monkeypatch, [(1, "RL")], parsed_reset=None)
    ctx = FakeCtx(out, wait_returns=[False], cancelled=True)  # wait False + cancelled
    res = asyncio.run(runner.run_text_analysis(ctx))
    assert res.cancelled is True


def test_success_first_try_no_retry(monkeypatch, tmp_path):
    out = _mk_output(tmp_path)
    (out / "01_text_analysis.json").write_text("{}")
    _patch_runner(monkeypatch, [(0, "ok")])
    ctx = FakeCtx(out, wait_returns=[])
    res = asyncio.run(runner.run_text_analysis(ctx))
    assert res.success is True
