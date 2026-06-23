"""reserc.md #66 — один preflight LLM-провайдера на весь auto-match job.

Раньше доступность ClaudeCodeProvider проверялась внутри suggest на КАЖДУЮ пару.
_preflight_llm делает это один раз: при недоступности/выключенном флаге →
use_llm=False на весь прогон (с диагностикой), fail-soft.
"""
from __future__ import annotations

from backend.app.services.stage_comparison import auto_match_jobs as amj
from backend.app.services.stage_comparison import stamp_llm_match as slm
from backend.app.services.stage_comparison import text_llm_provider as tlp


def test_not_requested():
    eff, diag = amj._preflight_llm(False)
    assert eff is False and diag["status"] == "not_requested"


def test_disabled_by_flag(monkeypatch):
    monkeypatch.setattr(slm, "stamp_llm_enabled", lambda: False)
    eff, diag = amj._preflight_llm(True)
    assert eff is False and diag["status"] == "disabled_by_flag"


def test_provider_unavailable(monkeypatch):
    monkeypatch.setattr(slm, "stamp_llm_enabled", lambda: True)

    class _FakeProvider:
        def check_availability(self):
            return False, "claude CLI not found"

    monkeypatch.setattr(tlp, "ClaudeCodeProvider", _FakeProvider)
    eff, diag = amj._preflight_llm(True)
    assert eff is False
    assert diag["status"] == "provider_unavailable"
    assert "claude CLI" in diag["reason"]


def test_provider_ok(monkeypatch):
    monkeypatch.setattr(slm, "stamp_llm_enabled", lambda: True)

    class _FakeProvider:
        def check_availability(self):
            return True, ""

    monkeypatch.setattr(tlp, "ClaudeCodeProvider", _FakeProvider)
    eff, diag = amj._preflight_llm(True)
    assert eff is True and diag["status"] == "ok"


def test_preflight_fail_soft(monkeypatch):
    monkeypatch.setattr(slm, "stamp_llm_enabled", lambda: True)

    class _BoomProvider:
        def check_availability(self):
            raise RuntimeError("boom")

    monkeypatch.setattr(tlp, "ClaudeCodeProvider", _BoomProvider)
    eff, diag = amj._preflight_llm(True)
    assert eff is False and diag["status"] == "preflight_exception"
