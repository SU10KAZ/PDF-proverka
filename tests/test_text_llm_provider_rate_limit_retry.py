"""reserc.md #84 — единый rate-limit retry для ClaudeCodeProvider.

invoke возвращал claude_rc=... без распознавания rate-limit. Теперь распознаёт
'usage limit reached'/'overloaded'/429 (через cli_utils.is_rate_limited, как
аудит) и ретраит с bounded backoff (как with_rate_limit_retry).
"""
from __future__ import annotations

import backend.app.services.stage_comparison.text_llm_provider as tlp


class _Proc:
    def __init__(self, rc, out, err):
        self.returncode, self.stdout, self.stderr = rc, out, err


def test_is_rate_limited_delegates():
    assert tlp._is_rate_limited(1, "", "overloaded, please retry") is True
    assert tlp._is_rate_limited(0, "ok", "") is False


def test_max_retries_sane():
    assert tlp._rate_limit_max_retries() >= 0


def test_invoke_retries_on_rate_limit(monkeypatch, tmp_path):
    prov = tlp.ClaudeCodeProvider()
    monkeypatch.setattr(prov, "_find_cli", lambda: "claude")
    monkeypatch.setattr(tlp, "_rate_limit_max_retries", lambda: 2)
    monkeypatch.setattr(tlp.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def _fake_run(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return _Proc(1, "", "overloaded; try again")
        return _Proc(0, '{"result":"ok"}', "")

    monkeypatch.setattr(tlp.subprocess, "run", _fake_run)
    res = prov.invoke(system_prompt="s", user_prompt="u", model="haiku",
                      timeout_sec=10, work_dir=tmp_path)
    assert res.status == "done"
    assert calls["n"] == 2  # был один ретрай


def test_invoke_no_retry_on_non_rate_limit(monkeypatch, tmp_path):
    prov = tlp.ClaudeCodeProvider()
    monkeypatch.setattr(prov, "_find_cli", lambda: "claude")
    monkeypatch.setattr(tlp, "_rate_limit_max_retries", lambda: 3)
    monkeypatch.setattr(tlp.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def _fake_run(*a, **k):
        calls["n"] += 1
        return _Proc(2, "", "some other fatal error")

    monkeypatch.setattr(tlp.subprocess, "run", _fake_run)
    res = prov.invoke(system_prompt="s", user_prompt="u", model="haiku",
                      timeout_sec=10, work_dir=tmp_path)
    assert res.status == "error"
    assert calls["n"] == 1  # без ретрая — не rate-limit


def test_invoke_gives_up_after_max_retries(monkeypatch, tmp_path):
    prov = tlp.ClaudeCodeProvider()
    monkeypatch.setattr(prov, "_find_cli", lambda: "claude")
    monkeypatch.setattr(tlp, "_rate_limit_max_retries", lambda: 2)
    monkeypatch.setattr(tlp.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def _fake_run(*a, **k):
        calls["n"] += 1
        return _Proc(1, "", "overloaded")

    monkeypatch.setattr(tlp.subprocess, "run", _fake_run)
    res = prov.invoke(system_prompt="s", user_prompt="u", model="haiku",
                      timeout_sec=10, work_dir=tmp_path)
    assert res.status == "error"
    assert calls["n"] == 3  # первая попытка + 2 ретрая
