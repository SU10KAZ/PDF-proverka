"""reserc.md #84 — единый rate-limit retry для ClaudeCodeProvider.

invoke возвращал claude_rc=... без распознавания rate-limit. Теперь распознаёт
'usage limit reached'/'overloaded'/429 (через cli_utils.is_rate_limited, как
аудит) и ретраит с bounded backoff (как with_rate_limit_retry).
"""
from __future__ import annotations

import threading
import time as _time
from pathlib import Path

import backend.app.services.stage_comparison.text_llm_provider as tlp


class _Proc:
    def __init__(self, rc, out, err):
        self.returncode, self.stdout, self.stderr = rc, out, err


def _sys_file_from_args(args):
    """Извлечь путь --append-system-prompt-file из argv claude."""
    for i, a in enumerate(args):
        if a == "--append-system-prompt-file":
            return args[i + 1]
    return None


# ── reserc.md #54 review (HIGH): уникальный temp-файл на вызов ───────────────

def test_invoke_uses_unique_tempfile_not_fixed_name(monkeypatch, tmp_path):
    """temp-файл системного промпта — уникальный (mkstemp), а не фиксированное
    имя _text_llm_system_prompt.tmp.md (иначе гонка при chunk_concurrency>1)."""
    prov = tlp.ClaudeCodeProvider()
    monkeypatch.setattr(prov, "_find_cli", lambda: "claude")
    seen = {}

    def _fake_run(args, **k):
        p = _sys_file_from_args(args)
        seen["path"] = p
        seen["exists_at_call"] = bool(p and Path(p).exists())
        return _Proc(0, '{"result":"ok"}', "")

    monkeypatch.setattr(tlp.subprocess, "run", _fake_run)
    res = prov.invoke(system_prompt="s", user_prompt="u", model="haiku",
                      timeout_sec=10, work_dir=tmp_path)
    assert res.status == "done"
    assert seen["exists_at_call"] is True
    assert Path(seen["path"]).name != "_text_llm_system_prompt.tmp.md"
    assert Path(seen["path"]).name.startswith("_text_llm_sys_")
    # после вызова temp-файл убран
    assert not Path(seen["path"]).exists()


def test_concurrent_invokes_no_tempfile_race(monkeypatch, tmp_path):
    """Два одновременных invoke с ОДНИМ work_dir используют РАЗНЫЕ temp-файлы,
    и файл каждого существует во время его subprocess (нет преждевременного
    unlink соседом) — это и есть фикс HIGH-находки review #54."""
    prov = tlp.ClaudeCodeProvider()
    monkeypatch.setattr(prov, "_find_cli", lambda: "claude")
    paths = []
    existed = []
    lock = threading.Lock()

    def _fake_run(args, **k):
        p = _sys_file_from_args(args)
        # окно перекрытия: пока «работаем», файл должен существовать
        _time.sleep(0.05)
        with lock:
            paths.append(p)
            existed.append(bool(p and Path(p).exists()))
        return _Proc(0, '{"result":"ok"}', "")

    monkeypatch.setattr(tlp.subprocess, "run", _fake_run)

    def _call():
        prov.invoke(system_prompt="s", user_prompt="u", model="haiku",
                    timeout_sec=10, work_dir=tmp_path)

    threads = [threading.Thread(target=_call) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(paths) == 4
    assert len(set(paths)) == 4, f"temp-файлы должны быть уникальны: {paths}"
    assert all(existed), "каждый temp-файл обязан существовать во время своего вызова"


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
