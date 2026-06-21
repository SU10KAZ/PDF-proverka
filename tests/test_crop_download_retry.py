"""reserc.md #11 — ретраи с backoff на скачивание crop_url до PDF-fallback.

Раньше download_and_convert делал единственный urlopen и на ЛЮБОЙ ошибке сразу
уходил в PDF-fallback. Теперь транзиентные сбои (5xx / сеть / timeout / 408 /
429) ретраятся с экспоненциальным backoff, а фатальные 4xx (404) — сразу raise
(ретрай не поможет → fallback наверху).

Без сети: urlopen и time.sleep замоканы.
"""
from __future__ import annotations

import urllib.error
import urllib.request

import pytest

from backend.app.pipeline.stages.crop_blocks import blocks


class _FakeResp:
    def __init__(self, data: bytes):
        self._data = data

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self) -> bytes:
        return self._data


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(blocks.time, "sleep", lambda s: sleeps.append(s))
    return sleeps


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://x/crop.pdf", code, "err", {}, None)


def test_transient_error_retried_then_succeeds(monkeypatch, _no_sleep):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=0):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise urllib.error.URLError("temporary network blip")
        return _FakeResp(b"%PDF-FAKE")

    monkeypatch.setattr(blocks.urllib.request, "urlopen", fake_urlopen)
    req = urllib.request.Request("http://x/crop.pdf")
    assert blocks._download_with_retry(req, timeout=5) == b"%PDF-FAKE"
    assert calls["n"] == 3                 # 2 сбоя отретраены + успех
    assert len(_no_sleep) == 2             # backoff перед каждым ретраем


def test_404_is_fatal_no_retry(monkeypatch, _no_sleep):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=0):
        calls["n"] += 1
        raise _http_error(404)

    monkeypatch.setattr(blocks.urllib.request, "urlopen", fake_urlopen)
    req = urllib.request.Request("http://x/crop.pdf")
    with pytest.raises(urllib.error.HTTPError) as ei:
        blocks._download_with_retry(req, timeout=5)
    assert ei.value.code == 404
    assert calls["n"] == 1                 # фатально — без ретраев
    assert len(_no_sleep) == 0


@pytest.mark.parametrize("code", [408, 429, 500, 503])
def test_retryable_codes_are_retried(monkeypatch, _no_sleep, code):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=0):
        calls["n"] += 1
        raise _http_error(code)

    monkeypatch.setattr(blocks.urllib.request, "urlopen", fake_urlopen)
    req = urllib.request.Request("http://x/crop.pdf")
    with pytest.raises(urllib.error.HTTPError):
        blocks._download_with_retry(req, timeout=5)
    # 1 исходная попытка + (retries-1) ретраев
    assert calls["n"] == blocks._CROP_DOWNLOAD_RETRIES
    assert len(_no_sleep) == blocks._CROP_DOWNLOAD_RETRIES - 1


def test_persistent_network_error_raises_after_retries(monkeypatch, _no_sleep):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=0):
        calls["n"] += 1
        raise TimeoutError("read timed out")

    monkeypatch.setattr(blocks.urllib.request, "urlopen", fake_urlopen)
    req = urllib.request.Request("http://x/crop.pdf")
    with pytest.raises(TimeoutError):
        blocks._download_with_retry(req, timeout=5)
    assert calls["n"] == blocks._CROP_DOWNLOAD_RETRIES
    assert len(_no_sleep) == blocks._CROP_DOWNLOAD_RETRIES - 1
