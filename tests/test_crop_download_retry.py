"""reserc.md #11 — ретраи с backoff на скачивание crop_url до PDF-fallback.

Раньше download_and_convert делал единственный urlopen и на ЛЮБОЙ ошибке сразу
уходил в PDF-fallback. Теперь транзиентные сбои (5xx / сеть / timeout / 408 /
429) ретраятся с экспоненциальным backoff, а фатальные 4xx (404) — сразу raise
(ретрай не поможет → fallback наверху).

Без сети: urlopen и time.sleep замоканы.
"""
from __future__ import annotations

import json
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


# ─── #10: failed_block_ids + failed_details в index.json ──────────────────────


def test_classify_crop_failure_reasons():
    assert blocks._classify_crop_failure("нет crop_url") == "no_crop_url"
    assert blocks._classify_crop_failure(ValueError("boom")) == "http_error"


def test_crop_index_records_failed_block_ids(monkeypatch, tmp_path):
    """#10: блок без crop_url и без PDF-fallback не теряется молча — он попадает
    в index.json как failed_block_ids/failed_details с причиной no_crop_url."""
    # один блок без crop_url; page_pdf_map пуст → fallback недоступен
    block = {
        "block_id": "b-lost", "crop_url": "", "page_num": 3,
        "coords_px": [0, 0, 10, 10], "ocr_label": "", "ocr_text": "",
    }
    monkeypatch.setattr(
        blocks, "_iter_image_blocks_from_ocr",
        lambda project_dir: ([block], {}, {}, [tmp_path / "fake_result.json"]),
    )
    out_dir = tmp_path / "blocks_test"
    res = blocks.crop_blocks_to_dir(
        str(tmp_path), out_dir,
        {"target_dpi": 100, "min_long_side_px": 800, "name": "test"},
        force=True,
    )
    assert res["failed_block_ids"] == ["b-lost"]
    index = json.loads((out_dir / "index.json").read_text(encoding="utf-8"))
    assert index["failed_block_ids"] == ["b-lost"]
    assert index["failed_details"][0]["reason"] == "no_crop_url"
    assert index["failed_details"][0]["block_id"] == "b-lost"
    assert index["total_blocks"] == 0
    assert index["total_expected"] == 1
    assert index["errors"] == 1


def test_full_crop_without_image_blocks_writes_empty_stage02_index(monkeypatch, tmp_path):
    """Успешный пустой crop обязан материализовать downstream-контракт index.json."""
    result_json = tmp_path / "empty_result.json"
    result_json.write_text(
        json.dumps({
            "pages": [{
                "page_number": 1,
                "width": 1000,
                "height": 1000,
                "blocks": [{"id": "txt-1", "block_type": "text"}],
            }],
        }),
        encoding="utf-8",
    )
    output_root = tmp_path / "run"
    blocks_dir = output_root / blocks.STAGE02_BLOCKS_DIRNAME
    blocks_dir.mkdir(parents=True)
    stale_png = blocks_dir / "block_stale.png"
    stale_png.write_bytes(b"stale")

    monkeypatch.setattr(blocks, "detect_all_result_jsons", lambda _project: [result_json])
    monkeypatch.setattr(blocks, "_load_project_info", lambda _project: {})
    monkeypatch.setattr(blocks, "_source_files", lambda _project, _info: object())
    monkeypatch.setattr(blocks, "_select_source_pdf", lambda *_args: None)
    monkeypatch.setattr(blocks, "gemma_output_root", lambda _project: output_root)

    result = blocks.crop_blocks(
        str(tmp_path),
        dpi=100,
        skip_small=False,
        output_dir_name=blocks.STAGE02_BLOCKS_DIRNAME,
    )

    assert result["total_blocks"] == 0
    assert not stale_png.exists()
    index_path = blocks_dir / "index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    assert index["blocks"] == []
    assert index["total_blocks"] == 0
    assert index["total_expected"] == 0
    assert index["profile"] == blocks.stage02_crop_policy()["profile"]
    assert index["dpi"] == 100
    assert index["skip_small"] is False
    assert blocks.crop_index_matches_policy(index_path, blocks.stage02_crop_policy())
