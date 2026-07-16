"""Тесты кэша кропов (без сети — fetch инжектируется)."""
from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.common.crop_cache import (
    MANIFEST_NAME,
    crop_filename,
    crops_complete,
    download_crops,
)

BLOCKS = {
    "document_id": "doc_x",
    "generated_at": "2026-07-15T05:51:33Z",
    "blocks": [
        {"block_id": "blk_a", "crop_url": "https://x/api/crops/tokA"},
        {"block_id": "blk_b", "crop_url": "https://x/api/crops/tokB"},
        {"block_id": "blk_stamp", "crop_url": None},  # штамп — качать нечего
    ],
}


def _fetch_ok(url, timeout):
    return 200, b"%PDF-1.7 " + url.encode()


def test_downloads_all_and_writes_manifest(tmp_path):
    man = download_crops(BLOCKS, tmp_path, fetch=_fetch_ok)
    assert man["counts"] == {"ok": 2, "skipped": 1}
    assert (tmp_path / "crops" / "blk_a.pdf").read_bytes().startswith(b"%PDF")
    disk = json.loads((tmp_path / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert disk["counts"] == man["counts"]
    assert disk["total_bytes"] > 0


def test_idempotent_second_run_uses_cache(tmp_path):
    download_crops(BLOCKS, tmp_path, fetch=_fetch_ok)
    calls = []

    def _fetch_count(url, timeout):
        calls.append(url)
        return 200, b"%PDF"

    man = download_crops(BLOCKS, tmp_path, fetch=_fetch_count)
    assert calls == []  # токены immutable — повторно не качаем
    assert man["counts"] == {"cached": 2, "skipped": 1}


def test_http_403_recorded_not_raised(tmp_path):
    man = download_crops(BLOCKS, tmp_path, fetch=lambda u, t: (403, b""))
    assert man["counts"] == {"error": 2, "skipped": 1}
    errs = [e for e in man["entries"] if e["status"] == "error"]
    assert all(e["reason"] == "http_403" for e in errs)


def test_fetch_exception_fail_soft(tmp_path):
    def _boom(url, timeout):
        raise OSError("network down")
    man = download_crops(BLOCKS, tmp_path, fetch=_boom)
    assert man["counts"]["error"] == 2
    assert (tmp_path / MANIFEST_NAME).is_file()


def test_crops_complete(tmp_path):
    assert crops_complete(tmp_path, BLOCKS) is False
    download_crops(BLOCKS, tmp_path, fetch=_fetch_ok)
    assert crops_complete(tmp_path, BLOCKS) is True
    # штамп без crop_url полноте не мешает
    (tmp_path / "crops" / crop_filename("blk_a")).unlink()
    assert crops_complete(tmp_path, BLOCKS) is False


def test_empty_blocks_data(tmp_path):
    man = download_crops({}, tmp_path, fetch=_fetch_ok)
    assert man["counts"] == {}
    assert man["entries"] == []


def test_ensure_crops_for_version_disabled_by_env(tmp_path, monkeypatch):
    from backend.app.services.common.crop_cache import ensure_crops_for_version
    monkeypatch.setenv("AUDIT_CROP_CACHE_ON_UPLOAD", "0")
    assert ensure_crops_for_version(tmp_path, background=False) is None


def test_ensure_crops_for_version_no_blocks_json(tmp_path, monkeypatch):
    from backend.app.services.common.crop_cache import ensure_crops_for_version
    monkeypatch.delenv("AUDIT_CROP_CACHE_ON_UPLOAD", raising=False)
    (tmp_path / "01_input").mkdir(parents=True)
    assert ensure_crops_for_version(tmp_path, background=False) is None
