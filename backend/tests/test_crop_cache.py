"""Тесты кэша кропов (без сети — fetch инжектируется)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.common.crop_cache import (
    MANIFEST_NAME,
    MODE_DOWNLOAD,
    MODE_LOCAL,
    cache_crops,
    crop_filename,
    crop_source_mode,
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


# ── Локальная вырезка из PDF (режим local_pdf) ──────────────────────────────

fitz = pytest.importorskip("fitz")


def _make_src_pdf(path) -> None:
    doc = fitz.open()
    page = doc.new_page(width=600, height=400)
    page.insert_text((60, 100), "ALPHA", fontsize=20)
    page.insert_text((400, 350), "BRAVO", fontsize=20)
    doc.save(str(path))
    doc.close()


def _local_blocks(good_page: int = 0) -> dict:
    return {
        "document_id": "doc_loc",
        "generated_at": "2026-07-16T09:00:00Z",
        "blocks": [
            {"block_id": "blk_good", "crop_url": "https://x/api/crops/tokG",
             "page_index": good_page, "coords_norm": [0.02, 0.15, 0.4, 0.35]},
            {"block_id": "blk_badpage", "crop_url": "https://x/api/crops/tokB",
             "page_index": 99, "coords_norm": [0.1, 0.1, 0.9, 0.9]},
            {"block_id": "blk_stamp", "crop_url": None,
             "page_index": 0, "coords_norm": [0.8, 0.8, 0.99, 0.99]},
        ],
    }


def _fetch_fail_if_called(url, timeout):
    raise AssertionError(f"сеть не должна использоваться: {url}")


def test_cache_crops_local_mode_cuts_from_pdf(tmp_path):
    pdf = tmp_path / "src.pdf"
    _make_src_pdf(pdf)
    blocks = _local_blocks()
    del blocks["blocks"][1]  # только валидный таргет + штамп
    man = cache_crops(blocks, tmp_path, pdf_path=pdf, mode=MODE_LOCAL,
                      fetch=_fetch_fail_if_called)
    assert man["crop_source_mode"] == MODE_LOCAL
    assert man["counts"] == {"ok": 1, "skipped": 1}
    ok = next(e for e in man["entries"] if e["status"] == "ok")
    assert ok["source"] == MODE_LOCAL and ok["bytes"] > 0
    crop = fitz.open(str(tmp_path / "crops" / "blk_good.pdf"))
    text = crop[0].get_text("text")
    crop.close()
    assert "ALPHA" in text and "BRAVO" not in text


def test_cache_crops_local_fallback_to_download(tmp_path):
    pdf = tmp_path / "src.pdf"
    _make_src_pdf(pdf)
    man = cache_crops(_local_blocks(), tmp_path, pdf_path=pdf, mode=MODE_LOCAL,
                      fetch=lambda u, t: (200, b"%PDF portal " + u.encode()))
    assert man["counts"] == {"ok": 2, "skipped": 1}
    by_id = {e["block_id"]: e for e in man["entries"]}
    assert by_id["blk_good"]["source"] == MODE_LOCAL
    assert by_id["blk_badpage"]["source"] == "portal"
    assert by_id["blk_badpage"]["fallback_from_local"].startswith("bad_page")


def test_cache_crops_local_fallback_error_keeps_both_reasons(tmp_path):
    pdf = tmp_path / "src.pdf"
    _make_src_pdf(pdf)
    man = cache_crops(_local_blocks(), tmp_path, pdf_path=pdf, mode=MODE_LOCAL,
                      fetch=lambda u, t: (403, b""))
    bad = next(e for e in man["entries"] if e["block_id"] == "blk_badpage")
    assert bad["status"] == "error"
    assert "local:" in bad["reason"] and "http_403" in bad["reason"]


def test_cache_crops_local_without_pdf_degrades_to_download(tmp_path):
    man = cache_crops(_local_blocks(), tmp_path, pdf_path=None, mode=MODE_LOCAL,
                      fetch=lambda u, t: (200, b"%PDF portal"))
    assert man["local_unavailable"] == "no_pdf"
    assert man["counts"] == {"ok": 2, "skipped": 1}
    assert all(e.get("source") == "portal"
               for e in man["entries"] if e["status"] == "ok")


def test_cache_crops_local_idempotent_second_run(tmp_path):
    pdf = tmp_path / "src.pdf"
    _make_src_pdf(pdf)
    blocks = _local_blocks()
    del blocks["blocks"][1]
    cache_crops(blocks, tmp_path, pdf_path=pdf, mode=MODE_LOCAL,
                fetch=_fetch_fail_if_called)
    man = cache_crops(blocks, tmp_path, pdf_path=pdf, mode=MODE_LOCAL,
                      fetch=_fetch_fail_if_called)
    assert man["counts"] == {"cached": 1, "skipped": 1}


def test_cache_crops_local_too_large_falls_back(tmp_path, monkeypatch):
    """Локальный кроп больше MAX_FILE_BYTES: файл удалён, блок докачан."""
    import backend.app.services.common.crop_cache as cc
    monkeypatch.setattr(cc, "MAX_FILE_BYTES", 700)  # локальный кроп ~1-2 КБ > 700
    pdf = tmp_path / "src.pdf"
    _make_src_pdf(pdf)
    blocks = _local_blocks()
    del blocks["blocks"][1]
    man = cc.cache_crops(blocks, tmp_path, pdf_path=pdf, mode=MODE_LOCAL,
                         fetch=lambda u, t: (200, b"%PDF portal small"))
    good = next(e for e in man["entries"] if e["block_id"] == "blk_good")
    assert good["status"] == "ok" and good["source"] == "portal"
    assert good["fallback_from_local"] == "too_large"
    assert (tmp_path / "crops" / "blk_good.pdf").read_bytes() == b"%PDF portal small"


def test_cache_crops_local_corrupt_pdf_all_fallback(tmp_path):
    pdf = tmp_path / "src.pdf"
    pdf.write_bytes("это не PDF вовсе".encode("utf-8"))
    blocks = _local_blocks()
    del blocks["blocks"][1]
    man = cache_crops(blocks, tmp_path, pdf_path=pdf, mode=MODE_LOCAL,
                      fetch=lambda u, t: (200, b"%PDF portal"))
    good = next(e for e in man["entries"] if e["block_id"] == "blk_good")
    assert good["status"] == "ok" and good["source"] == "portal"
    assert good["fallback_from_local"].startswith("open:")


def test_cache_crops_local_missing_fitz_all_fallback(tmp_path, monkeypatch):
    """Без PyMuPDF режим local_pdf обязан деградировать в скачивание,
    а не терять кропы (ImportError не должен пролетать наружу)."""
    import sys
    pdf = tmp_path / "src.pdf"
    _make_src_pdf(pdf)
    blocks = _local_blocks()
    del blocks["blocks"][1]
    monkeypatch.setitem(sys.modules, "fitz", None)
    man = cache_crops(blocks, tmp_path, pdf_path=pdf, mode=MODE_LOCAL,
                      fetch=lambda u, t: (200, b"%PDF portal"))
    good = next(e for e in man["entries"] if e["block_id"] == "blk_good")
    assert good["status"] == "ok" and good["source"] == "portal"
    assert good["fallback_from_local"].startswith("fitz_unavailable")


def test_download_budget_counts_cached_and_stops(tmp_path):
    """Бюджет = объём кэша документа (включая cached), гейт между чанками."""
    blocks = {
        "document_id": "doc_b",
        "blocks": [
            {"block_id": "blk_a", "crop_url": "https://x/api/crops/a"},
            {"block_id": "blk_b", "crop_url": "https://x/api/crops/b"},
            {"block_id": "blk_c", "crop_url": "https://x/api/crops/c"},
        ],
    }
    body = b"x" * 100
    man = cache_crops(blocks, tmp_path, mode=MODE_DOWNLOAD,
                      fetch=lambda u, t: (200, body),
                      concurrency=1, max_total_bytes=150)
    assert man["counts"] == {"ok": 2, "error": 1}
    err = next(e for e in man["entries"] if e["status"] == "error")
    assert err["reason"] == "total_budget_exceeded"
    # повторный прогон: 2 cached (200 байт) уже поверх бюджета → хвост не качается
    man2 = cache_crops(blocks, tmp_path, mode=MODE_DOWNLOAD,
                       fetch=lambda u, t: (200, body),
                       concurrency=1, max_total_bytes=150)
    assert man2["counts"] == {"cached": 2, "error": 1}
    assert man2["total_bytes"] == 200  # cached-байты видны в манифесте


def test_stale_tmp_cleaned_and_not_cached(tmp_path):
    pdf = tmp_path / "src.pdf"
    _make_src_pdf(pdf)
    blocks = _local_blocks()
    del blocks["blocks"][1]
    crops = tmp_path / "crops"
    crops.mkdir(parents=True)
    stale = crops / "blk_good.pdf.123-456.tmp"  # огрызок прибитого процесса
    stale.write_bytes(b"%PD")
    man = cache_crops(blocks, tmp_path, pdf_path=pdf, mode=MODE_LOCAL,
                      fetch=_fetch_fail_if_called)
    assert not stale.exists()
    good = next(e for e in man["entries"] if e["block_id"] == "blk_good")
    assert good["status"] == "ok" and good["source"] == MODE_LOCAL


def test_v2_input_names_excludes_crop_cache(tmp_path):
    """Кэш кропов в 01_input — derived-артефакт, не пользовательский
    исходник: не должен попадать в списки файлов версии (иначе сотни
    blk_*.pdf становятся кандидатами в «основной» PDF)."""
    from backend.app.services.common.version_service import _v2_input_names
    inp = tmp_path / "01_input"
    (inp / "crops").mkdir(parents=True)
    (inp / "мой_документ.pdf").write_bytes(b"%PDF")
    (inp / "crops" / "blk_a.pdf").write_bytes(b"%PDF crop")
    (inp / "crops_manifest.json").write_text("{}", encoding="utf-8")
    assert _v2_input_names(tmp_path) == ["мой_документ.pdf"]


def test_crop_source_mode_env(monkeypatch):
    monkeypatch.delenv("AUDIT_CROP_CACHE_SOURCE", raising=False)
    assert crop_source_mode() == MODE_LOCAL  # заглушка по умолчанию (АИ 16.07)
    monkeypatch.setenv("AUDIT_CROP_CACHE_SOURCE", "download")
    assert crop_source_mode() == MODE_DOWNLOAD
    monkeypatch.setenv("AUDIT_CROP_CACHE_SOURCE", "local_pdf")
    assert crop_source_mode() == MODE_LOCAL


def test_ensure_crops_for_version_local_cut(tmp_path, monkeypatch):
    """Интеграция: комплект версии с blocks.json + PDF режется без сети."""
    from backend.app.services.common.crop_cache import ensure_crops_for_version
    monkeypatch.delenv("AUDIT_CROP_CACHE_ON_UPLOAD", raising=False)
    monkeypatch.delenv("AUDIT_CROP_CACHE_SOURCE", raising=False)
    inp = tmp_path / "01_input"
    work = tmp_path / "02_work"
    inp.mkdir(parents=True)
    work.mkdir(parents=True)
    _make_src_pdf(work / "document.pdf")
    blocks = _local_blocks()
    del blocks["blocks"][1]  # без сетевого фолбэка
    (inp / "x_blocks.json").write_text(json.dumps(blocks), encoding="utf-8")
    man = ensure_crops_for_version(tmp_path, background=False)
    assert man is not None and man["counts"] == {"ok": 1, "skipped": 1}
    assert (inp / "crops" / "blk_good.pdf").stat().st_size > 0
