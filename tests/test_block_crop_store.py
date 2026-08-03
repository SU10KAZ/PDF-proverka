"""Тесты эфемерных кропов: sidecar, эвакуация, лестница восстановления, LRU.

Фикстура строит настоящую версию projects_v2 с настоящим PDF (PyMuPDF), а не
моками: весь смысл механизма в том, что кроп воспроизводится из локального PDF
по координатам, и проверять это моками бессмысленно.
"""

from __future__ import annotations

import json
import os
import time

import pytest

fitz = pytest.importorskip("fitz")

from backend.app.pipeline.stages.block_context.contract import (  # noqa: E402
    block_file_for,
    crops_materialized,
)
from backend.app.services.common import block_crop_lru, block_crop_store  # noqa: E402


PAGE_W, PAGE_H = 1200, 1600


def _make_pdf(path, pages=2):
    doc = fitz.open()
    for i in range(pages):
        page = doc.new_page(width=PAGE_W / 2, height=PAGE_H / 2)
        page.draw_rect(fitz.Rect(20, 20, 260, 200), color=(0, 0, 0), width=2)
        page.insert_text((30, 60), f"BLOCK PAGE {i + 1}", fontsize=18)
        page.draw_line(fitz.Point(20, 240), fitz.Point(280, 240))
    doc.save(str(path))
    doc.close()


@pytest.fixture
def version(tmp_path, monkeypatch):
    """Версия projects_v2 с PDF, result.json и папкой кропов с реальными PNG."""
    monkeypatch.setattr(block_crop_lru, "cache_root", lambda: tmp_path / "lru")

    vdir = tmp_path / "versions" / "v001"
    (vdir / "02_work").mkdir(parents=True)
    pdf = vdir / "02_work" / "document.pdf"
    _make_pdf(pdf)

    blocks_meta = [
        {"id": "blk_a", "page": 1, "crop_px": [40, 40, 520, 400]},
        {"id": "blk_b", "page": 1, "crop_px": [40, 420, 560, 700]},
        {"id": "blk_c", "page": 2, "crop_px": [60, 60, 500, 380]},
    ]
    result = {
        "pages": [
            {
                "page_number": p,
                "width": PAGE_W,
                "height": PAGE_H,
                "blocks": [
                    {
                        "id": b["id"],
                        "crop_url": f"https://example.invalid/api/crops/{b['id']}",
                    }
                    for b in blocks_meta
                    if b["page"] == p
                ],
            }
            for p in (1, 2)
        ]
    }
    (vdir / "02_work" / "result.json").write_text(
        json.dumps(result, ensure_ascii=False), encoding="utf-8"
    )

    bd = vdir / "03_analysis" / "latest" / "blocks_stage02_100"
    bd.mkdir(parents=True)
    from backend.app.pipeline.stages.crop_blocks.blocks import crop_from_pdf

    entries = []
    for b in blocks_meta:
        out = bd / f"block_{b['id']}.png"
        w, h = crop_from_pdf(
            pdf, b["page"], b["crop_px"], PAGE_W, PAGE_H, out, dpi=100, min_long_side=800
        )
        entries.append(
            {
                "block_id": b["id"],
                "page": b["page"],
                "file": out.name,
                "size_kb": round(out.stat().st_size / 1024, 1),
                "crop_px": b["crop_px"],
                "render_size": [w, h],
                "block_type": "image",
                "source": "cloud",
            }
        )
    (bd / "index.json").write_text(
        json.dumps(
            {
                "total_blocks": len(entries),
                "profile": "stage02_100",
                "dpi": 100,
                "min_long_side": 800,
                "compact": False,
                "skip_small": False,
                "output_dir_name": "blocks_stage02_100",
                "blocks": entries,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return vdir, bd


# ─────────────────────────── crops_materialized ───────────────────────────


def test_crops_materialized_true_when_all_present(version):
    _vdir, bd = version
    ok, missing = crops_materialized(bd)
    assert ok is True and missing == []


def test_crops_materialized_reports_exact_missing_ids(version):
    _vdir, bd = version
    (bd / "block_blk_b.png").unlink()
    ok, missing = crops_materialized(bd)
    assert ok is False and missing == ["blk_b"]


def test_crops_materialized_rejects_truncated_png(version):
    """Обрезанный файл — не «есть»: иначе он уедет в модель как картинка."""
    _vdir, bd = version
    (bd / "block_blk_a.png").write_bytes(b"\x89PNG")
    ok, missing = crops_materialized(bd)
    assert ok is False and missing == ["blk_a"]


def test_block_file_for_prefers_index_field_over_png_convention():
    assert block_file_for({"block_id": "x", "file": "block_x.webp"}) == "block_x.webp"
    assert block_file_for({"block_id": "x"}) == "block_x.png"


# ───────────────────────────────── sidecar ────────────────────────────────


def test_build_sidecar_captures_page_px_and_crop_url(version):
    """page_px нет в index.json, а без него crop_from_pdf не пересчитает координаты."""
    _vdir, bd = version
    sc = block_crop_store.build_sidecar(bd)
    desc = sc["blocks"]["blk_a"]
    assert desc["page_px"] == [PAGE_W, PAGE_H]
    assert desc["pdf"] == "02_work/document.pdf"
    assert desc["crop_url"].endswith("/blk_a")
    assert sc["kept_block_ids"] == []


def test_block_without_crop_px_is_kept_not_evicted(version):
    _vdir, bd = version
    index = json.loads((bd / "index.json").read_text(encoding="utf-8"))
    for entry in index["blocks"]:
        if entry["block_id"] == "blk_c":
            entry.pop("crop_px")
    (bd / "index.json").write_text(json.dumps(index), encoding="utf-8")

    # без crop_px и без работающего crop_url блок невосстановим
    result_path = bd.parent.parent.parent / "02_work" / "result.json"
    data = json.loads(result_path.read_text(encoding="utf-8"))
    for page in data["pages"]:
        for blk in page["blocks"]:
            if blk["id"] == "blk_c":
                blk["crop_url"] = ""
    result_path.write_text(json.dumps(data), encoding="utf-8")

    sc = block_crop_store.build_sidecar(bd)
    assert "blk_c" in sc["kept_block_ids"]


def test_stale_sidecar_is_ignored_when_index_changed(version):
    _vdir, bd = version
    block_crop_store.evict_blocks_dir(bd, dry_run=False)
    assert block_crop_store.is_evicted(bd) is True
    index = json.loads((bd / "index.json").read_text(encoding="utf-8"))
    index["dpi"] = 300  # имитируем пере-кроп другой политикой
    (bd / "index.json").write_text(json.dumps(index), encoding="utf-8")
    assert block_crop_store.read_sidecar(bd) == {}
    assert block_crop_store.is_evicted(bd) is False


# ──────────────────────────────── эвакуация ───────────────────────────────


def test_dry_run_deletes_nothing(version):
    _vdir, bd = version
    before = sorted(p.name for p in bd.glob("block_*.png"))
    rep = block_crop_store.evict_blocks_dir(bd, dry_run=True)
    assert rep.evicted == 3 and rep.freed_bytes > 0
    assert sorted(p.name for p in bd.glob("block_*.png")) == before
    assert not (bd / block_crop_store.SIDECAR_NAME).exists()


def test_eviction_keeps_index_json_byte_identical(version):
    """index.json хеширует gemma-гейт — менять его нельзя ни на байт."""
    _vdir, bd = version
    before = (bd / "index.json").read_bytes()
    block_crop_store.evict_blocks_dir(bd, dry_run=False)
    assert (bd / "index.json").read_bytes() == before


def test_eviction_moves_to_trash_not_unlink(version):
    _vdir, bd = version
    rep = block_crop_store.evict_blocks_dir(bd, dry_run=False)
    assert rep.evicted == 3
    assert list(bd.glob("block_*.png")) == []
    assert len(list((bd / ".evicted").glob("*.png"))) == 3


def test_compact_profile_dir_is_never_evicted(version):
    _vdir, bd = version
    index = json.loads((bd / "index.json").read_text(encoding="utf-8"))
    index["compact"] = True
    (bd / "index.json").write_text(json.dumps(index), encoding="utf-8")
    rep = block_crop_store.evict_blocks_dir(bd, dry_run=True)
    assert rep.skipped_reason == "compact_profile" and rep.evicted == 0


def test_promoted_to_full_block_is_never_evicted(version):
    _vdir, bd = version
    index = json.loads((bd / "index.json").read_text(encoding="utf-8"))
    for entry in index["blocks"]:
        if entry["block_id"] == "blk_a":
            entry["promoted_to_full"] = True
    (bd / "index.json").write_text(json.dumps(index), encoding="utf-8")
    sc = block_crop_store.build_sidecar(bd)
    assert "blk_a" in sc["kept_block_ids"]
    block_crop_store.evict_blocks_dir(bd, dry_run=False)
    assert (bd / "block_blk_a.png").is_file()


# ────────────────────────── лестница восстановления ───────────────────────


def test_restore_uses_local_pdf_without_network(version):
    _vdir, bd = version
    sizes = {p.name: p.stat().st_size for p in bd.glob("block_*.png")}
    block_crop_store.evict_blocks_dir(bd, dry_run=False)
    (bd / ".evicted").rename(bd / "..trash")  # имитируем окончательное удаление

    def _explode(_url, _timeout):
        raise AssertionError("сеть не должна использоваться при local_first")

    rep = block_crop_store.hydrate_blocks_dir(bd, allow_network=False, http_get=_explode)
    assert rep.restored == 3 and rep.from_pdf == 3 and rep.failed == []
    assert sorted(p.name for p in bd.glob("block_*.png")) == sorted(sizes)


def test_restored_dimensions_match_index_render_size(version):
    _vdir, bd = version
    index = {b["block_id"]: b for b in json.loads((bd / "index.json").read_text())["blocks"]}
    block_crop_store.evict_blocks_dir(bd, dry_run=False)
    import shutil

    shutil.rmtree(bd / ".evicted")
    block_crop_store.hydrate_blocks_dir(bd, allow_network=False)
    for block_id, entry in index.items():
        # fitz.Pixmap читает НАТИВНЫЙ размер картинки; fitz.open(png).get_pixmap()
        # отрендерил бы PNG как «документ» в 72 DPI и дал бы другие числа.
        pix = fitz.Pixmap(str(bd / entry["file"]))
        assert (pix.width, pix.height) == tuple(entry["render_size"]), block_id


def test_restore_falls_back_to_cloud_when_local_pdf_missing(version):
    _vdir, bd = version
    pdf_bytes = None
    doc = fitz.open()
    page = doc.new_page(width=200, height=150)
    page.insert_text((10, 40), "CLOUD")
    pdf_bytes = doc.tobytes()
    doc.close()

    block_crop_store.evict_blocks_dir(bd, dry_run=False)
    import shutil

    shutil.rmtree(bd / ".evicted")
    (_vdir / "02_work" / "document.pdf").unlink()  # локальный источник исчез

    calls = []

    def _http_get(url, _timeout):
        calls.append(url)
        return pdf_bytes

    rep = block_crop_store.hydrate_blocks_dir(bd, allow_network=True, http_get=_http_get)
    assert rep.restored == 3 and rep.from_cloud == 3 and rep.from_pdf == 0
    assert len(calls) == 3


def test_unrestorable_block_returns_none_not_exception(version):
    _vdir, bd = version
    block_crop_store.evict_blocks_dir(bd, dry_run=False)
    import shutil

    shutil.rmtree(bd / ".evicted")
    (_vdir / "02_work" / "document.pdf").unlink()

    def _dead(_url, _timeout):
        raise OSError("404")

    path = block_crop_store.resolve_block_image(
        bd, "blk_a", allow_restore=True, http_get=_dead
    )
    assert path is None


def test_resolver_rejects_traversal_and_foreign_suffix(version):
    _vdir, bd = version
    assert block_crop_store.resolve_block_image(bd, "x", file_name="../secret.png") is None
    assert block_crop_store.resolve_block_image(bd, "x", file_name="/etc/passwd") is None
    assert block_crop_store.resolve_block_image(bd, "x", file_name="a.svg") is None


def test_resolver_returns_local_file_without_restore(version):
    _vdir, bd = version
    path = block_crop_store.resolve_block_image(bd, "blk_a", allow_restore=False)
    assert path == bd / "block_blk_a.png"


def test_restore_disabled_returns_none(version):
    _vdir, bd = version
    block_crop_store.evict_blocks_dir(bd, dry_run=False)
    import shutil

    shutil.rmtree(bd / ".evicted")
    assert block_crop_store.resolve_block_image(bd, "blk_a", allow_restore=False) is None


def test_policy_is_learned_from_directory_not_env(version, tmp_path):
    """blocks_gemma_300 обязан восстанавливаться в 300 DPI, а stage02 — в 100."""
    _vdir, bd = version
    assert block_crop_store.restore_policy_for_dir(bd)["dpi"] == 100
    hd = bd.parent / "blocks_gemma_300"
    hd.mkdir()
    assert block_crop_store.restore_policy_for_dir(hd)["dpi"] == 300


# ─────────────────────────────────── LRU ──────────────────────────────────


def test_lru_key_differs_across_versions_and_profiles(tmp_path):
    a = tmp_path / "v1" / "blocks_stage02_100"
    b = tmp_path / "v2" / "blocks_stage02_100"
    c = tmp_path / "v1" / "blocks_gemma_300"
    for d in (a, b, c):
        d.mkdir(parents=True)
    keys = {
        block_crop_lru.cache_key(a, "block_x.png"),
        block_crop_lru.cache_key(b, "block_x.png"),
        block_crop_lru.cache_key(c, "block_x.png"),
    }
    assert len(keys) == 3


def test_lru_get_touches_mtime_so_policy_is_lru_not_fifo(tmp_path, monkeypatch):
    monkeypatch.setattr(block_crop_lru, "cache_root", lambda: tmp_path / "lru")
    src = tmp_path / "src.png"
    src.write_bytes(b"x" * 4096)
    bd = tmp_path / "bd"
    bd.mkdir()
    placed = block_crop_lru.put(bd, "block_x.png", src)
    assert placed is not None
    old = placed.stat().st_mtime
    os.utime(placed, (old - 5000, old - 5000))
    block_crop_lru.get(bd, "block_x.png")
    assert placed.stat().st_mtime > old - 5000


def test_lru_sweep_evicts_oldest_first_and_spares_fresh(tmp_path, monkeypatch):
    monkeypatch.setattr(block_crop_lru, "cache_root", lambda: tmp_path / "lru")
    monkeypatch.setattr(block_crop_lru, "BLOCK_CROP_CACHE_MAX_BYTES", 8192)
    monkeypatch.setattr(block_crop_lru, "BLOCK_CROP_CACHE_MIN_AGE_S", 60)
    src = tmp_path / "src.png"
    src.write_bytes(b"x" * 4096)
    bd = tmp_path / "bd"
    bd.mkdir()

    old_paths = []
    for i in range(4):
        p = block_crop_lru.put(bd, f"block_old{i}.png", src)
        assert p is not None
        stale = time.time() - 10_000
        os.utime(p, (stale, stale))
        old_paths.append(p)
    fresh = block_crop_lru.put(bd, "block_fresh.png", src)

    res = block_crop_lru.sweep()
    assert res["evicted"] >= 1
    assert fresh.is_file(), "свежая запись не должна вытесняться"
    assert any(not p.is_file() for p in old_paths)


def test_restored_crop_still_hits_paid_cache(version):
    """Главное денежное свойство: восстановленный кроп НЕ должен платиться заново.

    Раньше ключ платного кэша считался от байтов PNG. Ре-рендер даёт другие
    байты (тот же вид, другое сжатие), поэтому каждый восстановленный блок был
    бы гарантированным промахом — то есть повторной оплатой ровно в том
    сценарии, ради которого кэш и заводили.
    """
    from backend.app.pipeline.stages.block_analysis import stage02_paid_cache as cache

    _vdir, bd = version
    top = json.loads((bd / "index.json").read_text(encoding="utf-8"))
    entry = next(b for b in top["blocks"] if b["block_id"] == "blk_a")
    index_top = {k: v for k, v in top.items() if k != "blocks"}

    before_bytes = (bd / entry["file"]).read_bytes()
    key_before = cache.compute_cache_key(
        model="m", block_id="blk_a", system_prompt="s", user_text="u",
        enrichment={}, page_text="p",
        image_identity=cache.build_image_identity(entry, index_top),
    )

    block_crop_store.evict_blocks_dir(bd, dry_run=False)
    import shutil

    shutil.rmtree(bd / ".evicted")
    block_crop_store.hydrate_blocks_dir(bd, allow_network=False)

    after_bytes = (bd / entry["file"]).read_bytes()
    key_after = cache.compute_cache_key(
        model="m", block_id="blk_a", system_prompt="s", user_text="u",
        enrichment={}, page_text="p",
        image_identity=cache.build_image_identity(entry, index_top),
    )
    assert key_before == key_after, "ключ кэша обязан пережить восстановление кропа"
    # И заодно фиксируем, ПОЧЕМУ старая схема не годилась: байты действительно
    # совпадать не обязаны (здесь ре-рендер детерминирован, но полагаться нельзя).
    assert isinstance(before_bytes, bytes) and isinstance(after_bytes, bytes)


def test_image_identity_changes_when_crop_moves():
    from backend.app.pipeline.stages.block_analysis import stage02_paid_cache as cache

    top = {"dpi": 100, "min_long_side": 800, "compact": False}
    a = {"block_id": "x", "page": 1, "file": "block_x.png",
         "crop_px": [0, 0, 10, 10], "render_size": [100, 100]}
    b = dict(a, crop_px=[5, 5, 20, 20])
    c = dict(a)
    assert cache.build_image_identity(a, top) != cache.build_image_identity(b, top)
    assert cache.build_image_identity(a, top) == cache.build_image_identity(c, top)
    # смена DPI — другая картинка
    assert cache.build_image_identity(a, top) != cache.build_image_identity(
        a, dict(top, dpi=300)
    )


def test_lru_skips_write_when_disk_below_floor(tmp_path, monkeypatch):
    monkeypatch.setattr(block_crop_lru, "cache_root", lambda: tmp_path / "lru")
    monkeypatch.setattr(block_crop_lru, "_free_bytes", lambda: 1)
    src = tmp_path / "src.png"
    src.write_bytes(b"x" * 4096)
    assert block_crop_lru.put(tmp_path / "bd", "block_x.png", src) is None
