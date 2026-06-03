"""Тесты Large Sheet Enrichment — page-level tile-first OCR для огромных листов.

Все тесты гермитичны: синтетические PDF создаются через PyMuPDF, артефакты
пишутся под временный ``COMPARISON_ROOT``. Live Qwen / Opus / сеть НЕ
вызываются ни в одном тесте (см. ``_no_qwen`` fixture).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import large_sheet_enrichment as ls
from backend.app.services.stage_comparison import store as store_mod


# ─── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_test"))
    (tmp_path / "comparison_test").mkdir(exist_ok=True)


@pytest.fixture(autouse=True)
def _no_qwen(monkeypatch):
    """Жёсткая гарантия: ни одна Qwen-ветка не вызывается в тестах."""
    from backend.app.services.stage_comparison import graphic_llm_local as g

    async def boom(*a, **kw):  # pragma: no cover - не должно вызываться
        raise AssertionError("describe_image_local must NOT be called in large-sheet tests")

    monkeypatch.setattr(g, "describe_image_local", boom, raising=False)


def _make_large_sheet_pdf(path: Path, *, width=3000, height=600, n_circuits=40) -> Path:
    """A2x5-подобный широкий лист с форматом, QF-маркерами и штампом."""
    import fitz

    doc = fitz.open()
    page = doc.new_page(width=width, height=height)
    page.insert_text((40, 30), "Формат A2x5  Однолинейная схема ВРУ-1 АВР ГРЩ")
    for i in range(1, n_circuits + 1):
        x = 40 + (i % 10) * 280
        y = 70 + (i // 10) * 90
        page.insert_text(
            (x, y),
            f"QF{i} 3P 16A 380В ППГнг(А)-HF 5х2,5 Iрасч {i}.2А Ррасч {i}кВт ЩР-{i}",
        )
    # штамп в правом-нижнем углу
    page.insert_text((width - 700, height - 60),
                     "Стадия Р Лист 1 Листов 5 Шифр 13АВ Изм. Подп. Разраб. ГИП")
    doc.save(str(path))
    doc.close()
    return path


def _make_small_pdf(path: Path) -> Path:
    import fitz

    doc = fitz.open()
    page = doc.new_page(width=595, height=842)  # обычный A4
    page.insert_text((72, 72), "Пояснительная записка. Общие сведения о проекте.")
    doc.save(str(path))
    doc.close()
    return path


def _bind_fake_pair(monkeypatch, pdf_left: Path, pdf_right: Path | None = None):
    pair = {
        "id": "p1",
        "left": {"pdf_path": str(pdf_left), "result_json_path": None},
        "right": {"pdf_path": str(pdf_right or pdf_left), "result_json_path": None},
    }
    monkeypatch.setattr(store_mod, "_find_pair_meta",
                        lambda sid, pid: pair if pid == "p1" else None)
    fake_session = {"id": "s1", "pairs": [pair]}
    monkeypatch.setattr(store_mod, "get_session",
                        lambda sid: fake_session if sid == "s1" else None)
    return pair


# ─── 1. Detection ───────────────────────────────────────────────────────────

def test_detect_large_sheet_a2x5_returns_true(tmp_path):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    det = ls.detect_large_sheet_candidate(pdf, 1)
    assert det["is_large_sheet"] is True
    assert det["sheet_kind"] == "electrical_single_line"
    assert det["aspect_ratio"] >= 2.5
    assert det["format_hint"] == "A2x5"
    assert det["confidence"] > 0.5
    assert "qf_markers" in det["reason"]
    assert det["recommended_processing_mode"] == "large_sheet_tile_first"


def test_detect_small_sheet_returns_false(tmp_path):
    pdf = _make_small_pdf(tmp_path / "small.pdf")
    det = ls.detect_large_sheet_candidate(pdf, 1)
    assert det["is_large_sheet"] is False
    assert det["recommended_processing_mode"] == "standard_image_enrichment"


def test_detect_dense_scheme_md_block(tmp_path):
    pdf = _make_small_pdf(tmp_path / "small.pdf")
    det = ls.detect_large_sheet_candidate(
        pdf, 1, md_block={"block_type": "dense_scheme", "text": "ВРУ ЩР QF"}
    )
    assert det["is_large_sheet"] is True
    assert "dense_scheme_block" in det["reason"]


# ─── 2. Words extraction ────────────────────────────────────────────────────

def test_extract_page_words_returns_bbox(tmp_path):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    words = ls.extract_page_words(pdf, 1)
    assert len(words) > 50
    w = words[0]
    assert set(w.keys()) >= {"text", "bbox", "page", "source"}
    assert len(w["bbox"]) == 4
    assert all(isinstance(c, (int, float)) for c in w["bbox"])
    assert w["source"] == "pdf_text"
    assert any("QF" in x["text"] for x in words)


# ─── 3. Tile generation с overlap ───────────────────────────────────────────

def test_generate_page_tiles_overlapping(tmp_path):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    words = ls.extract_page_words(pdf, 1)
    render = ls.render_large_sheet_page(pdf, 1, tmp_path / "hr.png", mode="highres")
    tiles = ls.generate_page_tiles(render, words, tmp_path / "tiles",
                                   tile_size=1800, overlap=0.15, max_tiles=60)
    assert len(tiles) >= 2
    for t in tiles:
        assert Path(t["image_path"]).exists()
        assert len(t["bbox_px"]) == 4 and len(t["bbox_page"]) == 4
    # перекрытие: соседние tiles в одном ряду должны пересекаться по X
    row0 = sorted([t for t in tiles if t["row"] == 0], key=lambda t: t["col"])
    if len(row0) >= 2:
        a, b = row0[0], row0[1]
        assert b["bbox_px"][0] < a["bbox_px"][2], "tiles must overlap in X"


def test_max_tiles_budget_respected(tmp_path):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf", width=6000, height=1200)
    words = ls.extract_page_words(pdf, 1)
    render = ls.render_large_sheet_page(pdf, 1, tmp_path / "hr.png", mode="highres")
    tiles = ls.generate_page_tiles(render, words, tmp_path / "tiles",
                                   tile_size=800, overlap=0.1, max_tiles=6)
    assert len(tiles) <= 6


# ─── 4. Words assigned to tiles ─────────────────────────────────────────────

def test_words_assigned_to_tiles(tmp_path):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    words = ls.extract_page_words(pdf, 1)
    render = ls.render_large_sheet_page(pdf, 1, tmp_path / "hr.png", mode="highres")
    tiles = ls.generate_page_tiles(render, words, tmp_path / "tiles")
    assigned = sum(1 for w in words
                   if any(ls._intersects(w["bbox"], t["bbox_page"]) for t in tiles))
    assert assigned > 0
    # большинство слов должно попасть хотя бы в один tile
    assert assigned / len(words) >= 0.8
    # у каждого непустого tile есть прикреплённые слова
    assert any(t["word_count"] > 0 for t in tiles)


# ─── 5. Dry-run создаёт артефакты ───────────────────────────────────────────

def test_dry_run_creates_all_artifacts(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    res = ls.run_large_sheet_enrichment("s1", "p1", "left", 1, run_model=False)
    assert res["status"] == "dry_run"
    for key in ("page_enriched_json_path", "page_enriched_md_path",
                "diagnostics_path", "overview_path", "page_render_path"):
        assert Path(res[key]).exists(), key
    # words.json / zones.json / tile_results.json лежат в page-папке
    page_dir = Path(res["page_enriched_json_path"]).parent
    assert (page_dir / "words.json").exists()
    assert (page_dir / "zones.json").exists()
    assert (page_dir / "tile_results.json").exists()
    assert (page_dir / "tiles").is_dir()
    assert any((page_dir / "tiles").glob("tile_*.png"))


# ─── 6. run_model=false не вызывает Qwen ────────────────────────────────────

def test_run_model_false_does_not_call_qwen(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    # _no_qwen fixture уже подменил describe_image_local на boom.
    res = ls.run_large_sheet_enrichment("s1", "p1", "left", 1, run_model=False)
    assert res["model_ran"] is False
    diag = json.loads(Path(res["diagnostics_path"]).read_text(encoding="utf-8"))
    assert diag["model_ran"] is False
    # tile_results в режиме dry_run, без qwen-пейлоадов
    tr = json.loads((Path(res["page_enriched_json_path"]).parent / "tile_results.json")
                    .read_text(encoding="utf-8"))
    assert tr["mode"] == "dry_run"
    assert all(t["qwen"] is None for t in tr["tiles"])


def test_run_model_true_does_not_call_qwen_only_warns(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    res = ls.run_large_sheet_enrichment("s1", "p1", "left", 1, run_model=True)
    assert res["model_ran"] is False
    assert "live_model_not_implemented_in_this_build" in res["warnings"]


# ─── 7. tile prompt: nearby_text как данные, не как инструкция ───────────────

def test_tile_prompt_marks_nearby_text_as_data(tmp_path):
    p = ls.build_tile_prompt("scheme", ["QF33", "ВРУ-1 с.ш.1"], "electrical_single_line")
    assert "<nearby_text>" in p and "QF33" in p
    low = p.lower()
    assert "nearby_text" in low
    assert "не как инструкцию" in low  # явный запрет трактовать как инструкцию


def test_tile_prompt_per_zone_variants():
    assert "ШТАМПА" in ls.build_tile_prompt("title_block", [])
    assert "ПРИМЕЧАНИЯ" in ls.build_tile_prompt("notes", [])
    assert "таблиц" in ls.build_tile_prompt("table", []).lower()


# ─── 8. merge_tile_results: dedup + conflicts ───────────────────────────────

def _circuit_tile(tile_id, bbox, *, breaker, cable=None, load=None, conf=0.8):
    return {
        "tile_id": tile_id, "bbox_page": bbox,
        "qwen": {"circuits": [{
            "circuit_id": "ВРУ1-ОДН-33", "breaker": breaker,
            "cable": cable, "load_name": load, "confidence": conf,
        }]},
    }


def test_merge_dedups_same_circuit_across_overlapping_tiles():
    tr = [
        _circuit_tile("tile_0012", [0, 0, 100, 100], breaker="QF33", cable="ППГнг 5х2,5"),
        _circuit_tile("tile_0013", [90, 0, 190, 100], breaker="QF33", load="Вентиляция П1"),
    ]
    page = ls.merge_tile_results(tr, [], {"zones": []})
    assert len(page["circuits"]) == 1
    c = page["circuits"][0]
    assert sorted(c["source_tiles"]) == ["tile_0012", "tile_0013"]
    # объединение неконфликтующих полей
    assert c["cable"] == "ППГнг 5х2,5"
    assert c["load_name"] == "Вентиляция П1"
    assert not c["conflicts"]
    assert c["bbox_union"] == [0, 0, 190, 100]


def test_merge_preserves_conflicts():
    tr = [
        _circuit_tile("tile_0012", [0, 0, 100, 100], breaker="QF33"),
        _circuit_tile("tile_0013", [90, 0, 190, 100], breaker="QF34"),  # конфликт
    ]
    page = ls.merge_tile_results(tr, [], {"zones": []})
    assert len(page["circuits"]) == 1
    conflicts = page["circuits"][0]["conflicts"]
    assert any(cf["field"] == "breaker" for cf in conflicts)


# ─── 9/10. page_enriched.json / .md содержат ожидаемое ──────────────────────

def test_page_enriched_json_and_md_structure(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    res = ls.run_large_sheet_enrichment("s1", "p1", "left", 1, run_model=False)
    pe = json.loads(Path(res["page_enriched_json_path"]).read_text(encoding="utf-8"))
    assert set(pe.keys()) >= {"circuits", "equipment", "visible_text",
                              "scheme_graph", "tables", "notes", "title_block",
                              "uncertainties", "detection", "provenance"}
    assert "render" in pe["provenance"]
    md = Path(res["page_enriched_md_path"]).read_text(encoding="utf-8")
    assert "# Large Sheet Enrichment — page 1" in md
    assert "## Electrical circuits" in md
    assert "## Coverage diagnostics" in md


# ─── 11. diagnostics counts ─────────────────────────────────────────────────

def test_diagnostics_counts_tiles_and_words(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    res = ls.run_large_sheet_enrichment("s1", "p1", "left", 1, run_model=False)
    diag = json.loads(Path(res["diagnostics_path"]).read_text(encoding="utf-8"))
    assert diag["tiles_total"] >= 1
    assert diag["words_total"] > 0
    assert 0 <= diag["words_assigned_percent"] <= 100
    # dry-run: цепей нет (Qwen не запускался), но tiles/words есть
    assert diag["circuits_detected"] == 0


# ─── 12. routing gating только при включённом env ───────────────────────────

def test_should_route_only_when_env_enabled(monkeypatch):
    det_large = {"is_large_sheet": True}
    # выключено по умолчанию
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", raising=False)
    assert ls.should_route_to_large_sheet(det_large) is False
    assert ls.should_route_to_large_sheet(block_type="dense_scheme") is False
    # включено
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    assert ls.should_route_to_large_sheet(det_large) is True
    assert ls.should_route_to_large_sheet(block_type="dense_scheme") is True
    # обычный блок не маршрутизируется даже при включённом флаге
    assert ls.should_route_to_large_sheet({"is_large_sheet": False},
                                          block_type="photo_or_general") is False


def test_feature_disabled_by_default(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", raising=False)
    assert ls.large_sheet_enabled() is False


# ─── 13. Endpoints ──────────────────────────────────────────────────────────

def _client(monkeypatch, pdf_left: Path):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod

    _bind_fake_pair(monkeypatch, pdf_left)
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def test_endpoint_post_dry_run_returns_200(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    client = _client(monkeypatch, pdf)
    r = client.post(
        "/api/stage-comparison/sessions/s1/pairs/p1/large-sheet-enrichment",
        json={"side": "left", "page": 1, "run_model": False},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "dry_run"
    assert body["ran_model"] is False
    assert body["tiles_total"] >= 1


def test_endpoint_get_summary_and_scan(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    client = _client(monkeypatch, pdf)
    # до запуска — not_run
    r0 = client.get("/api/stage-comparison/sessions/s1/pairs/p1/large-sheet-enrichment",
                    params={"side": "left", "page": 1})
    assert r0.status_code == 200
    assert r0.json()["status"] == "not_run"
    # запустить dry-run
    client.post("/api/stage-comparison/sessions/s1/pairs/p1/large-sheet-enrichment",
                json={"side": "left", "page": 1})
    # теперь — сводка
    r1 = client.get("/api/stage-comparison/sessions/s1/pairs/p1/large-sheet-enrichment",
                    params={"side": "left", "page": 1})
    assert r1.status_code == 200
    assert r1.json()["status"] == "dry_run"
    # scan без page → список больших листов
    r2 = client.get("/api/stage-comparison/sessions/s1/pairs/p1/large-sheet-enrichment",
                    params={"side": "left"})
    assert r2.status_code == 200
    assert len(r2.json()["large_sheets"]) >= 1


def test_endpoint_run_model_requires_confirm(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    client = _client(monkeypatch, pdf)
    # run_model без confirm → 400
    r = client.post("/api/stage-comparison/sessions/s1/pairs/p1/large-sheet-enrichment",
                    json={"side": "left", "page": 1, "run_model": True})
    assert r.status_code == 400
    # run_model + confirm → rejected (Qwen всё равно не вызывается)
    r2 = client.post("/api/stage-comparison/sessions/s1/pairs/p1/large-sheet-enrichment",
                     json={"side": "left", "page": 1, "run_model": True, "confirm": True})
    assert r2.status_code == 200
    assert r2.json()["status"] == "rejected"
    assert r2.json()["ran_model"] is False


def test_endpoint_404_for_unknown_pair(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    client = _client(monkeypatch, pdf)
    r = client.post("/api/stage-comparison/sessions/s1/pairs/NOPE/large-sheet-enrichment",
                    json={"side": "left", "page": 1})
    assert r.status_code == 404
