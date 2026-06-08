"""Тесты Large Sheet Enrichment — page-level tile-first OCR для огромных листов.

Все тесты гермитичны: синтетические PDF создаются через PyMuPDF, артефакты
пишутся под временный ``COMPARISON_ROOT``. Live Qwen / Opus / сеть НЕ
вызываются ни в одном тесте (см. ``_no_qwen`` fixture).
"""
from __future__ import annotations

import asyncio
import json
import types
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import large_sheet_enrichment as ls
from backend.app.services.stage_comparison import large_sheet_enrichment_jobs as ls_jobs
from backend.app.services.stage_comparison import md_image_enrichment as mi
from backend.app.services.stage_comparison import paths as sc_paths
from backend.app.services.stage_comparison import store as store_mod


def _fake_describe_factory(calls, *, circuits=None, status="done"):
    """Фабрика fake describe_fn — НЕ ходит в сеть, возвращает DescribeResult-like
    dict. Считает вызовы в ``calls``."""
    circuits = circuits if circuits is not None else [
        {"circuit_id": "ВРУ1-ОДН-33", "breaker": "QF33",
         "cable": "ППГнг(А)-HF 5х2,5", "load_name": "Вентиляция П1", "confidence": 0.82},
    ]

    async def _fake(image_path, prompt, *, model=None):
        calls.append({"image_path": str(image_path), "model": model})
        parsed = {"circuits": list(circuits), "visible_text": ["QF33", "ВРУ-1"],
                  "equipment": [], "notes": [], "title_block": {}}
        return {"status": status,
                "parsed": (parsed if status in ("done", "partial") else None),
                "full_raw_response": json.dumps(parsed, ensure_ascii=False),
                "error": (None if status in ("done", "partial") else "json_parse_failed"),
                "duration_sec": 0.01}

    return _fake


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


def test_extract_page_words_handles_page_rotation(tmp_path):
    """Регрессия: на повёрнутой странице (/Rotate 270) слова должны попадать в
    координаты рендера (page.rect), а не в неповёрнутый mediabox. Иначе они не
    ложатся на tiles (words_assigned_percent ≈ 1%)."""
    import fitz

    doc = fitz.open()
    page = doc.new_page(width=2384, height=3370)  # портрет (mediabox)
    for i in range(1, 30):
        page.insert_text((100, 80 + i * 90), f"QF{i} 16A ВРУ ЩР-{i}")
    page.set_rotation(270)  # лист отображается ландшафтно 3370×2384
    pdf = tmp_path / "rotated.pdf"
    doc.save(str(pdf))
    doc.close()

    words = ls.extract_page_words(pdf, 1)
    assert len(words) > 10
    # все слова должны лежать в пределах ПОВЁРНУТОГО rect (3370×2384), не 3370 по Y
    render = ls.render_large_sheet_page(pdf, 1, tmp_path / "r.png", mode="highres")
    assert render["width_px"] >= render["height_px"]  # ландшафт
    pw, ph = render["page_width"], render["page_height"]
    assert abs(pw - 3370) < 2 and abs(ph - 2384) < 2
    for w in words:
        assert w["bbox"][2] <= pw + 1, "word X вне повёрнутого rect — поворот не применён"
        assert w["bbox"][3] <= ph + 1, "word Y вне повёрнутого rect — поворот не применён"
    # и они реально назначаются на tiles
    tiles = ls.generate_page_tiles(render, words, tmp_path / "tiles")
    assigned = sum(1 for w in words
                   if any(ls._intersects(w["bbox"], t["bbox_page"]) for t in tiles))
    assert assigned / len(words) >= 0.9


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


def test_sync_run_model_true_stays_dry_run_no_qwen(tmp_path, monkeypatch):
    """Sync-путь run_model=True не зовёт Qwen — он остаётся dry-run и
    подсказывает использовать live job."""
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    res = ls.run_large_sheet_enrichment("s1", "p1", "left", 1, run_model=True)
    assert res["model_ran"] is False
    assert res["status"] == "dry_run"
    assert "sync_model_run_not_supported_use_live_job" in res["warnings"]


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


# ─── Weak-id-aware merge (после controlled live-test) ───────────────────────

def _tile(tile_id, bbox, circuits):
    return {"tile_id": tile_id, "bbox_page": bbox, "qwen": {"circuits": circuits}}


def test_is_weak_circuit_id_matrix():
    for v in ("", "unknown", "1", "2", "206", "234", "2Р", "3P", "QS", "QF", "ab", None):
        assert ls.is_weak_circuit_id(v) is True, v
    for v in ("ВРУ1-ОДН-33", "QF33", "ЩР-12", "ВРП-7"):
        assert ls.is_weak_circuit_id(v) is False, v


def test_weak_id_1_does_not_merge_distinct_circuits():
    # одинаковый слабый id "1", но разные breaker/cable/load → НЕ объединять
    page = ls.merge_tile_results([
        _tile("t1", [0, 0, 100, 100],
              [{"circuit_id": "1", "breaker": "QF12", "cable": "ВВГ 3х2.5", "load_name": "Свет"}]),
        _tile("t2", [500, 500, 600, 600],
              [{"circuit_id": "1", "breaker": "QF99", "cable": "ВВГ 5х6", "load_name": "Розетки"}]),
    ], [], {"zones": []})
    assert len(page["circuits"]) == 2
    assert all(c["merge_method"] != "strong_id" for c in page["circuits"])


def test_weak_id_206_does_not_merge_distinct_meters():
    # "206" — номер счётчика; только load_name (слабый composite) → kept_separate
    page = ls.merge_tile_results([
        _tile("t1", [0, 0, 100, 100], [{"circuit_id": "206", "load_name": "Счётчик А"}]),
        _tile("t2", [500, 0, 600, 100], [{"circuit_id": "206", "load_name": "Счётчик Б"}]),
    ], [], {"zones": []})
    assert len(page["circuits"]) == 2
    assert all(c["merge_method"] == "kept_separate_weak_id" for c in page["circuits"])
    assert page["merge_stats"]["overmerge_prevented_count"] == 1


def test_strong_id_merges():
    page = ls.merge_tile_results([
        _tile("t1", [0, 0, 100, 100],
              [{"circuit_id": "ВРУ1-ОДН-33", "breaker": "QF33", "cable": "ППГнг 5х2,5"}]),
        _tile("t2", [9000, 9000, 9100, 9100],  # даже без overlap — strong id рулит
              [{"circuit_id": "ВРУ1-ОДН-33", "breaker": "QF33", "load_name": "Вентиляция"}]),
    ], [], {"zones": []})
    assert len(page["circuits"]) == 1
    c = page["circuits"][0]
    assert c["merge_method"] == "strong_id"
    assert c["cable"] == "ППГнг 5х2,5" and c["load_name"] == "Вентиляция"


def test_composite_overlap_merges():
    # слабый id, но одинаковый breaker+cable+load и пересекающиеся tiles → merge
    page = ls.merge_tile_results([
        _tile("t1", [0, 0, 100, 100],
              [{"circuit_id": "unknown", "breaker": "QF5", "cable": "ВВГ 3х2.5", "load_name": "Насос"}]),
        _tile("t2", [90, 0, 190, 100],
              [{"circuit_id": "unknown", "breaker": "QF5", "cable": "ВВГ 3х2.5", "load_name": "Насос"}]),
    ], [], {"zones": []})
    assert len(page["circuits"]) == 1
    assert page["circuits"][0]["merge_method"] == "overlap_confirmed"
    assert sorted(page["circuits"][0]["source_tiles"]) == ["t1", "t2"]


def test_same_breaker_diff_load_not_merged_conflict_group():
    page = ls.merge_tile_results([
        _tile("t1", [0, 0, 100, 100],
              [{"circuit_id": "unknown", "breaker": "QF7", "load_name": "Свет",
                "calculated_power_kw": 5}]),
        _tile("t2", [500, 0, 600, 100],
              [{"circuit_id": "unknown", "breaker": "QF7", "load_name": "Розетки",
                "calculated_power_kw": 10}]),
    ], [], {"zones": []})
    assert len(page["circuits"]) == 2  # не объединены
    groups = page["conflict_groups"]
    assert len(groups) == 1
    assert groups[0]["breaker"] == "QF7"
    assert set(groups[0]["loads"]) == {"СВЕТ", "РОЗЕТКИ"}


def test_merge_stats_and_diagnostics_counts():
    page = ls.merge_tile_results([
        _tile("t1", [0, 0, 100, 100],
              [{"circuit_id": "1", "breaker": "QF1", "cable": "ВВГ 3х2.5", "load_name": "L1"}]),
        _tile("t2", [9000, 0, 9100, 100],
              [{"circuit_id": "1", "breaker": "QF2", "cable": "ВВГ 5х6", "load_name": "L2"}]),
        _tile("t3", [0, 500, 100, 600],
              [{"circuit_id": "ВРУ1-ОДН-9", "breaker": "QF9", "cable": "ВВГ 3х4", "load_name": "L3"}]),
    ], [], {"zones": []})
    ms = page["merge_stats"]
    assert ms["circuits_raw_count"] == 3
    assert ms["circuits_merged_count"] == 3   # все разные
    assert ms["weak_id_count"] == 2           # "1" ×2 слабые, "ВРУ1-ОДН-9" сильный
    diag = ls.build_diagnostics([], [], page, tiles_processed=0, tiles_failed=0,
                                warnings=[], zones={"zones": []})
    assert diag["circuits_raw_count"] == 3
    assert diag["circuits_merged_count"] == 3
    assert diag["weak_id_count"] == 2
    assert "overmerge_prevented_count" in diag
    assert "conflict_groups_count" in diag


def test_page_enriched_has_merge_key_method_and_groups():
    page = ls.merge_tile_results([
        _tile("t1", [0, 0, 100, 100],
              [{"circuit_id": "ВРУ1-ОДН-33", "breaker": "QF33", "cable": "ППГнг 5х2,5"}]),
    ], [], {"zones": []})
    c = page["circuits"][0]
    assert "merge_key" in c and "merge_method" in c and "conflicts" in c
    assert "conflict_groups" in page and "merge_stats" in page


# ─── md_image_enrichment integration (gated, default OFF) ───────────────────

def _write_ls_artifact(sid, pid, side, page, *, circuits=200):
    pe = {
        "detection": {"sheet_kind": "electrical_single_line", "format_hint": "A2x5"},
        "mode": "model", "page": page, "side": side,
        "title_block": {"doc_code": "X", "organization": "ООО Y"},
        "circuits": [{"id": str(i), "breaker": f"QF{i}", "cable": "ППГнг",
                      "load_name": f"L{i}", "merge_method": "composite",
                      "conflicts": []} for i in range(circuits)],
        "conflict_groups": [], "merge_stats": {"circuits_raw_count": circuits},
    }
    diag = {"tiles_total": 8, "circuits_detected": circuits, "conflicts_count": 2,
            "overmerge_prevented_count": 1, "warnings": []}
    sc_paths.large_sheet_artifact_path(sid, pid, side, page, "page_enriched.json").write_text(
        json.dumps(pe, ensure_ascii=False), encoding="utf-8")
    sc_paths.large_sheet_artifact_path(sid, pid, side, page, "diagnostics.json").write_text(
        json.dumps(diag, ensure_ascii=False), encoding="utf-8")


def test_gate_disabled_returns_none(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", raising=False)
    mb = types.SimpleNamespace(page=24, block_id="b1")
    assert mi._maybe_large_sheet_block("s1", "p1", "left", mb, "dense_scheme") is None


def test_gate_enabled_missing_artifact_not_prepared(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    mb = types.SimpleNamespace(page=24, block_id="b1")
    upd = mi._maybe_large_sheet_block("s1", "p1", "left", mb, "dense_scheme")
    assert upd is not None
    assert upd["status"] == "large_sheet_not_prepared"
    assert upd["source"] == "large_sheet_enrichment"
    assert upd["usable_for_diff"] is False
    assert "Запустите Large Sheet Enrichment" in upd["large_sheet_md"]
    assert "large_sheet_not_prepared" in upd["large_sheet_warnings"]


def test_gate_enabled_existing_artifact_used_no_qwen(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    _write_ls_artifact("s1", "p1", "left", 24, circuits=5)
    mb = types.SimpleNamespace(page=24, block_id="b1")
    # candidate via existing artifact даже если block_type обычный
    upd = mi._maybe_large_sheet_block("s1", "p1", "left", mb, "photo_or_general")
    assert upd is not None
    assert upd["status"] == "done"
    assert upd["source"] == "large_sheet_enrichment"
    assert upd["large_sheet"] is True
    assert "Цепи" in upd["large_sheet_md"]
    assert upd["page_enriched_json_path"].endswith("page_enriched.json")
    assert isinstance(upd["diagnostics"], dict)


def test_gate_ordinary_block_old_path(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    mb = types.SimpleNamespace(page=5, block_id="b1")  # нет артефакта, не dense
    assert mi._maybe_large_sheet_block("s1", "p1", "left", mb, "photo_or_general") is None


def test_embed_summary_compact_does_not_exceed_limit(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    _write_ls_artifact("s1", "p1", "left", 24, circuits=200)
    mb = types.SimpleNamespace(page=24, block_id="b1")
    upd = mi._maybe_large_sheet_block("s1", "p1", "left", mb, "dense_scheme")
    body = upd["large_sheet_md"]
    # компактно: показаны первые 12, не все 200; есть указание на остаток
    assert "показаны первые 12" in body
    assert "ещё 188 цепей" in body
    assert len(body) < 6500


def test_build_enriched_md_renders_large_sheet_source():
    md = "### СТРАНИЦА 24\n\n### BLOCK [IMAGE]: img-024\nВРУ ЩР QF схема\n"
    blocks = mi.parse_md_blocks(md)
    descs = [{"order": b.order, "source": "large_sheet_enrichment", "status": "done",
              "large_sheet_md": "### Большой лист\n\nТЕЛО-СВОДКИ-LS\n"}
             for b in blocks if b.is_image]
    assert descs, "expected an image block"
    enriched = mi.build_enriched_md(blocks, descs)
    assert "QWEN_IMAGE_DESCRIPTION_START" in enriched
    assert "ТЕЛО-СВОДКИ-LS" in enriched


@pytest.mark.asyncio
async def test_enrich_side_uses_artifact_no_qwen(monkeypatch, tmp_path):
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", "https://test.example.com")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_AUTH", "basic")
    monkeypatch.setenv("NGROK_AUTH_USER", "u")
    monkeypatch.setenv("NGROK_AUTH_PASS", "p")

    md = tmp_path / "left.md"
    md.write_text("### СТРАНИЦА 24\n\n### BLOCK [IMAGE]: img-024\nВРУ ЩР QF однолинейная схема\n",
                  encoding="utf-8")
    _write_ls_artifact("sess", "pair", "left", 24, circuits=5)

    calls = {"n": 0}

    async def fake_describe(image_path, prompt):  # pragma: no cover
        calls["n"] += 1
        raise AssertionError("Qwen must NOT be called for large-sheet block")

    summary = await mi.enrich_side(
        "sess", "pair", "left",
        md_path=str(md), result_json_path=None,
        render_crop=lambda *a, **k: None,
        describe_fn=fake_describe,
        run_model=True,  # даже с run_model=True — Qwen не зовётся для large sheet
    )
    assert calls["n"] == 0
    ls_items = [it for it in summary.items if it.get("source") == "large_sheet_enrichment"]
    assert ls_items, "expected a large_sheet_enrichment item"
    assert ls_items[0]["status"] == "done"
    assert ls_items[0]["large_sheet"] is True
    # enriched MD на диске содержит компактную сводку
    p = sc_paths.text_enrichment_md_path("sess", "pair", "left")
    assert p.exists()
    assert "Large Sheet Enrichment" in p.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_enrich_side_disabled_keeps_old_flow(monkeypatch, tmp_path):
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", raising=False)
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", "https://test.example.com")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_AUTH", "basic")
    monkeypatch.setenv("NGROK_AUTH_USER", "u")
    monkeypatch.setenv("NGROK_AUTH_PASS", "p")

    md = tmp_path / "left.md"
    md.write_text("### СТРАНИЦА 24\n\n### BLOCK [IMAGE]: img-024\nВРУ ЩР QF однолинейная схема\n",
                  encoding="utf-8")
    _write_ls_artifact("sess2", "pair2", "left", 24, circuits=5)  # есть артефакт, но фича OFF

    calls = {"n": 0}

    async def fake_describe(image_path, prompt):
        calls["n"] += 1
        raise AssertionError("dry-run должен пропустить вызов")

    summary = await mi.enrich_side(
        "sess2", "pair2", "left",
        md_path=str(md), result_json_path=None,
        render_crop=lambda *a, **k: None,
        describe_fn=fake_describe,
        run_model=False,  # dry-run
    )
    # фича выключена → НИ один item не помечен large_sheet_enrichment
    assert all(it.get("source") != "large_sheet_enrichment" for it in summary.items)
    assert calls["n"] == 0


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
    # run_model + confirm → направляет на job endpoint (Qwen синхронно не вызывается)
    r2 = client.post("/api/stage-comparison/sessions/s1/pairs/p1/large-sheet-enrichment",
                     json={"side": "left", "page": 1, "run_model": True, "confirm": True})
    assert r2.status_code == 200
    assert r2.json()["status"] == "use_job_endpoint"
    assert r2.json()["ran_model"] is False


def test_endpoint_404_for_unknown_pair(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    client = _client(monkeypatch, pdf)
    r = client.post("/api/stage-comparison/sessions/s1/pairs/NOPE/large-sheet-enrichment",
                    json={"side": "left", "page": 1})
    assert r.status_code == 404


def test_endpoint_direct_run_model_points_to_job(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    client = _client(monkeypatch, pdf)
    # без confirm → 400
    r = client.post("/api/stage-comparison/sessions/s1/pairs/p1/large-sheet-enrichment",
                    json={"side": "left", "page": 1, "run_model": True})
    assert r.status_code == 400
    # с confirm → use_job_endpoint, Qwen НЕ вызывается синхронно
    r2 = client.post("/api/stage-comparison/sessions/s1/pairs/p1/large-sheet-enrichment",
                     json={"side": "left", "page": 1, "run_model": True, "confirm": True})
    assert r2.status_code == 200
    assert r2.json()["status"] == "use_job_endpoint"
    assert r2.json()["ran_model"] is False


# ─── STAGE 2: live tile runner (injected describe_fn, no network) ────────────

def test_live_runner_calls_model_saves_raw_prompt_and_merges(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    calls = []
    fake = _fake_describe_factory(calls)
    res = asyncio.run(ls.run_large_sheet_enrichment_live(
        "s1", "p1", "left", 1, describe_fn=fake, model="qwen-test"))
    assert res["status"] == "model"
    assert res["model_ran"] is True
    assert len(calls) == res["tiles_total"] >= 1   # один вызов на tile
    assert all(c["model"] == "qwen-test" for c in calls)
    # circuits смёржены в page_enriched.json
    pe = json.loads(Path(res["page_enriched_json_path"]).read_text(encoding="utf-8"))
    assert len(pe["circuits"]) >= 1
    assert any(c.get("breaker") == "QF33" for c in pe["circuits"])
    assert pe["mode"] == "model"
    # raw + prompt сохранены на диск
    page_dir = Path(res["page_enriched_json_path"]).parent
    assert any((page_dir / "raw").glob("tile_*.txt"))
    assert any((page_dir / "prompts").glob("tile_*.txt"))
    # tile_results.json в режиме model, qwen заполнен
    tr = json.loads((page_dir / "tile_results.json").read_text(encoding="utf-8"))
    assert tr["mode"] == "model"
    assert any(isinstance(t["qwen"], dict) for t in tr["tiles"])


def test_live_runner_cache_hit_does_not_call_model(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    # prepare один раз → стабильные tile-картинки на диске
    ctx = ls._prepare_page_artifacts("s1", "p1", "left", 1, tile_size=None, overlap=None)
    calls = []
    fake = _fake_describe_factory(calls)
    r1 = asyncio.run(ls._run_tiles_with_model(ctx, describe_fn=fake, model="m"))
    n = len(r1)
    assert len(calls) == n and n >= 1
    assert all(not t["from_cache"] for t in r1)
    # повтор с тем же ctx (те же файлы) → всё из кеша, модель не зовётся
    calls.clear()
    r2 = asyncio.run(ls._run_tiles_with_model(ctx, describe_fn=fake, model="m"))
    assert len(calls) == 0
    assert all(t["from_cache"] for t in r2)


def test_live_runner_fail_soft_on_bad_tile(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    ctx = ls._prepare_page_artifacts("s1", "p1", "left", 1, tile_size=None, overlap=None)
    n_tiles = len(ctx["tiles"])
    assert n_tiles >= 2

    # первый tile падает (invalid_json), остальные ок — page не должен упасть
    state = {"i": 0}

    async def mixed(image_path, prompt, *, model=None):
        state["i"] += 1
        if state["i"] == 1:
            return {"status": "invalid_json", "parsed": None,
                    "full_raw_response": "garbage", "error": "json_parse_failed"}
        return {"status": "done",
                "parsed": {"circuits": [{"circuit_id": "QF99", "breaker": "QF99"}]},
                "full_raw_response": "{}", "duration_sec": 0.01}

    res = asyncio.run(ls.run_large_sheet_enrichment_live(
        "s1", "p1", "left", 1, describe_fn=mixed, model="m"))
    assert res["status"] == "model"  # не упал
    assert res["tiles_failed"] >= 1
    assert res["tiles_done"] >= 1
    diag = json.loads(Path(res["diagnostics_path"]).read_text(encoding="utf-8"))
    assert diag["tiles_failed"] >= 1


def test_live_runner_on_tile_progress_per_tile(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    events = []
    calls = []
    fake = _fake_describe_factory(calls)
    res = asyncio.run(ls.run_large_sheet_enrichment_live(
        "s1", "p1", "left", 1, describe_fn=fake, model="m",
        on_tile_progress=lambda ev: events.append(ev)))
    assert len(events) == res["tiles_total"]
    for ev in events:
        assert {"tile_id", "index", "total", "status", "zone_hint", "duration_sec"} <= set(ev)
    assert [e["index"] for e in events] == list(range(1, len(events) + 1))


def test_live_runner_progress_callback_error_does_not_break(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    calls = []
    fake = _fake_describe_factory(calls)

    def boom_cb(ev):
        raise RuntimeError("callback explode")

    res = asyncio.run(ls.run_large_sheet_enrichment_live(
        "s1", "p1", "left", 1, describe_fn=fake, model="m", on_tile_progress=boom_cb))
    assert res["status"] == "model"  # callback-ошибка не уронила runner


# ─── STAGE 2: jobs ──────────────────────────────────────────────────────────

def test_job_without_confirm_rejected(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    job = ls_jobs.create_job("s1", scope="page", pair_id="p1", side="left",
                             page=1, confirm=False)
    assert job["status"] == "rejected_no_confirm"
    # в фон такой job не уходит — items тоже rejected
    assert all(it["status"] == "rejected_no_confirm" for it in job["items"])


def test_job_runs_and_updates_progress_per_tile(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    calls = []
    fake = _fake_describe_factory(calls)
    monkeypatch.setattr(ls_jobs, "_build_describe_fn", lambda cfg, model: fake)

    job = ls_jobs.create_job("s1", scope="page", pair_id="p1", side="left",
                             page=1, confirm=True)
    assert job["status"] == "queued"
    done = asyncio.run(ls_jobs.run_job("s1", job["id"]))
    assert done["status"] == "done"
    assert done["progress"]["done"] == 1
    item = done["items"][0]
    assert item["status"] == "done"
    assert item["tiles_total"] >= 1
    assert item["tiles_done"] >= 1
    assert len(calls) == item["tiles_total"]


def test_job_cancel_prevents_model_calls(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    _bind_fake_pair(monkeypatch, pdf)
    calls = []
    fake = _fake_describe_factory(calls)
    monkeypatch.setattr(ls_jobs, "_build_describe_fn", lambda cfg, model: fake)

    job = ls_jobs.create_job("s1", scope="page", pair_id="p1", side="left",
                             page=1, confirm=True)
    cancelled = ls_jobs.cancel_job("s1", job["id"])
    assert cancelled["status"] == "cancelled"
    # run после cancel → возвращает рано, Qwen не зовётся
    res = asyncio.run(ls_jobs.run_job("s1", job["id"]))
    assert res["status"] == "cancelled"
    assert len(calls) == 0


def test_job_endpoint_rejects_without_confirm(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    client = _client(monkeypatch, pdf)
    r = client.post("/api/stage-comparison/sessions/s1/large-sheet-enrichment-jobs",
                    json={"scope": "page", "pair_id": "p1", "side": "left", "page": 1})
    assert r.status_code == 200
    assert r.json()["status"] == "rejected_no_confirm"


def test_job_endpoint_confirm_creates_job_no_sync_qwen(tmp_path, monkeypatch):
    pdf = _make_large_sheet_pdf(tmp_path / "big.pdf")
    client = _client(monkeypatch, pdf)
    # не запускаем реальный фон-таск (и тем самым реальный Qwen)
    started = {"n": 0}
    monkeypatch.setattr(ls_jobs, "start_job_in_background",
                        lambda sid, jid: started.__setitem__("n", started["n"] + 1) or jid)
    r = client.post("/api/stage-comparison/sessions/s1/large-sheet-enrichment-jobs",
                    json={"scope": "page", "pair_id": "p1", "side": "left",
                          "page": 1, "confirm": True})
    assert r.status_code == 200
    body = r.json()
    assert body["id"].startswith("lsj_")
    assert body["status"] in ("queued", "running")
    assert started["n"] == 1
    # GET job + cancel
    jid = body["id"]
    g = client.get(f"/api/stage-comparison/sessions/s1/large-sheet-enrichment-jobs/{jid}")
    assert g.status_code == 200 and g.json()["id"] == jid
    c = client.post(f"/api/stage-comparison/sessions/s1/large-sheet-enrichment-jobs/{jid}/cancel")
    assert c.status_code == 200 and c.json()["status"] == "cancelled"


# ─── diff_anchors → IMAGE_DIFF_INDEX integration ────────────────────────────

def _page_enriched_sample() -> dict:
    return {
        "schema_version": 1,
        "circuits": [
            {"id": "circuit", "cable": "ППГнг(А)-HF-(5х6)пвх.40",
             "calculated_power_kw": 25.0, "calculated_current_a": 38.6},
            {"id": "1", "breaker": "QS", "cable": "ППГнг(А)-HF-(5х6)пвх.40",
             "load_name": "ЯК", "calculated_power_kw": 18.0, "calculated_current_a": 27.9},
            {"id": "206", "breaker": "PRSNO", "cable": "ППГнг(А)-HF-(3х10)пвх.32",
             "calculated_current_a": 46.4},
        ],
        "scheme_graph": {
            "nodes": [{"id": "QS"}, {"id": "Wh"}, {"id": "ЯК"}, {"id": "ЯУР"},
                      {"id": "УЭРМ-21-50-УХЛ4"}],
            "connections": [{"from": "QS", "to": "Wh"}, {"from": "ЯК", "to": "ЯУР"},
                            {"from": "УЭРМ-21-50-УХЛ4", "to": "QS"}],
        },
        "equipment": [],
        "title_block": {"doc_code": "ЛЛ213"},
        "detection": {"confidence": 0.8, "sheet_kind": "electrical_single_line"},
    }


def test_build_large_sheet_diff_anchors_v5_schema():
    anchors = ls.build_large_sheet_diff_anchors(_page_enriched_sample())
    labels = [x["raw_text"] for x in anchors["labels"]]
    ratings = [x["raw_text"] for x in anchors["ratings"]]
    conns = [(x["from_raw"], x["to_raw"]) for x in anchors["connections"]]
    # буквальные маркировки узлов попадают в labels
    assert "ЯК" in labels and "ЯУР" in labels and "УЭРМ-21-50-УХЛ4" in labels
    # слабые id цепей ('1' / '206' — чисто числовой/однознач.) отфильтрованы
    assert "1" not in labels and "206" not in labels
    # номиналы кабель/ток/мощность
    assert "ППГнг(А)-HF-(5х6)пвх.40" in ratings
    assert any(r.endswith("А") for r in ratings) and any(r.endswith("кВт") for r in ratings)
    # связи схемы
    assert ("ЯК", "ЯУР") in conns and ("QS", "Wh") in conns


def test_large_sheet_block_attaches_diff_anchors_for_index(tmp_path, monkeypatch):
    """_maybe_large_sheet_block кладёт diff_anchors в item['description'], и они
    проходят в IMAGE_DIFF_INDEX через общий _extract_anchors_from_description."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    sid, pid, side, page = "s1", "p1", "left", 24
    pe_dir = sc_paths.large_sheet_artifact_path(sid, pid, side, page, "page_enriched.json").parent
    pe_dir.mkdir(parents=True, exist_ok=True)
    pe = _page_enriched_sample()
    (pe_dir / "page_enriched.json").write_text(json.dumps(pe, ensure_ascii=False), encoding="utf-8")
    (pe_dir / "page_enriched.md").write_text("# md", encoding="utf-8")
    (pe_dir / "diagnostics.json").write_text(json.dumps({"status": "done"}), encoding="utf-8")

    mb = types.SimpleNamespace(page=page)
    upd = mi._maybe_large_sheet_block(sid, pid, side, mb, "dense_scheme")
    assert upd is not None and upd["source"] == "large_sheet_enrichment"
    assert isinstance(upd.get("description"), dict)
    assert upd["description"]["diff_anchors"]["labels"]

    # extractor видит large-sheet labels
    anchors = mi._extract_anchors_from_description(upd)
    assert "ЯК" in anchors["labels"] and "ЯУР" in anchors["labels"]

    # и они попадают в IMAGE_DIFF_INDEX
    upd.setdefault("page", page)
    idx = mi.build_image_diff_index([upd])
    assert "ЯУР" in idx and "УЭРМ-21-50-УХЛ4" in idx


# ─── Production embed / write-gate ──────────────────────────────────────────

async def _raise_describe(*a, **k):  # pragma: no cover - must never run in dry-run
    raise AssertionError("Qwen describe must NOT be called")


def _ls_md(tmp_path: Path) -> Path:
    md = tmp_path / "side.md"
    md.write_text(
        "### СТРАНИЦА 24\n\n### BLOCK [IMAGE]: img-024\nВРУ ЩР QF однолинейная схема\n",
        encoding="utf-8")
    return md


async def _run_enrich(sid, pid, side, md, *, run_model=False, force=False):
    return await mi.enrich_side(
        sid, pid, side,
        md_path=str(md), result_json_path=None,
        render_crop=lambda *a, **k: None,
        describe_fn=_raise_describe,
        run_model=run_model, force=force,
    )


@pytest.mark.asyncio
async def test_writegate_dryrun_rewrites_existing_md_with_summary(monkeypatch, tmp_path):
    """1. artifact exists + run_model=False + gate enabled → enriched MD
    перезаписан со встроенной large-sheet сводкой, хотя файл уже существовал
    и Qwen не звался."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    sid, pid, side = "wg1", "p", "left"
    md = _ls_md(tmp_path)
    _write_ls_artifact(sid, pid, side, 24, circuits=5)
    md_out = sc_paths.text_enrichment_md_path(sid, pid, side)
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text("OLD ENRICHED WITHOUT SUMMARY\n", encoding="utf-8")

    await _run_enrich(sid, pid, side, md, run_model=False)

    text = md_out.read_text(encoding="utf-8")
    assert "Large Sheet Enrichment" in text
    assert "OLD ENRICHED WITHOUT SUMMARY" not in text
    data = mi._read_image_descriptions(sid, pid, side)
    assert data["large_sheet_embedded"] is True
    assert data["enriched_md_written"] is True
    assert data["enriched_md_write_reason"] == "large_sheet_embedded"


@pytest.mark.asyncio
async def test_writegate_dryrun_does_not_call_qwen(monkeypatch, tmp_path):
    """2. artifact exists + run_model=False → Qwen не вызывается (гарантия —
    _raise_describe + autouse _no_qwen)."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    sid, pid, side = "wg2", "p", "left"
    md = _ls_md(tmp_path)
    _write_ls_artifact(sid, pid, side, 24, circuits=5)
    summary = await _run_enrich(sid, pid, side, md, run_model=False)
    ls = [it for it in summary.items if it.get("source") == "large_sheet_enrichment"]
    assert ls and ls[0]["status"] == "done"


@pytest.mark.asyncio
async def test_writegate_missing_artifact_not_prepared_for_candidate(monkeypatch, tmp_path):
    """3. artifact missing → large_sheet_not_prepared item только для candidate;
    non-candidate идёт обычным путём (нет large_sheet item)."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    md = _ls_md(tmp_path)

    # candidate (dense_scheme) без артефакта → not_prepared
    monkeypatch.setattr(mi, "classify_image_block", lambda *a, **k: "dense_scheme")
    summary = await _run_enrich("wg3", "p", "left", md, run_model=False)
    ls = [it for it in summary.items if it.get("source") == "large_sheet_enrichment"]
    assert ls, "candidate без артефакта → not_prepared item"
    assert ls[0]["status"] == "large_sheet_not_prepared"
    assert ls[0]["usable_for_diff"] is False
    data = mi._read_image_descriptions("wg3", "p", "left")
    assert data["large_sheet_embedded"] is False  # not_prepared ≠ embedded

    # non-candidate (не dense, нет артефакта) → НЕТ large_sheet item
    monkeypatch.setattr(mi, "classify_image_block", lambda *a, **k: "photo_or_general")
    summary2 = await _run_enrich("wg3b", "p", "left", md, run_model=False)
    assert all(it.get("source") != "large_sheet_enrichment" for it in summary2.items)


@pytest.mark.asyncio
async def test_writegate_disabled_does_not_rewrite(monkeypatch, tmp_path):
    """4. gate disabled → старое поведение: existing MD НЕ переписывается,
    сводка не вставляется, даже если артефакт на диске."""
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", raising=False)
    sid, pid, side = "wg4", "p", "left"
    md = _ls_md(tmp_path)
    _write_ls_artifact(sid, pid, side, 24, circuits=5)
    md_out = sc_paths.text_enrichment_md_path(sid, pid, side)
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text("OLD ENRICHED\n", encoding="utf-8")

    await _run_enrich(sid, pid, side, md, run_model=False)

    assert md_out.read_text(encoding="utf-8") == "OLD ENRICHED\n"  # не тронут
    data = mi._read_image_descriptions(sid, pid, side)
    assert data["large_sheet_embedded"] is False
    assert data["enriched_md_written"] is False


@pytest.mark.asyncio
async def test_writegate_ordinary_block_unchanged(monkeypatch, tmp_path):
    """5. обычный image-блок (не large sheet): write-gate не срабатывает,
    pre-existing MD не переписывается в dry-run."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    monkeypatch.setattr(mi, "classify_image_block", lambda *a, **k: "photo_or_general")
    sid, pid, side = "wg5", "p", "left"
    md = _ls_md(tmp_path)  # НЕТ артефакта → ordinary
    md_out = sc_paths.text_enrichment_md_path(sid, pid, side)
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text("ORDINARY OLD\n", encoding="utf-8")

    summary = await _run_enrich(sid, pid, side, md, run_model=False)

    assert all(it.get("source") != "large_sheet_enrichment" for it in summary.items)
    assert md_out.read_text(encoding="utf-8") == "ORDINARY OLD\n"
    data = mi._read_image_descriptions(sid, pid, side)
    assert data["large_sheet_embedded"] is False
    assert data["enriched_md_written"] is False


@pytest.mark.asyncio
async def test_writegate_idempotent_repeat(monkeypatch, tmp_path):
    """6. повторный прогон с тем же артефактом идемпотентен: второй раз контент
    идентичен → enriched MD не переписывается."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    sid, pid, side = "wg6", "p", "left"
    md = _ls_md(tmp_path)
    _write_ls_artifact(sid, pid, side, 24, circuits=5)
    md_out = sc_paths.text_enrichment_md_path(sid, pid, side)

    await _run_enrich(sid, pid, side, md, run_model=False)  # run 1 (md отсутствовал)
    text1 = md_out.read_text(encoding="utf-8")
    assert "Large Sheet Enrichment" in text1
    assert mi._read_image_descriptions(sid, pid, side)["enriched_md_written"] is True

    await _run_enrich(sid, pid, side, md, run_model=False)  # run 2
    data2 = mi._read_image_descriptions(sid, pid, side)
    assert md_out.read_text(encoding="utf-8") == text1            # контент идентичен
    assert data2["large_sheet_embedded"] is True
    assert data2["enriched_md_written"] is False                  # ничего не переписали
    assert data2["enriched_md_write_reason"] is None


@pytest.mark.asyncio
async def test_writegate_descriptions_json_has_large_sheet_source(monkeypatch, tmp_path):
    """7. image_descriptions.json содержит item source=large_sheet_enrichment
    с done/large_sheet/page_enriched paths/diagnostics."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    sid, pid, side = "wg7", "p", "left"
    md = _ls_md(tmp_path)
    _write_ls_artifact(sid, pid, side, 24, circuits=5)

    await _run_enrich(sid, pid, side, md, run_model=False)

    data = mi._read_image_descriptions(sid, pid, side)
    ls = [it for it in data["items"] if it.get("source") == "large_sheet_enrichment"]
    assert ls
    it = ls[0]
    assert it["status"] == "done"
    assert it["large_sheet"] is True
    assert it["page_enriched_json_path"].endswith("page_enriched.json")
    assert it["page_enriched_md_path"].endswith("page_enriched.md")
    assert isinstance(it["diagnostics"], dict)


def test_atomic_write_text_replaces_and_leaves_no_tmp(tmp_path):
    """8a. _atomic_write_text заменяет содержимое и не оставляет .tmp."""
    p = tmp_path / "x.md"
    p.write_text("old", encoding="utf-8")
    mi._atomic_write_text(p, "new content")
    assert p.read_text(encoding="utf-8") == "new content"
    assert not p.with_suffix(p.suffix + ".tmp").exists()


@pytest.mark.asyncio
async def test_writegate_enriched_md_written_atomically(monkeypatch, tmp_path):
    """8b. enriched MD пишется атомарно: после записи нет висящего .tmp,
    файл целиком содержит сводку."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED", "true")
    sid, pid, side = "wg8", "p", "left"
    md = _ls_md(tmp_path)
    _write_ls_artifact(sid, pid, side, 24, circuits=5)
    md_out = sc_paths.text_enrichment_md_path(sid, pid, side)
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text("OLD\n", encoding="utf-8")

    await _run_enrich(sid, pid, side, md, run_model=False)

    assert not md_out.with_suffix(md_out.suffix + ".tmp").exists()
    text = md_out.read_text(encoding="utf-8")
    assert "Large Sheet Enrichment" in text and text.endswith("\n")


# ─── 9. large_sheet-only LLM max_tokens override (default-off) ───────────────


def test_cfg_llm_max_tokens_default_off(monkeypatch):
    """9a. env не задан → None (поведение остаётся 5500 из общего graphic-config)."""
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_LLM_MAX_TOKENS", raising=False)
    assert ls.cfg_llm_max_tokens() is None


def test_cfg_llm_max_tokens_set_and_clamped(monkeypatch):
    """9b. заданный env читается; ниже минимума клампится; мусор → None (fail-safe)."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_LLM_MAX_TOKENS", "9000")
    assert ls.cfg_llm_max_tokens() == 9000
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_LLM_MAX_TOKENS", "10")
    assert ls.cfg_llm_max_tokens() == 256
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_LLM_MAX_TOKENS", "garbage")
    assert ls.cfg_llm_max_tokens() is None


def _base_graphic_cfg(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", "https://test.example.com")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS", "5500")
    from backend.app.services.stage_comparison import graphic_llm_local as g
    return g.load_local_graphic_llm_config()


def test_apply_override_noop_when_unset(monkeypatch):
    """9c. env не задан → cfg возвращается как есть (max_tokens=5500)."""
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_LLM_MAX_TOKENS", raising=False)
    base = _base_graphic_cfg(monkeypatch)
    out = ls_jobs._apply_large_sheet_llm_overrides(base)
    assert out.max_tokens == 5500


def test_apply_override_replaces_only_max_tokens(monkeypatch):
    """9d. env=9000 → max_tokens поднят, исходный cfg не мутирован, остальные
    поля те же (override = только max_tokens)."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_LLM_MAX_TOKENS", "9000")
    base = _base_graphic_cfg(monkeypatch)
    out = ls_jobs._apply_large_sheet_llm_overrides(base)
    assert out.max_tokens == 9000
    assert base.max_tokens == 5500  # исходный объект не тронут
    assert out.max_continuations == base.max_continuations
    assert out.image_long_side == base.image_long_side
    assert out.model == base.model


# ─── 10. large_sheet MD max_circuits override (default-off) ──────────────────


def test_cfg_md_max_circuits_default_off(monkeypatch):
    """10a. env не задан → 12 (прежнее поведение)."""
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_MD_MAX_CIRCUITS", raising=False)
    assert ls.cfg_md_max_circuits() == 12


def test_cfg_md_max_circuits_set_and_garbage(monkeypatch):
    """10b. заданный env читается; мусор → default 12 (fail-safe)."""
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_MD_MAX_CIRCUITS", "80")
    assert ls.cfg_md_max_circuits() == 80
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_MD_MAX_CIRCUITS", "garbage")
    assert ls.cfg_md_max_circuits() == 12


def _pe_with_n_circuits(n: int) -> dict:
    return {
        "detection": {"sheet_kind": "electrical_single_line", "format_hint": "A1"},
        "title_block": {"doc_code": "X", "sheet": "1"},
        "circuits": [
            {"id": f"ВРУ-{i}", "breaker": f"QF{i}", "cable": f"ППГнг-HF 5х{i}мм²",
             "load_name": f"Потребитель {i}", "calculated_power_kw": i,
             "calculated_current_a": i * 2, "merge_method": "strong_id",
             "conflicts": []}
            for i in range(1, n + 1)
        ],
    }


def test_embed_summary_default_12_truncates():
    """10c. при max_circuits=12 таблица обрезана и есть строка «первые 12»/«ещё»."""
    pe = _pe_with_n_circuits(40)
    body = ls.build_large_sheet_embed_summary(pe, {}, max_circuits=12, max_chars=6000)
    assert "показаны первые 12" in body
    assert "ещё 28 цепей" in body
    assert "ВРУ-40" not in body  # 40-я цепь не попала


def test_embed_summary_80_embeds_all_no_truncation():
    """10d. при max_circuits=80 + масштабированном max_chars все цепи в MD,
    нет «ещё … цепей» и нет посимвольной обрезки."""
    n = 55
    pe = _pe_with_n_circuits(n)
    mc = 80
    body = ls.build_large_sheet_embed_summary(
        pe, {}, max_circuits=mc, max_chars=max(6000, mc * 150 + 3000))
    assert f"показаны первые {n}" in body  # shown == все n
    assert "ещё" not in body
    assert "обрезано до" not in body
    assert "ВРУ-55" in body  # последняя цепь попала


# ─── 11. Rich render (default-off) ──────────────────────────────────────────


def test_md_rich_render_disabled_by_default(monkeypatch):
    """11a. флаг по умолчанию OFF; включается env'ом."""
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_MD_RICH_RENDER_ENABLED", raising=False)
    assert ls.md_rich_render_enabled() is False
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_MD_RICH_RENDER_ENABLED", "true")
    assert ls.md_rich_render_enabled() is True


def test_cfg_md_max_notes_default_and_set(monkeypatch):
    """11b. notes-лимит default 5 (прежнее), env переопределяет, мусор → 5."""
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_MD_MAX_NOTES", raising=False)
    assert ls.cfg_md_max_notes() == 5
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_MD_MAX_NOTES", "80")
    assert ls.cfg_md_max_notes() == 80
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_MD_MAX_NOTES", "xx")
    assert ls.cfg_md_max_notes() == 5


def _rich_pe():
    """Синтетический page_enriched с полями, которые теряются в standard-рендере:
    breaker_params, conflicts, scheme_graph.nodes.mode_*, visible_text (ТТ/Меркурий/
    АУКРМ), >5 notes."""
    return {
        "detection": {"sheet_kind": "electrical_single_line", "format_hint": "A1"},
        "title_block": {"doc_code": "X", "sheet": "1.1"},
        "circuits": [
            {"id": "ГРЩ1-РП1-5", "load_name": "Автостоянка", "breaker": "1QF5",
             "breaker_params": "3P 320A 380В 40кА", "cable": "5х185мм²",
             "calculated_power_kw": 62.5, "calculated_current_a": 103.9, "phase": "3",
             "conflicts": [{"field": "calculated_current_a", "values": [103.9, 137.1]}]},
            {"id": "ГРЩ1-РП1-12", "load_name": "Чиллер", "breaker": "1QF12",
             "breaker_params": "3P 800A 720А", "cable": "5х50мм²",
             "calculated_power_kw": 335.0, "calculated_current_a": 676.8, "phase": "3",
             "conflicts": []},
        ],
        "scheme_graph": {"nodes": [
            {"id": "VPU1", "label": "ВРУ1", "type": "distribution_board",
             "parameters": {"mode_normal": {"power_kw": 449.3, "current_A": 717.3},
                            "mode_emergency": {"power_kw": 414.5, "current_A": 751.1}}},
            {"id": "VPU4", "label": "ВРУ4", "type": "distribution_board",
             "parameters": {"mode_normal": {"power_kw": 335.3, "current_A": 526.5}}},
        ]},
        "visible_text": [
            "Меркурий 234 ARTX2-01 (D)POBR", "2ТА4...2ТА6 3хТШП-0.66 40/5, 0.5S",
            "АУКРМ №1", "АУКРМ-1 Qp=200 кВАр", "Шинопровод 3L/PEN Al 3200А, L=6м",
            "УЗИП1 QF4", "РЕ (ГЗШ)", "посторонний текст без ключевых слов",
        ],
        "notes": [f"Примечание {i}" for i in range(1, 9)],  # 8 notes (>5)
    }


def test_rich_render_outputs_breaker_params_and_modes():
    """11c. rich выводит breaker_params, conflicts и режимы щитов (mode_normal/
    emergency) — то, что standard-рендер терял на md_render."""
    body = ls.build_large_sheet_rich_embed_summary(_rich_pe(), {}, max_circuits=80,
                                                    max_notes=80, max_chars=40000)
    assert "3P 320A 380В 40кА" in body            # breaker_params
    assert "B. Режимы щитов" in body              # секция B
    assert "449.3" in body and "717.3" in body    # mode_normal ВРУ1
    assert "751.1" in body                        # mode_emergency
    assert "335.3" in body and "526.5" in body    # ВРУ4 normal
    assert "137.1" in body                        # conflicts alt current


def test_rich_render_metering_and_core():
    """11d. rich выводит учёт (ТТ/Меркурий) и компенсацию/вводы (АУКРМ/шинопровод/
    УЗИП) из visible_text."""
    body = ls.build_large_sheet_rich_embed_summary(_rich_pe(), {})
    assert "Меркурий 234" in body
    assert "40/5" in body
    assert "АУКРМ" in body and "200 кВАр" in body
    assert "3200А" in body
    assert "УЗИП" in body
    assert "посторонний текст" not in body  # нерелевантное не тащим


def test_rich_render_notes_beyond_five():
    """11e. rich выводит больше 5 примечаний при max_notes>5 (standard капал на 5)."""
    body = ls.build_large_sheet_rich_embed_summary(_rich_pe(), {}, max_notes=80)
    assert "Примечание 8" in body  # 8-е примечание попало
    # standard-рендер с notes[:5] восьмое не показал бы
    std = ls.build_large_sheet_embed_summary(_rich_pe(), {})
    assert "Примечание 8" not in std


def test_rich_render_respects_max_circuits():
    """11f. rich уважает max_circuits (не ломает фичу max_circuits)."""
    body = ls.build_large_sheet_rich_embed_summary(_rich_pe(), {}, max_circuits=1)
    assert "ГРЩ1-РП1-5" in body
    assert "ГРЩ1-РП1-12" not in body  # вторая цепь обрезана
    assert "ещё 1 цепей" in body
