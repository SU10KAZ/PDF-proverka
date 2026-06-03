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
