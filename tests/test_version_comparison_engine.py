"""Ядро «Сравнения версий» на синтетических листах (план §10.1).

Номера в именах тестов — номера сценариев из плана.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path

import cv2
import fitz
import numpy as np
import pytest

from backend.app.services.version_comparison import engine, paths, profile, settings, status as st
from tests import version_comparison_fixtures as fx

OBJ = "obj-test"


@pytest.fixture
def vc_root(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.setenv("CPU_POOL_WORKERS", "1")
    monkeypatch.delenv("VERSION_COMPARISON_DPI", raising=False)
    monkeypatch.delenv("VERSION_COMPARISON_TOL_PT", raising=False)
    return tmp_path


def _source(tmp: Path, name: str, spec: dict) -> tuple[str, dict]:
    pdf = fx.make_pdf(tmp / f"{name}.pdf", [spec])
    sha = hashlib.sha256(pdf.read_bytes()).hexdigest()
    dst = paths.source_pdf(OBJ, sha)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.copyfile(pdf, dst)
        os.chmod(dst, 0o444)
    return sha, profile.build(OBJ, sha)["pages"][0]


def compare(tmp: Path, spec_a: dict, spec_b: dict, *, reasons=None, tag="pair", before_compute=None) -> dict:
    sha_a, rec_a = _source(tmp, f"{tag}_a", spec_a)
    sha_b, rec_b = _source(tmp, f"{tag}_b", spec_b)
    task = {
        "sheet_pair_id": tag, "out_dir": str(tmp / "out" / tag), "reasons": reasons or [],
        "params": {"dpi": settings.dpi(), "tol_pt": settings.tolerance_pt()},
        "a": {"version_id": "va", "sha": sha_a, "page": 1, "pdf": str(paths.source_pdf(OBJ, sha_a)),
              "profile_dir": str(paths.profile_dir(OBJ, sha_a)), "profile": rec_a},
        "b": {"version_id": "vb", "sha": sha_b, "page": 1, "pdf": str(paths.source_pdf(OBJ, sha_b)),
              "profile_dir": str(paths.profile_dir(OBJ, sha_b)), "profile": rec_b},
    }
    if before_compute:
        before_compute(task)
    engine.compute_sheet_pair(task)
    return json.loads((tmp / "out" / tag / "result.json").read_text(encoding="utf-8"))


def base(**kw) -> dict:
    return fx.sheet("4", "Схема ВРУ-1", marker="альфа бета гамма дельта эпсилон", **kw)


def classes(result: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in result["regions"]:
        out[r["class"]] = out.get(r["class"], 0) + 1
    return out


# --------------------------------------------------------------- совпадение

def test_01a_identical_content_fast_path(vc_root):
    r = compare(vc_root, base(), base())
    assert r["identical_content"] is True
    assert r["status"] == "same"
    assert all(c["state"] in ("checked", "not_applicable") for c in r["channels"].values())


def test_01b_same_drawing_rewrapped_goes_full_path(vc_root):
    r = compare(vc_root, base(), base(transform={"dx": 0.0, "dy": 0.0}))
    assert r["identical_content"] is False
    assert r["status"] == "same", r["reasons"]
    assert r["regions"] == []


def test_02_shift_is_aligned(vc_root):
    r = compare(vc_root, base(), base(transform={"dx": 10.0, "dy": -7.0}))
    tr = r["transform"]
    assert tr["reliable"] and tr["method"] == "anchors"
    assert abs(tr["dx"] + 10.0) < 0.2 and abs(tr["dy"] - 7.0) < 0.2
    assert r["channels"]["geometry"]["diffs"] == 0, classes(r)
    assert r["status"] == "same"


def test_03_rotation_and_scale_are_aligned_without_warping(vc_root):
    r = compare(vc_root, base(), base(transform={"rot": 0.5, "scale": 1.02}))
    tr = r["transform"]
    assert tr["reliable"]
    assert abs(tr["rot_deg"]) > 0.3 and abs(tr["scale"] - 1.0) > 0.005
    assert r["channels"]["text"]["diffs"] == 0
    assert r["channels"]["geometry"]["diffs"] == 0, classes(r)


def test_04_page_rotate_attribute_is_normalized(vc_root):
    spec_b = base(rotate_attr=90)
    doc = fitz.open()
    fx.build_page(doc, spec_b)
    assert doc[0].rotation == 90 and abs(doc[0].rect.width - fx.W) < 1
    r = compare(vc_root, base(), spec_b)
    assert r["transform"]["reliable"]
    assert abs(r["transform"]["rot_deg"]) < 0.2
    assert r["channels"]["geometry"]["diffs"] == 0, classes(r)


# ------------------------------------------------------------- отличия

def test_05_one_digit_changed_is_text_region(vc_root):
    r = compare(vc_root, base(), base(values={"pp": "Рр=324.26кВт"}))
    assert r["status"] == "different"
    assert [w["text"] for w in r["text"]["only_a"]] == ["Рр=323.55кВт"]
    assert [w["text"] for w in r["text"]["only_b"]] == ["Рр=324.26кВт"]
    region = next(x for x in r["regions"] if x["class"] == "text")
    assert "Рр=323.55кВт" in region["text_before"] and "Рр=324.26кВт" in region["text_after"]
    # «Было → стало» с координатами обеих страниц и разбором чисел (план §5 шаг 5).
    changes = r["text"]["changes"]
    assert len(changes) == 1 and r["channels"]["text"]["diffs"] == 1
    ch = changes[0]
    assert (ch["kind"], ch["before"], ch["after"], ch["norm"]) == ("changed", "Рр=323.55кВт", "Рр=324.26кВт", None)
    assert ch["numbers"] == [{"before": "323.55", "after": "324.26"}]
    assert ch["a_bbox"] and ch["b_bbox"] and ch["id"] in region["text_changes"]


def test_text_change_on_shifted_page_has_b_page_coordinates(vc_root):
    r = compare(vc_root, base(), base(values={"pp": "Рр=324.26кВт"}, transform={"dx": 10.0, "dy": -7.0}))
    ch = next(c for c in r["text"]["changes"] if c["kind"] == "changed")
    # B сдвинут на (+10; −7): в координатах A слово на месте, на странице B — сдвинуто.
    assert abs(ch["b_in_a_bbox"][0] - ch["a_bbox"][0]) < 1.0
    assert abs((ch["b_bbox"][0] - ch["a_bbox"][0]) - 10.0) < 1.0 and abs((ch["b_bbox"][1] - ch["a_bbox"][1]) + 7.0) < 1.0


def test_text_classes_moved_phrase_and_normalization():
    from backend.app.services.version_comparison import textdiff

    def w(text, x, y, h=8.0):
        return {"text": text, "pt": [x, y, x + 5.0 * len(text), y + h]}
    # Фраза из двух слов заменена целиком — одно изменение, слова склеены по строке.
    ch = textdiff.build([w("Iр=538.17", 100, 100), w("А", 150, 100)], [w("Iр=540.02", 100, 100), w("А,", 150, 100)], [])
    assert [(c["kind"], c["before"], c["after"]) for c in ch] == [("changed", "Iр=538.17 А", "Iр=540.02 А,")]
    # Разные надписи одной строки, не перекрывающиеся по месту, — раздельные изменения.
    ch = textdiff.build([w("ЩР1", 100, 100)], [w("ЩР2", 100, 100), w("новое", 300, 100)], [])
    assert sorted((c["kind"], c["after"]) for c in ch) == [("added", "новое"), ("changed", "ЩР2")]
    # Нормализация классифицирует, но не скрывает.
    assert textdiff.classify("ППГнг(A)-HF", "ППГнг(А)-HF") == "symbol"      # латинская A → кириллическая А
    assert textdiff.classify("3,5", "3.5") == "number_format"
    assert textdiff.classify("Рр = 1", "Рр=1") == "spaces"
    assert textdiff.classify("3х1.5", "3х2.5") is None
    sp = textdiff.build([w("Рр", 100, 100), w("=", 112, 100), w("1", 120, 100)], [w("Рр=1", 100, 100)], [])
    assert sp[0]["norm"] == "spaces" and textdiff.significant(sp) == 0
    # Сдвиг надписи: слова строки с общим вектором — одна фраза с вектором.
    mv = [{"text": "Кабель", "a_pt": [100, 200, 130, 208], "b_in_a_pt": [100, 212, 130, 220], "shift_pt": 12.0},
          {"text": "силовой", "a_pt": [133, 200, 168, 208], "b_in_a_pt": [133, 212, 168, 220], "shift_pt": 12.0}]
    ch = textdiff.build([], [], mv)
    assert [(c["kind"], c["before"], c["shift_pt"]) for c in ch] == [("moved", "Кабель силовой", [0.0, 12.0])]
    assert textdiff.significant(ch) == 0
    assert textdiff.numbers("Рр=323,55 Iр=1", "Рр=323.55 Iр=2") == [{"before": "1", "after": "2"}]


def test_06_line_added_and_removed(vc_root):
    added = compare(vc_root, base(), base(content={"extra_line": True}), tag="add")
    assert classes(added).get("added") == 1 and added["channels"]["text"]["diffs"] == 0
    removed = compare(vc_root, base(), base(content={"remove_line": True}), tag="rm")
    assert classes(removed).get("removed", 0) >= 1 and removed["channels"]["text"]["diffs"] == 0


# ----------------------------------------------------- служебные объекты

def test_10_warning_overlay_is_separated_without_false_blue(vc_root):
    r = compare(vc_root, base(warning=True), base())
    assert [s["kind"] for s in r["separated"]["a"]] == ["warning"]
    assert r["channels"]["geometry"]["diffs"] == 0 and r["channels"]["color"]["diffs"] == 0
    assert r["channels"]["warnings"]["state"] == "checked" and r["channels"]["warnings"]["diffs"] == 1
    assert r["status"] == "same_zones_changed"
    assert any(o["kind"] == "warning" and o["change"] == "removed" for o in r["overlays"])
    assert r["channels"]["warnings"]["note"] == "исчезновение предупреждения не подтверждает согласования"


def test_11_freetext_annotation_does_not_leak_into_text(vc_root):
    note = {"type": "freetext", "rect": (100 * fx.MM, 190 * fx.MM, 180 * fx.MM, 205 * fx.MM),
            "text": "монтаж только после согласования"}
    r = compare(vc_root, base(annots=[note]), base())
    assert r["channels"]["text"]["diffs"] == 0
    assert len(r["annotations"]["removed"]) == 1
    assert r["status"] == "same_zones_changed"


def test_12_release_stamp_date_and_qr_are_service_zones(vc_root):
    r = compare(vc_root, base(release={"date": "09.07.2025", "qr_seed": 1}),
                base(release={"date": "04.08.2025", "qr_seed": 2}))
    kinds_a = sorted(s["kind"] for s in r["separated"]["a"])
    assert kinds_a == ["qr", "release_stamp"]
    rel = next(o for o in r["overlays"] if o["kind"] == "release_stamp")
    assert rel["change"] == "changed" and "09.07.2025" in rel["before"][0] and "04.08.2025" in rel["after"][0]
    assert r["channels"]["geometry"]["diffs"] == 0
    assert r["status"] == "same_zones_changed"
    # Поля служебных зон (план §5 шаг 6): дата выпуска и содержимое QR «было → стало».
    fields = {f["field"]: f for f in r["service"]}
    assert (fields["date"]["before"], fields["date"]["after"]) == ("09.07.2025", "04.08.2025")
    assert (fields["qr"]["before"], fields["qr"]["after"]) == ("https://qr.example.test/d/0001",
                                                               "https://qr.example.test/d/0002")
    assert "izm_row" not in fields and "title" not in fields


def test_stamp_fields_and_change_table_rows(vc_root):
    from backend.app.services.version_comparison import service
    rec_a = {"stamp": {"designation": "133/23-ГК-ЭМ2", "title": "План 1 этажа", "sheet_no": "19", "izm_max": 6},
             "izm_rows": [{"izm": 6, "text": "6 зам. - 1000/26 04.2026"}]}
    rec_b = {"stamp": {"designation": "133/23-ГК-ЭМ2", "title": "План 1 этажа", "sheet_no": "20", "izm_max": 7},
             "izm_rows": [{"izm": 7, "text": "7 зам. - 2010/26 06.2026"}, {"izm": 6, "text": "6 зам. - 1000/26 04.2026"}]}
    doc = fitz.open()
    page = doc.new_page()
    out = service.compare(rec_a, rec_b, doc, page, doc, page, "", "")
    got = [(f["field"], f["change"], f["before"], f["after"]) for f in out]
    assert got == [("sheet_no", "changed", "19", "20"), ("izm_max", "changed", "6", "7"),
                   ("izm_row", "added", "", "7 зам. - 2010/26 06.2026")]


def test_16_text_as_curves_needs_review(vc_root):
    r = compare(vc_root, base(as_curves=True), base(as_curves=True, values={"label": "ЩР7"}))
    assert r["status"] == "needs_review"
    assert {"NO_TEXT_LAYER", "TEXT_UNREADABLE_IN_DIFF_AREA"} & set(r["reasons"])


def test_16b_added_curve_text_on_text_page_is_unreadable(vc_root):
    # На листе есть текстовый слой, а новая надпись нарисована кривыми (SHX).
    r = compare(vc_root, base(), base(curve_text=[{"text": "ЩР-7 Ввод 2", "at": (153, 118)}]))
    assert r["status"] == "needs_review"
    assert "TEXT_UNREADABLE_IN_DIFF_AREA" in r["reasons"]
    assert any(g["text_unreadable"] for g in r["regions"])


def test_16c_cloud_ticks_and_filled_marks_are_not_text(vc_root):
    # Дуги облака (шире двух высот), ряд прямых штрихов и залитые квадраты
    # похожи на строку знаков, но надписью не являются: отличия есть, текст не нужен.
    r = compare(vc_root, base(), base(content={"cloud": True, "ticks": True, "squares": True}))
    assert "TEXT_UNREADABLE_IN_DIFF_AREA" not in r["reasons"], [g for g in r["regions"] if g["text_unreadable"]]
    assert r["status"] == "different"
    assert classes(r).get("added", 0) >= 3


def test_16d_text_reaching_into_stamp_zone_is_readable(vc_root):
    # Центр слова в зоне штампа (слово исключено из сравнения основной области),
    # верх букв — над её границей: это отличие с текстовым слоем, а не «кривые».
    r = compare(vc_root, base(), base(stamp_note="1234567"))
    assert "TEXT_UNREADABLE_IN_DIFF_AREA" not in r["reasons"], [g for g in r["regions"] if g["text_unreadable"]]
    assert not any(g["text_unreadable"] for g in r["regions"])


def test_scan_page_is_marked(vc_root):
    # Лист-картинка — скан; полупрозрачное предупреждение поверх вектора — нет.
    assert _source(vc_root, "scan", base(as_scan=True))[1]["scan"] is True
    assert _source(vc_root, "vec", base())[1]["scan"] is False
    assert _source(vc_root, "warn", base(warning=True))[1]["scan"] is False
    assert _source(vc_root, "ocr", base(as_scan="ocr"))[1]["scan"] is True, "невидимый OCR-слой — всё равно скан"
    r = compare(vc_root, base(as_scan=True), base(as_scan=True, values={"label": "ЩР7"}), tag="scan_pair")
    assert r["a"]["scan"] and r["b"]["scan"]
    assert r["status"] == "needs_review" and "NO_TEXT_LAYER" in r["reasons"]


def test_18_render_failure_is_never_same(vc_root, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("сбой рендера (тест)")
    monkeypatch.setattr(engine, "_render_into", boom)
    r = compare(vc_root, base(), base(transform={"dx": 0.0, "dy": 0.0}))
    assert r["status"] == "needs_review" and "RENDER_FAILED" in r["reasons"]


# ------------------------------------- пропуск мелких и цветовых изменений

def test_21_low_res_similarity_never_confirms_identity(vc_root):
    def same_mask(task):
        a_mask = Path(task["a"]["profile_dir"]) / "mask36" / "p0001.png"
        b_mask = Path(task["b"]["profile_dir"]) / "mask36" / "p0001.png"
        shutil.copyfile(a_mask, b_mask)  # признак сопоставления 36 dpi — одинаковый
    r = compare(vc_root, base(), base(values={"small": "1.6"}), before_compute=same_mask)
    assert r["status"] == "different"


def test_22_isolated_dot_is_a_region_not_noise(vc_root):
    r = compare(vc_root, base(), base(content={"dot": True}))
    assert classes(r).get("added") == 1


def test_23_lost_decimal_point_in_small_font(vc_root):
    r = compare(vc_root, base(), base(values={"small": "15"}))
    assert r["status"] == "different"
    assert {w["text"] for w in r["text"]["only_a"]} == {"1.5"}


def test_24_one_letter_in_small_label(vc_root):
    r = compare(vc_root, base(), base(values={"label": "ЩР7"}))
    assert {w["text"] for w in r["text"]["only_b"]} == {"ЩР7"}
    assert classes(r).get("text", 0) >= 1


def test_25_thin_line_removed(vc_root):
    r = compare(vc_root, base(), base(content={"thin_line": False}))
    assert classes(r).get("removed", 0) >= 1


def test_26_short_tick_near_existing_line(vc_root):
    r = compare(vc_root, base(), base(content={"tick": True}))
    assert classes(r).get("added", 0) >= 1


def test_27_thickness_change_within_tolerance_is_reported(vc_root):
    r = compare(vc_root, base(), base(content={"line_width": 0.7}))
    assert r["status"] == "same"
    assert r["within_tolerance"]["components"] > 0


def test_28_color_change_same_geometry(vc_root):
    r = compare(vc_root, base(content={"line_color": (1, 0, 0)}), base(content={"line_color": (0, 0, 1)}))
    assert r["channels"]["geometry"]["diffs"] == 0, classes(r)
    assert r["channels"]["color"]["diffs"] >= 1
    # Перекраска — именно «цвет», не «оформление»: прежнего цвета рядом в B нет.
    assert classes(r).get("color", 0) >= 1 and not classes(r).get("style"), classes(r)
    assert r["status"] == "different"


def test_word_kopiya_in_sheet_list_is_not_a_warning():
    from backend.app.services.version_comparison import engine

    def row(y, texts, h=6.0):
        x, out = 50.0, []
        for t_ in texts:
            out.append({"text": t_, "pt": [x, y, x + 6 * len(t_), y + h]})
            x += 6 * len(t_) + 4
        return out
    body = row(100, "20 План Павильона №2. Распределительные сети копия 1 промышленных коммуникаций".split())
    body += row(120, "Общие указания по монтажу электрооборудования".split())
    assert engine._warning_lines(body) == []
    stamp = body + row(300, ["НЕ", "СОГЛАСОВАНО"], h=30.0)
    assert [w["text"] for w in engine._warning_lines(stamp)] == ["НЕ СОГЛАСОВАНО"]


def test_partial_alignment_is_needs_review(vc_root):
    # Надписи те же (опорные слова сошлись), а рисунок другой: совмещена только
    # часть листа — «Нужна проверка», а не просто «Есть отличия».
    words = "альфа бета гамма дельта эпсилон дзета эта тета йота каппа лямбда мю ню кси"
    r = compare(vc_root, fx.sheet("4", "План", unique=1, marker=words), fx.sheet("4", "План", unique=2, marker=words))
    tr = r["transform"]
    assert tr["method"] == "anchors" and tr["reliable"] and tr["dice36"] < settings.ALIGN_MIN_DICE, tr
    assert "ALIGNMENT_PARTIAL" in r["reasons"] and r["status"] == "needs_review"


def test_changed_word_is_one_region(vc_root):
    # Две разнесённые цифры одного слова — одно отличие, а не россыпь кусков.
    r = compare(vc_root, base(), base(values={"pp": "Рр=353.25кВт"}))
    hits = [g for g in r["regions"] if "Рр=353.25кВт" in g["text_after"]]
    assert len(hits) == 1 and hits[0]["text_before"] == ["Рр=323.55кВт"], r["regions"]
    assert len(r["regions"]) == 1, classes(r)


def test_13_moved_table_is_block_moved_not_removed_and_added(vc_root):
    r = compare(vc_root, base(content={"table": {}}), base(content={"table": {"dx": 60 * fx.MM}}))
    blocks_ = [g for g in r["regions"] if g["class"] == "block_moved"]
    assert len(blocks_) == 1, classes(r)
    assert abs(blocks_[0]["shift_pt"][0] - 60 * fx.MM) < 1.0 and abs(blocks_[0]["shift_pt"][1]) < 1.0
    assert not (classes(r).get("removed") or classes(r).get("added")), classes(r)
    assert r["text"]["only_a"] == [] and r["text"]["only_b"] == []
    assert {c["kind"] for c in r["text"]["changes"]} == {"moved"}
    # Перенос блока целиком — не изменение (решение 15.10).
    assert r["status"] == "same", r["reasons"]
    assert r["channels"]["geometry"]["diffs"] == 0 and r["channels"]["geometry"]["shifts"] >= 1


def test_shifted_label_is_not_a_change(vc_root):
    # Та же надпись на 3 pt ниже — сдвиг, не изменение (решение 15.10).
    r = compare(vc_root, base(), base(content={"cable_text_dy": 3.0}))
    assert classes(r).get("text_moved", 0) >= 1, classes(r)
    assert r["status"] == "same", (r["reasons"], classes(r))
    assert r["channels"]["geometry"]["diffs"] == 0 and r["channels"]["text"]["diffs"] == 0


def test_symbol_shifted_with_its_label_is_not_a_change(vc_root):
    # Знак легенды переехал вместе с подписью (ЭМ2 (6)→(7), стр. 24→25) — это сдвиг.
    r = compare(vc_root, base(content={"legend": True}), base(content={"legend": True, "cable_text_dy": 3.0}))
    assert r["status"] == "same", (r["reasons"], classes(r))
    assert "TEXT_UNREADABLE_IN_DIFF_AREA" not in r["reasons"]


def test_line_next_to_shifted_label_is_still_a_change(vc_root):
    # Сдвиг надписи не прячет новую линию в той же области.
    r = compare(vc_root, base(), base(content={"cable_text_dy": 3.0, "underline": True}))
    assert r["status"] == "different", classes(r)
    assert classes(r).get("added", 0) + classes(r).get("changed", 0) >= 1, classes(r)


def test_13b_moved_table_with_change_inside(vc_root):
    r = compare(vc_root, base(content={"table": {}}), base(content={"table": {"dx": 60 * fx.MM, "p2": "9,0"}}))
    assert classes(r).get("block_moved") == 1, classes(r)
    changed = [c for c in r["text"]["changes"] if c["kind"] == "changed"]
    assert [(c["before"], c["after"]) for c in changed] == [("7,5", "9,0")], r["text"]["changes"]
    assert classes(r).get("text", 0) >= 1, "изменение внутри блока — отдельный регион"
    assert r["status"] == "different"


def test_13c_moved_table_with_change_and_recolored_line(vc_root):
    # Блок с изменением внутри и перекрашенная линия на том же листе: цветовой
    # регион проверяется после разбора блока (регрессия: имя переменной допуска).
    r = compare(vc_root, base(content={"table": {}, "line_color": (1, 0, 0)}),
                base(content={"table": {"dx": 60 * fx.MM, "p2": "9,0"}, "line_color": (0, 0, 1)}))
    assert classes(r).get("block_moved") == 1 and classes(r).get("color", 0) >= 1, classes(r)


def test_line_weight_change_is_style_not_removal(vc_root):
    # Дубль подложки толстым пером убран: линия осталась, изменилась толщина.
    r = compare(vc_root, base(content={"underlay": "dup"}), base(content={"underlay": "thin"}))
    c = classes(r)
    assert c.get("style", 0) >= 1 and not (c.get("removed") or c.get("added") or c.get("light")), c
    assert r["channels"]["geometry"]["diffs"] == 0 and r["channels"]["color"]["style"] >= 1
    assert r["status"] == "different", "оформление — тоже отличие, не «совпадает»"


def test_dark_line_removed_over_light_fill_is_removal(vc_root):
    # Светлая заливка — не «тонкая линия»: тёмная линия поверх неё удалена.
    r = compare(vc_root, base(content={"fill_line": True}), base())
    assert classes(r).get("removed", 0) >= 1 and not classes(r).get("style"), classes(r)


def test_crossing_order_change_is_style(vc_root):
    r = compare(vc_root, base(content={"crossing": "gray_top"}), base(content={"crossing": "blue_top"}))
    c = classes(r)
    assert c.get("style", 0) >= 1 and not c.get("color"), c
    assert r["channels"]["geometry"]["diffs"] == 0


def test_29_light_fill_change_is_not_lost(vc_root):
    r = compare(vc_root, base(), base(content={"light_fill": False}))
    assert classes(r).get("light", 0) >= 1
    assert r["status"] == "different"


def test_30_unchecked_channel_is_never_same():
    channels = {name: st.checked(0) for name in st.CHANNELS}
    channels["annotations"] = st.unchecked("тест")
    status, reasons = st.decide(channels, [])
    assert status == "needs_review" and "UNCHECKED_CHANNEL:annotations" in reasons
    partial = {name: st.checked(0) for name in st.CHANNELS if name != "color"}
    status, reasons = st.decide(partial, [])
    assert status == "needs_review" and "UNCHECKED_CHANNEL:color" in reasons


def test_31a_change_under_separable_warning_is_found(vc_root):
    r = compare(vc_root, base(), base(warning=True, values={"pp": "Рр=324.26кВт"}))
    assert [s["kind"] for s in r["separated"]["b"]] == ["warning"]
    assert r["status"] == "different"
    assert {w["text"] for w in r["text"]["only_b"]} == {"Рр=324.26кВт"}
    assert r["channels"]["warnings"]["note"] == "предупреждение появилось в B"


def test_31b_baked_warning_is_partial_coverage(vc_root):
    r = compare(vc_root, base(), base(content={"baked_warning": True}))
    assert r["channels"]["warnings"]["state"] == "unchecked"
    assert "PARTIAL_COVERAGE" in r["reasons"] and r["status"] == "needs_review"


def test_overlay_masks_and_preview_are_written(vc_root):
    r = compare(vc_root, base(), base(content={"extra_line": True}))
    out = vc_root / "out" / "pair"
    z = np.load(out / "masks.npz")
    assert set(engine.LAYERS) <= set(z.files)
    assert cv2.imread(str(out / "preview.png")) is not None
    region = r["regions"][0]
    assert len(region["bbox_a_pt"]) == 4 and len(region["bbox_b_pt"]) == 4


def test_registration_prefers_identity_when_only_a_moved_table_matches():
    # Все опорные слова — в таблице, которую перенесли; рамка и чертёж на месте
    # (ЭМ2 (6) стр. 6 → (7) стр. 7). Совмещение по словам сдвинуло бы лист.
    from backend.app.services.version_comparison import registration
    w, h = 400, 300                       # pt; маски 36 dpi = 0,5 px/pt
    def mask(table_dx, table_dy):
        m = np.zeros((h // 2, w // 2), np.uint8)
        m[5, 5:195] = m[145, 5:195] = 1   # рамка
        m[5:146, 5] = m[5:146, 195] = 1
        rng = np.random.default_rng(7)    # чертёж на месте — основная часть рисунка, без периода
        for _ in range(25):
            x, y, ln = int(rng.integers(8, 150)), int(rng.integers(50, 140)), int(rng.integers(15, 40))
            m[y, x:x + ln] = 1
            m[y:min(144, y + ln // 2), x] = 1
        x0, y0 = 10 + table_dx // 2, 10 + table_dy // 2
        for k in range(0, 31, 6):         # таблица — сетка
            m[y0 + k, x0:x0 + 40] = 1
        for k in range(0, 41, 20):
            m[y0:y0 + 31, x0 + k] = 1
        return m
    words_a = [[20 + 12 * i, 20, 28 + 12 * i, 26, f"слово{i}"] for i in range(12)]
    words_b = [[x0 + 150, y0 + 80, x1 + 150, y1 + 80, t] for x0, y0, x1, y1, t in words_a]
    reg = registration.register(words_a, words_b, [], [], mask(0, 0), mask(150, 80), 36)
    assert reg["identity"] is True and reg["method"] == "identity", reg
    assert reg["dice36"] >= 0.5
