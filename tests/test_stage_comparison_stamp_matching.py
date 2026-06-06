"""Тесты stamp/sheet-name матчинга страниц для сравнения стадий.

Покрывает:
  * normalize_sheet_name;
  * build_sheet_index (forward-fill продолжений, text_layer фолбэк);
  * match_sheet_indexes (глобальный матч distinctive-имени через большой сдвиг,
    in-order дубликаты, margin-подавление неоднозначных, валидность items);
  * store.suggest_alignment_by_stamp (обвязка чтения MD).
"""
from __future__ import annotations

from backend.app.services.stage_comparison import alignment as alignment_mod
from backend.app.services.stage_comparison import stamp_matching as sm


def _md(pages: list[tuple[int, object, object]]) -> str:
    """pages: список (page_no, sheet_no|None, sheet_name|None) → строка MD."""
    out: list[str] = []
    for pn, sno, snm in pages:
        out.append(f"## СТРАНИЦА {pn}")
        if sno is not None:
            out.append(f"**Лист:** {sno}")
        if snm is not None:
            out.append(f"**Наименование листа:** {snm}")
        out.append(f"какой-то текст страницы {pn}")
        out.append("")
    return "\n".join(out)


# ─── normalize_sheet_name ──────────────────────────────────────────────────

def test_normalize_basic():
    assert sm.normalize_sheet_name("ГРЩ-0,4кВ") == sm.normalize_sheet_name("грщ 0 4кв")
    assert sm.normalize_sheet_name("Лист 1 (из 17) Текстовая часть") == \
        sm.normalize_sheet_name("Текстовая часть")
    assert sm.normalize_sheet_name("") == ""
    assert sm.normalize_sheet_name("Ёлка") == sm.normalize_sheet_name("елка")


# ─── build_sheet_index ─────────────────────────────────────────────────────

def test_forward_fill_continuation_pages():
    md = _md([
        (1, "1 (из 3)", "Текстовая часть"),
        (2, "2", None),   # продолжение → наследует имя
        (3, "3", None),   # продолжение → наследует имя
        (4, "1 (из 1)", "Содержание тома"),
    ])
    idx = sm.build_sheet_index(md)
    by_page = {r.page: r for r in idx}
    assert by_page[2].norm_name == by_page[1].norm_name
    assert by_page[2].name_source == "inherited"
    assert by_page[3].norm_name == by_page[1].norm_name
    assert by_page[4].name_source == "md"
    assert by_page[4].norm_name != by_page[1].norm_name


def test_text_layer_fallback_for_unnamed_page():
    md = _md([(1, None, None)])  # ни имени, ни № листа
    idx = sm.build_sheet_index(md, extra_text_by_page={1: "Однолинейная схема ВРУ"})
    assert idx[0].name_source == "text_layer"
    assert idx[0].norm_name == sm.normalize_sheet_name("Однолинейная схема ВРУ")


# ─── match: distinctive имя через большой сдвиг страниц ─────────────────────

def test_distinctive_name_matches_across_large_offset():
    """Золотой кейс: схема ГРЩ уехала с p2 на p40 — должна найтись по имени."""
    left = _md([
        (1, "1", "Содержание тома"),
        (2, "2", "Однолинейная расчетная схема ГРЩ-0,4кВ"),
    ])
    right_pages = [(1, "1", "Содержание тома")]
    # 38 страниц-наполнителей с уникальными generic-именами
    for p in range(2, 40):
        right_pages.append((p, str(p), f"Наполнитель раздел {p}"))
    right_pages.append((40, "40", "Однолинейная расчетная схема ГРЩ-0,4кВ"))
    right = _md(right_pages)

    res = sm.match_sheet_indexes(sm.build_sheet_index(left),
                                 sm.build_sheet_index(right))
    matched = {(it["left_page"], it["right_page"]) for it in res["suggested_items"]
               if it["match"]}
    assert (1, 1) in matched          # содержание
    assert (2, 40) in matched         # схема ГРЩ найдена через сдвиг 38 страниц
    grsh = next(it for it in res["suggested_items"]
                if it["match"] and it["left_page"] == 2)
    assert grsh["match_type"] == "exact_name"
    assert grsh["score"] == 1.0


def test_inorder_duplicate_names_align_sequentially():
    left = _md([(1, "1", "Текстовая часть"), (2, "2", "Текстовая часть"),
                (3, "3", "Текстовая часть")])
    right = _md([(1, "1", "Текстовая часть"), (2, "2", "Текстовая часть")])
    res = sm.match_sheet_indexes(sm.build_sheet_index(left),
                                 sm.build_sheet_index(right))
    matched = {(it["left_page"], it["right_page"]) for it in res["suggested_items"]
               if it["match"]}
    assert matched == {(1, 1), (2, 2)}
    # третий левый лист без пары → left_only, не выдуман
    lo = [it for it in res["suggested_items"] if it["match_type"] == "left_only"]
    assert any(it["left_page"] == 3 for it in lo)


def test_ambiguous_generic_names_are_suppressed():
    """Имена с общим бойлерплейтом и без distinctive-хвоста НЕ матчатся
    наугад — margin-гейт оставляет их на ручной матч."""
    left = _md([(1, "1", "Общие данные система")])
    right = _md([
        (1, "1", "Общие данные система отопления"),
        (2, "2", "Общие данные система вентиляции"),
    ])
    res = sm.match_sheet_indexes(sm.build_sheet_index(left),
                                 sm.build_sheet_index(right))
    # левый лист неоднозначен между двумя похожими → НЕ предлагаем
    assert res["matched_count"] == 0
    assert any(it["left_page"] == 1 and it["match_type"] == "left_only"
               for it in res["suggested_items"])


def test_positive_fuzzy_with_clear_winner():
    left = _md([(1, "1", "Схема ВРУ ИТП расчетная")])
    right = _md([
        (1, "1", "Содержание тома"),
        (2, "2", "Схема ВРУ ИТП расчетная однолинейная"),
    ])
    res = sm.match_sheet_indexes(sm.build_sheet_index(left),
                                 sm.build_sheet_index(right))
    matched = {(it["left_page"], it["right_page"]) for it in res["suggested_items"]
               if it["match"]}
    assert (1, 2) in matched
    it = next(it for it in res["suggested_items"]
              if it["match"] and it["left_page"] == 1)
    # Богатый scoring помечает feature-backed fuzzy как fuzzy_structural
    # (общий вид «схема» + система «вру»); чистое имя → fuzzy_name.
    assert it["match_type"] in ("fuzzy_name", "fuzzy_structural")
    assert it["needs_review"] is True


# ─── items годятся для alignment.validate ──────────────────────────────────

def test_suggested_items_validate_cleanly():
    left = _md([(1, "1", "A"), (2, "2", "B"), (3, "3", "C")])
    right = _md([(1, "1", "B"), (2, "2", "C"), (3, "3", "D")])
    res = sm.match_sheet_indexes(sm.build_sheet_index(left),
                                 sm.build_sheet_index(right))
    normalized, errors = alignment_mod.validate(
        res["suggested_items"], res["left_page_count"], res["right_page_count"])
    assert errors == []   # ни одна страница не использована дважды
    # validate оставляет только канонические поля
    assert all(set(it.keys()) == {"slot", "left_page", "right_page", "mode", "note"}
               for it in normalized)


def test_empty_side_no_matches():
    # build_sheet_index("") отдаёт одну псевдо-страницу без имени → нечего матчить
    res = sm.match_sheet_indexes(sm.build_sheet_index(_md([(1, "1", "A")])),
                                 sm.build_sheet_index(""))
    assert res["matched_count"] == 0
    assert "no_sheet_names_found" in res["warnings"]
    # literal empty список → one_side_empty
    res2 = sm.match_sheet_indexes([], sm.build_sheet_index(_md([(1, "1", "A")])))
    assert "one_side_empty" in res2["warnings"]


# ─── store.suggest_alignment_by_stamp обвязка ──────────────────────────────

def test_store_suggest_by_stamp(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import store

    left_md = tmp_path / "left.md"
    right_md = tmp_path / "right.md"
    left_md.write_text(_md([(1, "1", "Содержание тома"),
                            (2, "2", "Схема ГРЩ")]), encoding="utf-8")
    right_md.write_text(_md([(1, "1", "Содержание тома"),
                             (2, "2", "Лист X"), (3, "3", "Схема ГРЩ")]),
                        encoding="utf-8")

    fake_pair = {
        "id": "pX",
        "left": {"md_path": str(left_md), "pdf_path": None, "result_json_path": None},
        "right": {"md_path": str(right_md), "pdf_path": None, "result_json_path": None},
    }
    monkeypatch.setattr(store, "_find_pair_meta", lambda s, p: fake_pair)

    res = store.suggest_alignment_by_stamp("sid", "pX")
    assert res["method"] == "stamp"
    matched = {(it["left_page"], it["right_page"]) for it in res["suggested_items"]
               if it["match"]}
    assert (1, 1) in matched
    assert (2, 3) in matched  # ГРЩ уехала на стр.3


def test_store_suggest_by_stamp_md_missing(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import store
    fake_pair = {"id": "pX",
                 "left": {"md_path": None, "pdf_path": None, "result_json_path": None},
                 "right": {"md_path": None, "pdf_path": None, "result_json_path": None}}
    monkeypatch.setattr(store, "_find_pair_meta", lambda s, p: fake_pair)
    res = store.suggest_alignment_by_stamp("sid", "pX")
    assert res["suggested_items"] == []
    assert "left_md_missing" in res["warnings"]
