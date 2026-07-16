"""Тесты ветки НОВОГО формата портала (*_results.md) в stage_comparison.

Покрывает:
  * evidence_first_fallback.build_fact_index — ветка results_md: страницы
    `## Page N`, документный `**Stamp:**`, номер/имя листа из per-block
    `> **Stamp:** … | Sheet: … | Name: …`, image-блоки, классификация,
    сырой срез body + strip_results_md_meta;
  * deterministic_fact_diff по документным штампам нового формата;
  * stamp_matching.build_sheet_index — имена листов нового формата,
    наследование продолжений, derived-заголовок НЕ из штамп-строки;
  * match_sheet_indexes на паре нового формата и на смешанной паре
    (старый Chandra ↔ новый results_md);
  * старый формат — прежний код-путь (ветка не срабатывает).
"""
from __future__ import annotations

from backend.app.services.stage_comparison import evidence_first_fallback as ef
from backend.app.services.stage_comparison import stamp_matching as sm

# ─── Билдеры фикстур ────────────────────────────────────────────────────────

DOC_STAMP = ("Code: ТЕСТ-АР1 | Stage: Р | Object: Жилой дом по адресу: "
             "г. Москва, ул. Тестовая, д. 1 | Organization: ОРГ")


def _block_stamp(sheet: str, name: str) -> str:
    return (f"Code: ТЕСТ-АР1 | Stage: Р | Sheet: {sheet} | Object: Жилой дом "
            f"по адресу: г. Москва | Name: {name} | Organization: ОРГ | Revisions: ")


def _new_md(pages: list[tuple[int, list[tuple[str, str, str, str]]]],
            *, doc_stamp: str = DOC_STAMP) -> str:
    """pages: [(page_no, [(тип, sheet, name, body), …]), …] → текст *_results.md."""
    out = [
        "# Document: ТЕСТ-АР1_V1.pdf",
        "",
        "Path: АР / ТЕСТ-АР1 / ТЕСТ-АР1_V1.pdf",
        "",
        "Generated: 2026-07-15 05:51:33 UTC",
        "",
        f"**Stamp:** {doc_stamp}",
        "",
        "---",
        "",
    ]
    n = 0
    for page_no, blocks in pages:
        out.append(f"## Page {page_no}")
        out.append("")
        for btype, sheet, name, body in blocks:
            n += 1
            out.append(f"### BLOCK #{n} [{btype}]: blk_{n:032x}")
            out.append("")
            out.append("> **Created:** 2026-07-07 15:22:34 UTC")
            out.append(f"> **Crop:** [Crop](https://portal.example/api/crops/tok{n})")
            out.append(f"> **Stamp:** {_block_stamp(sheet, name)}")
            out.append("")
            out.append(body)
            out.append("")
    return "\n".join(out)


def _old_md(pages: list[tuple[int, object, object]]) -> str:
    """Старый Chandra-формат: [(page_no, sheet_no|None, sheet_name|None), …]."""
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


_IMG_BODY = "**[IMAGE]** | Type: План | Axes: А-Д | Zone: — | Level: 2 этаж\n**Summary:** план"


# ─── build_fact_index: ветка results_md ─────────────────────────────────────

def test_fact_index_results_md_pages_sheets_and_stamp():
    md = _new_md([
        (1, [("TEXT", "", "", "Титульный лист")]),                    # пустой штамп
        (2, [("TEXT", "3", "Общие данные", "Пояснения. " * 40)]),
        (3, [("IMAGE", "5", "Кладочный план 2 этажа", _IMG_BODY)]),
    ])
    idx = ef.build_fact_index("left", md)
    assert [p.page for p in idx.pages] == [1, 2, 3]
    assert idx.stamp == DOC_STAMP                       # документный **Stamp:**
    by = {p.page: p for p in idx.pages}
    # Ключ листа = страница PDF; sheet/name — подписи из per-block **Stamp:**.
    assert by[1].sheet_no == "" and by[1].sheet_name == ""
    assert by[2].sheet_no == "3" and by[2].sheet_name == "Общие данные"
    assert by[3].sheet_no == "5" and by[3].sheet_name == "Кладочный план 2 этажа"
    # image-блоки распознаны по заголовку `[IMAGE]`, текстовые — нет.
    assert by[3].image_block_ids == ["blk_%032x" % 3]
    assert by[2].image_block_ids == []


def test_fact_index_results_md_classification():
    md = _new_md([
        (1, [("TEXT", "", "", "Титульный лист")]),                    # коротко → other
        (2, [("TEXT", "3", "Общие данные", "Пояснения. " * 40)]),     # текст → pz
        (3, [("IMAGE", "5", "Кладочный план 2 этажа", _IMG_BODY)]),   # план этажа → АР
    ])
    by = {p.page: p for p in ef.build_fact_index("x", md).pages}
    # Длина меряется по КОНТЕНТУ (без заголовков блоков/мета-цитат) — иначе
    # титул с одним словом «раздулся» бы бойлерплейтом до pz.
    assert by[1].section_class == "other"
    assert by[2].section_class == "pz"
    assert by[3].section_class == "architectural"


def test_fact_index_results_md_body_is_raw_slice_and_strip_meta():
    md = _new_md([(1, [("TEXT", "1", "Общие данные", "Содержимое листа")])])
    p = ef.build_fact_index("x", md).pages[0]
    # body — сырой срез (fidelity для чанкинга/verification): мета на месте.
    assert "### BLOCK #1 [TEXT]:" in p.body
    assert "> **Stamp:**" in p.body
    stripped = ef.strip_results_md_meta(p.body)
    assert "Содержимое листа" in stripped
    assert "BLOCK #1" not in stripped
    assert "**Stamp:**" not in stripped and "**Crop:**" not in stripped


def test_fact_index_results_md_with_enrichment_insertions():
    # Enrichment-вставки (Qwen-описания с `block_id:`) не ломают ветку и
    # учитываются в image_block_ids (дедуп с заголовками блоков).
    md = _new_md([(1, [("IMAGE", "2", "Кладочный план 2 этажа", _IMG_BODY)])])
    md += ("\n<!-- QWEN_IMAGE_DESCRIPTION_START\n"
           "block_id: blk_ffffffffffffffffffffffffffffffff\n-->\nОписание плана\n")
    p = ef.build_fact_index("x", md).pages[0]
    assert ("blk_%032x" % 1) in p.image_block_ids
    assert "blk_ffffffffffffffffffffffffffffffff" in p.image_block_ids
    assert len(p.image_block_ids) == 2


def test_fact_index_old_format_path_unchanged():
    # Старый Chandra-формат в ветку не попадает: прежние поля, штамп пустой.
    md = _old_md([(1, "3", "Общие данные")])
    idx = ef.build_fact_index("left", md)
    assert idx.pages[0].sheet_no == "3"
    assert idx.pages[0].sheet_name == "Общие данные"
    assert idx.stamp == ""


def test_deterministic_stamp_diff_results_md():
    left = _new_md([(1, [("TEXT", "1", "Общие данные", "Пояснения. " * 40)])])
    right = _new_md([(1, [("TEXT", "1", "Общие данные", "Пояснения. " * 40)])],
                    doc_stamp=DOC_STAMP.replace("Stage: Р", "Stage: РД"))
    smap = ef.build_scope_map(ef.build_fact_index("left", left),
                              ef.build_fact_index("right", right))
    types = [c["type"] for c in ef.deterministic_fact_diff(smap)]
    assert "stamp_changed" in types
    # Одинаковые штампы — изменения нет.
    smap_same = ef.build_scope_map(ef.build_fact_index("left", left),
                                   ef.build_fact_index("right", left))
    assert all(c["type"] != "stamp_changed"
               for c in ef.deterministic_fact_diff(smap_same))


# ─── stamp_matching.build_sheet_index: ветка results_md ─────────────────────

def test_build_sheet_index_results_md_names_and_inherit():
    md = _new_md([
        (1, [("TEXT", "", "", "Обложка")]),                       # безымянный титул
        (2, [("TEXT", "2", "Текстовая часть", "Раздел 1. Общие положения")]),
        (3, [("TEXT", "3", "", "Продолжение раздела")]),          # Sheet есть, Name пуст
        (4, [("IMAGE", "4", "Кладочный план 2 этажа", _IMG_BODY)]),
    ])
    by = {r.page: r for r in sm.build_sheet_index(md)}
    assert by[1].name_source == "none" and by[1].norm_name == ""
    assert by[2].name_source == "md"
    assert by[2].norm_name == sm.normalize_sheet_name("Текстовая часть")
    # forward-fill: продолжение листа наследует имя предыдущего именованного.
    assert by[3].name_source == "inherited"
    assert by[3].norm_name == by[2].norm_name
    assert by[4].is_graphic is True and by[2].is_graphic is False


def test_derived_title_not_taken_from_results_md_stamp_line():
    # Имя в штампе содержит известный заголовок («Ведомость …»), но тело — нет:
    # derived должен остаться пустым (штамп-строка вырезана перед извлечением).
    md = _new_md([(1, [("TEXT", "7", "Ведомость рабочих чертежей",
                        "Просто текст листа без известного заголовка")])])
    rec = sm.build_sheet_index(md)[0]
    assert rec.derived_name == ""
    assert rec.norm_name == sm.normalize_sheet_name("Ведомость рабочих чертежей")


def test_derived_title_from_results_md_content():
    # А из СОДЕРЖИМОГО блока derived-заголовок извлекаться должен.
    md = _new_md([(1, [("TEXT", "7", "Проект организации строительства",
                        "Календарный план строительства на 2026 год")])])
    rec = sm.build_sheet_index(md)[0]
    assert rec.derived_name == "Календарный план"


def test_build_sheet_index_results_md_text_layer_fallback():
    md = _new_md([(1, [("TEXT", "", "", "Обложка")])])
    idx = sm.build_sheet_index(md, extra_text_by_page={1: "Однолинейная схема ВРУ"})
    assert idx[0].name_source == "text_layer"
    assert idx[0].norm_name == sm.normalize_sheet_name("Однолинейная схема ВРУ")


# ─── match_sheet_indexes на новом формате ───────────────────────────────────

def test_match_results_md_pair_exact_name_across_offset():
    """Схема ГРЩ уехала со стр.2 на стр.6 — находится по имени из **Stamp:**."""
    left = _new_md([
        (1, [("TEXT", "1", "Содержание тома", "Содержание. " * 30)]),
        (2, [("IMAGE", "2", "Однолинейная расчетная схема ГРЩ-0,4кВ", _IMG_BODY)]),
    ])
    right_pages: list = [(1, [("TEXT", "1", "Содержание тома", "Содержание. " * 30)])]
    for p in range(2, 6):
        right_pages.append((p, [("TEXT", str(p), f"Наполнитель раздел {p}",
                                 f"текст раздела {p}. " * 20)]))
    right_pages.append(
        (6, [("IMAGE", "6", "Однолинейная расчетная схема ГРЩ-0,4кВ", _IMG_BODY)]))
    res = sm.match_sheet_indexes(sm.build_sheet_index(left),
                                 sm.build_sheet_index(_new_md(right_pages)))
    grsh = [it for it in res["suggested_items"] if it.get("left_page") == 2]
    assert grsh and grsh[0]["match"] is True
    assert grsh[0]["right_page"] == 6
    assert grsh[0]["match_type"] == "exact_name"


def test_match_mixed_old_and_new_format():
    """Старая стадия в Chandra-формате, новая — в *_results.md: матч по имени."""
    left = _old_md([
        (1, "1", "Содержание тома"),
        (2, "2", "Однолинейная расчетная схема ГРЩ-0,4кВ"),
    ])
    right = _new_md([
        (1, [("TEXT", "1", "Содержание тома", "Содержание. " * 30)]),
        (2, [("TEXT", "2", "Наполнитель раздел", "текст. " * 20)]),
        (3, [("IMAGE", "3", "Однолинейная расчетная схема ГРЩ-0,4кВ", _IMG_BODY)]),
    ])
    res = sm.match_sheet_indexes(sm.build_sheet_index(left),
                                 sm.build_sheet_index(right))
    by_left = {it.get("left_page"): it for it in res["suggested_items"]
               if it.get("left_page")}
    assert by_left[1]["match"] is True and by_left[1]["right_page"] == 1
    assert by_left[2]["match"] is True and by_left[2]["right_page"] == 3
    assert by_left[2]["match_type"] == "exact_name"
