from backend.app.services.version_comparison.specification_compare import (
    change_entries, compare, render_entry, render_markdown,
)


def side(label, rows, *, complete=True, issues=None, document=None, pdf_sha256=None):
    return {
        "label": label,
        "document": document or f"{label}.pdf",
        "source_sha256": f"hash-{label}",
        "pdf_sha256": pdf_sha256 or f"pdf-hash-{label}",
        "complete": complete,
        "issues": issues or [],
        "rows": rows,
    }


def row(rid, *, section="ЭОМ", parent="Щит ЩР-1", pos="1", name="Кабель",
        mark="ВВГ", code="A-1", supplier="", unit="м", qty="10", mass="",
        note="", page=1, sheet=None, block="b1", start=1, end=1, quote=None):
    return {
        "id": rid,
        "section": section,
        "parent": parent,
        "fields": {"pos": pos, "name": name, "mark": mark, "code": code,
                   "supplier": supplier, "unit": unit, "qty": qty, "mass": mass,
                   "note": note},
        "sources": [{"document": "spec.pdf", "page": page, **({"sheet": sheet} if sheet else {}), "block_id": block,
                     "line_start": start, "line_end": end,
                     "quote": quote if quote is not None else f"{pos} {name} {qty}"}],
    }


def test_whole_spec_page_shift_with_quantity_edit_matches_identity_and_keeps_both_refs():
    old = row("old", pos="1", qty="10", page=2, block="old-block", start=15, end=15)
    new = row("new", pos="1", qty="12", page=3, block="new-block", start=2, end=2)
    result = compare(side("old", [old]), side("new", [new]))

    assert result["status"] == "complete"
    assert result["old"]["pdf_sha256"] == "pdf-hash-old"
    assert result["new"]["pdf_sha256"] == "pdf-hash-new"
    assert result["counts"]["paired"] == 1
    assert result["counts"]["changed"] == 1
    assert result["counts"]["relocated"] == 1
    assert result["pairs"][0]["field_changes"] == [{"field": "qty", "old": "10", "new": "12"}]
    report = render_markdown(result)
    for expected in ("Количество: 10 м → 12 м", "стр. PDF 2", "стр. PDF 3"):
        assert expected in report
    for forbidden in ("old-block", "new-block", "hash-old", "hash-new", "цитат", "перемещ"):
        assert forbidden not in report


def test_exact_duplicates_are_multiset_matched_without_order_dependence():
    olds = [row("o1", pos="1", page=2), row("o2", pos="2", page=3)]
    news = [row("n2", pos="2", page=9), row("n1", pos="1", page=8)]
    first = compare(side("old", olds), side("new", news))
    second = compare(side("old", list(reversed(olds))), side("new", list(reversed(news))))

    assert first["counts"]["paired"] == 2
    assert first["counts"]["added"] == first["counts"]["removed"] == 0
    assert first["counts"]["unchanged"] == 2
    assert first["pairs"] == second["pairs"]


def test_exact_duplicates_keep_shared_position_before_inferring_renumbering():
    result = compare(side("old", [row("o1", pos="1"), row("o2", pos="2")]),
                     side("new", [row("n2", pos="2"), row("n3", pos="3")]))
    assert result["counts"]["renumbered"] == 1
    assert result["counts"]["changed"] == 0
    assert any(p["old"]["fields"]["pos"] == p["new"]["fields"]["pos"] == "2" for p in result["pairs"])


def test_decimal_normalization_does_not_round_long_quantities():
    a = "123456789012345678901234567890.1"
    b = "123456789012345678901234567890.2"
    result = compare(side("old", [row("o", qty=a)]), side("new", [row("n", qty=b)]))
    assert result["counts"]["changed"] == 1


def test_repeated_identity_with_different_quantities_is_unresolved_not_greedily_paired():
    olds = [row("o1", qty="10"), row("o2", qty="20")]
    news = [row("n1", qty="11"), row("n2", qty="21")]
    result = compare(side("old", olds), side("new", news))

    assert result["counts"]["paired"] == 0
    assert result["counts"]["unresolved_old"] == 2
    assert result["counts"]["unresolved_new"] == 2
    assert result["added"] == result["removed"] == []


def test_isolated_addition_and_removal_are_reported():
    old = row("removed", name="Насос", code="P1")
    new = row("added", name="Клапан", code="V9")
    result = compare(side("old", [old]), side("new", [new]))

    assert [r["id"] for r in result["removed"]] == ["removed"]
    assert [r["id"] for r in result["added"]] == ["added"]


def test_position_renumbering_is_separate_from_substantive_change():
    result = compare(side("old", [row("o", pos="01")]),
                     side("new", [row("n", pos="7", page=4)]))
    pair = result["pairs"][0]

    assert pair["renumbered"] is True
    assert pair["field_changes"] == []
    assert pair["relocated"] is True
    assert result["counts"]["changed"] == 0
    assert result["counts"]["renumbered"] == 1


def test_changed_row_shows_both_position_numbers_and_absent_quantity():
    result = compare(side("old", [row("o", pos="1", qty="")]),
                     side("new", [row("n", pos="10", qty="12")]))
    text = render_markdown(result)
    assert "Поз. 1 → 10" in text
    assert "Количество: не указано → 12 м" in text


def test_incomplete_side_suppresses_provisional_additions_and_removals():
    result = compare(side("old", [row("old-row")], complete=False,
                          issues=["не распознана часть таблицы"]),
                     side("new", [], complete=True))

    assert result["status"] == "incomplete"
    assert result["complete"] is False
    assert [r["id"] for r in result["removed"]] == ["old-row"]
    assert change_entries(result) == []
    report = render_markdown(result)
    assert "Сравнение неполное" in report
    assert "не сформированы" in report
    assert "old-row" not in report


def test_hierarchy_blocks_cross_section_identity_match():
    result = compare(side("old", [row("o", section="ЭОМ")]),
                     side("new", [row("n", section="ВК")]))

    assert result["pairs"] == []
    assert len(result["removed"]) == len(result["added"]) == 1


def test_same_name_but_conflicting_codes_is_left_for_review():
    old = row("o", name="Насос", mark="Модель", code="P-1")
    new = row("n", name="Насос", mark="Модель", code="P-2")
    result = compare(side("old", [old]), side("new", [new]))

    assert result["pairs"] == []
    assert result["added"] == result["removed"] == []
    assert result["counts"]["unresolved_old"] == result["counts"]["unresolved_new"] == 1


def test_numeric_decimal_normalization_handles_comma_and_zero_without_merging_empty():
    same = compare(side("old", [row("o", qty="1,50", mass="0,0")]),
                   side("new", [row("n", qty="1.500", mass="0")]))
    empty_vs_zero = compare(side("old", [row("o", qty="")]),
                            side("new", [row("n", qty="0")]))

    assert same["counts"]["unchanged"] == 1
    assert empty_vs_zero["counts"]["changed"] == 1
    assert empty_vs_zero["pairs"][0]["field_changes"] == [
        {"field": "qty", "old": "", "new": "0"}
    ]


def test_markdown_escapes_changed_values_and_hides_source_ledger_details():
    old = row("o", name="Труба | <сталь>", page=1, block="a", start=1, end=2,
              quote="до | <текст>")
    new = row("n", name="Труба | <сталь>", qty="11", page=5, block="z", start=8, end=9,
              quote="после | <текст>")
    report = render_markdown(compare(side("old", [old]), side("new", [new])))

    assert "&lt;сталь&gt;" in report
    assert "блок a" not in report
    assert "до" not in report and "после" not in report


def test_unkeyed_residuals_are_flagged_as_ambiguous_with_deterministic_order():
    olds = [row("o2", name="", mark="", code="", qty="2"),
            row("o1", name="", mark="", code="", qty="1")]
    news = [row("n", name="", mark="", code="", qty="3")]
    result = compare(side("old", olds), side("new", news))

    assert result["counts"]["unresolved_old"] == 2
    assert result["counts"]["unresolved_new"] == 1
    assert result["added"] == result["removed"] == []
    assert render_markdown(result) == render_markdown(
        compare(side("old", list(reversed(olds))), side("new", news)))


def test_same_page_with_regenerated_document_block_and_line_refs_is_not_relocation():
    old = row("o", page=7, block="old-md-block", start=12, end=13)
    new = row("n", qty="10,0", page=7, block="new-md-block", start=1, end=1)
    new["sources"][0]["document"] = "new-version.md"
    result = compare(side("old", [old]), side("new", [new]))
    pair = result["pairs"][0]
    report = render_markdown(result)

    assert pair["relocated"] is False
    assert result["counts"]["relocated"] == 0
    assert "old-md-block" not in report
    assert "new-md-block" not in report
    assert "изменений не выявлено" in report


def test_renumber_only_and_relocation_only_are_not_change_entries():
    renumber = compare(side("old", [row("o", pos="1", page=1)]),
                       side("new", [row("n", pos="2", page=1)]))
    relocation = compare(side("old", [row("o", page=1)]),
                         side("new", [row("n", page=2)]))

    assert change_entries(renumber) == []
    assert change_entries(relocation) == []
    assert "изменений не выявлено" in render_markdown(renumber)


def test_change_entries_include_pages_sheets_and_one_event_for_multisheet_row():
    old = row("o", qty="10", page=2, sheet="Лист 4")
    old["sources"].append({"document": "old.md", "page": 3, "sheet": "Лист 5",
                           "block_id": "b2", "line_start": 4, "line_end": 4, "quote": "q"})
    new = row("n", qty="12", page=8, sheet="Лист 9")
    result = compare(side("old", [old]), side("new", [new]))
    entries = change_entries(result)

    assert len(entries) == 1
    entry = entries[0]
    assert entry["old_pages"] == [2, 3]
    assert entry["new_pages"] == [8]
    assert entry["old_sheets"] == ["Лист 4", "Лист 5"]
    assert entry["new_sheets"] == ["Лист 9"]
    assert entry["old_locations"] == [
        {"sheet": "Лист 4", "page": 2}, {"sheet": "Лист 5", "page": 3}
    ]
    assert entry["new_locations"] == [{"sheet": "Лист 9", "page": 8}]
    assert "Количество: 10 м → 12 м" in entry["text"]
    assert "блок" not in entry["text"]
    rendered = render_entry(entry)
    assert "л.Лист 4 (стр. PDF 2)" in rendered
    assert "л.Лист 5 (стр. PDF 3)" in rendered
    assert "л.Лист 9 (стр. PDF 8)" in rendered


def test_addition_and_removal_entries_show_full_fields_in_before_after_form():
    result = compare(side("old", [row("gone", name="Насос", code="P1", qty="2",
                                        supplier="Завод А", mass="5", note="резерв")]),
                     side("new", [row("new", name="Клапан", code="V9", qty="3",
                                        supplier="Завод Б", mass="7", note="основной")]))
    entries = change_entries(result)

    assert len(entries) == 2
    assert any("не было →" in entry["text"] and "Клапан" in entry["text"] for entry in entries)
    assert any("Насос" in entry["text"] and "→ отсутствует" in entry["text"] for entry in entries)
    for value in ("Количество 3 м", "Поставщик Завод Б", "Масса 7", "Примечание основной"):
        assert value in next(e["text"] for e in entries if "Клапан" in e["text"])


def test_repeated_rows_with_same_name_and_quantity_are_not_deduplicated_when_sources_differ():
    old = [row("o1", pos="1", name="Лампа", mark="A", code="", page=1),
           row("o2", pos="2", name="Лампа", mark="B", code="", page=2)]
    new = [row("n1", pos="1", name="Лампа", mark="A", code="", qty="11", page=1),
           row("n2", pos="2", name="Лампа", mark="B", code="", qty="11", page=2)]
    entries = change_entries(compare(side("old", old), side("new", new)))

    assert len(entries) == 2
    assert entries[0]["id"] != entries[1]["id"]
