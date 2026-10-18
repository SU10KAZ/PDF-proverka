from backend.app.services.version_comparison.specification_compare import compare, render_markdown


def side(label, rows, *, complete=True, issues=None, document=None):
    return {
        "label": label,
        "document": document or f"{label}.pdf",
        "source_sha256": f"hash-{label}",
        "complete": complete,
        "issues": issues or [],
        "rows": rows,
    }


def row(rid, *, section="ЭОМ", parent="Щит ЩР-1", pos="1", name="Кабель",
        mark="ВВГ", code="A-1", supplier="", unit="м", qty="10", mass="",
        note="", page=1, block="b1", start=1, end=1, quote=None):
    return {
        "id": rid,
        "section": section,
        "parent": parent,
        "fields": {"pos": pos, "name": name, "mark": mark, "code": code,
                   "supplier": supplier, "unit": unit, "qty": qty, "mass": mass,
                   "note": note},
        "sources": [{"document": "spec.pdf", "page": page, "block_id": block,
                     "line_start": start, "line_end": end,
                     "quote": quote if quote is not None else f"{pos} {name} {qty}"}],
    }


def test_whole_spec_page_shift_with_quantity_edit_matches_identity_and_keeps_both_refs():
    old = row("old", pos="1", qty="10", page=2, block="old-block", start=15, end=15)
    new = row("new", pos="1", qty="12", page=3, block="new-block", start=2, end=2)
    result = compare(side("old", [old]), side("new", [new]))

    assert result["status"] == "complete"
    assert result["counts"]["paired"] == 1
    assert result["counts"]["changed"] == 1
    assert result["counts"]["relocated"] == 1
    assert result["pairs"][0]["field_changes"] == [{"field": "qty", "old": "10", "new": "12"}]
    report = render_markdown(result)
    for expected in ("spec.pdf, стр. 2, блок old-block, строки 15–15",
                     "spec.pdf, стр. 3, блок new-block, строки 2–2", "10» → «12",
                     "Количество: «10» → «12»", "hash-old", "hash-new",
                     "Источник строки изменился"):
        assert expected in report


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


def test_incomplete_side_does_not_turn_unseen_rows_into_confident_deletions():
    result = compare(side("old", [row("old-row")], complete=False,
                          issues=["не распознана часть таблицы"]),
                     side("new", [], complete=True))

    assert result["status"] == "incomplete"
    assert result["complete"] is False
    assert [r["id"] for r in result["removed"]] == ["old-row"]
    report = render_markdown(result)
    assert "предварительные" in report
    assert "одна или обе версии неполны" in report


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


def test_markdown_escapes_special_characters_and_lists_every_source_reference():
    old = row("o", name="Труба | <сталь>", page=1, block="a", start=1, end=2,
              quote="до | <текст>")
    new = row("n", name="Труба | <сталь>", qty="11", page=5, block="z", start=8, end=9,
              quote="после | <текст>")
    report = render_markdown(compare(side("old", [old]), side("new", [new])))

    assert "Труба \\| &lt;сталь&gt;" in report
    assert "до \\| &lt;текст&gt;" in report
    assert "после \\| &lt;текст&gt;" in report
    assert "стр. 1, блок a, строки 1–2" in report
    assert "стр. 5, блок z, строки 8–9" in report


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
    assert "old-md-block" in report
    assert "new-md-block" in report
    assert "Источник строки изменился" not in report
