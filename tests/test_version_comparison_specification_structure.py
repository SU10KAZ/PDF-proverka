from pathlib import Path

from backend.app.services.version_comparison.specification_source import load_source
from backend.app.services.version_comparison.specification_structure import prompt_for, schema, validate


def _source(tmp_path: Path, body: str):
    path = tmp_path / "spec.md"
    path.write_text(
        "# Document: spec.pdf\n## Page 1\n### BLOCK #1 [TEXT]: blk_aaaaaaaa\n" + body + "\n",
        encoding="utf-8",
    )
    return load_source(path, label="OLD")


def _line(source, text):
    return next(line["line"] for block in source["blocks"] for line in block["lines"] if line["text"] == text)


def test_materializes_exact_cells_sections_parents_continuations_and_plain_notes(tmp_path):
    heading = "Кабель силовой"
    child = "|  | Клемма |  |  |  |  |  |  |  |"
    continuation = "|  | с винтом |  |  |  |  |  |  |  |"
    note = "Примечание к разделу"
    source = _source(tmp_path, "\n".join([
        "| Поз. | Наименование | Марка | Код | Поставщик | Ед. изм. | Количество | Масса | Примечание |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        "Раздел А",
        "| 1 | Кабель силовой |  |  |  | м | 10 |  |  |",
        child,
        continuation,
        note,
    ]))
    parent_line = _line(source, "| 1 | Кабель силовой |  |  |  | м | 10 |  |  |")
    child_line = _line(source, child)
    continuation_line = _line(source, continuation)
    heading_line = _line(source, "Раздел А")
    note_line = _line(source, note)

    checked = validate(source, {
        "sections": [{"line": heading_line}],
        "parents": [{"parent_line": parent_line, "first_child_line": child_line, "last_child_line": child_line}],
        "continuations": [{"lines": [child_line, continuation_line]}],
        "notes": [{"line": note_line}],
    })

    assert checked["complete"], checked["issues"]
    assert len(checked["rows"]) == 3
    assembled = checked["rows"][1]
    assert assembled["fields"]["name"] == "Клемма с винтом"
    assert assembled["parent"] == heading
    assert [s["line_start"] for s in assembled["sources"]] == [child_line, continuation_line]
    assert checked["rows"][2]["fields"]["note"] == note
    assert checked["rows"][2]["section"] == "Раздел А"


def test_unclassified_plain_line_fails_closed_and_keeps_strict_validation(tmp_path):
    data = "| 1 | Кабель |  |  |  | м | 1 |  |  |"
    source = _source(tmp_path, "\n".join([
        "| Поз. | Наименование | Марка | Код | Поставщик | Ед. изм. | Количество | Масса | Примечание |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        data,
        "Не размеченная строка",
    ]))
    checked = validate(source, {"sections": [], "parents": [], "continuations": [], "notes": []})
    assert not checked["complete"]
    assert checked["rows"] == []
    assert any("Plain-line classification mismatch" in issue for issue in checked["issues"])


def test_rejects_continuation_that_merges_two_quantity_values(tmp_path):
    one = "| 1 | Кабель |  |  |  | м | 10 |  |  |"
    two = "|  | с наконечниками |  |  |  |  | 2 |  |  |"
    source = _source(tmp_path, "\n".join([
        "| Поз. | Наименование | Марка | Код | Поставщик | Ед. изм. | Количество | Масса | Примечание |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        one,
        two,
    ]))
    first, second = _line(source, one), _line(source, two)
    checked = validate(source, {"sections": [], "parents": [], "continuations": [{"lines": [first, second]}], "notes": []})
    assert not checked["complete"]
    assert checked["rows"] == []
    assert any("multiple qty values" in issue for issue in checked["issues"])


def test_schema_is_closed_and_prompt_does_not_request_cell_transcription(tmp_path):
    source = _source(tmp_path, "| Поз. | Наименование | Количество |\n| --- | --- | --- |\n| 1 | Кабель | 1 |")
    contract = schema()
    assert contract["additionalProperties"] is False
    assert contract["required"] == ["sections", "parents", "continuations", "notes"]
    prompt = prompt_for(source)
    assert "Не переписывай и не исправляй ячейки" in prompt
    assert "absolute" not in prompt.lower()


def test_invalid_structural_line_fails_closed_without_reading_unassigned_data(tmp_path):
    source = _source(tmp_path, '| Поз. | Наименование | Количество |\n| --- | --- | --- |\n| 1 | Кабель | 1 |')
    for reference in ({'line': 999}, {'line': []}, {'line': True}, {'line': 4, 'invented': 'x'}):
        result = validate(source, {'sections': [reference], 'parents': [], 'continuations': [], 'notes': []})
        assert not result['complete']
        assert result['rows'] == []


def test_confirmed_cross_version_table_heading_preserves_cells_but_is_not_a_removed_item(tmp_path):
    from backend.app.services.version_comparison.specification_compare import compare, change_entries
    header = '| Поз. | Наименование | Марка | Код | Поставщик | Ед. изм. | Количество | Масса | Примечание |'
    separator = '| --- | --- | --- | --- | --- | --- | --- | --- | --- |'
    heading = '|  |  | Электроустановочные | изделия |  |  |  |  |  |'
    item = '|  | Розетка | Р-1 |  |  | шт | 3 |  |  |'
    old = _source(tmp_path, '\n'.join([header, separator, heading, item]))
    ann = {'sections': [{'line': _line(old, heading)}], 'parents': [], 'continuations': [], 'notes': []}
    assert not validate(old, ann)['complete']
    parsed_old = validate(old, ann, table_section_titles=['Электроустановочные изделия'])
    assert parsed_old['complete'], parsed_old['issues']
    assert parsed_old['rows'][0]['fields']['mark'] == 'Электроустановочные'
    assert parsed_old['rows'][0]['role'] == 'section'
    new = _source(tmp_path, '\n'.join([header, separator, 'Электроустановочные изделия', item]))
    parsed_new = validate(new, {'sections': [{'line': _line(new, 'Электроустановочные изделия')}],
                                'parents': [], 'continuations': [], 'notes': []})
    assert parsed_new['complete'], parsed_new['issues']
    compared = compare(parsed_old, parsed_new)
    assert compared['counts']['paired'] == 1
    assert change_entries(compared) == []
