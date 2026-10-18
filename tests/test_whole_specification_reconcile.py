from copy import deepcopy

from backend.app.services.version_comparison.specification_reconcile import reconcile


def _source(label, lines, offset=0, block_id="blk_a"):
    return {
        "label": label,
        "source_sha256": f"{label}-different-hash",
        "blocks": [{"block_id": block_id, "page": 1 + offset, "lines": [
            {"line": number + offset, "text": text} for number, text in lines
        ]}],
    }


def _payload(section_line, parent=None, continuation=None):
    return {
        "sections": [{"line": section_line}],
        "parents": [parent] if parent else [],
        "continuations": [{"lines": continuation}] if continuation else [],
        "notes": [],
    }


def test_propagates_only_missing_parent_and_maps_by_section_line_ordinal():
    content = [
        (1, "Электрика"),
        (2, "| Поз. | Наименование | Количество |"),
        (3, "| --- | --- | --- |"),
        (4, "| 1 | Кабель |  |"),
        (5, "|  | 5х10 | 3 |"),
        (6, "|  | 5х25 | 2 |"),
        (7, "| 2 | Труба |  |"),
        (8, "|  | Ø20 | 4 |"),
    ]
    sources = {"V8": _source("V8", content), "V7": _source("V7", content, offset=100, block_id="other")}
    raw_v8 = _payload(1, {"parent_line": 4, "first_child_line": 5, "last_child_line": 6})
    raw_v7 = _payload(101)
    payloads = {"V8": raw_v8, "V7": raw_v7}

    result = reconcile(sources, payloads)

    assert result["V7"]["issues"] == []
    assert result["V7"]["payload"]["parents"] == [{
        "parent_line": 104, "first_child_line": 105, "last_child_line": 106,
    }]
    assert result["V7"]["provenance"][0]["source_key"] == "V8"
    assert result["V7"]["provenance"][0]["annotation"] == "parent"
    assert raw_v7 == _payload(101)


def test_requires_exact_full_section_content_and_ignores_headers_and_block_coordinates():
    base = [(1, "Раздел"), (2, "| Поз. | Наименование | Количество |"), (3, "|---|---|---|"),
            (4, "| 1 | Труба |  |"), (5, "|  | Ø20 | 1 |")]
    changed = [(20, "Раздел"), (21, "| Поз. | Наименование | Количество |"), (22, "|---|---|---|"),
               (23, "| 1 | Труба изменена |  |"), (24, "|  | Ø20 | 1 |")]
    sources = {"A": _source("A", base), "B": _source("B", changed, offset=50, block_id="another")}
    payloads = {"A": _payload(1, {"parent_line": 4, "first_child_line": 5, "last_child_line": 5}), "B": _payload(70)}
    result = reconcile(sources, payloads)
    assert result["B"]["payload"]["parents"] == []
    assert result["B"]["provenance"] == []
    assert result["B"]["issues"] == []


def test_conflicting_parent_overlap_fails_without_choosing_either():
    lines = [(1, "Раздел"), (2, "| Поз. | Наименование | Количество |"), (3, "|---|---|---|"),
             (4, "| 1 | Родитель А |  |"), (5, "| 2 | Родитель Б |  |"), (6, "|  | Дитя | 1 |")]
    sources = {"A": _source("A", lines), "B": _source("B", lines, offset=100)}
    payloads = {
        "A": _payload(1, {"parent_line": 4, "first_child_line": 6, "last_child_line": 6}),
        "B": _payload(101, {"parent_line": 105, "first_child_line": 106, "last_child_line": 106}),
    }
    before = deepcopy(payloads)
    result = reconcile(sources, payloads)
    assert result["A"]["payload"] == before["A"]
    assert result["B"]["payload"] == before["B"]
    assert result["A"]["provenance"] == result["B"]["provenance"] == []
    assert any("Conflicting parent overlap" in issue for issue in result["A"]["issues"])


def test_continuation_group_omission_is_filled_but_overlap_conflict_fails():
    lines = [(1, "Раздел"), (2, "| Поз. | Наименование | Количество |"), (3, "|---|---|---|"),
             (4, "| 1 | Кабель | 1 |"), (5, "|  | с наконечником |  |"), (6, "| 2 | Труба | 1 |")]
    sources = {"A": _source("A", lines), "B": _source("B", lines, offset=20)}
    payloads = {"A": _payload(1, continuation=[4, 5]), "B": _payload(21)}
    result = reconcile(sources, payloads)
    assert result["B"]["payload"]["continuations"] == [{"lines": [24, 25]}]
    assert result["B"]["provenance"][0]["annotation"] == "continuation"

    payloads["B"]["continuations"] = [{"lines": [5 + 20, 6 + 20]}]
    conflict = reconcile(sources, payloads)
    assert any("Conflicting continuation overlap" in issue for issue in conflict["A"]["issues"])
    assert conflict["B"]["provenance"] == []


def test_exact_plain_heading_confirms_table_heading_without_rewriting_cells():
    head = '| Поз. | Наименование | Марка | Код | Ед. изм. | Количество |'
    rule = '| --- | --- | --- | --- | --- | --- |'
    table_heading = '|  |  | Электроустановочные | изделия |  |  |'
    item = '|  | Розетка | Р-1 |  | шт | 3 |'
    sources = {
        'A': _source('A', [(1, head), (2, rule), (3, table_heading), (4, item)]),
        'B': _source('B', [(11, 'Электроустановочные изделия'), (12, head), (13, rule), (14, item)]),
    }
    payloads = {'A': {**_payload(3), 'sections': []}, 'B': _payload(11)}
    result = reconcile(sources, payloads)
    assert result['A']['payload']['sections'] == [{'line': 3}]
    assert result['A']['table_section_titles'] == ['Электроустановочные изделия']
    assert result['A']['provenance'][0]['source_key'] == 'B'
    assert not result['A']['issues']
    assert payloads['A']['sections'] == []
    assert sources['A']['blocks'][0]['lines'][2]['text'] == table_heading

    sources['A']['blocks'][0]['lines'][2]['text'] = '|  |  | Электроустановочные | изделия | шт | 2 |'
    result = reconcile(sources, payloads)
    assert result['A']['payload']['sections'] == []
