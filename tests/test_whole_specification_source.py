import hashlib
from pathlib import Path

from backend.app.services.version_comparison.specification_source import (
    extraction_schema,
    load_source,
    validate_extraction,
)


def _md(blocks: str, *, pages: str = "## Page 1\n") -> str:
    return "# Document: Spec.pdf\nPath: X\nGenerated: today\n---\n" + pages + blocks


def _block(block_id: str, body: str, ordinal: int = 1, kind: str = "TEXT") -> str:
    return f"### BLOCK #{ordinal} [{kind}]: {block_id}\n{body}\n"


def _row(block_id, line, quote, *, fields=None, section="", parent=""):
    values = {k: "" for k in ("pos", "name", "mark", "code", "supplier", "unit", "qty", "mass", "note")}
    values.update(fields or {})
    return {"block_id": block_id, "rows": [{
        "section": section, "parent": parent, "fields": values,
        "evidence": [{"line": line, "quote": quote}],
    }], "ignored": [], "issues": []}


def test_load_source_preserves_absolute_lines_hash_and_same_page_blocks(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "№ | Наименование | Кол-во\n1 | Кабель | 12 м", 1)
             + _block("blk_bbbbbbbb", "2 | Труба Ø20 | 3 шт", 2))
    path = tmp_path / "spec.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="NEW")
    assert source["complete"]
    assert source["scope"] == "all blocks"
    assert source["source_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert [b["block_id"] for b in source["blocks"]] == ["blk_aaaaaaaa", "blk_bbbbbbbb"]
    assert source["blocks"][0]["lines"][0]["line"] == md.splitlines().index("№ | Наименование | Кол-во") + 1
    assert source["blocks"][1]["page"] == 1


def test_validate_exact_grounding_non_numeric_qty_unicode_and_sources(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "7 | Кабель КГ | 12 пог. м — для щита №2"))
    path = tmp_path / "spec.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    line = source["blocks"][0]["lines"][0]
    payload = {"blocks": [_row("blk_aaaaaaaa", line["line"], line["text"], fields={
        "pos": "7", "name": "Кабель КГ", "qty": "12 пог. м", "note": "для щита №2",
    })]}
    result = validate_extraction(source, payload)
    assert result["complete"]
    assert result["rows"][0]["fields"]["qty"] == "12 пог. м"
    assert result["rows"][0]["sources"] == [{
        "document": "Spec.pdf", "page": 1, "block_id": "blk_aaaaaaaa",
        "line_start": line["line"], "line_end": line["line"], "quote": line["text"],
    }]


def test_rejects_omitted_block_and_unclassified_line(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "1 | Кабель | 12\n2 | Провод | 4", 1)
             + _block("blk_bbbbbbbb", "3 | Труба | 5", 2))
    path = tmp_path / "spec.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    line = source["blocks"][0]["lines"][0]
    result = validate_extraction(source, {"blocks": [_row("blk_aaaaaaaa", line["line"], line["text"])]})
    assert not result["complete"]
    assert result["rows"] == []
    assert any("block ids" in issue for issue in result["issues"])
    assert any("Unclassified source lines" in issue for issue in result["issues"])


def test_rejects_fabricated_value_bad_quote_and_duplicate_extraction(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "1 | Кабель | 12"))
    path = tmp_path / "spec.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    line = source["blocks"][0]["lines"][0]
    r = _row("blk_aaaaaaaa", line["line"], "Кабель", fields={"qty": "999"})["rows"][0]
    payload = {"blocks": [{"block_id": "blk_aaaaaaaa", "rows": [r, dict(r)], "ignored": [], "issues": []}]}
    result = validate_extraction(source, payload)
    assert not result["complete"]
    assert any("Fabricated or ungrounded qty" in issue for issue in result["issues"])
    assert any("Duplicate extraction" in issue for issue in result["issues"])


def test_cross_block_continuation_is_supported(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "1 | Щит управления", 1)
             + "## Page 2 — Sheet 2 — Продолжение\n"
             + _block("blk_bbbbbbbb", "ШУ-1 | 380 В | 1 шт", 2))
    path = tmp_path / "spec.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    first, second = source["blocks"]
    first_line, second_line = first["lines"][0], second["lines"][0]
    fields = {"mark": "ШУ-1", "name": "Щит управления", "qty": "1 шт"}
    row = {"section": "", "parent": "", "fields": {k: fields.get(k, "") for k in ("pos", "name", "mark", "code", "supplier", "unit", "qty", "mass", "note")},
           "evidence": [{"line": first_line["line"], "quote": first_line["text"]}, {"line": second_line["line"], "quote": second_line["text"]}]}
    payload = {"blocks": [
        {"block_id": first["block_id"], "rows": [row], "ignored": [], "issues": []},
        {"block_id": second["block_id"], "rows": [], "ignored": [], "issues": []},
    ]}
    result = validate_extraction(source, payload)
    assert result["complete"]
    assert {x["block_id"] for x in result["rows"][0]["sources"]} == {"blk_aaaaaaaa", "blk_bbbbbbbb"}


def test_pipe_table_name_can_continue_across_page_blocks(tmp_path: Path):
    head = "| Поз. | Наименование | Код | Количество |\n|---|---|---|---|\n| 2 | Труба гофрированная | A-2 | 1 шт |"
    tail = "|  | с зондом |  |  |"
    md = _md(_block("blk_aaaaaaaa", head, 1) + "## Page 2 — Sheet 2\n" + _block("blk_bbbbbbbb", tail, 2))
    path = tmp_path / "continued.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    first, second = source["blocks"]
    row_line, continuation_line = first["lines"][-1], second["lines"][0]
    fields = {k: "" for k in ("pos", "name", "mark", "code", "supplier", "unit", "qty", "mass", "note")}
    fields.update(pos="2", name="Труба гофрированная с зондом", code="A-2", qty="1 шт")
    row = {"section": "", "parent": "", "fields": fields,
           "evidence": [{"line": row_line["line"], "quote": row_line["text"]},
                        {"line": continuation_line["line"], "quote": continuation_line["text"]}]}
    payload = {"blocks": [
        {"block_id": first["block_id"], "rows": [row], "ignored": [
            {"line": first["lines"][0]["line"], "reason": "header"},
            {"line": first["lines"][1]["line"], "reason": "separator"}], "issues": []},
        {"block_id": second["block_id"], "rows": [], "ignored": [], "issues": []},
    ]}
    result = validate_extraction(source, payload)
    assert result["complete"], result["issues"]
    assert result["rows"][0]["fields"]["name"] == "Труба гофрированная с зондом"


def test_load_source_fails_closed_for_missing_page_image_and_duplicate_ids(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "**[IMAGE]** | Type: План", kind="IMAGE"), pages="## Page 1\n## Page 1\n")
    md += "## Page 2\n" + _block("blk_aaaaaaaa", "image")
    path = tmp_path / "spec.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD", pages=[1, 9])
    assert not source["complete"]
    assert any("Requested page not found" in issue for issue in source["issues"])
    assert any("unsupported image" in issue for issue in source["issues"])
    assert any("Duplicate block id" in issue for issue in source["issues"])
    assert any("Duplicate page header" in issue for issue in source["issues"])


def test_schema_is_closed_and_has_all_required_fields():
    schema = extraction_schema()
    assert schema["additionalProperties"] is False
    block = schema["properties"]["blocks"]["items"]
    row = block["properties"]["rows"]["items"]
    assert row["properties"]["fields"]["required"] == ["pos", "name", "mark", "code", "supplier", "unit", "qty", "mass", "note"]


def test_parseable_pipe_table_requires_every_recognized_cell(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "| Поз. | Наименование | Код | Ед. изм. | Количество | Масса | Примечание |\n"
                                      "|---|---|---|---|---|---|---|\n"
                                      "| 1 | Кабель | C-1 | м | 20 | 2 | Для щита |"))
    path = tmp_path / "table.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    header, separator, data = source["blocks"][0]["lines"]
    row = _row("blk_aaaaaaaa", data["line"], data["text"], fields={
        "pos": "1", "name": "Кабель", "code": "C-1", "unit": "м", "qty": "2", "mass": "2", "note": "Для щита",
    })["rows"][0]
    payload = {"blocks": [{"block_id": "blk_aaaaaaaa", "rows": [row],
                           "ignored": [{"line": header["line"], "reason": "header"},
                                       {"line": separator["line"], "reason": "separator"}], "issues": []}]}
    result = validate_extraction(source, payload)
    assert not result["complete"]
    assert any("Markdown table cell is missing or altered in qty" in issue for issue in result["issues"])


def test_table_rejects_numeric_substring_and_empty_rows(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "| Наименование | Количество |\n| Кабель | 20 |"))
    path = tmp_path / "table.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    header, data = source["blocks"][0]["lines"]
    empty = {"section": "", "parent": "", "fields": {k: "" for k in ("pos", "name", "mark", "code", "supplier", "unit", "qty", "mass", "note")},
             "evidence": [{"line": data["line"], "quote": data["text"]}]}
    short_qty = dict(empty)
    short_qty["fields"] = dict(empty["fields"], name="Кабель", qty="2")
    payload = {"blocks": [{"block_id": "blk_aaaaaaaa", "rows": [empty, short_qty],
                           "ignored": [{"line": header["line"], "reason": "header"}], "issues": []}]}
    result = validate_extraction(source, payload)
    assert not result["complete"]
    assert any("Empty extracted row" in issue for issue in result["issues"])
    assert any("qty" in issue and "missing or altered" in issue for issue in result["issues"])


def test_line_cannot_be_ignored_and_used_as_evidence_in_either_order(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "1 | Кабель | 12"))
    path = tmp_path / "spec.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    line = source["blocks"][0]["lines"][0]
    row = _row("blk_aaaaaaaa", line["line"], line["text"], fields={"name": "Кабель"})["rows"][0]
    payload = {"blocks": [{"block_id": "blk_aaaaaaaa", "rows": [row],
                           "ignored": [{"line": line["line"], "reason": "section"}], "issues": []}]}
    result = validate_extraction(source, payload)
    assert any("both evidence and ignored" in issue for issue in result["issues"])


def test_exact_quote_required_and_source_issues_are_preserved(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "Кабель   силовой"))
    path = tmp_path / "spec.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    line = source["blocks"][0]["lines"][0]
    bad_payload = {"blocks": [{"block_id": "blk_aaaaaaaa", "rows": [{
        "section": "", "parent": "", "fields": {k: "" for k in ("pos", "name", "mark", "code", "supplier", "unit", "qty", "mass", "note")},
        "evidence": [{"line": line["line"], "quote": "Кабель силовой"}],
    }], "ignored": [], "issues": []}]}
    source["issues"].append("upstream source issue")
    result = validate_extraction(source, bad_payload)
    assert "upstream source issue" in result["issues"]
    assert any("exact source substring" in issue for issue in result["issues"])


def test_bad_selectors_and_page_without_blocks_fail_closed(tmp_path: Path):
    path = tmp_path / "spec.md"
    path.write_text(_md("", pages="## Page 1\n"), encoding="utf-8")
    source = load_source(path, label="OLD", pages=[1, 1])
    assert not source["complete"]
    assert any("duplicates" in issue for issue in source["issues"])
    assert any("no parsed blocks" in issue for issue in source["issues"])
    invalid = load_source(path, label="OLD", pages=[True])
    assert not invalid["complete"]
    assert any("positive integers" in issue for issue in invalid["issues"])


def test_unknown_pipe_table_fails_closed_and_malformed_payload_ids_do_not_crash(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "| Альфа | Бета |\n|---|---|\n| 1 | 2 |"))
    path = tmp_path / "unknown-table.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    malformed = validate_extraction(source, {"blocks": [{"block_id": [], "rows": [], "ignored": [], "issues": []}]})
    assert not malformed["complete"]
    assert any("Unrecognized Markdown table header" in issue for issue in malformed["issues"])
    assert any("block ids must be strings" in issue for issue in malformed["issues"])


def test_partial_and_duplicate_table_headers_are_rejected(tmp_path: Path):
    for idx, header in enumerate((
        "| Поз. | Наименование | Непонятный показатель |",
        "| Поз. | Наименование | Марка | Тип |",
    )):
        md = _md(_block("blk_aaaaaaaa", header + "\n|---|---|---|---|\n| 1 | Кабель | А | Б |"))
        path = tmp_path / f"partial-{idx}.md"
        path.write_text(md, encoding="utf-8")
        source = load_source(path, label="OLD")
        line = source["blocks"][0]["lines"][0]
        row_line = source["blocks"][0]["lines"][-1]
        row = _row("blk_aaaaaaaa", row_line["line"], row_line["text"], fields={"pos": "1", "name": "Кабель"})["rows"][0]
        result = validate_extraction(source, {"blocks": [{"block_id": "blk_aaaaaaaa", "rows": [row],
            "ignored": [{"line": line["line"], "reason": "header"},
                        {"line": line["line"] + 1, "reason": "separator"}], "issues": []}]})
        assert not result["complete"]
        assert any("Unrecognized Markdown table header" in issue for issue in result["issues"])


def test_active_table_row_width_mismatch_is_rejected(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "| Наименование | Количество |\n|---|---|\n| Кабель | 20 | лишнее"))
    path = tmp_path / "width.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    lines = source["blocks"][0]["lines"]
    row = _row("blk_aaaaaaaa", lines[2]["line"], lines[2]["text"], fields={"name": "Кабель", "qty": "20"})["rows"][0]
    result = validate_extraction(source, {"blocks": [{"block_id": "blk_aaaaaaaa", "rows": [row],
        "ignored": [{"line": lines[0]["line"], "reason": "header"},
                    {"line": lines[1]["line"], "reason": "separator"}], "issues": []}]})
    assert not result["complete"]
    assert any("row width differs" in issue for issue in result["issues"])


def test_all_scope_detects_orphan_text_and_empty_pages(tmp_path: Path):
    orphan_md = "# Document: Spec.pdf\nPath: X\nGenerated: today\n---\n## Page 1\nОрphan data 12\n" + _block("blk_aaaaaaaa", "1 | Кабель | 12")
    path = tmp_path / "orphan.md"
    path.write_text(orphan_md, encoding="utf-8")
    source = load_source(path, label="OLD")
    assert not source["complete"]
    assert any("Unparsed content in all-block scope" in issue for issue in source["issues"])
    empty_page = "# Document: Spec.pdf\nPath: X\nGenerated: today\n---\n## Page 1\n" + _block("blk_aaaaaaaa", "1 | Кабель | 12") + "## Page 2\n"
    path.write_text(empty_page, encoding="utf-8")
    source = load_source(path, label="OLD")
    assert any("Page in all-block scope has no parsed blocks: 2" in issue for issue in source["issues"])


def test_plain_text_cannot_be_hidden_as_header_or_unconfirmed_section(tmp_path: Path):
    md = _md(_block("blk_aaaaaaaa", "Кабель 12\n1 | Кабель | 12"))
    path = tmp_path / "prose.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD")
    prose, data = source["blocks"][0]["lines"]
    row = _row("blk_aaaaaaaa", data["line"], data["text"], fields={"pos": "1", "name": "Кабель", "qty": "12"})["rows"][0]
    for reason in ("header", "section", "separator"):
        payload = {"blocks": [{"block_id": "blk_aaaaaaaa", "rows": [row],
            "ignored": [{"line": prose["line"], "reason": reason}], "issues": []}]}
        result = validate_extraction(source, payload)
        assert not result["complete"]
        assert any("hidden as" in issue or "Unconfirmed section" in issue for issue in result["issues"])


def test_selection_metadata_document_fallback_and_root_schema(tmp_path: Path):
    md = "## Page 1\n" + _block("blk_aaaaaaaa", "Кабель")
    path = tmp_path / "prepared-results.md"
    path.write_text(md, encoding="utf-8")
    source = load_source(path, label="OLD", block_ids=["blk_aaaaaaaa"])
    assert source["document"] == "prepared-results.md"
    assert source["selection"] == {"pages": [1], "block_ids": ["blk_aaaaaaaa"], "mode": "block_ids"}
    line = source["blocks"][0]["lines"][0]
    payload = {"blocks": [{"block_id": "blk_aaaaaaaa", "rows": [_row("blk_aaaaaaaa", line["line"], line["text"], fields={"name": "Кабель"})["rows"][0]], "ignored": [], "issues": []}]}
    payload["unexpected"] = True
    validated = validate_extraction(source, payload)
    assert not validated["complete"]
    assert validated["selection"] == source["selection"]
    assert validated["scope"] == source["scope"]
    assert any("unexpected top-level keys" in issue for issue in validated["issues"])
