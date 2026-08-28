from __future__ import annotations

import json

import fitz

from backend.app.services.stage_comparison import text_comparison
from backend.app.services.stage_comparison.production_text_flow import prepare_text_scope


def _write_document(root, name: str, page_texts: list[str]):
    directory = root / name
    directory.mkdir()
    pdf_path = directory / f"{name}.pdf"
    document = fitz.open()
    for text in page_texts:
        page = document.new_page(width=400, height=240)
        page.insert_text((36, 72), text, fontsize=12)
    document.save(pdf_path)
    document.close()
    markdown_path = directory / "document.md"
    markdown_path.write_text(
        "\n\n".join(
            f"## Page {page}\n\n"
            f"### BLOCK #1 [TEXT] : {name}-p{page}\n\n"
            f"{text}"
            for page, text in enumerate(page_texts, 1)
        ),
        encoding="utf-8",
    )
    return {
        "version_id": f"{name}-v1",
        "pdf_path": str(pdf_path),
        "md_path": str(markdown_path),
    }


def _pair(tmp_path):
    return {
        "id": "real-pdf-pair",
        "left": _write_document(
            tmp_path,
            "left",
            ["LEFT PAGE ONE 220 V", "LEFT SELECTED PUMP 5 KW", "LEFT PAGE THREE"],
        ),
        "right": _write_document(
            tmp_path,
            "right",
            ["RIGHT PAGE ONE 380 V", "RIGHT PAGE TWO", "RIGHT SELECTED PUMP 7 KW"],
        ),
    }


def _indexes():
    return {
        "left": [
            {"pdf_page": page, "sheet_number": f"L{page}"}
            for page in range(1, 4)
        ],
        "right": [
            {"pdf_page": page, "sheet_number": f"R{page}"}
            for page in range(1, 4)
        ],
    }


def test_page_scope_extracts_and_locates_only_selected_real_pdf_pages(
    tmp_path, monkeypatch,
):
    pair = _pair(tmp_path)
    calls = []
    original = text_comparison.extract_document_fragments

    def capture(**kwargs):
        calls.append(set(kwargs.get("selected_pages") or []))
        return original(**kwargs)

    monkeypatch.setattr(text_comparison, "extract_document_fragments", capture)
    kwargs = {
        "pair": pair,
        "comparison_groups": [{
            "id": "selected",
            "left_pages": [2],
            "right_pages": [3],
            "relation_type": "USER_SELECTED",
            "status": "HIGH",
        }],
        "sheet_indexes": _indexes(),
        "fitz": fitz,
        "generated_at": "2026-08-28T00:00:00+00:00",
    }
    first = prepare_text_scope(**kwargs)
    second = prepare_text_scope(**kwargs)

    assert calls == [{2}, {3}, {2}, {3}]
    assert [item["pdf_page"] for item in first["fragments"]["left"]] == [2]
    assert [item["pdf_page"] for item in first["fragments"]["right"]] == [3]
    for side in ("left", "right"):
        fragment = first["fragments"][side][0]
        assert fragment["bboxes"]
        assert fragment["source_location"]["pdf_page"] == fragment["pdf_page"]
        assert fragment["sheet_number"] in {"L2", "R3"}
    assert first == second
    assert first["constraints"]["uses_model"] is False


def test_document_scope_reuses_signed_full_document_cache(tmp_path, monkeypatch):
    pair = _pair(tmp_path)
    cache_dir = tmp_path / "cache"
    calls = 0
    original = text_comparison.extract_document_fragments

    def capture(**kwargs):
        nonlocal calls
        calls += 1
        assert kwargs.get("selected_pages") is None
        return original(**kwargs)

    monkeypatch.setattr(text_comparison, "extract_document_fragments", capture)
    kwargs = {
        "pair": pair,
        "comparison_groups": [{
            "id": "document-selected",
            "left_pages": [2],
            "right_pages": [3],
            "relation_type": "MATCHED",
            "status": "HIGH",
        }],
        "sheet_indexes": _indexes(),
        "fitz": fitz,
        "generated_at": "2026-08-28T00:00:00+00:00",
        "document_cache_dir": cache_dir,
    }
    first = prepare_text_scope(**kwargs)
    second = prepare_text_scope(**kwargs)

    assert calls == 2  # LEFT + RIGHT on the first run only.
    assert first == second
    assert first["extraction"]["mode"] == "DOCUMENT_CACHE"
    assert len(first["extraction"]["document_cache_signatures"]) == 2
    assert {item["pdf_page"] for item in second["fragments"]["left"]} == {2}
    assert {item["pdf_page"] for item in second["fragments"]["right"]} == {3}
    artifacts = sorted(cache_dir.glob("*.json"))
    assert len(artifacts) == 2
    cached = json.loads(artifacts[0].read_text(encoding="utf-8"))
    assert cached["constraints"] == {"uses_model": False, "complete_document": True}
    assert {item["pdf_page"] for item in cached["fragments"]} == {1, 2, 3}


def test_document_cache_invalidates_on_same_version_content_change(
    tmp_path, monkeypatch,
):
    pair = _pair(tmp_path)
    cache_dir = tmp_path / "cache"
    groups = [{
        "id": "document-selected",
        "left_pages": [2],
        "right_pages": [3],
        "relation_type": "MATCHED",
        "status": "HIGH",
    }]
    prepare_text_scope(
        pair,
        groups,
        sheet_indexes=_indexes(),
        fitz=fitz,
        generated_at="2026-08-28T00:00:00+00:00",
        document_cache_dir=cache_dir,
    )
    left_markdown = pair["left"]["md_path"]
    with open(left_markdown, "a", encoding="utf-8") as stream:
        stream.write("\n")

    calls = 0
    original = text_comparison.extract_document_fragments

    def capture(**kwargs):
        nonlocal calls
        calls += 1
        return original(**kwargs)

    monkeypatch.setattr(text_comparison, "extract_document_fragments", capture)
    prepare_text_scope(
        pair,
        groups,
        sheet_indexes=_indexes(),
        fitz=fitz,
        generated_at="2026-08-28T00:00:00+00:00",
        document_cache_dir=cache_dir,
    )

    assert calls == 1  # Changed LEFT is rebuilt; unchanged RIGHT remains cached.
    assert len(list(cache_dir.glob("*.json"))) == 3
