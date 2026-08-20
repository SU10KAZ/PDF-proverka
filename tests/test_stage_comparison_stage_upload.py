from __future__ import annotations

import io
from pathlib import Path

import fitz
import pytest

from backend.app.services.stage_comparison import scanner, stage_storage, stage_upload


def _pdf_bytes(label: str) -> bytes:
    document = fitz.open()
    page = document.new_page(width=200, height=120)
    page.insert_text((20, 40), label)
    payload = document.tobytes()
    document.close()
    return payload


@pytest.fixture
def comparison_dir(tmp_path, monkeypatch) -> Path:
    root = tmp_path / "projects_v2" / "objects" / "Object" / "comparison"
    obj = {"id": "object-1", "name": "Object", "projects_dir": str(tmp_path / "legacy")}
    monkeypatch.setattr(
        stage_upload,
        "resolve_object_dir",
        lambda object_id, create=False: (obj, root),
    )
    return root


def _folder_payload(label: str):
    pdf = _pdf_bytes(label)
    return [
        (io.BytesIO(pdf), f"bundle/{label}.pdf"),
        (io.BytesIO(b"# source markdown"), f"bundle/{label}.md"),
        (io.BytesIO(b"<p>source html</p>"), f"bundle/{label}.html"),
        (io.BytesIO(b'{"pages": []}'), f"bundle/{label}.json"),
        (io.BytesIO(b'{"blocks": []}'), f"bundle/{label}.blocks.json"),
    ]


def test_stage_1_and_stage_2_upload_preserve_sources(comparison_dir):
    first = stage_upload.replace_stage_from_folder(
        "object-1", "stage_1", _folder_payload("design"), "design-folder"
    )
    second = stage_upload.replace_stage_from_folder(
        "object-1", "stage_2", _folder_payload("working"), "working-folder"
    )

    assert first["status"] == second["status"] == "ok"
    assert second["ready_for_comparison"] is True
    for stage_name, stem in (("stage_1", "design"), ("stage_2", "working")):
        stage_dir = comparison_dir / stage_name
        assert stage_storage.is_versioned_stage(stage_dir)
        entries, warnings = scanner.scan_stage_folder(stage_dir)
        assert warnings == []
        assert len(entries) == 1
        assert entries[0].filename == f"{stem}.pdf"
        version_dir = entries[0].pdf_path.parents[1]
        input_names = {path.name for path in (version_dir / "01_input").iterdir()}
        assert {f"{stem}.pdf", f"{stem}.md", f"{stem}.html", f"{stem}.json", f"{stem}.blocks.json"} <= input_names
        assert (version_dir / "02_work" / "document.pdf").is_file()


def test_repeat_upload_creates_a_new_version(comparison_dir):
    stage_upload.replace_stage_from_folder(
        "object-1", "stage_1", _folder_payload("design"), "first"
    )
    stage_upload.replace_stage_from_folder(
        "object-1", "stage_1", _folder_payload("design"), "second"
    )

    document_dir = comparison_dir / "stage_1" / "documents" / "design"
    assert (document_dir / "versions" / "v001" / "01_input" / "design.pdf").is_file()
    assert (document_dir / "versions" / "v002" / "01_input" / "design.pdf").is_file()
    assert (document_dir / "current_version.txt").read_text(encoding="utf-8") == "v002"
    assert list((comparison_dir / "_stage_upload_backups").glob("stage_1_*"))
