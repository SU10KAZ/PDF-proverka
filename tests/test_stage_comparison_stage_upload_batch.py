from __future__ import annotations

import io
import zipfile
from pathlib import Path

import fitz
import pytest

from backend.app.services.stage_comparison import scanner, stage_upload


def _pdf_bytes(label: str) -> bytes:
    document = fitz.open()
    page = document.new_page(width=200, height=120)
    page.insert_text((20, 40), label)
    payload = document.tobytes()
    document.close()
    return payload


def _folder_payload(label: str):
    return [(io.BytesIO(_pdf_bytes(label)), f"bundle/{label}.pdf")]


def _zip_bytes(files: dict[str, bytes]) -> bytes:
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return payload.getvalue()


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


def test_folder_upload_error_names_the_failed_zip(comparison_dir):
    invalid_project = _zip_bytes({"readme.txt": b"PDF is missing"})

    with pytest.raises(
        stage_upload.StageUploadError,
        match=r"broken-project\.zip: Архив не содержит PDF-файлов",
    ):
        stage_upload.replace_stage_from_folder(
            "object-1",
            "stage_1",
            [(io.BytesIO(invalid_project), "selected/broken-project.zip")],
            "broken-project",
        )


def test_batch_upload_can_discard_intermediate_backups(comparison_dir):
    stage_upload.replace_stage_from_folder(
        "object-1", "stage_1", _folder_payload("first"), "first"
    )
    retained = stage_upload.replace_stage_from_folder(
        "object-1", "stage_1", _folder_payload("second"), "second", True
    )
    backup_root = comparison_dir / "_stage_upload_backups"
    retained_backups = set(backup_root.glob("stage_1_*"))

    assert retained["backup_path"]
    assert Path(retained["backup_path"]).is_dir()
    assert len(retained_backups) == 1

    without_intermediate = stage_upload.replace_stage_from_folder(
        "object-1", "stage_1", _folder_payload("third"), "third", False
    )

    assert without_intermediate["backup_path"] is None
    assert set(backup_root.glob("stage_1_*")) == retained_backups
    entries, warnings = scanner.scan_stage_folder(comparison_dir / "stage_1")
    assert warnings == []
    assert {entry.filename for entry in entries} == {"first.pdf", "second.pdf", "third.pdf"}
