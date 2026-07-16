import json
from pathlib import Path

from backend.app.services.common import discipline_service, object_service, project_service


def _seed_objects_file(path: Path, projects_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({
            "objects": [{
                "id": "existing",
                "name": "Existing",
                "projects_dir": str(projects_root / "Existing"),
                "created_at": "2026-01-01T00:00:00+00:00",
            }],
            "current_id": "existing",
        }),
        encoding="utf-8",
    )


def test_add_object_creates_v2_scaffold_without_legacy_directory(tmp_path, monkeypatch):
    objects_file = tmp_path / "backend" / "app" / "data" / "objects.json"
    projects_root = tmp_path / "projects"
    v2_root = tmp_path / "projects_v2"
    _seed_objects_file(objects_file, projects_root)

    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)
    monkeypatch.setattr("backend.app.core.config.PROJECTS_DIR", projects_root)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    obj = object_service.add_object("Событие 6.1 (Донстрой)")

    logical_legacy_path = projects_root / "Событие 6.1 (Донстрой)"
    assert obj["projects_dir"] == str(logical_legacy_path)
    assert not logical_legacy_path.exists()

    object_dir = v2_root / "objects" / "Sobytie_6_1_Donstroy"
    metadata = json.loads((object_dir / "object.json").read_text(encoding="utf-8"))
    assert metadata["object_id"] == obj["id"]
    assert metadata["display_name"] == obj["name"]
    assert metadata["legacy_path"] == str(logical_legacy_path)
    assert (object_dir / "DOC").is_dir()
    for code in discipline_service.get_supported_codes():
        assert (object_dir / "disciplines" / code / "documents").is_dir()

    result = project_service.save_uploaded_project_folder(
        object_id=obj["id"],
        discipline="EOM",
        project_name="Test document",
        files=[("Test document.pdf", b"%PDF-1.4\n")],
    )
    assert result["project_id"] == "EOM/Test document"
    assert not logical_legacy_path.exists()
    version_dir = (
        object_dir / "disciplines" / "EOM" / "documents" / "Test document"
        / "versions" / "v001"
    )
    for subdir in ("01_input", "02_work", "03_analysis", "04_review", "05_export", "99_service"):
        assert (version_dir / subdir).is_dir()


def test_add_object_keeps_legacy_creation_outside_v2_primary(tmp_path, monkeypatch):
    objects_file = tmp_path / "backend" / "app" / "data" / "objects.json"
    projects_root = tmp_path / "projects"
    _seed_objects_file(objects_file, projects_root)

    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)
    monkeypatch.setattr("backend.app.core.config.PROJECTS_DIR", projects_root)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "legacy")

    obj = object_service.add_object("Legacy object")

    assert Path(obj["projects_dir"]).is_dir()


def test_v2_primary_upload_keeps_object_id_after_display_name_rename(tmp_path, monkeypatch):
    """A temp staging basename must not create a hash-id orphan object."""
    objects_file = tmp_path / "backend" / "app" / "data" / "objects.json"
    projects_root = tmp_path / "projects"
    v2_root = tmp_path / "projects_v2"
    obj = {
        "id": "37022bfb",
        "name": "314. Событие 6.1 (Донстрой)",
        # The logical legacy path intentionally retains the pre-rename name.
        "projects_dir": str(projects_root / "Событие 6.1 (Донстрой)"),
        "created_at": "2026-07-13T17:48:51+00:00",
    }
    objects_file.parent.mkdir(parents=True, exist_ok=True)
    objects_file.write_text(
        json.dumps({"objects": [obj], "current_id": obj["id"]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)
    monkeypatch.setattr("backend.app.core.config.PROJECTS_DIR", projects_root)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    expected_object_dir = object_service._create_v2_object_scaffold(obj)
    result = project_service.save_uploaded_project_folder(
        object_id=obj["id"],
        discipline="OV",
        project_name="ПД-TEST_V1",
        files=[("ПД-TEST_V1.pdf", b"%PDF-1.4\n")],
    )

    assert result["project_id"] == "OV/ПД-TEST_V1"
    document_json = (
        expected_object_dir / "disciplines" / "OV" / "documents"
        / "ПД-TEST_V1" / "document.json"
    )
    metadata = json.loads(document_json.read_text(encoding="utf-8"))
    assert metadata["object_id"] == obj["id"]
    assert not (v2_root / "objects" / "Sobytie_6_1_Donstroy").exists()
    assert not Path(obj["projects_dir"]).exists()
