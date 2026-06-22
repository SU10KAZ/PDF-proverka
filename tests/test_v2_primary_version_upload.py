from __future__ import annotations

import io
import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_PDF_BYTES = b"%PDF-1.4\n%v2-upload\n%%EOF\n"
_MD_BYTES = "## СТРАНИЦА 1\n\n### [TEXT b1]\nUploaded MD.\n".encode("utf-8")
_RESULT_BYTES = json.dumps({"pages": []}, ensure_ascii=False).encode("utf-8")


def _set_v2_env(monkeypatch, v2_root: Path) -> None:
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "projects_v2")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))


def _reset_project_cache(monkeypatch, legacy_root: Path) -> None:
    import backend.app.services.common.project_service as ps

    legacy_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: legacy_root)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})


def _make_v2_doc(v2_root: Path, doc_code: str = "DOC-UPLOAD") -> Path:
    doc_dir = v2_root / "objects" / "OBJ" / "disciplines" / "GP" / "documents" / doc_code
    version_dir = doc_dir / "versions" / "v001"
    for subdir in (
        version_dir / "01_input",
        version_dir / "02_work",
        version_dir / "03_analysis" / "latest",
        version_dir / "04_review",
        version_dir / "05_export",
    ):
        subdir.mkdir(parents=True, exist_ok=True)
    (doc_dir / "document.json").write_text(json.dumps({
        "schema_version": 1,
        "document_code": doc_code,
        "object_folder": "OBJ",
        "discipline": "GP",
        "current_version": "v001",
        "version_ids": ["v001"],
        "versions": [{
            "version_id": "v001",
            "version_no": 1,
            "label": "V1",
            "status": "source_only",
            "source": "test",
        }],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    (doc_dir / "current_version.txt").write_text("v001", encoding="utf-8")
    info = {
        "project_id": doc_code,
        "document_code": doc_code,
        "name": doc_code,
        "section": "GP",
        "pdf_file": "",
        "pdf_files": [],
        "md_files": [],
        "version_id": "v001",
    }
    (version_dir / "01_input" / "project_info.json").write_text(
        json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    (version_dir / "version.json").write_text(json.dumps({
        "schema_version": 1,
        "version_id": "v001",
        "version_no": 1,
        "label": "V1",
        "analysis_status": "source_only",
        "project_info": info,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    return doc_dir


def _upload(client: TestClient, project_id: str, version_id: str, files: list[tuple[str, bytes]]):
    payload = [
        ("files", (name, io.BytesIO(content), "application/octet-stream"))
        for name, content in files
    ]
    return client.post(f"/api/projects/{project_id}/versions/{version_id}/files", files=payload)


def test_save_files_to_version_v2_primary_writes_input_and_work(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_v2_doc(v2_root)
    _set_v2_env(monkeypatch, v2_root)

    from backend.app.services.common import version_service
    from backend.app.services.storage.projects_v2_source_resolver import resolve_version_source_files

    result = version_service.save_files_to_version(
        "DOC-UPLOAD",
        "v001",
        [
            ("Uploaded.pdf", _PDF_BYTES),
            ("Uploaded_document.md", _MD_BYTES),
            ("Uploaded_result.json", _RESULT_BYTES),
        ],
    )

    version_dir = doc_dir / "versions" / "v001"
    assert result["version_id"] == "v001"
    assert (version_dir / "01_input" / "Uploaded.pdf").read_bytes() == _PDF_BYTES
    assert (version_dir / "01_input" / "Uploaded_document.md").read_bytes() == _MD_BYTES
    assert (version_dir / "01_input" / "Uploaded_result.json").read_bytes() == _RESULT_BYTES
    assert not (version_dir / "Uploaded.pdf").exists()

    assert (version_dir / "02_work" / "document.pdf").read_bytes() == _PDF_BYTES
    assert (version_dir / "02_work" / "document.md").read_bytes() == _MD_BYTES
    assert (version_dir / "02_work" / "result.json").read_bytes() == _RESULT_BYTES

    input_info = json.loads((version_dir / "01_input" / "project_info.json").read_text(encoding="utf-8"))
    assert input_info["pdf_files"] == ["Uploaded.pdf"]
    assert input_info["pdf_file"] == "Uploaded.pdf"
    assert input_info["md_files"] == ["Uploaded_document.md"]
    assert input_info["md_file"] == "Uploaded_document.md"
    version_info = json.loads((version_dir / "version.json").read_text(encoding="utf-8"))["project_info"]
    assert version_info["pdf_file"] == "Uploaded.pdf"

    sources = resolve_version_source_files(version_dir, "DOC-UPLOAD")
    assert sources.pdf_path == version_dir / "02_work" / "document.pdf"
    assert sources.md_path == version_dir / "02_work" / "document.md"
    assert sources.result_json_path == version_dir / "02_work" / "result.json"

    readiness = version_service.version_audit_readiness("DOC-UPLOAD", "v001")
    assert readiness["can_run_audit"] is True
    assert readiness["pdf_count"] >= 1


def test_v2_only_version_endpoints_create_and_upload(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_v2_doc(v2_root, "DOC-ENDPOINT")
    _set_v2_env(monkeypatch, v2_root)
    _reset_project_cache(monkeypatch, tmp_path / "legacy_projects")

    from backend.app.main import app

    client = TestClient(app)
    create = client.post(
        "/api/projects/DOC-ENDPOINT/versions",
        json={"label": "V2", "source": "test", "status": "new", "comment": "upload smoke"},
    )
    assert create.status_code == 200, create.text
    body = create.json()
    assert body["version"]["version_id"] == "v002"
    assert body["latest_version_id"] == "v002"

    v002 = doc_dir / "versions" / "v002"
    assert (v002 / "01_input" / "project_info.json").is_file()
    assert (v002 / "02_work").is_dir()
    doc_json = json.loads((doc_dir / "document.json").read_text(encoding="utf-8"))
    assert doc_json["current_version"] == "v002"
    assert doc_json["version_ids"] == ["v001", "v002"]

    upload = _upload(client, "DOC-ENDPOINT", "v002", [("Endpoint.pdf", _PDF_BYTES)])
    assert upload.status_code == 200, upload.text
    assert (v002 / "01_input" / "Endpoint.pdf").read_bytes() == _PDF_BYTES
    assert (v002 / "02_work" / "document.pdf").read_bytes() == _PDF_BYTES
    assert not (tmp_path / "legacy_projects" / "DOC-ENDPOINT").exists()

    files = client.get("/api/projects/DOC-ENDPOINT/versions/v002/files")
    assert files.status_code == 200, files.text
    names = {row["name"] for row in files.json()["files"]}
    assert "02_work/document.pdf" in names
    assert files.json()["project_info"]["pdf_file"] == "Endpoint.pdf"


def test_storage_write_facade_scaffold_is_visible_to_v2_adapter(monkeypatch, tmp_path):
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
    from backend.app.services.storage.storage_write_facade import (
        StorageWriteFacade,
        V2Target,
        WRITE_MODE_V2_PRIMARY,
    )

    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", WRITE_MODE_V2_PRIMARY)
    target = V2Target("OBJ", "AR", "DOC-NEW", "v1")
    facade = StorageWriteFacade(v2_root=tmp_path)

    result = facade.save_input_bundle(target, [("New.pdf", _PDF_BYTES)])

    assert result.v2_ok is True
    adapter = ProjectsV2Adapter(tmp_path)
    doc = adapter.find_document_by_project_id("DOC-NEW")
    assert doc is not None
    doc_dir = Path(doc["doc_dir"])
    assert adapter.current_version_id(doc_dir) == "v001"
    versions = adapter.list_versions(doc_dir)
    assert [v["version_id"] for v in versions] == ["v001"]
    assert (doc_dir / "versions" / "v001" / "01_input" / "New.pdf").read_bytes() == _PDF_BYTES
