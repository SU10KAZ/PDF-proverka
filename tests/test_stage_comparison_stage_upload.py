from __future__ import annotations

import io
import zipfile
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

from backend.app.services.stage_comparison import objects as objects_mod
from backend.app.services.stage_comparison import scanner
from backend.app.services.stage_comparison import stage_upload


def _zip_bytes(files: dict[str, bytes]) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    buf.seek(0)
    return buf


@pytest.fixture()
def comparison_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    v2_root = tmp_path / "projects_v2"
    (v2_root / "objects").mkdir(parents=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))
    test_objects = {
        "obj-256": {"id": "obj-256", "name": "256. Тестовый объект"},
        "obj-314": {"id": "obj-314", "name": "314. Событие 6.1 (Донстрой)"},
    }
    monkeypatch.setattr(
        stage_upload.object_service,
        "get_object_by_id",
        lambda object_id: test_objects.get(object_id),
    )
    monkeypatch.setattr(
        stage_upload.object_service,
        "list_objects",
        lambda: list(test_objects.values()),
    )
    return v2_root / "objects"


def test_uploads_two_stages_for_platform_object(comparison_root: Path) -> None:
    first = stage_upload.replace_stage_from_zip(
        "obj-256",
        "stage_1",
        _zip_bytes({"bundle/A.pdf": b"pdf-a", "bundle/A.md": b"md-a"}),
        "stage_1.zip",
    )
    assert first["ready_for_comparison"] is False
    assert first["pdf_count"] == 1

    second = stage_upload.replace_stage_from_zip(
        "obj-256",
        "stage_2",
        _zip_bytes({"bundle/A.pdf": b"pdf-b", "bundle/A.md": b"md-b"}),
        "stage_2.zip",
    )
    assert second["ready_for_comparison"] is True
    assert second["stage_pdf_counts"] == {"stage_1": 1, "stage_2": 1}

    obj_dir = comparison_root / "256_Testovyy_obekt" / "comparison"
    stage_1_doc = obj_dir / "stage_1" / "documents" / "A"
    stage_2_doc = obj_dir / "stage_2" / "documents" / "A"
    assert not (obj_dir / "stage_1" / "disciplines").exists()
    assert not (obj_dir / "stage_2" / "disciplines").exists()
    assert not (obj_dir / "stage_1" / "DOC").exists()
    assert not (obj_dir / "stage_2" / "DOC").exists()
    assert (stage_1_doc / "current_version.txt").read_text() == "v001"
    assert (stage_1_doc / "versions" / "v001" / "01_input" / "A.pdf").read_bytes() == b"pdf-a"
    assert (stage_1_doc / "versions" / "v001" / "02_work" / "document.pdf").read_bytes() == b"pdf-a"
    assert (stage_2_doc / "versions" / "v001" / "01_input" / "A.pdf").read_bytes() == b"pdf-b"
    assert (stage_2_doc / "versions" / "v001" / "02_work" / "document.pdf").read_bytes() == b"pdf-b"
    assert not (stage_1_doc / "versions" / "v001" / "04_review").exists()
    assert not (stage_1_doc / "versions" / "v001" / "05_export").exists()

    listed = objects_mod.list_objects()
    item = next(row for row in listed["items"] if row["name"] == "256. Тестовый объект")
    assert {stage["name"]: stage["pdf_count"] for stage in item["stages"]} == {
        "stage_1": 1,
        "stage_2": 1,
    }


def test_archives_of_different_objects_are_stored_in_separate_folders(
    comparison_root: Path,
) -> None:
    stage_upload.replace_stage_from_zip(
        "obj-256", "stage_1", _zip_bytes({"same.pdf": b"object-256"}), "256.zip"
    )
    stage_upload.replace_stage_from_zip(
        "obj-314", "stage_1", _zip_bytes({"same.pdf": b"object-314"}), "314.zip"
    )

    relative = Path("stage_1/documents/same/versions/v001/02_work/document.pdf")
    assert (comparison_root / "256_Testovyy_obekt" / "comparison" / relative).read_bytes() == b"object-256"
    assert (comparison_root / "314_Sobytie_6_1_Donstroy" / "comparison" / relative).read_bytes() == b"object-314"


def test_uploads_entire_browser_folder_in_one_request(comparison_root: Path) -> None:
    result = stage_upload.replace_stage_from_folder(
        "obj-256",
        "stage_1",
        [
            (io.BytesIO(b"pdf-a"), "selected/A.pdf"),
            (io.BytesIO(b"md-a"), "selected/A.md"),
            (io.BytesIO(b"pdf-b"), "selected/nested/B.pdf"),
        ],
        "selected",
    )

    assert result["upload_type"] == "folder"
    assert result["pdf_count"] == 2
    assert result["stage_pdf_counts"]["stage_1"] == 2
    root = comparison_root / "256_Testovyy_obekt" / "comparison" / "stage_1" / "documents"
    assert (root / "A" / "versions" / "v001" / "01_input" / "A.md").read_bytes() == b"md-a"
    assert (root / "B" / "versions" / "v001" / "02_work" / "document.pdf").read_bytes() == b"pdf-b"


def test_folder_selection_can_contain_zip_projects(comparison_root: Path) -> None:
    archive = _zip_bytes({"bundle/FromZip.pdf": b"pdf-zip", "bundle/FromZip.md": b"md-zip"})
    result = stage_upload.replace_stage_from_folder(
        "obj-256",
        "stage_1",
        [(archive, "selected/FromZip.zip")],
        "selected",
    )

    assert result["upload_type"] == "folder"
    assert result["pdf_count"] == 1
    doc = comparison_root / "256_Testovyy_obekt" / "comparison" / "stage_1" / "documents" / "FromZip"
    assert (doc / "versions" / "v001" / "02_work" / "document.pdf").read_bytes() == b"pdf-zip"
    assert (doc / "versions" / "v001" / "02_work" / "document.md").read_bytes() == b"md-zip"


def test_folder_upload_rejects_unsafe_relative_path(comparison_root: Path) -> None:
    with pytest.raises(stage_upload.StageUploadError, match="Небезопасный путь"):
        stage_upload.replace_stage_from_folder(
            "obj-256",
            "stage_1",
            [(io.BytesIO(b"pdf"), "../escape.pdf")],
            "selected",
        )


def test_reupload_creates_next_document_version_and_keeps_backup(comparison_root: Path) -> None:
    stage_upload.replace_stage_from_zip(
        "obj-256", "stage_1", _zip_bytes({"same.pdf": b"old"}), "old.zip"
    )
    result = stage_upload.replace_stage_from_zip(
        "obj-256", "stage_1", _zip_bytes({"same.pdf": b"new"}), "new.zip"
    )

    backup = Path(result["backup_path"])
    assert backup.is_dir()
    backup_doc = backup / "documents" / "same"
    assert (backup_doc / "versions" / "v001" / "02_work" / "document.pdf").read_bytes() == b"old"

    current_doc = (
        comparison_root / "256_Testovyy_obekt" / "comparison" / "stage_1"
        / "documents" / "same"
    )
    assert (current_doc / "current_version.txt").read_text() == "v002"
    assert (current_doc / "versions" / "v001" / "02_work" / "document.pdf").read_bytes() == b"old"
    assert (current_doc / "versions" / "v002" / "02_work" / "document.pdf").read_bytes() == b"new"

    entries, warnings = scanner.scan_stage_folder(current_doc.parents[1])
    assert warnings == []
    assert len(entries) == 1
    assert entries[0].to_dict()["filename"] == "same.pdf"
    assert entries[0].version_id == "v002"


def test_legacy_raw_stage_is_migrated_before_new_archive(comparison_root: Path) -> None:
    legacy_stage = comparison_root / "256_Testovyy_obekt" / "comparison" / "stage_1"
    legacy_stage.mkdir(parents=True)
    (legacy_stage / "legacy.pdf").write_bytes(b"legacy")

    result = stage_upload.replace_stage_from_zip(
        "obj-256", "stage_1", _zip_bytes({"new.pdf": b"new"}), "new.zip"
    )

    assert Path(result["backup_path"]).is_dir()
    entries, warnings = scanner.scan_stage_folder(legacy_stage)
    assert warnings == []
    assert {entry.to_dict()["filename"] for entry in entries} == {"legacy.pdf", "new.pdf"}
    assert result["stage_pdf_counts"]["stage_1"] == 2


@pytest.mark.parametrize(
    ("files", "message"),
    [
        ({"../escape.pdf": b"pdf"}, "Небезопасный путь"),
        ({"readme.md": b"no pdf"}, "не содержит PDF"),
    ],
)
def test_rejects_unsafe_or_pdf_less_archive(
    comparison_root: Path,
    files: dict[str, bytes],
    message: str,
) -> None:
    with pytest.raises(stage_upload.StageUploadError, match=message):
        stage_upload.replace_stage_from_zip(
            "obj-256", "stage_1", _zip_bytes(files), "stage.zip"
        )


def test_rejects_unknown_object(comparison_root: Path) -> None:
    with pytest.raises(stage_upload.StageUploadError, match="не найден"):
        stage_upload.replace_stage_from_zip(
            "missing", "stage_1", _zip_bytes({"A.pdf": b"pdf"}), "stage.zip"
        )


def test_allowlist_accepts_only_object_local_comparison_branch(
    comparison_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.app.services.stage_comparison import store

    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(comparison_root.parent / "legacy"))
    allowed = comparison_root / "256_Testovyy_obekt" / "comparison" / "stage_1" / "documents" / "A.pdf"
    store.assert_path_in_allowlist(str(allowed))
    with pytest.raises(PermissionError, match="path_outside_allowlist"):
        store.assert_path_in_allowlist(
            str(comparison_root / "256_Testovyy_obekt" / "disciplines" / "AR" / "secret.pdf")
        )


def test_rejects_symlink_used_as_stage_directory(comparison_root: Path) -> None:
    object_dir = comparison_root / "256_Testovyy_obekt" / "comparison"
    object_dir.mkdir(parents=True)
    outside = comparison_root.parent / "outside"
    outside.mkdir()
    (object_dir / "stage_1").symlink_to(outside, target_is_directory=True)

    with pytest.raises(stage_upload.StageUploadError, match="Символическая ссылка"):
        stage_upload.replace_stage_from_zip(
            "obj-256", "stage_1", _zip_bytes({"A.pdf": b"pdf"}), "stage.zip"
        )


@pytest.mark.asyncio
async def test_upload_endpoint_accepts_multipart_zip(
    comparison_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.app.api.routers import stage_comparison as router_mod

    # В текущем test stack AnyIO/TestClient зависает на любом router
    # run_in_threadpool (это также воспроизводилось старым endpoint сопоставления
    # тестом). Сервис отдельно покрыт синхронными тестами; здесь проверяем
    # multipart routing и контракт endpoint без инфраструктурного deadlock.
    async def _inline_threadpool(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(router_mod, "run_in_threadpool", _inline_threadpool)
    app = FastAPI()
    app.include_router(router_mod.router)
    payload = _zip_bytes({"A.pdf": b"pdf"}).getvalue()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/stage-comparison/objects/obj-256/stages/stage_1/upload",
            files={"file": ("stage_1.zip", payload, "application/zip")},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["object_name"] == "256. Тестовый объект"
    assert body["stage"] == "stage_1"
    assert body["pdf_count"] == 1


@pytest.mark.asyncio
async def test_upload_folder_endpoint_accepts_multiple_files(
    comparison_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.app.api.routers import stage_comparison as router_mod

    async def _inline_threadpool(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(router_mod, "run_in_threadpool", _inline_threadpool)
    app = FastAPI()
    app.include_router(router_mod.router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/stage-comparison/objects/obj-256/stages/stage_1/upload-folder",
            data={
                "relative_paths": '["selected/A.pdf", "selected/A.md"]',
                "folder_name": "selected",
            },
            files=[
                ("files", ("A.pdf", b"pdf", "application/pdf")),
                ("files", ("A.md", b"md", "text/markdown")),
            ],
        )

    assert response.status_code == 200
    body = response.json()
    assert body["upload_type"] == "folder"
    assert body["files_count"] == 2
    assert body["pdf_count"] == 1
