"""Фильтр загрузки: PDF того же размера, что у другой версии документа, не загружается.

На объекте 214 «Алия» новой версией загружали тот же самый PDF (в т.ч. под
новым номером «…_V4.pdf» = «…_V2.pdf»). Такую версию отклоняем с сообщением и
не создаём пустую версию-обрубок.
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.test_v2_primary_version_upload import (  # noqa: E402
    _make_v2_doc,
    _reset_project_cache,
    _set_v2_env,
)

_OLD_PDF = b"%PDF-1.4\n%old-edition\n%%EOF\n"
_SAME_SIZE_PDF = b"%PDF-1.4\n%new-edition\n%%EOF\n"   # другое содержимое, тот же размер
_NEW_PDF = b"%PDF-1.4\n%new-edition-longer\n%%EOF\n"

assert len(_OLD_PDF) == len(_SAME_SIZE_PDF) != len(_NEW_PDF)


def _doc_with_pdf(tmp_path, monkeypatch, code: str) -> Path:
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_v2_doc(v2_root, code)
    (doc_dir / "versions" / "v001" / "01_input" / "Old_V1.pdf").write_bytes(_OLD_PDF)
    _set_v2_env(monkeypatch, v2_root)
    return doc_dir


def _version_ids(doc_dir: Path) -> list[str]:
    return json.loads((doc_dir / "document.json").read_text(encoding="utf-8"))["version_ids"]


def _from_files(doc_dir: Path, tmp_path: Path, code: str, pdf: bytes, name: str = "New_V2.pdf"):
    from backend.app.services.common import version_service

    src = tmp_path / "incoming"
    src.mkdir(exist_ok=True)
    (src / name).write_bytes(pdf)
    return version_service.create_version_from_existing_files(
        code,
        candidate_files={"pdf": str(src / name)},
        source="upload_folder_modal",
        allowed_roots=[src],
        resolve_project_dir_fn=lambda pid, **kw: doc_dir,
    )


def test_same_size_pdf_rejected_and_no_empty_version(monkeypatch, tmp_path):
    doc_dir = _doc_with_pdf(tmp_path, monkeypatch, "DOC-SAME")
    from backend.app.services.common import version_service

    with pytest.raises(version_service.DuplicatePdfSizeError) as exc:
        _from_files(doc_dir, tmp_path, "DOC-SAME", _SAME_SIZE_PDF)

    msg = str(exc.value)
    assert "Версия не загружена" in msg
    assert "«New_V2.pdf»" in msg and "«Old_V1.pdf»" in msg and "версии V1" in msg
    assert f"{len(_OLD_PDF)} байт" in msg
    # версия-обрубок не создана
    assert _version_ids(doc_dir) == ["v001"]
    assert not (doc_dir / "versions" / "v002").exists()


def test_different_size_pdf_creates_version(monkeypatch, tmp_path):
    doc_dir = _doc_with_pdf(tmp_path, monkeypatch, "DOC-NEW")

    res = _from_files(doc_dir, tmp_path, "DOC-NEW", _NEW_PDF)

    assert res["version_id"] == "v002"
    assert (doc_dir / "versions" / "v002" / "01_input" / "New_V2.pdf").read_bytes() == _NEW_PDF


def test_duplicate_error_is_conflict_for_existing_handlers():
    from backend.app.services.common import version_service

    assert issubclass(version_service.DuplicatePdfSizeError, version_service.VersionFileConflictError)


def test_upload_endpoint_returns_409_with_message(monkeypatch, tmp_path):
    doc_dir = _doc_with_pdf(tmp_path, monkeypatch, "DOC-EP")
    _reset_project_cache(monkeypatch, tmp_path / "legacy_projects")
    from backend.app.main import app

    client = TestClient(app)
    create = client.post("/api/projects/DOC-EP/versions", json={"source": "test"})
    assert create.status_code == 200, create.text
    assert create.json()["version"]["version_id"] == "v002"

    resp = client.post(
        "/api/projects/DOC-EP/versions/v002/files",
        files=[("files", ("Copy.pdf", io.BytesIO(_SAME_SIZE_PDF), "application/pdf"))],
    )
    assert resp.status_code == 409, resp.text
    assert "совпадает с PDF уже загруженной версии" in resp.json()["detail"]
    assert not (doc_dir / "versions" / "v002" / "01_input" / "Copy.pdf").exists()


def test_reupload_into_same_version_is_not_compared_with_itself(monkeypatch, tmp_path):
    doc_dir = _doc_with_pdf(tmp_path, monkeypatch, "DOC-SELF")
    from backend.app.services.common import version_service

    res = version_service.save_files_to_version(
        "DOC-SELF", "v001", [("Old_V1.pdf", _OLD_PDF)],
        replace_existing=True, allow_v1_upload=True,
    )
    assert res["saved"] == ["Old_V1.pdf"]
    assert (doc_dir / "versions" / "v001" / "01_input" / "Old_V1.pdf").read_bytes() == _OLD_PDF


def test_version_read_failure_does_not_block_upload(monkeypatch, tmp_path):
    from backend.app.services.common import version_service

    def _boom(pid, **kw):
        raise RuntimeError("нет доступа")

    assert version_service.find_same_size_pdf_versions(
        "DOC-X", [("a.pdf", 10)], resolve_project_dir_fn=_boom,
    ) == []
