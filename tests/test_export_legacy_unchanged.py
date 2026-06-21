"""B3: legacy audit package export remains unchanged."""
from __future__ import annotations

import io
import json
import sys
import types
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


async def _response_bytes(response) -> bytes:
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk if isinstance(chunk, bytes) else chunk.encode())
    return b"".join(chunks)


def _fake_excel_run(args, **kwargs):
    out = Path(args[args.index("--out") + 1])
    out.write_bytes(b"xlsx")
    return types.SimpleNamespace(returncode=0)


@pytest.mark.asyncio
async def test_export_legacy_package_shape_unchanged(monkeypatch, tmp_path):
    from backend.app.api.routers import export

    project = tmp_path / "projects" / "OBJ" / "KJ" / "DOC-LEG"
    output = project / "_output"
    project.mkdir(parents=True)
    _write_json(project / "project_info.json", {"name": "DOC Legacy", "section": "KJ"})
    (project / "DOC-LEG_document.md").write_text("# md", encoding="utf-8")
    _write_json(output / "03_findings.json", {"findings": [{"id": "F-1"}]})
    _write_json(output / "01_text_analysis.json", {"text": True})
    _write_json(output / "blocks" / "index.json", {"blocks": []})
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "legacy")
    monkeypatch.setattr(export, "resolve_project_dir", lambda project_id: project)
    monkeypatch.setattr(export.version_service, "get_version_dir", lambda project_dir, project_id, version_id=None: project)
    monkeypatch.setattr(export.subprocess, "run", _fake_excel_run)

    response = await export.download_audit_package("DOC-LEG")
    body = await _response_bytes(response)

    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        names = set(zf.namelist())
        assert "project_info.json" in names
        assert "DOC-LEG_document.md" in names
        assert "03_findings.json" in names
        assert "01_text_analysis.json" in names
        assert "blocks/index.json" in names
        assert "audit_report.xlsx" in names
        assert "README.md" in names
