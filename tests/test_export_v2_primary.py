"""B3: audit package export from projects_v2-primary."""
from __future__ import annotations

import io
import json
import sys
import types
import zipfile
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"
_V2DIR = "AUDIT_PROJECTS_V2_DIR"


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _make_v2_export_doc(v2: Path, *, with_findings: bool = True) -> Path:
    code = "DOC-EXP"
    doc = v2 / "objects" / "OBJ_EXP" / "disciplines" / "KJ" / "documents" / code
    _write_json(v2 / "objects" / "OBJ_EXP" / "object.json", {"object_id": "obj-exp", "display_name": "Export Object"})
    _write_json(doc / "document.json", {
        "schema_version": 1,
        "document_code": code,
        "object_id": "obj-exp",
        "discipline": "KJ",
        "versions": [{"version_id": "v001", "version_no": 1, "label": "V1"}],
        "current_version": "v001",
    })
    (doc / "current_version.txt").write_text("v001\n", encoding="utf-8")
    vdir = doc / "versions" / "v001"
    _write_json(vdir / "version.json", {
        "version_id": "v001",
        "version_no": 1,
        "project_info": {"name": "DOC Export", "section": "KJ", "description": "v2 package"},
    })
    (vdir / "01_input").mkdir(parents=True, exist_ok=True)
    (vdir / "01_input" / "DOC-EXP.pdf").write_bytes(b"%PDF-export")
    (vdir / "01_input" / "DOC-EXP_document.md").write_text("# original md", encoding="utf-8")
    (vdir / "02_work").mkdir(parents=True, exist_ok=True)
    (vdir / "02_work" / "document.md").write_text("# normalized md", encoding="utf-8")
    latest = vdir / "03_analysis" / "latest"
    _write_json(latest / "02_text_analysis.json", {"text": True})
    _write_json(latest / "01_blocks_analysis.json", {"blocks": []})
    if with_findings:
        _write_json(latest / "03_findings.json", {"findings": [{"id": "F-1", "severity": "КРИТ"}]})
    _write_json(latest / "norm_checks.json", {"checks": []})
    _write_json(latest / "optimization.json", {"items": []})
    _write_json(latest / "document_graph.json", {"pages": []})
    _write_json(latest / "blocks_gemma_100" / "index.json", {"blocks": []})
    _write_json(latest / "expert_review.json", {"decisions": []})
    return doc


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
async def test_export_v2_primary_contains_pdf_latest_readme_and_excel(monkeypatch, tmp_path):
    from backend.app.api.routers import export

    v2 = tmp_path / "projects_v2"
    _make_v2_export_doc(v2)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))
    monkeypatch.setattr(export.subprocess, "run", _fake_excel_run)

    response = await export.download_audit_package("DOC-EXP")
    body = await _response_bytes(response)

    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        names = set(zf.namelist())
        assert "DOC-EXP.pdf" in names
        assert "document.md" in names
        assert "DOC-EXP_document.md" in names
        assert "03_findings.json" in names
        assert "02_text_analysis.json" in names
        assert "01_blocks_analysis.json" in names
        assert "norm_checks.json" in names
        assert "optimization.json" in names
        assert "document_graph.json" in names
        assert "blocks/index.json" in names
        assert "expert_review.json" in names
        assert "audit_report.xlsx" in names
        assert json.loads(zf.read("project_info.json").decode("utf-8"))["name"] == "DOC Export"
        readme = zf.read("README.md").decode("utf-8")
        assert "DOC Export" in readme
        assert "Всего замечаний" in readme


@pytest.mark.asyncio
async def test_export_v2_primary_empty_latest_returns_404(monkeypatch, tmp_path):
    from backend.app.api.routers import export

    v2 = tmp_path / "projects_v2"
    _make_v2_export_doc(v2, with_findings=False)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))

    with pytest.raises(HTTPException) as exc:
        await export.download_audit_package("DOC-EXP")
    assert exc.value.status_code == 404
