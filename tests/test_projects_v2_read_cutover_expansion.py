"""
Тесты read-cutover expansion: 3 дополнительных GET-endpoint'а на projects_v2.

  * GET /api/tiles/{project_id}/blocks                  (список блоков)
  * GET /api/tiles/{project_id}/blocks/image/{block_id} (PNG кропа, path-safe)
  * GET /api/projects/{project_id}/versions/{vid}/files (01_input listing)

Default-флаг ON → v2; `?storage=legacy` → legacy; v2-miss → 404 (без fallback);
read-only. Изоляция через AUDIT_PROJECTS_V2_DIR.
"""

from __future__ import annotations

import hashlib
import json
import sys
import urllib.parse
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
from backend.app.main import app  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"
client = TestClient(app)
Q = lambda s: urllib.parse.quote(s, safe="")


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _doc_with_blocks(v2, disc, code, *, run="run_x", n_blocks=3, status="complete"):
    doc = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
    _wj(doc / "document.json", {"document_code": code, "object_id": "0b540226",
        "discipline": disc, "kind": "plain",
        "versions": [{"version_id": "v001", "version_no": 1}], "current_version": "v001"})
    (doc / "current_version.txt").write_text("v001\n", encoding="utf-8")
    _wj(doc / "versions" / "v001" / "version.json",
        {"version_id": "v001", "version_no": 1, "analysis_status": status})
    # input files
    inp = doc / "versions" / "v001" / "01_input"
    inp.mkdir(parents=True, exist_ok=True)
    (inp / f"{code}.pdf").write_text("pdf", encoding="utf-8")
    (inp / f"{code}_document.md").write_text("md", encoding="utf-8")
    # blocks index + pngs under runs/<run>/blocks
    bd = doc / "versions" / "v001" / "03_analysis" / "runs" / run / "blocks"
    blocks = []
    for i in range(n_blocks):
        bid = f"AAA-BBB-{i:03d}"
        fn = f"block_{bid}.png"
        (bd / fn).parent.mkdir(parents=True, exist_ok=True)
        (bd / fn).write_bytes(b"\x89PNG\r\n" + bytes([i]))
        blocks.append({"block_id": bid, "page": i % 2, "file": fn, "size_kb": 1})
    _wj(bd / "index.json", {"total_blocks": n_blocks, "total_expected": n_blocks,
                            "errors": 0, "blocks": blocks})
    return doc


@pytest.fixture
def v2tree(tmp_path, monkeypatch):
    v2 = tmp_path / "projects_v2"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": "0b540226", "display_name": "213", "folder_name": OBJF})
    _doc_with_blocks(v2, "AI", "doc-complete", n_blocks=4)
    # doc without blocks (source_only) — должен давать 404 canary на /blocks
    d = v2 / "objects" / OBJF / "disciplines" / "ITP" / "documents" / "doc-source"
    _wj(d / "document.json", {"document_code": "doc-source", "object_id": "0b540226",
        "discipline": "ITP", "kind": "plain", "migration_kind": "legacy_findings_preserve",
        "versions": [{"version_id": "v001", "version_no": 1}], "current_version": "v001"})
    (d / "current_version.txt").write_text("v001\n", encoding="utf-8")
    _wj(d / "versions" / "v001" / "version.json",
        {"version_id": "v001", "version_no": 1, "analysis_status": "source_only"})
    (d / "versions" / "v001" / "01_input").mkdir(parents=True, exist_ok=True)
    (d / "versions" / "v001" / "01_input" / "doc-source.pdf").write_text("x", encoding="utf-8")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    return v2


def _default_on(mp): mp.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
def _default_off(mp): mp.delenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", raising=False)
LEG = "?storage=legacy"


def _tree_hash(v2: Path) -> str:
    h = hashlib.sha256()
    for f in sorted(v2.rglob("*")):
        if f.is_file():
            h.update(str(f.relative_to(v2)).encode()); h.update(f.read_bytes())
    return h.hexdigest()


# === /blocks ===============================================================

def test_blocks_default_on_v2(monkeypatch, v2tree):
    _default_on(monkeypatch)
    r = client.get("/api/tiles/doc-complete/blocks")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2"
    assert b["total_blocks"] == 4
    assert sum(p["block_count"] for p in b["pages"]) == 4

def test_blocks_force_legacy(monkeypatch, v2tree):
    """?storage=legacy → legacy path (output_dir не найден в tmp → 404 legacy, не v2)."""
    _default_on(monkeypatch)
    r = client.get(f"/api/tiles/doc-complete/blocks{LEG}")
    assert r.json().get("storage_backend") != "projects_v2" if r.status_code == 200 else True
    assert r.status_code in (404, 500) or r.json().get("storage_backend") != "projects_v2"

def test_blocks_default_off_legacy(monkeypatch, v2tree):
    _default_off(monkeypatch)
    r = client.get("/api/tiles/doc-complete/blocks")
    assert r.json().get("storage_backend") != "projects_v2" if r.status_code == 200 else True

def test_blocks_no_index_404_canary(monkeypatch, v2tree):
    """doc без blocks index → 404 canary (без silent fallback)."""
    _default_on(monkeypatch)
    r = client.get("/api/tiles/doc-source/blocks")
    assert r.status_code == 404
    assert "projects_v2 canary" in r.json()["detail"]


# === /blocks/image ========================================================

def test_block_image_default_on_v2(monkeypatch, v2tree):
    _default_on(monkeypatch)
    r = client.get("/api/tiles/doc-complete/blocks/image/AAA-BBB-001")
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/png"
    assert r.headers.get("x-storage-backend") == "projects_v2"
    assert r.content.startswith(b"\x89PNG")

def test_block_image_missing_404(monkeypatch, v2tree):
    _default_on(monkeypatch)
    r = client.get("/api/tiles/doc-complete/blocks/image/ZZZ-ZZZ-999")
    assert r.status_code == 404
    assert "projects_v2 canary" in r.json()["detail"]

def test_block_image_path_traversal_blocked(monkeypatch, v2tree):
    """Анти-traversal: block_id с ../ не выводит за пределы blocks dir."""
    _default_on(monkeypatch)
    r = client.get("/api/tiles/doc-complete/blocks/image/" + urllib.parse.quote("../../../etc/passwd", safe=""))
    assert r.status_code == 404

def test_block_image_force_legacy(monkeypatch, v2tree):
    _default_on(monkeypatch)
    r = client.get(f"/api/tiles/doc-complete/blocks/image/AAA-BBB-001{LEG}")
    # legacy path: output_dir не в tmp → 404/500 legacy (НЕ v2 png с x-storage-backend)
    assert r.headers.get("x-storage-backend") != "projects_v2"


# === /versions/{vid}/files ================================================

def test_version_files_default_on_v2(monkeypatch, v2tree):
    _default_on(monkeypatch)
    r = client.get("/api/projects/doc-complete/versions/v001/files")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2"
    assert b["file_count"] >= 2
    # LEGACY-форма: files[] — объекты {name,type,size,updated_at}, не строки
    assert all(isinstance(f, dict) and "name" in f for f in b["files"])
    assert any(f["name"].endswith(".pdf") for f in b["files"])

def test_version_files_legacy_form_v1(monkeypatch, v2tree):
    """legacy-форма v1 на входе резолвится (v001) и отдаётся в legacy-форме v1."""
    _default_on(monkeypatch)
    r = client.get("/api/projects/doc-complete/versions/v1/files")
    assert r.status_code == 200
    assert r.json()["version_id"] == "v1"  # denorm выхода: v00N → vN

def test_version_files_force_legacy(monkeypatch, v2tree):
    _default_on(monkeypatch)
    r = client.get(f"/api/projects/doc-complete/versions/v001/files{LEG}")
    assert r.json().get("storage_backend") != "projects_v2" if r.status_code == 200 else True


# === read-only =============================================================

def test_expansion_read_only(monkeypatch, v2tree):
    _default_on(monkeypatch)
    before = _tree_hash(v2tree)
    client.get("/api/tiles/doc-complete/blocks")
    client.get("/api/tiles/doc-complete/blocks/image/AAA-BBB-000")
    client.get("/api/projects/doc-complete/versions/v001/files")
    assert _tree_hash(v2tree) == before
