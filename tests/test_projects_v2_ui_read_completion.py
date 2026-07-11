"""
Тесты UI-read completion: block-map + document pages/page на projects_v2.

  * GET /api/findings/{project_id}/block-map
  * GET /api/document/{project_id}/pages
  * GET /api/document/{project_id}/page/{page_num}

Default-флаг ON → v2; `?storage=legacy` → legacy; v2-данных нет → 404/empty без 500;
read-only. Изоляция через AUDIT_PROJECTS_V2_DIR.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
from backend.app.main import app  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"
# raise_server_exceptions=False: force-legacy на bare v2-code → legacy не резолвит →
# FileNotFoundError → 500-ответ (а не raise); тест проверяет «не v2», не падая.
client = TestClient(app, raise_server_exceptions=False)

MD = """## СТРАНИЦА 1
**Лист:** 1
**Наименование листа:** Общие данные

### BLOCK [TEXT]: TXT-AAAA-001
Пояснительный текст страницы 1.

### BLOCK [IMAGE]: IMG-BBBB-001
**[ИЗОБРАЖЕНИЕ]** | Тип: схема | Оси: 1-5
**Краткое описание:** однолинейная схема

## СТРАНИЦА 2
**Лист:** 2

### BLOCK [TEXT]: TXT-AAAA-002
Текст страницы 2.
"""

GRAPH = {"pages": [
    {"page": 1, "sheet_no": "1", "text_blocks": [
        {"id": "TXT-AAAA-001", "text": "Пояснительный текст страницы 1."}]},
    {"page": 2, "sheet_no": "2", "text_blocks": [
        {"id": "TXT-AAAA-002", "text": "Текст страницы 2."}]},
]}


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _full_doc(v2, disc, code, *, run="run_x", with_md=True, with_graph=True, with_blocks=True):
    doc = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
    _wj(doc / "document.json", {"document_code": code, "object_id": "0b540226",
        "discipline": disc, "kind": "plain",
        "versions": [{"version_id": "v001", "version_no": 1}], "current_version": "v001"})
    (doc / "current_version.txt").write_text("v001\n", encoding="utf-8")
    _wj(doc / "versions" / "v001" / "version.json",
        {"version_id": "v001", "version_no": 1, "analysis_status": "complete"})
    latest = doc / "versions" / "v001" / "03_analysis" / "latest"
    # findings: f1 → image block via evidence; f2 → page-only (НЕ должно мапиться)
    _wj(latest / "03_findings.json", {"findings": [
        {"id": "F-001", "description": "проблема на схеме",
         "evidence": [{"type": "image", "block_id": "IMG-BBBB-001", "page": 1},
                      {"type": "text", "block_id": "TXT-AAAA-001", "page": 1}]},
        {"id": "F-002", "description": "общее замечание по листу 2", "page": 2},
    ]})
    if with_blocks:
        # 02_blocks_analysis + blocks index → all_block_ids/block_info
        _wj(latest / "01_blocks_analysis.json", {"block_analyses": [
            {"block_id": "IMG-BBBB-001", "page": 1}]})
        _wj(doc / "versions" / "v001" / "03_analysis" / "runs" / run / "blocks" / "index.json",
            {"total_blocks": 1, "blocks": [
                {"block_id": "IMG-BBBB-001", "page": 1, "file": "block_IMG-BBBB-001.png",
                 "ocr_label": "схема"}]})
    if with_graph:
        _wj(latest / "document_graph.json", GRAPH)
    inp = doc / "versions" / "v001" / "01_input"
    inp.mkdir(parents=True, exist_ok=True)
    if with_md:
        (inp / f"{code}_document.md").write_text(MD, encoding="utf-8")
        (inp / f"{code}_ocr.html").write_text(
            '<div class="block"><div class="block-header">TXT-AAAA-001</div>'
            '<div class="block-content"><p>BLOCK: TXT-AAAA-001</p><p>OCR HTML текст</p></div></div>',
            encoding="utf-8")
    return doc


@pytest.fixture
def v2tree(tmp_path, monkeypatch):
    v2 = tmp_path / "projects_v2"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": "0b540226", "display_name": "213", "folder_name": OBJF})
    _full_doc(v2, "AI", "doc-complete")
    # source_only: без MD, без graph, без блоков (block-map пустой, pages 404)
    d = _full_doc(v2, "ITP", "doc-source", with_md=False, with_graph=False, with_blocks=False)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    return v2


def _on(mp): mp.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
def _off(mp): mp.delenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", raising=False)
LEG = "?storage=legacy"


def _tree_hash(v2: Path) -> str:
    h = hashlib.sha256()
    for f in sorted(v2.rglob("*")):
        if f.is_file():
            h.update(str(f.relative_to(v2)).encode()); h.update(f.read_bytes())
    return h.hexdigest()


# === block-map ==============================================================

def test_block_map_default_on_v2(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get("/api/findings/doc-complete/block-map")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2"
    # F-001 → image block; F-002 (page-only) НЕ мапится (no false binding)
    assert b["block_map"] == {"F-001": ["IMG-BBBB-001"]}
    assert "F-002" not in b["block_map"]
    assert b["block_info"]["IMG-BBBB-001"]["ocr_label"] == "схема"
    # text_evidence: F-001 → TXT-AAAA-001 (через evidence type=text)
    assert "F-001" in b["text_evidence"]
    assert b["text_evidence"]["F-001"][0]["text_block_id"] == "TXT-AAAA-001"

def test_block_map_no_false_binding(monkeypatch, v2tree):
    _on(monkeypatch)
    b = client.get("/api/findings/doc-complete/block-map").json()
    # page-only finding не привязан к графическому блоку
    assert all(fid != "F-002" for fid in b["block_map"])

def test_block_map_source_only_empty_no_500(monkeypatch, v2tree):
    """doc без блоков/findings → пустой map, не 500."""
    _on(monkeypatch)
    r = client.get("/api/findings/doc-source/block-map")
    assert r.status_code == 200
    assert r.json()["block_map"] == {}

def test_block_map_force_legacy(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get(f"/api/findings/doc-complete/block-map{LEG}")
    assert r.json().get("storage_backend") != "projects_v2" if r.status_code == 200 else True


# === document pages / page ==================================================

def test_document_pages_default_on_v2(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get("/api/document/doc-complete/pages")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2"
    assert b["total_pages"] == 2
    p1 = b["pages"][0]
    assert p1["page_num"] == 1 and p1["sheet_label"] == "Общие данные"
    assert p1["text_blocks"] == 1 and p1["image_blocks"] == 1
    assert "blocks" not in p1  # pages — облегчённый контракт (без содержимого)

def test_document_page_default_on_v2(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get("/api/document/doc-complete/page/1")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2"
    assert b["page_num"] == 1
    ids = [blk["block_id"] for blk in b["blocks"]]
    assert "TXT-AAAA-001" in ids and "IMG-BBBB-001" in ids

def test_document_pages_source_only_404_no_500(monkeypatch, v2tree):
    """source_only без MD → 404 canary (не 500)."""
    _on(monkeypatch)
    r = client.get("/api/document/doc-source/pages")
    assert r.status_code == 404
    assert "projects_v2 canary" in r.json()["detail"]

def test_document_page_missing_404(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get("/api/document/doc-complete/page/99")
    assert r.status_code == 404
    assert "projects_v2 canary" in r.json()["detail"]

def test_document_pages_force_legacy(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get(f"/api/document/doc-complete/pages{LEG}")
    assert r.json().get("storage_backend") != "projects_v2" if r.status_code == 200 else True


# === read-only ==============================================================

def test_ui_read_completion_read_only(monkeypatch, v2tree):
    _on(monkeypatch)
    before = _tree_hash(v2tree)
    client.get("/api/findings/doc-complete/block-map")
    client.get("/api/document/doc-complete/pages")
    client.get("/api/document/doc-complete/page/1")
    client.get("/api/findings/doc-source/block-map")
    assert _tree_hash(v2tree) == before
