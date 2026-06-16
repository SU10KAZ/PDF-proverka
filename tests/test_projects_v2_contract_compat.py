"""
Contract-compat тесты: v2 read-canary ответы должны быть SHAPE-СОВМЕСТИМЫ с
legacy, иначе фронтенд ломается при AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=true.

Инцидент 2026-06-16: GET /api/projects под default-read отдал v2-native
{documents,count} вместо legacy {projects,object_name} → data.projects=undefined
→ весь UI упал. Эти тесты проверяют именно FRONTEND-КЛЮЧИ (не только HTTP 200),
по всем canary/default-read endpoint'ам:

  * legacy-ключи присутствуют (projects/object_name/pipeline/versions/blocks/...);
  * v2-native ключи НЕ вместо legacy (documents вместо projects → FAIL);
  * типы совместимы (projects — массив, pipeline — объект, version_id — 'vN');
  * source_only / без-анализа документы не дают 500;
  * ?storage=legacy → legacy; флаг default OFF → legacy; opt-in без canary → 403;
  * write-endpoint'ы не тронуты canary (resolve_read_backend только в GET).

Изоляция: AUDIT_DATA_DIR + AUDIT_PROJECTS_V2_DIR в tmp; get_current_object
монкипатчится на объект тестового дерева (для object-scope /api/projects).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import backend.app.main as _main_mod
from backend.app.main import app
import backend.app.services.common.object_service as object_service

client = TestClient(app, raise_server_exceptions=False)

OBJID = "testobj0001"
OBJF = "999_TestObject"
VN = re.compile(r"^v\d+$")  # legacy-форма version_id (НЕ zero-padded v001)


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


MD = """## СТРАНИЦА 1
**Лист:** 1
**Наименование листа:** Общие данные

### BLOCK [TEXT]: TXT-AAAA-001
Текст страницы 1.

### BLOCK [IMAGE]: IMG-BBBB-001
**[ИЗОБРАЖЕНИЕ]** | Тип: схема
**Краткое описание:** однолинейная схема
"""


def _make_doc(v2, disc, code, *, versions=("v001",), complete=True, with_md=True,
              status="complete"):
    doc = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
    vlist = [{"version_id": v, "version_no": i + 1} for i, v in enumerate(versions)]
    _wj(doc / "document.json", {
        "document_code": code, "object_id": OBJID, "discipline": disc,
        "kind": "plain", "versions": vlist, "current_version": versions[-1]})
    (doc / "current_version.txt").write_text(versions[-1] + "\n", encoding="utf-8")
    for i, v in enumerate(versions):
        vdir = doc / "versions" / v
        _wj(vdir / "version.json", {
            "version_id": v, "version_no": i + 1, "label": f"V{i+1}",
            "analysis_status": status})
        inp = vdir / "01_input"
        inp.mkdir(parents=True, exist_ok=True)
        (inp / f"{code}.pdf").write_text("%PDF-1.4\n", encoding="utf-8")
        if with_md:
            (inp / f"{code}_document.md").write_text(MD, encoding="utf-8")
        if complete:
            latest = vdir / "03_analysis" / "latest"
            _wj(latest / "01_text_analysis.json", {"ok": True})
            _wj(latest / "02_blocks_analysis.json", {"block_analyses": [
                {"block_id": "IMG-BBBB-001", "page": 1, "findings": []},
                {"block_id": "IMG-PARENT-1", "page": 2, "findings": [{"x": 1}]},
            ]})
            _wj(latest / "03_findings.json", {"findings": [
                {"id": "F-001", "severity": "Критическое",
                 "related_block_ids": ["IMG-BBBB-001"]}]})
            _wj(latest / "document_graph.json", {"pages": [{"page": 1, "sheet_no": "1"}]})
            run = vdir / "03_analysis" / "runs" / "run_x" / "blocks"
            _wj(run / "index.json", {"total_blocks": 4, "total_expected": 4, "errors": 0,
                "blocks": [
                    {"block_id": "IMG-BBBB-001", "page": 1, "ocr_label": "схема",
                     "file": "block_IMG-BBBB-001.png"},
                    {"block_id": "IMG-PARENT-1", "page": 2, "ocr_label": "лист"},
                    {"block_id": "IMG-CHILD-1", "page": 2, "ocr_label": "фрагмент"},
                    {"block_id": "IMG-SKIP-1", "page": 3, "ocr_label": "пусто"}]})
            (run.parent / "blocks" / "block_IMG-BBBB-001.png").write_bytes(b"\x89PNG\r\n")
            # block_batches: IMG-CHILD-1 свёрнут в IMG-PARENT-1 → merged_into
            _wj(vdir / "99_service" / "block_batches.json", {"batches": [
                {"blocks": [{"block_id": "IMG-PARENT-1",
                             "merged_block_ids": ["IMG-CHILD-1"]}]}]})
    return doc


@pytest.fixture
def v2env(tmp_path, monkeypatch):
    data = tmp_path
    v2 = data / "projects_v2"
    (data / "projects").mkdir(parents=True, exist_ok=True)  # пустой legacy projects/
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": OBJID, "display_name": "999 Test", "folder_name": OBJF})
    _make_doc(v2, "EOM", "TST-ГК-ЭМ1")                      # complete, 1 версия
    _make_doc(v2, "EOM", "TST-ГК-ЭМ2", versions=("v001", "v002"))  # 2 версии
    _make_doc(v2, "AR", "TST-ГК-АР-SRC", complete=False, with_md=False,
              status="source_only")                         # source_only
    monkeypatch.setenv("AUDIT_DATA_DIR", str(data))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", "true")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", "true")
    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    # current object = тестовый объект (для object-scope /api/projects)
    monkeypatch.setattr(object_service, "get_current_object",
                        lambda: {"id": OBJID, "name": "999 Test",
                                 "projects_dir": str(data / "projects")})
    return v2


def _on(mp): mp.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
def _off(mp): mp.delenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", raising=False)


# ───────────────────────── /api/projects (incident root) ─────────────────────

def test_projects_legacy_shape_not_documents(v2env, monkeypatch):
    _on(monkeypatch)
    r = client.get("/api/projects")
    assert r.status_code == 200
    b = r.json()
    # FRONTEND-CRITICAL: projects (array) + object_name; НЕ v2-native documents-as-container
    assert isinstance(b.get("projects"), list), f"projects must be array, got keys {list(b)}"
    assert "object_name" in b
    assert b["object_name"] == "999 Test"
    # объект-scope: только документы тестового объекта (3), не кросс-объект
    assert len(b["projects"]) == 3


def test_projects_item_has_all_legacy_keys(v2env, monkeypatch):
    _on(monkeypatch)
    items = client.get("/api/projects").json()["projects"]
    item = next(p for p in items if p["project_id"] == "TST-ГК-ЭМ1")
    for k in ("project_id", "name", "section", "description", "pipeline",
              "findings_count", "findings_by_severity", "optimization_count",
              "optimization_by_type", "pipeline_issues", "expert_review_status",
              "findings_review_status", "optimization_review_status", "has_pdf",
              "block_count", "version_id", "version_no", "version_label",
              "latest_version_id", "version_count", "has_versions",
              "is_latest_version", "versions_summary"):
        assert k in item, f"projects[] missing legacy key {k!r}"
    assert isinstance(item["pipeline"], dict) and "gemma_enrichment" in item["pipeline"]
    assert isinstance(item["findings_by_severity"], dict)
    assert isinstance(item["versions_summary"], list)
    assert VN.match(item["version_id"]), item["version_id"]
    assert VN.match(item["latest_version_id"]), item["latest_version_id"]
    assert item["section"] == "EOM"


def test_projects_multiversion_flags(v2env, monkeypatch):
    _on(monkeypatch)
    items = client.get("/api/projects").json()["projects"]
    mv = next(p for p in items if p["project_id"] == "TST-ГК-ЭМ2")
    assert mv["version_count"] == 2 and mv["has_versions"] is True
    assert mv["latest_version_id"] == "v2"  # denorm v002 → v2
    assert {v["version_id"] for v in mv["versions_summary"]} == {"v1", "v2"}


# ───────────────────────── /api/projects/{id} (template crash) ───────────────

def test_project_details_has_pipeline_object(v2env, monkeypatch):
    _on(monkeypatch)
    b = client.get("/api/projects/TST-ГК-ЭМ1").json()
    # index.html дереференсит currentProject.pipeline.gemma_enrichment БЕЗ guard
    assert isinstance(b.get("pipeline"), dict)
    assert "gemma_enrichment" in b["pipeline"] and "text_analysis" in b["pipeline"]
    for k in ("project_id", "name", "section", "findings_count", "version_id"):
        assert k in b
    assert VN.match(b["version_id"])


# ───────────────────────── versions (v001→v1 denorm) ─────────────────────────

def test_versions_legacy_form_and_latest(v2env, monkeypatch):
    _on(monkeypatch)
    b = client.get("/api/projects/TST-ГК-ЭМ2/versions").json()
    assert isinstance(b.get("versions"), list)
    assert "latest_version_id" in b and VN.match(b["latest_version_id"])
    assert b["latest_version_id"] == "v2"
    for v in b["versions"]:
        assert VN.match(v["version_id"]), f"version_id must be vN, got {v['version_id']}"


# ───────────────────────── blocks/analysis (classified dict) ─────────────────

def test_blocks_analysis_classified_dict(v2env, monkeypatch):
    _on(monkeypatch)
    b = client.get("/api/tiles/TST-ГК-ЭМ1/blocks/analysis").json()
    blocks = b.get("blocks")
    assert isinstance(blocks, dict), "frontend does Object.entries(data.blocks)"
    assert isinstance(b.get("counts"), dict)
    # классификация: IMG-BBBB-001 → has_findings (в findings.related),
    # IMG-PARENT-1 → has_findings (свой findings[]),
    # IMG-CHILD-1 → merged_into (parent IMG-PARENT-1),
    # IMG-SKIP-1 → skipped
    assert blocks["IMG-BBBB-001"]["status"] == "has_findings"
    assert blocks["IMG-PARENT-1"]["status"] == "has_findings"
    assert blocks["IMG-CHILD-1"]["status"] == "merged_into"
    assert blocks["IMG-CHILD-1"]["parent_block_id"] == "IMG-PARENT-1"
    assert blocks["IMG-SKIP-1"]["status"] == "skipped"
    assert b["counts"]["merged_into"] == 1 and b["counts"]["skipped"] == 1


def test_blocks_analysis_source_only_no_500(v2env, monkeypatch):
    _on(monkeypatch)
    r = client.get("/api/tiles/TST-ГК-АР-SRC/blocks/analysis")
    assert r.status_code == 200
    b = r.json()
    assert isinstance(b.get("blocks"), dict) and b["blocks"] == {}
    assert b["counts"] == {"has_findings": 0, "no_findings": 0,
                           "merged_into": 0, "skipped": 0}


# ───────────────────────── findings ──────────────────────────────────────────

def test_findings_has_list(v2env, monkeypatch):
    _on(monkeypatch)
    b = client.get("/api/findings/TST-ГК-ЭМ1").json()
    assert isinstance(b.get("findings"), list)
    assert b["findings"] and b["findings"][0]["id"] == "F-001"


def test_finding_by_id_top_level(v2env, monkeypatch):
    _on(monkeypatch)
    b = client.get("/api/findings/TST-ГК-ЭМ1/finding/F-001").json()
    # legacy отдаёт поля замечания на верхнем уровне (не под finding)
    assert b.get("id") == "F-001" and b.get("severity") == "Критическое"
    assert "finding" not in b or isinstance(b.get("finding"), (str, type(None)))


# ───────────────────────── source_only без 500 ───────────────────────────────

def test_source_only_endpoints_no_500(v2env, monkeypatch):
    _on(monkeypatch)
    for path in ("/api/projects/TST-ГК-АР-SRC",
                 "/api/projects/TST-ГК-АР-SRC/versions",
                 "/api/findings/TST-ГК-АР-SRC",
                 "/api/findings/TST-ГК-АР-SRC/block-map",
                 "/api/document/TST-ГК-АР-SRC/pages"):
        r = client.get(path)
        assert r.status_code < 500, f"{path} → {r.status_code}"


# ───────────────────────── force-legacy / flag-off / opt-in gate ─────────────

def test_storage_legacy_forces_legacy(v2env, monkeypatch):
    _on(monkeypatch)  # даже при default-read ON
    b = client.get("/api/projects?storage=legacy").json()
    assert b.get("storage_backend") != "projects_v2"
    # legacy projects/ пуст → projects=[] (но это ВСЁ ещё legacy-форма)
    assert isinstance(b.get("projects"), list)


def test_flag_off_default_legacy(v2env, monkeypatch):
    _off(monkeypatch)
    b = client.get("/api/projects").json()
    assert b.get("storage_backend") != "projects_v2"
    assert isinstance(b.get("projects"), list)


def test_optin_without_canary_403(tmp_path, monkeypatch):
    # canary флаг OFF → явный opt-in ?storage=projects_v2 → 403 (не silent legacy)
    monkeypatch.delenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", raising=False)
    monkeypatch.delenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", raising=False)
    r = client.get("/api/projects?storage=projects_v2")
    assert r.status_code == 403


# ───────────────────────── write endpoints не тронуты canary ─────────────────

def test_canary_only_in_get_handlers():
    """resolve_read_backend вызывается ТОЛЬКО в GET-обработчиках (read-only).

    Гарантия «write endpoints не тронуты»: ни один POST/PUT/DELETE/PATCH
    обработчик не маршрутизируется в v2.
    """
    routers_dir = Path(_main_mod.__file__).resolve().parent / "api" / "routers"
    offenders = []
    for pyf in routers_dir.glob("*.py"):
        lines = pyf.read_text(encoding="utf-8").splitlines()
        cur_method = None
        for ln in lines:
            m = re.search(r'@\w+\.(get|post|put|delete|patch)\(', ln)
            if m:
                cur_method = m.group(1)
            if "resolve_read_backend" in ln and cur_method not in (None, "get"):
                offenders.append(f"{pyf.name}: resolve_read_backend под @{cur_method}")
    assert not offenders, offenders
