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
import backend.app.services.storage.read_canary as RC
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

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
            _wj(latest / "02_text_analysis.json", {"ok": True})
            _wj(latest / "01_blocks_analysis.json", {"block_analyses": [
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


# ---------------------------------------------------------------------------
# gap-closure (2026-06-16): v2 read responses подтягивают РЕАЛЬНЫЕ значения
# (optimization / review-statuses / batches / pipeline / pdf-md / audit_date)
# из v2-источников; safe-default только когда источника реально нет.
# ---------------------------------------------------------------------------


def _gap_doc(v2, disc, code, *, findings=3, optimization=None, opt_meta=None,
             expert_decisions=None, batches=None, pipeline_stages=None,
             status="complete", kingsons=False):
    """Один v2-документ с управляемым набором gap-источников. kingsons=True кладёт
    всё в `99_service/legacy_output/<code>/_output` бандл (как legacy-preserve)."""
    doc = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
    dj = {"document_code": code, "object_id": OBJID, "discipline": disc, "kind": "plain",
          "versions": [{"version_id": "v001", "version_no": 1}], "current_version": "v001"}
    if kingsons:
        dj["migration_kind"] = "legacy_findings_preserve"
    _wj(doc / "document.json", dj)
    (doc / "current_version.txt").write_text("v001\n", encoding="utf-8")
    v = doc / "versions" / "v001"
    vj = {"version_id": "v001", "version_no": 1, "label": "V1", "analysis_status": status}
    if kingsons:
        vj["migration_kind"] = "legacy_findings_preserve"
    _wj(v / "version.json", vj)
    inp = v / "01_input"
    inp.mkdir(parents=True, exist_ok=True)
    (inp / f"{code}.pdf").write_text("%PDF-1.4\n" + "x" * 2048, encoding="utf-8")
    (inp / f"{code}_document.md").write_text("# md\n" + "y" * 1024, encoding="utf-8")
    if kingsons:
        latest = review = svc = v / "99_service" / "legacy_output" / code / "_output"
        latest.mkdir(parents=True, exist_ok=True)
    else:
        latest = v / "03_analysis" / "latest"
        review = v / "04_review"
        svc = v / "99_service"
        for d in (latest, review, svc):
            d.mkdir(parents=True, exist_ok=True)
    if findings is not None:
        _wj(latest / "03_findings.json", {"meta": {"audit_completed": "2026-06-01T12:00:00"},
            "findings": [{"id": f"F-{i}", "severity": "Критическое", "item_type": "finding"}
                         for i in range(findings)]})
    if optimization is not None:
        _wj(latest / "optimization.json", {"meta": opt_meta or {
            "total_items": optimization, "by_type": {"cheaper_analog": optimization},
            "estimated_savings_pct": 7.5}})
    if expert_decisions is not None:
        _wj(review / "expert_review.json", {"decisions": expert_decisions})
    if batches is not None:
        _wj(svc / "block_batches.json", {"total_batches": batches})
        for i in range(1, batches + 1):
            _wj(svc / ("block_batch_%03d.json" % i), {"blocks": [{"id": j} for j in range(20)]})
    if pipeline_stages is not None:
        _wj(svc / "pipeline_log.json", {"stages": pipeline_stages})
    return doc


def _status(v2, code):
    a = ProjectsV2Adapter(v2)
    return RC._v2_project_status(a, a.find_document(code))


@pytest.fixture
def gap_v2(tmp_path):
    v2 = tmp_path / "projects_v2"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": OBJID, "display_name": "999 Test", "folder_name": OBJF})
    return v2


def test_optimization_from_v2_when_file_present(gap_v2):
    _gap_doc(gap_v2, "EOM", "opt-doc", findings=4,
             optimization=6, opt_meta={"total_items": 6,
                                       "by_type": {"cheaper_analog": 2, "lifecycle": 4},
                                       "estimated_savings_pct": 9.0})
    s = _status(gap_v2, "opt-doc")
    assert s["optimization_count"] == 6
    assert s["optimization_by_type"] == {"cheaper_analog": 2, "lifecycle": 4}
    assert s["optimization_savings_pct"] == 9.0


def test_optimization_review_status_from_v2(gap_v2):
    # 2 findings + 2 optimizations; все 4 reviewed → complete; opt отдельно complete
    _gap_doc(gap_v2, "EOM", "rev-doc", findings=2, optimization=2,
             expert_decisions=[
                 {"item_type": "finding", "decision": "accepted"},
                 {"item_type": "finding", "decision": "rejected"},
                 {"item_type": "optimization", "decision": "accepted"},
                 {"item_type": "optimization", "decision": "accepted"}])
    s = _status(gap_v2, "rev-doc")
    assert s["expert_review_status"] == "complete"
    assert s["findings_review_status"] == "complete"
    assert s["optimization_review_status"] == "complete"


def test_expert_review_partial_from_v2(gap_v2):
    _gap_doc(gap_v2, "EOM", "rev-partial", findings=3, optimization=0,
             expert_decisions=[{"item_type": "finding", "decision": "accepted"}])
    s = _status(gap_v2, "rev-partial")
    assert s["findings_review_status"] == "partial"
    assert s["expert_review_status"] == "partial"
    assert s["optimization_review_status"] == ""  # нет оптимизаций → пусто


def test_kingsons_bundle_optimization_and_review(gap_v2):
    # King&Sons legacy-preserve: optimization.json + expert_review.json только в
    # legacy_output бандле (нет latest/04_review) → read_optimization/read_review
    # должны взять их из бандла.
    _gap_doc(gap_v2, "SS", "ks-doc", findings=None, optimization=3, kingsons=True,
             status="legacy_partial",
             expert_decisions=[{"item_type": "optimization", "decision": "accepted"},
                               {"item_type": "optimization", "decision": "accepted"},
                               {"item_type": "optimization", "decision": "rejected"}])
    s = _status(gap_v2, "ks-doc")
    assert s["optimization_count"] == 3                 # из бандла optimization.json
    assert s["optimization_review_status"] == "complete"  # 3/3 opt, из бандла expert_review.json


def test_batches_from_v2(gap_v2):
    _gap_doc(gap_v2, "EOM", "batch-doc", findings=1, batches=3)
    s = _status(gap_v2, "batch-doc")
    assert s["total_batches"] == 3 and s["completed_batches"] == 3


def test_pipeline_from_pipeline_log(gap_v2):
    _gap_doc(gap_v2, "EOM", "pl-doc", findings=2, pipeline_stages={
        "text_analysis": {"status": "done"}, "block_analysis": {"status": "done"},
        "findings_merge": {"status": "done"}, "norm_verify": {"status": "done"},
        "optimization": {"status": "error", "error": "boom"}})
    s = _status(gap_v2, "pl-doc")
    assert s["pipeline"]["text_analysis"] == "done"
    assert s["pipeline"]["norms_verified"] == "done"        # norm_verify → norms_verified
    assert s["pipeline"]["optimization"] == "error"
    assert any("optimization" in i for i in s["pipeline_issues"])


def test_safe_default_only_when_source_absent(gap_v2):
    # никаких optimization/expert_review/batches/pipeline_log → дефолты
    _gap_doc(gap_v2, "OV", "bare-doc", findings=None, status="none")
    s = _status(gap_v2, "bare-doc")
    assert s["optimization_count"] == 0 and s["optimization_by_type"] == {}
    assert s["expert_review_status"] == "" and s["optimization_review_status"] == ""
    assert s["total_batches"] == 0 and s["completed_batches"] == 0
    assert s["pipeline_issues"] == []
    # has_pdf/md по-прежнему реальные (01_input есть)
    assert s["has_pdf"] is True and s["has_md_file"] is True
    # frontend-critical keys сохранены
    assert isinstance(s["pipeline"], dict) and "gemma_enrichment" in s["pipeline"]


def test_pdf_md_audit_date_from_inputs(gap_v2):
    _gap_doc(gap_v2, "EOM", "io-doc", findings=2)
    s = _status(gap_v2, "io-doc")
    assert s["has_pdf"] is True and s["pdf_files"] == ["io-doc.pdf"]
    assert s["pdf_size_mb"] >= 0 and s["has_md_file"] is True
    assert s["last_audit_date"] == "2026-06-01T12:00:00"  # из meta.audit_completed


def test_gap_closure_storage_legacy_and_readonly(gap_v2, monkeypatch, tmp_path):
    # ?storage=legacy → legacy; default-v2 не пишет в projects_v2
    (tmp_path / "projects").mkdir(parents=True, exist_ok=True)
    _gap_doc(gap_v2, "EOM", "ro-doc", findings=2, optimization=2,
             expert_decisions=[{"item_type": "finding", "decision": "accepted"}])
    import hashlib
    def h():
        m = hashlib.sha256()
        for p in sorted((gap_v2 / "objects").rglob("*")):
            if p.is_file():
                m.update(str(p.relative_to(gap_v2)).encode()); m.update(p.read_bytes())
        return m.hexdigest()
    before = h()
    monkeypatch.setenv("AUDIT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(gap_v2))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", "true")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    monkeypatch.setattr(object_service, "get_current_object",
                        lambda: {"id": OBJID, "name": "999 Test",
                                 "projects_dir": str(tmp_path / "projects")})
    r = client.get("/api/projects?storage=legacy")
    assert r.json().get("storage_backend") != "projects_v2"
    r2 = client.get("/api/projects")  # default v2
    assert r2.json().get("storage_backend") == "projects_v2"
    assert h() == before, "default-v2 read must NOT write projects_v2"
