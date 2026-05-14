"""
test_version_aware_endpoints.py
-------------------------------
Тесты version-aware маршрутизации для read-эндпоинтов (findings, optimization,
blocks, document) и для pipeline job_key / output_dir.

Run:
    python -m pytest tests/test_version_aware_endpoints.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def projects_dir_v1_with_data(tmp_path, monkeypatch):
    """Изолированный projects/ с одним legacy V1, у которого есть findings + opt."""
    projects_dir = tmp_path / "projects"
    projects_dir.mkdir()
    pdir = projects_dir / "M31A"
    output = pdir / "_output"
    output.mkdir(parents=True)

    info = {
        "project_id": "M31A",
        "name": "M31A",
        "section": "EOM",
        "pdf_file": "document.pdf",
    }
    (pdir / "project_info.json").write_text(
        json.dumps(info, ensure_ascii=False), encoding="utf-8"
    )

    # findings + optimization V1
    findings = {
        "findings": [
            {"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "description": "X", "page": 1},
            {"id": "F-002", "severity": "ЭКОНОМИЧЕСКОЕ", "description": "Y", "page": 2},
            {"id": "F-003", "severity": "КРИТИЧЕСКОЕ", "description": "Z", "page": 3},
        ],
        "audit_date": "2026-05-01T00:00:00",
    }
    (output / "03_findings.json").write_text(
        json.dumps(findings, ensure_ascii=False), encoding="utf-8"
    )
    opt = {
        "meta": {"total_items": 2, "by_type": {"cable": 2}, "estimated_savings_pct": 11},
        "items": [
            {"id": "O-001", "type": "cable", "page": 5, "savings_pct": 10},
            {"id": "O-002", "type": "cable", "page": 6, "savings_pct": 12},
        ],
    }
    (output / "optimization.json").write_text(
        json.dumps(opt, ensure_ascii=False), encoding="utf-8"
    )
    # blocks/index.json
    blocks_dir = output / "blocks"
    blocks_dir.mkdir()
    (blocks_dir / "index.json").write_text(
        json.dumps({"total_blocks": 2, "blocks": [
            {"block_id": "AAA-BBB-001", "page": 1, "ocr_label": "L1"},
            {"block_id": "AAA-BBB-002", "page": 2, "ocr_label": "L2"},
        ]}, ensure_ascii=False),
        encoding="utf-8",
    )

    # Подменяем _get_projects_dir в project_service
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: projects_dir)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    # Очистка document-cache (между тестами разные tmp_path)
    monkeypatch.setattr(ps, "_document_cache", {})

    return projects_dir


@pytest.fixture
def client(projects_dir_v1_with_data):
    from backend.app.main import app
    return TestClient(app), projects_dir_v1_with_data


# ─── 1. V1 имеет findings, V2 пустая ────────────────────────────────────────


def test_findings_v1_returned_before_v2_created(client):
    c, _ = client
    r = c.get("/api/findings/M31A")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 3
    assert len(data["findings"]) == 3


def test_findings_v2_empty_after_create(client):
    c, _ = client
    # Создаём V2
    r = c.post("/api/projects/M31A/versions", json={"comment": "V2"})
    assert r.status_code == 200

    # GET findings без version_id → latest (V2). Папка V2 пустая → 404.
    r = c.get("/api/findings/M31A")
    assert r.status_code == 404

    # GET findings?version_id=v1 → исходные V1 findings
    r = c.get("/api/findings/M31A", params={"version_id": "v1"})
    assert r.status_code == 200
    assert r.json()["total"] == 3

    # GET findings?version_id=v2 → 404 (нет файла)
    r = c.get("/api/findings/M31A", params={"version_id": "v2"})
    assert r.status_code == 404


def test_findings_unknown_version_returns_404(client):
    c, _ = client
    r = c.get("/api/findings/M31A", params={"version_id": "v999"})
    assert r.status_code == 404
    assert "v999" in r.json().get("detail", "")


# ─── 2. Optimization isolation ───────────────────────────────────────────────


def test_optimization_v1_returned(client):
    c, _ = client
    r = c.get("/api/optimization/M31A")
    assert r.status_code == 200
    data = r.json()
    assert data["has_data"] is True
    assert data["data"]["meta"]["total_items"] == 2


def test_optimization_v2_empty_after_create(client):
    c, _ = client
    c.post("/api/projects/M31A/versions", json={"comment": "V2"})

    # latest = V2 — нет файла, has_data=False
    r = c.get("/api/optimization/M31A")
    assert r.status_code == 200
    assert r.json()["has_data"] is False

    # ?version_id=v1 — старые данные
    r = c.get("/api/optimization/M31A", params={"version_id": "v1"})
    assert r.status_code == 200
    assert r.json()["has_data"] is True
    assert r.json()["data"]["meta"]["total_items"] == 2


def test_optimization_unknown_version(client):
    c, _ = client
    r = c.get("/api/optimization/M31A", params={"version_id": "v999"})
    assert r.status_code == 404


# ─── 3. Blocks endpoint ─────────────────────────────────────────────────────


def test_blocks_v1_returned(client):
    c, _ = client
    r = c.get("/api/tiles/M31A/blocks")
    assert r.status_code == 200
    data = r.json()
    assert data["total_blocks"] == 2


def test_blocks_v2_empty(client):
    c, _ = client
    c.post("/api/projects/M31A/versions", json={"comment": "V2"})

    # latest=v2, нет blocks/index.json → 404
    r = c.get("/api/tiles/M31A/blocks")
    assert r.status_code == 404

    # v1 — есть
    r = c.get("/api/tiles/M31A/blocks", params={"version_id": "v1"})
    assert r.status_code == 200
    assert r.json()["total_blocks"] == 2


def test_blocks_unknown_version(client):
    c, _ = client
    r = c.get("/api/tiles/M31A/blocks", params={"version_id": "v999"})
    assert r.status_code == 404


# ─── 4. Document endpoint (MD) ─────────────────────────────────────────────


def test_document_v2_isolated(client, projects_dir_v1_with_data):
    """Document endpoint должен искать MD в папке версии, не в корне."""
    c, projects_dir = client
    # Положим MD в корень V1 и пропишем в info
    md_text = (
        "## СТРАНИЦА 1\n\n"
        "**Лист:** 1\n"
        "**Наименование листа:** Schema\n\n"
        "### [TEXT block_aaa]\n\n"
        "Hello V1.\n"
    )
    pdir = projects_dir / "M31A"
    (pdir / "document.md").write_text(md_text, encoding="utf-8")
    info = json.loads((pdir / "project_info.json").read_text(encoding="utf-8"))
    info["md_file"] = "document.md"
    (pdir / "project_info.json").write_text(
        json.dumps(info, ensure_ascii=False), encoding="utf-8"
    )

    # V1: pages есть
    r = c.get("/api/document/M31A/pages")
    assert r.status_code == 200
    assert r.json()["total_pages"] == 1

    # Создаём V2 (без MD)
    c.post("/api/projects/M31A/versions", json={"comment": "V2"})

    # V2: MD нет — 404
    r = c.get("/api/document/M31A/pages")
    assert r.status_code == 404

    # ?version_id=v1 по-прежнему отдаёт страницы
    r = c.get("/api/document/M31A/pages", params={"version_id": "v1"})
    assert r.status_code == 200


def test_document_unknown_version(client):
    c, _ = client
    r = c.get("/api/document/M31A/pages", params={"version_id": "v999"})
    assert r.status_code == 404


# ─── 5. Audit output_dir (через resolve_project_version_context) ───────────


def test_output_dir_v1_is_project_root(client, projects_dir_v1_with_data):
    """output_dir для V1 = project_dir/_output (корень)."""
    from backend.app.services.common import version_service
    ctx = version_service.resolve_project_version_context("M31A")
    expected = projects_dir_v1_with_data / "M31A" / "_output"
    assert ctx["output_dir"] == expected
    assert ctx["version_id"] == "v1"
    assert ctx["is_latest"] is True


def test_output_dir_v2_is_versions_subdir(client, projects_dir_v1_with_data):
    """output_dir для V2 = project_dir/_versions/v2/_output."""
    c, projects_dir = client
    c.post("/api/projects/M31A/versions", json={"comment": "V2"})

    from backend.app.services.common import version_service
    ctx = version_service.resolve_project_version_context("M31A")  # latest=v2
    expected = projects_dir_v1_with_data / "M31A" / "_versions" / "v2" / "_output"
    assert ctx["output_dir"] == expected
    assert ctx["version_id"] == "v2"

    # явный v1 даёт корень
    ctx_v1 = version_service.resolve_project_version_context("M31A", "v1")
    assert ctx_v1["output_dir"] == projects_dir_v1_with_data / "M31A" / "_output"


def test_output_dir_unknown_raises(client):
    from backend.app.services.common import version_service
    with pytest.raises(version_service.VersionNotFoundError):
        version_service.resolve_project_version_context("M31A", "v999")


# ─── 6. Pipeline job key ───────────────────────────────────────────────────


def test_pipeline_job_key_v1_is_legacy_string():
    """Для V1 (или version_id=None) ключ == project_id."""
    from backend.app.pipeline.manager import PipelineManager
    assert PipelineManager.job_key("M31A") == "M31A"
    assert PipelineManager.job_key("M31A", "v1") == "M31A"
    assert PipelineManager.job_key("M31A", None) == "M31A"


def test_pipeline_job_key_v2_has_version_suffix():
    """Для V2+ ключ строится как project_id:version_id."""
    from backend.app.pipeline.manager import PipelineManager
    assert PipelineManager.job_key("M31A", "v2") == "M31A:v2"
    assert PipelineManager.job_key("M31A", "v3") == "M31A:v3"


def test_is_running_does_not_mistake_v2_folder_name_for_project():
    """Для V2 output_dir.parent.name == 'v2', не project_id.
    is_running должен корректно различать."""
    from backend.app.pipeline.manager import pipeline_manager
    # Никаких active_jobs нет → all False
    assert pipeline_manager.is_running("M31A") is False
    assert pipeline_manager.is_running("M31A", "v2") is False
    # Имя папки версии "v2" само по себе не должно считаться project_id
    assert pipeline_manager.is_running("v2") is False


# ─── 7. Dashboard / list_projects: одна карточка ───────────────────────────


def test_list_projects_one_card_after_v2(client):
    c, _ = client
    c.post("/api/projects/M31A/versions", json={"comment": "V2"})

    r = c.get("/api/projects")
    assert r.status_code == 200
    ids = [p["project_id"] for p in r.json()["projects"]]
    assert ids.count("M31A") == 1
    # M31A_V2 не появляется как отдельная карточка
    assert "M31A_V2" not in ids
    assert "v2" not in ids


# ─── 8. Legacy без manifest: всё работает как раньше ───────────────────────


def test_legacy_findings_endpoint_without_version_id(client):
    """Проект без project_versions.json — findings читаются из корня."""
    c, projects_dir = client
    # Не создаём V2, не создаём manifest
    assert not (projects_dir / "M31A" / "project_versions.json").exists()

    r = c.get("/api/findings/M31A")
    assert r.status_code == 200
    assert r.json()["total"] == 3

    # version_id=v1 тоже работает (синтетический legacy)
    r = c.get("/api/findings/M31A", params={"version_id": "v1"})
    assert r.status_code == 200
    assert r.json()["total"] == 3


def test_legacy_summaries_use_latest_dir_after_v2(client, projects_dir_v1_with_data):
    """Сводка /findings/summary после создания V2 не должна подтягивать V1-данные."""
    c, projects_dir = client
    c.post("/api/projects/M31A/versions", json={"comment": "V2"})

    r = c.get("/api/findings/summary")
    assert r.status_code == 200
    summaries = r.json()["summaries"]
    # M31A — latest=v2, в V2 нет findings → проект не появляется в сводке вообще
    m31a = [s for s in summaries if s["project_id"] == "M31A"]
    assert m31a == [] or m31a[0]["total"] == 0
