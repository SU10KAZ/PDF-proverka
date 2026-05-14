"""
test_legacy_webapp_migrated_findings.py
---------------------------------------
Smoke-тесты: migrated_findings router подключён к legacy `webapp.main:app`
и не перехватывается catch-all `GET /api/projects/{project_id:path}` из
webapp.routers.projects.

Запуск:
    python -m pytest tests/test_legacy_webapp_migrated_findings.py -v
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


# ─── Fixtures (повторяют структуру test_migrated_findings_service.py,
#     но через webapp.main:app) ──────────────────────────────────────────


def _v1_finding(fid: str, **overrides) -> dict:
    f = {
        "id": fid,
        "severity": "КРИТИЧЕСКОЕ",
        "category": "cable_routing",
        "sheet": "Лист 7",
        "page": 12,
        "problem": "Кабель ВВГнг(А)-FRLS 5x10 проложен без огнестойких креплений",
        "description": "ПВХ-клипсы не соответствуют огнестойкости.",
        "norm": "СП 6.13130.2021, п. 4.3",
        "evidence": [{"type": "image", "block_id": "AAA-BBB-001", "page": 12}],
        "related_block_ids": ["AAA-BBB-001"],
    }
    f.update(overrides)
    return f


@pytest.fixture
def webapp_with_project(tmp_path, monkeypatch):
    """Сделать минимальный V1 проект + V2 версию и подменить projects_dir
    у backend.project_service (его использует migrated_findings router)."""
    projects_root = tmp_path / "projects"
    projects_root.mkdir()
    pdir = projects_root / "M31A"
    (pdir / "_output").mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({
            "project_id": "M31A", "name": "M31A",
            "section": "EOM", "pdf_file": "doc.pdf",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (pdir / "doc.pdf").write_bytes(b"%PDF-1.4 fake")

    # Подменяем глобал projects_dir у backend.project_service.
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: projects_root)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})

    # V1: 03_findings + expert_review (F-001 accepted) для нормального
    # сценария «есть, что мигрировать».
    out = pdir / "_output"
    findings = {
        "meta": {"total_findings": 1},
        "findings": [_v1_finding("F-001")],
    }
    out.joinpath("03_findings.json").write_text(
        json.dumps(findings, ensure_ascii=False), encoding="utf-8",
    )
    out.joinpath("expert_review.json").write_text(
        json.dumps({
            "decisions": [
                {"finding_id": "F-001", "decision": "accepted", "reason": ""},
            ],
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    # Создаём V2 через тот же version_service, который роутер использует.
    from backend.app.services.common import version_service
    version_service.create_next_version(
        project_dir=pdir, project_id="M31A",
        source="manual", status="new", comment="",
    )

    return projects_root, pdir


@pytest.fixture
def legacy_client(webapp_with_project):
    """TestClient над legacy webapp.main:app."""
    from webapp.main import app
    return TestClient(app)


# ─── 1. Router зарегистрирован ──────────────────────────────────────────


def test_legacy_app_has_migrated_endpoints(legacy_client):
    """OpenAPI должен включать оба migrated-findings пути."""
    r = legacy_client.get("/openapi.json")
    assert r.status_code == 200
    paths = r.json().get("paths", {})
    assert "/api/projects/{project_id}/versions/{version_id}/migrated-findings/check" in paths
    assert "/api/projects/{project_id}/versions/{version_id}/migrated-findings/report" in paths


# ─── 2. Catch-all из projects.py не перехватывает migrated-маршруты ────


def test_migrated_report_not_swallowed_by_projects_catch_all(legacy_client):
    """В webapp.routers.projects есть `GET /{project_id:path}`. Migrated
    router был зарегистрирован ДО projects.router, поэтому GET по migrated
    суффиксу должен возвращать осмысленный код (200/404 от migrated-роутера),
    а не "ProjectStatus" с project_id='M31A/versions/v2/migrated-findings/report'.
    """
    r = legacy_client.get(
        "/api/projects/M31A/versions/v2/migrated-findings/report",
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # Признаки именно migrated-эндпоинта (а не catch-all `GET /{project_id:path}`):
    assert "exists" in body
    assert body["project_id"] == "M31A"
    assert body["version_id"] == "v2"


# ─── 3. До запуска check — exists:false ─────────────────────────────────


def test_report_before_check_returns_exists_false(legacy_client):
    r = legacy_client.get(
        "/api/projects/M31A/versions/v2/migrated-findings/report",
    )
    assert r.status_code == 200
    body = r.json()
    assert body["exists"] is False
    assert body["report"] is None


# ─── 4. POST check на V2 — успех ────────────────────────────────────────


def test_post_check_v2_returns_summary(legacy_client):
    r = legacy_client.post(
        "/api/projects/M31A/versions/v2/migrated-findings/check",
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # summary-поля из migrated_findings_service: на верхнем уровне есть
    # status/source_version_id/report (минимум), детальный report лежит в
    # body["report"].
    assert body.get("status") == "ok"
    assert body.get("source_version_id") == "v1"
    assert "report" in body
    assert "items" in body["report"]


# ─── 5. После check — GET report exists:true ────────────────────────────


def test_report_after_check_exists_true(legacy_client):
    legacy_client.post(
        "/api/projects/M31A/versions/v2/migrated-findings/check",
    )
    r = legacy_client.get(
        "/api/projects/M31A/versions/v2/migrated-findings/report",
    )
    assert r.status_code == 200
    body = r.json()
    assert body["exists"] is True
    assert body["report"] is not None
    assert body["report"]["source_version_id"] == "v1"


# ─── 6. V1 → 400 ────────────────────────────────────────────────────────


def test_v1_returns_400(legacy_client):
    r = legacy_client.post(
        "/api/projects/M31A/versions/v1/migrated-findings/check",
    )
    assert r.status_code == 400


# ─── 7. Unknown version → 404 ───────────────────────────────────────────


def test_unknown_version_returns_404(legacy_client):
    r = legacy_client.post(
        "/api/projects/M31A/versions/v999/migrated-findings/check",
    )
    assert r.status_code == 404


# ─── 8. Unknown project → 404 ───────────────────────────────────────────


def test_unknown_project_returns_404(legacy_client):
    r = legacy_client.get(
        "/api/projects/__nope__/versions/v2/migrated-findings/report",
    )
    assert r.status_code == 404


# ─── 9. Старые legacy endpoints не сломались ────────────────────────────


def test_legacy_projects_list_still_works(legacy_client):
    """GET /api/projects продолжает работать после регистрации migrated роутера."""
    r = legacy_client.get("/api/projects")
    assert r.status_code == 200
    # Тело может быть пустым списком (в тестовой среде нет проектов в нашем
    # реальном projects/), главное — что роут не перехвачен и вернул JSON.
    assert isinstance(r.json(), (list, dict))


def test_legacy_project_versions_endpoint_still_works(legacy_client):
    """GET /api/projects/{id}/versions всё ещё доступен (не перехвачен)."""
    r = legacy_client.get("/api/projects/M31A/versions")
    # Может быть 200 или 404 в зависимости от моков, главное — это
    # НЕ ответ от catch-all `GET /{project_id:path}` (он вернул бы
    # ProjectStatus, и version_id оказался бы строкой 'versions').
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        body = r.json()
        # Endpoint /versions возвращает {versions: [...], latest_version_id: ...}.
        assert "versions" in body
