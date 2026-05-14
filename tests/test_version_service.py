"""
test_version_service.py
-----------------------
Тесты механизма версионности проектов.

Run:
    python -m pytest tests/test_version_service.py -v
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

from backend.app.services.common import version_service  # noqa: E402
from backend.app.services.common.version_service import (  # noqa: E402
    VERSIONS_MANIFEST_FILENAME,
    VersionNotFoundError,
    create_next_version,
    ensure_project_versions_manifest,
    get_latest_version_id,
    get_version_dir,
    get_versions_summary,
    read_project_versions,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def legacy_project_dir(tmp_path) -> Path:
    """Минимальный legacy-проект: только project_info.json + _output/."""
    pdir = tmp_path / "M31A"
    (pdir / "_output").mkdir(parents=True)
    info = {
        "project_id": "M31A",
        "name": "M31A",
        "section": "EOM",
        "pdf_file": "document.pdf",
    }
    (pdir / "project_info.json").write_text(
        json.dumps(info, ensure_ascii=False), encoding="utf-8"
    )
    return pdir


# ─── Базовые legacy-сценарии ─────────────────────────────────────────────────


def test_legacy_project_without_manifest_is_v1(legacy_project_dir):
    """Проект без project_versions.json должен считаться V1 (in-memory)."""
    assert not (legacy_project_dir / VERSIONS_MANIFEST_FILENAME).exists()

    manifest = read_project_versions(legacy_project_dir, "M31A")

    assert manifest["schema_version"] == 1
    assert manifest["logical_project_id"] == "M31A"
    assert manifest["latest_version_id"] == "v1"
    assert len(manifest["versions"]) == 1

    v1 = manifest["versions"][0]
    assert v1["version_id"] == "v1"
    assert v1["version_no"] == 1
    assert v1["label"] == "V1"
    assert v1["folder"] == "."
    assert v1["status"] == "legacy"
    assert v1["source"] == "legacy"

    # read_project_versions НЕ должен писать файл на диск
    assert not (legacy_project_dir / VERSIONS_MANIFEST_FILENAME).exists()


def test_get_latest_version_id_legacy(legacy_project_dir):
    assert get_latest_version_id(legacy_project_dir, "M31A") == "v1"


def test_get_version_dir_v1_returns_project_root(legacy_project_dir):
    assert get_version_dir(legacy_project_dir, "M31A") == legacy_project_dir
    assert get_version_dir(legacy_project_dir, "M31A", "v1") == legacy_project_dir


def test_get_version_dir_missing_version_raises(legacy_project_dir):
    with pytest.raises(VersionNotFoundError):
        get_version_dir(legacy_project_dir, "M31A", "v99")


# ─── ensure_project_versions_manifest ───────────────────────────────────────


def test_ensure_manifest_creates_file_for_legacy(legacy_project_dir):
    manifest_path = legacy_project_dir / VERSIONS_MANIFEST_FILENAME
    assert not manifest_path.exists()

    manifest = ensure_project_versions_manifest(legacy_project_dir, "M31A")

    assert manifest_path.exists()
    on_disk = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert on_disk["schema_version"] == 1
    assert on_disk["latest_version_id"] == "v1"
    assert on_disk["versions"][0]["version_id"] == "v1"
    assert manifest == on_disk


def test_ensure_manifest_idempotent(legacy_project_dir):
    """Второй вызов не должен перезаписать существующий манифест."""
    first = ensure_project_versions_manifest(legacy_project_dir, "M31A")
    first_created_at = first["versions"][0]["created_at"]

    second = ensure_project_versions_manifest(legacy_project_dir, "M31A")
    assert second["versions"][0]["created_at"] == first_created_at


def test_ensure_manifest_missing_project_dir_returns_in_memory(tmp_path):
    """Если папки нет, файл не создаётся, но возвращается legacy-структура."""
    fake = tmp_path / "does-not-exist"
    manifest = ensure_project_versions_manifest(fake, "ghost")
    assert manifest["latest_version_id"] == "v1"
    assert not fake.exists()


# ─── Чтение существующего/повреждённого манифеста ──────────────────────────


def test_read_manifest_with_existing_file(legacy_project_dir):
    """Подложим многоверсионный manifest и проверим, что он нормализуется."""
    payload = {
        "schema_version": 1,
        "logical_project_id": "M31A",
        "latest_version_id": "v2",
        "versions": [
            {
                "version_id": "v1",
                "version_no": 1,
                "label": "V1",
                "folder": ".",
                "created_at": "2026-01-01T00:00:00",
                "status": "legacy",
                "source": "legacy",
            },
            {
                "version_id": "v2",
                "version_no": 2,
                "label": "V2 (изм. 1)",
                "folder": "_versions/v2",
                "created_at": "2026-05-13T10:00:00",
                "status": "draft",
                "source": "manual",
            },
        ],
    }
    (legacy_project_dir / VERSIONS_MANIFEST_FILENAME).write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )

    manifest = read_project_versions(legacy_project_dir, "M31A")
    assert manifest["latest_version_id"] == "v2"
    assert len(manifest["versions"]) == 2

    v2 = manifest["versions"][1]
    assert v2["version_id"] == "v2"
    assert v2["folder"] == "_versions/v2"


def test_corrupted_manifest_falls_back_to_legacy(legacy_project_dir):
    """Невалидный JSON → возвращаем legacy in-memory без падения."""
    (legacy_project_dir / VERSIONS_MANIFEST_FILENAME).write_text(
        "{broken json", encoding="utf-8"
    )

    manifest = read_project_versions(legacy_project_dir, "M31A")
    assert manifest["latest_version_id"] == "v1"
    assert manifest["versions"][0]["folder"] == "."


def test_manifest_with_invalid_latest_id_recovers(legacy_project_dir):
    """latest_version_id указывает на несуществующую версию → берём последнюю."""
    payload = {
        "schema_version": 1,
        "logical_project_id": "M31A",
        "latest_version_id": "v42",
        "versions": [
            {"version_id": "v1", "version_no": 1, "label": "V1", "folder": "."}
        ],
    }
    (legacy_project_dir / VERSIONS_MANIFEST_FILENAME).write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )

    manifest = read_project_versions(legacy_project_dir, "M31A")
    assert manifest["latest_version_id"] == "v1"


# ─── get_versions_summary ──────────────────────────────────────────────────


def test_versions_summary_for_legacy(legacy_project_dir):
    summary = get_versions_summary(legacy_project_dir, "M31A")

    assert summary["project_id"] == "M31A"
    assert summary["latest_version_id"] == "v1"
    assert summary["version_count"] == 1
    assert summary["has_versions"] is False

    v1 = summary["versions"][0]
    assert v1["version_id"] == "v1"
    assert v1["is_latest"] is True


# ─── create_next_version ───────────────────────────────────────────────────


def test_create_next_version_writes_v2(legacy_project_dir):
    new_entry = create_next_version(
        legacy_project_dir, "M31A", label="V2 (изм. 1)", source="upload"
    )

    assert new_entry["version_id"] == "v2"
    assert new_entry["version_no"] == 2
    assert new_entry["folder"] == "_versions/v2"
    assert (legacy_project_dir / "_versions" / "v2" / "_output").is_dir()

    manifest = read_project_versions(legacy_project_dir, "M31A")
    assert manifest["latest_version_id"] == "v2"
    assert get_version_dir(legacy_project_dir, "M31A") == (
        legacy_project_dir / "_versions" / "v2"
    )


def test_create_next_version_stores_comment_and_seeds_info(legacy_project_dir):
    new_entry = create_next_version(
        legacy_project_dir, "M31A",
        comment="Новая редакция документации",
        source="manual",
    )
    assert new_entry["comment"] == "Новая редакция документации"
    assert new_entry["status"] == "new"

    # Seed project_info.json создан в папке V2
    seed_path = legacy_project_dir / "_versions" / "v2" / "project_info.json"
    assert seed_path.exists()
    seed = json.loads(seed_path.read_text(encoding="utf-8"))
    assert seed["project_id"] == "M31A"
    assert seed["version_id"] == "v2"
    assert seed["version_comment"] == "Новая редакция документации"
    assert seed["pdf_files"] == []  # V1 НЕ копируется

    # Манифест на диске тоже содержит comment
    manifest = read_project_versions(legacy_project_dir, "M31A")
    v2 = next(v for v in manifest["versions"] if v["version_id"] == "v2")
    assert v2["comment"] == "Новая редакция документации"


def test_create_v3_after_v2(legacy_project_dir):
    create_next_version(legacy_project_dir, "M31A", source="manual")
    create_next_version(legacy_project_dir, "M31A", source="manual")

    manifest = read_project_versions(legacy_project_dir, "M31A")
    assert manifest["latest_version_id"] == "v3"
    assert len(manifest["versions"]) == 3
    assert (legacy_project_dir / "_versions" / "v3" / "_output").is_dir()


# ─── API endpoint ──────────────────────────────────────────────────────────


@pytest.fixture
def api_client(tmp_path, monkeypatch):
    """TestClient, в котором PROJECTS_DIR подменён на изолированный tmp_path."""
    # Создаём fake projects dir и один legacy-проект внутри
    projects_dir = tmp_path / "projects"
    projects_dir.mkdir()
    pdir = projects_dir / "M31A"
    (pdir / "_output").mkdir(parents=True)
    info = {
        "project_id": "M31A",
        "name": "M31A",
        "section": "EOM",
        "pdf_file": "document.pdf",
    }
    (pdir / "project_info.json").write_text(
        json.dumps(info, ensure_ascii=False), encoding="utf-8"
    )

    # Подменяем _get_projects_dir в project_service
    import backend.app.services.common.project_service as ps

    monkeypatch.setattr(ps, "_get_projects_dir", lambda: projects_dir)
    # Сбрасываем кеш iter_project_dirs
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)

    from backend.app.main import app

    return TestClient(app), projects_dir


def test_api_versions_endpoint_legacy(api_client):
    client, projects_dir = api_client
    resp = client.get("/api/projects/M31A/versions")
    assert resp.status_code == 200

    data = resp.json()
    assert data["project_id"] == "M31A"
    assert data["latest_version_id"] == "v1"
    assert data["version_count"] == 1
    assert data["has_versions"] is False
    assert data["versions"][0]["version_id"] == "v1"
    assert data["versions"][0]["is_latest"] is True

    # GET не должен создавать файл-манифест
    assert not (projects_dir / "M31A" / VERSIONS_MANIFEST_FILENAME).exists()


def test_api_versions_endpoint_404(api_client):
    client, _ = api_client
    resp = client.get("/api/projects/does-not-exist/versions")
    assert resp.status_code == 404


def test_api_ensure_manifest_endpoint(api_client):
    client, projects_dir = api_client
    resp = client.post("/api/projects/M31A/versions/ensure-manifest")
    assert resp.status_code == 200

    manifest_path = projects_dir / "M31A" / VERSIONS_MANIFEST_FILENAME
    assert manifest_path.exists()
    on_disk = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert on_disk["latest_version_id"] == "v1"


def test_api_get_project_includes_version_fields(api_client):
    client, _ = api_client
    resp = client.get("/api/projects/M31A")
    assert resp.status_code == 200

    data = resp.json()
    assert data["version_id"] == "v1"
    assert data["version_no"] == 1
    assert data["version_label"] == "V1"
    assert data["latest_version_id"] == "v1"
    assert data["version_count"] == 1
    assert data["has_versions"] is False
    assert data["is_latest_version"] is True
    assert isinstance(data["versions_summary"], list)
    assert data["versions_summary"][0]["version_id"] == "v1"


# ─── V2 / dashboard isolation ───────────────────────────────────────────────


def _seed_v1_findings(projects_dir: Path):
    """Положить в V1 (корень) findings и optimization, чтобы убедиться,
    что V2 их НЕ подтягивает."""
    output = projects_dir / "M31A" / "_output"
    output.mkdir(parents=True, exist_ok=True)
    findings = {
        "findings": [
            {"id": "F-001", "severity": "КРИТИЧЕСКОЕ"},
            {"id": "F-002", "severity": "ЭКОНОМИЧЕСКОЕ"},
            {"id": "F-003", "severity": "КРИТИЧЕСКОЕ"},
        ],
        "audit_date": "2026-05-01T00:00:00",
    }
    (output / "03_findings.json").write_text(
        json.dumps(findings, ensure_ascii=False), encoding="utf-8"
    )
    opt = {"meta": {"total_items": 5, "by_type": {"cable": 5}, "estimated_savings_pct": 12}}
    (output / "optimization.json").write_text(
        json.dumps(opt, ensure_ascii=False), encoding="utf-8"
    )


def test_v1_status_shows_legacy_findings(api_client):
    client, projects_dir = api_client
    _seed_v1_findings(projects_dir)

    resp = client.get("/api/projects/M31A")
    assert resp.status_code == 200
    data = resp.json()
    assert data["version_id"] == "v1"
    assert data["findings_count"] == 3
    assert data["optimization_count"] == 5


def test_api_create_v2(api_client):
    client, projects_dir = api_client
    resp = client.post(
        "/api/projects/M31A/versions",
        json={"comment": "Новая редакция", "source": "manual"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["version"]["version_id"] == "v2"
    assert body["latest_version_id"] == "v2"
    assert body["version_count"] == 2

    # ФС: появилась папка V2 с пустым _output
    v2_dir = projects_dir / "M31A" / "_versions" / "v2"
    assert (v2_dir / "_output").is_dir()
    assert (v2_dir / "project_info.json").exists()

    # Манифест содержит обе версии и V2 — latest
    versions_resp = client.get("/api/projects/M31A/versions")
    versions = versions_resp.json()
    assert versions["latest_version_id"] == "v2"
    assert versions["version_count"] == 2
    by_id = {v["version_id"]: v for v in versions["versions"]}
    assert by_id["v1"]["is_latest"] is False
    assert by_id["v2"]["is_latest"] is True


def test_dashboard_shows_v2_with_zero_counts_after_v2_created(api_client):
    """Главное требование: после создания V2 показатели карточки = 0,
    даже если у V1 были findings/optimizations."""
    client, projects_dir = api_client
    _seed_v1_findings(projects_dir)

    # Создаём V2
    client.post("/api/projects/M31A/versions", json={"comment": "V2"})

    # Без version_id → latest (V2) с нулевыми показателями
    resp = client.get("/api/projects/M31A")
    assert resp.status_code == 200
    data = resp.json()
    assert data["version_id"] == "v2"
    assert data["is_latest_version"] is True
    assert data["findings_count"] == 0
    assert data["optimization_count"] == 0
    assert data["findings_by_severity"] == {}
    assert data["last_audit_date"] is None
    # Pipeline у V2 ещё не запускался — все этапы pending
    assert data["pipeline"]["findings"] == "pending"
    assert data["pipeline"]["text_analysis"] == "pending"


def test_get_project_v1_still_accessible_via_query(api_client):
    """V1 должна оставаться доступной через ?version_id=v1 после создания V2."""
    client, projects_dir = api_client
    _seed_v1_findings(projects_dir)
    client.post("/api/projects/M31A/versions", json={"comment": "V2"})

    resp = client.get("/api/projects/M31A", params={"version_id": "v1"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["version_id"] == "v1"
    assert data["is_latest_version"] is False
    assert data["findings_count"] == 3
    assert data["optimization_count"] == 5
    assert data["latest_version_id"] == "v2"


def test_get_project_unknown_version_returns_404(api_client):
    client, _ = api_client
    client.post("/api/projects/M31A/versions", json={})  # создаём V2

    resp = client.get("/api/projects/M31A", params={"version_id": "v999"})
    assert resp.status_code == 404
    assert "v999" in resp.json().get("detail", "")


def test_list_projects_returns_one_card_per_logical_project(api_client):
    """Не должно быть отдельной карточки M31A_V2 — только одна M31A."""
    client, _ = api_client
    client.post("/api/projects/M31A/versions", json={"comment": "V2"})

    resp = client.get("/api/projects")
    assert resp.status_code == 200
    ids = [p["project_id"] for p in resp.json()["projects"]]
    assert ids.count("M31A") == 1
    assert all("v2" not in pid.lower() for pid in ids)

    # И эта единственная карточка — V2
    card = next(p for p in resp.json()["projects"] if p["project_id"] == "M31A")
    assert card["version_id"] == "v2"
    assert card["has_versions"] is True
    assert card["findings_count"] == 0  # V1 не подтягивается
