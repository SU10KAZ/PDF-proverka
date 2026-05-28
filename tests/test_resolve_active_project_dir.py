"""
test_resolve_active_project_dir.py
----------------------------------
`resolve_active_project_dir` должен возвращать папку активной версии:
  - latest_version_id из project_versions.json (V2+ → _versions/v2/);
  - корень для legacy-проектов без манифеста;
  - bound version (через version_service.bind_version) перекрывает latest.

Это нужно prepare-pipeline'у, который иначе режет блоки из v1, хотя
оператор уже загрузил v2.

Run:
    python -m pytest tests/test_resolve_active_project_dir.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


@pytest.fixture
def projects_dir(tmp_path, monkeypatch):
    """Изолированный projects/ с одним M31A, у которого V1 в корне."""
    p = tmp_path / "projects"
    p.mkdir()
    pdir = p / "M31A"
    pdir.mkdir()
    (pdir / "project_info.json").write_text(
        json.dumps({"project_id": "M31A", "name": "M31A", "pdf_file": "v1.pdf"},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    (pdir / "v1.pdf").write_bytes(b"%PDF-1.4 v1")

    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})
    return p


def _write_manifest_v2(project_root: Path) -> Path:
    """Создать project_versions.json с latest=v2 + _versions/v2/."""
    manifest = {
        "schema_version": 1,
        "logical_project_id": "M31A",
        "latest_version_id": "v2",
        "versions": [
            {"version_id": "v1", "version_no": 1, "label": "V1",
             "folder": ".", "created_at": "2026-05-01T00:00:00",
             "status": "legacy", "source": "legacy"},
            {"version_id": "v2", "version_no": 2, "label": "V2",
             "folder": "_versions/v2", "created_at": "2026-05-28T00:00:00",
             "status": "new", "source": "edit_projects_modal"},
        ],
    }
    (project_root / "project_versions.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    v2_dir = project_root / "_versions" / "v2"
    v2_dir.mkdir(parents=True)
    (v2_dir / "v2.pdf").write_bytes(b"%PDF-1.4 v2")
    return v2_dir


def test_legacy_without_manifest_returns_root(projects_dir):
    """Проект без project_versions.json → корень (V1)."""
    from backend.app.services.common.project_service import (
        resolve_project_dir, resolve_active_project_dir,
    )
    root = resolve_project_dir("M31A")
    active = resolve_active_project_dir("M31A")
    assert active == root
    assert (active / "v1.pdf").exists()


def test_manifest_with_latest_v2_returns_version_dir(projects_dir):
    """latest_version_id=v2 → _versions/v2/, не корень."""
    from backend.app.services.common.project_service import (
        resolve_project_dir, resolve_active_project_dir,
    )
    root = resolve_project_dir("M31A")
    v2_dir = _write_manifest_v2(root)

    active = resolve_active_project_dir("M31A")
    assert active == v2_dir
    assert active != root
    assert (active / "v2.pdf").exists()


def test_bound_version_overrides_latest(projects_dir):
    """version_service.bind_version('v1') → корень даже при latest=v2."""
    from backend.app.services.common.project_service import (
        resolve_project_dir, resolve_active_project_dir,
    )
    from backend.app.services.common import version_service

    root = resolve_project_dir("M31A")
    _write_manifest_v2(root)

    token = version_service.bind_version("v1")
    try:
        active = resolve_active_project_dir("M31A")
        assert active == root
    finally:
        version_service.unbind_version(token)

    # После сброса binding снова возвращается latest=v2
    active_after = resolve_active_project_dir("M31A")
    assert active_after == root / "_versions" / "v2"
