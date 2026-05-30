"""
test_migrate_versions_to_container.py
-------------------------------------
Тесты одноразового мигратора legacy `_versions/v{N}/` → контейнер `<база>(main)/`.

Run:
    python -m pytest tests/test_migrate_versions_to_container.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common import version_service as vs  # noqa: E402
from backend.scripts import migrate_versions_to_container as mig  # noqa: E402


def _make_legacy_project(projects_dir: Path, base: str = "M31A") -> Path:
    """Создать legacy-проект `<disc>/<base>` с `_versions/v2` и manifest."""
    pdir = projects_dir / "EOM" / base
    (pdir / "_output").mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({"project_id": base, "name": base, "section": "EOM",
                    "pdf_file": "v1.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (pdir / "v1.pdf").write_text("V1PDF", encoding="utf-8")
    (pdir / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-V1"}]}), encoding="utf-8")

    v2 = pdir / "_versions" / "v2"
    (v2 / "_output").mkdir(parents=True)
    (v2 / "project_info.json").write_text(
        json.dumps({"project_id": base, "version_id": "v2"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (v2 / "v2.pdf").write_text("V2PDF", encoding="utf-8")

    (pdir / vs.VERSIONS_MANIFEST_FILENAME).write_text(
        json.dumps({
            "schema_version": 1,
            "logical_project_id": base,
            "latest_version_id": "v2",
            "versions": [
                {"version_id": "v1", "version_no": 1, "label": "V1",
                 "folder": ".", "created_at": "2026-05-01T00:00:00",
                 "status": "legacy", "source": "legacy"},
                {"version_id": "v2", "version_no": 2, "label": "V2",
                 "folder": "_versions/v2", "created_at": "2026-05-02T00:00:00",
                 "status": "new", "source": "edit_projects_modal",
                 "comment": "редакция 2"},
            ],
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    return pdir


def test_dry_run_changes_nothing(tmp_path):
    projects = tmp_path / "projects"
    pdir = _make_legacy_project(projects)

    legacy = mig.find_legacy_projects(projects)
    assert legacy == [pdir]

    plan = mig.plan_project(pdir)
    assert plan is not None
    # dry-run: применять не вызываем — на диске всё по-старому.
    assert (pdir / "_versions" / "v2").exists()
    assert not (projects / "EOM" / "M31A(main)").exists()


def test_apply_migrates_to_container(tmp_path):
    projects = tmp_path / "projects"
    pdir = _make_legacy_project(projects)

    plan = mig.plan_project(pdir)
    log: list = []
    mig.apply_plan(plan, log)

    container = projects / "EOM" / "M31A(main)"
    assert container.is_dir()
    # V1 переехал под родным именем, данные на месте.
    assert (container / "M31A" / "v1.pdf").read_text(encoding="utf-8") == "V1PDF"
    assert (container / "M31A" / "_output" / "03_findings.json").exists()
    # V2 стал братской папкой `<база> V2` с данными.
    assert (container / "M31A V2" / "v2.pdf").read_text(encoding="utf-8") == "V2PDF"
    # Старая папка проекта и _versions исчезли.
    assert not pdir.exists()
    assert not (container / "M31A" / "_versions").exists()
    # version_group.json записан и резолвится новым кодом.
    primary = container / "M31A"
    summary = vs.get_versions_summary(primary, "M31A")
    assert summary["latest_version_id"] == "v2"
    assert [v["folder"] for v in summary["versions"]] == ["M31A", "M31A V2"]
    assert vs.get_version_dir(primary, "M31A", "v2") == container / "M31A V2"


def test_basename_project_id_stable_after_migration(tmp_path, monkeypatch):
    """project_id (basename) не меняется → ссылки в KB/decisions не ломаются."""
    projects = tmp_path / "projects"
    pdir = _make_legacy_project(projects)
    mig.apply_plan(mig.plan_project(pdir), [])

    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: projects)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)

    ids = sorted(pid for pid, _ in ps.iter_project_dirs(force=True))
    assert ids == ["M31A"]  # один проект, basename сохранён
    resolved = ps.resolve_project_dir("M31A")
    assert resolved == projects / "EOM" / "M31A(main)" / "M31A"


def test_idempotent_skips_already_migrated(tmp_path):
    projects = tmp_path / "projects"
    pdir = _make_legacy_project(projects)
    mig.apply_plan(mig.plan_project(pdir), [])

    # Повторный поиск legacy уже ничего не находит (контейнер пропускается).
    assert mig.find_legacy_projects(projects) == []
