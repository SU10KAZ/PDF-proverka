"""
Тесты refresh_migrated_snapshot — безопасное обновление snapshot одного
мигрированного документа при legacy drift. Гермётичны (tmp_path).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"
sys.path.insert(0, str(_SCRIPTS))
import v2lib                          # noqa: E402
import refresh_migrated_snapshot as rms  # noqa: E402

DOC = "13АВ-РД-АР3-К6"
VER = "v002"


def _build_env(tmp_path, *, legacy_content: str, v2_content: str, recorded_sha: str):
    """Синтетический legacy + v2 + old_to_new_map с одной записью миграции."""
    legacy_folder = tmp_path / "projects" / "obj" / "AR" / "DOC V2.pdf"
    (legacy_folder / "_output").mkdir(parents=True)
    legacy_file = legacy_folder / "_output" / "pipeline_log.json"
    legacy_file.write_text(legacy_content, encoding="utf-8")

    v2_root = tmp_path / "projects_v2"
    v2_doc = v2_root / "objects" / "obj" / "disciplines" / "AR" / "documents" / DOC
    run_dir = v2_doc / "versions" / VER / "03_analysis" / "runs" / "run_x"
    run_dir.mkdir(parents=True)
    v2_file = run_dir / "pipeline_log.json"
    v2_file.write_text(v2_content, encoding="utf-8")

    (v2_root / "_system").mkdir(parents=True)
    map_path = v2_root / "_system" / "old_to_new_map.json"
    map_path.write_text(json.dumps({"schema_version": 1, "migrations": [{
        "object_id": "obj", "document_code": DOC, "version_id": VER,
        "legacy_folder_path": str(legacy_folder),
        "v2_document_dir": str(v2_doc),
        "analysis_run_id": "run_x",
        "files": [{
            "old_path": str(legacy_file), "new_path": str(v2_file),
            "sha256": recorded_sha, "bytes": len(v2_content), "role": "run",
        }],
    }]}, ensure_ascii=False), encoding="utf-8")
    return v2_root, map_path, legacy_file, v2_file


def _sha(text: str) -> str:
    import hashlib
    return hashlib.sha256(text.encode()).hexdigest()


# ---------------------------------------------------------------------------


def test_unknown_document_errors(tmp_path):
    v2_root, *_ = _build_env(tmp_path, legacy_content="A", v2_content="A", recorded_sha=_sha("A"))
    with pytest.raises(rms.RefreshError):
        rms.run_refresh(v2_root=v2_root, document="NOPE", version=VER,
                        stable_seconds=0, execute=True)
    with pytest.raises(rms.RefreshError):
        rms.run_refresh(v2_root=v2_root, document=DOC, version="v999",
                        stable_seconds=0, execute=True)


def test_dry_run_copies_nothing(tmp_path):
    # legacy drifted to "NEW", recorded/v2 = "OLD"
    v2_root, map_path, legacy_file, v2_file = _build_env(
        tmp_path, legacy_content="NEW", v2_content="OLD", recorded_sha=_sha("OLD"))
    res = rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                          stable_seconds=0, execute=False)
    assert res["stable"] is True
    assert len(res["diffs"]) == 1
    assert res["diffs"][0]["status"] == "modified"
    # v2 копия НЕ изменена, карта НЕ изменена, архива нет
    assert v2_file.read_text(encoding="utf-8") == "OLD"
    mp = json.loads(map_path.read_text(encoding="utf-8"))
    assert mp["migrations"][0]["files"][0]["sha256"] == _sha("OLD")
    assert not (v2_root / "_system" / "refresh_archive").exists()


def test_execute_updates_v2_and_map_and_archive(tmp_path):
    v2_root, map_path, legacy_file, v2_file = _build_env(
        tmp_path, legacy_content="NEW", v2_content="OLD", recorded_sha=_sha("OLD"))
    res = rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                          stable_seconds=0, execute=True)
    assert res["summary"]["applied_count"] == 1
    # v2 копия обновлена до legacy-содержимого
    assert v2_file.read_text(encoding="utf-8") == "NEW"
    # карта обновлена新 sha
    mp = json.loads(map_path.read_text(encoding="utf-8"))
    assert mp["migrations"][0]["files"][0]["sha256"] == _sha("NEW")
    # архив старой копии создан и содержит "OLD"
    arch_root = v2_root / "_system" / "refresh_archive" / v2lib.safe_component(DOC) / VER
    archived = list(arch_root.rglob("pipeline_log.json"))
    assert archived and archived[0].read_text(encoding="utf-8") == "OLD"
    # отчёты записаны
    assert (v2_root / "_system" / "refresh_report.json").exists()
    assert (v2_root / "_system" / "refresh_report.csv").exists()


def test_stability_fail_aborts(tmp_path):
    v2_root, map_path, legacy_file, v2_file = _build_env(
        tmp_path, legacy_content="NEW", v2_content="OLD", recorded_sha=_sha("OLD"))

    def mutate():  # имитируем продолжающийся live-аудит между снимками
        legacy_file.write_text("NEWER", encoding="utf-8")

    res = rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                          stable_seconds=0, execute=True, between_hook=mutate)
    assert res["stable"] is False
    # ничего не обновлено
    assert v2_file.read_text(encoding="utf-8") == "OLD"
    mp = json.loads(map_path.read_text(encoding="utf-8"))
    assert mp["migrations"][0]["files"][0]["sha256"] == _sha("OLD")


def test_unchanged_no_diffs(tmp_path):
    v2_root, map_path, legacy_file, v2_file = _build_env(
        tmp_path, legacy_content="SAME", v2_content="SAME", recorded_sha=_sha("SAME"))
    res = rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                          stable_seconds=0, execute=True)
    assert res["diffs"] == []
    assert res["summary"]["applied_count"] == 0


def test_legacy_not_modified(tmp_path):
    v2_root, map_path, legacy_file, v2_file = _build_env(
        tmp_path, legacy_content="NEW", v2_content="OLD", recorded_sha=_sha("OLD"))
    before = (legacy_file.read_text(encoding="utf-8"), int(legacy_file.stat().st_mtime))
    rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                    stable_seconds=0, execute=True)
    after = (legacy_file.read_text(encoding="utf-8"), int(legacy_file.stat().st_mtime))
    assert before == after  # legacy не тронут (только читается)
