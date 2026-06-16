"""
Тесты обработки нового класса drift: legacy получил НОВЫЕ analysis-файлы
после миграции, которых нет в old_to_new_map (`legacy_new_file_not_in_map`).
Гермётичны (tmp_path).
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import v2lib                              # noqa: E402
import refresh_migrated_snapshot as rms   # noqa: E402
import scan_migrated_drift as sd          # noqa: E402

DOC = "DOC-NEW"
VER = "v002"


def _sha(t: str) -> str:
    return hashlib.sha256(t.encode()).hexdigest()


def _build_env(tmp_path, *, with_new_analysis=True, with_noise=True, existing_map_files=None):
    """legacy с новыми analysis-файлами + v2 версия + map без них."""
    legacy_folder = tmp_path / "projects" / "obj" / "AR" / "DOC V2.pdf"
    out = legacy_folder / "_output"
    out.mkdir(parents=True)
    if with_new_analysis:
        (out / "01_text_analysis.json").write_text("T", encoding="utf-8")
        (out / "02_blocks_analysis.json").write_text("B", encoding="utf-8")
        (out / "03_findings.json").write_text("F", encoding="utf-8")
    if with_noise:
        # не-whitelist: backup / cache / debug — НЕ должны попадать
        (out / "_bench_backup_1").mkdir()
        (out / "_bench_backup_1" / "x.json").write_text("x", encoding="utf-8")
        (out / "cache").mkdir()
        (out / "cache" / "y.json").write_text("y", encoding="utf-8")
        (out / "step1_locality_debug.json").write_text("d", encoding="utf-8")

    v2_root = tmp_path / "projects_v2"
    v2_doc = v2_root / "objects" / "obj" / "disciplines" / "AR" / "documents" / DOC
    version_root = v2_doc / "versions" / VER
    (version_root / "03_analysis" / "latest").mkdir(parents=True)
    (version_root / "01_input").mkdir(parents=True)
    version_root.joinpath("version.json").write_text(json.dumps({
        "schema_version": 1, "version_id": VER, "version_no": 2,
    }, ensure_ascii=False), encoding="utf-8")

    (v2_root / "_system").mkdir(parents=True)
    map_path = v2_root / "_system" / "old_to_new_map.json"
    map_path.write_text(json.dumps({"schema_version": 1, "migrations": [{
        "object_id": "obj", "document_code": DOC, "version_id": VER,
        "legacy_folder_path": str(legacy_folder), "v2_document_dir": str(v2_doc),
        "analysis_run_id": "run_x",
        "files": existing_map_files or [],
    }]}, ensure_ascii=False), encoding="utf-8")
    return v2_root, map_path, legacy_folder, version_root


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------


def test_scan_finds_new_legacy_file_not_in_map(tmp_path):
    v2_root, *_ = _build_env(tmp_path)
    res = sd.run_scan(v2_root=v2_root, stable_seconds=0)
    types = {r["drift_type"] for r in res["rows"]}
    assert "legacy_new_file_not_in_map" in types
    names = {r["file"] for r in res["rows"] if r["drift_type"] == "legacy_new_file_not_in_map"}
    assert {"01_text_analysis.json", "02_blocks_analysis.json", "03_findings.json"} <= names
    assert res["documents"][0]["recommendation"] == sd.REC_REFRESH_SAFE


def test_scan_excludes_noise(tmp_path):
    v2_root, *_ = _build_env(tmp_path)
    res = sd.run_scan(v2_root=v2_root, stable_seconds=0)
    files = {r["file"] for r in res["rows"]}
    assert "x.json" not in files and "y.json" not in files
    assert "step1_locality_debug.json" not in files


# ---------------------------------------------------------------------------
# refresh --include-new-files
# ---------------------------------------------------------------------------


def test_refresh_adds_to_latest_and_run_refresh(tmp_path):
    v2_root, map_path, legacy_folder, version_root = _build_env(tmp_path)
    res = rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                          stable_seconds=0, execute=True, include_new_files=True)
    s = res["summary"]
    assert s["new_files_added"] >= 3
    latest = version_root / "03_analysis" / "latest"
    assert (latest / "01_text_analysis.json").exists()
    assert (latest / "02_blocks_analysis.json").exists()
    assert (latest / "03_findings.json").exists()
    # run_refresh_* создан с verbatim-копиями
    runs = list((version_root / "03_analysis" / "runs").glob("run_refresh_*"))
    assert len(runs) == 1
    assert (runs[0] / "03_findings.json").read_text(encoding="utf-8") == "F"


def test_refresh_updates_map(tmp_path):
    v2_root, map_path, *_ = _build_env(tmp_path)
    rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                    stable_seconds=0, execute=True, include_new_files=True)
    mp = json.loads(map_path.read_text(encoding="utf-8"))
    files = mp["migrations"][0]["files"]
    new_paths = [f["new_path"] for f in files]
    assert any("03_analysis/latest/01_text_analysis.json" in p for p in new_paths)
    assert any("run_refresh_" in p for p in new_paths)
    # у новых записей есть sha
    assert all(f["sha256"] for f in files if f.get("role") == "run_refresh_new")


def test_refresh_sets_analysis_status_complete(tmp_path):
    v2_root, map_path, legacy_folder, version_root = _build_env(tmp_path)
    rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                    stable_seconds=0, execute=True, include_new_files=True)
    meta = json.loads((version_root / "version.json").read_text(encoding="utf-8"))
    assert meta["analysis_status"] == "complete"
    assert meta["analysis_refresh_reason"] == "legacy_new_analysis_artifacts"


def test_refresh_partial_analysis_status(tmp_path):
    # только 01 + 03 -> partial
    v2_root, map_path, legacy_folder, version_root = _build_env(tmp_path, with_new_analysis=False)
    out = legacy_folder / "_output"
    (out / "01_text_analysis.json").write_text("T", encoding="utf-8")
    (out / "03_findings.json").write_text("F", encoding="utf-8")
    rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                    stable_seconds=0, execute=True, include_new_files=True)
    meta = json.loads((version_root / "version.json").read_text(encoding="utf-8"))
    assert meta["analysis_status"] == "partial"


def test_without_flag_new_files_not_added(tmp_path):
    v2_root, map_path, legacy_folder, version_root = _build_env(tmp_path)
    res = rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                          stable_seconds=0, execute=True, include_new_files=False)
    assert res["summary"]["new_files_added"] == 0
    assert not (version_root / "03_analysis" / "latest" / "01_text_analysis.json").exists()
    assert not list((version_root / "03_analysis" / "runs").glob("run_refresh_*"))


def test_unstable_legacy_not_updated(tmp_path):
    v2_root, map_path, legacy_folder, version_root = _build_env(tmp_path)

    def mutate():
        (legacy_folder / "_output" / "03_findings.json").write_text("CHANGED", encoding="utf-8")

    res = rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                          stable_seconds=0, execute=True, include_new_files=True,
                          between_hook=mutate)
    assert res["stable"] is False
    assert res["summary"]["new_files_added"] == 0
    assert not (version_root / "03_analysis" / "latest" / "01_text_analysis.json").exists()


def test_legacy_not_modified(tmp_path):
    v2_root, map_path, legacy_folder, version_root = _build_env(tmp_path)
    before = {str(p.relative_to(legacy_folder)): (v2lib.sha256_file(p), int(p.stat().st_mtime))
              for p in legacy_folder.rglob("*") if p.is_file()}
    rms.run_refresh(v2_root=v2_root, document=DOC, version=VER,
                    stable_seconds=0, execute=True, include_new_files=True)
    after = {str(p.relative_to(legacy_folder)): (v2lib.sha256_file(p), int(p.stat().st_mtime))
             for p in legacy_folder.rglob("*") if p.is_file()}
    assert before == after
