"""
Тесты scan_migrated_drift — read-only диагностика drift по мигрированным
документам. Гермётичны (tmp_path).
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import v2lib                     # noqa: E402
import scan_migrated_drift as sd  # noqa: E402

DOC = "DOC-X"
VER = "v002"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _build_env(tmp_path, *, legacy_content, v2_content, recorded_sha,
               legacy_exists=True, v2_exists=True):
    legacy_folder = tmp_path / "projects" / "obj" / "AR" / "DOC V2.pdf"
    (legacy_folder / "_output").mkdir(parents=True)
    legacy_file = legacy_folder / "_output" / "pipeline_log.json"
    if legacy_exists:
        legacy_file.write_text(legacy_content, encoding="utf-8")

    v2_root = tmp_path / "projects_v2"
    v2_doc = v2_root / "objects" / "obj" / "disciplines" / "AR" / "documents" / DOC
    run_dir = v2_doc / "versions" / VER / "03_analysis" / "runs" / "run_x"
    run_dir.mkdir(parents=True)
    v2_file = run_dir / "pipeline_log.json"
    if v2_exists:
        v2_file.write_text(v2_content, encoding="utf-8")

    (v2_root / "_system").mkdir(parents=True)
    map_path = v2_root / "_system" / "old_to_new_map.json"
    map_path.write_text(json.dumps({"schema_version": 1, "migrations": [{
        "object_id": "obj", "document_code": DOC, "version_id": VER,
        "legacy_folder_path": str(legacy_folder), "v2_document_dir": str(v2_doc),
        "analysis_run_id": "run_x",
        "files": [{"old_path": str(legacy_file), "new_path": str(v2_file),
                   "sha256": recorded_sha, "bytes": len(v2_content), "role": "run"}],
    }]}, ensure_ascii=False), encoding="utf-8")
    return v2_root, map_path, legacy_file, v2_file


# ---------------------------------------------------------------------------


def test_no_drift_empty_report(tmp_path):
    v2_root, *_ = _build_env(tmp_path, legacy_content="SAME", v2_content="SAME",
                             recorded_sha=_sha("SAME"))
    res = sd.run_scan(v2_root=v2_root, stable_seconds=0)
    assert res["summary"]["drift_documents"] == 0
    assert res["rows"] == []


def test_legacy_changed_v2_old(tmp_path):
    v2_root, *_ = _build_env(tmp_path, legacy_content="NEW", v2_content="OLD",
                             recorded_sha=_sha("OLD"))
    res = sd.run_scan(v2_root=v2_root, stable_seconds=0)
    assert res["summary"]["drift_documents"] == 1
    assert res["rows"][0]["drift_type"] == "legacy_changed_v2_old"
    assert res["documents"][0]["recommendation"] == sd.REC_REFRESH_SAFE


def test_v2_changed(tmp_path):
    v2_root, *_ = _build_env(tmp_path, legacy_content="OLD", v2_content="NEW",
                             recorded_sha=_sha("OLD"))
    res = sd.run_scan(v2_root=v2_root, stable_seconds=0)
    assert res["rows"][0]["drift_type"] == "v2_changed"
    assert res["documents"][0]["recommendation"] == sd.REC_MANUAL_REVIEW


def test_missing_legacy(tmp_path):
    v2_root, *_ = _build_env(tmp_path, legacy_content="X", v2_content="X",
                             recorded_sha=_sha("X"), legacy_exists=False)
    res = sd.run_scan(v2_root=v2_root, stable_seconds=0)
    assert res["rows"][0]["drift_type"] == "missing_legacy"
    assert res["documents"][0]["recommendation"] == sd.REC_MANUAL_REVIEW


def test_missing_v2(tmp_path):
    v2_root, *_ = _build_env(tmp_path, legacy_content="X", v2_content="X",
                             recorded_sha=_sha("X"), v2_exists=False)
    res = sd.run_scan(v2_root=v2_root, stable_seconds=0)
    assert res["rows"][0]["drift_type"] == "missing_v2"
    assert res["documents"][0]["recommendation"] == sd.REC_MANUAL_REVIEW


def test_unstable_legacy_wait_backend(tmp_path):
    v2_root, map_path, legacy_file, v2_file = _build_env(
        tmp_path, legacy_content="NEW", v2_content="OLD", recorded_sha=_sha("OLD"))

    def mutate():  # backend продолжает писать между снимками
        legacy_file.write_text("NEWER", encoding="utf-8")

    res = sd.run_scan(v2_root=v2_root, stable_seconds=0, between_hook=mutate)
    assert res["documents"][0]["stable"] is False
    assert res["documents"][0]["recommendation"] == sd.REC_WAIT_BACKEND


def test_stable_drift_refresh_safe(tmp_path):
    v2_root, *_ = _build_env(tmp_path, legacy_content="NEW", v2_content="OLD",
                             recorded_sha=_sha("OLD"))
    res = sd.run_scan(v2_root=v2_root, stable_seconds=0)
    assert res["documents"][0]["stable"] is True
    assert res["documents"][0]["recommendation"] == sd.REC_REFRESH_SAFE


def test_scan_changes_nothing(tmp_path):
    v2_root, map_path, legacy_file, v2_file = _build_env(
        tmp_path, legacy_content="NEW", v2_content="OLD", recorded_sha=_sha("OLD"))
    legacy_before = (legacy_file.read_text(encoding="utf-8"), int(legacy_file.stat().st_mtime))
    v2_before = (v2_file.read_text(encoding="utf-8"), int(v2_file.stat().st_mtime))
    map_before = map_path.read_text(encoding="utf-8")

    sd.run_scan(v2_root=v2_root, stable_seconds=0)

    assert (legacy_file.read_text(encoding="utf-8"), int(legacy_file.stat().st_mtime)) == legacy_before
    assert (v2_file.read_text(encoding="utf-8"), int(v2_file.stat().st_mtime)) == v2_before
    assert map_path.read_text(encoding="utf-8") == map_before  # карта не тронута scan'ом


def test_classify_file_unchanged_returns_none(tmp_path):
    p = tmp_path / "a.json"; p.write_text("Z", encoding="utf-8")
    q = tmp_path / "b.json"; q.write_text("Z", encoding="utf-8")
    f = {"old_path": str(p), "new_path": str(q), "sha256": _sha("Z")}
    assert sd.classify_file(f) is None
    # untracked (sha None) тоже None
    assert sd.classify_file({"old_path": str(p), "new_path": str(q), "sha256": None}) is None
