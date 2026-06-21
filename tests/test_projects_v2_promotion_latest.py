"""B2: promotion of projects_v2 run artifacts into latest."""
from __future__ import annotations

import json
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"
_V2DIR = "AUDIT_PROJECTS_V2_DIR"


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _make_v2_doc(
    v2_root: Path,
    *,
    obj_folder: str = "OBJ_FOLDER",
    disc: str = "KJ",
    doc_code: str = "DOC-B2",
    object_id: str = "obj-b2",
    versions=("v001",),
) -> Path:
    doc = v2_root / "objects" / obj_folder / "disciplines" / disc / "documents" / doc_code
    doc.mkdir(parents=True, exist_ok=True)
    _write_json(doc / "document.json", {
        "schema_version": 1,
        "document_code": doc_code,
        "object_id": object_id,
        "discipline": disc,
        "versions": [{"version_id": v, "version_no": i + 1} for i, v in enumerate(versions)],
        "current_version": versions[-1],
    })
    (doc / "current_version.txt").write_text(versions[-1] + "\n", encoding="utf-8")
    for version_id in versions:
        vdir = doc / "versions" / version_id
        _write_json(vdir / "version.json", {"version_id": version_id})
    return doc


def _manager():
    from backend.app.pipeline.manager import PipelineManager

    return object.__new__(PipelineManager)


def _job(project_id: str = "DOC-B2", version_id: str = "v001", job_id: str = "job_b2_1"):
    return types.SimpleNamespace(
        project_id=project_id,
        version_id=version_id,
        job_id=job_id,
        object_id=None,
    )


def _patch_no_legacy(monkeypatch):
    import backend.app.pipeline.manager as mgr

    monkeypatch.setattr(
        mgr,
        "resolve_project_dir",
        lambda pid, **kw: (_ for _ in ()).throw(FileNotFoundError(pid)),
    )


def test_v2_promote_per_stage(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    doc = _make_v2_doc(v2)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))
    _patch_no_legacy(monkeypatch)

    manager = _manager()
    job = _job()
    _doc_dir, version_dir, output_dir = manager._resolve_job_paths(job)
    payload = {"stage": "text"}
    _write_json(output_dir / "01_text_analysis.json", payload)

    results = manager._promote_v2_analysis_artifacts(job, ("01_text_analysis.json",))

    assert set(results) == {"01_text_analysis.json"}
    latest = version_dir / "03_analysis" / "latest" / "01_text_analysis.json"
    run_file = version_dir / "03_analysis" / "runs" / job.job_id / "01_text_analysis.json"
    assert json.loads(latest.read_text(encoding="utf-8")) == payload
    assert json.loads(run_file.read_text(encoding="utf-8")) == payload
    assert latest.read_bytes() == run_file.read_bytes()
    assert latest.resolve().is_relative_to(doc.resolve())


def test_v2_promote_end_of_audit_is_idempotent(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    _make_v2_doc(v2)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))
    _patch_no_legacy(monkeypatch)

    manager = _manager()
    job = _job(job_id="job_b2_done")
    _doc_dir, version_dir, output_dir = manager._resolve_job_paths(job)
    artifacts = {
        "03_findings.json": {"findings": [{"id": "F-1"}]},
        "norm_checks.json": {"checks": [1]},
        "optimization.json": {"items": ["x"]},
        "pipeline_log.json": {"stages": {"excel": {"status": "done"}}},
    }
    for name, payload in artifacts.items():
        _write_json(output_dir / name, payload)

    first = manager._promote_completed_audit_v2(job)
    second = manager._promote_completed_audit_v2(job)

    assert set(first) == set(artifacts)
    assert set(second) == set(artifacts)
    for name, payload in artifacts.items():
        latest = version_dir / "03_analysis" / "latest" / name
        run_file = version_dir / "03_analysis" / "runs" / job.job_id / name
        assert json.loads(latest.read_text(encoding="utf-8")) == payload
        assert latest.read_bytes() == run_file.read_bytes()


def test_v2_promotion_noop_legacy(monkeypatch, tmp_path):
    monkeypatch.setenv(_WMODE, "legacy")
    monkeypatch.setenv(_V2DIR, str(tmp_path / "projects_v2"))

    manager = _manager()
    assert manager._promote_v2_analysis_artifacts(_job(), ("01_text_analysis.json",)) == {}
    assert not (tmp_path / "projects_v2").exists()


def test_v2_adapter_fallback_read_from_runs_when_latest_empty(tmp_path):
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

    v2 = tmp_path / "projects_v2"
    doc = _make_v2_doc(v2)
    run_dir = doc / "versions" / "v001" / "03_analysis" / "runs" / "job_b2_run"
    _write_json(run_dir / "01_text_analysis.json", {"source": "run"})
    _write_json(run_dir / "02_blocks_analysis.json", {"blocks": [1]})
    _write_json(run_dir / "03_findings.json", {"findings": [{"severity": "high"}]})
    _write_json(run_dir / "pipeline_log.json", {"stages": {"done": True}})

    adapter = ProjectsV2Adapter(v2)
    analysis = adapter.latest_analysis_files(doc, "v001")

    assert analysis["has_01_text_analysis"] is True
    assert analysis["has_02_blocks_analysis"] is True
    assert analysis["has_03_findings"] is True
    assert adapter.read_text_analysis(doc, "v001") == {"source": "run"}
    assert adapter.read_blocks_analysis(doc, "v001") == {"blocks": [1]}
    assert adapter.findings_count(doc, "v001") == 1
    assert adapter.read_pipeline_log(doc, "v001") == {"stages": {"done": True}}


def test_v2_adapter_falls_back_per_file_when_latest_partial(tmp_path):
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

    v2 = tmp_path / "projects_v2"
    doc = _make_v2_doc(v2)
    latest = doc / "versions" / "v001" / "03_analysis" / "latest"
    run_dir = doc / "versions" / "v001" / "03_analysis" / "runs" / "job_b2_run"
    _write_json(latest / "01_text_analysis.json", {"source": "latest"})
    _write_json(run_dir / "01_text_analysis.json", {"source": "run"})
    _write_json(run_dir / "02_blocks_analysis.json", {"blocks": [1]})
    _write_json(run_dir / "03_findings.json", {"findings": [{"severity": "high"}]})

    adapter = ProjectsV2Adapter(v2)
    analysis = adapter.latest_analysis_files(doc, "v001")

    assert adapter.read_text_analysis(doc, "v001") == {"source": "latest"}
    assert adapter.read_blocks_analysis(doc, "v001") == {"blocks": [1]}
    assert adapter.read_findings(doc, "v001") == {"findings": [{"severity": "high"}]}
    assert analysis["present"] == [
        "01_text_analysis.json",
        "02_blocks_analysis.json",
        "03_findings.json",
    ]
    assert analysis["has_01_text_analysis"] is True
    assert analysis["has_02_blocks_analysis"] is True
    assert analysis["has_03_findings"] is True
