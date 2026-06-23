from __future__ import annotations

import json
from pathlib import Path

from backend.app.models.expert_review import ExpertDecision


def _write_json(path: Path, payload: dict | list) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _make_v2_doc(v2_root: Path, code: str = "DOC-REVIEW") -> tuple[Path, Path]:
    doc_dir = v2_root / "objects" / "OBJ" / "disciplines" / "AR" / "documents" / code
    vdir = doc_dir / "versions" / "v001"
    for rel in ("01_input", "02_work", "03_analysis/latest", "04_review", "05_export"):
        (vdir / rel).mkdir(parents=True, exist_ok=True)
    _write_json(v2_root / "objects" / "OBJ" / "object.json", {
        "schema_version": 1,
        "object_id": "obj-review",
        "display_name": "OBJ",
        "folder_name": "OBJ",
    })
    _write_json(doc_dir / "document.json", {
        "schema_version": 1,
        "document_code": code,
        "object_id": "obj-review",
        "object_folder": "OBJ",
        "discipline": "AR",
        "current_version": "v001",
        "versions": [{"version_id": "v001", "version_no": 1, "label": "V1"}],
        "version_ids": ["v001"],
    })
    (doc_dir / "current_version.txt").write_text("v001", encoding="utf-8")
    info = {"project_id": code, "document_code": code, "section": "AR"}
    _write_json(vdir / "01_input" / "project_info.json", info)
    _write_json(vdir / "version.json", {
        "schema_version": 1,
        "version_id": "v001",
        "version_no": 1,
        "label": "V1",
        "project_info": info,
    })
    return doc_dir, vdir


def _enable_v2_primary(monkeypatch, v2_root: Path) -> None:
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "projects_v2")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")


def _read_review_by_canary_priority(vdir: Path) -> dict:
    for path in (
        vdir / "04_review" / "expert_review.json",
        vdir / "03_analysis" / "latest" / "expert_review.json",
        vdir / "_output" / "expert_review.json",
    ):
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
    return {"decisions": []}


def _optimization_review_status(vdir: Path) -> str:
    review = _read_review_by_canary_priority(vdir)
    decisions = review.get("decisions") or []
    opt_ids = [
        item.get("id")
        for item in json.loads((vdir / "03_analysis" / "latest" / "optimization.json").read_text(encoding="utf-8")).get("items", [])
        if item.get("id")
    ]
    reviewed = {
        d.get("item_id")
        for d in decisions
        if d.get("item_type") == "optimization"
        and d.get("decision") in ("accepted", "rejected")
    }
    if opt_ids and all(item_id in reviewed for item_id in opt_ids):
        return "complete"
    if reviewed:
        return "partial"
    return ""


def _patch_kb_side_effects(monkeypatch, tmp_path):
    import backend.app.services.common.project_service as project_service
    import backend.app.services.knowledge_base.knowledge_base_service as kb
    from backend.app.services.storage import storage_write_facade as swf

    kb_root = tmp_path / "knowledge_base"
    monkeypatch.setattr(kb, "KNOWLEDGE_BASE_DIR", kb_root)
    monkeypatch.setattr(kb, "DECISIONS_LOG_FILE", kb_root / "decisions_log.json")
    monkeypatch.setattr(kb, "PATTERNS_FILE", kb_root / "patterns.json")
    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", lambda *args, **kwargs: None)
    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: tmp_path / "projects")
    return kb


def test_v2_primary_expert_review_saves_optimizations_to_canonical_04_review(monkeypatch, tmp_path):
    kb = _patch_kb_side_effects(monkeypatch, tmp_path)
    v2_root = tmp_path / "projects_v2"
    _doc_dir, vdir = _make_v2_doc(v2_root)
    legacy_project = tmp_path / "projects" / "DOC-REVIEW"
    legacy_project.mkdir(parents=True)
    _write_json(legacy_project / "project_info.json", {"project_id": "DOC-REVIEW", "section": "AR"})
    _enable_v2_primary(monkeypatch, v2_root)

    _write_json(vdir / "03_analysis" / "latest" / "optimization.json", {
        "items": [{"id": "OPT-1"}, {"id": "OPT-2"}],
        "meta": {"total_items": 2},
    })
    _write_json(vdir / "04_review" / "expert_review.json", {
        "project_id": "DOC-REVIEW",
        "decisions": [{
            "item_id": "F-1",
            "item_type": "finding",
            "decision": "accepted",
        }],
    })

    kb.save_expert_review("DOC-REVIEW", [
        ExpertDecision(item_id="OPT-1", item_type="optimization", decision="accepted"),
        ExpertDecision(item_id="OPT-2", item_type="optimization", decision="rejected"),
    ], reviewer="qa")

    canonical = json.loads((vdir / "04_review" / "expert_review.json").read_text(encoding="utf-8"))
    keys = {(d.get("item_type"), d.get("item_id")) for d in canonical["decisions"]}

    assert keys == {("finding", "F-1"), ("optimization", "OPT-1"), ("optimization", "OPT-2")}
    assert _optimization_review_status(vdir) == "complete"
    assert not (vdir / "_output" / "expert_review.json").exists()

    kb.save_expert_review("DOC-REVIEW", [
        ExpertDecision(item_id="OPT-1", item_type="optimization", decision="accepted"),
    ], reviewer="qa")
    canonical = json.loads((vdir / "04_review" / "expert_review.json").read_text(encoding="utf-8"))
    keys = [(d.get("item_type"), d.get("item_id")) for d in canonical["decisions"]]

    assert keys.count(("optimization", "OPT-1")) == 1
    assert set(keys) == {("finding", "F-1"), ("optimization", "OPT-1"), ("optimization", "OPT-2")}


def test_v2_primary_load_expert_review_prefers_04_review_over_fallbacks(monkeypatch, tmp_path):
    kb = _patch_kb_side_effects(monkeypatch, tmp_path)
    v2_root = tmp_path / "projects_v2"
    _doc_dir, vdir = _make_v2_doc(v2_root, "DOC-PRIORITY")
    _enable_v2_primary(monkeypatch, v2_root)
    _write_json(vdir / "04_review" / "expert_review.json", {
        "source": "04_review",
        "decisions": [{"item_id": "F-1", "item_type": "finding", "decision": "accepted"}],
    })
    _write_json(vdir / "03_analysis" / "latest" / "expert_review.json", {
        "source": "latest",
        "decisions": [{"item_id": "F-2", "item_type": "finding", "decision": "accepted"}],
    })
    _write_json(vdir / "_output" / "expert_review.json", {
        "source": "_output",
        "decisions": [{"item_id": "OPT-1", "item_type": "optimization", "decision": "accepted"}],
    })

    assert kb.load_expert_review("DOC-PRIORITY")["source"] == "04_review"


def test_legacy_expert_review_still_writes_output_and_merges_by_type_and_id(monkeypatch, tmp_path):
    kb = _patch_kb_side_effects(monkeypatch, tmp_path)
    legacy_project = tmp_path / "projects" / "DOC-LEGACY"
    out = legacy_project / "_output"
    out.mkdir(parents=True)
    _write_json(legacy_project / "project_info.json", {"project_id": "DOC-LEGACY", "section": "AR"})
    _write_json(out / "expert_review.json", {
        "project_id": "DOC-LEGACY",
        "decisions": [
            {"item_id": "F-1", "item_type": "finding", "decision": "accepted"},
            {"item_id": "SAME", "item_type": "finding", "decision": "accepted"},
        ],
    })

    kb.save_expert_review("DOC-LEGACY", [
        ExpertDecision(item_id="SAME", item_type="optimization", decision="rejected"),
    ], reviewer="qa")

    review = json.loads((out / "expert_review.json").read_text(encoding="utf-8"))
    keys = {(d.get("item_type"), d.get("item_id")) for d in review["decisions"]}

    assert keys == {
        ("finding", "F-1"),
        ("finding", "SAME"),
        ("optimization", "SAME"),
    }
    assert not (legacy_project / "04_review").exists()


def test_scan_expert_review_split_reports_output_decisions_missing_from_04(tmp_path):
    import importlib.util

    script_path = Path("scripts/projects_v2/scan_expert_review_split.py")
    spec = importlib.util.spec_from_file_location("scan_expert_review_split", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    v2_root = tmp_path / "projects_v2"
    _doc_dir, vdir = _make_v2_doc(v2_root, "DOC-SCAN")
    _write_json(vdir / "04_review" / "expert_review.json", {
        "decisions": [{
            "item_id": "F-1",
            "item_type": "finding",
            "decision": "accepted",
        }],
    })
    _write_json(vdir / "_output" / "expert_review.json", {
        "decisions": [
            {"item_id": "F-1", "item_type": "finding", "decision": "accepted"},
            {"item_id": "OPT-1", "item_type": "optimization", "decision": "rejected"},
        ],
    })

    report = module.scan(v2_root)

    assert report["split_count"] == 1
    assert report["split_versions"][0]["document_code"] == "DOC-SCAN"
    assert report["split_versions"][0]["missing_decisions"] == [{
        "item_type": "optimization",
        "item_id": "OPT-1",
        "decision": "rejected",
    }]

    _write_json(vdir / "04_review" / "expert_review.json", {
        "decisions": [
            {"item_id": "F-1", "item_type": "finding", "decision": "accepted"},
            {"item_id": "OPT-1", "item_type": "optimization", "decision": "rejected"},
        ],
    })

    assert module.scan(v2_root)["split_count"] == 0
