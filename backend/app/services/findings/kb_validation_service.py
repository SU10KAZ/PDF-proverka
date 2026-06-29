"""KB validation service: read saved decisions and run validation."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

from backend.app.services.common import version_service


_KB_FILE = "kb_validation.json"


def _output_dir(project_id: str, version_id: Optional[str] = None) -> Path:
    return version_service.resolve_version_output_dir(project_id, version_id)


# Read saved KB validation

def _kb_validation_path(project_id: str, version_id: Optional[str] = None) -> Optional[Path]:
    """Resolve kb_validation.json for legacy _output and projects_v2 layouts."""
    try:
        primary = _output_dir(project_id, version_id) / _KB_FILE
        if primary.exists():
            return primary
    except Exception:
        primary = None
    try:
        from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
        adapter = ProjectsV2Adapter()
        if not adapter.is_available():
            return primary if primary and primary.exists() else None
        doc = adapter.find_document_by_project_id(project_id)
        if not doc:
            return primary if primary and primary.exists() else None
        vid = adapter.resolve_version_id(doc, version_id)
        if not vid:
            return primary if primary and primary.exists() else None
        alt = adapter.version_dir(Path(doc["doc_dir"]), vid) / "03_analysis" / "latest" / _KB_FILE
        if alt.exists():
            return alt
    except Exception:
        pass
    return primary if primary and primary.exists() else None


def get_kb_validation(project_id: str, version_id: Optional[str] = None) -> dict | None:
    """Return saved KB-validation payload or None."""
    kb_path = _kb_validation_path(project_id, version_id)
    if not kb_path:
        return None
    with kb_path.open(encoding="utf-8") as f:
        return json.load(f)


def get_kb_decision_map(project_id: str, version_id: Optional[str] = None) -> dict[str, dict]:
    """Return finding_id -> decision_dict for fast lookup."""
    data = get_kb_validation(project_id, version_id)
    if not data:
        return {}
    return {d["finding_id"]: d for d in data.get("decisions", [])}


# Run KB validation

def run_kb_validation(
    project_id: str,
    version_id: Optional[str] = None,
    section: str = "TX",
    batch_size: int = 5,
    model: str = "sonnet",
) -> dict:
    """Run KB validation for a project and save the result."""
    import sys
    from pathlib import Path as _Path
    sys.path.insert(0, str(_Path(__file__).parent.parent.parent.parent.parent))

    from backend.app.pipeline.stages.findings_review.critic_v2.kb_gate import KBGate
    import datetime

    output_dir = _output_dir(project_id, version_id)
    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        # try verified
        alt = output_dir / "03a_norms_verified.json"
        if alt.exists():
            findings_path = alt
        else:
            raise FileNotFoundError(f"findings not found in {output_dir}")

    with findings_path.open(encoding="utf-8") as f:
        findings_raw = json.load(f)

    findings = findings_raw.get("findings", [])
    if not findings:
        raise ValueError("No findings to validate")

    import os
    prev = os.environ.get("KB_GATE_MODEL")
    os.environ["KB_GATE_MODEL"] = model
    try:
        gate = KBGate.from_env()
    finally:
        if prev is None:
            os.environ.pop("KB_GATE_MODEL", None)
        else:
            os.environ["KB_GATE_MODEL"] = prev
    all_decisions: list = []
    errors_count = 0

    for i in range(0, len(findings), batch_size):
        batch = findings[i : i + batch_size]
        try:
            result = gate.validate(batch, section=section)
            all_decisions.extend(result.decisions)
            errors_count += result.errors
        except Exception as e:
            errors_count += len(batch)

    output = {
        "generated_at": datetime.datetime.now().isoformat(),
        "project_id": project_id,
        "version_id": version_id,
        "section": section,
        "model": model,
        "total_findings": len(findings),
        "total_processed": len(all_decisions),
        "errors_count": errors_count,
        "decisions": [
            {
                "finding_id": d.finding_id,
                "llm_decision": d.llm_decision,
                "human_taxonomy_reason": d.human_taxonomy_reason,
                "confidence": d.confidence,
                "explanation": d.explanation,
                "kb_examples_used": d.kb_examples_used,
            }
            for d in all_decisions
        ],
    }

    out_path = output_dir / _KB_FILE
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    return output
