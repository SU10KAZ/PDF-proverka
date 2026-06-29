"""Evidence validation service: read/run evidence_validation.json."""
from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Optional

from backend.app.services.common import version_service
from backend.app.services.findings.kb_validation_service import get_kb_decision_map

_EVIDENCE_FILE = "evidence_validation.json"


def _output_dir(project_id: str, version_id: Optional[str] = None) -> Path:
    return version_service.resolve_version_output_dir(project_id, version_id)


def _evidence_path(project_id: str, version_id: Optional[str] = None) -> Optional[Path]:
    try:
        primary = _output_dir(project_id, version_id) / _EVIDENCE_FILE
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
        alt = adapter.version_dir(Path(doc["doc_dir"]), vid) / "03_analysis" / "latest" / _EVIDENCE_FILE
        if alt.exists():
            return alt
    except Exception:
        pass
    return primary if primary and primary.exists() else None


def get_evidence_validation(project_id: str, version_id: Optional[str] = None) -> dict | None:
    path = _evidence_path(project_id, version_id)
    if not path:
        return None
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def get_evidence_decision_map(project_id: str, version_id: Optional[str] = None) -> dict[str, dict]:
    data = get_evidence_validation(project_id, version_id)
    if not data:
        return {}
    return {d["finding_id"]: d for d in data.get("decisions", [])}


def run_evidence_validation(
    project_id: str,
    version_id: Optional[str] = None,
    section: str = "TX",
    *,
    graphic_model: Optional[str] = None,
    text_model: Optional[str] = None,
    force: bool = False,
    respect_kb_routing: bool = True,
) -> dict:
    from backend.app.pipeline.stages.findings_review.evidence_verifier import EvidenceVerifier

    output_dir = _output_dir(project_id, version_id)
    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        alt = output_dir / "03a_norms_verified.json"
        findings_path = alt if alt.exists() else findings_path
    if not findings_path.exists():
        raise FileNotFoundError(f"findings not found in {output_dir}")

    with findings_path.open(encoding="utf-8") as f:
        findings_raw = json.load(f)
    findings = findings_raw.get("findings", [])
    if not findings:
        raise ValueError("No findings to validate")

    kb_map = get_kb_decision_map(project_id, version_id) if respect_kb_routing else {}
    verifier = EvidenceVerifier(
        graphic_model=graphic_model,
        text_model=text_model,
        respect_kb_routing=respect_kb_routing,
    )
    result = verifier.verify_batch(
        project_id,
        findings,
        section=section,
        kb_map=kb_map,
        force=force,
        version_id=version_id,
    )

    output = {
        "generated_at": datetime.datetime.now().isoformat(),
        "project_id": project_id,
        "version_id": version_id,
        "section": section,
        "graphic_model": result.model_graphic,
        "text_model": result.model_text,
        "total_findings": len(findings),
        "total_processed": len(result.decisions),
        "skipped_count": len(result.skipped),
        "errors_count": result.errors,
        "decisions": [
            {
                "finding_id": d.finding_id,
                "llm_decision": d.llm_decision,
                "human_taxonomy_reason": d.human_taxonomy_reason,
                "confidence": d.confidence,
                "explanation": d.explanation,
                "verification_path": d.verification_path,
                "block_ids_used": d.block_ids_used,
                "evidence_checked": d.evidence_checked,
                "model_used": d.model_used,
            }
            for d in result.decisions
        ],
    }

    out_path = output_dir / _EVIDENCE_FILE
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    return output
