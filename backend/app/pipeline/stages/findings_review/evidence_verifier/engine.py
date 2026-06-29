"""Evidence Verifier engine — orchestrates verification per finding."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from .context_loader import load_finding_context
from .graphic_verifier import verify_graphic
from .kb_routing import should_run_evidence_verifier
from .parse import EVDecision, missing_decision
from .router import PATH_GRAPHIC, PATH_MIXED, PATH_TEXT, PATH_WEAK, route_verification_path
from .text_verifier import verify_text


@dataclass
class EVResult:
    decisions: list = field(default_factory=list)
    skipped: list = field(default_factory=list)
    errors: int = 0
    model_graphic: str = ""
    model_text: str = ""


class EvidenceVerifier:
    def __init__(
        self,
        *,
        graphic_model: Optional[str] = None,
        text_model: Optional[str] = None,
        respect_kb_routing: bool = True,
    ) -> None:
        self._graphic_model = graphic_model or os.environ.get(
            "EV_GRAPHIC_MODEL",
            os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "qwen/qwen3.6-35b-a3b"),
        )
        self._text_model = text_model or os.environ.get("EV_TEXT_MODEL", "sonnet")
        self._respect_kb_routing = respect_kb_routing

    def verify_finding(
        self,
        project_id: str,
        finding: dict,
        *,
        section: str = "",
        kb_decision: Optional[dict] = None,
        force: bool = False,
        version_id: Optional[str] = None,
    ) -> EVDecision:
        if self._respect_kb_routing and not force:
            run, reason = should_run_evidence_verifier(
                finding, kb_decision=kb_decision,
            )
            if not run:
                return EVDecision(
                    finding_id=str(finding.get("id", "?")),
                    llm_decision="accept",
                    human_taxonomy_reason=None,
                    explanation=f"EV пропущен: {reason}",
                    confidence=1.0,
                    verification_path="skipped",
                    block_ids_used=[],
                    evidence_checked=False,
                )

        ctx = load_finding_context(project_id, finding, version_id=version_id, section=section)
        path = route_verification_path(ctx)

        if path in (PATH_GRAPHIC, PATH_MIXED):
            decision = verify_graphic(ctx, model=self._graphic_model)
            if path == PATH_MIXED:
                decision.verification_path = PATH_MIXED
                if decision.llm_decision in ("borderline", "needs_human"):
                    text_d = verify_text(ctx, model=self._text_model)
                    if text_d.confidence > decision.confidence:
                        decision = text_d
                        decision.verification_path = PATH_MIXED
            return decision

        if path == PATH_TEXT:
            return verify_text(ctx, model=self._text_model)

        if ctx.md_excerpt:
            return verify_text(ctx, model=self._text_model)
        return missing_decision(
            finding,
            verification_path=PATH_WEAK,
            explanation="Недостаточно evidence для автоматической проверки.",
        )

    def verify_batch(
        self,
        project_id: str,
        findings: list[dict],
        *,
        section: str = "",
        kb_map: Optional[dict] = None,
        force: bool = False,
        version_id: Optional[str] = None,
    ) -> EVResult:
        kb_map = kb_map or {}
        result = EVResult(model_graphic=self._graphic_model, model_text=self._text_model)
        for f in findings:
            fid = str(f.get("id", ""))
            try:
                d = self.verify_finding(
                    project_id, f,
                    section=section,
                    kb_decision=kb_map.get(fid),
                    force=force,
                    version_id=version_id,
                )
                if d.verification_path == "skipped":
                    result.skipped.append(fid)
                result.decisions.append(d)
            except Exception:
                result.errors += 1
                result.decisions.append(missing_decision(f))
        return result
