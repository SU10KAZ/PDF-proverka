"""Build golden dataset from decisions_log + findings."""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

from backend.app.core.config import DECISIONS_LOG_FILE, KNOWLEDGE_BASE_DIR
from backend.app.services.findings.grounding_service import classify_grounding_level
from backend.app.services.knowledge_base.knowledge_base_service import _load_source_item_maps

from .classifier import classify_evidence_case


GOLDEN_SET_FILE = KNOWLEDGE_BASE_DIR / "evidence_golden_set.json"


def _load_decisions_log() -> list[dict]:
    if not DECISIONS_LOG_FILE.is_file():
        return []
    with DECISIONS_LOG_FILE.open(encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        return data.get("entries", [])
    return data if isinstance(data, list) else []


def _finding_snapshot(finding: dict) -> dict:
    return {
        "id": finding.get("id"),
        "severity": finding.get("severity"),
        "category": finding.get("category"),
        "problem": finding.get("problem") or finding.get("description"),
        "norm": finding.get("norm"),
        "sheet": finding.get("sheet"),
        "page": finding.get("page"),
        "grounding_level": finding.get("grounding_level") or classify_grounding_level(finding),
        "source_block_ids": finding.get("source_block_ids") or [],
        "related_block_ids": finding.get("related_block_ids") or [],
        "evidence": finding.get("evidence") or [],
        "evidence_text_refs": finding.get("evidence_text_refs") or [],
    }


def build_golden_set(
    *,
    limit: Optional[int] = None,
    item_type: str = "finding",
) -> dict:
    entries = _load_decisions_log()
    source_cache: dict = {}
    cases: list[dict] = []
    stats = Counter()

    for entry in entries:
        if item_type and entry.get("item_type") != item_type:
            continue
        project_id = str(entry.get("source_project") or "").strip()
        item_id = str(entry.get("item_id") or "").strip()
        if not project_id or not item_id:
            continue
        if project_id not in source_cache:
            try:
                fm, om = _load_source_item_maps(project_id)
                source_cache[project_id] = (fm, om)
            except Exception:
                source_cache[project_id] = ({}, {})
        findings_map, opt_map = source_cache[project_id]
        finding = findings_map.get(item_id) or opt_map.get(item_id)
        if not finding:
            continue

        expert = str(entry.get("expert_decision") or "").strip().lower()
        case_class = classify_evidence_case(finding, expert)
        stats[case_class] += 1
        cases.append({
            "decision_id": entry.get("id"),
            "source_project": project_id,
            "section": entry.get("section"),
            "item_id": item_id,
            "expert_decision": expert,
            "expert_reason": entry.get("expert_reason", ""),
            "case_class": case_class,
            "finding": _finding_snapshot(finding),
        })
        if limit and len(cases) >= limit:
            break

    payload = {
        "generated_at": datetime.now().isoformat(),
        "total_cases": len(cases),
        "by_class": dict(stats),
        "cases": cases,
    }
    GOLDEN_SET_FILE.parent.mkdir(parents=True, exist_ok=True)
    with GOLDEN_SET_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def load_golden_set() -> Optional[dict]:
    if not GOLDEN_SET_FILE.is_file():
        return None
    with GOLDEN_SET_FILE.open(encoding="utf-8") as f:
        return json.load(f)
