"""
backfill_highlights.py
----------------------
Восстановление highlight_regions в 03_findings.json из 02_blocks_analysis.json.

При findings_merge LLM иногда теряет highlight_regions из G-замечаний.
Этот модуль подтягивает координаты обратно по source_block_ids/related_block_ids.
"""
from __future__ import annotations

import json
from pathlib import Path


def backfill_project(project_dir: Path) -> dict:
    """Восстановить highlight_regions в 03_findings.json.

    Для каждого finding без highlight_regions ищет matching block_analysis
    по source_block_ids / related_block_ids / block_evidence и копирует
    highlight_regions оттуда.

    Returns:
        {"fixed": int, "checked": int}
    """
    project_dir = Path(project_dir)
    output_dir = project_dir / "_output"
    findings_path = output_dir / "03_findings.json"
    blocks_path = output_dir / "02_blocks_analysis.json"

    if not findings_path.exists():
        return {"fixed": 0, "checked": 0}

    try:
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"fixed": 0, "checked": 0}

    # Построить индекс block_id → highlight_regions из block_analyses
    hr_index: dict[str, list] = {}
    if blocks_path.exists():
        try:
            data02 = json.loads(blocks_path.read_text(encoding="utf-8"))
            for ba in data02.get("block_analyses", []):
                bid = ba.get("block_id", "")
                hrs = ba.get("highlight_regions") or []
                if bid and hrs:
                    hr_index[bid] = hrs
        except (json.JSONDecodeError, OSError):
            pass

    if not hr_index:
        return {"fixed": 0, "checked": len(fd.get("findings", []))}

    fixed = 0
    findings = fd.get("findings", [])

    for finding in findings:
        existing = finding.get("highlight_regions")
        if existing:
            continue

        # Collect block_ids to look up
        block_ids: list[str] = []
        for key in ("source_block_ids", "related_block_ids"):
            for bid in (finding.get(key) or []):
                if bid and bid not in block_ids:
                    block_ids.append(bid)
        # Also check block_evidence (single string)
        be = finding.get("block_evidence")
        if be and be not in block_ids:
            block_ids.append(be)
        # evidence[] with type=image
        for ev in (finding.get("evidence") or []):
            if isinstance(ev, dict) and ev.get("type") == "image":
                bid = ev.get("block_id", "")
                if bid and bid not in block_ids:
                    block_ids.append(bid)

        merged: list = []
        for bid in block_ids:
            for hr in hr_index.get(bid, []):
                if hr not in merged:
                    merged.append(hr)

        if merged:
            finding["highlight_regions"] = merged
            fixed += 1

    if fixed > 0:
        findings_path.write_text(
            json.dumps(fd, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return {"fixed": fixed, "checked": len(findings)}
