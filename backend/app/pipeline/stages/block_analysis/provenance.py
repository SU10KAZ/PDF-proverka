"""Provenance helpers for Stage 01 graphic findings.

Raw detector output stays attributable after Stage 03 renumbers and merges
findings.  The helpers are deliberately deterministic: they only assign
credit from explicit source finding IDs and never infer it from a shared page.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    BLOCKS_META_KEY,
    BLOCKS_META_KEY_LEGACY,
    resolve_existing,
)


STAGE01_PROMPT_VERSION = "stage01-findings-evidence-v3"
# Compatibility alias for older imports; new writers use the Stage 01 value.
STAGE02_PROMPT_VERSION = STAGE01_PROMPT_VERSION


def detector_for_model(model: str | None) -> str:
    """Return the stable detector identifier used in analytics and UI."""
    value = str(model or "").strip().lower()
    if value.startswith("deterministic/"):
        return "deterministic"
    if value.startswith("codex/") or value == "codex":
        return "codex"
    if "gpt" in value and not value.startswith("codex/"):
        return "gpt_openrouter"
    if value.startswith("claude-"):
        return "claude"
    # Провайдерский слой воркера (11F). Строку задаёт не конфигурация, а
    # локальная политика воркера, и до `stage_models.json` ей дела нет. Без
    # этой ветки она проваливалась в терминальный `openrouter` ниже — то есть
    # КАЖДОЕ графическое замечание, найденное на воркере по подписке, навсегда
    # получало провенанс платного HTTP-провайдера, которого там не было вовсе.
    if value.startswith("provider/"):
        return "worker_provider"
    if value:
        return "openrouter"
    return "unknown"


def detector_summary(found_by: Iterable[str]) -> str:
    """Compact machine value consumed by the UI badge."""
    detectors = {str(item).strip() for item in found_by if str(item).strip()}
    has_gpt = "gpt_openrouter" in detectors
    has_codex = "codex" in detectors
    if has_gpt and has_codex:
        return "gpt_codex"
    if has_gpt:
        return "gpt"
    if has_codex:
        return "codex"
    if detectors:
        return "other"
    return "unattributed"


def build_finding_provenance(
    *,
    model: str,
    run_id: str,
    raw_finding_id: str,
    mode: str = "independent",
    detected_at: str | None = None,
    context_source: str | None = None,
) -> dict[str, Any]:
    detector = detector_for_model(model)
    found_by = [detector]
    detection = {
        "detector": detector,
        "model": model,
        "prompt_version": STAGE01_PROMPT_VERSION,
        "run_id": run_id,
        "raw_finding_id": raw_finding_id,
        "mode": mode,
        "detected_at": detected_at or datetime.now(timezone.utc).isoformat(),
    }
    if context_source:
        detection["context_source"] = context_source
    result = {
        "found_by": found_by,
        "detector_summary": detector_summary(found_by),
        "detections": [detection],
    }
    if context_source:
        result["context_source"] = context_source
    return result


def _detection_key(item: dict) -> tuple[str, str, str, str, str]:
    return (
        str(item.get("detector") or ""),
        str(item.get("model") or ""),
        str(item.get("run_id") or ""),
        str(item.get("raw_finding_id") or ""),
        str(item.get("mode") or ""),
    )


def merge_provenance(values: Iterable[dict | None]) -> dict[str, Any]:
    """Merge provenance objects without losing independent detections."""
    found_by: list[str] = []
    detections: list[dict] = []
    seen_detections: set[tuple[str, str, str, str, str]] = set()

    for value in values:
        if not isinstance(value, dict):
            continue
        for detector in value.get("found_by") or []:
            detector = str(detector or "").strip()
            if detector and detector not in found_by:
                found_by.append(detector)
        for detection in value.get("detections") or []:
            if not isinstance(detection, dict):
                continue
            clean = dict(detection)
            detector = str(clean.get("detector") or "").strip()
            if detector and detector not in found_by:
                found_by.append(detector)
            key = _detection_key(clean)
            if key not in seen_detections:
                seen_detections.add(key)
                detections.append(clean)

    return {
        "found_by": found_by,
        "detector_summary": detector_summary(found_by),
        "detections": detections,
    }


def merge_detector_comparisons(values: Iterable[dict | None]) -> dict[str, Any]:
    """Aggregate Stage 01 relation labels without re-running semantic matching."""
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str, tuple[str, ...], str]] = set()
    for value in values:
        if not isinstance(value, dict):
            continue
        nested = value.get("relations") if isinstance(value.get("relations"), list) else [value]
        for raw in nested:
            if not isinstance(raw, dict):
                continue
            relation = str(raw.get("relation") or raw.get("primary_relation") or "").strip()
            if relation not in {"match", "extension", "new", "disputed"}:
                continue
            counterpart_refs = tuple(sorted(
                str(item).strip()
                for item in (raw.get("counterpart_refs") or [])
                if str(item).strip()
            ))
            origin = str(raw.get("origin") or "dual_comparison").strip()
            role = str(raw.get("role") or "").strip()
            key = (relation, role, counterpart_refs, origin)
            if key in seen:
                continue
            seen.add(key)
            records.append({
                "relation": relation,
                "role": role,
                "counterpart_refs": list(counterpart_refs),
                "confidence": raw.get("confidence"),
                "reason": raw.get("reason"),
                "reviewer_model": raw.get("reviewer_model"),
                "origin": origin,
            })

    if not records:
        return {}
    priority = {"disputed": 4, "extension": 3, "match": 2, "new": 1}
    primary = max(records, key=lambda item: priority[item["relation"]])["relation"]
    return {
        "schema_version": 1,
        "primary_relation": primary,
        "relations": records,
        "gap_search": any(item.get("origin") == "gap_search" for item in records),
    }


def is_disputed_comparison(value: Any) -> bool:
    """Return True when a finding carries an unresolved detector conflict."""
    if not isinstance(value, dict):
        return False
    if str(value.get("primary_relation") or value.get("relation") or "").strip() == "disputed":
        return True
    return any(
        isinstance(item, dict) and str(item.get("relation") or "").strip() == "disputed"
        for item in (value.get("relations") or [])
    )


def aggregate_traceability(items: Iterable[dict]) -> dict[str, Any]:
    """Collect source IDs and provenance while findings are deduplicated."""
    source_ids: list[str] = []
    provenance_values: list[dict] = []
    comparison_values: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        for source_id in item.get("source_finding_ids") or []:
            source_id = str(source_id or "").strip()
            if source_id and source_id not in source_ids:
                source_ids.append(source_id)
        provenance = item.get("provenance")
        if isinstance(provenance, dict):
            provenance_values.append(provenance)
        comparison = item.get("detector_comparison")
        if isinstance(comparison, dict):
            comparison_values.append(comparison)

    fields: dict[str, Any] = {}
    if source_ids:
        fields["source_finding_ids"] = source_ids
    if provenance_values:
        merged = merge_provenance(provenance_values)
        fields["provenance"] = merged
        fields["detector_summary"] = merged["detector_summary"]
    merged_comparison = merge_detector_comparisons(comparison_values)
    if merged_comparison:
        fields["detector_comparison"] = merged_comparison
    return fields


def _explicit_source_ids(finding: dict) -> list[str]:
    source_ids: list[str] = []
    candidate_fields = (
        "source_finding_ids",
        "origin_finding_ids",
        "merged_from_finding_ids",
    )
    for field in candidate_fields:
        for value in finding.get(field) or []:
            value = str(value or "").strip()
            if value and value not in source_ids:
                source_ids.append(value)
    for sub in finding.get("sub_findings") or []:
        if not isinstance(sub, dict):
            continue
        value = str(sub.get("original_id") or "").strip()
        if value and value not in source_ids:
            source_ids.append(value)
        for nested in sub.get("source_finding_ids") or []:
            nested = str(nested or "").strip()
            if nested and nested not in source_ids:
                source_ids.append(nested)
    return source_ids


def backfill_final_findings_provenance(
    output_dir: Path,
    findings_filename: str = "03_findings.json",
) -> dict[str, Any]:
    """Transfer explicit Stage 01 detector credit into a final findings file.

    Legacy block-analysis files are supported when they have a single model in
    ``stage02_meta.model``.  No block/page fallback is used because it would
    incorrectly credit every detector that happened to inspect the block.
    """
    output_dir = Path(output_dir)
    stage02_path = resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME)
    findings_path = output_dir / findings_filename
    if not stage02_path.is_file() or not findings_path.is_file():
        return {"updated": 0, "reason": "artifact_missing"}

    try:
        stage02 = json.loads(stage02_path.read_text(encoding="utf-8"))
        final_doc = json.loads(findings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"updated": 0, "reason": f"read_failed: {exc}"}

    stage02_meta = stage02.get(BLOCKS_META_KEY) or stage02.get(BLOCKS_META_KEY_LEGACY) or {}
    legacy_model = str(stage02_meta.get("model") or "")
    legacy_run_id = str(stage02_meta.get("run_id") or stage02.get("timestamp") or "legacy-stage02")
    raw_index: dict[str, dict] = {}
    raw_comparison_index: dict[str, dict] = {}
    for block in stage02.get("block_analyses") or []:
        if not isinstance(block, dict):
            continue
        for raw in block.get("findings") or []:
            if not isinstance(raw, dict):
                continue
            raw_id = str(raw.get("id") or "").strip()
            if not raw_id:
                continue
            provenance = raw.get("provenance")
            if not isinstance(provenance, dict) and legacy_model:
                provenance = build_finding_provenance(
                    model=legacy_model,
                    run_id=legacy_run_id,
                    raw_finding_id=raw_id,
                    detected_at=stage02.get("timestamp"),
                )
            if isinstance(provenance, dict):
                raw_index[raw_id] = provenance
            comparison = raw.get("detector_comparison")
            if isinstance(comparison, dict):
                raw_comparison_index[raw_id] = comparison

    items = final_doc.get("findings") or final_doc.get("items") or []
    updated = 0
    counts = {"gpt": 0, "codex": 0, "gpt_codex": 0, "other": 0, "unattributed": 0}
    comparison_counts = {"match": 0, "extension": 0, "new": 0, "disputed": 0, "unclassified": 0}
    for finding in items:
        if not isinstance(finding, dict):
            continue
        source_ids = _explicit_source_ids(finding)
        provenance_values: list[dict] = []
        existing = finding.get("provenance")
        if isinstance(existing, dict):
            provenance_values.append(existing)
        provenance_values.extend(raw_index[sid] for sid in source_ids if sid in raw_index)
        merged = merge_provenance(provenance_values)
        summary = merged["detector_summary"]
        counts[summary if summary in counts else "other"] += 1
        if merged["found_by"]:
            finding["source_finding_ids"] = source_ids
            finding["provenance"] = merged
            finding["detector_summary"] = summary
            updated += 1
        comparison_values = [
            raw_comparison_index[source_id]
            for source_id in source_ids
            if source_id in raw_comparison_index
        ]
        existing_comparison = finding.get("detector_comparison")
        if isinstance(existing_comparison, dict):
            comparison_values.insert(0, existing_comparison)
        merged_comparison = merge_detector_comparisons(comparison_values)
        if merged_comparison:
            finding["detector_comparison"] = merged_comparison
            comparison_counts[merged_comparison["primary_relation"]] += 1
        else:
            comparison_counts["unclassified"] += 1

    meta = final_doc.setdefault("meta", {})
    meta["finding_source_counts"] = counts
    meta["finding_source_schema_version"] = 1
    meta["finding_comparison_counts"] = comparison_counts
    meta["finding_comparison_schema_version"] = 1

    tmp = findings_path.with_suffix(findings_path.suffix + ".tmp")
    tmp.write_text(json.dumps(final_doc, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, findings_path)
    return {"updated": updated, "total": len(items), "counts": counts}
