"""Этап 6А.2: массовая оркестрация неизменённого анализа 6А.1.

Модуль не создаёт findings, не вызывает LLM/Vision/OCR и не изменяет входные
PreparedDocument либо артефакты этапов 1–6А.1.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Callable

import fitz

from . import semantic_diff as v6a
from . import semantic_diff_v6a1 as v6a1


SCHEMA_VERSION = 1
RESOLVED_LEVELS = {"exact", "strong"}
NEXT_ANALYSES = {"llm", "vision", "human", "deterministic_improvement"}
UNRESOLVED_REASONS = {
    "complex_text", "table_unresolved", "vector_only", "image_change",
    "complex_graphic", "entity_not_localized", "insufficient_local_evidence",
    "mixed_complex", "other",
}
PARITY_FIELDS = (
    "before", "after", "change_summary", "change_kind", "source",
    "evidence_level", "confidence", "requires_human_review", "table_changes",
    "inserted_table_rows", "removed_table_rows", "numeric_context_changes",
    "localized_entities_left", "localized_entities_right",
    "entity_location_uncertain", "stamp_field_changes",
)


def _is_resolved(result: dict) -> bool:
    return result.get("evidence_level") in RESOLVED_LEVELS


def _block_semantic_types(analysis: dict) -> set[str]:
    context = analysis.get("context") or {}
    blocks = (context.get("left") or {}).get("blocks") or []
    blocks += (context.get("right") or {}).get("blocks") or []
    return {str(block.get("semantic_type") or "").lower() for block in blocks if block.get("semantic_type")}


def classify_semantic_type(group: dict, analysis: dict) -> str:
    """Классификация для статистики; смысл изменения не домысливается."""
    if group.get("region_role") == "stamp":
        return "stamp"
    table = ((analysis.get("table_model") or {}).get("comparison") or {})
    if table.get("applicable") or table.get("table_replaced"):
        return "table"
    if analysis.get("numeric_context_changes"):
        return "numeric"
    kinds = set(group.get("change_types") or [])
    if kinds == {"image"}:
        return "image"
    if kinds == {"vector"}:
        return "vector"
    if len(kinds) > 1:
        return "mixed"
    semantic_types = _block_semantic_types(analysis)
    if semantic_types & {"scheme", "plan", "drawing", "graphic", "diagram"} and "text" not in kinds:
        return "complex_graphic"
    return "text"


def classify_unresolved(group: dict, analysis: dict, semantic_type: str) -> tuple[str | None, str | None]:
    """Выбрать только технический следующий анализ, не запуская его."""
    if _is_resolved(analysis):
        return None, None
    level = analysis.get("evidence_level")
    if semantic_type == "vector":
        return "vector_only", "vision"
    if semantic_type == "image":
        return "image_change", "vision"
    if semantic_type == "table":
        table = ((analysis.get("table_model") or {}).get("comparison") or {})
        next_step = "deterministic_improvement" if table.get("applicable") else "human"
        return "table_unresolved", next_step
    if semantic_type == "complex_graphic":
        return "complex_graphic", "vision"
    if semantic_type == "mixed":
        return "mixed_complex", "vision" if "image" in set(group.get("change_types") or []) else "llm"
    if level == "contextual" and v6a._pair_text_entries(analysis.get("evidence") or []):
        return "complex_text", "llm"
    if analysis.get("entity_location_uncertain") and not (
        analysis.get("localized_entities_left") or analysis.get("localized_entities_right")
    ):
        return "entity_not_localized", "deterministic_improvement"
    if level == "insufficient":
        return "insufficient_local_evidence", "human"
    return "other", "human"


def _existing_diagnostics(destination: Path, left_page: int, right_page: int, group_id: str) -> dict[str, Any]:
    comparison = destination.parent
    refs: dict[str, Any] = {}
    for suffix in ("groups", "atomic", "combined"):
        path = comparison / "change_detection" / "diagnostics" / f"v2_{left_page:03d}_v3_{right_page:03d}_{suffix}.png"
        if path.exists():
            refs[f"change_detection_{suffix}"] = str(path)
    pilot_stem = f"v2_{left_page:03d}_v3_{right_page:03d}_{group_id}"
    pilot = comparison / "semantic_diff_v6a1" / "diagnostics"
    crops = {side: str(pilot / f"{pilot_stem}_{side}.png") for side in ("v2", "v3", "overlay")
             if (pilot / f"{pilot_stem}_{side}.png").exists()}
    if crops:
        refs["reused_v6a1_crops"] = crops
    return refs


def _atomic_evidence(pair_item: dict, group: dict) -> list[dict]:
    wanted = set(group.get("atomic_region_ids") or [])
    return [region for region in pair_item.get("atomic_regions") or [] if region.get("region_id") in wanted]


def _numeric_stats(item: dict) -> dict[str, int]:
    contexts = item.get("number_contexts") or {}
    left, right = contexts.get("left") or [], contexts.get("right") or []
    changes = item.get("numeric_context_changes") or []
    table_changes = item.get("table_changes") or []
    proven_table = sum(
        bool(v6a1.normalize_number(change.get("before") or "") and v6a1.normalize_number(change.get("after") or ""))
        for change in table_changes
    )
    reliable_candidates = sum(bool(entry.get("context_reliable")) for entry in left + right)
    # Не считаем неизменившиеся надёжные числа «отклонёнными». Ambiguous здесь
    # означает только отсутствие пригодного label/context в исходном кандидате.
    ambiguous = sum(not entry.get("context_reliable") for entry in left + right)
    unit_pattern = re.compile(r"(?:^|[\s,])(мм|см|м|м²|м³|м2|м3|шт|а|в|квт|ква|%|°)(?:$|[\s,])", re.I)
    units = sum(bool(change.get("unit")) for change in changes) + sum(
        bool(unit_pattern.search(str(change.get("column_label") or ""))) for change in table_changes
    )
    return {
        "proven_changes": len(changes) + proven_table,
        "changes_with_reliable_context": len(changes) + proven_table,
        "changes_with_units": units,
        "rejected_or_ambiguous_candidates": ambiguous,
        "reliable_context_candidates": reliable_candidates,
    }


def build_output_item(pair_item: dict, group: dict, analysis: dict, destination: Path) -> dict[str, Any]:
    semantic_type = classify_semantic_type(group, analysis)
    unresolved_reason, next_analysis = classify_unresolved(group, analysis, semantic_type)
    assert unresolved_reason is None or unresolved_reason in UNRESOLVED_REASONS
    assert next_analysis is None or next_analysis in NEXT_ANALYSES
    atomics = _atomic_evidence(pair_item, group)
    geometry = [entry for entry in analysis.get("evidence") or [] if entry.get("kind") in {"vector", "image"}]
    exact_entities = list(analysis.get("localized_entities_left") or []) + list(
        analysis.get("localized_entities_right") or []
    )
    table = (analysis.get("table_model") or {}).get("comparison") or {}
    exact_entity_used = bool(exact_entities) and not (
        table.get("applicable") or analysis.get("numeric_context_changes") or analysis.get("stamp_field_changes")
    ) and analysis.get("evidence_level") in RESOLVED_LEVELS
    left_page, right_page = int(pair_item["left_page"]), int(pair_item["right_page"])
    result = {key: value for key, value in analysis.items() if key != "context"}
    return {
        "group_id": group["group_id"], "left_page": left_page, "right_page": right_page,
        "bbox": group["bbox"], "change_types": group.get("change_types") or [],
        "region_role": group.get("region_role"), "semantic_type": semantic_type,
        "atomic_region_ids": group.get("atomic_region_ids") or [], "block_ids": group.get("block_ids") or [],
        **result,
        "resolution_status": "deterministically_resolved" if _is_resolved(analysis) else "requires_additional_analysis",
        "unresolved_reason": unresolved_reason, "next_analysis": next_analysis,
        "sheet_review_required": pair_item.get("status") == "review_required",
        "sheet_review_reasons": pair_item.get("review_reasons") or [],
        "exact_entity_used_for_result": exact_entity_used,
        "atomic_region_evidence": atomics,
        "geometry_evidence": geometry,
        "numeric_diagnostics": _numeric_stats(result),
        "diagnostics": _existing_diagnostics(destination, left_page, right_page, group["group_id"]),
    }


def analyze_all_groups(change_detection: dict, analyzer: Callable[[dict, dict], dict], destination: Path) -> list[dict]:
    """Обойти каждую группу ровно один раз; callback делает собственно анализ 6А.1."""
    results = []
    for pair_item in change_detection.get("items") or []:
        for group in pair_item.get("change_groups") or []:
            results.append(build_output_item(pair_item, group, analyzer(pair_item, group), destination))
    return results


def compare_with_v6a1_pilot(items: list[dict], pilot: dict | None) -> dict[str, Any]:
    if not isinstance(pilot, dict):
        return {"available": False, "compared": 0, "mismatches": []}
    current = {(int(item["left_page"]), int(item["right_page"]), item["group_id"]): item for item in items}
    mismatches = []
    for old in pilot.get("items") or []:
        key = (int(old["left_page"]), int(old["right_page"]), old["group_id"])
        new = current.get(key)
        changed = [field for field in PARITY_FIELDS if new is None or new.get(field) != old.get(field)]
        if changed:
            mismatches.append({"left_page": key[0], "right_page": key[1], "group_id": key[2], "fields": changed})
    return {"available": True, "compared": len(pilot.get("items") or []), "mismatches": mismatches,
            "unchanged": not mismatches}


def summarize(items: list[dict]) -> dict[str, Any]:
    levels = Counter(item["evidence_level"] for item in items)
    types = Counter(item["semantic_type"] for item in items)
    reasons = Counter(item["unresolved_reason"] for item in items if item["unresolved_reason"])
    next_steps = Counter(item["next_analysis"] for item in items if item["next_analysis"])
    table_items = [item for item in items if item["semantic_type"] == "table"]
    entity_items = [item for item in items if item["localized_entities_left"] or item["localized_entities_right"]]
    exact_entities = [entity for item in items for entity in (
        list(item["localized_entities_left"]) + list(item["localized_entities_right"])
    )]
    numeric = Counter()
    for item in items:
        numeric.update(item["numeric_diagnostics"])
    type_statistics = {}
    for semantic_type in ("text", "numeric", "table", "vector", "image", "mixed", "stamp", "complex_graphic"):
        selected = [item for item in items if item["semantic_type"] == semantic_type]
        by_level = Counter(item["evidence_level"] for item in selected)
        resolved = sum(item["resolution_status"] == "deterministically_resolved" for item in selected)
        type_statistics[semantic_type] = {
            "total": len(selected),
            "exact": by_level["exact"], "strong": by_level["strong"],
            "contextual": by_level["contextual"], "insufficient": by_level["insufficient"],
            "deterministically_resolved": resolved,
            "automatic_resolution_rate": round(resolved / len(selected), 4) if selected else None,
        }
    return {
        "total_groups": len(items),
        "deterministically_resolved": sum(item["resolution_status"] == "deterministically_resolved" for item in items),
        "requires_additional_analysis": sum(item["resolution_status"] == "requires_additional_analysis" for item in items),
        "evidence_levels": {key: levels[key] for key in ("exact", "strong", "contextual", "insufficient")},
        "groups_by_type": {key: types[key] for key in ("text", "numeric", "table", "vector", "image", "mixed", "stamp", "complex_graphic")},
        "type_statistics": type_statistics,
        "unresolved_reasons": dict(sorted(reasons.items())),
        "next_analysis": {key: next_steps[key] for key in ("llm", "vision", "human", "deterministic_improvement")},
        "tables": {
            "groups": len(table_items),
            "fully_structured": sum(item["resolution_status"] == "deterministically_resolved" for item in table_items),
            "unresolved": sum(item["resolution_status"] != "deterministically_resolved" for item in table_items),
            "replacements": sum(item["table_model"]["comparison"].get("table_replaced", False) for item in table_items),
            "localized_cells": sum(len(item["table_changes"]) for item in table_items),
            "inserted_rows": sum(len(item["inserted_table_rows"]) for item in table_items
                                 if not item["table_model"]["comparison"].get("table_replaced")),
            "removed_rows": sum(len(item["removed_table_rows"]) for item in table_items
                                if not item["table_model"]["comparison"].get("table_replaced")),
            "rows_in_replacements": sum(len(item["inserted_table_rows"]) + len(item["removed_table_rows"])
                                        for item in table_items if item["table_model"]["comparison"].get("table_replaced")),
        },
        "entities": {
            "groups_with_exact_localization": len(entity_items),
            "localized_occurrences": sum(len(item["localized_entities_left"]) + len(item["localized_entities_right"]) for item in items),
            "unique_exact_entities": len({entity.get("entity") for entity in exact_entities if entity.get("entity")}),
            "groups_using_exact_entity": sum(item["exact_entity_used_for_result"] for item in items),
            "uncertain_occurrences": sum(item["entity_location_uncertain"] for item in items),
        },
        "numeric": dict(numeric),
        "sheet_review_required_groups": sum(item["sheet_review_required"] for item in items),
        "llm_calls": 0, "vision_calls": 0, "ocr_calls": 0, "findings_created": 0,
    }


def run_mass(left_pdf_path: str | Path, right_pdf_path: str | Path, left_document: dict, right_document: dict,
             change_detection: dict, destination: str | Path, *, pilot_v6a1: dict | None = None,
             padding_pt: float = v6a.DEFAULT_PADDING_PT) -> dict[str, Any]:
    destination = Path(destination)
    with fitz.open(left_pdf_path) as left_pdf, fitz.open(right_pdf_path) as right_pdf:
        def analyzer(pair_item: dict, group: dict) -> dict:
            return v6a1.analyze_group(
                left_pdf[int(pair_item["left_page"]) - 1], right_pdf[int(pair_item["right_page"]) - 1],
                left_document, right_document, pair_item, group, padding_pt=padding_pt,
            )
        items = analyze_all_groups(change_detection, analyzer, destination)
    input_groups = sum(len(item.get("change_groups") or []) for item in change_detection.get("items") or [])
    if len(items) != input_groups:
        raise RuntimeError(f"semantic_diff_v6a2_group_count_mismatch:{len(items)}!={input_groups}")
    parity = compare_with_v6a1_pilot(items, pilot_v6a1)
    if parity.get("available") and parity.get("mismatches"):
        raise RuntimeError("semantic_diff_v6a1_parity_failed")
    return {
        "schema_version": SCHEMA_VERSION, "kind": "stage_comparison_semantic_diff_v6a2_mass",
        "settings": {"padding_pt": padding_pt, "analysis_logic": "semantic_diff_v6a1_unchanged",
                     "llm_used": False, "vision_used": False, "ocr_added": False,
                     "findings_created": False, "previous_stages_changed": False,
                     "new_crops_generated": False},
        "source": {"kind": change_detection.get("kind"), "aligned_pairs": len(change_detection.get("items") or []),
                   "change_groups": input_groups},
        "items": items, "summary": summarize(items), "pilot_v6a1_parity": parity,
    }


def _md(value: Any, limit: int = 180) -> str:
    text = str(value if value not in (None, "") else "—").replace("|", "\\|").replace("\n", " ")
    return text[:limit]


def write_report(destination: str | Path, report: dict[str, Any]) -> tuple[Path, Path]:
    destination = Path(destination)
    json_path, md_path = destination / "semantic_diff.json", destination / "semantic_diff.md"
    v6a1._atomic_write(json_path, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    lines = [
        "# Этап 6А.2 — массовый детерминированный смысловой анализ", "",
        "Обработаны все change groups этапа 5Б.4 неизменённой логикой 6А.1. "
        "LLM, Vision и дополнительный OCR не вызывались; findings не создавались.", "",
        "| Pair | Group | Type | evidence | before | after | source | review | next |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in report["items"]:
        lines.append(
            f"| {item['left_page']}↔{item['right_page']} | `{item['group_id']}` | {item['semantic_type']} | "
            f"{item['evidence_level']} | {_md(item['before'])} | {_md(item['after'])} | {item['source']} | "
            f"{'да' if item['requires_human_review'] else 'нет'} | {_md(item['next_analysis'])} |"
        )
    lines += ["", "## Сводка", "", f"- Всего групп: {report['summary']['total_groups']}.",
              f"- Детерминированно разрешено: {report['summary']['deterministically_resolved']}.",
              f"- Требуют дополнительного анализа: {report['summary']['requires_additional_analysis']}.",
              f"- Evidence levels: `{json.dumps(report['summary']['evidence_levels'], ensure_ascii=False)}`.",
              f"- Типы: `{json.dumps(report['summary']['groups_by_type'], ensure_ascii=False)}`.",
              f"- Качество по типам: `{json.dumps(report['summary']['type_statistics'], ensure_ascii=False)}`.",
              f"- Следующий анализ: `{json.dumps(report['summary']['next_analysis'], ensure_ascii=False)}`.",
              f"- Таблицы: `{json.dumps(report['summary']['tables'], ensure_ascii=False)}`.",
              f"- Entities: `{json.dumps(report['summary']['entities'], ensure_ascii=False)}`.",
              f"- Числа: `{json.dumps(report['summary']['numeric'], ensure_ascii=False)}`.",
              f"- Паритет с пилотом 6А.1: `{json.dumps(report['pilot_v6a1_parity'], ensure_ascii=False)}`.", ""]
    v6a1._atomic_write(md_path, "\n".join(lines))
    diagnostics = destination / "diagnostics"
    by_pair: dict[tuple[int, int], list[dict]] = {}
    for item in report["items"]:
        by_pair.setdefault((item["left_page"], item["right_page"]), []).append(item)
    for (left_page, right_page), items in by_pair.items():
        compact = {
            "left_page": left_page, "right_page": right_page,
            "groups": [{key: item[key] for key in (
                "group_id", "bbox", "semantic_type", "evidence_level", "resolution_status",
                "unresolved_reason", "next_analysis", "sheet_review_required", "diagnostics",
            )} for item in items],
        }
        v6a1._atomic_write(
            diagnostics / f"v2_{left_page:03d}_v3_{right_page:03d}.json",
            json.dumps(compact, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        )
    return json_path, md_path


__all__ = ["classify_semantic_type", "classify_unresolved", "analyze_all_groups",
           "compare_with_v6a1_pilot", "summarize", "run_mass", "write_report"]
