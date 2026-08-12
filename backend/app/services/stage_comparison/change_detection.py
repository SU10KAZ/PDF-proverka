"""Этап 5Б.4: массовая оркестрация проверенной цепочки для всех aligned пар.

Модуль не содержит новых matcher/clustering thresholds. Он вызывает неизменные
этапы 5Б.1–5Б.3 и добавляет только диагностический статус ``review_required``.
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

from . import change_groups, change_regions


SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ReviewConfig:
    """Только мягкие diagnostic thresholds; расчёт regions/groups не меняют."""
    atomic_regions_soft_limit: int = 40
    change_groups_soft_limit: int = 18
    canonical_vector_soft_limit: int = 100
    largest_group_page_area_ratio: float = 0.28
    lowest_multi_group_fill_ratio: float = 0.01
    unexplained_visual_limit: int = 0


def select_alignment_pairs(alignment_report):
    aligned, fallback = [], []
    for item in alignment_report.get("items") or []:
        compact = {key: item.get(key) for key in ("left_page", "right_page", "status", "reason")}
        if item.get("status") == "aligned" and (item.get("transform") or {}).get("matrix"):
            aligned.append(item)
        elif item.get("status") in {"weak_alignment", "failed"}:
            fallback.append(compact)
    aligned.sort(key=lambda item: (int(item["left_page"]), int(item["right_page"])))
    fallback.sort(key=lambda item: (int(item["left_page"]), int(item["right_page"])))
    return aligned, fallback


def _review(metrics, groups, config):
    reasons = []
    if metrics["atomic_regions"] > config.atomic_regions_soft_limit:
        reasons.append("many_atomic_regions")
    if metrics["change_groups"] > config.change_groups_soft_limit:
        reasons.append("many_change_groups")
    if metrics["canonical_vector_differences"] > config.canonical_vector_soft_limit:
        reasons.append("many_canonical_vector_differences")
    if metrics["largest_group_page_area_ratio"] > config.largest_group_page_area_ratio:
        reasons.append("large_group_page_area")
    if metrics["lowest_fill_ratio"] is not None and metrics["lowest_fill_ratio"] < config.lowest_multi_group_fill_ratio:
        reasons.append("low_group_fill_ratio")
    if metrics["unexplained_visual_difference"] > config.unexplained_visual_limit:
        reasons.append("unexplained_visual_difference")
    if metrics["change_groups"] == 0 and metrics["unexplained_visual_difference"] > 0:
        reasons.append("visual_difference_without_groups")
    if groups and not any(group.get("atomic_region_ids") for group in groups):
        reasons.append("groups_only_from_supporting_evidence")
    return "review_required" if reasons else "ok", reasons


def run_change_detection(left_pdf, right_pdf, left_document, right_document, alignment_report, *, review_config=None):
    config = review_config or ReviewConfig()
    aligned, fallback = select_alignment_pairs(alignment_report)
    cleanup_items = []
    alignment_by_pair = {}
    for alignment in aligned:
        result = change_regions.analyze_pair(
            left_pdf, right_pdf, left_document, right_document, alignment,
            canonical_vectors=True,
        )
        result.pop("_images", None)
        cleanup_items.append(result)
        alignment_by_pair[(int(alignment["left_page"]), int(alignment["right_page"]))] = alignment

    cleanup_report = {"items": cleanup_items}
    atomic_report = change_regions.rebuild_regions_after_canonical(cleanup_report, left_document, right_document)
    group_report = change_groups.evaluate_change_groups(atomic_report, left_document, right_document)
    cleanup_by_pair = {(int(item["left_page"]), int(item["right_page"])): item for item in cleanup_items}
    atomic_by_pair = {(int(item["left_page"]), int(item["right_page"])): item for item in atomic_report.get("items") or []}
    output = []
    for grouped in group_report.get("items") or []:
        key = (int(grouped["left_page"]), int(grouped["right_page"]))
        cleanup, atomic, alignment = cleanup_by_pair[key], atomic_by_pair[key], alignment_by_pair[key]
        raw_text = [item for item in cleanup.get("raw_differences") or [] if item.get("kind") == "text"]
        images = [item for item in cleanup.get("raw_differences") or [] if item.get("kind") == "image"]
        unexplained = [item for item in cleanup.get("raw_differences") or [] if item.get("kind") == "unexplained_visual_difference"]
        groups = grouped.get("change_groups") or []
        multi_fill = [float(group["metrics"]["fill_ratio"]) for group in groups if int(group["metrics"]["atomic_region_count"]) > 1]
        metrics = {
            "raw_text_differences": len(raw_text),
            "raw_vector_differences": len(cleanup.get("raw_vector_differences") or []),
            "canonical_vector_differences": len(cleanup.get("canonical_vector_differences") or []),
            "image_differences": len(images),
            "atomic_regions": len(grouped.get("atomic_regions") or []),
            "change_groups": len(groups),
            "stamp_groups": sum(group.get("region_role") == "stamp" for group in groups),
            "drawing_groups": sum(group.get("region_role") != "stamp" for group in groups),
            "largest_group_page_area_ratio": max((float(group["metrics"]["page_area_ratio"]) for group in groups), default=0.0),
            "lowest_fill_ratio": min(multi_fill) if multi_fill else None,
            "unexplained_visual_difference": len(unexplained),
            "supporting_evidence": len(atomic.get("supporting_vector_evidence") or []),
        }
        status, review_reasons = _review(metrics, groups, config)
        output.append({
            "left_page": key[0], "right_page": key[1], "status": status,
            "review_reasons": review_reasons, "alignment": {"status": "aligned", "transform": alignment.get("transform"), "quality": alignment.get("quality")},
            "evidence": {
                "raw_text_differences": raw_text,
                "raw_vector_differences": cleanup.get("raw_vector_differences") or [],
                "canonical_vector_differences": cleanup.get("canonical_vector_differences") or [],
                "image_differences": images,
                "unexplained_visual_differences": unexplained,
                "supporting_evidence": atomic.get("supporting_vector_evidence") or [],
            },
            "atomic_regions": grouped.get("atomic_regions") or [],
            "change_groups": groups, "metrics": metrics,
            "diagnostics": {"alignment_overlay": (alignment.get("diagnostics") or {}).get("overlay")},
        })
    summary = {
        "aligned_pairs": len(output), "review_required": sum(item["status"] == "review_required" for item in output),
        "canonical_vector_differences": sum(item["metrics"]["canonical_vector_differences"] for item in output),
        "atomic_regions": sum(item["metrics"]["atomic_regions"] for item in output),
        "change_groups": sum(item["metrics"]["change_groups"] for item in output),
        "unexplained_visual_difference": sum(item["metrics"]["unexplained_visual_difference"] for item in output),
    }
    return {
        "schema_version": SCHEMA_VERSION, "kind": "stage_comparison_change_detection_v5b4",
        "settings": {"llm_used": False, "vision_used": False, "ocr_rerun": False, "findings_created": False, "alignment_recomputed": False, "review_config": asdict(config)},
        "items": output, "requires_alignment_fallback": fallback, "summary": summary,
    }


def write_diagnostics(directory, report, left_pdf):
    change_groups.write_diagnostics(directory, report, left_pdf)


def _atomic_write(path, payload):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(payload); output.flush(); os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        try: os.unlink(temporary)
        except OSError: pass
        raise


def write_report(directory, report):
    directory = Path(directory); json_path, md_path = directory / "change_detection.json", directory / "change_detection.md"
    _atomic_write(json_path, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    lines = ["# Массовая диагностика областей изменений — этап 5Б.4", "", "Обработаны только пары `aligned`. Findings и вкладка «Расхождения» не изменялись.", "", "| V2 | V3 | Text | Raw vector | Canonical vector | Images | Atomic | Groups | Stamp | Largest area | Lowest fill | Unexplained | Status |", "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
    for item in report.get("items") or []:
        metric = item["metrics"]
        fill = "—" if metric["lowest_fill_ratio"] is None else f"{metric['lowest_fill_ratio']:.2%}"
        lines.append("| " + " | ".join([
            str(item["left_page"]), str(item["right_page"]), str(metric["raw_text_differences"]), str(metric["raw_vector_differences"]),
            str(metric["canonical_vector_differences"]), str(metric["image_differences"]), str(metric["atomic_regions"]), str(metric["change_groups"]),
            str(metric["stamp_groups"]), f"{metric['largest_group_page_area_ratio']:.2%}", fill,
            str(metric["unexplained_visual_difference"]), item["status"] + ((": " + ", ".join(item["review_reasons"])) if item["review_reasons"] else ""),
        ]) + " |")
    lines += ["", "## Требуют fallback совмещения", ""]
    for item in report.get("requires_alignment_fallback") or []:
        lines.append(f"- V2 {item['left_page']} ↔ V3 {item['right_page']}: {item['status']} — {item.get('reason') or '—'}")
    lines += ["", "## Итого", "", *[f"- {key}: {value}" for key, value in (report.get("summary") or {}).items()], ""]
    _atomic_write(md_path, "\n".join(lines)); return json_path, md_path
