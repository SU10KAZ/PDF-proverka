# -*- coding: utf-8 -*-
"""Pipeline V2 — Dry Run / Orchestrator (offline-связка этапов 1–4 + графика).

Backend-only слой, который объединяет изолированные этапы Pipeline V2 в ОДИН
локальный offline dry-run и пишет на диск их артефакты:

    left_package / right_package (подготовленные пакеты)
      → [1] build_normalized_document_model        → *_normalized_document_model.json
      → [2] match_normalized_documents             → block_matching_report.json
      → [3] build_graphic_descriptor_report (×2)   → *_graphic_descriptor_report.json
            describe_matched_graphic_blocks        → graphic_descriptor_matched_report.json
      → [4] extract_entities_for_matched_documents → entity_extraction_report.json
      → [5] diff_entity_extraction_report          → entity_diff_report.json
      → [6] explain_entity_diff_report             → delta_explanation_report.json
      → pipeline_v2_summary.json / .md + pipeline_v2_manifest.json

Это НЕ UI, НЕ замена старой логики. Только оркестрация уже готовых чистых функций
+ сводка/манифест. Никаких сетевых вызовов, Qwen/Opus/OCR/PDF-render и скачивания
`crop_url`. Graphic Descriptor — вспомогательный «светофор» готовности графики к
diff; Delta Explanation — LLM-объяснение готовых дельт с ИНЪЕКТИРУЕМЫМ
``llm_runner`` (по умолчанию ``None`` → `skipped_no_runner`, без реального LLM).
Оба вспомогательных слоя fail-soft и НЕ валят обязательные этапы 1–2/4–5.

Fail-soft: writer'ы атомарны (tmp + os.replace), поэтому частично записанного
broken JSON не остаётся. Если этап падает — `status=failed`, в summary короткая
ошибка, уже записанные артефакты остаются валидными, последующие не пишутся.

См. docs/stage_comparison_pipeline_v2_dry_run.md.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

from backend.app.services.stage_comparison.pipeline_v2_prepared_ingest import (
    build_normalized_document_model,
    write_normalized_document_model,
)
from backend.app.services.stage_comparison.pipeline_v2_block_matching import (
    match_normalized_documents,
    write_block_matching_report,
)
from backend.app.services.stage_comparison.pipeline_v2_entity_extraction import (
    extract_entities_for_matched_documents,
    write_entity_extraction_report,
)
from backend.app.services.stage_comparison.pipeline_v2_entity_diff import (
    diff_entity_extraction_report,
    write_entity_diff_report,
)
from backend.app.services.stage_comparison.pipeline_v2_graphic_block_descriptor import (
    build_graphic_descriptor_report,
    describe_matched_graphic_blocks,
    write_graphic_descriptor_report,
)
from backend.app.services.stage_comparison.pipeline_v2_delta_explanation import (
    explain_entity_diff_report,
    write_delta_explanation_report,
)
from backend.app.services.stage_comparison.pipeline_v2_visual_equivalence_gate import (
    run_visual_equivalence_gate,
    write_visual_equivalence_gate_report,
)
from backend.app.services.stage_comparison.pipeline_v2_block_link_preview import (
    build_block_link_preview,
    write_block_link_preview_report,
)

SUMMARY_VERSION = 1
SUMMARY_KIND = "stage_comparison_pipeline_v2_dry_run_summary"
MANIFEST_KIND = "stage_comparison_pipeline_v2_manifest"
GRAPHIC_MATCHED_KIND = "stage_comparison_pipeline_v2_graphic_descriptor_matched"
NEXT_RECOMMENDED_STAGE = "delta_explanation"

# artifact-ключ → имя файла в out_dir
_ARTIFACT_FILENAMES = {
    "left_model": "left_normalized_document_model.json",
    "right_model": "right_normalized_document_model.json",
    "block_matching": "block_matching_report.json",
    "left_graphic": "left_graphic_descriptor_report.json",
    "right_graphic": "right_graphic_descriptor_report.json",
    "graphic_matched": "graphic_descriptor_matched_report.json",
    "visual_gate": "visual_equivalence_gate_report.json",
    "block_link_preview": "block_link_preview_report.json",
    "entity_extraction": "entity_extraction_report.json",
    "entity_diff": "entity_diff_report.json",
    "delta_explanation": "delta_explanation_report.json",
    "summary_json": "pipeline_v2_summary.json",
    "summary_md": "pipeline_v2_summary.md",
    "manifest": "pipeline_v2_manifest.json",
}
# Артефакты, перечисляемые в манифесте (manifest сам себя не хеширует).
_MANIFEST_ARTIFACT_KEYS = [
    "left_model", "right_model", "block_matching",
    "left_graphic", "right_graphic", "graphic_matched", "visual_gate",
    "block_link_preview",
    "entity_extraction", "entity_diff", "delta_explanation",
    "summary_json", "summary_md",
]


# ─── низкоуровневые помощники ────────────────────────────────────────────────


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _atomic_write_text(out_path: str | Path, text: str) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=out_path.name + ".", suffix=".tmp", dir=str(out_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, out_path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return out_path


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_kind(path: Path) -> Optional[str]:
    if path.suffix.lower() != ".json":
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("kind") if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return None


# ─── package paths / artifact paths ──────────────────────────────────────────


def normalize_package_paths(package: Any) -> dict:
    """Привести входной пакет к ``{pdf_path, result_json_path, document_md_path,
    ocr_html_path}`` (str | None). Bare-строка трактуется как result_json_path."""
    if isinstance(package, str):
        return {"pdf_path": None, "result_json_path": _clean(package),
                "document_md_path": None, "ocr_html_path": None}
    package = package or {}
    return {
        "pdf_path": _clean(package.get("pdf_path")),
        "result_json_path": _clean(package.get("result_json_path")),
        "document_md_path": _clean(package.get("document_md_path")),
        "ocr_html_path": _clean(package.get("ocr_html_path")),
    }


def build_pipeline_v2_artifact_paths(out_dir: str | Path) -> dict:
    """Карта artifact-ключ → ``Path`` в ``out_dir``."""
    out_dir = Path(out_dir)
    return {k: out_dir / name for k, name in _ARTIFACT_FILENAMES.items()}


# ─── inputs info + optional-file warnings ────────────────────────────────────

_OPTIONAL_KEYS = ("pdf_path", "document_md_path", "ocr_html_path")


def _input_info(paths: dict, warnings: list[str], side: str) -> dict:
    """Описание входа стороны + warnings про отсутствующие optional-файлы."""
    provided = {}
    exists = {}
    for key in ("pdf_path", "result_json_path", "document_md_path", "ocr_html_path"):
        p = paths.get(key)
        provided[key] = bool(p)
        exists[key] = bool(p and Path(p).exists())
        if key in _OPTIONAL_KEYS and p and not Path(p).exists():
            warnings.append(f"{side}: optional file missing on disk ({key}): {p}")
    return {**paths, "provided": provided, "exists": exists}


# ─── summary builders ────────────────────────────────────────────────────────


def _summary_of(report: Any) -> dict:
    if isinstance(report, dict) and isinstance(report.get("summary"), dict):
        return report["summary"]
    return {}


def _warnings_count(report: Any) -> int:
    if isinstance(report, dict) and isinstance(report.get("warnings"), list):
        return len(report["warnings"])
    return 0


def _merge_counts(a: Any, b: Any) -> dict:
    out = dict(a or {})
    for k, v in (b or {}).items():
        out[k] = out.get(k, 0) + v
    return out


def _risk_count(matched: list[dict], flag: str) -> int:
    return sum(1 for m in (matched or []) if flag in (m.get("risk_flags") or []))


def build_graphic_matched_report(matched: list[dict]) -> dict:
    """Обёртка-артефакт для matched_graphic_blocks (этап Graphic Descriptor)."""
    matched = matched or []
    return {
        "version": SUMMARY_VERSION,
        "kind": GRAPHIC_MATCHED_KIND,
        "summary": {
            "matched_graphic_blocks_total": len(matched),
            "low_token_overlap_total": _risk_count(matched, "low_token_overlap"),
            "one_side_not_usable_total": _risk_count(matched, "one_side_not_usable"),
            "graphic_type_mismatch_total": _risk_count(matched, "graphic_type_mismatch"),
            "discipline_mismatch_total": _risk_count(matched, "discipline_mismatch"),
        },
        "matched_graphic_blocks": matched,
        "warnings": [],
    }


def _graphic_descriptor_section(left_graphic: Any, right_graphic: Any,
                                matched: list[dict], graphic_error: Optional[str]) -> dict:
    lg, rg = _summary_of(left_graphic), _summary_of(right_graphic)
    matched = matched or []
    sec = {
        "left_graphic_blocks_total": lg.get("graphic_blocks_total", 0),
        "right_graphic_blocks_total": rg.get("graphic_blocks_total", 0),
        "left_usable_for_diff_total": lg.get("usable_for_diff_total", 0),
        "right_usable_for_diff_total": rg.get("usable_for_diff_total", 0),
        "left_needs_vision_enrichment_total": lg.get("needs_vision_enrichment_total", 0),
        "right_needs_vision_enrichment_total": rg.get("needs_vision_enrichment_total", 0),
        "left_manual_review_recommended_total": lg.get("manual_review_recommended_total", 0),
        "right_manual_review_recommended_total": rg.get("manual_review_recommended_total", 0),
        "matched_graphic_blocks_total": len(matched),
        "matched_low_token_overlap_total": _risk_count(matched, "low_token_overlap"),
        "matched_one_side_not_usable_total": _risk_count(matched, "one_side_not_usable"),
        "matched_graphic_type_mismatch_total": _risk_count(matched, "graphic_type_mismatch"),
        "matched_discipline_mismatch_total": _risk_count(matched, "discipline_mismatch"),
        "by_graphic_type": _merge_counts(lg.get("by_graphic_type"), rg.get("by_graphic_type")),
        "by_discipline": _merge_counts(lg.get("by_discipline"), rg.get("by_discipline")),
        "by_readiness": _merge_counts(lg.get("by_readiness"), rg.get("by_readiness")),
        "warnings_count": (_warnings_count(left_graphic) + _warnings_count(right_graphic)
                           + (1 if graphic_error else 0)),
    }
    if graphic_error:
        sec["error"] = graphic_error
    return sec


def _delta_explanation_status(enabled: bool, error: Optional[str], de_report: Any) -> str:
    if not enabled:
        return "disabled"
    if error:
        return "failed"
    s = _summary_of(de_report)
    selected = s.get("selected_total", 0)
    skipped = s.get("skipped_total", 0)
    failed = s.get("failed_total", 0)
    warns = s.get("warnings_count", 0)
    if selected == 0:
        return "completed"
    if skipped == selected:
        return "skipped_no_runner"
    if failed or warns:
        return "completed_with_warnings"
    return "completed"


def _delta_explanation_section(de_report: Any, de_enabled: bool, de_error: Optional[str]) -> dict:
    s = _summary_of(de_report)
    sec = {
        "enabled": bool(de_enabled),
        "status": _delta_explanation_status(de_enabled, de_error, de_report),
        "deltas_total": s.get("deltas_total", 0),
        "selected_total": s.get("selected_total", 0),
        "explained_total": s.get("explained_total", 0),
        "skipped_total": s.get("skipped_total", 0),
        "failed_total": s.get("failed_total", 0),
        "accepted_total": s.get("accepted_total", 0),
        "rejected_total": s.get("rejected_total", 0),
        "needs_human_review_total": s.get("needs_human_review_total", 0),
        "possible_ocr_noise_total": s.get("possible_ocr_noise_total", 0),
        "possible_weak_graphic_total": s.get("possible_weak_graphic_total", 0),
        "by_risk_level": s.get("by_risk_level", {}),
        "by_status": s.get("by_status", {}),
        "coverage_notes_total": len((de_report or {}).get("coverage_notes") or [])
        if isinstance(de_report, dict) else 0,
        "warnings_count": s.get("warnings_count", 0),
    }
    if de_error:
        sec["error"] = de_error
    return sec


# ─── delta sections (секционирование отчёта по вердиктам critic) ─────────────
#
# Offline-представление для инженера/генподрядчика: каждая ОБЪЯСНЁННАЯ дельта
# попадает ровно в одну главную секцию по приоритету
# llm_failed_or_skipped → likely_noise_hidden_by_default → weak_graphic_review
# → needs_review → confirmed_changes. Селекцию/prompt/diff это НЕ меняет.

_DELTA_SECTION_ORDER = [
    "confirmed_changes",
    "needs_review",
    "weak_graphic_review",
    "likely_noise_hidden_by_default",
    "llm_failed_or_skipped",
]

_DELTA_SECTION_DESCRIPTIONS = {
    "confirmed_changes": ("Accepted deterministic deltas that are grounded "
                          "and should be shown to engineer."),
    "needs_review": "Deltas requiring engineer/manual review.",
    "weak_graphic_review": "Deltas affected by weak/not usable graphic context.",
    "likely_noise_hidden_by_default": (
        "OCR/formatting noise or critic-rejected deltas; hidden by default "
        "(should_show_to_engineer=false, risk_level=none or verdict=reject)."),
    "llm_failed_or_skipped": ("Selected deltas without successful explanation "
                              "(failed, skipped or unparseable LLM response)."),
}

_WEAK_GRAPHIC_READINESS = ("low", "not_usable")
_EXAMPLE_VALUE_MAX = 60


def _cap_text(value: Any, limit: int = _EXAMPLE_VALUE_MAX) -> str:
    s = "" if value is None else str(value).strip()
    if len(s) <= limit:
        return s
    return s[: limit - 1].rstrip() + "…"


def classify_explained_delta_section(explanation: dict,
                                     delta: Optional[dict] = None) -> str:
    """Главная секция для одной объяснённой дельты (приоритетная, без двоения)."""
    e = explanation if isinstance(explanation, dict) else {}
    status = _clean(e.get("status"))
    model = e.get("model") or {}
    raw_status = _clean(model.get("raw_status"))
    critic = e.get("critic") or {}
    verdict = _clean(critic.get("verdict"))
    show = bool(critic.get("should_show_to_engineer", True))
    grounded = _clean((e.get("groundedness") or {}).get("verdict"))
    gc = e.get("graphic_context") or {}
    flags = set(e.get("quality_flags") or [])

    # 1) объяснение не состоялось: сбой/пропуск runner'а или нечитаемый ответ
    #    (llm_response_parse_failed = «объяснения нет», а не «нужна проверка»)
    if (status in ("failed", "skipped_no_runner")
            or (raw_status and raw_status != "ok")
            or "llm_response_parse_failed" in flags):
        return "llm_failed_or_skipped"
    # 2) скрываемое по умолчанию: ocr_noise + (не показывать ИЛИ risk none),
    #    а также явный reject критика (отвергнутая дельта сильнее, чем шум)
    if verdict == "reject" or status == "critic_rejected":
        return "likely_noise_hidden_by_default"
    if verdict == "possible_ocr_noise" and (not show or e.get("risk_level") == "none"):
        return "likely_noise_hidden_by_default"
    # 3) слабая графика
    if (verdict == "possible_weak_graphic" or bool(gc.get("needs_vision_enrichment"))
            or _clean(gc.get("readiness")) in _WEAK_GRAPHIC_READINESS):
        return "weak_graphic_review"
    # 4) ручная проверка (включая ocr_noise с show=true и risk≠none —
    #    инженер взглянет)
    if (verdict == "needs_human_review" or "needs_human_review" in flags
            or status == "needs_human_review"):
        return "needs_review"
    # 5) подтверждённое изменение
    if verdict == "accept" and show and grounded in ("grounded", "partially_grounded"):
        return "confirmed_changes"
    # fallback: всё неклассифицированное — на ручную проверку (safe default)
    return "needs_review"


def format_delta_section_examples(delta_ids: list, deltas_by_id: dict,
                                  explanations_by_id: dict,
                                  limit: int = 10) -> list[dict]:
    """Компактные примеры дельт секции (≤limit) для summary JSON/MD."""
    out: list[dict] = []
    for did in delta_ids[:limit]:
        d = deltas_by_id.get(did) or {}
        e = explanations_by_id.get(did) or {}
        critic = e.get("critic") or {}
        out.append({
            "delta_id": did,
            "entity_type": d.get("entity_type", "unknown"),
            "delta_type": d.get("delta_type", "unknown"),
            "subject": _cap_text(d.get("subject")),
            "field": d.get("field", ""),
            "old_value": _cap_text(d.get("old_value")),
            "new_value": _cap_text(d.get("new_value")),
            "critic_verdict": _clean(critic.get("verdict")) or None,
            "risk_level": e.get("risk_level"),
            "should_show_to_engineer": critic.get("should_show_to_engineer"),
        })
    return out


def build_delta_sections(entity_diff_report: Any, delta_explanation_report: Any,
                         graphic_descriptor_reports: Any = None) -> dict:
    """Секционирование объяснённых дельт + сводка coverage_notes.

    ``graphic_descriptor_reports`` зарезервирован для будущих эвристик —
    graphic-контекст каждой дельты уже встроен в её explanation
    (``graphic_context``), отдельный отчёт не требуется.
    """
    diff = entity_diff_report if isinstance(entity_diff_report, dict) else {}
    de = (delta_explanation_report
          if isinstance(delta_explanation_report, dict) else {})
    deltas_by_id = {d.get("delta_id"): d for d in (diff.get("deltas") or [])}
    explanations = [e for e in (de.get("explanations") or []) if isinstance(e, dict)]
    explanations_by_id = {e.get("delta_id"): e for e in explanations}

    ids_by_section: dict[str, list] = {k: [] for k in _DELTA_SECTION_ORDER}
    for e in explanations:
        did = e.get("delta_id")
        section = classify_explained_delta_section(e, deltas_by_id.get(did))
        ids_by_section[section].append(did)

    sections: dict[str, Any] = {"selected_total": len(explanations)}
    for key in _DELTA_SECTION_ORDER:
        ids = ids_by_section[key]
        sections[key] = {
            "count": len(ids),
            "delta_ids": ids,
            "description": _DELTA_SECTION_DESCRIPTIONS[key],
            "examples": format_delta_section_examples(
                ids, deltas_by_id, explanations_by_id),
        }

    notes = de.get("coverage_notes") or []
    kinds: dict[str, int] = {}
    for n in notes:
        if isinstance(n, dict):
            kinds[n.get("kind", "unknown")] = kinds.get(n.get("kind", "unknown"), 0) + 1
    sections["coverage_notes"] = {
        "count": len(notes),
        "weak_graphic": kinds.get("weak_graphic", 0),
        "matched_risk": kinds.get("matched_risk", 0),
    }
    return sections


def _block_link_preview_section(blp_report: Any, blp_enabled: bool,
                                blp_error: Optional[str]) -> dict:
    s = (blp_report.get("summary") if isinstance(blp_report, dict) else {}) or {}
    sec = {
        "enabled": bool(blp_enabled),
        "status": ("disabled" if not blp_enabled else
                   "failed" if blp_error else
                   (blp_report or {}).get("status", "not_run")
                   if isinstance(blp_report, dict) else "not_run"),
        "page_links_total": s.get("page_links_total", 0),
        "block_links_total": s.get("block_links_total", 0),
        "strong_links": s.get("strong_links", 0),
        "weak_links": s.get("weak_links", 0),
        "manual_review_links": s.get("manual_review_links", 0),
        "unmatched_left_blocks": s.get("unmatched_left_blocks", 0),
        "unmatched_right_blocks": s.get("unmatched_right_blocks", 0),
        "graphic_links_total": s.get("graphic_links_total", 0),
        "visual_gate_available": s.get("visual_gate_available", False),
    }
    if blp_error:
        sec["error"] = blp_error
    return sec


def _visual_gate_section(ve_report: Any, ve_enabled: bool,
                         ve_error: Optional[str]) -> dict:
    s = (ve_report.get("summary") if isinstance(ve_report, dict) else {}) or {}
    sec = {
        "enabled": bool(ve_enabled),
        "status": ("disabled" if not ve_enabled else
                   "failed" if ve_error else
                   (ve_report or {}).get("status", "not_run")
                   if isinstance(ve_report, dict) else "not_run"),
        "matched_graphic_blocks_total": s.get("matched_graphic_blocks_total", 0),
        "compared_total": s.get("compared_total", 0),
        "identical_visual": s.get("identical_visual", 0),
        "minor_visual": s.get("minor_visual", 0),
        "changed_visual": s.get("changed_visual", 0),
        "uncertain": s.get("uncertain", 0),
        "render_failed": s.get("render_failed", 0),
        "skipped": s.get("skipped", 0),
        "exclude_from_vision": s.get("exclude_from_vision", 0),
        "send_to_vision": s.get("send_to_vision", 0),
        "manual_review": s.get("manual_review", 0),
    }
    if ve_error:
        sec["error"] = ve_error
    return sec


def build_pipeline_v2_summary(*, left_model: Any, right_model: Any, block_report: Any,
                              entity_report: Any, diff_report: Any, inputs: dict,
                              artifact_paths: dict, warnings: list[str], status: str,
                              error: Optional[str] = None, left_graphic: Any = None,
                              right_graphic: Any = None, matched_graphic: Optional[list] = None,
                              graphic_error: Optional[str] = None, de_report: Any = None,
                              de_enabled: bool = True, de_error: Optional[str] = None,
                              ve_report: Any = None, ve_enabled: bool = True,
                              ve_error: Optional[str] = None,
                              blp_report: Any = None, blp_enabled: bool = True,
                              blp_error: Optional[str] = None) -> dict:
    """Собрать ``pipeline_v2_summary`` из артефактов этапов 1–4."""
    lm, rm = _summary_of(left_model), _summary_of(right_model)
    bm, em, dm = _summary_of(block_report), _summary_of(entity_report), _summary_of(diff_report)

    stages = {
        "prepared_ingest": {
            "left_pages_total": lm.get("pages_total", 0),
            "right_pages_total": rm.get("pages_total", 0),
            "left_blocks_total": lm.get("blocks_total", 0),
            "right_blocks_total": rm.get("blocks_total", 0),
            "left_by_page_type": lm.get("by_page_type", {}),
            "right_by_page_type": rm.get("by_page_type", {}),
            "warnings_count": _warnings_count(left_model) + _warnings_count(right_model),
        },
        "block_matching": {
            "page_matches_total": bm.get("page_matches_total", 0),
            "block_matches_total": bm.get("block_matches_total", 0),
            "unmatched_left_blocks": bm.get("unmatched_left_blocks", 0),
            "unmatched_right_blocks": bm.get("unmatched_right_blocks", 0),
            "strong_block_matches": bm.get("strong_block_matches", 0),
            "weak_block_matches": bm.get("weak_block_matches", 0),
            "warnings_count": _warnings_count(block_report),
        },
        "entity_extraction": {
            "left_entities_total": em.get("left_entities_total", 0),
            "right_entities_total": em.get("right_entities_total", 0),
            "by_entity_type": em.get("by_entity_type", {}),
            "by_semantic_group": em.get("by_semantic_group", {}),
            "warnings_count": _warnings_count(entity_report),
        },
        "entity_diff": {
            "deltas_total": dm.get("deltas_total", 0),
            "added_total": dm.get("added_total", 0),
            "removed_total": dm.get("removed_total", 0),
            "changed_total": dm.get("changed_total", 0),
            "uncertain_total": dm.get("uncertain_total", 0),
            "by_entity_type": dm.get("by_entity_type", {}),
            "by_delta_type": dm.get("by_delta_type", {}),
            "by_confidence": dm.get("by_confidence", {}),
            "warnings_count": _warnings_count(diff_report),
        },
    }
    summary = {
        "version": SUMMARY_VERSION,
        "kind": SUMMARY_KIND,
        "status": status,
        "artifacts": {k: p.name for k, p in artifact_paths.items()},
        "inputs": inputs,
        "stages": stages,
        "graphic_descriptor": _graphic_descriptor_section(
            left_graphic, right_graphic, matched_graphic, graphic_error),
        "visual_equivalence_gate": _visual_gate_section(ve_report, ve_enabled,
                                                        ve_error),
        "block_link_preview": _block_link_preview_section(blp_report, blp_enabled,
                                                          blp_error),
        "delta_explanation": _delta_explanation_section(de_report, de_enabled, de_error),
        "delta_sections": build_delta_sections(diff_report, de_report),
        "warnings": warnings,
        "next_recommended_stage": NEXT_RECOMMENDED_STAGE,
    }
    if error:
        summary["error"] = error
    return summary


def write_pipeline_v2_summary_json(out_path: str | Path, summary: dict) -> Path:
    """Атомарно записать summary JSON."""
    return _atomic_write_text(out_path, json.dumps(summary, ensure_ascii=False, indent=2))


def _md_counts_table(title: str, counts: dict) -> list[str]:
    if not counts:
        return [f"- {title}: —"]
    parts = ", ".join(f"{k}={v}" for k, v in counts.items())
    return [f"- {title}: {parts}"]


def _graphic_traffic_light(gd: dict) -> str:
    """Светофор пригодности графики для deterministic diff."""
    combined = (gd.get("left_graphic_blocks_total", 0)
                + gd.get("right_graphic_blocks_total", 0))
    if combined == 0:
        return "ℹ Графических блоков нет."
    not_usable = (gd.get("by_readiness", {}) or {}).get("not_usable", 0)
    manual = (gd.get("left_manual_review_recommended_total", 0)
              + gd.get("right_manual_review_recommended_total", 0))
    vision = (gd.get("left_needs_vision_enrichment_total", 0)
              + gd.get("right_needs_vision_enrichment_total", 0))
    if not_usable > 0 or manual > 0:
        return "🔴 Есть графические блоки, которые нельзя уверенно сравнить (нужна ручная проверка)."
    if vision > 0:
        return "🟡 Есть блоки, которым нужен vision enrichment."
    return "🟢 Графика пригодна для deterministic diff."


_DELTA_SECTION_MD_TITLES = [
    ("confirmed_changes", "### ✅ Подтверждённые изменения"),
    ("needs_review", "### 🟡 На ручную проверку"),
    ("weak_graphic_review", "### 🟠 Слабая графика / нужна доработка vision"),
    ("likely_noise_hidden_by_default", "### ⚪ Вероятный шум / скрывать по умолчанию"),
    ("llm_failed_or_skipped", "### 🔴 Ошибки или пропущенные объяснения"),
]


def _delta_example_md(ex: dict) -> str:
    show = ex.get("should_show_to_engineer")
    show_s = "—" if show is None else ("true" if show else "false")
    subj = ex.get("subject") or ex.get("field") or ""
    return (f"- {ex.get('entity_type')}/{ex.get('delta_type')} {subj}: "
            f"`{ex.get('old_value') or '∅'}` → `{ex.get('new_value') or '∅'}` "
            f"[{ex.get('critic_verdict') or '—'}, risk={ex.get('risk_level') or '—'}, "
            f"show={show_s}]")


def _delta_sections_md(ds: dict) -> list[str]:
    """Раздел «## Delta sections» для summary.md (компактный, ≤10 примеров)."""
    lines: list[str] = ["## Delta sections", ""]
    if not ds or not ds.get("selected_total"):
        lines.append("- Объяснённых дельт нет (LLM не запускался или selection пуст).")
        lines.append("")
        cov = (ds or {}).get("coverage_notes") or {}
        if cov.get("count"):
            lines.append(f"- Coverage notes: {cov.get('count', 0)} "
                         f"(weak_graphic={cov.get('weak_graphic', 0)}, "
                         f"matched_risk={cov.get('matched_risk', 0)})")
            lines.append("")
        return lines
    for key, title in _DELTA_SECTION_MD_TITLES:
        sec = ds.get(key) or {}
        count = sec.get("count", 0)
        lines.append(f"{title} — {count}")
        examples = sec.get("examples") or []
        if not examples:
            lines.append("- нет")
        else:
            for ex in examples[:10]:
                lines.append(_delta_example_md(ex))
            if count > len(examples):
                lines.append(f"- … ещё {count - len(examples)}")
        lines.append("")
    cov = ds.get("coverage_notes") or {}
    lines.append(f"- Coverage notes: {cov.get('count', 0)} "
                 f"(weak_graphic={cov.get('weak_graphic', 0)}, "
                 f"matched_risk={cov.get('matched_risk', 0)})")
    lines.append("")
    return lines


def write_pipeline_v2_summary_md(out_path: str | Path, summary: dict,
                                 artifact_paths: dict) -> Path:
    """Атомарно записать человекочитаемый Markdown-summary."""
    st = summary.get("stages", {})
    ing = st.get("prepared_ingest", {})
    blk = st.get("block_matching", {})
    ent = st.get("entity_extraction", {})
    dif = st.get("entity_diff", {})
    inp = summary.get("inputs", {})
    status = summary.get("status", "unknown")
    warnings = summary.get("warnings", []) or []

    lines: list[str] = []
    lines.append("# Pipeline V2 — Dry Run Summary")
    lines.append("")
    lines.append(f"**Статус:** `{status}`")
    if summary.get("error"):
        lines.append("")
        lines.append(f"**Ошибка:** {summary['error']}")
    lines.append("")
    lines.append(f"**Следующий этап:** `{summary.get('next_recommended_stage', '')}`")
    lines.append("")

    lines.append("## Входные файлы")
    for side in ("left", "right"):
        s = inp.get(side, {})
        lines.append(f"- **{side}**: result_json=`{s.get('result_json_path')}`; "
                     f"md=`{s.get('document_md_path')}`; html=`{s.get('ocr_html_path')}`; "
                     f"pdf=`{s.get('pdf_path')}`")
    lines.append("")

    lines.append("## [1] Prepared Ingest")
    lines.append(f"- Страниц: left={ing.get('left_pages_total', 0)}, "
                 f"right={ing.get('right_pages_total', 0)}")
    lines.append(f"- Блоков: left={ing.get('left_blocks_total', 0)}, "
                 f"right={ing.get('right_blocks_total', 0)}")
    lines += _md_counts_table("Типы страниц (left)", ing.get("left_by_page_type", {}))
    lines += _md_counts_table("Типы страниц (right)", ing.get("right_by_page_type", {}))
    lines.append("")

    lines.append("## [2] Block Matching")
    lines.append(f"- Сопоставлено страниц: {blk.get('page_matches_total', 0)}")
    lines.append(f"- Сопоставлено блоков: {blk.get('block_matches_total', 0)} "
                 f"(strong={blk.get('strong_block_matches', 0)}, "
                 f"weak={blk.get('weak_block_matches', 0)})")
    lines.append(f"- Непарных блоков: left={blk.get('unmatched_left_blocks', 0)}, "
                 f"right={blk.get('unmatched_right_blocks', 0)}")
    lines.append("")

    lines.append("## [3] Entity Extraction")
    lines.append(f"- Сущностей: left={ent.get('left_entities_total', 0)}, "
                 f"right={ent.get('right_entities_total', 0)}")
    lines += _md_counts_table("По типам", ent.get("by_entity_type", {}))
    lines += _md_counts_table("По группам", ent.get("by_semantic_group", {}))
    lines.append("")

    lines.append("## [4] Deterministic Entity Diff")
    lines.append(f"- Всего дельт: **{dif.get('deltas_total', 0)}**")
    lines.append(f"- added={dif.get('added_total', 0)}, removed={dif.get('removed_total', 0)}, "
                 f"changed={dif.get('changed_total', 0)}, uncertain={dif.get('uncertain_total', 0)}")
    lines += _md_counts_table("Уверенность", dif.get("by_confidence", {}))
    lines += _md_counts_table("По типам дельт", dif.get("by_entity_type", {}))
    lines.append("")

    gd = summary.get("graphic_descriptor", {}) or {}
    lines.append("## Graphic readiness")
    lines.append(f"- Графических блоков слева: {gd.get('left_graphic_blocks_total', 0)}")
    lines.append(f"- Графических блоков справа: {gd.get('right_graphic_blocks_total', 0)}")
    lines.append(f"- Пригодны для diff: left={gd.get('left_usable_for_diff_total', 0)}, "
                 f"right={gd.get('right_usable_for_diff_total', 0)}")
    lines.append(f"- Требуют vision enrichment: left={gd.get('left_needs_vision_enrichment_total', 0)}, "
                 f"right={gd.get('right_needs_vision_enrichment_total', 0)}")
    lines.append(f"- Ручная проверка: left={gd.get('left_manual_review_recommended_total', 0)}, "
                 f"right={gd.get('right_manual_review_recommended_total', 0)}")
    lines += _md_counts_table("По типам графики", gd.get("by_graphic_type", {}))
    lines += _md_counts_table("По дисциплинам", gd.get("by_discipline", {}))
    lines += _md_counts_table("По readiness", gd.get("by_readiness", {}))
    lines.append(f"- Риски matched-графики: low_token_overlap="
                 f"{gd.get('matched_low_token_overlap_total', 0)}, "
                 f"one_side_not_usable={gd.get('matched_one_side_not_usable_total', 0)}, "
                 f"discipline_mismatch={gd.get('matched_discipline_mismatch_total', 0)}, "
                 f"graphic_type_mismatch={gd.get('matched_graphic_type_mismatch_total', 0)}")
    lines.append(f"- {_graphic_traffic_light(gd)}")
    if gd.get("error"):
        lines.append(f"- ⚠ Ошибка graphic descriptor: {gd['error']}")
    lines.append("")

    ve = summary.get("visual_equivalence_gate", {}) or {}
    lines.append("## Visual equivalence gate (mark-only, до vision)")
    lines.append(f"- Status: `{ve.get('status', 'unknown')}`")
    lines.append(f"- Compared: {ve.get('compared_total', 0)} из "
                 f"{ve.get('matched_graphic_blocks_total', 0)} matched graphic pairs")
    lines.append(f"- identical_visual={ve.get('identical_visual', 0)}, "
                 f"minor_visual={ve.get('minor_visual', 0)}, "
                 f"changed_visual={ve.get('changed_visual', 0)}, "
                 f"uncertain={ve.get('uncertain', 0)}, "
                 f"render_failed={ve.get('render_failed', 0)}, "
                 f"skipped={ve.get('skipped', 0)}")
    lines.append(f"- excluded_from_vision={ve.get('exclude_from_vision', 0)}, "
                 f"send_to_vision={ve.get('send_to_vision', 0)}, "
                 f"manual_review={ve.get('manual_review', 0)}")
    if ve.get("error"):
        lines.append(f"- ⚠ Ошибка visual gate: {ve['error']}")
    lines.append("")

    blp = summary.get("block_link_preview", {}) or {}
    lines.append("## Block link preview (read-only, для UI «Связь блоков»)")
    lines.append(f"- Status: `{blp.get('status', 'unknown')}`")
    lines.append(f"- Page links: {blp.get('page_links_total', 0)}, "
                 f"block links: {blp.get('block_links_total', 0)} "
                 f"(graphic: {blp.get('graphic_links_total', 0)})")
    lines.append(f"- strong={blp.get('strong_links', 0)}, "
                 f"weak={blp.get('weak_links', 0)}, "
                 f"manual_review={blp.get('manual_review_links', 0)}, "
                 f"unmatched: left={blp.get('unmatched_left_blocks', 0)} / "
                 f"right={blp.get('unmatched_right_blocks', 0)}")
    if blp.get("error"):
        lines.append(f"- ⚠ Ошибка block link preview: {blp['error']}")
    lines.append("")

    de = summary.get("delta_explanation", {}) or {}
    lines.append("## Delta explanation / critic")
    lines.append(f"- Status: `{de.get('status', 'unknown')}`")
    lines.append(f"- Selected deltas: {de.get('selected_total', 0)} из {de.get('deltas_total', 0)}")
    lines.append(f"- Explained: {de.get('explained_total', 0)}")
    lines.append(f"- Needs human review: {de.get('needs_human_review_total', 0)}")
    lines.append(f"- Possible OCR noise: {de.get('possible_ocr_noise_total', 0)}")
    lines.append(f"- Possible weak graphic: {de.get('possible_weak_graphic_total', 0)}")
    lines.append(f"- Coverage notes: {de.get('coverage_notes_total', 0)}")
    de_status = de.get("status")
    if de_status == "disabled":
        lines.append("- ℹ LLM explanation отключён (delta_explanation.enabled=false).")
    elif de_status == "skipped_no_runner":
        lines.append("- ℹ LLM explanation не запускался: runner не передан (offline).")
    elif de_status == "failed":
        lines.append(f"- ⚠ LLM explanation упал: {de.get('error', 'см. warnings')}.")
    else:
        lines.append("- ✅ LLM explanation выполнен по выбранным дельтам.")
    lines.append("")

    lines += _delta_sections_md(summary.get("delta_sections") or {})

    lines.append("## Warnings")
    if warnings:
        for w in warnings[:10]:
            lines.append(f"- {w}")
        if len(warnings) > 10:
            lines.append(f"- … ещё {len(warnings) - 10}")
    else:
        lines.append("- нет")
    lines.append("")

    lines.append("## Артефакты")
    for key in _MANIFEST_ARTIFACT_KEYS + ["manifest"]:
        p = artifact_paths.get(key)
        if p is not None:
            mark = "✓" if Path(p).exists() else "—"
            lines.append(f"- `{p.name}` [{mark}]")
    lines.append("")

    lines.append("## Вывод")
    if status == "failed":
        lines.append(f"❌ Dry-run не завершён. Сначала устраните ошибку: "
                     f"{summary.get('error', 'см. warnings')}.")
    elif status == "completed_with_warnings":
        lines.append("⚠ Можно передавать в LLM explanation/critic, но проверьте warnings.")
    else:
        lines.append("✅ Готово к передаче в LLM explanation/critic.")
    lines.append("")

    return _atomic_write_text(out_path, "\n".join(lines))


def write_pipeline_v2_manifest(out_path: str | Path, artifact_paths: dict) -> Path:
    """Атомарно записать манифест: для каждого существующего артефакта —
    относительный путь, размер, sha256, kind."""
    out_path = Path(out_path)
    entries: list[dict] = []
    for key in _MANIFEST_ARTIFACT_KEYS:
        p = artifact_paths.get(key)
        if p is None or not Path(p).exists():
            entries.append({"key": key,
                            "filename": (Path(p).name if p is not None else None),
                            "exists": False, "size_bytes": None,
                            "sha256": None, "kind": None})
            continue
        p = Path(p)
        entries.append({
            "key": key,
            "filename": p.name,
            "relative_path": p.name,
            "exists": True,
            "size_bytes": p.stat().st_size,
            "sha256": _sha256(p),
            "kind": _read_kind(p),
        })
    manifest = {
        "version": SUMMARY_VERSION,
        "kind": MANIFEST_KIND,
        "artifacts": entries,
        "artifacts_total": sum(1 for e in entries if e["exists"]),
    }
    return _atomic_write_text(out_path, json.dumps(manifest, ensure_ascii=False, indent=2))


# ─── orchestrator ────────────────────────────────────────────────────────────


def run_pipeline_v2_dry_run(left_package: Any, right_package: Any, out_dir: str | Path,
                            options: Optional[dict] = None, llm_runner: Any = None) -> dict:
    """Прогнать этапы 1–5 (+ delta explanation) offline и записать артефакты.

    Возвращает summary-словарь (status ok/completed_with_warnings/failed).
    Чистый offline-конвейер: без сети/Qwen/Opus/crop-download.

    ``llm_runner`` ИНЪЕКТИРУЕТСЯ в delta explanation. По умолчанию ``None`` →
    объяснения `skipped_no_runner` (offline). Реальный runner подключается
    снаружи (controlled smoke), здесь не создаётся.
    """
    options = options or {}
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = build_pipeline_v2_artifact_paths(out_dir)

    left = normalize_package_paths(left_package)
    right = normalize_package_paths(right_package)
    warnings: list[str] = []
    inputs = {
        "left": _input_info(left, warnings, "left"),
        "right": _input_info(right, warnings, "right"),
    }

    de_opts = options.get("delta_explanation") or {}
    de_enabled = de_opts.get("enabled", True) is not False

    ve_opts = options.get("visual_gate") or {}
    ve_enabled = ve_opts.get("enabled", True) is not False

    blp_opts = options.get("block_link_preview") or {}
    blp_enabled = blp_opts.get("enabled", True) is not False

    status = "ok"
    error: Optional[str] = None
    left_model = right_model = block_report = entity_report = diff_report = None
    left_graphic = right_graphic = None
    matched_graphic: list = []
    graphic_error: Optional[str] = None
    ve_report = None
    ve_error: Optional[str] = None
    blp_report = None
    blp_error: Optional[str] = None
    de_report = None
    de_error: Optional[str] = None

    try:
        for side, pkg in (("left", left), ("right", right)):
            rj = pkg.get("result_json_path")
            if not rj:
                raise ValueError(f"{side}: result_json_path is required but missing")
            if not Path(rj).exists():
                raise FileNotFoundError(f"{side}: result_json_path not found: {rj}")

        # [1] prepared ingest
        left_model = build_normalized_document_model(
            left["result_json_path"], document_md_path=left["document_md_path"],
            ocr_html_path=left["ocr_html_path"], pdf_path=left["pdf_path"])
        right_model = build_normalized_document_model(
            right["result_json_path"], document_md_path=right["document_md_path"],
            ocr_html_path=right["ocr_html_path"], pdf_path=right["pdf_path"])
        write_normalized_document_model(paths["left_model"], left_model)
        write_normalized_document_model(paths["right_model"], right_model)

        # [2] block matching
        block_report = match_normalized_documents(
            left_model, right_model, options.get("matching"))
        write_block_matching_report(paths["block_matching"], block_report)

        # [3] graphic descriptor — вспомогательный, fail-soft: его падение НЕ
        #     валит обязательные этапы 1–4 (нижний try ловит ошибку).
        try:
            gopts = options.get("graphic")
            left_graphic = build_graphic_descriptor_report(left_model, side="left", options=gopts)
            right_graphic = build_graphic_descriptor_report(right_model, side="right", options=gopts)
            matched_graphic = describe_matched_graphic_blocks(
                left_model, right_model, block_report, gopts)
            write_graphic_descriptor_report(paths["left_graphic"], left_graphic)
            write_graphic_descriptor_report(paths["right_graphic"], right_graphic)
            write_graphic_descriptor_report(
                paths["graphic_matched"], build_graphic_matched_report(matched_graphic))
        except Exception as gexc:  # noqa: BLE001 — graphic не критичен
            graphic_error = f"{type(gexc).__name__}: {gexc}"

        # [3b] visual equivalence gate — mark-only наложение matched graphic
        #      блоков ДО vision; fail-soft: падение НЕ валит этапы 4-6,
        #      downstream пока не использует пометки (Stage 1: observe).
        if ve_enabled:
            try:
                ve_report = run_visual_equivalence_gate(
                    left_model, right_model, block_report,
                    left_graphic_report=left_graphic,
                    right_graphic_report=right_graphic,
                    graphic_matched_report=matched_graphic,
                    options=ve_opts)
                write_visual_equivalence_gate_report(paths["visual_gate"],
                                                     ve_report)
            except Exception as vexc:  # noqa: BLE001 — gate не критичен
                ve_error = f"{type(vexc).__name__}: {vexc}"

        # [3c] block link preview — read-only витрина предложенных связей
        #      для UI «Связь блоков»; fail-soft: падение НЕ валит этапы 4-6,
        #      ничего не применяет и существующие связи не меняет.
        if blp_enabled:
            try:
                blp_report = build_block_link_preview(
                    left_model, right_model, block_report,
                    left_graphic_report=left_graphic,
                    right_graphic_report=right_graphic,
                    visual_gate_report=ve_report)
                write_block_link_preview_report(paths["block_link_preview"],
                                                blp_report)
            except Exception as bexc:  # noqa: BLE001 — preview не критичен
                blp_error = f"{type(bexc).__name__}: {bexc}"

        # [4] entity extraction
        entity_report = extract_entities_for_matched_documents(
            left_model, right_model, block_report, options.get("extraction"))
        write_entity_extraction_report(paths["entity_extraction"], entity_report)

        # [5] deterministic entity diff
        diff_report = diff_entity_extraction_report(entity_report, options.get("diff"))
        write_entity_diff_report(paths["entity_diff"], diff_report)

        # [6] delta explanation / critic — вспомогательный LLM-слой, fail-soft.
        #     По умолчанию llm_runner=None → skipped_no_runner (offline).
        if de_enabled:
            try:
                de_report = explain_entity_diff_report(
                    diff_report,
                    graphic_descriptor_report={"left": left_graphic,
                                               "right": right_graphic,
                                               "matched": matched_graphic},
                    options=de_opts, llm_runner=llm_runner)
                write_delta_explanation_report(paths["delta_explanation"], de_report)
            except Exception as dexc:  # noqa: BLE001 — delta explanation не критичен
                de_error = f"{type(dexc).__name__}: {dexc}"

    except Exception as exc:  # fail-soft: фиксируем, не роняем процесс
        status = "failed"
        error = f"{type(exc).__name__}: {exc}"

    # собрать warnings из артефактов этапов
    for rpt, prefix in ((left_model, "ingest_left"), (right_model, "ingest_right"),
                        (block_report, "block_matching"),
                        (left_graphic, "graphic_left"), (right_graphic, "graphic_right"),
                        (entity_report, "entity_extraction"), (diff_report, "entity_diff")):
        if isinstance(rpt, dict):
            for w in rpt.get("warnings", []) or []:
                warnings.append(f"{prefix}: {w}")
    if graphic_error:
        warnings.append(f"graphic_descriptor: {graphic_error}")
    if isinstance(ve_report, dict):
        for w in ve_report.get("warnings", []) or []:
            warnings.append(f"visual_gate: {w}")
    if ve_error:
        warnings.append(f"visual_gate: {ve_error}")
    if isinstance(blp_report, dict):
        for w in blp_report.get("warnings", []) or []:
            warnings.append(f"block_link_preview: {w}")
    if blp_error:
        warnings.append(f"block_link_preview: {blp_error}")
    if isinstance(de_report, dict):
        # benign «no_llm_runner» (offline skip) НЕ повышаем до dry-run warning
        for w in de_report.get("warnings", []) or []:
            if str(w).startswith("no_llm_runner"):
                continue
            warnings.append(f"delta_explanation: {w}")
    if de_error:
        warnings.append(f"delta_explanation: {de_error}")

    if status != "failed" and warnings:
        status = "completed_with_warnings"

    summary = build_pipeline_v2_summary(
        left_model=left_model, right_model=right_model, block_report=block_report,
        entity_report=entity_report, diff_report=diff_report, inputs=inputs,
        artifact_paths=paths, warnings=warnings, status=status, error=error,
        left_graphic=left_graphic, right_graphic=right_graphic,
        matched_graphic=matched_graphic, graphic_error=graphic_error,
        de_report=de_report, de_enabled=de_enabled, de_error=de_error,
        ve_report=ve_report, ve_enabled=ve_enabled, ve_error=ve_error,
        blp_report=blp_report, blp_enabled=blp_enabled, blp_error=blp_error)

    # summary + manifest пишем всегда (best-effort, атомарно)
    write_pipeline_v2_summary_json(paths["summary_json"], summary)
    write_pipeline_v2_summary_md(paths["summary_md"], summary, paths)
    write_pipeline_v2_manifest(paths["manifest"], paths)
    return summary


__all__ = [
    "SUMMARY_VERSION",
    "SUMMARY_KIND",
    "MANIFEST_KIND",
    "GRAPHIC_MATCHED_KIND",
    "NEXT_RECOMMENDED_STAGE",
    "run_pipeline_v2_dry_run",
    "normalize_package_paths",
    "build_pipeline_v2_artifact_paths",
    "build_pipeline_v2_summary",
    "build_delta_sections",
    "classify_explained_delta_section",
    "format_delta_section_examples",
    "build_graphic_matched_report",
    "write_pipeline_v2_summary_md",
    "write_pipeline_v2_summary_json",
    "write_pipeline_v2_manifest",
]
