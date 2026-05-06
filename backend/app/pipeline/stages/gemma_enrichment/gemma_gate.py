"""Shared validation helpers for the mandatory Gemma enrichment stage."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BASE_BLOCKS_DIRNAME,
    GEMMA_HIGH_DETAIL_BLOCKS_DIRNAME,
    gemma_blocks_index_path,
    validate_gemma_summary,
)


GEMMA_STAGE_LABEL = "Gemma OCR enrichment / предварительное распознавание чертежей"
GEMMA_MIGRATION_STATUS_DETAIL = "legacy_gemma_migration_required"

_MIGRATION_REQUIRED_GEMMA_STATUSES = {
    "missing_blocks",
    "missing_summary",
    "blocks_index_hash_mismatch",
    "crop_policy_mismatch",
    "md_hash_mismatch",
    "schema_mismatch",
    "stage_mismatch",
    "coverage_counts_invalid",
    "coverage_ratio_mismatch",
    "uncovered_blocks_missing",
    "missing_high_detail_blocks",
    "high_detail_crop_policy_mismatch",
    "high_detail_blocks_index_hash_mismatch",
    "blocks_missing",
    "blocks_total_mismatch",
    "final_decisions_missing",
    "duplicate_block_id",
    "large_block_skipped_ids_mismatch",
}

def load_project_info(project_dir: Path) -> dict[str, Any]:
    info_path = project_dir / "project_info.json"
    if not info_path.exists():
        return {}
    try:
        data = json.loads(info_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def find_project_markdown(
    project_dir: Path,
    project_info: dict[str, Any] | None = None,
) -> Path | None:
    """Resolve the Chandra OCR markdown file used by the audit pipeline."""
    project_info = project_info or {}
    configured = project_info.get("md_file")
    if isinstance(configured, str) and configured.strip():
        md_path = Path(configured)
        if not md_path.is_absolute():
            md_path = project_dir / md_path
        if md_path.exists() and md_path.is_file():
            return md_path

    candidates = sorted(project_dir.glob("*_document.md"))
    if candidates:
        return candidates[0]
    return None


def partial_gemma_allowed(project_info: dict[str, Any] | None = None) -> bool:
    """Backward-compatible flag. Partial coverage is now first-class.

    Supported project_info forms:
      - {"allow_partial_gemma_enrichment": true}
      - {"gemma_enrichment": {"allow_partial": true}}
      - {"gemma_enrichment": {"partial_mode": "allow" | "allowed"}}
    """
    project_info = project_info or {}
    if project_info.get("allow_partial_gemma_enrichment") is True:
        return True

    gemma_cfg = project_info.get("gemma_enrichment")
    if not isinstance(gemma_cfg, dict):
        return False
    if gemma_cfg.get("allow_partial") is True:
        return True
    return str(gemma_cfg.get("partial_mode", "")).lower() in {"allow", "allowed", "true"}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def _image_blocks_total(output_dir: Path) -> tuple[int | None, bool]:
    index_path = gemma_blocks_index_path(output_dir.parent)
    if not index_path.exists():
        return None, False
    index = _load_json(index_path)
    blocks = index.get("blocks")
    if not isinstance(blocks, list):
        return 0, True
    image_blocks = [
        b for b in blocks
        if isinstance(b, dict) and (b.get("block_type") or "").lower() == "image"
    ]
    return len(image_blocks), True


def detect_gemma_migration_state(
    project_dir: Path,
    *,
    gemma_state: dict[str, Any] | None = None,
    project_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Detect legacy-completed projects that require Gemma schema v2 migration."""
    project_dir = Path(project_dir)
    output_dir = project_dir / "_output"
    if gemma_state is None:
        gemma_state = evaluate_gemma_enrichment(project_dir, project_info)

    has_01_text = (output_dir / "01_text_analysis.json").exists()
    has_02_blocks = (output_dir / "02_blocks_analysis.json").exists()
    has_03_findings = (output_dir / "03_findings.json").exists()
    has_norm_checks = (output_dir / "norm_checks.json").exists()
    has_03a = (output_dir / "03a_norms_verified.json").exists()
    log = _load_json(output_dir / "pipeline_log.json")
    stages = log.get("stages", {}) if isinstance(log, dict) else {}
    excel_done = str((stages.get("excel") or {}).get("status") or "") in {"done", "skipped"}
    legacy_completed_artifacts = has_01_text and has_02_blocks and (
        has_03_findings or has_norm_checks or has_03a or excel_done
    )
    gemma_status = str(gemma_state.get("status") or "")

    state = {
        "migration_required": False,
        "legacy_completed_artifacts": legacy_completed_artifacts,
        "status_detail": "",
        "migration_reason": "",
        "stage": "",
        "detail": "",
        "gemma_status": gemma_status,
    }
    if not legacy_completed_artifacts:
        return state
    if gemma_state.get("ready"):
        return state
    if gemma_status not in _MIGRATION_REQUIRED_GEMMA_STATUSES:
        return state

    if gemma_status == "missing_blocks":
        stage = "prepare"
        reason = "gemma_base_blocks_missing"
        action = "rerun prepare/crop и затем gemma_enrichment"
    elif gemma_status == "missing_summary":
        stage = "gemma_enrichment"
        reason = "gemma_enrichment_summary_missing"
        action = "rerun gemma_enrichment"
    elif gemma_status == "schema_mismatch":
        stage = "gemma_enrichment"
        reason = "gemma_enrichment_schema_mismatch"
        action = "rerun gemma_enrichment"
    else:
        stage = "gemma_enrichment"
        reason = f"gemma_enrichment_{gemma_status}"
        action = "rerun gemma_enrichment"

    detail = (
        "Проект был завершён по старой Gemma-схеме; для новой production architecture "
        f"и Gemma schema v2 требуется {action}: {gemma_state.get('detail') or gemma_status}"
    )
    state.update({
        "migration_required": True,
        "status_detail": GEMMA_MIGRATION_STATUS_DETAIL,
        "migration_reason": reason,
        "stage": stage,
        "detail": detail,
    })
    return state


def evaluate_gemma_enrichment(
    project_dir: Path,
    project_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return normalized readiness state for the mandatory Gemma stage."""
    project_dir = Path(project_dir)
    project_info = project_info if project_info is not None else load_project_info(project_dir)
    output_dir = project_dir / "_output"
    partial_allowed = partial_gemma_allowed(project_info)

    md_path = find_project_markdown(project_dir, project_info)
    state: dict[str, Any] = {
        "ready": False,
        "status": "missing_md",
        "label": GEMMA_STAGE_LABEL,
        "has_md": md_path is not None,
        "md_path": str(md_path) if md_path else "",
        "partial_allowed": True,
        "blocks_ok": 0,
        "blocks_total": 0,
        "base_blocks_ok": 0,
        "expected_blocks_total": 0,
        "high_detail_candidates": 0,
        "high_detail_ok": 0,
        "high_detail_skipped_large": 0,
        "uncovered_block_ids": [],
        "large_block_skipped_ids": [],
        "detail": "MD-файл не найден",
    }
    if md_path is None:
        return state

    image_total, has_blocks_index = _image_blocks_total(output_dir)
    if not has_blocks_index:
        state.update({
            "status": "missing_blocks",
            "detail": f"_output/{GEMMA_BASE_BLOCKS_DIRNAME}/index.json не найден",
        })
        return state

    summary_path = output_dir / "gemma_enrichment_summary.json"
    summary = _load_json(summary_path)
    validation = validate_gemma_summary(project_dir, md_path=md_path, summary=summary, min_coverage=0.0)
    expected_total = max(
        int(image_total or 0),
        int(summary.get("blocks_total") or 0),
    )
    state["expected_blocks_total"] = expected_total
    ok = int(validation.get("blocks_ok") or summary.get("blocks_ok") or 0)
    total = max(
        int(summary.get("blocks_total") or 0),
        expected_total,
    )

    state.update({
        "blocks_ok": ok,
        "blocks_total": total,
        "base_blocks_ok": int(summary.get("base_blocks_ok") or 0),
        "high_detail_candidates": int(summary.get("high_detail_candidates") or 0),
        "high_detail_ok": int(summary.get("high_detail_ok") or 0),
        "high_detail_skipped_large": int(summary.get("high_detail_skipped_large") or 0),
        "model": summary.get("model", ""),
        "timestamp": summary.get("timestamp") or summary.get("created_at") or "",
        "coverage_ratio": float(summary.get("coverage_ratio") or validation.get("coverage_ratio") or 0.0),
        "summary_reason_code": validation.get("reason_code"),
        "uncovered_block_ids": list(summary.get("uncovered_block_ids") or validation.get("uncovered_block_ids") or []),
        "large_block_skipped_ids": list(summary.get("large_block_skipped_ids") or validation.get("large_block_skipped_ids") or []),
    })

    if validation.get("valid"):
        status = str(validation.get("status") or ("no_blocks" if total == 0 else "ok"))
        detail = "Gemma stage выполнен: image-блоков нет"
        if status == "ok":
            detail = f"Gemma enrichment готов ({ok}/{total})"
        elif status == "partial":
            detail = (
                f"Gemma enrichment готов с предупреждениями ({ok}/{total}); "
                f"непокрытых блоков: {len(state['uncovered_block_ids'])}, "
                f"high-detail skipped_large: {state['high_detail_skipped_large']}"
            )
        state.update({
            "ready": True,
            "status": status,
            "detail": detail,
        })
        return state

    reason_code = validation.get("reason_code")
    if reason_code in {
        "blocks_index_hash_mismatch", "crop_policy_mismatch", "md_hash_mismatch",
        "schema_mismatch", "stage_mismatch", "coverage_counts_invalid",
        "coverage_ratio_mismatch", "uncovered_blocks_missing",
        "missing_high_detail_blocks", "high_detail_crop_policy_mismatch",
        "high_detail_blocks_index_hash_mismatch", "blocks_missing",
        "blocks_total_mismatch", "final_decisions_missing", "duplicate_block_id",
        "large_block_skipped_ids_mismatch",
    }:
        state.update({
            "status": reason_code,
            "detail": validation.get("reason", "Gemma summary не соответствует текущим входам"),
        })
        return state
    if reason_code == "missing_summary":
        state.update({
            "status": "missing_summary",
            "detail": "gemma_enrichment_summary.json отсутствует",
        })
        return state
    if expected_total == 0:
        state.update({
            "status": "missing",
            "detail": "Gemma stage ещё не подтвердил отсутствие image-блоков",
        })
        return state

    state.update({
        "status": "failed",
        "detail": validation.get("reason") or f"Gemma enrichment неполный ({ok}/{total})",
    })
    return state


def gemma_gate_error(state: dict[str, Any], target_stage: str) -> str:
    status = state.get("status")
    if status == "missing_md":
        return (
            f"{GEMMA_STAGE_LABEL}: MD-файл обязателен перед {target_stage}. "
            "Создайте *_document.md через OCR и повторите запуск."
        )
    if status == "missing_blocks":
        return (
            f"{GEMMA_STAGE_LABEL}: сначала нужен prepare/crop "
            f"(_output/{GEMMA_BASE_BLOCKS_DIRNAME}/index.json отсутствует)."
        )
    if status in {
        "missing_summary", "blocks_index_hash_mismatch", "crop_policy_mismatch",
        "md_hash_mismatch", "schema_mismatch", "stage_mismatch",
        "coverage_counts_invalid", "coverage_ratio_mismatch",
        "uncovered_blocks_missing", "missing_high_detail_blocks",
        "high_detail_crop_policy_mismatch", "high_detail_blocks_index_hash_mismatch",
        "blocks_missing", "blocks_total_mismatch", "final_decisions_missing",
        "duplicate_block_id", "large_block_skipped_ids_mismatch",
    }:
        return (
            f"{GEMMA_STAGE_LABEL}: {state.get('detail')}. "
            "Gemma enrichment будет нужно выполнить заново."
        )
    return f"{GEMMA_STAGE_LABEL}: {state.get('detail') or 'этап не готов'}"
