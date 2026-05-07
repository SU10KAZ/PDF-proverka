"""
Определение точки возобновления пайплайна.
Анализирует pipeline_log.json и выходные файлы для определения,
с какого этапа можно продолжить аудит.
"""
import json
from pathlib import Path

from webapp.services.gemma_gate import (
    GEMMA_STAGE_LABEL,
    detect_gemma_migration_state,
    evaluate_gemma_enrichment,
    gemma_gate_error,
)
from webapp.services.project_service import resolve_project_dir
from gemma_enrichment_contract import gemma_blocks_dir


def detect_resume_stage(project_id: str) -> dict:
    """
    Определить, с какого этапа можно продолжить пайплайн.
    Возвращает: {stage, stage_label, detail, can_resume}

    Поддерживает оба пайплайна: блоковый (OCR) и тайловый (legacy).
    """
    project_dir = resolve_project_dir(project_id)
    output_dir = project_dir / "_output"
    tiles_dir = output_dir / "tiles"
    gemma_state = evaluate_gemma_enrichment(project_dir)
    gemma_ready = bool(gemma_state.get("ready"))
    migration_state = detect_gemma_migration_state(project_dir, gemma_state=gemma_state)

    # Проверяем наличие ключевых файлов
    has_tiles = tiles_dir.is_dir() and any(tiles_dir.glob("page_*/*.png"))
    has_03 = (output_dir / "03_findings.json").exists()
    has_norm_checks = (output_dir / "norm_checks.json").exists()
    has_03a = (output_dir / "03a_norms_verified.json").exists()

    # OCR-пайплайн (блоки)
    blocks_dir = gemma_blocks_dir(project_dir)
    has_blocks = blocks_dir.is_dir() and (blocks_dir / "index.json").exists()
    runtime_batches_path = output_dir / "block_batches.runtime.json"
    legacy_batches_path = output_dir / "block_batches.json"
    has_block_batches = runtime_batches_path.exists() or legacy_batches_path.exists()
    has_02_blocks = (output_dir / "02_blocks_analysis.json").exists()
    has_01_text = (output_dir / "01_text_analysis.json").exists()

    # Legacy (тайлы)
    has_tile_batches = (output_dir / "tile_batches.json").exists()
    has_02_tiles = (output_dir / "02_tiles_analysis.json").exists()

    has_02 = has_02_blocks or has_02_tiles

    def _prepare_resume(detail: str) -> dict:
        return {
            "stage": "prepare",
            "stage_label": "Подготовка",
            "detail": detail,
            "can_resume": True,
        }

    def _gemma_resume(detail: str | None = None) -> dict:
        if migration_state.get("migration_required"):
            return _migration_resume()
        if gemma_state.get("status") == "missing_blocks":
            return _prepare_resume("Блоки не созданы; Gemma enrichment требует prepare/crop")
        return {
            "stage": "gemma_enrichment",
            "stage_label": GEMMA_STAGE_LABEL,
            "detail": detail or gemma_state.get("detail", "Gemma enrichment не готов"),
            "can_resume": True,
        }

    def _migration_resume() -> dict:
        stage = str(migration_state.get("stage") or "gemma_enrichment")
        return {
            "stage": stage,
            "stage_label": "Подготовка" if stage == "prepare" else GEMMA_STAGE_LABEL,
            "detail": migration_state.get("detail") or gemma_state.get("detail") or "Требуется миграция Gemma schema v2",
            "can_resume": True,
            "migration_required": True,
            "status_detail": migration_state.get("status_detail") or "legacy_gemma_migration_required",
            "migration_reason": migration_state.get("migration_reason") or "gemma_migration_required",
            "gemma_status": migration_state.get("gemma_status") or gemma_state.get("status") or "",
            "legacy_completed_artifacts": bool(migration_state.get("legacy_completed_artifacts")),
        }

    def _text_resume(detail: str) -> dict:
        if not gemma_ready:
            return _gemma_resume(
                f"{detail}; сначала требуется {GEMMA_STAGE_LABEL}: {gemma_state.get('detail')}"
            )
        return {
            "stage": "text_analysis",
            "stage_label": "Анализ текста",
            "detail": detail,
            "can_resume": True,
        }

    def _block_resume(detail: str, *, start_from: int | None = None, legacy_tile: bool = False) -> dict:
        if has_blocks and not gemma_ready:
            return _gemma_resume(gemma_gate_error(gemma_state, "block_analysis"))
        if has_blocks and not has_01_text:
            return {
                "stage": "text_analysis",
                "stage_label": "Анализ текста",
                "detail": "01_text_analysis.json отсутствует; resume на block_analysis запрещён",
                "can_resume": True,
            }

        if gemma_state.get("status") in {"partial_allowed", "partial"}:
            detail = f"{detail}; {gemma_state.get('detail')} (будет отражено в pipeline report)"

        stage = "tile_audit" if legacy_tile and not has_blocks else "block_analysis"
        result = {
            "stage": stage,
            "stage_label": "Анализ блоков",
            "detail": detail,
            "can_resume": True,
        }
        if start_from is not None:
            result["start_from"] = start_from
        return result

    def _findings_resume(detail: str) -> dict:
        if has_blocks and not gemma_ready:
            return _gemma_resume(gemma_gate_error(gemma_state, "findings_merge"))
        if has_blocks and not has_01_text:
            return {
                "stage": "text_analysis",
                "stage_label": "Анализ текста",
                "detail": "01_text_analysis.json отсутствует; findings_merge невозможен",
                "can_resume": True,
            }
        if has_blocks and not has_02_blocks:
            return _block_resume("02_blocks_analysis.json отсутствует; findings_merge невозможен")
        return {
            "stage": "findings_merge",
            "stage_label": "Свод замечаний",
            "detail": detail,
            "can_resume": True,
        }

    # Подсчёт завершённых батчей (блоки приоритет → тайлы fallback)
    completed_batches = 0
    total_batches = 0
    if has_block_batches:
        batches_file = runtime_batches_path if runtime_batches_path.exists() else legacy_batches_path
        batch_prefix = "block_batch"
    elif has_tile_batches:
        batches_file = output_dir / "tile_batches.json"
        batch_prefix = "tile_batch"
    else:
        batches_file = None
        batch_prefix = ""

    if batches_file:
        try:
            with open(batches_file, "r", encoding="utf-8") as f:
                bd = json.load(f)
            total_batches = bd.get("total_batches", len(bd.get("batches", [])))
            for i in range(1, total_batches + 1):
                bf = output_dir / f"{batch_prefix}_{i:03d}.json"
                if bf.exists() and bf.stat().st_size > 100:
                    completed_batches += 1
        except Exception:
            pass

    # ─── Приоритет 1: проверить pipeline_log на ошибочные этапы ───
    log_path = output_dir / "pipeline_log.json"
    if log_path.exists():
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                log = json.load(f)
            stages_log = log.get("stages", {})
            stage_order = [
                ("prepare", "prepare", "Подготовка"),
                ("crop_blocks", "crop_blocks", "Кроп блоков"),
                ("gemma_enrichment", "gemma_enrichment", GEMMA_STAGE_LABEL),
                ("text_analysis", "text_analysis", "Анализ текста"),
                ("block_analysis", "block_analysis", "Анализ блоков"),
                ("tile_audit", "tile_audit", "Анализ блоков"),
                ("main_audit", "main_audit", "Основной аудит"),
                ("findings_merge", "findings_merge", "Свод замечаний"),
                ("norm_verify", "norm_verify", "Верификация норм"),
            ]
            for log_key, resume_stage, label in stage_order:
                info = stages_log.get(log_key, {})
                if info.get("status") in ("error", "interrupted"):
                    if log_key == "gemma_enrichment" and not gemma_ready:
                        return _gemma_resume(f"Ошибка на этапе {GEMMA_STAGE_LABEL}")
                    if log_key in ("tile_audit", "block_analysis") and total_batches > 0 and completed_batches < total_batches:
                        return _block_resume(
                            f"Ошибка, пакеты: {completed_batches}/{total_batches}",
                            start_from=completed_batches + 1 if completed_batches > 0 else 1,
                            legacy_tile=(log_key == "tile_audit"),
                        )
                    if log_key in ("main_audit", "findings_merge") and not has_03:
                        return _findings_resume("Ошибка, 03_findings.json не создан")
                    if log_key == "prepare" and not has_tiles and not has_blocks:
                        return {
                            "stage": resume_stage,
                            "stage_label": label,
                            "detail": f"Ошибка на этапе {label}",
                            "can_resume": True,
                        }
                    if log_key == "crop_blocks" and not has_blocks:
                        return {
                            "stage": resume_stage,
                            "stage_label": label,
                            "detail": "Блоки не созданы",
                            "can_resume": True,
                        }
                    if log_key == "text_analysis" and not has_01_text:
                        return _text_resume("01_text_analysis.json не создан")
                    if log_key in ("block_analysis", "tile_audit") and not has_02:
                        return _block_resume(
                            "02_blocks_analysis.json не создан",
                            legacy_tile=(log_key == "tile_audit"),
                        )

            # Если финальный этап уже завершён, проект нельзя "продолжать",
            # даже если отсутствует вспомогательный снапшот 03a_norms_verified.json.
            excel_info = stages_log.get("excel", {})
            if excel_info.get("status") in ("done", "skipped"):
                return {
                    "stage": "completed",
                    "stage_label": "Завершён",
                    "detail": "Все этапы выполнены",
                    "can_resume": False,
                }
        except Exception:
            pass

    if migration_state.get("migration_required"):
        # Старый завершённый проект (есть findings/norms) — не показываем баннер миграции.
        if has_03 or has_norm_checks or has_03a:
            return {
                "stage": "completed",
                "stage_label": "Завершён",
                "detail": "Все этапы выполнены",
                "can_resume": False,
            }
        return _migration_resume()

    # ─── Приоритет 2: стандартная проверка по файлам ───
    if not has_tiles and not has_blocks:
        return _prepare_resume("Блоки не созданы")

    if has_blocks and not gemma_ready:
        return _gemma_resume(gemma_gate_error(gemma_state, "text_analysis"))

    if has_blocks and not has_01_text:
        return {
            "stage": "text_analysis",
            "stage_label": "Анализ текста",
            "detail": "01_text_analysis.json не создан",
            "can_resume": True,
        }

    if not has_02:
        if completed_batches > 0 and completed_batches < total_batches:
            return _block_resume(
                f"Пакеты: {completed_batches}/{total_batches}",
                start_from=completed_batches + 1,
                legacy_tile=has_tile_batches and not has_blocks,
            )
        else:
            return _block_resume(
                "02_blocks_analysis.json не создан",
                legacy_tile=has_tile_batches and not has_blocks,
            )

    if not has_03:
        return {
            "stage": "findings_merge",
            "stage_label": "Свод замечаний",
            "detail": "03_findings.json не создан",
            "can_resume": True,
        }

    if not has_norm_checks:
        return {
            "stage": "norm_verify",
            "stage_label": "Верификация норм",
            "detail": "norm_checks.json не создан",
            "can_resume": True,
        }

    if not has_03a:
        try:
            with open(output_dir / "norm_checks.json", "r", encoding="utf-8") as f:
                checks = json.load(f)
            needs_fix = any(c.get("needs_revision") for c in checks.get("checks", []))
            if needs_fix:
                return {
                    "stage": "norm_verify",
                    "stage_label": "Пересмотр замечаний",
                    "detail": "Есть нормы для пересмотра, 03a не создан",
                    "can_resume": True,
                }
        except Exception:
            pass

    # Всё завершено
    return {
        "stage": "completed",
        "stage_label": "Завершён",
        "detail": "Все этапы выполнены",
        "can_resume": False,
    }
