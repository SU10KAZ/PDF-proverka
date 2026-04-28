"""
Определение точки возобновления пайплайна.
Анализирует pipeline_log.json и выходные файлы для определения,
с какого этапа можно продолжить аудит.
"""
import json
from pathlib import Path

from webapp.services.project_service import resolve_project_dir


def detect_resume_stage(project_id: str) -> dict:
    """
    Определить, с какого этапа можно продолжить пайплайн.
    Возвращает: {stage, stage_label, detail, can_resume}

    Поддерживает оба пайплайна: блоковый (OCR) и тайловый (legacy).
    """
    output_dir = resolve_project_dir(project_id) / "_output"
    tiles_dir = output_dir / "tiles"

    # Проверяем наличие ключевых файлов
    has_tiles = tiles_dir.is_dir() and any(tiles_dir.glob("page_*/*.png"))
    has_03 = (output_dir / "03_findings.json").exists()
    has_norm_checks = (output_dir / "norm_checks.json").exists()
    has_03a = (output_dir / "03a_norms_verified.json").exists()

    # OCR-пайплайн (блоки)
    blocks_dir = output_dir / "blocks"
    has_blocks = blocks_dir.is_dir() and (blocks_dir / "index.json").exists()
    has_block_batches = (output_dir / "block_batches.json").exists()
    has_02_blocks = (output_dir / "02_blocks_analysis.json").exists()
    has_01_text = (output_dir / "01_text_analysis.json").exists()

    # Legacy (тайлы)
    has_tile_batches = (output_dir / "tile_batches.json").exists()
    has_02_tiles = (output_dir / "02_tiles_analysis.json").exists()

    has_02 = has_02_blocks or has_02_tiles

    # Подсчёт завершённых батчей (блоки приоритет → тайлы fallback)
    completed_batches = 0
    total_batches = 0
    if has_block_batches:
        batches_file = output_dir / "block_batches.json"
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
                    if log_key in ("tile_audit", "block_analysis") and total_batches > 0 and completed_batches < total_batches:
                        return {
                            "stage": resume_stage,
                            "stage_label": label,
                            "detail": f"Ошибка, пакеты: {completed_batches}/{total_batches}",
                            "start_from": completed_batches + 1 if completed_batches > 0 else 1,
                            "can_resume": True,
                        }
                    if log_key in ("main_audit", "findings_merge") and not has_03:
                        return {
                            "stage": resume_stage,
                            "stage_label": label,
                            "detail": "Ошибка, 03_findings.json не создан",
                            "can_resume": True,
                        }
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
                        return {
                            "stage": resume_stage,
                            "stage_label": label,
                            "detail": "01_text_analysis.json не создан",
                            "can_resume": True,
                        }

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

    # ─── Приоритет 2: стандартная проверка по файлам ───
    if not has_tiles and not has_blocks:
        return {
            "stage": "prepare",
            "stage_label": "Подготовка",
            "detail": "Блоки не созданы",
            "can_resume": True,
        }

    if not has_02:
        if completed_batches > 0 and completed_batches < total_batches:
            return {
                "stage": "tile_audit",
                "stage_label": "Анализ блоков",
                "detail": f"Пакеты: {completed_batches}/{total_batches}",
                "start_from": completed_batches + 1,
                "can_resume": True,
            }
        else:
            return {
                "stage": "tile_audit",
                "stage_label": "Анализ блоков",
                "detail": "02_blocks_analysis.json не создан",
                "can_resume": True,
            }

    if not has_03:
        return {
            "stage": "main_audit",
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
