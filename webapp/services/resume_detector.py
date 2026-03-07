"""
Определение точки возобновления пайплайна.
Анализирует pipeline_log.json и выходные файлы для определения,
с какого этапа можно продолжить аудит.
"""
import json
from pathlib import Path

from webapp.config import PROJECTS_DIR


def detect_resume_stage(project_id: str) -> dict:
    """
    Определить, с какого этапа можно продолжить пайплайн.
    Возвращает: {stage, stage_label, detail, can_resume}

    Логика: сначала ищем этапы с ошибкой в pipeline_log,
    затем — незавершённые по файлам.
    """
    output_dir = PROJECTS_DIR / project_id / "_output"
    tiles_dir = output_dir / "tiles"

    # Проверяем наличие ключевых файлов
    has_tiles = tiles_dir.is_dir() and any(tiles_dir.glob("page_*//*.png"))
    has_batches = (output_dir / "tile_batches.json").exists()
    has_02 = (output_dir / "02_tiles_analysis.json").exists()
    has_03 = (output_dir / "03_findings.json").exists()
    has_norm_checks = (output_dir / "norm_checks.json").exists()
    has_03a = (output_dir / "03a_norms_verified.json").exists()

    # Подсчёт завершённых батчей
    completed_batches = 0
    total_batches = 0
    if has_batches:
        try:
            with open(output_dir / "tile_batches.json", "r", encoding="utf-8") as f:
                bd = json.load(f)
            total_batches = bd.get("total_batches", len(bd.get("batches", [])))
            for i in range(1, total_batches + 1):
                bf = output_dir / f"tile_batch_{i:03d}.json"
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
            # Порядок проверки этапов (от раннего к позднему)
            stage_order = [
                ("prepare", "prepare", "Подготовка"),
                ("tile_audit", "tile_audit", "Анализ тайлов"),
                ("main_audit", "main_audit", "Основной аудит"),
                ("norm_verify", "norm_verify", "Верификация норм"),
            ]
            for log_key, resume_stage, label in stage_order:
                info = stages_log.get(log_key, {})
                if info.get("status") == "error":
                    # Для tile_audit: если батчи не завершены — переделать
                    if log_key == "tile_audit" and total_batches > 0 and completed_batches < total_batches:
                        return {
                            "stage": "tile_audit",
                            "stage_label": label,
                            "detail": f"Ошибка, пакеты: {completed_batches}/{total_batches}",
                            "start_from": completed_batches + 1 if completed_batches > 0 else 1,
                            "can_resume": True,
                        }
                    # Для main_audit: если 03_findings нет — переделать
                    if log_key == "main_audit" and not has_03:
                        return {
                            "stage": "main_audit",
                            "stage_label": label,
                            "detail": "Ошибка, 03_findings.json не создан",
                            "can_resume": True,
                        }
                    # Общий случай: ошибка без валидного результата
                    if log_key == "prepare" and not has_tiles:
                        return {
                            "stage": resume_stage,
                            "stage_label": label,
                            "detail": f"Ошибка на этапе {label}",
                            "can_resume": True,
                        }
        except Exception:
            pass

    # ─── Приоритет 2: стандартная проверка по файлам ───
    if not has_tiles:
        return {
            "stage": "prepare",
            "stage_label": "Подготовка",
            "detail": "Тайлы не созданы",
            "can_resume": True,
        }

    if not has_02:
        if completed_batches > 0 and completed_batches < total_batches:
            return {
                "stage": "tile_audit",
                "stage_label": "Анализ тайлов",
                "detail": f"Пакеты: {completed_batches}/{total_batches}",
                "start_from": completed_batches + 1,
                "can_resume": True,
            }
        else:
            return {
                "stage": "tile_audit",
                "stage_label": "Анализ тайлов",
                "detail": "02_tiles_analysis.json не создан",
                "can_resume": True,
            }

    if not has_03:
        return {
            "stage": "main_audit",
            "stage_label": "Основной аудит",
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
        # Проверяем — нужен ли 03a? (может, все нормы актуальны)
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
