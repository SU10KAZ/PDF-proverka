# -*- coding: utf-8 -*-
"""Production data-root health / sanity check (offline, read-only).

Зачем
-----
``/api/info == 200`` НЕДОСТАТОЧНО как healthcheck: backend может отвечать 200,
но читать пустой/неполный data root (deploy worktree вместо MAIN) — тогда
исчезают объекты (Alia/Балчуг), projects=0, Excel-решения сохраняются не в тот
tree, Pipeline V2 панели → not_found. Инцидент 2026-06-14.

Этот модуль даёт ДЕТЕРМИНИРОВАННУЮ проверку «правильный ли data root читает
backend»: ожидается ``objects >= 3``, ``projects >= ~100``, существующий
comparison root и наличие Pipeline V2 артефактов известной пары.

Чистая функция ``evaluate_production_data_roots`` принимает уже собранные
значения (объекты/проекты/пути) и возвращает структурированный вердикт —
без сети, без моделей, без записи на диск. Сбор live-значений — дело CLI/caller'а
(``scripts/check_production_data_roots.sh``).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

HEALTH_KIND = "stage_comparison_production_data_root_health"

# Пороги ожидаемого MAIN-состояния.
MIN_OBJECTS = 3        # 213 / 214 Alia / 272 Балчуг
MIN_PROJECTS = 100     # ~109 проектов на MAIN

# Объектов <= этого = опасный deploy-root симптом (объекты исчезли).
DANGEROUS_OBJECTS_MAX = 1

STATUS_OK = "ok"
STATUS_WARNING = "warning"
STATUS_DANGEROUS = "dangerous"


def evaluate_production_data_roots(
        *,
        objects_count: Optional[int],
        projects_count: Optional[int],
        comparison_root: Optional[str],
        comparison_root_exists: Optional[bool] = None,
        pipeline_v2_artifacts_present: Optional[bool] = None,
        base_dir: Optional[str] = None,
        min_objects: int = MIN_OBJECTS,
        min_projects: int = MIN_PROJECTS) -> dict[str, Any]:
    """Оценить, читает ли backend правильный (полный, MAIN) data root.

    Возвращает ``{kind, status, checks, warnings, dangerous, ...}``. Не делает
    сетевых вызовов и не пишет на диск.
    """
    warnings: list[str] = []
    dangerous = False

    oc = objects_count if isinstance(objects_count, int) else None
    pc = projects_count if isinstance(projects_count, int) else None

    # comparison_root существование: если флаг не передан — проверяем сами (FS read)
    if comparison_root_exists is None and comparison_root:
        try:
            comparison_root_exists = Path(comparison_root).is_dir()
        except OSError:
            comparison_root_exists = False

    objects_ok = oc is not None and oc >= min_objects
    projects_ok = pc is not None and pc >= min_projects
    comparison_ok = bool(comparison_root) and bool(comparison_root_exists)

    # ── dangerous: deploy-root incident-симптомы ─────────────────────────────
    if oc is not None and oc <= DANGEROUS_OBJECTS_MAX:
        dangerous = True
        warnings.append(
            f"objects_count={oc} <= {DANGEROUS_OBJECTS_MAX}: вероятно, backend "
            "читает НЕПОЛНЫЙ deploy data root (объекты Alia/Балчуг исчезли). "
            "Проверьте AUDIT_DATA_DIR/COMPARISON_ROOT → должны указывать на MAIN.")
    if pc == 0:
        dangerous = True
        warnings.append("projects_count=0: backend читает пустой data root "
                        "(не MAIN). Excel-решения и аудит будут писаться не туда.")
    if comparison_root and comparison_root_exists is False:
        dangerous = True
        warnings.append(f"comparison_root не существует: {comparison_root!r}")

    # ── warnings: ниже ожидаемого, но не катастрофа ──────────────────────────
    if not dangerous:
        if oc is not None and not objects_ok:
            warnings.append(
                f"objects_count={oc} < ожидаемых {min_objects} "
                "(213/214 Alia/272 Балчуг) — возможен неполный root.")
        if pc is not None and not projects_ok:
            warnings.append(
                f"projects_count={pc} < ожидаемых {min_projects} — возможен неполный root.")
    if oc is None:
        warnings.append("objects_count недоступен (endpoint не отдал данные).")
    if pc is None:
        warnings.append("projects_count недоступен (endpoint не отдал данные).")
    if not comparison_root:
        warnings.append("comparison_root неизвестен (нет в /api/info data_roots).")

    # Pipeline V2 артефакты известной пары
    if pipeline_v2_artifacts_present is False:
        warnings.append("Pipeline V2 артефакты эталонной пары отсутствуют в "
                        "active comparison root — возможен неверный root.")

    # base_dir != comparison_root parent → диагностический сигнал (не сам по себе
    # ошибка: разнесение код/данные легитимно, но стоит знать)
    drift_note = None
    if base_dir and comparison_root:
        try:
            bd = str(Path(base_dir).expanduser().resolve())
            cr_parent = str(Path(comparison_root).expanduser().resolve().parent)
            if bd != cr_parent:
                drift_note = (f"base_dir ({bd}) != parent(comparison_root) "
                              f"({cr_parent}) — код и данные в разных worktree "
                              "(ожидаемо для deploy-code + MAIN-data, но проверьте).")
        except OSError:
            pass

    if dangerous:
        status = STATUS_DANGEROUS
    elif warnings:
        status = STATUS_WARNING
    else:
        status = STATUS_OK

    return {
        "kind": HEALTH_KIND,
        "status": status,
        "dangerous": dangerous,
        "checks": {
            "objects_ok": objects_ok,
            "projects_ok": projects_ok,
            "comparison_root_exists": comparison_ok,
            "pipeline_v2_artifacts_present": (
                bool(pipeline_v2_artifacts_present)
                if pipeline_v2_artifacts_present is not None else None),
        },
        "objects_count": oc,
        "projects_count": pc,
        "comparison_root": comparison_root,
        "base_dir": base_dir,
        "thresholds": {"min_objects": min_objects, "min_projects": min_projects},
        "drift_note": drift_note,
        "warnings": warnings,
    }


__all__ = [
    "HEALTH_KIND", "MIN_OBJECTS", "MIN_PROJECTS", "DANGEROUS_OBJECTS_MAX",
    "STATUS_OK", "STATUS_WARNING", "STATUS_DANGEROUS",
    "evaluate_production_data_roots",
]
