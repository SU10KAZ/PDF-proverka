"""
storage_read_facade.py — подготовительный фасад выбора backend хранилища.

Инкапсулирует выбор источника чтения документа:

  * `legacy`            — текущее production-хранилище `projects/` (ДЕФОЛТ);
  * `projects_v2`       — новое хранилище (через ProjectsV2Adapter);
  * `dual_read_shadow`  — параллельное чтение legacy+v2 + сравнение (DualReadService).

ВАЖНО (подготовительный этап, НЕ cutover):
  * по умолчанию режим всегда `legacy`;
  * фасад НЕ подключён к существующим production endpoints — он лишь готовит
    класс/функции. Создание/использование фасада НЕ меняет поведение backend/UI;
  * в режиме `legacy` фасад НЕ читает `projects_v2` и НЕ реализует legacy-чтение
    заново — он лишь сообщает, что обслуживание остаётся за существующими
    сервисами (`project_service` и т.д.);
  * в режиме `projects_v2` чтение идёт через adapter (read-only), но это
    используется ТОЛЬКО при явно выставленном флаге и нигде не вызывается из
    production;
  * в режиме `dual_read_shadow` — только тесты/CLI/shadow API.

Режим читается из env `AUDIT_STORAGE_BACKEND` (тот же флаг, что у adapter).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from backend.app.services.storage.projects_v2_adapter import (
    ProjectsV2Adapter, get_storage_backend,
)

MODE_LEGACY = "legacy"
MODE_V2 = "projects_v2"
MODE_DUAL_READ_SHADOW = "dual_read_shadow"
_VALID_MODES = {MODE_LEGACY, MODE_V2, MODE_DUAL_READ_SHADOW}

_MODE_ENV = "AUDIT_STORAGE_BACKEND"


def get_storage_mode() -> str:
    """Текущий режим чтения. Default `legacy`.

    `legacy` (default) | `projects_v2` | `dual_read_shadow`. Любое неизвестное
    значение трактуется как `legacy` (безопасный дефолт). Совместимо с
    `projects_v2_adapter.get_storage_backend()` (legacy / projects_v2); значение
    `dual_read_shadow` распознаётся дополнительно здесь.
    """
    val = (os.environ.get(_MODE_ENV) or "").strip().lower()
    return val if val in _VALID_MODES else MODE_LEGACY


class StorageReadFacade:
    """Фасад чтения документа из выбранного backend. НЕ подключён к production."""

    def __init__(self, v2_root: Optional[Path] = None, mode: Optional[str] = None):
        # mode можно зафиксировать явно (для тестов); иначе читается из env на вызов
        self._fixed_mode = mode if mode in _VALID_MODES else None
        self._v2_root = v2_root

    # -- mode --
    @property
    def mode(self) -> str:
        return self._fixed_mode or get_storage_mode()

    def is_legacy(self) -> bool:
        return self.mode == MODE_LEGACY

    def is_v2(self) -> bool:
        return self.mode == MODE_V2

    def is_dual_read_shadow(self) -> bool:
        return self.mode == MODE_DUAL_READ_SHADOW

    def uses_projects_v2(self) -> bool:
        """True только если режим явно НЕ legacy (т.е. v2 или dual_read_shadow)."""
        return self.mode in (MODE_V2, MODE_DUAL_READ_SHADOW)

    # -- read --
    def document_snapshot(self, document_code: str,
                          object_id: Optional[str] = None) -> dict:
        """Возвращает снимок документа согласно режиму. READ-ONLY.

        ВАЖНО: в режиме `legacy` фасад НЕ читает legacy сам и НЕ трогает
        production — он сообщает, что обслуживание остаётся за существующими
        сервисами. Это гарантирует, что подключение фасада (когда оно будет)
        в legacy-режиме ничего не меняет.
        """
        mode = self.mode
        if mode == MODE_LEGACY:
            return {
                "backend": MODE_LEGACY,
                "document_code": document_code,
                "handled_by": "existing_legacy_services",  # project_service и т.д.
                "v2_used": False,
                "note": "facade no-op в legacy: production-путь не изменён",
            }
        if mode == MODE_V2:
            adapter = ProjectsV2Adapter(self._v2_root)
            doc = adapter.find_document(document_code, object_id=object_id)
            if doc is None:
                return {"backend": MODE_V2, "document_code": document_code,
                        "found": False, "v2_used": True}
            snap = adapter.document_snapshot(doc["object_folder"], doc["discipline"],
                                             doc["document_code"])
            return {"backend": MODE_V2, "found": True, "v2_used": True, "snapshot": snap}
        # dual_read_shadow — только тесты/CLI/shadow API
        from backend.app.services.storage.projects_v2_dual_read import DualReadService
        result = DualReadService(self._v2_root).compare_document(document_code, object_id=object_id)
        return {"backend": MODE_DUAL_READ_SHADOW, "v2_used": True,
                "dual_read": result}


def production_uses_v2() -> bool:
    """Хелпер для диагностики: использует ли production v2-чтение СЕЙЧАС.

    Всегда False на этом этапе: ни один production read-path не подключён к
    фасаду/adapter; даже при `AUDIT_STORAGE_BACKEND=projects_v2` существующие
    endpoints продолжают читать legacy. Этот флаг станет значимым только после
    отдельного этапа подключения (см. docs/projects_v2_migration_plan.md).
    """
    return False
