"""Загрузка встроенного каталога эталонов без зависимости от workspace.

Каталог является частью pipeline-stage и должен одинаково работать в локальном
репозитории, контейнере и собранном backend-пакете. Исследовательская папка
``experiments`` не читается ни одним runtime-путём.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


CATALOG_SCHEMA_VERSION = 1
CATALOG_DIR = Path(__file__).resolve().parent
MANIFEST_PATH = CATALOG_DIR / "manifest.json"
RULES_PATH = CATALOG_DIR / "rules.json"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def load_catalog_manifest() -> dict[str, Any]:
    payload = _read_json(MANIFEST_PATH)
    if not isinstance(payload, dict):
        raise RuntimeError("Каталог эталонов: manifest должен быть объектом")
    if payload.get("schema_version") != CATALOG_SCHEMA_VERSION:
        raise RuntimeError("Каталог эталонов: несовместимая версия manifest")
    if not payload.get("catalog_version") or not isinstance(payload.get("disciplines"), dict):
        raise RuntimeError("Каталог эталонов: manifest заполнен не полностью")
    return payload


@lru_cache(maxsize=1)
def load_reference_rules() -> dict[str, Any]:
    payload = _read_json(RULES_PATH)
    if not isinstance(payload, dict) or payload.get("schema_version") != CATALOG_SCHEMA_VERSION:
        raise RuntimeError("Каталог эталонов: rules.json несовместим")
    return payload


@lru_cache(maxsize=1)
def load_reference_records() -> tuple[dict[str, Any], ...]:
    manifest = load_catalog_manifest()
    records: list[dict[str, Any]] = []
    for discipline, meta in sorted(manifest["disciplines"].items()):
        if not isinstance(meta, dict) or not meta.get("file"):
            raise RuntimeError(f"Каталог эталонов: не задан файл дисциплины {discipline}")
        path = CATALOG_DIR / str(meta["file"])
        payload = _read_json(path)
        rows = payload.get("records") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise RuntimeError(f"Каталог эталонов: {path.name} не содержит records")
        expected = int(meta.get("records") or 0)
        if expected != len(rows):
            raise RuntimeError(
                f"Каталог эталонов: {discipline}, ожидалось {expected}, найдено {len(rows)}"
            )
        for row in rows:
            if not isinstance(row, dict):
                raise RuntimeError(f"Каталог эталонов: повреждена запись {discipline}")
            if str(row.get("discipline") or "") != discipline:
                raise RuntimeError(f"Каталог эталонов: неверная дисциплина в {path.name}")
            if not row.get("block_id") or not row.get("profile_id"):
                raise RuntimeError(f"Каталог эталонов: запись без block_id/profile_id в {path.name}")
            records.append(row)
    expected_total = int(manifest.get("records_total") or 0)
    if expected_total != len(records):
        raise RuntimeError(
            f"Каталог эталонов: ожидалось {expected_total} записей, найдено {len(records)}"
        )
    return tuple(records)


def catalog_runtime_info() -> dict[str, Any]:
    """Компактные сведения, которые записываются в результат pipeline-stage."""
    manifest = load_catalog_manifest()
    rules = load_reference_rules()
    return {
        "catalog_version": manifest["catalog_version"],
        "schema_version": manifest["schema_version"],
        "records_total": manifest["records_total"],
        "profiles_total": manifest["profiles_total"],
        "disciplines_total": len(manifest["disciplines"]),
        "rules_version": rules.get("rules_version"),
        "runtime_source": "pipeline_stage_embedded_catalog",
    }
