"""Исходно-разрешительная документация объекта (раздел IRD).

Документы ИРД (ТЗ, вендор-лист, ТУ и прочее) загружаются в портал обычным
комплектом PDF + `*_results.md` в раздел `IRD`, но не аудируются (флаг
`"auditable": false` в `_registry.json`). Этапы анализа других разделов
читают их как справочник:

  * вендор-лист — текстом в задание оптимизации (`task_builder`);
  * весь перечень — каталогом «тип / название / путь к MD»: модель-агент
    открывает файл сама, только когда документ ей нужен.

Тип документа берётся из `project_info.ird_type`, а без него определяется по
названию. Всё fail-soft: недоступное хранилище или битые метаданные дают
пустой список — анализ идёт как без ИРД.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

IRD_CODE = "IRD"

IRD_TYPE_LABELS: dict[str, str] = {
    "tz": "Техническое задание",
    "vendor_list": "Вендор-лист",
    "tu": "Технические условия",
    "other": "Прочее",
}

# Порядок важен: вендор-лист у заказчиков — приложение к ТЗ, и «ТЗ» в его
# названии не делает его заданием.
_TYPE_PATTERNS: tuple[tuple[str, re.Pattern], ...] = (
    ("vendor_list", re.compile(r"вендор|vendor|рекомендуемых\s+производител", re.I)),
    ("tz", re.compile(r"(?i:техническ\w*\s+задани)|(?<![А-ЯЁA-Z])ТЗ(?![А-ЯЁA-Z])")),
    ("tu", re.compile(r"(?i:технически\w*\s+услови)|(?<![А-ЯЁA-Z])ТУ(?![А-ЯЁA-Z])")),
)


def detect_ird_type(*names: Optional[str]) -> str:
    """Тип документа ИРД по названию/коду/имени файла."""
    text = " ".join(str(n) for n in names if n)
    for ird_type, pattern in _TYPE_PATTERNS:
        if pattern.search(text):
            return ird_type
    return "other"


def _object_folder(adapter, object_id: Optional[str]) -> Optional[str]:
    if not object_id:
        from backend.app.services.common import object_service, project_service
        object_id = project_service._get_bound_object_id() or object_service.get_current_id()
    if not object_id:
        return None
    return next(
        (o["folder_name"] for o in adapter.list_objects() if o.get("object_id") == object_id),
        None,
    )


def list_ird_documents(object_id: Optional[str] = None) -> list[dict]:
    """Документы ИРД объекта (по умолчанию — связанного/текущего).

    Элемент: document_code, name, version_id, ird_type, ird_type_label,
    md_path (str или None, если MD в версии нет).
    """
    try:
        from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
        from backend.app.services.storage.projects_v2_source_resolver import (
            load_version_project_info,
            resolve_v2_source_files,
        )
        adapter = ProjectsV2Adapter()
        if not adapter.is_available():
            return []
        folder = _object_folder(adapter, object_id)
        if not folder:
            return []
        docs = adapter.list_documents(object_folder=folder, discipline=IRD_CODE)
    except Exception:
        return []

    result: list[dict] = []
    for doc in docs:
        code = doc.get("document_code") or ""
        vid = doc.get("current_version")
        if code.startswith("_") or not vid:
            continue
        try:
            version_dir = adapter.version_dir(Path(doc["doc_dir"]), vid)
            info = load_version_project_info(version_dir) or {}
            md_path = resolve_v2_source_files(version_dir, code).md_path
        except Exception:
            continue
        name = info.get("name") or code
        ird_type = info.get("ird_type")
        if ird_type not in IRD_TYPE_LABELS:
            ird_type = detect_ird_type(name, code, md_path.name if md_path else None)
        result.append({
            "document_code": code,
            "name": name,
            "version_id": vid,
            "ird_type": ird_type,
            "ird_type_label": IRD_TYPE_LABELS[ird_type],
            "md_path": str(md_path) if md_path else None,
        })
    return result


def find_vendor_list_md(object_id: Optional[str] = None) -> Optional[Path]:
    """MD вендор-листа из ИРД объекта; None — вендор-листа в ИРД нет."""
    for doc in list_ird_documents(object_id):
        if doc["ird_type"] == "vendor_list" and doc["md_path"]:
            return Path(doc["md_path"])
    return None


def render_ird_catalog(object_id: Optional[str] = None) -> str:
    """Каталог ИРД для задания модели; пустая строка — документов нет."""
    docs = [d for d in list_ird_documents(object_id) if d["md_path"]]
    if not docs:
        return ""
    lines = [
        "## Source documents of the object (ИРД)",
        "",
        "Customer's source documents (technical assignment, vendor list, "
        "technical conditions). They are NOT audited. Open a document's MD "
        "only when a decision depends on it (e.g. a requirement of the "
        "technical assignment); cite it as «<type>, <name>, п. N».",
        "",
        "| Type | Document | MD path |",
        "|---|---|---|",
    ]
    for d in docs:
        lines.append(f"| {d['ird_type_label']} | {d['name']} | `{d['md_path']}` |")
    return "\n".join(lines)
