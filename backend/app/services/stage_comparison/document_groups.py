"""Группы «один документ слева → несколько документов справа».

Один проект стадии П часто раскладывается в РД на несколько проектов
(например, по корпусам). Сравнивать такой проект с каждым корпусом отдельно
нельзя: листы остальных корпусов выглядели бы «удалёнными» в каждой паре.
Поэтому правые документы группы склеиваются в одну сборку
(``project_assemblies``), сборка прикрепляется к ``stage_2`` и сравнивается
с левым документом как обычная пара один к одному.

Реестр групп хранится рядом с данными сравнения объекта::

    projects_v2/objects/<object>/comparison/document_groups.json

Ключи — коды документов (``document_code``), а не пути PDF: сессия
сравнения пересоздаётся при каждой загрузке, а код документа переживает
новые версии. Группа существует, только пока в ней не меньше двух правых
документов; один документ — это обычная пара без сборки.
"""
from __future__ import annotations

import json
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from backend.app.services.common.atomic_json import atomic_write_json
from backend.app.services.project_assemblies import service as assemblies

from . import stage_upload

SCHEMA = "stage_document_groups/1"
LEFT_STAGE = "stage_1"
RIGHT_STAGE = "stage_2"
MIN_MEMBERS = 2
BUILDING_STATUSES = {"QUEUED", "PREPARING", "RUNNING"}

# Сборка запускается в отдельном процессе, но подготовка версии (хеши и копии
# исходных файлов) и правка реестра должны идти по одной на процесс бэкенда.
_lock = threading.Lock()


class DocumentGroupError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _comparison_dir(object_id: str) -> Path:
    _obj, comparison = stage_upload.resolve_object_dir(object_id, create=False)
    return comparison


def _registry_path(object_id: str) -> Path:
    return _comparison_dir(object_id) / "document_groups.json"


def _read(object_id: str) -> dict[str, Any]:
    path = _registry_path(object_id)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        data = {}
    except (OSError, ValueError) as exc:
        raise DocumentGroupError("GROUPS_CORRUPT", "Повреждён реестр групп документов") from exc
    if not isinstance(data, dict) or not isinstance(data.get("groups"), list):
        data = {"schema": SCHEMA, "object_id": object_id, "groups": []}
    return data


def _write(object_id: str, data: dict[str, Any]) -> None:
    data["schema"] = SCHEMA
    data["object_id"] = object_id
    data["updated_at"] = _now()
    atomic_write_json(_registry_path(object_id), data)


def _current_sources(object_id: str, stage: str) -> dict[str, dict[str, Any]]:
    """Текущие версии документов стадии по коду: ссылка источника сборки и готовность."""
    rows = assemblies.list_sources(object_id)["items"]
    return {
        row["document_code"]: row
        for row in rows
        if row.get("kind") == "comparison" and row.get("stage") == stage and row.get("current")
    }


def _unique(codes: Iterable[Any]) -> list[str]:
    result: list[str] = []
    for code in codes:
        text = str(code or "").strip()
        if text and text not in result:
            result.append(text)
    return result


_CODE_TOKEN = re.compile(r"\d+|[^\W\d_]+")


def _natural_key(code: str) -> tuple:
    """ОВ3-1 < ОВ3-2 < … < ОВ3-10; разделители («-», «_», «.») не сравниваются.

    Иначе «СТ26_01-14-ОВ3-4» и «СТ26-01-14-ОВ3-1» упорядочил бы символ-разделитель
    в префиксе, а не номер секции. Числа идут раньше букв (ВК-7 < ВК-АС).
    """
    tokens = tuple(
        (0, int(token), "") if token.isdigit() else (1, 0, token.casefold())
        for token in _CODE_TOKEN.findall(code)
    )
    return tokens, code


def _ordered(codes: Iterable[str]) -> list[str]:
    return sorted(_unique(codes), key=_natural_key)


def _assembly_state(object_id: str, group: dict[str, Any]) -> dict[str, Any]:
    assembly_id = str(group.get("assembly_id") or "")
    version_id = str(group.get("assembly_version_id") or "")
    if not assembly_id or not version_id:
        return {}
    try:
        return assemblies.get_assembly(object_id, assembly_id, version_id)
    except assemblies.AssemblyError as exc:
        return {"status": "FAILED", "error": str(exc)}


def _assembly_member_versions(state: dict[str, Any]) -> list[tuple[str, str]]:
    """Участники собранной версии В ПОРЯДКЕ склейки: порядок — часть состава."""
    return [
        (str(source.get("document_code")), str(source.get("version_id")))
        for source in state.get("sources") or []
        if isinstance(source, dict)
    ]


def _view(object_id: str, group: dict[str, Any], right: dict[str, dict[str, Any]]) -> dict[str, Any]:
    state = _assembly_state(object_id, group)
    status = str(state.get("status") or ("NOT_BUILT" if len(group["members"]) >= MIN_MEMBERS else "SINGLE"))
    built_versions = _assembly_member_versions(state)
    current_versions = {
        code: str(right[code]["version_id"]) for code in group["members"] if code in right
    }
    # Нужна пересборка, если после сборки загрузили новую версию участника,
    # участник пропал со стороны stage_2 или сборка склеена не в порядке кодов.
    expected = [(code, current_versions.get(code)) for code in _ordered(group["members"])]
    stale = bool(state) and built_versions != expected
    return {
        "group_id": group["group_id"],
        "left_document_code": group["left_document_code"],
        "members": [
            {
                "document_code": code,
                "version_id": current_versions.get(code),
                "present": code in right,
            }
            for code in _ordered(group["members"])
        ],
        "assembly_id": group.get("assembly_id") or None,
        "assembly_version_id": group.get("assembly_version_id") or None,
        "attached_version_id": group.get("attached_version_id") or None,
        "status": status,
        "stale": stale,
        "error": state.get("error") if status not in {"READY", *BUILDING_STATUSES} else None,
        "page_count": (state.get("totals") or {}).get("page_count"),
        "updated_at": group.get("updated_at"),
    }


def _attach_ready(object_id: str, group: dict[str, Any]) -> bool:
    """Прикрепить готовую версию сборки вместо прежней. True — реестр изменился."""
    version_id = str(group.get("assembly_version_id") or "")
    attached = str(group.get("attached_version_id") or "")
    if not version_id or attached == version_id:
        return False
    if _assembly_state(object_id, group).get("status") != "READY":
        return False
    assemblies.attach(object_id, group["assembly_id"], version_id, RIGHT_STAGE)
    if attached:
        assemblies.detach(object_id, group["assembly_id"], attached, RIGHT_STAGE)
    group["attached_version_id"] = version_id
    return True


def _detach_all(object_id: str, group: dict[str, Any]) -> None:
    attached = str(group.get("attached_version_id") or "")
    if group.get("assembly_id") and attached:
        assemblies.detach(object_id, group["assembly_id"], attached, RIGHT_STAGE)
    group["attached_version_id"] = None


def _require_enabled() -> None:
    if not assemblies.enabled():
        raise DocumentGroupError(
            "ASSEMBLIES_DISABLED",
            "Сборки выключены (PROJECT_ASSEMBLIES_ENABLED): группы документов недоступны",
        )


def list_groups(object_id: str) -> dict[str, Any]:
    """Группы объекта с состоянием сборки; готовую сборку сразу прикрепляет."""
    _require_enabled()
    with _lock:
        data = _read(object_id)
        changed = False
        for group in data["groups"]:
            changed = _attach_ready(object_id, group) or changed
        if changed:
            _write(object_id, data)
        right = _current_sources(object_id, RIGHT_STAGE)
        return {
            "schema": SCHEMA,
            "object_id": object_id,
            "groups": [_view(object_id, group, right) for group in data["groups"]],
        }


def _left_discipline(object_id: str, left_code: str) -> str:
    row = _current_sources(object_id, LEFT_STAGE).get(left_code) or {}
    return str(row.get("discipline") or "OTHER")


def _launch_build(object_id: str, group: dict[str, Any], right: dict[str, dict[str, Any]]) -> None:
    missing = [code for code in group["members"] if code not in right]
    if missing:
        raise DocumentGroupError(
            "MEMBER_NOT_FOUND",
            "Справа нет документов: " + ", ".join(missing),
        )
    group["members"] = _ordered(group["members"])
    refs = [right[code]["source_ref"] for code in group["members"]]
    try:
        prepared = assemblies.create_assembly(
            object_id=object_id,
            name=f"{group['left_document_code']} · сборка из {len(refs)}",
            section=_left_discipline(object_id, group["left_document_code"]),
            source_refs=refs,
            author="document-group",
            assembly_id=group.get("assembly_id") or None,
            build=False,
        )
    except assemblies.AssemblyError as exc:
        raise DocumentGroupError(exc.code, str(exc)) from exc
    group["assembly_id"] = prepared["assembly_id"]
    group["assembly_version_id"] = prepared["version_id"]


def add_members(object_id: str, left_document_code: str, member_codes: list[str]) -> dict[str, Any]:
    """Добавить правые документы к левому. Два и больше — собирается сборка."""
    _require_enabled()
    left_code = str(left_document_code or "").strip()
    if not left_code:
        raise DocumentGroupError("LEFT_REQUIRED", "Не указан документ слева")
    codes = _unique(member_codes)
    if not codes:
        raise DocumentGroupError("MEMBERS_REQUIRED", "Не указаны документы справа")
    with _lock:
        if left_code not in _current_sources(object_id, LEFT_STAGE):
            raise DocumentGroupError("LEFT_NOT_FOUND", f"Слева нет документа {left_code}")
        right = _current_sources(object_id, RIGHT_STAGE)
        data = _read(object_id)
        groups = data["groups"]
        # Правый документ принадлежит не более чем одной группе.
        for other in groups:
            if other["left_document_code"] != left_code:
                other["members"] = [code for code in other["members"] if code not in codes]
        group = next((g for g in groups if g["left_document_code"] == left_code), None)
        if group is None:
            group = {
                "group_id": "grp_" + uuid.uuid4().hex[:16],
                "left_document_code": left_code,
                "members": [],
                "created_at": _now(),
            }
            groups.append(group)
        before = list(group["members"])
        group["members"] = _ordered([*group["members"], *codes])
        group["updated_at"] = _now()
        dissolved = _drop_small_groups(object_id, groups)
        if len(group["members"]) >= MIN_MEMBERS and (
            group["members"] != before or not group.get("assembly_version_id")
            or _view(object_id, group, right)["stale"]
        ):
            _launch_build(object_id, group, right)
        _write(object_id, data)
        return {
            "group": _view(object_id, group, right) if group in groups else None,
            "released": dissolved,
        }


def _drop_small_groups(object_id: str, groups: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Убрать группы меньше двух документов; вернуть освободившиеся пары."""
    released: list[dict[str, str]] = []
    for group in list(groups):
        if len(group["members"]) >= MIN_MEMBERS:
            continue
        _detach_all(object_id, group)
        for code in group["members"]:
            released.append({"left_document_code": group["left_document_code"], "document_code": code})
        groups.remove(group)
    return released


def remove_member(object_id: str, group_id: str, document_code: str) -> dict[str, Any]:
    """Вернуть правый документ из группы в общий список."""
    _require_enabled()
    with _lock:
        data = _read(object_id)
        group = next((g for g in data["groups"] if g["group_id"] == group_id), None)
        if group is None:
            raise DocumentGroupError("GROUP_NOT_FOUND", "Группа не найдена")
        if document_code not in group["members"]:
            raise DocumentGroupError("MEMBER_NOT_FOUND", f"В группе нет документа {document_code}")
        group["members"] = [code for code in group["members"] if code != document_code]
        group["updated_at"] = _now()
        released = _drop_small_groups(object_id, data["groups"])
        right = _current_sources(object_id, RIGHT_STAGE)
        if group in data["groups"]:
            _launch_build(object_id, group, right)
        _write(object_id, data)
        return {
            "group": _view(object_id, group, right) if group in data["groups"] else None,
            "released": released,
        }


def rebuild(object_id: str, group_id: str) -> dict[str, Any]:
    """Собрать группу заново (после ошибки или новой версии участника)."""
    _require_enabled()
    with _lock:
        data = _read(object_id)
        group = next((g for g in data["groups"] if g["group_id"] == group_id), None)
        if group is None:
            raise DocumentGroupError("GROUP_NOT_FOUND", "Группа не найдена")
        right = _current_sources(object_id, RIGHT_STAGE)
        if _view(object_id, group, right)["status"] in BUILDING_STATUSES:
            raise DocumentGroupError("GROUP_BUSY", "Сборка группы уже выполняется")
        _launch_build(object_id, group, right)
        group["updated_at"] = _now()
        _write(object_id, data)
        return {"group": _view(object_id, group, right), "released": []}


def hidden_right_documents(stage_b_path: str | Path | None, documents: list[dict[str, Any]]) -> set[str]:
    """PDF правой стороны, которые живут внутри групп и не стоят в раскладке пар.

    Это участники групп и все версии сборок групп: в раскладке их заменяет
    сама группа. Сборки, созданные вручную на вкладке «Сборки», не скрываются.
    """
    if not stage_b_path or not assemblies.enabled():
        return set()
    object_id = assemblies.object_id_for_stage_path(stage_b_path)
    if not object_id:
        return set()
    try:
        groups = _read(object_id)["groups"]
    except (DocumentGroupError, stage_upload.StageUploadError):
        return set()
    member_codes = {code for group in groups for code in group.get("members") or []}
    assembly_ids = {group.get("assembly_id") for group in groups if group.get("assembly_id")}
    hidden: set[str] = set()
    for document in documents:
        ref = document.get("assembly_ref")
        if isinstance(ref, dict):
            if ref.get("assembly_id") in assembly_ids:
                hidden.add(str(document.get("pdf_path")))
        elif document.get("document_code") in member_codes:
            hidden.add(str(document.get("pdf_path")))
    return hidden


def grouped_left_codes(stage_a_path: str | Path | None) -> set[str]:
    if not stage_a_path or not assemblies.enabled():
        return set()
    object_id = assemblies.object_id_for_stage_path(stage_a_path)
    if not object_id:
        return set()
    try:
        return {group["left_document_code"] for group in _read(object_id)["groups"]}
    except (DocumentGroupError, stage_upload.StageUploadError):
        return set()


__all__ = [
    "DocumentGroupError",
    "add_members",
    "grouped_left_codes",
    "hidden_right_documents",
    "list_groups",
    "rebuild",
    "remove_member",
]
