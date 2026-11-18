"""Группы документов на доске пар: несколько документов на одной или обеих сторонах.

Один проект стадии П часто раскладывается в РД на несколько проектов
(например, по корпусам), и наоборот: несколько проектов П сводятся в один
проект РД. Сравнивать такие документы попарно нельзя: листы остальных
проектов выглядели бы «удалёнными» в каждой паре. Поэтому документы одной
стороны группы склеиваются в сборку (``project_assemblies``), сборка
прикрепляется к своей стадии (слева — ``stage_1``, справа — ``stage_2``) и
сравнивается с другой стороной как обычная пара один к одному. Допустимы все
сочетания: 1 → N, N → 1 и N ↔ M (сборка против сборки).

Реестр групп хранится рядом с данными сравнения объекта — в рабочей папке
своей версии сравнения (``comparison_versions``)::

    projects_v2/objects/<object>/comparison/document_groups.json            ← Версия 1
    projects_v2/objects/<object>/comparison/versions/cvNNN/document_groups.json

Функции принимают ``object_id`` или область версии ``<object_id>@cvNNN``; она
передаётся в сборки как есть.

Ключи — коды документов (``document_code``), а не пути PDF: сессия
сравнения пересоздаётся при каждой загрузке, а код документа переживает
новые версии. Одна группа — одна строка доски пар. Строка привязана к
раскладке пар через якорь (``anchor``) — документ, вокруг которого группа
возникла; остальные участники и сборки группы в раскладке не стоят.

Схема ``stage_document_groups/2``: у группы стороны ``left``/``right`` с
участниками и своей сборкой. Записи схемы 1 (``left_document_code`` +
правые ``members``) читаются как группа «один слева → несколько справа»;
при записи группа, представимая в схеме 1 (слева один документ), дублирует
её поля, чтобы откат на прежний релиз не ломал доску пар.
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

from . import comparison_versions
from . import stage_upload

SCHEMA = "stage_document_groups/2"
SIDES = ("left", "right")
SIDE_STAGE = {"left": "stage_1", "right": "stage_2"}
OTHER_SIDE = {"left": "right", "right": "left"}
SIDE_LABEL = {"left": "слева", "right": "справа"}
MIN_MEMBERS = 2
BUILDING_STATUSES = {"QUEUED", "PREPARING", "RUNNING"}
_SIDE_FIELDS = ("assembly_id", "assembly_version_id", "attached_version_id")
# Худший статус стороны определяет статус группы.
_STATUS_RANK = ("FAILED", "RUNNING", "PREPARING", "QUEUED", "NOT_BUILT", "READY", "SINGLE")

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


def _plain_object_id(object_id: str) -> str:
    """Объект реестра без версии сравнения — для полей JSON и ответов."""
    return comparison_versions.split_scope(object_id)[0]


def _check_side(side: str) -> str:
    if side not in SIDES:
        raise DocumentGroupError("BAD_SIDE", "Сторона группы должна быть left или right")
    return side


def _empty_side(members: Iterable[Any] = ()) -> dict[str, Any]:
    return {"members": _unique(members), "manual_order": False,
            "assembly_id": None, "assembly_version_id": None, "attached_version_id": None}


def _normalize(record: dict[str, Any]) -> dict[str, Any]:
    """Запись реестра → группа схемы 2 (запись схемы 1 — «один слева → N справа»)."""
    group = {key: record[key] for key in ("group_id", "independent", "created_at", "updated_at") if key in record}
    if isinstance(record.get("left"), dict) and isinstance(record.get("right"), dict):
        for side in SIDES:
            raw = record[side]
            data = _empty_side(raw.get("members") or [])
            data["manual_order"] = bool(raw.get("manual_order"))
            for key in _SIDE_FIELDS:
                data[key] = raw.get(key) or None
            group[side] = data
        anchor = record.get("anchor") if isinstance(record.get("anchor"), dict) else {}
        group["anchor"] = {"side": anchor.get("side"), "document_code": anchor.get("document_code")}
    else:
        left_code = str(record.get("left_document_code") or "").strip()
        group["left"] = _empty_side([left_code])
        right = _empty_side(record.get("members") or [])
        right["manual_order"] = bool(record.get("manual_order"))
        for key in _SIDE_FIELDS:
            right[key] = record.get(key) or None
        group["right"] = right
        group["anchor"] = {"side": "left", "document_code": left_code}
    _fix_anchor(group)
    return group


def _serialize(group: dict[str, Any]) -> dict[str, Any]:
    record = {
        "group_id": group["group_id"],
        "anchor": dict(group["anchor"]),
        "left": dict(group["left"]),
        "right": dict(group["right"]),
        "created_at": group.get("created_at"),
        "updated_at": group.get("updated_at"),
    }
    if group.get("independent"):
        record["independent"] = True
    # Поля схемы 1 для отката. Группу с несколькими документами слева схема 1
    # выразить не может: прежний релиз увидит её пустой и не тронет правую сторону.
    left = group["left"]["members"]
    representable = len(left) == 1
    record["left_document_code"] = left[0] if left else None
    record["members"] = list(group["right"]["members"]) if representable else []
    if representable:
        record["manual_order"] = bool(group["right"].get("manual_order"))
        for key in _SIDE_FIELDS:
            record[key] = group["right"].get(key)
    return record


def _read(object_id: str) -> dict[str, Any]:
    path = _registry_path(object_id)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        data = {}
    except (OSError, ValueError) as exc:
        raise DocumentGroupError("GROUPS_CORRUPT", "Повреждён реестр групп документов") from exc
    if not isinstance(data, dict) or not isinstance(data.get("groups"), list):
        data = {"schema": SCHEMA, "object_id": _plain_object_id(object_id), "groups": []}
    data["groups"] = [_normalize(record) for record in data["groups"] if isinstance(record, dict)]
    return data


def _write(object_id: str, data: dict[str, Any]) -> None:
    payload = dict(data)
    payload["schema"] = SCHEMA
    payload["object_id"] = _plain_object_id(object_id)
    payload["updated_at"] = _now()
    payload["groups"] = [_serialize(group) for group in data["groups"]]
    atomic_write_json(_registry_path(object_id), payload)


def _current_sources(object_id: str) -> dict[str, dict[str, dict[str, Any]]]:
    """Текущие версии документов обеих сторон по коду: ссылка источника сборки и готовность."""
    rows = assemblies.list_sources(object_id)["items"]
    result: dict[str, dict[str, dict[str, Any]]] = {side: {} for side in SIDES}
    for row in rows:
        if row.get("kind") != "comparison" or not row.get("current"):
            continue
        for side, stage in SIDE_STAGE.items():
            if row.get("stage") == stage:
                result[side][row["document_code"]] = row
    return result


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


def _member_order(side_data: dict[str, Any]) -> list[str]:
    members = side_data["members"]
    return _unique(members) if side_data.get("manual_order") else _ordered(members)


def _needs_assembly(group: dict[str, Any], side: str) -> bool:
    """Сторона склеивается, если на ней два документа и больше.

    Независимое сравнение («Копировать сравнение») собирает правую сторону даже
    из одного документа: иначе оно совпало бы с основной парой.
    """
    count = len(group[side]["members"])
    if side == "right" and group.get("independent"):
        return count >= 1
    return count >= MIN_MEMBERS


def _fix_anchor(group: dict[str, Any]) -> None:
    """Якорь — участник группы; выбывший якорь заменяет первый участник той же стороны."""
    anchor = group.get("anchor") or {}
    side = anchor.get("side") if anchor.get("side") in SIDES else "left"
    if anchor.get("document_code") in group[side]["members"]:
        group["anchor"] = {"side": side, "document_code": anchor["document_code"]}
        return
    for candidate in (side, OTHER_SIDE[side]):
        members = _member_order(group[candidate])
        if members:
            group["anchor"] = {"side": candidate, "document_code": members[0]}
            return
    group["anchor"] = {"side": side, "document_code": None}


def _side_label(group: dict[str, Any], side: str) -> str:
    """Код, которым сторона подписывает строку: единственный документ или якорь."""
    members = _member_order(group[side])
    if len(members) == 1:
        return members[0]
    anchor = group["anchor"]
    if anchor["side"] == side and anchor["document_code"] in members:
        return anchor["document_code"]
    return members[0] if members else ""


def _assembly_state(object_id: str, side_data: dict[str, Any]) -> dict[str, Any]:
    assembly_id = str(side_data.get("assembly_id") or "")
    version_id = str(side_data.get("assembly_version_id") or "")
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


def _side_view(object_id: str, group: dict[str, Any], side: str,
               sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    data = group[side]
    assembled = _needs_assembly(group, side)
    state = _assembly_state(object_id, data) if assembled else {}
    status = str(state.get("status") or ("NOT_BUILT" if assembled else "SINGLE"))
    built_versions = _assembly_member_versions(state)
    current_versions = {code: str(sources[code]["version_id"]) for code in data["members"] if code in sources}
    # Нужна пересборка, если после сборки загрузили новую версию участника,
    # участник пропал со своей стороны или изменён порядок склейки.
    expected = [(code, current_versions.get(code)) for code in _member_order(data)]
    return {
        "members": [
            {"document_code": code, "version_id": current_versions.get(code), "present": code in sources}
            for code in _member_order(data)
        ],
        "assembled": assembled,
        "manual_order": bool(data.get("manual_order")),
        # id сохраняется и у стороны без сборки: её прежние версии скрываются с доски.
        "assembly_id": data.get("assembly_id"),
        "assembly_version_id": data.get("assembly_version_id") if assembled else None,
        "attached_version_id": data.get("attached_version_id") if assembled else None,
        "status": status,
        "stale": bool(state) and built_versions != expected,
        "error": state.get("error") if status not in {"READY", "SINGLE", *BUILDING_STATUSES} else None,
        "page_count": (state.get("totals") or {}).get("page_count"),
    }


def _status_rank(status: str) -> int:
    return _STATUS_RANK.index(status) if status in _STATUS_RANK else -1


def _view(object_id: str, group: dict[str, Any], sources: dict[str, dict[str, dict[str, Any]]]) -> dict[str, Any]:
    sides = {side: _side_view(object_id, group, side, sources[side]) for side in SIDES}
    worst = min((sides[side]["status"] for side in SIDES), key=_status_rank)
    right = sides["right"]
    return {
        "group_id": group["group_id"],
        "independent": bool(group.get("independent")),
        "anchor": dict(group["anchor"]),
        "left": sides["left"],
        "right": sides["right"],
        "status": worst,
        "stale": any(sides[side]["stale"] for side in SIDES),
        "error": next((sides[side]["error"] for side in SIDES if sides[side]["error"]), None),
        # Совместимые поля схемы 1: правая сторона и левый документ строки.
        "left_document_code": _side_label(group, "left") or None,
        "right_document_code": _side_label(group, "right") or None,
        "members": right["members"],
        "assembly_id": right["assembly_id"],
        "assembly_version_id": right["assembly_version_id"],
        "attached_version_id": right["attached_version_id"],
        "page_count": right["page_count"],
        "updated_at": group.get("updated_at"),
    }


def _attach_ready(object_id: str, group: dict[str, Any], side: str) -> bool:
    """Прикрепить готовую версию сборки стороны вместо прежней. True — реестр изменился."""
    data = group[side]
    version_id = str(data.get("assembly_version_id") or "")
    attached = str(data.get("attached_version_id") or "")
    if not version_id or attached == version_id:
        return False
    if _assembly_state(object_id, data).get("status") != "READY":
        return False
    assemblies.attach(object_id, data["assembly_id"], version_id, SIDE_STAGE[side])
    if attached:
        assemblies.detach(object_id, data["assembly_id"], attached, SIDE_STAGE[side])
    data["attached_version_id"] = version_id
    return True


def _detach_side(object_id: str, group: dict[str, Any], side: str) -> None:
    data = group[side]
    attached = str(data.get("attached_version_id") or "")
    if data.get("assembly_id") and attached:
        assemblies.detach(object_id, data["assembly_id"], attached, SIDE_STAGE[side])
    data["attached_version_id"] = None


def _release_side_assembly(object_id: str, group: dict[str, Any], side: str) -> None:
    """Сторона снова из одного документа: сборку отцепить, id сохранить на случай роста."""
    _detach_side(object_id, group, side)
    group[side]["assembly_version_id"] = None


def _require_enabled() -> None:
    if not assemblies.enabled():
        raise DocumentGroupError(
            "ASSEMBLIES_DISABLED",
            "Сборки выключены (PROJECT_ASSEMBLIES_ENABLED): группы документов недоступны",
        )


def list_groups(object_id: str, *, attach: bool = True) -> dict[str, Any]:
    """Группы объекта с состоянием сборок; готовые сборки сразу прикрепляет.

    ``attach=False`` — просмотр (viewer): ничего не прикрепляет и не пишет.
    """
    _require_enabled()
    with _lock:
        data = _read(object_id)
        changed = False
        for group in data["groups"] if attach else ():
            for side in SIDES:
                changed = _attach_ready(object_id, group, side) or changed
        if changed:
            _write(object_id, data)
        sources = _current_sources(object_id)
        return {
            "schema": SCHEMA,
            "object_id": _plain_object_id(object_id),
            "groups": [_view(object_id, group, sources) for group in data["groups"]],
        }


def _section(group: dict[str, Any], sources: dict[str, dict[str, dict[str, Any]]]) -> str:
    """Раздел сборки — раздел первого найденного документа строки (сначала слева)."""
    for side in SIDES:
        for code in _member_order(group[side]):
            row = sources[side].get(code)
            if row and row.get("discipline"):
                return str(row["discipline"])
    return "OTHER"


def _launch_build(object_id: str, group: dict[str, Any], side: str,
                  sources: dict[str, dict[str, dict[str, Any]]]) -> None:
    data = group[side]
    available = sources[side]
    missing = [code for code in data["members"] if code not in available]
    if missing:
        raise DocumentGroupError(
            "MEMBER_NOT_FOUND",
            f"{SIDE_LABEL[side].capitalize()} нет документов: " + ", ".join(missing),
        )
    data["members"] = _member_order(data)
    refs = [available[code]["source_ref"] for code in data["members"]]
    try:
        prepared = assemblies.create_assembly(
            object_id=object_id,
            # Сборку называет документ противоположной стороны строки: правая
            # сборка «ИОС2.1 · сборка из 9» — как до появления левых групп.
            # Напротив пока пусто — ведущий документ своей стороны.
            name=f"{_side_label(group, OTHER_SIDE[side]) or _side_label(group, side)} · сборка из {len(refs)}",
            section=_section(group, sources),
            source_refs=refs,
            author="document-group",
            assembly_id=data.get("assembly_id") or None,
            build=False,
        )
    except assemblies.AssemblyError as exc:
        raise DocumentGroupError(exc.code, str(exc)) from exc
    data["assembly_id"] = prepared["assembly_id"]
    data["assembly_version_id"] = prepared["version_id"]


def _ensure_side(object_id: str, group: dict[str, Any], side: str,
                 sources: dict[str, dict[str, dict[str, Any]]], *, force: bool = False) -> None:
    """Собрать сторону, если её состав изменился или сборки ещё нет."""
    if not _needs_assembly(group, side):
        _release_side_assembly(object_id, group, side)
        return
    if force or not group[side].get("assembly_version_id") \
            or _side_view(object_id, group, side, sources[side])["stale"]:
        _launch_build(object_id, group, side, sources)


def _settle(object_id: str, groups: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Распустить выродившиеся группы; вернуть освободившиеся пары.

    Обычная группа живёт, пока хотя бы на одной стороне два документа.
    Противоположная сторона может быть пустой: проекты одной стадии можно
    объединить до загрузки другой (18.11.2026), документ напротив встанет в
    группу позже. Распавшаяся группа «один к одному» возвращает пару
    ``{left_document_code, right_document_code, anchor_side}``, чтобы она встала
    обычной строкой на месте якоря.
    """
    released: list[dict[str, str]] = []
    for group in list(groups):
        left, right = (_member_order(group[side]) for side in SIDES)
        if group.get("independent"):
            alive = bool(left) and bool(right)
        else:
            alive = len(left) >= MIN_MEMBERS or len(right) >= MIN_MEMBERS
        if alive:
            for side in SIDES:
                if not _needs_assembly(group, side):
                    _release_side_assembly(object_id, group, side)
            _fix_anchor(group)
            continue
        for side in SIDES:
            _detach_side(object_id, group, side)
        if len(left) == 1 and len(right) == 1 and not group.get("independent"):
            released.append({"left_document_code": left[0], "right_document_code": right[0],
                             "anchor_side": group["anchor"]["side"]})
        groups.remove(group)
    return released


def _find_group(groups: list[dict[str, Any]], group_id: str) -> dict[str, Any]:
    group = next((g for g in groups if g["group_id"] == group_id), None)
    if group is None:
        raise DocumentGroupError("GROUP_NOT_FOUND", "Группа не найдена")
    return group


def add_members(
    object_id: str, row_document_code: str | None, member_codes: list[str],
    *, side: str = "right", new_group: bool = False, group_id: str | None = None,
    anchor_document_code: str | None = None,
) -> dict[str, Any]:
    """Добавить документы на сторону ``side`` строки.

    ``row_document_code`` — документ противоположной стороны той же строки
    (для правой стороны — левый документ, для левой — правый). Два документа и
    больше на стороне склеиваются в сборку.

    Напротив может быть пусто (другая стадия ещё не загружена): тогда строку
    задаёт ``group_id`` её группы или ``anchor_document_code`` — документ той
    же стороны, на который перетащили остальные; он ведёт строку новой группы.
    """
    _require_enabled()
    side = _check_side(side)
    other = OTHER_SIDE[side]
    row_code = str(row_document_code or "").strip()
    anchor_code = str(anchor_document_code or "").strip()
    if not row_code and (new_group or not (group_id or anchor_code)):
        raise DocumentGroupError(f"{other.upper()}_REQUIRED", f"Не указан документ {SIDE_LABEL[other]}")
    codes = _unique(member_codes)
    if not codes:
        raise DocumentGroupError("MEMBERS_REQUIRED", f"Не указаны документы {SIDE_LABEL[side]}")
    if new_group and group_id:
        raise DocumentGroupError("BAD_GROUP_TARGET", "Нельзя одновременно создать и выбрать группу")
    if new_group and side != "right":
        raise DocumentGroupError("BAD_GROUP_TARGET", "Отдельное сравнение создаётся только справа")
    with _lock:
        sources = _current_sources(object_id)
        if row_code and row_code not in sources[other]:
            raise DocumentGroupError(
                f"{other.upper()}_NOT_FOUND", f"{SIDE_LABEL[other].capitalize()} нет документа {row_code}",
            )
        if not row_code and not group_id and anchor_code not in sources[side]:
            raise DocumentGroupError(
                f"{side.upper()}_NOT_FOUND", f"{SIDE_LABEL[side].capitalize()} нет документа {anchor_code}",
            )
        data = _read(object_id)
        groups = data["groups"]
        if group_id:
            group = next((g for g in groups if g["group_id"] == group_id
                          and (not row_code or row_code in g[other]["members"])), None)
            if group is None:
                raise DocumentGroupError("GROUP_NOT_FOUND", "Группа документов не найдена")
        elif row_code:
            group = None if new_group else next((g for g in groups
                if row_code in g[other]["members"] and not g.get("independent")), None)
        else:
            # Напротив пусто: документ-цель уже может вести свою группу.
            group = next((g for g in groups
                          if anchor_code in g[side]["members"] and not g.get("independent")), None)
            codes = _unique([anchor_code, *codes])
            if group is None and len(codes) < MIN_MEMBERS:
                raise DocumentGroupError("MEMBERS_REQUIRED", "Для группы нужны хотя бы два проекта")
        if group is not None and group.get("independent") and side != "right":
            raise DocumentGroupError("BAD_GROUP_TARGET", "В отдельное сравнение документы добавляются только справа")
        if new_group and any(set(codes) & set(g[side]["members"]) for g in groups):
            raise DocumentGroupError("MEMBER_ALREADY_GROUPED", "Проект уже входит в другую группу")
        for other_group in groups:
            anchor = other_group["anchor"]
            if other_group is not group and anchor["side"] == side and anchor["document_code"] in codes:
                raise DocumentGroupError(
                    "MEMBER_IS_GROUP_ANCHOR",
                    f"Документ {anchor['document_code']} ведёт строку другой группы — сначала уберите её участников",
                )
        # Документ стороны входит только в одно сравнение, даже при том же документе напротив.
        for other_group in groups:
            if other_group is not group:
                other_group[side]["members"] = [c for c in other_group[side]["members"] if c not in codes]
        if group is None and not row_code:
            # Группа одной стороны: строку ведёт документ, на который перетащили.
            group = {
                "group_id": "grp_" + uuid.uuid4().hex[:16],
                "anchor": {"side": side, "document_code": anchor_code},
                side: _empty_side([anchor_code]),
                other: _empty_side(),
                "created_at": _now(),
            }
            groups.append(group)
        if group is None:
            group = {
                "group_id": "grp_" + uuid.uuid4().hex[:16],
                "anchor": {"side": other, "document_code": row_code},
                other: _empty_side([row_code]),
                side: _empty_side(),
                "created_at": _now(),
            }
            if new_group:
                group["independent"] = True
            groups.append(group)
        target = group[side]
        before = list(target["members"])
        target["members"] = (
            _unique([*_member_order(target), *codes]) if target.get("manual_order")
            else _ordered([*target["members"], *codes])
        )
        group["updated_at"] = _now()
        released = _settle(object_id, groups)
        if group in groups:
            _ensure_side(object_id, group, side, sources, force=target["members"] != before)
        _write(object_id, data)
        return {
            "group": _view(object_id, group, sources) if group in groups else None,
            "released": released,
        }


def remove_member(object_id: str, group_id: str, document_code: str, side: str = "right") -> dict[str, Any]:
    """Вернуть документ стороны из группы в общий список."""
    _require_enabled()
    side = _check_side(side)
    with _lock:
        data = _read(object_id)
        group = _find_group(data["groups"], group_id)
        members = group[side]["members"]
        if document_code not in members:
            raise DocumentGroupError("MEMBER_NOT_FOUND", f"В группе нет документа {document_code}")
        if len(members) == 1 and not group.get("independent"):
            raise DocumentGroupError(
                "MEMBER_LAST", f"Это единственный документ {SIDE_LABEL[side]} в строке — его нельзя убрать из группы",
            )
        group[side]["members"] = [code for code in members if code != document_code]
        group["updated_at"] = _now()
        released = _settle(object_id, data["groups"])
        sources = _current_sources(object_id)
        if group in data["groups"]:
            _ensure_side(object_id, group, side, sources, force=True)
        _write(object_id, data)
        return {
            "group": _view(object_id, group, sources) if group in data["groups"] else None,
            "released": released,
        }


def reorder_members(object_id: str, group_id: str, member_codes: list[str], side: str = "right") -> dict[str, Any]:
    """Сохранить порядок склейки стороны. Пересборка запускается отдельным действием."""
    _require_enabled()
    side = _check_side(side)
    with _lock:
        data = _read(object_id)
        group = _find_group(data["groups"], group_id)
        sources = _current_sources(object_id)
        if _side_view(object_id, group, side, sources[side])["status"] in BUILDING_STATUSES:
            raise DocumentGroupError("GROUP_BUSY", "Дождитесь завершения сборки")
        members = group[side]["members"]
        if len(member_codes) != len(members) or set(member_codes) != set(members):
            raise DocumentGroupError("BAD_MEMBER_ORDER", "Порядок должен содержать всех участников группы ровно один раз")
        group[side]["members"] = list(member_codes)
        group[side]["manual_order"] = True
        group["updated_at"] = _now()
        _write(object_id, data)
        return {"group": _view(object_id, group, sources), "released": []}


def rebuild(object_id: str, group_id: str, side: str | None = None) -> dict[str, Any]:
    """Собрать сторону группы заново (после ошибки, новой версии участника, смены порядка).

    Без ``side`` пересобираются стороны, которым это нужно; если таких нет —
    все собираемые стороны группы.
    """
    _require_enabled()
    with _lock:
        data = _read(object_id)
        group = _find_group(data["groups"], group_id)
        sources = _current_sources(object_id)
        assembled = [s for s in SIDES if _needs_assembly(group, s)]
        if side is not None:
            targets = [s for s in assembled if s == _check_side(side)]
        else:
            views = {s: _side_view(object_id, group, s, sources[s]) for s in assembled}
            targets = [s for s in assembled if views[s]["stale"] or views[s]["status"] != "READY"] or assembled
        if not targets:
            raise DocumentGroupError("NOTHING_TO_BUILD", "У группы нет стороны со сборкой")
        if any(_side_view(object_id, group, s, sources[s])["status"] in BUILDING_STATUSES for s in targets):
            raise DocumentGroupError("GROUP_BUSY", "Сборка группы уже выполняется")
        for target in targets:
            _launch_build(object_id, group, target, sources)
        group["updated_at"] = _now()
        _write(object_id, data)
        return {"group": _view(object_id, group, sources), "released": []}


def _groups_for_stage_path(stage_path: str | Path | None) -> list[dict[str, Any]]:
    """Группы версии сравнения, которой принадлежит папка стадии."""
    if not stage_path or not assemblies.enabled():
        return []
    scope = assemblies.scope_for_stage_path(stage_path)
    if not scope:
        return []
    try:
        return _read(scope)["groups"]
    except (DocumentGroupError, stage_upload.StageUploadError):
        return []


def hidden_documents(stage_path: str | Path | None, side: str, documents: list[dict[str, Any]]) -> set[str]:
    """PDF стороны, которые живут внутри групп и не стоят в раскладке пар.

    Это участники групп, кроме якорей строк, и все версии сборок групп: в
    раскладке их заменяет сама группа. Сборки, созданные не группами, не
    скрываются.
    """
    side = _check_side(side)
    groups = _groups_for_stage_path(stage_path)
    if not groups:
        return set()
    anchors = {g["anchor"]["document_code"] for g in groups if g["anchor"]["side"] == side}
    member_codes = {code for g in groups for code in g[side]["members"]} - anchors
    assembly_ids = {g[side]["assembly_id"] for g in groups if g[side].get("assembly_id")}
    hidden: set[str] = set()
    for document in documents:
        ref = document.get("assembly_ref")
        if isinstance(ref, dict):
            if ref.get("assembly_id") in assembly_ids:
                hidden.add(str(document.get("pdf_path")))
        elif document.get("document_code") in member_codes:
            hidden.add(str(document.get("pdf_path")))
    return hidden


def grouped_codes(stage_path: str | Path | None, side: str) -> set[str]:
    """Коды документов стороны, уже сопоставленные своей группе (вместе с якорями)."""
    side = _check_side(side)
    return {code for g in _groups_for_stage_path(stage_path) for code in g[side]["members"]}


def open_anchor_codes(stage_path: str | Path | None, side: str) -> set[str]:
    """Якоря групп стороны ``side``, напротив которых пока пусто.

    Автосопоставление подбирает им документ другой стороны как обычным
    документам; портал добавляет найденный документ в группу.
    """
    side = _check_side(side)
    return {
        g["anchor"]["document_code"] for g in _groups_for_stage_path(stage_path)
        if g["anchor"]["side"] == side and not g.get("independent") and not g[OTHER_SIDE[side]]["members"]
    }


__all__ = [
    "DocumentGroupError",
    "add_members",
    "grouped_codes",
    "hidden_documents",
    "list_groups",
    "open_anchor_codes",
    "rebuild",
    "remove_member",
    "reorder_members",
]
