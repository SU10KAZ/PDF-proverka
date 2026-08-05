"""Audit-представление блока: высокосигнальный контекст для поиска замечаний.

Отвечает на вопросы проверяющего, а не описывает каждое помещение:
какие правила действуют, какие решения типовые, где связи неполные,
какие группы обслуживают несколько помещений, где несколько точек
управления, какие размеры не подтверждены, что не привязано и где
GEOMETRY_CONFLICT.

Формируется ТОЛЬКО из semantic_graph.json (детектор, full и compact не
меняются). Штатные повторяющиеся решения агрегируются: 60 помещений
покомнатно не перечисляются, обычные связи и привязки сводятся в
счётчики и диапазоны.

Секционный контракт (:func:`build_audit_context`) отдаёт разделы по
отдельности, чтобы сборщик промпта брал только нужное. Фильтрация по
квартире/помещениям/группам выполняется ПО ГРАФУ до рендера — текст
никогда не обрезается постфактум.
"""
from __future__ import annotations

import collections

from .render_md_compact import _fmt_numbers, _humanize_conflict, _kt

SECTION_KEYS = ("summary", "sheet_rules", "ceilings", "lighting_control",
                "dimensions", "uncertainties")

DISCLAIMER = ("Непривязанность и неполная связь не являются доказанным дефектом "
              "проекта. Они определяют области для проверки по изображению.")

# правила листа, способные участвовать в проверках
RULE_PATTERNS = ("центр", "высот", "ось", "оси", "разнос", "вертикал", "привязк")


def _rooms_of_apartment(graph: dict, apt_id: str) -> list[str]:
    return [r["mark"] for r in graph["rooms"] if r["apartment"] == apt_id]


def filter_graph(graph: dict, *, apartment_id: str | None = None,
                 room_ids: list[str] | None = None,
                 group_ids: list[str] | None = None) -> dict:
    """Подграф под выбранную область. Возвращает НОВЫЙ dict (граф не мутируется).

    Фильтры комбинируются: помещения = room_ids ∩ (помещения квартиры) ∪
    (помещения выбранных групп). Всё, что не относится к выбранной
    области, из подграфа исключается ДО рендера.
    """
    if apartment_id is None and not room_ids and not group_ids:
        return graph

    groups = graph["groups"]
    if group_ids:
        groups = [g for g in groups if g["group_id"] in set(group_ids)]
    if apartment_id is not None:
        groups = [g for g in groups if g["apartment"] == apartment_id]

    keep_rooms: set[str] = set()
    if room_ids:
        keep_rooms |= set(room_ids)
    if apartment_id is not None:
        apt_rooms = set(_rooms_of_apartment(graph, apartment_id))
        keep_rooms = (keep_rooms & apt_rooms) if keep_rooms else apt_rooms
    if group_ids:
        group_rooms = {r for g in groups for r in (g.get("rooms") or [])}
        keep_rooms = (keep_rooms & group_rooms) if keep_rooms else group_rooms
    if not keep_rooms and not (room_ids or apartment_id or group_ids):
        keep_rooms = {r["mark"] for r in graph["rooms"]}

    group_keys = {g["group_id"] for g in groups}
    keep_apts = {g["apartment"] for g in groups} | {
        m.split(".")[1] for m in keep_rooms if m.count(".") == 2}

    def dev_kept(dev) -> bool:
        if dev.get("room") in keep_rooms:
            return True
        return bool(set(dev.get("group_ids") or ()) & group_keys)

    sub = dict(graph)
    sub["rooms"] = [r for r in graph["rooms"] if r["mark"] in keep_rooms]
    sub["apartments"] = [a for a in graph["apartments"] if a["id"] in keep_apts]
    sub["groups"] = groups
    sub["lights"] = [x for x in graph["lights"] if dev_kept(x)]
    sub["switches"] = [x for x in graph["switches"] if dev_kept(x)]
    sub["master_switches"] = [x for x in graph["master_switches"]
                              if x.get("room") in keep_rooms or x.get("apartment") in keep_apts]
    sub["ceiling_zones"] = [z for z in graph["ceiling_zones"] if z["room"] in keep_rooms]
    kept_dev_ids = {d["dim_id"] for dev in sub["lights"] + sub["switches"] + sub["master_switches"]
                    for d in dev.get("dimensions", [])}
    sub["dimensions"] = [d for d in graph["dimensions"] if d["dim_id"] in kept_dev_ids]
    sub["unresolved_symbols"] = [s for s in graph["unresolved_symbols"]
                                 if (s.get("room") or s.get("room_candidate")) in keep_rooms]
    return sub


# ------------------------------------------------------------- секции

def _section_summary(graph: dict) -> str:
    sheet = graph.get("sheet") or {}
    v = graph["validation"]
    head = [sheet.get("doc_number") or "обозначение не извлечено"]
    if sheet.get("building"):
        head.append(f"корпус {sheet['building']}")
    if sheet.get("floors_label"):
        head.append(sheet["floors_label"])
    if sheet.get("sheet_no"):
        head.append(f"лист {sheet['sheet_no']}")
    lines = ["## Лист", "", "- " + " · ".join(head)]
    lines.append(f"- Состав: квартир {len(graph['apartments'])}, помещений {len(graph['rooms'])}, "
                 f"потолочных марок {len(graph['ceiling_zones'])}, световых точек "
                 f"{len(graph['lights'])}, выключателей {len(graph['switches'])}, "
                 f"мастер-выключателей {len(graph['master_switches'])}.")
    status = graph.get("status") or "complete"
    if status != "complete":
        lines.append(f"- Извлечение частичное ({status}) — часть контура не разобрана.")
    _ = v
    return "\n".join(lines)


def _section_sheet_rules(graph: dict) -> str:
    lines = ["## Правила листа (проверяемые требования)", ""]
    rules = [r for r in (graph.get("sheet_rules") or [])
             if any(p in r["text"].lower() for p in RULE_PATTERNS)]
    for rule in rules:
        lines.append(f"- {rule['text'].rstrip('.').capitalize()}.")
    if not rules:
        lines.append("- Проверяемые требования из примечаний листа не извлечены.")
    return "\n".join(lines)


def _section_ceilings(graph: dict) -> str:
    zones = graph["ceiling_zones"]
    lines = ["## Потолки", ""]
    if not zones:
        lines.append("- Потолочные марки не извлечены.")
        return "\n".join(lines)
    types = collections.Counter(z["ceiling_type"] for z in zones if z["ceiling_type"])
    elevs = collections.Counter(z["elevation"] for z in zones if z["elevation"])
    types_txt = ", ".join(f"тип {t} — {n}" for t, n in sorted(types.items()))
    elev_txt = ", ".join(f"{e} — {n}" for e, n in sorted(elevs.items()))
    lines.append(f"- Марок {len(zones)}: {types_txt}.")
    lines.append(f"- Отметки: {elev_txt}." + (
        " Единая отметка по листу." if len(elevs) == 1 else
        " Разные отметки — проверить сопряжения уровней."))
    rooms_wo = [r["mark"] for r in graph["rooms"] if not r["ceiling_zones"]]
    if rooms_wo:
        by_apt = collections.Counter(m.split(".")[1] for m in rooms_wo)
        apt_txt = ", ".join(f"кв. {a} — {n}" for a, n in sorted(by_apt.items()))
        lines.append(f"- Без подтверждённой марки: {len(rooms_wo)} помещений ({apt_txt}).")
    no_room = [z for z in zones if z["room"] is None]
    if no_room:
        lines.append(f"- Марок без подтверждённого помещения: {len(no_room)}.")
    multi = [z for z in zones if z.get("state") == "multiple_tags_zone_boundary_not_extracted"]
    if multi:
        lines.append(f"- Помещений с несколькими марками без выделенных границ зон: "
                     f"{len({z['room'] for z in multi})}.")
    return "\n".join(lines)


def _classify_groups(graph: dict) -> dict:
    lights = {x["id"]: x for x in graph["lights"]}
    switches = {x["id"]: x for x in graph["switches"]}
    simple, multi, cross, incomplete = [], [], [], []
    for g in graph["groups"]:
        if g["state"] != "confirmed":
            incomplete.append(g)
            continue
        sw_rooms = {switches[s].get("room") for s in g["switches"] if s in switches}
        li_rooms = {lights[l].get("room") for l in g["lights"] if l in lights}
        sw_rooms.discard(None)
        li_rooms.discard(None)
        is_multi = len(g["switches"]) > 1
        is_cross = bool(sw_rooms and li_rooms and not (sw_rooms & li_rooms))
        if is_multi:
            multi.append((g, sorted(sw_rooms)))
        if is_cross:
            cross.append((g, sorted(sw_rooms), sorted(li_rooms)))
        if not is_multi and not is_cross:
            simple.append(g)
    return {"simple": simple, "multi": multi, "cross": cross, "incomplete": incomplete}


def _section_lighting_control(graph: dict) -> str:
    cls = _classify_groups(graph)
    total = len(graph["groups"])
    confirmed = total - len(cls["incomplete"])
    lights = graph["lights"]
    wall = sum(1 for x in lights if x["kind"] == "wall_light_output")
    lines = ["## Освещение и управление", ""]
    lines.append(f"- Световых точек {len(lights)} (настенных {wall}), выключателей "
                 f"{len(graph['switches'])}; групп {total}: полных {confirmed}, "
                 f"неполных {len(cls['incomplete'])}.")

    if cls["simple"]:
        by_apt = collections.defaultdict(list)
        for g in cls["simple"]:
            by_apt[g["apartment"]].append(g["number"])
        parts = [f"кв. {apt} — {_fmt_numbers(nums)}" for apt, nums in sorted(by_apt.items())]
        lines.append(f"- Типовые связи (один выключатель, свет в том же помещении), групп "
                     f"{len(cls['simple'])}: " + "; ".join(parts) + ".")

    if cls["cross"]:
        lines.append("")
        lines.append(f"**Группы, обслуживающие другие помещения ({len(cls['cross'])}):**")
        by_apt = collections.defaultdict(lambda: collections.defaultdict(list))
        for g, sw_rooms, li_rooms in cls["cross"]:
            key = (", ".join(sw_rooms), ", ".join(li_rooms))
            by_apt[g["apartment"]][key].append(g["number"])
        for apt, routes in sorted(by_apt.items()):
            pieces = []
            for (src, dst), numbers in sorted(routes.items(),
                                              key=lambda kv: min(int(n) for n in kv[1])):
                word = "группа" if len(numbers) == 1 else "группы"
                pieces.append(f"{word} {_fmt_numbers(numbers)} ({src} → {dst})")
            lines.append(f"- Кв. {apt}: " + "; ".join(pieces) + ".")

    if cls["multi"]:
        lines.append("")
        lines.append(f"**Несколько точек управления ({len(cls['multi'])}):**")
        by_apt = collections.defaultdict(list)
        for g, sw_rooms in cls["multi"]:
            by_apt[g["apartment"]].append((g["number"], len(g["switches"]), sw_rooms))
        for apt, items in sorted(by_apt.items()):
            pieces = []
            for num, n, rooms in sorted(items, key=lambda x: int(x[0])):
                where = ", ".join(rooms) or "помещение не подтверждено"
                prep = "в" if len(rooms) < n else ""
                pieces.append(f"{num} — {n} точки {prep} {where}".replace("  ", " "))
            lines.append(f"- Кв. {apt}: " + "; ".join(pieces) + ".")

    if cls["incomplete"]:
        lines.append("")
        lines.append(f"**Неполные группы ({len(cls['incomplete'])}) — области проверки:**")
        for g in sorted(cls["incomplete"], key=lambda x: (x["apartment"], int(x["number"]))):
            rooms_txt = ", ".join(g.get("rooms") or []) or "помещение не подтверждено"
            if g["state"] == "switches_only":
                found = f"{len(g['switches'])} выключател" + ("ь" if len(g["switches"]) == 1 else "я")
                miss = "световой вывод не привязан"
            elif g["state"] == "lights_only":
                found = f"{len(g['lights'])} световой вывод"
                miss = "выключатель не привязан"
            else:
                found = f"{len(g['lights'])} св. точек, {len(g['switches'])} выкл."
                miss = "связь не подтверждена"
            tail = ""
            if g.get("unresolved_participants"):
                tail = (f"; рядом {len(g['unresolved_participants'])} неклассифицированный "
                        "символ с этой подписью")
            lines.append(f"- Кв. {g['apartment']}, группа {g['number']}: найдено {found} "
                         f"({rooms_txt}); {miss}{tail}.")
    return "\n".join(lines)


def _section_dimensions(graph: dict) -> str:
    dims = graph["dimensions"]
    bound = [d for d in dims if str(d.get("binding_state") or "").startswith("device_to")]
    lines = ["## Размерные привязки", ""]
    if not dims:
        lines.append("- Размерные конструкции не извлечены.")
        return "\n".join(lines)
    values = collections.Counter(d["value_mm"] for d in bound)
    typical = ", ".join(f"{v} мм — {n}" for v, n in values.most_common(4))
    lines.append(f"- Конструкций {len(dims)}, связаны с устройствами {len(bound)}"
                 + (f"; значения: {typical}." if typical else "."))

    weak = []
    for dev in graph["switches"] + graph["master_switches"] + graph["lights"]:
        for d in dev.get("dimensions", []):
            if d["tier"] >= 3:
                continue
            why = ("масштабная сверка не пройдена"
                   if d.get("binding") != "proximity_only" else
                   "выносная цепочка не подтверждена (только близость)")
            weak.append((dev.get("room") or "помещение не подтверждено",
                         d["value_mm"], _kt(dev["kind"]), why))
    if weak:
        lines.append("")
        lines.append(f"**Не подтверждённые привязки ({len(weak)}) — сверить по изображению:**")
        for room, value, kind, why in sorted(set(weak)):
            lines.append(f"- {room[0].upper() + room[1:]}: {value} мм у устройства "
                         f"«{kind}» — {why}.")

    if values:
        rare = [f"{v} мм ({n})" for v, n in sorted(values.items()) if n <= 2]
        if rare:
            lines.append(f"- Единичные значения привязок: {', '.join(rare)} — проверить "
                         "соответствие правилу листа.")
    return "\n".join(lines)


def _section_uncertainties(graph: dict) -> str:
    lines = ["## Неопределённости и конфликты", ""]
    dev_by_kind = collections.Counter()
    candidates = collections.Counter()
    for dev in graph["lights"] + graph["switches"] + graph["master_switches"]:
        if dev.get("room") is None:
            dev_by_kind[_kt(dev["kind"])] += 1
            rb = dev.get("room_binding") or {}
            cand = rb.get("candidate") or rb.get("nearest_mark")
            if cand:
                candidates[cand] += 1
    if dev_by_kind:
        kinds_txt = ", ".join(f"{k} — {n}" for k, n in sorted(dev_by_kind.items()))
        top = ", ".join(f"{room} ({n})" for room, n in candidates.most_common(5))
        lines.append(f"- Устройств без подтверждённого помещения: {sum(dev_by_kind.values())} "
                     f"({kinds_txt}); вероятные помещения: {top}.")
    zones_wo = [z for z in graph["ceiling_zones"] if z["room"] is None]
    if zones_wo:
        near = collections.Counter(z.get("nearest_mark") for z in zones_wo if z.get("nearest_mark"))
        top = ", ".join(f"{room} ({n})" for room, n in near.most_common(4))
        lines.append(f"- Потолочных марок без помещения: {len(zones_wo)}"
                     + (f"; вероятные помещения: {top}." if top else "."))
    unres = graph.get("unresolved_symbols") or []
    if unres:
        with_groups = sum(1 for s in unres if s.get("adjacent_group_numbers"))
        lines.append(f"- Символов, не совпавших с легендой: {len(unres)}"
                     + (f", из них {with_groups} с подписью группы." if with_groups else "."))
    conflicts = graph.get("conflicts") or []
    if conflicts:
        lines.append("")
        lines.append(f"**GEOMETRY_CONFLICT ({len(conflicts)}):**")
        for c in conflicts:
            lines.append(f"- {_humanize_conflict(c)}")
    lines.append("")
    lines.append(f"> {DISCLAIMER}")
    return "\n".join(lines)


_SECTION_BUILDERS = {
    "summary": _section_summary,
    "sheet_rules": _section_sheet_rules,
    "ceilings": _section_ceilings,
    "lighting_control": _section_lighting_control,
    "dimensions": _section_dimensions,
    "uncertainties": _section_uncertainties,
}


def build_audit_context(graph: dict, sections: list[str] | None = None,
                        apartment_id: str | None = None,
                        room_ids: list[str] | None = None,
                        group_ids: list[str] | None = None) -> dict:
    """Секции audit-контекста. Фильтрация выполняется по графу ДО рендера.

    :param sections: подмножество :data:`SECTION_KEYS` (None — все);
    :param apartment_id: только эта квартира;
    :param room_ids: только эти помещения;
    :param group_ids: только эти группы (``"<квартира>:<номер>"``).
    """
    wanted = [s for s in (sections or SECTION_KEYS) if s in _SECTION_BUILDERS]
    sub = filter_graph(graph, apartment_id=apartment_id, room_ids=room_ids,
                       group_ids=group_ids)
    return {key: _SECTION_BUILDERS[key](sub) for key in wanted}


def render_markdown_audit(graph: dict, sections: list[str] | None = None,
                          apartment_id: str | None = None,
                          room_ids: list[str] | None = None,
                          group_ids: list[str] | None = None) -> str:
    """Audit-Markdown целиком или по выбранной области."""
    ctx = build_audit_context(graph, sections=sections, apartment_id=apartment_id,
                              room_ids=room_ids, group_ids=group_ids)
    sheet = graph.get("sheet") or {}
    title = "# Аудит-контекст: план потолков и освещения"
    if sheet.get("sheet_no") and (sections is None or "summary" in ctx):
        title += f" — лист {sheet['sheet_no']}"
    body = [title, ""]
    for key in SECTION_KEYS:
        if key in ctx:
            body.append(ctx[key])
            body.append("")
    return "\n".join(body).rstrip() + "\n"


# публичное имя по контракту задачи
build_ar_ceiling_lighting_audit_context = build_audit_context
