"""Рендер человекочитаемого Markdown-описания квартир.

Правила: инженерный связный текст, без координат и внутренних id в
основном тексте; tier 3–5 — в основных разделах, tier 2 — только в
«Требует проверки»; «не извлечено» вместо «отсутствует»; общие
примечания листа — один раз.
"""
from __future__ import annotations

import collections

KIND_TEXT = {
    "light_output": "вывод под светильник",
    "chandelier_output": "вывод под люстру",
    "switch_1": "одноклавишный выключатель",
    "switch_2": "двухклавишный выключатель",
    "switch_changeover": "переключатель с нескольких мест",
    "master_switch": "мастер-выключатель",
}


def _plural_ru(n: int, one: str, few: str, many: str) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def _groups_phrase(numbers: list[str]) -> str:
    if not numbers:
        return "без извлечённого номера группы"
    if len(numbers) == 1:
        return f"группы {numbers[0]}"
    return "групп " + " и ".join(numbers)


def render_markdown(graph: dict) -> str:
    out: list[str] = []
    sheet = graph["sheet"]
    v = graph["validation"]

    out.append("# План потолков и освещения — поквартирное описание\n")
    out.append("## Данные листа\n")
    out.append(f"- Обозначение: {sheet.get('doc_number') or 'не извлечено'}")
    out.append(f"- Наименование: {sheet.get('sheet_name') or 'не извлечено'}")
    out.append(f"- Корпус: {sheet.get('building') or sheet.get('building_part_from_marks') or 'не извлечено'}")
    out.append(f"- Этажи: {sheet.get('floors_label') or 'не извлечено'}")
    sheet_no = sheet.get("sheet_no")
    if sheet_no:
        tail = f" (листов {sheet['sheets_total']})" if sheet.get("sheets_total") else ""
        stage = f", стадия {sheet['stage']}" if sheet.get("stage") else ""
        out.append(f"- Лист: {sheet_no}{tail}{stage}")
    else:
        out.append("- Лист: номер листа из векторного текста не извлечён")
    zero = sheet.get("zero_level")
    out.append(f"- Отметка 0,000: {('= ' + zero) if zero else 'не извлечено'}")
    if sheet.get("address"):
        out.append(f"- Объект: {sheet['address']}")
    out.append(f"- Источник: {graph['source']['pdf_file']}, векторный слой, стр. PDF "
               f"{graph['source']['page_index'] + 1}")
    out.append("- Метод: детерминированное извлечение из векторного слоя PDF "
               "(без LLM, без OCR, без растрового распознавания)\n")

    out.append("## Общие условные обозначения и правила листа\n")
    out.append("Расшифрованные условные обозначения (из легенды самого листа):\n")
    for tpl in graph["legend"]:
        if tpl["kind"] == "unresolved_legend_row":
            continue
        out.append(f"- {tpl['label']}")
    if graph["sheet_rules"]:
        out.append("\nПримечания листа (действуют на все квартиры, в помещениях не повторяются):\n")
        for rule in graph["sheet_rules"]:
            out.append(f"{rule['no']}. {rule['text']}")
    out.append("")

    rooms_by_apt = collections.defaultdict(list)
    for room in graph["rooms"]:
        rooms_by_apt[room["apartment"]].append(room)
    zones_by_id = {z["zone_id"]: z for z in graph["ceiling_zones"]}
    lights_by_id = {x["id"]: x for x in graph["lights"]}
    switches_by_id = {x["id"]: x for x in graph["switches"]}
    masters_by_id = {x["id"]: x for x in graph["master_switches"]}
    groups_by_apt = collections.defaultdict(list)
    for g in graph["groups"]:
        groups_by_apt[g["apartment"]].append(g)
    unresolved_by_room = collections.defaultdict(list)
    for sym in graph["unresolved_symbols"]:
        room = sym.get("room") or sym.get("room_candidate")
        if room:
            unresolved_by_room[room].append(sym)

    for apt in graph["apartments"]:
        out.extend(_render_apartment(apt, rooms_by_apt[apt["id"]], graph, zones_by_id,
                                     lights_by_id, switches_by_id, masters_by_id,
                                     groups_by_apt[apt["id"]], unresolved_by_room))

    out.extend(_render_unassigned(graph))
    out.extend(_render_block_control(graph, v))
    return "\n".join(out).rstrip() + "\n"


def _card_line(value, unit=" м²"):
    return f"{value}{unit}" if value else "не извлечено"


def _render_apartment(apt, rooms, graph, zones_by_id, lights_by_id, switches_by_id,
                      masters_by_id, groups, unresolved_by_room) -> list[str]:
    out = [f"\n## Квартира {apt['id']}\n"]
    card = apt.get("card")
    out.append("**Паспорт квартиры**\n")
    if card:
        out.append(f"- Тип: {card.get('type') or 'не извлечено'}")
        out.append(f"- Жилая площадь: {_card_line(card.get('living_area'))}")
        out.append(f"- Площадь квартиры: {_card_line(card.get('apartment_area'))}")
        out.append(f"- Общая площадь: {_card_line(card.get('total_area'))}")
    else:
        out.append("- Карточка квартиры из векторного текста не извлечена.")
    out.append(f"- Количество помещений: {len(rooms)}\n")
    if card and card.get("requires_review"):
        out.append("> Площади карточки не прошли контроль «жилая ≤ квартира ≤ общая» — "
                   "проверить чтение карточки.\n")

    rooms = sorted(rooms, key=lambda r: r["room_suffix"])
    for room in rooms:
        out.extend(_render_room(room, graph, zones_by_id, lights_by_id, switches_by_id,
                                masters_by_id, groups, unresolved_by_room))

    out.extend(_render_groups_table(apt, groups, lights_by_id, switches_by_id))
    out.extend(_render_master_section(apt, masters_by_id, graph))
    out.extend(_render_apartment_control(apt, rooms, groups, graph))
    return out


def _room_title(room) -> str:
    name = room["name"] if room["name"] else "наименование по ведомости не извлечено"
    return f"### Помещение {room['mark']} — {name}\n"


def _render_room(room, graph, zones_by_id, lights_by_id, switches_by_id, masters_by_id,
                 groups, unresolved_by_room) -> list[str]:
    out = [_room_title(room)]
    review: list[str] = []

    zones = [zones_by_id[z] for z in room["ceiling_zones"]]
    resolved_zones = [z for z in zones if z["extent"] == "room"]
    if len(resolved_zones) == 1:
        z = resolved_zones[0]
        out.append(f"В помещении определена потолочная зона типа {z['ceiling_type']} "
                   f"с отметкой {z['elevation']} (распространена на всё помещение: "
                   "единственная потолочная марка).")
    elif len(zones) > 1:
        kinds = ", ".join(f"тип {z['ceiling_type']} / {z['elevation']}" for z in zones)
        out.append(f"В помещении {len(zones)} потолочные марки ({kinds}); границы зон "
                   "в векторном слое не выделены, распределение по площади не выполнялось.")
        review.append("Несколько потолочных марок в одном помещении — зоны их действия "
                      "требуют ручной сверки с чертежом.")
    elif zones:
        z = zones[0]
        out.append(f"Потолочная марка: тип {z['ceiling_type']}, отметка {z['elevation']} "
                   f"(состояние: {z['state']}).")
    else:
        out.append("Потолочная марка в этом помещении из векторного слоя не извлечена.")
    out.append("")

    lights = [lights_by_id[i] for i in room["lights"]]
    if lights:
        out.append("**Освещение**\n")
        for light in sorted(lights, key=lambda x: (x["kind"], x["groups"])):
            line = f"- {KIND_TEXT[light['kind']].capitalize()}, {_groups_phrase(light['groups'])}."
            extras = []
            if light.get("centered_by_guides"):
                extras.append("положение — по пересечению линий центрирования")
            z = light.get("ceiling_zone")
            if z:
                extras.append(f"потолочная зона: тип {z['ceiling_type']}, отметка {z['elevation']}")
            if extras:
                line += " " + "; ".join(extras).capitalize() + "."
            out.append(line)
        out.append("")

    switches = [switches_by_id[i] for i in room["switches"]]
    masters = [masters_by_id[i] for i in room["master_switches"]]
    if switches or masters:
        out.append("**Управление**\n")
        for sw in sorted(switches, key=lambda x: (x["kind"], x["groups"])):
            place = " у дверного проёма" if sw.get("near_door") and sw.get("dimensions") else ""
            out.append(f"- {KIND_TEXT[sw['kind']].capitalize()} {_groups_phrase(sw['groups'])}{place}.")
        for m in masters:
            out.append("- Мастер-выключатель «М» (зелёное обозначение по легенде листа).")
        chains = _confirmed_chains(room, groups, lights_by_id, switches_by_id)
        if chains:
            out.append("- Подтверждённые связи:")
            for chain in chains:
                out.append(f"  - `{chain}`")
        out.append("")

    dims_lines = _room_dimensions(room, switches, masters, lights)
    if dims_lines:
        out.append("**Размерные привязки**\n")
        out.extend(dims_lines)
        out.append("")

    for sym in unresolved_by_room.get(room["mark"], []):
        numbers = sorted(set(sym.get("adjacent_group_numbers") or []))
        num_txt = f" с подписями групп {', '.join(numbers)}" if numbers else ""
        state = "в области помещения" if sym.get("room") else "предположительно в этом помещении"
        review.append(f"Обнаружен символ электроустановочного изделия{num_txt}, не совпавший "
                      f"ни с одним условным обозначением легенды ({state}); тип не назначен.")
    for dev in _weak_devices_of_room(room, graph):
        review.append(dev)

    if review:
        out.append("**Требует проверки**\n")
        for item in review:
            out.append(f"- {item}")
        out.append("")
    return out


def _confirmed_chains(room, groups, lights_by_id, switches_by_id) -> list[str]:
    chains = []
    for g in groups:
        if g["state"] != "confirmed":
            continue
        room_switches = [s for s in g["switches"] if s in room["switches"]]
        if not room_switches:
            continue
        light_kinds = collections.Counter(lights_by_id[i]["kind"] for i in g["lights"])
        lights_txt = ", ".join(
            f"{n} × {KIND_TEXT[k]}" if n > 1 else KIND_TEXT[k]
            for k, n in sorted(light_kinds.items()))
        sw_txt = " + ".join(sorted({KIND_TEXT[switches_by_id[i]["kind"]] for i in room_switches}))
        rooms_txt = ", ".join(g["rooms"]) if g["rooms"] else "помещение не извлечено"
        chains.append(f"{sw_txt} группы {g['number']} → группа {g['number']} → {lights_txt} ({rooms_txt})")
    return chains


def _room_dimensions(room, switches, masters, lights) -> list[str]:
    lines = []
    for dev in switches + masters:
        for dim in dev.get("dimensions", []):
            if dim["to"] == "wall_or_opening":
                target = "к стене / грани проёма"
            elif dim["to"] == "device_axis":
                target = "к оси соседнего устройства"
            else:
                continue  # неразрешённый конец → не утверждаем
            name = KIND_TEXT[dev["kind"]]
            mark = "" if dim["tier"] >= 3 else " (масштабная сверка не пройдена)"
            lines.append(f"- Ось устройства «{name}» привязана размером {dim['value_mm']} мм "
                         f"{target}{mark}.")
    return sorted(set(lines))


def _weak_devices_of_room(room, graph) -> list[str]:
    notes = []
    for dev in graph["lights"] + graph["switches"] + graph["master_switches"]:
        rb = dev.get("room_binding") or {}
        if dev.get("room") is None and rb.get("candidate") == room["mark"]:
            notes.append(f"{KIND_TEXT[dev['kind']].capitalize()} {_groups_phrase(dev.get('groups') or [])} "
                         f"стоит в полосе неопределённости открытой планировки у этого помещения — "
                         "принадлежность помещению не подтверждена.")
    return notes


def _render_groups_table(apt, groups, lights_by_id, switches_by_id) -> list[str]:
    out = [f"### Сводка групп освещения квартиры {apt['id']}\n"]
    if not groups:
        out.append("Групповые номера в этой квартире из векторного слоя не извлечены.\n")
        return out
    out.append("| Группа | Световые точки | Помещения | Выключатели | Состояние |")
    out.append("|---|---|---|---|---|")
    state_ru = {
        "confirmed": "подтверждена",
        "lights_only": "только светильники (выключатель не привязан)",
        "switches_only": "только выключатели (световой вывод не привязан)",
        "incomplete": "неполная",
    }
    for g in sorted(groups, key=lambda x: int(x["number"])):
        light_kinds = collections.Counter(lights_by_id[i]["kind"] for i in g["lights"])
        lights_txt = "; ".join(f"{KIND_TEXT[k]} ×{n}" if n > 1 else KIND_TEXT[k]
                               for k, n in sorted(light_kinds.items())) or "—"
        sw_kinds = collections.Counter(switches_by_id[i]["kind"] for i in g["switches"])
        sw_txt = "; ".join(f"{KIND_TEXT[k]} ×{n}" if n > 1 else KIND_TEXT[k]
                           for k, n in sorted(sw_kinds.items())) or "—"
        state = state_ru.get(g["state"], g["state"])
        if g.get("unresolved_participants"):
            state += f"; +{len(g['unresolved_participants'])} неразрешённый символ с этой группой"
        rooms_txt = ", ".join(g["rooms"]) or "—"
        out.append(f"| {g['number']} | {lights_txt} | {rooms_txt} | {sw_txt} | {state} |")
    out.append("")
    return out


def _render_master_section(apt, masters_by_id, graph) -> list[str]:
    out = ["### Мастер-выключатель\n"]
    ids = apt.get("master_switches") or []
    if not ids:
        out.append("Мастер-выключатель в пределах восстановленных областей этой квартиры "
                   "не привязан (либо не предусмотрен на плане, либо привязка не извлечена).\n")
        return out
    for mid in ids:
        m = masters_by_id[mid]
        room_txt = f"в помещении {m['room']}" if m.get("room") else "положение внутри квартиры"
        near = " у дверного проёма" if m.get("near_door") else ""
        out.append(f"- Мастер-выключатель «М» расположен {room_txt}{near}. "
                   "По условным обозначениям листа он относится к квартире; конкретный "
                   "перечень отключаемых групп на плане отдельно не показан, поэтому "
                   "прямые связи с группами не создавались.")
    out.append("")
    return out


def _render_apartment_control(apt, rooms, groups, graph) -> list[str]:
    zones = sum(len(r["ceiling_zones"]) for r in rooms)
    lights = sum(len(r["lights"]) for r in rooms)
    switches = sum(len(r["switches"]) for r in rooms)
    masters = len(apt.get("master_switches") or [])
    confirmed = sum(1 for g in groups if g["state"] == "confirmed")
    incomplete = len(groups) - confirmed
    weak = 0
    for dev in graph["lights"] + graph["switches"] + graph["master_switches"]:
        rb = dev.get("room_binding") or {}
        if dev.get("room") is None and str(rb.get("candidate", "")).split(".")[1:2] == [apt["id"]]:
            weak += 1
    unres = sum(1 for s in graph["unresolved_symbols"]
                if str(s.get("room") or s.get("room_candidate") or "").split(".")[1:2] == [apt["id"]])
    conf = sum(1 for c in graph["conflicts"]
               if apt["id"] in str(c.get("what", "")) or
               any(apt["id"] == str(r).split(".")[1] for r in (c.get("rooms") or [])))
    out = ["### Контроль полноты квартиры\n"]
    out.append(f"- Помещений: {len(rooms)}; потолочных марок: {zones}; "
               f"световых точек: {lights}; выключателей: {switches}; "
               f"мастер-выключателей: {masters}.")
    out.append(f"- Группы: подтверждённых {confirmed}, неполных {incomplete}.")
    out.append(f"- Непривязанных к помещению устройств: {weak}; "
               f"неразрешённых символов: {unres}; геометрических конфликтов: {conf}.")
    out.append("")
    return out


def _render_unassigned(graph) -> list[str]:
    out = ["\n## Непривязанные объекты и ограничения извлечения\n"]
    items = []
    for dev in graph["lights"] + graph["switches"] + graph["master_switches"]:
        if dev.get("room") is None:
            rb = dev.get("room_binding") or {}
            cand = rb.get("candidate") or rb.get("nearest_mark")
            items.append(f"{KIND_TEXT[dev['kind']].capitalize()} {_groups_phrase(dev.get('groups') or [])}: "
                         f"помещение не подтверждено (кандидат — {cand}); объект физически "
                         "найден в области плана.")
    for sym in graph["unresolved_symbols"]:
        if not (sym.get("room") or sym.get("room_candidate")):
            items.append("Символ, не совпавший с легендой, вне восстановленных областей помещений.")
    for z in graph["ceiling_zones"]:
        if z["room"] is None:
            items.append(f"Потолочная марка (тип {z['ceiling_type']}, отметка {z['elevation']}): "
                         f"помещение не подтверждено (ближайшая марка — {z.get('nearest_mark')}).")
    ledger_counts = collections.Counter(l["kind"] for l in graph["semantic_ledger"])
    if items:
        out.extend(f"- {i}" for i in items)
    else:
        out.append("- Все найденные объекты плана привязаны.")
    out.append("")
    out.append("Прочее непривязанное хранится в semantic_ledger (semantic_graph.json), "
               "по видам записей:")
    for kind, n in sorted(ledger_counts.items()):
        out.append(f"- {kind}: {n}")
    out.append("")
    out.append("> Непривязанность и «не извлечено» — ограничение векторной разметки, "
               "а НЕ дефект проекта: замечания из этого не формируются.")
    return out


def _render_block_control(graph, v) -> list[str]:
    out = ["\n## Контроль результата по всему блоку\n"]
    out.append(f"- Квартир: {v['apartments_total']}; помещений: {v['rooms_total']} "
               f"(с наименованием по ведомости: {v['rooms_named']}).")
    out.append(f"- Областей помещений восстановлено строго: {v['rooms_region_resolved']}; "
               f"остальные — открытая планировка (watershed) или не подтверждены.")
    out.append(f"- Потолочных марок: {v['ceiling_zones_total']}; световых точек: {v['lights_total']} "
               f"(в помещениях: {v['lights_in_rooms']}); выключателей: {v['switches_total']} "
               f"(в помещениях: {v['switches_in_rooms']}); мастер-выключателей: "
               f"{v['master_switches_total']}.")
    out.append(f"- Группы освещения: подтверждено {v['groups_confirmed']}, "
               f"неполных {v['groups_incomplete']} (нумерация групп локальна для квартиры).")
    out.append(f"- Размерных конструкций: {v['dimensions_total']}, из них привязано к "
               f"устройствам: {v['dimensions_device_bound']}.")
    out.append(f"- Неразрешённых символов: {v['unresolved_symbols_total']}; "
               f"GEOMETRY_CONFLICT: {v['conflicts_total']}; записей semantic_ledger: "
               f"{v['ledger_total']}.")
    out.append("- Время обработки — в metrics.json (ключ timing).")
    return out
