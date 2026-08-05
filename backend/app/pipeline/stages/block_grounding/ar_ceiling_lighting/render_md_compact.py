"""Компактное инженерное описание блока (для вкладки txt UI).

Формируется ТОЛЬКО из semantic_graph.json — извлечение, связи, tier и
конфликты не меняются; меняется представление. Полный технический рендер
остаётся в render_md.render_markdown и хранится отдельным артефактом.

Правила компактности: без координат, sym-ID, tier и техдампа; категории
помещения — по одной строке; каждая неоднозначность — ровно один раз в
наиболее подходящем месте; сводки вместо повторных таблиц.
"""
from __future__ import annotations

import collections
import re

from .legend import KIND_RU
from .render_md import KIND_TEXT

# короткие имена световых точек для строки «Связь»
KIND_SHORT = {
    "chandelier_output": "люстра",
    "light_output": "светильник",
    "wall_light_output": "настенный светильник",
}

MASTER_NOTE = ("Мастер-выключатель относится к квартире; конкретный перечень "
               "отключаемых групп на плане не указан.")

# порядок видов в строке условных обозначений
LEGEND_ORDER = (
    "light_output", "chandelier_output", "wall_light_output", "group_label",
    "switch_1", "switch_2", "switch_changeover", "master_switch",
    "smoke_detector", "ceiling_type_tag", "ceiling_elevation_tag",
)


def _kt(kind: str) -> str:
    return KIND_TEXT.get(kind, kind)


def _fmt_numbers(numbers: list[str]) -> str:
    """«1–4, 7 и 8»: диапазоны по возрастанию, последняя связка через «и»."""
    try:
        vals = sorted({int(n) for n in numbers})
    except (TypeError, ValueError):
        return ", ".join(sorted(set(numbers)))
    parts: list[str] = []
    i = 0
    while i < len(vals):
        j = i
        while j + 1 < len(vals) and vals[j + 1] == vals[j] + 1:
            j += 1
        if i == j:
            parts.append(str(vals[i]))
        elif j == i + 1:
            parts.extend((str(vals[i]), str(vals[j])))  # пара — через запятую
        else:
            parts.append(f"{vals[i]}–{vals[j]}")
        i = j + 1
    if len(parts) > 1:
        return ", ".join(parts[:-1]) + " и " + parts[-1]
    return parts[0] if parts else ""


def _groups_word(numbers: list[str], case: str = "nom") -> str:
    """case='nom': «группа 7» / «группы 3 и 4»; 'gen': «группы 7» / «групп 3 и 4»."""
    uniq = sorted(set(numbers), key=lambda v: (len(v), v))
    if not uniq:
        return "без извлечённого номера группы"
    if case == "gen":
        word = "группы" if len(uniq) == 1 else "групп"
    else:
        word = "группа" if len(uniq) == 1 else "группы"
    return f"{word} {_fmt_numbers(uniq)}"


def _area(v) -> str:
    return f"{str(v).replace('.', ',')} м²" if v else "не извлечено"


def _humanize_conflict(c: dict) -> str:
    what = str(c.get("what") or "")
    detail = str(c.get("detail") or "")
    m = re.search(r"конец размера «?(\d+)»?", what, re.I)
    if m:
        return (f"Конец размера {m.group(1)} мм равноудалён от двух устройств — "
                "привязка не выбрана.")
    if "пересечение символов" in what:
        return ("Мастер-выключатель пересекается с другим классифицированным "
                "устройством — интерпретация области не выбрана.")
    if "потолочные марки" in what:
        return f"{what.capitalize()}: границы зон не выделены — зоны не распространялись."
    if "легенда" in what:
        return f"{what.capitalize()} — приоритет не выбирался."
    text = f"{what}: {detail}".strip(": ")
    return re.sub(r"\bsym-\d+\b", "символ", text).capitalize() + "."


def render_markdown_compact(graph: dict) -> str:
    sheet = graph.get("sheet") or {}
    v = graph["validation"]
    out: list[str] = []

    # --- шапка листа ---
    title_sheet = f" — Лист {sheet['sheet_no']}" if sheet.get("sheet_no") else ""
    out.append(f"# План потолков и освещения{title_sheet}\n")
    head1 = [sheet.get("doc_number") or "обозначение не извлечено"]
    if sheet.get("building"):
        head1.append(f"Корпус {sheet['building']}")
    if sheet.get("floors_label"):
        head1.append(sheet["floors_label"])
    out.append("- " + " · ".join(head1))
    head2 = []
    if sheet.get("sheet_no"):
        tail = f" из {sheet['sheets_total']}" if sheet.get("sheets_total") else ""
        stage = f", стадия {sheet['stage']}" if sheet.get("stage") else ""
        head2.append(f"Лист {sheet['sheet_no']}{tail}{stage}")
    if sheet.get("zero_level"):
        head2.append(f"0,000 = {sheet['zero_level']}")
    if head2:
        out.append("- " + " · ".join(head2))
    status = graph.get("status") or "complete"
    if status == "complete":
        out.append("- Извлечение: полное (детерминированно, из векторного слоя).")
    else:
        out.append(f"- Извлечение: частичное ({status}).")
        for w in (graph.get("warnings") or [])[:3]:
            out.append(f"  - {w.split(':', 1)[-1].strip()}")
    out.append("")

    # --- общие правила листа (один раз) ---
    out.append("## Общие правила листа\n")
    for rule in graph.get("sheet_rules") or []:
        out.append(f"- {rule['text']}")
    out.append(f"- {MASTER_NOTE}")
    seen_kinds = []
    for tpl in graph.get("legend") or []:
        kind = tpl.get("kind")
        if kind and kind != "unresolved_legend_row" and kind in KIND_RU and kind not in seen_kinds:
            seen_kinds.append(kind)
    seen_kinds.sort(key=lambda k: LEGEND_ORDER.index(k) if k in LEGEND_ORDER else 99)
    legend_kinds = [KIND_RU[k] for k in seen_kinds]
    if legend_kinds:
        out.append("- Обозначения листа: " + " · ".join(legend_kinds) + ".")
    out.append("")

    # --- индексы ---
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

    # свет каждой подтверждённой группы: короткое имя вида + помещения света
    group_light_rooms: dict[str, list[str]] = {}
    group_light_kinds: dict[str, list[str]] = {}
    for g in graph["groups"]:
        rooms_l = sorted({lights_by_id[l].get("room") for l in g["lights"]
                          if lights_by_id[l].get("room")})
        group_light_rooms[g["group_id"]] = rooms_l
        group_light_kinds[g["group_id"]] = [
            KIND_SHORT.get(lights_by_id[l]["kind"], "светильник") for l in g["lights"]]

    for apt in graph["apartments"]:
        out.extend(_render_apartment(apt, rooms_by_apt[apt["id"]], graph, zones_by_id,
                                     lights_by_id, switches_by_id, masters_by_id,
                                     groups_by_apt[apt["id"]], group_light_rooms,
                                     group_light_kinds))

    out.extend(_render_tail(graph, v))
    return "\n".join(out).rstrip() + "\n"


def _render_apartment(apt, rooms, graph, zones_by_id, lights_by_id, switches_by_id,
                      masters_by_id, groups, group_light_rooms, group_light_kinds) -> list[str]:
    card = apt.get("card") or {}
    title = f"\n## Квартира {apt['id']}"
    if card.get("type"):
        title += f" — тип {card['type']}"
    out = [title]
    if card:
        out.append(f"Площади: жилая {_area(card.get('living_area'))} · "
                   f"квартиры {_area(card.get('apartment_area'))} · "
                   f"общая {_area(card.get('total_area'))}.")
    master_ids = apt.get("master_switches") or []
    master_txt = ""
    if master_ids:
        places = []
        for mid in master_ids:
            m = masters_by_id[mid]
            place = m.get("room") or "помещение не подтверждено"
            places.append(place + (", у входа" if m.get("near_door") else ""))
        master_txt = " · мастер-выключатель: " + "; ".join(places)
    out.append(f"Состав: {len(rooms)} помещений{master_txt}.")
    out.append("")

    rooms = sorted(rooms, key=lambda r: r["room_suffix"])
    for room in rooms:
        out.extend(_render_room(room, graph, zones_by_id, lights_by_id, switches_by_id,
                                groups, group_light_rooms, group_light_kinds))

    out.extend(_render_groups_summary(groups, switches_by_id, group_light_rooms))
    return out


def _room_ceiling_line(room, zones_by_id) -> tuple[str, str | None]:
    """(строка «Потолок», отметка помещения — если единственная)."""
    zones = [zones_by_id[z] for z in room["ceiling_zones"]]
    if not zones:
        return "- **Потолок:** марка не подтверждена.", None
    if len(zones) == 1:
        z = zones[0]
        return f"- **Потолок:** тип {z['ceiling_type']}, отметка {z['elevation']}.", z["elevation"]
    kinds = " и ".join(f"тип {z['ceiling_type']} ({z['elevation']})" for z in zones)
    return (f"- **Потолок:** {len(zones)} марки — {kinds}; границы зон не выделены "
            "— требует проверки.", None)


def _render_room(room, graph, zones_by_id, lights_by_id, switches_by_id,
                 groups, group_light_rooms, group_light_kinds) -> list[str]:
    name = room["name"] or "наименование не подтверждено"
    out = [f"### {room['mark']} — {name}"]

    ceiling_line, _room_elev = _room_ceiling_line(room, zones_by_id)
    out.append(ceiling_line)

    # --- Свет: агрегировано по виду и группам ---
    lights = [lights_by_id[i] for i in room["lights"]]
    if lights:
        agg = collections.Counter()
        for light in lights:
            agg[(light["kind"], tuple(light.get("groups") or ()))] += 1
        parts = []
        for (kind, grp_nums), n in sorted(agg.items()):
            piece = _kt(kind)
            if n > 1:
                piece = f"{n} × {piece}"
            piece += f", {_groups_word(list(grp_nums))}"
            parts.append(piece)
        out.append("- **Свет:** " + "; ".join(parts) + ".")

    # --- Управление: выключатели помещения; свет в другом помещении — хвостом ---
    switches = [switches_by_id[i] for i in room["switches"]]
    if switches:
        parts = []
        for sw in sorted(switches, key=lambda x: (x["kind"], x["groups"])):
            piece = f"{_kt(sw['kind'])} {_groups_word(sw['groups'], case='gen')}"
            other_rooms = sorted({
                r for gid in (sw.get("group_ids") or [])
                for r in group_light_rooms.get(gid, ()) if r != room["mark"]})
            if other_rooms:
                piece += f" — свет в {', '.join(other_rooms)}"
            parts.append(piece)
        out.append("- **Управление:** " + "; ".join(parts) + ".")

    # --- Связь: подтверждённые группы этого помещения одной строкой ---
    chains = []
    for g in groups:
        if g["state"] != "confirmed":
            continue
        room_switches = [s for s in g["switches"] if s in room["switches"]]
        light_here = room["mark"] in group_light_rooms.get(g["group_id"], ())
        if not room_switches and not light_here:
            continue
        if not room_switches:
            continue  # связь описывается у выключателя, у света не дублируется
        kinds = group_light_kinds.get(g["group_id"]) or []
        light_short = ", ".join(sorted(set(kinds))) or "свет"
        chains.append(f"выключатель → группа {g['number']} → {light_short}")
    if chains:
        out.append("- **Связь:** " + "; ".join(chains) + ".")

    # --- Привязки: подтверждённые (tier 3) одной строкой; tier 2 → проверка ---
    confirmed_dims = []
    review = []
    for sw in switches:
        for dim in sw.get("dimensions", []):
            target = ("до стены/грани проёма" if dim["to"] == "wall_or_opening"
                      else "до оси соседнего устройства" if dim["to"] == "device_axis"
                      else None)
            if target is None:
                continue
            if dim["tier"] >= 3:
                confirmed_dims.append(f"{dim['value_mm']} мм {target}")
            else:
                why = ("масштабная сверка не пройдена"
                       if dim.get("binding") != "proximity_only"
                       else "связь только по близости")
                review.append(f"размер {dim['value_mm']} мм — {why}, привязка не утверждается")
    if confirmed_dims:
        out.append("- **Привязка:** " + "; ".join(sorted(set(confirmed_dims))) + ".")
    if review:
        out.append("- Требует проверки: " + "; ".join(sorted(set(review))) + ".")
    out.append("")
    return out


def _render_groups_summary(groups, switches_by_id, group_light_rooms) -> list[str]:
    if not groups:
        return []
    confirmed = [g for g in groups if g["state"] == "confirmed"]
    sw_only = [g for g in groups if g["state"] == "switches_only"]
    li_only = [g for g in groups if g["state"] == "lights_only"]
    other = [g for g in groups if g["state"] not in ("confirmed", "switches_only", "lights_only")]
    parts = []
    if confirmed:
        parts.append(f"{_fmt_numbers([g['number'] for g in confirmed])} — связи подтверждены")
    if sw_only:
        parts.append(f"{_fmt_numbers([g['number'] for g in sw_only])} — найдены только "
                     "выключатели, световые выводы не привязаны")
    if li_only:
        parts.append(f"{_fmt_numbers([g['number'] for g in li_only])} — найдены только "
                     "световые выводы, выключатели не привязаны")
    if other:
        parts.append(f"{_fmt_numbers([g['number'] for g in other])} — неполные")
    out = ["**Группы квартиры:** " + "; ".join(parts) + "."]
    # доп. строки только для нескольких точек управления
    multi = [g for g in confirmed if len(g["switches"]) > 1]
    for g in multi:
        sw_rooms = sorted({switches_by_id[s].get("room") for s in g["switches"]
                           if switches_by_id[s].get("room")})
        out.append(f"- Группа {g['number']}: {len(g['switches'])} точки управления"
                   + (f" ({', '.join(sw_rooms)})" if sw_rooms else "") + ".")
    out.append("")
    return out


def _render_tail(graph, v) -> list[str]:
    out = ["\n## Непривязанное и конфликты\n"]
    items = []
    unassigned_dev = collections.Counter()
    for dev in graph["lights"] + graph["switches"] + graph["master_switches"]:
        if dev.get("room") is None:
            unassigned_dev[_kt(dev["kind"])] += 1
    if unassigned_dev:
        pieces = [f"{name} ×{n}" if n > 1 else name for name, n in sorted(unassigned_dev.items())]
        items.append("Устройства без подтверждённого помещения: " + ", ".join(pieces)
                     + " (полосы неопределённости открытых планировок).")
    zones_unassigned = sum(1 for z in graph["ceiling_zones"] if z["room"] is None)
    if zones_unassigned:
        items.append(f"Потолочные марки без подтверждённого помещения: {zones_unassigned}.")
    for c in graph.get("conflicts") or []:
        items.append(_humanize_conflict(c))
    ledger_counts = collections.Counter(l["kind"] for l in graph["semantic_ledger"])
    known = {"number_without_construction": "числа без размерной конструкции",
             "colored_linework": "цветные линии без якорного элемента",
             "group_label_unbound": "подписи групп без символа",
             "unresolved_symbol": "неклассифицированные символы"}
    ledger_bits = [f"{known.get(k, k)} — {n}" for k, n in sorted(ledger_counts.items())
                   if k in known and n]
    if ledger_bits:
        items.append("Прочее (в semantic_ledger): " + "; ".join(ledger_bits) + ".")
    out.extend(f"- {i}" for i in items)
    if not items:
        out.append("- Все найденные объекты плана привязаны.")
    out.append("")

    out.append("## Итог блока\n")
    wall = v.get("wall_lights_total", 0)
    out.append(f"{v['apartments_total']} квартир · {v['rooms_total']} помещений · "
               f"{v['ceiling_zones_total']} потолочных марок · "
               f"{v['lights_total']} световых точек (из них {wall} настенных) · "
               f"{v['switches_total']} выключателей · "
               f"{v['groups_confirmed']} подтверждённых и {v['groups_incomplete']} "
               f"неполных групп · {v['dimensions_total']} размерных конструкций "
               f"({v['dimensions_device_bound']} связаны с устройствами) · "
               f"{v['conflicts_total']} геометрических конфликта.")
    return out
