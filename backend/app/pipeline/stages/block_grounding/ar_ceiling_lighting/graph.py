"""Сборка семантического графа «квартира → помещение → потолочная зона →
световой вывод → группа → выключатель → размерная привязка».

Provenance-шкала (бриф): 5 — семантика кода; 4 — текстовая аннотация/
таблица листа; 3 — подтверждённая CAD-геометрия; 2 — близость (только в
«Требует проверки»); 1 — инвентарь (semantic_ledger); 0 — не извлечено.
Конфликт двух сильных толкований → GEOMETRY_CONFLICT, без молчаливого
выбора. «Не извлечено» ≠ «не предусмотрено проектом».
"""
from __future__ import annotations

import collections
import math

from .rooms import nearest_mark, room_of_point
from .spatial import SpatialIndex, bbox_gap

LIGHT_KINDS = ("light_output", "chandelier_output", "wall_light_output")
SWITCH_KINDS = ("switch_1", "switch_2", "switch_changeover")
OTHER_DEVICE_KINDS = ("smoke_detector",)

GROUP_BIND_GAP = 7.0       # pt: подпись группы должна примыкать к символу
GROUP_AMBIGUITY_MARGIN = 1.4
DOOR_NEAR_MM = 500.0


def assemble(cp, inv, ref, syms, ceil_markers, ceil_unpaired, labels, dims,
             consumed_labels, marks, marks_rejected, room_data, dim_conflicts=None) -> dict:
    conflicts: list[dict] = list(dim_conflicts or [])
    conflicts.extend(ref.get("conflicts") or [])
    ledger: list[dict] = []

    rooms = _build_rooms(ref, marks, room_data)
    apartments = _build_apartments(ref, rooms)

    linework = [s for s in syms if s["kind"] == "colored_linework"]
    syms = [s for s in syms if s["kind"] != "colored_linework"]
    for lw in linework:
        ledger.append({"kind": "colored_linework", "bbox": lw["bbox"],
                       "state": "no_anchor_element",
                       "detail": f"линий {lw['signature']['n_axis_lines'] + lw['signature']['n_diag_lines']}"})

    lights, switches, masters, others, unresolved_syms = _split_devices(syms)
    for dev in lights + switches + masters + others:
        _bind_room(dev, room_data, marks)

    for sym in unresolved_syms:
        if sym.get("reason") == "multiple_templates_match":
            conflicts.append({
                "type": "GEOMETRY_CONFLICT",
                "what": "классификация символа",
                "bbox": sym["bbox"],
                "candidates": sym.get("matched_kinds") or [],
                "detail": "несколько сильных шаблонов легенды претендуют на один символ — "
                          "тип не выбран",
            })
    _master_overlap_conflicts(switches + lights, masters, conflicts)

    _bind_group_labels(labels, consumed_labels, lights, switches, masters, conflicts, ledger,
                       unresolved_syms)

    zones = _build_ceiling_zones(ceil_markers, room_data, marks, rooms, conflicts)
    _attach_lights_to_zones(lights, zones, rooms)

    for sym in unresolved_syms:
        mark, quality = room_of_point(room_data, (sym["bbox"][0] + sym["bbox"][2]) / 2,
                                      (sym["bbox"][1] + sym["bbox"][3]) / 2)
        sym["room"] = mark if quality in ("strict", "watershed_strong") else None
        sym["room_candidate"] = mark

    groups = _build_groups(lights, switches, rooms, conflicts)
    _attach_unresolved_to_groups(unresolved_syms, groups, rooms)
    _bind_masters(masters, rooms, apartments)
    _door_adjacency(inv, cp, switches + masters, dims)
    _attach_dimensions(dims, lights, switches, masters)
    for dev in others:
        dev.setdefault("groups", [])

    for sym in unresolved_syms:
        ledger.append({"kind": "unresolved_symbol", "bbox": sym["bbox"],
                       "state": sym["reason"], "detail": sym.get("detail") or "",
                       "residuals": sym.get("residuals") or []})
    for f in ceil_unpaired:
        ledger.append({"kind": "ceiling_frame_unpaired", "bbox": f["bbox"],
                       "state": "no_composite_pair", "detail": f.get("text") or ""})
    for rej in marks_rejected:
        if rej["why"] == "scope=reference":
            continue  # копии марок в ведомости/легенде — штатный reference
        ledger.append({"kind": "room_mark_rejected", "bbox": rej["bbox"],
                       "state": rej["why"], "detail": rej["mark"]})
    for dev in lights + switches + masters + others:
        if dev.get("room") is None:
            ledger.append({"kind": f"{dev['category']}_room_unassigned", "bbox": dev["bbox"],
                           "state": dev["room_binding"]["state"],
                           "detail": f"{dev['id']}: ближайшая марка {dev['room_binding'].get('nearest_mark')}"})

    _link_rooms_to_apartments(rooms, apartments)
    graph = {
        "schema_version": 1,
        "profile_id": "ar_ceiling_lighting",
        "sheet": _sheet_block(ref, marks),
        "legend": [{"kind": t["kind"], "label": t["label"], "signature": t["signature"]}
                   for t in ref["templates"]],
        "sheet_rules": ref["sheet_rules"],
        "apartments": apartments,
        "rooms": rooms,
        "ceiling_zones": zones,
        "lights": lights,
        "switches": switches,
        "master_switches": masters,
        "other_devices": others,
        "groups": groups,
        "dimensions": dims,
        "unresolved_symbols": unresolved_syms,
        "conflicts": conflicts,
        "semantic_ledger": ledger,
        "warnings": list(ref.get("warnings") or []),
        "legend_sections": ref.get("legend_sections") or [],
    }
    graph["nodes"], graph["edges"] = _flat_projection(cp, graph)
    graph["validation"] = _validation(graph, room_data)
    return graph


# ----------------------------------------------------------- помещения

def _build_rooms(ref, marks, room_data) -> list[dict]:
    schedule = ref["room_schedule"]
    rooms = []
    for m in marks:
        region = room_data["regions"].get(m["mark"], {})
        sched = schedule.get(m["mark"])
        rooms.append({
            "mark": m["mark"],
            "apartment": m["apartment"],
            "room_suffix": m["room_suffix"],
            "name": sched["name"] if sched else None,
            "name_binding": {
                "state": "schedule_row_match" if sched else "not_extracted",
                "tier": 4 if sched else 0,
                "evidence": [f"строка ведомости «{m['mark']}»"] if sched else [],
            },
            "apartment_binding": {"state": "mark_semantics", "tier": 5,
                                  "evidence": [f"марка {m['mark']}"]},
            "region_state": region.get("state", "not_built"),
            "region_bbox": region.get("bbox"),
            "region_cells": region.get("cells", 0),
            "merged_with": region.get("merged_with"),
            "bbox": m["bbox"],
            "center": m["center"],
            "ceiling_zones": [],
            "lights": [],
            "switches": [],
            "master_switches": [],
        })
    return rooms


def _build_apartments(ref, rooms) -> list[dict]:
    cards = {c["apartment"]: c for c in ref["apartment_cards"]}
    apts: dict[str, dict] = {}
    for room in rooms:
        apt = apts.setdefault(room["apartment"], {
            "id": room["apartment"], "rooms": [], "card": None,
            "card_binding": {"state": "not_extracted", "tier": 0, "evidence": []},
        })
        apt["rooms"].append(room["mark"])
    for apt_id, apt in apts.items():
        card = cards.get(apt_id)
        if card:
            apt["card"] = {k: card[k] for k in
                           ("type", "living_area", "apartment_area", "total_area",
                            "areas_raw", "type_raw", "requires_review")}
            apt["card_binding"] = {"state": "perimeter_card_number_match", "tier": 4,
                                   "evidence": [f"карточка «{apt_id}» на периметре плана"]}
    for card_id, card in cards.items():
        if card_id not in apts:
            apts[card_id] = {"id": card_id, "rooms": [], "card": {
                "type": card["type"], "living_area": card["living_area"],
                "apartment_area": card["apartment_area"], "total_area": card["total_area"],
                "areas_raw": card["areas_raw"], "type_raw": card["type_raw"],
                "requires_review": True},
                "card_binding": {"state": "card_without_rooms", "tier": 1, "evidence": []}}
    out = [apts[k] for k in sorted(apts)]
    for apt in out:
        apt["rooms"].sort(key=lambda mk: int(mk.rsplit(".", 1)[1]))
    return out


def _link_rooms_to_apartments(rooms, apartments) -> None:
    by_apt = {a["id"]: a for a in apartments}
    for apt in apartments:
        apt.setdefault("groups", [])
        apt.setdefault("master_switches", [])
    for room in rooms:
        by_apt[room["apartment"]].setdefault("rooms", [])


# ----------------------------------------------------------- устройства

def _split_devices(syms) -> tuple[list, list, list, list, list]:
    lights, switches, masters, others, unresolved = [], [], [], [], []
    ordered = sorted(syms, key=lambda s: (round(s["center"][1], 1), round(s["center"][0], 1)))
    for sym in ordered:
        base = {
            "symbol_id": sym["symbol_id"], "kind": sym["kind"], "bbox": sym["bbox"],
            "center": sym["center"], "signature": sym.get("signature"),
            "classification_source": sym.get("classification_source") or "sheet_legend",
        }
        if sym.get("label_overlay_circles"):
            base["label_overlay_circles"] = sym["label_overlay_circles"]
        if sym["kind"] in LIGHT_KINDS:
            base.update({"id": f"light-{len(lights) + 1}", "category": "light",
                         "groups": [], "group_bindings": []})
            lights.append(base)
        elif sym["kind"] in SWITCH_KINDS:
            base.update({"id": f"switch-{len(switches) + 1}", "category": "switch",
                         "gangs": {"switch_1": 1, "switch_2": 2, "switch_changeover": 1}[sym["kind"]],
                         "groups": [], "group_bindings": [], "dimensions": []})
            switches.append(base)
        elif sym["kind"] == "master_switch":
            base.update({"id": f"master-{len(masters) + 1}", "category": "master_switch",
                         "groups": [], "group_bindings": [], "dimensions": []})
            masters.append(base)
        elif sym["kind"] in OTHER_DEVICE_KINDS:
            base.update({"id": f"other-{len(others) + 1}", "category": "other_device",
                         "groups": [], "group_bindings": []})
            others.append(base)
        else:
            unresolved.append(sym)
    return lights, switches, masters, others, unresolved


def _master_overlap_conflicts(devices, masters, conflicts) -> None:
    """Пересечение классифицированного устройства с мастер-выключателем —
    два сильных толкования одной области: явный конфликт, не молчаливый
    выбор одного из символов."""
    for m in masters:
        mb = m["bbox"]
        for dev in devices:
            b = dev["bbox"]
            ix = min(mb[2], b[2]) - max(mb[0], b[0])
            iy = min(mb[3], b[3]) - max(mb[1], b[1])
            if ix > 0.5 and iy > 0.5:
                conflicts.append({
                    "type": "GEOMETRY_CONFLICT",
                    "what": "пересечение символов устройств",
                    "bbox": (max(mb[0], b[0]), max(mb[1], b[1]),
                             min(mb[2], b[2]), min(mb[3], b[3])),
                    "candidates": sorted([m["symbol_id"], dev["symbol_id"]]),
                    "detail": "область мастер-выключателя пересекается с другим "
                              "классифицированным устройством",
                })


def _bind_room(dev, room_data, marks) -> None:
    mark, quality = room_of_point(room_data, *dev["center"])
    near, dist = nearest_mark(marks, *dev["center"])
    if mark is not None and quality in ("strict", "watershed_strong"):
        state = "point_in_room_region" if quality == "strict" else "open_plan_dominant_zone"
        dev["room"] = mark
        dev["room_binding"] = {"state": state, "tier": 3,
                               "evidence": [f"центр в области {mark} ({quality})"]}
    elif mark is not None:
        # слабое доминирование в открытой планировке — только «Требует проверки»
        dev["room"] = None
        dev["room_binding"] = {"state": "open_plan_weak_dominance", "tier": 2,
                               "candidate": mark, "nearest_mark": near,
                               "nearest_dist_pt": round(dist, 1), "evidence": []}
    else:
        dev["room"] = None
        dev["room_binding"] = {"state": "room_region_unresolved", "tier": 2,
                               "nearest_mark": near, "nearest_dist_pt": round(dist, 1),
                               "evidence": []}


def _bind_group_labels(labels, consumed, lights, switches, masters, conflicts, ledger,
                       unresolved_syms=()) -> None:
    devices = lights + switches + masters
    index = SpatialIndex(cell=14.0)
    for i, dev in enumerate(devices):
        index.insert(i, dev["bbox"])
    unres_index = SpatialIndex(cell=14.0)
    unresolved_syms = list(unresolved_syms)
    for i, sym in enumerate(unresolved_syms):
        unres_index.insert(i, sym["bbox"])
    for lab in labels:
        if lab["label_id"] in consumed:
            continue
        if lab.get("color_family") != "red" or len(lab["value"]) > 2:
            # чёрные числа — размерные значения; без конструкции остаются
            # в инвентаре и не становятся ни размером, ни группой
            ledger.append({"kind": "number_without_construction", "bbox": lab["bbox"],
                           "state": "not_a_dimension_not_a_group", "detail": lab["value"]})
            continue
        # зазор нормируется кеглем подписи: крупные цифры у светильников
        # стоят дальше от символа, чем мелкие внутри выключателей
        lab_h = max(lab["bbox"][3] - lab["bbox"][1], lab["bbox"][2] - lab["bbox"][0])
        pad = max(GROUP_BIND_GAP, 1.25 * lab_h)
        cx = (lab["bbox"][0] + lab["bbox"][2]) / 2
        cy = (lab["bbox"][1] + lab["bbox"][3]) / 2
        containers = []
        cand = []
        for i in index.query(lab["bbox"], pad=pad):
            dev = devices[i]
            b = dev["bbox"]
            if b[0] <= cx <= b[2] and b[1] <= cy <= b[3]:
                containers.append(dev)
            gap = bbox_gap(lab["bbox"], dev["bbox"])
            if gap <= pad:
                cand.append((round(gap, 2), dev))
        if len(containers) == 1:
            # цифра стоит внутри контура символа (диалект CAD) — однозначно
            dev = containers[0]
            dev["groups"].append(lab["value"])
            dev["group_bindings"].append({
                "number": lab["value"], "label_id": lab["label_id"], "gap_pt": 0.0,
                "tier": 3, "state": "label_inside_symbol",
                "split_from_span": lab.get("split_from_span", False),
            })
            continue
        if len(containers) > 1:
            conflicts.append({
                "type": "GEOMETRY_CONFLICT",
                "what": f"подпись группы «{lab['value']}»",
                "bbox": lab["bbox"],
                "candidates": [d["id"] for d in containers],
                "detail": "цифра внутри двух перекрывающихся символов",
            })
            continue
        cand.sort(key=lambda item: (item[0], item[1]["id"]))
        if not cand:
            near_unres = None
            for i in unres_index.query(lab["bbox"], pad=pad):
                gap = bbox_gap(lab["bbox"], unresolved_syms[i]["bbox"])
                if gap <= pad and (near_unres is None or gap < near_unres[0]):
                    near_unres = (gap, unresolved_syms[i])
            if near_unres is not None:
                sym = near_unres[1]
                sym.setdefault("adjacent_group_numbers", []).append(lab["value"])
                ledger.append({"kind": "group_label_at_unresolved_symbol", "bbox": lab["bbox"],
                               "state": "symbol_not_classified", "detail": lab["value"],
                               "symbol_id": sym["symbol_id"]})
            else:
                ledger.append({"kind": "group_label_unbound", "bbox": lab["bbox"],
                               "state": "no_symbol_within_gap", "detail": lab["value"]})
            continue
        if len(cand) > 1 and cand[1][0] - cand[0][0] < GROUP_AMBIGUITY_MARGIN:
            conflicts.append({
                "type": "GEOMETRY_CONFLICT",
                "what": f"подпись группы «{lab['value']}»",
                "bbox": lab["bbox"],
                "candidates": [c[1]["id"] for c in cand[:3]],
                "detail": "два символа на сопоставимом расстоянии — привязка не выбрана",
            })
            continue
        dev = cand[0][1]
        dev["groups"].append(lab["value"])
        dev["group_bindings"].append({
            "number": lab["value"], "label_id": lab["label_id"], "gap_pt": cand[0][0],
            "tier": 3, "state": "label_adjacent_to_symbol",
            "split_from_span": lab.get("split_from_span", False),
        })
    for dev in devices:
        dev["groups"] = sorted(set(dev["groups"]), key=lambda v: (len(v), v))


# ------------------------------------------------------------- потолки

def _build_ceiling_zones(markers, room_data, marks, rooms, conflicts) -> list[dict]:
    zones = []
    per_room = collections.Counter()
    marker_room = {}
    for mk in markers:
        room, quality = room_of_point(room_data, *mk["center"])
        if quality == "watershed_weak":
            room = None  # марка в полосе неопределённости открытой планировки
        marker_room[mk["marker_id"]] = room
        if room:
            per_room[room] += 1
    for mk in markers:
        room = marker_room[mk["marker_id"]]
        near, dist = nearest_mark(marks, *mk["center"])
        if room is None:
            extent, state, tier = None, "room_region_unresolved", 2
        elif per_room[room] == 1:
            extent, state, tier = "room", "single_ceiling_tag_in_room", 3
        else:
            extent, state, tier = None, "multiple_tags_zone_boundary_not_extracted", 2
            conflicts.append({
                "type": "GEOMETRY_CONFLICT",
                "what": f"потолочные марки в помещении {room}",
                "bbox": mk["bbox"],
                "detail": f"в помещении {per_room[room]} марок потолка, граница зон "
                          "в векторном слое не выделена — зоны не распространяются",
            })
        zones.append({
            "zone_id": mk["marker_id"],
            "ceiling_type": mk["ceiling_type"],
            "elevation": mk["elevation"],
            "room": room,
            "nearest_mark": near if room is None else None,
            "extent": extent,
            "state": state,
            "tier": tier,
            "bbox": mk["bbox"],
            "center": mk["center"],
            "evidence": mk["evidence"],
        })
    by_room = collections.defaultdict(list)
    for z in zones:
        if z["room"]:
            by_room[z["room"]].append(z["zone_id"])
    for room in rooms:
        room["ceiling_zones"] = by_room.get(room["mark"], [])
    return zones


def _attach_lights_to_zones(lights, zones, rooms) -> None:
    room_zones = collections.defaultdict(list)
    for z in zones:
        if z["room"] and z["extent"] == "room":
            room_zones[z["room"]].append(z)
    for light in lights:
        light["ceiling_zone"] = None
        if light.get("room") and len(room_zones.get(light["room"], [])) == 1:
            z = room_zones[light["room"]][0]
            light["ceiling_zone"] = {"zone_id": z["zone_id"], "ceiling_type": z["ceiling_type"],
                                     "elevation": z["elevation"], "tier": min(3, z["tier"])}


# -------------------------------------------------------------- группы

def _build_groups(lights, switches, rooms, conflicts) -> list[dict]:
    room_apt = {r["mark"]: r["apartment"] for r in rooms}
    groups: dict[str, dict] = {}

    def touch(apt, number):
        gid = f"{apt}:{number}"
        return groups.setdefault(gid, {
            "group_id": gid, "apartment": apt, "number": number,
            "lights": [], "switches": [], "rooms": set(), "state": "incomplete",
        })

    for light in lights:
        apt = room_apt.get(light.get("room"))
        for number in light["groups"]:
            if apt is None:
                continue
            g = touch(apt, number)
            g["lights"].append(light["id"])
            if light.get("room"):
                g["rooms"].add(light["room"])
            light.setdefault("group_ids", []).append(g["group_id"])
    for sw in switches:
        apt = room_apt.get(sw.get("room"))
        for number in sw["groups"]:
            if apt is None:
                continue
            g = touch(apt, number)
            g["switches"].append(sw["id"])
            if sw.get("room"):
                g["rooms"].add(sw["room"])
            sw.setdefault("group_ids", []).append(g["group_id"])

    out = []
    for gid in sorted(groups, key=lambda g: (groups[g]["apartment"], int(groups[g]["number"]))):
        g = groups[gid]
        g["rooms"] = sorted(g["rooms"])
        if g["lights"] and g["switches"]:
            g["state"] = "confirmed"
        elif g["lights"]:
            g["state"] = "lights_only"
        else:
            g["state"] = "switches_only"
        out.append(g)

    by_room = collections.defaultdict(lambda: {"lights": [], "switches": [], "masters": []})
    for light in lights:
        if light.get("room"):
            by_room[light["room"]]["lights"].append(light["id"])
    for sw in switches:
        if sw.get("room"):
            by_room[sw["room"]]["switches"].append(sw["id"])
    for room in rooms:
        room["lights"] = by_room[room["mark"]]["lights"]
        room["switches"] = by_room[room["mark"]]["switches"]
    return out


def _attach_unresolved_to_groups(unresolved_syms, groups, rooms) -> None:
    """Неразрешённый символ с примыкающими цифрами групп — честный участник
    группы без типа: группа не «неполная молча», а ждёт классификации."""
    room_apt = {r["mark"]: r["apartment"] for r in rooms}
    by_id = {g["group_id"]: g for g in groups}
    for sym in unresolved_syms:
        numbers = sorted(set(sym.get("adjacent_group_numbers") or []))
        apt = room_apt.get(sym.get("room"))
        if not numbers or apt is None:
            continue
        for number in numbers:
            gid = f"{apt}:{number}"
            g = by_id.get(gid)
            if g is None:
                g = {"group_id": gid, "apartment": apt, "number": number,
                     "lights": [], "switches": [], "rooms": [], "state": "incomplete"}
                by_id[gid] = g
                groups.append(g)
            g.setdefault("unresolved_participants", []).append(sym["symbol_id"])
            if sym.get("room") and sym["room"] not in g["rooms"]:
                g["rooms"] = sorted({*g["rooms"], sym["room"]})
    groups.sort(key=lambda g: (g["apartment"], int(g["number"])))


def _bind_masters(masters, rooms, apartments) -> None:
    room_apt = {r["mark"]: r["apartment"] for r in rooms}
    by_apt = {a["id"]: a for a in apartments}
    for m in masters:
        apt = room_apt.get(m.get("room"))
        m["apartment"] = apt
        m["intended_scope"] = {
            "scope": "apartment" if apt else None,
            "tier": 3 if apt else 2,
            "state": "master_in_apartment_room" if apt else "room_region_unresolved",
            "note": "по условным обозначениям листа относится к квартире; перечень "
                    "отключаемых групп на плане отдельно не показан",
        }
        if apt:
            by_apt[apt].setdefault("master_switches", []).append(m["id"])
        if m.get("room"):
            for room in rooms:
                if room["mark"] == m["room"]:
                    room["master_switches"].append(m["id"])


# ---------------------------------------------------- двери и размеры

def _door_adjacency(inv, cp, devices, dims) -> None:
    scale = None
    for d in dims:
        if d.get("sheet_scale_mm_per_pt"):
            scale = d["sheet_scale_mm_per_pt"]
            break
    doors = [d for d in inv["drawings"] if "DOOR" in (d["layer"] or "").upper()]
    index = SpatialIndex(cell=30.0)
    for i, d in enumerate(doors):
        index.insert(i, d["rect"])
    for dev in devices:
        pad = (DOOR_NEAR_MM / scale) if scale else 30.0
        best = None
        for i in index.query(dev["bbox"], pad=pad):
            gap = bbox_gap(dev["bbox"], doors[i]["rect"])
            if best is None or gap < best:
                best = gap
        if best is not None and scale:
            dev["door_gap_mm"] = round(best * scale)
        dev["near_door"] = bool(best is not None and (not scale or best * (scale or 1) <= DOOR_NEAR_MM))


def _attach_dimensions(dims, lights, switches, masters) -> None:
    """Раздача размеров устройствам.

    tier 3 — только подтверждённая цепочка (extension_chain_confirmed)
    И пройденная масштабная сверка; конец-«кандидат по близости» даёт
    tier 2 / requires_review и живёт только в «Требует проверки»."""
    sym_to_dev = {d["symbol_id"]: d for d in switches + masters + lights}
    for dim in dims:
        attached = [e for e in dim["ends"] if e["attached_to"] == "device_axis"]
        candidates = [e for e in dim["ends"] if e["attached_to"] == "device_axis_candidate"]
        rest = [e for e in dim["ends"]
                if e["attached_to"] not in ("device_axis", "device_axis_candidate")]
        dim["binding_state"] = "unbound"
        if not attached and not candidates:
            continue
        target_desc = "wall_or_opening" if any(e["attached_to"] == "wall_or_opening" for e in rest) else \
            ("device_axis" if len(attached) + len(candidates) == 2 else "unresolved")
        dim["binding_state"] = f"device_to_{target_desc}" if attached else "device_candidate_only"
        for e in attached + candidates:
            dev = sym_to_dev.get(e.get("device_id"))
            if dev is None:
                continue
            end_tier = e.get("binding_tier", 2)
            confirmed = end_tier >= 3 and dim.get("scale_consistent", False)
            dev.setdefault("dimensions", []).append({
                "dim_id": dim["dim_id"], "value_mm": dim["value_mm"],
                "to": target_desc, "orientation": dim["orientation"],
                "tier": 3 if confirmed else 2,
                "binding": e.get("binding"),
                "requires_review": not confirmed,
                "scale_consistent": dim.get("scale_consistent", False),
            })


# --------------------------------------------------- плоская проекция

def _flat_projection(cp, graph) -> tuple[list, list]:
    width = cp.media_rect[2] or 1.0
    height = cp.media_rect[3] or 1.0
    nodes, edges = [], []

    def node(nid, label, node_type, bbox, tier, state):
        b = bbox or (0, 0, 0, 0)
        nodes.append({"id": nid, "label": label, "node_type": node_type,
                      "x": round((b[0] + b[2]) / 2, 2), "y": round((b[1] + b[3]) / 2, 2),
                      "bbox_page": [round(b[0] / width, 6), round(b[1] / height, 6),
                                    round(b[2] / width, 6), round(b[3] / height, 6)],
                      "field_state": "present", "tier": tier, "state": state})

    def edge(src, dst, edge_type, tier, state):
        edges.append({"id": f"edge-{len(edges) + 1}", "source": src, "target": dst,
                      "edge_type": edge_type, "tier": tier, "state": state})

    for apt in graph["apartments"]:
        node(f"apt-{apt['id']}", f"Квартира {apt['id']}", "apartment", None,
             apt["card_binding"]["tier"], apt["card_binding"]["state"])
    for room in graph["rooms"]:
        node(room["mark"], room["mark"], "room", room["bbox"], 5, room["region_state"])
        edge(f"apt-{room['apartment']}", room["mark"], "contains", 5, "mark_semantics")
    for z in graph["ceiling_zones"]:
        node(z["zone_id"], f"потолок тип {z['ceiling_type']} {z['elevation'] or ''}".strip(),
             "ceiling_zone", z["bbox"], z["tier"], z["state"])
        if z["room"]:
            edge(z["room"], z["zone_id"], "contains", z["tier"], z["state"])
    for light in graph["lights"]:
        node(light["id"], light["kind"], "lighting_point", light["bbox"], 3, "classified_by_legend")
        if light.get("room"):
            edge(light["room"], light["id"], "contains",
                 light["room_binding"]["tier"], light["room_binding"]["state"])
    for sw in graph["switches"]:
        node(sw["id"], sw["kind"], "switch", sw["bbox"], 3, "classified_by_legend")
        if sw.get("room"):
            edge(sw["room"], sw["id"], "contains",
                 sw["room_binding"]["tier"], sw["room_binding"]["state"])
    for m in graph["master_switches"]:
        node(m["id"], "master_switch", "master_switch", m["bbox"], 3, "classified_by_legend")
        if m.get("apartment"):
            edge(m["id"], f"apt-{m['apartment']}", "intended_scope",
                 m["intended_scope"]["tier"], m["intended_scope"]["state"])
    for dev in graph.get("other_devices") or []:
        node(dev["id"], dev["kind"], "other_device", dev["bbox"], 3, "classified_by_legend")
        if dev.get("room"):
            edge(dev["room"], dev["id"], "contains",
                 dev["room_binding"]["tier"], dev["room_binding"]["state"])
    for g in graph["groups"]:
        node(g["group_id"], f"группа {g['number']} кв. {g['apartment']}", "lighting_group",
             None, 3, g["state"])
        for lid in g["lights"]:
            edge(lid, g["group_id"], "member_of_group", 3, "label_adjacent_to_symbol")
        for sid in g["switches"]:
            edge(sid, g["group_id"], "controls", 3, "label_adjacent_to_symbol")
    for dim in graph["dimensions"]:
        if dim.get("binding_state", "unbound").startswith("device_to"):
            node(dim["dim_id"], f"{dim['value_mm']} мм", "dimension",
                 (dim["center"][0] - 2, dim["center"][1] - 2,
                  dim["center"][0] + 2, dim["center"][1] + 2),
                 3 if dim.get("scale_consistent") else 2, dim["binding_state"])
            for e in dim["ends"]:
                if e.get("device_id"):
                    for dev in graph["lights"] + graph["switches"] + graph["master_switches"]:
                        if dev["symbol_id"] == e["device_id"]:
                            edge(dim["dim_id"], dev["id"], "dimension_anchor",
                                 3 if dim.get("scale_consistent") else 2, "dimension_construction")
    return nodes, edges


def _validation(graph, room_data) -> dict:
    lights = graph["lights"]
    switches = graph["switches"]
    groups = graph["groups"]
    return {
        "apartments_total": len(graph["apartments"]),
        "rooms_total": len(graph["rooms"]),
        "rooms_named": sum(1 for r in graph["rooms"] if r["name"]),
        "rooms_region_resolved": sum(1 for r in graph["rooms"] if r["region_state"] == "resolved"),
        "ceiling_zones_total": len(graph["ceiling_zones"]),
        "lights_total": len(lights),
        "wall_lights_total": sum(1 for x in lights if x["kind"] == "wall_light_output"),
        "lights_in_rooms": sum(1 for x in lights if x.get("room")),
        "other_devices_total": len(graph.get("other_devices") or []),
        "switches_total": len(switches),
        "switches_in_rooms": sum(1 for x in switches if x.get("room")),
        "master_switches_total": len(graph["master_switches"]),
        "groups_confirmed": sum(1 for g in groups if g["state"] == "confirmed"),
        "groups_incomplete": sum(1 for g in groups if g["state"] != "confirmed"),
        "dimensions_total": len(graph["dimensions"]),
        "dimensions_device_bound": sum(1 for d in graph["dimensions"]
                                       if d.get("binding_state", "").startswith("device_to")),
        "unresolved_symbols_total": len(graph["unresolved_symbols"]),
        "conflicts_total": len(graph["conflicts"]),
        "ledger_total": len(graph["semantic_ledger"]),
        "barrier_segments": room_data.get("barrier_segments", 0),
        "nodes_total": len(graph["nodes"]),
        "edges_total": len(graph["edges"]),
    }


def _sheet_block(ref, marks) -> dict:
    meta = dict(ref["sheet_meta"])
    parts = sorted({m["building_part"] for m in marks})
    meta["building_part_from_marks"] = parts[0] if len(parts) == 1 else parts
    meta["floors_label"] = ref.get("floors_label") or None
    return meta
