"""Детерминированные профили структурных схем СОВ и СКУД.

Профиль описывает два независимых слоя:

* размещение: корпус/сооружение → этажная полоса → помещение → оборудование;
* связь: типизированная сеть → подтверждённые конечные узлы.

OCR и LLM не используются. Неподтверждённая цветная топология не достраивается по
порядку элементов на листе.
"""
from __future__ import annotations

import collections
import math
import re
from pathlib import Path
from typing import Optional


PROFILE_TOWER_PAIR = "sov_structural_tower_pair"
PROFILE_MULTITOWER = "sov_structural_multitower"
PROFILE_SKUD_SITE = "skud_structural_site"

_ALIA_NODE_PATTERNS = (
    ("ospd_cabinet", re.compile(r"^ОСПД\d+(?:\.\d+)+$", re.IGNORECASE)),
    ("access_controller", re.compile(r"^К\d+(?:\.[\dА-Яа-яA-Za-z-]+){2,4}$")),
    ("access_module", re.compile(
        r"^STR[\dА-Яа-яA-Za-z]+(?:\.[\dА-Яа-яA-Za-z]+){1,4}(?:\([КK]\))?$",
        re.IGNORECASE,
    )),
    ("concierge_monitor", re.compile(r"^DP\d+(?:\.\d+){1,3}$", re.IGNORECASE)),
    ("door_panel", re.compile(r"^ТД\d+(?:\.\d+){1,3}$", re.IGNORECASE)),
    ("call_panel", re.compile(r"^VP\d+(?:\.\d+){1,3}$", re.IGNORECASE)),
    ("power_supply", re.compile(r"^UG\d+(?:\.\d+)*$", re.IGNORECASE)),
)
_RS485_PAIR_RE = re.compile(
    r"(К\d+(?:\.[\dА-Яа-яA-Za-z-]+){2,4})\s*-\s*"
    r"(STR[\dА-Яа-яA-Za-z]+(?:\.[\dА-Яа-яA-Za-z]+){1,4}(?:\([КK]\))?)"
)
_ETHERNET_PAIR_RE = re.compile(
    r"((?:DP|VP|ТД)\d+(?:\.\d+){1,3})\s*-\s*(ОСПД\d+(?:\.\d+)+)",
    re.IGNORECASE,
)


def _center(bbox) -> tuple[float, float]:
    return ((float(bbox[0]) + float(bbox[2])) / 2,
            (float(bbox[1]) + float(bbox[3])) / 2)


def _bbox_page(bbox, page) -> list[float]:
    return [
        round(float(bbox[0]) / float(page.rect.width), 6),
        round(float(bbox[1]) / float(page.rect.height), 6),
        round(float(bbox[2]) / float(page.rect.width), 6),
        round(float(bbox[3]) / float(page.rect.height), 6),
    ]


def _text_lines(page) -> list[dict]:
    result = []
    for block in page.get_text("dict").get("blocks") or []:
        for line in block.get("lines") or []:
            text = " ".join(
                str(span.get("text") or "").strip()
                for span in line.get("spans") or []
                if str(span.get("text") or "").strip()
            )
            if not text:
                continue
            bbox = tuple(float(value) for value in line["bbox"])
            result.append({
                "text": re.sub(r"\s+", " ", text).strip(),
                "bbox": bbox,
                "center": _center(bbox),
                "direction": tuple(round(float(v), 3) for v in line.get("dir") or (1, 0)),
            })
    return result


def classify_structural_access_profile(vector_text: str) -> Optional[str]:
    text = re.sub(r"\s+", " ", vector_text or " ")
    if "Пожарный отсек" in text and ("Жилой дом" in text or "павильон" in text.lower()):
        return PROFILE_SKUD_SITE
    buildings = {int(value) for value in re.findall(r"\b([1-6])\s+Корпус\b", text)}
    if {3, 4, 5, 6} <= buildings and "ОСПД3" in text and "STR3" in text:
        return PROFILE_MULTITOWER
    if {1, 2} <= buildings and "ОСПД1" in text and "STR1" in text:
        return PROFILE_TOWER_PAIR
    return None


def _source(pdf_path: Path, profile_id: str, block_id: Optional[str]) -> dict:
    return {
        "pdf_file": Path(pdf_path).name,
        "page_index": 0,
        "block_id": block_id,
        "profile_id": profile_id,
    }


def _building_rows(lines, page) -> list[dict]:
    rows = []
    for line in lines:
        match = re.fullmatch(r"([1-6])\s+Корпус", line["text"], re.IGNORECASE)
        if not match:
            continue
        rows.append({
            "id": f"building-{match.group(1)}",
            "number": int(match.group(1)),
            "name": line["text"],
            "header_bbox_page": _bbox_page(line["bbox"], page),
            "x": line["center"][0],
        })
    rows.sort(key=lambda item: item["x"])
    for index, row in enumerate(rows):
        left = 0.0 if index == 0 else (rows[index - 1]["x"] + row["x"]) / 2
        right = float(page.rect.width) if index == len(rows) - 1 else (row["x"] + rows[index + 1]["x"]) / 2
        row["x_range"] = [round(left, 3), round(right, 3)]
    return rows


def _building_for_x(buildings, x: float) -> Optional[dict]:
    return next((row for row in buildings if row["x_range"][0] <= x <= row["x_range"][1]), None)


def _floor_scope(text: str) -> Optional[dict]:
    value = text.strip()
    if re.fullmatch(r"Кровля", value, re.IGNORECASE):
        return {"kind": "roof", "label": value}
    if re.fullmatch(r"Тех\.\s*этаж/кровля", value, re.IGNORECASE):
        return {"kind": "technical_roof", "label": value}
    match = re.fullmatch(r"(-?\d+)(?:\s*[-–]\s*(\d+))?(?:-?й)?\s+этаж(?:а|ей)?", value, re.IGNORECASE)
    if not match:
        return None
    start = int(match.group(1))
    if match.group(2):
        return {"kind": "range", "from": start, "to": int(match.group(2)), "label": value}
    return {"kind": "single", "value": start, "label": value}


def _floor_rows(lines, buildings, page) -> list[dict]:
    floors = []
    for line in lines:
        scope = _floor_scope(line["text"])
        if not scope:
            continue
        building = _building_for_x(buildings, line["center"][0])
        if not building:
            continue
        floor_id = (
            str(scope.get("value")) if scope["kind"] == "single"
            else f"{scope.get('from')}-{scope.get('to')}" if scope["kind"] == "range"
            else scope["kind"]
        )
        floors.append({
            "id": f"{building['id']}/floor:{floor_id}",
            "building_id": building["id"],
            "scope": scope,
            "anchor_y": round(line["center"][1], 3),
            "anchor_x": round(line["center"][0], 3),
            "y_range": None,
            "bbox_page": _bbox_page(line["bbox"], page),
            "source_state": "present",
            "display_state": "present",
            "equipment_ids": [],
            "room_ids": [],
        })
    return floors


def _bind_floor_geometry(page, buildings, floors) -> None:
    """Назначить этажам реальные Y-границы длинных горизонталей таблицы."""
    from backend.app.pipeline.stages.block_grounding.vector_path_graph import flatten_line_segments

    segments = flatten_line_segments(page.get_drawings(), color_mode="black", stroke_only=True, min_length=20)
    page_rect = page.rect
    for building in buildings:
        left, right = building["x_range"]
        min_length = (right - left) * 0.45
        boundaries = []
        for segment in segments:
            p1, p2 = segment["p1"], segment["p2"]
            if abs(p1[1] - p2[1]) > 0.5 or segment["length"] < min_length:
                continue
            mid_x = (p1[0] + p2[0]) / 2
            y = (p1[1] + p2[1]) / 2
            if not (left <= mid_x <= right and -1 <= y <= float(page_rect.height) + 1):
                continue
            boundaries.append(y)
        unique = []
        for value in sorted(boundaries):
            if not unique or abs(value - unique[-1]) > 1.0:
                unique.append(value)
        building_floors = [item for item in floors if item["building_id"] == building["id"]]
        for floor in building_floors:
            y = float(floor["anchor_y"])
            above = max((value for value in unique if value <= y), default=0.0)
            below = min((value for value in unique if value >= y), default=float(page_rect.height))
            if below - above >= 10:
                floor["y_range"] = [round(above, 3), round(below, 3)]


def _alia_nodes(page, lines, buildings) -> list[dict]:
    limit = min(
        [line["bbox"][1] for line in lines if line["text"].startswith("Примечания")]
        or [float(page.rect.height) * 0.84]
    )
    nodes = []
    for word in page.get_text("words"):
        bbox = tuple(float(value) for value in word[:4])
        if _center(bbox)[1] >= limit:
            continue
        token = str(word[4]).strip(" ,;:*")
        node_type = next((kind for kind, pattern in _ALIA_NODE_PATTERNS if pattern.fullmatch(token)), None)
        if not node_type:
            continue
        building = _building_for_x(buildings, _center(bbox)[0])
        encoded = re.match(r"^(?:STR|ОСПД|К|DP)(\d+)", token, re.IGNORECASE)
        if not encoded and {item["number"] for item in buildings} == {1, 2}:
            encoded = re.match(r"^VP(\d+)", token, re.IGNORECASE)
        if encoded:
            encoded_building = next(
                (item for item in buildings if item["number"] == int(encoded.group(1))), None
            )
            building = encoded_building or building
        nodes.append({
            "id": f"node-{len(nodes) + 1}",
            "label": token,
            "node_type": node_type,
            "building_id": building["id"] if building else None,
            "floor_band_id": None,
            "room_id": None,
            "x": round(_center(bbox)[0], 3),
            "y": round(_center(bbox)[1], 3),
            "bbox_page": _bbox_page(bbox, page),
            "field_state": "present",
        })
    workstation_candidates = []
    for line in lines:
        if line["center"][1] >= limit or line["text"].upper() != "АРМ СКУД":
            continue
        building = _building_for_x(buildings, line["center"][0])
        workstation_candidates.append({
            "id": f"node-{len(nodes) + 1}", "label": "АРМ СКУД",
            "node_type": "workstation", "building_id": building["id"] if building else None,
            "floor_band_id": None, "room_id": None,
            "x": round(line["center"][0], 3), "y": round(line["center"][1], 3),
            "bbox_page": _bbox_page(line["bbox"], page), "field_state": "present",
        })
    # Одна и та же подпись встречается у символа АРМ и повторно на кабельной линии.
    # В пределах корпуса узлом является верхняя подпись рядом с самим символом.
    by_building = collections.defaultdict(list)
    for item in workstation_candidates:
        by_building[item.get("building_id")].append(item)
    for items in by_building.values():
        item = min(items, key=lambda row: row["y"])
        item["id"] = f"node-{len(nodes) + 1}"
        nodes.append(item)
    return nodes


def _ensure_inferred_basement(floors, nodes, buildings):
    for building in buildings:
        building_nodes = [item for item in nodes if item.get("building_id") == building["id"]]
        has_minus_one = any(
            item["building_id"] == building["id"]
            and item["scope"].get("kind") == "single" and item["scope"].get("value") == -1
            for item in floors
        )
        basement_nodes = [item for item in building_nodes if ".-1." in item["label"]]
        if has_minus_one or not basement_nodes:
            continue
        floors.append({
            "id": f"{building['id']}/floor:-1",
            "building_id": building["id"],
            "scope": {"kind": "single", "value": -1, "label": "-1 этаж"},
            "anchor_y": round(max(item["y"] for item in basement_nodes), 3),
            "anchor_x": round(building["x_range"][0], 3),
            "y_range": None,
            "bbox_page": None,
            "source_state": "inferred_from_equipment_code",
            "display_state": "present",
            "equipment_ids": [], "room_ids": [],
        })


def _nearest_floor(floors, building_id, y) -> Optional[dict]:
    rows = [item for item in floors if item["building_id"] == building_id]
    containing = [item for item in rows if item.get("y_range")
                  and item["y_range"][0] - 0.8 <= y <= item["y_range"][1] + 0.8]
    if containing:
        return min(containing, key=lambda item: abs(float(item["anchor_y"]) - y))
    return min(rows, key=lambda item: abs(float(item["anchor_y"]) - y)) if rows else None


def _room_rows(lines, buildings, floors, page) -> list[dict]:
    rooms = []
    for line in lines:
        text = line["text"]
        if not re.match(r"^(?:пом\.|Помещение)\s*№", text, re.IGNORECASE) and text != "Территория":
            continue
        building = _building_for_x(buildings, line["center"][0])
        if not building:
            continue
        floor = _nearest_floor(floors, building["id"], line["center"][1])
        rooms.append({
            "id": f"room-{len(rooms) + 1}",
            "name": text,
            "kind": "territory" if text == "Территория" else "room",
            "building_id": building["id"],
            "floor_band_id": floor["id"] if floor else None,
            "x": round(line["center"][0], 3), "y": round(line["center"][1], 3),
            "bbox_page": _bbox_page(line["bbox"], page),
            "equipment_ids": [],
        })
    return rooms


def _bind_alia_hierarchy(nodes, floors, rooms):
    floor_by_id = {item["id"]: item for item in floors}
    room_by_id = {item["id"]: item for item in rooms}
    for node in nodes:
        if not node.get("building_id"):
            continue
        floor = _nearest_floor(floors, node["building_id"], node["y"])
        if floor:
            node["floor_band_id"] = floor["id"]
            floor["equipment_ids"].append(node["id"])
        candidates = [
            room for room in rooms
            if room["building_id"] == node["building_id"]
            and room.get("floor_band_id") == node.get("floor_band_id")
            and abs(room["y"] - node["y"]) <= 105
        ]
        if candidates:
            room = min(candidates, key=lambda item: abs(item["x"] - node["x"]) + 1.4 * abs(item["y"] - node["y"]))
            node["room_id"] = room["id"]
            room["equipment_ids"].append(node["id"])
    for room in rooms:
        if room.get("floor_band_id") in floor_by_id:
            floor_by_id[room["floor_band_id"]]["room_ids"].append(room["id"])
    for floor in floors:
        if not floor["equipment_ids"]:
            floor["display_state"] = "shown_empty"


def _po_rows(lines, buildings, floors, page) -> list[dict]:
    result = []
    for line in lines:
        if not re.fullmatch(r"ПО\s*№\s*\d+", line["text"], re.IGNORECASE):
            continue
        nearest_floor_anchor = min(
            floors, key=lambda item: abs(float(item.get("anchor_x") or 0) - line["center"][0])
        ) if floors else None
        building = next(
            (item for item in buildings if nearest_floor_anchor
             and item["id"] == nearest_floor_anchor["building_id"]),
            None,
        ) or _building_for_x(buildings, line["center"][0])
        if not building:
            continue
        floor = _nearest_floor(floors, building["id"], line["center"][1])
        result.append({
            "id": f"{building['id']}/po-{len([x for x in result if x['building_id'] == building['id']]) + 1}",
            "label": line["text"], "building_id": building["id"],
            "floor_band_id": floor["id"] if floor else None,
            "bbox_page": _bbox_page(line["bbox"], page),
        })
    return result


def _node_for_label(nodes, label, *, building_id=None) -> Optional[dict]:
    candidates = [item for item in nodes if item["label"].lower() == label.lower()]
    if building_id:
        same = [item for item in candidates if item.get("building_id") == building_id]
        if same:
            candidates = same
    return candidates[0] if candidates else None


def _alia_networks(page, nodes, buildings) -> tuple[list[dict], list[dict]]:
    text = " ".join(page.get_text().split())
    networks, edges = [], []
    seen = set()
    for controller_label, endpoint_label in _RS485_PAIR_RE.findall(text):
        key = ("rs485", controller_label, endpoint_label)
        if key in seen:
            continue
        seen.add(key)
        controller = _node_for_label(nodes, controller_label)
        endpoint = _node_for_label(nodes, endpoint_label,
                                   building_id=controller.get("building_id") if controller else None)
        network_id = f"network-{len(networks) + 1}"
        endpoint_ids = [item["id"] for item in (controller, endpoint) if item]
        networks.append({
            "id": network_id, "network_type": "rs485", "color": "magenta",
            "label": f"{controller_label}-{endpoint_label}", "endpoint_ids": endpoint_ids,
            "path_state": "annotation_confirmed" if len(endpoint_ids) == 2 else "requires_review",
        })
        if len(endpoint_ids) == 2:
            edges.append({"id": f"edge-{len(edges) + 1}", "network_id": network_id,
                          "from": endpoint_ids[0], "to": endpoint_ids[1], "edge_state": "present"})
    for source_label, cabinet_label in _ETHERNET_PAIR_RE.findall(text):
        key = ("ethernet", source_label, cabinet_label)
        if key in seen:
            continue
        seen.add(key)
        cabinet = _node_for_label(nodes, cabinet_label)
        source = _node_for_label(nodes, source_label,
                                 building_id=cabinet.get("building_id") if cabinet else None)
        # Если подпись явно ведёт в шкаф другого корпуса, сохраняем межкорпусную связь.
        if not source:
            source = _node_for_label(nodes, source_label)
        network_id = f"network-{len(networks) + 1}"
        endpoint_ids = [item["id"] for item in (source, cabinet) if item]
        networks.append({
            "id": network_id, "network_type": "ethernet", "color": "blue",
            "label": f"{source_label}-{cabinet_label}", "endpoint_ids": endpoint_ids,
            "path_state": "annotation_confirmed" if len(endpoint_ids) == 2 else "requires_review",
        })
        if len(endpoint_ids) == 2:
            edges.append({"id": f"edge-{len(edges) + 1}", "network_id": network_id,
                          "from": endpoint_ids[0], "to": endpoint_ids[1], "edge_state": "present"})
    for building in buildings:
        cabinets = [item for item in nodes if item["building_id"] == building["id"]
                    and item["node_type"] == "ospd_cabinet"]
        if len(cabinets) < 2:
            continue
        networks.append({
            "id": f"network-{len(networks) + 1}", "network_type": "optical_backbone",
            "color": "orange", "label": f"оптическая магистраль {building['name']}",
            "endpoint_ids": [item["id"] for item in cabinets],
            "path_state": "geometry_grouped",
        })
    return networks, edges


def _control_domains(nodes, po_zones, networks, buildings) -> list[dict]:
    domains = []
    for controller in (item for item in nodes if item["node_type"] == "access_controller"):
        match = re.match(r"К(?P<building>\d+)\.(?P<domain>\d+)", controller["label"])
        if not match:
            continue
        expected_cabinet = f"ОСПД{match.group('building')}.{match.group('domain')}"
        cabinet = _node_for_label(nodes, expected_cabinet, building_id=controller.get("building_id"))
        if not cabinet:
            candidates = [item for item in nodes if item["node_type"] == "ospd_cabinet"
                          and item.get("building_id") == controller.get("building_id")]
            cabinet = min(candidates, key=lambda item: math.hypot(
                item["x"] - controller["x"], item["y"] - controller["y"]
            )) if candidates else None
        prefix = f"STR{match.group('building')}.{match.group('domain')}."
        modules = [item for item in nodes if item["node_type"] == "access_module"
                   and item["label"].startswith(prefix)]
        if not modules:
            modules = [item for item in nodes if item["node_type"] == "access_module"
                       and item.get("building_id") == controller.get("building_id")
                       and item.get("floor_band_id") == controller.get("floor_band_id")]
        zones = [item for item in po_zones if item.get("building_id") == controller.get("building_id")]
        po = min(zones, key=lambda item: abs(
            ((item.get("bbox_page") or [0, 0, 0, 0])[1]) - controller["bbox_page"][1]
        )) if zones else None
        domain_networks = [item["id"] for item in networks
                           if controller["id"] in (item.get("endpoint_ids") or [])]
        domains.append({
            "id": f"control-domain-{len(domains) + 1}",
            "building_id": controller.get("building_id"),
            "po_id": po.get("id") if po else None,
            "cabinet_id": cabinet.get("id") if cabinet else None,
            "controller_id": controller["id"],
            "module_ids": [item["id"] for item in modules],
            "network_ids": domain_networks,
            "domain_state": "present" if cabinet and modules else "requires_review",
        })
    return domains


def _color_metrics(page) -> dict:
    from backend.app.pipeline.stages.block_grounding.vector_path_graph import flatten_line_segments

    drawings = [item for item in page.get_drawings()
                if item.get("rect") and item["rect"].intersects(page.rect)]
    segments = flatten_line_segments(drawings, color_mode="any", stroke_only=False, min_length=1)
    counts = collections.Counter(item.get("color") for item in segments)
    mapping = {
        "black": (0.0, 0.0, 0.0), "green": (0.18, 0.722, 0.0),
        "blue": (0.0, 0.0, 1.0), "magenta": (1.0, 0.0, 1.0),
        "orange": (1.0, 0.498, 0.0),
    }
    return {name: counts.get(color, 0) for name, color in mapping.items()}


def _build_alia(page, pdf_path: Path, profile_id: str, block_id: Optional[str]) -> dict:
    lines = _text_lines(page)
    buildings = _building_rows(lines, page)
    nodes = _alia_nodes(page, lines, buildings)
    floors = _floor_rows(lines, buildings, page)
    _ensure_inferred_basement(floors, nodes, buildings)
    _bind_floor_geometry(page, buildings, floors)
    rooms = _room_rows(lines, buildings, floors, page)
    _bind_alia_hierarchy(nodes, floors, rooms)
    po_zones = _po_rows(lines, buildings, floors, page)
    networks, edges = _alia_networks(page, nodes, buildings)
    control_domains = _control_domains(nodes, po_zones, networks, buildings) if profile_id == PROFILE_MULTITOWER else []
    for building in buildings:
        building["floor_band_ids"] = [item["id"] for item in floors if item["building_id"] == building["id"]]
        building["room_ids"] = [item["id"] for item in rooms if item["building_id"] == building["id"]]
        building["control_zone_ids"] = [item["id"] for item in po_zones if item["building_id"] == building["id"]]
        building["control_domain_ids"] = [item["id"] for item in control_domains
                                             if item["building_id"] == building["id"]]
        building["equipment_ids"] = [item["id"] for item in nodes if item["building_id"] == building["id"]]

    total = len(nodes)
    building_bound = sum(1 for item in nodes if item.get("building_id"))
    floor_bound = sum(1 for item in nodes if item.get("floor_band_id"))
    room_bound = sum(1 for item in nodes if item.get("room_id"))
    confirmed_edges = sum(1 for item in edges if item.get("edge_state") == "present")
    warnings = []
    inferred = [item["id"] for item in floors if item["source_state"] != "present"]
    if inferred:
        warnings.append("этажные полосы выведены из кода оборудования: " + ", ".join(inferred))
    if room_bound < total:
        warnings.append("часть инфраструктурных узлов расположена вне подписанных помещений")
    return {
        "profile_id": profile_id,
        "source": _source(pdf_path, profile_id, block_id),
        "buildings": buildings,
        "control_zones": po_zones,
        "control_domains": control_domains,
        "floor_bands": floors,
        "rooms": rooms,
        "nodes": nodes,
        "networks": networks,
        "edges": edges,
        "external_references": sorted(set(re.findall(r"13АВ-РД-[А-ЯA-Z0-9.\-]+", page.get_text()))),
        "validation": {
            "buildings_total": len(buildings),
            "floor_bands_total": len(floors),
            "shown_empty_floor_bands": sum(1 for item in floors if item["display_state"] == "shown_empty"),
            "rooms_total": len(rooms),
            "nodes_total": total,
            "nodes_building_bound": building_bound,
            "nodes_floor_bound": floor_bound,
            "nodes_room_bound": room_bound,
            "building_bind_rate": round(building_bound / max(total, 1), 3),
            "floor_bind_rate": round(floor_bound / max(total, 1), 3),
            "room_bind_rate": round(room_bound / max(total, 1), 3),
            "networks_total": len(networks),
            "confirmed_edges": confirmed_edges,
            "control_domains_total": len(control_domains),
            "control_domains_complete": sum(1 for item in control_domains if item["domain_state"] == "present"),
            "node_types": dict(collections.Counter(item["node_type"] for item in nodes)),
            "colored_segments": _color_metrics(page),
            "hierarchy_state": "present" if buildings and floor_bound == total else "partial",
            "topology_state": "partial_confirmed" if confirmed_edges else "not_extracted",
        },
        "warnings": warnings,
        "status": "ok" if buildings and floor_bound == total and confirmed_edges else "needs_review",
    }


_SKUD_STRUCTURE_NAMES = (
    "Жилой дом", "Одноэтажный павильон общественного назначения",
    "Одноэтажный павильон административного назначения", "Въезд в подземный паркинг",
)
_SKUD_LOCATION_RE = re.compile(
    r"(?:Коридор|Кроссовая|Лестничная клетка|Вестибюль|Тамбур|Калитка|Ворота|"
    r"Вход в павильон|Выход на кровлю|Въезд|Рампа|Узел\s*\d+|"
    r"Пом\. службы|Помещение прокладки|Диспетчерская|Комната охраны|Территория)",
    re.IGNORECASE,
)
_SKUD_INFRA_PATTERNS = (
    ("ospd_cabinet", re.compile(r"^ОСПД[._А-Яа-яA-Za-z0-9]+$", re.IGNORECASE)),
    ("switch", re.compile(r"^SW\.\d+(?:\.\d+)*$", re.IGNORECASE)),
    ("access_controller", re.compile(r"^STR\d+(?:\.\d+)+$", re.IGNORECASE)),
    ("power_supply", re.compile(r"^UG\d+(?:\.\d+)+$", re.IGNORECASE)),
)
_SKUD_ACCESS_RE = re.compile(
    r"^(?:[КK]\d+\.\d+|Y\d+\.\d+|BGB\d+\.\d+|ZM\d+\.\d+|BTM\d+\.\d+|\d+MD\d+\.\d+)$",
    re.IGNORECASE,
)


def _build_skud_site(page, pdf_path: Path, block_id: Optional[str]) -> dict:
    lines = _text_lines(page)
    structures = []
    for name in _SKUD_STRUCTURE_NAMES:
        for line in lines:
            if line["text"] != name:
                continue
            structures.append({
                "id": f"structure-{len(structures) + 1}", "name": name,
                "x": round(line["center"][0], 3), "y": round(line["center"][1], 3),
                "bbox_page": _bbox_page(line["bbox"], page),
            })
    floors = []
    for line in lines:
        scope = _floor_scope(line["text"])
        if scope:
            floors.append({
                "id": f"floor-band-{len(floors) + 1}", "scope": scope,
                "x": round(line["center"][0], 3), "y": round(line["center"][1], 3),
                "bbox_page": _bbox_page(line["bbox"], page),
            })
    overlays_by_number = {}
    for line in lines:
        match = re.fullmatch(r"Пожарный отсек\s*(\d+)", line["text"], re.IGNORECASE)
        if match:
            number = int(match.group(1))
            overlay = overlays_by_number.setdefault(number, {
                "id": f"fire-compartment-{match.group(1)}", "kind": "fire_compartment",
                "label": f"Пожарный отсек {number}", "member_ids": [],
                "anchor_bboxes_page": [], "membership_state": "not_traced",
            })
            overlay["anchor_bboxes_page"].append(_bbox_page(line["bbox"], page))
    overlays = list(overlays_by_number.values())
    locations = []
    for line in lines:
        if not _SKUD_LOCATION_RE.search(line["text"]):
            continue
        locations.append({
            "id": f"location-{len(locations) + 1}", "name": line["text"],
            "structure_id": None, "floor_band_id": None,
            "x": round(line["center"][0], 3), "y": round(line["center"][1], 3),
            "bbox_page": _bbox_page(line["bbox"], page), "device_ids": [],
        })

    infrastructure, access_devices = [], []
    for word in page.get_text("words"):
        token = str(word[4]).strip(" ,;:*")
        bbox = tuple(float(value) for value in word[:4])
        node_type = next((kind for kind, pattern in _SKUD_INFRA_PATTERNS if pattern.fullmatch(token)), None)
        if node_type:
            infrastructure.append({
                "id": f"infra-{len(infrastructure) + 1}", "label": token,
                "node_type": node_type, "location_id": None,
                "x": round(_center(bbox)[0], 3), "y": round(_center(bbox)[1], 3),
                "bbox_page": _bbox_page(bbox, page),
            })
        elif _SKUD_ACCESS_RE.fullmatch(token):
            access_devices.append({
                "id": f"access-device-{len(access_devices) + 1}", "label": token,
                "x": round(_center(bbox)[0], 3), "y": round(_center(bbox)[1], 3),
                "location_id": None, "bbox_page": _bbox_page(bbox, page),
            })
    # Позиционная подпись location находится над/рядом с дверным комплектом. Большой
    # допуск допустим только внутри одного локального кластера; далёкие узлы остаются unbound.
    for device in access_devices:
        if not locations:
            continue
        distance, location = min(
            (math.hypot(device["x"] - row["x"], device["y"] - row["y"]), row)
            for row in locations
        )
        if distance <= 230:
            device["location_id"] = location["id"]
            location["device_ids"].append(device["id"])
    for node in infrastructure:
        if not locations:
            continue
        distance, location = min(
            (math.hypot(node["x"] - row["x"], node["y"] - row["y"]), row)
            for row in locations
        )
        if distance <= 220:
            node["location_id"] = location["id"]

    access_points = []
    for location in locations:
        if len(location["device_ids"]) < 2:
            continue
        access_points.append({
            "id": f"access-point-{len(access_points) + 1}",
            "location_id": location["id"], "device_ids": location["device_ids"],
            "assembly_state": "geometry_grouped",
        })
    bound_devices = sum(1 for item in access_devices if item.get("location_id"))
    warnings = [
        "пожарные отсеки сохранены как overlays; их геометрическое членство ещё не трассируется",
        "цветовая легенда профиля СОВ не применяется к этому CAD-диалекту",
    ]
    return {
        "profile_id": PROFILE_SKUD_SITE,
        "source": _source(pdf_path, PROFILE_SKUD_SITE, block_id),
        "site": {"id": "site-1", "structure_ids": [item["id"] for item in structures]},
        "structures": structures,
        "floor_bands": floors,
        "locations": locations,
        "access_points": access_points,
        "access_devices": access_devices,
        "infrastructure_nodes": infrastructure,
        "overlays": overlays,
        "networks": [], "edges": [],
        "validation": {
            "structures_total": len(structures), "floor_bands_total": len(floors),
            "locations_total": len(locations), "access_points_total": len(access_points),
            "access_devices_total": len(access_devices), "access_devices_location_bound": bound_devices,
            "access_device_bind_rate": round(bound_devices / max(len(access_devices), 1), 3),
            "infrastructure_nodes_total": len(infrastructure),
            "fire_compartments_total": len(overlays),
            "colored_segments": _color_metrics(page),
            "hierarchy_state": "site_inventory",
            "topology_state": "visual_unverified",
        },
        "warnings": warnings,
        "status": "site_inventory" if structures and access_points else "needs_review",
    }


def build_structural_access_graph(pdf_path: Path, *, block_id: Optional[str] = None) -> Optional[dict]:
    """Построить структуру одного cropped PDF структурной схемы СОВ/СКУД."""
    try:
        import fitz

        doc = fitz.open(str(pdf_path))
    except Exception:
        return None
    try:
        if doc.page_count != 1:
            return None
        page = doc[0]
        profile_id = classify_structural_access_profile(page.get_text())
        if profile_id in (PROFILE_TOWER_PAIR, PROFILE_MULTITOWER):
            return _build_alia(page, Path(pdf_path), profile_id, block_id)
        if profile_id == PROFILE_SKUD_SITE:
            return _build_skud_site(page, Path(pdf_path), block_id)
        return None
    except Exception:
        return None
    finally:
        doc.close()


def evaluate_structural_access_gate(graph: Optional[dict]) -> dict:
    if not graph:
        return {"use": False, "mode": "none", "reasons": ["структурный граф не построен"],
                "warnings": [], "metrics": {}}
    profile_id = graph.get("profile_id")
    validation = graph.get("validation") or {}
    warnings = list(graph.get("warnings") or [])
    reasons = []
    if profile_id == PROFILE_TOWER_PAIR:
        if validation.get("buildings_total") != 2:
            reasons.append("ожидалось два корпуса")
        if (validation.get("floor_bind_rate") or 0) < 0.95:
            reasons.append("привязка оборудования к этажным полосам ниже 95%")
        if (validation.get("confirmed_edges") or 0) < 5:
            reasons.append("мало подтверждённых связей")
        mode = "hierarchy_and_confirmed_edges"
    elif profile_id == PROFILE_MULTITOWER:
        if validation.get("buildings_total") != 4:
            reasons.append("ожидались корпуса 3–6")
        if (validation.get("floor_bind_rate") or 0) < 0.95:
            reasons.append("привязка оборудования к этажным полосам ниже 95%")
        if (validation.get("confirmed_edges") or 0) < 5:
            reasons.append("мало подтверждённых связей")
        mode = "hierarchy_and_confirmed_edges"
    else:
        if (validation.get("structures_total") or 0) < 3:
            reasons.append("мало сооружений площадки")
        if (validation.get("access_points_total") or 0) < 3:
            reasons.append("мало сгруппированных точек доступа")
        if (validation.get("access_device_bind_rate") or 0) < 0.90:
            reasons.append("привязка устройств к locations ниже 90%")
        if (validation.get("infrastructure_nodes_total") or 0) < 5:
            reasons.append("мало инфраструктурных узлов")
        mode = "site_inventory"
    return {"use": not reasons, "mode": mode, "reasons": reasons,
            "warnings": warnings, "metrics": validation}


def render_structural_access_markdown(graph: dict) -> str:
    if not graph:
        return ""
    v = graph.get("validation") or {}
    lines = [
        f"# Структурная схема: {graph.get('profile_id')}", "",
        f"Источник: `{(graph.get('source') or {}).get('pdf_file')}`", "",
        f"Статус: `{graph.get('status')}`.", "",
    ]
    if graph.get("profile_id") in (PROFILE_TOWER_PAIR, PROFILE_MULTITOWER):
        node_by_id = {item["id"]: item for item in graph.get("nodes") or []}
        floors = {item["id"]: item for item in graph.get("floor_bands") or []}
        rooms = {item["id"]: item for item in graph.get("rooms") or []}
        lines += [
            f"Корпусов: {v.get('buildings_total')}; этажных полос: {v.get('floor_bands_total')}; "
            f"узлов: {v.get('nodes_total')}; подтверждённых связей: {v.get('confirmed_edges')}.",
            "", "## Иерархия", "",
        ]
        for building in graph.get("buildings") or []:
            lines.append(f"### {building['name']}")
            lines.append("")
            for floor_id in building.get("floor_band_ids") or []:
                floor = floors[floor_id]
                equipment = [node_by_id[item]["label"] for item in floor["equipment_ids"]]
                room_names = [rooms[item]["name"] for item in floor["room_ids"]]
                suffix = "shown_empty" if floor["display_state"] == "shown_empty" else ", ".join(equipment) or "—"
                lines.append(
                    f"- **{floor['scope']['label']}**: {suffix}; помещения: "
                    f"{', '.join(room_names) or '—'}"
                )
            lines.append("")
        lines += ["## Подтверждённые связи", ""]
        for edge in graph.get("edges") or []:
            network = next(item for item in graph["networks"] if item["id"] == edge["network_id"])
            lines.append(
                f"- `{network['network_type']}`: {node_by_id[edge['from']]['label']} → "
                f"{node_by_id[edge['to']]['label']}"
            )
    else:
        lines += [
            f"Сооружений: {v.get('structures_total')}; этажных полос: {v.get('floor_bands_total')}; "
            f"locations: {v.get('locations_total')}; точек доступа: {v.get('access_points_total')}.",
            "", "## Сооружения", "",
        ]
        lines += [f"- {item['name']}" for item in graph.get("structures") or []]
        lines += ["", "## Точки доступа", ""]
        location_by_id = {item["id"]: item for item in graph.get("locations") or []}
        device_by_id = {item["id"]: item for item in graph.get("access_devices") or []}
        for access_point in graph.get("access_points") or []:
            location = location_by_id[access_point["location_id"]]
            labels = [device_by_id[item]["label"] for item in access_point["device_ids"]]
            lines.append(f"- **{location['name']}**: {', '.join(labels)}")
    if graph.get("warnings"):
        lines += ["", "## Требует внимания", ""] + [f"- {item}" for item in graph["warnings"]]
    return "\n".join(lines) + "\n"
