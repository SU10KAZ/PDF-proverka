"""Детерминированные логические структуры для корпуса схем ALIA.

Модуль читает только векторный слой PDF: текст, координаты и CAD paths. Он не
дорисовывает электрические связи по визуальному впечатлению. Связи имеют явное
состояние происхождения: код оборудования, колонка, геометрическая группа либо
только инвентарь без подтверждённой топологии.
"""
from __future__ import annotations

import collections
import math
import re
from pathlib import Path
from typing import Optional

from .vector_path_graph import build_segment_components, point_segment_distance
from .profiled_graph_localization import (
    ru_network_type, ru_node_type, ru_profile, ru_state,
)


PROFILE_CCTV = "cctv_floor_network"
PROFILE_SOUE = "voice_alarm_line_topology"
PROFILE_FIBER = "fiber_ring_backbone"
PROFILE_METER_WATER = "metering_floor_bus_water"
PROFILE_METER_HEAT = "metering_floor_bus_heat"
PROFILE_AK_CONTROL = "automation_control_hierarchy"
PROFILE_AK_DISPATCH = "dispatch_integration_backbone"
PROFILE_LIFT = "lift_dispatch_floor_topology"
PROFILE_MGN = "mgn_intercom_floor_bus"
PROFILE_CABINET_COMM = "cabinet_commutation_graph"
PROFILE_CABINET_LAYOUT = "cabinet_rack_layout"
PROFILE_FUNCTIONAL = "functional_process_io"
PROFILE_EXTERNAL = "external_terminal_wiring"
PROFILE_NICHE = "multidiscipline_niche_layout"

ALL_PROFILES = (
    PROFILE_CCTV, PROFILE_SOUE, PROFILE_FIBER, PROFILE_METER_WATER,
    PROFILE_METER_HEAT, PROFILE_AK_CONTROL, PROFILE_AK_DISPATCH,
    PROFILE_LIFT, PROFILE_MGN, PROFILE_CABINET_COMM,
    PROFILE_CABINET_LAYOUT, PROFILE_FUNCTIONAL, PROFILE_EXTERNAL,
    PROFILE_NICHE,
)


def _center(bbox) -> tuple[float, float]:
    return ((float(bbox[0]) + float(bbox[2])) / 2,
            (float(bbox[1]) + float(bbox[3])) / 2)


def _bbox_page(bbox, page) -> list[float]:
    return [round(float(bbox[0]) / page.rect.width, 6),
            round(float(bbox[1]) / page.rect.height, 6),
            round(float(bbox[2]) / page.rect.width, 6),
            round(float(bbox[3]) / page.rect.height, 6)]


def _lines(page) -> list[dict]:
    result = []
    for block in page.get_text("dict").get("blocks") or []:
        for line in block.get("lines") or []:
            text = " ".join(
                str(span.get("text") or "").strip()
                for span in line.get("spans") or []
                if str(span.get("text") or "").strip()
            )
            text = re.sub(r"\s+", " ", text).strip()
            if not text:
                continue
            bbox = tuple(float(v) for v in line["bbox"])
            result.append({"text": text, "bbox": bbox, "center": _center(bbox)})
    return result


def classify_alia_scheme_profile(vector_text: str) -> Optional[str]:
    text = re.sub(r"\s+", " ", vector_text or " ")
    upper = text.upper()
    # Уникальная марка переговорного устройства надёжнее общих слов о шкафе
    # и стойке, которые также присутствуют внутри этой схемы.
    if "АПУ-2Н" in upper and "ИНТЕРФЕЙСА CAN" in upper:
        return PROFILE_MGN
    # Описание Chandra передаёт назначение схемы обычными словами и не всегда
    # содержит конкретные марки оборудования, на которых строились старые правила.
    if any(x in upper for x in ("СХЕМА НИШИ", "КОМПОНОВКА НИШИ", "МЕЖДИСЦИПЛИНАРНАЯ НИША")):
        return PROFILE_NICHE
    if any(x in upper for x in ("СХЕМА ВНЕШНИХ ПОДКЛЮЧЕНИЙ", "ВНЕШНИЕ ПОДКЛЮЧЕНИЯ", "КЛЕММНЫЕ ПОДКЛЮЧЕНИЯ К ВНЕШНИМ")):
        return PROFILE_EXTERNAL
    if any(x in upper for x in ("ФУНКЦИОНАЛЬНАЯ СХЕМА", "СХЕМА АВТОМАТИЗАЦИИ ТЕХНОЛОГИЧЕСКОГО ПРОЦЕССА")):
        return PROFILE_FUNCTIONAL
    if any(x in upper for x in ("КОМПОНОВКА ШКАФА", "КОМПОНОВКА СТОЙКИ", "РАЗМЕЩЕНИЕ ОБОРУДОВАНИЯ В ШКАФУ", "СХЕМА СТОЙКИ")):
        return PROFILE_CABINET_LAYOUT
    if any(x in upper for x in ("СХЕМА КОММУТАЦИИ", "КОММУТАЦИОННАЯ СХЕМА ШКАФА")):
        return PROFILE_CABINET_COMM
    if any(x in upper for x in ("ДОМОФОН", "ПЕРЕГОВОРНОЕ УСТРОЙСТВО МГН")) and any(x in upper for x in ("СТРУКТУРН", "СХЕМА", "ШИНА")):
        return PROFILE_MGN
    if "ДИСПЕТЧЕРИЗАЦ" in upper and "ЛИФТ" in upper:return PROFILE_LIFT
    if any(x in upper for x in ("УЧЕТА ВОДЫ", "УЧЁТА ВОДЫ", "ВОДОСЧЕТЧИК", "ВОДОСЧЁТЧИК")) and any(x in upper for x in ("СТРУКТУРН", "СХЕМА", "ШИНА")):
        return PROFILE_METER_WATER
    if any(x in upper for x in ("УЧЕТА ТЕПЛА", "УЧЁТА ТЕПЛА", "ТЕПЛОСЧЕТЧИК", "ТЕПЛОСЧЁТЧИК")) and any(x in upper for x in ("СТРУКТУРН", "СХЕМА", "ШИНА")):
        return PROFILE_METER_HEAT
    if any(x in upper for x in ("ВОЛОКОННО-ОПТИЧ", "ОПТИЧЕСКОЕ КОЛЬЦО", "ВОЛС")) and any(x in upper for x in ("СТРУКТУРН", "МАГИСТРАЛ", "КОЛЬЦ")):
        return PROFILE_FIBER
    if any(x in upper for x in ("СОУЭ", "РЕЧЕВОГО ОПОВЕЩЕНИЯ", "ЛИНИЙ ОПОВЕЩЕНИЯ")) and any(x in upper for x in ("СТРУКТУРН", "СХЕМА", "ТОПОЛОГ")):
        return PROFILE_SOUE
    if any(x in upper for x in ("ВИДЕОНАБЛЮД", "СИСТЕМЫ СОТ", "КАМЕР ВИДЕОНАБЛЮДЕНИЯ", "ОХРАННОГО ТЕЛЕВИДЕНИЯ")) and any(x in upper for x in ("СТРУКТУРН", "СХЕМА", "ТОПОЛОГ")):
        return PROFILE_CCTV
    if "ДИСПЕТЧЕРИЗАЦ" in upper and any(x in upper for x in ("ИНТЕГРАЦ", "МАГИСТРАЛ", "СТРУКТУРН")):
        return PROFILE_AK_DISPATCH
    if any(x in upper for x in ("АВТОМАТИЗАЦ", "АВТОМАТИЧЕСКОГО УПРАВЛЕНИЯ")) and any(x in upper for x in ("ИЕРАРХ", "СТРУКТУРН", "СХЕМА УПРАВЛЕНИЯ")):
        return PROFILE_AK_CONTROL
    if "ГИЛЬЗОПАКЕТ" in upper and ("СС/АК" in upper or "ЭОМ-СПЗ" in upper):
        return PROFILE_NICHE
    if "НАИМЕНОВАНИЕ ПАРАМЕТРА" in upper and "ХТ1" in upper:
        return PROFILE_EXTERNAL
    if "ПРИТОК" in upper and "PDS" in upper and "П2.Б2" in upper:
        return PROFILE_FUNCTIONAL
    if "ТЕЛЕКОММУНИКАЦИОННЫЙ ШКАФ ШСОУЭ" in upper:
        return PROFILE_CABINET_LAYOUT
    if "СХЕМА КОММУТАЦИИ" in upper or (
        "SONAR SPM" in upper and "ЛИНИЯ ОПОВЕЩЕНИЯ" in upper
    ):
        return PROFILE_CABINET_COMM
    if "ЛИФТОВОЙ БЛОК" in upper and "ШАХТА ЛИФТА" in upper:
        return PROFILE_LIFT
    if "АСКУВ-" in upper:
        return PROFILE_METER_WATER
    if "АСКУТ-" in upper:
        return PROFILE_METER_HEAT
    if "АРМ СОТ" in upper and re.search(r"\bВК\d+(?:\.\d+){2,3}\b", text):
        return PROFILE_CCTV
    if "ШСПА" in upper and "L9" in upper and "ШСОУЭ" in upper:
        return PROFILE_FIBER
    if "ШСОУЭ" in upper and re.search(r"V\d+\s*\(\s*Р\s*=", text, re.I):
        return PROFILE_SOUE
    if "АРМ АСУД" in upper and "ЩД-АСУД" in upper and "ШАУВ" in upper:
        return PROFILE_AK_DISPATCH
    if "ШАУВ" in upper and "В СИСТЕМУ ДИСПЕТЧЕРИЗАЦИИ" in upper:
        return PROFILE_AK_CONTROL
    return None


def _source(pdf_path: Path, profile_id: str, block_id: Optional[str]) -> dict:
    return {"pdf_file": Path(pdf_path).name, "page_index": 0,
            "block_id": block_id, "profile_id": profile_id}


def _node(page, line, node_type: str, index: int, **extra) -> dict:
    x, y = line["center"]
    result = {
        "id": f"node-{index}", "label": line["text"], "node_type": node_type,
        "x": round(x, 3), "y": round(y, 3),
        "bbox_page": _bbox_page(line["bbox"], page),
        "container_ids": [], "field_state": "present",
    }
    result.update(extra)
    return result


def _unique_nodes(nodes: list[dict], *, spatial: bool = True) -> list[dict]:
    result, seen = [], set()
    for item in nodes:
        key = (item["node_type"], item["label"].lower())
        if spatial:
            key += (round(item["x"], 1), round(item["y"], 1))
        if key in seen:
            continue
        seen.add(key)
        item["id"] = f"node-{len(result) + 1}"
        result.append(item)
    return result


def _floor_scope(text: str) -> Optional[dict]:
    value = text.strip()
    if re.fullmatch(r"Кровля", value, re.I):
        return {"kind": "roof", "label": value}
    match = re.fullmatch(
        r"(-?\d+)(?:\s*(?:-|–|\.\.\.)\s*(-?\d+))?\s*этаж(?:и|а|ей)?(?:\s*\([^)]*\))?",
        value, re.I,
    )
    if not match:
        return None
    start = int(match.group(1))
    if match.group(2):
        return {"kind": "range", "from": start, "to": int(match.group(2)), "label": value}
    return {"kind": "single", "value": start, "label": value}


def _buildings(page, lines) -> list[dict]:
    rows = []
    for line in lines:
        match = re.fullmatch(r"(?:Корпус\s*(\d+)|(\d+)\s*Корпус)", line["text"], re.I)
        if not match:
            continue
        number = int(match.group(1) or match.group(2))
        rows.append({"id": f"building-{number}", "number": number,
                     "label": f"Корпус {number}", "x": round(line["center"][0], 3),
                     "y": round(line["center"][1], 3),
                     "bbox_page": _bbox_page(line["bbox"], page),
                     "floor_ids": [], "node_ids": []})
    # Один корпус может быть подписан повторно у внешней ссылки — берём верхнюю/основную подпись.
    by_number = {}
    for row in rows:
        previous = by_number.get(row["number"])
        if previous is None or row["y"] < previous["y"]:
            by_number[row["number"]] = row
    rows = sorted(by_number.values(), key=lambda item: item["x"])
    for index, row in enumerate(rows):
        left = 0.0 if index == 0 else (rows[index - 1]["x"] + row["x"]) / 2
        right = page.rect.width if index == len(rows) - 1 else (row["x"] + rows[index + 1]["x"]) / 2
        row["x_range"] = [round(left, 3), round(right, 3)]
    return rows


def _encoded_building(label: str, buildings: list[dict]) -> Optional[dict]:
    numbers = {item["number"]: item for item in buildings}
    patterns = (
        r"(?:ОСПД|ШСОУЭ|ВК|ШУЛ|АСКУ[ВТ]|ЩД-АСКУВТ)[._-]?(\d+)",
        r"(?:ШАУВ|ВРУ|ЩД-АСУД\.И).*?-К?(\d+)",
        r"(?:Л|SQ/Л\.)(\d+)\.",
    )
    for pattern in patterns:
        match = re.search(pattern, label, re.I)
        if match and int(match.group(1)) in numbers:
            return numbers[int(match.group(1))]
    return None


def _floors(page, lines, buildings) -> list[dict]:
    result = []
    for line in lines:
        scope = _floor_scope(line["text"])
        if not scope:
            continue
        containing = [item for item in buildings
                      if item["x_range"][0] <= line["center"][0] <= item["x_range"][1]]
        building = containing[0] if containing else None
        suffix = (scope.get("value") if scope["kind"] == "single" else
                  f"{scope.get('from')}-{scope.get('to')}" if scope["kind"] == "range" else "roof")
        result.append({
            "id": f"floor-{len(result) + 1}", "scope": scope,
            "building_id": building["id"] if building else None,
            "x": round(line["center"][0], 3), "y": round(line["center"][1], 3),
            "bbox_page": _bbox_page(line["bbox"], page), "node_ids": [],
            "semantic_key": str(suffix),
        })
    return result


def _bind_hierarchy(nodes, buildings, floors) -> None:
    by_id = {item["id"]: item for item in buildings}
    for node in nodes:
        building = _encoded_building(node["label"], buildings)
        if not building:
            containing = [item for item in buildings
                          if item["x_range"][0] <= node["x"] <= item["x_range"][1]]
            building = containing[0] if containing else None
        if building:
            node["building_id"] = building["id"]
            node["container_ids"].append(building["id"])
            building["node_ids"].append(node["id"])
            candidates = [item for item in floors if item.get("building_id") == building["id"]]
            if candidates:
                floor = min(candidates, key=lambda item: abs(item["y"] - node["y"]))
                node["floor_id"] = floor["id"]
                node["container_ids"].append(floor["id"])
                floor["node_ids"].append(node["id"])
    for building in buildings:
        building["floor_ids"] = [item["id"] for item in floors
                                  if item.get("building_id") == building["id"]]


def _references(text: str) -> list[str]:
    return sorted(set(re.findall(r"13АВ-РД-[А-ЯA-Z0-9._-]+", text, re.I)))


def _color_counts(page) -> dict:
    counts = collections.Counter()
    for drawing in page.get_drawings():
        color = drawing.get("color")
        if color is None:
            continue
        key = tuple(round(float(value), 2) for value in color)
        counts[str(key)] += sum(1 for item in drawing.get("items") or [] if item[0] == "l")
    return dict(counts)


def _base_graph(page, pdf_path, block_id, profile_id, *, containers, nodes,
                networks=None, edges=None, warnings=None, validation=None) -> dict:
    validation = dict(validation or {})
    semantic_ledger=[]
    for line in _lines(page):
        text=re.sub(r"\s+"," ",str(line.get("text") or "")).strip()
        if not text:continue
        x,y=line["center"]
        semantic_ledger.append({"id":f"text-{len(semantic_ledger)+1}","text":text,
          "bbox_page":_bbox_page(line["bbox"],page),"x":round(x,3),"y":round(y,3),
          "evidence_state":"текстовый слой PDF с координатами"})
    validation.update({
        "containers_total": len(containers), "nodes_total": len(nodes),
        "networks_total": len(networks or []), "edges_total": len(edges or []),
        "node_types": dict(collections.Counter(item["node_type"] for item in nodes)),
        "colored_line_segments": _color_counts(page),
        "coordinate_text_records_total":len(semantic_ledger),
        "coordinate_text_characters_total":sum(len(item["text"]) for item in semantic_ledger),
        "coordinate_text_coverage":"полный реестр строк PDF",
    })
    return {
        "schema_version": 1, "profile_id": profile_id,
        "source": _source(pdf_path, profile_id, block_id),
        "containers": containers, "nodes": nodes,
        "networks": networks or [], "edges": edges or [],
        "semantic_ledger":semantic_ledger,
        "external_references": _references(page.get_text()),
        "validation": validation, "warnings": warnings or [],
        "status": "ok",
    }


def _build_cctv(page, pdf_path, block_id):
    lines = _lines(page); buildings = _buildings(page, lines); floors = _floors(page, lines, buildings)
    patterns = (
        ("ospd_cabinet", re.compile(r"^ОСПД\d+(?:\.\d+)+$", re.I)),
        ("camera", re.compile(r"^ВК\d+(?:\.\d+){2,3}$", re.I)),
        ("lift_camera_cabinet", re.compile(r"^ШУЛ\d+(?:\.\d+){2,3}$", re.I)),
        ("workstation", re.compile(r"^АРМ СОТ$", re.I)),
        ("power_supply", re.compile(r"^UG\d+$", re.I)),
    )
    nodes=[]
    for line in lines:
        kind=next((k for k,p in patterns if p.fullmatch(line["text"])),None)
        if kind:nodes.append(_node(page,line,kind,len(nodes)+1))
    nodes=_unique_nodes(nodes); _bind_hierarchy(nodes,buildings,floors)
    networks=[]; edges=[]
    cabinets={n["label"]:n for n in nodes if n["node_type"]=="ospd_cabinet"}
    for camera in [n for n in nodes if n["node_type"] in ("camera","lift_camera_cabinet")]:
        match=re.match(r"^(?:ВК|ШУЛ)(\d+)\.(\d+)\.",camera["label"],re.I)
        cabinet=cabinets.get(f"ОСПД{match.group(1)}.{match.group(2)}") if match else None
        if not cabinet:continue
        nid=f"network-{len(networks)+1}"; networks.append({"id":nid,"network_type":"ethernet_domain",
            "label":f"{camera['label']} → {cabinet['label']}","endpoint_ids":[camera["id"],cabinet["id"]],
            "path_state":"confirmed_by_equipment_code"})
        edges.append({"id":f"edge-{len(edges)+1}","network_id":nid,"from":camera["id"],"to":cabinet["id"],
                      "edge_state":"semantic_confirmed"})
    bound=sum(bool(n.get("building_id")) for n in nodes)
    containers=buildings+floors
    return _base_graph(page,pdf_path,block_id,PROFILE_CCTV,containers=containers,nodes=nodes,
        networks=networks,edges=edges,validation={"buildings_total":len(buildings),"floor_bands_total":len(floors),
        "nodes_building_bound":bound,"building_bind_rate":round(bound/max(len(nodes),1),3),
        "topology_state":"equipment_code_confirmed"})


def _build_soue(page,pdf_path,block_id):
    lines=_lines(page); nodes=[]
    cabinets=[]; circuits=[]; rooms=[]; endpoints=[]
    for line in lines:
        t=line["text"]
        if re.fullmatch(r"ШСОУЭ\s*\d+\.\d+",t,re.I):
            n=_node(page,line,"voice_alarm_cabinet",len(nodes)+1); cabinets.append(n);nodes.append(n)
        elif re.fullmatch(r"V\d+\s*\(.*Р\s*=.*L\s*=.*\)",t,re.I):
            power=re.search(r"Р\s*=\s*(\d+)\s*Вт",t,re.I); length=re.search(r"L\s*=\s*(\d+)",t,re.I)
            n=_node(page,line,"speaker_circuit",len(nodes)+1,
                    attributes={"power_w":int(power.group(1)) if power else None,
                                "length_m":int(length.group(1)) if length else None})
            circuits.append(n);nodes.append(n)
        elif re.match(r"^пом\s*\.?",t,re.I) or t.lower()=="рампа":
            n=_node(page,line,"served_room",len(nodes)+1);rooms.append(n);nodes.append(n)
        elif re.fullmatch(r"ST\d+(?:\.\d+)+",t,re.I):
            n=_node(page,line,"line_terminator",len(nodes)+1);endpoints.append(n);nodes.append(n)
    nodes=_unique_nodes(nodes)
    cabinets=[n for n in nodes if n["node_type"]=="voice_alarm_cabinet"]
    circuits=[n for n in nodes if n["node_type"]=="speaker_circuit"]
    rooms=[n for n in nodes if n["node_type"]=="served_room"]
    endpoints=[n for n in nodes if n["node_type"]=="line_terminator"]
    containers=[];networks=[];edges=[];bound_members=set();bound_circuits=0
    for circuit in circuits:
        cabinet=min(cabinets,key=lambda n:math.hypot(n["x"]-circuit["x"],n["y"]-circuit["y"])) if cabinets else None
        members=[n for n in rooms+endpoints if abs(n["y"]-circuit["y"])<=28]
        cid=f"circuit-{len(containers)+1}";containers.append({"id":cid,"container_type":"speaker_line",
            "label":circuit["label"],"cabinet_id":cabinet["id"] if cabinet else None,
            "member_ids":[n["id"] for n in members]})
        for n in members:n["container_ids"].append(cid)
        network_id=f"network-{len(networks)+1}"
        networks.append({"id":network_id,"network_type":"speaker_radial",
            "label":circuit["label"],"endpoint_ids":[n["id"] for n in ([cabinet,circuit] if cabinet else [circuit])+members],
            "path_state":"row_geometry_grouped"})
        if cabinet:
            bound_circuits+=1;edges.append({"id":f"edge-{len(edges)+1}","network_id":network_id,
              "from":cabinet["id"],"to":circuit["id"],"edge_type":"cabinet_feeds_radial",
              "edge_state":"nearest_row_geometry"})
        for member in members:
            bound_members.add(member["id"]);edges.append({"id":f"edge-{len(edges)+1}","network_id":network_id,
              "from":circuit["id"],"to":member["id"],"edge_type":"radial_serves_endpoint",
              "edge_state":"same_row_geometry"})
    return _base_graph(page,pdf_path,block_id,PROFILE_SOUE,containers=containers,nodes=nodes,networks=networks,edges=edges,
        validation={"cabinets_total":len(cabinets),"circuits_total":len(circuits),"rooms_total":len(rooms),
                    "terminators_total":len(endpoints),"circuits_cabinet_bound":bound_circuits,
                    "members_bound":len(bound_members),
                    "member_bind_rate":round(len(bound_members)/max(len(rooms)+len(endpoints),1),3),
                    "topology_state":"radial_edges_confirmed_by_rows"})


def _build_fiber(page,pdf_path,block_id):
    lines=_lines(page);nodes=[];segments=[]
    for line in lines:
        t=line["text"]
        if re.fullmatch(r"(?:ШСПА|ШСОУЭ\d+\.\d+)",t,re.I):nodes.append(_node(page,line,"fiber_cabinet",len(nodes)+1))
        elif re.fullmatch(r"L\d+",t,re.I):segments.append(_node(page,line,"fiber_segment_label",len(nodes)+len(segments)+1))
        elif "Помещение СС" in t or t=="Автостоянка":nodes.append(_node(page,line,"location",len(nodes)+1))
    nodes=_unique_nodes(nodes+segments,spatial=False); segments=[n for n in nodes if n["node_type"]=="fiber_segment_label"]
    cabinets=[n for n in nodes if n["node_type"]=="fiber_cabinet"]
    networks=[{"id":"network-1","network_type":"fiber_ring","label":"ВОК L1–L9",
               "endpoint_ids":[n["id"] for n in cabinets],"segment_ids":[n["id"] for n in segments],
               "path_state":"closed_backbone_with_segment_labels"}]
    return _base_graph(page,pdf_path,block_id,PROFILE_FIBER,containers=[],nodes=nodes,networks=networks,
        validation={"cabinets_total":len(cabinets),"fiber_segments_total":len(segments),
                    "ring_closed":len(segments)>=9 and len(cabinets)>=7,"topology_state":"ring_confirmed"})


def _build_metering(page,pdf_path,block_id,profile_id):
    lines=_lines(page);buildings=_buildings(page,lines);floors=_floors(page,lines,buildings);nodes=[]
    prefix="АСКУВ" if profile_id==PROFILE_METER_WATER else "АСКУТ"
    patterns=(("meter_branch",re.compile(rf"^{prefix}-\d+\.\d+\s*\(\d+\s*счет\.\)$",re.I)),
              ("interface_splitter",re.compile(r"^РИ$",re.I)),
              ("data_collector",re.compile(r'^УСПД\s+"Пульсар"',re.I)),
              ("automation_panel",re.compile(r"^ЩД-АСКУВТ",re.I)),
              ("network_cabinet",re.compile(r"^ОСПД",re.I)),
              ("workstation",re.compile(r"^АРМ АСКУЭР$",re.I)),
              ("network_interface",re.compile(r"^(?:IP|ANT)-",re.I)),
              ("power_supply",re.compile(r"^БП\d",re.I)))
    for line in lines:
        kind=next((k for k,p in patterns if p.search(line["text"])),None)
        if not kind:continue
        attrs={}
        if kind=="meter_branch":
            m=re.search(r"\((\d+)\s*счет",line["text"],re.I);attrs["declared_meters"]=int(m.group(1)) if m else 0
        nodes.append(_node(page,line,kind,len(nodes)+1,attributes=attrs))
    nodes=_unique_nodes(nodes);_bind_hierarchy(nodes,buildings,floors)
    branches=[n for n in nodes if n["node_type"]=="meter_branch"]
    splitters=[n for n in nodes if n["node_type"]=="interface_splitter"]
    core=[n for n in nodes if n["node_type"] in ("data_collector","automation_panel","network_cabinet","workstation")]
    blue_segments=[]
    for drawing in page.get_drawings():
        if drawing.get("fill") is not None:continue
        color=tuple(round(float(value),2) for value in (drawing.get("color") or (0,0,0)))
        if color!=(0.0,0.0,1.0):continue
        for item in drawing.get("items") or []:
            if item[0]!="l":continue
            p1=(float(item[1].x),float(item[1].y));p2=(float(item[2].x),float(item[2].y));length=math.dist(p1,p2)
            if length>=2:blue_segments.append({"id":f"meter-segment-{len(blue_segments)+1}",
              "p1":p1,"p2":p2,"length":round(length,3),"color":color})
    component_graph=build_segment_components(blue_segments,tolerance=.7)
    component_by_segment={index:component_graph["component_by_node"].get(pair[0])
                          for index,pair in enumerate(component_graph["segment_nodes"])}
    attached=collections.defaultdict(list)
    for node in nodes:
        if not blue_segments:continue
        distance,index=min(((point_segment_distance((node["x"],node["y"]),segment),index)
                            for index,segment in enumerate(blue_segments)),key=lambda item:item[0])
        if distance<=35 and component_by_segment.get(index):
            node["meter_bus_component_id"]=component_by_segment[index];node["bus_distance"]=round(distance,3)
            attached[component_by_segment[index]].append(node)
    components={component["id"]:component for component in component_graph["components"]}
    networks=[];edges=[];bound_branches=set();bound_splitters=set()
    for component_id,members in attached.items():
        anchors=[node for node in members if node["node_type"] in ("meter_branch","data_collector","automation_panel")]
        if not anchors or len(members)<2:continue
        anchor=min(anchors,key=lambda node:(0 if node["node_type"]=="meter_branch" else 1,node["x"]))
        component=components[component_id];network_id=f"network-{len(networks)+1}"
        networks.append({"id":network_id,"network_type":"rs485_meter_bus","label":anchor["label"],
          "endpoint_ids":[node["id"] for node in members],
          "segment_ids":[blue_segments[index]["id"] for index in component["segment_indexes"]],
          "path_state":"blue_path_component"})
        for member in members:
            if member["id"]==anchor["id"]:continue
            edges.append({"id":f"edge-{len(edges)+1}","network_id":network_id,"from":anchor["id"],
              "to":member["id"],"edge_type":"member_of_meter_bus","edge_state":"blue_path_component"})
        bound_branches.update(n["id"] for n in members if n["node_type"]=="meter_branch")
        bound_splitters.update(n["id"] for n in members if n["node_type"]=="interface_splitter")
    networks.append({"id":f"network-{len(networks)+1}","network_type":"metering_core","label":f"{prefix} — магистраль",
                     "endpoint_ids":[n["id"] for n in core],"path_state":"equipment_role_inventory"})
    declared=sum(n["attributes"].get("declared_meters",0) for n in branches)
    graph=_base_graph(page,pdf_path,block_id,profile_id,containers=buildings+floors,nodes=nodes,networks=networks,edges=edges,
        validation={"buildings_total":len(buildings),"floor_bands_total":len(floors),"branches_total":len(branches),
                    "declared_meters_total":declared,"core_nodes_total":len(core),
                    "branches_path_bound":len(bound_branches),
                    "splitters_path_bound":len(bound_splitters),
                    "splitter_bind_rate":round(len(bound_splitters)/max(len(splitters),1),3),
                    "blue_bus_segments":len(blue_segments),"topology_state":"blue_bus_paths_confirmed"})
    graph["bus_segments"]=blue_segments
    return graph


def _ak_nodes(page, lines, dispatch: bool) -> list[dict]:
    patterns = [
        ("automation_cabinet",re.compile(r"^ШАУВ[- _А-ЯA-Z0-9./]+$",re.I)),
        ("ospd_cabinet",re.compile(r"^ОСПД[._А-ЯA-Z0-9-]+$",re.I)),
        ("distribution_panel",re.compile(r"^(?:ЩД-АСУД|ВРУ)[._А-ЯA-Z0-9-]+$",re.I)),
        ("workstation",re.compile(r"^АРМ АСУД$",re.I)),
    ]
    if dispatch:
        patterns += [("field_controller",re.compile(r"^(?:ДР|LS|СУ|CУ|БД)[-_А-ЯA-Z0-9.]+$",re.I)),
                     ("gateway",re.compile(r"^YCJ-RS002$",re.I))]
    else:
        patterns += [("engineering_system",re.compile(r"^(?:Вж|Вкр|Вк|Всу|Пл|Пж|ПД|У)\d[А-ЯA-Zа-я0-9.Б.-]*$",re.I)),
                     ("local_panel",re.compile(r"^ПУ-[А-ЯA-Z0-9.]+$",re.I))]
    nodes=[]
    for line in lines:
        kind=next((k for k,p in patterns if p.fullmatch(line["text"])),None)
        if kind:nodes.append(_node(page,line,kind,len(nodes)+1))
    return _unique_nodes(nodes)


def _build_ak(page,pdf_path,block_id,profile_id):
    dispatch=profile_id==PROFILE_AK_DISPATCH;lines=_lines(page);buildings=_buildings(page,lines)
    floors=_floors(page,lines,buildings);nodes=_ak_nodes(page,lines,dispatch);_bind_hierarchy(nodes,buildings,floors)
    networks=[];edges=[]
    if not dispatch:
        cabinets=[n for n in nodes if n["node_type"]=="automation_cabinet"]
        for system in [n for n in nodes if n["node_type"]=="engineering_system"]:
            same=[n for n in cabinets if n.get("building_id")==system.get("building_id")]
            if not same:continue
            cabinet=min(same,key=lambda n:math.hypot(n["x"]-system["x"],n["y"]-system["y"]))
            nid=f"network-{len(networks)+1}";networks.append({"id":nid,"network_type":"local_control",
                "label":f"{system['label']} → {cabinet['label']}","endpoint_ids":[system["id"],cabinet["id"]],
                "path_state":"nearest_same_building_geometry"})
            edges.append({"id":f"edge-{len(edges)+1}","network_id":nid,"from":system["id"],"to":cabinet["id"],
                          "edge_state":"geometry_associated"})
        systems=[n for n in nodes if n["node_type"]=="engineering_system"]
        for panel in [n for n in nodes if n["node_type"]=="local_panel"]:
            suffix=re.sub(r"^ПУ-", "", panel["label"], flags=re.I)
            candidates=[n for n in systems if n["label"].upper()==suffix.upper()
                        and n.get("building_id")==panel.get("building_id")]
            if not candidates:continue
            system=min(candidates,key=lambda n:math.hypot(n["x"]-panel["x"],n["y"]-panel["y"]))
            nid=f"network-{len(networks)+1}";networks.append({"id":nid,"network_type":"local_panel_control",
              "label":f"{panel['label']} → {system['label']}","endpoint_ids":[panel["id"],system["id"]],
              "path_state":"equipment_code_and_same_building"})
            edges.append({"id":f"edge-{len(edges)+1}","network_id":nid,"from":panel["id"],"to":system["id"],
                          "edge_type":"local_panel_controls_system","edge_state":"semantic_code_confirmed"})
    else:
        types=("automation_cabinet","distribution_panel","ospd_cabinet","field_controller","gateway","workstation")
        networks=[{"id":"network-1","network_type":"dispatch_integration","label":"АСУД.И",
                   "endpoint_ids":[n["id"] for n in nodes if n["node_type"] in types],
                   "path_state":"multicolor_backbone_grouped"}]
        def connect_tier(source_type,target_type,edge_type,*,same_building=True):
            targets=[n for n in nodes if n["node_type"]==target_type]
            for source in [n for n in nodes if n["node_type"]==source_type]:
                candidates=[n for n in targets if not same_building or n.get("building_id")==source.get("building_id")]
                if not candidates:continue
                target=min(candidates,key=lambda n:math.hypot(n["x"]-source["x"],n["y"]-source["y"]))
                edges.append({"id":f"edge-{len(edges)+1}","network_id":"network-1","from":source["id"],
                  "to":target["id"],"edge_type":edge_type,
                  "edge_state":"nearest_same_building_geometry" if same_building else "backbone_endpoint"})
        connect_tier("field_controller","automation_cabinet","field_to_automation")
        connect_tier("automation_cabinet","distribution_panel","automation_to_distribution")
        connect_tier("gateway","distribution_panel","gateway_to_distribution")
        connect_tier("distribution_panel","ospd_cabinet","distribution_to_ospd")
        connect_tier("ospd_cabinet","workstation","ospd_to_workstation",same_building=False)
    bound=sum(bool(n.get("building_id")) for n in nodes)
    connected={edge["from"] for edge in edges}|{edge["to"] for edge in edges}
    return _base_graph(page,pdf_path,block_id,profile_id,containers=buildings+floors,nodes=nodes,
        networks=networks,edges=edges,validation={"buildings_total":len(buildings),"floor_bands_total":len(floors),
        "nodes_building_bound":bound,"building_bind_rate":round(bound/max(len(nodes),1),3),
        "connected_nodes_total":len(connected),"connected_node_rate":round(len(connected)/max(len(nodes),1),3),
        "topology_state":"hierarchy_edges_confirmed" if edges else "backbone_grouped"})


def _build_lift_or_mgn(page,pdf_path,block_id,profile_id):
    lines=_lines(page);buildings=_buildings(page,lines);floors=_floors(page,lines,buildings);nodes=[]
    if profile_id==PROFILE_LIFT:
        patterns=(("lift_shaft",re.compile(r"^Шахта лифта",re.I)),("lift_block",re.compile(r"^ЛБ\d",re.I)),
          ("intercom",re.compile(r"^(?:УП|МПС|ПУЭП-Н)$",re.I)),("sensor",re.compile(r"^SQ/Л",re.I)),
          ("ospd_cabinet",re.compile(r"^ОСПД",re.I)),("workstation",re.compile(r"^АРМ АСУД$",re.I)),
          ("lift_station",re.compile(r"^Станция$",re.I)))
        network_type="lift_ethernet_and_intercom"
    else:
        patterns=(("call_unit",re.compile(r"^АПУ-2Н$",re.I)),("can_repeater",re.compile(r"^РШ$",re.I)),
          ("automation_panel",re.compile(r"^Щит ЩАСУД",re.I)),("ospd_cabinet",re.compile(r"^ОСПД",re.I)),
          ("workstation",re.compile(r"^АРМ АСУД$",re.I)))
        network_type="can_intercom_bus"
    for line in lines:
        kind=next((k for k,p in patterns if p.search(line["text"])),None)
        if kind:nodes.append(_node(page,line,kind,len(nodes)+1))
    nodes=_unique_nodes(nodes);_bind_hierarchy(nodes,buildings,floors)
    shafts=[n for n in nodes if n["node_type"]=="lift_shaft"]
    containers=buildings+floors+[{"id":f"shaft-{i+1}","container_type":"lift_shaft","label":n["label"],
                                "anchor_node_id":n["id"]} for i,n in enumerate(shafts)]
    networks=[{"id":"network-1","network_type":network_type,"label":network_type,
               "endpoint_ids":[n["id"] for n in nodes if n["node_type"]!="lift_shaft"],
               "path_state":"colored_bus_grouped"}]
    edges=[]
    def connect(source_type,target_type,edge_type,*,same_building=True):
        targets=[n for n in nodes if n["node_type"]==target_type]
        for source in [n for n in nodes if n["node_type"]==source_type]:
            candidates=[n for n in targets if not same_building or (
                        source.get("building_id") and n.get("building_id")==source.get("building_id"))]
            if same_building and not candidates:candidates=targets
            if not candidates:continue
            target=min(candidates,key=lambda n:math.hypot(n["x"]-source["x"],n["y"]-source["y"]))
            edges.append({"id":f"edge-{len(edges)+1}","network_id":"network-1","from":source["id"],
              "to":target["id"],"edge_type":edge_type,
              "edge_state":"nearest_same_building_geometry" if same_building else "backbone_endpoint"})
    if profile_id==PROFILE_LIFT:
        connect("sensor","lift_station","sensor_to_station")
        connect("lift_station","lift_block","station_to_lift_block")
        connect("intercom","lift_block","intercom_to_lift_block")
        connect("lift_block","lift_shaft","lift_block_to_shaft")
        connect("lift_block","ospd_cabinet","lift_to_ospd")
    else:
        connect("call_unit","can_repeater","call_unit_to_can_bus")
        connect("can_repeater","automation_panel","can_bus_to_panel")
        connect("automation_panel","ospd_cabinet","panel_to_ospd")
    connect("ospd_cabinet","workstation","ospd_to_workstation",same_building=False)
    connected={edge["from"] for edge in edges}|{edge["to"] for edge in edges}
    return _base_graph(page,pdf_path,block_id,profile_id,containers=containers,nodes=nodes,networks=networks,edges=edges,
        validation={"buildings_total":len(buildings),"floor_bands_total":len(floors),"shafts_total":len(shafts),
                    "connected_nodes_total":len(connected),
                    "connected_node_rate":round(len(connected)/max(len(nodes),1),3),
                    "topology_state":"hierarchy_edges_confirmed"})


_SONAR_RE=re.compile(r"^SONAR\s+[A-Z]+[A-Z0-9-]*(?:\s*\([^)]*\))?$",re.I)


def _build_cabinet(page,pdf_path,block_id,layout: bool):
    lines=_lines(page);nodes=[]
    for line in lines:
        if _SONAR_RE.fullmatch(line["text"]):nodes.append(_node(page,line,"rack_device",len(nodes)+1))
    nodes=_unique_nodes(nodes,spatial=True)
    spread_x=max((n["x"] for n in nodes),default=0)-min((n["x"] for n in nodes),default=0)
    spread_y=max((n["y"] for n in nodes),default=0)-min((n["y"] for n in nodes),default=0)
    order_axis="x" if spread_x>spread_y else "y"
    nodes.sort(key=lambda n:n[order_axis])
    for i,node in enumerate(nodes):node["physical_order"]=i+1
    profile=PROFILE_CABINET_LAYOUT if layout else PROFILE_CABINET_COMM
    container={"id":"cabinet-1","container_type":"telecommunication_cabinet","label":"ШСОУЭ0.1",
               "member_ids":[n["id"] for n in nodes]}
    networks=[];edges=[];route_segments=[];traced_types=set()
    if not layout:
        text=page.get_text()
        network_defs=(("power_230v","Линия питания 220В"),("power_24v","Линия питания 24В"),
          ("speaker","Линия оповещения"),("dap","Линия DAP-интерфейса"),
          ("ethernet","Линия связи Ethernet"),("fiber","Линия ВОЛС"),("aps","Адресная линия АПС"))
        legend={kind:label for kind,label in network_defs if label.lower() in text.lower()}
        for drawing in page.get_drawings():
            if drawing.get("fill") is not None:continue
            color=tuple(round(float(v),2) for v in (drawing.get("color") or (0,0,0)))
            if max(color)-min(color)<.2:continue
            for item in drawing.get("items") or []:
                if item[0]!="l":continue
                p1=(float(item[1].x),float(item[1].y));p2=(float(item[2].x),float(item[2].y));length=math.dist(p1,p2)
                if length>=1:route_segments.append({"id":f"cabinet-segment-{len(route_segments)+1}",
                  "p1":p1,"p2":p2,"length":round(length,3),"color":color})
        component_graph=build_segment_components(route_segments,tolerance=.7)
        positions=[n[order_axis] for n in nodes]
        maximum=max(positions,default=0)+max(300,spread_x,spread_y)
        bands=[]
        for index,node in enumerate(nodes):
            low=0 if index==0 else (positions[index-1]+positions[index])/2
            high=maximum if index==len(nodes)-1 else (positions[index]+positions[index+1])/2
            bands.append((node,low,high))
        orth_axis="y" if order_axis=="x" else "x"
        orth_values=[n[orth_axis] for n in nodes]
        orth_low=min(orth_values,default=0)-80;orth_high=max(orth_values,default=0)+650
        members=collections.defaultdict(set)
        for segment_index,segment in enumerate(route_segments):
            component_id=component_graph["component_by_node"].get(component_graph["segment_nodes"][segment_index][0])
            for node,low,high in bands:
                for point in (segment["p1"],segment["p2"]):
                    axis_value=point[0] if order_axis=="x" else point[1]
                    orth_value=point[1] if order_axis=="x" else point[0]
                    if low<=axis_value<=high and orth_low<=orth_value<=orth_high:
                        members[component_id].add(node["id"]);break
        component_by_id={component["id"]:component for component in component_graph["components"]}
        color_types={
          (1.0,0.0,0.0):"power_230v",(0.6,0.11,0.12):"power_24v",
          (0.0,0.0,1.0):"speaker",(1.0,0.0,1.0):"dap",(0.0,1.0,0.0):"ethernet",
          (0.25,0.0,1.0):"fiber",(1.0,0.5,0.0):"aps",(0.6,0.61,0.22):"aps",
        }
        for component_id,member_ids in members.items():
            if len(member_ids)<2:continue
            component=component_by_id[component_id];colors=collections.Counter(
              route_segments[index]["color"] for index in component["segment_indexes"])
            network_type=color_types.get(colors.most_common(1)[0][0],"colored_connection")
            traced_types.add(network_type);nid=f"network-{len(networks)+1}"
            ordered_ids=[n["id"] for n in nodes if n["id"] in member_ids]
            networks.append({"id":nid,"network_type":network_type,"label":legend.get(network_type,network_type),
              "endpoint_ids":ordered_ids,"segment_ids":[route_segments[i]["id"] for i in component["segment_indexes"]],
              "path_state":"colored_path_confirmed" if len(ordered_ids)==2 else "multi_device_bus"})
            if len(ordered_ids)==2:
                edges.append({"id":f"edge-{len(edges)+1}","network_id":nid,"from":ordered_ids[0],"to":ordered_ids[1],
                              "edge_type":network_type,"edge_state":"colored_path_confirmed"})
        for network_type,label in legend.items():
            if network_type not in traced_types:
                networks.append({"id":f"network-{len(networks)+1}","network_type":network_type,"label":label,
                                 "endpoint_ids":[],"path_state":"legend_only"})
    graph=_base_graph(page,pdf_path,block_id,profile,containers=[container],nodes=nodes,networks=networks,edges=edges,
        warnings=[] if layout else ["цветная сеть с более чем двумя аппаратами хранится без попарного разбиения"],
        validation={"rack_devices_total":len(nodes),"physical_order_complete":all(n.get("physical_order") for n in nodes),
                    "physical_order_axis":order_axis,"traced_network_types":len(traced_types),
                    "colored_connections":len(edges),"topology_state":"physical_layout" if layout else "colored_paths_confirmed"})
    if route_segments:graph["route_segments"]=route_segments
    return graph


def _build_functional(page,pdf_path,block_id):
    lines=_lines(page);nodes=[]
    patterns=(("process_system",re.compile(r"^П2\.Б2$",re.I)),("motor",re.compile(r"^М\d*$",re.I)),
      ("actuator",re.compile(r"^Y\d+$",re.I)),("temperature_sensor",re.compile(r"^(?:TE|TS)$",re.I)),
      ("pressure_sensor",re.compile(r"^PDS$",re.I)),("airflow",re.compile(r"^Приток$",re.I)),
      ("served_space",re.compile(r"^ПОН №2",re.I)))
    for line in lines:
        kind=next((k for k,p in patterns if p.search(line["text"])),None)
        if kind:nodes.append(_node(page,line,kind,len(nodes)+1))
    nodes=_unique_nodes(nodes)
    components=sorted([n for n in nodes if n["node_type"] not in ("served_space","process_system")],
                      key=lambda n:n["x"])
    clusters=[]
    for component in components:
        if not clusters or component["x"]-clusters[-1][-1]["x"]>45:clusters.append([component])
        else:clusters[-1].append(component)
    stations=[]
    for cluster in clusters:
        x=sum(n["x"] for n in cluster)/len(cluster);y=sum(n["y"] for n in cluster)/len(cluster)
        line={"text":f"процессная позиция {len(stations)+1}","bbox":(x,y,x,y),"center":(x,y)}
        station=_node(page,line,"process_station",len(nodes)+len(stations)+1,
                      member_ids=[n["id"] for n in cluster],station_state="x_cluster")
        station["id"]=f"station-{len(stations)+1}";stations.append(station)
        for member in cluster:member["process_station_id"]=station["id"]
    edges=[]
    for left,right in zip(stations,stations[1:]):
        edges.append({"id":f"edge-{len(edges)+1}","from":left["id"],"to":right["id"],
          "edge_type":"airflow_sequence","edge_state":"x_order_geometry"})
    for station,cluster in zip(stations,clusters):
        for member in cluster:
            edges.append({"id":f"edge-{len(edges)+1}","from":station["id"],"to":member["id"],
              "edge_type":"instrument_at_process_station","edge_state":"x_cluster_geometry"})
    served=[n for n in nodes if n["node_type"]=="served_space"]
    if stations and served:
        for space in served:
            edges.append({"id":f"edge-{len(edges)+1}","from":stations[-1]["id"],"to":space["id"],
              "edge_type":"airflow_serves_space","edge_state":"diagram_endpoint"})
    containers=[{"id":f"process-{i+1}","container_type":"process_system","label":system["label"],
                 "anchor_node_id":system["id"],"station_ids":[s["id"] for s in stations]}
                for i,system in enumerate(n for n in nodes if n["node_type"]=="process_system")]
    network={"id":"network-1","network_type":"air_process","label":"Приток → ПОН №2",
             "endpoint_ids":[s["id"] for s in stations]+[n["id"] for n in served],
             "path_state":"ordered_stations_with_instrument_members"}
    all_nodes=nodes+stations
    return _base_graph(page,pdf_path,block_id,PROFILE_FUNCTIONAL,containers=containers,nodes=all_nodes,
        networks=[network],edges=edges,validation={"process_nodes_total":len(components),
          "process_stations_total":len(stations),"station_members_bound":sum(len(c) for c in clusters),
          "process_sequence_length":len(stations),"topology_state":"process_station_graph"})


def _build_external(page,pdf_path,block_id):
    lines=_lines(page);nodes=[];cables=[]
    patterns=(("field_device",re.compile(r"^(?:TE|TS|PDS|М|Y)\d+$",re.I)),
      ("terminal_strip",re.compile(r"^ХТ\d+$",re.I)),("control_cabinet",re.compile(r"^ШАУВ",re.I)),
      ("power_input",re.compile(r"^Q\d+$",re.I)))
    for line in lines:
        kind=next((k for k,p in patterns if p.search(line["text"])),None)
        if kind:nodes.append(_node(page,line,kind,len(nodes)+1))
        if re.search(r"(?:КСП|ППГ).*мм",line["text"],re.I):cables.append(line["text"])
    nodes=_unique_nodes(nodes);devices=[n for n in nodes if n["node_type"]=="field_device"]
    cabinets=[n for n in nodes if n["node_type"]=="control_cabinet"]
    cabinet=max(cabinets,key=lambda n:n["x"]) if cabinets else None
    strips=[n for n in nodes if n["node_type"]=="terminal_strip"]
    connections=[];edges=[];strip_cabinet_edges=set()
    for device in devices:
        nearest=min(strips,key=lambda n:abs(n["x"]-device["x"])) if strips else None
        via=nearest if nearest and abs(nearest["x"]-device["x"])<=160 else None
        connection_id=f"connection-{len(connections)+1}"
        connections.append({"id":connection_id,"field_node_id":device["id"],
            "cabinet_node_id":cabinet["id"] if cabinet else None,"column_x":device["x"],
            "terminal_strip_id":via["id"] if via else None,"connection_state":"column_alignment"})
        target=via or cabinet
        if target:
            edges.append({"id":f"edge-{len(edges)+1}","network_id":"network-1","from":device["id"],
              "to":target["id"],"edge_type":"field_wiring_column","edge_state":"column_alignment"})
        if via and cabinet and via["id"] not in strip_cabinet_edges:
            strip_cabinet_edges.add(via["id"]);edges.append({"id":f"edge-{len(edges)+1}","network_id":"network-1",
              "from":via["id"],"to":cabinet["id"],"edge_type":"terminal_strip_to_cabinet",
              "edge_state":"enclosed_terminal_strip"})
    networks=[{"id":"network-1","network_type":"external_wiring","label":"ШАУВ — внешние подключения",
               "endpoint_ids":[n["id"] for n in nodes],"cable_types":sorted(set(cables)),
               "path_state":"column_and_terminal_labels"}]
    graph=_base_graph(page,pdf_path,block_id,PROFILE_EXTERNAL,containers=[],nodes=nodes,networks=networks,edges=edges,
        validation={"field_devices_total":len(devices),"terminal_strips_total":sum(n["node_type"]=="terminal_strip" for n in nodes),
                    "connections_total":len(connections),"cable_types_total":len(set(cables)),
                    "field_edges_total":sum(e["edge_type"]=="field_wiring_column" for e in edges),
                    "topology_state":"column_edges_confirmed"})
    graph["connections"]=connections
    return graph


def _build_niche(page,pdf_path,block_id):
    lines=_lines(page);nodes=[];systems=[];conduits=[]
    system_re=re.compile(r"^(?:СС|СС/АК|АК|СПЗ|СБ|СОТС?|СОВ|СОУЭ(?: ОСПД)?|ЭОМ(?:[-/].*)?)$",re.I)
    conduit_re=re.compile(r"^\d+\s*(?:шт\.)?\s*ВГП\s*Ду=\d+мм$",re.I)
    for line in lines:
        if system_re.fullmatch(line["text"]):
            n=_node(page,line,"discipline_allocation",len(nodes)+1);systems.append(n);nodes.append(n)
        elif conduit_re.fullmatch(line["text"]):
            n=_node(page,line,"conduit_group",len(nodes)+1);conduits.append(n);nodes.append(n)
        elif re.fullmatch(r"EI\d+",line["text"],re.I):nodes.append(_node(page,line,"fire_rating",len(nodes)+1))
        elif re.fullmatch(r"2\.МОП\.\d+",line["text"],re.I):nodes.append(_node(page,line,"room",len(nodes)+1))
    nodes=_unique_nodes(nodes)
    zones=[]
    for room in [n for n in nodes if n["node_type"]=="room"]:
        members=[n for n in nodes if n["node_type"]!="room" and math.hypot(n["x"]-room["x"],n["y"]-room["y"])<260]
        zones.append({"id":f"niche-zone-{len(zones)+1}","container_type":"niche","label":room["label"],
                      "member_ids":[n["id"] for n in members]})
    return _base_graph(page,pdf_path,block_id,PROFILE_NICHE,containers=zones,nodes=nodes,
        validation={"discipline_allocations_total":len([n for n in nodes if n["node_type"]=="discipline_allocation"]),
                    "conduit_groups_total":len([n for n in nodes if n["node_type"]=="conduit_group"]),
                    "niche_zones_total":len(zones),"topology_state":"physical_containment"})


def _build_page_graph(page, pdf_path: Path, block_id: Optional[str], profile_hint=None) -> Optional[dict]:
    profile=profile_hint or classify_alia_scheme_profile(page.get_text())
    if profile==PROFILE_CCTV:return _build_cctv(page,pdf_path,block_id)
    if profile==PROFILE_SOUE:return _build_soue(page,pdf_path,block_id)
    if profile==PROFILE_FIBER:return _build_fiber(page,pdf_path,block_id)
    if profile in (PROFILE_METER_WATER,PROFILE_METER_HEAT):return _build_metering(page,pdf_path,block_id,profile)
    if profile in (PROFILE_AK_CONTROL,PROFILE_AK_DISPATCH):return _build_ak(page,pdf_path,block_id,profile)
    if profile in (PROFILE_LIFT,PROFILE_MGN):return _build_lift_or_mgn(page,pdf_path,block_id,profile)
    if profile==PROFILE_CABINET_COMM:return _build_cabinet(page,pdf_path,block_id,False)
    if profile==PROFILE_CABINET_LAYOUT:return _build_cabinet(page,pdf_path,block_id,True)
    if profile==PROFILE_FUNCTIONAL:return _build_functional(page,pdf_path,block_id)
    if profile==PROFILE_EXTERNAL:return _build_external(page,pdf_path,block_id)
    if profile==PROFILE_NICHE:return _build_niche(page,pdf_path,block_id)
    return None


def build_alia_scheme_graph(pdf_path: Path, *, block_id: Optional[str]=None, profile_hint=None) -> Optional[dict]:
    try:
        import fitz
        doc=fitz.open(str(pdf_path))
    except Exception:
        return None
    try:
        if doc.page_count!=1:return None
        return _build_page_graph(doc[0], Path(pdf_path), block_id, profile_hint)
    except Exception:
        return None
    finally:
        doc.close()


def _clip_copied_page(page, pdf_points) -> None:
    commands=[f"{pdf_points[0][0]:.5f} {pdf_points[0][1]:.5f} m"]
    commands.extend(f"{x:.5f} {y:.5f} l" for x,y in pdf_points[1:])
    prefix=("q\n"+"\n".join(commands)+"\nh W n\n").encode("ascii")
    doc=page.parent;contents=page.get_contents()
    original=b"\n".join(doc.xref_stream(xref) for xref in contents)
    first=contents[0];doc.update_stream(first,prefix+original+b"\nQ\n")
    doc.xref_set_key(page.xref,"Contents",f"{first} 0 R")


def build_alia_scheme_graph_from_source(
    pdf_path: Path, *, page_index: int, bbox_norm, polygon_norm=None,
    block_id: Optional[str]=None, profile_hint=None,
) -> Optional[dict]:
    """Построить профиль прямо из блока многолистового исходного PDF."""
    try:
        import fitz
        source=fitz.open(str(pdf_path))
    except Exception:
        return None
    cropped=None
    try:
        if not bbox_norm or not (0<=page_index<source.page_count):return None
        source_page=source[page_index];w,h=source_page.rect.width,source_page.rect.height
        crop=fitz.Rect(float(bbox_norm[0])*w,float(bbox_norm[1])*h,
                       float(bbox_norm[2])*w,float(bbox_norm[3])*h)&source_page.rect
        if crop.is_empty:return None
        unrotated=crop*source_page.derotation_matrix;unrotated.normalize()
        # Координаты page.rect начинаются в (0, 0) текущего CropBox. set_cropbox
        # принимает координаты MediaBox, поэтому возвращаем исходное смещение.
        offset=source_page.cropbox_position
        unrotated=fitz.Rect(unrotated.x0+offset.x,unrotated.y0+offset.y,
                           unrotated.x1+offset.x,unrotated.y1+offset.y)
        cropped=fitz.open();cropped.insert_pdf(source,from_page=page_index,to_page=page_index)
        target=cropped[0]
        if polygon_norm:
            inverse=~source_page.transformation_matrix
            points=[tuple(fitz.Point(float(x)*w,float(y)*h)*source_page.derotation_matrix*inverse)
                    for x,y in polygon_norm]
            _clip_copied_page(target,points)
        target.set_cropbox(unrotated)
        return _build_page_graph(target,Path(pdf_path),block_id,profile_hint)
    except Exception:
        return None
    finally:
        if cropped is not None:cropped.close()
        source.close()


_GATE_RULES={
 PROFILE_CCTV:("hierarchy_and_code_edges",{"buildings_total":4,"nodes_total":20,"edges_total":10}),
 PROFILE_SOUE:("radial_line_structure",{"cabinets_total":2,"circuits_total":7,"rooms_total":30}),
 PROFILE_FIBER:("ring_structure",{"cabinets_total":7,"fiber_segments_total":9}),
 PROFILE_METER_WATER:("meter_bus",{"branches_total":5,"declared_meters_total":100}),
 PROFILE_METER_HEAT:("meter_bus",{"branches_total":4,"declared_meters_total":100}),
 PROFILE_AK_CONTROL:("control_hierarchy",{"buildings_total":5,"nodes_total":40,"edges_total":10}),
 PROFILE_AK_DISPATCH:("integration_inventory",{"buildings_total":5,"nodes_total":25,"networks_total":1}),
 PROFILE_LIFT:("lift_floor_inventory",{"floor_bands_total":20,"nodes_total":10}),
 PROFILE_MGN:("mgn_floor_inventory",{"floor_bands_total":20,"nodes_total":20}),
 PROFILE_CABINET_COMM:("cabinet_network_inventory",{"rack_devices_total":8,"networks_total":4}),
 PROFILE_CABINET_LAYOUT:("rack_layout",{"rack_devices_total":8}),
 PROFILE_FUNCTIONAL:("process_sequence",{"process_nodes_total":8,"edges_total":5}),
 PROFILE_EXTERNAL:("terminal_columns",{"field_devices_total":10,"connections_total":10}),
 PROFILE_NICHE:("physical_containment",{"discipline_allocations_total":6,"conduit_groups_total":3}),
}


def evaluate_alia_scheme_gate(graph: Optional[dict]) -> dict:
    if not graph:return {"use":False,"mode":"none","reasons":["граф не построен"],"metrics":{}}
    profile=graph.get("profile_id");validation=graph.get("validation") or {};mode,minimums=_GATE_RULES.get(profile,("unknown",{}))
    reasons=[]
    for key,minimum in minimums.items():
        value=validation.get(key,0)
        if value<minimum:reasons.append(f"{key}: {value} < {minimum}")
    complete_reasons=[]
    if profile==PROFILE_CCTV:
        if validation.get("building_bind_rate",0)<.9:complete_reasons.append("менее 90% узлов привязано к корпусам")
        if validation.get("edges_total",0)<10:complete_reasons.append("недостаточно кодово подтверждённых связей камер с ОСПД")
    elif profile==PROFILE_SOUE:
        if validation.get("circuits_cabinet_bound",0)<validation.get("circuits_total",0):
            complete_reasons.append("не все радиальные линии привязаны к шкафам")
        if validation.get("member_bind_rate",0)<.85:complete_reasons.append("менее 85% помещений/оконечников включено в радиалы")
    elif profile==PROFILE_FIBER:
        if not validation.get("ring_closed"):complete_reasons.append("кольцо ВОК не замкнуто")
    elif profile in (PROFILE_METER_WATER,PROFILE_METER_HEAT):
        if validation.get("branches_path_bound",0)<validation.get("branches_total",0):
            complete_reasons.append("не все подписанные ветви найдены на синих CAD-путях")
        if validation.get("splitter_bind_rate",0)<.8:complete_reasons.append("менее 80% РИ включено в шины")
    elif profile==PROFILE_AK_CONTROL:
        if validation.get("connected_node_rate",0)<.7:complete_reasons.append("менее 70% узлов включено в иерархию управления")
        if validation.get("edges_total",0)<10:complete_reasons.append("недостаточно подтверждённых связей управления")
    elif profile==PROFILE_AK_DISPATCH:
        if validation.get("connected_node_rate",0)<.9:complete_reasons.append("менее 90% узлов включено в иерархию диспетчеризации")
        if validation.get("edges_total",0)<20:complete_reasons.append("не построен многоуровневый backbone")
    elif profile==PROFILE_LIFT:
        if validation.get("connected_node_rate",0)<.8:complete_reasons.append("менее 80% лифтовых узлов включено в иерархию")
    elif profile==PROFILE_MGN:
        if validation.get("connected_node_rate",0)<.85:complete_reasons.append("менее 85% переговорных устройств включено в CAN-иерархию")
    elif profile==PROFILE_CABINET_COMM:
        if validation.get("colored_connections",0)<5:complete_reasons.append("меньше пяти связей подтверждено цветными путями")
        if validation.get("traced_network_types",0)<3:complete_reasons.append("протрассировано меньше трёх типов сетей шкафа")
    elif profile==PROFILE_CABINET_LAYOUT:
        if not validation.get("physical_order_complete"):complete_reasons.append("не восстановлен физический порядок оборудования")
    elif profile==PROFILE_FUNCTIONAL:
        if validation.get("process_stations_total",0)<3:complete_reasons.append("не построена последовательность процессных позиций")
        if validation.get("station_members_bound",0)<validation.get("process_nodes_total",0):
            complete_reasons.append("не все датчики/исполнители привязаны к процессным позициям")
    elif profile==PROFILE_EXTERNAL:
        if validation.get("field_edges_total",0)<validation.get("field_devices_total",0):
            complete_reasons.append("не все полевые устройства доведены до клемм/шкафа")
    elif profile==PROFILE_NICHE:
        if validation.get("niche_zones_total",0)<1:complete_reasons.append("не построены контейнеры ниш")
    else:complete_reasons.append("для профиля не определён строгий критерий полноты")
    readiness="complete" if not complete_reasons else "topology_partial"
    graph["readiness"]={"status":readiness,"complete":not complete_reasons,"reasons":complete_reasons}
    return {"use":not reasons,"complete":not complete_reasons,"readiness":readiness,
            "mode":mode,"reasons":reasons,"complete_reasons":complete_reasons,
            "warnings":list(graph.get("warnings") or []),"metrics":validation}


def render_alia_scheme_markdown(graph: dict) -> str:
    if not graph:return ""
    v=graph.get("validation") or {}
    lines=[f"# Эталонная текстовая разметка СС: {ru_profile(graph['profile_id'])}","",f"**Источник:** {graph['source']['pdf_file']}", "",
           f"Узлов: {v.get('nodes_total',0)}; контейнеров: {v.get('containers_total',0)}; "
           f"сетей: {v.get('networks_total',0)}; рёбер: {v.get('edges_total',0)}.","","## Узлы",""]
    groups=collections.defaultdict(list)
    for node in graph.get("nodes") or []:groups[node["node_type"]].append(node["label"])
    for kind,labels in sorted(groups.items()):
        preview=", ".join(labels[:24]);suffix=f" … (+{len(labels)-24})" if len(labels)>24 else ""
        lines.append(f"- **{ru_node_type(kind)}** ({len(labels)}): {preview}{suffix}")
    if graph.get("networks"):
        lines += ["","## Сети",""]
        for network in graph["networks"]:
            label = str(network.get("label") or "")
            label = label.replace(" external wiring", " — внешние подключения").replace(" core", " — магистраль")
            label_text = "" if re.fullmatch(r"[a-z0-9_]+", label) else f" — {label}"
            lines.append(f"- **{ru_network_type(network['network_type'])}**{label_text}; "
                         f"подтверждение: {ru_state(network.get('path_state'))}; "
                         f"узлов: {len(network.get('endpoint_ids') or [])}.")
    readiness=graph.get("readiness") or {}
    if readiness:
        lines += ["","## Готовность","",f"Состояние: **{ru_state(readiness.get('status'))}**."]
        lines += [f"- {reason}" for reason in readiness.get("reasons") or []]
    if graph.get("warnings"):
        lines += ["","## Ограничения",""]+[f"- {warning}" for warning in graph["warnings"]]
    return "\n".join(lines)+"\n"
