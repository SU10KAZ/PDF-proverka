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
    if "АПУ-2Н" in upper and "ИНТЕРФЕЙСА CAN" in upper:
        return PROFILE_MGN
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
    validation.update({
        "containers_total": len(containers), "nodes_total": len(nodes),
        "networks_total": len(networks or []), "edges_total": len(edges or []),
        "node_types": dict(collections.Counter(item["node_type"] for item in nodes)),
        "colored_line_segments": _color_counts(page),
    })
    return {
        "schema_version": 1, "profile_id": profile_id,
        "source": _source(pdf_path, profile_id, block_id),
        "containers": containers, "nodes": nodes,
        "networks": networks or [], "edges": edges or [],
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
    containers=[];networks=[]
    for circuit in circuits:
        cabinet=min(cabinets,key=lambda n:math.hypot(n["x"]-circuit["x"],n["y"]-circuit["y"])) if cabinets else None
        members=[n for n in rooms+endpoints if abs(n["y"]-circuit["y"])<=28]
        cid=f"circuit-{len(containers)+1}";containers.append({"id":cid,"container_type":"speaker_line",
            "label":circuit["label"],"cabinet_id":cabinet["id"] if cabinet else None,
            "member_ids":[n["id"] for n in members]})
        for n in members:n["container_ids"].append(cid)
        networks.append({"id":f"network-{len(networks)+1}","network_type":"speaker_radial",
            "label":circuit["label"],"endpoint_ids":[n["id"] for n in ([cabinet] if cabinet else [])+members],
            "path_state":"row_geometry_grouped"})
    return _base_graph(page,pdf_path,block_id,PROFILE_SOUE,containers=containers,nodes=nodes,networks=networks,
        validation={"cabinets_total":len(cabinets),"circuits_total":len(circuits),"rooms_total":len(rooms),
                    "terminators_total":len(endpoints),"topology_state":"radial_rows_grouped"})


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
    core=[n for n in nodes if n["node_type"] in ("data_collector","automation_panel","network_cabinet","workstation")]
    networks=[{"id":"network-1","network_type":"rs485_meter_bus","label":prefix,
               "endpoint_ids":[n["id"] for n in branches+core],"path_state":"branch_labels_and_colored_bus"}]
    declared=sum(n["attributes"].get("declared_meters",0) for n in branches)
    return _base_graph(page,pdf_path,block_id,profile_id,containers=buildings+floors,nodes=nodes,networks=networks,
        validation={"buildings_total":len(buildings),"floor_bands_total":len(floors),"branches_total":len(branches),
                    "declared_meters_total":declared,"core_nodes_total":len(core),"topology_state":"bus_grouped"})


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
    else:
        types=("automation_cabinet","distribution_panel","ospd_cabinet","field_controller","gateway","workstation")
        networks=[{"id":"network-1","network_type":"dispatch_integration","label":"АСУД.И",
                   "endpoint_ids":[n["id"] for n in nodes if n["node_type"] in types],
                   "path_state":"multicolor_backbone_grouped"}]
    bound=sum(bool(n.get("building_id")) for n in nodes)
    return _base_graph(page,pdf_path,block_id,profile_id,containers=buildings+floors,nodes=nodes,
        networks=networks,edges=edges,validation={"buildings_total":len(buildings),"floor_bands_total":len(floors),
        "nodes_building_bound":bound,"building_bind_rate":round(bound/max(len(nodes),1),3),
        "topology_state":"geometry_associated" if edges else "backbone_grouped"})


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
    return _base_graph(page,pdf_path,block_id,profile_id,containers=containers,nodes=nodes,networks=networks,
        validation={"buildings_total":len(buildings),"floor_bands_total":len(floors),"shafts_total":len(shafts),
                    "topology_state":"bus_grouped"})


_SONAR_RE=re.compile(r"^SONAR\s+[A-Z]+[A-Z0-9-]*(?:\s*\([^)]*\))?$",re.I)


def _build_cabinet(page,pdf_path,block_id,layout: bool):
    lines=_lines(page);nodes=[]
    for line in lines:
        if _SONAR_RE.fullmatch(line["text"]):nodes.append(_node(page,line,"rack_device",len(nodes)+1))
    nodes=_unique_nodes(nodes,spatial=False)
    nodes.sort(key=lambda n:n["y"])
    for i,node in enumerate(nodes):node["physical_order"]=i+1
    profile=PROFILE_CABINET_LAYOUT if layout else PROFILE_CABINET_COMM
    container={"id":"cabinet-1","container_type":"telecommunication_cabinet","label":"ШСОУЭ0.1",
               "member_ids":[n["id"] for n in nodes]}
    networks=[]
    if not layout:
        text=page.get_text()
        network_defs=(("power_230v","Линия питания 220В"),("power_24v","Линия питания 24В"),
          ("speaker","Линия оповещения"),("dap","Линия DAP-интерфейса"),
          ("ethernet","Линия связи Ethernet"),("fiber","Линия ВОЛС"),("aps","Адресная линия АПС"))
        networks=[{"id":f"network-{i+1}","network_type":kind,"label":label,"endpoint_ids":[],
                   "path_state":"legend_present_ports_require_tracing"}
                  for i,(kind,label) in enumerate(network_defs) if label.lower() in text.lower()]
    return _base_graph(page,pdf_path,block_id,profile,containers=[container],nodes=nodes,networks=networks,
        warnings=[] if layout else ["межпортовые рёбра не выдаются до трассировки цветных концов"],
        validation={"rack_devices_total":len(nodes),"physical_order_complete":all(n.get("physical_order") for n in nodes),
                    "topology_state":"physical_layout" if layout else "ports_inventory"})


def _build_functional(page,pdf_path,block_id):
    lines=_lines(page);nodes=[]
    patterns=(("process_system",re.compile(r"^П2\.Б2$",re.I)),("motor",re.compile(r"^М\d*$",re.I)),
      ("actuator",re.compile(r"^Y\d+$",re.I)),("temperature_sensor",re.compile(r"^(?:TE|TS)$",re.I)),
      ("pressure_sensor",re.compile(r"^PDS$",re.I)),("airflow",re.compile(r"^Приток$",re.I)),
      ("served_space",re.compile(r"^ПОН №2",re.I)))
    for line in lines:
        kind=next((k for k,p in patterns if p.search(line["text"])),None)
        if kind:nodes.append(_node(page,line,kind,len(nodes)+1))
    nodes=_unique_nodes(nodes);process=[n for n in nodes if n["node_type"] not in ("served_space",)]
    sequence=sorted(process,key=lambda n:(n["x"],n["y"]))
    edges=[{"id":f"edge-{i+1}","from":sequence[i]["id"],"to":sequence[i+1]["id"],
            "edge_type":"process_order","edge_state":"x_order_geometry"} for i in range(max(0,len(sequence)-1))]
    return _base_graph(page,pdf_path,block_id,PROFILE_FUNCTIONAL,containers=[],nodes=nodes,edges=edges,
        validation={"process_nodes_total":len(process),"process_sequence_length":len(sequence),
                    "topology_state":"process_order_geometry"})


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
    cabinet=next((n for n in nodes if n["node_type"]=="control_cabinet"),None)
    connections=[]
    for device in devices:
        connections.append({"id":f"connection-{len(connections)+1}","field_node_id":device["id"],
            "cabinet_node_id":cabinet["id"] if cabinet else None,"column_x":device["x"],
            "connection_state":"column_alignment"})
    networks=[{"id":"network-1","network_type":"external_wiring","label":"ШАУВ external wiring",
               "endpoint_ids":[n["id"] for n in nodes],"cable_types":sorted(set(cables)),
               "path_state":"column_and_terminal_labels"}]
    graph=_base_graph(page,pdf_path,block_id,PROFILE_EXTERNAL,containers=[],nodes=nodes,networks=networks,
        validation={"field_devices_total":len(devices),"terminal_strips_total":sum(n["node_type"]=="terminal_strip" for n in nodes),
                    "connections_total":len(connections),"cable_types_total":len(set(cables)),
                    "topology_state":"column_aligned"})
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


def _build_page_graph(page, pdf_path: Path, block_id: Optional[str]) -> Optional[dict]:
    profile=classify_alia_scheme_profile(page.get_text())
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


def build_alia_scheme_graph(pdf_path: Path, *, block_id: Optional[str]=None) -> Optional[dict]:
    try:
        import fitz
        doc=fitz.open(str(pdf_path))
    except Exception:
        return None
    try:
        if doc.page_count!=1:return None
        return _build_page_graph(doc[0], Path(pdf_path), block_id)
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
    block_id: Optional[str]=None,
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
        return _build_page_graph(target,Path(pdf_path),block_id)
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
    return {"use":not reasons,"mode":mode,"reasons":reasons,
            "warnings":list(graph.get("warnings") or []),"metrics":validation}


def render_alia_scheme_markdown(graph: dict) -> str:
    if not graph:return ""
    v=graph.get("validation") or {}
    lines=[f"# ALIA: {graph['profile_id']}","",f"Источник: `{graph['source']['pdf_file']}`", "",
           f"Узлов: {v.get('nodes_total',0)}; контейнеров: {v.get('containers_total',0)}; "
           f"сетей: {v.get('networks_total',0)}; рёбер: {v.get('edges_total',0)}.","","## Узлы",""]
    groups=collections.defaultdict(list)
    for node in graph.get("nodes") or []:groups[node["node_type"]].append(node["label"])
    for kind,labels in sorted(groups.items()):
        preview=", ".join(labels[:24]);suffix=f" … (+{len(labels)-24})" if len(labels)>24 else ""
        lines.append(f"- **{kind}** ({len(labels)}): {preview}{suffix}")
    if graph.get("networks"):
        lines += ["","## Сети",""]
        for network in graph["networks"]:
            lines.append(f"- `{network['network_type']}` — {network['label']} "
                         f"[{network.get('path_state','')}] ({len(network.get('endpoint_ids') or [])} узлов)")
    if graph.get("warnings"):
        lines += ["","## Ограничения",""]+[f"- {warning}" for warning in graph["warnings"]]
    return "\n".join(lines)+"\n"
