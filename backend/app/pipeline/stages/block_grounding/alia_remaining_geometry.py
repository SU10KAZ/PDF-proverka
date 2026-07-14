"""Профили оставшихся графических грамматик слаботочных систем ALIA."""
from __future__ import annotations

import collections
import math
import re
from pathlib import Path
from typing import Optional

from .alia_scheme_geometry import (
    _base_graph, _bbox_page, _center, _lines, _node,
    _unique_nodes, _clip_copied_page,
)
from .vector_path_graph import build_segment_components, point_segment_distance
from .profiled_graph_localization import (
    ru_node_type, ru_profile, ru_state, ru_subtype,
)


PROFILE_PLAN = "discipline_floor_plan"
PROFILE_WIRING = "device_terminal_wiring"
PROFILE_PRINCIPLE = "control_circuit_graph"
PROFILE_INSTALL = "installation_assembly"
PROFILE_PANEL = "physical_panel_layout"
PROFILE_ACCESS = "access_point_assembly"
ALL_REMAINING_PROFILES = (PROFILE_PLAN, PROFILE_WIRING, PROFILE_PRINCIPLE,
                          PROFILE_INSTALL, PROFILE_PANEL, PROFILE_ACCESS)

_ID_HINTS = {
 "4EVJ-MYPD-7P7":(PROFILE_PLAN,"cctv"),"9KLR-W3LY-EAA":(PROFILE_PLAN,"voice_alarm"),
 "TQVW-YHVA-NDM":(PROFILE_PLAN,"cable_tray"),"6FTW-WUQ7-MUY":(PROFILE_WIRING,"aps"),
 "9DUU-WMJM-L9E":(PROFILE_WIRING,"voice_alarm"),"FM6P-FX9L-NCQ":(PROFILE_WIRING,"metering"),
 "7YGP-T3UA-WN6":(PROFILE_WIRING,"access"),"9HAV-4CNX-H63":(PROFILE_WIRING,"camera"),
 "9WRW-TQYG-JG6":(PROFILE_PRINCIPLE,"metering_panel"),"6LCH-PCEN-6HU":(PROFILE_PRINCIPLE,"asud_panel"),
 "UYXQ-YMYF-CKF":(PROFILE_INSTALL,"sensor"),"7WXA-AQFE-PRY":(PROFILE_INSTALL,"equipment_mount"),
 "4KLT-LGFG-KCJ":(PROFILE_INSTALL,"firestop"),"6FEC-3LVL-WVQ":(PROFILE_INSTALL,"cable_entry"),
 "93XF-ML4D-VL3":(PROFILE_PANEL,"metering_panel"),"7CGA-7KRN-WWF":(PROFILE_PANEL,"asud_panel"),
 "7T3V-XE4Q-QQX":(PROFILE_PANEL,"aps_cabinet"),"9CGA-EMPC-RHP":(PROFILE_ACCESS,"gate"),
 "7KUU-4A7Y-LQ6":(PROFILE_ACCESS,"mortise_lock"),
}


def classify_remaining_profile(text: str, *, context_text: str="", block_id: Optional[str]=None,
                               prefer_block_hint: bool=True):
    if prefer_block_hint and block_id in _ID_HINTS:return _ID_HINTS[block_id]
    s=(text+" "+context_text).upper()
    if "ПЛАН" in s and any(x in s for x in ("РАЗМЕЩЕНИЯ","ПОДЗЕМНОЙ","РАСПОЛОЖЕНИЯ", "КАБЕЛЬНЫХ ТРАСС",
        "ВИДЕОНАБЛЮДЕНИЯ", "ДОМОФОН", "СКС", "СОУЭ", "АПС", "СЛАБОТОЧ")):
        subtype="cable_tray" if "КАБЕЛЕНЕСУЩ" in s or "ЛОТК" in s else "cctv" if "СОТ" in s else "voice_alarm"
        return PROFILE_PLAN,subtype
    if "СХЕМА ЭЛЕКТРИЧЕСКАЯ ПРИНЦИПИАЛЬНАЯ" in s or ("QF1" in s and "XS1" in s and "X0" in s):
        return PROFILE_PRINCIPLE,"control_panel"
    if any(x in s for x in ("СХЕМА ПОДКЛЮЧЕНИЙ","СХЕМА ЭЛЕКТРИЧЕСКИХ ПОДКЛЮЧЕНИЙ","СХЕМА ПОДКЛЮЧЕНИЯ УСТРОЙСТВА",
        "КЛЕММНАЯ СХЕМА", "RJ-45 (POE)","RS485-A")):
        return PROFILE_WIRING,"device"
    if any(x in s for x in ("МОНТАЖНАЯ СХЕМА","КРЕПЛЕНИЕ","ОГНЕСТОЙК","ОГНЕЗАЩИТ","УЗЕЛ ВВОДА",
        "УЗЕЛ ПРОХОДА", "ПРОХОДКА КАБЕЛЯ", "ГЕРМЕТИЗАЦ", "МОНТАЖНЫЙ УЗЕЛ", "УЗЕЛ УСТАНОВКИ")):
        return PROFILE_INSTALL,"assembly"
    if "УЗЛ" in s and any(x in s for x in ("СЛАБОТОЧ", "СЕТЕЙ СВЯЗИ", "КАБЕЛ", "ЛОТК", "ГИЛЬЗОПАКЕТ", "УЭРМ")):
        return PROFILE_INSTALL,"assembly"
    if any(x in s for x in ("ВИД СПЕРЕДИ","ФАСАД ШКАФА","ЭСКИЗ НАПОЛНЕНИЯ ШКАФА")):
        return PROFILE_PANEL,"cabinet"
    if "ВИД СО СТОРОНЫ ВХОДА" in s or "ЗАЩИЩАЕМОГО ПОМЕЩЕНИЯ" in s:
        return PROFILE_ACCESS,"access_point"
    return None,None


def _path_metrics(page) -> dict:
    counts=collections.Counter();segments=0
    for d in page.get_drawings():
        color=tuple(round(float(v),2) for v in (d.get("color") or (0,0,0)))
        n=sum(item[0]=="l" for item in d.get("items") or []);segments+=n;counts[str(color)]+=n
    return {"line_segments":segments,"segments_by_color":dict(counts),
            "vivid_segments":sum(n for c,n in counts.items() if c not in ("(0.0, 0.0, 0.0)","(0.5, 0.5, 0.5)"))}


def _token_nodes(page, patterns) -> list[dict]:
    nodes=[]
    for w in page.get_text("words"):
        token=str(w[4]).strip(" ,;:")
        kind=next((k for k,p in patterns if p.fullmatch(token)),None)
        if not kind:continue
        line={"text":token,"bbox":tuple(float(v) for v in w[:4]),"center":_center(w[:4])}
        nodes.append(_node(page,line,kind,len(nodes)+1))
    return _unique_nodes(nodes)


def _plan_axes(raw_axes, page):
    groups=collections.defaultdict(list)
    for node in raw_axes:groups[node["label"].upper()].append(node)
    axes=[]
    for label,items in groups.items():
        xs=[n["x"] for n in items];ys=[n["y"] for n in items]
        if len(items)<2:continue
        if max(ys)-min(ys)>page.rect.height*.35 and max(xs)-min(xs)<page.rect.width*.08:
            orientation="vertical";position=sum(xs)/len(xs)
        elif max(xs)-min(xs)>page.rect.width*.35 and max(ys)-min(ys)<page.rect.height*.08:
            orientation="horizontal";position=sum(ys)/len(ys)
        else:continue
        axes.append({"id":f"axis-{len(axes)+1}","label":items[0]["label"],"orientation":orientation,
                     "position":round(position,3),"anchor_bboxes_page":[n["bbox_page"] for n in items]})
    return axes


def _route_segments(page,subtype):
    colors={"cctv":{(0.0,0.0,1.0)},"voice_alarm":{(1.0,0.0,0.0),(1.0,0.5,0.0)},
            "cable_tray":{(0.0,0.0,1.0),(1.0,0.0,1.0),(1.0,0.0,0.0)}}.get(subtype,set())
    minimum={"cctv":2.0,"voice_alarm":5.0,"cable_tray":5.0}.get(subtype,5.0);segments=[]
    for d in page.get_drawings():
        color=tuple(round(float(v),2) for v in (d.get("color") or (0,0,0)))
        if color not in colors:continue
        for item in d.get("items") or []:
            if item[0]!="l":continue
            p1=(float(item[1].x),float(item[1].y));p2=(float(item[2].x),float(item[2].y));length=math.dist(p1,p2)
            if length>=minimum:segments.append({"id":f"route-segment-{len(segments)+1}","p1":p1,"p2":p2,
                                                "length":round(length,3),"color":color})
    return segments


def _wire_segments(page):
    segments=[]
    for d in page.get_drawings():
        color=tuple(round(float(v),2) for v in (d.get("color") or (0,0,0)))
        if color!=(0.0,0.0,0.0):continue
        for item in d.get("items") or []:
            if item[0]!="l":continue
            p1=(float(item[1].x),float(item[1].y));p2=(float(item[2].x),float(item[2].y));length=math.dist(p1,p2)
            if length>=1:segments.append({"id":f"wire-segment-{len(segments)+1}","p1":p1,"p2":p2,
                                          "length":round(length,3),"color":color})
    return segments


def _point_in_rect(point, bbox, margin=0.0):
    return (bbox[0]-margin<=point[0]<=bbox[2]+margin and
            bbox[1]-margin<=point[1]<=bbox[3]+margin)


def _principle_apparatus(page):
    """Аппараты и их внешние корпуса на принципиальной схеме.

    Корпус нужен не как декоративный контейнер: он задаёт границу, на которой
    провод становится внешней цепью аппарата. Вложенные таблицы клемм внутри
    корпуса не должны объединять независимые проводники.
    """
    patterns=(("breaker",re.compile(r"QF\d+",re.I)),
      ("terminal_block",re.compile(r"X(?:S)?\d+",re.I)),
      ("device_designator",re.compile(r"[AАGГ]\d+",re.I)),
      ("power_supply",re.compile(r"(?:UPS|ИБП|БП[A-ZА-Я0-9-]*)",re.I)))
    apparatus=_token_nodes(page,patterns)
    drawings=page.get_drawings();candidates=[]
    for drawing in drawings:
        rect=drawing.get("rect");kinds=tuple(item[0] for item in drawing.get("items") or [])
        if (not rect or kinds!=("qu",) or drawing.get("fill") is not None or
                not 20<=float(rect.width)<=400 or not 15<=float(rect.height)<=250):
            continue
        bbox=(float(rect.x0),float(rect.y0),float(rect.x1),float(rect.y1))
        nearby=[node for node in apparatus if _point_in_rect((node["x"],node["y"]),bbox,12)]
        if nearby:candidates.append({"bbox":bbox,"nearby":nearby})
    regions=[]
    for candidate in candidates:
        bbox=candidate["bbox"];area=(bbox[2]-bbox[0])*(bbox[3]-bbox[1])
        nested=any(
          other is not candidate and _point_in_rect((bbox[0],bbox[1]),other["bbox"],.2)
          and _point_in_rect((bbox[2],bbox[3]),other["bbox"],.2)
          and (other["bbox"][2]-other["bbox"][0])*(other["bbox"][3]-other["bbox"][1])>area
          for other in candidates)
        if nested:continue
        def priority(node):
            label=node["label"].upper()
            if re.fullmatch(r"[AАGГ]\d+",label):return 0
            if label.startswith(("БП","UPS")):return 1
            return 2
        owner=min(candidate["nearby"],key=priority)
        regions.append({"id":f"apparatus-region-{len(regions)+1}","bbox":bbox,"owner":owner,
                        "nearby":candidate["nearby"]})
    suppressed={node["id"] for region in regions for node in region["nearby"] if node is not region["owner"]}
    apparatus=[node for node in apparatus if node["id"] not in suppressed]
    for index,node in enumerate(apparatus,1):
        node["id"]=f"apparatus-{index}"
        node["apparatus_type"]=node.pop("node_type");node["node_type"]="apparatus"
    for region in regions:
        region["owner_id"]=region["owner"]["id"]
        region["bbox_page"]=_bbox_page(region.pop("bbox"),page)
        region.pop("owner");region.pop("nearby")
    return apparatus,regions


def _principle_segments(page,regions,terminal_blocks):
    """Внешние электрические линии без рамок аппаратов и шин клеммников."""
    absolute=[]
    for region in regions:
        b=region["bbox_page"]
        absolute.append((b[0]*page.rect.width,b[1]*page.rect.height,
                         b[2]*page.rect.width,b[3]*page.rect.height))
    raw=[];segments=[]
    for drawing in page.get_drawings():
        if drawing.get("fill") is not None:continue
        color=tuple(round(float(v),3) for v in (drawing.get("color") or (0,0,0)))
        # Проводники в ALIA экспортированы чёрным, RGB-красным, синим и зелёным.
        if not (max(color)<=.05 or max(color)-min(color)>=.45):continue
        for item in drawing.get("items") or []:
            if item[0]!="l":continue
            p1=(float(item[1].x),float(item[1].y));p2=(float(item[2].x),float(item[2].y))
            length=math.dist(p1,p2)
            if length<1:continue
            row={"id":f"principle-segment-{len(raw)+1}","p1":p1,"p2":p2,
                 "length":round(length,3),"color":color};raw.append(row)
            if any(_point_in_rect(p1,bbox,.4) and _point_in_rect(p2,bbox,.4) for bbox in absolute):
                continue
            # Горизонтальная черта X/XS является рамкой клеммника, а не перемычкой.
            horizontal=abs(p1[1]-p2[1])<=.5
            terminal_row=horizontal and any(
              abs((p1[1]+p2[1])/2-node["y"])<=15 and max(p1[0],p2[0])>=node["x"]-8
              and min(p1[0],p2[0])<=node["x"]+180 for node in terminal_blocks)
            if terminal_row:continue
            row["id"]=f"principle-segment-{len(segments)+1}";segments.append(row)
    return raw,segments,absolute


def _principle_ports(page,apparatus,regions,segments,absolute_regions):
    """Физические порты аппаратов: границы корпусов плюс подписи QF/X."""
    ports=[]
    apparatus_by_id={node["id"]:node for node in apparatus}
    # Для аппаратов в корпусе надёжнее сама точка пересечения проводом границы.
    for region,bbox in zip(regions,absolute_regions):
        owner=apparatus_by_id[region["owner_id"]];points=[]
        for segment in segments:
            for point in (segment["p1"],segment["p2"]):
                on_vertical=(abs(point[0]-bbox[0])<=1.1 or abs(point[0]-bbox[2])<=1.1) and bbox[1]-1<=point[1]<=bbox[3]+1
                on_horizontal=(abs(point[1]-bbox[1])<=1.1 or abs(point[1]-bbox[3])<=1.1) and bbox[0]-1<=point[0]<=bbox[2]+1
                if (on_vertical or on_horizontal) and not any(math.dist(point,old)<=1 for old in points):points.append(point)
        for point_index,point in enumerate(points,1):
            line={"text":f"порт {point_index}","bbox":(*point,*point),"center":point}
            ports.append(_node(page,line,"apparatus_port",len(ports)+1,parent_id=owner["id"],
                               parent_label=owner["label"],parent_source="enclosing_region_boundary",
                               port_state="geometry_anchor"))
    terminal_re=re.compile(r"(?:\d{1,2}|L|N|PE|[+\-]|J2|XP5|Ethernet|CAN-[A-ZА-Я]+)",re.I)
    words=page.get_text("words")
    terminal_blocks=[node for node in apparatus if node.get("apparatus_type")=="terminal_block"]
    breakers=[node for node in apparatus if node.get("apparatus_type")=="breaker"]
    for word in words:
        label=str(word[4]).strip(" ,;:")
        if not terminal_re.fullmatch(label):continue
        point=_center(word[:4]);parent=None;source=None
        candidates=[node for node in terminal_blocks if abs(point[1]-node["y"])<=18
                    and node["x"]-8<=point[0]<=node["x"]+180]
        if candidates:
            parent=min(candidates,key=lambda node:math.dist(point,(node["x"],node["y"])));source="terminal_row"
        if not parent:
            # Обозначение двухполюсного QF стоит слева от его контактов.
            candidates=[node for node in breakers if node["x"]-5<=point[0]<=node["x"]+60
                        and abs(point[1]-node["y"])<=55]
            if candidates:
                parent=max(candidates,key=lambda node:node["x"]);source="symbol_proximity"
        if not parent:continue
        line={"text":label,"bbox":tuple(float(v) for v in word[:4]),"center":point}
        ports.append(_node(page,line,"terminal_port",len(ports)+1,parent_id=parent["id"],
                           parent_label=parent["label"],parent_source=source,port_state="text_anchor"))
    # Один физический порт иногда имеет и геометрический, и текстовый якорь. Они
    # останутся разными provenance-записями, но ниже схлопнутся на уровне аппарата.
    for index,port in enumerate(ports,1):port["id"]=f"port-{index}"
    return ports


def _route_components(segments,tolerance=1.0):
    parent=list(range(len(segments)));by_point=collections.defaultdict(list)
    def find(i):
        while parent[i]!=i:parent[i]=parent[parent[i]];i=parent[i]
        return i
    def union(a,b):
        a,b=find(a),find(b)
        if a!=b:parent[b]=a
    for i,s in enumerate(segments):
        for p in (s["p1"],s["p2"]):by_point[(round(p[0]/tolerance),round(p[1]/tolerance))].append(i)
    for members in by_point.values():
        for other in members[1:]:union(members[0],other)
    grouped=collections.defaultdict(list)
    for i in range(len(segments)):grouped[find(i)].append(i)
    comps=[]
    for indexes in grouped.values():
        degree=collections.Counter()
        for i in indexes:
            for p in (segments[i]["p1"],segments[i]["p2"]):degree[(round(p[0]/tolerance),round(p[1]/tolerance))]+=1
        comps.append({"id":f"route-{len(comps)+1}","segment_ids":[segments[i]["id"] for i in indexes],
          "length":round(sum(segments[i]["length"] for i in indexes),3),"branch_points":sum(v>=3 for v in degree.values()),
          "endpoint_count":sum(v==1 for v in degree.values())})
    return comps


def _build_plan(page,pdf,block_id,subtype):
    raw_axes=_token_nodes(page,(("grid_axis",re.compile(r"(?:П\.)?[А-ЯA-Z]+|(?:П\.)?\d+(?:\.\d+)?",re.I)),))
    axes=_plan_axes(raw_axes,page)
    patterns={
      "cctv":(("camera",re.compile(r".*ВК\d+(?:\.\d+){2,3}.*",re.I)),("cabinet",re.compile(r".*ОСПД\s*\d+(?:\.\d+)+.*",re.I))),
      "voice_alarm":(("speaker",re.compile(r"ST\d+(?:\.\d+)+",re.I)),("cabinet",re.compile(r"ШСОУЭ\d+\.\d+",re.I))),
      "cable_tray":(("tray_callout",re.compile(r"(?:СПЗ|СБ|ЛК|LOK)\d*",re.I)),),
    }.get(subtype,())
    devices=_token_nodes(page,patterns)
    annotation_edges=[]
    if subtype=="cctv":
        expanded=[]
        for raw in devices:
            cabinet=re.search(r"ОСПД\s*\d+(?:\.\d+)+",raw["label"],re.I)
            cameras=re.findall(r"ВК\d+(?:\.\d+){2,3}",raw["label"],re.I)
            if not cabinet or not cameras:continue
            cabinet_node={**raw,"label":cabinet.group(0).replace(" ",""),"node_type":"cabinet"}
            camera_node={**raw,"label":cameras[-1],"node_type":"camera"}
            expanded.extend((cabinet_node,camera_node))
        devices=_unique_nodes(expanded,spatial=False)
    for i,n in enumerate(devices,1):n["id"]=f"equipment-{i}"
    rooms=[]
    for line in _lines(page):
        if re.match(r"^\d+\.[А-ЯA-Z0-9.]+\s+",line["text"],re.I):
            rooms.append({"id":f"room-{len(rooms)+1}","label":line["text"],"bbox_page":_bbox_page(line["bbox"],page),
                          "polygon":None,"polygon_state":"label_only"})
    segments=_route_segments(page,subtype);components=_route_components(segments);edges=[];attached=0
    for device in devices:
        if not segments:continue
        distance,segment=min(
            ((point_segment_distance((device["x"],device["y"]),s),s) for s in segments),
            key=lambda item:item[0],
        )
        if distance<=18:
            component=next(c for c in components if segment["id"] in c["segment_ids"]);device["route_id"]=component["id"]
            edges.append({"id":f"edge-{len(edges)+1}","from":device["id"],"to":component["id"],
                          "edge_type":"equipment_route_attachment","edge_state":"geometry_endpoint","distance":round(distance,3)});attached+=1
    if subtype=="cctv":
        cabinets={n["label"]:n for n in devices if n["node_type"]=="cabinet"}
        for camera in (n for n in devices if n["node_type"]=="camera"):
            m=re.match(r"ВК(\d+)\.(\d+)\.",camera["label"],re.I);cabinet=cabinets.get(f"ОСПД{m.group(1)}.{m.group(2)}") if m else None
            if cabinet:
                annotation_edges.append({"id":f"edge-annotation-{len(annotation_edges)+1}","from":camera["id"],"to":cabinet["id"],
                  "edge_type":"camera_cabinet","edge_state":"annotation_confirmed"})
        edges.extend(annotation_edges)
    vertical=sorted([a for a in axes if a["orientation"]=="vertical"],key=lambda a:a["position"])
    horizontal=sorted([a for a in axes if a["orientation"]=="horizontal"],key=lambda a:a["position"])
    containers=[{"id":"plan-1","container_type":"floor_plan","label":subtype,
                 "axis_ids":[a["id"] for a in axes],"room_ids":[r["id"] for r in rooms],"device_ids":[n["id"] for n in devices]}]
    networks=[{"id":c["id"],"network_type":"route_component","label":f"{subtype} {c['id']}",
               "endpoint_ids":[n["id"] for n in devices if n.get("route_id")==c["id"]],"path_state":"endpoint_connected",**c} for c in components]
    graph=_base_graph(page,pdf,block_id,PROFILE_PLAN,containers=containers,nodes=devices,networks=networks,edges=edges,
      validation={"subtype":subtype,"axes_total":len(axes),"vertical_axes":len(vertical),"horizontal_axes":len(horizontal),
       "rooms_total":len(rooms),"room_polygons_total":0,"devices_total":len(devices),"devices_route_attached":attached,
       "annotation_connections":len(annotation_edges),
       "device_route_bind_rate":round(attached/max(len(devices),1),3),"route_segments_total":len(segments),
       "route_components_total":len(components),"route_branches_total":sum(c["branch_points"] for c in components),
       "spatial_cells_total":max(len(vertical)-1,0)*max(len(horizontal)-1,0),
       **_path_metrics(page),"hierarchy_state":"partial" if not rooms else "labels_only",
       "topology_state":"route_graph_partial" if components else "not_extracted"},
      warnings=["контуры помещений ещё не восстановлены","чистое X-пересечение трасс не считается соединением"])
    graph["grid"]={"axes":axes,"cells_state":"derivable_from_axis_intervals"};graph["rooms"]=rooms;graph["route_segments"]=segments
    return graph


def _build_wiring(page,pdf,block_id,subtype):
    terminal_patterns={
      "aps":r"(?:[+\-](?:\d+[НК]|U\d+)|ЭКР|R3-Link|АЛС|X\d+\.\d+|ШС\d+|ОТКР\d*|ЗАКР\d*|ВХОД|ВЫХ|БР|N|L|PE)",
      "voice_alarm":r"(?:N|L|PE|[+\-]Р|Земля|АЛС|[+\-]24В|ВХОД|ВЫХОД|БР|ПИТ\.?)",
      "metering":r"(?:RS485|RS-485|GND|[ABАВ]|[+\-]|Х\d+)",
      "access":r"(?:D0|D1/T?|DOOR\d*|EXIT\d*|GND\d*|RED|GREEN|BEEP|SENS\d*|[+\-]D|COM|NC|NO|PE|N|L|АЛС)",
      "camera":r"(?:RJ-45|PoE|ВК|ОСПД\d+(?:\.\d+)*)",
    }
    patterns=(("terminal",re.compile(terminal_patterns.get(subtype,r"X\d+"),re.I)),
      ("apparatus",re.compile(r"(?:ИВЭПР|АРК|АМ|РМ|МДУ|УПД|РИ|АСКУВ|ВК|ОСПД)[A-ZА-Я0-9._/-]*",re.I)),
      ("cable",re.compile(r"(?:U/UTP|КСП|КИС|МКШ)[A-ZА-Я0-9()./_-]*",re.I)))
    nodes=_token_nodes(page,patterns);paths=_path_metrics(page)
    if subtype=="metering":
        nodes=[n for n in nodes if n["node_type"]!="terminal" or n["label"] not in ("а","б","в","a","b")]
        for i,n in enumerate(nodes,1):n["id"]=f"node-{i}"
    terminals=[n for n in nodes if n["node_type"]=="terminal"]
    segments=_wire_segments(page);components=_route_components(segments,.7);by_component=collections.defaultdict(list);attached=0
    component_by_segment={sid:c["id"] for c in components for sid in c["segment_ids"]}
    for terminal in terminals:
        if not segments:continue
        distance,segment=min(((point_segment_distance((terminal["x"],terminal["y"]),s),s) for s in segments),key=lambda x:x[0])
        attach_limit=25 if subtype=="metering" else 15
        if distance<=attach_limit:
            cid=component_by_segment[segment["id"]];terminal["wire_component_id"]=cid
            terminal["wire_distance"]=round(distance,3);by_component[cid].append(terminal);attached+=1
    networks=[];edges=[]
    for cid,members in by_component.items():
        if len(members)<2:continue
        state="confirmed_pair" if len(members)==2 else "multi_terminal_review"
        nid=f"network-{len(networks)+1}";networks.append({"id":nid,"network_type":subtype,"label":cid,
          "endpoint_ids":[n["id"] for n in members],"path_state":state})
        if len(members)==2:edges.append({"id":f"edge-{len(edges)+1}","network_id":nid,"from":members[0]["id"],
                                        "to":members[1]["id"],"edge_state":"path_confirmed"})
    if subtype=="camera":
        camera=next((n for n in nodes if n["label"].upper()=="ВК"),None)
        cabinet=next((n for n in nodes if n["label"].upper().startswith("ОСПД")),None)
        if camera and cabinet:
            nid=f"network-{len(networks)+1}";networks.append({"id":nid,"network_type":"poe_ethernet",
              "label":"RJ-45 PoE","endpoint_ids":[camera["id"],cabinet["id"]],"path_state":"annotation_confirmed"})
            edges.append({"id":f"edge-{len(edges)+1}","network_id":nid,"from":camera["id"],"to":cabinet["id"],
                          "edge_state":"annotation_confirmed"})
    graph=_base_graph(page,pdf,block_id,PROFILE_WIRING,containers=[],nodes=nodes,networks=networks,edges=edges,
      validation={"subtype":subtype,"terminals_total":len(terminals),"components_total":len(nodes)-len(terminals),
       "wire_segments_total":len(segments),"wire_components_total":len(components),"terminals_wire_attached":attached,
       "terminal_attach_rate":round(attached/max(len(terminals),1),3),"terminal_networks_total":len(networks),
       "confirmed_connections":len(edges),"multi_terminal_networks":sum(n["path_state"]=="multi_terminal_review" for n in networks),
       **paths,"topology_state":"partial_confirmed" if edges else "semantic_partial"},
      warnings=["многоклеммные сети не разбиваются на выдуманные пары"])
    graph["wire_segments"]=segments
    return graph


def _build_principle(page,pdf,block_id,subtype):
    apparatus,regions=_principle_apparatus(page)
    terminal_blocks=[node for node in apparatus if node.get("apparatus_type")=="terminal_block"]
    raw_segments,segments,absolute_regions=_principle_segments(page,regions,terminal_blocks)
    ports=_principle_ports(page,apparatus,regions,segments,absolute_regions)
    apparatus_by_id={node["id"]:node for node in apparatus}
    for region in regions:
        owner=apparatus_by_id[region["owner_id"]]
        region.update({"container_type":"apparatus_body","label":owner["label"],
                       "member_ids":[owner["id"]]+[port["id"] for port in ports
                                                      if port["parent_id"]==owner["id"]]})
    component_graph=build_segment_components(segments,tolerance=.7)
    component_by_segment={index:component_graph["component_by_node"].get(pair[0])
                          for index,pair in enumerate(component_graph["segment_nodes"])}
    members=collections.defaultdict(list);attached=0
    for port in ports:
        if not segments:continue
        distance,index=min(((point_segment_distance((port["x"],port["y"]),segment),index)
                            for index,segment in enumerate(segments)),key=lambda item:item[0])
        limit=2 if port["port_state"]=="geometry_anchor" else 15
        if distance<=limit and component_by_segment.get(index):
            cid=component_by_segment[index];port["circuit_component_id"]=cid
            port["wire_distance"]=round(distance,3);members[cid].append(port);attached+=1
    component_rows={component["id"]:component for component in component_graph["components"]}
    networks=[];edges=[];connected_apparatus=set();multi=0
    for cid,items in members.items():
        by_parent=collections.defaultdict(list)
        for item in items:by_parent[item["parent_id"]].append(item)
        if len(by_parent)<2:continue
        component=component_rows[cid]
        endpoint_rows=[{"apparatus_id":parent_id,"apparatus":ports_for_parent[0]["parent_label"],
                        "port_ids":[port["id"] for port in ports_for_parent],
                        "terminal_labels":sorted({port["label"] for port in ports_for_parent
                                                  if port["node_type"]=="terminal_port"})}
                       for parent_id,ports_for_parent in by_parent.items()]
        state="confirmed_pair" if len(by_parent)==2 else "multi_apparatus_bus"
        if state=="multi_apparatus_bus":multi+=1
        nid=f"circuit-{len(networks)+1}"
        networks.append({"id":nid,"network_type":"control_and_power","label":cid,
          "endpoint_ids":[row["apparatus_id"] for row in endpoint_rows],"endpoints":endpoint_rows,
          "path_state":state,"segment_ids":[segments[i]["id"] for i in component["segment_indexes"]],
          "length":round(sum(segments[i]["length"] for i in component["segment_indexes"]),3)})
        connected_apparatus.update(by_parent)
        if len(by_parent)==2:
            left,right=endpoint_rows
            edges.append({"id":f"edge-{len(edges)+1}","network_id":nid,"from":left["apparatus_id"],
              "to":right["apparatus_id"],"from_port_ids":left["port_ids"],"to_port_ids":right["port_ids"],
              "edge_type":"electrical_path","edge_state":"path_confirmed"})
    paths=_path_metrics(page);all_nodes=apparatus+ports
    graph=_base_graph(page,pdf,block_id,PROFILE_PRINCIPLE,containers=regions,nodes=all_nodes,
      networks=networks,edges=edges,validation={"subtype":subtype,"apparatus_total":len(apparatus),
       "apparatus_attached":len(connected_apparatus),
       "apparatus_attach_rate":round(len(connected_apparatus)/max(len(apparatus),1),3),
       "ports_total":len(ports),"ports_wire_attached":attached,
       "port_attach_rate":round(attached/max(len(ports),1),3),"wire_segments_raw":len(raw_segments),
       "wire_segments_total":len(segments),"circuit_components_total":len(component_graph["components"]),
       "confirmed_pair_circuits":len(edges),"multi_apparatus_circuits":multi,**paths,
       "topology_state":"confirmed_with_buses" if edges else "circuit_inventory"},
      warnings=["многоаппаратные шины хранятся как гиперсети без выдуманных попарных рёбер",
                "внутренняя графика корпуса аппарата исключена из внешней электрической цепи"])
    graph["wire_segments"]=segments
    return graph


def _build_install(page,pdf,block_id,subtype):
    lines=_lines(page);nodes=[]
    skip=re.compile(r"^(?:Изм\.|Дата|Разраб|Проверил|Лист)",re.I)
    for line in lines:
        t=line["text"]
        if skip.search(t) or len(t)<3:continue
        kind="dimension" if re.fullmatch(r"[ØRr]?\d+(?:[-xх×.]\d+)*(?:мм|м)?",t,re.I) else "assembly_part"
        nodes.append(_node(page,line,kind,len(nodes)+1))
    nodes=_unique_nodes(nodes);parts=[n for n in nodes if n["node_type"]=="assembly_part"]
    return _base_graph(page,pdf,block_id,PROFILE_INSTALL,
      containers=[{"id":"assembly-1","container_type":subtype,"label":subtype,"member_ids":[n["id"] for n in nodes]}],
      nodes=nodes,validation={"subtype":subtype,"parts_total":len(parts),"dimensions_total":len(nodes)-len(parts),
                             **_path_metrics(page),"topology_state":"physical_assembly"})


def _build_panel(page,pdf,block_id,subtype):
    lines=_lines(page);nodes=[]
    for line in lines:
        t=line["text"]
        if re.fullmatch(r"(?:QF|X|XS|UPS|ИВЭПР|АМ|R3|Концентратор)[A-ZА-Я0-9._/-]*",t,re.I):
            nodes.append(_node(page,line,"panel_component",len(nodes)+1))
        elif re.fullmatch(r"\d{2,4}",t):nodes.append(_node(page,line,"dimension",len(nodes)+1))
    nodes=_unique_nodes(nodes)
    for i,n in enumerate(sorted((n for n in nodes if n["node_type"]=="panel_component"),key=lambda x:x["y"]),1):n["physical_order"]=i
    return _base_graph(page,pdf,block_id,PROFILE_PANEL,
      containers=[{"id":"panel-1","container_type":"panel","label":subtype,"member_ids":[n["id"] for n in nodes]}],
      nodes=nodes,validation={"subtype":subtype,"components_total":sum(n["node_type"]=="panel_component" for n in nodes),
      "dimensions_total":sum(n["node_type"]=="dimension" for n in nodes),**_path_metrics(page),"topology_state":"physical_layout"})


def _build_access(page,pdf,block_id,subtype):
    nodes=_token_nodes(page,(("callout",re.compile(r"[1-9]")),("dimension",re.compile(r"\d{3,4}(?:-\d{3,4})?мм?",re.I))))
    views=[line for line in _lines(page) if line["text"].lower().startswith("вид со стороны")]
    containers=[{"id":f"view-{i+1}","container_type":"access_view","label":line["text"],
                 "bbox_page":_bbox_page(line["bbox"],page)} for i,line in enumerate(views)]
    return _base_graph(page,pdf,block_id,PROFILE_ACCESS,containers=containers,nodes=nodes,
      validation={"subtype":subtype,"views_total":len(views),"callouts_total":sum(n["node_type"]=="callout" for n in nodes),
      "dimensions_total":sum(n["node_type"]=="dimension" for n in nodes),**_path_metrics(page),"topology_state":"access_assembly"})


def _dispatch(page,pdf,block_id,context="",profile_hint=None,subtype_hint=None):
    classified_profile,classified_subtype=classify_remaining_profile(page.get_text(),context_text=context,block_id=block_id)
    profile=profile_hint or classified_profile;subtype=subtype_hint or classified_subtype or "блок слаботочных систем"
    if profile==PROFILE_PLAN:return _build_plan(page,pdf,block_id,subtype)
    if profile==PROFILE_WIRING:return _build_wiring(page,pdf,block_id,subtype)
    if profile==PROFILE_PRINCIPLE:return _build_principle(page,pdf,block_id,subtype)
    if profile==PROFILE_INSTALL:return _build_install(page,pdf,block_id,subtype)
    if profile==PROFILE_PANEL:return _build_panel(page,pdf,block_id,subtype)
    if profile==PROFILE_ACCESS:return _build_access(page,pdf,block_id,subtype)
    return None


def build_remaining_graph(pdf_path:Path,*,block_id=None,context_text="",profile_hint=None,subtype_hint=None):
    try:
        import fitz;doc=fitz.open(str(pdf_path))
        if doc.page_count!=1:doc.close();return None
        result=_dispatch(doc[0],Path(pdf_path),block_id,context_text,profile_hint,subtype_hint);doc.close();return result
    except Exception:return None


def build_remaining_graph_from_source(pdf_path:Path,*,page_index:int,bbox_norm,polygon_norm=None,block_id=None,context_text="",profile_hint=None,subtype_hint=None):
    try:
        import fitz;src=fitz.open(str(pdf_path));sp=src[page_index];w,h=sp.rect.width,sp.rect.height
        crop=fitz.Rect(bbox_norm[0]*w,bbox_norm[1]*h,bbox_norm[2]*w,bbox_norm[3]*h)&sp.rect
        ur=crop*sp.derotation_matrix;ur.normalize();off=sp.cropbox_position
        ur=fitz.Rect(ur.x0+off.x,ur.y0+off.y,ur.x1+off.x,ur.y1+off.y)
        out=fitz.open();out.insert_pdf(src,from_page=page_index,to_page=page_index);p=out[0]
        if polygon_norm:
            inv=~sp.transformation_matrix;points=[tuple(fitz.Point(x*w,y*h)*sp.derotation_matrix*inv) for x,y in polygon_norm]
            _clip_copied_page(p,points)
        p.set_cropbox(ur);g=_dispatch(p,Path(pdf_path),block_id,context_text,profile_hint,subtype_hint);out.close();src.close();return g
    except Exception:return None


_MIN={PROFILE_PLAN:("spatial_plan",{"line_segments":100}),PROFILE_WIRING:("terminal_inventory",{"line_segments":20}),
 PROFILE_PRINCIPLE:("circuit_inventory",{"apparatus_total":3}),PROFILE_INSTALL:("assembly",{"parts_total":3}),
 PROFILE_PANEL:("physical_panel",{"line_segments":50}),PROFILE_ACCESS:("access_assembly",{"callouts_total":3})}


def evaluate_remaining_gate(graph):
    if not graph:return {"use":False,"mode":"none","reasons":["граф не построен"],"metrics":{}}
    mode,req=_MIN[graph["profile_id"]];v=graph["validation"];reasons=[f"{k}: {v.get(k,0)} < {n}" for k,n in req.items() if v.get(k,0)<n]
    complete_reasons=[];profile=graph["profile_id"]
    if profile==PROFILE_PLAN:
        subtype=v.get("subtype")
        if v.get("axes_total",0)<2:complete_reasons.append("не построена координатная сетка")
        if v.get("route_components_total",0)<1:complete_reasons.append("не построен граф трасс")
        if subtype=="cctv" and v.get("annotation_connections",0)<1:
            complete_reasons.append("камеры не связаны со шкафами по кабельным аннотациям")
        if subtype=="voice_alarm" and v.get("devices_route_attached",0)<3:
            complete_reasons.append("не найдены три фактических подключения шкафов к трассам")
    elif profile==PROFILE_WIRING:
        if v.get("subtype")!="camera" and v.get("terminal_attach_rate",0)<.8:
            complete_reasons.append("менее 80% клемм привязано к проводам")
        if v.get("confirmed_connections",0)<1:complete_reasons.append("нет подтверждённых пар")
    elif profile==PROFILE_PRINCIPLE:
        if v.get("confirmed_pair_circuits",0)<2:complete_reasons.append("меньше двух подтверждённых межаппаратных цепей")
        if v.get("apparatus_attach_rate",0)<.7:complete_reasons.append("менее 70% аппаратов включено в подтверждённые цепи")
        if v.get("port_attach_rate",0)<.8:complete_reasons.append("менее 80% извлечённых портов привязано к проводникам")
    elif profile==PROFILE_INSTALL:
        if v.get("parts_total",0)<3:complete_reasons.append("неполный состав сборки")
    elif profile==PROFILE_PANEL:
        if v.get("components_total",0)<3:complete_reasons.append("неполный состав щита")
    elif profile==PROFILE_ACCESS:
        if v.get("views_total",0)<2 or v.get("callouts_total",0)<3:complete_reasons.append("неполная сборка точки доступа")
    readiness="complete" if not complete_reasons else (
        "topology_partial" if profile in (PROFILE_PLAN,PROFILE_WIRING,PROFILE_PRINCIPLE) else "hierarchy_built"
    )
    graph["readiness"]={"status":readiness,"complete":not complete_reasons,"reasons":complete_reasons}
    return {"use":not reasons,"complete":not complete_reasons,"readiness":readiness,
            "mode":mode,"reasons":reasons,"complete_reasons":complete_reasons,
            "warnings":graph.get("warnings",[]),"metrics":v}


def render_remaining_markdown(graph):
    v=graph["validation"];lines=[f"# Эталонная текстовая разметка СС: {ru_profile(graph['profile_id'])}","",
      f"**Назначение блока:** {ru_subtype(v.get('subtype'))}",
      f"Узлов: {v['nodes_total']}; контейнеров: {v['containers_total']}; линейных сегментов: {v.get('line_segments',0)}.","","## Состав",""]
    by=collections.defaultdict(list)
    for n in graph["nodes"]:by[n["node_type"]].append(n["label"])
    for k,labels in sorted(by.items()):lines.append(f"- **{ru_node_type(k)}** ({len(labels)}): {', '.join(labels[:30])}")
    if graph["profile_id"]==PROFILE_PLAN:
        axes=(graph.get("grid") or {}).get("axes") or []
        lines += ["","## Координатная сетка","",
          f"Вертикальных осей: {sum(a['orientation']=='vertical' for a in axes)}; "
          f"горизонтальных: {sum(a['orientation']=='horizontal' for a in axes)}; "
          f"пространственных ячеек: {v.get('spatial_cells_total',0)}.","","## Трассы",""]
        for route_number,route in enumerate((graph.get("networks") or [])[:40], 1):
            lines.append(f"- Трасса {route_number}: сегментов {len(route.get('segment_ids') or [])}, "
                         f"длина {route.get('length')}, ветвлений {route.get('branch_points')}, "
                         f"концов {route.get('endpoint_count')}")
        lines += ["",f"Привязано устройств к трассам: {v.get('devices_route_attached',0)}/"
                  f"{v.get('devices_total',0)}."]
    elif graph["profile_id"]==PROFILE_PRINCIPLE:
        lines += ["","## Электрическая топология","",
          f"Аппаратов в цепях: {v.get('apparatus_attached',0)}/{v.get('apparatus_total',0)}; "
          f"портов на проводниках: {v.get('ports_wire_attached',0)}/{v.get('ports_total',0)}; "
          f"подтверждённых пар: {v.get('confirmed_pair_circuits',0)}; "
          f"многоаппаратных шин: {v.get('multi_apparatus_circuits',0)}."]
        for network_number,network in enumerate((graph.get("networks") or [])[:40], 1):
            labels=" ↔ ".join(endpoint.get("apparatus","") for endpoint in network.get("endpoints") or [])
            lines.append(f"- Цепь {network_number}: {labels}; подтверждение — {ru_state(network['path_state'])}.")
    readiness=graph.get("readiness") or {}
    if readiness:
        lines += ["","## Готовность","",f"Состояние: **{ru_state(readiness.get('status'))}**."]
        lines += [f"- {x}" for x in readiness.get("reasons") or []]
    if graph.get("warnings"):lines += ["","## Ограничения",""]+[f"- {x}" for x in graph["warnings"]]
    return "\n".join(lines)+"\n"
