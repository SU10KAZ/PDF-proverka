"""Детерминированные логические описания графических блоков ОВ.

Профиль читает только текстовый и CAD-слои PDF. На планах и схемах
чистое X-пересечение линий не образует связь: компоненты собираются по
совпадающим концам CAD-отрезков.
"""
from __future__ import annotations

import collections
import math
import re
from pathlib import Path
from typing import Optional

from .vector_path_graph import point_segment_distance


PROFILE_PLAN="hvac_floor_plan"
PROFILE_HEATING="heating_axonometry"
PROFILE_VENT="ventilation_axonometry"
PROFILE_HYDRONIC="hydronic_principle"
PROFILE_DETAIL="hvac_installation_detail"
PROFILE_SECTION="hvac_section_layout"
PROFILE_EQUIPMENT="hvac_equipment_drawing"
PROFILE_CHART="hvac_performance_chart"
PROFILE_SITE="hvac_site_overview"
ALL_HVAC_PROFILES=(PROFILE_PLAN,PROFILE_HEATING,PROFILE_VENT,PROFILE_HYDRONIC,PROFILE_DETAIL,
                   PROFILE_SECTION,PROFILE_EQUIPMENT,PROFILE_CHART,PROFILE_SITE)


_ID_HINTS={
 "4E3Q-H7XC-R3E":(PROFILE_PLAN,"heating_technical_floor"),"XDVL-N9MY-WPD":(PROFILE_PLAN,"parking_heat_supply"),
 "6449-PTQM-3CE":(PROFILE_PLAN,"ventilation_chamber"),"7HHP-346Y-QQN":(PROFILE_PLAN,"smoke_control_floor"),
 "6NVH-VX34-T9J":(PROFILE_PLAN,"roof_ventilation"),"6UYH-QKDN-FCV":(PROFILE_PLAN,"duct_support_frames"),
 "4MDN-MDUE-J3J":(PROFILE_HEATING,"register_branches"),"HWHE-WFDG-X33":(PROFILE_HEATING,"parking_heating"),
 "4GA3-VAUX-GMW":(PROFILE_HEATING,"residential_manifold"),"69MR-KRXY-PGN":(PROFILE_HEATING,"interbuilding_backbone"),
 "74Y6-7MAY-4LL":(PROFILE_HEATING,"vertical_riser"),"94LF-KX9G-4K7":(PROFILE_HEATING,"air_curtain_heat_supply"),
 "9TA4-TWED-GPD":(PROFILE_HEATING,"air_heater_heat_supply"),
 "6YJU-G3H6-AKY":(PROFILE_VENT,"residential_exhaust_and_smoke"),"73DW-EPE3-L9J":(PROFILE_VENT,"general_supply_exhaust"),
 "6DFX-9QLG-GWK":(PROFILE_VENT,"parking_exhaust_smoke"),"4FF3-LUTX-TNE":(PROFILE_VENT,"smoke_control_risers"),
 "D93H-NJWV-PGN":(PROFILE_VENT,"multiple_exhaust_systems"),
 "46E6-AM6E-P9J":(PROFILE_HYDRONIC,"mixing_unit"),"9DJT-YMQJ-FPR":(PROFILE_HYDRONIC,"collector_and_room_input"),
 "99HG-J7DH-GEX":(PROFILE_HYDRONIC,"heat_exchanger_circuit"),
 "9CMD-MH46-FHH":(PROFILE_DETAIL,"radiator_connection"),"7GRK-4L7N-4E3":(PROFILE_DETAIL,"convector_and_vertical_radiator"),
 "JUXA-A7YA-VCQ":(PROFILE_DETAIL,"pipe_floor_passages"),"97XN-PGV6-TDR":(PROFILE_DETAIL,"riser_supports"),
 "JJ6P-YNNT-NHA":(PROFILE_DETAIL,"duct_wall_fire_damper"),"7WM4-YRVT-XWR":(PROFILE_DETAIL,"grille_and_fire_damper"),
 "9UX3-LLAF-KMM":(PROFILE_DETAIL,"roof_equipment_supports"),
 "6MQV-RVD4-6TC":(PROFILE_SECTION,"air_heater_piping_sections"),"7FMY-7KPW-9XE":(PROFILE_SECTION,"parking_vent_chambers"),
 "4MAN-KE3V-6KR":(PROFILE_SECTION,"duct_and_equipment_sections"),
 "U44J-7AAC-6XE":(PROFILE_EQUIPMENT,"heat_exchanger"),"66YJ-FKDN-E7P":(PROFILE_EQUIPMENT,"ahu_section_composition"),
 "7HRD-GYUE-34H":(PROFILE_EQUIPMENT,"ahu_exploded_view"),
 "7XPK-GJMK-7W7":(PROFILE_CHART,"pump_curve"),"6G36-HFKH-CQV":(PROFILE_CHART,"fan_curve"),
 "9RKR-4Y9V-76N":(PROFILE_SITE,"site_heat_source"),
}


def classify_hvac_profile(text:str,*,block_id:Optional[str]=None,prefer_block_hint:bool=True):
    if prefer_block_hint and block_id in _ID_HINTS:return _ID_HINTS[block_id]
    upper=re.sub(r"\s+"," ",text or "").upper()
    if "АЭРОДИНАМИЧЕСК" in upper or ("Q," in upper and any(x in upper for x in ("PS","PV","НАПОР","КПД"))):
        return PROFILE_CHART,"performance_curve"
    if any(x in upper for x in ("ОБЩИЙ ВИД", "ВЗРЫВ-СХЕМ", "ВЗРЫВ СХЕМ")) and any(
        x in upper for x in ("УСТАНОВК", "ВЕНТИЛЯТОР", "ТЕПЛООБМЕННИК")
    ):
        return PROFILE_EQUIPMENT,"equipment_drawing"
    if "СМЕСИТЕЛЬН" in upper or "ПРИНЦИПИАЛЬНАЯ ГИДРАВЛИЧЕСКАЯ" in upper:
        return PROFILE_HYDRONIC,"hydronic"
    if ("УЗЛ" in upper and any(x in upper for x in ("ВОЗДУХОВОД", "ВЕНТИЛЯЦ", "КЛАПАН", "ТРАВЕРС", "КОММУНИКАЦ", "ТРУБОПРОВОД"))) or any(x in upper for x in ("УЗЕЛ ПРОХОДА","СКОЛЬЗЯЩАЯ ОПОРА","НЕПОДВИЖНАЯ ОПОРА","ВНУТРИПОЛЬН",
        "УЗЕЛ КРЕПЛЕНИЯ", "УЗЕЛ УСТАНОВКИ", "МОНТАЖНЫЙ УЗЕЛ", "ПРОХОД ЧЕРЕЗ")):
        return PROFILE_DETAIL,"installation"
    if "ПЛАН" in upper and any(x in upper for x in ("ОТОПЛЕН","ВЕНТИЛЯЦ","ТЕПЛОСНАБЖ","КРОВЛ", "ХОЛОДОСНАБЖ", "ДЫМОУДАЛ")):
        return PROFILE_PLAN,"floor"
    if "РАЗРЕЗ" in upper and any(x in upper for x in ("ВОЗДУХОВОД","ВЕНТКАМЕР","ВОЗДУХОНАГРЕВ")):
        return PROFILE_SECTION,"section"
    if "АКСОНОМЕТ" in upper or "ИЗОМЕТРИЧ" in upper:
        if any(x in upper for x in ("ВЕНТИЛЯЦ", "ВОЗДУХОВОД", "ВОЗДУХОЗАБОР", "ДЫМОУДАЛ", "ВЕНТИЛЯЦИОНН", "КЛАПАН")):return PROFILE_VENT,"axonometry"
        if any(x in upper for x in ("ОТОПЛЕН", "ТЕПЛОСНАБЖ", "Т11", "Т21")):return PROFILE_HEATING,"axonometry"
    if "ГЕНЕРАЛЬНЫЙ ПЛАН" in upper and any(x in upper for x in ("ТЕПЛОСНАБЖ", "ТЕПЛОВ", "ОВ")):
        return PROFILE_SITE,"site"
    vent_marks=re.findall(r"(?<![A-ZА-Я0-9])(?:В(?:Ж|А|КР|М|ОС|УС|СУ|Х|ПМ)?|П(?:Д|А|КР|ПМ)?|ДУ)[A-ZА-Я]*\d+(?:[.,]\d+)*",upper)
    if len(vent_marks)>=1 or ("ВЕНТИЛЯЦ" in upper and any(x in upper for x in ("АКСОНОМЕТ", "ИЗОМЕТРИЧ", "СХЕМ"))):
        return PROFILE_VENT,"axonometry"
    if len(re.findall(r"\b(?:СТ\.|Т11|Т21|Т12|Т22)",upper))>=1 or ("ОТОПЛЕН" in upper and any(x in upper for x in ("АКСОНОМЕТ", "СХЕМ"))):
        return PROFILE_HEATING,"axonometry"
    return None,None


def _point(page,x,y):
    if not page.rotation:return float(x),float(y)
    import fitz
    p=fitz.Point(float(x),float(y))*page.rotation_matrix
    return float(p.x),float(p.y)


def _bbox(page,bbox):
    points=[_point(page,bbox[0],bbox[1]),_point(page,bbox[2],bbox[1]),
            _point(page,bbox[2],bbox[3]),_point(page,bbox[0],bbox[3])]
    xs=[p[0] for p in points];ys=[p[1] for p in points]
    return min(xs),min(ys),max(xs),max(ys)


def _center(bbox):return (float(bbox[0])+float(bbox[2]))/2,(float(bbox[1])+float(bbox[3]))/2


def _bbox_norm(page,bbox):
    return [round(bbox[0]/page.rect.width,6),round(bbox[1]/page.rect.height,6),
            round(bbox[2]/page.rect.width,6),round(bbox[3]/page.rect.height,6)]


def _lines(page):
    result=[]
    for block in page.get_text("dict").get("blocks") or []:
        for line in block.get("lines") or []:
            text=" ".join(str(span.get("text") or "").strip() for span in line.get("spans") or []
                          if str(span.get("text") or "").strip())
            text=re.sub(r"\s+"," ",text).strip()
            if not text:continue
            bbox=_bbox(page,line["bbox"]);result.append({"text":text,"bbox":bbox,"center":_center(bbox)})
    return result


def _words(page):
    result=[]
    for word in page.get_text("words"):
        bbox=_bbox(page,word[:4]);result.append({"text":str(word[4]).strip(" ,;:"),"bbox":bbox,"center":_center(bbox)})
    return result


def _node(page,item,node_type,index,**extra):
    x,y=item["center"]
    row={"id":f"node-{index}","label":item["text"],"node_type":node_type,"x":round(x,3),"y":round(y,3),
         "bbox_page":_bbox_norm(page,item["bbox"]),"container_ids":[],"field_state":"present"}
    row.update(extra);return row


def _unique(nodes):
    result=[];seen=set()
    for node in nodes:
        key=(node["node_type"],node["label"].lower(),round(node["x"],1),round(node["y"],1))
        if key in seen:continue
        seen.add(key);node["id"]=f"node-{len(result)+1}";result.append(node)
    return result


def _token_nodes(page,patterns,*,use_lines=False):
    result=[]
    for item in (_lines(page) if use_lines else _words(page)):
        kind=next((kind for kind,pattern in patterns if pattern.search(item["text"])),None)
        if kind:result.append(_node(page,item,kind,len(result)+1))
    return _unique(result)


def _segments(page,*,vivid_only=False,min_length=2.0):
    result=[]
    for drawing in page.get_drawings():
        if drawing.get("fill") is not None:continue
        color=tuple(round(float(value),3) for value in (drawing.get("color") or (0,0,0)))
        chroma=max(color)-min(color);neutral=chroma<.08 and sum(color)/3>.12
        if vivid_only and chroma<.16:continue
        if not vivid_only and neutral:continue
        for item in drawing.get("items") or []:
            if item[0]!="l":continue
            p1=_point(page,item[1].x,item[1].y);p2=_point(page,item[2].x,item[2].y);length=math.dist(p1,p2)
            if length<min_length:continue
            result.append({"id":f"segment-{len(result)+1}","p1":p1,"p2":p2,
                           "length":round(length,3),"color":color})
    return result


def _components(segments,tolerance=.8):
    parent=list(range(len(segments)));points=collections.defaultdict(list)
    def find(index):
        while parent[index]!=index:parent[index]=parent[parent[index]];index=parent[index]
        return index
    def union(left,right):
        left,right=find(left),find(right)
        if left!=right:parent[right]=left
    for index,segment in enumerate(segments):
        for point in (segment["p1"],segment["p2"]):
            points[(round(point[0]/tolerance),round(point[1]/tolerance))].append(index)
    for indexes in points.values():
        for index in indexes[1:]:union(indexes[0],index)
    groups=collections.defaultdict(list)
    for index in range(len(segments)):groups[find(index)].append(index)
    result=[]
    for indexes in groups.values():
        degree=collections.Counter()
        for index in indexes:
            for point in (segments[index]["p1"],segments[index]["p2"]):
                degree[(round(point[0]/tolerance),round(point[1]/tolerance))]+=1
        result.append({"id":f"route-{len(result)+1}","segment_indexes":indexes,
          "segment_ids":[segments[index]["id"] for index in indexes],
          "length":round(sum(segments[index]["length"] for index in indexes),3),
          "branch_points":sum(value>=3 for value in degree.values()),"endpoint_count":sum(value==1 for value in degree.values())})
    return result


def _attach(nodes,segments,components,*,limit=25):
    by_segment={sid:component["id"] for component in components for sid in component["segment_ids"]}
    attached=0
    for node in nodes:
        if not segments:continue
        distance,segment=min(((point_segment_distance((node["x"],node["y"]),candidate),candidate)
                              for candidate in segments),key=lambda item:item[0])
        if distance<=limit:
            node["route_id"]=by_segment[segment["id"]];node["route_distance"]=round(distance,3);attached+=1
    return attached


def _preferred_parent(child,candidates):
    """Сначала общий CAD-компонент, затем явно помеченное пространственное приближение."""
    same=[node for node in candidates if child.get("route_id") and node.get("route_id")==child.get("route_id")]
    pool=same or candidates
    parent=min(pool,key=lambda node:math.hypot(node["x"]-child["x"],node["y"]-child["y"]))
    state="same_cad_component" if same else "nearest_geometry"
    return parent,state


def _axes(page):
    pattern=re.compile(r"^(?:[A-ZА-Я]{1,3}|\d+(?:\.\d+)?)$",re.I);groups=collections.defaultdict(list)
    for word in _words(page):
        if pattern.fullmatch(word["text"]):groups[word["text"].upper()].append(word)
    result=[]
    for label,items in groups.items():
        if len(items)<2:continue
        xs=[item["center"][0] for item in items];ys=[item["center"][1] for item in items]
        if max(ys)-min(ys)>page.rect.height*.3 and max(xs)-min(xs)<page.rect.width*.1:
            orientation="vertical";position=sum(xs)/len(xs)
        elif max(xs)-min(xs)>page.rect.width*.3 and max(ys)-min(ys)<page.rect.height*.1:
            orientation="horizontal";position=sum(ys)/len(ys)
        else:continue
        result.append({"id":f"axis-{len(result)+1}","label":label,"orientation":orientation,
                       "position":round(position,3)})
    return result


def _bind_nearest_axes(nodes,axes):
    vertical=[axis for axis in axes if axis["orientation"]=="vertical"]
    horizontal=[axis for axis in axes if axis["orientation"]=="horizontal"]
    for node in nodes:
        if vertical:node["nearest_vertical_axis"]=min(vertical,key=lambda axis:abs(node["x"]-axis["position"]))["label"]
        if horizontal:node["nearest_horizontal_axis"]=min(horizontal,key=lambda axis:abs(node["y"]-axis["position"]))["label"]


def _base(page,pdf,block_id,profile,subtype,*,nodes,containers=None,networks=None,edges=None,validation=None,warnings=None):
    validation=dict(validation or {});containers=containers or [];networks=networks or [];edges=edges or []
    # Предметные узлы намеренно строятся только по распознанным инженерным
    # правилам. Но эталон не должен терять остальные надписи исходного PDF:
    # размеры, примечания и редкие марки могут ещё не входить в словарь
    # конкретной дисциплины. Поэтому рядом с графом храним полный
    # координатный реестр строк. Он не создаёт неподтверждённых рёбер, зато
    # делает описание обратимо проверяемым по текстовому слою.
    semantic_ledger=[]
    for line in _lines(page):
        text=re.sub(r"\s+"," ",str(line.get("text") or "")).strip()
        if not text:continue
        semantic_ledger.append({
          "id":f"text-{len(semantic_ledger)+1}","text":text,
          "bbox_page":_bbox_norm(page,line["bbox"]),
          "x":round(line["center"][0],3),"y":round(line["center"][1],3),
          "evidence_state":"текстовый слой PDF с координатами"})
    validation.update({"subtype":subtype,"nodes_total":len(nodes),"containers_total":len(containers),
      "networks_total":len(networks),"edges_total":len(edges),
      "node_types":dict(collections.Counter(node["node_type"] for node in nodes)),
      "coordinate_text_records_total":len(semantic_ledger),
      "coordinate_text_characters_total":sum(len(item["text"]) for item in semantic_ledger),
      "coordinate_text_coverage":"полный реестр строк PDF"})
    return {"schema_version":1,"profile_id":profile,"source":{"pdf_file":Path(pdf).name,"page_index":0,
      "block_id":block_id},"containers":containers,"nodes":nodes,"networks":networks,"edges":edges,
      "semantic_ledger":semantic_ledger,"validation":validation,"warnings":warnings or [],"status":"ok"}


def _build_plan(page,pdf,block_id,subtype):
    patterns=(("system",re.compile(r"^(?:Т[12]\d(?:\.\d+)*|(?i:(?:Вж|Вкр|Ва|В|ПД|Пкр|П)[A-ZА-Яа-я]*\d+(?:\.\d+)*)|ДУ[A-ZА-Яа-я]*\d+(?:\.\d+)*)$")),
      ("riser",re.compile(r"^Ст\.\s*\d+(?:\.\d+)*$",re.I)),
      ("equipment",re.compile(r"^(?:КПУ|РКДМ|КДМ|КВК|КРК|SPL|VO-|KVR|AMP|CAV)[A-ZА-Яа-я0-9._/-]*",re.I)),
      ("duct_or_pipe_size",re.compile(r"^(?:[\u00d8∅ф]\d{2,4}|\d{2,4}[xх×]\d+(?:[.,]\d+)?)$",re.I)))
    nodes=_token_nodes(page,patterns)
    room_pattern=re.compile(r"(?:венткамер|техническ(?:ое|ий) помещ|коридор|автостоянк|лестничн|холл|тамбур|кладов|сануз|насосн|электрощитов|ИТП)",re.I)
    for line in _lines(page):
        if len(line["text"])<=80 and room_pattern.search(line["text"]):
            nodes.append(_node(page,line,"room_label",len(nodes)+1))
    nodes=_unique(nodes);axes=_axes(page);_bind_nearest_axes(nodes,axes)
    rooms=[node for node in nodes if node["node_type"]=="room_label"]
    for node in nodes:
        if node["node_type"]=="room_label" or not rooms:continue
        room=min(rooms,key=lambda item:math.hypot(node["x"]-item["x"],node["y"]-item["y"]))
        distance=math.hypot(node["x"]-room["x"],node["y"]-room["y"])
        if distance<=max(page.rect.width,page.rect.height)*.25:
            node["nearest_room_label"]=room["label"];node["room_binding_state"]="spatial_nearest"
    segments=_segments(page,vivid_only=True,min_length=3);components=_components(segments,tolerance=2.0)
    technical_nodes=[node for node in nodes if node["node_type"]!="room_label"]
    attached=_attach(technical_nodes,segments,components,limit=28)
    networks=[]
    for component in components:
      members=[node for node in nodes if node.get("route_id")==component["id"]]
      semantic=[node["label"] for node in members if node["node_type"] in ("system","riser","equipment")]
      networks.append({"id":component["id"],"network_type":"hvac_route",
        "label":" / ".join(dict.fromkeys(semantic)) or subtype,
        "endpoint_ids":[node["id"] for node in members],
        "path_state":"cad_endpoint_component",**component})
    graph=_base(page,pdf,block_id,PROFILE_PLAN,subtype,nodes=nodes,networks=networks,
      validation={"axes_total":len(axes),"systems_total":sum(n["node_type"]=="system" for n in nodes),
       "unique_systems_total":len({n["label"].strip().upper() for n in nodes if n["node_type"]=="system"}),
       "equipment_total":sum(n["node_type"]=="equipment" for n in nodes),"nodes_route_attached":attached,
       "room_labels_total":sum(n["node_type"]=="room_label" for n in nodes),
       "node_route_bind_rate":round(attached/max(len(technical_nodes),1),3),"route_segments_total":len(segments),
       "route_components_total":len(components),"route_branches_total":sum(c["branch_points"] for c in components),
       "topology_state":"colored_route_graph"},warnings=["X-пересечение без CAD-конца не считается связью",
       "полигоны помещений не восстанавливаются без отдельного контурного профиля"])
    graph["grid"]={"axes":axes};graph["route_segments"]=segments;return graph


_HEAT_PATTERNS=(("source",re.compile(r"^(?:ИТП|.*от ИТП.*)$",re.I)),
 ("pipe_system",re.compile(r"^Т[12]\d(?:\.\d+)*$",re.I)),("riser",re.compile(r"^Ст\.\s*\d+(?:\.\d+)*$",re.I)),
 ("heating_device",re.compile(r"^(?:РГ-|SPL|Kermi|КРК|ВТЗ)[A-ZА-Яа-я0-9._/-]*",re.I)),
 ("valve",re.compile(r"^(?:BVR|MNF|RJIP|AB-QM|MSV|ASV|Danfoss)[A-ZА-Яа-я0-9._/-]*",re.I)),
 ("level",re.compile(r"^[+\-]\d+[.,]\d{3}$")),("floor",re.compile(r"^L\d+_K\d+$",re.I)),
 ("pipe_size",re.compile(r"^(?:[Ø∅ф]\d+|\d+[xх×]\d+(?:[.,]\d+)?)$",re.I)))


def _build_heating(page,pdf,block_id,subtype):
    nodes=_token_nodes(page,_HEAT_PATTERNS);segments=_segments(page,vivid_only=False,min_length=2);components=_components(segments,tolerance=4.0)
    attached=_attach(nodes,segments,components,limit=22)
    sources=[n for n in nodes if n["node_type"]=="source"]
    if not sources:
        line={"text":"внешний источник теплоносителя","bbox":(0,page.rect.height,0,page.rect.height),"center":(0,page.rect.height)}
        source=_node(page,line,"source",len(nodes)+1,field_state="inferred_from_system_scheme");nodes.append(source);sources=[source]
    systems=[n for n in nodes if n["node_type"]=="pipe_system"];risers=[n for n in nodes if n["node_type"]=="riser"]
    devices=[n for n in nodes if n["node_type"] in ("heating_device","valve")];levels=[n for n in nodes if n["node_type"] in ("level","floor")]
    edges=[]
    def link(parent,child,edge_type,state):
        edges.append({"id":f"edge-{len(edges)+1}","from":parent["id"],"to":child["id"],"edge_type":edge_type,"edge_state":state})
    for system in systems:link(min(sources,key=lambda n:math.hypot(n["x"]-system["x"],n["y"]-system["y"])),system,"source_to_system","semantic_system_source")
    parents=systems or sources
    for riser in risers:
        parent,state=_preferred_parent(riser,parents);link(parent,riser,"system_to_riser",state)
    parents=risers or systems or sources
    for child in devices+levels:
        parent,state=_preferred_parent(child,parents);link(parent,child,
          "riser_to_device" if child in devices else "riser_to_level",state)
    route_networks=[{"id":component["id"],"network_type":"hydronic_path","label":subtype,
      "endpoint_ids":[n["id"] for n in nodes if n.get("route_id")==component["id"]],
      "path_state":"cad_endpoint_component",**component} for component in components]
    graph=_base(page,pdf,block_id,PROFILE_HEATING,subtype,nodes=nodes,networks=route_networks,edges=edges,
      validation={"sources_total":len(sources),"systems_total":len(systems),
       "unique_systems_total":len({n["label"].strip().upper() for n in systems}),"risers_total":len(risers),
       "devices_total":len(devices),"levels_total":len(levels),"route_segments_total":len(segments),
       "route_components_total":len(components),"nodes_route_attached":attached,
       "structural_edges_total":len(edges),
       "cad_confirmed_edges_total":sum(edge["edge_state"]=="same_cad_component" for edge in edges),
       "semantic_edges_total":sum(edge["edge_state"]=="semantic_system_source" for edge in edges),
       "spatial_edges_total":sum(edge["edge_state"]=="nearest_geometry" for edge in edges),
       "topology_state":"source_system_riser_device"},
      warnings=["точный гидравлический порядок внутри многоточечной CAD-компоненты не домысливается"])
    graph["route_segments"]=segments;return graph


_VENT_SYSTEM_RE=re.compile(r"^(?:(?i:В(?:ж|а|к|кр|м|н|о|ос|у|ус|пм|су|х|Елк|Елш)?|П(?:Д|а|ж|кр|м|пм|л|Есс)?)[A-ZА-Яа-я]*\d+(?:\.\d+)*|ДУ(?:ас)?[A-ZА-Яа-я]*\d+(?:\.\d+)*)$")
_VENT_PATTERNS=(("vent_system",_VENT_SYSTEM_RE),
 ("damper",re.compile(r"^(?:КПУ|РКДМ|КДМ|КВК|РОН|CAV|AMP)[A-ZА-Яа-я0-9._/-]*",re.I)),
 ("terminal",re.compile(r"^(?:Зонт|Решетка|Дефлектор|Вентилятор|VO-|VR-)[A-ZА-Яа-я0-9._/-]*",re.I)),
 ("duct_size",re.compile(r"^(?:[Ø∅ф]\d{2,4}|\d{2,4}[xх×]\d{2,4})$",re.I)),
 ("level",re.compile(r"^[+\-]\d+[.,]\d{3}$")),("fire_rating",re.compile(r"^EI\s*\d+",re.I)))


def _build_vent(page,pdf,block_id,subtype):
    nodes=_token_nodes(page,_VENT_PATTERNS);segments=_segments(page,vivid_only=False,min_length=2);components=_components(segments,tolerance=4.0)
    attached=_attach(nodes,segments,components,limit=24);systems=[n for n in nodes if n["node_type"]=="vent_system"]
    apparatus=[n for n in nodes if n["node_type"] in ("damper","terminal","fire_rating")]
    edges=[];networks=[];assignments=collections.defaultdict(list)
    for member in apparatus:
        same=[system for system in systems if member.get("route_id") and system.get("route_id")==member.get("route_id")]
        candidates=same or systems
        if not candidates:continue
        system=min(candidates,key=lambda candidate:math.hypot(member["x"]-candidate["x"],member["y"]-candidate["y"]))
        distance=math.hypot(member["x"]-system["x"],member["y"]-system["y"])
        if same or distance<=max(page.rect.width,page.rect.height)*.45:
            assignments[system["id"]].append((member,"same_cad_component" if same else "nearest_system_geometry"))
    system_groups=collections.defaultdict(list)
    for system in systems:system_groups[system["label"].strip().upper()].append(system)
    for grouped_systems in system_groups.values():
        system=grouped_systems[0];assigned=[]
        for occurrence in grouped_systems:assigned.extend(assignments[occurrence["id"]])
        # Один и тот же аппарат не дублируется из-за повторной подписи марки на схеме.
        unique_assigned={member["id"]:(member,state) for member,state in assigned}
        assigned=list(unique_assigned.values());members=[member for member,_ in assigned]
        network_id=f"network-{len(networks)+1}";networks.append({"id":network_id,"network_type":"air_system",
          "label":system["label"],"endpoint_ids":[item["id"] for item in grouped_systems]+[n["id"] for n in members],
          "path_state":"same_cad_component" if assigned and all(state=="same_cad_component" for _,state in assigned)
                       else "nearest_system_geometry"})
        for member,state in assigned:
            source=min(grouped_systems,key=lambda occurrence:math.hypot(member["x"]-occurrence["x"],member["y"]-occurrence["y"]))
            edges.append({"id":f"edge-{len(edges)+1}","network_id":network_id,"from":source["id"],
              "to":member["id"],"edge_type":"system_to_air_device","edge_state":state})
    graph=_base(page,pdf,block_id,PROFILE_VENT,subtype,nodes=nodes,networks=networks,edges=edges,
      validation={"systems_total":len(systems),"unique_systems_total":len(system_groups),
       "dampers_total":sum(n["node_type"]=="damper" for n in nodes),
       "terminals_total":sum(n["node_type"]=="terminal" for n in nodes),"levels_total":sum(n["node_type"]=="level" for n in nodes),
       "duct_sizes_total":sum(n["node_type"]=="duct_size" for n in nodes),"route_segments_total":len(segments),
       "route_components_total":len(components),"nodes_route_attached":attached,"structural_edges_total":len(edges),
       "cad_confirmed_edges_total":sum(edge["edge_state"]=="same_cad_component" for edge in edges),
       "spatial_edges_total":sum(edge["edge_state"]=="nearest_system_geometry" for edge in edges),
       "topology_state":"system_air_device_hierarchy"},
      warnings=["X-пересечение воздуховодов без CAD-конца не считается тройником"])
    graph["route_segments"]=segments;graph["route_components"]=components;return graph


_HYDRONIC_PATTERNS=(("pump",re.compile(r"насос|RMV|UPS",re.I)),("heat_exchanger",re.compile(r"теплообмен",re.I)),
 ("collector",re.compile(r"коллектор",re.I)),("valve",re.compile(r"клапан|кран|BVR|MNF|RJIP|AB-QM",re.I)),
 ("filter",re.compile(r"фильтр",re.I)),("instrument",re.compile(r"термоманометр|манометр|термометр",re.I)),
 ("pipe_size",re.compile(r"(?:[Ø∅ф]\s*\d+|Ду\s*\d+)",re.I)),
 ("apparatus_callout",re.compile(r"^\d{1,2}$")))


def _build_hydronic(page,pdf,block_id,subtype):
    nodes=_token_nodes(page,_HYDRONIC_PATTERNS,use_lines=True);segments=_segments(page,vivid_only=False,min_length=1.5);components=_components(segments,tolerance=4.0)
    attached=_attach(nodes,segments,components,limit=20);members=collections.defaultdict(list)
    for node in nodes:
        if node.get("route_id"):members[node["route_id"]].append(node)
    networks=[];edges=[]
    for route_id,items in members.items():
        if len(items)<2:continue
        network_id=f"network-{len(networks)+1}";state="confirmed_pair" if len(items)==2 else "multi_apparatus_hydronic_path"
        networks.append({"id":network_id,"network_type":"hydronic_circuit",
          "label":"Подтверждённая гидравлическая связь" if len(items)==2 else "Общая гидравлическая цепь",
          "source_route_id":route_id,"endpoint_ids":[n["id"] for n in items],"path_state":state})
        if len(items)==2:edges.append({"id":f"edge-{len(edges)+1}","network_id":network_id,"from":items[0]["id"],
          "to":items[1]["id"],"edge_type":"hydronic_path","edge_state":"path_confirmed"})
    apparatus=[node for node in nodes if node["node_type"]!="pipe_size"]
    if not networks and len(apparatus)>=2:
        networks.append({"id":"network-1","network_type":"hydronic_assembly","label":subtype,
          "endpoint_ids":[node["id"] for node in apparatus],"path_state":"spatial_inventory"})
    graph=_base(page,pdf,block_id,PROFILE_HYDRONIC,subtype,nodes=nodes,networks=networks,edges=edges,
      validation={"apparatus_total":sum(n["node_type"]!="pipe_size" for n in nodes),
       "unique_apparatus_total":len({(n["node_type"],n["label"].strip().upper()) for n in nodes if n["node_type"]!="pipe_size"}),
       "pipe_sizes_total":sum(n["node_type"]=="pipe_size" for n in nodes),
       "route_segments_total":len(segments),"route_components_total":len(components),"nodes_route_attached":attached,
       "hydronic_networks_total":len(networks),"confirmed_pairs_total":len(edges),
       "multi_apparatus_networks":sum(n["path_state"]=="multi_apparatus_hydronic_path" for n in networks),
       "spatial_inventory_networks":sum(n["path_state"]=="spatial_inventory" for n in networks),
       "topology_state":"apparatus_path_components"},warnings=["многоаппаратная гидравлическая цепь не разбивается на выдуманные пары"])
    graph["route_segments"]=segments;return graph


def _physical_nodes(page):
    skip=re.compile(r"^(?:Изм\.|(?:Лист|Page)\s*\d*|Дата|Разраб|Проверил|Масштаб)",re.I);nodes=[]
    dimension=re.compile(r"^(?:[RØ∅ф]?\s*\d+(?:[.,]\d+)?(?:\s*[xх×-]\s*\d+(?:[.,]\d+)?)*(?:\s*мм)?)$",re.I)
    for line in _lines(page):
        if len(line["text"])<2 or skip.search(line["text"]):continue
        text=line["text"]
        if re.match(r"^(?:Разрез|View|Вид|Узел)\b",text,re.I):continue
        if re.fullmatch(r"[+\-]\d+[.,]\d{3}",text):kind="elevation"
        elif dimension.fullmatch(text):kind="dimension"
        elif re.search(r"труб|воздуховод|патруб",text,re.I):kind="pipeline_part"
        elif re.search(r"изоляц|минераловат|МБОР|гермет|мастик|огнезащ",text,re.I):kind="insulation_or_fireproofing"
        elif re.search(r"болт|гайк|шайб|анкер|хомут|саморез|шпильк",text,re.I):kind="fastener"
        elif re.search(r"опор|рама|кронштейн|профиль|уголок|траверс",text,re.I):kind="support_part"
        else:kind="assembly_part"
        nodes.append(_node(page,line,kind,len(nodes)+1))
    return _unique(nodes)


def _views(page):
    result=[];pattern=re.compile(r"^(?:(?:Разрез|View|Вид|Узел)\s*[A-ZА-Я0-9-]*.*|\d{1,2}-\d{1,2})$",re.I)
    for line in _lines(page):
        if pattern.search(line["text"]):result.append({"id":f"view-{len(result)+1}","container_type":"drawing_view",
          "label":line["text"],"bbox_page":_bbox_norm(page,line["bbox"]),
          "anchor_x":round(line["center"][0],3),"anchor_y":round(line["center"][1],3),"member_ids":[]})
    return result


def _assign_nodes_to_views(nodes,views):
    if not views:return 0
    assigned=0
    for node in nodes:
        view=min(views,key=lambda item:math.hypot(node["x"]-item["anchor_x"],node["y"]-item["anchor_y"]))
        node["container_ids"]=[view["id"]];view["member_ids"].append(node["id"]);assigned+=1
    return assigned


def _build_detail(page,pdf,block_id,subtype):
    nodes=_physical_nodes(page);views=_views(page);segments=_segments(page,vivid_only=False,min_length=2)
    if not views:
        views=[{"id":"view-1","container_type":"installation_detail","label":subtype,"member_ids":[n["id"] for n in nodes]}]
        for node in nodes:node["container_ids"]=["view-1"]
    assigned=_assign_nodes_to_views(nodes,views) if views[0].get("anchor_x") is not None else len(nodes)
    graph=_base(page,pdf,block_id,PROFILE_DETAIL,subtype,nodes=nodes,containers=views,
      validation={"views_total":len(views),"parts_total":sum(n["node_type"] not in ("dimension","elevation") for n in nodes),
       "pipeline_parts_total":sum(n["node_type"]=="pipeline_part" for n in nodes),
       "supports_total":sum(n["node_type"]=="support_part" for n in nodes),
       "fasteners_total":sum(n["node_type"]=="fastener" for n in nodes),
       "insulation_total":sum(n["node_type"]=="insulation_or_fireproofing" for n in nodes),
       "dimensions_total":sum(n["node_type"]=="dimension" for n in nodes),
       "elevations_total":sum(n["node_type"]=="elevation" for n in nodes),"line_segments_total":len(segments),
       "view_members_assigned":assigned,
       "topology_state":"physical_assembly_views"})
    return graph


def _build_section(page,pdf,block_id,subtype):
    patterns=(("elevation",re.compile(r"^[+\-]\d+[.,]\d{3}$")),
      ("duct_or_pipe_size",re.compile(r"^[Ø∅ф]?\d{2,4}(?:[xх×]\d{2,4})?$",re.I)),
      ("equipment",re.compile(r"(?:вентилятор|клапан|воздухонагреватель|рама|Big Foot|КПУ|РКДМ)",re.I)))
    nodes=_token_nodes(page,patterns,use_lines=True);views=_views(page);segments=_segments(page,vivid_only=False,min_length=2)
    assigned=_assign_nodes_to_views(nodes,views)
    graph=_base(page,pdf,block_id,PROFILE_SECTION,subtype,nodes=nodes,containers=views,
      validation={"views_total":len(views),"elevations_total":sum(n["node_type"]=="elevation" for n in nodes),
       "equipment_total":sum(n["node_type"]=="equipment" for n in nodes),"sizes_total":sum(n["node_type"]=="duct_or_pipe_size" for n in nodes),
       "line_segments_total":len(segments),"view_members_assigned":assigned,"topology_state":"section_equipment_layout"})
    return graph


def _geometry_parts(page):
    points=[];page_area=page.rect.width*page.rect.height
    for drawing in page.get_drawings():
        rect=drawing.get("rect")
        if not rect:continue
        bbox=_bbox(page,(rect.x0,rect.y0,rect.x1,rect.y1));area=max(0,bbox[2]-bbox[0])*max(0,bbox[3]-bbox[1])
        if not 20<=area<=page_area*.18:continue
        center=_center(bbox)
        if any(math.dist(center,old["center"])<8 for old in points):continue
        points.append({"center":center,"bbox":bbox})
    return points


def _equipment_groups(page,nodes,subtype):
    geometry=[node for node in nodes if node["node_type"]=="geometry_part"]
    if not geometry:
        return [{"id":"equipment-1","container_type":"equipment","label":subtype,
                 "member_ids":[node["id"] for node in nodes]}]
    ordered=sorted(geometry,key=lambda node:node["x"]);groups=[];current=[ordered[0]]
    for node in ordered[1:]:
        if node["x"]-current[-1]["x"]>page.rect.width*.08:
            groups.append(current);current=[node]
        else:current.append(node)
    groups.append(current)
    containers=[]
    for index,group in enumerate(groups,1):
        center=sum(node["x"] for node in group)/len(group)
        containers.append({"id":f"equipment-group-{index}","container_type":"physical_equipment_group",
          "label":f"Физическая группа {index}","anchor_x":round(center,3),"member_ids":[]})
    for node in nodes:
        container=min(containers,key=lambda item:abs(node["x"]-item["anchor_x"]))
        container["member_ids"].append(node["id"]);node["container_ids"]=[container["id"]]
    return containers


def _build_equipment(page,pdf,block_id,subtype):
    patterns=(("model",re.compile(r"(?:НН№?\d+|NSK\s*\d+|PatAIR|KVR|RMV|PVO|VO-)[A-ZА-Яа-я0-9._/-]*",re.I)),
      ("port",re.compile(r"^(?:F[1-4]|Вход|Выход|DN\d+)$",re.I)),
      ("module",re.compile(r"(?:фильтр|вентилятор|нагреватель|охладитель|рекуператор|секция)",re.I)),
      ("dimension",re.compile(r"^(?:[RØ∅ф]?\d+(?:[.,]\d+)?(?:[xх×]\d+(?:[.,]\d+)?)*)$",re.I)))
    nodes=_token_nodes(page,patterns,use_lines=True);parts=_geometry_parts(page)
    for part in parts:
        item={"text":f"геометрическая часть {len(nodes)+1}","bbox":part["bbox"],"center":part["center"]}
        nodes.append(_node(page,item,"geometry_part",len(nodes)+1,field_state="geometry_only"))
    nodes=_unique(nodes);views=_views(page)
    containers=views or _equipment_groups(page,nodes,subtype)
    if views:_assign_nodes_to_views(nodes,views)
    graph=_base(page,pdf,block_id,PROFILE_EQUIPMENT,subtype,nodes=nodes,containers=containers,
      validation={"models_total":sum(n["node_type"]=="model" for n in nodes),"ports_total":sum(n["node_type"]=="port" for n in nodes),
       "modules_total":sum(n["node_type"]=="module" for n in nodes),"dimensions_total":sum(n["node_type"]=="dimension" for n in nodes),
       "geometry_parts_total":sum(n["node_type"]=="geometry_part" for n in nodes),"views_total":len(views),
       "physical_groups_total":len(containers),
       "topology_state":"physical_equipment_composition"})
    return graph


def _build_chart(page,pdf,block_id,subtype):
    patterns=(("model",re.compile(r"(?:RMV|PVO|VO-|PatAIR|VR-)[A-ZА-Яа-я0-9.,/_-]*",re.I)),
      ("axis",re.compile(r"^(?:Q|P|Pv|Ps|H|NPSH|η|КПД)(?:,.*)?$",re.I)),
      ("value",re.compile(r"^\d+(?:[.,]\d+)?$")))
    nodes=_token_nodes(page,patterns);curve_paths=0;curve_items=0;curve_networks=[]
    for drawing in page.get_drawings():
        kinds=[item[0] for item in drawing.get("items") or []]
        if "c" in kinds or len(kinds)>=3:
            curve_paths+=1;curve_items+=len(kinds)
            curve_networks.append({"id":f"curve-{curve_paths}","network_type":"performance_curve",
              "label":f"Кривая {curve_paths}","endpoint_ids":[],"path_state":"vector_curve_geometry",
              "geometry_items":len(kinds)})
    raster_regions=[]
    for info in page.get_image_info(xrefs=True):
        bbox=_bbox(page,info["bbox"]);width=bbox[2]-bbox[0];height=bbox[3]-bbox[1]
        if width<page.rect.width*.2 or height<page.rect.height*.15:continue
        item={"text":f"крупная растровая область листа {len(raster_regions)+1}","bbox":bbox,"center":_center(bbox)}
        raster_regions.append(_node(page,item,"raster_sheet_region",len(nodes)+len(raster_regions)+1,
                                    field_state="raster_geometry_only"))
    nodes=_unique(nodes+raster_regions)
    for node in nodes:
        if node["node_type"]=="raster_sheet_region":
            curve_networks.append({"id":f"raster-curve-{len(curve_networks)+1}",
              "network_type":"raster_sheet_region","label":node["label"],"endpoint_ids":[node["id"]],
              "path_state":"embedded_raster"})
    graph=_base(page,pdf,block_id,PROFILE_CHART,subtype,nodes=nodes,networks=curve_networks,
      validation={"models_total":sum(n["node_type"]=="model" for n in nodes),"axes_total":sum(n["node_type"]=="axis" for n in nodes),
       "numeric_values_total":sum(n["node_type"]=="value" for n in nodes),"curve_paths_total":curve_paths,
       "curve_items_total":curve_items,"raster_regions_total":len(raster_regions),
       "curve_representation_state":"vector" if curve_paths else "embedded_raster",
       "topology_state":"performance_curve_inventory"},
      warnings=["точки растровой кривой не векторизуются и не превращаются в выдуманные значения"] if raster_regions else [])
    return graph


def _build_site(page,pdf,block_id,subtype):
    patterns=(("building",re.compile(r"^[КK]\s*\d+$",re.I)),
      ("parking_zone",re.compile(r"^П\s*\.\s*\d+(?:\s*-\s*П\s*\.\s*\d+)?$",re.I)),
      ("heat_source",re.compile(r"ИТП|ввод теплоносит",re.I)))
    nodes=_token_nodes(page,patterns,use_lines=True);buildings=[n for n in nodes if n["node_type"]=="building"]
    sources=[n for n in nodes if n["node_type"]=="heat_source"];edges=[]
    if sources:
        for building in buildings:edges.append({"id":f"edge-{len(edges)+1}","from":sources[0]["id"],"to":building["id"],
          "edge_type":"site_heat_distribution","edge_state":"overview_semantic"})
    networks=[]
    if sources:
        networks=[{"id":"site-network-1","network_type":"site_heat_network","label":"Распределение теплоносителя по корпусам",
          "endpoint_ids":[sources[0]["id"]]+[building["id"] for building in buildings],
          "path_state":"overview_semantic"}]
    return _base(page,pdf,block_id,PROFILE_SITE,subtype,nodes=nodes,networks=networks,edges=edges,
      validation={"buildings_total":len(buildings),"parking_zones_total":sum(n["node_type"]=="parking_zone" for n in nodes),
       "sources_total":len(sources),"topology_state":"site_source_to_buildings"})


def _dispatch(page,pdf,block_id,profile_hint=None,subtype_hint=None):
    classified_profile,classified_subtype=classify_hvac_profile(page.get_text(),block_id=block_id)
    profile=profile_hint or classified_profile;subtype=subtype_hint or classified_subtype or "блок ОВ"
    if profile==PROFILE_PLAN:return _build_plan(page,pdf,block_id,subtype)
    if profile==PROFILE_HEATING:return _build_heating(page,pdf,block_id,subtype)
    if profile==PROFILE_VENT:return _build_vent(page,pdf,block_id,subtype)
    if profile==PROFILE_HYDRONIC:return _build_hydronic(page,pdf,block_id,subtype)
    if profile==PROFILE_DETAIL:return _build_detail(page,pdf,block_id,subtype)
    if profile==PROFILE_SECTION:return _build_section(page,pdf,block_id,subtype)
    if profile==PROFILE_EQUIPMENT:return _build_equipment(page,pdf,block_id,subtype)
    if profile==PROFILE_CHART:return _build_chart(page,pdf,block_id,subtype)
    if profile==PROFILE_SITE:return _build_site(page,pdf,block_id,subtype)
    return None


def build_hvac_graph(pdf_path:Path,*,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        with fitz.open(str(pdf_path)) as doc:
            if doc.page_count!=1:return None
            graph=_dispatch(doc[0],Path(pdf_path),block_id,profile_hint,subtype_hint)
        fitz.TOOLS.store_shrink(100)
        return graph
    except Exception:
        return None


def _clip_copied_page(page, pdf_points):
    commands=[f"{pdf_points[0][0]:.5f} {pdf_points[0][1]:.5f} m"]
    commands.extend(f"{x:.5f} {y:.5f} l" for x,y in pdf_points[1:])
    prefix=("q\n"+"\n".join(commands)+"\nh W n\n").encode("ascii")
    doc=page.parent;contents=page.get_contents()
    original=b"\n".join(doc.xref_stream(xref) for xref in contents)
    first=contents[0];doc.update_stream(first,prefix+original+b"\nQ\n")
    doc.xref_set_key(page.xref,"Contents",f"{first} 0 R")


def build_hvac_graph_from_source(pdf_path:Path,*,page_index:int,bbox_norm,polygon_norm=None,block_id=None,profile_hint=None,subtype_hint=None):
    """Построить ОВ-профиль из полигона исходного многолистового PDF."""
    try:
        import fitz
        source=fitz.open(str(pdf_path));cropped=None
        try:
            if not bbox_norm or not (0<=page_index<source.page_count):return None
            source_page=source[page_index];w,h=source_page.rect.width,source_page.rect.height
            crop=fitz.Rect(float(bbox_norm[0])*w,float(bbox_norm[1])*h,
                           float(bbox_norm[2])*w,float(bbox_norm[3])*h)&source_page.rect
            if crop.is_empty:return None
            unrotated=crop*source_page.derotation_matrix;unrotated.normalize();offset=source_page.cropbox_position
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
            return _dispatch(target,Path(pdf_path),block_id,profile_hint,subtype_hint)
        finally:
            if cropped is not None:cropped.close()
            source.close();fitz.TOOLS.store_shrink(100)
    except Exception:
        return None


def evaluate_hvac_gate(graph):
    if not graph:return {"use":False,"complete":False,"readiness":"none","mode":"none","reasons":["граф не построен"],"complete_reasons":[],"metrics":{}}
    v=graph["validation"];profile=graph["profile_id"];reasons=[];complete=[]
    if v.get("nodes_total",0)<1 and v.get("networks_total",0)<1 and v.get("containers_total",0)<1:
        reasons.append("нет ни семантических узлов, ни геометрических сетей/контейнеров")
    if profile==PROFILE_PLAN:
        if v.get("route_segments_total",0)<20:complete.append("не извлечён слой трасс")
        if v.get("route_components_total",0)<1:complete.append("нет графа трасс")
    elif profile==PROFILE_HEATING:
        if v.get("systems_total",0)+v.get("risers_total",0)<2:complete.append("не извлечены системы/стояки")
        if v.get("structural_edges_total",0)<2:complete.append("не построена иерархия от источника")
    elif profile==PROFILE_VENT:
        if v.get("systems_total",0)<2:complete.append("не извлечены вентсистемы")
        if v.get("route_segments_total",0)<30:complete.append("не извлечена геометрия воздуховодов")
    elif profile==PROFILE_HYDRONIC:
        if v.get("apparatus_total",0)<3:complete.append("менее трёх аппаратов")
        if v.get("route_segments_total",0)<15:complete.append("нет геометрии гидравлической схемы")
    elif profile==PROFILE_DETAIL:
        if v.get("parts_total",0)<3:complete.append("неполный состав монтажного узла")
        if v.get("line_segments_total",0)<10:complete.append("нет геометрии узла")
    elif profile==PROFILE_SECTION:
        if v.get("views_total",0)<1:complete.append("не найдены разрезы")
        if v.get("equipment_total",0)+v.get("sizes_total",0)+v.get("elevations_total",0)<3:complete.append("неполная структура разреза")
    elif profile==PROFILE_EQUIPMENT:
        if v.get("models_total",0)+v.get("modules_total",0)+v.get("geometry_parts_total",0)<3:complete.append("не разложен состав оборудования")
    elif profile==PROFILE_CHART:
        if v.get("curve_paths_total",0)+v.get("raster_regions_total",0)<1:complete.append("не найдены кривые")
        if v.get("numeric_values_total",0)<5:complete.append("недостаточно числовых точек/параметров")
    elif profile==PROFILE_SITE:
        if v.get("buildings_total",0)<4:complete.append("неполная схема корпусов")
    if profile==PROFILE_PLAN:
        depth="semantic_hierarchy" if v.get("systems_total",0)+v.get("equipment_total",0)>0 else "geometry_inventory"
    elif profile in (PROFILE_HEATING,PROFILE_VENT):
        depth="engineering_graph" if v.get("cad_confirmed_edges_total",0)>0 else "semantic_hierarchy"
    elif profile==PROFILE_HYDRONIC:
        depth="engineering_graph" if v.get("confirmed_pairs_total",0)>0 else "spatial_inventory"
    elif profile in (PROFILE_DETAIL,PROFILE_SECTION,PROFILE_EQUIPMENT):depth="physical_hierarchy"
    elif profile==PROFILE_CHART:
        depth="analytical_geometry" if v.get("curve_paths_total",0)>0 else "raster_inventory"
    else:depth="semantic_hierarchy"
    v["description_depth"]=depth
    readiness="complete" if not complete else "topology_partial"
    graph["readiness"]={"status":readiness,"complete":not complete,"reasons":complete,
      "description_depth":depth,"vectograph_level":depth=="engineering_graph"}
    return {"use":not reasons,"complete":not complete,"readiness":readiness,"mode":profile,"reasons":reasons,
            "complete_reasons":complete,"warnings":graph.get("warnings",[]),"metrics":v}


_PROFILE_RU={
 PROFILE_PLAN:"План отопления и вентиляции",PROFILE_HEATING:"Аксонометрическая схема отопления",
 PROFILE_VENT:"Аксонометрическая схема вентиляции",PROFILE_HYDRONIC:"Принципиальная гидравлическая схема",
 PROFILE_DETAIL:"Монтажный узел ОВ",PROFILE_SECTION:"Разрез систем ОВ",
 PROFILE_EQUIPMENT:"Чертёж оборудования ОВ",PROFILE_CHART:"Рабочая характеристика оборудования",
 PROFILE_SITE:"План-схема теплоснабжения площадки",
}
_SUBTYPE_RU={
 "floor":"план этажа","axonometry":"аксонометрическая схема",
 "performance_curve":"рабочая характеристика оборудования",
 "installation":"монтажный узел","section":"разрез систем ОВ",
 "hydronic":"гидравлическая схема",
 "heating_technical_floor":"отопление технического этажа","parking_heat_supply":"теплоснабжение автостоянки",
 "ventilation_chamber":"вентиляционные камеры","smoke_control_floor":"общеобменная и противодымная вентиляция этажа",
 "roof_ventilation":"вентиляция кровли","duct_support_frames":"воздуховоды и монтажные рамы",
 "register_branches":"отопление технических помещений и кладовых","parking_heating":"отопление подземной автостоянки",
 "residential_manifold":"отопление жилой части и мест общего пользования","interbuilding_backbone":"магистраль от ИТП к корпусам",
 "vertical_riser":"вертикальная схема стояка","air_curtain_heat_supply":"отопление автостоянки и тепловых завес",
 "air_heater_heat_supply":"теплоснабжение воздухонагревателей",
 "residential_exhaust_and_smoke":"вытяжная и противодымная вентиляция жилой части",
 "general_supply_exhaust":"приточные и вытяжные системы","parking_exhaust_smoke":"вытяжная и противодымная вентиляция автостоянки",
 "smoke_control_risers":"вертикальные схемы противодымной вентиляции","multiple_exhaust_systems":"группа вытяжных систем",
 "mixing_unit":"смесительный узел","collector_and_room_input":"этажный коллекторный узел и ввод в помещения",
 "heat_exchanger_circuit":"контур с теплообменником","radiator_connection":"подключение радиатора",
 "convector_and_vertical_radiator":"внутрипольный конвектор и вертикальный радиатор",
 "pipe_floor_passages":"прокладка труб в полу и проходы через перекрытие","riser_supports":"крепление стояка и компенсаторы",
 "duct_wall_fire_damper":"проход воздуховода и противопожарный клапан","grille_and_fire_damper":"вентиляционная решётка и клапан в стене",
 "roof_equipment_supports":"монтаж вентиляционного оборудования на кровле",
 "air_heater_piping_sections":"разрезы обвязки воздухонагревателей","parking_vent_chambers":"разрезы вытяжных венткамер",
 "duct_and_equipment_sections":"разрезы воздуховодов и вентиляционного оборудования",
 "heat_exchanger":"общий вид теплообменника","ahu_section_composition":"компоновка секций вентиляционной установки",
 "ahu_exploded_view":"взрыв-схема вентиляционной установки","pump_curve":"расчётный лист насоса",
 "fan_curve":"аэродинамическая характеристика вентилятора","site_heat_source":"корпуса и ввод теплоносителя",
 "heating_floor":"отопление этажа","ventilation_floor":"вентиляция этажа",
 "parking_ventilation":"вентиляция автостоянки","heating_scheme":"схема отопления",
 "heat_supply_scheme":"схема теплоснабжения","ventilation_scheme":"схема общеобменной вентиляции",
 "smoke_control_scheme":"схема противодымной вентиляции","hydronic_collector":"коллекторный узел",
 "hydronic_heat_exchanger":"контур теплообменника","hydronic_mixing_unit":"смесительный узел",
 "duct_penetration":"проход воздуховода","fire_damper_detail":"узел противопожарного клапана",
 "pipe_support":"крепление трубопровода","duct_support":"крепление воздуховода",
 "insulation_detail":"теплоизоляция или огнезащита","vent_chamber_section":"разрез вентиляционной камеры",
 "duct_section":"разрез воздуховодов","air_heater_section":"разрез воздухонагревателя",
 "equipment_general_view":"общий вид оборудования","ahu_drawing":"чертёж вентиляционной установки",
}
_NODE_RU={
 "system":"система ОВ","riser":"стояк","equipment":"оборудование","duct_or_pipe_size":"размер воздуховода или трубы",
 "room_label":"помещение или зона",
 "source":"источник теплоносителя","pipe_system":"система трубопроводов","heating_device":"отопительный прибор",
 "valve":"арматура","level":"высотная отметка","floor":"этаж","pipe_size":"размер трубопровода",
 "vent_system":"вентиляционная система","damper":"клапан","terminal":"воздухораспределитель или вентустройство",
 "duct_size":"сечение воздуховода","fire_rating":"предел огнестойкости","pump":"насос",
 "heat_exchanger":"теплообменник","collector":"коллектор","filter":"фильтр",
 "instrument":"контрольно-измерительный прибор","apparatus_callout":"позиция оборудования",
 "dimension":"размер","assembly_part":"элемент монтажного узла","pipeline_part":"труба, воздуховод или патрубок",
 "insulation_or_fireproofing":"изоляция или огнезащита","fastener":"крепёж","support_part":"опора, рама или кронштейн",
 "elevation":"высотная отметка",
 "model":"модель оборудования","port":"патрубок или присоединение","module":"секция оборудования",
 "geometry_part":"геометрически выделенная часть","axis":"ось графика","value":"числовое значение",
 "raster_sheet_region":"крупная растровая область листа","building":"корпус","parking_zone":"участок автостоянки",
 "heat_source":"источник тепла",
}
_NETWORK_RU={
 "hvac_route":"трасса ОВ","hydronic_path":"трасса теплоносителя","air_system":"вентиляционная система",
 "hydronic_circuit":"гидравлический контур","hydronic_assembly":"состав гидравлической схемы",
 "performance_curve":"векторный путь рабочей характеристики","raster_sheet_region":"крупная растровая область листа",
 "site_heat_network":"распределительная сеть теплоносителя",
}
_STATE_RU={
 "cad_endpoint_component":"линии соединены общими конечными точками",
 "semantic_system_source":"связь определена по обозначению системы и источника",
 "nearest_geometry":"пространственная привязка к ближайшему элементу",
 "nearest_system_geometry":"пространственная привязка к ближайшей системе",
 "confirmed_pair":"соединение двух элементов подтверждено непрерывной линией",
 "path_confirmed":"соединение подтверждено непрерывной линией",
 "multi_apparatus_hydronic_path":"общая цепь нескольких аппаратов без выдуманных попарных связей",
 "spatial_inventory":"состав известен, попарные соединения не подтверждены",
 "same_cad_component":"элементы принадлежат одной непрерывной CAD-трассе",
 "vector_curve_geometry":"геометрия кривой подтверждена векторным слоем",
 "embedded_raster":"графическая область присутствует только во встроенном изображении",
 "overview_semantic":"связь следует из обзорной схемы, точная трасса не показана",
 "nearest_same_building_geometry":"пространственная привязка внутри того же корпуса",
 "equipment_code_and_same_building":"связь подтверждена кодом оборудования и корпусом",
}
_EDGE_RU={
 "source_to_system":"источник → система трубопроводов","system_to_riser":"система → стояк",
 "riser_to_device":"стояк → прибор или арматура","riser_to_level":"стояк → высотная отметка",
 "system_to_air_device":"вентиляционная система → устройство","hydronic_path":"гидравлическое соединение",
 "site_heat_distribution":"источник тепла → корпус",
}
_DEPTH_RU={
 "engineering_graph":"инженерный граф с частью связей, подтверждённых непрерывной CAD-геометрией",
 "semantic_hierarchy":"предметная иерархия; часть связей определена по обозначениям и расположению",
 "physical_hierarchy":"физическая структура видов, разрезов и составных частей",
 "geometry_inventory":"геометрический граф трасс без достаточного количества читаемых марок",
 "spatial_inventory":"состав схемы и пространственные группы без подтверждённого порядка соединений",
 "analytical_geometry":"аналитическая структура с векторной геометрией рабочих кривых",
 "raster_inventory":"параметры и область графика; сама кривая доступна только как изображение",
}


def _profile_summary_ru(profile,v):
    if profile==PROFILE_PLAN:
        return (f"Выделено {v.get('route_segments_total',0)} участков трасс, "
          f"{v.get('route_components_total',0)} связных групп и {v.get('route_branches_total',0)} точек ветвления.")
    if profile==PROFILE_HEATING:
        return (f"Найдено систем трубопроводов: {v.get('unique_systems_total',0)} "
          f"(обозначений на схеме: {v.get('systems_total',0)}), стояков: {v.get('risers_total',0)}, "
          f"приборов и арматуры: {v.get('devices_total',0)}, структурных связей: {v.get('structural_edges_total',0)}.")
    if profile==PROFILE_VENT:
        return (f"Найдено вентиляционных систем: {v.get('unique_systems_total',0)} "
          f"(обозначений на схеме: {v.get('systems_total',0)}), клапанов: {v.get('dampers_total',0)}, "
          f"вентустройств: {v.get('terminals_total',0)}, участков воздуховодов: {v.get('route_segments_total',0)}.")
    if profile==PROFILE_HYDRONIC:
        return (f"Найдено типов аппаратов и позиционных обозначений: {v.get('unique_apparatus_total',0)} "
          f"(вхождений: {v.get('apparatus_total',0)}), "
          f"гидравлических групп: {v.get('hydronic_networks_total',0)}, "
          f"подтверждённых попарных соединений: {v.get('confirmed_pairs_total',0)}.")
    if profile==PROFILE_DETAIL:
        return (f"Выделено видов и разрезов: {v.get('views_total',0)}, элементов узла: {v.get('parts_total',0)}, "
          f"размеров: {v.get('dimensions_total',0)}.")
    if profile==PROFILE_SECTION:
        return (f"Выделено разрезов: {v.get('views_total',0)}, высотных отметок: {v.get('elevations_total',0)}, "
          f"единиц оборудования: {v.get('equipment_total',0)}, размеров: {v.get('sizes_total',0)}.")
    if profile==PROFILE_EQUIPMENT:
        return (f"Найдено моделей: {v.get('models_total',0)}, секций: {v.get('modules_total',0)}, "
          f"присоединений: {v.get('ports_total',0)}, геометрически выделенных частей: {v.get('geometry_parts_total',0)}.")
    if profile==PROFILE_CHART:
        representation="векторными линиями" if v.get("curve_paths_total",0) else "встроенным растровым изображением"
        return (f"Рабочая характеристика представлена {representation}; числовых значений в текстовом слое: "
          f"{v.get('numeric_values_total',0)}.")
    return (f"Найдено корпусов: {v.get('buildings_total',0)}, источников тепла: {v.get('sources_total',0)}, "
      f"связей источник–корпус: {v.get('edges_total',0)}.")


def _engineering_tree_ru(graph):
    profile=graph["profile_id"];v=graph["validation"];nodes=graph.get("nodes") or []
    by=collections.defaultdict(list)
    for node in nodes:by[node["node_type"]].append(node["label"])
    uniq=lambda kind:list(dict.fromkeys(by.get(kind,[])))
    if profile==PROFILE_PLAN:
        axes=(graph.get("grid") or {}).get("axes") or []
        systems=uniq("system");equipment=uniq("equipment");risers=uniq("riser")
        return ["план ОВ",f"├── координатные оси: {len(axes)}",
          f"├── помещения и зоны: {len(uniq('room_label'))}",
          f"├── системы: {', '.join(systems[:12]) if systems else 'марки не прочитаны'}",
          f"├── стояки: {', '.join(risers[:12]) if risers else 'не выделены'}",
          f"├── оборудование: {', '.join(equipment[:12]) if equipment else 'не выделено по маркам'}",
          f"└── связные группы трасс: {v.get('route_components_total',0)}"]
    if profile==PROFILE_HEATING:
        return ["источник теплоносителя",
          f"└── системы трубопроводов: {len(uniq('pipe_system'))}",
          f"    ├── стояки: {len(uniq('riser'))}",f"    ├── уровни и этажи: {len(by.get('level',[]))+len(by.get('floor',[]))}",
          f"    └── приборы и арматура: {len(by.get('heating_device',[]))+len(by.get('valve',[]))}"]
    if profile==PROFILE_VENT:
        tree=["вентиляционные системы"]
        for index,network in enumerate((graph.get("networks") or [])[:20]):
            prefix="└──" if index==min(len(graph.get("networks") or []),20)-1 else "├──"
            tree.append(f"{prefix} {network.get('label')}: связанных устройств {max(0,len(network.get('endpoint_ids') or [])-1)}")
        return tree
    if profile==PROFILE_HYDRONIC:
        return ["гидравлическая схема",
          f"├── насосы: {len(by.get('pump',[]))}",f"├── теплообменники и коллекторы: {len(by.get('heat_exchanger',[]))+len(by.get('collector',[]))}",
          f"├── арматура, фильтры и приборы: {len(by.get('valve',[]))+len(by.get('filter',[]))+len(by.get('instrument',[]))}",
          f"└── подтверждённые попарные соединения: {v.get('confirmed_pairs_total',0)}"]
    if profile in (PROFILE_DETAIL,PROFILE_SECTION,PROFILE_EQUIPMENT):
        containers=graph.get("containers") or [];root="физическая структура блока"
        tree=[root]
        for index,container in enumerate(containers[:20]):
            prefix="└──" if index==min(len(containers),20)-1 else "├──"
            label=_SUBTYPE_RU.get(container.get("label"),container.get("label") or f"группа {index+1}")
            tree.append(f"{prefix} {label}: элементов {len(container.get('member_ids') or [])}")
        return tree
    if profile==PROFILE_CHART:
        return ["рабочая характеристика оборудования",
          f"├── модель: {', '.join(uniq('model')[:5]) if uniq('model') else 'не прочитана'}",
          f"├── оси: {', '.join(uniq('axis')[:10]) if uniq('axis') else 'не выделены'}",
          f"├── векторные пути графика: {v.get('curve_paths_total',0)}",
          f"└── растровые области графика: {v.get('raster_regions_total',0)}"]
    return ["источник тепла",f"└── корпуса: {', '.join(uniq('building')) if uniq('building') else 'не выделены'}"]


def render_hvac_markdown(graph):
    """Человекочитаемое русское описание ОВ; машинные коды остаются только в JSON."""
    v=graph["validation"];profile=graph["profile_id"];subtype=v.get("subtype")
    lines=[f"# Эталонная текстовая разметка ОВ: {_PROFILE_RU.get(profile,'Графический блок ОВ')}","",
      f"**Назначение блока:** {_SUBTYPE_RU.get(subtype,subtype or 'не определено')}",
      f"**Источник:** {graph['source']['pdf_file']}",
      "**Способ разбора:** детерминированно из текстового и графического слоёв PDF, без домысливания связей.","",
      "## 1. Краткий результат","",_profile_summary_ru(profile,v),"",
      f"**Уровень описания:** {_DEPTH_RU.get(v.get('description_depth'),'структура блока')}.","",
      "### Инженерное дерево","","```text",*_engineering_tree_ru(graph),"```","",
      "## 2. Состав блока",""]
    groups=collections.defaultdict(list)
    for node in graph.get("nodes") or []:groups[node["node_type"]].append(node["label"])
    if groups:
        for kind,labels in sorted(groups.items(),key=lambda item:_NODE_RU.get(item[0],item[0])):
            preview=", ".join(labels[:24]);suffix=f" … (ещё {len(labels)-24})" if len(labels)>24 else ""
            lines.append(f"- **{_NODE_RU.get(kind,'Элемент')} — {len(labels)}:** {preview}{suffix}")
    else:
        lines.append("- Текстовые обозначения отсутствуют; ниже сохранён геометрический граф трасс.")
    containers=graph.get("containers") or []
    if containers:
        lines += ["","### Виды, разрезы и физические группы",""]
        for index,container in enumerate(containers[:30],1):
            label=container.get("label") or "вид без отдельной подписи"
            lines.append(f"- **Группа {index}:** {_SUBTYPE_RU.get(label,label)}")
    if profile==PROFILE_PLAN and (graph.get("grid") or {}).get("axes"):
        axes=graph["grid"]["axes"]
        vertical=[str(axis["label"]) for axis in axes if axis["orientation"]=="vertical"]
        horizontal=[str(axis["label"]) for axis in axes if axis["orientation"]=="horizontal"]
        lines += ["","### Координатные оси","",
          f"- Вертикальные: {', '.join(vertical) if vertical else 'не выделены'}.",
          f"- Горизонтальные: {', '.join(horizontal) if horizontal else 'не выделены' }."]
    if profile==PROFILE_PLAN:
        positioned=[node for node in graph.get("nodes") or [] if node["node_type"] in ("system","riser","equipment")]
        if positioned:
            route_numbers={network["id"]:index for index,network in enumerate(graph.get("networks") or [],1)}
            lines += ["","### Пространственная привязка обозначений","",]
            for node in positioned[:40]:
                where=[]
                if node.get("nearest_room_label"):where.append(f"ближайшая зона «{node['nearest_room_label']}»")
                axes=[node.get("nearest_vertical_axis"),node.get("nearest_horizontal_axis")]
                axes=[str(axis) for axis in axes if axis]
                if axes:where.append(f"ближайшие оси {', '.join(axes)}")
                if node.get("route_id") in route_numbers:where.append(f"трасса {route_numbers[node['route_id']]}")
                lines.append(f"- **{node['label']}:** {'; '.join(where) if where else 'координата сохранена, семантическая зона не определена'}.")
    networks=graph.get("networks") or []
    lines += ["","## 3. Связи и трассы",""]
    if networks:
        shown=networks
        route_inventory=all(network.get("network_type") in ("hvac_route","hydronic_path") for network in networks)
        if route_inventory:
            significant=[network for network in networks if network.get("endpoint_ids") or network.get("branch_points")]
            shown=sorted(significant or networks,key=lambda network:(
              len(network.get("endpoint_ids") or []),network.get("branch_points",0),network.get("length",0)),reverse=True)[:20]
            lines.append("Ниже перечислены основные связные группы; мелкие фрагменты учтены в общей статистике.")
        else:
            shown=networks[:40]
        for index,network in enumerate(shown,1):
            kind=_NETWORK_RU.get(network.get("network_type"),"инженерная сеть")
            label=network.get("label") or ""
            if label in _SUBTYPE_RU:label=_SUBTYPE_RU[label]
            state=_STATE_RU.get(network.get("path_state"),"состояние связи зафиксировано в геометрии")
            details=[f"связанных обозначений: {len(network.get('endpoint_ids') or [])}"]
            if network.get("segment_ids") is not None:details.append(f"участков: {len(network.get('segment_ids') or [])}")
            if network.get("branch_points") is not None:details.append(f"ветвлений: {network.get('branch_points',0)}")
            if network.get("length") is not None:details.append(f"условная длина: {network.get('length')}")
            label_text=f" «{label}»" if label else ""
            lines.append(f"- **{kind} {index}{label_text}:** {'; '.join(details)}. Доказательность: {state}.")
        if len(networks)>len(shown):lines.append(f"- Остальные учтённые геометрические группы: {len(networks)-len(shown)}.")
    else:
        lines.append("- Отдельные сети не сформированы: для этого типа блока описывается физический состав или график.")
    edges=graph.get("edges") or []
    if edges:
        node_labels={node["id"]:node["label"] for node in graph.get("nodes") or []}
        lines += ["","### Явные связи",""]
        grouped_edges=collections.Counter()
        for edge in edges:
            relation=_EDGE_RU.get(edge.get("edge_type"),"связь")
            state=_STATE_RU.get(edge.get("edge_state"),"определена по структуре схемы")
            key=(node_labels.get(edge["from"],"элемент"),node_labels.get(edge["to"],"элемент"),relation,state)
            grouped_edges[key]+=1
        for (source,target,relation,state),count in grouped_edges.most_common(60):
            suffix=f"; повторений на схеме: {count}" if count>1 else ""
            lines.append(f"- {source} → {target}: {relation}; {state}{suffix}.")
        if len(grouped_edges)>60:lines.append(f"- Остальные группы однотипных связей: {len(grouped_edges)-60}.")
    readiness=graph.get("readiness") or {}
    lines += ["","## 4. Полнота и ограничения",""]
    if readiness.get("complete"):
        lines.append("Состав и доступная геометрия полностью описаны по правилам данного типа блока.")
    else:
        lines.append("Описание построено частично; ниже перечислены недостающие признаки.")
    if readiness.get("vectograph_level"):
        lines.append("- По глубине связей блок приблизился к Вектографу: часть рёбер подтверждена непрерывной CAD-геометрией.")
    elif v.get("description_depth")=="physical_hierarchy":
        lines.append("- Это физическая, а не потоковая схема: полнота оценивается по видам и составным частям, а не по цепочке теплоносителя или воздуха.")
    else:
        lines.append("- До уровня топологии Вектографа не хватает подтверждённой последовательности всех соединений; непроверенные рёбра не добавлены.")
    lines += [f"- {reason}" for reason in readiness.get("reasons") or []]
    lines += [f"- {warning}" for warning in graph.get("warnings") or []]
    if not readiness.get("reasons") and not graph.get("warnings"):
        lines.append("- Дополнительные ограничения не выявлены.")
    return "\n".join(lines)+"\n"
