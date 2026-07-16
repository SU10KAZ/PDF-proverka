"""Детерминированные графы ВК: вода, канализация, пожаротушение и наружные сети."""
from __future__ import annotations

import collections
import math
import re
from pathlib import Path
from typing import Optional

from .hvac_geometry import (
    _axes, _base, _bbox, _bbox_norm, _bind_nearest_axes, _center, _clip_copied_page,
    _components, _equipment_groups, _geometry_parts, _lines, _node, _point,
    _preferred_parent, _segments, _token_nodes, _unique, _views, _words,
    _assign_nodes_to_views, _attach,
)


PROFILE_PLAN="vk_floor_plan"
PROFILE_WATER="water_supply_axonometry"
PROFILE_SEWER="sewer_axonometry"
PROFILE_FIRE="fire_water_axonometry"
PROFILE_PRINCIPLE="vk_principle_scheme"
PROFILE_CONTROL="fire_control_scheme"
PROFILE_DETAIL="vk_installation_detail"
PROFILE_DRAIN="roof_drain_detail"
PROFILE_EXTERNAL="external_network_profile"
PROFILE_EQUIPMENT="pump_equipment_drawing"
PROFILE_CHART="pump_performance_chart"
ALL_WATER_PROFILES=(PROFILE_PLAN,PROFILE_WATER,PROFILE_SEWER,PROFILE_FIRE,PROFILE_PRINCIPLE,
                    PROFILE_CONTROL,PROFILE_DETAIL,PROFILE_DRAIN,PROFILE_EXTERNAL,PROFILE_EQUIPMENT,PROFILE_CHART)


def _hints(profile, pairs):return {block_id:(profile,subtype) for block_id,subtype in pairs}
_ID_HINTS={
 **_hints(PROFILE_PLAN,(("MDLY-9UXJ-QRP","combined_floor_networks"),("6CX9-JXYR-JDK","roof_drainage_plan"),
  ("7A7L-NA33-TTC","fire_floor_multi_zone"),("7QC7-K997-6CC","technical_space_sewer"),
  ("FVYW-RTGN-LHU","roof_funnel_plan"),("9TK6-RQPJ-4PA","parking_fire_water_plan"),
  ("4P9Y-F4UA-AAA","site_drain_inlets"),("9YVP-DUNF-PUA","external_water_general_plan"),
  ("7AKP-3XAW-JKM","pump_station_plan"),("467M-RFQ7-QRR","meter_room_plan"),
  ("7KEX-XQLY-YWF","powder_fire_equipment_plan"),("9JFW-T6AA-636","external_water_site_fragment"))),
 **_hints(PROFILE_WATER,(("7JJW-ATHW-AYM","building_water_supply"),("6G6D-UVFW-Q9J","multi_system_water_supply"),
  ("A4MD-MVUG-Q9L","mixed_water_and_sewer"))),
 **_hints(PROFILE_SEWER,(("4UWA-VKAX-GUA","sewer_vent_valves"),("APVK-QGG7-TYF","domestic_sewer_risers"),
  ("6VW4-PCVA-TCN","pumped_sewer_k1"),("9TJP-AC7N-A9N","pumped_sewer_k13"),
  ("9RKH-3GDJ-XL6","surface_drainage_trays"),("9HL9-X3HA-UYF","storm_sewer"),("4R3M-HJDQ-AKM","drain_riser_supports"))),
 **_hints(PROFILE_FIRE,(("6ELG-WKPH-TTX","apt_water_systems"),("4VEF-CC3P-P7K","fire_multizone_risers"),
  ("3KXN-ADNV-YUD","fire_two_zones"),("4FY9-4HEJ-RDA","apt_floor_groups"),("6W3G-49JU-UUR","fire_riser_cabinets"))),
 **_hints(PROFILE_PRINCIPLE,(("ACNM-K9RG-GAW","reducer_meter_node"),("9VED-4RWC-JGT","meter_insert"),
  ("YTCD-PQTW-CNX","meter_unit"),("LTML-9LCT-7YM","drain_pump_piping"),("RTYX-NMUE-QWP","apt_hydraulic_systems"),
  ("VPGV-LP9X-3TQ","external_water_nodes"),("6E4A-R664-LNC","water_distribution_unit"))),
 **_hints(PROFILE_CONTROL,(("A9PA-PLLL-JRA","powder_control_hierarchy"),("9DWQ-NEDP-VVQ","powder_equipment_connection"))),
 **_hints(PROFILE_DETAIL,(("6YAL-WPEH-4QT","riser_compensators"),("6DRP-QK9J-LRK","sml_supports_and_passages"),
  ("6NCR-VDKH-Q4P","steel_pipe_passages"),("4K36-7GUG-EMY","sewer_passages"),("9X6H-QW63-CQN","trap_and_riser_support"),
  ("9KH3-9LNQ-6HD","support_ring_and_fire_collar"),("4YNR-JG9T-7UR","fire_cabinets"),
  ("4FPX-79PV-RQ6","diaphragm_and_sprinkler"),("9FNF-T7X3-DNQ","irrigation_tap"),
  ("7UUT-3L4G-JRP","hydrant_well_ladder"),("7F3X-JEHG-YNG","chamber_sump"),
  ("YRJY-669K-XH7","sewer_wall_outlet"),("4HC7-9DGM-A7F","hydrant_well_details"))),
 **_hints(PROFILE_DRAIN,(("4TCK-G7PP-FG3","sidewalk_drain_funnel"),("9HFH-H69X-XNE","slab_drain_funnel"),
  ("9GGD-TJKE-CAW","surface_drain_funnel"),("476G-H4YC-DKY","roof_funnel"),("6JVJ-J4CY-LE6","pfo_roof_funnel"))),
 **_hints(PROFILE_EXTERNAL,(("9TP6-RNQ3-QLN","geological_pipe_section"),("69PE-67GK-DWK","valve_sections"),
  ("46AK-ANKJ-MTT","chamber_sections"),("UWWH-AC7Y-KT3","pipe_construction_sections"),
  ("7VQA-JP9G-K4D","storm_sewer_longitudinal_profile"),("4L3V-JMVA-KRA","pump_station_sections"),
  ("6P9R-MJWM-7JJ","fire_pump_sections"))),
 **_hints(PROFILE_CHART,(("6TC7-T7MM-CHH","drain_pump_curves"),("4JG6-HM3Q-PVJ","pump_curves"))),
 **_hints(PROFILE_EQUIPMENT,(("64XC-ADXH-6HP","submersible_pump_connection"),("43DY-YCC6-TP7","submersible_pump_views"),
  ("4K6C-D3XN-PCE","fire_pump_unit"),("9WXQ-AVKX-7NC","modular_fire_pump_unit"))),
}


def classify_water_profile(text:str,*,block_id:Optional[str]=None,prefer_block_hint:bool=True):
    if prefer_block_hint and block_id in _ID_HINTS:return _ID_HINTS[block_id]
    upper=re.sub(r"\s+"," ",text or "").upper()
    if "ПРОДОЛЬНЫЙ ПРОФИЛЬ" in upper:return PROFILE_EXTERNAL,"longitudinal_profile"
    if "РАБОЧ" in upper and any(x in upper for x in ("КПД","NPSH","НАПОР")):return PROFILE_CHART,"pump_curve"
    if "МПТ" in upper and any(x in upper for x in ("ПОДКЛЮЧ","СТРУКТУРН")):return PROFILE_CONTROL,"powder_fire"
    if any(x in upper for x in ("ВОДОМЕРН","СЧЕТЧИК ВОДЫ","СЧЁТЧИК ВОДЫ","УЗЕЛ Г")):return PROFILE_PRINCIPLE,"meter_node"
    if any(x in upper for x in ("УЗЛ", "ДЕТАЛЬ", "СХЕМА УСТАНОВКИ")) and any(x in upper for x in ("ВОРОНК","ТРАП","КРЕПЛЕН","ПРОХОД","МУФТ", "ТРУБОПРОВОД", "ГИЛЬЗ", "WDU", "ВОДОСНАБ")):return PROFILE_DETAIL,"installation"
    if "ПЛАН" in upper and any(x in upper for x in ("ВОДОСНАБ","ВОДООТВ","КАНАЛИЗ","ВОДОПРОВОД","ПОЖАРОТУШ","ВОРОНК","ТРАП")):
        return PROFILE_PLAN,"floor_plan"
    if "АКСОНОМЕТ" in upper or "ИЗОМЕТРИЧ" in upper:
        if any(x in upper for x in ("КАНАЛИЗ", "ВОДООТВОД", "К1", "К2")):return PROFILE_SEWER,"sewer_risers"
        if any(x in upper for x in ("ПОЖАРОТУШ", "ПОЖАРНЫЙ ВОДОПРОВОД", "В2", "В21")):return PROFILE_FIRE,"fire_risers"
        if any(x in upper for x in ("ВОДОСНАБ", "ВОДОПРОВОД", "В1", "Т3", "Т4")):return PROFILE_WATER,"water_supply"
    if "ПРИНЦИПИАЛЬНАЯ СХЕМА" in upper and any(x in upper for x in ("НАСОСНАЯ СТАНЦ", "ВОДОМЕР", "ВОДОСНАБ", "ВОДОПРОВОД", "ПОЖАРОТУШ")):
        return PROFILE_PRINCIPLE,"principle_scheme"
    if "СХЕМА ПОДКЛЮЧЕНИ" in upper and any(x in upper for x in ("ВОДООТВЕД", "КАНАЛИЗ", "ВОДОСНАБ")):
        return PROFILE_PRINCIPLE,"connection_scheme"
    if len(re.findall(r"\b(?:В2|В21)\.\d",upper))>=2:return PROFILE_FIRE,"fire_risers"
    if len(re.findall(r"\bК\d",upper))>=2:return PROFILE_SEWER,"sewer_risers"
    if len(re.findall(r"\b(?:В1|Т3|Т4)\d*(?:\.\d+)*",upper))>=2:return PROFILE_WATER,"water_supply"
    return None,None


_SYSTEM_RE=re.compile(r"^(?:В(?:1|2|21|11)\d*(?:\.\d+)*|Т[34]\d*(?:\.\d+)*|К\d+(?:\.\d+)*(?:н|с)?)$",re.I)
_RISER_RE=re.compile(r"Ст\s*\.\s*(?:В|К|Т)\s*[A-ZА-Яа-я0-9.()_\-х×,]+",re.I)
_ELEV_RE=re.compile(r"^[+\-]?\d{1,3}[.,]\d{3}$")
_DIAMETER_RE=re.compile(r"^(?:[Ø∅⌀øф]\s*\d+(?:[xх×]\s*\d+(?:[.,]\d+)?)?|Ду\s*\d+|DN\s*\d+)$",re.I)
_SLOPE_RE=re.compile(r"^(?:i\s*=\s*)?0[.,]0\d+$",re.I)
_ENGINEERING_LINE_PATTERNS=(
 ("system",re.compile(r"(?<![\w.])(?:В\s*(?:1|2|21|11)\s*\d*(?:\s*\.\s*\d+)*|Т\s*[34]\s*\d*(?:\s*\.\s*\d+)*|К\s*\d+(?:\s*\.\s*\d+)*\s*(?:н|с)?)(?![\w.])",re.I)),
 ("riser",_RISER_RE),
 ("elevation",re.compile(r"(?<!\d)[+\-]?\d{1,3}[.,]\d{3}(?!\d)")),
 ("floor",re.compile(r"(?:\b(?:Этаж|эт\.)\s*[+\-]?\d{1,2}(?=[\s,;.)\"']|$)|\b[+\-]?\d{1,2}\s*(?:Этаж|эт\.)(?=[\s,;.)\"']|$))",re.I)),
 ("diameter",re.compile(r"(?:[Ø∅⌀øф]\s*\d+(?:\s*[xх×]\s*\d+(?:[.,]\d+)?)?|(?:Ду|DN)\s*\d+)",re.I)),
 ("slope",re.compile(r"(?:\bi\s*=\s*0[.,]0\d+\b|\b0[.,]0(?:0[1-9]|[1-9]\d*)\b)",re.I)),
 ("pipe_material",re.compile(r"\b(?:SML|ПЭ\s*\d*|ПНД|ВЧШГ|ПВХ|нПВХ|сталь\w*|чугун\w*|полиэтилен\w*|полипропилен\w*)\b",re.I)),
)
_CONTINUATION_RE=re.compile(r"(?:далее\s+(?:см\s*\.?|смотреть)|продолжение\s+см\s*\.?|см\s*\.\s*(?:лист|том|комплект|раздел|узел))",re.I)
_ENGINEERING_NOTE_PATTERNS=(
 ("water_meter",re.compile(r"сч[её]тчик|водомер",re.I)),
 ("drain_inlet",re.compile(r"воронк|трап|пескоуловител",re.I)),
 ("valve",re.compile(r"клапан|задвижк|вентиль|редуктор|регулятор давления|фильтр",re.I)),
 ("pump",re.compile(r"\b(?:насос\w*|КНС|ДНС)\b",re.I)),
 ("collector",re.compile(r"\bколлектор\w*\b",re.I)),
 ("sprinkler",re.compile(r"\b(?:спринклер\w*|оросител\w*)\b",re.I)),
 ("sanitary_fixture",re.compile(r"унитаз|умывальник|раковин|мойк|душев|ванн|санитарн.*прибор",re.I)),
 ("pipe_insulation",re.compile(r"теплоизоляц|изоляц.*труб|цилиндр.*толщин",re.I)),
 ("support_requirement",re.compile(r"неподвижн.*опор|креплен.*труб|хомут|кронштейн|шпильк",re.I)),
 ("penetration_requirement",re.compile(r"проходк|гильз|заделк|огнезащ|противопожарн.*муфт",re.I)),
 ("hydraulic_parameter",re.compile(r"(?:\bQ\s*=|\bH\s*=|м[³3]\s*/\s*ч|л\s*/\s*с|\bМПа\b|\bbar\b|\bбар\b)",re.I)),
)
_SYSTEM_DEVICE_TYPES=("pump","drain_inlet","fire_cabinet","valve","sprinkler","collector","water_meter","sanitary_fixture")
_SYSTEM_PARAMETER_TYPES=("diameter","slope","pipe_material","continuation_reference","elevation","floor",
                         "pipe_insulation","support_requirement","penetration_requirement","hydraulic_parameter")


def _engineering_line_nodes(page):
    """Извлечь все инженерные факты, в том числе несколько фактов из одной подписи."""
    nodes=[]
    for line in _lines(page):
        for kind,pattern in _ENGINEERING_LINE_PATTERNS:
            for match in pattern.finditer(line["text"]):
                if kind=="system" and re.search(r"\bРД\b",line["text"],re.I) and not re.search(
                  r"систем|сет|стояк|\bСт\s*\.|\bОп\s*\.|\bПод\s*\.|выпуск",line["text"],re.I):
                    continue
                if kind=="elevation":
                    value=match.group().replace(",",".")
                    if not value.startswith(("+","-")) and re.fullmatch(r"0\.0(?!00)\d+",value):
                        continue
                item=dict(line);item["text"]=re.sub(r"\s+"," ",match.group()).strip(" ;,")
                if kind=="system":
                    item["text"]=re.sub(r"\s+","",item["text"])
                nodes.append(_node(page,item,kind,len(nodes)+1,source_label=line["text"]))
        if _CONTINUATION_RE.search(line["text"]):
            item=dict(line);item["text"]=line["text"][:500]
            nodes.append(_node(page,item,"continuation_reference",len(nodes)+1))
        for kind,pattern in _ENGINEERING_NOTE_PATTERNS:
            if pattern.search(line["text"]):
                item=dict(line);item["text"]=line["text"][:500]
                nodes.append(_node(page,item,kind,len(nodes)+1,field_state="engineering_annotation"))
    return _unique(nodes)


def _image_nodes(page,start=0):
    result=[]
    for info in page.get_image_info(xrefs=True):
        bbox=_bbox(page,info["bbox"]);w=bbox[2]-bbox[0];h=bbox[3]-bbox[1]
        if w<page.rect.width*.16 or h<page.rect.height*.12:continue
        item={"text":f"крупная растровая область {len(result)+1}","bbox":bbox,"center":_center(bbox)}
        result.append(_node(page,item,"raster_region",start+len(result)+1,field_state="raster_geometry_only"))
    if not result and page.get_images(full=True):
        bbox=(0.0,0.0,float(page.rect.width),float(page.rect.height))
        item={"text":f"растровая мозаика листа, фрагментов {len(page.get_images(full=True))}","bbox":bbox,"center":_center(bbox)}
        result.append(_node(page,item,"raster_mosaic",start+1,field_state="raster_mosaic_only"))
    return result


def _semantic_nodes(page,*,include_rooms=False):
    patterns=(("fire_cabinet",re.compile(r"^(?:ШПК|ШП-К)[A-ZА-Яа-я0-9._()/-]*",re.I)),
      ("pump",re.compile(r"^(?:Насос|КНС|ДНС|НС-|Grundfos|Wilo)[A-ZА-Яа-я0-9._()/-]*",re.I)),
      ("drain_inlet",re.compile(r"^(?:Воронка|Трап|HL\d|PFO|VMCO)[A-ZА-Яа-я0-9._()/-]*",re.I)),
      ("valve",re.compile(r"^(?:Кран|Клапан|Задвижка|Вентиль|Редуктор|Фильтр)[A-ZА-Яа-я0-9._()/-]*",re.I)),
      ("collector",re.compile(r"^Коллектор[A-ZА-Яа-я0-9._()/-]*",re.I)),
      ("sprinkler",re.compile(r"^(?:Спринклер\w*|Оросител\w*)$",re.I)))
    nodes=_token_nodes(page,patterns,use_lines=False)+_engineering_line_nodes(page)
    if include_rooms:
        room_re=re.compile(r"(?:насосн|водомерн|сануз|технич.*помещ|автостоян|коридор|ИТП|камера|колодец)",re.I)
        for line in _lines(page):
            if len(line["text"])<=90 and room_re.search(line["text"]):nodes.append(_node(page,line,"room_label",len(nodes)+1))
    return _unique(nodes)


def _route_networks(nodes,components,network_type,label):
    result=[]
    for component in components:
        members=[node for node in nodes if node.get("route_id")==component["id"]]
        semantic=[node["label"] for node in members if node["node_type"] in ("system","riser",*_SYSTEM_DEVICE_TYPES)]
        result.append({"id":component["id"],"network_type":network_type,
          "label":" / ".join(dict.fromkeys(semantic)) or label,
          "endpoint_ids":[node["id"] for node in members],"path_state":"cad_endpoint_component",**component})
    return result


def _build_plan(page,pdf,block_id,subtype):
    nodes=_semantic_nodes(page,include_rooms=True);axes=_axes(page);_bind_nearest_axes(nodes,axes)
    rooms=[node for node in nodes if node["node_type"]=="room_label"]
    for node in nodes:
        if node in rooms or not rooms:continue
        room=min(rooms,key=lambda item:math.hypot(node["x"]-item["x"],node["y"]-item["y"]))
        if math.hypot(node["x"]-room["x"],node["y"]-room["y"])<=max(page.rect.width,page.rect.height)*.25:
            node["nearest_room_label"]=room["label"]
    segments=_segments(page,vivid_only=True,min_length=3)
    if len(segments)<20:segments=_segments(page,vivid_only=False,min_length=3)
    components=_components(segments,tolerance=2.0);technical=[node for node in nodes if node["node_type"]!="room_label"]
    attached=_attach(technical,segments,components,limit=30);images=_image_nodes(page,len(nodes));nodes=_unique(nodes+images)
    networks=_route_networks(nodes,components,"water_or_sewer_route",subtype)
    graph=_base(page,pdf,block_id,PROFILE_PLAN,subtype,nodes=nodes,networks=networks,
      validation={"axes_total":len(axes),"systems_total":sum(n["node_type"]=="system" for n in nodes),
       "unique_systems_total":len({n["label"].upper() for n in nodes if n["node_type"]=="system"}),
       "risers_total":sum(n["node_type"]=="riser" for n in nodes),"equipment_total":sum(n["node_type"] in _SYSTEM_DEVICE_TYPES for n in nodes),
       "diameters_total":sum(n["node_type"]=="diameter" for n in nodes),
       "materials_total":sum(n["node_type"]=="pipe_material" for n in nodes),
       "slopes_total":sum(n["node_type"]=="slope" for n in nodes),
       "elevations_total":sum(n["node_type"]=="elevation" for n in nodes),
       "continuations_total":sum(n["node_type"]=="continuation_reference" for n in nodes),
       "room_labels_total":len(rooms),"nodes_route_attached":attached,"route_segments_total":len(segments),
       "route_components_total":len(components),"route_branches_total":sum(c["branch_points"] for c in components),
       "raster_regions_total":len(images),"topology_state":"plan_route_graph"},
      warnings=["X-пересечение без конечной точки CAD не считается подключением",
                "ближайшее помещение является пространственной подсказкой, а не восстановленным полигоном комнаты"])
    graph["grid"]={"axes":axes};graph["route_segments"]=segments;return graph


def _build_axonometry(page,pdf,block_id,subtype,profile):
    nodes=_semantic_nodes(page);segments=_segments(page,vivid_only=False,min_length=2)
    components=_components(segments,tolerance=4.0);attached=_attach(nodes,segments,components,limit=24)
    systems=[n for n in nodes if n["node_type"]=="system"];risers=[n for n in nodes if n["node_type"]=="riser"]
    apparatus=[n for n in nodes if n["node_type"] in _SYSTEM_DEVICE_TYPES]
    levels=[n for n in nodes if n["node_type"] in ("floor","elevation")];edges=[]
    parameters=[n for n in nodes if n["node_type"] in _SYSTEM_PARAMETER_TYPES and n not in levels]
    def link(parent,child,edge_type,state):edges.append({"id":f"edge-{len(edges)+1}","from":parent["id"],"to":child["id"],"edge_type":edge_type,"edge_state":state})
    for riser in risers:
        if systems:
            parent,state=_preferred_parent(riser,systems);link(parent,riser,"system_to_riser",state)
    parents=risers or systems
    for child in apparatus+levels+parameters:
        if parents:
            parent,state=_preferred_parent(child,parents);link(parent,child,
              "riser_to_equipment" if child in apparatus else ("riser_to_level" if child in levels else "riser_to_parameter"),state)
    grouped=collections.defaultdict(list)
    for system in systems:grouped[system["label"].strip().upper()].append(system)
    networks=[]
    for occurrences in grouped.values():
        ids={node["id"] for node in occurrences}
        descendants=[edge["to"] for edge in edges if edge["from"] in ids]
        networks.append({"id":f"system-{len(networks)+1}","network_type":"logical_water_system",
          "label":occurrences[0]["label"],"endpoint_ids":[*ids,*dict.fromkeys(descendants)],
          "path_state":"mixed_evidence"})
    images=_image_nodes(page,len(nodes));nodes=_unique(nodes+images)
    graph=_base(page,pdf,block_id,profile,subtype,nodes=nodes,networks=networks,edges=edges,
      validation={"systems_total":len(systems),"unique_systems_total":len(grouped),"risers_total":len(risers),
       "unique_risers_total":len({n["label"].upper() for n in risers}),"equipment_total":len(apparatus),
       "diameters_total":sum(n["node_type"]=="diameter" for n in nodes),
       "materials_total":sum(n["node_type"]=="pipe_material" for n in nodes),
       "slopes_total":sum(n["node_type"]=="slope" for n in nodes),
       "continuations_total":sum(n["node_type"]=="continuation_reference" for n in nodes),
       "levels_total":len(levels),"unique_floors_total":len({n["label"].upper() for n in nodes if n["node_type"]=="floor"}),
       "unique_elevations_total":len({n["label"] for n in nodes if n["node_type"]=="elevation"}),"route_segments_total":len(segments),
       "route_components_total":len(components),"nodes_route_attached":attached,"structural_edges_total":len(edges),
       "cad_confirmed_edges_total":sum(e["edge_state"]=="same_cad_component" for e in edges),
       "spatial_edges_total":sum(e["edge_state"]=="nearest_geometry" for e in edges),
       "raster_regions_total":len(images),"topology_state":"system_riser_level_equipment"},
      warnings=["разрывы труб на условных обозначениях соединяются только при близких конечных точках",
                "пространственные рёбра явно отделены от связей по одной CAD-компоненте"])
    graph["route_segments"]=segments;graph["route_components"]=components;return graph


_APPARATUS_PATTERNS=(("pump",re.compile(r"насос|КНС|ДНС|НС-",re.I)),
 ("water_meter",re.compile(r"счетчик|счётчик|водомер",re.I)),("filter",re.compile(r"фильтр",re.I)),
 ("pressure_reducer",re.compile(r"редукц|регулятор давления",re.I)),
 ("valve",re.compile(r"клапан|кран|задвижк|вентиль",re.I)),("collector",re.compile(r"коллектор",re.I)),
 ("instrument",re.compile(r"манометр|датчик давления|расходомер",re.I)),
 ("pressure_damper",re.compile(r"гашени[ея] напора",re.I)),("diameter",re.compile(r"(?:[Ø∅⌀øф]|Ду|DN)\s*\d+",re.I)),
 ("position",re.compile(r"^\d{1,2}$")))


def _build_principle(page,pdf,block_id,subtype):
    nodes=_unique(_token_nodes(page,_APPARATUS_PATTERNS,use_lines=True)+_engineering_line_nodes(page));segments=_segments(page,vivid_only=False,min_length=1.5)
    components=_components(segments,tolerance=4.0);attached=_attach(nodes,segments,components,limit=22)
    members=collections.defaultdict(list)
    for node in nodes:
        if node.get("route_id"):members[node["route_id"]].append(node)
    networks=[];edges=[]
    for route_id,items in members.items():
        apparatus=[node for node in items if node["node_type"] in {kind for kind,_ in _APPARATUS_PATTERNS if kind not in ("diameter",)}]
        if len(apparatus)<2:continue
        state="confirmed_pair" if len(apparatus)==2 else "multi_apparatus_path"
        network_id=f"circuit-{len(networks)+1}"
        networks.append({"id":network_id,"network_type":"water_circuit",
          "label":"Подтверждённая связь" if len(apparatus)==2 else "Общая цепь аппаратов",
          "source_route_id":route_id,"endpoint_ids":[n["id"] for n in apparatus],"path_state":state})
        if len(apparatus)==2:edges.append({"id":f"edge-{len(edges)+1}","network_id":network_id,
          "from":apparatus[0]["id"],"to":apparatus[1]["id"],"edge_type":"water_path","edge_state":"path_confirmed"})
    apparatus=[node for node in nodes if node["node_type"] in {kind for kind,_ in _APPARATUS_PATTERNS if kind not in ("diameter",)}]
    if not networks and len(apparatus)>=2:networks=[{"id":"assembly-1","network_type":"water_assembly",
      "label":"Состав принципиальной схемы","endpoint_ids":[n["id"] for n in apparatus],"path_state":"spatial_inventory"}]
    images=_image_nodes(page,len(nodes));nodes=_unique(nodes+images)
    graph=_base(page,pdf,block_id,PROFILE_PRINCIPLE,subtype,nodes=nodes,networks=networks,edges=edges,
      validation={"apparatus_total":len(apparatus),"unique_apparatus_total":len({(n["node_type"],n["label"].upper()) for n in apparatus}),
       "systems_total":sum(n["node_type"]=="system" for n in nodes),"elevations_total":sum(n["node_type"]=="elevation" for n in nodes),
       "diameters_total":sum(n["node_type"]=="diameter" for n in nodes),"materials_total":sum(n["node_type"]=="pipe_material" for n in nodes),
       "slopes_total":sum(n["node_type"]=="slope" for n in nodes),"continuations_total":sum(n["node_type"]=="continuation_reference" for n in nodes),
       "route_segments_total":len(segments),"route_components_total":len(components),"nodes_route_attached":attached,
       "confirmed_pairs_total":len(edges),"multi_apparatus_networks":sum(n["path_state"]=="multi_apparatus_path" for n in networks),
       "spatial_inventory_networks":sum(n["path_state"]=="spatial_inventory" for n in networks),
       "raster_regions_total":len(images),"topology_state":"apparatus_path_components"},
      warnings=["многоаппаратная цепь не разбивается на вымышленные попарные соединения"])
    graph["route_segments"]=segments;return graph


_CONTROL_PATTERNS=(("control_module",re.compile(r"(?:МПТ|МПП|БКИ|С2000|Рубеж)[A-ZА-Яа-я0-9._/-]*",re.I)),
 ("detector",re.compile(r"(?:ИПР|извещатель|датчик)[A-ZА-Яа-я0-9._/-]*",re.I)),
 ("actuator",re.compile(r"(?:модуль пожаротушения|пуск|оповещатель|табло)[A-ZА-Яа-я0-9._/-]*",re.I)),
 ("terminal",re.compile(r"^(?:X|XT|КЛ)\d+(?::\d+)?$",re.I)),
 ("cable",re.compile(r"(?:КПС|ВВГ|КВВГ|нг-LS)[A-ZА-Яа-я0-9._xх×/-]*",re.I)),
 ("power",re.compile(r"(?:220В|24В|12В|~220)",re.I)))


def _build_control(page,pdf,block_id,subtype):
    nodes=_token_nodes(page,_CONTROL_PATTERNS,use_lines=True);segments=_segments(page,vivid_only=False,min_length=1.5)
    components=_components(segments,tolerance=3.0);attached=_attach(nodes,segments,components,limit=22)
    by=collections.defaultdict(list)
    for node in nodes:
        if node.get("route_id"):by[node["route_id"]].append(node)
    networks=[];edges=[]
    for route_id,items in by.items():
        if len(items)<2:continue
        state="confirmed_pair" if len(items)==2 else "multi_terminal_path";nid=f"control-{len(networks)+1}"
        networks.append({"id":nid,"network_type":"control_circuit","label":"Цепь управления",
          "source_route_id":route_id,"endpoint_ids":[n["id"] for n in items],"path_state":state})
        if len(items)==2:edges.append({"id":f"edge-{len(edges)+1}","network_id":nid,"from":items[0]["id"],
          "to":items[1]["id"],"edge_type":"control_connection","edge_state":"path_confirmed"})
    images=_image_nodes(page,len(nodes));nodes=_unique(nodes+images)
    return _base(page,pdf,block_id,PROFILE_CONTROL,subtype,nodes=nodes,networks=networks,edges=edges,
      validation={"modules_total":sum(n["node_type"]=="control_module" for n in nodes),
       "field_devices_total":sum(n["node_type"] in ("detector","actuator") for n in nodes),
       "terminals_total":sum(n["node_type"]=="terminal" for n in nodes),"cables_total":sum(n["node_type"]=="cable" for n in nodes),
       "route_segments_total":len(segments),"nodes_route_attached":attached,"confirmed_pairs_total":len(edges),
       "multi_terminal_networks":sum(n["path_state"]=="multi_terminal_path" for n in networks),
       "raster_regions_total":len(images),"topology_state":"control_module_device_circuits"},
      warnings=["многоточечная цепь сохраняется как общая сеть без выдуманного порядка клемм"])


def _detail_nodes(page):
    nodes=[];dimension=re.compile(r"^(?:[RØ∅⌀øф]?\s*\d{1,4}(?:[.,]\d{1,3})?(?:\s*[xх×-]\s*\d{1,4}(?:[.,]\d{1,3})?)*)$",re.I)
    skip=re.compile(r"^(?:Изм\.|Лист|Дата|Разраб|Проверил|Масштаб)",re.I)
    for line in _lines(page):
        text=line["text"]
        if len(text)<2 or skip.search(text) or re.match(r"^(?:Узел|Разрез|Вид)\b",text,re.I):continue
        if _ELEV_RE.fullmatch(text):kind="elevation"
        elif dimension.fullmatch(text):kind="dimension"
        elif re.search(r"воронк|трап",text,re.I):kind="drain_inlet"
        elif re.search(r"ШПК|пожарн.*шкаф",text,re.I):kind="fire_cabinet"
        elif re.search(r"спринклер|оросител",text,re.I):kind="sprinkler"
        elif re.search(r"муфт.*противопож|огнезащ|гермет|мастик",text,re.I):kind="fire_seal"
        elif re.search(r"труб|патруб|стояк|выпуск",text,re.I):kind="pipe_part"
        elif re.search(r"болт|гайк|шайб|анкер|хомут|саморез|шпильк",text,re.I):kind="fastener"
        elif re.search(r"опор|кольцо|рама|кронштейн|профиль|уголок|траверс",text,re.I):kind="support_part"
        elif re.search(r"гидрант|лестниц|скоб",text,re.I):kind="well_equipment"
        elif re.search(r"бетон|стяжк|гидроизоляц|утеплител|мембран|плитк|грунт|песок|щебень",text,re.I):kind="construction_layer"
        else:kind="assembly_part"
        nodes.append(_node(page,line,kind,len(nodes)+1))
    return _unique(nodes)


def _build_detail(page,pdf,block_id,subtype,profile):
    nodes=_detail_nodes(page);views=_views(page);images=_image_nodes(page,len(nodes));nodes=_unique(nodes+images)
    if not views:
        views=[{"id":"view-1","container_type":"physical_detail","label":subtype,"member_ids":[n["id"] for n in nodes]}]
        for node in nodes:node["container_ids"]=["view-1"]
    else:_assign_nodes_to_views(nodes,views)
    segments=_segments(page,vivid_only=False,min_length=2)
    return _base(page,pdf,block_id,profile,subtype,nodes=nodes,containers=views,
      validation={"views_total":len(views),"parts_total":sum(n["node_type"] not in ("dimension","elevation","raster_region","raster_mosaic") for n in nodes),
       "dimensions_total":sum(n["node_type"]=="dimension" for n in nodes),"layers_total":sum(n["node_type"]=="construction_layer" for n in nodes),
       "supports_total":sum(n["node_type"]=="support_part" for n in nodes),"fasteners_total":sum(n["node_type"]=="fastener" for n in nodes),
       "primary_devices_total":sum(n["node_type"] in ("drain_inlet","fire_cabinet","sprinkler","well_equipment") for n in nodes),
       "line_segments_total":len(segments),"raster_regions_total":len(images),"topology_state":"physical_views_and_parts"})


def _build_external(page,pdf,block_id,subtype):
    patterns=(("elevation",_ELEV_RE),("diameter",_DIAMETER_RE),
      ("well_or_chamber",re.compile(r"(?:Колодец|Камера|ВК-|ПГ-|ДК-)[A-ZА-Яа-я0-9._/-]*",re.I)),
      ("picket",re.compile(r"^(?:ПК|Пк)\s*\d+(?:\+\d+(?:[.,]\d+)?)?$")),
      ("distance",re.compile(r"^\d+(?:[.,]\d+)?\s*м$",re.I)),
      ("ground_level",re.compile(r"(?:отм.*земл|уровень земли|черная отметка|чёрная отметка)",re.I)),
      ("pipe_material",re.compile(r"(?:ПЭ|ПНД|ВЧШГ|сталь|чугун)[A-ZА-Яа-я0-9._/-]*",re.I)))
    nodes=_unique(_token_nodes(page,patterns,use_lines=True)+_engineering_line_nodes(page));views=_views(page);images=_image_nodes(page,len(nodes));nodes=_unique(nodes+images)
    if views:_assign_nodes_to_views(nodes,views)
    segments=_segments(page,vivid_only=False,min_length=2);wells=[n for n in nodes if n["node_type"]=="well_or_chamber"]
    edges=[]
    if subtype=="storm_sewer_longitudinal_profile":
        for left,right in zip(sorted(wells,key=lambda n:n["x"]),sorted(wells,key=lambda n:n["x"])[1:]):
            edges.append({"id":f"edge-{len(edges)+1}","from":left["id"],"to":right["id"],
              "edge_type":"profile_sequence","edge_state":"left_to_right_profile_order"})
    return _base(page,pdf,block_id,PROFILE_EXTERNAL,subtype,nodes=nodes,containers=views,edges=edges,
      validation={"views_total":len(views),"wells_total":len(wells),"elevations_total":sum(n["node_type"]=="elevation" for n in nodes),
       "pickets_total":sum(n["node_type"]=="picket" for n in nodes),"diameters_total":sum(n["node_type"]=="diameter" for n in nodes),
       "systems_total":sum(n["node_type"]=="system" for n in nodes),"materials_total":sum(n["node_type"]=="pipe_material" for n in nodes),
       "profile_edges_total":len(edges),"line_segments_total":len(segments),"raster_regions_total":len(images),
       "topology_state":"external_profile_or_section"},
      warnings=["порядок колодцев слева направо применяется только к продольному профилю"])


def _build_equipment(page,pdf,block_id,subtype):
    patterns=(("model",re.compile(r"(?:Grundfos|Wilo|Flygt|Lowara|CO\s*\d|MVL|SK-FFS)[A-ZА-Яа-я0-9._/ -]*",re.I)),
      ("port",re.compile(r"^(?:DN\d+|Вход|Выход|Напорный патрубок)$",re.I)),
      ("dimension",re.compile(r"^(?:[RØ∅⌀øф]?\d+(?:[.,]\d+)?(?:[xх×]\d+(?:[.,]\d+)?)*)$",re.I)),
      ("electrical",re.compile(r"(?:кВт|Вольт|220В|380В|кабель|подключение)",re.I)))
    nodes=_unique(_token_nodes(page,patterns,use_lines=True)+_engineering_line_nodes(page));parts=_geometry_parts(page)
    for part in parts:
        item={"text":f"геометрически выделенная часть {len(nodes)+1}","bbox":part["bbox"],"center":part["center"]}
        nodes.append(_node(page,item,"geometry_part",len(nodes)+1,field_state="geometry_only"))
    images=_image_nodes(page,len(nodes));nodes=_unique(nodes+images);views=_views(page)
    containers=views or _equipment_groups(page,nodes,subtype)
    if views:_assign_nodes_to_views(nodes,views)
    return _base(page,pdf,block_id,PROFILE_EQUIPMENT,subtype,nodes=nodes,containers=containers,
      validation={"models_total":sum(n["node_type"]=="model" for n in nodes),"ports_total":sum(n["node_type"]=="port" for n in nodes),
       "dimensions_total":sum(n["node_type"]=="dimension" for n in nodes),"electrical_total":sum(n["node_type"]=="electrical" for n in nodes),
       "diameters_total":sum(n["node_type"]=="diameter" for n in nodes),"materials_total":sum(n["node_type"]=="pipe_material" for n in nodes),
       "geometry_parts_total":sum(n["node_type"]=="geometry_part" for n in nodes),"physical_groups_total":len(containers),
       "raster_regions_total":len(images),"topology_state":"pump_physical_composition"})


def _build_chart(page,pdf,block_id,subtype):
    patterns=(("model",re.compile(r"(?:Grundfos|Wilo|Flygt|Lowara|SEV|SLV)[A-ZА-Яа-я0-9._/-]*",re.I)),
      ("axis",re.compile(r"^(?:Q|H|P|P1|P2|NPSH|η|КПД)(?:,.*)?$",re.I)),("value",re.compile(r"^\d+(?:[.,]\d+)?$")))
    nodes=_token_nodes(page,patterns);paths=[]
    for drawing in page.get_drawings():
        kinds=[item[0] for item in drawing.get("items") or []]
        if "c" in kinds or len(kinds)>=3:paths.append(len(kinds))
    images=_image_nodes(page,len(nodes));nodes=_unique(nodes+images);networks=[]
    for count in paths:networks.append({"id":f"curve-{len(networks)+1}","network_type":"performance_path",
      "label":f"Векторный путь {len(networks)+1}","endpoint_ids":[],"path_state":"vector_curve_geometry","geometry_items":count})
    for node in nodes:
        if node["node_type"]=="raster_region":networks.append({"id":f"raster-{len(networks)+1}","network_type":"raster_sheet_region",
          "label":node["label"],"endpoint_ids":[node["id"]],"path_state":"embedded_raster"})
    return _base(page,pdf,block_id,PROFILE_CHART,subtype,nodes=nodes,networks=networks,
      validation={"models_total":sum(n["node_type"]=="model" for n in nodes),"axes_total":sum(n["node_type"]=="axis" for n in nodes),
       "numeric_values_total":sum(n["node_type"]=="value" for n in nodes),"vector_paths_total":len(paths),
       "raster_regions_total":len(images),"curve_evidence_state":"unclassified_vector_path_candidates",
       "topology_state":"pump_performance_inventory"},
      warnings=["точки растровой характеристики не превращаются в вымышленные числовые значения",
                "векторные пути являются кандидатами кривых; сетка, рамка и кривая без калибровки осей не смешиваются с числовым рядом"])


def _dispatch(page,pdf,block_id,profile_hint=None,subtype_hint=None):
    classified_profile,classified_subtype=classify_water_profile(page.get_text(),block_id=block_id)
    profile=profile_hint or classified_profile;subtype=subtype_hint or classified_subtype or "вариант исходного комплекта"
    if profile==PROFILE_PLAN:graph=_build_plan(page,pdf,block_id,subtype)
    elif profile in (PROFILE_WATER,PROFILE_SEWER,PROFILE_FIRE):graph=_build_axonometry(page,pdf,block_id,subtype,profile)
    elif profile==PROFILE_PRINCIPLE:graph=_build_principle(page,pdf,block_id,subtype)
    elif profile==PROFILE_CONTROL:graph=_build_control(page,pdf,block_id,subtype)
    elif profile in (PROFILE_DETAIL,PROFILE_DRAIN):graph=_build_detail(page,pdf,block_id,subtype,profile)
    elif profile==PROFILE_EXTERNAL:graph=_build_external(page,pdf,block_id,subtype)
    elif profile==PROFILE_EQUIPMENT:graph=_build_equipment(page,pdf,block_id,subtype)
    elif profile==PROFILE_CHART:graph=_build_chart(page,pdf,block_id,subtype)
    else:return None
    text=page.get_text().strip();graph["validation"].update({
      "pdf_text_characters":len(text),"pdf_words_total":len(page.get_text("words")),
      "source_layer_state":"text_available" if text else "no_pdf_text_layer"})
    fact_counts=collections.Counter(node["node_type"] for node in graph.get("nodes") or [])
    graph["validation"]["engineering_fact_counts"]={
      kind:fact_counts.get(kind,0) for kind in (
        "system","riser","floor","elevation","diameter","slope","pipe_material","continuation_reference",
        "pump","water_meter","drain_inlet","valve","fire_cabinet","sprinkler","sanitary_fixture",
        "pipe_insulation","support_requirement","penetration_requirement","hydraulic_parameter")
      if fact_counts.get(kind,0)}
    if not text:
        graph.setdefault("warnings",[]).append(
          "Внутри вырезанного PDF нет доступного текстового слоя: состав ограничен векторной и растровой геометрией")
    return graph


def build_water_graph(pdf_path:Path,*,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        with fitz.open(str(pdf_path)) as doc:
            if doc.page_count!=1:return None
            graph=_dispatch(doc[0],Path(pdf_path),block_id,profile_hint,subtype_hint)
        fitz.TOOLS.store_shrink(100);return graph
    except Exception:return None


def build_water_graph_from_source(pdf_path:Path,*,page_index:int,bbox_norm,polygon_norm=None,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        source=fitz.open(str(pdf_path));cropped=None
        try:
            if not bbox_norm or not (0<=page_index<source.page_count):return None
            sp=source[page_index];w,h=sp.rect.width,sp.rect.height
            crop=fitz.Rect(float(bbox_norm[0])*w,float(bbox_norm[1])*h,float(bbox_norm[2])*w,float(bbox_norm[3])*h)&sp.rect
            if crop.is_empty:return None
            unrotated=crop*sp.derotation_matrix;unrotated.normalize();offset=sp.cropbox_position
            unrotated=fitz.Rect(unrotated.x0+offset.x,unrotated.y0+offset.y,unrotated.x1+offset.x,unrotated.y1+offset.y)
            cropped=fitz.open();cropped.insert_pdf(source,from_page=page_index,to_page=page_index);target=cropped[0]
            if polygon_norm:
                inverse=~sp.transformation_matrix
                points=[tuple(fitz.Point(float(x)*w,float(y)*h)*sp.derotation_matrix*inverse) for x,y in polygon_norm]
                _clip_copied_page(target,points)
            target.set_cropbox(unrotated);return _dispatch(target,Path(pdf_path),block_id,profile_hint,subtype_hint)
        finally:
            if cropped is not None:cropped.close()
            source.close();fitz.TOOLS.store_shrink(100)
    except Exception:return None


_SECONDARY_FACT_PATTERNS=(
 *_ENGINEERING_LINE_PATTERNS,
 ("equipment",re.compile(r"\b(?:насос\w*|КНС|ДНС|гидрант\w*|воронк\w*|трап\w*|ШПК[\w./-]*|"
   r"сч[её]тчик\w*|водомер\w*|коллектор\w*|редуктор\w*|клапан\w*|задвижк\w*|вентил\w*|"
   r"кран\w*|фильтр\w*|манометр\w*|спринклер\w*|оросител\w*|пескоуловител\w*)\b",re.I)),
 ("insulation",re.compile(r"\b(?:теплоизоляц\w*|изоляц\w*\s+труб\w*)\b",re.I)),
 ("support",re.compile(r"\b(?:неподвижн\w*\s+опор\w*|креплен\w*\s+труб\w*|хомут\w*|кронштейн\w*|шпильк\w*)\b",re.I)),
 ("penetration",re.compile(r"\b(?:проходк\w*|гильз\w*|заделк\w*|огнезащ\w*|противопожарн\w*\s+муфт\w*)\b",re.I)),
)


def _fact_key(value):
    value=str(value or "").casefold().replace("ё","е").replace("×","х").replace("x","х").replace(",",".")
    return re.sub(r"[^0-9a-zа-я+№.\-]+","",value)


def add_water_secondary_facts(graph,text,*,source="описание исходного реестра"):
    """Добавить OCR/описательные подсказки, не выдавая их за геометрию PDF."""
    if not graph or not (text or "").strip():return graph
    existing="\n".join(node.get("label","") for node in graph.get("nodes") or [])
    existing_key=_fact_key(existing);facts=[];seen=set()
    for line in str(text).splitlines():
        for kind,pattern in _SECONDARY_FACT_PATTERNS:
            for match in pattern.finditer(line):
                if kind=="system" and re.search(r"\bРД\b",line,re.I) and not re.search(
                  r"систем|сет|стояк|\bСт\s*\.|\bОп\s*\.|\bПод\s*\.|выпуск",line,re.I):
                    continue
                if kind=="elevation":
                    value=match.group().replace(",",".")
                    if not value.startswith(("+","-")) and re.fullmatch(r"0\.0(?!00)\d+",value):continue
                label=re.sub(r"\s+"," ",match.group()).strip(" ;,")
                key=(kind,_fact_key(label))
                if not key[1] or key in seen or key[1] in existing_key:continue
                seen.add(key);facts.append({"fact_type":kind,"label":label,"evidence_state":"secondary_description_only"})
        if _CONTINUATION_RE.search(line):
            label=re.sub(r"\s+"," ",line).strip()[:500];key=("continuation_reference",_fact_key(label))
            if key[1] and key not in seen and key[1] not in existing_key:
                seen.add(key);facts.append({"fact_type":"continuation_reference","label":label,
                  "evidence_state":"secondary_description_only"})
    graph["secondary_facts"]={"source":source,"facts":facts,
      "warning":"эти сведения не имеют координат и не участвуют в построении CAD-рёбер"}
    graph["validation"]["secondary_facts_total"]=len(facts)
    return graph


def evaluate_water_gate(graph):
    if not graph:return {"use":False,"complete":False,"readiness":"none","mode":"none","reasons":["граф не построен"],"complete_reasons":[],"metrics":{}}
    v=graph["validation"];p=graph["profile_id"];reasons=[];complete=[]
    evidence=(v.get("nodes_total",0)+v.get("networks_total",0)+v.get("containers_total",0)+v.get("raster_regions_total",0)
              +v.get("route_segments_total",0)+v.get("line_segments_total",0)+v.get("geometry_parts_total",0))
    if evidence<1:reasons.append("нет текстовой, векторной или растровой структуры")
    if p==PROFILE_PLAN:
        if v.get("route_segments_total",0)<20 and v.get("raster_regions_total",0)<1:complete.append("не извлечены трассы плана")
        depth="semantic_hierarchy" if v.get("systems_total",0)+v.get("equipment_total",0)>0 else "geometry_inventory"
    elif p in (PROFILE_WATER,PROFILE_SEWER,PROFILE_FIRE):
        if v.get("route_segments_total",0)<20 and v.get("raster_regions_total",0)<1:complete.append("нет геометрии аксонометрии")
        if v.get("unique_systems_total",0)<1 and v.get("route_segments_total",0)<20 and v.get("raster_regions_total",0)<1:
            complete.append("не найдены системы или геометрия схемы")
        depth="engineering_graph" if v.get("cad_confirmed_edges_total",0)>0 else (
          "semantic_hierarchy" if v.get("unique_systems_total",0)>0 else "geometry_inventory")
    elif p==PROFILE_PRINCIPLE:
        if v.get("apparatus_total",0)<2 and v.get("route_segments_total",0)<20 and v.get("raster_regions_total",0)<1:
            complete.append("не найден состав или геометрия узла")
        depth="engineering_graph" if v.get("confirmed_pairs_total",0)>0 else (
          "spatial_inventory" if v.get("apparatus_total",0)>1 else "geometry_inventory")
    elif p==PROFILE_CONTROL:
        if v.get("route_segments_total",0)<10:complete.append("не извлечены цепи управления")
        depth="engineering_graph" if v.get("confirmed_pairs_total",0)>0 else "semantic_hierarchy"
    elif p in (PROFILE_DETAIL,PROFILE_DRAIN):
        if v.get("parts_total",0)<2 and v.get("line_segments_total",0)<10 and v.get("raster_regions_total",0)<1:
            complete.append("не найден состав или геометрия узла")
        depth="physical_hierarchy"
    elif p==PROFILE_EXTERNAL:
        if v.get("line_segments_total",0)<2 and v.get("raster_regions_total",0)<1:complete.append("нет геометрии профиля или разреза")
        depth="engineering_graph" if v.get("profile_edges_total",0)>0 else "physical_hierarchy"
    elif p==PROFILE_EQUIPMENT:
        if v.get("geometry_parts_total",0)+v.get("raster_regions_total",0)<1:complete.append("не выделено оборудование")
        depth="physical_hierarchy"
    else:
        if v.get("vector_paths_total",0)+v.get("raster_regions_total",0)<1:complete.append("не найден график")
        depth="analytical_geometry" if v.get("vector_paths_total",0)>0 else "raster_inventory"
    if v.get("source_layer_state")=="no_pdf_text_layer":
        complete.append("нет текстового слоя PDF: полнота марок, параметров и подписей не может быть подтверждена без OCR")
    v["description_depth"]=depth;status="complete" if not complete else "topology_partial"
    graph["readiness"]={"status":status,"complete":not complete,"reasons":complete,"description_depth":depth,
      "vectograph_level":depth=="engineering_graph"}
    return {"use":not reasons,"complete":not complete,"readiness":status,"mode":p,"reasons":reasons,
      "complete_reasons":complete,"warnings":graph.get("warnings",[]),"metrics":v}


_PROFILE_RU={PROFILE_PLAN:"План систем ВК",PROFILE_WATER:"Аксонометрическая схема водоснабжения",
 PROFILE_SEWER:"Аксонометрическая схема канализации и водоотведения",PROFILE_FIRE:"Схема противопожарного водопровода и АПТ",
 PROFILE_PRINCIPLE:"Принципиальная схема узла ВК",PROFILE_CONTROL:"Схема управления пожаротушением",
 PROFILE_DETAIL:"Монтажный узел ВК",PROFILE_DRAIN:"Узел водосточной воронки",
 PROFILE_EXTERNAL:"Профиль или разрез наружных сетей",PROFILE_EQUIPMENT:"Чертёж насосного оборудования",
 PROFILE_CHART:"Рабочая характеристика насосного оборудования"}
_SUBTYPE_RU={
 "floor_plan":"план этажа","water_supply":"схема водоснабжения",
 "sewer_risers":"стояки канализации","fire_risers":"стояки противопожарного водопровода",
 "meter_node":"водомерный узел","installation":"монтажный узел",
 "combined_floor_networks":"внутренние сети этажа","roof_drainage_plan":"стояки водоотведения на кровле",
 "fire_floor_multi_zone":"системы пожаротушения этажа","technical_space_sewer":"канализация технического пространства",
 "roof_funnel_plan":"водосточные воронки кровли","parking_fire_water_plan":"противопожарный водопровод автостоянки",
 "site_drain_inlets":"расположение воронок и трапов","external_water_general_plan":"наружный водопровод на генеральном плане",
 "pump_station_plan":"план насосных станций","meter_room_plan":"помещение водомерного узла",
 "powder_fire_equipment_plan":"оборудование порошкового пожаротушения","external_water_site_fragment":"ввод наружного водопровода",
 "building_water_supply":"водоснабжение здания","multi_system_water_supply":"системы В11, В1.3 и Т3.3",
 "mixed_water_and_sewer":"совмещённые сети водоснабжения и канализации","sewer_vent_valves":"водоотведение с вентиляционными клапанами",
 "domestic_sewer_risers":"стояки хозяйственно-бытовой канализации","pumped_sewer_k1":"напорное водоотведение К1",
 "pumped_sewer_k13":"напорное водоотведение К13","surface_drainage_trays":"водоотведение от лотков стилобата",
 "storm_sewer":"ливневая канализация","drain_riser_supports":"стояк водостока и опоры",
 "apt_water_systems":"системы автоматического пожаротушения","fire_multizone_risers":"многоэтажный противопожарный водопровод",
 "fire_two_zones":"противопожарный водопровод двух зон","apt_floor_groups":"АПТ по группам этажей",
 "fire_riser_cabinets":"пожарный стояк и поэтажные шкафы","reducer_meter_node":"редукционно-водомерный узел",
 "meter_insert":"водомерная вставка","meter_unit":"водомерный узел","drain_pump_piping":"обвязка дренажных насосов",
 "apt_hydraulic_systems":"гидравлические системы АПТ","external_water_nodes":"узлы наружной водопроводной сети",
 "water_distribution_unit":"коллекторный узел водоснабжения","powder_control_hierarchy":"структура управления модулем пожаротушения",
 "powder_equipment_connection":"подключение оборудования порошкового тушения",
 "riser_compensators":"крепление стояка с компенсаторами","sml_supports_and_passages":"крепления SML и проходки",
 "steel_pipe_passages":"проход стальных труб","sewer_passages":"проходы канализации",
 "trap_and_riser_support":"трап и крепление стояка","support_ring_and_fire_collar":"опорное кольцо и противопожарная муфта",
 "fire_cabinets":"пожарные шкафы и промывочный кран","diaphragm_and_sprinkler":"диафрагма и спринклер",
 "irrigation_tap":"поливочный кран","hydrant_well_ladder":"лестница и гидрант в колодце","chamber_sump":"приямок камеры",
 "sewer_wall_outlet":"выпуск канализации через стену","hydrant_well_details":"детали колодца с гидрантом",
 "sidewalk_drain_funnel":"воронка в зоне тротуара","slab_drain_funnel":"воронка в железобетонной плите",
 "surface_drain_funnel":"воронка поверхностного водостока","roof_funnel":"кровельная воронка","pfo_roof_funnel":"кровельная воронка PFO",
 "geological_pipe_section":"геологический разрез с трубопроводом","valve_sections":"разрезы трубопроводной арматуры",
 "chamber_sections":"разрезы водопроводных камер","pipe_construction_sections":"конструктивные разрезы прокладки труб",
 "storm_sewer_longitudinal_profile":"продольный профиль дождевой канализации","pump_station_sections":"разрезы насосной станции",
 "fire_pump_sections":"разрезы противопожарной насосной","drain_pump_curves":"характеристики дренажного насоса",
 "pump_curves":"характеристики насосного оборудования","submersible_pump_connection":"погружной насос и подключение",
 "submersible_pump_views":"погружной насос в двух проекциях","fire_pump_unit":"насосная установка пожаротушения",
 "modular_fire_pump_unit":"модульная насосная установка"}
_NODE_RU={"system":"система","riser":"стояк","fire_cabinet":"пожарный шкаф","pump":"насос",
 "drain_inlet":"воронка или трап","valve":"арматура","sprinkler":"спринклер или ороситель","diameter":"диаметр",
 "elevation":"высотная отметка","slope":"уклон","floor":"этаж","room_label":"помещение или зона",
 "raster_region":"крупная растровая область","raster_mosaic":"растровая мозаика листа","water_meter":"счётчик воды","filter":"фильтр",
 "pressure_reducer":"редуктор давления","collector":"коллектор","instrument":"измерительный прибор",
 "pressure_damper":"гаситель напора","position":"позиция","control_module":"модуль управления","detector":"извещатель",
 "actuator":"исполнительное устройство","terminal":"клемма","cable":"кабель","power":"питание",
 "sanitary_fixture":"санитарный прибор","pipe_insulation":"изоляция трубопровода",
 "support_requirement":"опора или требование крепления","penetration_requirement":"проходка или заделка",
 "hydraulic_parameter":"гидравлический параметр",
 "equipment":"оборудование и арматура","insulation":"изоляция","support":"опоры и крепления",
 "penetration":"проходки и заделки",
 "dimension":"размер","fire_seal":"противопожарная заделка","pipe_part":"труба или патрубок","fastener":"крепёж",
 "support_part":"опора или кронштейн","well_equipment":"оборудование колодца","construction_layer":"слой конструкции",
 "assembly_part":"элемент узла","well_or_chamber":"колодец или камера","picket":"пикет","distance":"расстояние",
 "ground_level":"уровень земли","pipe_material":"материал трубы","continuation_reference":"ссылка на продолжение",
 "model":"модель","port":"патрубок",
 "electrical":"электрические параметры","geometry_part":"геометрически выделенная часть","axis":"ось графика","value":"числовое значение"}
_DEPTH_RU={"engineering_graph":"инженерный граф с CAD-подтверждёнными связями",
 "semantic_hierarchy":"предметная иерархия с частью пространственных связей",
 "physical_hierarchy":"физическая структура видов и составных частей","geometry_inventory":"геометрический граф без читаемых марок",
 "spatial_inventory":"состав без подтверждённого порядка соединений","analytical_geometry":"аналитическая структура с векторными путями",
 "raster_inventory":"параметры и растровая область без ложной векторизации"}
_STATE_RU={"cad_endpoint_component":"линии соединены конечными точками CAD","same_cad_component":"общая непрерывная CAD-трасса",
 "nearest_geometry":"пространственная привязка","mixed_evidence":"смешанная доказательность связей",
 "confirmed_pair":"подтверждённая пара","multi_apparatus_path":"общая цепь нескольких аппаратов",
 "spatial_inventory":"попарные связи не подтверждены","path_confirmed":"соединение подтверждено линией",
 "multi_terminal_path":"общая многоточечная цепь","vector_curve_geometry":"векторная геометрия пути",
 "embedded_raster":"данные присутствуют только во встроенном изображении"}
_NETWORK_RU={"water_or_sewer_route":"трасса ВК","logical_water_system":"система",
 "water_circuit":"гидравлический контур","water_assembly":"состав узла","control_circuit":"цепь управления",
 "performance_path":"путь рабочего графика","raster_sheet_region":"растровая область листа"}


def _summary_ru(graph):
    v=graph["validation"];p=graph["profile_id"]
    if p==PROFILE_PLAN:return f"Участков трасс: {v.get('route_segments_total',0)}; связных групп: {v.get('route_components_total',0)}; систем: {v.get('unique_systems_total',0)}; оборудования: {v.get('equipment_total',0)}; диаметров: {v.get('diameters_total',0)}; материалов: {v.get('materials_total',0)}."
    if p in (PROFILE_WATER,PROFILE_SEWER,PROFILE_FIRE):return f"Систем: {v.get('unique_systems_total',0)}; стояков: {v.get('unique_risers_total',0)}; этажей: {v.get('unique_floors_total',0)}; уникальных отметок: {v.get('unique_elevations_total',0)}; оборудования: {v.get('equipment_total',0)}; диаметров: {v.get('diameters_total',0)}; материалов: {v.get('materials_total',0)}; CAD-рёбер: {v.get('cad_confirmed_edges_total',0)}."
    if p==PROFILE_PRINCIPLE:return f"Типов аппаратов и позиций: {v.get('unique_apparatus_total',0)}; отметок: {v.get('elevations_total',0)}; диаметров: {v.get('diameters_total',0)}; материалов: {v.get('materials_total',0)}; подтверждённых пар: {v.get('confirmed_pairs_total',0)}; общих цепей: {v.get('multi_apparatus_networks',0)}."
    if p==PROFILE_CONTROL:return f"Модулей: {v.get('modules_total',0)}; полевых устройств: {v.get('field_devices_total',0)}; клемм: {v.get('terminals_total',0)}; подтверждённых пар: {v.get('confirmed_pairs_total',0)}."
    if p in (PROFILE_DETAIL,PROFILE_DRAIN):return f"Видов: {v.get('views_total',0)}; частей: {v.get('parts_total',0)}; размеров: {v.get('dimensions_total',0)}; слоёв конструкции: {v.get('layers_total',0)}."
    if p==PROFILE_EXTERNAL:return f"Видов: {v.get('views_total',0)}; колодцев и камер: {v.get('wells_total',0)}; отметок: {v.get('elevations_total',0)}; участков линий: {v.get('line_segments_total',0)}."
    if p==PROFILE_EQUIPMENT:return f"Моделей: {v.get('models_total',0)}; размеров: {v.get('dimensions_total',0)}; физических групп: {v.get('physical_groups_total',0)}; растровых областей: {v.get('raster_regions_total',0)}."
    return f"Векторных путей: {v.get('vector_paths_total',0)}; осей: {v.get('axes_total',0)}; числовых значений: {v.get('numeric_values_total',0)}; растровых областей: {v.get('raster_regions_total',0)}."


def _tree_ru(graph):
    p=graph["profile_id"];v=graph["validation"];nodes=graph.get("nodes") or [];by=collections.defaultdict(list)
    for node in nodes:by[node["node_type"]].append(node["label"])
    unique=lambda kind:list(dict.fromkeys(by.get(kind,[])))
    if p==PROFILE_PLAN:return ["план ВК",f"├── системы: {', '.join(unique('system')[:12]) or 'марки не прочитаны'}",
      f"├── стояки: {len(unique('riser'))}",f"├── оборудование: {len(by.get('pump',[]))+len(by.get('drain_inlet',[]))+len(by.get('fire_cabinet',[]))}",
      f"└── связные группы трасс: {v.get('route_components_total',0)}"]
    if p in (PROFILE_WATER,PROFILE_SEWER,PROFILE_FIRE):return ["системы ВК",f"├── системы: {', '.join(unique('system')[:20]) or 'не прочитаны'}",
      f"├── стояки: {len(unique('riser'))}",f"├── этажи: {v.get('unique_floors_total',0)}; отметки: {v.get('unique_elevations_total',0)}",
      f"└── приборы и арматура: {v.get('equipment_total',0)}"]
    if p in (PROFILE_PRINCIPLE,PROFILE_CONTROL):return ["функциональная схема",f"├── узлы: {v.get('nodes_total',0)}",
      f"├── сети: {v.get('networks_total',0)}",f"└── подтверждённые рёбра: {v.get('confirmed_pairs_total',0)}"]
    if p in (PROFILE_DETAIL,PROFILE_DRAIN,PROFILE_EXTERNAL,PROFILE_EQUIPMENT):
        tree=["физическая структура блока"];containers=graph.get("containers") or []
        for i,c in enumerate(containers[:20]):tree.append(("└──" if i==min(len(containers),20)-1 else "├──")+f" { _SUBTYPE_RU.get(c.get('label'),c.get('label') or 'группа')}: элементов {len(c.get('member_ids') or [])}")
        if not containers:tree.append(f"└── элементов: {v.get('nodes_total',0)}")
        return tree
    return ["рабочая характеристика",f"├── модель: {', '.join(unique('model')[:5]) or 'не прочитана'}",
      f"├── оси: {', '.join(unique('axis')[:10]) or 'не выделены'}",f"├── векторные пути: {v.get('vector_paths_total',0)}",
      f"└── растровые области: {v.get('raster_regions_total',0)}"]


def render_water_markdown(graph):
    v=graph["validation"];p=graph["profile_id"];subtype=v.get("subtype");readiness=graph.get("readiness") or {}
    lines=[f"# Эталонная текстовая разметка ВК: {_PROFILE_RU.get(p,'Графический блок ВК')}","",
      f"**Назначение блока:** {_SUBTYPE_RU.get(subtype,subtype or 'не определено')}",f"**Источник:** {graph['source']['pdf_file']}",
      "**Способ разбора:** детерминированно из текстового, векторного и растрового слоёв PDF; неподтверждённые связи не добавляются.","",
      "## 1. Краткий результат","",_summary_ru(graph),"",f"**Уровень описания:** {_DEPTH_RU.get(v.get('description_depth'),'структура блока')}.","",
      f"**Текстовый слой PDF:** {'доступен' if v.get('source_layer_state')=='text_available' else 'отсутствует; текстовые марки не могут быть восстановлены без OCR' }.","",
      "### Инженерное дерево","","```text",*_tree_ru(graph),"```","","## 2. Состав блока",""]
    groups=collections.defaultdict(list)
    for node in graph.get("nodes") or []:groups[node["node_type"]].append(node["label"])
    if groups:
        for kind,labels in sorted(groups.items(),key=lambda item:_NODE_RU.get(item[0],item[0])):
            preview=", ".join(labels[:24]);suffix=f" … (ещё {len(labels)-24})" if len(labels)>24 else ""
            lines.append(f"- **{_NODE_RU.get(kind,'элемент')} — {len(labels)}:** {preview}{suffix}")
    else:lines.append("- Текстовые обозначения отсутствуют; сохранена геометрическая или растровая структура.")
    if graph.get("containers"):
        lines += ["","### Виды и физические группы",""]
        for i,c in enumerate(graph["containers"][:30],1):lines.append(f"- **Группа {i}:** {_SUBTYPE_RU.get(c.get('label'),c.get('label') or 'без подписи')} — элементов {len(c.get('member_ids') or [])}.")
    lines += ["","## 3. Системы, связи и трассы",""]
    networks=graph.get("networks") or []
    if networks:
        shown=networks
        if all(n.get("network_type")=="water_or_sewer_route" for n in networks):
            significant=[n for n in networks if n.get("endpoint_ids") or n.get("branch_points")]
            shown=sorted(significant or networks,key=lambda n:(len(n.get("endpoint_ids") or []),n.get("branch_points",0),n.get("length",0)),reverse=True)[:20]
            lines.append("Ниже перечислены основные связные трассы; мелкие фрагменты учтены в общей статистике.")
        else:shown=networks[:40]
        for i,n in enumerate(shown,1):
            label=n.get("label") or "";label=_SUBTYPE_RU.get(label,label)
            state=_STATE_RU.get(n.get("path_state"),"состояние зафиксировано")
            details=[f"обозначений: {len(n.get('endpoint_ids') or [])}"]
            if n.get("segment_ids") is not None:details.append(f"участков: {len(n.get('segment_ids') or [])}")
            if n.get("branch_points") is not None:details.append(f"ветвлений: {n.get('branch_points',0)}")
            lines.append(f"- **{_NETWORK_RU.get(n.get('network_type'),'сеть')} {i}{f' «{label}»' if label else ''}:** {'; '.join(details)}. Доказательность: {state}.")
        if len(networks)>len(shown):lines.append(f"- Остальные учтённые группы: {len(networks)-len(shown)}.")
    else:lines.append("- Отдельные сети не сформированы: блок описывается как физическая структура.")
    edges=graph.get("edges") or []
    if edges:
        labels={n["id"]:n["label"] for n in graph.get("nodes") or []};grouped=collections.Counter()
        for edge in edges:grouped[(labels.get(edge["from"],"элемент"),labels.get(edge["to"],"элемент"),_STATE_RU.get(edge.get("edge_state"),"структурная связь"))]+=1
        lines += ["","### Явные связи",""]
        for (source,target,state),count in grouped.most_common(60):lines.append(f"- {source} → {target}: {state}{f'; повторений {count}' if count>1 else ''}.")
    secondary=(graph.get("secondary_facts") or {}).get("facts") or []
    if secondary:
        lines += ["","## 4. Дополнительные сведения из вторичного описания","",
          "Эти подписи сохранены, но не имеют координат в PDF и не используются как доказательство соединений."]
        grouped_secondary=collections.defaultdict(list)
        for fact in secondary:grouped_secondary[fact["fact_type"]].append(fact["label"])
        for kind,labels in grouped_secondary.items():
            unique_labels=list(dict.fromkeys(labels));preview=", ".join(unique_labels[:30])
            lines.append(f"- **{_NODE_RU.get(kind,kind)}:** {preview}{' …' if len(unique_labels)>30 else ''}")
    lines += ["",f"## {'5' if secondary else '4'}. Полнота и ограничения",""]
    lines.append("Состав и доступная геометрия полностью описаны по правилам данного типа блока." if readiness.get("complete") else "Описание частичное; недостающие признаки перечислены ниже.")
    if readiness.get("vectograph_level"):lines.append("- Часть рёбер достигла уровня Вектографа: связь подтверждена общей CAD-трассой.")
    elif v.get("description_depth")=="physical_hierarchy":lines.append("- Для физического чертежа полнота определяется видами и составными частями, а не потоковой цепочкой.")
    else:lines.append("- Неподтверждённый порядок соединений не домысливается.")
    lines += [f"- {reason}" for reason in readiness.get("reasons") or []]
    lines += [f"- {warning}" for warning in graph.get("warnings") or []]
    return "\n".join(lines)+"\n"
