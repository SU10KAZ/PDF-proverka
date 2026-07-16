"""Детерминированные графы ЭОМ вне расчётных однолинейных схем Вектографа."""
from __future__ import annotations

import collections
import math
import re
from pathlib import Path
from typing import Optional

from .hvac_geometry import (
    _axes,_base,_bbox,_bbox_norm,_bind_nearest_axes,_center,_clip_copied_page,_components,
    _geometry_parts,_lines,_node,_point,_preferred_parent,_segments,_token_nodes,_unique,_views,
    _assign_nodes_to_views,_attach,
)


PROFILE_SINGLELINE="electrical_singleline"
PROFILE_PANEL="panel_circuit_scheme"
PROFILE_DISTRIBUTION="electrical_distribution_plan"
PROFILE_LIGHTING="lighting_plan"
PROFILE_CABLE="cable_route_plan"
PROFILE_LIGHTNING="lightning_grounding_plan"
PROFILE_BONDING="equipotential_scheme"
PROFILE_OZDS="ozds_topology"
PROFILE_DETAIL="electrical_installation_detail"
PROFILE_EQUIPMENT="electrical_equipment_drawing"
PROFILE_ILLUMINANCE="illuminance_calculation"
PROFILE_SITE="electrical_site_overview"
ALL_ELECTRICAL_PROFILES=(PROFILE_SINGLELINE,PROFILE_PANEL,PROFILE_DISTRIBUTION,PROFILE_LIGHTING,
 PROFILE_CABLE,PROFILE_LIGHTNING,PROFILE_BONDING,PROFILE_OZDS,PROFILE_DETAIL,PROFILE_EQUIPMENT,
 PROFILE_ILLUMINANCE,PROFILE_SITE)


def classify_electrical_profile(text:str):
    u=re.sub(r"\s+"," ",text or "").upper()
    if "ОЗДС" in u or "ДЕРАТИЗАЦ" in u:return PROFILE_OZDS
    if "ОДНОЛИНЕЙН" in u or ("СХЕМА" in u and "ВРУ" in u and "РУ=" in u):return PROFILE_SINGLELINE
    if "УРАВНИВАН" in u and "СХЕМ" in u:return PROFILE_BONDING
    if "МОЛНИЕЗАЩ" in u or ("ЗАЗЕМЛ" in u and "ПЛАН" in u):return PROFILE_LIGHTNING
    if "СВЕТОТЕХНИЧ" in u or "РАСЧЕТ ОСВЕЩЕННОСТ" in u:return PROFILE_ILLUMINANCE
    if ("УЗЛ" in u and any(x in u for x in ("ЗАЗЕМЛ", "МОЛНИЕПРИЕМ", "МОЛНИЕПРИЁМ", "КАБЕЛЬН", "ПРОВОД", "ЭЛЕКТРИЧ"))) or any(x in u for x in ("УЗЕЛ ЗАЗЕМЛ", "СОЕДИНЕНИЕ ЗАЗЕМЛ", "СВАРНОЕ СОЕДИНЕНИЕ", "УЗЕЛ КРЕПЛЕНИЯ",
        "УЗЕЛ ПРОХОДА", "МОНТАЖНЫЙ УЗЕЛ", "ДЕТАЛЬ УСТАНОВКИ")):return PROFILE_DETAIL
    if any(x in u for x in ("ОБЩИЙ ВИД ВРУ", "ЧЕРТЕЖ ШКАФ", "ЧЕРТЁЖ ШКАФ", "ЧЕРТЕЖ УСТРОЙСТВА",
        "ЧЕРТЁЖ УСТРОЙСТВА", "ГАБАРИТНЫЙ ЧЕРТЕЖ", "ГАБАРИТНЫЙ ЧЕРТЁЖ")):return PROFILE_EQUIPMENT
    if "СХЕМА ЩИТ" in u or "ПРИНЦИПИАЛЬНАЯ СХЕМА УПРАВЛЕНИЯ" in u or len(re.findall(r"\bQF\s*\d",u))>=3:return PROFILE_PANEL
    if "ФРАГМЕНТ ПЛАНА ЭЛЕКТРОЩИТОВОЙ" in u:return PROFILE_DISTRIBUTION
    if "ОСВЕТ" in u and ("ПЛАН" in u or "ГРУППОВЫХ СЕТ" in u):return PROFILE_LIGHTING
    if any(x in u for x in ("КАБЕЛЬНЫХ ЛОТКОВ","ОГНЕЗАЩИТНОГО КОРОБА","КАБЕЛЬНЫХ ТРАСС")):return PROFILE_CABLE
    if any(x in u for x in ("КОМПЕНСАТОР ВЫСОТЫ","ДВЕРЬ 016","ОБОГРЕВ ВОДОСТ","СВАРНЫЕ СОЕДИНЕНИЯ")):return PROFILE_DETAIL
    if any(x in u for x in ("УСТРОЙСТВО ЭТАЖНОЕ","ОБЩИЙ ВИД ВРУ","ЧЕРТЕЖ ШКАФ")):return PROFILE_EQUIPMENT
    if "СХЕМА РАСПОЛОЖЕНИЯ" in u and ("КОРПУС" in u or "ЗДАН" in u):return PROFILE_SITE
    if "ПЛАН" in u and any(x in u for x in ("ЭЛЕКТРОСНАБ","ЭЛЕКТРООБОРУД","ГРУППОВЫЕ СЕТИ","РОЗЕТ")):return PROFILE_DISTRIBUTION
    if "ПЛАН" in u and "ЭОМ" in u and any(x in u for x in ("ИНЖЕНЕРН", "ТРАСС", "ЩИТ")):return PROFILE_DISTRIBUTION
    return None


_PANEL_RE=re.compile(r"\b(?:ВРУ|ГРЩ|ЩР|ЩО|ЩАО|ЩЭ|УЭРВ|ШР|ШУ|ЯУО|ГЗШ|ШДУ|ЩК|ЩМ)[A-ZА-Яа-я0-9._/\-]*",re.I)
_DEVICE_RE=re.compile(r"(?:\b(?:QF|QS|QD|QFА|FU|KM|SF)\s*[A-ZА-Яа-я0-9._/\-]*|\b(?:КМ|АВ|УЗО|АВДТ)\s*[-]?\s*\d[A-ZА-Яа-я0-9._/\-]*)",re.I)
_CIRCUIT_RE=re.compile(r"(?:\bГр\.?\s*\d+(?:\.\d+)*|\b(?:К|K)\d+(?:\.\d+){1,3}(?:-\d+)?|\bЛиния\s*\d+|\bL[123]\b|\bN\b|\bPE\b|\bPEN\b)",re.I)
_CABLE_RE=re.compile(r"\b(?:ВВГ|ППГ|ПВ|ПВС|КГ|NYM|N2XH|КПС|КВВГ|FRLS|нг(?:\([АA]\))?[-А-ЯA-Z]*)[^\n;,]{0,70}",re.I)
_ELEV_RE=re.compile(r"(?<!\d)[+\-]?\d{1,3}[,.]\d{3}(?!\d)")
_SIZE_RE=re.compile(r"(?:\b\d{1,4}\s*[xх×]\s*\d{1,4}(?:\s*[xх×]\s*\d{1,4})?\b|\b\d+(?:[,.]\d+)?\s*мм[²2]?\b)",re.I)
_POWER_RE=re.compile(r"\b\d+(?:[,.]\d+)?\s*(?:кВт|Вт|А|В|кВА|лк)\b",re.I)
_FIRE_RE=re.compile(r"\b(?:E\d+|EI\s*\d+|EIS\s*\d+|FRLS|FRHF)\b",re.I)


def _line_fact_nodes(page,extra=()):
    patterns=(("panel",_PANEL_RE),("protective_device",_DEVICE_RE),("circuit",_CIRCUIT_RE),
      ("cable",_CABLE_RE),("elevation",_ELEV_RE),("size",_SIZE_RE),("electrical_value",_POWER_RE),
      ("fire_rating",_FIRE_RE),("equipment",re.compile(r"\b(?:светильник\w*|розетк\w*|выключател\w*|электровывод\w*|сч[её]тчик\w*|насос\w*|вентилятор\w*|клапан\w*)\b",re.I)),*extra)
    nodes=[]
    # `_lines` удобен для горизонтальных подписей, но планы ЭОМ часто печатают
    # номера групп вдоль трассы под 90°. Добавляем исходные PDF-spans с bbox,
    # чтобы вертикальные марки не исчезали из предметного слоя.
    source_lines=list(_lines(page))
    for block in page.get_text("dict").get("blocks",[]):
        for pdf_line in block.get("lines",[]):
            spans=pdf_line.get("spans",[])
            joined="".join(str(span.get("text") or "") for span in spans).strip()
            if joined and spans:
                boxes=[span.get("bbox") or (0,0,0,0) for span in spans]
                bbox=_bbox(page,(min(x[0] for x in boxes),min(x[1] for x in boxes),
                  max(x[2] for x in boxes),max(x[3] for x in boxes)))
                source_lines.append({"text":joined,"bbox":bbox,"center":_center(bbox)})
            for span in spans:
                text=str(span.get("text") or "").strip()
                if not text:continue
                bbox=_bbox(page,span.get("bbox") or (0,0,0,0))
                source_lines.append({"text":text,"bbox":bbox,"center":_center(bbox)})
    for line in source_lines:
        for kind,pattern in patterns:
            for match in pattern.finditer(line["text"]):
                label=re.sub(r"\s+"," ",match.group()).strip(" ;,")
                if kind=="circuit" and label.upper() in {"N","PE","PEN"} and len(line["text"])>40:continue
                item=dict(line);item["text"]=label
                nodes.append(_node(page,item,kind,len(nodes)+1,source_label=line["text"]))
    return _unique(nodes)


def _images(page,start=0):
    result=[]
    for info in page.get_image_info(xrefs=True):
        bbox=_bbox(page,info["bbox"]);w=bbox[2]-bbox[0];h=bbox[3]-bbox[1]
        if w<page.rect.width*.15 or h<page.rect.height*.1:continue
        item={"text":f"крупная растровая область {len(result)+1}","bbox":bbox,"center":_center(bbox)}
        result.append(_node(page,item,"raster_region",start+len(result)+1,field_state="raster_geometry_only"))
    if not result and page.get_images(full=True):
        bbox=(0.,0.,float(page.rect.width),float(page.rect.height));item={"text":"растровая мозаика листа","bbox":bbox,"center":_center(bbox)}
        result.append(_node(page,item,"raster_mosaic",start+1,field_state="raster_mosaic_only"))
    return result


def _route_geometry(page,*,vivid=False,tolerance=3.0):
    segments=_segments(page,vivid_only=vivid,min_length=2)
    if vivid and len(segments)<20:segments=_segments(page,vivid_only=False,min_length=2)
    if len(segments)>60000:segments=segments[:60000]
    return segments,_components(segments,tolerance=tolerance)


def _networks(nodes,components,kind,label):
    result=[]
    for component in components:
        members=[n for n in nodes if n.get("route_id")==component["id"]]
        if not members and not component.get("branch_points"):continue
        names=list(dict.fromkeys(n["label"] for n in members if n["node_type"] in
          ("panel","protective_device","circuit","fixture","socket","switch","equipment","ozds_device",
           "lightning_device","grounding_device","bonding_target","tray","cable")))
        result.append({"id":f"network-{len(result)+1}","network_type":kind,"label":" / ".join(names[:8]) or label,
          "endpoint_ids":[n["id"] for n in members],"source_route_id":component["id"],
          "path_state":"cad_endpoint_component",**{k:v for k,v in component.items() if k!="id"}})
    return result


def _build_plan(page,pdf,block_id,profile,subtype):
    extra=(
      ("fixture",re.compile(r"\b(?:светильник\w*|LED|БАП|табло\w*|указатель\w*|огонь\w*)\b",re.I)),
      ("socket",re.compile(r"\b(?:розетк\w*|электровывод\w*)\b",re.I)),
      ("switch",re.compile(r"\b(?:выключател\w*|переключател\w*|датчик\w*\s+движен)\b",re.I)),
      ("equipment",re.compile(r"\b(?:насос\w*|вентилятор\w*|лифт\w*|клапан\w*|нагревател\w*|оборудован\w*)\b",re.I)),
      ("room",re.compile(r"\b(?:электрощитов\w*|насосн\w*|коридор\w*|лестничн\w*|паркинг\w*|техпомещ\w*)\b",re.I)),
    )
    nodes=_line_fact_nodes(page,extra);axes=_axes(page);_bind_nearest_axes(nodes,axes)
    rooms=[n for n in nodes if n["node_type"]=="room"]
    for node in nodes:
        if node in rooms or not rooms:continue
        room=min(rooms,key=lambda r:math.hypot(node["x"]-r["x"],node["y"]-r["y"]))
        if math.hypot(node["x"]-room["x"],node["y"]-room["y"])<max(page.rect.width,page.rect.height)*.22:
            node["nearest_room_label"]=room["label"]
    segments,components=_route_geometry(page,vivid=True,tolerance=2.5);attached=_attach(nodes,segments,components,limit=28)
    images=_images(page,len(nodes));nodes=_unique(nodes+images);networks=_networks(nodes,components,
      "lighting_route" if profile==PROFILE_LIGHTING else ("cable_route" if profile==PROFILE_CABLE else "power_route"),subtype)
    graph=_base(page,pdf,block_id,profile,subtype,nodes=nodes,networks=networks,
      validation={"axes_total":len(axes),"panels_total":sum(n["node_type"]=="panel" for n in nodes),
       "circuits_total":sum(n["node_type"]=="circuit" for n in nodes),"cables_total":sum(n["node_type"]=="cable" for n in nodes),
       "fixtures_total":sum(n["node_type"]=="fixture" for n in nodes),"sockets_total":sum(n["node_type"]=="socket" for n in nodes),
       "switches_total":sum(n["node_type"]=="switch" for n in nodes),"equipment_total":sum(n["node_type"]=="equipment" for n in nodes),
       "rooms_total":len(rooms),"route_segments_total":len(segments),"route_components_total":len(components),
       "route_branches_total":sum(c["branch_points"] for c in components),"nodes_route_attached":attached,
       "raster_regions_total":len(images),"topology_state":"electrical_plan_routes"},
      warnings=["пересечение линий без общей конечной точки CAD не считается электрическим соединением",
                "помещение является ближайшей пространственной подписью, а не восстановленным полигоном"])
    graph["grid"]={"axes":axes};graph["route_segments"]=segments;return graph


def _build_panel(page,pdf,block_id,subtype):
    extra=(("meter",re.compile(r"\b(?:сч[её]тчик\w*|Меркурий|ПСЧ)\b",re.I)),
      ("bus",re.compile(r"\b(?:шина|шинопровод|PE|N|PEN)\b",re.I)),
      ("load",re.compile(r"\b(?:квартир\w*|освещен\w*|розеточн\w*|насос\w*|резерв)\b",re.I)))
    nodes=_line_fact_nodes(page,extra);segments,components=_route_geometry(page,tolerance=3.5);attached=_attach(nodes,segments,components,limit=24)
    major={"panel","protective_device","meter","bus","load"};networks=[];edges=[]
    for component in components:
        members=[n for n in nodes if n.get("route_id")==component["id"] and n["node_type"] in major]
        if len(members)<2:continue
        state="confirmed_pair" if len(members)==2 else "multi_device_bus";network={"id":f"network-{len(networks)+1}",
          "network_type":"panel_circuit","label":"цепь щита","endpoint_ids":[n["id"] for n in members],
          "source_route_id":component["id"],"path_state":state};networks.append(network)
        if len(members)==2:edges.append({"id":f"edge-{len(edges)+1}","network_id":network["id"],"from":members[0]["id"],
          "to":members[1]["id"],"edge_type":"electrical_connection","edge_state":"path_confirmed"})
    images=_images(page,len(nodes));nodes=_unique(nodes+images)
    graph=_base(page,pdf,block_id,PROFILE_PANEL,subtype,nodes=nodes,networks=networks,edges=edges,
      validation={"panels_total":sum(n["node_type"]=="panel" for n in nodes),
       "protective_devices_total":sum(n["node_type"]=="protective_device" for n in nodes),
       "meters_total":sum(n["node_type"]=="meter" for n in nodes),"loads_total":sum(n["node_type"]=="load" for n in nodes),
       "circuits_total":sum(n["node_type"]=="circuit" for n in nodes),"cables_total":sum(n["node_type"]=="cable" for n in nodes),
       "route_segments_total":len(segments),"route_components_total":len(components),"nodes_route_attached":attached,
       "confirmed_pairs_total":len(edges),"multi_device_networks_total":sum(n["path_state"]=="multi_device_bus" for n in networks),
       "raster_regions_total":len(images),"topology_state":"panel_device_bus_graph"},
      warnings=["общая шина нескольких аппаратов не превращается в произвольную последовательную цепь"])
    graph["route_segments"]=segments;return graph


def _build_lightning(page,pdf,block_id,subtype,profile):
    extra=(("lightning_device",re.compile(r"\b(?:молниеприем\w*|молниеотвод\w*|токоотвод\w*)\b",re.I)),
      ("grounding_device",re.compile(r"\b(?:заземлител\w*|электрод\w*|контур\w*\s+зазем|полоса\w*)\b",re.I)),
      ("bonding_target",re.compile(r"\b(?:ГЗШ|ДСУП|металлоконструкц\w*|трубопровод\w*|лоток\w*|PE|PEN)\b",re.I)),
      ("material",re.compile(r"\b(?:сталь\w*|мед\w*|оцинкован\w*|FeZn|Cu)\b",re.I)))
    nodes=_line_fact_nodes(page,extra);segments,components=_route_geometry(page,vivid=False,tolerance=3);attached=_attach(nodes,segments,components,limit=26)
    images=_images(page,len(nodes));nodes=_unique(nodes+images);networks=_networks(nodes,components,
      "equipotential_bonding" if profile==PROFILE_BONDING else "lightning_grounding_route",subtype)
    edges=[];parents=[n for n in nodes if n["node_type"] in ("panel","grounding_device")]
    for child in [n for n in nodes if n["node_type"] in ("lightning_device","bonding_target")]:
        if parents:
            parent,state=_preferred_parent(child,parents);edges.append({"id":f"edge-{len(edges)+1}","from":parent["id"],"to":child["id"],
              "edge_type":"grounding_or_bonding","edge_state":state})
    graph=_base(page,pdf,block_id,profile,subtype,nodes=nodes,networks=networks,edges=edges,
      validation={"lightning_devices_total":sum(n["node_type"]=="lightning_device" for n in nodes),
       "grounding_devices_total":sum(n["node_type"]=="grounding_device" for n in nodes),
       "bonding_targets_total":sum(n["node_type"]=="bonding_target" for n in nodes),"materials_total":sum(n["node_type"]=="material" for n in nodes),
       "route_segments_total":len(segments),"route_components_total":len(components),"nodes_route_attached":attached,
       "cad_confirmed_edges_total":sum(e["edge_state"]=="same_cad_component" for e in edges),
       "raster_regions_total":len(images),"topology_state":"grounding_bonding_graph"},
      warnings=["пространственная привязка к ближайшему контуру отделена от связи по общей CAD-компоненте"])
    graph["route_segments"]=segments;return graph


def _build_ozds(page,pdf,block_id,subtype):
    extra=(("ozds_device",re.compile(r"\b(?:ДР|ВУ|БП|БУ|ПУ)\s*[-№]?\s*\d*[A-ZА-Яа-я0-9._/\-]*",re.I)),
      ("barrier",re.compile(r"\b(?:электробарьер\w*|барьер\w*|электрод\w*|сетк\w*)\b",re.I)),
      ("zone",re.compile(r"\b(?:вход\w*|проем\w*|вентканал\w*|подвал\w*|этаж\w*)\b",re.I)))
    nodes=_line_fact_nodes(page,extra);segments,components=_route_geometry(page,vivid=False,tolerance=3);attached=_attach(nodes,segments,components,limit=24)
    images=_images(page,len(nodes));nodes=_unique(nodes+images);networks=_networks(nodes,components,"ozds_circuit",subtype)
    return _base(page,pdf,block_id,PROFILE_OZDS,subtype,nodes=nodes,networks=networks,
      validation={"devices_total":sum(n["node_type"]=="ozds_device" for n in nodes),"barriers_total":sum(n["node_type"]=="barrier" for n in nodes),
       "zones_total":sum(n["node_type"]=="zone" for n in nodes),"cables_total":sum(n["node_type"]=="cable" for n in nodes),
       "route_segments_total":len(segments),"route_components_total":len(components),"nodes_route_attached":attached,
       "raster_regions_total":len(images),"topology_state":"ozds_device_circuits"},
      warnings=["цепь нескольких блоков сохраняется общей сетью без выдуманного порядка подключения"])


def _physical_nodes(page):
    nodes=[];skip=re.compile(r"^(?:Изм\.|Лист|Дата|Разраб|Проверил|Масштаб)",re.I)
    for line in _lines(page):
        text=line["text"]
        if len(text)<2 or skip.search(text):continue
        if _ELEV_RE.fullmatch(text):kind="elevation"
        elif _SIZE_RE.fullmatch(text) or re.fullmatch(r"[RØ∅]?\s*\d{1,4}(?:[,.]\d+)?",text,re.I):kind="dimension"
        elif re.search(r"кабел|провод|труб|лоток|короб",text,re.I):kind="route_part"
        elif re.search(r"болт|гайк|шайб|анкер|хомут|саморез|шпильк",text,re.I):kind="fastener"
        elif re.search(r"кронштейн|опор|профил|рам|стойк",text,re.I):kind="support_part"
        elif re.search(r"шкаф|щит|панел|УЭРВ|ВРУ",text,re.I):kind="equipment_part"
        elif re.search(r"зазем|металлосвяз|свар",text,re.I):kind="grounding_part"
        elif re.search(r"бетон|кирпич|перекрыт|стен|гермет|огнезащ",text,re.I):kind="construction_part"
        else:kind="assembly_part"
        nodes.append(_node(page,line,kind,len(nodes)+1))
    return _unique(nodes)


def _build_physical(page,pdf,block_id,subtype,profile):
    nodes=_physical_nodes(page);parts=_geometry_parts(page)
    if profile==PROFILE_EQUIPMENT:
        for part in parts:
            item={"text":f"геометрически выделенная часть {len(nodes)+1}","bbox":part["bbox"],"center":part["center"]}
            nodes.append(_node(page,item,"geometry_part",len(nodes)+1,field_state="geometry_only"))
    views=_views(page);images=_images(page,len(nodes));nodes=_unique(nodes+images)
    if views:_assign_nodes_to_views(nodes,views)
    else:
        views=[{"id":"view-1","container_type":"physical_drawing","label":subtype,"member_ids":[n["id"] for n in nodes]}]
        for n in nodes:n["container_ids"]=["view-1"]
    segments=_segments(page,vivid_only=False,min_length=2)
    if len(segments)>60000:segments=segments[:60000]
    return _base(page,pdf,block_id,profile,subtype,nodes=nodes,containers=views,
      validation={"views_total":len(views),"parts_total":sum(n["node_type"] not in ("dimension","elevation","raster_region","raster_mosaic") for n in nodes),
       "dimensions_total":sum(n["node_type"]=="dimension" for n in nodes),"fasteners_total":sum(n["node_type"]=="fastener" for n in nodes),
       "supports_total":sum(n["node_type"]=="support_part" for n in nodes),"equipment_parts_total":sum(n["node_type"]=="equipment_part" for n in nodes),
       "geometry_parts_total":sum(n["node_type"]=="geometry_part" for n in nodes),"line_segments_total":len(segments),
       "raster_regions_total":len(images),"topology_state":"physical_views_and_parts"})


def _build_illuminance(page,pdf,block_id,subtype):
    extra=(("illuminance",re.compile(r"\b\d+(?:[,.]\d+)?\s*(?:лк|lux)\b",re.I)),
      ("fixture_model",re.compile(r"\b(?:LED|ДПО|ДБО|СПО|ЛПО|ARLIGHT|VARTON|IEK)[A-ZА-Яа-я0-9._/\- ]*",re.I)),
      ("quantity",re.compile(r"\b\d+\s*шт\.?\b",re.I)),("room",re.compile(r"\b(?:коридор|паркинг|кладов|холл|лестниц|помещен)\w*",re.I)))
    nodes=_line_fact_nodes(page,extra);images=_images(page,len(nodes));nodes=_unique(nodes+images);paths=[]
    for drawing in page.get_drawings():
        if any(item[0]=="c" for item in drawing.get("items") or []):paths.append(len(drawing.get("items") or []))
    networks=[{"id":f"field-{i+1}","network_type":"illuminance_field_candidate","label":f"векторное поле {i+1}",
      "endpoint_ids":[],"path_state":"uncalibrated_vector_field","geometry_items":count} for i,count in enumerate(paths[:200])]
    return _base(page,pdf,block_id,PROFILE_ILLUMINANCE,subtype,nodes=nodes,networks=networks,
      validation={"illuminance_values_total":sum(n["node_type"]=="illuminance" for n in nodes),
       "fixture_models_total":sum(n["node_type"]=="fixture_model" for n in nodes),"quantities_total":sum(n["node_type"]=="quantity" for n in nodes),
       "rooms_total":sum(n["node_type"]=="room" for n in nodes),"vector_fields_total":len(paths),"raster_regions_total":len(images),
       "topology_state":"illuminance_calculation_inventory"},
      warnings=["цветовое поле без калиброванной легенды не превращается в выдуманную матрицу значений"])


def _build_site(page,pdf,block_id,subtype):
    extra=(("building",re.compile(r"\b(?:К|корпус\s*)[1-7]\b",re.I)),
      ("parking",re.compile(r"\b(?:ПА|автостоянк\w*)\b",re.I)),("floor_count",re.compile(r"\b\d{1,2}\s*эт\.?\b",re.I)))
    nodes=_line_fact_nodes(page,extra);segments,components=_route_geometry(page,vivid=True,tolerance=4);attached=_attach(nodes,segments,components,limit=35)
    networks=_networks(nodes,components,"site_power_context",subtype);images=_images(page,len(nodes));nodes=_unique(nodes+images)
    return _base(page,pdf,block_id,PROFILE_SITE,subtype,nodes=nodes,networks=networks,
      validation={"buildings_total":sum(n["node_type"]=="building" for n in nodes),"parking_zones_total":sum(n["node_type"]=="parking" for n in nodes),
       "route_segments_total":len(segments),"route_components_total":len(components),"nodes_route_attached":attached,
       "raster_regions_total":len(images),"topology_state":"site_building_context"})


def _dispatch(page,pdf,block_id,profile,subtype):
    if profile in (PROFILE_DISTRIBUTION,PROFILE_LIGHTING,PROFILE_CABLE):graph=_build_plan(page,pdf,block_id,profile,subtype)
    elif profile==PROFILE_PANEL:graph=_build_panel(page,pdf,block_id,subtype)
    elif profile in (PROFILE_LIGHTNING,PROFILE_BONDING):graph=_build_lightning(page,pdf,block_id,subtype,profile)
    elif profile==PROFILE_OZDS:graph=_build_ozds(page,pdf,block_id,subtype)
    elif profile in (PROFILE_DETAIL,PROFILE_EQUIPMENT):graph=_build_physical(page,pdf,block_id,subtype,profile)
    elif profile==PROFILE_ILLUMINANCE:graph=_build_illuminance(page,pdf,block_id,subtype)
    elif profile==PROFILE_SITE:graph=_build_site(page,pdf,block_id,subtype)
    else:return None
    text=page.get_text().strip();graph["validation"].update({"pdf_text_characters":len(text),
      "pdf_words_total":len(page.get_text("words")),"source_layer_state":"text_available" if text else "no_pdf_text_layer"})
    if not text:graph.setdefault("warnings",[]).append("в PDF нет доступного текстового слоя; марки требуют OCR с координатами")
    return graph


def build_electrical_graph(pdf_path:Path,*,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        with fitz.open(str(pdf_path)) as doc:
            if doc.page_count!=1:return None
            profile=profile_hint or classify_electrical_profile(doc[0].get_text())
            graph=_dispatch(doc[0],Path(pdf_path),block_id,profile,subtype_hint or "electrical_block")
        fitz.TOOLS.store_shrink(100);return graph
    except Exception:return None


def build_electrical_graph_from_source(pdf_path:Path,*,page_index:int,bbox_norm,polygon_norm=None,block_id=None,profile_hint=None,subtype_hint=None):
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
            target.set_cropbox(unrotated);profile=profile_hint or classify_electrical_profile(target.get_text())
            return _dispatch(target,Path(pdf_path),block_id,profile,subtype_hint or "electrical_block")
        finally:
            if cropped is not None:cropped.close()
            source.close();fitz.TOOLS.store_shrink(100)
    except Exception:return None


def evaluate_electrical_gate(graph):
    if not graph:return {"use":False,"complete":False,"reasons":["граф не построен"],"complete_reasons":[]}
    v=graph["validation"];p=graph["profile_id"];reasons=[];complete=[]
    evidence=v.get("nodes_total",0)+v.get("networks_total",0)+v.get("containers_total",0)+v.get("route_segments_total",0)+v.get("line_segments_total",0)+v.get("raster_regions_total",0)
    if evidence<1:reasons.append("нет текстовой, векторной или растровой структуры")
    if p in (PROFILE_DISTRIBUTION,PROFILE_LIGHTING,PROFILE_CABLE):
        if v.get("route_segments_total",0)<10 and v.get("raster_regions_total",0)<1:complete.append("не извлечена геометрия плана")
        depth="engineering_graph" if any((n.get("endpoint_ids") for n in graph.get("networks") or [])) else ("semantic_hierarchy" if v.get("nodes_total",0)>1 else "geometry_inventory")
    elif p==PROFILE_PANEL:
        if v.get("protective_devices_total",0)<1 and v.get("route_segments_total",0)<20:complete.append("не найден состав щита")
        depth="engineering_graph" if v.get("confirmed_pairs_total",0)>0 else "semantic_hierarchy"
    elif p in (PROFILE_LIGHTNING,PROFILE_BONDING):
        depth="engineering_graph" if v.get("cad_confirmed_edges_total",0)>0 else "semantic_hierarchy"
    elif p==PROFILE_OZDS:depth="engineering_graph" if any(n.get("endpoint_ids") for n in graph.get("networks") or []) else "semantic_hierarchy"
    elif p in (PROFILE_DETAIL,PROFILE_EQUIPMENT):depth="physical_hierarchy"
    elif p==PROFILE_ILLUMINANCE:depth="analytical_geometry"
    else:depth="semantic_hierarchy"
    if v.get("source_layer_state")=="no_pdf_text_layer":complete.append("нет текстового слоя PDF: полнота подписей не подтверждена")
    v["description_depth"]=depth;graph["readiness"]={"complete":not complete,"status":"complete" if not complete else "source_or_topology_partial",
      "reasons":complete,"description_depth":depth,"vectograph_level":depth=="engineering_graph"}
    return {"use":not reasons,"complete":not complete,"reasons":reasons,"complete_reasons":complete,"metrics":v}


def add_electrical_secondary_description(graph,text,*,source="исходное описание блока"):
    if graph is not None and (text or "").strip():
        graph["secondary_description"]={"source":source,"text":re.sub(r"\s+"," ",str(text)).strip(),
          "evidence_state":"secondary_description_only",
          "warning":"описание не имеет координат и не участвует в построении CAD-рёбер"}
        graph["validation"]["secondary_description_present"]=True
    return graph


_PROFILE_RU={PROFILE_PANEL:"Схема электрического щита",PROFILE_DISTRIBUTION:"План силовых сетей",
 PROFILE_LIGHTING:"План электроосвещения",PROFILE_CABLE:"План кабельных трасс",
 PROFILE_LIGHTNING:"Молниезащита и заземление",PROFILE_BONDING:"Система уравнивания потенциалов",
 PROFILE_OZDS:"Охранно-защитная дератизационная система",PROFILE_DETAIL:"Монтажный узел ЭОМ",
 PROFILE_EQUIPMENT:"Чертёж электрооборудования",PROFILE_ILLUMINANCE:"Светотехнический расчёт",
 PROFILE_SITE:"Площадочная схема ЭОМ"}
_NODE_RU={"panel":"щит или шкаф","protective_device":"защитный аппарат","circuit":"цепь или группа","cable":"кабель",
 "elevation":"отметка","size":"размер или сечение","electrical_value":"электрический параметр","fire_rating":"огнестойкость",
 "fixture":"светильник или указатель","socket":"розетка или электровывод","switch":"выключатель","equipment":"электроприёмник",
 "room":"помещение","meter":"счётчик","bus":"шина","load":"нагрузка","lightning_device":"элемент молниезащиты",
 "grounding_device":"заземлитель или контур","bonding_target":"объект уравнивания потенциалов","material":"материал",
 "ozds_device":"блок ОЗДС","barrier":"защитный барьер","zone":"защищаемая зона","dimension":"размер","route_part":"кабельная часть",
 "fastener":"крепёж","support_part":"опора или рама","equipment_part":"часть оборудования","grounding_part":"часть заземления",
 "construction_part":"строительная часть","assembly_part":"элемент узла","geometry_part":"геометрически выделенная часть",
 "illuminance":"освещённость","fixture_model":"модель светильника","quantity":"количество","building":"корпус",
 "parking":"автостоянка","floor_count":"этажность","raster_region":"растровая область","raster_mosaic":"растровая мозаика"}
_DEPTH_RU={"engineering_graph":"инженерный граф с CAD-подтверждённой топологией","semantic_hierarchy":"предметная структура с честными пространственными связями",
 "physical_hierarchy":"физическая структура видов и частей","geometry_inventory":"геометрический инвентарь без читаемых марок",
 "analytical_geometry":"аналитическая структура расчётного поля"}
_SUBTYPE_RU={
 "calculation_singleline":"расчётная однолинейная схема","principle_vru":"принципиальная схема ВРУ",
 "lighting_vru_fragment":"фрагмент ВРУ для освещения","floor_panel":"этажный щит","apartment_panel":"квартирный щит",
 "lighting_distribution_singleline":"многоуровневая схема распределения освещения","switchroom_layout":"план электрощитовой",
 "apartment_or_meter_connection":"подключение квартир или счётчика","switch_contact_diagram":"диаграмма контактов переключателя",
 "building_distribution":"распределительные сети здания","parking_distribution":"распределительные сети автостоянки",
 "roof_distribution":"распределительные сети кровли","common_system_power":"силовое питание общедомовых систем",
 "fire_system_power":"силовое питание противопожарных систем","interior_equipment":"электрооборудование интерьера",
 "socket_and_outlet_plan":"розетки и электровыводы","multidiscipline_niche":"многодисциплинарная инженерная ниша",
 "building_lighting":"освещение здания","parking_lighting":"освещение автостоянки","roof_lighting":"освещение кровли",
 "obstruction_lights":"заградительные огни","fixture_coordinates":"координатная привязка светильников",
 "fixture_marking":"маркировка светильников","floor_wall_fixtures":"напольные и настенные светильники",
 "switching_groups":"группы включения освещения","fixture_layout_fragment":"фрагмент расстановки светильников",
 "emergency_lighting_scenario":"сценарий аварийного освещения","cable_trays":"кабельные лотки",
 "fireproof_box":"огнезащитный кабельный короб","general_cable_routes":"общие кабельные трассы",
 "lightning_roof":"молниезащита кровли","lightning_facade":"токоотводы на фасадах",
 "equipotential_plan":"план заземления и уравнивания потенциалов","combined_grounding_plan":"совмещённый план заземления и молниезащиты",
 "main_bonding":"основная система уравнивания потенциалов","structure":"структурная схема ОЗДС",
 "device_wiring":"подключение блоков ОЗДС","floor_plan":"план оборудования ОЗДС","ozds_installation":"монтаж ОЗДС",
 "grounding_welds":"сварные соединения заземления","heating_cable":"обогрев водосточных лотков",
 "cable_or_fixture_mount":"крепление кабелей или светильников","bonding_detail":"узел металлосвязи","tray_support":"опора кабельного лотка",
 "floor_distribution_unit":"этажное распределительное устройство","height_compensator":"компенсатор высоты",
 "panel_door":"дверь электрощита","cabinet_view":"вид электротехнического шкафа","main_switchboard_view":"общий вид ВРУ",
 "calculation_map":"карта светотехнического расчёта","building_power_context":"расположение корпусов и автостоянок",
}
_EDGE_STATE_RU={"path_confirmed":"соединение подтверждено CAD-линией","same_cad_component":"общая непрерывная CAD-трасса",
 "nearest_geometry":"пространственная привязка"}


def render_electrical_markdown(graph):
    v=graph["validation"];p=graph["profile_id"];subtype=v.get("subtype");ready=graph.get("readiness") or {}
    lines=[f"# Эталонная текстовая разметка ЭОМ: {_PROFILE_RU.get(p,'Графический блок ЭОМ')}","",
      f"**Тип блока:** {_SUBTYPE_RU.get(subtype,str(subtype).replace('_',' '))}",f"**Источник:** {graph['source']['pdf_file']}",
      "**Метод:** текст, координаты и CAD-геометрия PDF; неподтверждённые соединения не добавляются.","",
      "## 1. Краткий результат","",f"Узлов: {v.get('nodes_total',0)}; сетей: {v.get('networks_total',0)}; явных рёбер: {v.get('edges_total',0)}.",
      f"**Уровень описания:** {_DEPTH_RU.get(v.get('description_depth'),'структура блока')}.",
      f"**Текстовый слой PDF:** {'доступен' if v.get('source_layer_state')=='text_available' else 'отсутствует'}.","",
      "### Инженерное дерево","","```text",_PROFILE_RU.get(p,"блок ЭОМ").lower()]
    counts=collections.Counter(n["node_type"] for n in graph.get("nodes") or [])
    for i,(kind,count) in enumerate(counts.most_common(12)):
        lines.append(("└──" if i==min(len(counts),12)-1 else "├──")+f" {_NODE_RU.get(kind,kind)}: {count}")
    lines += ["```","","## 2. Состав блока",""]
    grouped=collections.defaultdict(list)
    for node in graph.get("nodes") or []:grouped[node["node_type"]].append(node["label"])
    for kind,labels in sorted(grouped.items(),key=lambda item:_NODE_RU.get(item[0],item[0])):
        preview=", ".join(labels[:24]);lines.append(f"- **{_NODE_RU.get(kind,kind)} — {len(labels)}:** {preview}{' …' if len(labels)>24 else ''}")
    if graph.get("containers"):
        lines += ["","### Виды и физические группы",""]
        for i,c in enumerate(graph["containers"][:30],1):
            container_label=c.get("label") or "без подписи"
            container_label=_SUBTYPE_RU.get(container_label,container_label)
            lines.append(f"- Группа {i}: {container_label}; элементов {len(c.get('member_ids') or [])}.")
    lines += ["","## 3. Цепи, трассы и связи",""]
    networks=graph.get("networks") or []
    for i,n in enumerate(sorted(networks,key=lambda x:len(x.get("endpoint_ids") or []),reverse=True)[:40],1):
        network_label=n.get("label") or "без подписи"
        network_label=_SUBTYPE_RU.get(network_label,network_label)
        lines.append(f"- Цепь {i} «{network_label}»: обозначений {len(n.get('endpoint_ids') or [])}; доказательство — {'общая CAD-трасса' if n.get('path_state') in ('cad_endpoint_component','confirmed_pair') else 'общая многоточечная сеть'}.")
    if not networks:lines.append("- Отдельные электрические цепи не сформированы; сохранена физическая или аналитическая структура.")
    edges=graph.get("edges") or [];labels={n["id"]:n["label"] for n in graph.get("nodes") or []}
    if edges:
        lines += ["","### Явные связи",""]
        for edge in edges[:80]:lines.append(f"- {labels.get(edge['from'],'элемент')} → {labels.get(edge['to'],'элемент')}: {_EDGE_STATE_RU.get(edge.get('edge_state'),'структурная связь')}.")
    secondary=graph.get("secondary_description") or {}
    if secondary:
        lines += ["","## 4. Дополнительное описание без координат","",secondary["text"],"",
          "Это описание сохранено как вторичный источник и не используется для создания соединений."]
    lines += ["",f"## {'5' if secondary else '4'}. Полнота и ограничения","",
      "Доступная структура блока описана полностью по правилам профиля." if ready.get("complete") else "Описание ограничено признаками, перечисленными ниже."]
    lines += [f"- {reason}" for reason in ready.get("reasons") or []]
    lines += [f"- {warning}" for warning in graph.get("warnings") or []]
    return "\n".join(lines)+"\n"
