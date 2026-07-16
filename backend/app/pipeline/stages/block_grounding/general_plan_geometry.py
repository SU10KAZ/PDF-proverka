"""Детерминированные графы генерального плана, рельефа, покрытий и водоотвода."""
from __future__ import annotations

import collections,math,re
from pathlib import Path

from .hvac_geometry import (_axes,_base,_bbox,_center,_clip_copied_page,_components,_node,
                            _point,_segments,_unique,_views,_assign_nodes_to_views)


PROFILE_AXIS="gp_axis_plan";PROFILE_GENERAL="gp_general_plan";PROFILE_STAKEOUT="gp_stakeout_plan"
PROFILE_GRADING="gp_grading_plan";PROFILE_GRADING_DETAIL="gp_grading_detail";PROFILE_EARTH="gp_earthwork_plan"
PROFILE_PAVEMENT="gp_pavement_plan";PROFILE_ROAD="gp_road_structure";PROFILE_SURFACE="gp_surface_layout"
PROFILE_MAF="gp_small_forms_plan";PROFILE_DRAIN_PLAN="gp_drainage_plan";PROFILE_DRAIN_PROFILE="gp_drainage_profile"
PROFILE_DRAIN_DETAIL="gp_drainage_detail"
ALL_GP_PROFILES=(PROFILE_AXIS,PROFILE_GENERAL,PROFILE_STAKEOUT,PROFILE_GRADING,PROFILE_GRADING_DETAIL,
 PROFILE_EARTH,PROFILE_PAVEMENT,PROFILE_ROAD,PROFILE_SURFACE,PROFILE_MAF,PROFILE_DRAIN_PLAN,
 PROFILE_DRAIN_PROFILE,PROFILE_DRAIN_DETAIL)
PLAN_PROFILES={PROFILE_AXIS,PROFILE_GENERAL,PROFILE_STAKEOUT,PROFILE_GRADING,PROFILE_EARTH,
 PROFILE_PAVEMENT,PROFILE_SURFACE,PROFILE_MAF,PROFILE_DRAIN_PLAN}
PHYSICAL_PROFILES={PROFILE_GRADING_DETAIL,PROFILE_ROAD,PROFILE_DRAIN_PROFILE,PROFILE_DRAIN_DETAIL}


def classify_gp_profile(text:str):
    u=re.sub(r"\s+"," ",text or "").upper()
    if any(x in u for x in ("ПРОДОЛЬНЫЙ ПРОФИЛЬ", "ПОПЕРЕЧНЫЙ ПРОФИЛЬ")) and any(x in u for x in ("ЛОТК","ВОДООТВОД","ДРЕНАЖ")):return PROFILE_DRAIN_PROFILE
    if any(x in u for x in ("СТЫКОВК","ПОДРЕЗК","ПЕСКОУЛОВИТ","КАСКАДНЫЙ ПЕРЕХОДНИК")):return PROFILE_DRAIN_DETAIL
    if ("УЗЕЛ" in u or "СЕЧЕНИ" in u or "РАЗРЕЗ" in u) and any(x in u for x in ("ДОРОЖН", "ТРОТУАР", "ГАЗОН", "БОРТОВ", "ПОКРЫТИ")):return PROFILE_ROAD
    if "КОНСТРУКЦ" in u and any(x in u for x in ("ДОРОЖН","ПОКРЫТИ")):return PROFILE_ROAD
    if "ПЛАН ЗЕМЛЯНЫХ МАСС" in u:return PROFILE_EARTH
    if "ПЛАН ОРГАНИЗАЦИИ РЕЛЬЕФА" in u:return PROFILE_GRADING
    if "СЕЧЕНИЕ" in u and any(x in u for x in ("СХОД","ЛЕСТНИЦ","РЕЛЬЕФ")):return PROFILE_GRADING_DETAIL
    if "РАЗБИВОЧН" in u and "ОС" in u:return PROFILE_AXIS
    if "КОМПОНОВОЧНЫЙ ПЛАН ОСЕЙ" in u:return PROFILE_AXIS
    if "РАЗБИВОЧНЫЙ ЧЕРТЕЖ" in u:return PROFILE_STAKEOUT
    if any(x in u for x in ("ПЛАН ДОРОЖНЫХ ПОКРЫТИЙ", "ПЛАН ПОКРЫТИЙ", "СХЕМА ПОКРЫТИЙ")):return PROFILE_PAVEMENT
    if "РАСКЛАДК" in u or "СХЕМА МОЩЕНИЯ" in u or "РЕЗИНОВОЙ КРОШК" in u:return PROFILE_SURFACE
    if "МАЛЫХ АРХИТЕКТУРНЫХ ФОРМ" in u or ("ПЛАН" in u and any(x in u for x in ("МАФ", "ИГРОВОГО ОБОРУДОВАНИЯ", "ПЛОЩАДОК"))):return PROFILE_MAF
    if any(x in u for x in ("СХЕМА ПОСАДКИ", "ПЛАН ПОСАДКИ", "ПЛАН ЦВЕТНИКОВ", "СХЕМА ЦВЕТНИКА",
        "ЭКСПЛИКАЦИЕЙ РАСТЕНИЙ", "ПОСАДОЧНОГО МАТЕРИАЛА")):return PROFILE_GENERAL
    if any(x in u for x in ("ДРЕНАЖНЫХ ЛОТКОВ","ПОВЕРХНОСТНОГО ВОДООТВОДА","COMPOMAX")):return PROFILE_DRAIN_PLAN
    if any(x in u for x in ("ГЕНЕРАЛЬНЫЙ ПЛАН", "ПЛАН БЛАГОУСТРОЙСТВА", "ПЛАН ОЗЕЛЕНЕНИЯ", "ПЛАН БЛАГОУСТРОЙСТВА И ОЗЕЛЕНЕНИЯ")):return PROFILE_GENERAL
    return None


PATTERNS=(
 ("building",re.compile(r"\b(?:корпус\s*№?\s*\d+|К\s*[1-7])\b",re.I)),
 ("floor_count",re.compile(r"\b\d{1,2}\s*эт\.?\b",re.I)),
 ("elevation",re.compile(r"(?<!\d)(?:[+\-]\d{1,3}|\d{2,3})[,.]\d{2,3}(?!\d)")),
 ("coordinate",re.compile(r"\b[XY]\s*=\s*\d+(?:[,.]\d+)?",re.I)),
 ("radius",re.compile(r"\bR\s*=?\s*\d+(?:[,.]\d+)?",re.I)),
 ("slope",re.compile(r"(?:\bi\s*=\s*[+\-]?\d+(?:[,.]\d+)?|\d+(?:[,.]\d+)?\s*‰)",re.I)),
 ("dimension",re.compile(r"\b\d+(?:[,.]\d+)?\s*(?:мм|см|м)\b",re.I)),
 ("area_or_volume",re.compile(r"\b\d+(?:[,.]\d+)?\s*м[²2³3]\b",re.I)),
 ("surface",re.compile(r"\b(?:асфальтобетон\w*|брусчатк\w*|плитк\w*|газон\w*|резинов\w*\s+крошк\w*|щебен\w*|щебён\w*|песок|геотекстил\w*|бетон\w*|грунт\w*)\b",re.I)),
 ("small_form",re.compile(r"\b(?:МАФ\s*[-№]?\s*[A-ZА-Яа-я0-9.]+|скамь\w*|урн\w*|качел\w*|горк\w*|пергол\w*|велопарков\w*|площадк\w*)\b",re.I)),
 ("drainage",re.compile(r"\b(?:DN\s*\d+|лоток\w*|пескоуловител\w*|заглушк\w*|переходник\w*|водоотвод\w*|CompoMax\w*)\b",re.I)),
 ("earthwork",re.compile(r"\b(?:насып\w*|выемк\w*|об[ъь]ем\w*|баланс\w*\s+земляных\w*\s+масс)\b",re.I)),
)


def _source_lines(page):
    result=[]
    for block in page.get_text("dict").get("blocks",[]):
        for line in block.get("lines",[]):
            spans=line.get("spans",[]);text="".join(str(span.get("text") or "") for span in spans).strip()
            if not text:continue
            bbox=_bbox(page,line.get("bbox") or (0,0,0,0));result.append({"text":text,"bbox":bbox,"center":_center(bbox)})
    return result


def _facts(page):
    nodes=[]
    for line in _source_lines(page):
        for kind,pattern in PATTERNS:
            for match in pattern.finditer(line["text"]):
                label=re.sub(r"\s+"," ",match.group()).strip(" ;,.")
                item=dict(line);item["text"]=label;nodes.append(_node(page,item,kind,len(nodes)+1,source_label=line["text"]))
    return _unique(nodes)


def _inside_segments(page,*,vivid=False,cap=60000):
    w,h=float(page.rect.width),float(page.rect.height);result=[]
    for drawing in page.get_drawings():
        rect=drawing.get("rect")
        if rect:
            db=_bbox(page,(rect.x0,rect.y0,rect.x1,rect.y1))
            if db[2]<-2 or db[0]>w+2 or db[3]<-2 or db[1]>h+2:continue
        if drawing.get("fill") is not None:continue
        color=tuple(round(float(value),3) for value in (drawing.get("color") or (0,0,0)));chroma=max(color)-min(color)
        neutral=chroma<.08 and sum(color)/3>.12
        if vivid and chroma<.16:continue
        if not vivid and neutral:continue
        for item in drawing.get("items") or []:
            if item[0]!="l":continue
            p1=_point(page,item[1].x,item[1].y);p2=_point(page,item[2].x,item[2].y);length=math.dist(p1,p2)
            if length<2:continue
            xs=(p1[0],p2[0]);ys=(p1[1],p2[1])
            if max(xs)<-2 or min(xs)>w+2 or max(ys)<-2 or min(ys)>h+2:continue
            result.append({"id":f"segment-{len(result)+1}","p1":p1,"p2":p2,"length":round(length,3),"color":color})
            if len(result)>=cap:return result
    if vivid and len(result)<15:return _inside_segments(page,vivid=False,cap=cap)
    return result


def _attach(nodes,segments,components,limit=25):
    by_segment={sid:c["id"] for c in components for sid in c["segment_ids"]};attached=0
    for node in nodes:
        best=None
        for segment in segments:
            ax,ay=segment["p1"];bx,by=segment["p2"];px,py=node["x"],node["y"]
            dx,dy=bx-ax,by-ay;t=max(0,min(1,((px-ax)*dx+(py-ay)*dy)/(dx*dx+dy*dy))) if dx or dy else 0
            distance=math.hypot(px-(ax+t*dx),py-(ay+t*dy))
            if best is None or distance<best[0]:best=(distance,segment)
        if best and best[0]<=limit:node["route_id"]=by_segment[best[1]["id"]];node["route_distance"]=round(best[0],3);attached+=1
    return attached


def _plan(page,pdf,block_id,profile,subtype):
    nodes=_facts(page)
    # На генплане имя корпуса часто отсутствует в текстовом слое, но подпись
    # этажности расположена внутри его контура. Сохраняем такой контур как
    # безымянное здание, не выдумывая номер.
    for floor in [n for n in nodes if n["node_type"]=="floor_count"]:
        item={"text":f"здание с подписью «{floor['label']}»","bbox":(floor["x"]-1,floor["y"]-1,floor["x"]+1,floor["y"]+1),"center":(floor["x"],floor["y"])}
        nodes.append(_node(page,item,"building_footprint",len(nodes)+1,field_state="confirmed_by_floor_count_position"))
    # В сетке земляных масс подписанная разность является расчётной ячейкой.
    if profile==PROFILE_EARTH:
        for delta in [n for n in nodes if n["node_type"]=="elevation" and n["label"].startswith(("+","-"))]:
            item={"text":f"ячейка земляных масс Δ={delta['label']}","bbox":(delta["x"]-1,delta["y"]-1,delta["x"]+1,delta["y"]+1),"center":(delta["x"],delta["y"])}
            nodes.append(_node(page,item,"earthwork_cell",len(nodes)+1,field_state="delta_elevation_with_coordinates"))
    nodes=_unique(nodes);axes=_axes(page);segments=_inside_segments(page,vivid=True);components=_components(segments,tolerance=3);attached=_attach(nodes,segments,components,30)
    networks=[]
    for component in components:
        members=[n for n in nodes if n.get("route_id")==component["id"]]
        if not members and not component.get("branch_points"):continue
        labels=list(dict.fromkeys(n["label"] for n in members if n["node_type"] in ("building","small_form","drainage","surface","earthwork")))
        networks.append({"id":f"network-{len(networks)+1}","network_type":"site_geometry_component","label":" / ".join(labels[:8]) or subtype,
          "endpoint_ids":[n["id"] for n in members],"source_route_id":component["id"],"path_state":"cad_endpoint_component",
          **{k:v for k,v in component.items() if k!="id"}})
    graph=_base(page,pdf,block_id,profile,subtype,nodes=nodes,networks=networks,validation={
      "axes_total":len(axes),"buildings_total":sum(n["node_type"] in ("building","building_footprint") for n in nodes),
      "elevations_total":sum(n["node_type"]=="elevation" for n in nodes),"coordinates_total":sum(n["node_type"]=="coordinate" for n in nodes),
      "surfaces_total":sum(n["node_type"]=="surface" for n in nodes),"small_forms_total":sum(n["node_type"]=="small_form" for n in nodes),
      "drainage_elements_total":sum(n["node_type"]=="drainage" for n in nodes),"earthwork_facts_total":sum(n["node_type"] in ("earthwork","earthwork_cell") for n in nodes),
      "route_segments_total":len(segments),"route_components_total":len(components),"route_branches_total":sum(c["branch_points"] for c in components),
      "nodes_route_attached":attached,"topology_state":"site_plan_geometry"},warnings=[
      "CAD-компонента отражает непрерывность линии; назначение дороги, контура или сети определяется только по подписи",
      "пересечение линий без общей конечной точки не создаёт соединение"])
    graph["grid"]={"axes":axes};graph["route_segments"]=segments;return graph


def _physical(page,pdf,block_id,profile,subtype):
    nodes=_facts(page);views=_views(page)
    # В вырезке дорожной одежды исходный лист содержит >120 тыс. штриховок.
    # Их разворачивание в отдельные сегменты не несёт инженерного смысла: слои,
    # материалы и толщины уже имеют координаты. Векторный PDF сохраняется целиком,
    # а hatch-пути честно не выдаются за топологию.
    segments=[] if profile==PROFILE_ROAD else _inside_segments(page,vivid=False,cap=40000)
    if not views:views=[{"id":"view-1","container_type":"drawing_view","label":subtype,"bbox_page":[0,0,1,1],
      "anchor_x":float(page.rect.width)/2,"anchor_y":float(page.rect.height)/2,"member_ids":[]}]
    _assign_nodes_to_views(nodes,views)
    containers=views
    edges=[]
    if profile==PROFILE_ROAD:
        layers=sorted((n for n in nodes if n["node_type"]=="surface"),key=lambda n:(n["y"],n["x"]))
        for upper,lower in zip(layers,layers[1:]):
            if abs(upper["x"]-lower["x"])<page.rect.width*.5:edges.append({"id":f"edge-{len(edges)+1}","from":upper["id"],"to":lower["id"],
              "edge_type":"последовательность слоёв","edge_state":"vertical_order_in_section"})
    return _base(page,pdf,block_id,profile,subtype,nodes=nodes,containers=containers,edges=edges,validation={
      "views_total":len(containers),"layers_total":sum(n["node_type"]=="surface" for n in nodes),
      "dimensions_total":sum(n["node_type"]=="dimension" for n in nodes),"elevations_total":sum(n["node_type"]=="elevation" for n in nodes),
      "drainage_elements_total":sum(n["node_type"]=="drainage" for n in nodes),"physical_line_segments_total":len(segments),
      "layer_order_edges_total":len(edges),"vector_hatching_state":"preserved_not_expanded" if profile==PROFILE_ROAD else "expanded_visible_segments",
      "topology_state":"physical_section_or_detail"},warnings=[
      "порядок слоёв задаётся вертикальным порядком подписей в одной конструкции",
      "штриховки дорожной одежды сохранены в векторном PDF, но не разворачиваются в десятки тысяч псевдоузлов" if profile==PROFILE_ROAD else
      "монтажный вид не превращается в гидравлическую сеть без непрерывной трассы"])


def _dispatch(page,pdf,block_id,profile,subtype):
    graph=_plan(page,pdf,block_id,profile,subtype) if profile in PLAN_PROFILES else (_physical(page,pdf,block_id,profile,subtype) if profile in PHYSICAL_PROFILES else None)
    if graph:
        text=page.get_text().strip();graph["validation"].update({"pdf_text_characters":len(text),"pdf_words_total":len(page.get_text("words")),
          "source_layer_state":"text_available" if text else "no_pdf_text_layer"})
    return graph


def build_gp_graph(pdf_path:Path,*,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        with fitz.open(str(pdf_path)) as doc:
            if doc.page_count!=1:return None
            profile=profile_hint or classify_gp_profile(doc[0].get_text());return _dispatch(doc[0],Path(pdf_path),block_id,profile,subtype_hint or "графический блок ГП")
    except Exception:return None


def build_gp_graph_from_source(pdf_path:Path,*,page_index:int,bbox_norm,polygon_norm=None,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        source=fitz.open(str(pdf_path));cropped=None
        try:
            if not bbox_norm or not 0<=page_index<source.page_count:return None
            sp=source[page_index];w,h=sp.rect.width,sp.rect.height;crop=fitz.Rect(*(float(v) for v in (bbox_norm[0]*w,bbox_norm[1]*h,bbox_norm[2]*w,bbox_norm[3]*h)))&sp.rect
            unrotated=crop*sp.derotation_matrix;unrotated.normalize();offset=sp.cropbox_position
            unrotated=fitz.Rect(unrotated.x0+offset.x,unrotated.y0+offset.y,unrotated.x1+offset.x,unrotated.y1+offset.y)
            cropped=fitz.open();cropped.insert_pdf(source,from_page=page_index,to_page=page_index);target=cropped[0]
            if polygon_norm:
                inverse=~sp.transformation_matrix;points=[tuple(fitz.Point(float(x)*w,float(y)*h)*sp.derotation_matrix*inverse) for x,y in polygon_norm];_clip_copied_page(target,points)
            target.set_cropbox(unrotated);profile=profile_hint or classify_gp_profile(target.get_text())
            return _dispatch(target,Path(pdf_path),block_id,profile,subtype_hint or "графический блок ГП")
        finally:
            if cropped is not None:cropped.close()
            source.close();fitz.TOOLS.store_shrink(100)
    except Exception:return None


def evaluate_gp_gate(graph):
    if not graph:return {"use":False,"complete":False,"reasons":["граф не построен"],"complete_reasons":[]}
    v=graph["validation"];evidence=v.get("nodes_total",0)+v.get("route_segments_total",0)+v.get("physical_line_segments_total",0);reasons=[];partial=[]
    if evidence<1:reasons.append("нет предметной или геометрической структуры")
    if graph["profile_id"] in PLAN_PROFILES:depth="engineering_graph" if any(n.get("endpoint_ids") for n in graph.get("networks",[])) else "semantic_hierarchy"
    else:depth="physical_hierarchy"
    if v.get("source_layer_state")=="no_pdf_text_layer":partial.append("нет текстового слоя PDF")
    v["description_depth"]=depth;graph["readiness"]={"complete":not partial,"status":"complete" if not partial else "source_partial","reasons":partial}
    return {"use":not reasons,"complete":not partial,"reasons":reasons,"complete_reasons":partial,"metrics":v}


def add_gp_secondary_description(graph,text,*,source="описание исходного блока"):
    if graph is not None and str(text or "").strip():
        graph["secondary_description"]={"source":source,"text":re.sub(r"\s+"," ",str(text)).strip(),
          "evidence_state":"вторичное описание без координат","warning":"не участвует в построении узлов и рёбер"}
    return graph


PROFILE_RU={PROFILE_AXIS:"План координационных осей",PROFILE_GENERAL:"Генеральный план",PROFILE_STAKEOUT:"Разбивочный план",
 PROFILE_GRADING:"План организации рельефа",PROFILE_GRADING_DETAIL:"Узел вертикальной планировки",PROFILE_EARTH:"План земляных масс",
 PROFILE_PAVEMENT:"План дорожных покрытий",PROFILE_ROAD:"Конструкция дорожной одежды",PROFILE_SURFACE:"Раскладка покрытий",
 PROFILE_MAF:"План малых архитектурных форм",PROFILE_DRAIN_PLAN:"План поверхностного водоотвода",
 PROFILE_DRAIN_PROFILE:"Профиль поверхностного водоотвода",PROFILE_DRAIN_DETAIL:"Монтажный узел водоотвода"}
NODE_RU={"axis":"координационная ось","building":"корпус","floor_count":"этажность","elevation":"отметка",
 "coordinate":"геодезическая координата","radius":"радиус","slope":"уклон","dimension":"размер","area_or_volume":"площадь или объём",
 "surface":"материал или покрытие","small_form":"малая архитектурная форма","drainage":"элемент водоотвода","earthwork":"земляные работы",
 "building_footprint":"контур здания по подписи этажности","earthwork_cell":"ячейка земляных масс"}
DEPTH_RU={"engineering_graph":"инженерный граф с CAD-подтверждённой геометрией","semantic_hierarchy":"предметная структура плана",
 "physical_hierarchy":"физическая структура видов, слоёв и деталей"}


def render_gp_markdown(graph):
    v=graph["validation"];ready=graph.get("readiness") or {};lines=[f"# Эталонная текстовая разметка ГП: {PROFILE_RU.get(graph['profile_id'],'Графический блок ГП')}","",
      f"**Источник:** {graph['source']['pdf_file']}","**Метод:** текст с координатами и CAD-геометрия PDF; неподтверждённые связи не добавляются.","",
      "## 1. Краткий результат","",f"Узлов: {v.get('nodes_total',0)}; геометрических сетей: {v.get('networks_total',0)}; явных рёбер: {v.get('edges_total',0)}.",
      f"**Уровень описания:** {DEPTH_RU.get(v.get('description_depth'),'предметная структура')}.","","### Инженерное дерево","","```text",PROFILE_RU.get(graph["profile_id"],"блок ГП").lower()]
    counts=collections.Counter(n["node_type"] for n in graph.get("nodes",[]))
    for i,(kind,count) in enumerate(counts.most_common(14)):lines.append(("└──" if i==min(len(counts),14)-1 else "├──")+f" {NODE_RU.get(kind,kind)}: {count}")
    lines += ["```","","## 2. Состав и параметры",""]
    grouped=collections.defaultdict(list)
    for node in graph.get("nodes",[]):grouped[node["node_type"]].append(node["label"])
    for kind,labels in grouped.items():lines.append(f"- **{NODE_RU.get(kind,kind)} — {len(labels)}:** {', '.join(list(dict.fromkeys(labels))[:30])}{' …' if len(set(labels))>30 else ''}")
    lines += ["","## 3. Геометрия и связи",""]
    if graph.get("networks"):
        for i,network in enumerate(graph["networks"][:40],1):lines.append(f"- Геометрическая компонента {i}: подписей {len(network.get('endpoint_ids') or [])}, ветвлений {network.get('branch_points',0)}; доказательство — непрерывные CAD-отрезки.")
    elif graph.get("edges"):
        labels={n["id"]:n["label"] for n in graph.get("nodes",[])}
        for edge in graph["edges"][:80]:lines.append(f"- {labels.get(edge['from'],'слой')} → {labels.get(edge['to'],'слой')}: следующий слой по вертикальному порядку конструкции.")
    else:lines.append("- Попарная топология неприменима или не подтверждена; сохранён состав и физическая геометрия.")
    secondary=graph.get("secondary_description")
    if secondary:lines += ["","## 4. Дополнительное описание без координат","",secondary["text"],"",
      "Это описание не используется для создания узлов и связей."]
    lines += ["",f"## {'5' if secondary else '4'}. Полнота и ограничения","","Доступная структура описана полностью по правилам профиля." if ready.get("complete") else "Описание ограничено исходным PDF."]
    lines += [f"- {x}" for x in ready.get("reasons",[])];lines += [f"- {x}" for x in graph.get("warnings",[])];return "\n".join(lines)+"\n"
