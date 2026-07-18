"""Детерминированные предметные графы архитектурных планов, фасадов и узлов."""
from __future__ import annotations
import collections,math,re
from pathlib import Path
from .hvac_geometry import (_axes,_base,_bbox,_bbox_norm,_center,_clip_copied_page,_node,_unique,_views,_assign_nodes_to_views)
from .general_plan_geometry import _inside_segments

PROFILE_MASONRY_PLAN="ar_masonry_plan";PROFILE_MASONRY_DETAIL="ar_masonry_detail";PROFILE_STAIR="ar_stair_drawing"
PROFILE_RAILING="ar_railing_detail";PROFILE_SECTION="ar_section";PROFILE_OPENING_PLAN="ar_opening_plan"
PROFILE_FACADE="ar_facade";PROFILE_DETAIL="ar_detail";PROFILE_MARKING="ar_marking_plan"
PROFILE_OPENING_DRAWING="ar_opening_drawing";PROFILE_FLOOR="ar_floor_plan";PROFILE_ROOF_PLAN="ar_roof_plan"
PROFILE_ROOF_DETAIL="ar_roof_detail";PROFILE_FINISH="ar_finish_plan";PROFILE_FOUNDATION="ar_equipment_foundation"
PROFILE_WALL_ELEVATION="ar_wall_elevation";PROFILE_CEILING="ar_ceiling_plan";PROFILE_FLOOR_FINISH="ar_floor_finish_plan"
PROFILE_INTERIOR_ELECTRICAL="ar_interior_electrical_plan";PROFILE_FURNITURE="ar_furniture_plan"
PROFILE_FLOOR_WALL_JUNCTION="ar_floor_wall_junction_detail"
ALL_AR_PROFILES=(PROFILE_MASONRY_PLAN,PROFILE_MASONRY_DETAIL,PROFILE_STAIR,PROFILE_RAILING,PROFILE_SECTION,
 PROFILE_OPENING_PLAN,PROFILE_FACADE,PROFILE_DETAIL,PROFILE_MARKING,PROFILE_OPENING_DRAWING,PROFILE_FLOOR,
 PROFILE_ROOF_PLAN,PROFILE_ROOF_DETAIL,PROFILE_FINISH,PROFILE_FOUNDATION,PROFILE_WALL_ELEVATION,PROFILE_CEILING,
 PROFILE_FLOOR_FINISH,PROFILE_INTERIOR_ELECTRICAL,PROFILE_FURNITURE,PROFILE_FLOOR_WALL_JUNCTION)
PLAN_PROFILES={PROFILE_MASONRY_PLAN,PROFILE_OPENING_PLAN,PROFILE_MARKING,PROFILE_FLOOR,PROFILE_ROOF_PLAN,
 PROFILE_FINISH,PROFILE_FOUNDATION,PROFILE_CEILING,PROFILE_FLOOR_FINISH,PROFILE_INTERIOR_ELECTRICAL,PROFILE_FURNITURE}
ELEVATION_PROFILES={PROFILE_SECTION,PROFILE_FACADE,PROFILE_WALL_ELEVATION}
DETAIL_PROFILES=set(ALL_AR_PROFILES)-PLAN_PROFILES-ELEVATION_PROFILES

def classify_ar_profile(text):
    u=re.sub(r"\s+"," ",text or "").upper()
    if "ПЛАН РАССТАНОВКИ МЕБЕЛИ" in u or ("МЕБЕЛ" in u and "ПЛАН" in u):
        return PROFILE_FURNITURE
    opening=any(x in u for x in ("ДВЕР", "ОКОН", "ОКНО", "ЛЮК", "ПРОЕМ", "ПРОЁМ", "ВИТРАЖ"))
    # Интерьерная развёртка почти всегда содержит двери, виды и габариты. Если
    # проверять opening первым, она ошибочно становится «эскизом двери» и теряет
    # размерные цепочки стен (реальный пример АИ2: 580+580 против 1155 мм).
    if "РАЗВЕРТ" in u:return PROFILE_WALL_ELEVATION
    # Фронтальные виды и схемы заполнения проёмов важнее попутных ссылок на
    # лестницы, кладочные планы и дополнительных разрезов внутри описания.
    if opening and (any(x in u for x in ("ФРОНТАЛЬН", "ВИД СПЕРЕДИ", "ЭСКИЗ", "СХЕМЫ ДВЕР",
        "СХЕМА ДВЕР", "СХЕМЫ ЛЮК", "СХЕМА ЛЮК", "ЧЕРТЕЖ ДВЕР", "ЧЕРТЁЖ ДВЕР",
        "ЧЕРТЕЖ ЛЮК", "ЧЕРТЁЖ ЛЮК", "ВЕДОМОСТЬ ДВЕР", "ТАБЛИЦА ДВЕР", "СХЕМА ЗАПОЛНЕНИЯ",
        "СХЕМЫ ЗАПОЛНЕНИЯ")) or ("СХЕМ" in u and any(x in u for x in ("ГАБАРИТ", "МАРКИРОВ", "ОГНЕСТОЙКОСТ")))):
        return PROFILE_OPENING_DRAWING
    if opening and "ПЛАН" in u and any(x in u for x in ("ОТВЕРСТ", "ПРОЕМ", "ПРОЁМ")):
        return PROFILE_OPENING_PLAN
    if (("ПРИМЫКАН" in u and "ПОЛ" in u and "СТЕН" in u) or
        ("ГИДРОИЗОЛЯЦ" in u and "ПЛАВАЮЩ" in u and "ПОЛ" in u)):
        return PROFILE_FLOOR_WALL_JUNCTION
    if "ФАСАД" in u:return PROFILE_FACADE
    if ("ОГРАЖДЕНИ" in u or "ПЕРИЛ" in u) and any(x in u for x in ("УЗЕЛ", "ДЕТАЛ", "СХЕМ", "ЧЕРТЕЖ", "ЧЕРТЁЖ")):return PROFILE_RAILING
    if any(x in u for x in ("ЧЕРТЕЖ ЛЕСТНИЦ", "ЧЕРТЁЖ ЛЕСТНИЦ", "СХЕМА ЛЕСТНИЦ", "ПЛАН ЛЕСТНИЦ",
        "РАЗРЕЗ ЛЕСТНИЦ", "ЛЕСТНИЧНЫЙ МАРШ", "ЛЕСТНИЧНОГО МАРША")):return PROFILE_STAIR
    if "КРОВЛ" in u:
        if "ФУНДАМЕНТ" in u:return PROFILE_FOUNDATION
        if any(x in u for x in ("УЗЕЛ","ПРИМЫКАН","ВОРОНК","ПАРАПЕТ")):return PROFILE_ROOF_DETAIL
        return PROFILE_ROOF_PLAN
    if any(x in u for x in ("КЛАДОЧНЫЙ ПЛАН", "КЛАДОЧНОГО ПЛАНА", "ПЛАН КЛАДКИ", "СХЕМА КЛАДКИ")):
        if "ОТВЕРСТ" in u:return PROFILE_OPENING_PLAN
        if "УЗЛ" in u or "ДЕТАЛ" in u:return PROFILE_MASONRY_DETAIL
        return PROFILE_MASONRY_PLAN
    if any(x in u for x in ("УЗЕЛ КЛАДКИ", "УЗЕЛ КРЕПЛЕНИЯ КЛАДКИ", "КРЕПЛЕНИЕ ГАЗОБЕТОН",
        "КРЕПЛЕНИЯ ГАЗОБЕТОН", "КЛАДОЧНЫЙ УЗЕЛ")):return PROFILE_MASONRY_DETAIL
    if any(x in u for x in ("СХЕМА РАСКЛАДКИ БЛОКОВ", "СХЕМЫ РАСКЛАДКИ БЛОКОВ", "РЯД КЛАДКИ")):
        return PROFILE_MASONRY_PLAN
    if "СХЕМ" in u and "ВЕНТИЛЯЦИОНН" in u and "РЕШЕТ" in u:return PROFILE_DETAIL
    if "МАРКИРОВОЧН" in u:return PROFILE_MARKING
    if "ПОТОЛ" in u or "ОСВЕТИТЕЛЬНЫХ ПРИБОРОВ" in u:return PROFILE_CEILING
    if "РОЗЕТОК" in u or "ЭЛЕКТРООБОРУДОВАН" in u:return PROFILE_INTERIOR_ELECTRICAL
    if "МЕБЕЛ" in u:return PROFILE_FURNITURE
    if "ПОЛОВ" in u or "НАПОЛЬНЫХ ПОКРЫТИЙ" in u:return PROFILE_FLOOR_FINISH
    if "ОТДЕЛК" in u or "КВАРТИР" in u:return PROFILE_FINISH
    if any(x in u for x in ("АРХИТЕКТУРНЫЙ РАЗРЕЗ", "ПОПЕРЕЧНЫЙ РАЗРЕЗ", "ПРОДОЛЬНЫЙ РАЗРЕЗ",
        "ВЕРТИКАЛЬНЫЙ РАЗРЕЗ", "РАЗРЕЗ ЗДАНИЯ", "РАЗРЕЗ ПО ЗДАНИЮ")):return PROFILE_SECTION
    if "РАЗРЕЗ" in u and not any(x in u for x in ("ДОПОЛНИТЕЛЬНО ПОКАЗАН", "ТАКЖЕ ПОКАЗАН")):return PROFILE_SECTION
    if "УЗЕЛ" in u or "ПРИМЫКАН" in u:return PROFILE_DETAIL
    if "ПЛАН" in u:return PROFILE_FLOOR
    return None

PATTERNS=(
 ("room",re.compile(r"\b(?:квартир\w*\s*№?\s*\d+|сануз\w*|коридор\w*|холл\w*|кухн\w*|спальн\w*|гостин\w*|кладов\w*|лестничн\w*\s+клетк\w*|тамбур\w*|помещени\w*)\b",re.I)),
 ("opening",re.compile(r"\b(?:ДПМ|ДО|ДВ|Д|ОК|ВК|ЛЮК)\s*[-№]?\s*[A-ZА-Яа-я0-9.]+",re.I)),
 ("elevation",re.compile(r"(?<!\d)[+\-]\d{1,3}[,.]\d{3}(?!\d)")),
 ("dimension",re.compile(r"(?:\b\d{2,5}\s*[xх×]\s*\d{2,5}\b|\bR\s*=?\s*\d+(?:[,.]\d+)?|\b\d+(?:[,.]\d+)?\s*мм\b)",re.I)),
 ("material",re.compile(r"\b(?:кирпич\w*|газобетон\w*|пеноблок\w*|блок\w*\s+ячеист\w*|ГКЛ|ГВЛ|минват\w*|утеплител\w*|штукатур\w*|керамогранит\w*|плитк\w*|краск\w*|ламинат\w*|бетон\w*|сталь\w*|алюмини\w*|стекл\w*|мембран\w*|гидроизоляц\w*|Вибростек(?:-М)?|ЭППС|экструзионн\w*\s+пенополистирол\w*|Ц/п\s+раствор\s+М100|Биополь\s+ХПП|Техноэласт|эластичн\w*\s+гидро-?)\b",re.I)),
 ("construction_element",re.compile(r"\b(?:Ж/б\s+плита|перегородк\w*\s+из\s+блоков)\b",re.I)),
 ("layer_count",re.compile(r"\b\d+\s+сло(?:я|ев)\b",re.I)),
 ("installation_requirement",re.compile(r"\b(?:наносить\s+в\s+\d+\s+сло(?:я|ев)|рекомендуется\s+проклеить)\b",re.I)),
 ("finish_mark",re.compile(r"\b(?:СТ|ПЛ|ПТ|ПОТ|ПОЛ|ОТД)\s*[-.]\s*\d+[A-ZА-Яа-я0-9.]*",re.I)),
 ("furniture_or_equipment",re.compile(r"\b(?:шкаф\w*|стол\w*|стул\w*|диван\w*|кроват\w*|тумб\w*|кухонн\w*\s+гарнитур\w*|оборудован\w*|сантехприбор\w*)\b",re.I)),
 ("furniture_mark",re.compile(r"(?<![A-Za-zА-Яа-яЁё0-9])[MМ]\s*[-–]?\s*\d{1,3}(?![A-Za-zА-Яа-яЁё0-9])",re.I)),
 ("electrical_fixture",re.compile(r"\b(?:розетк\w*|выключател\w*|светильник\w*|бра\b|электровывод\w*|датчик\w*)\b",re.I)),
 ("roof_element",re.compile(r"\b(?:воронк\w*|парапет\w*|аэратор\w*|ходов\w*\s+дорожк\w*|водосток\w*|дефлектор\w*)\b",re.I)),
 ("stair_or_railing",re.compile(r"\b(?:лестниц\w*|ступен\w*|косоур\w*|тетив\w*|перил\w*|ограждени\w*|поручень\w*|балясин\w*)\b",re.I)),
 ("fire_rating",re.compile(r"\b(?:EI|EIS|REI)\s*\d+\b",re.I)),
 ("fastener",re.compile(r"\b(?:анкер\w*|болт\w*|саморез\w*|дюбел\w*|шпильк\w*|кронштейн\w*)\b",re.I)),
)

def _lines(page):
    result=[]
    for b in page.get_text("dict").get("blocks",[]):
        for line in b.get("lines",[]):
            text="".join(str(s.get("text") or "") for s in line.get("spans",[])).strip()
            if text:
                bbox=_bbox(page,line["bbox"]);result.append({"text":text,"bbox":bbox,"center":_center(bbox)})
    return result
def _facts(page,profile=None):
    nodes=[]
    for line in _lines(page):
        for kind,pattern in PATTERNS:
            for m in pattern.finditer(line["text"]):
                item=dict(line);item["text"]=re.sub(r"\s+"," ",m.group()).strip(" ;,.");nodes.append(_node(page,item,kind,len(nodes)+1,source_label=line["text"]))
        if profile in {PROFILE_FLOOR_WALL_JUNCTION,PROFILE_WALL_ELEVATION} and re.fullmatch(r"\s*\d{2,5}\s*",line["text"]):
            item=dict(line);item["text"]=line["text"].strip();nodes.append(_node(page,item,"dimension",len(nodes)+1,source_label=line["text"]))
    return _unique(nodes)
def _architectural_views(page):
    result=_views(page);pattern=re.compile(r"^(?:Развертка|Фасад|Сечение|Фрагмент|Схема\s+(?:устройства|примыкания))\s*[^,;]{0,50}$",re.I)
    for line in _lines(page):
        if pattern.search(line["text"]):result.append({"id":f"view-{len(result)+1}","container_type":"architectural_view","label":line["text"],
          "bbox_page":_bbox_norm(page,line["bbox"]),"anchor_x":round(line["center"][0],3),"anchor_y":round(line["center"][1],3),"member_ids":[]})
    return result
def _nearest_room(nodes,page):
    rooms=[n for n in nodes if n["node_type"]=="room"]
    for node in nodes:
        if node in rooms or not rooms:continue
        room=min(rooms,key=lambda r:math.hypot(node["x"]-r["x"],node["y"]-r["y"]));distance=math.hypot(node["x"]-room["x"],node["y"]-room["y"])
        if distance<max(page.rect.width,page.rect.height)*.2:node["nearest_room_label"]=room["label"];node["room_binding_state"]="пространственно ближайшая подпись"
def _raster_nodes(page,start):
    result=[]
    for info in page.get_image_info(xrefs=True):
        bbox=_bbox(page,info.get("bbox") or (0,0,0,0))
        if (bbox[2]-bbox[0])*(bbox[3]-bbox[1])<page.rect.width*page.rect.height*.03:continue
        item={"text":f"растровая область {len(result)+1}","bbox":bbox,"center":_center(bbox)}
        result.append(_node(page,item,"raster_region",start+len(result)+1,field_state="геометрия растра без координатных подписей"))
    return result
def _plan(page,pdf,block_id,profile,subtype):
    nodes=_facts(page,profile);nodes=_unique(nodes+_raster_nodes(page,len(nodes)));axes=_axes(page);_nearest_room(nodes,page);segments=_inside_segments(page,vivid=False,cap=30000)
    graph=_base(page,pdf,block_id,profile,subtype,nodes=nodes,validation={"axes_total":len(axes),"rooms_total":sum(n["node_type"]=="room" for n in nodes),
      "openings_total":sum(n["node_type"]=="opening" for n in nodes),"materials_total":sum(n["node_type"]=="material" for n in nodes),
      "furniture_total":sum(n["node_type"]=="furniture_or_equipment" for n in nodes),"electrical_fixtures_total":sum(n["node_type"]=="electrical_fixture" for n in nodes),
      "roof_elements_total":sum(n["node_type"]=="roof_element" for n in nodes),"physical_line_segments_total":len(segments),"topology_state":"architectural_spatial_plan"},
      warnings=["ближайшая подпись помещения не заменяет восстановленный полигон комнаты","линии стен сохраняются как физическая геометрия, а не инженерная сеть"])
    graph["grid"]={"axes":axes};return graph
def _view_graph(page,pdf,block_id,profile,subtype):
    nodes=_facts(page,profile);nodes=_unique(nodes+_raster_nodes(page,len(nodes)));views=_architectural_views(page)
    if not views:views=[{"id":"view-1","container_type":"architectural_view","label":subtype,"bbox_page":[0,0,1,1],"anchor_x":page.rect.width/2,"anchor_y":page.rect.height/2,"member_ids":[]}]
    _assign_nodes_to_views(nodes,views);segments=_inside_segments(page,vivid=False,cap=30000);edges=[]
    if profile==PROFILE_ROOF_DETAIL:
        layers=sorted([n for n in nodes if n["node_type"]=="material"],key=lambda n:(n["y"],n["x"]))
        for a,b in zip(layers,layers[1:]):
            if abs(a["x"]-b["x"])<page.rect.width*.4:edges.append({"id":f"edge-{len(edges)+1}","from":a["id"],"to":b["id"],"edge_type":"порядок слоёв","edge_state":"vertical_order_in_detail"})
    return _base(page,pdf,block_id,profile,subtype,nodes=nodes,containers=views,edges=edges,validation={"views_total":len(views),
      "elevations_total":sum(n["node_type"]=="elevation" for n in nodes),"dimensions_total":sum(n["node_type"]=="dimension" for n in nodes),
      "openings_total":sum(n["node_type"]=="opening" for n in nodes),"materials_total":sum(n["node_type"]=="material" for n in nodes),
      "construction_elements_total":sum(n["node_type"]=="construction_element" for n in nodes),
      "installation_requirements_total":sum(n["node_type"]=="installation_requirement" for n in nodes),
      "fasteners_total":sum(n["node_type"]=="fastener" for n in nodes),"stair_railing_parts_total":sum(n["node_type"]=="stair_or_railing" for n in nodes),
      "physical_line_segments_total":len(segments),"layer_order_edges_total":len(edges),"topology_state":"architectural_views_and_parts"},
      warnings=["виды и детали образуют физическую иерархию, а не потоковую сеть","порядок слоёв создаётся только внутри одного координатного вида"])
def _dispatch(page,pdf,block_id,profile,subtype):
    graph=_plan(page,pdf,block_id,profile,subtype) if profile in PLAN_PROFILES else _view_graph(page,pdf,block_id,profile,subtype) if profile in ELEVATION_PROFILES|DETAIL_PROFILES else None
    if graph:
        text=page.get_text().strip();graph["validation"].update({"pdf_text_characters":len(text),"pdf_words_total":len(page.get_text("words")),"source_layer_state":"text_available" if text else "no_pdf_text_layer"})
    return graph
def build_ar_graph(pdf_path:Path,*,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        with fitz.open(str(pdf_path)) as d:
            if d.page_count!=1:return None
            p=profile_hint or classify_ar_profile(d[0].get_text());return _dispatch(d[0],Path(pdf_path),block_id,p,subtype_hint or "архитектурный блок")
    except Exception:return None
def build_ar_graph_from_source(pdf_path:Path,*,page_index,bbox_norm,polygon_norm=None,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        source=fitz.open(str(pdf_path));cropped=None
        try:
            sp=source[page_index];w,h=sp.rect.width,sp.rect.height;crop=fitz.Rect(bbox_norm[0]*w,bbox_norm[1]*h,bbox_norm[2]*w,bbox_norm[3]*h)&sp.rect
            ur=crop*sp.derotation_matrix;ur.normalize();off=sp.cropbox_position;ur=fitz.Rect(ur.x0+off.x,ur.y0+off.y,ur.x1+off.x,ur.y1+off.y)
            cropped=fitz.open();cropped.insert_pdf(source,from_page=page_index,to_page=page_index);target=cropped[0]
            if polygon_norm:
                inv=~sp.transformation_matrix;pts=[tuple(fitz.Point(float(x)*w,float(y)*h)*sp.derotation_matrix*inv) for x,y in polygon_norm];_clip_copied_page(target,pts)
            target.set_cropbox(ur);p=profile_hint or classify_ar_profile(target.get_text());return _dispatch(target,Path(pdf_path),block_id,p,subtype_hint or "архитектурный блок")
        finally:
            if cropped is not None:cropped.close()
            source.close();fitz.TOOLS.store_shrink(100)
    except Exception:return None
def add_ar_secondary_description(graph,text):
    if graph is not None and str(text or "").strip():graph["secondary_description"]={"source":"описание исходного блока","text":re.sub(r"\s+"," ",str(text)).strip(),"evidence_state":"без координат","warning":"не участвует в рёбрах"}
    return graph
def evaluate_ar_gate(graph):
    if not graph:return {"use":False,"complete":False,"reasons":["граф не построен"],"complete_reasons":[]}
    v=graph["validation"];e=v.get("nodes_total",0)+v.get("physical_line_segments_total",0);reasons=[] if e else ["нет предметной или физической структуры"];partial=[]
    if v.get("source_layer_state")=="no_pdf_text_layer":partial.append("нет текстового слоя PDF")
    depth="semantic_hierarchy" if graph["profile_id"] in PLAN_PROFILES else "physical_hierarchy";v["description_depth"]=depth;graph["readiness"]={"complete":not partial,"reasons":partial}
    return {"use":not reasons,"complete":not partial,"reasons":reasons,"complete_reasons":partial,"metrics":v}
PROFILE_RU={PROFILE_MASONRY_PLAN:"Кладочный план",PROFILE_MASONRY_DETAIL:"Узел кладки",PROFILE_STAIR:"Лестница",PROFILE_RAILING:"Ограждение",
 PROFILE_SECTION:"Архитектурный разрез",PROFILE_OPENING_PLAN:"План отверстий",PROFILE_FACADE:"Фасад",PROFILE_DETAIL:"Архитектурный узел",
 PROFILE_MARKING:"Маркировочный план",PROFILE_OPENING_DRAWING:"Эскиз заполнения проёма",PROFILE_FLOOR:"План этажа",PROFILE_ROOF_PLAN:"План кровли",
 PROFILE_ROOF_DETAIL:"Узел кровли",PROFILE_FINISH:"Отделка помещений",PROFILE_FOUNDATION:"Фундамент под оборудование",
 PROFILE_WALL_ELEVATION:"Развёртка стены",PROFILE_CEILING:"План потолка и освещения",PROFILE_FLOOR_FINISH:"План полов",
 PROFILE_INTERIOR_ELECTRICAL:"Интерьерный план электрооборудования",PROFILE_FURNITURE:"План мебели и оборудования",
 PROFILE_FLOOR_WALL_JUNCTION:"Узел примыкания пола и стены"}
NODE_RU={"room":"помещение","opening":"дверь, окно или проём","elevation":"отметка","dimension":"размер","material":"материал",
 "finish_mark":"марка отделки","furniture_or_equipment":"мебель или оборудование","electrical_fixture":"электроустановочное изделие",
 "roof_element":"элемент кровли","stair_or_railing":"элемент лестницы или ограждения","fire_rating":"огнестойкость","fastener":"крепёж"}
NODE_RU.update({"construction_element":"элемент конструкции","layer_count":"количество слоёв","installation_requirement":"требование к монтажу"})
NODE_RU["furniture_mark"]="марка мебели"
NODE_RU["raster_region"]="растровая область без читаемых подписей"
def render_ar_markdown(graph):
    v=graph["validation"];lines=[f"# Эталонная текстовая разметка АР: {PROFILE_RU.get(graph['profile_id'],'Архитектурный блок')}","",f"**Источник:** {graph['source']['pdf_file']}",
      "**Метод:** текст с координатами и физическая CAD-геометрия PDF; неподтверждённые отношения не добавляются.","","## 1. Краткий результат","",
      f"Узлов: {v.get('nodes_total',0)}; видов: {v.get('containers_total',0)}; явных рёбер: {v.get('edges_total',0)}.",
      f"**Уровень описания:** {'предметная структура архитектурного плана' if v.get('description_depth')=='semantic_hierarchy' else 'физическая структура видов и деталей'}.","","### Архитектурное дерево","","```text",PROFILE_RU.get(graph['profile_id'],'блок АР').lower()]
    counts=collections.Counter(n["node_type"] for n in graph.get("nodes",[]))
    for i,(k,c) in enumerate(counts.most_common(14)):lines.append(("└──" if i==min(len(counts),14)-1 else "├──")+f" {NODE_RU.get(k,k)}: {c}")
    lines += ["```","","## 2. Состав и параметры",""];group=collections.defaultdict(list)
    for n in graph.get("nodes",[]):group[n["node_type"]].append(n["label"])
    for k,labels in group.items():
        counts_by_label=collections.Counter(labels)
        rendered=[f"{label} × {count}" if count>1 else label for label,count in counts_by_label.items()]
        lines.append(f"- **{NODE_RU.get(k,k)} — {len(labels)}:** {', '.join(rendered[:30])}{' …' if len(rendered)>30 else ''}")
    lines += ["","## 3. Виды и отношения",""]
    if graph.get("containers"):
        for i,c in enumerate(graph["containers"][:30],1):
            label=c.get("label") or "без подписи"
            if re.fullmatch(r"[a-z0-9_]+",str(label)):label="основной вид"
            lines.append(f"- Вид {i}: {label}; элементов {len(c.get('member_ids') or [])}.")
    if graph.get("edges"):
        lab={n["id"]:n["label"] for n in graph["nodes"]}
        for e in graph["edges"][:80]:lines.append(f"- {lab.get(e['from'],'слой')} → {lab.get(e['to'],'слой')}: координатный порядок слоёв внутри узла.")
    if not graph.get("containers") and not graph.get("edges"):lines.append("- План сохраняет пространственные подписи и геометрию; потоковая топология неприменима.")
    if graph.get("secondary_description"):lines += ["","## 4. Дополнительное описание без координат","",graph["secondary_description"]["text"],"","Описание не создаёт узлы и рёбра."]
    lines += ["",f"## {'5' if graph.get('secondary_description') else '4'}. Полнота и ограничения","","Доступная структура описана полностью." if graph.get("readiness",{}).get("complete") else "Полнота ограничена источником."]
    lines += [f"- {x}" for x in graph.get("warnings",[])];return "\n".join(lines)+"\n"
