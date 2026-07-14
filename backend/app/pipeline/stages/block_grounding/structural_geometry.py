"""Предметные графы железобетонных и металлических конструкций."""
from __future__ import annotations
import collections,re
from pathlib import Path
from .hvac_geometry import _axes,_base,_bbox,_bbox_norm,_center,_clip_copied_page,_node,_unique,_views,_assign_nodes_to_views
from .general_plan_geometry import _inside_segments
KJ_FORMWORK="kj_formwork_plan";KJ_REINFORCEMENT="kj_reinforcement_plan";KJ_SECTION="kj_reinforcement_section";KJ_MARKING="kj_marking_plan";KJ_EMBEDDED="kj_embedded_parts";KJ_DETAIL="kj_structural_detail"
ALL_KJ_PROFILES=(KJ_FORMWORK,KJ_REINFORCEMENT,KJ_SECTION,KJ_MARKING,KJ_EMBEDDED,KJ_DETAIL)
KM_LAYOUT="km_layout_plan";KM_MEMBER="km_member_drawing";KM_CONNECTION="km_connection_detail";KM_LADDER="km_ladder_drawing";KM_FACADE_LAYOUT="km_facade_layout";KM_FACADE_DETAIL="km_facade_detail";KM_MOCKUP="km_mockup"
ALL_KM_PROFILES=(KM_LAYOUT,KM_MEMBER,KM_CONNECTION,KM_LADDER,KM_FACADE_LAYOUT,KM_FACADE_DETAIL,KM_MOCKUP)
PLAN={KJ_FORMWORK,KJ_REINFORCEMENT,KJ_MARKING,KJ_EMBEDDED,KM_LAYOUT,KM_FACADE_LAYOUT,KM_MOCKUP}
PATTERNS=(
 ("structural_mark",re.compile(r"\b(?:Стм|Км|Пм|Бм|ЗД|КЖ|Ст)\s*[-№]?\s*[A-ZА-Яа-я0-9.\-]+",re.I)),
 ("rebar",re.compile(r"(?:[Ø∅⌀ф]\s*\d{1,3}\s*(?:А|A)\s*\d{3,4}[A-ZА-Яа-я]*|\b\d{1,3}\s*(?:А|A)\s*\d{3,4}[A-ZА-Яа-я]*|\b(?:арматур\w*\s+)?[АA]\s*(?:400|500)\s*[СC]?)",re.I)),
 ("position",re.compile(r"\b(?:поз\.?|позиц(?:ия|ии))\s*№?\s*\d+[A-ZА-Яа-я]?",re.I)),
 ("spacing",re.compile(r"\bшаг\s*\d+(?:[,.]\d+)?\s*(?:мм)?",re.I)),
 ("elevation",re.compile(r"(?<!\d)[+\-]\d{1,3}[,.]\d{3}(?!\d)")),
 ("dimension",re.compile(r"(?:\b\d{2,5}\s*[xх×]\s*\d{2,5}(?:\s*[xх×]\s*\d{2,5})?\b|\b\d+(?:[,.]\d+)?\s*мм\b)",re.I)),
 ("concrete",re.compile(r"\bВ\s*\d{2,3}(?:[,.]\d+)?\s*(?:W\s*\d+)?\s*(?:F\s*\d+)?",re.I)),
 ("cover",re.compile(r"\bзащитн\w*\s+сло\w*[^\n;]{0,35}",re.I)),
 ("embedded",re.compile(r"\b(?:закладн\w*|анкерн\w*|шпильк\w*|пластин\w*)\b",re.I)),
 ("section_mark",re.compile(r"\b(?:разрез|сечение)\s*\d+[\-–]\d+",re.I)),
 ("steel_profile",re.compile(r"\b(?:L|HEA|HEB|IPE|UPN|Швеллер|Уголок|Труба)\s*[-]?\s*\d+[A-ZА-Яа-я0-9xх×.\-]*",re.I)),
 ("bolt",re.compile(r"\b(?:болт|анкер)\w*\s*(?:М|M)\s*\d+",re.I)),
 ("weld",re.compile(r"\b(?:сварн\w*\s+шов\w*|катет\w*\s*\d+)\b",re.I)),
 ("plate",re.compile(r"\b(?:пластин\w*|лист)\s*[-]?\s*\d+[xх×]\d+(?:[xх×]\d+)?",re.I)),
)
def classify_kj_profile(text):
    u=re.sub(r"\s+"," ",text or "").upper()
    if "СХЕМА РАСПОЛОЖЕНИЯ ЛЕСТНИЦ" in u:return KJ_MARKING
    if ("АРМИРОВАН" in u or "АРМАТУР" in u or "ВЫПУСК" in u) and ("СЕЧЕНИ" in u or "РАЗРЕЗ" in u):return KJ_SECTION
    if "ЗАКЛАДН" in u and any(x in u for x in ("ПЛАН", "СХЕМА", "ДЕТАЛ", "РАСПОЛОЖ")):return KJ_EMBEDDED
    if "ОПАЛУБ" in u:return KJ_FORMWORK
    if "МАРКИРОВОЧ" in u:return KJ_MARKING
    if "АРМИРОВАН" in u or "АРМАТУР" in u or "ВЫПУСК" in u:return KJ_REINFORCEMENT
    if "ЗАКЛАДН" in u:return KJ_EMBEDDED
    if "СЕЧЕНИ" in u or "РАЗРЕЗ" in u:return KJ_SECTION
    if any(x in u for x in ("ВЕРТИКАЛЬНЫЕ КОНСТРУКЦ","ПЛИТА ПЕРЕКРЫТИЯ", "ЖЕЛЕЗОБЕТОННЫЙ УЗЕЛ", "УЗЕЛ ЖЕЛЕЗОБЕТОН")):return KJ_DETAIL
def classify_km_profile(text):
    u=re.sub(r"\s+"," ",text or "").upper()
    if "НАВЕСН" in u or "НВФ" in u or "ФАСАДН" in u:return KM_FACADE_DETAIL if "УЗЕЛ" in u else KM_FACADE_LAYOUT
    if "MOCKUP" in u or "МОКАП" in u:return KM_MOCKUP
    if "СТРЕМЯНК" in u or "ЛЕСТНИЦ" in u:return KM_LADDER
    if ("УЗЛ" in u or "СОЕДИНЕН" in u or "КРЕПЛЕН" in u) and any(x in u for x in ("МЕТАЛ", "БОЛТ", "СВАР", "БАЛК", "КОЛОНН", "ПРОФИЛ", "АНКЕР")):return KM_CONNECTION
    if "СХЕМ" in u and "РАСПОЛОЖЕНИ" in u:return KM_LAYOUT
    if ("СПЕЦИФИКАЦ" in u and any(x in u for x in ("ЭЛЕМЕНТ", "ПРОФИЛ"))) or "ПРОФИЛЬНЫХ ЭЛЕМЕНТ" in u or "ГЕОМЕТРИЧЕСК" in u and "СЕЧЕНИ" in u:return KM_MEMBER
    if "УЗЕЛ" in u or "СОЕДИНЕН" in u:return KM_CONNECTION
    if any(x in u for x in ("РАМА","КОЛОНН","БАЛК","КАРКАС")):return KM_MEMBER
    if ("РАЗРЕЗ" in u or "СЕЧЕНИ" in u) and any(x in u for x in ("МЕТАЛЛИЧ", "МЕТАЛЛОКОНСТРУК", "МОСТ", "ПЛОЩАДК")):return KM_MEMBER
def _lines(page):
    out=[]
    for b in page.get_text("dict").get("blocks",[]):
        for line in b.get("lines",[]):
            text="".join(str(s.get("text") or "") for s in line.get("spans",[])).strip()
            if text:
                bb=_bbox(page,line["bbox"]);out.append({"text":text,"bbox":bb,"center":_center(bb)})
    return out
def _facts(page):
    nodes=[]
    for line in _lines(page):
        for kind,p in PATTERNS:
            for m in p.finditer(line["text"]):
                item=dict(line);item["text"]=re.sub(r"\s+"," ",m.group()).strip(" ;,.");nodes.append(_node(page,item,kind,len(nodes)+1,source_label=line["text"]))
    for info in page.get_image_info(xrefs=True):
        bb=_bbox(page,info.get("bbox") or (0,0,0,0))
        if (bb[2]-bb[0])*(bb[3]-bb[1])>page.rect.width*page.rect.height*.04:
            item={"text":"растровая область","bbox":bb,"center":_center(bb)};nodes.append(_node(page,item,"raster_region",len(nodes)+1,field_state="без текстовых координат"))
    return _unique(nodes)
def _dispatch(page,pdf,block_id,profile,subtype):
    nodes=_facts(page)
    if str(profile or "").startswith("km_") and not nodes and page.get_contents():
        item={"text":"векторная геометрия металлической конструкции","bbox":(0,0,float(page.rect.width),float(page.rect.height)),"center":(page.rect.width/2,page.rect.height/2)}
        nodes=[_node(page,item,"vector_geometry_region",1,field_state="векторный PDF без распознанных подписей")]
    segments=[] if str(profile or "").startswith("km_") else _inside_segments(page,vivid=False,cap=40000);axes=_axes(page);containers=[]
    if profile not in PLAN:
        containers=_views(page) or [{"id":"view-1","container_type":"structural_view","label":"основной вид","bbox_page":[0,0,1,1],"anchor_x":page.rect.width/2,"anchor_y":page.rect.height/2,"member_ids":[]}];_assign_nodes_to_views(nodes,containers)
    graph=_base(page,pdf,block_id,profile,subtype,nodes=nodes,containers=containers,validation={"axes_total":len(axes),"marks_total":sum(n["node_type"]=="structural_mark" for n in nodes),
      "rebar_total":sum(n["node_type"]=="rebar" for n in nodes),"positions_total":sum(n["node_type"]=="position" for n in nodes),"spacings_total":sum(n["node_type"]=="spacing" for n in nodes),
      "elevations_total":sum(n["node_type"]=="elevation" for n in nodes),"dimensions_total":sum(n["node_type"]=="dimension" for n in nodes),"embedded_facts_total":sum(n["node_type"]=="embedded" for n in nodes),
      "physical_line_segments_total":len(segments),"vector_geometry_state":"preserved_not_expanded" if str(profile or "").startswith("km_") else "expanded_segments",
      "views_total":len(containers),"topology_state":"structural_plan" if profile in PLAN else "structural_views_and_sections"},
      warnings=["арматурные марки и позиции сохраняются по координатам; попарная связь не создаётся без выноски или отдельного правила","линии опалубки и арматуры являются физической геометрией, а не потоковой сетью"])
    graph["grid"]={"axes":axes};text=page.get_text().strip();graph["validation"].update({"pdf_text_characters":len(text),"source_layer_state":"text_available" if text else "no_pdf_text_layer"});return graph
def build_kj_graph(path:Path,*,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        with fitz.open(str(path)) as d:
            if d.page_count!=1:return None
            return _dispatch(d[0],Path(path),block_id,profile_hint or classify_kj_profile(d[0].get_text()),subtype_hint or "конструктивный блок")
    except Exception:return None
def build_km_graph(path:Path,*,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        with fitz.open(str(path)) as d:
            if d.page_count!=1:return None
            return _dispatch(d[0],Path(path),block_id,profile_hint or classify_km_profile(d[0].get_text()),subtype_hint or "металлическая конструкция")
    except Exception:return None
def build_kj_graph_from_source(path:Path,*,page_index,bbox_norm,polygon_norm=None,block_id=None,profile_hint=None,subtype_hint=None):
    try:
        import fitz
        source=fitz.open(str(path));cropped=None
        try:
            sp=source[page_index];w,h=sp.rect.width,sp.rect.height;cr=fitz.Rect(bbox_norm[0]*w,bbox_norm[1]*h,bbox_norm[2]*w,bbox_norm[3]*h)&sp.rect;ur=cr*sp.derotation_matrix;ur.normalize();off=sp.cropbox_position;ur=fitz.Rect(ur.x0+off.x,ur.y0+off.y,ur.x1+off.x,ur.y1+off.y);cropped=fitz.open();cropped.insert_pdf(source,from_page=page_index,to_page=page_index);target=cropped[0]
            if polygon_norm:
                inv=~sp.transformation_matrix;pts=[tuple(fitz.Point(float(x)*w,float(y)*h)*sp.derotation_matrix*inv) for x,y in polygon_norm];_clip_copied_page(target,pts)
            target.set_cropbox(ur);return _dispatch(target,Path(path),block_id,profile_hint or classify_kj_profile(target.get_text()),subtype_hint or "конструктивный блок")
        finally:
            if cropped is not None:cropped.close()
            source.close()
    except Exception:return None
def add_structural_secondary(graph,text):
    if graph is not None and str(text or "").strip():graph["secondary_description"]={"text":re.sub(r"\s+"," ",str(text)).strip(),"evidence_state":"без координат","warning":"не создаёт связей"}
    return graph
def evaluate_structural_gate(graph):
    if not graph:return {"use":False,"complete":False,"reasons":["граф не построен"],"complete_reasons":[]}
    v=graph["validation"];e=v.get("nodes_total",0)+v.get("physical_line_segments_total",0);reasons=[] if e else ["нет структуры"];partial=[]
    if v.get("source_layer_state")=="no_pdf_text_layer":partial.append("нет текстового слоя PDF")
    depth="semantic_hierarchy" if graph["profile_id"] in PLAN else "physical_hierarchy";v["description_depth"]=depth;graph["readiness"]={"complete":not partial,"reasons":partial};return {"use":not reasons,"complete":not partial,"reasons":reasons,"complete_reasons":partial,"metrics":v}
PROFILE_RU={KJ_FORMWORK:"Опалубочный план",KJ_REINFORCEMENT:"План армирования",KJ_SECTION:"Сечение армирования",KJ_MARKING:"Маркировочная схема",KJ_EMBEDDED:"Закладные детали",KJ_DETAIL:"Железобетонный узел"}
PROFILE_RU.update({KM_LAYOUT:"Монтажная схема металлоконструкций",KM_MEMBER:"Чертёж металлического элемента",KM_CONNECTION:"Узел соединения",KM_LADDER:"Стремянка или лестница",KM_FACADE_LAYOUT:"Монтажная схема фасадной системы",KM_FACADE_DETAIL:"Узел фасадной системы",KM_MOCKUP:"Фрагмент мокапа"})
NODE_RU={"structural_mark":"марка конструкции","rebar":"арматура","position":"позиция","spacing":"шаг арматуры","elevation":"отметка","dimension":"размер","concrete":"бетон","cover":"защитный слой","embedded":"закладной или анкерный элемент","section_mark":"обозначение сечения","raster_region":"растровая область"}
NODE_RU.update({"steel_profile":"стальной профиль","bolt":"болт или анкер","weld":"сварной шов","plate":"пластина"})
NODE_RU["vector_geometry_region"]="векторная геометрия без читаемых марок"
def render_structural_markdown(graph):
    v=graph["validation"];discipline="КМ" if graph["profile_id"].startswith("km_") else "КЖ";lines=[f"# Эталонная текстовая разметка {discipline}: {PROFILE_RU.get(graph['profile_id'],'Конструктивный блок')}","",f"**Источник:** {graph['source']['pdf_file']}","**Метод:** текст с координатами и физическая CAD-геометрия PDF; неподтверждённые связи не добавляются.","","## 1. Краткий результат","",f"Узлов: {v.get('nodes_total',0)}; видов: {v.get('containers_total',0)}.",f"**Уровень:** {'предметная структура плана' if v.get('description_depth')=='semantic_hierarchy' else 'физическая структура сечений и деталей'}.","","### Конструктивное дерево","","```text",PROFILE_RU.get(graph['profile_id'],f'блок {discipline}').lower()]
    counts=collections.Counter(n["node_type"] for n in graph["nodes"])
    for i,(k,c) in enumerate(counts.most_common(12)):lines.append(("└──" if i==min(len(counts),12)-1 else "├──")+f" {NODE_RU.get(k,k)}: {c}")
    lines += ["```","","## 2. Состав",""];group=collections.defaultdict(list)
    for n in graph["nodes"]:group[n["node_type"]].append(n["label"])
    for k,labs in group.items():lines.append(f"- **{NODE_RU.get(k,k)} — {len(labs)}:** {', '.join(list(dict.fromkeys(labs))[:30])}{' …' if len(set(labs))>30 else ''}")
    if graph.get("secondary_description"):lines += ["","## 3. Дополнительное описание без координат","",graph["secondary_description"]["text"],"","Описание не создаёт рёбра."]
    lines += ["",f"## {'4' if graph.get('secondary_description') else '3'}. Полнота и ограничения","","Доступная структура описана полностью." if graph.get("readiness",{}).get("complete") else "Полнота ограничена источником."]+[f"- {x}" for x in graph.get("warnings",[])];return "\n".join(lines)+"\n"
