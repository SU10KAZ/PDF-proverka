"""Графы технологических планов автостоянки, лифтов и мусороудаления."""
from __future__ import annotations
import collections,re
from pathlib import Path
from .hvac_geometry import _axes,_base,_bbox,_center,_clip_copied_page,_node,_unique,_views,_assign_nodes_to_views
from .general_plan_geometry import _inside_segments
PARKING="tx_parking_plan";PARKING_DETAIL="tx_parking_detail";LIFT_PLAN="tx_lift_plan";LIFT_SECTION="tx_lift_section";LIFT_EQUIPMENT="tx_lift_equipment";LIFT_ASSIGNMENT="tx_lift_assignment";WASTE_PLAN="tx_waste_plan";WASTE_DETAIL="tx_waste_detail"
ALL_TX_PROFILES=(PARKING,PARKING_DETAIL,LIFT_PLAN,LIFT_SECTION,LIFT_EQUIPMENT,LIFT_ASSIGNMENT,WASTE_PLAN,WASTE_DETAIL);PLAN={PARKING,LIFT_PLAN,WASTE_PLAN}
PATTERNS=(("lift",re.compile(r"\bЛ\s*\d+\.\d+[ПГ]?\b",re.I)),("parking_space",re.compile(r"\b(?:м/м|машиномест\w*)\s*№?\s*\d+",re.I)),
 ("accessibility",re.compile(r"\b(?:МГН|инвалид\w*|маломобильн\w*)\b",re.I)),("parking_device",re.compile(r"\b(?:колесоотбойник\w*|демпфер\w*|дорожн\w*\s+знак\w*|разметк\w*)\b",re.I)),
 ("shaft",re.compile(r"\b(?:лифтов\w*\s+шахт\w*|шахт\w*\s+лифт\w*)\b",re.I)),("lift_device",re.compile(r"\b(?:кабин\w*|противовес\w*|лебедк\w*|шкаф\w*\s+управлен\w*|буфер\w*|ограничител\w*|монтажн\w*\s+крюк\w*|двер\w*\s+шахт\w*)\b",re.I)),
 ("capacity",re.compile(r"\b\d+(?:[,.]\d+)?\s*(?:кг|чел\.)\b",re.I)),("speed",re.compile(r"\b\d+(?:[,.]\d+)?\s*м\s*/\s*с\b",re.I)),
 ("elevation",re.compile(r"(?<!\d)[+\-]\d{1,3}[,.]\d{3}(?!\d)")),("dimension",re.compile(r"\b\d{2,5}\s*[xх×]\s*\d{2,5}\b",re.I)),
 ("fire_rating",re.compile(r"\b(?:EI|EIS)\s*\d+\b",re.I)),("waste_device",re.compile(r"\b(?:мусоропровод\w*|ствол\w*|загрузочн\w*\s+клапан\w*|контейнер\w*|мусоросборн\w*|шибер\w*)\b",re.I)))
def classify_tx_profile(text):
 u=re.sub(r"\s+"," ",text or "").upper()
 if any(x in u for x in ("МУСОРОУДАЛ", "МУСОРОПРОВОД", "МУСОРОСБОР")):
  return WASTE_DETAIL if any(x in u for x in ("УЗЕЛ", "ДЕТАЛ", "РАЗРЕЗ", "СЕЧЕНИ")) else WASTE_PLAN
 if ("АВТОСТОЯН" in u or "МАШИНОМЕСТ" in u or "ПАРКИНГ" in u) and "ПЛАН" in u:
  return PARKING_DETAIL if any(x in u for x in ("УЗЕЛ РАМП", "ДЕТАЛЬ РАМП", "СХЕМА УСТАНОВКИ КОЛЕСООТБОЙ", "УЗЕЛ РАЗМЕТК")) else PARKING
 if "ЛИФТ" in u or "ШАХТ" in u:
  if "ЗАДАНИЕ" in u:return LIFT_ASSIGNMENT
  if any(x in u for x in ("ДВЕРЬ ШАХТЫ","ШКАФОВ УПРАВЛЕНИЯ","МОНТАЖНЫХ КРЮКОВ", "ОБОРУДОВАНИЕ ЛИФТА", "ОБЩИЙ ВИД ЛИФТА")):return LIFT_EQUIPMENT
  if "РАЗРЕЗ" in u or "СЕЧЕНИ" in u:return LIFT_SECTION
  return LIFT_PLAN
 if any(x in u for x in ("ПОДЪЕМН", "ПОДЪЁМН", "ГИДРАВЛИЧЕСКАЯ СТАНЦИЯ", "ПОДЪЕМНОЙ ПЛАТФОРМ", "ПОДЪЁМНОЙ ПЛАТФОРМ")):
  return LIFT_EQUIPMENT
 if "АВТОСТОЯН" in u or "МАШИНОМЕСТ" in u:return PARKING_DETAIL if any(x in u for x in ("РАМП","КОЛЕСООТБОЙ","РАЗМЕТК")) else PARKING
def _lines(page):
 out=[]
 for b in page.get_text("dict").get("blocks",[]):
  for line in b.get("lines",[]):
   text="".join(str(s.get("text") or "") for s in line.get("spans",[])).strip()
   if text:bb=_bbox(page,line["bbox"]);out.append({"text":text,"bbox":bb,"center":_center(bb)})
 return out
def _facts(page):
 nodes=[]
 for line in _lines(page):
  for kind,p in PATTERNS:
   for m in p.finditer(line["text"]):item=dict(line);item["text"]=re.sub(r"\s+"," ",m.group()).strip(" ;,.");nodes.append(_node(page,item,kind,len(nodes)+1,source_label=line["text"]))
 for info in page.get_image_info(xrefs=True):
  bb=_bbox(page,info.get("bbox") or (0,0,0,0))
  if (bb[2]-bb[0])*(bb[3]-bb[1])>page.rect.width*page.rect.height*.04:item={"text":"растровая область","bbox":bb,"center":_center(bb)};nodes.append(_node(page,item,"raster_region",len(nodes)+1))
 return _unique(nodes)
def _dispatch(page,pdf,bid,profile,subtype):
 nodes=_facts(page);axes=_axes(page);segments=_inside_segments(page,vivid=False,cap=30000);views=[]
 if profile not in PLAN:views=_views(page) or [{"id":"view-1","container_type":"technology_view","label":"основной вид","bbox_page":[0,0,1,1],"anchor_x":page.rect.width/2,"anchor_y":page.rect.height/2,"member_ids":[]}];_assign_nodes_to_views(nodes,views)
 g=_base(page,pdf,bid,profile,subtype,nodes=nodes,containers=views,validation={"axes_total":len(axes),"lifts_total":sum(n['node_type']=='lift' for n in nodes),"parking_spaces_total":sum(n['node_type']=='parking_space' for n in nodes),"lift_devices_total":sum(n['node_type']=='lift_device' for n in nodes),"parking_devices_total":sum(n['node_type']=='parking_device' for n in nodes),"waste_devices_total":sum(n['node_type']=='waste_device' for n in nodes),"elevations_total":sum(n['node_type']=='elevation' for n in nodes),"physical_line_segments_total":len(segments),"views_total":len(views),"topology_state":"technology_plan_or_views"},warnings=["положение оборудования сохраняется по координатам; технологическая связь не создаётся только по соседству","разрез лифта и узел парковки являются физической иерархией, а не потоковой сетью"]);g["grid"]={"axes":axes};text=page.get_text().strip();g["validation"].update({"source_layer_state":"text_available" if text else "no_pdf_text_layer","pdf_text_characters":len(text)});return g
def build_tx_graph(path:Path,*,block_id=None,profile_hint=None,subtype_hint=None):
 try:
  import fitz
  with fitz.open(str(path)) as d:return _dispatch(d[0],Path(path),block_id,profile_hint or classify_tx_profile(d[0].get_text()),subtype_hint or "технологический блок") if d.page_count==1 else None
 except:return None
def build_tx_graph_from_source(path:Path,*,page_index,bbox_norm,polygon_norm=None,block_id=None,profile_hint=None,subtype_hint=None):
 try:
  import fitz
  source=fitz.open(str(path));cropped=None
  try:
   sp=source[page_index];w,h=sp.rect.width,sp.rect.height;cr=fitz.Rect(bbox_norm[0]*w,bbox_norm[1]*h,bbox_norm[2]*w,bbox_norm[3]*h)&sp.rect;ur=cr*sp.derotation_matrix;ur.normalize();off=sp.cropbox_position;ur=fitz.Rect(ur.x0+off.x,ur.y0+off.y,ur.x1+off.x,ur.y1+off.y);cropped=fitz.open();cropped.insert_pdf(source,from_page=page_index,to_page=page_index);target=cropped[0]
   if polygon_norm:
    inv=~sp.transformation_matrix;pts=[tuple(fitz.Point(float(x)*w,float(y)*h)*sp.derotation_matrix*inv) for x,y in polygon_norm];_clip_copied_page(target,pts)
   target.set_cropbox(ur);return _dispatch(target,Path(path),block_id,profile_hint or classify_tx_profile(target.get_text()),subtype_hint or "технологический блок")
  finally:
   if cropped is not None:cropped.close()
   source.close()
 except:return None
def evaluate_tx_gate(g):
 if not g:return {"use":False,"complete":False,"reasons":["граф не построен"],"complete_reasons":[]}
 v=g["validation"];use=v.get("nodes_total",0)+v.get("physical_line_segments_total",0)>0;partial=[]
 if v.get("source_layer_state")=="no_pdf_text_layer":partial.append("нет текстового слоя PDF")
 v["description_depth"]="semantic_hierarchy" if g["profile_id"] in PLAN else "physical_hierarchy";g["readiness"]={"complete":not partial,"reasons":partial};return {"use":use,"complete":not partial,"reasons":[] if use else ["нет структуры"],"complete_reasons":partial,"metrics":v}
RU={PARKING:"План автостоянки",PARKING_DETAIL:"Узел автостоянки",LIFT_PLAN:"План лифтовых шахт",LIFT_SECTION:"Разрез лифта",LIFT_EQUIPMENT:"Оборудование лифта",LIFT_ASSIGNMENT:"Строительное задание лифта",WASTE_PLAN:"План мусороудаления",WASTE_DETAIL:"Узел мусороудаления"}
NR={"lift":"лифт","parking_space":"машиноместо","accessibility":"доступность МГН","parking_device":"оборудование автостоянки","shaft":"лифтовая шахта","lift_device":"оборудование лифта","capacity":"грузоподъёмность","speed":"скорость","elevation":"отметка","dimension":"размер","fire_rating":"огнестойкость","waste_device":"оборудование мусороудаления","raster_region":"растровая область"}
def render_tx_markdown(g):
 v=g["validation"];lines=[f"# Эталонная текстовая разметка ТХ: {RU.get(g['profile_id'],'Технологический блок')}","",f"**Источник:** {g['source']['pdf_file']}","","## 1. Краткий результат","",f"Узлов: {v.get('nodes_total',0)}; видов: {v.get('containers_total',0)}.","","### Технологическое дерево","","```text",RU.get(g['profile_id'],'блок ТХ').lower()];co=collections.Counter(n['node_type'] for n in g['nodes'])
 for i,(k,c) in enumerate(co.most_common(12)):lines.append(("└──" if i==min(len(co),12)-1 else "├──")+f" {NR.get(k,k)}: {c}")
 lines += ["```","","## 2. Состав",""];gr=collections.defaultdict(list)
 for n in g['nodes']:gr[n['node_type']].append(n['label'])
 for k,l in gr.items():lines.append(f"- **{NR.get(k,k)} — {len(l)}:** {', '.join(list(dict.fromkeys(l))[:30])}")
 lines += ["","## 3. Полнота и ограничения","","Доступная структура описана полностью." if g.get('readiness',{}).get('complete') else "Полнота ограничена источником."]+[f"- {x}" for x in g.get('warnings',[])];return "\n".join(lines)+"\n"
