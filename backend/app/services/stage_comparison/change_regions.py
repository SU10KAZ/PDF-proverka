"""Этап 5Б: детерминированный пилот локальных областей изменений.

Работает ТОЛЬКО с matrix из ``sheet_alignment.json`` (V3 → V2).  Не запускает
повторное выравнивание, LLM/OCR и не пишет findings.  Все raw evidence остаются
в JSON, а области получаются только пространственной кластеризацией.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import cv2
import numpy as np

from .sheet_alignment import transform_bbox, transform_points

SCHEMA_VERSION = 1
PILOT_PAIRS = ((14, 13, "text_table", "спецификация с текстом и таблицей"), (8, 7, "vector", "векторный план с устойчивой геометрией"), (10, 9, "mixed", "план, таблица и текст на одном листе"))
_TOKEN = re.compile(r"[^0-9a-zа-яё]+", re.IGNORECASE)


def _norm(value: str) -> str:
    return _TOKEN.sub("", unicodedata.normalize("NFKC", value or "").lower().replace("ё", "е"))


def _box_union(boxes):
    rows = [box for box in boxes if box and len(box) == 4]
    return [min(box[0] for box in rows), min(box[1] for box in rows), max(box[2] for box in rows), max(box[3] for box in rows)] if rows else None


def _expand(box, gap): return [box[0] - gap, box[1] - gap, box[2] + gap, box[3] + gap]
def _intersects(a, b): return a[0] <= b[2] and b[0] <= a[2] and a[1] <= b[3] and b[1] <= a[3]
def _area(box): return max(0., box[2] - box[0]) * max(0., box[3] - box[1])
def _overlap(a, b):
    inter = [max(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), min(a[3], b[3])]
    return _area(inter) / max(_area(a), 1e-6)
def _center(box): return ((box[0] + box[2]) / 2, (box[1] + box[3]) / 2)
def _distance(a, b):
    ax, ay = _center(a); bx, by = _center(b); return math.hypot(ax - bx, ay - by)


def _words(page, matrix=None):
    result = []
    for index, row in enumerate(page.get_text("words") or []):
        x0, y0, x1, y1, value = row[:5]
        token = _norm(str(value))
        if not token: continue
        bbox = [float(x0), float(y0), float(x1), float(y1)]
        if matrix is not None: bbox = transform_bbox(matrix, bbox)
        result.append({"id": f"word_{index}", "text": str(value), "token": token, "bbox": bbox})
    return result


def compare_text(left_page, right_page, matrix) -> list[dict]:
    """Word-level evidence: exact, changed value, added/removed and moved."""
    left, right = _words(left_page), _words(right_page, matrix)
    by_token = defaultdict(list)
    for item in right: by_token[item["token"]].append(item)
    used, raw = set(), []
    unmatched_left = []
    # Exact tokens pair by nearest position. Micromovement <= 2.5 pt is noise.
    for item in left:
        choices = [candidate for candidate in by_token[item["token"]] if candidate["id"] not in used]
        if not choices: unmatched_left.append(item); continue
        candidate = min(choices, key=lambda other: _distance(item["bbox"], other["bbox"]))
        used.add(candidate["id"])
        if _distance(item["bbox"], candidate["bbox"]) > 2.5:
            raw.append({"kind": "text", "change": "moved", "bbox": _box_union([item["bbox"], candidate["bbox"]]), "left_ref": item["id"], "right_ref": candidate["id"], "left_value": item["text"], "right_value": candidate["text"]})
    unmatched_right = [item for item in right if item["id"] not in used]
    used_right = set()
    # Different word/value at same position is one changed evidence, not add+remove.
    for item in unmatched_left:
        choices = [candidate for candidate in unmatched_right if candidate["id"] not in used_right and _distance(item["bbox"], candidate["bbox"]) <= 18]
        if choices:
            candidate = min(choices, key=lambda other: _distance(item["bbox"], other["bbox"])); used_right.add(candidate["id"])
            raw.append({"kind": "text", "change": "changed", "bbox": _box_union([item["bbox"], candidate["bbox"]]), "left_ref": item["id"], "right_ref": candidate["id"], "left_value": item["text"], "right_value": candidate["text"]})
        else:
            raw.append({"kind": "text", "change": "removed", "bbox": item["bbox"], "left_ref": item["id"], "left_value": item["text"]})
    for item in unmatched_right:
        if item["id"] not in used_right:
            raw.append({"kind": "text", "change": "added", "bbox": item["bbox"], "right_ref": item["id"], "right_value": item["text"]})
    return raw


def _drawings(page, matrix=None):
    rows = []
    for index, drawing in enumerate(page.get_drawings() or []):
        rect = drawing.get("rect")
        if rect is None: continue
        box = [float(rect.x0), float(rect.y0), float(rect.x1), float(rect.y1)]
        if matrix is not None: box = transform_bbox(matrix, box)
        # Path type/count/line width resist PDF object order but not geometry changes.
        signature = (str(drawing.get("type") or ""), len(drawing.get("items") or []), round(float(drawing.get("width") or 0), 1))
        rows.append({"id": f"vector_{index}", "bbox": box, "signature": signature})
    return rows


def compare_vectors(left_page, right_page, matrix) -> list[dict]:
    """Primitive-level comparison with tolerant spatial matching after affine."""
    left, right = _drawings(left_page), _drawings(right_page, matrix)
    cell, grid = 35., defaultdict(list)
    for item in right:
        x, y = _center(item["bbox"]); grid[(int(x // cell), int(y // cell))].append(item)
    used, unmatched = set(), []
    for item in left:
        x, y = _center(item["bbox"]); key = (int(x // cell), int(y // cell)); candidates = []
        for ix in range(key[0] - 1, key[0] + 2):
            for iy in range(key[1] - 1, key[1] + 2):
                candidates.extend(candidate for candidate in grid[(ix, iy)] if candidate["id"] not in used and candidate["signature"] == item["signature"])
        if not candidates:
            unmatched.append(("removed", item)); continue
        candidate = min(candidates, key=lambda other: _distance(item["bbox"], other["bbox"]))
        tolerance = max(2.2, .015 * max(item["bbox"][2] - item["bbox"][0], item["bbox"][3] - item["bbox"][1], 1))
        if _distance(item["bbox"], candidate["bbox"]) <= tolerance or _overlap(_expand(item["bbox"], tolerance), candidate["bbox"]) > .9:
            used.add(candidate["id"])
        else:
            unmatched.append(("removed", item))
    for item in right:
        if item["id"] not in used: unmatched.append(("added", item))
    # Opposite unmatched primitives in the same local place are one geometry change.
    raw, consumed = [], set()
    for number, (change, item) in enumerate(unmatched):
        if number in consumed: continue
        if change != "removed":
            consumed.add(number); raw.append({"kind": "vector", "change": "added", "bbox": item["bbox"], "right_ref": item["id"]}); continue
        candidates = [(other_number, other) for other_number, (other_change, other) in enumerate(unmatched) if other_number not in consumed and other_change == "added" and _distance(item["bbox"], other["bbox"]) < 18]
        if candidates:
            other_number, other = min(candidates, key=lambda row: _distance(item["bbox"], row[1]["bbox"])); consumed.update({number, other_number})
            raw.append({"kind": "vector", "change": "changed", "bbox": _box_union([item["bbox"], other["bbox"]]), "left_ref": item["id"], "right_ref": other["id"]})
        else:
            consumed.add(number); raw.append({"kind": "vector", "change": change, "bbox": item["bbox"], ("left_ref" if change == "removed" else "right_ref"): item["id"]})
    return raw


def _canonical_segments(page, matrix=None):
    """Свести линии/прямоугольники PDF к order-independent отрезкам.

    Соседние коллинеарные части объединяются: ``одна линия ↔ несколько
    сегментов`` не создаёт diff. Кривые остаются fallback bbox через старый raw
    слой, чтобы не потерять маленький реальный контур.
    """
    rows=[]
    for drawing in page.get_drawings() or []:
        for item in drawing.get("items") or []:
            kind=str(item[0]) if item else ""
            if kind=="l": pairs=[(item[1],item[2])]
            elif kind=="re":
                r=item[1]; pairs=[((r.x0,r.y0),(r.x1,r.y0)),((r.x1,r.y0),(r.x1,r.y1)),((r.x1,r.y1),(r.x0,r.y1)),((r.x0,r.y1),(r.x0,r.y0))]
            else: pairs=[]
            for a,b in pairs:
                points=np.asarray([[float(a[0]),float(a[1])],[float(b[0]),float(b[1])]],dtype=float)
                if matrix is not None: points=transform_points(matrix,points)
                length=float(np.linalg.norm(points[1]-points[0]))
                if length>.15: rows.append(points)
    # deterministic merging of only virtually touching collinear segments
    rows.sort(key=lambda p:(round(min(p[0,0],p[1,0]),3),round(min(p[0,1],p[1,1]),3),round(max(p[0,0],p[1,0]),3),round(max(p[0,1],p[1,1]),3)))
    # Очень насыщенные листы содержат десятки тысяч path-items. Для них сначала
    # убираем точные export-дубли; составные линии всё равно проходят через
    # близкое matching ниже, без затратного O(n²) merge всех сегментов.
    if len(rows)>3000:
        unique={}
        for segment in rows:
            a,b=segment
            key=tuple(round(v/1.5) for point in sorted([(a[0],a[1]),(b[0],b[1])]) for v in point)
            unique.setdefault(key,segment)
        merged=list(unique.values())
        return [{"id":f"canonical_vector_{i}","bbox":[float(min(s[:,0])),float(min(s[:,1])),float(max(s[:,0])),float(max(s[:,1]))],"points":s.tolist()} for i,s in enumerate(merged)]
    merged=[]
    for segment in rows:
        direction=segment[1]-segment[0]; length=np.linalg.norm(direction); unit=direction/length
        # make orientation direction-independent
        if unit[0]<-1e-8 or (abs(unit[0])<1e-8 and unit[1]<0): unit=-unit; segment=segment[::-1]
        joined=False
        # После сортировки потенциально стыкующиеся сегменты находятся рядом;
        # ограничение окна защищает крупный чертёж от квадратичной сложности.
        start=max(0,len(merged)-32)
        for index,existing in enumerate(merged[start:],start):
            e_dir=existing[1]-existing[0]; e_len=np.linalg.norm(e_dir); e_unit=e_dir/e_len
            if abs(np.cross(unit,e_unit))>.012: continue
            if max(abs(np.cross(e_unit,segment[0]-existing[0])),abs(np.cross(e_unit,segment[1]-existing[0])))>2.2: continue
            projected=[np.dot(e_unit,p-existing[0]) for p in segment]
            lo,hi=min(0,*projected),max(e_len,*projected)
            # gap more than 3pt represents two independent short lines.
            if min(e_len,max(projected))-max(0,min(projected))>3: continue
            merged[index]=np.asarray([existing[0]+e_unit*lo,existing[0]+e_unit*hi]); joined=True; break
        if not joined: merged.append(segment)
    return [{"id":f"canonical_vector_{i}","bbox":[float(min(s[:,0])),float(min(s[:,1])),float(max(s[:,0])),float(max(s[:,1]))],"points":s.tolist()} for i,s in enumerate(merged)]


def compare_vectors_canonical(left_page,right_page,matrix):
    """Вернуть raw/canonical evidence + метрики очистки для 5Б.1."""
    raw=compare_vectors(left_page,right_page,matrix)
    left,right=_canonical_segments(left_page),_canonical_segments(right_page,matrix)
    cell,grid=45.,defaultdict(list)
    for item in right:
        x,y=_center(item["bbox"]);grid[(int(x//cell),int(y//cell))].append(item)
    used=set(); canonical=[]; equivalent=0
    # Exact quantized endpoint key is the cheap fast path and removes different
    # PDF object order / duplicated export before any spatial search.
    exact=defaultdict(list)
    for item in right:
        key=tuple(round(v/1.5) for point in sorted(item["points"]) for v in point); exact[key].append(item)
    for item in left:
        exact_key=tuple(round(v/1.5) for point in sorted(item["points"]) for v in point)
        same=next((candidate for candidate in exact[exact_key] if candidate["id"] not in used),None)
        if same is not None:
            used.add(same["id"]);equivalent+=1;continue
        x,y=_center(item["bbox"]); key=(int(x//cell),int(y//cell)); candidates=[]
        for ix in range(key[0]-1,key[0]+2):
            for iy in range(key[1]-1,key[1]+2): candidates += [v for v in grid[(ix,iy)] if v["id"] not in used]
        a=np.asarray(item["points"]); best=None;best_error=1e99
        for other in candidates:
            b=np.asarray(other["points"]); error=min(max(np.linalg.norm(a[0]-b[0]),np.linalg.norm(a[1]-b[1])),max(np.linalg.norm(a[0]-b[1]),np.linalg.norm(a[1]-b[0])))
            if error<best_error:best,best_error=other,error
        length=max(np.linalg.norm(a[1]-a[0]),1); tolerance=max(2.2,length*.006)
        if best is not None and best_error<=tolerance: used.add(best["id"]);equivalent+=1
        else: canonical.append({"kind":"vector","change":"removed","bbox":item["bbox"],"left_ref":item["id"]})
    for item in right:
        if item["id"] not in used: canonical.append({"kind":"vector","change":"added","bbox":item["bbox"],"right_ref":item["id"]})
    # Pair local opposite changes into modified segment evidence.
    consumed=set(); compact=[]
    for i,item in enumerate(canonical):
        if i in consumed:continue
        if item["change"]=="removed":
            candidates=[(j,o) for j,o in enumerate(canonical) if j not in consumed and o["change"]=="added" and _distance(item["bbox"],o["bbox"])<12]
            if candidates:
                j,other=min(candidates,key=lambda x:_distance(item["bbox"],x[1]["bbox"]));consumed|={i,j};compact.append({"kind":"vector","change":"changed","bbox":_box_union([item["bbox"],other["bbox"]]),"left_ref":item.get("left_ref"),"right_ref":other.get("right_ref")});continue
        consumed.add(i);compact.append(item)
    # Если path-level разложение оказалось детальнее исходного drawing diff,
    # не подменяем им результат. Компактируем уже подтверждённые raw evidence
    # пространственно: полный список остаётся в raw_vector_differences.
    if len(compact)>len(raw):
        groups=cluster_differences(raw,10000,10000)
        compact=[]
        for group in groups:
            source=[raw[int(ref.split("_")[1])] for ref in group["evidence_ids"]]
            changes=Counter(item["change"] for item in source)
            change=next(iter(changes)) if len(changes)==1 else "changed"
            compact.append({"kind":"vector","change":change,"bbox":group["bbox"],"raw_refs":group["evidence_ids"],"representative_ref":group["evidence_ids"][0]})
        equivalent=max(equivalent,max(0,len(raw)-len(compact)))
    return {"raw":raw,"canonical":compact,"metrics":{"raw_vector_primitives":len(raw),"canonical_input_left":len(left),"canonical_input_right":len(right),"matched_as_equivalent":equivalent,"removed_duplicate_or_export_noise":max(0,len(raw)-len(compact)),"canonical_added":sum(i["change"]=="added" for i in compact),"canonical_removed":sum(i["change"]=="removed" for i in compact),"canonical_modified":sum(i["change"]=="changed" for i in compact)}}


def _images(page, matrix=None):
    document = page.parent; rows = []
    for index, image in enumerate(page.get_images(full=True) or []):
        xref = int(image[0])
        try:
            pixels = hashlib.sha256(bytes(__import__("fitz").Pixmap(document, xref).samples)).hexdigest()
            for placement, rect in enumerate(page.get_image_rects(xref)):
                box = [float(rect.x0), float(rect.y0), float(rect.x1), float(rect.y1)]
                rows.append({"id": f"image_{index}_{placement}", "digest": pixels, "bbox": transform_bbox(matrix, box) if matrix is not None else box})
        except Exception: pass
    return rows


def compare_images(left_page, right_page, matrix) -> list[dict]:
    left, right, used, raw = _images(left_page), _images(right_page, matrix), set(), []
    for item in left:
        choices = [other for other in right if other["id"] not in used and _distance(item["bbox"], other["bbox"]) < 12]
        if not choices: raw.append({"kind":"image","change":"removed","bbox":item["bbox"],"left_ref":item["id"]}); continue
        other = min(choices, key=lambda row: _distance(item["bbox"], row["bbox"])); used.add(other["id"])
        if item["digest"] != other["digest"] or _distance(item["bbox"], other["bbox"]) > 2.5:
            raw.append({"kind":"image","change":"changed","bbox":_box_union([item["bbox"],other["bbox"]]),"left_ref":item["id"],"right_ref":other["id"]})
    raw.extend({"kind":"image","change":"added","bbox":item["bbox"],"right_ref":item["id"]} for item in right if item["id"] not in used)
    return raw


def _render(page, long_side=1400):
    import fitz
    scale = long_side / max(float(page.rect.width), float(page.rect.height), 1.)
    pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), colorspace=fitz.csGRAY, alpha=False)
    return np.frombuffer(pix.samples, np.uint8).reshape(pix.height, pix.width)


def _visual_unexplained(left_page, right_page, matrix, evidence):
    left, right = _render(left_page), _render(right_page)
    scale_left = np.array([[left.shape[1]/left_page.rect.width,0,0],[0,left.shape[0]/left_page.rect.height,0],[0,0,1.]])
    inv_right = np.array([[right_page.rect.width/right.shape[1],0,0],[0,right_page.rect.height/right.shape[0],0],[0,0,1.]])
    warped = cv2.warpAffine(right, (scale_left @ np.asarray(matrix) @ inv_right)[:2], (left.shape[1], left.shape[0]), borderValue=255)
    mask = cv2.absdiff(left, warped) > 45
    mask = cv2.morphologyEx(mask.astype(np.uint8), cv2.MORPH_OPEN, np.ones((2,2),np.uint8))
    count, _, stats, _ = cv2.connectedComponentsWithStats(mask, 8)
    raw = []
    for n in range(1, count):
        x,y,w,h,area = stats[n]
        if area < 35: continue
        box = [x*left_page.rect.width/left.shape[1], y*left_page.rect.height/left.shape[0], (x+w)*left_page.rect.width/left.shape[1], (y+h)*left_page.rect.height/left.shape[0]]
        if not any(_intersects(_expand(box, 8), item["bbox"]) for item in evidence):
            raw.append({"kind":"unexplained_visual_difference","change":"visual_only","bbox":box,"pixel_area":int(area)})
    return raw, left, warped


def cluster_differences(items, page_width, page_height):
    """Детерминированно склеить близкие evidence, но отдельно от штампа."""
    gap = max(12., math.hypot(page_width, page_height) * .004)
    roles = []
    for item in items:
        cx, cy = _center(item["bbox"]); roles.append("stamp" if cx > page_width*.63 and cy > page_height*.64 else "drawing")
    parent = list(range(len(items)))
    def find(i):
        while parent[i] != i: parent[i] = parent[parent[i]]; i = parent[i]
        return i
    def join(i,j):
        i,j=find(i),find(j)
        if i != j: parent[j]=i
    grid=defaultdict(list); cell=max(gap*2,1)
    for i,item in enumerate(items):
        box=_expand(item["bbox"],gap); x0,y0,x1,y1=(int(v//cell) for v in box)
        for x in range(x0,x1+1):
            for y in range(y0,y1+1):
                for j in grid[(x,y)]:
                    if roles[i]==roles[j] and _intersects(_expand(item["bbox"],gap),items[j]["bbox"]): join(i,j)
                grid[(x,y)].append(i)
    grouped=defaultdict(list)
    for i,item in enumerate(items): grouped[find(i)].append((i,item))
    regions=[]
    for serial, rows in enumerate(sorted(grouped.values(), key=lambda rows: (_box_union([x[1]["bbox"] for x in rows])[1], _box_union([x[1]["bbox"] for x in rows])[0])),1):
        evidence=[item for _,item in rows]; bbox=_expand(_box_union([item["bbox"] for item in evidence]),6)
        counts=Counter(f"{item['kind']}_{item['change']}" for item in evidence)
        kinds=sorted({item["kind"] for item in evidence if item["kind"]!="unexplained_visual_difference"}) or ["visual"]
        regions.append({"region_id":f"region_{serial:03d}","bbox":[round(v,3) for v in bbox],"change_types":kinds,"region_role":roles[rows[0][0]],"diff_counts":dict(sorted(counts.items())),"evidence_ids":[f"raw_{i:04d}" for i,_ in rows],"strength":round(min(1.,len(evidence)/12),3),"confidence":round(.65+min(.3,len(evidence)*.02),3)})
    return regions


def _blocks_for_region(region, left_page, right_page, matrix):
    for side,page,transform in (("left",left_page,None),("right",right_page,matrix)):
        values=[]
        for block in page.get("blocks") or []:
            box=block.get("bbox_pdf_visual")
            if box:
                mapped=transform_bbox(transform,box) if transform is not None else box; overlap=_overlap(region["bbox"],mapped)
                if overlap >= .02: values.append({"block_id":block.get("block_id"),"overlap":round(overlap,4)})
                # Большой region может лишь касаться основной надписи; не даём
                # этому перекрасить весь чертёж в stamp. Нужен центр в штампе
                # либо существенное покрытие самой области.
                cx, cy = _center(region["bbox"])
                if block.get("type")=="stamp" and ((mapped[0] <= cx <= mapped[2] and mapped[1] <= cy <= mapped[3]) or _overlap(region["bbox"], mapped) >= .55): region["region_role"]="stamp"
        region[f"{side}_block_ids"]=[row["block_id"] for row in values]; region[f"{side}_block_overlap"]=values


def _diagnostic_images(output_dir, stem, left, warped, raw, regions, width, height):
    output_dir.mkdir(parents=True,exist_ok=True); base=cv2.cvtColor(left,cv2.COLOR_GRAY2BGR)
    def point(box): return (int(box[0]*left.shape[1]/width),int(box[1]*left.shape[0]/height)),(int(box[2]*left.shape[1]/width),int(box[3]*left.shape[0]/height))
    raw_image=base.copy()
    for item in raw:
        p,q=point(item["bbox"]); cv2.rectangle(raw_image,p,q,(0,0,255),1)
    regions_image=base.copy(); overlay=cv2.addWeighted(base,.5,cv2.cvtColor(warped,cv2.COLOR_GRAY2BGR),.5,0)
    for index,region in enumerate(regions,1):
        color=(0,128,255) if region["region_role"]=="stamp" else (255,0,0); p,q=point(region["bbox"])
        for image in (regions_image,overlay): cv2.rectangle(image,p,q,color,2); cv2.putText(image,f"R{index}",(p[0],max(12,p[1]-4)),cv2.FONT_HERSHEY_SIMPLEX,.45,color,1,cv2.LINE_AA)
    paths={"raw_diff":output_dir/f"{stem}_raw.png","regions":output_dir/f"{stem}_regions.png","overlay_regions":output_dir/f"{stem}_overlay_regions.png"}
    cv2.imwrite(str(paths["raw_diff"]),raw_image);cv2.imwrite(str(paths["regions"]),regions_image);cv2.imwrite(str(paths["overlay_regions"]),overlay)
    return {key:str(value) for key,value in paths.items()}


def analyze_pair(left_pdf,right_pdf,left_prepared,right_prepared,alignment,*,canonical_vectors=False):
    import fitz
    matrix=np.asarray(alignment["transform"]["matrix"],dtype=float); lp,rp=int(alignment["left_page"]),int(alignment["right_page"])
    left_doc,right_doc=fitz.open(str(left_pdf)),fitz.open(str(right_pdf))
    try:
        left,right=left_doc[lp-1],right_doc[rp-1]
        vector_detail=compare_vectors_canonical(left,right,matrix) if canonical_vectors else {"raw":compare_vectors(left,right,matrix),"canonical":None,"metrics":None}
        raw=compare_text(left,right,matrix)+(vector_detail["canonical"] if canonical_vectors else vector_detail["raw"])+compare_images(left,right,matrix)
        # Линия может иметь нулевую площадь bbox, но всё равно быть реальным
        # изменением. Вектор не отбрасываем только из-за толщины линии.
        filtered=[item for item in raw if _area(item["bbox"]) >= .05 or item["kind"] in {"text","vector"}]
        visual,left_image,warped=_visual_unexplained(left,right,matrix,filtered); filtered+=visual
        regions=cluster_differences(filtered,float(left.rect.width),float(left.rect.height))
        left_meta=next((page for page in left_prepared.get("pages") or [] if int(page.get("pdf_page") or 0)==lp),{})
        right_meta=next((page for page in right_prepared.get("pages") or [] if int(page.get("pdf_page") or 0)==rp),{})
        for region in regions:_blocks_for_region(region,left_meta,right_meta,matrix)
        for index,item in enumerate(filtered): item["evidence_id"]=f"raw_{index:04d}"
        return {"left_page":lp,"right_page":rp,"alignment_status":alignment["status"],"alignment_matrix":alignment["transform"]["matrix"],"raw_differences":filtered,"raw_vector_differences":vector_detail["raw"],"canonical_vector_differences":vector_detail["canonical"],"vector_metrics":vector_detail["metrics"],"regions":regions,"summary":{"raw":len(raw),"after_noise_filter":len(filtered),"regions":len(regions),"by_type":dict(Counter(item["kind"] for item in filtered)),"unexplained_visual_difference":len(visual)},"_images":(left_image,warped,float(left.rect.width),float(left.rect.height))}
    finally:left_doc.close();right_doc.close()


def run_pilot(left_pdf,right_pdf,left_prepared,right_prepared,alignment_report,directory,*,canonical_vectors=False):
    directory=Path(directory); diagnostics=directory/"diagnostics"; items=[]; selected=[]
    by_pair={(int(row["left_page"]),int(row["right_page"])):row for row in alignment_report.get("items") or []}
    for left,right,kind,reason in PILOT_PAIRS:
        alignment=by_pair.get((left,right))
        if not alignment or alignment.get("status")!="aligned": continue
        result=analyze_pair(left_pdf,right_pdf,left_prepared,right_prepared,alignment,canonical_vectors=canonical_vectors); images=result.pop("_images")
        result["pilot_kind"],result["selection_reason"]=kind,reason
        result["diagnostics"]=_diagnostic_images(diagnostics,f"v2_{left:03d}_v3_{right:03d}",images[0],images[1],result["raw_differences"],result["regions"],images[2],images[3])
        items.append(result); selected.append({"left_page":left,"right_page":right,"kind":kind,"reason":reason})
    return {"schema_version":SCHEMA_VERSION,"kind":"stage_comparison_change_regions_pilot","settings":{"llm_used":False,"ocr_rerun":False,"findings_created":False,"alignment_recomputed":False,"source_coordinate_system":"V2 PDF; V3 transformed by Stage 5A matrix"},"pilot_selection":selected,"items":items,"summary":{"pairs":len(items),"raw_differences":sum(x["summary"]["raw"] for x in items),"after_noise_filter":sum(x["summary"]["after_noise_filter"] for x in items),"regions":sum(x["summary"]["regions"] for x in items),"by_type":dict(Counter(kind for item in items for kind,count in item["summary"]["by_type"].items() for _ in range(count)))}}


def _write(path,payload):
    path.parent.mkdir(parents=True,exist_ok=True); fd,tmp=tempfile.mkstemp(prefix=path.name,suffix=".tmp",dir=path.parent)
    try:
        with os.fdopen(fd,"w",encoding="utf-8") as out:out.write(payload);out.flush();os.fsync(out.fileno())
        os.replace(tmp,path)
    except BaseException:
        try:os.unlink(tmp)
        except OSError:pass
        raise


def write_report(directory,report):
    directory=Path(directory); json_path,md_path=directory/"change_regions.json",directory/"change_regions.md"; _write(json_path,json.dumps(report,ensure_ascii=False,indent=2,sort_keys=True)+"\n")
    lines=["# Пилот локальных областей изменений", "", "Только диагностический результат: карта листов, alignment и existing findings не менялись.", ""]
    for item in report["items"]:
        summary=item["summary"]; lines += [f"## V2 {item['left_page']} ↔ V3 {item['right_page']} — {item['pilot_kind']}", "", f"Выбор: {item['selection_reason']}.", "", f"Raw differences: {summary['raw']}; после noise filter: {summary['after_noise_filter']}; регионов: {summary['regions']}.", "", "| Region | Role | Types | Bbox V2 | Evidence |", "| --- | --- | --- | --- | ---: |"]
        metrics=item.get("vector_metrics")
        if metrics:
            lines[ -2:-2 ] = ["", f"Vector: raw {metrics['raw_vector_primitives']} → canonical {metrics['canonical_added'] + metrics['canonical_removed'] + metrics['canonical_modified']}; equivalent {metrics['matched_as_equivalent']}; compacted/export noise {metrics['removed_duplicate_or_export_noise']}.", ""]
        for region in item["regions"]: lines.append(f"| {region['region_id']} | {region['region_role']} | {', '.join(region['change_types'])} | {region['bbox']} | {len(region['evidence_ids'])} |")
        paths=item["diagnostics"]; lines += ["", f"[raw diff]({os.path.relpath(paths['raw_diff'],directory)}) · [regions]({os.path.relpath(paths['regions'],directory)}) · [overlay]({os.path.relpath(paths['overlay_regions'],directory)})", ""]
    _write(md_path,"\n".join(lines));return json_path,md_path


def rebuild_regions_after_canonical(cleanup_report,left_prepared,right_prepared):
    """5Б.2: regions строятся заново только из canonical/text/image evidence.

    Long canonical primitives остаются supporting evidence, но не растягивают
    bbox всей страницы без локального подтверждения.
    """
    items=[]
    left_map={int(p.get("pdf_page")):p for p in left_prepared.get("pages") or []}
    right_map={int(p.get("pdf_page")):p for p in right_prepared.get("pages") or []}
    for old in cleanup_report.get("items") or []:
        lp,rp=int(old["left_page"]),int(old["right_page"]); page=left_map.get(lp) or {}; size=page.get("page_size") or {}; w,h=float(size.get("width") or 1),float(size.get("height") or 1)
        non_vector=[x for x in old.get("raw_differences") or [] if x.get("kind")!="vector"]
        vector=old.get("canonical_vector_differences") or []; seeds=[]; supporting=[]
        for evidence in non_vector:
            # Page-wide placed image / export layer is confirmation, not a
            # local bbox seed; otherwise it hides table/cell-level changes.
            diagonal=math.hypot(evidence["bbox"][2]-evidence["bbox"][0],evidence["bbox"][3]-evidence["bbox"][1])
            if evidence.get("kind")=="text" and evidence.get("change")=="moved" and diagonal>60: supporting.append(dict(evidence,supporting_reason="ambiguous_repeated_text_anchor"))
            elif _area(evidence["bbox"])/max(w*h,1)>.18: supporting.append(dict(evidence,supporting_reason="page_spanning_nonvector_layer"))
            else: seeds.append(evidence)
        for evidence in vector:
            ratio=_area(evidence["bbox"])/max(w*h,1)
            if ratio>.18: supporting.append(dict(evidence,supporting_reason="long_or_page_spanning_primitive"))
            else: seeds.append(evidence)
        regions=cluster_differences(seeds,w,h)
        matrix=old.get("alignment_matrix")
        for region in regions:
            region["page_area_ratio"]=round(_area(region["bbox"])/max(w*h,1),6)
            _blocks_for_region(region,page,right_map.get(rp) or {},matrix)
        items.append({"left_page":lp,"right_page":rp,"evidence":seeds,"supporting_vector_evidence":supporting,"regions":regions,"before_regions":old.get("regions") or [],"summary":{"regions_before":len(old.get("regions") or []),"regions_after":len(regions),"supporting_long_vectors":len(supporting)}})
    return {"schema_version":SCHEMA_VERSION,"kind":"stage_comparison_change_regions_rebuilt_after_canonical","settings":{"llm_used":False,"findings_created":False,"clustering_input":"text+image+canonical_vector_only","long_vectors":"supporting_not_bbox_seed"},"items":items,"summary":{"pairs":len(items),"regions_before":sum(i["summary"]["regions_before"] for i in items),"regions_after":sum(i["summary"]["regions_after"] for i in items)}}
