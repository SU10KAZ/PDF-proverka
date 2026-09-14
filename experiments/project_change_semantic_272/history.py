"""Quarantine source revision histories, including unheaded continuations."""
from pathlib import Path
import re
import hashlib
import json
from collections import defaultdict

import fitz
from experiments.project_change_272.inventory import ROOT,sha,read,immutable


def pages_from_markdown(text):
    pages={}
    for match in re.finditer(r'^## Page (\d+)\s*\n(.*?)(?=^## Page \d+\s*$|\Z)',text,re.M|re.S):
        pages[int(match[1])]=match[2]
    return pages


def history_pages(text,embargo=()):
    excluded={}
    active=False
    for page,body in sorted(pages_from_markdown(text).items()):
        if page in embargo:
            # The supplied text has already been blanked. Never read through an
            # embargo just to identify a boundary; retain conservative state.
            continue
        normalized=re.sub(r'\s+',' ',body).casefold()
        rows=[[c.strip() for c in line.strip().strip('|').split('|')] for line in body.splitlines()
              if line.strip().startswith('|') and not re.fullmatch(r'[\s|:\-]+',line)]
        substantive=[r for r in rows if len(r)>=3 and sum(len(c) for c in r)>35]
        references=sum(bool(re.search(r'\b(?:тч|гч|том|лист\w*)\b',r[-1],re.I)) for r in substantive)
        header=('ранее разработан' in normalized and 'суть изменени' in normalized)
        title=bool(re.search(r'справка\s+о\s+(?:внесенн\w*\s+)?изменени',normalized))
        continuation=active and bool(substantive)
        orphan=len(substantive)>=2 and references/max(1,len(substantive))>=.4
        if header or title or continuation or orphan:
            excluded[page]='EXPLICIT_HISTORY' if header or title else 'CONTINUED_OR_LOCATOR_COLUMN_HISTORY'
            active=True
        elif body.strip():
            active=False
    return excluded


def grid_signature(page):
    """Internal long column boundaries, excluding sheet edges and the stamp."""
    vertical=defaultdict(list)
    width,height=page.rect.width,page.rect.height
    for drawing in page.get_drawings():
        for item in drawing['items']:
            if item[0]=='l':segments=[(item[1],item[2])]
            elif item[0]=='re':segments=[(item[1].tl,item[1].bl),(item[1].tr,item[1].br)]
            else:continue
            for a,b in segments:
                if abs(a.x-b.x)<.5 and .14*width<a.x<.92*width:
                    y0,y1=sorted([a.y,b.y]);y1=min(y1,.9*height)
                    if y1>y0:vertical[round(a.x,1)].append((y0,y1))
    xs=sorted(x/width for x,rows in vertical.items()
              if max(y for _,y in rows)-min(y for y,_ in rows)>.18*height)
    clusters=[]
    for x in xs:
        if clusters and x-clusters[-1][-1]<.004:clusters[-1].append(x)
        else:clusters.append([x])
    return [sum(c)/len(c) for c in clusters]


def same_history_grid(previous,current):
    return 2<=len(previous)<=5 and len(previous)==len(current) and all(abs(a-b)<.006 for a,b in zip(previous,current))


def document_history(document,embargo):
    key=hashlib.sha256(json.dumps(dict(sources=document['artifacts'],embargo=embargo,
        code_sha256=sha(__file__)),sort_keys=True).encode()).hexdigest()
    cache=ROOT/'semantic_v2/history_roles'/(key+'.json')
    if cache.exists():return {int(k):v for k,v in read(cache).items()}
    result=history_pages(Path(document['artifacts']['work_md']['path']).read_text(),embargo)
    previous=[]
    with fitz.open(document['artifacts']['pdf']['path']) as pdf:
        for number,page in enumerate(pdf,1):
            if number in embargo:continue
            if number in result:
                previous=grid_signature(page)
            elif previous:
                current=grid_signature(page)
                if same_history_grid(previous,current):
                    result[number]='NATIVE_PDF_CONTINUATION_OF_HISTORY_GRID'
                    previous=current
                else:previous=[]
    immutable(cache,result)
    return result
