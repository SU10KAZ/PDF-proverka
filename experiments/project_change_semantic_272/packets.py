"""DEV-only proposal packets. Retrieval is never an identity certificate."""
from collections import defaultdict
from pathlib import Path
import hashlib
import json
import re
import fitz
import signal
import inspect

from experiments.project_change_272.inventory import ROOT, read, immutable, now, sha
from experiments.project_change_272.policy import admitted_pairs
from experiments.project_change_272.revision_claims import claims, native_sources, retrieve, tokens
from .history import document_history, pages_from_markdown
from .access import prepared_pairs

BASE = ROOT / 'semantic_v2'
MAX_CHARS = 28000


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def normalize(text):
    return re.sub(r'\s+', ' ', text).strip()


def evidence(item, side):
    item = dict(item, side=side)
    item['evidence_id'] = side + '_' + digest(item)[:20]
    # A native label on a diagram proves only its explicit annotation.
    item['route'] = {'PDF_NATIVE_DRAWING_LABEL':'GRAPHIC', 'PDF_NATIVE_TABLE':'TABLE'}.get(item['source_kind'],'TEXT')
    return item


def sources(document, embargo):
    fingerprint=digest(dict(artifacts=document['artifacts'],version=document['document_version'],
        embargo=embargo,source_code=inspect.getsource(sources),native_code=inspect.getsource(native_sources),
        page_parser=inspect.getsource(pages_from_markdown),fitz_version=fitz.VersionBind))
    cache=BASE/'source_pools'/(fingerprint+'.json')
    if cache.exists():
        stored=read(cache)
        if stored['rows_sha256']!=digest(stored['rows']):raise ValueError('Native source cache drift')
        return stored['rows']
    rows=native_sources(document,embargo)
    regions=defaultdict(list)
    image_regions=defaultdict(list)
    drawing_pages=set()
    for b in read(document['artifacts']['blocks']['path'])['blocks']:
        if b.get('block_type')=='table':regions[b['page_index']+1].append(b['coords_norm'])
        if b.get('block_type')=='image':
            c=b['coords_norm']
            image_regions[b['page_index']+1].append(c)
            if (c[2]-c[0])*(c[3]-c[1])>=.2:drawing_pages.add(b['page_index']+1)
    from pathlib import Path
    md_pages=pages_from_markdown(Path(document['artifacts']['work_md']['path']).read_text())
    timed_out=set()
    with fitz.open(document['artifacts']['pdf']['path']) as pdf:
        # Legacy v002 marks some table-containing OCR parents as TEXT. Recover
        # actual ruled table geometry; reject outer sheet frames as tables.
        for number,body in md_pages.items():
            # Dense vector plans have thousands of intersecting lines and are
            # not a bounded table-detection problem. Their explicit labels
            # retain the GRAPHIC route; do not run grid detection on drawings.
            if number in embargo or number in drawing_pages or sum(line.startswith('|') for line in body.splitlines())<3:
                continue
            page=pdf[number-1]
            def expired(signum,frame):raise TimeoutError('Bounded grid detection expired')
            previous=signal.signal(signal.SIGALRM,expired)
            signal.setitimer(signal.ITIMER_REAL,2.0)
            try:
                found=page.find_tables(strategy='lines_strict')
                tables=found.tables if found is not None else []
                if found is None:timed_out.add(number)
            except TimeoutError:
                tables=[];timed_out.add(number)
            finally:
                signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous)
            for table in tables:
                box=fitz.Rect(table.bbox)
                if table.row_count>=2 and table.col_count>=2 and box.get_area()<page.rect.get_area()*.8:
                    regions[number].append([box.x0/page.rect.width,box.y0/page.rect.height,box.x1/page.rect.width,box.y1/page.rect.height])
        for row in rows:
            page=pdf[row['page']-1]
            row['bbox_native']=list(row['bbox'])
            box=fitz.Rect(row['bbox_native'])*page.rotation_matrix
            row['bbox']=list(box)
            row['pdf_rotation']=page.rotation
            row['coordinate_space']='DISPLAY_PDF_POINTS'
            graphic=any((box & fitz.Rect(c[0]*page.rect.width,c[1]*page.rect.height,
                    c[2]*page.rect.width,c[3]*page.rect.height)).get_area()>=box.get_area()*.7
                    for c in image_regions.get(row['page'],[]))
            row['source_kind']='PDF_NATIVE_DRAWING_LABEL' if graphic else 'PDF_NATIVE_TEXT'
            if row['page'] in timed_out:
                row['requires_visual_scope']=True
                row['layout_gap']='GRID_DETECTION_TIME_BOUND'
            overlaps=[(box & fitz.Rect(c[0]*page.rect.width,c[1]*page.rect.height,c[2]*page.rect.width,c[3]*page.rect.height)).get_area()/max(1,box.get_area()) for c in regions.get(row['page'],[])]
            if max(overlaps,default=0)>=.98:
                row['source_kind']='PDF_NATIVE_TABLE'
            elif max(overlaps,default=0)>.1:
                row['requires_visual_scope']=True
    immutable(cache,dict(fingerprint=fingerprint,rows_sha256=digest(rows),rows=rows))
    return rows


def bounded(rows, limit):
    out = []
    used = 0
    for row in rows:
        n = len(json.dumps(row, ensure_ascii=False))
        if n + used <= limit:
            out.append(row)
            used += n
    return out


def scope_context(pool,anchors,side):
    """Keep the anchor and bounded physical continuations from admitted pools.

    Missing adjacent pages stay missing: never jump across an embargo/history
    or load a fresh PDF page outside the already admitted source pool.
    """
    if not anchors:return []
    anchor=anchors[0]
    first=bounded([evidence(e,side) for e in pool if e['page']==anchor],7500)
    before=[evidence(e,side) for e in pool if e['page']==anchor-1]
    after=[evidence(e,side) for e in pool if e['page']==anchor+1]
    neighbors=list(reversed(bounded(list(reversed(before)),1500)))+bounded(after,1500)
    secondary=[evidence(e,side) for page in anchors[1:] for e in pool if e['page']==page]
    seen=set();out=[]
    for row in first+neighbors+secondary:
        if row['evidence_id'] not in seen:out.append(row);seen.add(row['evidence_id'])
    return bounded(out,11000)


def packet(pair, query, pools, kind, locator, include_continuations=False):
    # Complete native blocks only. Skipped blocks and retrieval truncation are
    # explicit; neither an incomplete packet nor a full page establishes absence.
    from .sheet_scopes import document_scopes
    frames={s:document_scopes(pair[s],pair['embargo_pages'][s]) for s in ['old','new']}
    selected = {};new_anchor=None
    for side in ['new', 'old']:
        eligible=pools[side];matched_pages=[]
        if side=='old' and new_anchor is not None:
            key=frames['new'].get(new_anchor,{}).get('purpose_key')
            if key:
                matched_pages=[n for n,r in frames['old'].items() if r.get('purpose_key')==key]
                if matched_pages:eligible=[r for r in eligible if r['page'] in matched_pages]
                else:
                    # Different drawing purposes are not the same inventory.
                    eligible=[r for r in eligible if not frames['old'].get(r['page'],{}).get('purpose_key')]
        ranked = retrieve(query, eligible, k=10)
        # Recover the source scope around the best anchors. Isolated matches
        # from many similar calculations can mix operating modes and omit the
        # declaration which gives a number its engineering owner.
        anchors=[]
        if side=='new' and kind=='CHANGED_NATIVE_SCOPE':anchors.append(locator['page'])
        if side=='new' and locator.get('new_anchor_page') is not None:anchors=[locator['new_anchor_page']]
        for row in ranked:
            page=row['evidence']['page']
            if page not in anchors:anchors.append(page)
            if len(anchors)>=2:break
        if not anchors and matched_pages:anchors=sorted(matched_pages)[:2]
        if side=='new' and anchors:new_anchor=anchors[0]
        if include_continuations:
            selected[side] = scope_context(pools[side],anchors,side)
        else:
            scoped=[r for page in anchors for r in pools[side] if r['page']==page]
            selected[side]=bounded([evidence(r,side) for r in scoped],11000)
    body = dict(pair_index=pair['index'], pair_key=pair['pair_key'], partition=pair['partition'],
                source_versions={s: pair[s]['document_version'] for s in ['old', 'new']},
                proposal_kind=kind, proposal_query=query[:3500], proposal_locator=locator,
                evidence=selected, coverage_complete=False,
                proposal_is_not_truth=True, scope_policy='same complete cipher pair; embargo pages removed')
    add_sheet_scopes(pair,body,frames)
    for _ in range(3):
        excess=len(json.dumps(body,ensure_ascii=False))-MAX_CHARS+100
        if excess<=0:break
        side=max(['old','new'],key=lambda s:len(json.dumps(body['evidence'][s],ensure_ascii=False)))
        size=sum(len(json.dumps(e,ensure_ascii=False)) for e in body['evidence'][side])
        body['evidence'][side]=bounded(body['evidence'][side],max(0,size-excess))
        body['truncated_for_scope_metadata']=True
    body['packet_id'] = digest(body)[:24]
    assert len(json.dumps(body, ensure_ascii=False)) <= MAX_CHARS
    return body


def add_sheet_scopes(pair,body,frames=None):
    from .sheet_scopes import document_scopes
    if frames is None:frames={s:document_scopes(pair[s],pair['embargo_pages'][s]) for s in ['old','new']}
    metadata={}
    for side in ['old','new']:
        pages=list(dict.fromkeys(e['page'] for e in body['evidence'][side]))
        titles=[];metadata[side]=[]
        for page in pages:
            frame=frames[side].get(page)
            if not frame:continue
            metadata[side].append(dict(page=page,status=frame['status'],title=frame.get('title','')[:220],purpose_key=frame.get('purpose_key')))
            for b in frame.get('title_blocks',[]):
                titles.append(evidence(dict(page=page,bbox=b['bbox_display'],bbox_native=b['bbox_native'],
                    pdf_rotation=frame['pdf_rotation'],coordinate_space='DISPLAY_PDF_POINTS',quote=b['quote'],
                    source_kind='PDF_NATIVE_DRAWING_LABEL',document_version=pair[side]['document_version'],
                    source_receipt=pair[side]['artifacts']['pdf'],scope_role='SHEET_TITLE'),side))
        seen=set();result=[]
        anchor=pages[0] if pages else None
        ordered=[e for e in titles if e['page']==anchor]+body['evidence'][side]+[e for e in titles if e['page']!=anchor]
        for e in ordered:
            if e['evidence_id'] not in seen:result.append(e);seen.add(e['evidence_id'])
        body['evidence'][side]=result
    body['sheet_scopes']=metadata


def prepare(name='dev_packets_v8_graphic_scopes', partition='DEV', candidate=None):
    pairs=prepared_pairs(partition,candidate)
    out = BASE / name
    immutable(out/'MANIFEST.json', dict(created_at=now(), split_sha256=sha(ROOT/'SPLIT.json'),
        partition=partition, code_sha256=sha(__file__), max_packet_characters=MAX_CHARS,
        history_role_code_sha256=sha(Path(__file__).with_name('history.py')),
        candidate_manifest=str(candidate) if candidate else None,
        source_policy='No archived answers or other projects; reserved partition requires matching frozen candidate',
        selection='All explicit first-page source claims and every eligible engineering native page; graphic pages remain eligible with unchanged labels; retrieval only'))
    counts = {}
    for pair in pairs:
        declared = claims(pair['new'])
        history={s:document_history(pair[s],pair['embargo_pages'][s]) for s in ['old','new']}
        immutable(out/'history_quarantine'/(str(pair['index'])+'.json'),history)
        pools = {s: [e for e in sources(pair[s], pair['embargo_pages'][s]) if e['page'] not in history[s]] for s in ['old', 'new']}
        packets = [packet(pair, c['old_state_claim_in_NEW']+'\n'+c['new_state_claim_in_NEW'], pools,
                          'NEW_SOURCE_CLAIM', dict(page=c['page'], line=c['markdown_line'])) for c in declared]
        old_texts = {normalize(e['quote']) for e in pools['old']}
        pages = defaultdict(list)
        for e in pools['new']:
            pages[e['page']].append(e)
        scored = []
        for page, rows in pages.items():
            graphic=any(e['source_kind']=='PDF_NATIVE_DRAWING_LABEL' for e in rows)
            changed = [e for e in rows if (graphic or normalize(e['quote']) not in old_texts) and len(e['quote'])>=(25 if graphic else 80)]
            query = '\n'.join(e['quote'] for e in changed)
            # Exclude title/admin pages using positive source content, not
            # project-specific known answers or validation evidence.
            if len(query)<(100 if graphic else 300) or len(tokens(query))<(8 if graphic else 35):
                continue
            if not graphic and not re.search(r'предусмотр|систем|оборудован|трубопровод|расход|площадь|нагруз|помещен',query,re.I):
                continue
            novelty = sum(len(e['quote']) for e in changed)/max(1,sum(len(e['quote']) for e in rows))
            scored.append((novelty, page, query))
        for novelty,page,query in sorted(scored,key=lambda r:(-r[0],r[1])):
            packets.append(packet(pair,query,pools,'CHANGED_NATIVE_SCOPE',dict(page=page,novelty=novelty)))
        for p in packets:
            immutable(out/'packets'/(p['packet_id']+'.json'),p)
        counts[pair['index']] = len(packets)
    immutable(out/'COUNTS.json',counts)
    print(out,counts,flush=True)
    return out


if __name__=='__main__':
    prepare()
