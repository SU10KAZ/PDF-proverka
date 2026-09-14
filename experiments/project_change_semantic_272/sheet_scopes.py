"""Source-only drawing-title inventory, bound to the document-code title block.

These are explicit scope witnesses for research, not an automatic OLD/NEW match.
Unknown/ambiguous titles remain unknown. Sheet numbers never establish identity.
"""
import re
import hashlib
import json
import fitz

from experiments.project_change_272.inventory import ROOT,read,immutable,sha,now
from .access import prepared_pairs
from .history import document_history
BASE=ROOT/'semantic_v2'


def code_key(text):
    # Same printed glyphs occur in mixed Latin/Cyrillic title-block codes.
    text=text.casefold().translate(str.maketrans('abcehkmoptxy','авсенкмортху'))
    return re.sub(r'[^a-zа-яё0-9]','',text)


TITLE=re.compile(r'\b(?:план|схема|разрез|фасад|спецификаци\w*|ведомост\w*|конструкци\w*|узлы|узел|развертк\w*|профил\w*)\b',re.I)


def title_scope(page,document):
    w,h=page.rect.width,page.rect.height
    if max(w,h)<1000:return None
    blocks=[]
    for native in page.get_text('blocks'):
        if native[6]!=0:continue
        # PyMuPDF native text coordinates are unrotated; page.rect and rendered
        # source images use the displayed orientation. Preserve both frames.
        display=fitz.Rect(native[:4])*page.rotation_matrix
        if display.x0>.5*w and display.y0>.65*h:
            blocks.append(tuple(display)+native[4:7]+(list(native[:4]),))
    codes=[b for b in blocks if code_key(document['document_code']) in code_key(b[4])]
    if not codes:return dict(status='UNKNOWN_NO_PINNED_DOCUMENT_CODE_IN_STAMP')
    anchor=max(codes,key=lambda b:b[1])
    possible=[b for b in blocks if b[1]>anchor[1] and TITLE.search(b[4])]
    if not possible:return dict(status='UNKNOWN_NO_TITLE_BELOW_DOCUMENT_CODE',code_quote=anchor[4].strip())
    start=max(possible,key=lambda b:b[1])
    # The repeated section name sits higher than the actual sheet title in the
    # admitted DEV stamps. Keep adjoining title lines, not adjacent signatures.
    width=max(1,start[2]-start[0])
    lines=sorted([b for b in blocks if start[1]-.5<=b[1]<=start[3]+32 and
                  min(b[2],start[2])-max(b[0],start[0])>.45*min(width,max(1,b[2]-b[0]))],key=lambda b:(b[1],b[0]))
    quote='\n'.join(b[4].strip() for b in lines)
    return dict(status='EXPLICIT_STAMP_TITLE_CANDIDATE',title=quote,code_quote=anchor[4].strip(),
        title_blocks=[dict(quote=b[4].strip(),bbox_display=list(b[:4]),bbox_native=b[7]) for b in lines],
        code_bbox_display=list(anchor[:4]),code_bbox_native=anchor[7],pdf_rotation=page.rotation,
        policy='Source scope candidate only; no entity or geometry match certified')


def purpose_key(title):
    # Exact explicit-purpose retrieval key. It deliberately is not an entity ID.
    lines=[line for line in title.splitlines() if not re.fullmatch(r'\s*\d{1,2}[./]\d{2,4}\s*',line)]
    text=' '.join(lines).casefold().replace('ё','е')
    text=re.sub(r'\b[мm]\s*1\s*:\s*\d+',' ',text)
    words=re.findall(r'[а-яa-z]+|[-+]?\d+(?:[.,]\d+)?',text)
    drop={'схема','план','на','в','м','m','отм','отметке','лист','листе','и'}
    return ' '.join(w[:8] if w.isalpha() else w.replace(',','.') for w in words if w not in drop)


def document_scopes(document,embargo):
    excluded=set(embargo)|set(document_history(document,embargo))
    key=hashlib.sha256(json.dumps(dict(pdf=document['artifacts']['pdf'],version=document['document_version'],
        code=document['document_code'],excluded=sorted(excluded),module_sha256=sha(__file__)),sort_keys=True).encode()).hexdigest()
    cache=BASE/'sheet_scope_cache'/(key+'.json')
    if cache.exists():return {int(k):v for k,v in read(cache).items()}
    rows={}
    with fitz.open(document['artifacts']['pdf']['path']) as pdf:
        for number,page in enumerate(pdf,1):
            if number in excluded:continue
            scope=title_scope(page,document)
            if scope is None:continue
            if scope.get('title'):scope['purpose_key']=purpose_key(scope['title'])
            rows[number]=dict(page=number,document_version=document['document_version'],
                source_receipt=document['artifacts']['pdf'],**scope)
    immutable(cache,rows)
    return rows


def inventory(name,partition='DEV',candidate=None):
    pairs=prepared_pairs(partition,candidate);out=BASE/'sheet_scopes'/name;rows=[]
    immutable(out/'MANIFEST.json',dict(created_at=now(),partition=partition,code_sha256=sha(__file__),
        split_sha256=sha(ROOT/'SPLIT.json'),candidate_manifest=str(candidate) if candidate else None,
        purpose='Read-only source titles; no state comparison or quality decision'))
    for pair in pairs:
        for side in ['old','new']:
            doc=pair[side]
            for number,scope in document_scopes(doc,pair['embargo_pages'][side]).items():
                rows.append(dict(pair_index=pair['index'],pair_key=pair['pair_key'],side=side,**scope))
    immutable(out/'SCOPES.json',rows)
    print(out,'drawing pages',len(rows),'explicit titles',sum(r['status']=='EXPLICIT_STAMP_TITLE_CANDIDATE' for r in rows),flush=True)
    return out


if __name__=='__main__':inventory('dev_v1')
