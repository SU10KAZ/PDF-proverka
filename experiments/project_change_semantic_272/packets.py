"""DEV-only proposal packets. Retrieval is never an identity certificate."""
from collections import defaultdict
import hashlib
import json
import re
import fitz

from experiments.project_change_272.inventory import ROOT, read, immutable, now, sha
from experiments.project_change_272.policy import admitted_pairs
from experiments.project_change_272.revision_claims import claims, native_sources, retrieve, tokens

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
    rows=native_sources(document,embargo)
    regions=defaultdict(list)
    for b in read(document['artifacts']['blocks']['path'])['blocks']:
        if b.get('block_type')=='table':regions[b['page_index']+1].append(b['coords_norm'])
    with fitz.open(document['artifacts']['pdf']['path']) as pdf:
        for row in rows:
            page=pdf[row['page']-1];box=fitz.Rect(row['bbox'])
            if any((box & fitz.Rect(c[0]*page.rect.width,c[1]*page.rect.height,c[2]*page.rect.width,c[3]*page.rect.height)).get_area()>=box.get_area()*.7 for c in regions.get(row['page'],[])):
                row['source_kind']='PDF_NATIVE_TABLE'
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


def packet(pair, query, pools, kind, locator):
    # Complete native blocks only. Skipped blocks and retrieval truncation are
    # explicit; neither an incomplete packet nor a full page establishes absence.
    selected = {}
    for side in ['old', 'new']:
        ranked = retrieve(query, pools[side], k=10)
        # Recover the source scope around the best anchors. Isolated matches
        # from many similar calculations can mix operating modes and omit the
        # declaration which gives a number its engineering owner.
        anchors=[]
        if side=='new' and kind=='CHANGED_NATIVE_SCOPE':anchors.append(locator['page'])
        for row in ranked:
            page=row['evidence']['page']
            if page not in anchors:anchors.append(page)
            if len(anchors)>=2:break
        scoped=[r for page in anchors for r in pools[side] if r['page']==page]
        selected[side] = bounded([evidence(r, side) for r in scoped], 11000)
    body = dict(pair_index=pair['index'], pair_key=pair['pair_key'], partition='DEV',
                source_versions={s: pair[s]['document_version'] for s in ['old', 'new']},
                proposal_kind=kind, proposal_query=query[:3500], proposal_locator=locator,
                evidence=selected, coverage_complete=False,
                proposal_is_not_truth=True, scope_policy='same complete cipher pair; embargo pages removed')
    body['packet_id'] = digest(body)[:24]
    assert len(json.dumps(body, ensure_ascii=False)) <= MAX_CHARS
    return body


def prepare(name='dev_packets_v3_scopes'):
    admitted_pairs('DEV')
    out = BASE / name
    immutable(out/'MANIFEST.json', dict(created_at=now(), split_sha256=sha(ROOT/'SPLIT.json'),
        partition='DEV', code_sha256=sha(__file__), max_packet_characters=MAX_CHARS,
        source_policy='No archived answers, other projects, VALIDATION, or FINAL_HOLDOUT',
        selection='All explicit first-page source claims plus up to four changed native page scopes per pair; retrieval only'))
    counts = {}
    for pair in read(ROOT/'sources/DEV/PAIRS.json'):
        declared = claims(pair['new'])
        claim_pages = {c['page'] for c in declared}
        pools = {s: sources(pair[s], pair['embargo_pages'][s]) for s in ['old', 'new']}
        pools['new'] = [e for e in pools['new'] if e['page'] not in claim_pages]
        packets = [packet(pair, c['old_state_claim_in_NEW']+'\n'+c['new_state_claim_in_NEW'], pools,
                          'NEW_SOURCE_CLAIM', dict(page=c['page'], line=c['markdown_line'])) for c in declared]
        old_texts = {normalize(e['quote']) for e in pools['old']}
        pages = defaultdict(list)
        for e in pools['new']:
            pages[e['page']].append(e)
        scored = []
        for page, rows in pages.items():
            changed = [e for e in rows if normalize(e['quote']) not in old_texts and len(e['quote'])>=80]
            query = '\n'.join(e['quote'] for e in changed)
            # Exclude title/admin pages using positive source content, not
            # project-specific known answers or validation evidence.
            if len(query)<300 or len(tokens(query))<35:
                continue
            if not re.search(r'предусмотр|систем|оборудован|трубопровод|расход|площадь|нагруз|помещен',query,re.I):
                continue
            novelty = sum(len(e['quote']) for e in changed)/max(1,sum(len(e['quote']) for e in rows))
            scored.append((novelty, page, query))
        for novelty,page,query in sorted(scored,key=lambda r:(-r[0],r[1]))[:4]:
            packets.append(packet(pair,query,pools,'CHANGED_NATIVE_SCOPE',dict(page=page,novelty=novelty)))
        for p in packets:
            immutable(out/'packets'/(p['packet_id']+'.json'),p)
        counts[pair['index']] = len(packets)
    immutable(out/'COUNTS.json',counts)
    print(out,counts,flush=True)
    return out


if __name__=='__main__':
    prepare()
